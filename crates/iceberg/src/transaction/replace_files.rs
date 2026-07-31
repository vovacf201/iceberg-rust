// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::Arc;

use uuid::Uuid;

use super::snapshot::{DefaultManifestProcess, MergeManifestProcess, SnapshotProducer};
use super::{
    MANIFEST_MERGE_ENABLED, MANIFEST_MERGE_ENABLED_DEFAULT, MANIFEST_MIN_MERGE_COUNT,
    MANIFEST_MIN_MERGE_COUNT_DEFAULT, MANIFEST_TARGET_SIZE_BYTES,
    MANIFEST_TARGET_SIZE_BYTES_DEFAULT,
};
use crate::error::Result;
use crate::spec::{
    DataContentType, DataFile, ManifestEntry, ManifestFile, ManifestStatus, Operation,
};
use crate::table::Table;
use crate::transaction::snapshot::SnapshotProduceOperation;
use crate::transaction::{ActionCommit, TransactionAction};

/// Which snapshot [`Operation`] a file replacement records.
///
/// `rewrite_files` and `overwrite_files` differ only in this value
pub(crate) trait ReplaceFilesMode: Send + Sync + 'static {
    const OPERATION: Operation;
}

/// Files were added and removed without changing table data (compaction,
/// changing file format, relocating files).
pub struct Rewrite;

/// Files were added and removed in a logical overwrite.
pub struct Overwrite;

impl ReplaceFilesMode for Rewrite {
    const OPERATION: Operation = Operation::Replace;
}

impl ReplaceFilesMode for Overwrite {
    const OPERATION: Operation = Operation::Overwrite;
}

/// A blanket `impl<M: ReplaceFilesMode> SnapshotProduceOperation for M` would
/// collide with `impl SnapshotProduceOperation for FastAppendOperation`: the
/// compiler cannot prove `FastAppendOperation` will never implement
/// `ReplaceFilesMode`. This wrapper carries the shared implementation instead.
pub(crate) struct ReplaceFilesOperation<M: ReplaceFilesMode>(PhantomData<M>);

impl<M: ReplaceFilesMode> ReplaceFilesOperation<M> {
    pub(crate) fn new() -> Self {
        Self(PhantomData)
    }
}

impl<M: ReplaceFilesMode> SnapshotProduceOperation for ReplaceFilesOperation<M> {
    fn operation(&self) -> Operation {
        M::OPERATION
    }

    async fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        // generate delete manifest entries from removed files
        let snapshot = snapshot_produce
            .table
            .metadata()
            .snapshot_for_ref(snapshot_produce.target_branch());

        if let Some(snapshot) = snapshot {
            let gen_manifest_entry = |old_entry: &Arc<ManifestEntry>| {
                let builder = ManifestEntry::builder()
                    .status(ManifestStatus::Deleted)
                    .snapshot_id(old_entry.snapshot_id().unwrap())
                    .sequence_number(old_entry.sequence_number().unwrap())
                    .file_sequence_number(old_entry.file_sequence_number().unwrap())
                    .data_file(old_entry.data_file().clone());

                builder.build()
            };

            let manifest_list = snapshot
                .load_manifest_list(
                    snapshot_produce.table.file_io(),
                    snapshot_produce.table.metadata(),
                )
                .await?;

            let mut deleted_entries = Vec::new();

            for manifest_file in manifest_list.entries() {
                let manifest = manifest_file
                    .load_manifest(snapshot_produce.table.file_io())
                    .await?;

                for entry in manifest.entries() {
                    if entry.content_type() == DataContentType::Data
                        && snapshot_produce
                            .removed_data_file_paths
                            .contains(entry.data_file().file_path())
                    {
                        deleted_entries.push(gen_manifest_entry(entry));
                    }

                    if (entry.content_type() == DataContentType::PositionDeletes
                        || entry.content_type() == DataContentType::EqualityDeletes)
                        && snapshot_produce
                            .removed_delete_file_paths
                            .contains(entry.data_file().file_path())
                    {
                        deleted_entries.push(gen_manifest_entry(entry));
                    }
                }
            }

            Ok(deleted_entries)
        } else {
            Ok(vec![])
        }
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &mut SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let table_metadata_ref = snapshot_produce.table.metadata();
        let file_io_ref = snapshot_produce.table.file_io();

        let Some(snapshot) = snapshot_produce
            .table
            .metadata()
            .snapshot_for_ref(snapshot_produce.target_branch())
        else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(file_io_ref, table_metadata_ref)
            .await?;

        let mut existing_files = Vec::new();

        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(file_io_ref).await?;

            let found_deleted_files: HashSet<_> = manifest
                .entries()
                .iter()
                .filter_map(|entry| {
                    if snapshot_produce
                        .removed_data_file_paths
                        .contains(entry.data_file().file_path())
                        || snapshot_produce
                            .removed_delete_file_paths
                            .contains(entry.data_file().file_path())
                    {
                        Some(entry.data_file().file_path().to_string())
                    } else {
                        None
                    }
                })
                .collect();

            if found_deleted_files.is_empty() {
                existing_files.push(manifest_file.clone());
            } else {
                // Rewrite the manifest file without the deleted data files
                let survives = |entry: &ManifestEntry| {
                    entry.is_alive() && !found_deleted_files.contains(entry.data_file().file_path())
                };

                if manifest.entries().iter().any(|entry| survives(entry)) {
                    let mut manifest_writer = snapshot_produce.new_manifest_writer(
                        manifest_file.content,
                        manifest_file.partition_spec_id,
                    )?;

                    for entry in manifest.entries() {
                        // Carry survivors forward as `Existing`: `add_entry` would
                        // restamp them as `Added` under the new snapshot and drop
                        // their file sequence number.
                        if survives(entry) {
                            manifest_writer.add_existing_entry((**entry).clone())?;
                        }
                    }

                    existing_files.push(manifest_writer.write_manifest_file().await?);
                }
            }
        }

        Ok(existing_files)
    }
}

/// Transaction action that replaces one set of files with another.
///
/// `M` is sealed to [`Rewrite`] and [`Overwrite`] via the [`RewriteFilesAction`] /
/// [`OverwriteFilesAction`] type aliases below; `ReplaceFilesMode` itself stays
/// `pub(crate)` so no other type can be substituted for `M`.
#[allow(private_bounds)]
pub struct ReplaceFilesAction<M: ReplaceFilesMode> {
    target_size_bytes: u32,
    min_count_to_merge: u32,
    merge_enabled: bool,

    // below are properties used to create SnapshotProducer when commit
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    removed_data_files: Vec<DataFile>,
    removed_delete_files: Vec<DataFile>,
    snapshot_id: Option<i64>,
    new_data_file_sequence_number: Option<i64>,
    target_branch: Option<String>,
    enable_delete_filter_manager: bool,
    check_file_existence: bool,
    validate_from_snapshot_id: Option<i64>,

    _mode: PhantomData<M>,
}

/// Rewrites files without changing table data — compaction and friends.
pub type RewriteFilesAction = ReplaceFilesAction<Rewrite>;

/// Rewrites files as a logical overwrite.
pub type OverwriteFilesAction = ReplaceFilesAction<Overwrite>;

#[allow(private_bounds)]
impl<M: ReplaceFilesMode> ReplaceFilesAction<M> {
    pub fn new() -> Self {
        Self {
            target_size_bytes: MANIFEST_TARGET_SIZE_BYTES_DEFAULT,
            min_count_to_merge: MANIFEST_MIN_MERGE_COUNT_DEFAULT,
            merge_enabled: MANIFEST_MERGE_ENABLED_DEFAULT,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::new(),
            added_data_files: Vec::new(),
            added_delete_files: Vec::new(),
            removed_data_files: Vec::new(),
            removed_delete_files: Vec::new(),
            snapshot_id: None,
            new_data_file_sequence_number: None,
            target_branch: None,
            enable_delete_filter_manager: false,
            check_file_existence: false,
            validate_from_snapshot_id: None,
            _mode: PhantomData,
        }
    }

    /// Add data files to the snapshot.
    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        for file in data_files {
            match file.content_type() {
                DataContentType::Data => self.added_data_files.push(file),
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                    self.added_delete_files.push(file)
                }
            }
        }

        self
    }

    /// Add remove files to the snapshot.
    pub fn delete_files(mut self, remove_data_files: impl IntoIterator<Item = DataFile>) -> Self {
        for file in remove_data_files {
            match file.content_type() {
                DataContentType::Data => self.removed_data_files.push(file),
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                    self.removed_delete_files.push(file)
                }
            }
        }

        self
    }

    pub fn set_snapshot_properties(&mut self, properties: HashMap<String, String>) -> &mut Self {
        let target_size_bytes: u32 = properties
            .get(MANIFEST_TARGET_SIZE_BYTES)
            .and_then(|s| s.parse().ok())
            .unwrap_or(MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
        let min_count_to_merge: u32 = properties
            .get(MANIFEST_MIN_MERGE_COUNT)
            .and_then(|s| s.parse().ok())
            .unwrap_or(MANIFEST_MIN_MERGE_COUNT_DEFAULT);
        let merge_enabled = properties
            .get(MANIFEST_MERGE_ENABLED)
            .and_then(|s| s.parse().ok())
            .unwrap_or(MANIFEST_MERGE_ENABLED_DEFAULT);

        self.target_size_bytes = target_size_bytes;
        self.min_count_to_merge = min_count_to_merge;
        self.merge_enabled = merge_enabled;
        self.snapshot_properties = properties;

        self
    }

    /// Set commit UUID for the snapshot.
    pub fn set_commit_uuid(&mut self, commit_uuid: Uuid) -> &mut Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set key metadata for manifest files.
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    /// Set snapshot id
    pub fn set_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    /// Enable delete filter manager for this snapshot.
    /// By default, delete filter manager is disabled.
    pub fn set_enable_delete_filter_manager(mut self, enable_delete_filter_manager: bool) -> Self {
        self.enable_delete_filter_manager = enable_delete_filter_manager;
        self
    }

    pub fn set_target_branch(mut self, target_branch: String) -> Self {
        self.target_branch = Some(target_branch);
        self
    }

    // If the compaction should use the sequence number of the snapshot at compaction start time for
    // new data files, instead of using the sequence number of the newly produced snapshot.
    // This avoids commit conflicts with updates that add newer equality deletes at a higher sequence number.
    pub fn set_new_data_file_sequence_number(mut self, seq: i64) -> Self {
        self.new_data_file_sequence_number = Some(seq);
        self
    }

    pub fn set_check_file_existence(mut self, check: bool) -> Self {
        self.check_file_existence = check;
        self
    }

    /// Validate that no delete file targeting a removed data file was committed after
    /// `snapshot_id`, which should be the snapshot this operation read.
    ///
    /// An operation that rewrites data files materializes the deletes that applied at the snapshot
    /// it read. If a concurrent writer adds deletes for one of the data files being removed, those
    /// deletes are never materialized, and removing the data file retires the new delete file as
    /// dangling — resurrecting the rows it deleted. Setting this makes the commit fail instead, so
    /// the operation can be retried against the current snapshot.
    ///
    /// Only delete files that record `referenced_data_file` are covered; see
    /// [`SnapshotProducer::validate_no_new_deletes_for_data_files`].
    pub fn validate_from_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.validate_from_snapshot_id = Some(snapshot_id);
        self
    }
}

#[async_trait::async_trait]
impl<M: ReplaceFilesMode> TransactionAction for ReplaceFilesAction<M> {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let mut snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_id,
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
            self.added_delete_files.clone(),
            self.removed_data_files.clone(),
            self.removed_delete_files.clone(),
        );

        if let Some(seq) = self.new_data_file_sequence_number {
            snapshot_producer.set_new_data_file_sequence_number(seq);
        }

        if let Some(branch) = &self.target_branch {
            snapshot_producer.set_target_branch(branch.clone());
        }

        if self.enable_delete_filter_manager {
            snapshot_producer.enable_delete_filter_manager();
        }

        if self.check_file_existence {
            snapshot_producer.validate_data_file_changes().await?;
        }

        if let Some(starting_snapshot_id) = self.validate_from_snapshot_id {
            snapshot_producer
                .validate_no_new_deletes_for_data_files(starting_snapshot_id)
                .await?;
        }

        if self.merge_enabled {
            let process =
                MergeManifestProcess::new(self.target_size_bytes, self.min_count_to_merge);
            snapshot_producer
                .commit(ReplaceFilesOperation::<M>::new(), process)
                .await
        } else {
            snapshot_producer
                .commit(ReplaceFilesOperation::<M>::new(), DefaultManifestProcess)
                .await
        }
    }
}

impl<M: ReplaceFilesMode> Default for ReplaceFilesAction<M> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use uuid::Uuid;

    use super::{Overwrite, ReplaceFilesMode, ReplaceFilesOperation, Rewrite};
    use crate::delete_file_index::FIELD_ID_POSITIONAL_DELETE_FILE_PATH;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, Literal, MAIN_BRANCH,
        ManifestContentType, ManifestListWriter, ManifestStatus, ManifestWriterBuilder, Operation,
        Snapshot, SnapshotReference, SnapshotRetention, Struct, Summary,
    };
    use crate::table::Table;
    use crate::transaction::snapshot::{SnapshotProduceOperation, SnapshotProducer};
    use crate::transaction::tests::{
        PARENT_SEQUENCE_NUMBER, PARENT_SNAPSHOT_ID, REMOVED_DELETE_FILE, RETAINED_DELETE_FILE,
        make_v2_table_with_delete_manifest, position_delete_file,
    };

    /// The snapshot a concurrent writer adds *after* the one a rewrite read.
    const NEW_SNAPSHOT_ID: i64 = 43;
    const NEW_SEQUENCE_NUMBER: i64 = PARENT_SEQUENCE_NUMBER + 1;
    const REWRITTEN_DATA_FILE: &str = "test/rewritten-data.parquet";
    const UNTOUCHED_DATA_FILE: &str = "test/untouched-data.parquet";
    const NEW_DELETION_VECTOR: &str = "memory:///test/location/data/new-dv.puffin";
    const NEW_DELETE_MANIFEST: &str = "memory:///test/location/metadata/delete-manifest-2.avro";
    const NEW_MANIFEST_LIST: &str = "memory:///test/location/metadata/manifest-list-2.avro";

    /// A deletion vector: a Puffin position-delete that names the single data file it applies to.
    fn deletion_vector(table: &Table, referenced_data_file: &str) -> DataFile {
        DataFileBuilder::default()
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .content(DataContentType::PositionDeletes)
            .file_path(NEW_DELETION_VECTOR.to_string())
            .file_format(DataFileFormat::Puffin)
            .file_size_in_bytes(128)
            .record_count(3)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .referenced_data_file(Some(referenced_data_file.to_string()))
            .content_offset(Some(4))
            .content_size_in_bytes(Some(64))
            .build()
            .unwrap()
    }

    /// Extends [`make_v2_table_with_delete_manifest`] with a *newer* snapshot whose delete manifest
    /// holds a deletion vector for `referenced_data_file` — i.e. the state left behind by a
    /// concurrent writer that added deletes after a rewrite read the table.
    async fn make_table_with_deletion_vector_added_after_parent(
        referenced_data_file: &str,
    ) -> Table {
        let base = make_v2_table_with_delete_manifest().await;
        let delete_file = deletion_vector(&base, referenced_data_file);
        make_table_with_delete_added_after_parent(base, delete_file).await
    }

    /// Extends [`make_v2_table_with_delete_manifest`] with a newer snapshot whose delete manifest
    /// holds `delete_file` — the state a concurrent writer leaves behind.
    async fn make_table_with_delete_added_after_parent(
        base: Table,
        delete_file: DataFile,
    ) -> Table {
        let file_io = base.file_io().clone();

        let mut manifest_writer = ManifestWriterBuilder::new(
            file_io.new_output(NEW_DELETE_MANIFEST).unwrap(),
            Some(NEW_SNAPSHOT_ID),
            None,
            base.metadata().current_schema().clone(),
            base.metadata().default_partition_spec().as_ref().clone(),
        )
        .build_v2_deletes();

        // Written as `Existing` with explicit numbers so the entry's own sequence number is pinned
        // rather than inherited, which is what the validation filters on.
        manifest_writer
            .add_existing_file(
                delete_file,
                NEW_SNAPSHOT_ID,
                NEW_SEQUENCE_NUMBER,
                Some(NEW_SEQUENCE_NUMBER),
            )
            .unwrap();
        let delete_manifest = manifest_writer.write_manifest_file().await.unwrap();

        let mut manifest_list_writer = ManifestListWriter::v2(
            file_io.new_output(NEW_MANIFEST_LIST).unwrap(),
            NEW_SNAPSHOT_ID,
            Some(PARENT_SNAPSHOT_ID),
            NEW_SEQUENCE_NUMBER,
        );
        manifest_list_writer
            .add_manifests(vec![delete_manifest].into_iter())
            .unwrap();
        manifest_list_writer.close().await.unwrap();

        let new_snapshot = Snapshot::builder()
            .with_snapshot_id(NEW_SNAPSHOT_ID)
            .with_parent_snapshot_id(Some(PARENT_SNAPSHOT_ID))
            .with_timestamp_ms(base.metadata().last_updated_ms() + 2)
            .with_sequence_number(NEW_SEQUENCE_NUMBER)
            .with_schema_id(0)
            .with_manifest_list(NEW_MANIFEST_LIST)
            .with_summary(Summary {
                operation: Operation::Overwrite,
                additional_properties: HashMap::new(),
            })
            .build();

        let metadata = base
            .metadata()
            .clone()
            .into_builder(Some("s3://bucket/test/location/metadata/v2.json".into()))
            .add_snapshot(new_snapshot)
            .unwrap()
            .set_ref(MAIN_BRANCH, SnapshotReference {
                snapshot_id: NEW_SNAPSHOT_ID,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            })
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        base.with_metadata(Arc::new(metadata))
    }

    fn producer_removing<'a>(table: &'a Table, data_file_path: &str) -> SnapshotProducer<'a> {
        let removed = DataFileBuilder::default()
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .content(DataContentType::Data)
            .file_path(data_file_path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let mut producer = SnapshotProducer::new(
            table,
            Uuid::now_v7(),
            None,
            None,
            HashMap::new(),
            vec![],
            vec![],
            vec![removed],
            vec![],
        );
        // Rewrites must keep the starting snapshot's sequence number so existing equality deletes
        // still apply to the replacement files; the validation enforces it.
        producer.set_new_data_file_sequence_number(PARENT_SEQUENCE_NUMBER);
        producer
    }

    /// A position delete that records no `referenced_data_file` but whose path bounds identify a
    /// single target — the shape Iceberg Java infers from, and which the scan side already handles.
    fn position_delete_with_path_bounds(
        table: &Table,
        path: &str,
        lower: &str,
        upper: &str,
    ) -> DataFile {
        DataFileBuilder::default()
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .content(DataContentType::PositionDeletes)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(128)
            .record_count(3)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .lower_bounds(HashMap::from([(
                FIELD_ID_POSITIONAL_DELETE_FILE_PATH,
                Datum::string(lower),
            )]))
            .upper_bounds(HashMap::from([(
                FIELD_ID_POSITIONAL_DELETE_FILE_PATH,
                Datum::string(upper),
            )]))
            .build()
            .unwrap()
    }

    fn equality_delete(table: &Table, path: &str) -> DataFile {
        DataFileBuilder::default()
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .content(DataContentType::EqualityDeletes)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(128)
            .record_count(3)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .equality_ids(Some(vec![1]))
            .build()
            .unwrap()
    }

    /// The conflict this validation exists for: a rewrite materialized the deletes that applied at
    /// `PARENT_SNAPSHOT_ID`, then a concurrent writer added a deletion vector for one of the data
    /// files being rewritten. Committing would drop that DV as dangling without ever applying its
    /// deletes, so the deleted rows would come back.
    #[tokio::test]
    async fn test_validate_rejects_new_deletion_vector_for_a_rewritten_data_file() {
        let table = make_table_with_deletion_vector_added_after_parent(REWRITTEN_DATA_FILE).await;
        let producer = producer_removing(&table, REWRITTEN_DATA_FILE);

        let error = producer
            .validate_no_new_deletes_for_data_files(PARENT_SNAPSHOT_ID)
            .await
            .expect_err("a delete added after the starting snapshot must be a conflict");

        let message = error.to_string();
        assert!(
            message.contains(REWRITTEN_DATA_FILE) && message.contains(NEW_DELETION_VECTOR),
            "error should name both the data file and the new delete file, got: {message}"
        );
    }

    /// A deletion vector for a data file this operation does not touch is not a conflict.
    #[tokio::test]
    async fn test_validate_allows_new_deletion_vector_for_an_untouched_data_file() {
        let table = make_table_with_deletion_vector_added_after_parent(UNTOUCHED_DATA_FILE).await;
        let producer = producer_removing(&table, REWRITTEN_DATA_FILE);

        producer
            .validate_no_new_deletes_for_data_files(PARENT_SNAPSHOT_ID)
            .await
            .expect("a delete for an unrelated data file must not conflict");
    }

    /// Validating against the current snapshot means nobody else committed, so the deletes already
    /// present were visible to the operation and were materialized.
    #[tokio::test]
    async fn test_validate_allows_when_nothing_was_committed_concurrently() {
        let table = make_table_with_deletion_vector_added_after_parent(REWRITTEN_DATA_FILE).await;
        let producer = producer_removing(&table, REWRITTEN_DATA_FILE);

        producer
            .validate_no_new_deletes_for_data_files(NEW_SNAPSHOT_ID)
            .await
            .expect("no concurrent commit means no conflict");
    }

    /// Review of #202, point 1: a position delete with no `referenced_data_file` whose path bounds
    /// identify a single target must still be attributed. Skipping it means "no conflict found",
    /// which loses the deletes — the opposite of fail-safe.
    #[tokio::test]
    async fn test_validate_rejects_bounds_identified_position_delete_for_a_rewritten_data_file() {
        let base = make_v2_table_with_delete_manifest().await;
        let delete_file = position_delete_with_path_bounds(
            &base,
            "test/new-position-delete.parquet",
            REWRITTEN_DATA_FILE,
            REWRITTEN_DATA_FILE,
        );
        let table = make_table_with_delete_added_after_parent(base, delete_file).await;
        let producer = producer_removing(&table, REWRITTEN_DATA_FILE);

        let error = producer
            .validate_no_new_deletes_for_data_files(PARENT_SNAPSHOT_ID)
            .await
            .expect_err("a bounds-identified delete for a removed data file must conflict");
        assert!(
            error.to_string().contains(REWRITTEN_DATA_FILE),
            "got: {error}"
        );
    }

    /// The same inference must not over-reject: bounds pointing at a file we keep is not a conflict.
    #[tokio::test]
    async fn test_validate_allows_bounds_identified_position_delete_for_an_untouched_data_file() {
        let base = make_v2_table_with_delete_manifest().await;
        let delete_file = position_delete_with_path_bounds(
            &base,
            "test/new-position-delete.parquet",
            UNTOUCHED_DATA_FILE,
            UNTOUCHED_DATA_FILE,
        );
        let table = make_table_with_delete_added_after_parent(base, delete_file).await;
        let producer = producer_removing(&table, REWRITTEN_DATA_FILE);

        producer
            .validate_no_new_deletes_for_data_files(PARENT_SNAPSHOT_ID)
            .await
            .expect("a delete bounded to an unrelated data file must not conflict");
    }

    /// A new position delete that names no target and whose bounds span several files cannot be shown
    /// to leave the removed files alone, so it must be treated as a conflict rather than assumed safe.
    #[tokio::test]
    async fn test_validate_rejects_unattributable_position_delete() {
        let base = make_v2_table_with_delete_manifest().await;
        let delete_file = position_delete_with_path_bounds(
            &base,
            "test/new-multi-file-position-delete.parquet",
            "test/aaa.parquet",
            "test/zzz.parquet",
        );
        let table = make_table_with_delete_added_after_parent(base, delete_file).await;
        let producer = producer_removing(&table, REWRITTEN_DATA_FILE);

        let error = producer
            .validate_no_new_deletes_for_data_files(PARENT_SNAPSHOT_ID)
            .await
            .expect_err("an unattributable new delete file must be treated as a conflict");
        assert!(
            error
                .to_string()
                .contains("does not identify the data file"),
            "got: {error}"
        );
    }

    /// Review of #202, point 2: equality deletes are safe *because* the replacement files keep the
    /// starting snapshot's sequence number, so a new one must not be reported as a conflict once that
    /// prerequisite holds — otherwise every equality-delete table would be unrewritable.
    #[tokio::test]
    async fn test_validate_allows_new_equality_delete_when_sequence_number_is_preserved() {
        let base = make_v2_table_with_delete_manifest().await;
        let delete_file = equality_delete(&base, "test/new-equality-delete.parquet");
        let table = make_table_with_delete_added_after_parent(base, delete_file).await;
        let producer = producer_removing(&table, REWRITTEN_DATA_FILE);

        producer
            .validate_no_new_deletes_for_data_files(PARENT_SNAPSHOT_ID)
            .await
            .expect("a new equality delete is handled by sequence number, not by data file");
    }

    /// Review of #202, point 2: that prerequisite is enforced, not assumed. Without the starting
    /// snapshot's sequence number on the replacement files, pre-existing equality deletes would stop
    /// applying to them, so the guarantee would be false.
    #[tokio::test]
    async fn test_validate_requires_the_starting_sequence_number_to_be_preserved() {
        let table = make_table_with_deletion_vector_added_after_parent(UNTOUCHED_DATA_FILE).await;
        let removed = DataFileBuilder::default()
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .content(DataContentType::Data)
            .file_path(REWRITTEN_DATA_FILE.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        // Deliberately *not* calling `set_new_data_file_sequence_number`.
        let producer = SnapshotProducer::new(
            &table,
            Uuid::now_v7(),
            None,
            None,
            HashMap::new(),
            vec![],
            vec![],
            vec![removed],
            vec![],
        );

        let error = producer
            .validate_no_new_deletes_for_data_files(PARENT_SNAPSHOT_ID)
            .await
            .expect_err("validation must refuse to promise anything without the sequence number");
        assert!(
            error.to_string().contains("sequence number"),
            "got: {error}"
        );
    }

    /// Review of #202, point 3: a snapshot that exists but is not on the branch's history would make
    /// every sequence-number comparison meaningless, so it must be rejected.
    #[tokio::test]
    async fn test_validate_rejects_starting_snapshot_that_is_not_an_ancestor() {
        let table = make_table_with_deletion_vector_added_after_parent(REWRITTEN_DATA_FILE).await;
        let producer = producer_removing(&table, REWRITTEN_DATA_FILE);

        // Sanity: the genuine ancestor passes the ancestry check (and then trips the DV conflict).
        let ancestor_error = producer
            .validate_no_new_deletes_for_data_files(PARENT_SNAPSHOT_ID)
            .await
            .expect_err("this fixture conflicts on the DV");
        assert!(
            !ancestor_error.to_string().contains("not an ancestor"),
            "PARENT_SNAPSHOT_ID must be recognised as an ancestor, got: {ancestor_error}"
        );

        let error = producer
            .validate_no_new_deletes_for_data_files(-12345)
            .await
            .expect_err("a snapshot outside the branch history must be rejected");
        let message = error.to_string();
        // Assert on the ancestry wording specifically. Accepting "contains the snapshot id" would
        // also match the sequence-number error, which made this test pass with the ancestry check
        // removed entirely.
        assert!(
            message.contains("not an ancestor"),
            "expected an ancestry rejection, got: {message}"
        );
    }

    /// A starting snapshot that is not in the metadata means the caller's assumption is broken, so
    /// this must fail loudly rather than silently validating nothing.
    #[tokio::test]
    async fn test_validate_rejects_unknown_starting_snapshot() {
        let table = make_table_with_deletion_vector_added_after_parent(REWRITTEN_DATA_FILE).await;
        let producer = producer_removing(&table, REWRITTEN_DATA_FILE);

        let error = producer
            .validate_no_new_deletes_for_data_files(-999)
            .await
            .expect_err("an unknown starting snapshot must be rejected");
        assert!(error.to_string().contains("-999"), "got: {error}");
    }

    #[test]
    fn test_modes_map_to_their_operations() {
        assert_eq!(Rewrite::OPERATION, Operation::Replace);
        assert_eq!(Overwrite::OPERATION, Operation::Overwrite);
        assert_eq!(
            ReplaceFilesOperation::<Rewrite>::new().operation(),
            Operation::Replace
        );
        assert_eq!(
            ReplaceFilesOperation::<Overwrite>::new().operation(),
            Operation::Overwrite
        );
    }

    /// Regression test: a rewrite/overwrite that removes one delete file must not
    /// mark *unrelated* delete files as deleted.
    ///
    /// `delete_entries` once guarded the delete-file branch with
    ///   `content == PositionDeletes || content == EqualityDeletes && removed.contains(path)`
    /// and because `&&` binds tighter than `||`, every `PositionDeletes` entry in
    /// the parent snapshot matched regardless of `removed_delete_file_paths`.
    async fn assert_only_removed_delete_files_marked<M: ReplaceFilesMode>() {
        let table = make_v2_table_with_delete_manifest().await;
        let removed = position_delete_file(&table, REMOVED_DELETE_FILE);

        let producer = SnapshotProducer::new(
            &table,
            Uuid::now_v7(),
            None,
            None,
            HashMap::new(),
            vec![],
            vec![],
            vec![],
            vec![removed],
        );

        let deleted_entries = ReplaceFilesOperation::<M>::new()
            .delete_entries(&producer)
            .await
            .unwrap();
        let deleted_paths: Vec<&str> = deleted_entries
            .iter()
            .map(|entry| entry.data_file().file_path())
            .collect();

        assert_eq!(
            deleted_paths,
            vec![REMOVED_DELETE_FILE],
            "only the removed delete file should be marked deleted; \
             {RETAINED_DELETE_FILE} must stay live"
        );
    }

    /// Regression test: rewriting a partially-deleted *delete* manifest must
    /// preserve its `Deletes` content type, and must carry survivors forward as
    /// `Existing` rather than restamping them as `Added`.
    async fn assert_delete_manifest_carried_forward_intact<M: ReplaceFilesMode>() {
        let table = make_v2_table_with_delete_manifest().await;
        let removed = position_delete_file(&table, REMOVED_DELETE_FILE);

        let mut producer = SnapshotProducer::new(
            &table,
            Uuid::now_v7(),
            None,
            None,
            HashMap::new(),
            vec![],
            vec![],
            vec![],
            vec![removed],
        );

        let existing = ReplaceFilesOperation::<M>::new()
            .existing_manifest(&mut producer)
            .await
            .unwrap();

        assert_eq!(existing.len(), 1, "the delete manifest should be rewritten");
        assert_eq!(
            existing[0].content,
            ManifestContentType::Deletes,
            "a rewritten delete manifest must stay a Deletes manifest"
        );

        let entries = existing[0].load_manifest(table.file_io()).await.unwrap();
        let paths: Vec<&str> = entries
            .entries()
            .iter()
            .map(|entry| entry.data_file().file_path())
            .collect();
        assert_eq!(paths, vec![RETAINED_DELETE_FILE]);

        let retained = &entries.entries()[0];
        assert_eq!(retained.status(), ManifestStatus::Existing);
        assert_eq!(retained.snapshot_id(), Some(PARENT_SNAPSHOT_ID));
        assert_eq!(retained.sequence_number(), Some(PARENT_SEQUENCE_NUMBER));
        assert_eq!(
            retained.file_sequence_number(),
            Some(PARENT_SEQUENCE_NUMBER)
        );
    }

    #[tokio::test]
    async fn test_overwrite_only_marks_removed_delete_files() {
        assert_only_removed_delete_files_marked::<Overwrite>().await;
    }

    #[tokio::test]
    async fn test_rewrite_only_marks_removed_delete_files() {
        assert_only_removed_delete_files_marked::<Rewrite>().await;
    }

    #[tokio::test]
    async fn test_overwrite_preserves_delete_manifest_content_type() {
        assert_delete_manifest_carried_forward_intact::<Overwrite>().await;
    }

    #[tokio::test]
    async fn test_rewrite_preserves_delete_manifest_content_type() {
        assert_delete_manifest_carried_forward_intact::<Rewrite>().await;
    }
}
