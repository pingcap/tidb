// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/dxf/importinto`: IMPORT INTO as a DXF task.
//!
//! The task's steps for local sort are `import` (encode the chunks of one
//! engine into local engines, then ingest them) and `post-process`
//! (rebase the allocators and verify the checksum).

mod clean_up;
mod collect_conflicts;
mod conflict_resolution;
pub mod job;
pub mod planner;
pub mod proto;
pub mod runtime_info;
pub mod scheduler;
pub mod task_executor;

use std::sync::{Arc, RwLock};

pub use tidb_dxf_importinto_taskkey::for_job as task_key;

/// A failure of this package: Go's error text, or a MySQL error.
pub type Error = Arc<dyn std::error::Error + Send + Sync>;

/// A Go error without a code (`errors.New`, `errors.Errorf`). It is not a
/// MySQL error, so `common.IsRetryableError` only retries it by message,
/// like Go.
pub(crate) fn plain(message: impl Into<String>) -> Error {
    Arc::new(tidb_dxf_storage::StorageError::Other(message.into()))
}

/// A statement error, kept as the Go `terror` it was raised as, so the DXF
/// task error and the job's error message read like Go's `err.Error()`.
pub(crate) fn driver(error: tidb_driver_error::DriverError) -> Error {
    let m = error.to_mysql_error();
    match m.identity {
        Some(identity) => {
            Arc::new(tidb_terror::TerrorError::normalize(m.message.as_str(), &identity.rfc_code(), identity.code()))
        }
        None => Arc::new(m),
    }
}

/// A failed `VerifyChecksum`: a mismatch stays Go's normalized
/// `ErrChecksumMismatch`, which `common.IsRetryableError` does not retry.
pub(crate) fn verify_checksum_error(error: tidb_executor_importer::table_import::VerifyChecksumError) -> Error {
    use tidb_executor_importer::table_import::VerifyChecksumError;
    match error {
        VerifyChecksumError::Remote(error) => driver(error),
        VerifyChecksumError::Mismatch(error) => Arc::new(error),
    }
}

/// What Go's import reaches through the TiKV store (`kv.Storage` with its
/// PD client and codec) and the instance's server info.
pub trait ImportStore: Send + Sync {
    /// Go `ingestctrl.NewBackend`'s import client over the store's PD.
    ///
    /// # Errors
    /// The store has no TiKV to import into.
    fn import_client(&self) -> Result<Arc<dyn tidb_ingestor_ingestctrl::region_job::ImportClient>, String>;
    /// Go `GetCodec().GetKeyspace()`: the keyspace prefix of encoded keys.
    fn keyspace(&self) -> Vec<u8>;
    /// Go `autoid.NewAllocator(...).Rebase(ctx, base, false)` of the
    /// table's allocator `tp`.
    ///
    /// # Errors
    /// The allocator's store failed.
    fn rebase_allocator(
        &self,
        db_id: i64,
        table: &tidb_model::TableInfo,
        tp: tidb_lightning_backend_kv::AllocatorType,
        base: i64,
    ) -> Result<(), String>;
    /// Go `store.NewEtcdCli(store)`, for the PiTR and changefeed prechecks.
    fn etcd(&self) -> Option<Arc<dyn tidb_domain_serverinfo::serverinfo_syncer::EtcdOps>>;
    /// Go `infosync.GetServerInfo()`: this instance's server info JSON.
    fn local_server_info(&self) -> Option<serde_json::Value>;
    /// Go `GetPDClient().GetClusterID(ctx)`.
    fn cluster_id(&self) -> Option<u64>;
    /// Go `ddl.CheckImportIntoTableIsEmpty`: whether no physical table of
    /// `table` has a record key (`existsTableRow`), at a fresh snapshot.
    ///
    /// # Errors
    /// The snapshot could not be read.
    fn table_is_empty(&self, table: &tidb_model::TableInfo) -> Result<bool, String>;
    /// Go `CalcMaxNodeCountByStoresNum`: `max(3, stores / 3)`, or 0 when
    /// PD cannot list the stores.
    fn max_node_count_by_stores(&self) -> usize;
    /// Go `importer.GetRegionSplitSizeKeys`: TiKV's
    /// `coprocessor.region-split-size` and `region-split-keys`, from the
    /// first store that answers.
    ///
    /// # Errors
    /// No store answered.
    fn region_split_size_keys(&self) -> Result<(i64, i64), String> {
        Err("get region split size and keys failed".to_owned())
    }
    /// Go `kv.Storage`'s `Begin` and `GetSnapshot`, which the conflict
    /// steps read and delete rows through. `None` for a store without TiKV.
    fn kv_storage(&self) -> Option<Arc<dyn tidb_txnkv::kv_storage::Storage>> {
        None
    }
}

static STORE: RwLock<Option<Arc<dyn ImportStore>>> = RwLock::new(None);

/// Sets the store the IMPORT INTO tasks of this instance import into.
pub fn set_import_store(store: Option<Arc<dyn ImportStore>>) {
    *STORE.write().unwrap_or_else(std::sync::PoisonError::into_inner) = store;
}

/// The store set by [`set_import_store`].
///
/// # Errors
/// No store is set on this instance.
pub fn import_store() -> Result<Arc<dyn ImportStore>, Error> {
    STORE
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
        .ok_or_else(|| plain("IMPORT INTO needs a TiKV store, and this instance has none"))
}

/// Go `executor.RunImportQuery(ctx, sctx, q, importSQL, memQuota, output)`:
/// runs the SELECT of the IMPORT INTO statement `import_sql` at
/// `q.ReadTS` in the worker session `sctx`, sending the result chunks to
/// `output`. The executor's session tier provides it
/// ([`set_import_query_runner`]); `sctx` takes the submitter's session
/// states, so the caller destroys it.
pub type ImportQueryRunner = fn(
    ctx: &tidb_gocontext::Context,
    sctx: &dyn tidb_dxf_storage::SessionContext,
    q: &tidb_executor_importer::query_source::QuerySource,
    import_sql: &str,
    mem_quota: i64,
    output: &crossbeam_channel::Sender<tidb_executor_importer::chunk_process::QueryChunk>,
) -> Result<(), Error>;

static IMPORT_QUERY_RUNNER: RwLock<Option<ImportQueryRunner>> = RwLock::new(None);

/// Sets the runner of import queries ([`ImportQueryRunner`]).
pub fn set_import_query_runner(runner: ImportQueryRunner) {
    *IMPORT_QUERY_RUNNER.write().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(runner);
}

/// Go `executor.RunImportQuery`, through the runner the session tier set.
pub(crate) fn run_import_query(
    ctx: &tidb_gocontext::Context,
    sctx: &dyn tidb_dxf_storage::SessionContext,
    q: &tidb_executor_importer::query_source::QuerySource,
    import_sql: &str,
    mem_quota: i64,
    output: &crossbeam_channel::Sender<tidb_executor_importer::chunk_process::QueryChunk>,
) -> Result<(), Error> {
    let runner = IMPORT_QUERY_RUNNER
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .ok_or_else(|| plain("this instance cannot run import queries"))?;
    runner(ctx, sctx, q, import_sql, mem_quota, output)
}

/// Go `importinto`'s `init`: registers the task type's scheduler,
/// executor and cleanup factories.
pub fn register() {
    scheduler::register();
    task_executor::register();
    clean_up::register();
}

#[cfg(test)]
mod tests {
    use tidb_executor_importer::{Plan, PostOpLevel};
    use tidb_executor_importer::table_import::verify_checksum;
    use tidb_ingestor_ingestctrl::checksum::RemoteChecksum;

    /// Go `common.IsRetryableError` does not retry `ErrChecksumMismatch`
    /// (it is not in `retryableErrorIDs`), so a post-process subtask with a
    /// mismatched checksum fails instead of running again forever.
    /// Go `importStepExecutor.RunSubtask` returns the chunk's
    /// `errors.Annotatef(ErrEncodeKV...)`, which is not retryable: a bad row
    /// fails the subtask instead of running it again forever.
    #[test]
    fn plain_errors_are_not_retryable() {
        let error = super::plain(
            "when encoding 1-th data row in this chunk: [Lightning:Restore:ErrEncodeKV]encode kv error in file t1.csv:0 at offset 0",
        );
        assert!(!tidb_lightning_common::is_retryable_error(error.as_ref()));
        assert!(tidb_lightning_common::is_retryable_error(super::plain("rate: wait").as_ref()));
    }

    #[test]
    fn checksum_mismatch_is_not_retryable() {
        let local = tidb_lightning_verification::KvChecksum::make(1, 1, 1);
        let plan = Plan { checksum: PostOpLevel::Required, ..Plan::default() };
        let unequal = || Ok(RemoteChecksum { checksum: 2, total_kvs: 2, total_bytes: 2, ..Default::default() });
        let error = super::verify_checksum_error(verify_checksum(&plan, &local, unequal).unwrap_err());
        assert!(!tidb_lightning_common::is_retryable_error(error.as_ref()));
        assert_eq!(
            error.to_string(),
            "[Lighting:Restore:ErrChecksumMismatch]checksum mismatched remote vs local => (checksum: 2 vs 1) (total_kvs: 2 vs 1) (total_bytes:2 vs 1)"
        );
    }
}
