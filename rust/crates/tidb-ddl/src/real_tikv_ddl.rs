// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Executing one catalog change against a real cluster.
//!
//! One transaction reads the catalog, plans the change against exactly that
//! snapshot, and publishes every meta-key mutation through the same optimistic
//! two-phase commit the DML path uses. There is no second transaction and no
//! ordering between transactions to get wrong: the object, the schema-version
//! bump, and the diff that makes the version readable are one atom.
//!
//! After the commit, [`commit_cluster_ddl`] publishes the new version the way
//! Go's owner does: `pkg/ddl/job_worker.go` calls
//! `schemaVerSyncer.OwnerUpdateGlobalVersion` once the version is durable, so
//! every other node's etcd watch fires instead of waiting out its lease. The
//! notification is deliberately *after* the commit and deliberately not fatal
//! — see [`SchemaVersionNotifier`] — and the etcd key, value encoding, and
//! watch side are documented in [`tidb_infoschema_issyncer::catalog_watch`]'s module doc.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tidb_kv::cluster_storage::ClusterSnapshot;
use tidb_store_driver_txn::cluster_storage::{MutationBuffer, SwappableSnapshot};
use tidb_model::{ActionType, Job};
use tidb_store_driver_txn::transaction::StoreClient;
use tidb_txnkv::transaction::OptimisticCoordinatorError;
use tidb_store_driver_txn::transaction::RealOptimisticTransactionOpener;

use tidb_infoschema::cluster_catalog::{load_info_schema, MetaSnapshot};
use crate::cluster_ddl::{plan_persisted_ddl_job_failure, plan_persisted_ddl_job_step, recover_ddl_action, CheckConstraintValidation, DdlWrite, ExchangePartitionValidation, MdlInfoUpdate, PersistedDdlJobFailure, PersistedDdlJobPlan};
use tidb_ddl_build::plan_error::DdlPlanError;

use tidb_store_driver_txn::cluster_table_storage::{LockKeysOutcome, SessionTransaction};
use tidb_ddl_jobsubmit::finish_insert_attempt;
use tidb_store_driver_error::pessimistic_lock_error::LockSqlError;
use tidb_infoschema_issyncer::real_tikv_catalog::{SnapshotMetaSnapshot, TransactionMetaSnapshot};

pub(crate) mod add_index_reorg;
/// Go `operator.AsyncPipeline` of the ingest backfill, as Go's test hooks see it.
pub use add_index_reorg::ingest::PipelineError;
pub use add_index_reorg::operators as backfilling_operators;
mod backfilling;
pub mod delete_range;

/// What undoes a delivered label step: the rules its IDs held before it.
struct LabelDeliveryReceipt {
    manager: std::sync::Arc<dyn tidb_domain_infosync::label_manager::LabelRuleManager>,
    original: Vec<tidb_ddl_label::Rule>,
    rule_ids: Vec<String>,
}

struct PlacementDeliveryReceipt {
    pd_http_client: tikv_client::pd_http::Client,
    rollback: Vec<tidb_placement::Bundle>,
}

fn deliver_placement_bundles(
    pd_http_client: &tikv_client::pd_http::Client,
    bundles: &[tidb_placement::Bundle],
    timeout: Duration,
) -> Result<Option<PlacementDeliveryReceipt>, ClusterDdlError> {
    if bundles.is_empty() {
        return Ok(None);
    }
    crate::placement_delivery::put_rule_bundles(pd_http_client, bundles, timeout).map_err(
        |error| {
            ClusterDdlError::NotCommitted(format!(
                "failed to notify PD the placement rules: {error}"
            ))
        },
    )?;
    // IDs allocated by an optimistic attempt are withdrawn if the catalog
    // transaction loses its race.
    let rollback = bundles
        .iter()
        .map(|bundle| tidb_placement::Bundle {
            id: bundle.id.clone(),
            ..tidb_placement::Bundle::default()
        })
        .collect();
    Ok(Some(PlacementDeliveryReceipt {
        pd_http_client: pd_http_client.clone(),
        rollback,
    }))
}

/// Go's job step makes its label-rule calls before the version is
/// published and fails the job when one fails. The step's calls go to the
/// node's label manager here, before the catalog transaction commits; the
/// rules they touch are read first, so a transaction that loses its commit
/// race (or a later call that fails) puts them back.
fn deliver_label_rules(write: &DdlWrite) -> Result<Option<LabelDeliveryReceipt>, ClusterDdlError> {
    if write.label_rules.is_empty() {
        return Ok(None);
    }
    let manager = tidb_domain_infosync::label_manager::label_rule_manager();
    let mut rule_ids = write
        .label_rules
        .iter()
        .flat_map(|step| step.write.rule_ids())
        .collect::<Vec<_>>();
    rule_ids.sort();
    rule_ids.dedup();
    let original = manager
        .get_label_rules(&rule_ids)
        .map_err(|error| {
            ClusterDdlError::NotCommitted(format!("failed to get PD the label rules: {error}"))
        })?
        .into_values()
        .collect();
    let receipt = LabelDeliveryReceipt {
        manager,
        original,
        rule_ids,
    };
    for step in &write.label_rules {
        if let Err(error) = step.write.apply(receipt.manager.as_ref()) {
            let mut cause = ClusterDdlError::NotCommitted(match step.error_context {
                Some(context) => format!("{context}: {error}"),
                None => error,
            });
            if let Err(error) = restore_label_rules(&receipt) {
                cause = ClusterDdlError::NotCommitted(format!(
                    "{cause}; and restoring the attempt's label rules failed: {error}"
                ));
            }
            return Err(cause);
        }
    }
    Ok(Some(receipt))
}

fn restore_label_rules(receipt: &LabelDeliveryReceipt) -> Result<(), String> {
    let present = receipt
        .original
        .iter()
        .map(|rule| rule.id.as_str())
        .collect::<std::collections::HashSet<_>>();
    let delete_rules = receipt
        .rule_ids
        .iter()
        .filter(|id| !present.contains(id.as_str()))
        .cloned()
        .collect();
    let patch = tidb_ddl_label::new_rule_patch(receipt.original.clone(), delete_rules);
    receipt.manager.update_label_rules(&patch)
}

fn restore_placement(
    receipt: &PlacementDeliveryReceipt,
    timeout: Duration,
) -> Result<(), crate::placement_delivery::PlacementDeliveryError> {
    crate::placement_delivery::put_rule_bundles(&receipt.pd_http_client, &receipt.rollback, timeout)
}

fn compensate_external_delivery(
    mut cause: ClusterDdlError,
    placement_receipt: Option<&PlacementDeliveryReceipt>,
    label_receipt: Option<&LabelDeliveryReceipt>,
    timeout: Duration,
) -> ClusterDdlError {
    if let Some(receipt) = label_receipt {
        if let Err(error) = restore_label_rules(receipt) {
            cause = ClusterDdlError::NotCommitted(format!(
                "{cause}; and restoring the attempt's label rules failed: {error}"
            ));
        }
    }
    if let Some(receipt) = placement_receipt {
        if let Err(error) = restore_placement(receipt, timeout) {
            cause = ClusterDdlError::NotCommitted(format!(
                "{cause}; and restoring the attempt's placement bundles failed: {error}"
            ));
        }
    }
    cause
}

/// Why a catalog change did not happen.
#[derive(Clone, Debug)]
pub enum ClusterDdlError {
    /// The change could not be planned from the observed catalog.
    Plan(DdlPlanError),
    /// The transaction coordinator failed before or during publication.
    Transaction(OptimisticCoordinatorError),
    /// Another DDL changed the catalog between this statement's snapshot and
    /// its commit.
    ///
    /// This node runs no owner election: `SchemaVersionKey` and `NextGlobalID`
    /// are in every catalog change's write set, so a competing DDL turns this
    /// transaction into a definite write conflict. The commit paths RETRY it
    /// from a fresh snapshot -- re-read, re-plan, re-commit, which is Go's
    /// `kv.RunInNewTxn(retryable=true)` running every DDL meta write -- so a
    /// client sees this error only after `kv.MaxRetryCnt` (100) attempts all
    /// lost their race. Re-planning is what keeps the retry sound: the
    /// version a fresh attempt publishes describes the catalog that now
    /// exists, never the one that no longer does.
    ConcurrentSchemaChange {
        /// The version this statement planned to produce.
        planned_version: i64,
        /// TiKV's own conflict diagnostic.
        detail: String,
    },
    /// A determinate commit failure with the exact driver error identity Go
    /// exposes through `ToTiDBErr`.
    Commit(LockSqlError),
    /// Publication reached a terminal state that is not a commit, so the
    /// catalog change cannot be reported as done.
    NotCommitted(String),
    /// The DDL transaction was published and then lost its answer, so whether
    /// it committed is unknown. Kept distinct from `NotCommitted`, which
    /// asserts the very thing nobody knows; Go answers it with
    /// `terror.ErrResultUndetermined` and closes the connection
    /// (`pkg/server/conn.go:1288-1291`).
    Undetermined(String),
    /// The index entries for the rows the table already holds could not be
    /// built, so the change was abandoned before anything was published.
    Backfill(DdlPlanError),
    /// An index value failed evaluation or violated a SQL constraint.
    IndexBackfill(LockSqlError),
    /// A CHECK schema phase is durable but not yet acknowledged by every
    /// registered node, so advancing would violate the two-version invariant.
    SchemaSync(String),
}

impl fmt::Display for ClusterDdlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Plan(error) => write!(formatter, "{error}"),
            Self::Transaction(error) => write!(formatter, "catalog transaction: {error}"),
            Self::ConcurrentSchemaChange {
                planned_version,
                detail,
            } => write!(
                formatter,
                "another DDL changed the catalog while this statement was preparing schema \
                 version {planned_version}; this node performs DDL as the single catalog \
                 writer and refuses to interleave: {detail}"
            ),
            // Go `pkg/parser/terror/terror.go:265-269`:
            // `mysql.Message("execution result undetermined", nil)`.
            Self::Undetermined(detail) => {
                write!(formatter, "execution result undetermined: {detail}")
            }
            Self::Commit(error) => formatter.write_str(&error.message),
            Self::NotCommitted(state) => {
                write!(formatter, "catalog change did not commit: {state}")
            }
            Self::Backfill(detail) => write!(
                formatter,
                "the index entries for the rows this table already holds could not be \
                 built, so nothing was published: {detail}"
            ),
            Self::IndexBackfill(error) => formatter.write_str(&error.message),
            Self::SchemaSync(detail) => write!(
                formatter,
                "CHECK schema phase is committed but not synchronized: {detail}"
            ),
        }
    }
}

impl ClusterDdlError {
    /// A reorganization step's error, kept as Go keeps the `err` it returns:
    /// a `*terror.Error` cause keeps its identity (`DdlPlanError::from_cause`).
    pub(crate) fn backfill(error: &(dyn std::error::Error + 'static)) -> Self {
        Self::Backfill(DdlPlanError::from_cause(error))
    }

    /// A storage failure of a reorganization step: a SQL failure keeps the
    /// Go identity it was raised with; the others are plain errors.
    pub(crate) fn backfill_storage(error: tidb_kv::storage::StorageError) -> Self {
        match error {
            tidb_kv::storage::StorageError::Sql(error) => Self::Backfill(
                crate::cluster_ddl::reorg_failure(error.code, &error.message, error.identity),
            ),
            error => Self::backfill_message(error.to_string()),
        }
    }

    /// A reorganization step's plain `errors.New` / `errors.Errorf`.
    pub(crate) fn backfill_message(message: impl Into<String>) -> Self {
        Self::Backfill(DdlPlanError::Encode(message.into()))
    }
}

impl std::error::Error for ClusterDdlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Plan(error) | Self::Backfill(error) => Some(error),
            Self::Transaction(error) => Some(error),
            Self::ConcurrentSchemaChange { .. }
            | Self::Commit(_)
            | Self::NotCommitted(_)
            | Self::Undetermined(_)
            | Self::IndexBackfill(_)
            | Self::SchemaSync(_) => None,
        }
    }
}

impl From<DdlPlanError> for ClusterDdlError {
    fn from(error: DdlPlanError) -> Self {
        Self::Plan(error)
    }
}

impl From<tidb_ddl_jobsubmit::JobSubmitError> for ClusterDdlError {
    fn from(error: tidb_ddl_jobsubmit::JobSubmitError) -> Self {
        Self::Plan(DdlPlanError::from(error))
    }
}

impl From<OptimisticCoordinatorError> for ClusterDdlError {
    fn from(error: OptimisticCoordinatorError) -> Self {
        Self::Transaction(error)
    }
}

/// Go's warning level for one DDL warning (`contextutil.WarnLevel*`): job
/// warnings are `StmtCtx.AppendWarning` (Warning); `IF [NOT] EXISTS`
/// suppressions are `AppendNote` (Note) — `DROP TABLE IF EXISTS nosuch`
/// leaves `Note | 1051 | Unknown table 'x.nosuch'`.
use tidb_context::WarningLevel;

/// Go `StmtCtx.AppendNote(proto.GenWithStackByArgs(...))`: the note an
/// `IF [NOT] EXISTS` files, carrying the Go error `proto` it suppressed.
#[must_use]
pub fn ddl_note(error: tidb_terror::TerrorError) -> tidb_context::SqlWarn {
    tidb_context::SqlWarn::new(
        WarningLevel::Note,
        tidb_context::WarnErr::Terror(error),
    )
}

/// What one catalog change did.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterDdlReport {
    /// The change was published at this schema version.
    Applied {
        /// The published schema version.
        schema_version: i64,
        /// The ID of the object created, if the change created one.
        created_id: Option<i64>,
        /// Go `job.Warning`: what the change did differently from what was
        /// written, for the caller to raise as a statement warning.
        warnings: Vec<tidb_context::SqlWarn>,
    },
    /// `IF [NOT] EXISTS` was already satisfied, so nothing was written.
    AlreadySatisfied {
        /// What was already true.
        detail: String,
        /// The warnings the statement raises even though it changed nothing.
        warnings: Vec<tidb_context::SqlWarn>,
    },
}

/// Announces a committed schema version so peers reload without waiting for
/// their lease tick.
///
/// The committed version is already durable in TiKV. Persisted workers
/// propagate publication failure in MDL mode; without MDL they still wait for
/// loaded self-version keys, which periodic schema reload can advance.
/// Direct publication retains its existing best-effort notification policy.
pub trait SchemaVersionNotifier {
    /// Publishes `version`; the owner decides the source-mode failure policy.
    fn notify(&self, version: i64) -> Result<(), String>;
}

impl SchemaVersionNotifier for tidb_pd_client::EtcdClient {
    fn notify(&self, version: i64) -> Result<(), String> {
        self.put_global_schema_version(version)
            .map_err(|error| error.to_string())
    }
}

/// Stages the index entries an index change owes for the rows a table already
/// holds.
///
/// It is a trait because the walk itself is NOT this crate's to own: the entry
/// for a row is exactly what an `INSERT` would have written, and that single
/// definition lives in the executor's `TableCommon` over the same
/// [`ClusterTableStorage`] seam the session writes through. Implementing the
/// walk here would be a second index-entry writer, and two of those drift —
/// which on an index means a query returns rows that are not there, or misses
/// rows that are, with no error either way.
///
/// [`ClusterTableStorage`]: tidb_store_driver_txn::cluster_storage::ClusterTableStorage
pub trait IndexBackfiller: Sync {
    /// The worker's backfiller as Go's asynchronous `reorgFn` holds it (the
    /// closure captures the worker and its `ddlCtx`).
    fn shared(&self) -> Arc<dyn IndexBackfiller + Send>;

    /// Go `isReorgRunnable`'s owner half for a local reorganization: the
    /// worker is not closed (`ddlCtx.ctx`) and this node owns DDL
    /// (`ddlCtx.isOwner`).
    ///
    /// # Errors
    /// The worker stopped or lost the ownership.
    fn check_owner(&self) -> Result<(), String> {
        Ok(())
    }

    /// Go `w.sessPool`: the worker's sessions, which a RECOVER step reaches
    /// GC through. `None` for a worker without one.
    fn session_pool(&self) -> Option<Arc<dyn tidb_ddl_build::session_pool::DdlSessionPool>> {
        None
    }

    /// Go `jobCtx.oldDDLCtx.startMode`.
    fn start_mode(&self) -> crate::StartMode {
        crate::StartMode::Normal
    }

    /// Go `jobCtx.store`'s region cache and PD client. `None` for a worker
    /// without one.
    fn region_store(&self) -> Option<Arc<dyn tidb_kv::region_map::RegionStore>> {
        None
    }

    /// Go `ddlCtx.autoidCli`: the AutoID service a job's rebase goes
    /// through. `None` for a worker without one.
    fn auto_id_client(&self) -> Option<Arc<dyn tidb_autoid::service_rpc::AutoIdServiceRpc>> {
        None
    }

    /// Go `checkForeignKeyConstrain` for an `ADD FOREIGN KEY`: the child's
    /// existing rows against its parent, through the same snapshot and
    /// buffer as the other stages.
    fn stage_foreign_key_check(
        &self,
        _plan: &tidb_ddl_build::session_pool::ForeignKeyCheck,
        _snapshot: Arc<Mutex<dyn ClusterSnapshot>>,
        _buffer: &MutationBuffer,
    ) -> Result<(), LockSqlError> {
        Err(foreign_key_check_unavailable())
    }

    /// Go `checkExchangePartitionRecordValidation` for an `EXCHANGE
    /// PARTITION ... WITH VALIDATION`, through the same snapshot and buffer
    /// as the other stages.
    fn stage_exchange_partition_check(
        &self,
        _plan: &ExchangePartitionValidation,
        _snapshot: Arc<Mutex<dyn ClusterSnapshot>>,
        _buffer: &MutationBuffer,
    ) -> Result<(), LockSqlError> {
        Err(LockSqlError::from(tidb_terror::MysqlError::unknown(
            "this path cannot validate an exchanged partition's rows",
        )))
    }

    /// Go `checkCacheTableSize` for an `ALTER TABLE ... CACHE`: the table's
    /// stored keys and values may total at most 64 MiB (`table too large`).
    fn stage_cache_table_check(
        &self,
        _table: &tidb_model::TableInfo,
        _snapshot: Arc<Mutex<dyn ClusterSnapshot>>,
        _buffer: &MutationBuffer,
    ) -> Result<(), LockSqlError> {
        Err(cache_table_check_unavailable())
    }

    /// One unit of ADD INDEX's transactional reorganization through a batch
    /// transaction's snapshot and buffer (Go's backfill workers). The caller
    /// commits the buffer, with the batch's row locks, in that transaction.
    /// Go `checkForNullValue`'s query: whether `schema`.`table` holds a
    /// NULL in column `column_id`, read on a pooled session.
    fn has_null_value(
        &self,
        _schema: &str,
        _table: &tidb_model::TableInfo,
        _column_id: i64,
    ) -> Result<bool, LockSqlError> {
        Err(LockSqlError::from(tidb_terror::MysqlError::unknown(
            "this path cannot read a table's rows",
        )))
    }

    fn reorg_add_index(
        &self,
        _reorg: &crate::cluster_ddl::add_index::AddIndexReorg,
        _work: AddIndexReorgWork<'_>,
        _snapshot: Arc<Mutex<dyn ClusterSnapshot>>,
        _buffer: &MutationBuffer,
    ) -> Result<AddIndexReorgAnswer, LockSqlError> {
        Err(LockSqlError::from(tidb_terror::MysqlError::unknown(
            "this path cannot reorganize an index",
        )))
    }

    /// Go `indexIngestWorker`'s table and index condition checkers, which
    /// `writeChunk` encodes a scanned chunk's rows with.
    fn ingest_chunk_encoder(
        &self,
        _reorg: &crate::cluster_ddl::add_index::AddIndexReorg,
    ) -> Result<Box<dyn IngestChunkEncoder>, LockSqlError> {
        Err(LockSqlError::from(tidb_terror::MysqlError::unknown(
            "this path cannot encode index entries",
        )))
    }

    /// The node's live row-ID allocators, for the `mysql.*` rows the owner
    /// inserts (Go's internal INSERTs: the reorg handle, the delete ranges).
    fn system_row_ids(&self) -> Arc<dyn tidb_table::table_from_meta::TableAutoIds>;
}

/// See [`IndexBackfiller::reorg_add_index`].
/// See [`IndexBackfiller::reorg_add_index`].
/// See [`IndexBackfiller::reorg_add_index`].
/// See [`IndexBackfiller::reorg_add_index`].
pub enum AddIndexReorgWork<'a> {
    /// Go `getTableRange` for one physical table.
    TableRange {
        /// The physical table.
        physical_id: i64,
    },
    /// Go `addIndexTxnWorker.BackfillData` for every index being added, over
    /// the rows in `[start, end)`.
    Backfill {
        /// The physical table.
        physical_id: i64,
        /// The batch's first key.
        start: &'a [u8],
        /// The range's end.
        end: &'a [u8],
        /// Go `backfillCtx.batchCnt`: the rows of one batch.
        batch_size: usize,
    },
    /// Go `tableScanWorker.scanRecords` and `indexIngestWorker.WriteChunk`.
    Ingest {
        /// The physical table.
        physical_id: i64,
        /// The scan range's first key.
        start: &'a [u8],
        /// The scan range's exclusive end.
        end: &'a [u8],
        /// The number of rows to decode.
        batch_size: usize,
    },
    /// Go `updateColumnWorker.BackfillData` over the rows in
    /// `[start, end)` (the reorganization's `column`).
    UpdateColumn {
        /// The physical table.
        physical_id: i64,
        /// The batch's first key.
        start: &'a [u8],
        /// The range's end.
        end: &'a [u8],
        /// Go `backfillCtx.batchCnt`: the rows of one batch.
        batch_size: usize,
    },
    /// Go `reorgPartitionWorker.BackfillData` over a reorganized partition's
    /// rows in `[start, end)`: each is copied to the partition the new scheme
    /// routes it to.
    PartitionCopy {
        /// The reorganized partition.
        physical_id: i64,
        /// The batch's first key.
        start: &'a [u8],
        /// The range's end.
        end: &'a [u8],
        /// Go `backfillCtx.batchCnt`: the rows of one batch.
        batch_size: usize,
    },
    /// Go `cleanUpIndexWorker.BackfillData` over a dropped partition's rows
    /// in `[start, end)`: their entries in the reorganization's global
    /// indexes go.
    CleanUpGlobalIndex {
        /// The dropped partition.
        physical_id: i64,
        /// The batch's first key.
        start: &'a [u8],
        /// The range's end.
        end: &'a [u8],
        /// Go `backfillCtx.batchCnt`: the rows of one batch.
        batch_size: usize,
    },
    /// Go `GetRangeEndKey` over a backfill task's `[start, end)`.
    RangeEndKey {
        /// The task's first key.
        start: &'a [u8],
        /// The task's region end.
        end: &'a [u8],
    },
    /// Go `checkModifyColumnData` over the whole table.
    CheckColumn(&'a crate::cluster_ddl::add_index::ColumnDataCheck),
    /// Go `mergeIndexWorker.BackfillData` over one index's temporary
    /// entries in `[start, end)`.
    Merge {
        /// The index.
        index_id: i64,
        /// The physical table.
        physical_id: i64,
        /// The batch's first key.
        start: &'a [u8],
        /// The range's end.
        end: &'a [u8],
        /// Go `backfillCtx.batchCnt`: the rows of one batch.
        batch_size: usize,
    },
}

/// See [`IndexBackfiller::ingest_chunk_encoder`].
pub trait IngestChunkEncoder: Send {
    /// Go `writeChunk`: the index KVs of a chunk of `physical_id`'s rows.
    ///
    /// # Errors
    /// A row's index entry could not be encoded.
    fn encode(
        &mut self,
        physical_id: i64,
        rows: &tidb_ddl_build::table_backfill::IngestRows,
    ) -> Result<Vec<tidb_ddl_build::table_backfill::IndexKV>, LockSqlError>;
}

/// See [`IndexBackfiller::reorg_add_index`].
pub enum AddIndexReorgAnswer {
    /// The record range, `None` for an empty physical table.
    Range(Option<(Vec<u8>, Vec<u8>)>),
    /// What a batch did.
    Batch(tidb_ddl_build::backfilling::ReorgBatch),
    /// Go `IndexRecordChunk`: the rows an ingest scan read, with no
    /// transaction mutations.
    Ingest(tidb_ddl_build::table_backfill::IngestRows),
    /// What a column check found: `None` when every row fits.
    Checked(Option<LockSqlError>),
    /// A task's actual end key.
    EndKey(Vec<u8>),
}

/// The default for a backfiller that cannot read a table's stored bytes.
fn cache_table_check_unavailable() -> LockSqlError {
    LockSqlError::from(tidb_terror::MysqlError::unknown(
        "this path cannot measure a table for ALTER TABLE ... CACHE",
    ))
}

/// The default for a backfiller that cannot read a child and its parent.
fn foreign_key_check_unavailable() -> LockSqlError {
    LockSqlError::from(tidb_terror::MysqlError::unknown(
        "this path cannot check a foreign key's existing rows",
    ))
}

/// Executes Go's existing-row CHECK validation from the DDL transaction's
/// own row snapshot before the candidate metadata is published.
pub trait CheckConstraintValidator {
    /// Proves every stored row satisfies the candidate enforced constraint.
    /// Retains source error identity for the worker's checkpoint and rollback
    /// decision; conversion to the SQL wire format happens after completion.
    fn validate(
        &self,
        plan: &CheckConstraintValidation,
        snapshot: Arc<Mutex<dyn ClusterSnapshot>>,
        buffer: &MutationBuffer,
    ) -> Result<(), DdlPlanError>;
}



/// Owner-side synchronization between committed DDL schema phases.
///
/// The implementation reloads the owner's own catalog and then waits for the
/// existing synchronizer, which selects per-job MDL acknowledgements or
/// loaded self-version keys according to the process's source mode.
pub trait DdlSchemaSync {
    /// Stable owner id stored in `mysql.tidb_mdl_info.owner_id`.
    fn owner_id(&self) -> &str;

    /// Waits until every registered node has acknowledged `version` for
    /// `ddl_job_id`.
    fn wait_version_synced(&self, ddl_job_id: i64, version: i64) -> Result<(), String>;

    /// Removes the per-job acknowledgement keys after the final phase.
    fn clean_job_versions(&self, ddl_job_id: i64) -> Result<(), String>;

    /// Go `jobContext.infoCache`: the owner node's schema cache, which a job
    /// step reads its schema from. `None` for a run without one (bootstrap,
    /// tests), whose steps read the whole schema from their snapshot.
    fn info_cache(&self) -> Option<Arc<tidb_infoschema_issyncer::catalog_watch::InfoCache>> {
        None
    }
}

/// Go's `ddlJobDoneChMap`: the done signal of each job this node's executor
/// submitted, shared with the owner scheduler. The submitter registers a
/// signal before the job's insert commits (`registerJobDoneChannels`), the
/// scheduler closes it once the job is done (`jobContext.notifyDone`), and
/// the waiting session removes it when it returns (`delJobDoneCh`).
#[derive(Debug, Default)]
pub struct DdlJobDoneChMap {
    channels: Mutex<std::collections::HashMap<i64, Arc<JobDoneCh>>>,
}

impl DdlJobDoneChMap {
    /// Go `JobSubmitter.registerJobDoneChannels`: a signal per job, and the
    /// cleanup that drops them when the insert fails.
    pub fn register_job_done_channels(
        &self,
        specs: &[tidb_ddl_jobsubmit::JobSpec],
    ) -> impl FnOnce() + '_ {
        let job_ids = specs.iter().map(|spec| spec.job.id).collect::<Vec<_>>();
        {
            let mut channels = self
                .channels
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for job_id in &job_ids {
                channels.insert(*job_id, Arc::new(JobDoneCh::default()));
            }
        }
        move || {
            let mut channels = self
                .channels
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for job_id in &job_ids {
                channels.remove(job_id);
            }
        }
    }

    /// Go `ddlJobDoneChMap.Load`.
    #[must_use]
    pub fn load(&self, job_id: i64) -> Option<Arc<JobDoneCh>> {
        self.channels
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&job_id)
            .cloned()
    }

    /// Go `executor.delJobDoneCh`.
    pub fn delete(&self, job_id: i64) {
        self.channels
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&job_id);
    }
}

/// One job's done signal: Go's `chan struct{}`, which `notifyDone` closes.
#[derive(Debug, Default)]
pub struct JobDoneCh {
    closed: Mutex<bool>,
    condvar: std::sync::Condvar,
}

impl JobDoneCh {
    /// Go `jobContext.notifyDone`: `close(notifyCh)`.
    pub fn notify_done(&self) {
        *self
            .closed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.condvar.notify_all();
    }

    /// Waits until the signal is closed or `deadline` passes; true when it
    /// was closed.
    pub fn wait_until(&self, deadline: std::time::Instant) -> bool {
        let mut closed = self
            .closed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while !*closed {
            let now = std::time::Instant::now();
            if now >= deadline {
                return false;
            }
            closed = self
                .condvar
                .wait_timeout(closed, deadline - now)
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
        }
        true
    }
}

const FAST_DDL_INTERVAL_POLICY: &[Duration] = &[Duration::from_millis(500)];
const NORMAL_DDL_INTERVAL_POLICY: &[Duration] = &[
    Duration::from_millis(500),
    Duration::from_millis(500),
    Duration::from_secs(1),
];
const SLOW_DDL_INTERVAL_POLICY: &[Duration] = &[
    Duration::from_millis(500),
    Duration::from_millis(500),
    Duration::from_secs(1),
    Duration::from_secs(1),
    Duration::from_secs(3),
];

/// Go `getIntervalFromPolicy`.
fn get_interval_from_policy(policy: &[Duration], i: usize) -> (Duration, bool) {
    match policy.get(i) {
        Some(interval) => (*interval, true),
        None => (policy[policy.len() - 1], false),
    }
}

/// Go `getJobCheckInterval`: how often a waiting session re-reads a job's
/// history, by the job's action and the number of checks so far.
#[must_use]
pub fn get_job_check_interval(action: ActionType, i: usize) -> (Duration, bool) {
    match action {
        ActionType::ACTION_ADD_INDEX
        | ActionType::ACTION_ADD_PRIMARY_KEY
        | ActionType::ACTION_MODIFY_COLUMN
        | ActionType::ACTION_REORGANIZE_PARTITION
        | ActionType::ACTION_REMOVE_PARTITIONING
        | ActionType::ACTION_ALTER_TABLE_PARTITIONING => {
            get_interval_from_policy(SLOW_DDL_INTERVAL_POLICY, i)
        }
        ActionType::ACTION_CREATE_TABLE | ActionType::ACTION_CREATE_SCHEMA => {
            get_interval_from_policy(FAST_DDL_INTERVAL_POLICY, i)
        }
        _ => get_interval_from_policy(NORMAL_DDL_INTERVAL_POLICY, i),
    }
}

/// Go `chooseLeaseTime`.
#[must_use]
pub fn choose_lease_time(t: Duration, max: Duration) -> Duration {
    if t.is_zero() || t > max {
        max
    } else {
        t
    }
}

/// One step of a persisted job, as Go's owner runs it.
#[derive(Clone)]
struct DdlPhase<'worker> {
    ddl_job_id: i64,
    check_owner: &'worker dyn Fn() -> Result<(), String>,
    schema_state: crate::cluster_ddl::DdlJobSchemaState,
}

struct CommittedDdlPhase {
    report: ClusterDdlReport,
    persisted_job_terminal: bool,
    run_error: Option<(DdlPlanError, i64)>,
    /// The job as the step left it.
    job: Box<tidb_model::Job>,
}

// Go isRetryableJobError tests the next error against the CURRENT process
// limit, after the checkpoint committed. Match the original error: a plain
// action failure is retryable even though toTError persists it as DDL/-1.
pub(crate) fn is_retryable_job_error(error: &DdlPlanError, error_count: i64, limit: i64) -> bool {
    if error_count.wrapping_add(1) >= limit {
        return false;
    }
    let message = error.to_string();
    if tidb_dbterror::REORG_RETRYABLE_ERR_MSGS
        .iter()
        .any(|pattern| message.contains(pattern))
    {
        return true;
    }
    error
        .mysql_error_code()
        .is_none_or(|code| tidb_dbterror::REORG_RETRYABLE_ERR_CODES.contains(&code))
}

// Go worker.loadGlobalVars uses an independent session: a SET GLOBAL committed
// after the action snapshot must affect this error. Failed refresh retains the
// process value published by the previous refresh or local SET GLOBAL.
fn refresh_ddl_error_count_limit(
    sess_pool: &dyn tidb_ddl_build::session_pool::DdlSessionPool,
    ddl_job_id: i64,
) -> i64 {
    // Go `w.loadGlobalVars(vardef.TiDBDDLErrorCountLimit)`.
    let refreshed = sess_pool
        .load_global_vars(&[tidb_vardef::tidb_vars::TIDB_DDL_ERROR_COUNT_LIMIT])
        .map_err(|error| error.to_string())
        .map(|vars| {
            vars.into_iter()
                .find(|(name, _)| name == tidb_vardef::tidb_vars::TIDB_DDL_ERROR_COUNT_LIMIT)
                .map(|(_, value)| {
                    value
                        .parse::<i64>()
                        .unwrap_or(tidb_vardef::defaults::DEF_TIDB_DDL_ERROR_COUNT_LIMIT)
                })
        });
    match refreshed {
        Ok(Some(limit)) => tidb_vardef::set_ddl_error_count_limit(limit),
        Ok(None) => {}
        // Go's `jobCtx.logger` is `DDLLogger().With(zap.Int64("jobID", ...))`.
        Err(error) => tidb_ddl_logutil::ddl_logger()
            .with_fields(&[tidb_log::Field::new(
                "jobID",
                tidb_log::Value::I64(ddl_job_id),
            )])
            .error("load DDL global variable failed", &[zap_error(error)]),
    }
    tidb_vardef::ddl_error_count_limit()
}

enum DdlPhaseOutcome {
    Paused,
    SchemaSync {
        version: i64,
        mdl_info: Option<MdlInfoUpdate>,
        publish_version: bool,
        job: Box<tidb_model::Job>,
    },
    Committed(CommittedDdlPhase),
    /// The job's next step needs a reorganization run first.
    Reorg(Box<crate::cluster_ddl::add_index::AddIndexReorg>),
}

/// The reason the shared persisted worker released its job to the scheduler.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PersistedDdlJobOutcome {
    /// The terminal job was committed to history.
    Finished,
    /// The active job remains paused; SQL waiters must keep waiting for history.
    Paused,
}

/// Go `systable.Manager.GetJobByID`: delivery reads on a pooled session.
pub(crate) fn get_job_by_id(
    backfiller: &dyn IndexBackfiller,
    job_id: i64,
) -> Result<tidb_model::JobW, tidb_ddl_systable::SystemTableManagerError> {
    let pool = backfiller.session_pool().and_then(|pool| pool.sess_pool())
        .ok_or_else(|| tidb_ddl_systable::SystemTableManagerError::Sql("the DDL session pool has no sessions".to_owned()))?;
    tidb_ddl_systable::Manager::get_job_by_id(
        &tidb_ddl_systable::SystemTableManager::new(pool), job_id,
    )
}

/// Go `jobScheduler.deliveryJob`: refresh after a step, retry transient read
/// failures every 500ms, and end delivery on `systable.ErrNotFound`.
fn refresh_delivered_job(
    backfiller: &dyn IndexBackfiller,
    job_w: &mut tidb_model::JobW,
    check_owner: &dyn Fn() -> Result<(), String>,
    wait_retry: &dyn Fn(Duration) -> Result<(), String>,
) -> Result<bool, ClusterDdlError> {
    let job_id = {
        let mut job = job_w.job.as_ref().expect("delivered job").write();
        tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/beforeRefreshJob", &mut *job);
        job.id
    };
    loop {
        check_owner().map_err(ClusterDdlError::SchemaSync)?;
        let fetched = get_job_by_id(backfiller, job_id);
        let (next, mut error) = match fetched {
            Ok(job) => (Some(job), None),
            Err(error) => (None, Some(error)),
        };
        tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/mockGetJobByIDFail", &mut error);
        match error {
            None => {
                *job_w = next.expect("GetJobByID succeeded");
                return Ok(true);
            }
            Some(tidb_ddl_systable::SystemTableManagerError::NotFound) => return Ok(false),
            Some(error) => {
                tidb_ddl_logutil::ddl_logger().error("get job failed", &[zap_error(error)]);
                wait_retry(Duration::from_millis(500)).map_err(ClusterDdlError::SchemaSync)?;
            }
        }
    }
}

/// Go `jobScheduler.deliveryJob`: execute the delivered JobW and refresh
/// it through systable.Manager between steps until final state or ErrNotFound.
#[allow(clippy::too_many_arguments)]
pub fn run_persisted_ddl_job<S: StoreClient>(
    opener: Arc<RealOptimisticTransactionOpener<S>>,
    mut job_w: tidb_model::JobW,
    timeout: Duration,
    job_done: Option<&JobDoneCh>,
    notifier: Option<&dyn SchemaVersionNotifier>,
    backfiller: &dyn IndexBackfiller,
    check_constraint_validator: &dyn CheckConstraintValidator,
    schema_sync: &dyn DdlSchemaSync,
    check_owner: &(dyn Fn() -> Result<(), String> + Sync),
    wait_retry: &dyn Fn(Duration) -> Result<(), String>,
) -> Result<PersistedDdlJobOutcome, ClusterDdlError> {
    if let Some(job) = job_w.job.as_ref() {
        tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/beforeDeliveryJob", &mut *job.write());
    }
    let sess_pool = backfiller.session_pool().ok_or_else(||
        ClusterDdlError::NotCommitted("the DDL worker has no session pool".to_owned()))?;
    let worker = crate::cluster_ddl::DdlStepWorker {
        sess_pool,
        step_buffer: MutationBuffer::new(),
        step_sql: Default::default(),
        store: Some(Arc::new(StoreSnapshotMeta { opener: Arc::clone(&opener), timeout })),
        region_store: backfiller.region_store(),
        auto_ids: backfiller.auto_id_client(),
        start_mode: backfiller.start_mode(),
        step_ctx: Default::default(),
        advisory_locks: Default::default(),
    };
    let mut schema_state = crate::cluster_ddl::DdlJobSchemaState::new(
        tidb_vardef::is_mdl_enabled(tidb_config_kerneltype::is_next_gen()), job_w.clone(), worker,
    );
    schema_state.info_cache = schema_sync.info_cache();
    let reorg_slot = add_index_reorg::ReorgSlot::default();
    let outcome = (|| loop {
        let result = transit_one_job_step_and_wait_sync(
            &opener, &mut job_w, &mut schema_state, timeout, job_done, notifier,
            backfiller, check_constraint_validator, schema_sync, check_owner, wait_retry, &reorg_slot,
        );
        match result {
            Ok(()) if job_w.job.as_ref().unwrap().read().in_final_state() => {
                // Go InFinalState also includes PAUSED: release this worker,
                // retaining the active row for an explicit resume.
                return Ok(if job_w.job.as_ref().unwrap().read().is_paused() {
                    PersistedDdlJobOutcome::Paused
                } else {
                    PersistedDdlJobOutcome::Finished
                });
            }
            Ok(()) => {}
            Err(error) => {
                tidb_ddl_logutil::ddl_logger().info(
                    "transit one job step and wait sync failed", &[zap_error(error)],
                );
            }
        }
        if !refresh_delivered_job(backfiller, &mut job_w, check_owner, wait_retry)? {
            return Ok(PersistedDdlJobOutcome::Finished);
        }
    })();
    // Go `deliveryJob`'s deferred `afterDeliveryJob` and
    // `wk.sess.ReleaseAllAdvisoryLocks()`: job-scoped locks outlive the
    // steps but not the delivery.
    tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/afterDeliveryJob", &mut job_w);
    schema_state.worker.release_all_advisory_locks();
    outcome
}

/// Go `jobScheduler.transitOneJobStepAndWaitSync`: recover the previous
/// version barrier, execute the delivered object, and wait for this step.
#[allow(clippy::too_many_arguments)]
fn transit_one_job_step_and_wait_sync<S: StoreClient>(
    opener: &Arc<RealOptimisticTransactionOpener<S>>,
    job_w: &mut tidb_model::JobW,
    schema_state: &mut crate::cluster_ddl::DdlJobSchemaState,
    timeout: Duration,
    job_done: Option<&JobDoneCh>,
    notifier: Option<&dyn SchemaVersionNotifier>,
    backfiller: &dyn IndexBackfiller,
    check_constraint_validator: &dyn CheckConstraintValidator,
    schema_sync: &dyn DdlSchemaSync,
    check_owner: &(dyn Fn() -> Result<(), String> + Sync),
    wait_retry: &dyn Fn(Duration) -> Result<(), String>,
    reorg_slot: &add_index_reorg::ReorgSlot,
) -> Result<(), ClusterDdlError> {
    let ddl_job_id = job_w.job.as_ref().unwrap().read().id;
    let mut step_committed = false;
    loop {
        // Go `runOneJobStep`: a job that turned to its rollback while a
        // reorganization ran cancels the step context the reorganization
        // still holds.
        if job_w.job.as_ref().unwrap().read().is_rollingback() {
            reorg_slot.cancel_step();
        }
        schema_state.mdl_enabled =
            tidb_vardef::is_mdl_enabled(tidb_config_kerneltype::is_next_gen());
        schema_state.delivered_job = job_w.clone();
        let outcome = commit_cluster_ddl_phase(
            Arc::clone(&opener),
            DdlPhase {
                ddl_job_id,
                check_owner,
                schema_state: schema_state.clone(),
            },
            timeout,
            notifier,
            backfiller,
            check_constraint_validator,
            schema_sync.owner_id(),
        )?;
        match outcome {
            DdlPhaseOutcome::Paused => {
                let mut job = job_w.job.as_ref().unwrap().write();
                tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", &mut *job);
                return Ok(());
            }
            DdlPhaseOutcome::Reorg(reorg) => {
                // Go `runReorgJob`: the owner reorganizes in its own
                // transactions; the next step reads the outcome.
                let outcome = add_index_reorg::run_add_index_reorg(
                    &opener,
                    timeout,
                    backfiller,
                    &reorg,
                    check_owner,
                    reorg_slot,
                )?;
                schema_state.reorg.push(outcome);
            }
            DdlPhaseOutcome::SchemaSync {
                version,
                mdl_info,
                publish_version,
                mut job,
            } => {
                check_owner().map_err(ClusterDdlError::SchemaSync)?;
                // Go `transitOneJobStepAndWaitSync`'s `beforeWaitSchemaSynced`
                // (job, schemaVer), as a snapshot the callback reads.
                tidb_failpoint::inject_call(
                    "github.com/pingcap/tidb/pkg/ddl/beforeWaitSchemaSynced",
                    &mut ((*job).clone(), version),
                );
                if publish_version {
                    // Non-MDL recovery republishes the latest nonempty diff;
                    // MDL recovery only waits for its retained row.
                    notify_schema_version(notifier, version);
                }
                schema_sync
                    .wait_version_synced(ddl_job_id, version)
                    .map_err(ClusterDdlError::SchemaSync)?;
                check_owner().map_err(ClusterDdlError::SchemaSync)?;
                schema_state.synced_version = Some(version);
                schema_state.pending_version = None;
                // Like Go cleanMDLInfo, cleanup is best effort after the
                // acknowledgement succeeded. Only this owner run may reuse
                // that success; a replacement recovers any retained MDL row.
                if let Some(mdl_info) = mdl_info {
                    // Go `cleanMDLInfo`: the statement on a pooled session, in
                    // its own transaction.
                    let pool = &schema_state.worker.sess_pool;
                    let cleaned = pool
                        .exec_restricted_sql(&mdl_info.clean_sql(ddl_job_id, schema_sync.owner_id()));
                    if let Err(error) = cleaned {
                        tidb_ddl_logutil::ddl_logger().warn(
                            "unexpected error when clean mdl info",
                            &[
                                tidb_log::Field::new("job ID", tidb_log::Value::I64(ddl_job_id)),
                                zap_error(error),
                            ],
                        );
                    }
                }
                if step_committed {
                    tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", &mut *job);
                    *job_w.job.as_ref().unwrap().write() = *job;
                    return Ok(());
                }
            }
            DdlPhaseOutcome::Committed(committed) if committed.persisted_job_terminal => {
                // Go `handleJobDone`: after the history commit, the reorg
                // handles are cleaned and only then is the submitting
                // session notified (`jobCtx.notifyDone`); `cleanMDLInfo`
                // follows in `transitOneJobStepAndWaitSync`.
                // Go `removeJobCtx`: the job context goes with the job.
                crate::job_ctx::remove_job_ctx(ddl_job_id);
                if let Err(error) = add_index_reorg::clean_ddl_reorg_handles(
                    schema_state.worker.sess_pool.as_ref(),
                    ddl_job_id,
                ) {
                    // Go also carries `zap.Stringer("job", job)`; this loop
                    // holds only the job's id.
                    tidb_ddl_logutil::ddl_logger().warn(
                        "Failed removing the DDL reorg entry in tidb_ddl_reorg",
                        &[zap_error(error)],
                    );
                }
                if let Some(job_done) = job_done {
                    job_done.notify_done();
                }
                if schema_state.mdl_enabled {
                    if let Err(error) = schema_sync.clean_job_versions(ddl_job_id) {
                        tidb_ddl_logutil::ddl_logger().warn(
                            "delete versions failed",
                            &[
                                tidb_log::Field::new("job ID", tidb_log::Value::I64(ddl_job_id)),
                                zap_error(error),
                            ],
                        );
                    }
                }
                let mut job = committed.job;
                tidb_failpoint::inject_call(
                    "github.com/pingcap/tidb/pkg/ddl/beforeWaitSchemaSynced",
                    &mut ((*job).clone(), 0_i64),
                );
                tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", &mut *job);
                *job_w.job.as_ref().unwrap().write() = *job;
                return Ok(());
            }
            DdlPhaseOutcome::Committed(committed) => {
                // Reorganization outcomes are read by exactly one step.
                schema_state.reorg.clear();
                if let ClusterDdlReport::Applied { schema_version, .. } = &committed.report {
                    if *schema_version > 0 {
                        schema_state.pending_version = Some(*schema_version);
                    }
                }
                // Go `transitOneJobStepAndWaitSync` waits for the step's
                // version (the next pass's schema sync, which then calls
                // `afterWaitSchemaSynced`) or, with no version to wait for,
                // calls it now.
                let mut job = committed.job;
                if schema_state.pending_version.is_none() {
                    tidb_failpoint::inject_call(
                        "github.com/pingcap/tidb/pkg/ddl/beforeWaitSchemaSynced",
                        &mut ((*job).clone(), 0_i64),
                    );
                    tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", &mut *job);
                }
                *job_w.job.as_ref().unwrap().write() = *job;
                step_committed = true;
                if let Some((error, error_count)) = committed.run_error {
                    if is_retryable_job_error(
                        &error,
                        error_count,
                        tidb_vardef::ddl_error_count_limit(),
                    ) {
                        check_owner().map_err(ClusterDdlError::SchemaSync)?;
                        // Go WaitTimeWhenErrorOccurred defaults to one second.
                        // This wait belongs to the committed job step, not the
                        // scheduler's scan cadence or the submitting session.
                        wait_retry(Duration::from_secs(1)).map_err(ClusterDdlError::SchemaSync)?;
                    }
                }
                if schema_state.pending_version.is_none() {
                    return Ok(());
                }
            }
        }
    }
}

/// Pinned Go `GenGIDAndInsertJobsWithRetry`: lock the global-ID key, assign
/// every action-owned ID, run the caller's registration callback, encode all
/// rows, and commit the allocation and inserts atomically.
pub fn gen_gid_and_insert_jobs_with_retry<S, F, Cleanup>(
    opener: Arc<RealOptimisticTransactionOpener<S>>,
    sess_pool: &dyn tidb_ddl_build::session_pool::DdlSessionPool,
    specs: &mut [tidb_ddl_jobsubmit::JobSpec],
    timeout: Duration,
    mut before_insert_with_assigned_ids: F,
) -> Result<(), ClusterDdlError>
where
    S: StoreClient,
    F: FnMut(&[tidb_ddl_jobsubmit::JobSpec]) -> Option<Cleanup>,
    Cleanup: FnOnce(),
{
    let mut attempt = 0_u32;
    loop {
        let transaction = SessionTransaction::begin_pessimistic(
            Arc::clone(&opener),
            timeout,
            ddl_transaction_options(),
        )?;
        let buffer = MutationBuffer::new();
        let lock_outcome =
            match transaction.lock_keys(vec![tidb_meta::key::next_global_id_kv_key()]) {
                Ok(outcome) => outcome,
                Err(error) => {
                    let _ = transaction.rollback();
                    return Err(ClusterDdlError::NotCommitted(error.to_string()));
                }
            };
        let for_update_ts = match lock_outcome {
            LockKeysOutcome::Locked { for_update_ts, .. } => for_update_ts,
            // A newer commit beat the lock: Go's `genGIDAndCallWithRetry`
            // treats the retryable error by starting the attempt over in a
            // new transaction (the statement's staged state is discarded
            // with it).
            LockKeysOutcome::RetryStatement { .. } if attempt + 1 < tidb_txnkv::MAX_RETRY_COUNT => {
                let _ = transaction.rollback();
                std::thread::sleep(tidb_txnkv::retry_backoff_delay(attempt));
                attempt += 1;
                continue;
            }
            LockKeysOutcome::RetryStatement { .. } => {
                let _ = transaction.rollback();
                return Err(ClusterDdlError::NotCommitted(
                    "the global id key kept conflicting".to_owned(),
                ));
            }
            LockKeysOutcome::StatementError(error) | LockKeysOutcome::TransactionError(error) => {
                let _ = transaction.rollback();
                return Err(ClusterDdlError::NotCommitted(error.message));
            }
        };
        // Go `ddlSe`: the job insert is the submitting session's statement
        // in this transaction (reading at its for-update timestamp, as Go's
        // snapshot does).
        let planned = (|| -> Result<_, ClusterDdlError> {
            let mut snapshot = SnapshotMetaSnapshot::new(
                transaction
                    .snapshot_at_for(for_update_ts, true)
                    .map_err(|error| ClusterDdlError::backfill_storage(error))?,
            );
            let mut ddl_se = sess_pool.open_step_session(for_update_ts, &buffer)
                .map_err(DdlPlanError::from_mysql_error)?;
            // The session's BEGIN found an empty buffer; bound now, the
            // statement stages into the transaction that holds the lock (an
            // empty handle attaches without replacing its locks).
            transaction.bind_mutation_buffer(&buffer);
            let mut execute = |sql: &str| ddl_se.execute(sql, &[]).map(drop);
            // Go `GenGIDAndInsertJobsWithRetry`: metadata allocates IDs;
            // the session resolves its own table for the SQL insert.
            let mutations = tidb_ddl_jobsubmit::plan_assign_global_ids(&mut snapshot, specs)?;
            let cleanup = before_insert_with_assigned_ids(specs);
            if let Err(error) = tidb_ddl_jobsubmit::insert_ddl_jobs_2_table(&mut execute, specs) {
                if let Some(cleanup) = cleanup { cleanup(); }
                return Err(error.into());
            }
            Ok((mutations, cleanup))
        })();
        let (mutations, cleanup) = match planned {
            Ok(planned) => planned,
            Err(error) => {
                let _ = transaction.rollback();
                return Err(error);
            }
        };
        match finish_insert_attempt(transaction.commit_with(&buffer, mutations), cleanup) {
            Ok(_) => return Ok(()),
            Err(error) => {
                let cause = classify_session_ddl_commit_error(0, error);
                if matches!(cause, ClusterDdlError::ConcurrentSchemaChange { .. })
                    && attempt + 1 < tidb_txnkv::MAX_RETRY_COUNT
                {
                    std::thread::sleep(tidb_txnkv::retry_backoff_delay(attempt));
                    attempt += 1;
                    continue;
                }
                return Err(cause);
            }
        }
    }
}

/// Go `executor.AlterTableCache` and `genPlacementPolicyID` before submission.
pub(crate) fn prepare_executor_job<S: StoreClient>(
    opener: &Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    backfiller: &dyn IndexBackfiller,
    spec: &tidb_ddl_jobsubmit::JobSpec,
) -> Result<(), ClusterDdlError> {
    // Go `AlterTableCache`: `checkCacheTableSize`, then `replace into
    // mysql.table_cache_meta values (%?, 'NONE', 0, 0)`, each before the job.
    if spec.job.type_ == tidb_model::action_type::ActionType::ACTION_ALTER_CACHE_TABLE {
        let table_id = spec.job.table_id;
        let sess_pool = backfiller.session_pool().ok_or_else(|| {
            ClusterDdlError::NotCommitted("the DDL worker has no session pool".to_owned())
        })?;
        add_index_reorg::reorg_transaction(
            &opener,
            timeout,
            &crate::job_ctx::internal_ddl_source(),
            |meta, handle, buffer, _| {
                let catalog = load_info_schema(meta).map_err(DdlPlanError::Meta)?;
                let table = catalog
                    .stored_schemas()
                    .iter()
                    .flat_map(|database| database.read().deprecated_tables.iter_deref())
                    .find(|table| table.read().id == table_id)
                    .ok_or_else(|| {
                        ClusterDdlError::backfill_message(format!("table {table_id} is gone"))
                    })?;
                let table = table.read();
                backfiller
                    .stage_cache_table_check(&table, handle, buffer)
                    .map_err(ClusterDdlError::IndexBackfill)?;
                buffer.reset();
                Ok(((), Vec::new()))
            },
        )?;
        sess_pool
            .exec_restricted_sql_with(
                "replace into mysql.table_cache_meta values (%?, 'NONE', 0, 0)",
                &[tidb_sqlescape::SqlArg::Signed(table_id)],
            )
            .map_err(DdlPlanError::from_mysql_error)?;
    }
    // Go `genPlacementPolicyID`: the executor draws the policy's id from the
    // policies' own sequence, in a transaction of its own, before the job.
    if spec.job.type_ == tidb_model::action_type::ActionType::ACTION_CREATE_PLACEMENT_POLICY {
        let policy_id = add_index_reorg::reorg_transaction(
            &opener,
            timeout,
            &crate::job_ctx::internal_ddl_source(),
            |meta, _, _, _| {
                let (id, mutation) = tidb_ddl_jobsubmit::reserve_placement_policy_id(meta)?;
                Ok((id, vec![mutation]))
            },
        )?;
        crate::cluster_ddl::placement_policy::gen_placement_policy_id(&spec, policy_id);
    }
    Ok(())
}

/// Go `dom.GetSnapshotMeta(ts)`: a meta reader over the store's snapshot at
/// `ts` (`store.GetSnapshot(kv.NewVersion(ts))`), outside any transaction.
pub(crate) struct StoreSnapshotMeta<S: StoreClient> {
    pub(crate) opener: Arc<RealOptimisticTransactionOpener<S>>,
    pub(crate) timeout: Duration,
}

impl<S: StoreClient> StoreSnapshotMeta<S> {
    fn meta_at(&self, ts: u64) -> Result<SnapshotMetaSnapshot, DdlPlanError> {
        let snapshot = tidb_store_driver_txn::cluster_table_storage::StatementSnapshot::open_at(
            Arc::clone(&self.opener),
            ts,
            self.timeout,
        )
            .map_err(|error| {
                DdlPlanError::from_mysql_error(tidb_terror::MysqlError::unknown(error.to_string()))
            })?;
        Ok(SnapshotMetaSnapshot::new(Box::new(snapshot)))
    }
}

impl<S: StoreClient> crate::cluster_ddl::SnapshotMetaAt for StoreSnapshotMeta<S> {
    fn table_at(
        &self,
        ts: u64,
        schema_id: i64,
        table_id: i64,
    ) -> Result<Option<tidb_model::TableInfo>, DdlPlanError> {
        let mut meta = self.meta_at(ts)?;
        tidb_infoschema::cluster_catalog::with_meta_reader(&mut meta, |reader| {
            match reader.table(schema_id, table_id) {
                Err(tidb_meta::MetaError::DatabaseNotExists) => Ok(None),
                other => other,
            }
        })
        .map_err(DdlPlanError::Meta)
    }

    fn auto_ids_at(
        &self,
        ts: u64,
        schema_id: i64,
        table_id: i64,
    ) -> Result<tidb_model::AutoIdGroup, DdlPlanError> {
        let mut meta = self.meta_at(ts)?;
        tidb_infoschema::cluster_catalog::with_meta_reader(&mut meta, |reader| {
            reader.auto_ids(schema_id, table_id).get()
        })
        .map_err(DdlPlanError::Meta)
    }

    fn database_at(
        &self,
        ts: u64,
        schema_id: i64,
    ) -> Result<Option<tidb_model::db::DBInfo>, DdlPlanError> {
        let mut meta = self.meta_at(ts)?;
        tidb_infoschema::cluster_catalog::with_meta_reader(&mut meta, |reader| {
            reader.database(schema_id)
        })
        .map_err(DdlPlanError::Meta)
    }

    fn tables_at(&self, ts: u64, schema_id: i64) -> Result<Vec<tidb_model::TableInfo>, DdlPlanError> {
        let mut meta = self.meta_at(ts)?;
        tidb_infoschema::cluster_catalog::with_meta_reader(&mut meta, |reader| {
            reader.tables(schema_id)
        })
        .map_err(DdlPlanError::Meta)
    }

    fn schema_version_at(&self, ts: u64) -> Result<i64, DdlPlanError> {
        let mut meta = self.meta_at(ts)?;
        tidb_infoschema::cluster_catalog::with_meta_reader(&mut meta, |reader| reader.schema_version())
            .map_err(DdlPlanError::Meta)
    }

    fn databases_at(&self, ts: u64) -> Result<Vec<tidb_model::db::DBInfo>, DdlPlanError> {
        let mut meta = self.meta_at(ts)?;
        tidb_infoschema::cluster_catalog::with_meta_reader(&mut meta, |reader| reader.databases())
            .map_err(DdlPlanError::Meta)
    }

    fn schema_diff_commit_ts(&self, version: i64, start_ts: u64) -> Result<u64, DdlPlanError> {
        use tidb_txnkv::kv_storage::Storage as _;
        self.opener
            .mvcc_commit_ts(&tidb_meta::key::schema_diff_kv_key(version), start_ts)
            .map_err(|error| DdlPlanError::from_mysql_error(tidb_terror::MysqlError::unknown(error)))
    }
}

/// Go `JobSubmitter.SubmitBatch`'s admission:
/// `sysTblMgr.HasFlashbackClusterJob(ctx, minJobIDRefresher.GetCurrMinJobID())`
/// on a session of `ddl.sessPool`.
pub fn check_flashback_cluster_job(
    session_pool: &dyn tidb_ddl_build::session_pool::DdlSessionPool,
    min_job_id: i64,
) -> Result<(), ClusterDdlError> {
    let encode = |error: tidb_ddl_systable::SystemTableManagerError| {
        ClusterDdlError::from(tidb_ddl_jobsubmit::JobSubmitError::Encode(
            error.to_string(),
        ))
    };
    let pool = session_pool.sess_pool().ok_or_else(|| {
        encode(tidb_ddl_systable::SystemTableManagerError::Sql(
            "the DDL session pool has no sessions".to_owned(),
        ))
    })?;
    let has_flashback_cluster_job = tidb_ddl_systable::Manager::has_flashback_cluster_job(
        &tidb_ddl_systable::SystemTableManager::new(pool),
        min_job_id,
    )
    .map_err(encode)?;
    if has_flashback_cluster_job {
        return Err(tidb_ddl_jobsubmit::JobSubmitError::FlashbackClusterJob.into());
    }
    Ok(())
}

/// Reads one terminal DDL job from Go's authoritative meta history.
pub fn load_history_persisted_ddl_job<S: StoreClient>(
    opener: Arc<RealOptimisticTransactionOpener<S>>,
    ddl_job_id: i64,
    timeout: Duration,
) -> Result<Option<Job>, ClusterDdlError> {
    let transaction = SessionTransaction::begin(
        opener,
        timeout,
        ddl_transaction_options(),
    )?;
    let history = {
        let mut snapshot = SnapshotMetaSnapshot::new(
            transaction
                .snapshot()
                .map_err(|error| ClusterDdlError::backfill_storage(error))?,
        );
        snapshot
            .get(&tidb_meta::key::ddl_job_history_kv_key(ddl_job_id))
            .map_err(DdlPlanError::Meta)?
            .map(|encoded| {
                let mut job = Job::default();
                job.decode(&encoded)
                    .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
                Ok::<Job, DdlPlanError>(job)
            })
            .transpose()?
    };
    transaction
        .rollback()
        .map_err(ClusterDdlError::NotCommitted)?;
    Ok(history)
}

#[allow(clippy::too_many_arguments)]
fn commit_cluster_ddl_phase<S: StoreClient>(
    opener: Arc<RealOptimisticTransactionOpener<S>>,
    phase: DdlPhase<'_>,
    timeout: Duration,
    notifier: Option<&dyn SchemaVersionNotifier>,
    backfiller: &dyn IndexBackfiller,
    check_constraint_validator: &dyn CheckConstraintValidator,
    owner_id: &str,
) -> Result<DdlPhaseOutcome, ClusterDdlError> {
    (phase.check_owner)().map_err(ClusterDdlError::SchemaSync)?;
    // Go `transitOneJobStepAndWaitSync` checks the previous schema barrier
    // before `transitOneJobStep`'s hook and prepareTxn. A cancellation in
    // beforeTransitOneJobStep must be visible at the new transaction's TSO.
    let mut job_w = phase.schema_state.delivered_job.clone();
    let preflight = SessionTransaction::begin(Arc::clone(&opener), timeout, ddl_transaction_options())?;
    let recovery = crate::cluster_ddl::schema_sync_before_step(
        &mut SnapshotMetaSnapshot::new(preflight.snapshot()
            .map_err(|error| ClusterDdlError::backfill_storage(error))?),
        &phase.schema_state, &job_w.job.as_ref().unwrap().read(),
    );
    preflight.rollback().map_err(ClusterDdlError::NotCommitted)?;
    if let Some(PersistedDdlJobPlan::SchemaSync { version, mdl_info, publish_version, job }) = recovery? {
        return Ok(DdlPhaseOutcome::SchemaSync { version, mdl_info, publish_version, job });
    }
    tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/beforeTransitOneJobStep", &mut job_w);
    // Go `prepareTxn`: the step's transaction carries the job's resource
    // group tagger, published once the step has loaded the job.
    let step_kv_context = tidb_txnkv::StatementKvContextCell::default();
    let transaction = SessionTransaction::begin(
        Arc::clone(&opener),
        timeout,
        tidb_store_driver_txn::session_commit_protocol::SessionTransactionOptions::from(
            tidb_store_driver_txn::session_commit_protocol::bootstrap_commit_protocol(),
        )
        .with_statement_kv_context(step_kv_context.clone())
        // Go's scheduler context: `kv.WithInternalSourceType(ctx,
        // kv.InternalTxnDDL)` (`ddl.Start`).
        .with_request_source(tidb_txnkv::RequestSource {
            internal: true,
            source_type: tidb_txnkv::INTERNAL_TXN_DDL.to_owned(),
            explicit_source_type: String::new(),
        }),
    )?;
    let start_ts = transaction.start_ts();
    // Go `w.sess`'s membuffer for this step: the step's session statements
    // stage into it, and it commits with the step's meta writes.
    let mut buffer = MutationBuffer::new();
    let mut schema_state = phase.schema_state.clone();
    schema_state.step_kv_context = Some(step_kv_context);
    // Go `w.sess` for this step, kept until the step commits: the MDL row
    // below runs on it too.
    let mut step_sql: crate::job_txn::StepSqlSlot = Default::default();
    schema_state.worker.step_buffer = buffer.clone();
    schema_state.worker.step_sql = Arc::clone(&step_sql);
    // Go `transitOneJobStep`: `jobCtx.mustRollbackTxnOnError = false`.
    schema_state.worker.step_ctx = Default::default();
    let sess_pool = Arc::clone(&schema_state.worker.sess_pool);
    let plan = {
        let mut snapshot = SnapshotMetaSnapshot::new(
            transaction
                .snapshot()
                .map_err(|error| ClusterDdlError::backfill_storage(error))?,
        )
        .with_auto_ids_opt(schema_state.worker.auto_ids.as_ref());
        match plan_persisted_ddl_job_step(
            &mut snapshot,
            phase.ddl_job_id,
            start_ts,
            schema_state,
            &|| refresh_ddl_error_count_limit(&*sess_pool, phase.ddl_job_id),
        ) {
            Ok(PersistedDdlJobPlan::Paused) => {
                transaction
                    .rollback()
                    .map_err(ClusterDdlError::NotCommitted)?;
                return Ok(DdlPhaseOutcome::Paused);
            }
            Ok(PersistedDdlJobPlan::SchemaSync {
                version,
                mdl_info,
                publish_version,
                job,
            }) => {
                transaction
                    .rollback()
                    .map_err(ClusterDdlError::NotCommitted)?;
                return Ok(DdlPhaseOutcome::SchemaSync {
                    version,
                    mdl_info,
                    publish_version,
                    job,
                });
            }
            Ok(PersistedDdlJobPlan::Reorg(reorg)) => {
                transaction
                    .rollback()
                    .map_err(ClusterDdlError::NotCommitted)?;
                return Ok(DdlPhaseOutcome::Reorg(Box::new(reorg)));
            }
            Ok(PersistedDdlJobPlan::Step(step)) => Ok(step),
            Err(error) => Err(error),
        }
    };
    let step = match plan {
        Ok(step) => step,
        Err(error) => {
            let _ = transaction.rollback();
            return Err(error.into());
        }
    };
    let mut persisted_job_terminal = step.terminal;
    let mut run_error = step.run_error.map(|error| (error, step.error_count));
    let mut job = step.job;
    let mut write = step.write;
    if write.check_constraint_validation.is_some() {
        let stage_action = || {
            transaction
                .snapshot()
                .map_err(|error| ClusterDdlError::backfill_storage(error))
                .and_then(|snapshot| {
                    let slot = Arc::new(Mutex::new(SwappableSnapshot::new()));
                    slot.lock()
                        .map_err(|_| {
                            ClusterDdlError::backfill_message("snapshot slot poisoned".to_owned())
                        })?
                        .bind(snapshot);
                    let handle: Arc<Mutex<dyn ClusterSnapshot>> = slot;
                    if let Some(validation) = &write.check_constraint_validation {
                        check_constraint_validator
                            .validate(validation, Arc::clone(&handle), &buffer)
                            .map_err(ClusterDdlError::Plan)?;
                    }
                    Ok(())
                })
        };
        let staged = recover_ddl_action(stage_action);
        let failure = match staged {
            Ok(Ok(())) => None,
            Err(()) => Some(PersistedDdlJobFailure::Panic),
            Ok(Err(error)) => Some(PersistedDdlJobFailure::Error(match error {
                ClusterDdlError::Plan(error) => error,
                error => DdlPlanError::Encode(error.to_string()),
            })),
        };
        if let Some(failure) = failure {
            let ddl_job_id = phase.ddl_job_id;
            // Rust's planned metadata and staged rows are not applied until
            // commit. Discard both, as Go resets failed action mutations, but
            // keep this transaction's snapshot and conflict protection; the
            // checkpoint's own statements stage into the fresh buffer.
            buffer = MutationBuffer::new();
            step_sql = Default::default();
            let session = crate::job_txn::StepSession {
                pool: Arc::clone(&sess_pool),
                buffer: buffer.clone(),
                sql: Arc::clone(&step_sql),
                start_mode: backfiller.start_mode(),
                step_ctx: Default::default(),
            };
            let checkpoint = plan_persisted_ddl_job_failure(
                ddl_job_id, start_ts, failure, &job,
                &|| refresh_ddl_error_count_limit(&*sess_pool, ddl_job_id), &session,
            );
            let checkpoint = match checkpoint {
                Ok(checkpoint) => checkpoint,
                Err(error) => {
                    let _ = transaction.rollback();
                    return Err(error.into());
                }
            };
            write = checkpoint.write;
            job = checkpoint.job;
            persisted_job_terminal = checkpoint.terminal;
            run_error = checkpoint
                .run_error
                .map(|error| (error, checkpoint.error_count));
        }
    }
    let placement_receipt = if let Some(pd_http_client) = opener.store().pd_http_client() {
        match deliver_placement_bundles(&pd_http_client, &write.placement_bundles, timeout) {
            Ok(receipt) => receipt,
            Err(error) => {
                let _ = transaction.rollback();
                return Err(error);
            }
        }
    } else {
        // Go `infosync.PutRuleBundlesWithDefaultRetry` on a node without
        // PD: the info syncer's `mockPlacementManager`.
        if !write.placement_bundles.is_empty() {
            if let Err(error) = tidb_domain_infosync::placement_manager::put_rule_bundles_with_default_retry(
                &write.placement_bundles,
            ) {
                let _ = transaction.rollback();
                return Err(ClusterDdlError::NotCommitted(format!(
                    "failed to notify PD the placement rules: {error}"
                )));
            }
        }
        None
    };
    let label_receipt = match deliver_label_rules(&write) {
        Ok(receipt) => receipt,
        Err(error) => {
            let _ = transaction.rollback();
            return Err(compensate_external_delivery(
                error,
                placement_receipt.as_ref(),
                None,
                timeout,
            ));
        }
    };
    // Go `createTableAffinityGroupsInPD`, as in the single-phase path.
    if let Err(error) = write.affinity_groups.apply_create() {
        let _ = transaction.rollback();
        return Err(compensate_external_delivery(
            ClusterDdlError::NotCommitted(error),
            placement_receipt.as_ref(),
            label_receipt.as_ref(),
            timeout,
        ));
    }
    if let Some(update) = &write.resource_group_update {
        if let Err(error) = update.apply() {
            let _ = transaction.rollback();
            return Err(compensate_external_delivery(
                ClusterDdlError::NotCommitted(error),
                placement_receipt.as_ref(),
                label_receipt.as_ref(),
                timeout,
            ));
        }
    }
    if let Some(mdl_info) = &write.mdl_info_update {
        let registered = mdl_info.register(
            &crate::job_txn::StepSession {
                pool: Arc::clone(&sess_pool), buffer: buffer.clone(),
                sql: Arc::clone(&step_sql), start_mode: backfiller.start_mode(),
                step_ctx: Default::default(),
            }, start_ts, write.ddl_job_id, write.schema_version, owner_id,
        );
        if let Err(error) = registered {
            let _ = transaction.rollback();
            return Err(error.into());
        }
    }
    // The step's session ends; its writes stay in the step's buffer.
    drop(
        step_sql
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take(),
    );
    if let Err(error) = (phase.check_owner)() {
        let _ = transaction.rollback();
        return Err(compensate_external_delivery(
            ClusterDdlError::SchemaSync(error),
            placement_receipt.as_ref(),
            label_receipt.as_ref(),
            timeout,
        ));
    }
    let planned_version = write.schema_version;
    match transaction.commit_with(&buffer, write.mutations) {
        Ok(_) => {
            // Go `deleteTableAffinityGroupsInPD`: best-effort, once durable.
            write.affinity_groups.apply_delete();
            if planned_version != 0 {
                if phase.schema_state.mdl_enabled {
                    if let Some(notifier) = notifier {
                        notifier
                            .notify(planned_version)
                            .map_err(ClusterDdlError::SchemaSync)?;
                    }
                } else {
                    // Periodic reload can advance non-MDL self-version
                    // keys even when the notification is unavailable.
                    notify_schema_version(notifier, planned_version);
                }
            }
            Ok(DdlPhaseOutcome::Committed(CommittedDdlPhase {
                report: ClusterDdlReport::Applied {
                    schema_version: planned_version,
                    created_id: write.created_id,
                    warnings: write.warnings.clone(),
                },
                persisted_job_terminal,
                run_error,
                job,
            }))
        }
        Err(error) => {
            let cause = classify_session_ddl_commit_error(planned_version, error);
            if matches!(cause, ClusterDdlError::Undetermined(_)) {
                Err(cause)
            } else {
                Err(compensate_external_delivery(
                    cause,
                    placement_receipt.as_ref(),
                    label_receipt.as_ref(),
                    timeout,
                ))
            }
        }
    }
}

/// Go `errno.ErrWriteConflict`, which a lost optimistic prewrite reports.
const WRITE_CONFLICT_CODE: u16 = 9007;

/// Restores the DDL-domain identity after [`SessionTransaction`] has rendered
/// a terminal commit outcome as a SQL error.
#[must_use]
pub fn classify_session_ddl_commit_error(
    planned_version: i64,
    error: LockSqlError,
) -> ClusterDdlError {
    if error.is_result_undetermined() {
        return ClusterDdlError::Undetermined(
            "the session transaction returned no commit verdict".to_owned(),
        );
    }
    // Every catalog change writes `SchemaVersionKey` from a value it read, so
    // TiKV's 9007 here means the same thing it means on the meta-only path:
    // something else committed over this transaction's snapshot.
    if error.code == WRITE_CONFLICT_CODE {
        ClusterDdlError::ConcurrentSchemaChange {
            planned_version,
            detail: error.message,
        }
    } else {
        ClusterDdlError::Commit(error)
    }
}

/// Publishes a committed version, downgrading every failure to a warning.
///
/// Only a committed version is ever announced: a rolled-back change must not
/// make peers reload to a version that does not exist. The announcement is
/// also never retried here — Go retries inside its own etcd helper, and a
/// second attempt from this path would only delay a statement whose result is
/// already durable.
pub fn notify_schema_version(notifier: Option<&dyn SchemaVersionNotifier>, version: i64) {
    if version == 0 {
        return;
    }
    let Some(notifier) = notifier else {
        return;
    };
    match notifier.notify(version) {
        // Not a log line: rust/scripts/run-realtikv-ddl-notify.sh greps it on
        // stderr. Go logs nothing on success.
        Ok(()) => eprintln!(
            "{{\"event\":\"schema_version_notified\",\"schema_version\":{version}}}"
        ),
        // Go `updateGlobalVersionAndWaitSynced`.
        Err(error) => tidb_ddl_logutil::ddl_logger().info(
            "update latest schema version failed",
            &[
                tidb_log::Field::new("ver", tidb_log::Value::I64(version)),
                zap_error(error),
            ],
        ),
    }
}

/// zap `zap.Error(err)`.
fn zap_error(error: impl fmt::Display) -> tidb_log::Field {
    tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_txnkv::transaction::{OptimisticCommitOutcome, OptimisticTransactionReceipt, TransactionCause, UndeterminedTransaction};

    #[test]
    fn committed_action_retry_matches_go_classification_and_budget() {
        let coded = |code, message: &str| {
            DdlPlanError::from_mysql_error(tidb_terror::MysqlError::sql_error(code, message))
        };
        for &code in tidb_dbterror::REORG_RETRYABLE_ERR_CODES.iter() {
            let error = coded(code, "temporary source error");
            assert!(is_retryable_job_error(&error, 1, 3), "{code}");
            assert!(!is_retryable_job_error(&error, 2, 3), "{code}");
        }
        for code in [1049, 1061, 3819, 1105] {
            assert!(!is_retryable_job_error(&coded(code, "permanent"), 1, 3));
        }
        for pattern in tidb_dbterror::REORG_RETRYABLE_ERR_MSGS {
            // Go tests messages before the typed code, but after the limit.
            let error = coded(1105, &format!("wrapped: {pattern}: failed"));
            assert!(is_retryable_job_error(&error, 0, 3));
            assert!(!is_retryable_job_error(&error, 2, 3));
        }
        let plain = DdlPlanError::Encode("unknown action failure".into());
        assert!(is_retryable_job_error(&plain, 1, 3));
        let typed =
            DdlPlanError::from(tidb_dbterror::ERR_NOT_OWNER.generate("owner changed"));
        assert!(is_retryable_job_error(&typed, 1, 3));
        assert_eq!(typed.to_job_error().rfc_code(), "ddl:8201");
        let permanent = DdlPlanError::from(
            tidb_dbterror::ERR_CHECK_CONSTRAINT_IS_VIOLATED.generate("invalid row"),
        );
        assert!(!is_retryable_job_error(&permanent, 1, 3));
        let unknown = DdlPlanError::from(tidb_terror::TerrorError::synthesize(
            tidb_terror::TerrorClass::Ddl,
            tidb_terror::CODE_UNKNOWN,
            "typed unknown",
        ));
        assert!(
            !is_retryable_job_error(&unknown, 1, 3),
            "Go does not retry typed unknown errors"
        );
        for limit in [0, 1, 2] {
            assert!(!is_retryable_job_error(&plain, 1, limit));
        }
    }

    #[test]
    fn a_session_ddl_commit_keeps_an_undetermined_verdict_distinct() {
        let outcome = OptimisticCommitOutcome::Undetermined(UndeterminedTransaction {
            receipt: OptimisticTransactionReceipt::new(1, 2, b"key".to_vec(), 1),
            cause: TransactionCause::Transport {
                detail: "commit response lost".to_owned(),
            },
        });
        let error = tidb_store_driver_error::pessimistic_lock_error::commit_outcome_to_sql_error(&outcome)
            .expect_err("an unknown commit verdict must not answer success");
        assert!(matches!(
            classify_session_ddl_commit_error(61, error),
            ClusterDdlError::Undetermined(_)
        ));
    }

    #[test]
    fn session_ddl_commit_classification_preserves_determinate_errors() {
        let conflict = LockSqlError::from(tidb_terror::MysqlError::from_terror(
            &tidb_kv::kv_errors::ERR_WRITE_CONFLICT
                .gen_with_stack("write conflict at schema version key"),
        ));
        assert!(matches!(
            classify_session_ddl_commit_error(61, conflict),
            ClusterDdlError::ConcurrentSchemaChange {
                planned_version: 61,
                ..
            }
        ));

        let ordinary = LockSqlError::from(tidb_terror::MysqlError::unknown(
            "prewrite transport failed definitively",
        ));
        assert!(matches!(
            classify_session_ddl_commit_error(61, ordinary),
            ClusterDdlError::Commit(error)
                if error.code == 1105
                    && error.state == *b"HY000"
                    && error.message == "prewrite transport failed definitively"
        ));
    }

    #[test]
    fn a_failed_notification_is_a_warning_and_never_reaches_the_client() {
        struct Broken;
        impl SchemaVersionNotifier for Broken {
            fn notify(&self, _: i64) -> Result<(), String> {
                Err("etcd 127.0.0.1:2379 unreachable".to_owned())
            }
        }
        // Go's `job_worker.go` logs this failure and continues (outside MDL):
        // the version is already durable, and every node's lease tick still
        // finds it. Returning nothing is the whole contract being asserted.
        notify_schema_version(Some(&Broken), 61);
        notify_schema_version(None, 61);
    }

    #[test]
    fn only_the_committed_version_is_announced() {
        use std::cell::RefCell;

        struct Recording(RefCell<Vec<i64>>);
        impl SchemaVersionNotifier for Recording {
            fn notify(&self, version: i64) -> Result<(), String> {
                self.0.borrow_mut().push(version);
                Ok(())
            }
        }
        let recording = Recording(RefCell::new(Vec::new()));
        notify_schema_version(Some(&recording), 61);
        assert_eq!(*recording.0.borrow(), vec![61]);
    }
}

/// Preserves the one DDL outcome that is not a failure verdict while mapping
/// every determinate catalog error to its ordinary client diagnostic.
pub fn cluster_ddl_error(error: ClusterDdlError) -> tidb_store_driver_error::sql_query_error::SqlQueryError {
    match error {
        ClusterDdlError::Undetermined(_) => tidb_store_driver_error::sql_query_error::SqlQueryError::result_undetermined(),
        ClusterDdlError::Commit(error) => tidb_store_driver_error::sql_query_error::lock_sql_error(&error),
        ClusterDdlError::IndexBackfill(error) => tidb_store_driver_error::sql_query_error::lock_sql_error(&error),
        // The wire triple of the persisted job error, labelled with the Go
        // identity only when the refusal was a Go error (a plain one has none).
        ClusterDdlError::Plan(error) => tidb_store_driver_error::sql_query_error::SqlQueryError::from(
            tidb_terror::MysqlError::from_sql_error(error.to_sql_error()),
        )
        .with_identity(error.identity()),
        other => tidb_store_driver_error::sql_query_error::SqlQueryError::unknown(other.to_string()),
    }
}


/// Go's DDL context (`ddl.Start`'s `kv.WithInternalSourceType(ctx,
/// kv.InternalTxnDDL)`), from which the submitter's and scheduler's
/// transactions begin.
pub(crate) fn ddl_transaction_options(
) -> tidb_store_driver_txn::session_commit_protocol::SessionTransactionOptions {
    tidb_store_driver_txn::session_commit_protocol::SessionTransactionOptions::from(
        tidb_store_driver_txn::session_commit_protocol::bootstrap_commit_protocol(),
    )
    .with_request_source(crate::job_ctx::internal_ddl_source())
}
