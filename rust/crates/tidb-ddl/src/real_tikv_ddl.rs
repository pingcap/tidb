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

use tidb_ddl_build::session_context::DdlSessionContext;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tidb_kv::cluster_storage::ClusterSnapshot;
use tidb_store_driver_txn::cluster_storage::{MutationBuffer, SwappableSnapshot};
use tidb_model::{ActionType, Job};
use tidb_store_driver_txn::transaction::StoreClient;
use tidb_txnkv::transaction::OptimisticCoordinatorError;
use tidb_store_driver_txn::transaction::RealOptimisticTransactionOpener;

use tidb_infoschema::cluster_catalog::{load_cluster_catalog, MetaSnapshot};
use crate::cluster_ddl::{lower_ddl_with_context, plan_persisted_ddl_job_failure, plan_persisted_ddl_job_step, prepare_check_constraint_job_submission, recover_ddl_action, CheckConstraintValidation, DdlPlan, DdlStatement, DdlWrite, ExchangePartitionValidation, MdlInfoUpdate, PersistedDdlJobFailure, PersistedDdlJobPlan};
use tidb_ddl_build::plan_error::DdlPlanError;
use tidb_ddl_build::table_info_build::DdlAdmissionError;

use tidb_store_driver_txn::cluster_table_storage::{LockKeysOutcome, SessionTransaction};
use tidb_ddl_jobsubmit::{finish_insert_attempt, plan_insert_attempt};
use tidb_ddl_systable::ddl_job_table::DdlJobTable;
use tidb_store_driver_error::pessimistic_lock_error::LockSqlError;
use tidb_infoschema_issyncer::real_tikv_catalog::{SnapshotMetaSnapshot, TransactionMetaSnapshot};

mod add_index_reorg;
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

/// Parses and admits one text-protocol statement as a catalog change.
///
/// `None` means the statement is not a DDL this node owns — including when it
/// does not parse at all, so an unparsable statement is still reported by the
/// query path that owns the same text. This mirrors
/// `tidb_exec::real_tikv_dml::prepare_text_write`: the server never parses SQL or
/// names catalog types itself, so the dependency direction stays
/// `tidb-server -> tidb-exec -> tidb-txnkv`.
///
/// Parsing and default-value admission deliberately share one context: scanner
/// mode, temporal SQL-mode bits, and session-zone normalization cannot drift
/// between the two halves of one `CREATE TABLE`.
pub fn prepare_cluster_ddl_with_context(
    sql: &str,
    default_schema: &str,
    context: &crate::cluster_ddl::DdlContext,
) -> Result<Option<DdlStatement>, DdlAdmissionError> {
    let Ok(statement) = tidb_parser::parse_with_sql_mode(sql, context.sql_mode()) else {
        return Ok(None);
    };
    prepare_cluster_ddl_parsed(&statement, default_schema, context)
}

/// [`prepare_cluster_ddl_with_context`] over a statement the caller parsed
/// under the same context's SQL mode.
pub fn prepare_cluster_ddl_parsed(
    statement: &tidb_ast::Stmt,
    default_schema: &str,
    context: &crate::cluster_ddl::DdlContext,
) -> Result<Option<DdlStatement>, DdlAdmissionError> {
    lower_ddl_with_context(statement, default_schema, context)
}

/// Why a catalog change did not happen.
#[derive(Debug)]
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
    Backfill(String),
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

impl std::error::Error for ClusterDdlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Plan(error) => Some(error),
            Self::Transaction(error) => Some(error),
            Self::ConcurrentSchemaChange { .. }
            | Self::Commit(_)
            | Self::NotCommitted(_)
            | Self::Undetermined(_)
            | Self::Backfill(_)
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
/// definition lives in the executor's `KvTable` over the same
/// [`ClusterTableStorage`] seam the session writes through. Implementing the
/// walk here would be a second index-entry writer, and two of those drift —
/// which on an index means a query returns rows that are not there, or misses
/// rows that are, with no error either way.
///
/// [`ClusterTableStorage`]: tidb_store_driver_txn::cluster_storage::ClusterTableStorage
pub trait IndexBackfiller {
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

    /// The node's live row-ID allocators, for the `mysql.*` rows the owner
    /// inserts (Go's internal INSERTs: the reorg handle, the delete ranges).
    fn system_row_ids(&self) -> Arc<dyn tidb_table::table_from_meta::TableAutoIds>;
}

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
    },
}

/// See [`IndexBackfiller::reorg_add_index`].
pub enum AddIndexReorgAnswer {
    /// The record range, `None` for an empty physical table.
    Range(Option<(Vec<u8>, Vec<u8>)>),
    /// What a batch did.
    Batch(tidb_ddl_build::backfilling::ReorgBatch),
    /// What a column check found: `None` when every row fits.
    Checked(Option<LockSqlError>),
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
}

// Go isRetryableJobError tests the next error against the CURRENT process
// limit, after the checkpoint committed. Match the original error: a plain
// action failure is retryable even though toTError persists it as DDL/-1.
fn is_retryable_job_error(error: &DdlPlanError, error_count: i64, limit: i64) -> bool {
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
fn refresh_ddl_error_count_limit<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
    timeout: Duration,
    ddl_job_id: i64,
) -> i64 {
    let refreshed = (|| -> Result<_, String> {
        let mut tx = opener
            .begin_read_only()
            .map_err(|error| error.to_string())?;
        let result = (|| {
            let mut snapshot = TransactionMetaSnapshot::new(&mut tx, timeout);
            let catalog = load_cluster_catalog(&mut snapshot).map_err(|error| error.to_string())?;
            tidb_syssession::cluster_sysvar_load::load_cluster_sysvars(&mut snapshot, &catalog)
                .map_err(|error| error.to_string())
        })();
        let finished = tx
            .finish_without_writes()
            .map_err(|error| error.to_string());
        let vars = result?;
        finished?;
        Ok(vars
            .into_iter()
            .find(|(name, _)| name == tidb_vardef::tidb_vars::TIDB_DDL_ERROR_COUNT_LIMIT)
            .map(|(_, value)| {
                value
                    .parse::<i64>()
                    .unwrap_or(tidb_vardef::defaults::DEF_TIDB_DDL_ERROR_COUNT_LIMIT)
            }))
    })();
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
    },
    Committed(CommittedDdlPhase),
    /// The job's next step needs a reorganization run first.
    Reorg(Box<crate::cluster_ddl::add_index::AddIndexReorg>),
}

/// Answers a statement Go's executor finishes without submitting a job: the
/// ALTER forms it accepts and changes nothing for (`plan_ddl`). The catalog
/// is read at one snapshot and nothing is written.
pub fn answer_ddl_without_job<S: StoreClient>(
    opener: Arc<RealOptimisticTransactionOpener<S>>,
    statement: &DdlStatement,
    timeout: Duration,
) -> Result<ClusterDdlReport, ClusterDdlError> {
    let transaction = SessionTransaction::begin(
        opener,
        timeout,
        tidb_store_driver_txn::session_commit_protocol::bootstrap_commit_protocol(),
    )?;
    let plan = transaction
        .snapshot()
        .map_err(|error| ClusterDdlError::Backfill(error.to_string()))
        .and_then(|snapshot| {
            crate::cluster_ddl::plan_ddl(&mut SnapshotMetaSnapshot::new(snapshot), statement)
                .map_err(ClusterDdlError::from)
        });
    transaction
        .rollback()
        .map_err(ClusterDdlError::NotCommitted)?;
    match plan? {
        DdlPlan::AlreadySatisfied { detail, warnings } => {
            Ok(ClusterDdlReport::AlreadySatisfied { detail, warnings })
        }
        DdlPlan::Write(_) => unreachable!("a statement without a job writes nothing"),
    }
}

/// The reason the shared persisted worker released its job to the scheduler.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PersistedDdlJobOutcome {
    /// The terminal job was committed to history.
    Finished,
    /// The active job remains paused; SQL waiters must keep waiting for history.
    Paused,
}

/// Runs an integrated persisted action until it finishes or pauses. A durable
/// MDL row always takes precedence over the next action or history transaction.
/// Returning Paused releases the worker without completing the submitting SQL.
/// `wait_retry` waits for the requested interval or returns an error when this
/// owner's worker context is cancelled. It must not use statement cancellation.
#[allow(clippy::too_many_arguments)]
pub fn run_persisted_ddl_job<S: StoreClient>(
    opener: Arc<RealOptimisticTransactionOpener<S>>,
    ddl_job_id: i64,
    timeout: Duration,
    job_done: Option<&JobDoneCh>,
    notifier: Option<&dyn SchemaVersionNotifier>,
    backfiller: &dyn IndexBackfiller,
    check_constraint_validator: &dyn CheckConstraintValidator,
    schema_sync: &dyn DdlSchemaSync,
    check_owner: &dyn Fn() -> Result<(), String>,
    wait_retry: &dyn Fn(Duration) -> Result<(), String>,
) -> Result<PersistedDdlJobOutcome, ClusterDdlError> {
    let mut schema_state = crate::cluster_ddl::DdlJobSchemaState::new(tidb_vardef::is_mdl_enabled(
        tidb_config_kerneltype::is_next_gen(),
    ));
    loop {
        schema_state.mdl_enabled =
            tidb_vardef::is_mdl_enabled(tidb_config_kerneltype::is_next_gen());
        let outcome = commit_cluster_ddl_phase_with_retry(
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
            DdlPhaseOutcome::Paused => return Ok(PersistedDdlJobOutcome::Paused),
            DdlPhaseOutcome::Reorg(reorg) => {
                // Go `runReorgJob`: the owner reorganizes in its own
                // transactions; the next step reads the outcome.
                let outcome = add_index_reorg::run_add_index_reorg(
                    &opener,
                    timeout,
                    backfiller,
                    &reorg,
                    check_owner,
                )?;
                schema_state.reorg.push(outcome);
            }
            DdlPhaseOutcome::SchemaSync {
                version,
                mdl_info,
                publish_version,
            } => {
                check_owner().map_err(ClusterDdlError::SchemaSync)?;
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
                    if let Err(error) = clean_mdl_info_with_retry(
                        Arc::clone(&opener),
                        timeout,
                        &mdl_info,
                        ddl_job_id,
                        version,
                        schema_sync.owner_id(),
                    ) {
                        tidb_ddl_logutil::ddl_logger().warn(
                            "unexpected error when clean mdl info",
                            &[
                                tidb_log::Field::new("job ID", tidb_log::Value::I64(ddl_job_id)),
                                zap_error(error),
                            ],
                        );
                    }
                }
            }
            DdlPhaseOutcome::Committed(committed) if committed.persisted_job_terminal => {
                // Go `handleJobDone`: after the history commit, the reorg
                // handles are cleaned and only then is the submitting
                // session notified (`jobCtx.notifyDone`); `cleanMDLInfo`
                // follows in `transitOneJobStepAndWaitSync`.
                #[cfg(feature = "failpoints")]
                fail::fail_point!("beforeCleanupDDLReorgHandles");
                if let Err(error) =
                    add_index_reorg::clean_ddl_reorg_handles(&opener, timeout, ddl_job_id)
                {
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
                return Ok(PersistedDdlJobOutcome::Finished);
            }
            DdlPhaseOutcome::Committed(committed) => {
                // Reorganization outcomes are read by exactly one step.
                schema_state.reorg.clear();
                if let ClusterDdlReport::Applied { schema_version, .. } = &committed.report {
                    if *schema_version > 0 {
                        schema_state.pending_version = Some(*schema_version);
                    }
                }
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
            }
        }
    }
}

/// Pinned Go `GenGIDAndInsertJobsWithRetry`: lock the global-ID key, assign
/// every action-owned ID, run the caller's registration callback, encode all
/// rows, and commit the allocation and inserts atomically.
pub fn gen_global_ids_and_insert_jobs_with_retry<S, F, Cleanup>(
    opener: Arc<RealOptimisticTransactionOpener<S>>,
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
            tidb_store_driver_txn::session_commit_protocol::bootstrap_commit_protocol(),
        )?;
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
        let planned = (|| -> Result<_, ClusterDdlError> {
            let mut snapshot = SnapshotMetaSnapshot::new(
                transaction
                    .snapshot_at_for(for_update_ts, true)
                    .map_err(|error| ClusterDdlError::Backfill(error.to_string()))?,
            );
            let catalog = load_cluster_catalog(&mut snapshot).map_err(DdlPlanError::from)?;
            plan_insert_attempt(
                &mut snapshot,
                &catalog,
                specs,
                &mut before_insert_with_assigned_ids,
            )
            .map_err(Into::into)
        })();
        let (mutations, cleanup) = match planned {
            Ok(planned) => planned,
            Err(error) => {
                let _ = transaction.rollback();
                return Err(error);
            }
        };
        let buffer = MutationBuffer::new();
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

/// Submits one persisted CHECK DDL job without executing it.
///
/// Go's submitter and owner scheduler are separate. Exposing the boundary is
/// what lets a non-owner connection enqueue work and wait while the elected
/// owner (possibly another server) executes it. Answers the job's id and
/// action.
pub fn submit_check_constraint_job_with_retry<S: StoreClient>(
    opener: Arc<RealOptimisticTransactionOpener<S>>,
    statement: &DdlStatement,
    timeout: Duration,
    upgrading: bool,
    min_job_id: i64,
    done_channels: Option<&DdlJobDoneChMap>,
) -> Result<(i64, ActionType), ClusterDdlError> {
    let preparation = SessionTransaction::begin(
        Arc::clone(&opener),
        timeout,
        tidb_store_driver_txn::session_commit_protocol::bootstrap_commit_protocol(),
    )?;
    let start_ts = preparation.start_ts();
    let prepared = {
        let mut snapshot = SnapshotMetaSnapshot::new(
            preparation
                .snapshot()
                .map_err(|error| ClusterDdlError::Backfill(error.to_string()))?,
        );
        prepare_check_constraint_job_submission(
            &mut snapshot,
            statement,
            start_ts,
            upgrading,
            min_job_id,
        )
    };
    let mut spec = match prepared {
        Ok(Some(spec)) => spec,
        Ok(None) => {
            let _ = preparation.rollback();
            return Err(ClusterDdlError::NotCommitted(
                "the statement is not a CHECK DDL job".to_owned(),
            ));
        }
        Err(error) => {
            let _ = preparation.rollback();
            return Err(error.into());
        }
    };
    preparation
        .rollback()
        .map_err(ClusterDdlError::NotCommitted)?;

    // Go `BeforeInsertWithAssignedIDs: s.registerJobDoneChannels`.
    gen_global_ids_and_insert_jobs_with_retry(
        opener,
        std::slice::from_mut(&mut spec),
        timeout,
        |specs| done_channels.map(|channels| channels.register_job_done_channels(specs)),
    )?;
    Ok((spec.job.id, spec.job.type_))
}

/// What submitting an ADD INDEX came to.
pub enum AddIndexSubmitted {
    /// The job queued, by id and action, with the executor's statement
    /// warnings.
    Job(i64, ActionType, Vec<tidb_context::SqlWarn>),
    /// `IF NOT EXISTS` over an existing index: nothing queued.
    AlreadySatisfied(ClusterDdlReport),
}

/// Go's executor halves for every statement this node submits as a
/// persisted job: the statement checked and its job built and prepared for
/// insertion (`doDDLJob2` up to the insert), against `snapshot`.
/// `has_null` answers Go `checkForNullValue`'s query on a pooled
/// session.
///
/// # Errors
///
/// The statement's refusal.
#[allow(clippy::too_many_arguments)]
pub fn prepare_persisted_job_submission<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    job_context: &dyn DdlSessionContext,
    targets: &[(String, String)],
    start_ts: u64,
    upgrading: bool,
    min_job_id: i64,
    store_count: usize,
    tiflash_stores: &dyn Fn() -> Result<u64, String>,
    has_null: &mut dyn FnMut(&str, &tidb_model::TableInfo, i64) -> Result<bool, DdlPlanError>,
) -> Result<Option<crate::cluster_ddl::add_index::AddIndexSubmission>, DdlPlanError> {
    let snapshot = &mut *snapshot;
    match statement {
        DdlStatement::MultiSchemaChange { .. } => {
            crate::cluster_ddl::multi_schema::prepare_multi_schema_job_submission(
                snapshot,
                statement,
                job_context,
                start_ts,
                upgrading,
                min_job_id,
                store_count,
                tiflash_stores,
                has_null,
            )
        }
        DdlStatement::AddForeignKey { .. } => {
            crate::cluster_ddl::foreign_key::prepare_add_foreign_key_job_submission(
                snapshot,
                statement,
                job_context,
                start_ts,
                upgrading,
                min_job_id,
                store_count,
            )
        }
        DdlStatement::DropTable { .. }
        | DdlStatement::DropView { .. }
        | DdlStatement::DropSequence { .. }
        | DdlStatement::DropDatabase { .. } => {
            crate::cluster_ddl::drop_object::prepare_drop_object_job_submission(
                snapshot,
                statement,
                job_context,
                targets,
                start_ts,
                upgrading,
                min_job_id,
            )
        }
        DdlStatement::CreatePlacementPolicy { .. }
        | DdlStatement::AlterPlacementPolicy { .. }
        | DdlStatement::DropPlacementPolicy { .. } => {
            crate::cluster_ddl::placement_policy::prepare_placement_policy_job_submission(
                snapshot,
                statement,
                job_context,
                start_ts,
                upgrading,
                min_job_id,
            )
        }
        DdlStatement::CreateResourceGroup { .. }
        | DdlStatement::AlterResourceGroup { .. }
        | DdlStatement::DropResourceGroup { .. } => {
            crate::cluster_ddl::resource_group::prepare_resource_group_job_submission(
                snapshot,
                statement,
                job_context,
                start_ts,
                upgrading,
                min_job_id,
            )
        }
        DdlStatement::ExchangePartition { .. } => {
            crate::cluster_ddl::exchange_partition::prepare_exchange_partition_job_submission(
                snapshot,
                statement,
                job_context,
                start_ts,
                upgrading,
                min_job_id,
            )
        }
        DdlStatement::AddPartitions { .. }
        | DdlStatement::DropPartitions { .. }
        | DdlStatement::TruncatePartitions { .. } => {
            match crate::cluster_ddl::partition::prepare_partition_job_submission(
                snapshot,
                statement,
                job_context,
                start_ts,
                upgrading,
                min_job_id,
            ) {
                // An ADD on a HASH/KEY table is Go's
                // `hashPartitionManagement`, a reorganization.
                Ok(None) => crate::cluster_ddl::reorganize_partition::prepare_reorganize_partition_job_submission(
                    snapshot,
                    statement,
                    job_context,
                    start_ts,
                    upgrading,
                    min_job_id,
                    store_count,
                ),
                other => other,
            }
        }
        DdlStatement::ReorganizePartitions { .. } => {
            crate::cluster_ddl::reorganize_partition::prepare_reorganize_partition_job_submission(
                snapshot,
                statement,
                job_context,
                start_ts,
                upgrading,
                min_job_id,
                store_count,
            )
        }
        DdlStatement::LockTables { .. } | DdlStatement::UnlockTables { .. } => {
            crate::cluster_ddl::table_lock::prepare_table_lock_job_submission(
                snapshot,
                statement,
                job_context,
                start_ts,
                upgrading,
                min_job_id,
            )
        }
        DdlStatement::ModifySchemaCharsetAndCollate { .. }
        | DdlStatement::ModifySchemaDefaultPlacement { .. } => {
            crate::cluster_ddl::alter_schema::prepare_alter_schema_job_submission(
                snapshot,
                statement,
                job_context,
                start_ts,
                upgrading,
                min_job_id,
            )
        }
        DdlStatement::CreateView { .. }
        | DdlStatement::CreateSequence { .. }
        | DdlStatement::CreateTableLike { .. }
        | DdlStatement::AlterSequence { .. } => {
            crate::cluster_ddl::create_object::prepare_create_object_job_submission(
                snapshot,
                statement,
                job_context,
                start_ts,
                upgrading,
                min_job_id,
            )
        }
        DdlStatement::CreateTable { .. } => {
            crate::cluster_ddl::create_table::prepare_create_table_job_submission(
                snapshot,
                statement,
                job_context,
                start_ts,
                upgrading,
                min_job_id,
            )
        }
        DdlStatement::TruncateTable { .. } => {
            crate::cluster_ddl::truncate::prepare_truncate_table_job_submission(
                snapshot,
                statement,
                job_context,
                start_ts,
                upgrading,
                min_job_id,
            )
        }
        statement if crate::cluster_ddl::alter_table::is_alter_table_statement(statement) => {
            crate::cluster_ddl::alter_table::prepare_alter_table_job_submission(
                snapshot,
                statement,
                job_context,
                start_ts,
                upgrading,
                min_job_id,
                tiflash_stores,
            )
        }
        DdlStatement::RenameTable { .. } | DdlStatement::RenameTables { .. } => {
            crate::cluster_ddl::rename_table::prepare_rename_table_job_submission(
                snapshot,
                statement,
                job_context,
                start_ts,
                upgrading,
                min_job_id,
            )
        }
        DdlStatement::CreateDatabase { .. } => {
            crate::cluster_ddl::create_schema::prepare_create_schema_job_submission(
                snapshot,
                statement,
                job_context,
                start_ts,
                upgrading,
                min_job_id,
            )
        }
        DdlStatement::ModifyColumn { .. } | DdlStatement::RenameColumn { .. } => {
            crate::cluster_ddl::modify_column::prepare_modify_column_job_submission(
                snapshot,
                statement,
                job_context,
                start_ts,
                upgrading,
                min_job_id,
                store_count,
                has_null,
            )
        }
        DdlStatement::AddColumn { .. } => {
            crate::cluster_ddl::add_column::prepare_add_column_job_submission(
                snapshot,
                statement,
                job_context,
                start_ts,
                upgrading,
                min_job_id,
            )
        }
        DdlStatement::DropColumn { .. } => {
            crate::cluster_ddl::drop_column::prepare_drop_column_job_submission(
                snapshot,
                statement,
                job_context,
                start_ts,
                upgrading,
                min_job_id,
            )
        }
        DdlStatement::DropIndex { .. } | DdlStatement::DropPrimaryKey { .. } => {
            crate::cluster_ddl::drop_index::prepare_drop_index_job_submission(
                snapshot,
                statement,
                job_context,
                start_ts,
                upgrading,
                min_job_id,
            )
        }
        _ => crate::cluster_ddl::add_index::prepare_add_index_job_submission(
            snapshot,
            statement,
            start_ts,
            upgrading,
            min_job_id,
            store_count,
            has_null,
            job_context,
        ),
    }
}

/// Go's executor up to `doDDLJob2` for the statement kinds this node submits
/// as persisted jobs (ADD / DROP INDEX and PRIMARY KEY, DROP TABLE / VIEW /
/// SEQUENCE / DATABASE): one job queued without executing it; the owner
/// steps it through Go's schema states.
pub fn submit_persisted_job_with_retry<S: StoreClient>(
    opener: Arc<RealOptimisticTransactionOpener<S>>,
    statement: &DdlStatement,
    job_context: &dyn DdlSessionContext,
    targets: &[(String, String)],
    timeout: Duration,
    upgrading: bool,
    min_job_id: i64,
    backfiller: &dyn IndexBackfiller,
    auto_ids: Option<&Arc<tidb_autoid::auto_id_client::AutoIdClient>>,
    done_channels: Option<&DdlJobDoneChMap>,
) -> Result<AddIndexSubmitted, ClusterDdlError> {
    // Go `CalcMaxNodeCountByStoresNum` reads PD's store list.
    let store_count = opener
        .store()
        .store_addresses()
        .map_or(0, |stores| stores.len());
    let preparation = SessionTransaction::begin(
        Arc::clone(&opener),
        timeout,
        tidb_store_driver_txn::session_commit_protocol::bootstrap_commit_protocol(),
    )?;
    let start_ts = preparation.start_ts();
    let prepared = {
        let mut snapshot = SnapshotMetaSnapshot::new(
            preparation
                .snapshot()
                .map_err(|error| ClusterDdlError::Backfill(error.to_string()))?,
        )
        .with_auto_ids_opt(auto_ids);
        // Go `checkForNullValue`'s query, on a pooled session.
        let mut has_null = |schema: &str, table: &tidb_model::TableInfo, column_id: i64| {
            backfiller
                .has_null_value(schema, table, column_id)
                .map_err(|error| {
                    DdlPlanError::from_mysql_error(
                        tidb_terror::MysqlError::from_parts(
                            error.code,
                            error.state,
                            error.message,
                        )
                        .with_identity_opt(error.identity),
                    )
                })
        };
        let tiflash_stores = || opener.store().tiflash_store_count();
        prepare_persisted_job_submission(
            &mut snapshot,
            statement,
            job_context,
            targets,
            start_ts,
            upgrading,
            min_job_id,
            store_count,
            &tiflash_stores,
            &mut has_null,
        )
    };
    let _ = preparation.rollback();
    let (mut spec, warnings) = match prepared? {
        Some(crate::cluster_ddl::add_index::AddIndexSubmission::Job(spec, warnings)) => {
            (*spec, warnings)
        }
        Some(crate::cluster_ddl::add_index::AddIndexSubmission::AlreadySatisfied {
            detail,
            warnings,
        }) => {
            return Ok(AddIndexSubmitted::AlreadySatisfied(
                ClusterDdlReport::AlreadySatisfied { detail, warnings },
            ))
        }
        None => {
            return Err(ClusterDdlError::NotCommitted(
                "the statement is not submitted as a persisted job".to_owned(),
            ))
        }
    };
    // Go `AlterTableCache`: `checkCacheTableSize`, then `replace into
    // mysql.table_cache_meta values (%?, 'NONE', 0, 0)`, each before the job.
    if spec.job.type_ == tidb_model::action_type::ActionType::ACTION_ALTER_CACHE_TABLE {
        let table_id = spec.job.table_id;
        add_index_reorg::reorg_transaction(&opener, timeout, |meta, handle, buffer, _| {
            let catalog = load_cluster_catalog(meta).map_err(DdlPlanError::Catalog)?;
            let table = catalog
                .databases
                .iter()
                .flat_map(|database| database.tables.iter())
                .find(|table| table.id == table_id)
                .ok_or_else(|| ClusterDdlError::Backfill(format!("table {table_id} is gone")))?;
            backfiller
                .stage_cache_table_check(table, handle, buffer)
                .map_err(ClusterDdlError::IndexBackfill)?;
            buffer.reset();
            let mut writes = Vec::new();
            crate::cluster_ddl::write_table_cache_meta_row(&catalog, table_id, &mut writes)?;
            Ok(((), writes))
        })?;
    }
    // Go `genPlacementPolicyID`: the executor draws the policy's id from the
    // policies' own sequence, in a transaction of its own, before the job.
    if spec.job.type_ == tidb_model::action_type::ActionType::ACTION_CREATE_PLACEMENT_POLICY {
        let policy_id = add_index_reorg::reorg_transaction(&opener, timeout, |meta, _, _, _| {
            let (id, mutation) = tidb_ddl_jobsubmit::reserve_placement_policy_id(meta)?;
            Ok((id, vec![mutation]))
        })?;
        crate::cluster_ddl::placement_policy::set_placement_policy_id(&spec, policy_id);
    }
    // Go `BeforeInsertWithAssignedIDs: s.registerJobDoneChannels`.
    gen_global_ids_and_insert_jobs_with_retry(
        opener,
        std::slice::from_mut(&mut spec),
        timeout,
        |specs| done_channels.map(|channels| channels.register_job_done_channels(specs)),
    )?;
    Ok(AddIndexSubmitted::Job(
        spec.job.id,
        spec.job.type_,
        warnings,
    ))
}

/// Loads the persisted active DDL queue from a fresh cluster snapshot.
///
/// The returned order is the job table's primary-key order, matching Go's
/// scheduler query `ORDER BY job_id`. The read transaction is always rolled
/// back because queue inspection publishes no state.
pub fn load_active_persisted_ddl_jobs<S: StoreClient>(
    opener: Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    min_job_id: i64,
) -> Result<Vec<Job>, ClusterDdlError> {
    let mut cached = None;
    load_active_persisted_ddl_jobs_cached(opener, timeout, min_job_id, &mut cached)
}

/// Scheduler-tick variant of [`load_active_persisted_ddl_jobs`] that keeps
/// the located `mysql.tidb_ddl_job` table metadata across ticks.
///
/// Go's owner polls `mysql.tidb_ddl_job` through a session whose
/// InfoSchema caches the system table, so a poll costs one indexed read of
/// the job table and never a catalog walk. Re-deriving the table from a
/// full [`load_cluster_catalog`] on every one-second tick re-parses every
/// schema's `TableInfo` JSON each time and showed up as a double-digit
/// percentage of node CPU under load. The bootstrap system table's layout
/// does not change between ticks, so only its row scan is per-tick work;
/// on any read error the cache is dropped so the next tick re-locates the
/// table exactly like the uncached path.
pub fn load_active_persisted_ddl_jobs_cached<S: StoreClient>(
    opener: Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    min_job_id: i64,
    cached_job_table: &mut Option<DdlJobTable>,
) -> Result<Vec<Job>, ClusterDdlError> {
    let transaction = SessionTransaction::begin(
        opener,
        timeout,
        tidb_store_driver_txn::session_commit_protocol::bootstrap_commit_protocol(),
    )?;
    let jobs = {
        let mut snapshot = SnapshotMetaSnapshot::new(
            transaction
                .snapshot()
                .map_err(|error| ClusterDdlError::Backfill(error.to_string()))?,
        );
        if cached_job_table.is_none() {
            let catalog = load_cluster_catalog(&mut snapshot).map_err(DdlPlanError::Catalog)?;
            *cached_job_table = Some(
                DdlJobTable::locate(&catalog)
                    .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
            );
        }
        // Requiring `&mut snapshot` means the cache can only hold table
        // metadata; the row scan below is always against this tick's
        // fresh snapshot.
        let load = cached_job_table
            .as_ref()
            .expect("job table cache is populated above")
            .load_from(&mut snapshot, min_job_id);
        match load {
            Ok(active) => active.into_iter().map(|active| active.job).collect(),
            Err(error) => {
                // A locate-time layout change (or a transient snapshot
                // error) invalidates the cached table; the next tick
                // re-derives it from the catalog like the uncached path.
                *cached_job_table = None;
                return Err(DdlPlanError::Encode(error.to_string()).into());
            }
        }
    };
    transaction
        .rollback()
        .map_err(ClusterDdlError::NotCommitted)?;
    Ok(jobs)
}

/// Reads pinned Go `systable.Manager.GetMinJobID` in a fresh read-only
/// transaction for `MinJobIDRefresher`.
pub fn load_min_persisted_ddl_job_id<S: StoreClient>(
    opener: Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    previous_min_job_id: i64,
) -> Result<i64, ClusterDdlError> {
    let mut cached = None;
    load_min_persisted_ddl_job_id_cached(opener, timeout, previous_min_job_id, &mut cached)
}

/// Refresher variant of [`load_min_persisted_ddl_job_id`] that caches the
/// located job table across refresh ticks for the same reason as
/// [`load_active_persisted_ddl_jobs_cached`].
pub fn load_min_persisted_ddl_job_id_cached<S: StoreClient>(
    opener: Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    previous_min_job_id: i64,
    cached_job_table: &mut Option<DdlJobTable>,
) -> Result<i64, ClusterDdlError> {
    let transaction = SessionTransaction::begin(
        opener,
        timeout,
        tidb_store_driver_txn::session_commit_protocol::bootstrap_commit_protocol(),
    )?;
    let minimum = {
        let mut snapshot = SnapshotMetaSnapshot::new(
            transaction
                .snapshot()
                .map_err(|error| ClusterDdlError::Backfill(error.to_string()))?,
        );
        if cached_job_table.is_none() {
            let catalog = load_cluster_catalog(&mut snapshot).map_err(DdlPlanError::Catalog)?;
            *cached_job_table = Some(
                DdlJobTable::locate(&catalog)
                    .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
            );
        }
        let read = cached_job_table
            .as_ref()
            .expect("job table cache is populated above")
            .min_job_id(&mut snapshot, previous_min_job_id);
        match read {
            Ok(minimum) => minimum.unwrap_or_default(),
            Err(error) => {
                *cached_job_table = None;
                return Err(DdlPlanError::Encode(error.to_string()).into());
            }
        }
    };
    transaction
        .rollback()
        .map_err(ClusterDdlError::NotCommitted)?;
    Ok(minimum)
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
        tidb_store_driver_txn::session_commit_protocol::bootstrap_commit_protocol(),
    )?;
    let history = {
        let mut snapshot = SnapshotMetaSnapshot::new(
            transaction
                .snapshot()
                .map_err(|error| ClusterDdlError::Backfill(error.to_string()))?,
        );
        snapshot
            .get(&tidb_meta::key::ddl_job_history_kv_key(ddl_job_id))
            .map_err(DdlPlanError::Catalog)?
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
fn commit_cluster_ddl_phase_with_retry<S: StoreClient>(
    opener: Arc<RealOptimisticTransactionOpener<S>>,
    phase: DdlPhase<'_>,
    timeout: Duration,
    notifier: Option<&dyn SchemaVersionNotifier>,
    backfiller: &dyn IndexBackfiller,
    check_constraint_validator: &dyn CheckConstraintValidator,
    owner_id: &str,
) -> Result<DdlPhaseOutcome, ClusterDdlError> {
    let mut attempt: u32 = 0;
    loop {
        match commit_cluster_ddl_with_backfill_once(
            Arc::clone(&opener),
            phase.clone(),
            timeout,
            notifier,
            backfiller,
            check_constraint_validator,
            owner_id,
        ) {
            Err(ClusterDdlError::ConcurrentSchemaChange { .. })
                if attempt + 1 < tidb_txnkv::MAX_RETRY_COUNT =>
            {
                std::thread::sleep(tidb_txnkv::retry_backoff_delay(attempt));
                attempt += 1;
            }
            outcome => return outcome,
        }
    }
}

fn commit_cluster_ddl_with_backfill_once<S: StoreClient>(
    opener: Arc<RealOptimisticTransactionOpener<S>>,
    phase: DdlPhase<'_>,
    timeout: Duration,
    notifier: Option<&dyn SchemaVersionNotifier>,
    backfiller: &dyn IndexBackfiller,
    check_constraint_validator: &dyn CheckConstraintValidator,
    owner_id: &str,
) -> Result<DdlPhaseOutcome, ClusterDdlError> {
    (phase.check_owner)().map_err(ClusterDdlError::SchemaSync)?;
    let transaction = SessionTransaction::begin(
        Arc::clone(&opener),
        timeout,
        tidb_store_driver_txn::session_commit_protocol::bootstrap_commit_protocol(),
    )?;
    let start_ts = transaction.start_ts();
    let row_ids = backfiller.system_row_ids();
    let plan = {
        let mut snapshot = SnapshotMetaSnapshot::new(
            transaction
                .snapshot()
                .map_err(|error| ClusterDdlError::Backfill(error.to_string()))?,
        );
        match plan_persisted_ddl_job_step(
            &mut snapshot,
            phase.ddl_job_id,
            start_ts,
            phase.schema_state.clone(),
            &|| refresh_ddl_error_count_limit(&opener, timeout, phase.ddl_job_id),
            Some(row_ids.as_ref()),
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
            }) => {
                transaction
                    .rollback()
                    .map_err(ClusterDdlError::NotCommitted)?;
                return Ok(DdlPhaseOutcome::SchemaSync {
                    version,
                    mdl_info,
                    publish_version,
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
    let mut write = step.write;
    let mut buffer = MutationBuffer::new();
    if write.check_constraint_validation.is_some() {
        let stage_action = || {
            transaction
                .snapshot()
                .map_err(|error| ClusterDdlError::Backfill(error.to_string()))
                .and_then(|snapshot| {
                    let slot = Arc::new(Mutex::new(SwappableSnapshot::new()));
                    slot.lock()
                        .map_err(|_| {
                            ClusterDdlError::Backfill("snapshot slot poisoned".to_owned())
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
            let checkpoint = transaction
                .snapshot()
                .map_err(|error| DdlPlanError::Encode(error.to_string()))
                .and_then(|snapshot| {
                    plan_persisted_ddl_job_failure(
                        &mut SnapshotMetaSnapshot::new(snapshot),
                        ddl_job_id,
                        start_ts,
                        failure,
                        &|| refresh_ddl_error_count_limit(&opener, timeout, ddl_job_id),
                        Some(row_ids.as_ref()),
                    )
                });
            let checkpoint = match checkpoint {
                Ok(checkpoint) => checkpoint,
                Err(error) => {
                    let _ = transaction.rollback();
                    return Err(error.into());
                }
            };
            // Rust's planned metadata and staged rows are not applied until
            // commit. Discard both, as Go resets failed action mutations, but
            // keep this transaction's snapshot and conflict protection.
            buffer = MutationBuffer::new();
            write = checkpoint.write;
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
        if let Err(error) = mdl_info.append_mutations(
            write.ddl_job_id,
            write.schema_version,
            owner_id,
            &mut write.mutations,
        ) {
            let _ = transaction.rollback();
            return Err(error.into());
        }
    }
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

fn clean_mdl_info_with_retry<S: StoreClient>(
    opener: Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    mdl_info: &MdlInfoUpdate,
    ddl_job_id: i64,
    schema_version: i64,
    owner_id: &str,
) -> Result<(), ClusterDdlError> {
    let mut attempt: u32 = 0;
    loop {
        let transaction = SessionTransaction::begin(
            Arc::clone(&opener),
            timeout,
            tidb_store_driver_txn::session_commit_protocol::bootstrap_commit_protocol(),
        )?;
        let mut mutations = Vec::new();
        let cleanup = {
            let mut snapshot = SnapshotMetaSnapshot::new(
                transaction
                    .snapshot()
                    .map_err(|error| ClusterDdlError::Backfill(error.to_string()))?,
            );
            mdl_info.append_delete_mutations(&mut snapshot, ddl_job_id, owner_id, &mut mutations)
        };
        if let Err(error) = cleanup {
            let _ = transaction.rollback();
            return Err(error.into());
        }
        if mutations.is_empty() {
            return transaction
                .rollback()
                .map_err(ClusterDdlError::NotCommitted);
        }
        let buffer = MutationBuffer::new();
        match transaction.commit_with(&buffer, mutations) {
            Ok(_) => return Ok(()),
            Err(error) => {
                let cause = classify_session_ddl_commit_error(schema_version, error);
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
                .generate_with_stack("write conflict at schema version key"),
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
