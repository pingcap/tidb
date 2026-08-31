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
//! watch side are documented in [`crate::catalog_watch`]'s module doc.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tidb_executor::cluster_storage::{ClusterSnapshot, MutationBuffer, SwappableSnapshot};
use tidb_model::Job;
use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::{
    OptimisticCommitOutcome, OptimisticCoordinatorError, RealOptimisticTransactionOpener,
    TransactionCause, MAX_OPTIMISTIC_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES,
};
use tidb_txnkv::transaction::{StorePdCapability, StoreWriteClient, StoreWriteLoader};

use crate::cluster_catalog::{load_cluster_catalog, MetaSnapshot};
use crate::cluster_ddl::{
    lower_ddl_with_context, plan_check_constraint_job_rollingback, plan_ddl,
    plan_persisted_check_constraint_job_step, prepare_check_constraint_job_submission,
    CheckConstraintValidation, DdlAdmissionError, DdlPlan, DdlPlanError, DdlStatement, DdlWrite,
    ExchangePartitionValidation, IndexBackfill, MdlInfoUpdate,
};
use crate::cluster_table_storage::{LockKeysOutcome, SessionTransaction};
use crate::ddl_job_submit::{finish_insert_attempt, plan_insert_attempt};
use crate::pessimistic_lock_error::{transaction_cause_to_sql_error, LockSqlError};
use crate::real_tikv_catalog::{SnapshotMetaSnapshot, TransactionMetaSnapshot};
use crate::table_info_build::default_ddl_statement_context;

struct LabelDeliveryReceipt {
    endpoint: String,
    original: Vec<tidb_executor::ddl_label::Rule>,
    rule_ids: [String; 2],
}

struct PlacementDeliveryReceipt {
    endpoint: String,
    rollback: Vec<tidb_placement::Bundle>,
}

fn deliver_placement_bundles(
    endpoint: &str,
    bundles: &[tidb_placement::Bundle],
    rollback_bundles: &[tidb_placement::Bundle],
    timeout: Duration,
) -> Result<Option<PlacementDeliveryReceipt>, ClusterDdlError> {
    if bundles.is_empty() {
        return Ok(None);
    }
    crate::placement_delivery::put_rule_bundles(endpoint, bundles, timeout).map_err(|error| {
        ClusterDdlError::NotCommitted(format!("failed to notify PD the placement rules: {error}"))
    })?;
    let rollback = if rollback_bundles.is_empty() {
        // Non-exchange DDL keeps the existing retry behavior: IDs allocated
        // by an optimistic attempt are withdrawn if the catalog transaction
        // loses its race. Exchange supplies exact pre-change bundles because
        // its groups already existed under the swapped physical IDs.
        bundles
            .iter()
            .map(|bundle| tidb_placement::Bundle {
                id: bundle.id.clone(),
                ..tidb_placement::Bundle::default()
            })
            .collect()
    } else {
        rollback_bundles.to_vec()
    };
    Ok(Some(PlacementDeliveryReceipt {
        endpoint: endpoint.to_owned(),
        rollback,
    }))
}

fn deliver_exchange_label_rules(
    endpoint: &str,
    write: &DdlWrite,
    timeout: Duration,
) -> Result<Option<LabelDeliveryReceipt>, ClusterDdlError> {
    let Some(swap) = &write.exchange_partition_label_swap else {
        return Ok(None);
    };
    let codec = tidb_executor::ddl_label::CodecV1;
    let rule_ids = swap.rule_ids(&codec);
    let original =
        crate::label_delivery::get_label_rules(endpoint, &rule_ids, timeout).map_err(|error| {
            ClusterDdlError::NotCommitted(format!("failed to get PD the label rules: {error}"))
        })?;
    let patch = swap.patch(&codec, &original);
    crate::label_delivery::patch_label_rules(endpoint, &patch, timeout).map_err(|error| {
        ClusterDdlError::NotCommitted(format!("failed to notify PD the label rules: {error}"))
    })?;
    Ok(Some(LabelDeliveryReceipt {
        endpoint: endpoint.to_owned(),
        original,
        rule_ids,
    }))
}

fn restore_exchange_label_rules(
    receipt: &LabelDeliveryReceipt,
    timeout: Duration,
) -> Result<(), crate::label_delivery::LabelDeliveryError> {
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
    let patch = tidb_executor::ddl_label::new_rule_patch(receipt.original.clone(), delete_rules);
    crate::label_delivery::patch_label_rules(&receipt.endpoint, &patch, timeout)
}

fn restore_placement(
    receipt: &PlacementDeliveryReceipt,
    timeout: Duration,
) -> Result<(), crate::placement_delivery::PlacementDeliveryError> {
    crate::placement_delivery::put_rule_bundles(&receipt.endpoint, &receipt.rollback, timeout)
}

fn compensate_external_delivery(
    mut cause: ClusterDdlError,
    placement_receipt: Option<&PlacementDeliveryReceipt>,
    label_receipt: Option<&LabelDeliveryReceipt>,
    timeout: Duration,
) -> ClusterDdlError {
    if let Some(receipt) = label_receipt {
        if let Err(error) = restore_exchange_label_rules(receipt, timeout) {
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
/// [`crate::real_tikv_dml::prepare_text_write`]: the server never parses SQL or
/// names catalog types itself, so the dependency direction stays
/// `tidb-server -> tidb-exec -> tidb-txnkv`.
pub fn prepare_cluster_ddl(
    sql: &str,
    default_schema: &str,
) -> Result<Option<DdlStatement>, DdlAdmissionError> {
    let context = default_ddl_statement_context();
    prepare_cluster_ddl_with_context(sql, default_schema, &context)
}

/// [`prepare_cluster_ddl`] under the statement's actual SQL mode and time zone.
///
/// Parsing and default-value admission deliberately share one context: scanner
/// mode, temporal SQL-mode bits, and session-zone normalization cannot drift
/// between the two halves of one `CREATE TABLE`.
pub fn prepare_cluster_ddl_with_context(
    sql: &str,
    default_schema: &str,
    context: &tidb_executor::StmtContext,
) -> Result<Option<DdlStatement>, DdlAdmissionError> {
    let Ok(statement) = tidb_parser::parse_with_sql_mode(sql, context.sql_mode()) else {
        return Ok(None);
    };
    lower_ddl_with_context(&statement, default_schema, context)
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
    /// The change needs an index backfill and this path has no backfiller.
    ///
    /// Publishing the meta half alone would create an index that exists and is
    /// EMPTY: every query the planner routes through it silently loses the rows
    /// whose entries were never written. The refusal is what keeps that
    /// unreachable on a path that cannot walk the table.
    BackfillUnavailable,
    /// `EXCHANGE PARTITION ... WITH VALIDATION` rejected a row or failed
    /// while reading/evaluating the validation scan.
    ExchangeValidation(LockSqlError),
    /// The change needs exchange validation and this path cannot read rows.
    ExchangeValidationUnavailable,
    /// Enabling an enforced CHECK rejected an existing row or failed while
    /// reading/evaluating the validation scan.
    CheckConstraintValidation(LockSqlError),
    /// The change needs CHECK validation and this path cannot read rows.
    CheckConstraintValidationUnavailable,
    /// A persisted DDL statement was sent to the non-scheduler execution path.
    PersistedJobRequired,
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
            Self::BackfillUnavailable => write!(
                formatter,
                "this path cannot perform an index change: it would publish an index \
                 that exists but holds no entry for any row the table already has"
            ),
            Self::ExchangeValidation(error) => formatter.write_str(&error.message),
            Self::ExchangeValidationUnavailable => write!(
                formatter,
                "this path cannot validate EXCHANGE PARTITION rows before publishing the ID swap"
            ),
            Self::CheckConstraintValidation(error) => formatter.write_str(&error.message),
            Self::CheckConstraintValidationUnavailable => write!(
                formatter,
                "this path cannot validate existing rows before enabling a CHECK constraint"
            ),
            Self::PersistedJobRequired => {
                formatter.write_str("this DDL must be submitted to the persisted DDL job queue")
            }
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
            | Self::BackfillUnavailable
            | Self::ExchangeValidation(_)
            | Self::ExchangeValidationUnavailable
            | Self::CheckConstraintValidation(_)
            | Self::CheckConstraintValidationUnavailable
            | Self::PersistedJobRequired
            | Self::SchemaSync(_) => None,
        }
    }
}

impl From<DdlPlanError> for ClusterDdlError {
    fn from(error: DdlPlanError) -> Self {
        Self::Plan(error)
    }
}

impl From<OptimisticCoordinatorError> for ClusterDdlError {
    fn from(error: OptimisticCoordinatorError) -> Self {
        Self::Transaction(error)
    }
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
        warning: Option<String>,
    },
    /// `IF [NOT] EXISTS` was already satisfied, so nothing was written.
    AlreadySatisfied {
        /// What was already true.
        detail: String,
        /// The warning the statement raises even though it changed nothing.
        warning: Option<String>,
    },
}

/// Announces a committed schema version so peers reload without waiting for
/// their lease tick.
///
/// A trait rather than a concrete etcd client because failing to announce is
/// not a DDL failure, and the caller decides how loud that is. Go's
/// `pkg/ddl/job_worker.go` logs `"update latest schema version failed"` at
/// Info and carries on when the PUT fails (it only propagates the error when
/// MDL is enabled, which this tier has no equivalent of): the version is
/// already durable in TiKV, and every node's `lease/2` reload still finds it.
pub trait SchemaVersionNotifier {
    /// Publishes `version`. The error is for logging, never for the client.
    fn notify(&self, version: i64) -> Result<(), String>;
}

impl SchemaVersionNotifier for tidb_pd_client::EtcdClient {
    fn notify(&self, version: i64) -> Result<(), String> {
        self.put_global_schema_version(version)
            .map_err(|error| error.to_string())
    }
}

/// Reads the catalog, plans one change against that snapshot, and publishes it.
///
/// The planning read and the mutation share one transaction and therefore one
/// `start_ts`, which is what lets the write set be derived from observed values
/// (`NextGlobalID + 1`, `SchemaVersionKey + 1`) instead of guessed ones.
///
/// `notifier` is the etcd leg. `None` keeps the tick-only behaviour, which is
/// what a node started without a reachable etcd falls back to.
pub fn commit_cluster_ddl<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    statement: &DdlStatement,
    timeout: Duration,
    notifier: Option<&dyn SchemaVersionNotifier>,
) -> Result<ClusterDdlReport, ClusterDdlError> {
    // Go `kv.RunInNewTxn(..., retryable=true, ...)` (`pkg/kv/txn.go`), which
    // is how every DDL meta write runs there (`pkg/ddl/ddl.go`): a write
    // conflict rolls the attempt back, backs off with full jitter
    // (`kv.BackOff`), and re-runs with a FRESH transaction -- up to
    // `kv.MaxRetryCnt` (100) attempts. A fresh attempt re-reads the catalog
    // and RE-PLANS, so the version it publishes describes the catalog that
    // now exists; that is what makes the retry safe where re-committing the
    // stale plan would not be. Two sessions creating `sbtest1`/`sbtest2`
    // concurrently (sysbench's parallel `prepare`) is the canonical shape:
    // Go serializes them through the DDL job queue and both succeed, so a
    // client must never see the conflict. `Undetermined` is NOT retried --
    // Go's `IsTxnRetryableError` is false there, and re-running a change
    // that may have committed could apply it twice.
    let mut attempt: u32 = 0;
    loop {
        match commit_cluster_ddl_once(opener, statement, timeout, notifier) {
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

fn commit_cluster_ddl_once<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    statement: &DdlStatement,
    timeout: Duration,
    notifier: Option<&dyn SchemaVersionNotifier>,
) -> Result<ClusterDdlReport, ClusterDdlError> {
    let call = UnaryCallContext::with_timeout(timeout);
    // How many mutations a change needs is only known after the catalog has
    // been read — a DROP DATABASE deletes one key per stored table — so the
    // transaction opens at this path's own ceiling and the commit checks the
    // real mutation set against it. A database with more tables than that fits
    // is refused at commit, loudly, rather than dropped in pieces.
    let mut transaction =
        opener.begin(MAX_OPTIMISTIC_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES)?;
    let start_ts = transaction.start_ts();
    let plan = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        plan_ddl(&mut snapshot, statement, start_ts)?
    };
    let write = match plan {
        DdlPlan::AlreadySatisfied { detail, warning } => {
            transaction.finish_without_writes()?;
            return Ok(ClusterDdlReport::AlreadySatisfied { detail, warning });
        }
        DdlPlan::Write(write) => write,
    };
    // This path publishes meta keys only. A change that also owes index
    // entries has to go through `commit_cluster_ddl_with_backfill`, which
    // reads the table's rows on the SAME transaction; committing the meta half
    // here would leave an index that answers queries with nothing.
    if !write.backfill.is_empty() {
        transaction.finish_without_writes()?;
        return Err(ClusterDdlError::BackfillUnavailable);
    }
    if write.exchange_partition_validation.is_some() {
        transaction.finish_without_writes()?;
        return Err(ClusterDdlError::ExchangeValidationUnavailable);
    }
    if write.check_constraint_validation.is_some() {
        transaction.finish_without_writes()?;
        return Err(ClusterDdlError::CheckConstraintValidationUnavailable);
    }
    // Go sends the placement bundles INSIDE the DDL job, before the schema
    // version is published, and fails the job when PD refuses
    // (`PutRuleBundlesWithDefaultRetry`). Delivering before the commit is the
    // transactional analogue: if PD will not take the rules, the catalog must
    // not claim them. The reverse order would publish a table whose rows live
    // somewhere other than where it says they do.
    //
    // An embedded store answers `None` for the endpoint and has no PD to tell,
    // so delivery is skipped rather than attempted against an address that
    // does not exist.
    let placement_receipt = if let Some(endpoint) = opener.pd().http_endpoint() {
        match deliver_placement_bundles(
            &endpoint,
            &write.placement_bundles,
            &write.placement_rollback_bundles,
            timeout,
        ) {
            Ok(receipt) => receipt,
            Err(error) => {
                transaction.finish_without_writes()?;
                return Err(error);
            }
        }
    } else {
        None
    };
    let label_receipt = if let Some(endpoint) = opener.pd().http_endpoint() {
        match deliver_exchange_label_rules(&endpoint, &write, timeout) {
            Ok(receipt) => receipt,
            Err(error) => {
                transaction.finish_without_writes()?;
                return Err(compensate_external_delivery(
                    error,
                    placement_receipt.as_ref(),
                    None,
                    timeout,
                ));
            }
        }
    } else {
        None
    };
    let planned_version = write.schema_version;
    // The bundles above are keyed by ids THIS attempt allocated. In Go the ids
    // a bundle carries are already durable when the job worker delivers it --
    // job submission committed them under `lockGlobalIDKey`
    // (`pkg/ddl/jobsubmit/submit.go`) -- so a retried job re-delivers the SAME
    // groups. Here a rolled-back attempt's ids die with it, and the retry
    // re-plans with fresh ones; the delivered bundles must go back too, or PD
    // keeps rules for ids the catalog never published and some later object
    // inherits them.
    let outcome = match transaction.commit(write.mutations, &call) {
        Ok(outcome) => outcome,
        Err(error) => {
            return Err(compensate_external_delivery(
                ClusterDdlError::Transaction(error),
                placement_receipt.as_ref(),
                label_receipt.as_ref(),
                timeout,
            ));
        }
    };
    match outcome {
        OptimisticCommitOutcome::Committed(_) => {
            notify_schema_version(notifier, planned_version);
            Ok(ClusterDdlReport::Applied {
                schema_version: planned_version,
                created_id: write.created_id,
                warning: write.warning,
            })
        }
        OptimisticCommitOutcome::RolledBack(rolled_back) => Err(compensate_external_delivery(
            classify(planned_version, &rolled_back.cause),
            placement_receipt.as_ref(),
            label_receipt.as_ref(),
            timeout,
        )),
        OptimisticCommitOutcome::CleanupFailed(cleanup_failed) => {
            Err(compensate_external_delivery(
                classify(planned_version, &cleanup_failed.cause),
                placement_receipt.as_ref(),
                label_receipt.as_ref(),
                timeout,
            ))
        }
        // An undetermined commit may have PUBLISHED the catalog that claims
        // these bundles; withdrawing them here could strip a live table's
        // placement, so they stay.
        OptimisticCommitOutcome::Undetermined(undetermined) => Err(ClusterDdlError::Undetermined(
            format!("{:?}", undetermined.cause),
        )),
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
/// [`ClusterTableStorage`]: tidb_executor::cluster_storage::ClusterTableStorage
pub trait IndexBackfiller {
    /// Reads the table's rows through `snapshot` and stages every entry the
    /// change adds or removes into `buffer`.
    ///
    /// The caller publishes the buffer in the same transaction the snapshot
    /// reads at, so an implementation must not commit anything itself.
    fn stage(
        &self,
        plan: &IndexBackfill,
        snapshot: Arc<Mutex<dyn ClusterSnapshot>>,
        buffer: &MutationBuffer,
    ) -> Result<(), String>;
}

/// Executes Go's `checkExchangePartitionRecordValidation` from the DDL
/// transaction's own row snapshot.
pub trait ExchangePartitionValidator {
    /// Proves every standalone-table row routes to the named partition.
    fn validate(
        &self,
        plan: &ExchangePartitionValidation,
        snapshot: Arc<Mutex<dyn ClusterSnapshot>>,
        buffer: &MutationBuffer,
    ) -> Result<(), LockSqlError>;
}

/// Executes Go's existing-row CHECK validation from the DDL transaction's
/// own row snapshot before the candidate metadata is published.
pub trait CheckConstraintValidator {
    /// Proves every stored row satisfies the candidate enforced constraint.
    fn validate(
        &self,
        plan: &CheckConstraintValidation,
        snapshot: Arc<Mutex<dyn ClusterSnapshot>>,
        buffer: &MutationBuffer,
    ) -> Result<(), LockSqlError>;
}

/// Owner-side synchronization between committed CHECK schema phases.
///
/// The implementation reloads the owner's own catalog and then waits for the
/// existing per-job acknowledgements from every registered TiDB node. There
/// is intentionally no lease-delay fallback here: a phase may advance only
/// after the nodes that can serve writes have loaded its writable metadata.
pub trait CheckConstraintSchemaSync {
    /// Stable owner id stored in `mysql.tidb_mdl_info.owner_id`.
    fn owner_id(&self) -> &str;

    /// Waits until every registered node has acknowledged `version` for
    /// `ddl_job_id`.
    fn wait_version_synced(&self, ddl_job_id: i64, version: i64) -> Result<(), String>;

    /// Removes the per-job acknowledgement keys after the final phase.
    fn clean_job_versions(&self, ddl_job_id: i64) -> Result<(), String>;
}

#[derive(Clone, Copy)]
enum DdlPhase<'statement> {
    Initial(&'statement DdlStatement),
    PersistedCheckConstraint { ddl_job_id: i64 },
}

struct CommittedDdlPhase {
    report: ClusterDdlReport,
    ddl_job_id: i64,
    schema_version: i64,
    persisted_job_terminal: bool,
    mdl_info: Option<MdlInfoUpdate>,
}

enum DdlPhaseOutcome {
    AlreadySatisfied(ClusterDdlReport),
    Committed(CommittedDdlPhase),
}

/// Publishes one catalog change together with the index entries it owes.
///
/// One `SessionTransaction` serves all three halves at one `start_ts`: the
/// catalog read that plans the change, the row walk that builds the entries,
/// and the prewrite that publishes both. That is the whole reason this is not
/// [`commit_cluster_ddl`] with an extra step — an index built from rows read at
/// a different timestamp than it is written at can point at a row that does not
/// exist yet, or miss one that does.
///
/// **What this node does NOT do, stated rather than hidden.** Go runs
/// `ActionAddIndex` through `delete only` -> `write only` -> `reorg` -> `public`
/// precisely so that a concurrent `INSERT` on another node maintains the
/// half-built index while the reorg scans. There is no job queue and no schema
/// state machine here; the index becomes public at the one commit, and a row
/// committed by another writer between this transaction's `start_ts` and its
/// commit is indexed by neither half. This is the same single-writer assumption
/// [`crate::cluster_ddl`]'s module doc already states for `DROP TABLE`, widened
/// from "no concurrent DDL" to "no concurrent WRITE to the table being
/// indexed", and it is the one thing to fix before this tier serves a second
/// writer.
pub fn commit_cluster_ddl_with_backfill<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
>(
    opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
    statement: &DdlStatement,
    timeout: Duration,
    notifier: Option<&dyn SchemaVersionNotifier>,
    backfiller: &dyn IndexBackfiller,
    exchange_validator: &dyn ExchangePartitionValidator,
    check_constraint_validator: &dyn CheckConstraintValidator,
    schema_sync: &dyn CheckConstraintSchemaSync,
) -> Result<ClusterDdlReport, ClusterDdlError> {
    if matches!(
        statement,
        DdlStatement::AddCheckConstraint { .. }
            | DdlStatement::DropCheckConstraint { .. }
            | DdlStatement::AlterCheckConstraint { .. }
    ) {
        return Err(ClusterDdlError::PersistedJobRequired);
    }

    match commit_cluster_ddl_phase_with_retry(
        opener,
        DdlPhase::Initial(statement),
        timeout,
        notifier,
        backfiller,
        exchange_validator,
        check_constraint_validator,
        schema_sync.owner_id(),
    )? {
        DdlPhaseOutcome::AlreadySatisfied(report) => Ok(report),
        DdlPhaseOutcome::Committed(committed) => Ok(committed.report),
    }
}

/// Runs one already-submitted persisted CHECK DDL job until it reaches history.
///
/// This is the worker half of Go's scheduler contract. It accepts a job ID,
/// reloads the active row before every step, and therefore resumes jobs
/// submitted by another server or abandoned by a former owner.
#[allow(clippy::too_many_arguments)]
pub fn run_persisted_check_constraint_job_to_completion<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
>(
    opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
    ddl_job_id: i64,
    timeout: Duration,
    notifier: Option<&dyn SchemaVersionNotifier>,
    backfiller: &dyn IndexBackfiller,
    exchange_validator: &dyn ExchangePartitionValidator,
    check_constraint_validator: &dyn CheckConstraintValidator,
    schema_sync: &dyn CheckConstraintSchemaSync,
) -> Result<ClusterDdlReport, ClusterDdlError> {
    loop {
        let outcome = match commit_cluster_ddl_phase_with_retry(
            Arc::clone(&opener),
            DdlPhase::PersistedCheckConstraint { ddl_job_id },
            timeout,
            notifier,
            backfiller,
            exchange_validator,
            check_constraint_validator,
            schema_sync.owner_id(),
        ) {
            Err(ClusterDdlError::CheckConstraintValidation(validation_error))
                if validation_error.code
                    == tidb_error::tidb::errcode::ErrCheckConstraintViolated =>
            {
                mark_check_constraint_job_rollingback_with_retry(
                    Arc::clone(&opener),
                    ddl_job_id,
                    &validation_error,
                    timeout,
                )?;
                let rollback = commit_cluster_ddl_phase_with_retry(
                    Arc::clone(&opener),
                    DdlPhase::PersistedCheckConstraint { ddl_job_id },
                    timeout,
                    notifier,
                    backfiller,
                    exchange_validator,
                    check_constraint_validator,
                    schema_sync.owner_id(),
                )?;
                let DdlPhaseOutcome::Committed(rollback) = rollback else {
                    unreachable!("a persisted CHECK rollback is a worker write")
                };
                synchronize_committed_check_phase(
                    Arc::clone(&opener),
                    timeout,
                    &rollback,
                    schema_sync,
                )?;
                if let Err(error) = schema_sync.clean_job_versions(ddl_job_id) {
                    eprintln!(
                        "{{\"level\":\"warning\",\"event\":\"ddl_job_versions_cleanup_failed\",\"job_id\":{ddl_job_id},\"error\":{}}}",
                        serde_json::to_string(&error)
                            .unwrap_or_else(|_| "\"unprintable\"".to_owned())
                    );
                }
                return Err(ClusterDdlError::CheckConstraintValidation(validation_error));
            }
            outcome => outcome?,
        };
        let DdlPhaseOutcome::Committed(committed) = outcome else {
            unreachable!("a queued CHECK action is always a worker write")
        };
        synchronize_committed_check_phase(Arc::clone(&opener), timeout, &committed, schema_sync)?;
        if committed.persisted_job_terminal {
            if let Err(error) = schema_sync.clean_job_versions(ddl_job_id) {
                eprintln!(
                    "{{\"level\":\"warning\",\"event\":\"ddl_job_versions_cleanup_failed\",\"job_id\":{ddl_job_id},\"error\":{}}}",
                    serde_json::to_string(&error)
                        .unwrap_or_else(|_| "\"unprintable\"".to_owned())
                );
            }
            return Ok(committed.report);
        }
    }
}

/// Pinned Go `GenGIDAndInsertJobsWithRetry`: lock the global-ID key, assign
/// every action-owned ID, run the caller's registration callback, encode all
/// rows, and commit the allocation and inserts atomically.
pub fn gen_global_ids_and_insert_jobs_with_retry<C, L, P, F, Cleanup>(
    opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
    specs: &mut [crate::ddl_job_submit::JobSpec],
    timeout: Duration,
    mut before_insert_with_assigned_ids: F,
) -> Result<(), ClusterDdlError>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
    F: FnMut(&[crate::ddl_job_submit::JobSpec]) -> Option<Cleanup>,
    Cleanup: FnOnce(),
{
    let mut attempt = 0_u32;
    loop {
        let transaction = SessionTransaction::begin_pessimistic(
            Arc::clone(&opener),
            timeout,
            crate::session_commit_protocol::session_commit_protocol(),
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
            LockKeysOutcome::Locked { for_update_ts, .. }
            | LockKeysOutcome::RetryStatement { for_update_ts, .. } => for_update_ts,
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
/// owner (possibly another server) executes it.
pub fn submit_check_constraint_job_with_retry<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
>(
    opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
    statement: &DdlStatement,
    timeout: Duration,
    upgrading: bool,
    min_job_id: i64,
) -> Result<i64, ClusterDdlError> {
    let preparation = SessionTransaction::begin(
        Arc::clone(&opener),
        timeout,
        crate::session_commit_protocol::session_commit_protocol(),
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

    gen_global_ids_and_insert_jobs_with_retry(
        opener,
        std::slice::from_mut(&mut spec),
        timeout,
        |_| Option::<fn()>::None,
    )?;
    Ok(spec.job.id)
}

/// Loads the persisted active DDL queue from a fresh cluster snapshot.
///
/// The returned order is the job table's primary-key order, matching Go's
/// scheduler query `ORDER BY job_id`. The read transaction is always rolled
/// back because queue inspection publishes no state.
pub fn load_active_persisted_ddl_jobs<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
>(
    opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
    timeout: Duration,
    min_job_id: i64,
) -> Result<Vec<Job>, ClusterDdlError> {
    let transaction = SessionTransaction::begin(
        opener,
        timeout,
        crate::session_commit_protocol::session_commit_protocol(),
    )?;
    let jobs = {
        let mut snapshot = SnapshotMetaSnapshot::new(
            transaction
                .snapshot()
                .map_err(|error| ClusterDdlError::Backfill(error.to_string()))?,
        );
        let catalog = load_cluster_catalog(&mut snapshot).map_err(DdlPlanError::Catalog)?;
        let table = crate::ddl_job_table::DdlJobTable::locate(&catalog)
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
        table
            .load_from(&mut snapshot, min_job_id)
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?
            .into_iter()
            .map(|active| active.job)
            .collect()
    };
    transaction
        .rollback()
        .map_err(ClusterDdlError::NotCommitted)?;
    Ok(jobs)
}

/// Reads pinned Go `systable.Manager.GetMinJobID` in a fresh read-only
/// transaction for `MinJobIDRefresher`.
pub fn load_min_persisted_ddl_job_id<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
>(
    opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
    timeout: Duration,
    previous_min_job_id: i64,
) -> Result<i64, ClusterDdlError> {
    let transaction = SessionTransaction::begin(
        opener,
        timeout,
        crate::session_commit_protocol::session_commit_protocol(),
    )?;
    let minimum = {
        let mut snapshot = SnapshotMetaSnapshot::new(
            transaction
                .snapshot()
                .map_err(|error| ClusterDdlError::Backfill(error.to_string()))?,
        );
        let catalog = load_cluster_catalog(&mut snapshot).map_err(DdlPlanError::Catalog)?;
        crate::ddl_systable::SystemTableManager::new(&catalog)
            .get_min_job_id(&mut snapshot, previous_min_job_id)
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?
    };
    transaction
        .rollback()
        .map_err(ClusterDdlError::NotCommitted)?;
    Ok(minimum)
}

/// Reads one terminal DDL job from Go's authoritative meta history.
pub fn load_history_persisted_ddl_job<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
>(
    opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
    ddl_job_id: i64,
    timeout: Duration,
) -> Result<Option<Job>, ClusterDdlError> {
    let transaction = SessionTransaction::begin(
        opener,
        timeout,
        crate::session_commit_protocol::session_commit_protocol(),
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

fn mark_check_constraint_job_rollingback_with_retry<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
>(
    opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
    ddl_job_id: i64,
    validation_error: &LockSqlError,
    timeout: Duration,
) -> Result<(), ClusterDdlError> {
    let mut attempt = 0_u32;
    loop {
        let transaction = SessionTransaction::begin(
            Arc::clone(&opener),
            timeout,
            crate::session_commit_protocol::session_commit_protocol(),
        )?;
        let mutations = {
            let mut snapshot = SnapshotMetaSnapshot::new(
                transaction
                    .snapshot()
                    .map_err(|error| ClusterDdlError::Backfill(error.to_string()))?,
            );
            plan_check_constraint_job_rollingback(
                &mut snapshot,
                ddl_job_id,
                validation_error.code,
                &validation_error.message,
            )
        };
        let mutations = match mutations {
            Ok(mutations) => mutations,
            Err(error) => {
                let _ = transaction.rollback();
                return Err(error.into());
            }
        };
        let buffer = MutationBuffer::new();
        match transaction.commit_with(&buffer, mutations) {
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

fn synchronize_committed_check_phase<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
>(
    opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
    timeout: Duration,
    committed: &CommittedDdlPhase,
    schema_sync: &dyn CheckConstraintSchemaSync,
) -> Result<(), ClusterDdlError> {
    let Some(mdl_info) = &committed.mdl_info else {
        return Ok(());
    };
    schema_sync
        .wait_version_synced(committed.ddl_job_id, committed.schema_version)
        .map_err(ClusterDdlError::SchemaSync)?;
    if let Err(error) = clean_mdl_info_with_retry(
        opener,
        timeout,
        mdl_info,
        committed.ddl_job_id,
        committed.schema_version,
        schema_sync.owner_id(),
    ) {
        eprintln!(
            "{{\"level\":\"warning\",\"event\":\"ddl_mdl_info_cleanup_failed\",\"job_id\":{},\"error\":{}}}",
            committed.ddl_job_id,
            serde_json::to_string(&error.to_string())
                .unwrap_or_else(|_| "\"unprintable\"".to_owned())
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn commit_cluster_ddl_phase_with_retry<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
>(
    opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
    phase: DdlPhase<'_>,
    timeout: Duration,
    notifier: Option<&dyn SchemaVersionNotifier>,
    backfiller: &dyn IndexBackfiller,
    exchange_validator: &dyn ExchangePartitionValidator,
    check_constraint_validator: &dyn CheckConstraintValidator,
    owner_id: &str,
) -> Result<DdlPhaseOutcome, ClusterDdlError> {
    let mut attempt: u32 = 0;
    loop {
        match commit_cluster_ddl_with_backfill_once(
            Arc::clone(&opener),
            phase,
            timeout,
            notifier,
            backfiller,
            exchange_validator,
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

fn commit_cluster_ddl_with_backfill_once<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
>(
    opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
    phase: DdlPhase<'_>,
    timeout: Duration,
    notifier: Option<&dyn SchemaVersionNotifier>,
    backfiller: &dyn IndexBackfiller,
    exchange_validator: &dyn ExchangePartitionValidator,
    check_constraint_validator: &dyn CheckConstraintValidator,
    owner_id: &str,
) -> Result<DdlPhaseOutcome, ClusterDdlError> {
    let transaction = SessionTransaction::begin(
        Arc::clone(&opener),
        timeout,
        crate::session_commit_protocol::session_commit_protocol(),
    )?;
    let start_ts = transaction.start_ts();
    let plan = {
        let mut snapshot = SnapshotMetaSnapshot::new(
            transaction
                .snapshot()
                .map_err(|error| ClusterDdlError::Backfill(error.to_string()))?,
        );
        match phase {
            DdlPhase::Initial(statement) => {
                plan_ddl(&mut snapshot, statement, start_ts).map(|plan| (plan, false))
            }
            DdlPhase::PersistedCheckConstraint { ddl_job_id } => {
                plan_persisted_check_constraint_job_step(&mut snapshot, ddl_job_id, start_ts)
                    .map(|step| (DdlPlan::Write(Box::new(step.write)), step.terminal))
            }
        }
    };
    let (plan, persisted_job_terminal) = match plan {
        Ok(plan) => plan,
        Err(error) => {
            let _ = transaction.rollback();
            return Err(error.into());
        }
    };
    let write = match plan {
        DdlPlan::AlreadySatisfied { detail, warning } => {
            transaction
                .rollback()
                .map_err(ClusterDdlError::NotCommitted)?;
            return Ok(DdlPhaseOutcome::AlreadySatisfied(
                ClusterDdlReport::AlreadySatisfied { detail, warning },
            ));
        }
        DdlPlan::Write(write) => *write,
    };
    let buffer = MutationBuffer::new();
    if !write.backfill.is_empty()
        || write.exchange_partition_validation.is_some()
        || write.check_constraint_validation.is_some()
    {
        let staged = transaction
            .snapshot()
            .map_err(|error| ClusterDdlError::Backfill(error.to_string()))
            .and_then(|snapshot| {
                let slot = Arc::new(Mutex::new(SwappableSnapshot::new()));
                slot.lock()
                    .map_err(|_| ClusterDdlError::Backfill("snapshot slot poisoned".to_owned()))?
                    .bind(snapshot);
                let handle: Arc<Mutex<dyn ClusterSnapshot>> = slot;
                for backfill in &write.backfill {
                    backfiller
                        .stage(backfill, Arc::clone(&handle), &buffer)
                        .map_err(ClusterDdlError::Backfill)?;
                }
                if let Some(validation) = &write.exchange_partition_validation {
                    exchange_validator
                        .validate(validation, Arc::clone(&handle), &buffer)
                        .map_err(ClusterDdlError::ExchangeValidation)?;
                }
                if let Some(validation) = &write.check_constraint_validation {
                    check_constraint_validator
                        .validate(validation, Arc::clone(&handle), &buffer)
                        .map_err(ClusterDdlError::CheckConstraintValidation)?;
                }
                Ok(())
            });
        if let Err(error) = staged {
            // Nothing has been published: the entries only ever existed in this
            // process's buffer, so a failed backfill leaves the cluster exactly
            // as it was, index and rows both.
            let _ = transaction.rollback();
            return Err(error);
        }
    }
    let placement_receipt = if let Some(endpoint) = opener.pd().http_endpoint() {
        match deliver_placement_bundles(
            &endpoint,
            &write.placement_bundles,
            &write.placement_rollback_bundles,
            timeout,
        ) {
            Ok(receipt) => receipt,
            Err(error) => {
                let _ = transaction.rollback();
                return Err(error);
            }
        }
    } else {
        None
    };
    let label_receipt = if let Some(endpoint) = opener.pd().http_endpoint() {
        match deliver_exchange_label_rules(&endpoint, &write, timeout) {
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
        }
    } else {
        None
    };
    let mut write = write;
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
    let planned_version = write.schema_version;
    match transaction.commit_with(&buffer, write.mutations) {
        Ok(_) => {
            notify_schema_version(notifier, planned_version);
            Ok(DdlPhaseOutcome::Committed(CommittedDdlPhase {
                report: ClusterDdlReport::Applied {
                    schema_version: planned_version,
                    created_id: write.created_id,
                    warning: write.warning,
                },
                ddl_job_id: write.ddl_job_id,
                schema_version: planned_version,
                persisted_job_terminal,
                mdl_info: write.mdl_info_update,
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

fn clean_mdl_info_with_retry<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
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
            crate::session_commit_protocol::session_commit_protocol(),
        )?;
        let mut mutations = Vec::new();
        mdl_info.append_delete_mutations(ddl_job_id, schema_version, owner_id, &mut mutations)?;
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
        Ok(()) => eprintln!(
            "{{\"event\":\"schema_version_notified\",\"schema_version\":{version}}}"
        ),
        Err(error) => eprintln!(
            "{{\"event\":\"schema_version_notify_failed\",\"level\":\"warning\",\"schema_version\":{version},\"error\":{}}}",
            serde_json::to_string(&error).unwrap_or_else(|_| "\"unprintable\"".to_owned())
        ),
    }
}

/// Names a write conflict for what it is on this path: a concurrent DDL.
///
/// Every catalog change writes `SchemaVersionKey` from a value it read, so
/// TiKV's own conflict detection is this node's mutual exclusion. Any other
/// cause keeps its coordinator-level meaning.
fn classify(planned_version: i64, cause: &TransactionCause) -> ClusterDdlError {
    match cause {
        TransactionCause::WriteConflict { detail } => ClusterDdlError::ConcurrentSchemaChange {
            planned_version,
            detail: detail.clone(),
        },
        other => ClusterDdlError::Commit(transaction_cause_to_sql_error(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_txnkv::transaction::{OptimisticTransactionReceipt, UndeterminedTransaction};

    #[test]
    fn a_write_conflict_on_a_catalog_change_is_named_a_concurrent_ddl() {
        // `SchemaVersionKey` is in every catalog change's write set, so a TiKV
        // write conflict here can mean only one thing.
        let error = classify(
            61,
            &TransactionCause::WriteConflict {
                detail: "optimistic write conflict: conflict_ts 450".to_owned(),
            },
        );
        assert!(matches!(
            error,
            ClusterDdlError::ConcurrentSchemaChange {
                planned_version: 61,
                ..
            }
        ));
        assert!(error
            .to_string()
            .contains("refuses to interleave: optimistic write conflict"));
    }

    #[test]
    fn a_session_ddl_commit_keeps_an_undetermined_verdict_distinct() {
        let outcome = OptimisticCommitOutcome::Undetermined(UndeterminedTransaction {
            receipt: OptimisticTransactionReceipt::new(1, 2, b"key".to_vec(), 1),
            cause: TransactionCause::Transport {
                detail: "commit response lost".to_owned(),
            },
        });
        let error = crate::pessimistic_lock_error::commit_outcome_to_sql_error(&outcome)
            .expect_err("an unknown commit verdict must not answer success");
        assert!(matches!(
            classify_session_ddl_commit_error(61, error),
            ClusterDdlError::Undetermined(_)
        ));
    }

    #[test]
    fn session_ddl_commit_classification_preserves_determinate_errors() {
        let conflict = LockSqlError {
            code: WRITE_CONFLICT_CODE,
            state: *b"HY000",
            message: "write conflict at schema version key".to_owned(),
        };
        assert!(matches!(
            classify_session_ddl_commit_error(61, conflict),
            ClusterDdlError::ConcurrentSchemaChange {
                planned_version: 61,
                ..
            }
        ));

        let ordinary = LockSqlError {
            code: 1105,
            state: *b"HY000",
            message: "prewrite transport failed definitively".to_owned(),
        };
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

    #[test]
    fn any_other_failure_keeps_its_own_meaning_rather_than_blaming_a_concurrent_ddl() {
        let error = classify(
            61,
            &TransactionCause::Region {
                detail: "not leader".to_owned(),
            },
        );
        assert!(matches!(
            &error,
            ClusterDdlError::Commit(driver_error)
                if driver_error.code == 1105 && driver_error.message.contains("not leader")
        ));
        assert!(!error.to_string().contains("another DDL"));
    }

    #[test]
    fn backoff_exhaustion_keeps_its_driver_error_identity() {
        let error = classify(
            61,
            &TransactionCause::BackoffExhausted {
                kind: tidb_txnkv::region::RegionBackoffKind::TikvRpc,
                detail: "tikvRPC backoffer exhausted".to_owned(),
            },
        );
        assert!(matches!(
            error,
            ClusterDdlError::Commit(error)
                if error.code == tidb_error::tidb::errcode::ErrTiKVServerTimeout
        ));
    }
}
