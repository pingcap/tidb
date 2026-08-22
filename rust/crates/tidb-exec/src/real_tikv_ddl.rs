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
use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::{
    OptimisticCommitOutcome, OptimisticCoordinatorError, RealOptimisticTransactionOpener,
    TransactionCause, MAX_OPTIMISTIC_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES,
};
use tidb_txnkv::transaction::{StorePdCapability, StoreWriteClient, StoreWriteLoader};

use crate::cluster_ddl::{
    lower_ddl_with_context, plan_ddl, DdlAdmissionError, DdlPlan, DdlPlanError, DdlStatement,
    IndexBackfill,
};
use crate::cluster_table_storage::SessionTransaction;
use crate::pessimistic_lock_error::{transaction_cause_to_sql_error, LockSqlError};
use crate::real_tikv_catalog::{SnapshotMetaSnapshot, TransactionMetaSnapshot};
use crate::table_info_build::default_ddl_statement_context;

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
    /// transaction into a definite write conflict. Failing here is the point —
    /// retrying silently could publish a schema version whose diff describes a
    /// catalog that no longer exists.
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
            | Self::BackfillUnavailable => None,
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
    if write.backfill.is_some() {
        transaction.finish_without_writes()?;
        return Err(ClusterDdlError::BackfillUnavailable);
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
    if !write.placement_bundles.is_empty() {
        if let Some(endpoint) = opener.pd().http_endpoint() {
            if let Err(error) = crate::placement_delivery::put_rule_bundles(
                &endpoint,
                &write.placement_bundles,
                timeout,
            ) {
                transaction.finish_without_writes()?;
                return Err(ClusterDdlError::NotCommitted(error.to_string()));
            }
        }
    }
    let planned_version = write.schema_version;
    match transaction.commit(write.mutations, &call)? {
        OptimisticCommitOutcome::Committed(_) => {
            notify_schema_version(notifier, planned_version);
            Ok(ClusterDdlReport::Applied {
                schema_version: planned_version,
                created_id: write.created_id,
                warning: write.warning,
            })
        }
        OptimisticCommitOutcome::RolledBack(rolled_back) => {
            Err(classify(planned_version, &rolled_back.cause))
        }
        OptimisticCommitOutcome::CleanupFailed(cleanup_failed) => {
            Err(classify(planned_version, &cleanup_failed.cause))
        }
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
) -> Result<ClusterDdlReport, ClusterDdlError> {
    let transaction = SessionTransaction::begin(opener, timeout)?;
    let start_ts = transaction.start_ts();
    let plan = {
        let mut snapshot = SnapshotMetaSnapshot::new(
            transaction
                .snapshot()
                .map_err(|error| ClusterDdlError::Backfill(error.to_string()))?,
        );
        plan_ddl(&mut snapshot, statement, start_ts)
    };
    let plan = match plan {
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
            return Ok(ClusterDdlReport::AlreadySatisfied { detail, warning });
        }
        DdlPlan::Write(write) => write,
    };
    let buffer = MutationBuffer::new();
    if let Some(backfill) = &write.backfill {
        let staged = transaction
            .snapshot()
            .map_err(|error| ClusterDdlError::Backfill(error.to_string()))
            .and_then(|snapshot| {
                let slot = Arc::new(Mutex::new(SwappableSnapshot::new()));
                slot.lock()
                    .map_err(|_| ClusterDdlError::Backfill("snapshot slot poisoned".to_owned()))?
                    .bind(snapshot);
                let handle: Arc<Mutex<dyn ClusterSnapshot>> = slot;
                backfiller
                    .stage(backfill, handle, &buffer)
                    .map_err(ClusterDdlError::Backfill)
            });
        if let Err(error) = staged {
            // Nothing has been published: the entries only ever existed in this
            // process's buffer, so a failed backfill leaves the cluster exactly
            // as it was, index and rows both.
            let _ = transaction.rollback();
            return Err(error);
        }
    }
    let planned_version = write.schema_version;
    match transaction.commit_with(&buffer, write.mutations) {
        Ok(_) => {
            notify_schema_version(notifier, planned_version);
            Ok(ClusterDdlReport::Applied {
                schema_version: planned_version,
                created_id: write.created_id,
                warning: write.warning,
            })
        }
        Err(error) => Err(classify_session_ddl_commit_error(planned_version, error)),
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
