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
use std::time::Duration;

use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::{
    OptimisticCommitOutcome, OptimisticCoordinatorError, RealOptimisticTransactionOpener,
    TransactionCause, MAX_OPTIMISTIC_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES,
};

use crate::cluster_ddl::{
    lower_ddl, plan_ddl, DdlAdmissionError, DdlPlan, DdlPlanError, DdlStatement,
};
use crate::real_tikv_catalog::TransactionMetaSnapshot;

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
    let Ok(statement) = tidb_parser::parse(sql) else {
        return Ok(None);
    };
    lower_ddl(&statement, default_schema)
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
    /// Publication reached a terminal state that is not a commit, so the
    /// catalog change cannot be reported as done.
    NotCommitted(String),
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
            Self::NotCommitted(state) => {
                write!(formatter, "catalog change did not commit: {state}")
            }
        }
    }
}

impl std::error::Error for ClusterDdlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Plan(error) => Some(error),
            Self::Transaction(error) => Some(error),
            Self::ConcurrentSchemaChange { .. } | Self::NotCommitted(_) => None,
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
    },
    /// `IF [NOT] EXISTS` was already satisfied, so nothing was written.
    AlreadySatisfied {
        /// What was already true.
        detail: String,
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
pub fn commit_cluster_ddl(
    opener: &RealOptimisticTransactionOpener,
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
        DdlPlan::AlreadySatisfied { detail } => {
            transaction.finish_without_writes()?;
            return Ok(ClusterDdlReport::AlreadySatisfied { detail });
        }
        DdlPlan::Write(write) => write,
    };
    let planned_version = write.schema_version;
    match transaction.commit(write.mutations, &call)? {
        OptimisticCommitOutcome::Committed(_) => {
            notify_schema_version(notifier, planned_version);
            Ok(ClusterDdlReport::Applied {
                schema_version: planned_version,
                created_id: write.created_id,
            })
        }
        OptimisticCommitOutcome::RolledBack(rolled_back) => {
            Err(classify(planned_version, &rolled_back.cause))
        }
        OptimisticCommitOutcome::CleanupFailed(cleanup_failed) => {
            Err(classify(planned_version, &cleanup_failed.cause))
        }
        other => Err(ClusterDdlError::NotCommitted(format!(
            "{:?}",
            other.state()
        ))),
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
        other => ClusterDdlError::NotCommitted(format!("{other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(matches!(error, ClusterDdlError::NotCommitted(_)));
        assert!(!error.to_string().contains("another DDL"));
    }
}
