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

//! Publishing the `mysql` schema bootstrap on one real transaction.
//!
//! [`tidb_exec::mysql_bootstrap::bootstrap_mysql_schema`]'s committing caller,
//! shared by the `mysql-bootstrap` binary (against a live cluster) and the
//! `--store unistore` boot (against the embedded store, which starts empty on
//! every run and therefore bootstraps on every run -- exactly Go's
//! `session.BootstrapSession` over mockstore). A keyspace that already
//! carries any bootstrap object is refused by the plan, and nothing is
//! written.

use std::time::Duration;

use tidb_exec::mysql_bootstrap::{
    bootstrap_mysql_schema, read_ddl_table_version, utc_now_timestamp, BootstrapEnvironment,
};
use tidb_exec::pessimistic_lock_error::commit_outcome_to_sql_error;
use tidb_exec::real_tikv_catalog::TransactionMetaSnapshot;
use tidb_exec::real_tikv_ddl::{notify_schema_version, SchemaVersionNotifier};
use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::{
    OptimisticCommitOutcome, RealOptimisticTransactionOpener, StorePdCapability, StoreWriteClient,
    StoreWriteLoader,
};
use tidb_util::timeutil::infer_system_tz;

/// Plans and commits the bootstrap on one transaction.
///
/// A first read-only pass exists only to size the write budget the coordinator
/// insists on knowing before it spends a timestamp; the plan that is published
/// is re-made on the writing transaction itself, so the freshness check and the
/// commit share one `start_ts`.
pub fn publish_bootstrap<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    timeout: Duration,
) -> Result<(OptimisticCommitOutcome, i64), String> {
    let call = UnaryCallContext::with_timeout(timeout);
    let mut sizing = opener
        .begin_read_only()
        .map_err(|error| error.to_string())?;
    let mut environment = BootstrapEnvironment {
        system_tz: infer_system_tz(),
        // Go's `new_collations_enabled_on_first_bootstrap` default, which this
        // row then freezes for the life of the cluster.
        new_collation_enabled: true,
        cluster_id: opener.cluster_id(),
        current_timestamp: utc_now_timestamp(),
        ddl_table_version: 0,
    };
    let planned = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut sizing, timeout);
        environment.ddl_table_version =
            read_ddl_table_version(&mut snapshot).map_err(|error| error.to_string())?;
        // `u64::MAX` is not a placeholder timestamp: the plan's size depends on
        // how wide `update_ts` prints in each table's JSON, so sizing at the
        // widest possible timestamp is what makes this budget a ceiling for
        // whatever timestamp PD hands the writing transaction.
        bootstrap_mysql_schema(&mut snapshot, u64::MAX, &environment)
            .map_err(|error| error.to_string())?
    };
    sizing
        .finish_without_writes()
        .map_err(|error| error.to_string())?;
    let bytes: usize = planned
        .mutations
        .iter()
        .map(|mutation| mutation.key().len() + mutation.value().len())
        .sum();

    let mut transaction = opener
        .begin(planned.mutations.len(), bytes)
        .map_err(|error| error.to_string())?;
    let start_ts = transaction.start_ts();
    let write = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        bootstrap_mysql_schema(&mut snapshot, start_ts, &environment)
            .map_err(|error| error.to_string())?
    };
    eprintln!(
        "{{\"event\":\"bootstrap_publishing\",\"tables\":{},\"schema_version\":{},\"start_ts\":{start_ts}}}",
        write.created_tables.len(),
        write.schema_version
    );
    let schema_version = write.schema_version;
    let outcome = transaction
        .commit(write.mutations, &call)
        .map_err(|error| error.to_string())?;
    Ok((outcome, schema_version))
}

/// Refuses every bootstrap side effect unless the commit really landed, then
/// announces the schema version. Failing to announce is a warning: the
/// version is already durable and every node's tick still finds it.
pub fn notify_committed_bootstrap(
    outcome: &OptimisticCommitOutcome,
    schema_version: i64,
    notifier: Option<&dyn SchemaVersionNotifier>,
) -> Result<i64, String> {
    commit_outcome_to_sql_error(outcome).map_err(|error| error.message)?;
    notify_schema_version(notifier, schema_version);
    Ok(schema_version)
}

#[cfg(test)]
mod tests {
    use super::{notify_committed_bootstrap, SchemaVersionNotifier};
    use std::cell::Cell;
    use tidb_txnkv::region::RegionBackoffKind;
    use tidb_txnkv::transaction::{
        CommittedTransaction, OptimisticCommitOutcome, OptimisticTransactionReceipt,
        RolledBackTransaction, TransactionCause, UndeterminedTransaction,
    };

    struct RecordingNotifier(Cell<usize>);

    impl SchemaVersionNotifier for RecordingNotifier {
        fn notify(&self, _: i64) -> Result<(), String> {
            self.0.set(self.0.get() + 1);
            Ok(())
        }
    }

    fn receipt() -> OptimisticTransactionReceipt {
        OptimisticTransactionReceipt::new(1, 2, b"bootstrap".to_vec(), 1)
    }

    #[test]
    fn bootstrap_side_effects_require_a_committed_outcome() {
        let notifier = RecordingNotifier(Cell::new(0));
        let rolled_back = OptimisticCommitOutcome::RolledBack(RolledBackTransaction {
            receipt: receipt(),
            cause: TransactionCause::BackoffExhausted {
                kind: RegionBackoffKind::RegionMiss,
                detail: "regionMiss backoffer exhausted".to_owned(),
            },
        });
        let error = notify_committed_bootstrap(&rolled_back, 61, Some(&notifier))
            .expect_err("a rolled-back bootstrap must not be announced");
        assert!(error.contains("Region is unavailable"), "{error}");
        assert_eq!(notifier.0.get(), 0);

        let undetermined = OptimisticCommitOutcome::Undetermined(UndeterminedTransaction {
            receipt: receipt(),
            cause: TransactionCause::Transport {
                detail: "commit response lost".to_owned(),
            },
        });
        let error = notify_committed_bootstrap(&undetermined, 61, Some(&notifier))
            .expect_err("an undetermined bootstrap must not be announced");
        assert_eq!(error, "execution result undetermined");
        assert_eq!(notifier.0.get(), 0);

        let committed = OptimisticCommitOutcome::Committed(CommittedTransaction {
            receipt: receipt(),
            secondary_failures: Vec::new(),
        });
        assert_eq!(
            notify_committed_bootstrap(&committed, 61, Some(&notifier))
                .expect("only a confirmed commit is announced"),
            61
        );
        assert_eq!(notifier.0.get(), 1);
    }
}
