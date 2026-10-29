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
//! [`crate::mysql_bootstrap::bootstrap_mysql_schema`]'s committing caller,
//! shared by cluster startup and `--store unistore` boot (against the
//! embedded store, which starts empty on
//! every run and therefore bootstraps on every run -- exactly Go's
//! `session.BootstrapSession` over mockstore). A keyspace that already
//! carries any bootstrap object is refused by the plan, and nothing is
//! written.

use std::time::Duration;

use crate::mysql_bootstrap::{plan_mysql_bootstrap_with_layout, read_ddl_table_version, BootstrapEnvironment, BootstrapLayout};
use tidb_ddl::pre_split::BootstrapSplits;
use tidb_datatype::current_time::utc_now_timestamp;
use tidb_store_driver::pessimistic_lock_error::commit_outcome_to_sql_error;
use tidb_infoschema_issyncer::real_tikv_catalog::TransactionMetaSnapshot;
use tidb_ddl::real_tikv_ddl::{notify_schema_version, SchemaVersionNotifier};
use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::{
    OptimisticCommitOutcome, RealOptimisticTransactionOpener, StoreClient,
};
use tidb_util::timeutil::infer_system_tz;

thread_local! {
    static LAYOUT_OVERRIDE: std::cell::Cell<Option<BootstrapLayout>> =
        const { std::cell::Cell::new(None) };
}

/// Makes the bootstraps this thread publishes use `layout` instead of the
/// build's kernel layout, so one test binary can exercise both kernels'
/// layouts. Production never calls it.
#[doc(hidden)]
pub fn override_layout_on_this_thread(layout: Option<BootstrapLayout>) {
    LAYOUT_OVERRIDE.with(|cell| cell.set(layout));
}

/// The layout this build's kernel bootstraps.
fn layout() -> BootstrapLayout {
    LAYOUT_OVERRIDE
        .with(std::cell::Cell::get)
        .or_else(layout_from_test_environment)
        .unwrap_or_else(BootstrapLayout::of_kernel)
}

/// Debug builds only: `TIDB_RS_TEST_BOOTSTRAP_LAYOUT=nextgen|classic` runs a
/// whole test binary over the other kernel's layout.
fn layout_from_test_environment() -> Option<BootstrapLayout> {
    if !cfg!(debug_assertions) {
        return None;
    }
    match std::env::var("TIDB_RS_TEST_BOOTSTRAP_LAYOUT").ok()?.as_str() {
        "nextgen" => Some(BootstrapLayout::NextGen),
        "classic" => Some(BootstrapLayout::Classic),
        _ => None,
    }
}

/// Plans and commits the bootstrap on one transaction.
///
/// Bootstrap reads and writes share one transaction timestamp.
pub fn publish_bootstrap<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
    timeout: Duration,
) -> Result<(OptimisticCommitOutcome, i64), String> {
    publish_bootstrap_with_splits(opener, timeout).map(|(outcome, version, _)| (outcome, version))
}

/// [`publish_bootstrap`], also naming the region splits the new tables owe
/// (`tidb_ddl::pre_split::split_bootstrap_tables`), whose ids are the ones
/// the bootstrap actually gave them.
pub fn publish_bootstrap_with_splits<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
    timeout: Duration,
) -> Result<(OptimisticCommitOutcome, i64, BootstrapSplits), String> {
    publish_bootstrap_with_effects(opener, timeout)
        .map(|published| (published.outcome, published.schema_version, published.splits))
}

/// What a published bootstrap left outside its own transaction: the region
/// splits its tables owe and the resource manager write it made. A
/// bootstrapped store image (Go testkit's) replays both over a store that
/// starts from the image's data.
pub struct PublishedBootstrap {
    /// The bootstrap transaction's commit.
    pub outcome: OptimisticCommitOutcome,
    /// The schema version the bootstrap published.
    pub schema_version: i64,
    /// See [`publish_bootstrap_with_splits`].
    pub splits: BootstrapSplits,
    /// The resource manager write made before the commit.
    pub resource_group_update: tidb_ddl::cluster_ddl::ResourceGroupUpdate,
}

/// [`publish_bootstrap_with_splits`], also returning the resource manager
/// write it made.
pub fn publish_bootstrap_with_effects<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
    timeout: Duration,
) -> Result<PublishedBootstrap, String> {
    let mut environment = BootstrapEnvironment {
        system_tz: infer_system_tz(),
        // Go's `new_collations_enabled_on_first_bootstrap` default, which this
        // row then freezes for the life of the cluster.
        new_collation_enabled: true,
        cluster_id: opener.cluster_id(),
        current_timestamp: utc_now_timestamp(),
        ddl_table_version: 0,
    };
    let mut transaction = opener.begin().map_err(|error| error.to_string())?;
    let start_ts = transaction.start_ts();
    let write = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        environment.ddl_table_version =
            read_ddl_table_version(&mut snapshot).map_err(|error| error.to_string())?;
        plan_mysql_bootstrap_with_layout(&mut snapshot, start_ts, &environment, layout())
            .map_err(|error| error.to_string())?
    };
    let schema_version = write.schema_version;
    let splits = write.splits.clone();
    // The commit's deadline starts once the plan is made: a classic plan runs
    // every bootstrap statement as a DDL job, which takes longer than one
    // round trip.
    let call = UnaryCallContext::with_timeout(timeout);
    // Go's `onAlterResourceGroup` calls `infosync.ModifyResourceGroup` inside
    // the job step, before the step's transaction commits, and a failure
    // cancels the job -- which `mustExecute` turns into a fatal bootstrap.
    if let Err(error) = write.resource_group_update.apply() {
        let _ = transaction.rollback(&call);
        return Err(format!("update resource group failed: {error}"));
    }
    let resource_group_update = write.resource_group_update.clone();
    let outcome = transaction
        .commit(write.mutations, &call)
        .map_err(|error| error.to_string())?;
    Ok(PublishedBootstrap {
        outcome,
        schema_version,
        splits,
        resource_group_update,
    })
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

/// Go `session.bootstrap`'s line once the owner finished the bootstrap.
pub fn log_bootstrap_successful(started: std::time::Instant) {
    tidb_util::logutil::bg_logger().info(
        "bootstrap successful",
        &[tidb_log::Field::new(
            "take time",
            tidb_log::Value::Duration(
                i64::try_from(started.elapsed().as_nanos()).unwrap_or(i64::MAX),
            ),
        )],
    );
}

#[cfg(test)]
mod tests {
    use super::{notify_committed_bootstrap, SchemaVersionNotifier};
    use std::cell::Cell;
    use tidb_txnkv::retry::RegionBackoffKind;
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
