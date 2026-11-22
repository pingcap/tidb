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

//! Go `session.bootstrap`: publish DDL through pooled sessions, then seed DML.
//!
//! Shared by cluster startup and `--store unistore` boot. Go commits the
//! DDL jobs before `doDMLWorks`, so a bootstrap interrupted between those
//! phases resumes through the statements' IF NOT EXISTS checks.

use std::time::Duration;

use crate::mysql_bootstrap::{get_ddl_table_version, BootstrapEnvironment, BootstrapLayout};
use tidb_ddl::pre_split::BootstrapSplits;
use tidb_datatype::current_time::utc_now_timestamp;
use tidb_store_driver_error::pessimistic_lock_error::commit_outcome_to_sql_error;
use tidb_infoschema_issyncer::real_tikv_catalog::TransactionMetaSnapshot;
use tidb_ddl::real_tikv_ddl::{notify_schema_version, SchemaVersionNotifier};
use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::OptimisticCommitOutcome;
use tidb_store_driver_txn::transaction::{RealOptimisticTransactionOpener, StoreClient};
use tidb_timeutil::infer_system_tz;

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

/// Go `session.bootstrap`: execute DDL jobs, then commit bootstrap DML.
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
    publish(opener, timeout, None)
}

/// Go `bootstrap(s)`'s `doDDLWorks` and `doDMLWorks` on the bootstrap
/// domain: each DDL statement runs through `domain`'s DDL, whose owner
/// executes the job, over a store whose `InitDDLTables` (and next-gen
/// `bootstrapSchemas`) already ran
/// ([`crate::mysql_bootstrap::init_ddl_tables`]).
pub fn publish_bootstrap_on<S: StoreClient>(
    domain: &tidb_domain_core::domain::Domain,
    opener: &RealOptimisticTransactionOpener<S>,
    timeout: Duration,
) -> Result<PublishedBootstrap, String> {
    publish(opener, timeout, Some(domain))
}

/// Go `bootstrapSessionImpl`'s `bootstrapSchemas` (next-gen) and
/// `InitDDLTables` under this build's layout, before the bootstrap domain
/// starts. Returns the id batches whose regions they owe a split.
pub fn init_system_tables<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
    timeout: Duration,
) -> Result<BootstrapSplits, String> {
    let layout = layout();
    let mut global_batches = Vec::new();
    if layout == BootstrapLayout::NextGen {
        global_batches.extend(crate::mysql_bootstrap::bootstrap_schemas(opener, timeout)?);
    }
    global_batches.extend(crate::mysql_bootstrap::init_ddl_tables(opener, timeout, layout)?);
    Ok(BootstrapSplits {
        global_batches,
        table_ids: Vec::new(),
    })
}

fn publish<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
    timeout: Duration,
    domain: Option<&tidb_domain_core::domain::Domain>,
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
    environment.ddl_table_version = get_ddl_table_version(
        &mut TransactionMetaSnapshot::new(&mut transaction, timeout),
    ).map_err(|error| error.to_string())?;
    let _ = transaction.rollback(&UnaryCallContext::with_timeout(timeout));
    let (write, bootstrap_pool) = crate::mysql_bootstrap::jobs::execute_on(opener, timeout, &environment, layout(), domain)
        .map_err(|error| error.to_string())?;
    let transaction = opener.begin().map_err(|error| error.to_string())?;
    let schema_version = write.schema_version;
    let splits = write.splits.clone();
    // The DML commit starts after all DDL jobs have completed.
    let call = UnaryCallContext::with_timeout(timeout);
    let resource_group_update = write.resource_group_update.clone();
    let outcome = transaction
        .commit(write.mutations, &call)
        .map_err(|error| error.to_string())?;
    // Go `bootstrap(s)`'s `doDMLWorks(s)` on the bootstrap domain the DDL ran
    // on; a caller that brought its own domain runs it on that domain's
    // session itself.
    if let Some(pool) = bootstrap_pool {
        commit_outcome_to_sql_error(&outcome).map_err(|error| error.message)?;
        let bootstrap_domain = pool.store_domain().ok_or_else(|| "the bootstrap pool has no domain".to_owned())?;
        crate::domain::cluster_session_node::do_dml_works(
            &bootstrap_domain,
            &crate::bootstrap::TxnUpgradeStore::new(opener, timeout),
            &tidb_ddl_serverstate::MemSyncer::new(),
        )?;
    }
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
    tidb_logutil::bg_logger().info(
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
    use super::{notify_committed_bootstrap, BootstrapLayout, SchemaVersionNotifier};
    use std::cell::Cell;
    use tidb_txnkv::transaction::{CommittedTransaction, OptimisticCommitOutcome, OptimisticTransactionReceipt, RolledBackTransaction, TransactionCause, UndeterminedTransaction};

    /// Go `TestBootstrap` (`pkg/session/bootstrap_test.go`) and the
    /// `TestCreateTables` refresh-failure callback: bootstrap runs the same
    /// delivery loop as user DDL, survives a transient GetJobByID failure,
    /// and leaves the root account and global variables queryable by SQL.
    #[test]
    fn test_bootstrap() {
        // Go `cmd/tidb-server.setGlobalVars` normalizes this legacy config
        // before BootstrapSession; tests/integrationtest/config.toml uses it.
        let restore_config = tidb_config::config_tree::config::restore_func();
        tidb_config::config_tree::config::update_global(|config| config.lease = "0".to_owned());
        use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
        use std::time::Duration;
        use tidb_ddl_build::session_pool::DdlSessionPool;
        let (_pd, opener) = tidb_ddl::embedded_store::in_process_write_stack().unwrap();
        let refreshes = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&refreshes);
        let _refresh = tidb_failpoint::enable_call_for_test(
            "github.com/pingcap/tidb/pkg/ddl/mockGetJobByIDFail", move |arg| {
                if observed.fetch_add(1, Ordering::SeqCst) == 0 {
                    *arg.downcast_mut::<Option<tidb_ddl_systable::SystemTableManagerError>>().unwrap() =
                        Some(tidb_ddl_systable::SystemTableManagerError::Sql("mock get job by ID failed".to_owned()));
                }
            },
        );
        let (outcome, _) = super::publish_bootstrap(&opener, Duration::from_secs(20)).unwrap();
        tidb_store_driver_error::pessimistic_lock_error::commit_outcome_to_sql_error(&outcome).unwrap();
        assert!(refreshes.load(Ordering::SeqCst) >= 2, "bootstrap must use deliveryJob and retry GetJobByID");
        let pool = crate::ddl_session_pool::NodeDdlSessionPool::for_store(
            opener, Duration::from_secs(20), Arc::new(tidb_table::table_from_meta::LocalTableAutoIds::default()),
            true, Duration::from_millis(500),
        ).unwrap();
        let se = pool.open_sql_session().unwrap();
        let root = tidb_ddl_build::session_pool::execute(&se, "select Host, User from mysql.user", "test", &[]).unwrap();
        assert_eq!(root.len(), 1);
        assert_eq!(root[0][0].go_bytes(), b"%");
        assert_eq!(root[0][1].go_bytes(), b"root");
        assert!(tidb_ddl_build::session_pool::execute(&se, "select variable_name from mysql.global_variables", "test", &[]).unwrap().len() > 1);
        assert!(tidb_ddl_build::session_pool::execute(&se, "select job_id from mysql.tidb_ddl_job", "test", &[]).unwrap().is_empty());
        restore_config();
    }

    /// Go `TestBootstrapWithError`: DDL completed but the session died
    /// before doDMLWorks; the next session resumes and seeds the tables.
    #[test]
    fn test_bootstrap_with_error() {
        use std::{sync::Arc, time::Duration};
        use tidb_ddl_build::session_pool::DdlSessionPool;
        let (_pd, opener) = tidb_ddl::embedded_store::in_process_write_stack().unwrap();
        let timeout = Duration::from_secs(20);
        let environment = crate::mysql_bootstrap::BootstrapEnvironment {
            system_tz: tidb_timeutil::infer_system_tz(), new_collation_enabled: true,
            cluster_id: opener.cluster_id(), current_timestamp: tidb_datatype::current_time::utc_now_timestamp(),
            ddl_table_version: 0,
        };
        // Stop after the DDL jobs, as Go's test stops after doDDLWorks.
        let _ddl_only = crate::mysql_bootstrap::jobs::execute_on(
            &opener, timeout, &environment, super::layout(), None,
        ).unwrap();
        let (outcome, _) = super::publish_bootstrap(&opener, timeout).unwrap();
        tidb_store_driver_error::pessimistic_lock_error::commit_outcome_to_sql_error(&outcome).unwrap();
        let pool = crate::ddl_session_pool::NodeDdlSessionPool::for_store(
            opener, timeout, Arc::new(tidb_table::table_from_meta::LocalTableAutoIds::default()),
            true, Duration::from_millis(500),
        ).unwrap();
        let se = pool.open_sql_session().unwrap();
        let rows = tidb_ddl_build::session_pool::execute(&se, "select Host, User from mysql.user", "test", &[]).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].go_bytes(), b"%");
        assert_eq!(rows[0][1].go_bytes(), b"root");
    }

    /// The bootstrapped `mysql` schema and seed rows of a store, as text.
    fn bootstrapped_rows(opener: tidb_ddl::embedded_store::InProcessOpener) -> Vec<String> {
        use std::{sync::Arc, time::Duration};
        use tidb_ddl_build::session_pool::DdlSessionPool;
        let pool = crate::ddl_session_pool::NodeDdlSessionPool::for_store(
            opener, Duration::from_secs(20), Arc::new(tidb_table::table_from_meta::LocalTableAutoIds::default()),
            true, Duration::from_millis(500),
        ).unwrap();
        let se = pool.open_sql_session().unwrap();
        let mut rendered = Vec::new();
        for sql in [
            "select table_name, tidb_table_id from information_schema.tables where table_schema in ('mysql', 'sys') order by table_name",
            "select Host, User from mysql.user",
            "select variable_name, variable_value from mysql.global_variables order by variable_name",
            "select variable_name from mysql.tidb where variable_name not in ('bootstrapped_time') order by variable_name",
        ] {
            for row in tidb_ddl_build::session_pool::execute(&se, sql, "test", &[]).unwrap() {
                rendered.push(
                    row.iter()
                        .map(|datum| match datum {
                            tidb_datatype::Datum::String(value) => {
                                String::from_utf8_lossy(value.bytes()).into_owned()
                            }
                            other => format!("{other:?}"),
                        })
                        .collect::<Vec<_>>()
                        .join(","),
                );
            }
        }
        rendered
    }

    /// Go `bootstrapSessionImpl` then `bootstrap(s)` on the bootstrap
    /// domain: `InitDDLTables` commits on its own first (a second node's run
    /// creates nothing), then the DDL jobs run through the given domain's
    /// DDL, and `doDMLWorks`' statements run on its session. The store ends
    /// with the same `mysql` schema and rows as the one-step publish.
    #[test]
    fn bootstrap_on_domain_after_init_ddl_tables_matches_one_step_publish() {
        bootstrap_on_domain_matches_one_step_publish(BootstrapLayout::Classic, 0);
    }

    /// [`bootstrap_on_domain_after_init_ddl_tables_matches_one_step_publish`]
    /// under the next-gen layout, whose `bootstrapSchemas` also runs first
    /// and cuts one batch per `versionedBootstrapSchemas` group.
    #[test]
    fn bootstrap_on_domain_after_bootstrap_schemas_matches_one_step_publish() {
        bootstrap_on_domain_matches_one_step_publish(BootstrapLayout::NextGen, 2);
    }

    fn bootstrap_on_domain_matches_one_step_publish(layout: BootstrapLayout, schema_batches: usize) {
        use std::time::Duration;
        super::override_layout_on_this_thread(Some(layout));
        let timeout = Duration::from_secs(20);
        let (_pd_one_step, one_step) = tidb_ddl::embedded_store::in_process_write_stack().unwrap();
        let (outcome, _) = super::publish_bootstrap(&one_step, timeout).unwrap();
        tidb_store_driver_error::pessimistic_lock_error::commit_outcome_to_sql_error(&outcome).unwrap();

        let (_pd_on_domain, on_domain) = tidb_ddl::embedded_store::in_process_write_stack().unwrap();
        let splits = super::init_system_tables(&on_domain, timeout).unwrap();
        assert_eq!(
            splits.global_batches.len(),
            schema_batches + tidb_metadef::DDL_TABLE_VERSION_TABLES.len()
        );
        assert!(splits.table_ids.is_empty());
        assert!(
            super::init_system_tables(&on_domain, timeout).unwrap().global_batches.is_empty(),
            "a second InitDDLTables finds every DDL table version"
        );
        let pool = crate::ddl_session_pool::NodeDdlSessionPool::for_bootstrap(on_domain.clone(), timeout).unwrap();
        let domain = pool.store_domain().unwrap();
        let published = super::publish_bootstrap_on(&domain, &on_domain, timeout).unwrap();
        tidb_store_driver_error::pessimistic_lock_error::commit_outcome_to_sql_error(&published.outcome).unwrap();
        assert!(published.splits.global_batches.is_empty(), "InitDDLTables already cut its tables");
        // Only the classic layout's `mysql.*` tables are `CREATE TABLE` jobs.
        assert_eq!(published.splits.table_ids.is_empty(), layout == BootstrapLayout::NextGen);
        // Go `bootstrap(s)`'s `doDMLWorks(s)`, on the bootstrap domain's
        // session as a server runs it.
        crate::domain::cluster_session_node::do_dml_works(
            &domain,
            &crate::bootstrap::TxnUpgradeStore::new(&on_domain, timeout),
            &tidb_ddl_serverstate::MemSyncer::new(),
        )
        .unwrap();
        drop(domain);
        drop(pool);

        let expected = bootstrapped_rows(one_step);
        assert!(expected.iter().any(|row| row.contains("tidb_ddl_job")), "{expected:?}");
        assert_eq!(bootstrapped_rows(on_domain), expected);
        super::override_layout_on_this_thread(None);
    }

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
                terminal: tikv_client::retry::BO_REGION_MISS.terminal_error,
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
