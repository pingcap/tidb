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

//! The `--store unistore` run path: the same SQL node over the embedded store.
//!
//! Go boundary: `cmd/tidb-server` registers the unistore driver
//! (`session.RegisterStore("unistore", mockstore.EmbedUnistoreDriver{})`) and
//! everything above `kv.Storage` runs unchanged. This module is that
//! registration's Rust half: it builds the in-process capability triple
//! (client, region plane, TSO) from `tidb-unistore`, derives the SAME
//! ordinary session/catalog lifecycle used by TiKV, with the embedded store
//! supplying the same storage capabilities. Bootstrap and catalog reloads run
//! against that store; no PD or etcd connection is needed.

use std::sync::Arc;
use std::time::Duration;

use tidb_distsql::cop_paging::DirectUnaryRuntimeConfig;
use tidb_distsql::DirectUnaryQueryTransport;
use tidb_exec::real_tikv_read::RealTiKvSessionTransportFactory;
use tidb_txnkv::gc_state::TxnSafePointRefresher;
use tidb_txnkv::pd_capability::CapabilityTimestampSource;
use tidb_txnkv::region::RegionCache;
use tidb_txnkv::transaction::RealOptimisticTransactionOpener;
use tidb_txnkv::{SharedReadAuthority, SharedReadOpener};
use tidb_unistore::client::InProcessClient;
use tidb_unistore::kv_handler::KvHandler;
use tidb_unistore::mvcc_store::MvccStore;
use tidb_unistore::region_loader::{InProcessRegionLoader, IN_PROCESS_STORE_ID};
use tidb_unistore::tso::InProcessPd;

use crate::real_tikv_node::{configured_account_store, RunConfiguredNodeError};
use crate::sql_node::ConcurrentSqlNode;
use crate::SqlQueryError;

/// Go `main.go:498-529`: after the drain, the process exits with
/// `exitCodeForSignal(sig)`. A SIGINT-started shutdown exits 130 HERE,
/// after every teardown above has run; any other outcome flows back to the
/// binary's ordinary exit mapping.
fn finish_with_signal_code(
    result: Result<(), RunConfiguredNodeError>,
    last_signal: &crate::shutdown_signal::LastSignal,
) -> Result<(), RunConfiguredNodeError> {
    let code = crate::shutdown_signal::exit_code_for_recorded(last_signal);
    if result.is_ok() && code != 0 {
        std::process::exit(i32::from(code));
    }
    result
}

/// The per-statement RPC budget an in-process call gets. Nothing waits on a
/// network, so this bounds only local lock waits.
const IN_PROCESS_TIMEOUT: Duration = Duration::from_secs(20);

/// The transport an in-process session reads through: the SAME
/// `DirectUnaryQueryTransport` machinery as production, with the embedded
/// client and the whole-keyspace region plane underneath.
pub type InProcessReadTransport = DirectUnaryQueryTransport<InProcessClient, InProcessRegionLoader>;

/// The read-session factory over the embedded store, mirror of
/// `ProductionReadSessionFactory`: cloneable handles only, no lifecycle
/// ownership, no second worker per session.
pub struct InProcessReadSessionFactory {
    read_opener: SharedReadOpener<InProcessClient, InProcessRegionLoader>,
    lock_timestamp_source: CapabilityTimestampSource<InProcessPd>,
}

impl RealTiKvSessionTransportFactory for InProcessReadSessionFactory {
    type Transport = InProcessReadTransport;

    fn open_session_transport(&self) -> Result<Self::Transport, String> {
        // `from_read_authority` builds the BATCH-FIRST transport, which takes
        // "one BatchCommands attempt before the response-owned synchronous
        // retry loop". In process there is no BatchCommands transport to
        // attempt: `InProcessClient::begin` runs the coprocessor inline and
        // hands back an `ImmediatePending` already holding the answer, and
        // nothing drives `pending_batches` to collect it. Measured on this
        // tip -- the handler returns 8 bytes with no error, `try_complete` is
        // never called, and the attempt burns the whole `IN_PROCESS_TIMEOUT`
        // before reporting `query deadline exceeded`. Every query carrying an
        // expression or an aggregate died that way, `select count(*)`
        // included.
        //
        // So this node opens the session itself and takes the SYNCHRONOUS
        // transport, which is what `async_begin: None` selects. The batch
        // attempt is not merely unhelpful here; it is unanswerable.
        let runtime = self
            .read_opener
            .open_session()
            .map_err(|_| "region cache lifecycle".to_owned())?;
        DirectUnaryQueryTransport::with_shared_runtime(
            runtime,
            DirectUnaryRuntimeConfig {
                default_timeout: IN_PROCESS_TIMEOUT,
                ..DirectUnaryRuntimeConfig::default()
            },
            self.lock_timestamp_source.clone(),
        )
        .map_err(|error| error.to_string())
    }
}

/// The embedded write stack: one store, its region plane, its TSO, and the
/// generic transaction opener over all three. Every unistore surface --
/// bootstrap, reads and writes alike -- derives from this one build.
type InProcessOpener =
    RealOptimisticTransactionOpener<InProcessClient, InProcessRegionLoader, InProcessPd>;

#[cfg(test)]
mod transaction_buffer_tests {
    use super::*;
    use tidb_exec::cluster_table_storage::SessionTransaction;
    use tidb_executor::cluster_storage::MutationBuffer;
    use tidb_txnkv::{transaction::BufferMutation, AssertionOp, Key, UnaryCallContext};

    #[test]
    fn lightweight_pessimistic_statement_retries_obey_config() {
        if crate::isolate_process_globals() {
            return;
        }
        use tidb_config::config_tree::config::update_global;
        use tidb_exec::multi_statement_transaction::MultiStatementTransaction;
        use tidb_planner::read_only_scan::{ConfiguredColumn, ConfiguredTable, ReadLockWait};
        use tidb_planner::txn_mode::SessionTxnMode;

        let (_authority, _pd, opener) = in_process_write_stack().unwrap();
        let table = ConfiguredTable::new(
            "test",
            "t",
            42,
            [ConfiguredColumn::clustered_primary_key("id", 1)],
        );
        for limit in [0, 1] {
            update_global(|config| config.pessimistic_txn.max_retry_count = limit);
            let mut transaction = MultiStatementTransaction::begin(
                &opener,
                SessionTxnMode::Pessimistic,
                false,
                tidb_exec::session_commit_protocol::bootstrap_commit_protocol(),
                table.clone(),
                IN_PROCESS_TIMEOUT,
                IN_PROCESS_TIMEOUT,
            )
            .unwrap();
            // Publish after the locking reader's for_update_ts. The real
            // embedded TiKV must report a write conflict on its first lock.
            let mut writer = opener.begin().unwrap();
            writer
                .commit(
                    vec![BufferMutation::set(
                        tidb_tablecodec::table_key::encode_row_key_with_handle(
                            42,
                            &tidb_tablecodec::table_key::RecordHandle::Int(1),
                        ),
                        b"row",
                    )
                    .unwrap()],
                    &UnaryCallContext::with_timeout(IN_PROCESS_TIMEOUT),
                )
                .unwrap();
            let result = transaction.lock_handles(&[1], ReadLockWait::Blocking);
            if limit == 0 {
                let error = result.expect_err("the first conflict exhausts a zero budget");
                assert_eq!(error.sql_error().code, 1105);
                assert_eq!(
                    error.sql_error().message,
                    "pessimistic lock retry limit reached"
                );
                assert!(error.keeps_transaction_open());
            } else {
                result.unwrap();
            }
            transaction.rollback().unwrap();
        }
    }

    #[test]
    fn unbound_session_commit_preserves_deleted_insert_constraint() {
        let (_authority, _pd, opener) = in_process_write_stack().unwrap();
        let call = UnaryCallContext::with_timeout(IN_PROCESS_TIMEOUT);
        let key = Key::from_bytes(b"existing".to_vec());
        let mut seed = opener.begin().unwrap();
        seed.commit(
            vec![BufferMutation::set(key.as_bytes(), b"original").unwrap()],
            &call,
        )
        .unwrap();

        let transaction = SessionTransaction::begin(
            Arc::new(opener.clone()),
            IN_PROCESS_TIMEOUT,
            tidb_exec::session_commit_protocol::bootstrap_commit_protocol(),
        )
        .unwrap();
        let buffer = MutationBuffer::new();
        buffer
            .stage_owned_batch([
                (
                    key.clone(),
                    Some(b"replacement".to_vec()),
                    true,
                    AssertionOp::AssertUnknown,
                ),
                (key.clone(), None, false, AssertionOp::AssertNone),
            ])
            .unwrap();
        buffer.mark_presume_key_not_exists_with_hint(&key, "existing", "PRIMARY");
        let error = transaction
            .commit(&buffer)
            .expect_err("deleting a lazy insert must retain its native CheckNotExists mutation");
        assert_eq!(error.code, 1062, "{error:?}");
        assert_eq!(buffer.native_owner(), None);
        assert!(buffer.is_empty());
        let mut reader = opener.begin().unwrap();
        assert_eq!(
            reader.snapshot_get(key.as_bytes(), &call).unwrap().value,
            Some(b"original".to_vec())
        );
        reader.finish_without_writes().unwrap();
    }

    #[test]
    fn bound_session_commit_checks_tables_from_appended_mutations() {
        use tidb_txnkv::transaction::{SchemaLeaseChecker, SchemaLeaseError};
        #[derive(Default)]
        struct CheckedTables(std::sync::Mutex<Vec<Vec<i64>>>);
        impl SchemaLeaseChecker for CheckedTables {
            fn check_by_schema_ver(&self, _: u64, tables: &[i64]) -> Result<(), SchemaLeaseError> {
                self.0.lock().unwrap().push(tables.to_vec());
                Ok(())
            }
        }
        let (_authority, _pd, opener) = in_process_write_stack().unwrap();
        let mut transaction = SessionTransaction::begin(
            Arc::new(opener),
            IN_PROCESS_TIMEOUT,
            tidb_exec::session_commit_protocol::bootstrap_commit_protocol(),
        )
        .unwrap();
        let checked = Arc::new(CheckedTables::default());
        transaction.set_schema_lease_checker(checked.clone());
        let buffer = MutationBuffer::new();
        transaction.bind_mutation_buffer(&buffer);
        let table_key = |id| {
            tidb_tablecodec::table_key::encode_row_key_with_handle(
                id,
                &tidb_tablecodec::table_key::RecordHandle::Int(1),
            )
        };
        buffer
            .set(Key::from_bytes(table_key(10)), b"row".to_vec())
            .unwrap();
        transaction
            .commit_with(
                &buffer,
                vec![BufferMutation::set(table_key(20), b"row").unwrap()],
            )
            .unwrap();
        let tables = checked.0.lock().unwrap();
        assert!(!tables.is_empty(), "commit must check the schema lease");
        assert!(tables.iter().all(|ids| ids == &[10, 20]), "{tables:?}");
    }

    #[test]
    fn restricted_session_commit_keeps_locks_when_attaching_an_empty_sql_handle() {
        let (_authority, _pd, opener) = in_process_write_stack().unwrap();
        let transaction = SessionTransaction::begin_pessimistic(
            Arc::new(opener.clone()),
            IN_PROCESS_TIMEOUT,
            tidb_exec::session_commit_protocol::bootstrap_commit_protocol(),
        )
        .unwrap();
        let key = b"locked".to_vec();
        assert!(matches!(
            transaction.lock_keys(vec![key.clone()]).unwrap(),
            tidb_exec::cluster_table_storage::LockKeysOutcome::Locked { .. }
        ));
        transaction
            .commit_with(
                &MutationBuffer::new(),
                vec![BufferMutation::set(key.clone(), b"value").unwrap()],
            )
            .unwrap();
        let mut reader = opener.begin().unwrap();
        let call = UnaryCallContext::with_timeout(IN_PROCESS_TIMEOUT);
        assert_eq!(
            reader.snapshot_get(&key, &call).unwrap().value,
            Some(b"value".to_vec())
        );
        reader.finish_without_writes().unwrap();
    }
}

pub(crate) fn in_process_write_stack() -> Result<
    (
        SharedReadAuthority<InProcessClient, InProcessRegionLoader>,
        InProcessPd,
        InProcessOpener,
    ),
    SqlQueryError,
> {
    let pd = InProcessPd::new();
    // Go's unistore store holds THE cluster's mock PD client
    // (`mock_region.go`), so its async-commit/1PC `minCommitTS` draws from
    // the same oracle start timestamps do. The embedded store mirrors that:
    // one shared oracle, both faster protocols available.
    let client = InProcessClient::over(KvHandler {
        store: MvccStore::with_pd(pd.oracle()),
    });
    let cache = RegionCache::new(InProcessRegionLoader);
    let read_authority = SharedReadAuthority::start_with_lock_resolver(client, cache)
        .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
    // The embedded store never garbage-collects, so the read floor is a
    // static zero -- Go's unistore behavior for a store with no PD to ask.
    let gc_state = TxnSafePointRefresher::start_with_source(|| Ok(0))
        .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
    // The bootstrapped cluster protocol -- async commit and 1PC are both ON
    // on TiKV (`session_commit_protocol`) -- and the shared-oracle store can
    // answer both, exactly as Go's unistore does under its mock PD.
    let transaction_opener = RealOptimisticTransactionOpener::from_capabilities(
        read_authority.opener(),
        pd.clone(),
        IN_PROCESS_TIMEOUT,
        gc_state,
    )
    .map_err(|error| SqlQueryError::unknown(error.to_string()))?
    .with_commit_protocol(tidb_exec::session_commit_protocol::bootstrap_commit_protocol());
    Ok((read_authority, pd, transaction_opener))
}

/// Runs the wide cluster-session surface over the embedded store.
///
/// Go's `--store unistore` path in full: the store starts empty on every
/// run, so the boot FIRST publishes the `mysql` schema bootstrap --
/// `session.BootstrapSession`'s work -- then loads the catalog it just
/// wrote and serves the same session driver the cluster node serves.
/// There is no etcd and no peer, so the watch legs are simply absent;
/// the reload ticks still run, against this process's own store.
pub(crate) fn run_unistore_cluster_session(
    config: crate::node_config::NodeConfig,
    spill_storage: Arc<tidb_util::spill_storage::SpillStorage>,
    memory_arbitrator: Option<Arc<tidb_util::memory::MemArbitrator>>,
) -> Result<(), crate::real_tikv_node::RunConfiguredNodeError> {
    use crate::real_tikv_node::RunConfiguredNodeError;

    // Go's unistore client-go observes the single in-process store (id 1);
    // materialize the store-scoped dashboard series for it.
    tidb_txnkv::client_go_metrics::init_embedded_store_series(IN_PROCESS_STORE_ID);
    let users = configured_account_store(&config)?;
    let users = Arc::new(users);
    let stack =
        unistore_cluster_session_stack(&config, &users, Some(spill_storage), memory_arbitrator)?;
    let UnistoreClusterStack {
        factory,
        schema_version,
        stats,
        _reloader: reloader,
        _sysvar_reloader: sysvar_reloader,
        _stats_reloader: stats_reloader,
        _binding_reloader: binding_reloader,
        _async_stats_loader,
        _stats_maintenance: stats_maintenance,
        _read_authority: read_authority,
    } = stack;
    if tidb_config::config_tree::config::get_global_config()
        .instance
        .tidb_enable_stats_owner
        .load()
    {
        factory
            .campaign_stats_owner(config.stats_lease)
            .map_err(|error| {
                RunConfiguredNodeError::Engine(SqlQueryError::unknown(format!(
                    "campaign stats owner failed: {error}"
                )))
            })?;
    }
    factory.start_stats_usage_workers(config.stats_lease);
    factory.start_analyze_jobs_cleanup_worker(config.stats_lease);
    factory.start_historical_stats_worker();
    let stats_receipt = stats.receipt();

    let node = ConcurrentSqlNode::bind(&config, Arc::clone(&factory), Arc::clone(&users))
        .map_err(RunConfiguredNodeError::Node)?;
    // Go starts the status HTTP server beside the SQL listener
    // (`cfg.Status.ReportStatus`, default true); `/status` is the first
    // thing `main_test.go` and every health probe reads. A failed bind
    // logs and continues, as Go's does.
    let _status_server = if config.report_status {
        let schema_factory = Arc::clone(&factory);
        match crate::http_status::start_status_listener_with_routes(
            &config.status_host,
            config.status_port,
            node.tracker(),
            tidb_mysql::runtime_versions().server_version,
            tidb_util::versioninfo::TIDB_GIT_HASH.to_owned(),
            crate::http_status::StatusRoutes {
                schema: Some(Arc::new(move || schema_factory.catalog_snapshot())),
                settings: Some(factory.status_settings()),
                peer: Some(factory.peer_service()),
                security: config.cluster_security.clone(),
            },
        ) {
            Ok(server) => {
                eprintln!(
                    "{{\"event\":\"status_listener_ready\",\"address\":\"{}\"}}",
                    server.local_addr()
                );
                Some(server)
            }
            Err(error) => {
                eprintln!("{{\"event\":\"status_listener_error\",\"error\":\"{error}\"}}");
                None
            }
        }
    } else {
        None
    };
    let address = node.local_addr().map_err(RunConfiguredNodeError::Node)?;
    let shutdown = node.shutdown_handle();
    let last_signal =
        crate::shutdown_signal::install(move || shutdown.shutdown()).map_err(|error| {
            RunConfiguredNodeError::Engine(SqlQueryError::unknown(error.to_string()))
        })?;
    eprintln!(
        "{{\"event\":\"cluster_session_node_ready\",\"address\":\"{address}\",\"store\":\"unistore\",\"schema_version\":{schema_version},\"max_connections\":{},\"account_count\":{},\"stats_loaded\":{},\"stats_pseudo\":{}}}",
        config.max_connections,
        users.len(),
        stats_receipt.loaded,
        stats_receipt.pseudo,
    );
    let result = node.run().map_err(RunConfiguredNodeError::Node);
    drop(stats_maintenance);
    drop(reloader);
    drop(sysvar_reloader);
    drop(stats_reloader);
    drop(binding_reloader);
    drop(read_authority);
    finish_with_signal_code(result, &last_signal)
}

/// The cluster-session factory over the embedded store, plus the guards
/// that keep its store and reload threads alive. One build, two callers:
/// the running node and the in-tree tests that pin coprocessor-backed
/// execution -- a test over this stack exercises the SAME bootstrap,
/// catalog, and `CopScanSource` the live `--store unistore
/// --cluster-session` process serves.
pub(crate) struct UnistoreClusterStack {
    pub(crate) factory: Arc<crate::cluster_session_node::ClusterSessionFactory>,
    pub(crate) schema_version: i64,
    pub(crate) stats: Arc<tidb_exec::stats_watch::SharedStats>,
    pub(crate) _stats_maintenance:
        Option<crate::cluster_session_node::stats_maintenance::StatsMaintenanceWorker>,
    // Guards, dropped in declaration order: reload threads first, then the
    // store they read from.
    pub(crate) _reloader: tidb_exec::catalog_watch::CatalogReloader,
    pub(crate) _sysvar_reloader: crate::cluster_sysvar_seam::SysvarReloader,
    pub(crate) _stats_reloader: tidb_exec::stats_watch::StatsReloader,
    pub(crate) _binding_reloader: crate::cluster_binding_seam::BindingReloader,
    pub(crate) _async_stats_loader: tidb_exec::stats_watch::AsyncStatsLoader,
    pub(crate) _read_authority: SharedReadAuthority<InProcessClient, InProcessRegionLoader>,
}

/// Builds the wide cluster-session factory over the embedded store:
/// bootstrap publish, account seeding, catalog/stats/sysvar followers, and
/// the in-process coprocessor.
pub(crate) fn unistore_cluster_session_stack(
    config: &crate::node_config::NodeConfig,
    users: &Arc<crate::ConfiguredUserStore>,
    spill_storage: Option<Arc<tidb_util::spill_storage::SpillStorage>>,
    memory_arbitrator: Option<Arc<tidb_util::memory::MemArbitrator>>,
) -> Result<UnistoreClusterStack, crate::real_tikv_node::RunConfiguredNodeError> {
    use crate::cluster_session_node::{
        ClusterSessionFactory, RealClusterDdl, RealClusterTransactions,
    };
    use crate::real_tikv_node::RunConfiguredNodeError;

    let engine = RunConfiguredNodeError::Engine;
    let (read_authority, _pd, opener) = in_process_write_stack().map_err(engine)?;

    // The bootstrap: every boot, because the store is empty every boot.
    let (outcome, schema_version) =
        crate::bootstrap_publish::publish_bootstrap(&opener, IN_PROCESS_TIMEOUT)
            .map_err(|error| engine(SqlQueryError::unknown(error.to_string())))?;
    let schema_version =
        crate::bootstrap_publish::notify_committed_bootstrap(&outcome, schema_version, None)
            .map_err(|error| engine(SqlQueryError::unknown(error.to_string())))?;
    eprintln!("{{\"event\":\"bootstrap_committed\",\"schema_version\":{schema_version}}}");

    // Go's bootstrap INSERTs `root@%` into `mysql.user`; this node provisions
    // its startup identity from `--auth-file` instead, so it has to become a
    // real row before any account statement rewrites the set -- see
    // `cluster_account_seam::seed_cluster_accounts` for what happened when it
    // did not.
    let seeded = crate::cluster_account_seam::seed_cluster_accounts(
        &opener,
        &users.accounts(),
        IN_PROCESS_TIMEOUT,
    )
    .map_err(engine)?;
    if !seeded.is_empty() {
        eprintln!(
            "{{\"event\":\"accounts_seeded\",\"identities\":{}}}",
            seeded.len()
        );
    }

    let startup =
        tidb_exec::real_tikv_catalog::load_catalog_from_cluster(&opener, IN_PROCESS_TIMEOUT)
            .map_err(|error| engine(SqlQueryError::unknown(error.to_string())))?;

    // The bootstrap persisted the system time zone and the global-variable
    // rows this instant; reading them back through the same seam is the
    // production boot's own order.
    crate::real_tikv_node::load_cluster_startup_variables(&users, &opener)?;

    let (catalog, reloader, schema_validator) =
        crate::real_tikv_node::spawn_catalog_reloader(startup, opener.clone(), config.schema_lease)
            .map_err(|error| engine(SqlQueryError::unknown(error.to_string())))?;
    let (stats, stats_reloader, async_stats_loader) = crate::real_tikv_node::spawn_node_stats(
        Arc::clone(&catalog),
        opener.clone(),
        config.stats_lease,
        IN_PROCESS_TIMEOUT,
    )
    .map_err(|error| engine(SqlQueryError::unknown(error.to_string())))?;

    let sysvar_publication_fence = crate::cluster_sysvar_seam::SysvarPublicationFence::default();
    let sysvar_reloader = crate::cluster_sysvar_seam::SysvarReloader::spawn(
        users.global_vars(),
        opener.clone(),
        crate::real_tikv_node::sysvar_reload_interval(config.schema_lease),
        IN_PROCESS_TIMEOUT,
        sysvar_publication_fence.clone(),
    )
    .map_err(|error| engine(SqlQueryError::unknown(error.to_string())))?;

    let transport_factory = Arc::new(InProcessReadSessionFactory {
        read_opener: read_authority.opener(),
        lock_timestamp_source: CapabilityTimestampSource(_pd.clone()),
    });
    // This node's own server-info record: the id Go mints with `uuid.New()`
    // for its DDL owner, and the address/ports/labels a peer would read.
    // With no etcd client the syncer publishes nothing and answers reads
    // with this node alone -- Go's `etcdCli == nil` path, and exactly what
    // `information_schema.TIDB_SERVERS_INFO` shows on a single node.
    let server_info = Arc::new(
        tidb_domain::serverinfo_syncer::Syncer::new_with_status_endpoint_claim(
            crate::serverinfo_etcd::node_server_info(config),
            None,
            config.report_status,
        ),
    );
    let stats_owner: Arc<dyn tidb_owner::Manager> = Arc::new(tidb_owner::MockManager::new(
        tidb_owner::Context::background(),
        server_info.local_server_info().static_info.id.clone(),
        // Go Domain.NewOwnerManager supplies store.UUID(). Opener clones
        // share this store authority, while independent embedded stores do not.
        Some(&format!("embedded-authority-{}", opener.authority_id())),
        crate::cluster_session_node::STATS_OWNER_KEY,
    ));
    let cop_scans: Arc<dyn tidb_executor::remote_scan::PushdownScanner> =
        Arc::new(tidb_exec::cop_scan::CopScanSource::new(transport_factory));
    let (bindings, binding_reloader) = crate::cluster_binding_seam::start_binding_cache(
        opener.clone(),
        Arc::clone(&catalog),
        users.global_vars(),
        IN_PROCESS_TIMEOUT,
        Arc::new(tidb_owner::MockManager::new(
            tidb_owner::Context::background(),
            server_info.local_server_info().static_info.id.clone(),
            Some(&format!("embedded-authority-{}", opener.authority_id())),
            "/tidb/bindinfo/owner",
        )),
    )
    .map_err(|error| engine(SqlQueryError::unknown(error)))?;
    let cluster_ddl = Arc::new(
        RealClusterDdl::new(
            opener.clone(),
            Arc::clone(&catalog),
            IN_PROCESS_TIMEOUT,
            // No etcd: schema changes announce themselves to nobody, and the
            // reload tick above is the only follower -- correct for one node.
            None,
            Arc::clone(&server_info),
            None,
            Arc::clone(&schema_validator),
            config.run_ddl,
        )
        .map_err(|error| {
            crate::real_tikv_node::RunConfiguredNodeError::Engine(SqlQueryError::unknown(format!(
                "campaign DDL owner failed: {error}"
            )))
        })?,
    );

    let factory = ClusterSessionFactory::new(
        Arc::new(RealClusterTransactions::new(
            opener.clone(),
            IN_PROCESS_TIMEOUT,
        )),
        cluster_ddl,
        Arc::new(crate::cluster_account_seam::RealClusterAccountWriter::new(
            Arc::new(opener.clone()),
            users.accounts(),
            IN_PROCESS_TIMEOUT,
            None,
        )),
        Arc::new(crate::cluster_sysvar_seam::RealClusterSysvarWriter::new(
            Arc::new(opener.clone()),
            users.global_vars(),
            IN_PROCESS_TIMEOUT,
            None,
            sysvar_publication_fence,
        )),
        Arc::new(crate::cluster_analyze_seam::RealClusterAnalyze::new(
            Arc::new(opener.clone()),
            Arc::clone(&stats),
            IN_PROCESS_TIMEOUT,
            config.stats_lease.slow_save_interval(),
        )),
        Arc::new(crate::cluster_stats_lock_seam::RealClusterStatsLock::new(
            Arc::new(opener.clone()),
            IN_PROCESS_TIMEOUT,
        )),
        catalog,
        users.accounts(),
        users.global_vars(),
        Arc::clone(&stats),
        Arc::new(crate::cluster_auto_id_seam::ClusterTableAutoIds::new(
            opener,
            IN_PROCESS_TIMEOUT,
        )),
    )
    .with_cop_scans(cop_scans)
    .with_server_info(server_info)
    .with_cluster_peer_client(Arc::new(tidb_exec::cluster_peer::ClusterPeerClient::new(
        tidb_txnkv::rpc::TonicCoprocessorClient::with_security(Arc::new(
            config.cluster_security.clone(),
        ))
        .map_err(|error| engine(SqlQueryError::unknown(error.to_string())))?,
    )))
    .with_cluster_config_client(Arc::new(
        tidb_exec::cluster_config::ClusterConfigClient::new(&config.cluster_security)
            .map_err(|error| engine(SqlQueryError::unknown(error)))?,
    ))
    .with_stats_owner(stats_owner);
    let factory = match spill_storage {
        Some(storage) => factory.with_spill_storage(storage),
        None => factory,
    };
    let factory = match memory_arbitrator {
        Some(arbitrator) => factory.with_mem_arbitrator(arbitrator),
        None => factory,
    };
    let factory = factory.with_bindings(bindings);
    factory.attach_login_storage(&users);

    let stats_maintenance =
        crate::cluster_session_node::stats_maintenance::StatsMaintenanceWorker::start(
            &factory,
            config.stats_lease,
            config.schema_lease,
            async_stats_loader.initialization(),
        )
        .map_err(|error| engine(SqlQueryError::unknown(error)))?;

    Ok(UnistoreClusterStack {
        factory,
        schema_version,
        stats,
        _stats_maintenance: stats_maintenance,
        _reloader: reloader,
        _sysvar_reloader: sysvar_reloader,
        _stats_reloader: stats_reloader,
        _binding_reloader: binding_reloader,
        _async_stats_loader: async_stats_loader,
        _read_authority: read_authority,
    })
}

/// [`tidb_exec::meta_txn`] against real (embedded) storage: Go's
/// `kv.Transaction` memory-buffer contract, `meta.NewMutator(txn)`, and
/// `kv.RunInNewTxn`'s retry on a write conflict.
#[cfg(test)]
mod meta_txn_tests {
    use super::*;
    use tidb_exec::meta_txn::MetaTxnStorage;
    use tidb_meta::transaction::{Mutator, RawTransaction};
    use tidb_txnkv::{run_in_new_txn, NewTxnStorage, NewTxnTransaction, RunInNewTxnContext};

    /// The whole embedded stack. The opener borrows the read authority's
    /// background cache and the PD oracle, so all three must live as long as
    /// any transaction -- dropping the first two closes cache admission.
    struct Stack {
        _read_authority: SharedReadAuthority<InProcessClient, InProcessRegionLoader>,
        _pd: InProcessPd,
        opener: InProcessOpener,
    }

    fn stack() -> Stack {
        let (read_authority, pd, opener) = in_process_write_stack().expect("embedded write stack");
        Stack {
            _read_authority: read_authority,
            _pd: pd,
            opener,
        }
    }

    /// A write is visible to the same transaction before commit, absent from
    /// every other transaction until then, and durable after it -- Go's memory
    /// buffer in front of the snapshot.
    #[test]
    fn writes_read_back_before_commit_and_persist_after_it() {
        let stack = stack();
        let opener = &stack.opener;
        let mut storage = MetaTxnStorage::new(opener, IN_PROCESS_TIMEOUT);

        let mut writer = storage.begin().unwrap();
        writer.set(b"mt/a".to_vec(), b"1".to_vec()).unwrap();
        assert_eq!(writer.get(b"mt/a").unwrap(), Some(b"1".to_vec()));

        let mut peer = storage.begin().unwrap();
        assert_eq!(peer.get(b"mt/a").unwrap(), None, "uncommitted writes stay private");
        NewTxnTransaction::rollback(&mut peer).unwrap();

        NewTxnTransaction::commit(&mut writer).unwrap();
        let mut reader = storage.begin().unwrap();
        assert_eq!(reader.get(b"mt/a").unwrap(), Some(b"1".to_vec()));
        NewTxnTransaction::rollback(&mut reader).unwrap();
    }

    /// A scan merges the buffer over the snapshot: a buffered set appears, a
    /// buffered delete hides a committed key, and the order is byte order.
    #[test]
    fn scans_merge_the_buffer_over_the_snapshot() {
        let stack = stack();
        let opener = &stack.opener;
        let mut storage = MetaTxnStorage::new(opener, IN_PROCESS_TIMEOUT);
        let mut seed = storage.begin().unwrap();
        seed.set(b"ms/1".to_vec(), b"one".to_vec()).unwrap();
        seed.set(b"ms/3".to_vec(), b"three".to_vec()).unwrap();
        NewTxnTransaction::commit(&mut seed).unwrap();

        let mut txn = storage.begin().unwrap();
        txn.set(b"ms/2".to_vec(), b"two".to_vec()).unwrap();
        txn.delete(b"ms/3").unwrap();
        assert_eq!(
            txn.scan_prefix(b"ms/").unwrap(),
            vec![
                (b"ms/1".to_vec(), b"one".to_vec()),
                (b"ms/2".to_vec(), b"two".to_vec()),
            ]
        );
        NewTxnTransaction::rollback(&mut txn).unwrap();

        // The rollback discarded both buffered changes.
        let mut after = storage.begin().unwrap();
        assert_eq!(
            after.scan_prefix(b"ms/").unwrap(),
            vec![
                (b"ms/1".to_vec(), b"one".to_vec()),
                (b"ms/3".to_vec(), b"three".to_vec()),
            ]
        );
        NewTxnTransaction::rollback(&mut after).unwrap();
    }

    /// The point of the adapter: Go's ported `meta.Mutator` round-trips a meta
    /// structure through real storage. `SetRUStats` then `GetRUStats` is the
    /// pair the RU statistics writer depends on.
    #[test]
    fn the_meta_mutator_round_trips_ru_stats_through_real_storage() {
        use tidb_meta::transaction::{DailyRuStats, GroupRuStats, RuConsumption, RuStats};
        let stack = stack();
        let opener = &stack.opener;
        let mut storage = MetaTxnStorage::new(opener, IN_PROCESS_TIMEOUT);
        let stats = RuStats {
            latest: Some(Box::new(DailyRuStats {
                end_time: chrono::DateTime::parse_from_rfc3339("2026-10-08T00:00:00Z")
                    .unwrap()
                    .with_timezone(&chrono::Utc),
                stats: Some(vec![GroupRuStats {
                    id: 3,
                    name: "rg1".to_owned(),
                    ru_consumption: Some(RuConsumption::default()),
                }]),
            })),
            previous: None,
        };
        run_in_new_txn(&RunInNewTxnContext::default(), &mut storage, true, |txn| {
            Mutator::new(&mut *txn).set_ru_stats(Some(&stats))?;
            Ok(())
        })
        .unwrap();

        let mut read = storage.begin().unwrap();
        let loaded = Mutator::new(&mut read).ru_stats().unwrap();
        assert_eq!(loaded, Some(stats));
        NewTxnTransaction::rollback(&mut read).unwrap();
    }

    /// Go `kv.RunInNewTxn` retries a write conflict and only that: a peer that
    /// commits the same key between this attempt's read and its commit makes
    /// the first attempt lose, and the retry re-reads and wins.
    #[test]
    fn run_in_new_txn_retries_a_write_conflict() {
        let stack = stack();
        let opener = &stack.opener;
        let mut storage = MetaTxnStorage::new(opener, IN_PROCESS_TIMEOUT);
        let mut seed = storage.begin().unwrap();
        seed.set(b"mc/k".to_vec(), b"0".to_vec()).unwrap();
        NewTxnTransaction::commit(&mut seed).unwrap();

        let mut attempts = 0;
        let peer_opener = opener;
        run_in_new_txn(&RunInNewTxnContext::default(), &mut storage, true, |txn| {
            attempts += 1;
            let current = txn.get(b"mc/k")?.unwrap_or_default();
            if attempts == 1 {
                // A peer commits the same key after this attempt read it.
                let mut peer_storage = MetaTxnStorage::new(peer_opener, IN_PROCESS_TIMEOUT);
                let mut peer = peer_storage.begin().unwrap();
                peer.set(b"mc/k".to_vec(), b"peer".to_vec()).unwrap();
                NewTxnTransaction::commit(&mut peer).unwrap();
            }
            let mut next = current.clone();
            next.extend_from_slice(b"+");
            txn.set(b"mc/k".to_vec(), next)?;
            Ok(())
        })
        .unwrap();
        assert_eq!(attempts, 2, "the conflicted attempt retried once");

        let mut read = storage.begin().unwrap();
        assert_eq!(
            read.get(b"mc/k").unwrap(),
            Some(b"peer+".to_vec()),
            "the retry re-read the peer's value before writing"
        );
        NewTxnTransaction::rollback(&mut read).unwrap();
    }
}
