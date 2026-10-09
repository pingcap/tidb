//! Booting the convergence node: what is read from the cluster before the
//! first connection, and which reload threads outlive the boot.
//!
//! [`run_cluster_session_node`] is this node's `main`: it connects to PD,
//! reads the catalog, the accounts, the global variables and the statistics
//! out of the cluster, spawns the reloader thread and etcd watch that keep
//! each of them following a Go peer's changes, builds the
//! [`ClusterSessionFactory`](super::ClusterSessionFactory) every connection
//! is opened from, and serves the MySQL port until shutdown. It mirrors the
//! bootstrap half of Go's `pkg/session/session.go` (`BootstrapSession` and
//! the `domain.Domain` reload loops it starts) rather than the statement
//! lifecycle, which stays in [`super`].
//!
//! The tuple handed to `run_with_process_shutdown` is ordered, and the order
//! is load-bearing: every reload thread holds its own PD handle, so each is
//! joined before the authority's shutdown drain, and a watch is always
//! dropped before the reloader it nudges.

use std::sync::Arc;
use std::time::Duration;

use tidb_exec::cop_scan::CopScanSource;
use tidb_exec::ddl_systable::MinJobIdRefresher;
use tidb_exec::real_tikv_catalog::load_catalog_from_cluster;
use tidb_exec::real_tikv_read::ProductionReadProcessAuthority;
use tidb_executor::remote_scan::PushdownScanner;
use tidb_planner::read_only_scan::{ConfiguredColumn, ConfiguredTable};

use crate::cluster_account_seam::RealClusterAccountWriter;
use crate::cluster_analyze_seam::RealClusterAnalyze;
use crate::cluster_session::SkippedTable;
use crate::cluster_stats_lock_seam::RealClusterStatsLock;
use crate::cluster_sysvar_seam::{RealClusterSysvarWriter, SysvarPublicationFence};
use crate::node_config::NodeConfig;
use crate::real_tikv_node::{
    node_accounts, run_with_process_shutdown, spawn_catalog_reloader, spawn_privilege_watch,
    RunConfiguredNodeError,
};

/// The per-request RPC deadline for the transaction tier's row reads and
/// writes. Deliberately NOT [`super::CONTROL_PLANE_TIMEOUT`]: those five
/// seconds budget metadata round trips that answer in milliseconds, while a
/// data read can legitimately wait behind concurrent coprocessor scans on the
/// same store. Go budgets the same surface with `tikvGRPCTimeout`-scale
/// deadlines (tens of seconds), and a request queued past ITS deadline failed
/// with `timeout_ms: 0` -- the admission-time zero the old constant produced.
const TRANSACTION_RPC_TIMEOUT: Duration = Duration::from_secs(60);
use crate::sql_node::{ConcurrentSqlNode, SqlQueryError};

use super::{
    ClusterSessionFactory, RealClusterDdl, RealClusterTransactions, CONTROL_PLANE_TIMEOUT,
};

/// The coprocessor read deadline is a statement-runtime budget, not the
/// five-second boot/catalog control-plane deadline. TPC-H partial aggregates
/// legitimately spend more than five seconds in TiKV on a cold SF1 cache.
const COPROCESSOR_QUERY_TIMEOUT: Duration = Duration::from_secs(300);
/// \`ANALYZE\` reads whole tables -- one prefix scan over \`cast_info\` is
/// minutes, not milliseconds -- so its reads carry the query budget rather
/// than the five-second control-plane one whose expiry aborted every large
/// table's statistics run mid-scan.
const ANALYZE_READ_TIMEOUT: Duration = COPROCESSOR_QUERY_TIMEOUT;

/// Starts the convergence node: wide SQL over cluster storage and cluster
/// accounts, served on the MySQL port.
pub fn run_cluster_session_node(config: NodeConfig) -> Result<(), RunConfiguredNodeError> {
    let spill_storage = crate::open_spill_storage(&config)?;
    let memory_arbitrator = crate::MemoryArbitratorAuthority::open(&config)?;
    run_cluster_session_node_with_spill(config, spill_storage, memory_arbitrator.arbitrator())
}

pub(crate) fn run_cluster_session_node_with_spill(
    config: NodeConfig,
    spill_storage: Arc<tidb_util::spill_storage::SpillStorage>,
    memory_arbitrator: Option<Arc<tidb_util::memory::MemArbitrator>>,
) -> Result<(), RunConfiguredNodeError> {
    let mut loaded = None;
    let authority = ProductionReadProcessAuthority::connect_with_catalog_and_security(
        config.pd_endpoints.clone(),
        COPROCESSOR_QUERY_TIMEOUT,
        Arc::new(config.cluster_security.clone()),
        |opener| {
            // tiup's deploy->patch->start flow boots this node against a
            // FRESH keyspace before any Go TiDB ever ran, so `mysql.*` does
            // not exist yet. The convergence node owns the cluster's system
            // catalog from then on, so bootstrap it right here and let the
            // catalog load below read the schema just published. A cluster a
            // TiDB already bootstrapped skips the publish entirely.
            let accounts = tidb_exec::real_tikv_privileges::load_accounts_from_cluster(
                opener,
                COPROCESSOR_QUERY_TIMEOUT,
            )
            .map_err(|error| error.to_string())?;
            if !accounts.bootstrap.already_bootstrapped() {
                let (outcome, schema_version) = crate::bootstrap_publish::publish_bootstrap(
                    opener,
                    COPROCESSOR_QUERY_TIMEOUT,
                )
                .map_err(|error| error.to_string())?;
                let schema_version = crate::bootstrap_publish::notify_committed_bootstrap(
                    &outcome,
                    schema_version,
                    None,
                )
                .map_err(|error| error.to_string())?;
                eprintln!(
                    "{{\"event\":\"cluster_bootstrap_published\",\"schema_version\":{schema_version}}}"
                );
            }
            loaded = Some(
                load_catalog_from_cluster(opener, COPROCESSOR_QUERY_TIMEOUT)
                    .map_err(|error| error.to_string())?,
            );
            // The authority insists on naming one bounded-read table because
            // the single-relation coprocessor path is built around one
            // relation. This node never opens a bounded read session -- every
            // statement goes through the session driver -- so the table is
            // inert, and naming a real one would only make startup depend on
            // the cluster happening to hold a table of that shape.
            Ok(ConfiguredTable::new(
                "",
                "",
                1,
                Vec::<ConfiguredColumn>::new(),
            ))
        },
    )
    .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error.to_string())))?;
    let startup = loaded.expect("the catalog closure ran exactly once");
    let schema_version = startup.schema_version;

    // `node_accounts` also hands back the privilege reloader (landed in
    // parallel); it must stay alive for the node's run and drop before the
    // authority's shutdown drain, like the catalog reloader below.
    let (users, privilege_reloader) = node_accounts(&config, &authority)?;
    crate::real_tikv_node::load_cluster_startup_variables(&users, &authority.transaction_opener())?;
    if let Some(pd) = authority.pd_client() {
        users
            .global_vars()
            .set_pd_region_policy(Arc::new(ProcessPdRegionPolicy(pd)));
    }
    // The cluster-session path owns one process-wide sysvar reloader below.
    // Its persisted boot image was installed synchronously above, before this
    // reloader and, crucially, before bind. This is independent of
    // privilege-cache policy.
    let (catalog, reloader, schema_validator) =
        spawn_catalog_reloader(startup, authority.transaction_opener(), config.schema_lease)
            .map_err(|error| {
                RunConfiguredNodeError::Engine(SqlQueryError::unknown(error.to_string()))
            })?;
    // Statistics always resolve targets from this published catalog, so the
    // reload loop follows DDL-added tables and changed column types instead of
    // freezing the boot image.
    let (stats, stats_reloader, async_stats_loader) = crate::real_tikv_node::spawn_node_stats(
        Arc::clone(&catalog),
        authority.transaction_opener(),
        config.stats_lease,
        COPROCESSOR_QUERY_TIMEOUT,
    )
    .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error.to_string())))?;
    // The account half of the same division: the reloader's tick is what makes
    // a peer's `GRANT` reach this node at all, and this watch is what makes it
    // arrive in a round trip instead of an interval.
    let privilege_watcher = spawn_privilege_watch(&config, privilege_reloader.as_ref());
    // The sysvar half of the same division: a Go peer's `SET GLOBAL` is
    // durable in `mysql.global_variables` the instant it commits, and this
    // reloader is what makes THIS node notice it -- one tick, or one round
    // trip if the etcd watch fires first. The fallback is capped at Go's
    // 30-second `LoadSysVarCacheLoop` interval even with a longer lease.
    let sysvar_publication_fence = SysvarPublicationFence::default();
    let sysvar_reloader = crate::cluster_sysvar_seam::SysvarReloader::spawn(
        users.global_vars(),
        authority.transaction_opener(),
        crate::real_tikv_node::sysvar_reload_interval(config.schema_lease),
        CONTROL_PLANE_TIMEOUT,
        sysvar_publication_fence.clone(),
    )
    .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error.to_string())))?;
    let sysvar_watcher = crate::real_tikv_node::spawn_sysvar_watch(&config, Some(&sysvar_reloader));
    // Publish the effective startup value immediately before the schema-ack
    // worker is created.  The sysvar reloader can perform an initial empty
    // snapshot while bootstrap tables are still converging; leaving the
    // process flag at false in that window makes this peer use the legacy
    // per-node schema-version key even though Go's effective default is ON.
    let mdl_enabled = users
        .global_vars()
        .get(tidb_vardef::tidb_vars::TIDB_ENABLE_MDL)
        .map_or(tidb_vardef::defaults::DEF_TIDB_ENABLE_MDL, |value| {
            value.eq_ignore_ascii_case("ON") || value == "1"
        });
    tidb_vardef::set_enable_mdl(mdl_enabled);
    // The node's coprocessor: base-table scans now carry their predicate,
    // their row cap and their column list to the region, and only the
    // surviving rows come back. The session's own staged writes are merged on
    // top of them client-side, which is Go's `UnionScan` over a distsql
    // reader.
    //
    // TiFlash discovery and range lookup retain the node's existing PD and
    // canonical region-cache capabilities (Go MPPClient.ConstructMPPTasks).
    let cop_scans: Arc<dyn PushdownScanner> = {
        let catalog = Arc::clone(&catalog);
        let tiflash_mpp =
            crate::cluster_session_node::build_tiflash_mpp_source(&authority, move || {
                catalog.load().schema_version
            });
        match tiflash_mpp {
            Some(source) => {
                Arc::new(CopScanSource::new(authority.transport_factory()).with_tiflash_mpp(source))
            }
            None => Arc::new(CopScanSource::new(authority.transport_factory())),
        }
    };
    // This node's identity in the cluster: `/tidb/server/info/<uuid>` under
    // a lease, plus the `/topology/tidb/<host:port>` pair, refreshed for as
    // long as the process lives -- Go's `Domain.Init` starting the
    // server-info syncer beside its reloaders. A node with no reachable
    // etcd still HAS the record; it just publishes nowhere, and
    // `information_schema.TIDB_SERVERS_INFO` then reports this node alone,
    // which is Go's `etcdCli == nil` answer.
    let server_etcd = crate::real_tikv_node::connect_schema_notifier(&config).map(|client| {
        Arc::new(crate::serverinfo_etcd::EtcdClientOps::new(client))
            as Arc<dyn tidb_domain::serverinfo_syncer::EtcdOps>
    });
    // A failed cluster connection is not a standalone deployment.
    if config.global_config.enable_global_kill && server_etcd.is_none() {
        return Err(RunConfiguredNodeError::Engine(SqlQueryError::unknown(
            "cluster server identity requires its configured etcd client",
        )));
    }
    let server_identity = config.global_config.enable_global_kill.then(|| {
        tidb_domain::server_id::ServerIdAuthority::new(
            server_etcd.clone(),
            config.global_config.enable_32bits_connection_id,
        )
    });
    let mut info = crate::serverinfo_etcd::node_server_info(&config);
    if let Some(identity) = &server_identity {
        let identity = Arc::clone(identity);
        info.static_info.server_id_getter = Some(Arc::new(move || identity.id()));
    }
    let server_info = Arc::new(
        tidb_domain::serverinfo_syncer::Syncer::new_with_status_endpoint_claim(
            info,
            server_etcd,
            config.report_status,
        ),
    );
    let mut server_info_runner = match tidb_domain::serverinfo_syncer::SyncerRunner::start(
        Arc::clone(&server_info),
        tidb_domain::serverinfo_syncer::SyncIntervals::default(),
    ) {
        Ok(runner) => Some(runner),
        Err(error) => {
            // Go GlobalInfoSyncerInit fails startup if initial leased
            // publication fails; serving invisibly also hides GC protection.
            return Err(RunConfiguredNodeError::Engine(SqlQueryError::unknown(
                error,
            )));
        }
    };
    let server_id_keeper = server_identity
        .as_ref()
        .map(|identity| {
            tidb_domain::server_id::ServerIdKeeper::start(
                Arc::clone(identity),
                Arc::clone(&server_info),
                Default::default(),
            )
        })
        .transpose()
        .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error)))?;
    let discovery = authority
        .pd_client()
        .map(|pd| {
            tidb_exec::cluster_discovery::PdClusterDiscovery::new(pd, &config.cluster_security).map(
                |discovery| {
                    Arc::new(discovery) as Arc<dyn tidb_domain::cluster_topology::ClusterDiscovery>
                },
            )
        })
        .transpose()
        .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error)))?;
    let cluster_topology = Arc::new(tidb_domain::cluster_topology::ClusterTopology::new(
        Arc::clone(&server_info),
        discovery,
    ));
    let replica_read_checker = crate::cluster_topology::ReplicaReadChecker::start(
        Arc::clone(&cluster_topology),
        users.global_vars(),
    )
    .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error.to_string())))?;
    let stats_owner: Arc<dyn tidb_owner::Manager> =
        match crate::real_tikv_node::connect_schema_notifier(&config) {
            Some(client) => Arc::new(tidb_owner::OwnerManager::new(
                tidb_owner::Context::background(),
                client as Arc<dyn tidb_owner::OwnerStore>,
                super::STATS_OWNER_PROMPT,
                server_info.local_server_info().static_info.id,
                super::STATS_OWNER_KEY,
            )),
            None => Arc::new(tidb_owner::MockManager::new(
                tidb_owner::Context::background(),
                server_info.local_server_info().static_info.id,
                // Match the store-scoped mock DDL owner and Go store.UUID().
                Some(&format!(
                    "embedded-authority-{}",
                    authority.transaction_opener().authority_id()
                )),
                super::STATS_OWNER_KEY,
            )),
        };
    let (bindings, binding_reloader) = crate::cluster_binding_seam::start_binding_cache(
        authority.transaction_opener(),
        Arc::clone(&catalog),
        users.global_vars(),
        CONTROL_PLANE_TIMEOUT,
        match crate::real_tikv_node::connect_schema_notifier(&config) {
            Some(client) => Arc::new(tidb_owner::OwnerManager::new(
                tidb_owner::Context::background(),
                client as Arc<dyn tidb_owner::OwnerStore>,
                "bindinfo",
                server_info.local_server_info().static_info.id.clone(),
                "/tidb/bindinfo/owner",
            )),
            None => Arc::new(tidb_owner::MockManager::new(
                tidb_owner::Context::background(),
                server_info.local_server_info().static_info.id.clone(),
                Some(&format!(
                    "embedded-authority-{}",
                    authority.transaction_opener().authority_id()
                )),
                "/tidb/bindinfo/owner",
            )),
        },
    )
    .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error)))?;
    // The other half of being registered: a node the owner can SEE must also
    // ANSWER. Go's `WaitVersionSynced` waits on every `/tidb/server/info`
    // entry, so this acknowledger is spawned exactly when the registration
    // above is -- a node with no reachable etcd registers nowhere, is waited
    // on by nobody, and correctly spawns no acknowledger either.
    let schema_pins = Arc::new(super::schema_sync::SchemaPinRegistry::default());
    let min_job_id_refresher = Arc::new(MinJobIdRefresher::new());
    let schema_sync_ack = match crate::real_tikv_node::connect_schema_notifier(&config) {
        Some(etcd) => Some(
            super::schema_sync::SchemaSyncAck::spawn(
                Arc::clone(&catalog),
                authority.transaction_opener(),
                Arc::clone(&schema_pins),
                Arc::clone(&min_job_id_refresher),
                etcd,
                server_info.local_server_info().static_info.id,
                Arc::clone(&server_info),
                Arc::clone(&schema_validator),
                reloader.waker(),
                reloader.stats_source(),
                super::schema_sync::MDL_CHECK_LOOK_DURATION,
                CONTROL_PLANE_TIMEOUT,
            )
            .map_err(|error| {
                RunConfiguredNodeError::Engine(SqlQueryError::unknown(format!(
                    "initialize schema-version syncer failed: {error}"
                )))
            })?,
        ),
        None => None,
    };
    let schema_version_syncer = schema_sync_ack
        .as_ref()
        .map(super::schema_sync::SchemaSyncAck::syncer);
    let auto_id_etcd =
        crate::real_tikv_node::connect_schema_notifier(&config).ok_or_else(|| {
            RunConfiguredNodeError::Engine(SqlQueryError::unknown(
                "cluster AutoID discovery requires its configured etcd client",
            ))
        })?;
    let auto_id_service = Arc::new(
        tidb_exec::auto_id_client::AutoIdClient::new(
            Arc::new(tidb_exec::auto_id_client::EtcdAutoIdLeader(auto_id_etcd)),
            config.cluster_security.clone(),
        )
        .map_err(|e| RunConfiguredNodeError::Engine(SqlQueryError::unknown(e.to_string())))?,
    );
    let cluster_ddl = Arc::new(
        RealClusterDdl::new_with_min_job_id_refresher(
            authority.transaction_opener(),
            Arc::clone(&catalog),
            CONTROL_PLANE_TIMEOUT,
            crate::real_tikv_node::connect_schema_notifier(&config),
            Arc::clone(&server_info),
            schema_version_syncer,
            Arc::clone(&schema_validator),
            config.run_ddl,
            min_job_id_refresher,
        )
        .map_err(|error| {
            RunConfiguredNodeError::Engine(SqlQueryError::unknown(format!(
                "campaign DDL owner failed: {error}"
            )))
        })?
        .with_auto_ids(auto_id_service.clone()),
    );
    // One DDL-gated worker. Its guard joins before the node releases DDL/PD.
    let replica_poll = crate::cluster_session_node::build_tiflash_replica_poll(
        Arc::clone(&catalog),
        &config.pd_endpoints,
        &config.cluster_security,
        cluster_ddl.clone(),
    );
    let global_config_keeper = crate::global_config_sync::GlobalConfigKeeper::start(
        authority.pd_client(),
    )
    .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error.to_string())))?;
    let factory = ClusterSessionFactory::new(
        // Row reads and writes are DATA-plane traffic: a statement's snapshot
        // gets queue behind concurrent coprocessor scans on the same store,
        // so their admission deadline must tolerate store-side latency rather
        // than the 5s control-plane budget -- go gives the same surface
        // `coprocessor` timeouts measured in tens of seconds
        // (`tikv-worker`/client default 30s+).
        Arc::new(RealClusterTransactions::new(
            authority.transaction_opener(),
            TRANSACTION_RPC_TIMEOUT,
        )),
        cluster_ddl,
        Arc::new(RealClusterAccountWriter::new(
            Arc::new(authority.transaction_opener()),
            users.accounts(),
            CONTROL_PLANE_TIMEOUT,
            crate::real_tikv_node::connect_schema_notifier(&config),
        )),
        Arc::new(RealClusterSysvarWriter::new(
            Arc::new(authority.transaction_opener()),
            users.global_vars(),
            CONTROL_PLANE_TIMEOUT,
            crate::real_tikv_node::connect_schema_notifier(&config),
            sysvar_publication_fence,
        )),
        Arc::new(RealClusterAnalyze::new(
            Arc::new(authority.transaction_opener()),
            Arc::clone(&stats),
            // Go's ANALYZE reads rows and stats tables through the same
            // data-plane requests any query uses, each bounded per request
            // (`TiKVClient.CoprReqTimeout`, coprocessor.go:1791) rather than
            // by a control-plane budget. This statement walks the whole
            // catalog plus every row of the table, so its requests take the
            // data-plane deadline.
            COPROCESSOR_QUERY_TIMEOUT,
            config.stats_lease.slow_save_interval(),
        )),
        Arc::new(RealClusterStatsLock::new(
            Arc::new(authority.transaction_opener()),
            COPROCESSOR_QUERY_TIMEOUT,
        )),
        catalog,
        users.accounts(),
        users.global_vars(),
        Arc::clone(&stats),
        // One registry for the whole node, so every connection inserting
        // into a table allocates from the one range this node reserved --
        // Go's per-`tidb-server` allocator, not a per-session one.
        Arc::new(
            crate::cluster_auto_id_seam::ClusterTableAutoIds::new(
                authority.transaction_opener(),
                CONTROL_PLANE_TIMEOUT,
            )
            .with_service(auto_id_service),
        ),
    )
    .with_global_config_syncer(global_config_keeper.syncer())
    .with_cop_scans(cop_scans)
    .with_server_info(Arc::clone(&server_info))
    .with_server_identity(server_identity)
    .with_cluster_topology(cluster_topology)
    .with_cluster_peer_client(Arc::new(tidb_exec::cluster_peer::ClusterPeerClient::new(
        authority.store_rpc_opener().ok_or_else(|| {
            RunConfiguredNodeError::Engine(SqlQueryError::unknown("store RPC owner is closed"))
        })?,
    )))
    .with_cluster_config_client(Arc::new(
        tidb_exec::cluster_config::ClusterConfigClient::new(&config.cluster_security)
            .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error)))?,
    ))
    .with_stats_owner(stats_owner)
    .with_schema_pins(schema_pins)
    .with_schema_validator(Arc::clone(&schema_validator))
    .with_spill_storage(spill_storage);
    let factory = match memory_arbitrator {
        Some(arbitrator) => factory.with_mem_arbitrator(arbitrator),
        None => factory,
    };
    let factory = factory.with_bindings(bindings);
    factory.attach_login_storage(&users);
    if let Some(runner) = &mut server_info_runner {
        let processes = factory.processes();
        let globals = users.global_vars();
        let opener = authority.transaction_opener();
        runner
            .start_min_start_ts_reporter(move || {
                use tidb_txnkv::pd_capability::{PdCapability, TimestampFutureWait};
                let mut active = tidb_txnkv::ACTIVE_START_TS.snapshot();
                active.extend(
                    processes
                        .snapshot()
                        .into_iter()
                        .map(|process| process.cur_txn_start_ts),
                );
                let internal = processes.internal_session_start_ts();
                let version =
                    TimestampFutureWait::wait(PdCapability::timestamp_future(opener.pd())?)?;
                let now = std::time::UNIX_EPOCH + Duration::from_millis(version >> 18);
                for ts in internal {
                    let _ = tidb_txnkv::print_long_time_internal_txn(now, ts, false);
                    active.push(ts);
                }
                let max_wait = globals
                    .get("tidb_gc_max_wait_time")
                    .map_err(|error| format!("{error:?}"))?
                    .parse::<u64>()
                    .map_err(|error| error.to_string())?;
                Ok(tidb_txnkv::report_min_start_ts(
                    version,
                    max_wait,
                    active,
                    &tidb_txnkv::GLOBAL_INNER_TXN_START_TS,
                ))
            })
            .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error)))?;
    }

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
    let stats_maintenance = super::stats_maintenance::StatsMaintenanceWorker::start(
        &factory,
        config.stats_lease,
        config.schema_lease,
        async_stats_loader.initialization(),
    )
    .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error)))?;
    factory.start_auto_analyze_worker(config.stats_lease);
    factory.start_analyze_jobs_cleanup_worker(config.stats_lease);
    factory.start_historical_stats_worker();
    // Go `Domain.Start` runs `requestUnitsWriterLoop` beside the other domain
    // loops (`pkg/domain/domain.go:830`). This node serves the null keyspace,
    // which the ResourceManager request names as Go's `NullKeyspaceID`.
    if let Some(pd) = authority.pd_client() {
        factory.start_ru_stats_writer(
            pd,
            Arc::new(authority.transaction_opener()),
            None,
            TRANSACTION_RPC_TIMEOUT,
        );
    }
    let workload_etcd = crate::real_tikv_node::connect_schema_notifier(&config);
    let workload_store = workload_etcd
        .as_ref()
        .map(|client| Arc::clone(client) as Arc<dyn tidb_workloadrepo::RepositoryStore>);
    let workload_owner_factory = workload_etcd.as_ref().map(|client| {
        let client = Arc::clone(client);
        let instance_id = server_info.local_server_info().static_info.id.clone();
        Arc::new(move |key: &str, prompt: &str| {
            Arc::new(tidb_owner::OwnerManager::new(
                tidb_owner::Context::background(),
                Arc::clone(&client) as Arc<dyn tidb_owner::OwnerStore>,
                prompt,
                instance_id.clone(),
                key,
            )) as Arc<dyn tidb_owner::Manager>
        }) as tidb_workloadrepo::OwnerFactory
    });
    let workload_repository = tidb_workloadrepo::Worker::new(
        workload_store,
        Some(Arc::new(super::WorkloadRepositorySessionPool::new(
            &factory,
        ))),
        workload_owner_factory,
        server_info.local_server_info().static_info.id.clone(),
    );
    let globals = users.global_vars();
    for (name, apply) in [
        (
            tidb_workloadrepo::REPOSITORY_SAMPLING_INTERVAL,
            tidb_workloadrepo::Worker::set_sampling_interval
                as fn(&tidb_workloadrepo::Worker, &str) -> Result<(), String>,
        ),
        (
            tidb_workloadrepo::REPOSITORY_SNAPSHOT_INTERVAL,
            tidb_workloadrepo::Worker::set_snapshot_interval,
        ),
        (
            tidb_workloadrepo::REPOSITORY_RETENTION_DAYS,
            tidb_workloadrepo::Worker::set_retention_days,
        ),
    ] {
        if let Ok(value) = globals.get(name) {
            let _ = apply(&workload_repository, &value);
        }
    }
    assert!(
        factory
            .set_workload_repository(Arc::clone(&workload_repository))
            .is_ok(),
        "workload repository is installed once"
    );
    if globals
        .get(tidb_workloadrepo::REPOSITORY_DEST)
        .is_ok_and(|value| value == "table")
        && workload_repository.start().is_err()
    {
        workload_repository.stop();
    }
    let skipped = render_skipped(factory.boot_skipped_tables());
    let stats_receipt = stats.receipt();

    run_with_process_shutdown(
        (
            // Retained through worker drain; the closure releases leases
            // after protected sessions and background storage work retire.
            server_id_keeper,
            server_info_runner,
            replica_read_checker,
            // Dropped beside it: once the registration is gone nobody waits
            // on this node, so the acknowledger has nothing left to say.
            schema_sync_ack,
            workload_repository,
            stats_maintenance,
            replica_poll,
            global_config_keeper,
            factory,
            reloader,
            privilege_watcher,
            privilege_reloader,
            sysvar_watcher,
            sysvar_reloader,
            stats_reloader,
            binding_reloader,
            async_stats_loader,
        ),
        authority,
        move |(
            server_id_keeper,
            server_info_runner,
            replica_read_checker,
            schema_sync_ack,
            workload_repository,
            stats_maintenance,
            replica_poll,
            global_config_keeper,
            factory,
            reloader,
            privilege_watcher,
            privilege_reloader,
            sysvar_watcher,
            sysvar_reloader,
            stats_reloader,
            binding_reloader,
            async_stats_loader,
        )| {
            let settings = factory.status_settings();
            let peer = Some(factory.peer_service());
            let status_catalog = Arc::clone(&factory);
            let node =
                ConcurrentSqlNode::bind(&config, factory, Arc::clone(&users)).map_err(|error| {
                    crate::real_tikv_node::emit_connections_startup_failure(&error);
                    RunConfiguredNodeError::Node(error)
                })?;
            let _status_server = start_cluster_status(
                &config,
                node.tracker(),
                Arc::new(move || status_catalog.catalog_snapshot()),
                settings,
                peer,
            )
            .map_err(|error| {
                RunConfiguredNodeError::Engine(SqlQueryError::unknown(error.to_string()))
            })?;
            let address = node.local_addr().map_err(|error| {
                crate::real_tikv_node::emit_connections_startup_failure(&error);
                RunConfiguredNodeError::Node(error)
            })?;
            let shutdown = node.shutdown_handle();
            ctrlc::set_handler(move || shutdown.shutdown()).map_err(|error| {
                crate::real_tikv_node::emit_connections_startup_failure(&error);
                RunConfiguredNodeError::Signal(error)
            })?;
            eprintln!(
            "{{\"event\":\"cluster_session_node_ready\",\"address\":\"{address}\",\"schema_version\":{schema_version},\"max_connections\":{},\"account_count\":{},\"skipped_tables\":[{skipped}],\"stats_loaded\":{},\"stats_pseudo\":{}}}",
            config.max_connections,
            users.len(),
            stats_receipt.loaded,
            stats_receipt.pseudo,
        );
            let outcome = node.run().map_err(RunConfiguredNodeError::Node);
            workload_repository.stop();
            drop(replica_read_checker);
            drop(stats_maintenance);
            drop(replica_poll);
            drop(global_config_keeper);
            // The reload threads hold their own transaction openers; joining
            // them here releases those PD handles before the authority's
            // shutdown drain.
            drop(reloader);
            drop(privilege_watcher);
            drop(privilege_reloader);
            drop(sysvar_watcher);
            drop(sysvar_reloader);
            drop(stats_reloader);
            drop(binding_reloader);
            drop(async_stats_loader);
            drop(workload_repository);
            // The status callback retains the factory. Its final drop joins
            // internal workers and flushes statistics while timestamp and
            // identity leases still protect those storage transactions.
            drop(_status_server);
            drop(server_id_keeper);
            drop(server_info_runner);
            outcome
        },
    )
}

/// Renders the boot-time refusals for the node's ready event.
fn render_skipped(skipped: &[SkippedTable]) -> String {
    skipped
        .iter()
        .map(|table| {
            format!(
                "{{\"table\":{:?},\"reason\":{:?}}}",
                table.name, table.reason
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn start_cluster_status(
    config: &NodeConfig,
    tracker: Arc<crate::sql_node::ConnectionTracker>,
    schema: crate::http_status::SchemaSource,
    settings: crate::http_settings::Settings,
    peer: Option<crate::peer_rpc::PeerService>,
) -> std::io::Result<Option<crate::http_status::StatusServer>> {
    // Go Server.Run starts status HTTP beside SQL when ReportStatus is set.
    // Publishing server info alone does not make that endpoint reachable.
    if !config.report_status {
        return Ok(None);
    }
    match crate::http_status::start_status_listener_with_routes(
        &config.status_host,
        config.status_port,
        tracker,
        tidb_mysql::runtime_versions().server_version,
        tidb_util::versioninfo::TIDB_GIT_HASH.to_owned(),
        crate::http_status::StatusRoutes {
            schema: Some(schema),
            settings: Some(settings),
            peer,
            security: config.cluster_security.clone(),
        },
    ) {
        Ok(server) => {
            eprintln!(
                "{{\"event\":\"status_listener_ready\",\"address\":\"{}\"}}",
                server.local_addr()
            );
            Ok(Some(server))
        }
        Err(error) => {
            eprintln!("{{\"event\":\"status_listener_error\",\"error\":\"{error}\"}}");
            Err(error)
        }
    }
}

#[cfg(test)]
mod status_tests {
    use super::*;
    use std::io::{Read, Write};

    fn test_settings() -> crate::http_settings::Settings {
        crate::http_settings::Settings::new(
            tidb_session::GlobalSysvars::new(),
            Arc::new(|_, _| Err("test has no storage".into())),
        )
    }

    #[test]
    fn cluster_status_respects_disabled_flag_and_bind_failure() {
        let mut config = NodeConfig::parse([
            "--store=tikv",
            "--path=127.0.0.1:2379",
            "--status=0",
            "--status-host=127.0.0.1",
            "--report-status=false",
        ])
        .unwrap();
        let tracker = Arc::new(crate::sql_node::ConnectionTracker::default());
        let schema: crate::http_status::SchemaSource =
            Arc::new(|| tidb_exec::cluster_catalog::ClusterCatalog {
                databases: Vec::new(),
                schema_version: 1,
            });
        assert!(
            start_cluster_status(
                &config,
                Arc::clone(&tracker),
                Arc::clone(&schema),
                test_settings(),
                None
            )
            .unwrap()
            .is_none()
        );
        let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        config.report_status = true;
        config.status_port = occupied.local_addr().unwrap().port();
        assert!(start_cluster_status(&config, tracker, schema, test_settings(), None).is_err());
    }

    #[test]
    fn cluster_status_serves_health_settings_and_metrics() {
        let config = NodeConfig::parse([
            "--store=tikv",
            "--path=127.0.0.1:2379",
            "--status=0",
            "--status-host=127.0.0.1",
        ])
        .unwrap();
        let tracker = Arc::new(crate::sql_node::ConnectionTracker::default());
        let schema = Arc::new(|| tidb_exec::cluster_catalog::ClusterCatalog {
            databases: Vec::new(),
            schema_version: 1,
        });
        let server = start_cluster_status(&config, tracker, schema, test_settings(), None)
            .unwrap()
            .expect("cluster startup must bind the advertised status service like Go");
        for path in ["/status", "/metrics", "/settings", "/config", "/schema"] {
            let mut stream = std::net::TcpStream::connect(server.local_addr()).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            write!(
                stream,
                "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
            )
            .unwrap();
            let mut response = String::new();
            match stream.read_to_string(&mut response) {
                Ok(_) => {}
                Err(error) => panic!("read {path} failed: {error}"),
            }
            assert!(response.starts_with("HTTP/1.1 200"), "{path}: {response}");
            if path == "/metrics" {
                // Go's /metrics exports `tidb_server_panic_total 0` from
                // registration alone; `tidb_server_connections` gains its
                // series only once a connection has existed, exactly as
                // Go's GaugeVec does.
                assert!(response.contains("tidb_monitor_time_jump_back_total"));
            }
        }
    }
}

struct ProcessPdRegionPolicy(tidb_pd_client::PdClient);
impl std::fmt::Debug for ProcessPdRegionPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessPdRegionPolicy")
            .finish_non_exhaustive()
    }
}
impl tidb_session::vars::PdRegionPolicy for ProcessPdRegionPolicy {
    fn set_follower_handle(&self, enabled: bool) {
        self.0.set_enable_follower_handle(enabled);
    }
    fn set_tso_follower_proxy(&self, enabled: bool) {
        self.0.set_enable_tso_follower_proxy(enabled);
    }
    fn set_tso_batch_wait(&self, wait: Duration) {
        self.0
            .set_max_tso_batch_wait_interval(wait)
            .expect("validated TSO batch wait");
    }
    fn set_tso_rpc_concurrency(&self, concurrency: isize) {
        self.0.set_tso_client_rpc_concurrency(concurrency);
    }
}
