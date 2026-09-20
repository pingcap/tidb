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
    spawn_schema_version_watch, RunConfiguredNodeError,
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
    let authority = ProductionReadProcessAuthority::connect_with_catalog(
        config.pd_endpoints.clone(),
        COPROCESSOR_QUERY_TIMEOUT,
        |opener| {
            // tiup's deploy->patch->start flow boots this node against a
            // FRESH keyspace before any Go TiDB ever ran, so `mysql.*` does
            // not exist yet. The convergence node owns the cluster's system
            // catalog from then on, so bootstrap it right here -- the same
            // transaction path the mysql-bootstrap tool uses -- and let the
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
    // The cluster-session path owns one process-wide sysvar reloader below.
    // Its persisted boot image was installed synchronously above, before this
    // reloader and, crucially, before bind. This is independent of
    // privilege-cache policy.
    let (catalog, reloader) =
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
    // The watch only makes the reload *prompt*; the tick above is what makes
    // it correct. It is listed before the reloader in the tuple below so it is
    // dropped first: a watch may not outlive the thread it nudges.
    let watcher = spawn_schema_version_watch(&config, &reloader);
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
    let (bindings, binding_reloader) = crate::cluster_binding_seam::start_binding_cache(
        authority.transaction_opener(),
        Arc::clone(&catalog),
        users.global_vars(),
        CONTROL_PLANE_TIMEOUT,
    )
    .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error)))?;
    // The node's coprocessor: base-table scans now carry their predicate,
    // their row cap and their column list to the region, and only the
    // surviving rows come back. The session's own staged writes are merged on
    // top of them client-side, which is Go's `UnionScan` over a distsql
    // reader.
    //
    // The TiFlash MPP lowering shares the node's PD seeds: one client for
    // the store listing (`engine=tiflash`) and the record-range region scan
    // the dispatch needs (Go's `MPPClient.ConstructMPPTasks`).
    let cop_scans: Arc<dyn PushdownScanner> = {
        let catalog = Arc::clone(&catalog);
        let tiflash_mpp = crate::cluster_session_node::build_tiflash_mpp_source(
            &config.pd_endpoints,
            move || catalog.load().schema_version,
        );
        match tiflash_mpp {
            Some(source) => Arc::new(
                CopScanSource::new(authority.transport_factory()).with_tiflash_mpp(source),
            ),
            None => Arc::new(CopScanSource::new(authority.transport_factory())),
        }
    };
    // The replica-availability poller: Go `PollTiFlashRoutine`
    // (ddl_tiflash_api.go:638) flipped onto this node's own catalog and
    // transaction authority. The handle is forgotten, not stored: the poller
    // lives for the process lifetime and the node's own exit ends it.
    let replica_poll = crate::cluster_session_node::build_tiflash_replica_poll(
        authority.transaction_opener(),
        Arc::clone(&catalog),
        &config.pd_endpoints,
    );
    std::mem::forget(replica_poll);
    // The replica-availability poller: Go `PollTiFlashRoutine`
    // (ddl_tiflash_api.go:638) flipped onto this node's own catalog and
    // transaction authority.
    let replica_poll = crate::cluster_session_node::build_tiflash_replica_poll(
        authority.transaction_opener(),
        Arc::clone(&catalog),
        &config.pd_endpoints,
    );
    // A detached poller lives for the process lifetime; the handle dropping
    // merely detaches it, which is the intended shutdown story.
    std::mem::forget(replica_poll);
    // This node's identity in the cluster: `/tidb/server/info/<uuid>` under
    // a lease, plus the `/topology/tidb/<host:port>` pair, refreshed for as
    // long as the process lives -- Go's `Domain.Init` starting the
    // server-info syncer beside its reloaders. A node with no reachable
    // etcd still HAS the record; it just publishes nowhere, and
    // `information_schema.TIDB_SERVERS_INFO` then reports this node alone,
    // which is Go's `etcdCli == nil` answer.
    let server_info = Arc::new(
        tidb_domain::serverinfo_syncer::Syncer::new_with_status_endpoint_claim(
            crate::serverinfo_etcd::node_server_info(&config),
            crate::real_tikv_node::connect_schema_notifier(&config).map(|client| {
                Arc::new(crate::serverinfo_etcd::EtcdClientOps::new(client))
                    as Arc<dyn tidb_domain::serverinfo_syncer::EtcdOps>
            }),
            config.report_status,
        ),
    );
    let server_info_runner = match tidb_domain::serverinfo_syncer::SyncerRunner::start(
        Arc::clone(&server_info),
        tidb_domain::serverinfo_syncer::SyncIntervals::default(),
    ) {
        Ok(runner) => Some(runner),
        Err(error) => {
            // Go logs and carries on: a node that cannot publish itself
            // still serves SQL, it is just invisible to its peers.
            eprintln!("{{\"event\":\"server_info_syncer_unavailable\",\"error\":{error:?}}}");
            None
        }
    };
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
    // The other half of being registered: a node the owner can SEE must also
    // ANSWER. Go's `WaitVersionSynced` waits on every `/tidb/server/info`
    // entry, so this acknowledger is spawned exactly when the registration
    // above is -- a node with no reachable etcd registers nowhere, is waited
    // on by nobody, and correctly spawns no acknowledger either.
    let schema_pins = Arc::new(super::schema_sync::SchemaPinRegistry::default());
    let schema_sync_ack = match crate::real_tikv_node::connect_schema_notifier(&config) {
        Some(etcd) => Some(
            super::schema_sync::SchemaSyncAck::spawn(
                Arc::clone(&catalog),
                authority.transaction_opener(),
                Arc::clone(&schema_pins),
                etcd,
                server_info.local_server_info().static_info.id,
                Arc::clone(&server_info),
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
    let cluster_ddl = Arc::new(
        RealClusterDdl::new(
            authority.transaction_opener(),
            Arc::clone(&catalog),
            CONTROL_PLANE_TIMEOUT,
            crate::real_tikv_node::connect_schema_notifier(&config),
            Arc::clone(&server_info),
            schema_version_syncer,
            config.run_ddl,
        )
        .map_err(|error| {
            RunConfiguredNodeError::Engine(SqlQueryError::unknown(format!(
                "campaign DDL owner failed: {error}"
            )))
        })?,
    );
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
        Arc::new(crate::cluster_auto_id_seam::ClusterTableAutoIds::new(
            authority.transaction_opener(),
            CONTROL_PLANE_TIMEOUT,
        )),
    )
    .with_cop_scans(cop_scans)
    .with_server_info(Arc::clone(&server_info))
    .with_stats_owner(stats_owner)
    .with_schema_pins(schema_pins)
    .with_spill_storage(spill_storage);
    let factory = match memory_arbitrator {
        Some(arbitrator) => factory.with_mem_arbitrator(arbitrator),
        None => factory,
    };
    let factory = factory.with_bindings(bindings);
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
    factory.start_auto_analyze_worker(
        config.stats_lease,
        tidb_config::config_tree::config::get_global_config()
            .performance
            .run_auto_analyze,
    );
    factory.start_analyze_jobs_cleanup_worker(config.stats_lease);
    factory.start_historical_stats_worker();
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
            // Dropped FIRST: the runner removes this node's published
            // records before the etcd handles below it go away.
            server_info_runner,
            // Dropped beside it: once the registration is gone nobody waits
            // on this node, so the acknowledger has nothing left to say.
            schema_sync_ack,
            workload_repository,
            factory,
            watcher,
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
            server_info_runner,
            schema_sync_ack,
            workload_repository,
            factory,
            watcher,
            reloader,
            privilege_watcher,
            privilege_reloader,
            sysvar_watcher,
            sysvar_reloader,
            stats_reloader,
            binding_reloader,
            async_stats_loader,
        )| {
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
            // The reload threads hold their own transaction openers; joining
            // them here releases those PD handles before the authority's
            // shutdown drain. The watch goes first: it nudges the reloader,
            // so it must not outlive it.
            drop(watcher);
            drop(reloader);
            drop(privilege_watcher);
            drop(privilege_reloader);
            drop(sysvar_watcher);
            drop(sysvar_reloader);
            drop(stats_reloader);
            drop(binding_reloader);
            drop(async_stats_loader);
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
            settings_json: Some(config.startup_config_json()),
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
            start_cluster_status(&config, Arc::clone(&tracker), Arc::clone(&schema))
                .unwrap()
                .is_none()
        );
        let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        config.report_status = true;
        config.status_port = occupied.local_addr().unwrap().port();
        assert!(start_cluster_status(&config, tracker, schema).is_err());
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
        let server = start_cluster_status(&config, tracker, schema)
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
