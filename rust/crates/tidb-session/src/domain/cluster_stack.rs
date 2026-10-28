//! Booting the convergence node: what is read from the cluster before the
//! first connection, and which reload threads outlive the boot.
//!
//! [`run_cluster_session_node`] is this node's `main`: it connects to PD,
//! reads the catalog, the accounts, the global variables and the statistics
//! out of the cluster, spawns the reloader thread and etcd watch that keep
//! each of them following a Go peer's changes, builds the
//! [`Domain`](crate::cluster_session_node::Domain) every connection
//! is opened from, and serves the MySQL port until shutdown. It mirrors the
//! bootstrap half of Go's `pkg/session/session.go` (`BootstrapSession` and
//! the `domain.Domain` reload loops it starts) rather than the statement
//! lifecycle, which stays in [`super`].
//!

//! The TiKV cluster's domain: what the node reads from the cluster before the
//! first connection, and which reload threads outlive the boot. This is Go's
//! `createStoreDDLOwnerMgrAndDomain` (`cmd/tidb-server/main.go`) over
//! `session.BootstrapSession` / `domain.NewDomain` + `Domain.Init` / `Start`:
//! the store, the etcd client, the catalog, statistics, privilege, system
//! variable and binding reloaders, the DDL owner, the GC worker, the
//! workload repository and the session factory. The server binds its
//! listeners over the [`ClusterStack`] it returns.

use crate::domain::cluster_session_node::DomainLoginPolicy as _;
use std::sync::Arc;
use std::time::Duration;

use tidb_ddl::ddl_systable::MinJobIdRefresher;
use tidb_cluster::real_tikv_catalog::load_catalog_from_cluster_at;
use tidb_cluster::real_tikv_read::ProductionReadProcessAuthority;

use crate::domain::cluster_account_seam::RealClusterAccountWriter;
use crate::domain::cluster_analyze_seam::RealClusterAnalyze;
use crate::domain::cluster_session::SkippedTable;
use crate::domain::cluster_stats_lock_seam::RealClusterStatsLock;
use crate::domain::cluster_sysvar_seam::{RealClusterSysvarWriter, SysvarPublicationFence};
use crate::domain::node_config::NodeConfig;
use crate::domain::real_tikv_node::{
    node_accounts, spawn_catalog_reloader, spawn_privilege_watch, RunConfiguredNodeError,
};

/// The per-request RPC deadline for the transaction tier's row reads and
/// writes. Deliberately NOT [`crate::cluster_session_node::CONTROL_PLANE_TIMEOUT`]: those five
/// seconds budget metadata round trips that answer in milliseconds, while a
/// data read can legitimately wait behind concurrent coprocessor scans on the
/// same store. Go budgets the same surface with `tikvGRPCTimeout`-scale
/// deadlines (tens of seconds), and a request queued past ITS deadline failed
/// with `timeout_ms: 0` -- the admission-time zero the old constant produced.
const TRANSACTION_RPC_TIMEOUT: Duration = Duration::from_secs(60);
use crate::domain::session_api::SqlQueryError;

use crate::domain::cluster_session_node::{
    Domain, RealClusterDdl, RealClusterTransactions, CONTROL_PLANE_TIMEOUT,
};

/// The coprocessor read deadline is a statement-runtime budget, not the
/// five-second boot/catalog control-plane deadline. TPC-H partial aggregates
/// legitimately spend more than five seconds in TiKV on a cold SF1 cache.
const COPROCESSOR_QUERY_TIMEOUT: Duration = Duration::from_secs(300);

/// The cluster node's domain, built by [`cluster_session_stack`]: the session
/// factory the listeners open sessions from, the store authority the process
/// closes last, and the background workers in the order they are retired.
pub struct ClusterStack {
    /// The session factory (Go `session.CreateSession` over the domain).
    pub factory: Arc<Domain>,
    /// The node's accounts.
    pub users: Arc<crate::domain::configured_user_store::ConfiguredUserStore>,
    /// The workload repository, stopped before the workers retire.
    pub workload_repository: Arc<tidb_workloadrepo::Worker>,
    /// The schema version the node booted at.
    pub schema_version: i64,
    /// The boot catalog's refusals, rendered for the ready event.
    pub skipped: String,
    /// The statistics the node booted with.
    pub stats_receipt: tidb_exec::stats_watch::StatsReceipt,
    /// The domain loops, retired in this order after the listeners drain:
    /// each reload thread holds its own PD handle, so each is joined before
    /// the authority's shutdown drain, and a watch is dropped before the
    /// reloader it nudges.
    pub workers: Vec<Box<dyn std::any::Any>>,
    /// The server-id and server-info leases, released after the workers and
    /// the resource-group controller, while nothing still writes under them.
    pub leases: Vec<Box<dyn std::any::Any>>,
    resource_groups: Option<(
        tidb_txnkv::driver::tikv_store::TikvStore,
        Arc<tikv_client::pd_resource_group::controller::ResourceGroupsController>,
    )>,
}

impl ClusterStack {
    /// Go `Domain.Close`'s resource-group half: the controller stops before
    /// the authority closes the store it runs on.
    pub fn stop_resource_groups(&mut self) {
        if let Some((store, controller)) = self.resource_groups.take() {
            stop_resource_groups_controller(&store, &controller);
        }
    }
}

/// Builds the TiKV cluster node's domain; see the module documentation. The
/// store authority comes back beside the stack: the process shuts it down
/// after every worker and the factory are gone.
pub fn cluster_session_stack(
    config: &NodeConfig,
    spill_storage: Arc<tidb_util::spill_storage::SpillStorage>,
    memory_arbitrator: Option<Arc<tidb_util::memory::MemArbitrator>>,
) -> Result<(ClusterStack, ProductionReadProcessAuthority), RunConfiguredNodeError> {
    let mut loaded = None;
    // Go `tikv.NewKVStore` in `driver.TiKVDriver.Open`: the node's one
    // client-rust store, which loads the txn safe point before serving.
    let tikv_store = tidb_txnkv::driver::tikv_store::TikvStore::connect_with_security(
        config.pd_endpoints.clone(),
        &config.cluster_security,
    )
    .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error)))?;
    // Go `InfoSyncer.initResourceManagerClient(pdCli)`, which
    // `domain.Init` runs before `BootstrapSession` reaches `bootstrap`: the
    // resource-group jobs -- the bootstrap's own `ALTER RESOURCE GROUP
    // default` among them -- write PD's resource manager through it.
    tidb_domain::resource_manager_client::init_resource_manager_client(Some(Arc::new(
        PdResourceManagerClient {
            store: tikv_store.clone(),
        },
    )
        as Arc<dyn tidb_domain::resource_manager_client::ResourceManagerClient>));
    // Go `initDomainSysVars` registering `setGlobalResourceControl` over the
    // driver's `EnableGlobalResourceControlFunc = tikv.EnableResourceControl`,
    // before any global variable loads: the sysvar turns client-rust's
    // resource-control dispatch on and off.
    *crate::vars::SET_GLOBAL_RESOURCE_CONTROL
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::new(|enable| {
        if enable {
            tikv_client::enable_resource_control();
        } else {
            tikv_client::disable_resource_control();
        }
    }));
    // Go `domain.NewDomain`'s `etcdClient`: the node's one etcd client, which
    // every owner, syncer, notifier and watch below shares.
    let etcd = crate::domain::real_tikv_node::connect_etcd_client(config);
    // The cluster's regions as `SPLIT`, `SHOW TABLE ... REGIONS`,
    // `TABLESAMPLE REGIONS()` and the pre-split after CREATE TABLE reach them.
    let region_store: Arc<dyn tidb_kv::region_map::RegionStore> =
        Arc::new(tidb_exec::tikv_region_store::TikvRegionStore::new(
            tikv_store.clone(),
            config.pd_endpoints.clone(),
        ));
    let authority = ProductionReadProcessAuthority::connect_with_store(
        COPROCESSOR_QUERY_TIMEOUT,
        tikv_store,
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
                let started = std::time::Instant::now();
                let (outcome, schema_version, bootstrap_splits) =
                    tidb_exec::bootstrap_publish::publish_bootstrap_with_splits(
                        opener,
                        COPROCESSOR_QUERY_TIMEOUT,
                    )
                    .map_err(|error| error.to_string())?;
                tidb_exec::bootstrap_publish::notify_committed_bootstrap(
                    &outcome,
                    schema_version,
                    None,
                )
                .map_err(|error| error.to_string())?;
                tidb_exec::bootstrap_publish::log_bootstrap_successful(started);
                // Go `EnableSplitTableRegion`: the bootstrap cut every system
                // table's region (re-cutting an existing boundary is a no-op).
                if tidb_config::config_tree::config::get_global_config().split_table {
                    tidb_exec::pre_split::split_bootstrap_tables(
                        region_store.as_ref(),
                        &bootstrap_splits,
                    );
                }
            }
            loaded = Some(
                load_catalog_from_cluster_at(opener, COPROCESSOR_QUERY_TIMEOUT)
                    .map_err(|error| error.to_string())?,
            );
            Ok(())
        },
    )
    .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error.to_string())))?;
    let (startup, startup_lease_grant_ts) = loaded.expect("the catalog closure ran exactly once");
    let schema_version = startup.schema_version;

    // `node_accounts` also hands back the privilege reloader (landed in
    // parallel); it must stay alive for the node's run and drop before the
    // authority's shutdown drain, like the catalog reloader below.
    let (users, privilege_reloader) = node_accounts(config, &authority)?;
    crate::domain::real_tikv_node::load_cluster_startup_variables(&users, &authority.transaction_opener())?;
    // Go `domain_sysvars.go`: the TSO and follower sysvars tune the store's
    // PD client (`store.GetPDClient().UpdateOption`).
    if let Some(options) = authority.tikv_store().store().pd_client().pd_options() {
        users
            .global_vars()
            .set_pd_region_policy(Arc::new(ProcessPdRegionPolicy(options)));
    }
    // The cluster-session path owns one process-wide sysvar reloader below.
    // Its persisted boot image was installed synchronously above, before this
    // reloader and, crucially, before bind. This is independent of
    // privilege-cache policy.
    let auto_id_etcd = etcd.clone().ok_or_else(|| {
        RunConfiguredNodeError::Engine(SqlQueryError::unknown(
            "cluster AutoID discovery requires its configured etcd client",
        ))
    })?;
    let auto_id_service = Arc::new(
        tidb_cluster::auto_id_client::AutoIdClient::new(
            Arc::new(tidb_cluster::auto_id_client::EtcdAutoIdLeader(auto_id_etcd)),
            config.cluster_security.clone(),
        )
        .map_err(|e| RunConfiguredNodeError::Engine(SqlQueryError::unknown(e.to_string())))?,
    );
    // One registry for the whole node, so every connection inserting into a
    // table allocates from the one range this node reserved -- Go's
    // per-`tidb-server` allocator, not a per-session one. The statistics
    // writers allocate their `mysql.*` rows from it too.
    let node_auto_ids: Arc<dyn tidb_cluster::table_from_meta::TableAutoIds> = Arc::new(
        crate::domain::cluster_auto_id_seam::ClusterTableAutoIds::new(
            authority.transaction_opener(),
            CONTROL_PLANE_TIMEOUT,
        )
        .with_service(auto_id_service.clone()),
    );
    let (catalog, reloader, schema_validator) = spawn_catalog_reloader(
        startup,
        startup_lease_grant_ts,
        authority.transaction_opener(),
        config.schema_lease,
        Arc::clone(&node_auto_ids),
    )
    .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error.to_string())))?;
    // Statistics always resolve targets from this published catalog, so the
    // reload loop follows DDL-added tables and changed column types instead of
    // freezing the boot image.
    let (stats, stats_reloader, async_stats_loader) = crate::domain::real_tikv_node::spawn_node_stats(
        Arc::clone(&catalog),
        authority.transaction_opener(),
        config.stats_lease,
        COPROCESSOR_QUERY_TIMEOUT,
    )
    .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error.to_string())))?;
    // The account half of the same division: the reloader's tick is what makes
    // a peer's `GRANT` reach this node at all, and this watch is what makes it
    // arrive in a round trip instead of an interval.
    let privilege_watcher = spawn_privilege_watch(etcd.as_deref(), privilege_reloader.as_ref());
    // The sysvar half of the same division: a Go peer's `SET GLOBAL` is
    // durable in `mysql.global_variables` the instant it commits, and this
    // reloader is what makes THIS node notice it -- one tick, or one round
    // trip if the etcd watch fires first. The fallback is capped at Go's
    // 30-second `LoadSysVarCacheLoop` interval even with a longer lease.
    let sysvar_publication_fence = SysvarPublicationFence::default();
    let sysvar_reloader = crate::domain::cluster_sysvar_seam::SysvarReloader::spawn(
        users.global_vars(),
        authority.transaction_opener(),
        crate::domain::real_tikv_node::sysvar_reload_interval(config.schema_lease),
        CONTROL_PLANE_TIMEOUT,
        sysvar_publication_fence.clone(),
    )
    .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error.to_string())))?;
    let sysvar_watcher =
        crate::domain::real_tikv_node::spawn_sysvar_watch(etcd.as_deref(), Some(&sysvar_reloader));
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
    let mpp_client: Arc<dyn tidb_txnkv::MppClient> = {
        let catalog = Arc::clone(&catalog);
        Arc::new(crate::domain::cluster_session_node::build_mpp_client(
            &authority,
            move || catalog.load().schema_version,
        ))
    };
    // This node's identity in the cluster: `/tidb/server/info/<uuid>` under
    // a lease, plus the `/topology/tidb/<host:port>` pair, refreshed for as
    // long as the process lives -- Go's `Domain.Init` starting the
    // server-info syncer beside its reloaders. A node with no reachable
    // etcd still HAS the record; it just publishes nowhere, and
    // `information_schema.TIDB_SERVERS_INFO` then reports this node alone,
    // which is Go's `etcdCli == nil` answer.
    let server_etcd = etcd.clone().map(|client| {
        Arc::new(crate::domain::serverinfo_etcd::EtcdClientOps::new(client))
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
    let mut info = crate::domain::serverinfo_etcd::node_server_info(config);
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
    // Go `initResourceGroupsController`, after the server ID is fetched.
    let resource_groups_controller = start_resource_groups_controller(
        authority.tikv_store(),
        server_identity.as_ref().map_or(0, |identity| identity.id()),
    )?;
    let discovery = Some(tidb_exec::cluster_discovery::PdClusterDiscovery::new(
        authority.tikv_store().clone(),
        &config.cluster_security,
        COPROCESSOR_QUERY_TIMEOUT,
    ))
    .map(|discovery| {
        discovery.map(|discovery| {
            Arc::new(discovery) as Arc<dyn tidb_domain::cluster_topology::ClusterDiscovery>
        })
    })
    .transpose()
    .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error)))?;
    let cluster_topology = Arc::new(tidb_domain::cluster_topology::ClusterTopology::new(
        Arc::clone(&server_info),
        discovery,
    ));
    let replica_read_checker = crate::domain::cluster_topology::ReplicaReadChecker::start(
        Arc::clone(&cluster_topology),
        users.global_vars(),
    )
    .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error.to_string())))?;
    let stats_owner: Arc<dyn tidb_owner::Manager> = match etcd.clone() {
        Some(client) => Arc::new(tidb_owner::OwnerManager::new(
            tidb_owner::Context::background(),
            client as Arc<dyn tidb_owner::OwnerStore>,
            crate::domain::cluster_session_node::STATS_OWNER_PROMPT,
            server_info.local_server_info().static_info.id,
            crate::domain::cluster_session_node::STATS_OWNER_KEY,
        )),
        None => Arc::new(tidb_owner::MockManager::new(
            tidb_owner::Context::background(),
            server_info.local_server_info().static_info.id,
            // Match the store-scoped mock DDL owner and Go store.UUID().
            Some(&format!(
                "embedded-authority-{}",
                authority.transaction_opener().authority_id()
            )),
            crate::domain::cluster_session_node::STATS_OWNER_KEY,
        )),
    };
    let (bindings, binding_reloader) = crate::domain::cluster_binding_seam::start_binding_cache(
        authority.transaction_opener(),
        Arc::clone(&catalog),
        users.global_vars(),
        CONTROL_PLANE_TIMEOUT,
        match etcd.clone() {
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
    let schema_pins = Arc::new(crate::domain::cluster_session_node::schema_sync::SchemaPinRegistry::default());
    let min_job_id_refresher = Arc::new(MinJobIdRefresher::new());
    let schema_sync_ack = match etcd.clone() {
        Some(etcd) => Some(
            crate::domain::cluster_session_node::schema_sync::SchemaSyncAck::spawn(
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
                crate::domain::cluster_session_node::schema_sync::MDL_CHECK_LOOK_DURATION,
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
        .map(crate::domain::cluster_session_node::schema_sync::SchemaSyncAck::syncer);
    let cluster_ddl = Arc::new(
        RealClusterDdl::new_with_min_job_id_refresher(
            authority.transaction_opener(),
            Arc::clone(&catalog),
            CONTROL_PLANE_TIMEOUT,
            etcd.clone(),
            Arc::clone(&server_info),
            schema_version_syncer,
            Arc::clone(&schema_validator),
            config.run_ddl,
            min_job_id_refresher,
            Arc::new(tidb_exec::ddl_session_pool::ExecutorDdlSessionPool),
            Arc::clone(&node_auto_ids),
        )
        .map_err(|error| {
            RunConfiguredNodeError::Engine(SqlQueryError::unknown(format!(
                "campaign DDL owner failed: {error}"
            )))
        })?
        .with_auto_ids(auto_id_service.clone()),
    );
    // Go `GlobalInfoSyncerInit(..., store.GetPDHTTPClient(), ...)`: the
    // info syncer's managers share the store's PD HTTP client.
    let pd_http_client = authority.tikv_store().pd_http_client();
    // Go `InfoSyncer` init's `affinity.InitManager(is.pdHTTPCli)`: the DDL
    // job workers create and delete PD affinity groups through it.
    tidb_domain::affinity::init_manager(pd_http_client.as_ref().map(|client| {
        Arc::new(tidb_exec::pd_affinity_client::PdHttpAffinityClient::new(
            client,
        )) as Arc<dyn tidb_domain::affinity::PdAffinityClient>
    }));
    // Go `InfoSyncer.initLabelRuleManager`: PD's region-label rules, which
    // the DDL job workers write and `information_schema.attributes` reads.
    tidb_domain::label_manager::init_label_rule_manager(pd_http_client.as_ref().map(|client| {
        Arc::new(tidb_exec::label_delivery::PdLabelManager::new(client))
            as Arc<dyn tidb_domain::label_manager::LabelRuleManager>
    }));
    // One DDL-gated worker. Its guard joins before the node releases DDL/PD.
    let replica_poll = crate::domain::cluster_session_node::build_tiflash_replica_poll(
        Arc::clone(&catalog),
        pd_http_client,
        &config.cluster_security,
        cluster_ddl.clone(),
    );
    let global_config_keeper =
        crate::domain::global_config_sync::GlobalConfigKeeper::start(Some(authority.tikv_store().clone()))
            .map_err(|error| {
                RunConfiguredNodeError::Engine(SqlQueryError::unknown(error.to_string()))
            })?;
    let factory = Domain::new(
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
            etcd.clone(),
        )),
        Arc::new(RealClusterSysvarWriter::new(
            Arc::new(authority.transaction_opener()),
            users.global_vars(),
            CONTROL_PLANE_TIMEOUT,
            etcd.clone(),
            sysvar_publication_fence,
        )),
        Arc::new(
            RealClusterAnalyze::new(
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
                Arc::clone(&node_auto_ids),
            ),
        ),
        Arc::new(RealClusterStatsLock::new(
            Arc::new(authority.transaction_opener()),
            COPROCESSOR_QUERY_TIMEOUT,
        )),
        catalog,
        users.accounts(),
        users.global_vars(),
        Arc::clone(&stats),
        Arc::clone(&node_auto_ids),
        crate::domain::cluster_session_node::open_pool_session,
    )
    .with_global_config_syncer(global_config_keeper.syncer())
    .with_mpp_client(mpp_client)
    .with_kv_client(Arc::new(tidb_exec::cop_client::CopClient::new(
        authority.transport_factory(),
    )))
    .with_region_store(region_store)
    .with_server_info(Arc::clone(&server_info))
    .with_connection_ids(crate::domain::session_api::new_connection_id_allocator(
        &config.global_config,
        server_identity.clone(),
    ))
    .with_server_identity(server_identity)
    .with_cluster_topology(cluster_topology)
    .with_cluster_peer_client(Arc::new(tidb_exec::cluster_peer::ClusterPeerClient::new(
        authority.tikv_store().clone(),
    )))
    .with_cluster_config_client(Arc::new(
        tidb_exec::cluster_config::ClusterConfigClient::new(&config.cluster_security)
            .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error)))?,
    ))
    .with_cluster_diagnostics_client(Arc::new(
        tidb_exec::cluster_diagnostics::ClusterDiagnosticsClient::new(&config.cluster_security)
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
                let mut active = tidb_txnkv::ACTIVE_START_TS.snapshot();
                active.extend(
                    processes
                        .snapshot()
                        .into_iter()
                        .map(|process| process.cur_txn_start_ts),
                );
                let internal = processes.internal_session_start_ts();
                // Go `ReportMinStartTS`: `store.CurrentVersion(kv.GlobalTxnScope)`.
                let version = opener.store().get_timestamp_async().wait()?;
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
    let stats_maintenance = crate::domain::cluster_session_node::stats_maintenance::StatsMaintenanceWorker::start(
        &factory,
        config.stats_lease,
        config.schema_lease,
        async_stats_loader.initialization(),
    )
    .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error)))?;
    factory.start_auto_analyze_worker(config.stats_lease);
    factory.start_analyze_jobs_cleanup_worker(config.stats_lease);
    factory.start_historical_stats_worker();
    factory.load_signing_cert_loop(
        &config.global_config.security.session_token_signing_cert,
        &config.global_config.security.session_token_signing_key,
    );
    // Go `Domain.Start` runs `requestUnitsWriterLoop` beside the other domain
    // loops (`pkg/domain/domain.go:830`). This node serves the null keyspace,
    // which the ResourceManager request names as Go's `NullKeyspaceID`.
    factory.install_bdr_role_store(
        Arc::new(authority.transaction_opener()),
        TRANSACTION_RPC_TIMEOUT,
    );
    factory.start_ru_stats_writer(
        authority.tikv_store().clone(),
        Arc::new(authority.transaction_opener()),
        TRANSACTION_RPC_TIMEOUT,
    );
    // Go `Domain.Start` -> `gcworker.NewGCWorker` + `Start`: the GC leader
    // lease, safe points, lock resolution and delete ranges on TiKV.
    let gc_worker = Some(crate::domain::cluster_session_node::gc_worker::start_gc_worker(
        &factory,
        authority.tikv_store(),
        Arc::new(authority.transaction_opener()),
    ))
    .transpose()
    .map_err(|error| {
        RunConfiguredNodeError::Engine(SqlQueryError::unknown(format!(
            "start gc worker failed: {error}"
        )))
    })?;
    let workload_etcd = etcd.clone();
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
        Some(Arc::new(crate::domain::cluster_session_node::WorkloadRepositorySessionPool::new(
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
    // The controller stops in the closure, before the authority closes the
    // store it runs on.
    let tikv_store_for_shutdown = authority.tikv_store().clone();

    Ok((ClusterStack {
        factory,
        users,
        workload_repository: Arc::clone(&workload_repository),
        schema_version,
        skipped,
        stats_receipt,
        workers: vec![
            Box::new(replica_read_checker),
            Box::new(gc_worker),
            Box::new(stats_maintenance),
            Box::new(replica_poll),
            Box::new(global_config_keeper),
            Box::new(reloader),
            Box::new(privilege_watcher),
            Box::new(privilege_reloader),
            Box::new(sysvar_watcher),
            Box::new(sysvar_reloader),
            Box::new(stats_reloader),
            Box::new(binding_reloader),
            Box::new(async_stats_loader),
            Box::new(workload_repository),
        ],
        // Dropped last: once the registration is gone nobody waits on this
        // node, so the acknowledger has nothing left to say.
        leases: vec![
            Box::new(server_id_keeper),
            Box::new(server_info_runner),
            Box::new(schema_sync_ack),
        ],
        resource_groups: Some((tikv_store_for_shutdown, resource_groups_controller)),
    }, authority))
}

/// Go `runaway.MaxWaitDuration`: the longest a request waits for its
/// resource group's tokens.
const RESOURCE_GROUP_MAX_WAIT_DURATION: Duration = Duration::from_secs(30);

/// Go `Domain.initResourceGroupsController`: the node's resource-group
/// controller over the store's PD client (`rmclient.NewResourceGroupController`
/// with the server ID as its client ID and the store's keyspace -- none, so
/// `NullKeyspaceID`), started and installed as client-rust's resource-control
/// interceptor (`tikv.SetResourceControlInterceptor`).
pub fn start_resource_groups_controller(
    store: &tidb_txnkv::driver::tikv_store::TikvStore,
    server_id: u64,
) -> Result<
    Arc<tikv_client::pd_resource_group::controller::ResourceGroupsController>,
    RunConfiguredNodeError,
> {
    start_resource_groups_controller_over(store, server_id, store.store().pd_client())
}

/// [`start_resource_groups_controller`] over any store and resource-manager
/// provider: Go takes the provider from `store.GetPDClient()`, which for
/// unistore is its mock `pdClient`.
pub fn start_resource_groups_controller_over<PdC: tikv_client::PdClient>(
    store: &tidb_txnkv::driver::tikv_store::TikvStore<PdC>,
    server_id: u64,
    provider: Arc<dyn tikv_client::pd_resource_group::controller::ResourceGroupProvider>,
) -> Result<
    Arc<tikv_client::pd_resource_group::controller::ResourceGroupsController>,
    RunConfiguredNodeError,
> {
    use tikv_client::pd_resource_group::controller as rmclient;
    let controller = store
        .block_on(rmclient::new_resource_group_controller(
            server_id,
            provider,
            None,
            tikv_client::tikv::NULLSPACE_ID,
            vec![rmclient::with_max_wait_duration(
                RESOURCE_GROUP_MAX_WAIT_DURATION,
            )],
        ))
        .map_err(|error| {
            RunConfiguredNodeError::Engine(SqlQueryError::unknown(error.to_string()))
        })?;
    {
        let _runtime = store.runtime().enter();
        controller.start();
    }
    tikv_client::set_resource_control_interceptor(controller.clone());
    // Go `SetupTopProfiling(..., dom)` binds the domain as the RU-version
    // provider; its `GetRUVersion` asks this controller.
    tidb_util::topsql_stmtstats::bind_ru_version_provider(Some(Arc::new(DomainRuVersion(
        Arc::clone(&controller),
    ))));
    Ok(controller)
}

/// Go `Domain.GetRUVersion`: the resource-groups controller's RU version.
struct DomainRuVersion(Arc<tikv_client::pd_resource_group::controller::ResourceGroupsController>);

impl tidb_util::topsql_stmtstats::RuVersionProvider for DomainRuVersion {
    fn get_ru_version(&self) -> tidb_util::topsql_stmtstats::RuVersion {
        tidb_util::topsql_stmtstats::RuVersion(self.0.get_ru_version())
    }
}

/// Go `Domain.Close`'s controller teardown: the interceptor is removed and
/// the controller's loop stopped.
pub fn stop_resource_groups_controller<PdC: tikv_client::PdClient>(
    store: &tidb_txnkv::driver::tikv_store::TikvStore<PdC>,
    controller: &Arc<tikv_client::pd_resource_group::controller::ResourceGroupsController>,
) {
    tikv_client::unset_resource_control_interceptor();
    // Go `topsql.Close`: `BindRUVersionProvider(nil)`.
    tidb_util::topsql_stmtstats::bind_ru_version_provider(None);
    // Go cancels the controller's context and logs nothing.
    let _ = store.block_on(controller.stop());
}

/// Renders the boot-time refusals for the node's ready event.
pub fn render_skipped(skipped: &[SkippedTable]) -> String {
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

/// Go `store.GetPDClient().UpdateOption` for the TSO and follower sysvars.
struct ProcessPdRegionPolicy(Arc<tikv_client::pd_options::Options>);
impl std::fmt::Debug for ProcessPdRegionPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessPdRegionPolicy")
            .finish_non_exhaustive()
    }
}
impl crate::vars::PdRegionPolicy for ProcessPdRegionPolicy {
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

/// PD's `ResourceManagerClient` through the store's PD client, for the
/// keyspace the store serves.
struct PdResourceManagerClient {
    store: tidb_txnkv::driver::tikv_store::TikvStore,
}

impl tidb_domain::resource_manager_client::ResourceManagerClient for PdResourceManagerClient {
    fn add_resource_group(
        &self,
        group: tidb_proto::resource_manager::ResourceGroup,
    ) -> Result<String, String> {
        let pd = self.store.store().pd_client();
        self.store
            .block_on(async move { pd.add_resource_group(group).await })
            .map_err(|error| error.to_string())
    }

    fn modify_resource_group(
        &self,
        group: tidb_proto::resource_manager::ResourceGroup,
    ) -> Result<String, String> {
        let pd = self.store.store().pd_client();
        self.store
            .block_on(async move { pd.modify_resource_group(group).await })
            .map_err(|error| error.to_string())
    }

    fn delete_resource_group(&self, name: &str) -> Result<String, String> {
        let pd = self.store.store().pd_client();
        let name = name.to_owned();
        self.store
            .block_on(async move { pd.delete_resource_group(&name).await })
            .map_err(|error| error.to_string())
    }
}
