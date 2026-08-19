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
//! generic session factory the production node uses, and serves the same
//! listener. No PD is dialed, no etcd is watched, and no catalog is loaded:
//! the served table comes from the command line, which is why the
//! cluster-catalog flags are refused by name below.

use std::sync::Arc;
use std::time::Duration;

use tidb_distsql::cop_paging::DirectUnaryRuntimeConfig;
use tidb_distsql::DirectUnaryQueryTransport;
use tidb_exec::real_tikv_read::{
    ReadSessionAdmissionOwner, RealTiKvReadSessionOpener, RealTiKvSessionTransportFactory,
};
use tidb_txnkv::gc_state::TxnSafePointRefresher;
use tidb_txnkv::pd_capability::CapabilityTimestampSource;
use tidb_txnkv::region::RegionCache;
use tidb_txnkv::transaction::RealOptimisticTransactionOpener;
use tidb_txnkv::{SharedReadAuthority, SharedReadOpener};
use tidb_unistore::client::InProcessClient;
use tidb_unistore::region_loader::{InProcessRegionLoader, IN_PROCESS_CLUSTER_ID};
use tidb_unistore::tso::InProcessPd;

use crate::node_config::NodeConfig;
use crate::real_tikv_node::{
    configured_account_store, configured_table, served_table_descriptor, RealTiKvSessionFactory,
    RunConfiguredNodeError,
};
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
        DirectUnaryQueryTransport::from_read_authority(
            &self.read_opener,
            DirectUnaryRuntimeConfig {
                default_timeout: IN_PROCESS_TIMEOUT,
                ..DirectUnaryRuntimeConfig::default()
            },
            self.lock_timestamp_source.clone(),
        )
        .map_err(|error| error.to_string())
    }
}

/// The concrete factory instantiation the unistore node serves through --
/// same generic shape as production, embedded parameters throughout.
pub type UnistoreSessionFactory = RealTiKvSessionFactory<
    InProcessReadSessionFactory,
    CapabilityTimestampSource<InProcessPd>,
    InProcessClient,
    InProcessRegionLoader,
    InProcessPd,
>;

/// The embedded write stack: one store, its region plane, its TSO, and the
/// generic transaction opener over all three. Every unistore surface --
/// single-table and cluster-session alike -- derives from this one build.
type InProcessOpener =
    RealOptimisticTransactionOpener<InProcessClient, InProcessRegionLoader, InProcessPd>;

fn in_process_write_stack() -> Result<
    (
        SharedReadAuthority<InProcessClient, InProcessRegionLoader>,
        InProcessPd,
        InProcessOpener,
    ),
    SqlQueryError,
> {
    let client = InProcessClient::new();
    let pd = InProcessPd::new();
    let cache = RegionCache::new(InProcessRegionLoader);
    let read_authority = SharedReadAuthority::start(client, cache)
        .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
    // The embedded store never garbage-collects, so the read floor is a
    // static zero -- Go's unistore behavior for a store with no PD to ask.
    let gc_state = TxnSafePointRefresher::start_with_source(|| Ok(0))
        .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
    // The opener's default protocol -- classic two-phase only -- stands: the
    // embedded store names its async-commit/1PC refusal in prewrite, so the
    // node must not offer what the store will abort.
    let transaction_opener = RealOptimisticTransactionOpener::from_capabilities(
        read_authority.opener(),
        pd.clone(),
        IN_PROCESS_TIMEOUT,
        gc_state,
    )
    .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
    Ok((read_authority, pd, transaction_opener))
}

/// Builds the whole in-process node: store, region plane, TSO, transaction
/// opener, session factory. Fails closed on any flag that needs a cluster
/// catalog, naming the flag.
pub(crate) fn unistore_session_factory(
    config: &NodeConfig,
) -> Result<
    (
        UnistoreSessionFactory,
        SharedReadAuthority<InProcessClient, InProcessRegionLoader>,
        ReadSessionAdmissionOwner,
    ),
    SqlQueryError,
> {
    if !config.load_tables.is_empty() {
        return Err(SqlQueryError::unknown(
            "--store unistore serves command-line tables only; --load-table needs a cluster catalog",
        ));
    }
    if config.load_privileges {
        return Err(SqlQueryError::unknown(
            "--store unistore has no bootstrapped mysql.* to load; drop --load-privileges",
        ));
    }
    let table = match config.read_tables.as_slice() {
        [one] => configured_table(one),
        [] => {
            return Err(SqlQueryError::unknown(
                "--store unistore requires exactly one --read-table",
            ))
        }
        _ => {
            return Err(SqlQueryError::unknown(
                "multiple configured tables require the multi-relation dispatcher",
            ))
        }
    };

    let (read_authority, pd, transaction_opener) = in_process_write_stack()?;

    let transport_factory = InProcessReadSessionFactory {
        read_opener: read_authority.opener(),
        lock_timestamp_source: CapabilityTimestampSource(pd.clone()),
    };
    let (opener, admission) = RealTiKvReadSessionOpener::new_with_admission_owner(
        table,
        transport_factory,
        CapabilityTimestampSource(pd),
        IN_PROCESS_CLUSTER_ID,
    );
    let factory = RealTiKvSessionFactory::from_opener_parts(
        opener,
        transaction_opener,
        read_authority.authority_id(),
    );
    Ok((factory, read_authority, admission))
}

/// Runs the SQL node over the embedded store until shutdown.
///
/// Same listener, same session code, same flags as the production node;
/// only the store underneath differs, which is the entire point.
pub(crate) fn run_unistore_node(
    config: NodeConfig,
    spill_storage: Arc<tidb_util::disk::SpillStorage>,
    memory_arbitrator: Option<Arc<tidb_util::memory::MemArbitrator>>,
) -> Result<(), RunConfiguredNodeError> {
    let users = configured_account_store(&config)?;
    let users = Arc::new(users);
    let (factory, read_authority, admission) =
        unistore_session_factory(&config).map_err(RunConfiguredNodeError::Engine)?;
    let factory = factory.with_spill_storage(spill_storage);
    let factory = match memory_arbitrator {
        Some(arbitrator) => factory.with_mem_arbitrator(arbitrator),
        None => factory,
    };
    let factory = Arc::new(factory);
    let served_table = factory.served_table().clone();
    let node = ConcurrentSqlNode::bind(&config, factory, Arc::clone(&users))
        .map_err(RunConfiguredNodeError::Node)?;
    // Go starts the status HTTP server beside the SQL listener
    // (`cfg.Status.ReportStatus`, default true); `/status` is the first
    // thing `main_test.go` and every health probe reads. A failed bind
    // logs and continues, as Go's does.
    let _status_server = if config.report_status {
        match crate::http_status::start_status_listener(
            &config.status_host,
            config.status_port,
            node.tracker(),
            config.version_info.server_version.clone(),
            config.version_info.git_hash.clone(),
        ) {
            Ok(server) => {
                eprintln!(
                    "{{\"event\":\"status_listener_ready\",\"address\":\"{}\"}}",
                    server.local_addr()
                );
                Some(server)
            }
            Err(error) => {
                eprintln!(
                    "{{\"event\":\"status_listener_error\",\"error\":\"{error}\"}}"
                );
                None
            }
        }
    } else {
        None
    };
    let address = node.local_addr().map_err(RunConfiguredNodeError::Node)?;
    let shutdown_grace_ms = node.shutdown_grace_ms();
    let shutdown = node.shutdown_handle();
    let last_signal = crate::shutdown_signal::install(move || shutdown.shutdown())
        .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error.to_string())))?;
    let table_descriptors = served_table_descriptor(&served_table);
    eprintln!(
        "{{\"event\":\"sql_node_ready\",\"address\":\"{address}\",\"store\":\"unistore\",\"cluster_id\":{IN_PROCESS_CLUSTER_ID},\"tables\":[{table_descriptors}],\"max_connections\":{},\"account_count\":{},\"shutdown_grace_ms\":{shutdown_grace_ms}}}",
        config.max_connections,
        users.len(),
    );
    let result = node.run().map_err(RunConfiguredNodeError::Node);
    // The admission owner and read authority outlive every session by
    // construction; drop order alone ends the store with the node.
    drop(admission);
    drop(read_authority);
    finish_with_signal_code(result, &last_signal)
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
    spill_storage: Arc<tidb_util::disk::SpillStorage>,
    memory_arbitrator: Option<Arc<tidb_util::memory::MemArbitrator>>,
) -> Result<(), crate::real_tikv_node::RunConfiguredNodeError> {
    use crate::real_tikv_node::RunConfiguredNodeError;

    let users = configured_account_store(&config)?;
    let users = Arc::new(users);
    let stack = unistore_cluster_session_stack(&config, &users)?;
    let UnistoreClusterStack {
        factory,
        schema_version,
        stats,
        cop_source: _,
        _reloader: reloader,
        _sysvar_reloader: sysvar_reloader,
        _stats_reloader: stats_reloader,
        _read_authority: read_authority,
    } = stack;
    let factory = factory.with_spill_storage(spill_storage);
    let factory = match memory_arbitrator {
        Some(arbitrator) => factory.with_mem_arbitrator(arbitrator),
        None => factory,
    };
    let factory = Arc::new(factory);
    let stats_receipt = stats.receipt();

    let node = ConcurrentSqlNode::bind(&config, factory, Arc::clone(&users))
        .map_err(RunConfiguredNodeError::Node)?;
    // Go starts the status HTTP server beside the SQL listener
    // (`cfg.Status.ReportStatus`, default true); `/status` is the first
    // thing `main_test.go` and every health probe reads. A failed bind
    // logs and continues, as Go's does.
    let _status_server = if config.report_status {
        match crate::http_status::start_status_listener(
            &config.status_host,
            config.status_port,
            node.tracker(),
            config.version_info.server_version.clone(),
            config.version_info.git_hash.clone(),
        ) {
            Ok(server) => {
                eprintln!(
                    "{{\"event\":\"status_listener_ready\",\"address\":\"{}\"}}",
                    server.local_addr()
                );
                Some(server)
            }
            Err(error) => {
                eprintln!(
                    "{{\"event\":\"status_listener_error\",\"error\":\"{error}\"}}"
                );
                None
            }
        }
    } else {
        None
    };
    let address = node.local_addr().map_err(RunConfiguredNodeError::Node)?;
    let shutdown = node.shutdown_handle();
    let last_signal = crate::shutdown_signal::install(move || shutdown.shutdown())
        .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error.to_string())))?;
    eprintln!(
        "{{\"event\":\"cluster_session_node_ready\",\"address\":\"{address}\",\"store\":\"unistore\",\"schema_version\":{schema_version},\"max_connections\":{},\"account_count\":{},\"stats_loaded\":{},\"stats_pseudo\":{}}}",
        config.max_connections,
        users.len(),
        stats_receipt.loaded,
        stats_receipt.pseudo,
    );
    let result = node.run().map_err(RunConfiguredNodeError::Node);
    drop(reloader);
    drop(sysvar_reloader);
    drop(stats_reloader);
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
    pub(crate) factory: crate::cluster_session_node::ClusterSessionFactory,
    pub(crate) schema_version: i64,
    pub(crate) stats: Arc<tidb_exec::stats_watch::SharedStats>,
    /// The node's one coprocessor, kept concrete so a test can read the
    /// served/refused receipt the live proof reads. The run path serves it
    /// only through the factory's `dyn` handle above.
    #[allow(dead_code)]
    pub(crate) cop_source: Arc<tidb_exec::cop_scan::CopScanSource<InProcessReadSessionFactory>>,
    // Guards, dropped in declaration order: reload threads first, then the
    // store they read from.
    pub(crate) _reloader: tidb_exec::catalog_watch::CatalogReloader,
    pub(crate) _sysvar_reloader: crate::cluster_sysvar_seam::SysvarReloader,
    pub(crate) _stats_reloader: tidb_exec::stats_watch::StatsReloader,
    pub(crate) _read_authority: SharedReadAuthority<InProcessClient, InProcessRegionLoader>,
}

/// Builds the wide cluster-session factory over the embedded store:
/// bootstrap publish, account seeding, catalog/stats/sysvar followers, and
/// the in-process coprocessor.
pub(crate) fn unistore_cluster_session_stack(
    config: &crate::node_config::NodeConfig,
    users: &Arc<crate::ConfiguredUserStore>,
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

    let (catalog, reloader) =
        crate::real_tikv_node::spawn_catalog_reloader(startup, opener.clone(), config.schema_lease)
            .map_err(|error| engine(SqlQueryError::unknown(error.to_string())))?;
    let (stats, stats_reloader) = crate::real_tikv_node::spawn_node_stats(
        Arc::clone(&catalog),
        opener.clone(),
        config.schema_lease,
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
    let server_info = Arc::new(tidb_domain::serverinfo_syncer::Syncer::new(
        crate::serverinfo_etcd::node_server_info(config),
        None,
    ));
    let cop_source = Arc::new(tidb_exec::cop_scan::CopScanSource::new(transport_factory));
    let cop_scans: Arc<dyn tidb_executor::remote_scan::PushdownScanner> =
        Arc::clone(&cop_source) as _;

    let factory = ClusterSessionFactory::new(
        Arc::new(RealClusterTransactions::new(
            opener.clone(),
            IN_PROCESS_TIMEOUT,
        )),
        Arc::new(RealClusterDdl::new(
            opener.clone(),
            Arc::clone(&catalog),
            IN_PROCESS_TIMEOUT,
            // No etcd: schema changes announce themselves to nobody, and the
            // reload tick above is the only follower -- correct for one node.
            None,
        )),
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
    .with_server_info(server_info);

    Ok(UnistoreClusterStack {
        factory,
        schema_version,
        stats,
        cop_source,
        _reloader: reloader,
        _sysvar_reloader: sysvar_reloader,
        _stats_reloader: stats_reloader,
        _read_authority: read_authority,
    })
}
