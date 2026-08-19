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

use tidb_exec::cop_scan::CopScanSource;
use tidb_exec::real_tikv_catalog::load_catalog_from_cluster;
use tidb_exec::real_tikv_read::ProductionReadProcessAuthority;
use tidb_executor::remote_scan::PushdownScanner;
use tidb_planner::read_only_scan::{ConfiguredColumn, ConfiguredTable};

use crate::cluster_account_seam::RealClusterAccountWriter;
use crate::cluster_analyze_seam::RealClusterAnalyze;
use crate::cluster_session::SkippedTable;
use crate::cluster_sysvar_seam::{RealClusterSysvarWriter, SysvarPublicationFence};
use crate::node_config::NodeConfig;
use crate::real_tikv_node::{
    node_accounts, run_with_process_shutdown, spawn_catalog_reloader, spawn_privilege_watch,
    spawn_schema_version_watch, RunConfiguredNodeError,
};
use crate::sql_node::{ConcurrentSqlNode, SqlQueryError};

use super::{
    ClusterSessionFactory, RealClusterDdl, RealClusterTransactions, CONTROL_PLANE_TIMEOUT,
};

/// Starts the convergence node: wide SQL over cluster storage and cluster
/// accounts, served on the MySQL port.
pub fn run_cluster_session_node(config: NodeConfig) -> Result<(), RunConfiguredNodeError> {
    let spill_storage = crate::open_spill_storage(&config)?;
    let memory_arbitrator = crate::MemoryArbitratorAuthority::open(&config)?;
    run_cluster_session_node_with_spill(config, spill_storage, memory_arbitrator.arbitrator())
}

pub(crate) fn run_cluster_session_node_with_spill(
    config: NodeConfig,
    spill_storage: Arc<tidb_util::disk::SpillStorage>,
    memory_arbitrator: Option<Arc<tidb_util::memory::MemArbitrator>>,
) -> Result<(), RunConfiguredNodeError> {
    let mut loaded = None;
    let authority = ProductionReadProcessAuthority::connect_with_catalog(
        config.pd_endpoints.clone(),
        CONTROL_PLANE_TIMEOUT,
        |opener| {
            loaded = Some(
                load_catalog_from_cluster(opener, CONTROL_PLANE_TIMEOUT)
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
    let (stats, stats_reloader) = crate::real_tikv_node::spawn_node_stats(
        Arc::clone(&catalog),
        authority.transaction_opener(),
        config.schema_lease,
        CONTROL_PLANE_TIMEOUT,
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
    // The node's coprocessor: base-table scans now carry their predicate,
    // their row cap and their column list to the region, and only the
    // surviving rows come back. The session's own staged writes are merged on
    // top of them client-side, which is Go's `UnionScan` over a distsql
    // reader.
    let cop_scans: Arc<dyn PushdownScanner> =
        Arc::new(CopScanSource::new(authority.transport_factory()));
    // This node's identity in the cluster: `/tidb/server/info/<uuid>` under
    // a lease, plus the `/topology/tidb/<host:port>` pair, refreshed for as
    // long as the process lives -- Go's `Domain.Init` starting the
    // server-info syncer beside its reloaders. A node with no reachable
    // etcd still HAS the record; it just publishes nowhere, and
    // `information_schema.TIDB_SERVERS_INFO` then reports this node alone,
    // which is Go's `etcdCli == nil` answer.
    let server_info = Arc::new(tidb_domain::serverinfo_syncer::Syncer::new(
        crate::serverinfo_etcd::node_server_info(&config),
        crate::real_tikv_node::connect_schema_notifier(&config).map(|client| {
            Arc::new(crate::serverinfo_etcd::EtcdClientOps::new(client))
                as Arc<dyn tidb_domain::serverinfo_syncer::EtcdOps>
        }),
    ));
    let server_info_runner = match tidb_domain::serverinfo_syncer::SyncerRunner::start(
        Arc::clone(&server_info),
        tidb_domain::serverinfo_syncer::SyncIntervals::default(),
    ) {
        Ok(runner) => Some(runner),
        Err(error) => {
            // Go logs and carries on: a node that cannot publish itself
            // still serves SQL, it is just invisible to its peers.
            eprintln!(
                "{{\"event\":\"server_info_syncer_unavailable\",\"error\":{error:?}}}"
            );
            None
        }
    };
    let factory = ClusterSessionFactory::new(
        Arc::new(RealClusterTransactions::new(
            authority.transaction_opener(),
            CONTROL_PLANE_TIMEOUT,
        )),
        Arc::new(RealClusterDdl::new(
            authority.transaction_opener(),
            Arc::clone(&catalog),
            CONTROL_PLANE_TIMEOUT,
            crate::real_tikv_node::connect_schema_notifier(&config),
        )),
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
            CONTROL_PLANE_TIMEOUT,
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
    .with_server_info(server_info)
    .with_spill_storage(spill_storage);
    let factory = match memory_arbitrator {
        Some(arbitrator) => factory.with_mem_arbitrator(arbitrator),
        None => factory,
    };
    let factory = Arc::new(factory);
    let skipped = render_skipped(factory.boot_skipped_tables());
    let stats_receipt = stats.receipt();

    run_with_process_shutdown(
        (
            // Dropped FIRST: the runner removes this node's published
            // records before the etcd handles below it go away.
            server_info_runner,
            factory,
            watcher,
            reloader,
            privilege_watcher,
            privilege_reloader,
            sysvar_watcher,
            sysvar_reloader,
            stats_reloader,
        ),
        authority,
        move |(
            server_info_runner,
            factory,
            watcher,
            reloader,
            privilege_watcher,
            privilege_reloader,
            sysvar_watcher,
            sysvar_reloader,
            stats_reloader,
        )| {
            let node =
                ConcurrentSqlNode::bind(&config, factory, Arc::clone(&users)).map_err(|error| {
                    crate::real_tikv_node::emit_connections_startup_failure(&error);
                    RunConfiguredNodeError::Node(error)
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
