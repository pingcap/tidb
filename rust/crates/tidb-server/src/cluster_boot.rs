//! Booting the convergence node: what is read from the cluster before the
//! first connection, and which reload threads outlive the boot.
//!
//! [`run_cluster_session_node`] is this node's `main`: it connects to PD,
//! reads the catalog, the accounts, the global variables and the statistics
//! out of the cluster, spawns the reloader thread and etcd watch that keep
//! each of them following a Go peer's changes, builds the
//! [`Domain`](tidb_session::domain::cluster_session_node::Domain) every connection
//! is opened from, and serves the MySQL port until shutdown. It mirrors the
//! bootstrap half of Go's `pkg/session/session.go` (`BootstrapSession` and
//! the `domain.Domain` reload loops it starts) rather than the statement
//! lifecycle, which stays in [`super`].
//!

//! Booting the convergence node's network front end over the TiKV cluster's
//! domain ([`tidb_session::domain::cluster_stack`]): the MySQL listener, the
//! status port and the shutdown signal, as Go's `cmd/tidb-server` starts
//! `server.NewServer` over the domain `createStoreDDLOwnerMgrAndDomain` built.

use std::sync::Arc;

use crate::cluster_stack::{cluster_session_stack, ClusterStack};
use tidb_domain_core::node_config::NodeConfig;
use crate::process_shutdown::run_with_process_shutdown;
use tidb_session::domain::real_tikv_node::RunConfiguredNodeError;

use crate::sql_node::ConcurrentSqlNode;
use tidb_store_driver_error::sql_query_error::SqlQueryError;

/// Starts the convergence node: wide SQL over cluster storage and cluster
/// accounts, served on the MySQL port.
pub fn run_cluster_session_node(config: NodeConfig) -> Result<(), RunConfiguredNodeError> {
    let spill_storage = crate::open_spill_storage(&config)?;
    let memory_arbitrator = crate::MemoryArbitratorAuthority::open(&config)?;
    run_cluster_session_node_with_spill(config, spill_storage, memory_arbitrator.arbitrator())
}

pub(crate) fn run_cluster_session_node_with_spill(
    config: NodeConfig,
    spill_storage: Arc<tidb_spill_storage::SpillStorage>,
    memory_arbitrator: Option<Arc<tidb_memory::MemArbitrator>>,
) -> Result<(), RunConfiguredNodeError> {
    // Go `setGlobalVars`, before the store and the domain are created.
    let global_vars = tidb_domain::global_sysvars::GlobalSysvars::new();
    crate::set_global_vars::set_global_vars(&config, &global_vars);
    let (mut stack, authority) =
        cluster_session_stack(&config, global_vars, spill_storage, memory_arbitrator)?;
    // Go `store/driver` -> `gcworker.NewGCWorker` + `Start`: the GC leader
    // lease, safe points, lock resolution and delete ranges on TiKV. It
    // retires with the stack's workers, right after the replica-read checker.
    let gc_worker = tidb_gcworker::node::start_gc_worker(
        &stack.factory,
        authority.tikv_store(),
        Arc::new(authority.transaction_opener()),
    )
    .map_err(|error| {
        RunConfiguredNodeError::Engine(SqlQueryError::unknown(format!(
            "start gc worker failed: {error}"
        )))
    })?;
    stack.workers.insert(1, Box::new(gc_worker));
    // Go `repository.SetupRepository(dom)` in the server's main; the worker
    // retires last of the workers, and `StopRepository` stops it first.
    let workload_repository = tidb_workloadrepo::node::setup_repository(&stack.factory);
    stack.workers.push(Box::new(Arc::clone(&workload_repository)));
    run_with_process_shutdown(stack, authority, move |mut stack: ClusterStack| {
        let factory = Arc::clone(&stack.factory);
        let users = Arc::clone(&stack.users);
        let settings = crate::node_status::status_settings(&factory);
        let peer = Some(crate::node_status::peer_service(&factory));
        let status_catalog = Arc::clone(&factory);
        let node =
            ConcurrentSqlNode::bind(&config, factory, Arc::clone(&users)).map_err(|error| {
                tidb_session::domain::real_tikv_node::emit_connections_startup_failure(&error);
                RunConfiguredNodeError::Node(Box::new(error))
            })?;
        let _status_server = start_cluster_status(
            &config,
            node.tracker(),
            Arc::new(move || status_catalog.catalog_snapshot()),
            settings,
            peer,
        )
        .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error.to_string())))?;
        let address = node.local_addr().map_err(|error| {
            tidb_session::domain::real_tikv_node::emit_connections_startup_failure(&error);
            RunConfiguredNodeError::Node(Box::new(error))
        })?;
        let shutdown = node.shutdown_handle();
        ctrlc::set_handler(move || shutdown.shutdown()).map_err(|error| {
            tidb_session::domain::real_tikv_node::emit_connections_startup_failure(&error);
            RunConfiguredNodeError::Signal(error)
        })?;
        // Not log content (Go's counterpart is the "server is running
        // MySQL protocol" entry logged at bind): rust/scripts wait for
        // this JSON readiness line on stderr and read its fields.
        eprintln!(
            "{{\"event\":\"cluster_session_node_ready\",\"address\":\"{address}\",\"schema_version\":{},\"max_connections\":{},\"account_count\":{},\"skipped_tables\":[{}],\"stats_loaded\":{},\"stats_pseudo\":{}}}",
            stack.schema_version,
            config.max_connections,
            users.len(),
            stack.skipped,
            stack.stats_receipt.loaded,
            stack.stats_receipt.pseudo,
        );
        let outcome = node.run().map_err(|error| RunConfiguredNodeError::Node(Box::new(error)));
        workload_repository.stop();
        // The reload threads hold their own transaction openers; joining
        // them here releases those PD handles before the authority's
        // shutdown drain.
        for worker in stack.workers.drain(..) {
            drop(worker);
        }
        // The status callback retains the factory. Its final drop joins
        // internal workers and flushes statistics while timestamp and
        // identity leases still protect those storage transactions.
        drop(_status_server);
        stack.stop_resource_groups();
        for lease in stack.leases.drain(..) {
            drop(lease);
        }
        outcome
    })
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
    crate::log_status_listen(&config.status_host, config.status_port);
    match crate::http_status::start_status_listener_with_routes(
        &config.status_host,
        config.status_port,
        tracker,
        tidb_mysql::runtime_versions().server_version,
        tidb_versioninfo::TIDB_GIT_HASH.to_owned(),
        crate::http_status::StatusRoutes {
            schema: Some(schema),
            settings: Some(settings),
            peer,
            security: config.cluster_security.clone(),
        },
    ) {
        Ok(server) => Ok(Some(server)),
        Err(error) => {
            crate::log_status_listen_failed(&error);
            Err(error)
        }
    }
}

#[cfg(test)]
mod status_tests {
    use super::*;
    use std::io::{Read, Write};
    use std::time::Duration;

    fn test_settings() -> crate::http_settings::Settings {
        crate::http_settings::Settings::new(
            tidb_domain::global_sysvars::GlobalSysvars::new(),
            Arc::new(|_, _| Err("test has no storage".into())),
        )
    }

    #[test]
    fn cluster_status_respects_disabled_flag_and_bind_failure() {
        let mut config = crate::parse_node_config([
            "--store=tikv",
            "--path=127.0.0.1:2379",
            "--status=0",
            "--status-host=127.0.0.1",
            "--report-status=false",
        ])
        .unwrap();
        let tracker = Arc::new(crate::sql_node::ConnectionTracker::default());
        let schema: crate::http_status::SchemaSource =
            Arc::new(|| tidb_infoschema::cluster_catalog::ClusterCatalog {
                resource_groups: Vec::new(),
                policies: Vec::new(),
                databases: Vec::new(),
                schema_version: 1,
            });
        assert!(start_cluster_status(
            &config,
            Arc::clone(&tracker),
            Arc::clone(&schema),
            test_settings(),
            None
        )
        .unwrap()
        .is_none());
        let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        config.report_status = true;
        config.status_port = occupied.local_addr().unwrap().port();
        assert!(start_cluster_status(&config, tracker, schema, test_settings(), None).is_err());
    }

    #[test]
    fn cluster_status_serves_health_settings_and_metrics() {
        let config = crate::parse_node_config([
            "--store=tikv",
            "--path=127.0.0.1:2379",
            "--status=0",
            "--status-host=127.0.0.1",
        ])
        .unwrap();
        let tracker = Arc::new(crate::sql_node::ConnectionTracker::default());
        let schema = Arc::new(|| tidb_infoschema::cluster_catalog::ClusterCatalog {
            resource_groups: Vec::new(),
            policies: Vec::new(),
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

