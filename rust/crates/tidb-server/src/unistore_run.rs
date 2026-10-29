// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The `--store unistore` run path: the network front end over the embedded
//! store's domain (`tidb_session::domain::unistore_node`), as Go's
//! `cmd/tidb-server` serves a `mockstore.EmbedUnistoreDriver` store.

use std::sync::Arc;

use tidb_session::domain::real_tikv_node::{configured_account_store, RunConfiguredNodeError};
use tidb_store_driver_error::sql_query_error::SqlQueryError;
use tidb_session::domain::unistore_node::*;

use crate::sql_node::ConcurrentSqlNode;

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

/// Runs the wide cluster-session surface over the embedded store.
///
/// Go's `--store unistore` path in full: the store starts empty on every
/// run, so the boot FIRST publishes the `mysql` schema bootstrap --
/// `session.BootstrapSession`'s work -- then loads the catalog it just
/// wrote and serves the same session driver the cluster node serves.
/// There is no etcd and no peer, so the watch legs are simply absent;
/// the reload ticks still run, against this process's own store.
pub fn run_unistore_cluster_session(
    config: tidb_domain_core::node_config::NodeConfig,
    spill_storage: Arc<tidb_spill_storage::SpillStorage>,
    memory_arbitrator: Option<Arc<tidb_memory::MemArbitrator>>,
) -> Result<(), tidb_session::domain::real_tikv_node::RunConfiguredNodeError> {
    use tidb_session::domain::real_tikv_node::RunConfiguredNodeError;

    // Go's unistore client-go observes the single in-process store (id 1);
    // materialize the store-scoped dashboard series for it.
    tidb_txnkv::client_go_metrics::init_embedded_store_series(
        tidb_unistore_tikv::region_manager::STORE_ID,
    );
    // Go `setGlobalVars`, before the store and the domain are created.
    let global_vars = tidb_domain::global_sysvars::GlobalSysvars::new();
    crate::set_global_vars::set_global_vars(&config, &global_vars);
    let users = configured_account_store(&config, global_vars)?;
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
        rpc: _rpc,
        // Go `initResourceGroupsController`: the controller lives as long as
        // the server, stopped by its guard when this function returns.
        _resource_groups,
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
        .map_err(|error| RunConfiguredNodeError::Node(Box::new(error)))?;
    // Go starts the status HTTP server beside the SQL listener
    // (`cfg.Status.ReportStatus`, default true); `/status` is the first
    // thing `main_test.go` and every health probe reads. A failed bind ends
    // the boot: Go `Server.Run` returns the `startStatusHTTP` error and
    // `main` exits through `terror.MustNil`.
    let _status_server = if config.report_status {
        let schema_factory = Arc::clone(&factory);
        tidb_session::domain::log_status_listen(&config.status_host, config.status_port);
        match crate::http_status::start_status_listener_with_routes(
            &config.status_host,
            config.status_port,
            node.tracker(),
            tidb_mysql::runtime_versions().server_version,
            tidb_versioninfo::TIDB_GIT_HASH.to_owned(),
            crate::http_status::StatusRoutes {
                schema: Some(Arc::new(move || schema_factory.catalog_snapshot())),
                settings: Some(crate::node_status::status_settings(&factory)),
                peer: Some(crate::node_status::peer_service(&factory)),
                security: config.cluster_security.clone(),
            },
        ) {
            Ok(server) => Some(server),
            Err(error) => {
                tidb_session::domain::log_status_listen_failed(&error);
                return Err(RunConfiguredNodeError::Engine(SqlQueryError::unknown(
                    error.to_string(),
                )));
            }
        }
    } else {
        None
    };
    let address = node.local_addr().map_err(|error| RunConfiguredNodeError::Node(Box::new(error)))?;
    let shutdown = node.shutdown_handle();
    let last_signal =
        crate::shutdown_signal::install(move || shutdown.shutdown()).map_err(|error| {
            RunConfiguredNodeError::Engine(SqlQueryError::unknown(error.to_string()))
        })?;
    // Not log content (Go's counterpart is the "server is running MySQL
    // protocol" entry logged at bind): rust/scripts wait for this JSON
    // readiness line on stderr and read its fields.
    eprintln!(
        "{{\"event\":\"cluster_session_node_ready\",\"address\":\"{address}\",\"store\":\"unistore\",\"schema_version\":{schema_version},\"max_connections\":{},\"account_count\":{},\"stats_loaded\":{},\"stats_pseudo\":{}}}",
        config.max_connections,
        users.len(),
        stats_receipt.loaded,
        stats_receipt.pseudo,
    );
    let result = node.run().map_err(|error| RunConfiguredNodeError::Node(Box::new(error)));
    drop(stats_maintenance);
    drop(reloader);
    drop(sysvar_reloader);
    drop(stats_reloader);
    drop(binding_reloader);
    finish_with_signal_code(result, &last_signal)
}
