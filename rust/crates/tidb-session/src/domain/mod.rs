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

//! The node-level composition below the network front end: Go's
//! `domain.Domain` (the store, the catalog and statistics reloaders, the DDL
//! owner, the system variable and binding caches) and the session
//! construction of `pkg/session` (`CreateSession`, `BootstrapSession`), with
//! the session interface the MySQL front end drives. `tidb-server` keeps only
//! the network: the listener, the protocol and the status port, as Go's
//! `cmd/tidb-server` and `pkg/server` do.

use std::sync::Arc;

use self::node_config::NodeConfig;
use self::real_tikv_node::RunConfiguredNodeError;
use tidb_store_driver_error::sql_query_error::SqlQueryError;

pub mod peer_address;
pub mod session_api;
pub mod resultset_source;
pub mod wire_status;
pub mod pipeline_session;
pub mod configured_user_store;
pub mod auth_identity;
pub mod native_password;
pub mod secure_transport;
pub mod cluster_session_node;
pub mod cluster_stack;
pub mod global_config_sync;
pub mod cluster_topology;
pub mod serverinfo_etcd;
pub mod real_tikv_node;
pub mod unistore_node;
pub(crate) mod embedded_resource_groups;
pub mod mock_store;

#[cfg(test)]
use tidb_test_isolation::isolate_process_globals;




use tidb_logutil::{log_recovered_panic, zap_error, zap_stack};
use tidb_domain_core::node_config;

/// Go `net.JoinHostPort`.
pub fn join_host_port(host: &str, port: impl std::fmt::Display) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// Go `listenStatusHTTPServer`'s line, logged before the status port is
/// bound.
pub fn log_status_listen(host: &str, port: u16) {
    tidb_logutil::bg_logger().info(
        "for status and metrics report",
        &[tidb_log::Field::new(
            "listening on addr",
            tidb_log::Value::Str(join_host_port(host, port)),
        )],
    );
}

/// Go `listenStatusHTTPServer`'s line when the status port cannot be bound.
pub fn log_status_listen_failed(error: &std::io::Error) {
    tidb_logutil::bg_logger().info("listen failed", &[zap_error(error)]);
}

/// Go `Server.initTiDBListener`'s line once the MySQL port is bound.
pub fn log_mysql_listening(host: &str, port: impl std::fmt::Display) {
    tidb_logutil::bg_logger().info(
        "server is running MySQL protocol",
        &[tidb_log::Field::new(
            "addr",
            tidb_log::Value::Str(join_host_port(host, port)),
        )],
    );
}

pub fn open_spill_storage(
    config: &NodeConfig,
) -> Result<Arc<tidb_spill_storage::SpillStorage>, RunConfiguredNodeError> {
    tidb_deadlockhistory::recording::configure_deadlock_history(
        config.deadlock_history_capacity,
        config.deadlock_history_collect_retryable,
    );
    tidb_sem_v2::install_variable_registry();
    if config.sem_enabled && !config.sem_config.is_empty() {
        tidb_sem::disable();
        tidb_sem_v2::enable(&config.sem_config)
            .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error)))?;
    } else if config.sem_enabled {
        tidb_sem_v2::disable();
        tidb_sem::enable();
    } else {
        tidb_sem_v2::disable();
        tidb_sem::disable();
    }
    tidb_spill_storage::SpillStorage::open(config.spill_storage.clone())
        .map(Arc::new)
        .map_err(RunConfiguredNodeError::Spill)
}

fn open_memory_arbitrator(
    config: &NodeConfig,
) -> Result<Option<Arc<tidb_memory::MemArbitrator>>, RunConfiguredNodeError> {
    let mode = tidb_memory::parse_work_mode_text(&config.memory_arbitrator.mode);
    let limit =
        tidb_memory::parse_server_memory_limit(&config.memory_arbitrator.server_memory_limit)
            .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error)))?;
    let (soft_bytes, soft_ratio, soft_mode) =
        tidb_memory::parse_soft_limit_text(&config.memory_arbitrator.soft_limit);
    let state_dir = config.spill_storage.path.join("mem-arbitrator");
    let arbitrator = tidb_memory::MemArbitrator::new(
        i64::try_from(limit).expect("validated server memory limit fits i64"),
        tidb_memory::DEF_POOL_STATUS_SHARDS,
        tidb_memory::DEF_POOL_QUOTA_SHARDS,
        64 << 10,
        Box::new(tidb_memory::RuntimeMemStateRecorder::new(&state_dir)),
    );
    arbitrator.set_soft_limit(soft_bytes, soft_ratio, soft_mode);
    if !arbitrator.auto_run(
        tidb_memory::MemArbitratorActions::default(),
        tidb_memory::DEF_AWAIT_FREE_POOL_ALLOC_ALIGN_SIZE,
        tidb_memory::DEF_AWAIT_FREE_POOL_SHARD_NUM,
        tidb_memory::DEF_TASK_TICK_DUR,
    ) {
        return Err(RunConfiguredNodeError::Engine(SqlQueryError::unknown(
            "failed to start global memory arbitrator".to_owned(),
        )));
    }
    arbitrator.set_work_mode(mode);
    Ok(Some(arbitrator))
}

pub struct MemoryArbitratorAuthority {
    arbitrator: Option<Arc<tidb_memory::MemArbitrator>>,
    registration: Option<tidb_memory::ProcessArbitratorRegistration>,
}

impl MemoryArbitratorAuthority {
    pub fn open(config: &NodeConfig) -> Result<Self, RunConfiguredNodeError> {
        let arbitrator = open_memory_arbitrator(config)?;
        let registration = arbitrator
            .as_ref()
            .map(tidb_memory::install_process_arbitrator);
        Ok(Self {
            arbitrator,
            registration,
        })
    }

    pub fn arbitrator(&self) -> Option<Arc<tidb_memory::MemArbitrator>> {
        self.arbitrator.as_ref().map(Arc::clone)
    }
}

impl Drop for MemoryArbitratorAuthority {
    fn drop(&mut self) {
        if let Some(arbitrator) = self.arbitrator.as_ref() {
            let _ = arbitrator.stop();
        }
        self.registration.take();
    }
}
