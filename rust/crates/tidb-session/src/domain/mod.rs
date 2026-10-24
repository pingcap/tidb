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
use self::session_api::SqlQueryError;

pub mod session_api;
pub mod resultset_source;
pub mod wire_status;
pub mod pipeline_session;
pub mod configured_user_store;
pub mod auth_identity;
pub mod native_password;
pub mod secure_transport;
pub mod cluster_privileges;
pub mod cluster_account_seam;
pub mod cluster_analyze_seam;
pub mod cluster_auto_id_seam;
pub mod cluster_binding_seam;
pub mod cluster_stats_lock_seam;
pub mod cluster_sysvar_seam;
pub mod cluster_session;
pub mod cluster_session_node;
pub mod cluster_stack;
pub mod session_transaction;
pub mod global_config_sync;
pub mod cluster_topology;
pub mod serverinfo_etcd;
pub mod set_global_vars;
pub mod node_config;
pub mod main_flags;
pub mod real_tikv_node;
pub mod unistore_node;

// Tests of process-global sysvar hooks must not share those hooks with other
// simulated nodes. Run the original test body in its own harness process;
// threads created inside the body still exercise the real publication races.
#[cfg(test)]
pub fn isolate_process_globals() -> bool {
    const MARKER: &str = "TIDB_SERVER_ISOLATED_GLOBALS_TEST";
    let thread = std::thread::current();
    let name = thread.name().expect("named Rust test thread");
    if std::env::var(MARKER).as_deref() == Ok(name) {
        return false;
    }
    let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
        .args(["--exact", name, "--nocapture"])
        .env(MARKER, name)
        .output()
        .expect("start isolated global-state test");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() && stdout.contains("1 passed; 0 failed"),
        "isolated test {name} failed or was not selected: {}\n{stdout}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    true
}

/// zap `zap.Error(err)` under TiDB's default `disable-error-stack`: an
/// `error` field carrying the error text.
pub fn zap_error(error: impl std::fmt::Display) -> tidb_log::Field {
    tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string()))
}

/// zap `zap.Stack("stack")`: the current stack trace.
pub fn zap_stack() -> tidb_log::Field {
    tidb_log::Field::new(
        "stack",
        tidb_log::Value::Str(std::backtrace::Backtrace::force_capture().to_string()),
    )
}

/// Go `util.Recover(metrics.LabelDomain, funcInfo, nil, false)`'s line for a
/// domain worker whose panic surfaced when it was joined. The panicking
/// thread's stack is gone by then, so the entry carries no `stack` field.
pub fn log_recovered_panic(func_info: &str, payload: &(dyn std::any::Any + Send)) {
    let recovered = payload
        .downcast_ref::<&str>()
        .map(|message| (*message).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_default();
    tidb_util::logutil::bg_logger().error(
        "panic in the recoverable goroutine",
        &[
            tidb_log::Field::new("label", tidb_log::Value::Str("domain".to_owned())),
            tidb_log::Field::new("funcInfo", tidb_log::Value::Str(func_info.to_owned())),
            tidb_log::Field::new("r", tidb_log::Value::Str(recovered)),
        ],
    );
}

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
    tidb_util::logutil::bg_logger().info(
        "for status and metrics report",
        &[tidb_log::Field::new(
            "listening on addr",
            tidb_log::Value::Str(join_host_port(host, port)),
        )],
    );
}

/// Go `listenStatusHTTPServer`'s line when the status port cannot be bound.
pub fn log_status_listen_failed(error: &std::io::Error) {
    tidb_util::logutil::bg_logger().info("listen failed", &[zap_error(error)]);
}

/// Go `Server.initTiDBListener`'s line once the MySQL port is bound.
pub fn log_mysql_listening(host: &str, port: impl std::fmt::Display) {
    tidb_util::logutil::bg_logger().info(
        "server is running MySQL protocol",
        &[tidb_log::Field::new(
            "addr",
            tidb_log::Value::Str(join_host_port(host, port)),
        )],
    );
}

pub fn open_spill_storage(
    config: &NodeConfig,
) -> Result<Arc<tidb_util::spill_storage::SpillStorage>, RunConfiguredNodeError> {
    tidb_exec::configure_deadlock_history(
        config.deadlock_history_capacity,
        config.deadlock_history_collect_retryable,
    );
    crate::sysvar::install_sem_v2_sysvar_registry();
    if config.sem_enabled && !config.sem_config.is_empty() {
        tidb_util::sem::disable();
        tidb_util::sem_v2::enable(&config.sem_config)
            .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error)))?;
    } else if config.sem_enabled {
        tidb_util::sem_v2::disable();
        tidb_util::sem::enable();
    } else {
        tidb_util::sem_v2::disable();
        tidb_util::sem::disable();
    }
    tidb_util::spill_storage::SpillStorage::open(config.spill_storage.clone())
        .map(Arc::new)
        .map_err(RunConfiguredNodeError::Spill)
}

fn open_memory_arbitrator(
    config: &NodeConfig,
) -> Result<Option<Arc<tidb_util::memory::MemArbitrator>>, RunConfiguredNodeError> {
    let mode = tidb_util::memory::parse_work_mode_text(&config.memory_arbitrator.mode);
    let limit =
        tidb_util::memory::parse_server_memory_limit(&config.memory_arbitrator.server_memory_limit)
            .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error)))?;
    let (soft_bytes, soft_ratio, soft_mode) =
        tidb_util::memory::parse_soft_limit_text(&config.memory_arbitrator.soft_limit);
    let state_dir = config.spill_storage.path.join("mem-arbitrator");
    let arbitrator = tidb_util::memory::MemArbitrator::new(
        i64::try_from(limit).expect("validated server memory limit fits i64"),
        tidb_util::memory::DEF_POOL_STATUS_SHARDS,
        tidb_util::memory::DEF_POOL_QUOTA_SHARDS,
        64 << 10,
        Box::new(tidb_util::memory::RuntimeMemStateRecorder::new(&state_dir)),
    );
    arbitrator.set_soft_limit(soft_bytes, soft_ratio, soft_mode);
    if !arbitrator.auto_run(
        tidb_util::memory::MemArbitratorActions::default(),
        tidb_util::memory::DEF_AWAIT_FREE_POOL_ALLOC_ALIGN_SIZE,
        tidb_util::memory::DEF_AWAIT_FREE_POOL_SHARD_NUM,
        tidb_util::memory::DEF_TASK_TICK_DUR,
    ) {
        return Err(RunConfiguredNodeError::Engine(SqlQueryError::unknown(
            "failed to start global memory arbitrator".to_owned(),
        )));
    }
    arbitrator.set_work_mode(mode);
    Ok(Some(arbitrator))
}

pub struct MemoryArbitratorAuthority {
    arbitrator: Option<Arc<tidb_util::memory::MemArbitrator>>,
    registration: Option<tidb_util::memory::ProcessArbitratorRegistration>,
}

impl MemoryArbitratorAuthority {
    pub fn open(config: &NodeConfig) -> Result<Self, RunConfiguredNodeError> {
        let arbitrator = open_memory_arbitrator(config)?;
        let registration = arbitrator
            .as_ref()
            .map(tidb_util::memory::install_process_arbitrator);
        Ok(Self {
            arbitrator,
            registration,
        })
    }

    pub fn arbitrator(&self) -> Option<Arc<tidb_util::memory::MemArbitrator>> {
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
