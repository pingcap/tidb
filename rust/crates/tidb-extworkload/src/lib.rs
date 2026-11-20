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

//! Go `pkg/extworkload`: a [`Manager`] lets TiDB coordinate background
//! workloads (GCV2, TTL, and auto-analyze) with an external workload
//! controller. TiDB only creates a Manager in Starter deploy mode when
//! `[external-workload]` is enabled.

pub mod client;

use std::time::Duration;

use tidb_config::external_workload::{
    ExternalWorkload, ExternalWorkloadRole, ROLE_AUTO_ANALYZE_WORKER, ROLE_GCV2_WORKER, ROLE_MASTER,
    ROLE_TTL_TASK_WORKER,
};
use tidb_log::{Field, Value};
use tikv_client::proto::keyspacepb::KeyspaceMeta;

use client::{CallContext, CallLabels, Client, ClientError, ClientOption};

/// Go `dialTimeout`: bounds the synchronous dial + Ping during manager
/// creation.
const DIAL_TIMEOUT: Duration = Duration::from_secs(30);

/// Go `requestTimeout`.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Go `metrics.WorkerActionInit` and its siblings.
const WORKER_ACTION_INIT: &str = "init";
const WORKER_ACTION_REGISTER: &str = "register";
const WORKER_ACTION_RECYCLE: &str = "recycle";
const WORKER_ACTION_ABORT: &str = "abort";

/// Go `Manager`: coordinates TiDB background workloads with the external
/// workload controller.
pub trait Manager: Send + Sync {
    /// Go `Close`.
    ///
    /// # Errors
    ///
    /// The client could not be closed.
    fn close(&self) -> Result<(), ClientError>;
    /// Go `Role`: the role this TiDB is acting in.
    fn role(&self) -> ExternalWorkloadRole;
    /// Go `Meta`: the keyspace metadata bound to this TiDB.
    fn meta(&self) -> &KeyspaceMeta;

    /// Go `InitializeGCV2`: seeds the controller with an initial
    /// keyspace-level GC task.
    ///
    /// # Errors
    ///
    /// The controller call failed.
    fn initialize_gcv2(&self, gc_life_time: Duration) -> Result<(), ClientError>;
    /// Go `AbortGCV2`: asks the controller to abort all outstanding
    /// keyspace-level GC tasks.
    ///
    /// # Errors
    ///
    /// The controller call failed.
    fn abort_gcv2(&self) -> Result<(), ClientError>;
    /// Go `RegisterGCV2`: reports that a keyspace-level GC round at
    /// `safe_point` has completed.
    ///
    /// # Errors
    ///
    /// The controller call failed.
    fn register_gcv2(&self, safe_point: u64, gc_life_time: Duration) -> Result<(), ClientError>;
    /// Go `RecycleGCV2`: reports that keyspace-level GC up to `safe_point`
    /// has been processed.
    ///
    /// # Errors
    ///
    /// The controller call failed.
    fn recycle_gcv2(&self, safe_point: u64) -> Result<(), ClientError>;
    /// Go `UpdateGCLifeTime`: reports that the user's `gc_life_time` has
    /// changed.
    ///
    /// # Errors
    ///
    /// The controller call failed.
    fn update_gc_life_time(&self, gc_life_time: Duration) -> Result<(), ClientError>;

    /// Go `RegisterTTLTableInfo`: reports that a table's TTL metadata has
    /// been created or altered.
    ///
    /// # Errors
    ///
    /// The controller call failed.
    fn register_ttl_table_info(&self, table_id: i64, ttl_job_enable: bool) -> Result<(), ClientError>;
    /// Go `DeleteTTLTableInfo`: reports that TTL was removed from a table,
    /// or the table was dropped.
    ///
    /// # Errors
    ///
    /// The controller call failed.
    fn delete_ttl_table_info(&self, table_id: i64) -> Result<(), ClientError>;
    /// Go `RecycleTTLTask`: reports that a TTL job has completed.
    ///
    /// # Errors
    ///
    /// The controller call failed.
    fn recycle_ttl_task(&self, completed_job_create_time: u64) -> Result<(), ClientError>;
    /// Go `UpdateTTLJobEnable`: reports a change in the
    /// `tidb_ttl_job_enable` system variable.
    ///
    /// # Errors
    ///
    /// The controller call failed.
    fn update_ttl_job_enable(&self, ttl_job_enable: bool) -> Result<(), ClientError>;

    /// Go `RegisterAutoAnalyze`: reports a newly registered auto-analyze
    /// task.
    ///
    /// # Errors
    ///
    /// The controller call failed.
    fn register_auto_analyze(&self, task_id: u64) -> Result<(), ClientError>;
    /// Go `RecycleAutoAnalyze`: reports that an auto-analyze task has
    /// completed.
    ///
    /// # Errors
    ///
    /// The controller call failed.
    fn recycle_auto_analyze(&self, task_id: u64) -> Result<(), ClientError>;
}

/// Go `IsEnabled`: whether a Manager is present.
#[must_use]
pub fn is_enabled(m: Option<&dyn Manager>) -> bool {
    m.is_some()
}

/// Go `IsMaster`: whether this TiDB is in the regular TiDB role.
#[must_use]
pub fn is_master(m: Option<&dyn Manager>) -> bool {
    role_is(m, ROLE_MASTER)
}

/// Go `IsGCV2Worker`: whether this TiDB is a dedicated keyspace-level GC
/// worker.
#[must_use]
pub fn is_gcv2_worker(m: Option<&dyn Manager>) -> bool {
    role_is(m, ROLE_GCV2_WORKER)
}

/// Go `IsTTLTaskWorker`: whether this TiDB should run TTL jobs.
#[must_use]
pub fn is_ttl_task_worker(m: Option<&dyn Manager>) -> bool {
    role_is(m, ROLE_TTL_TASK_WORKER)
}

/// Go `IsAutoAnalyzeWorker`: whether this TiDB should run auto-analyze jobs.
#[must_use]
pub fn is_auto_analyze_worker(m: Option<&dyn Manager>) -> bool {
    role_is(m, ROLE_AUTO_ANALYZE_WORKER)
}

/// Go `managerStoreKey`'s value: the manager bound to the node's store (a
/// node process opens one store).
static STORE_MANAGER: std::sync::RwLock<Option<std::sync::Arc<dyn Manager>>> = std::sync::RwLock::new(None);

/// Go `SetManagerForStore`: binds the manager to the store. Passing `None`
/// removes it.
pub fn set_manager_for_store(mgr: Option<std::sync::Arc<dyn Manager>>) {
    *STORE_MANAGER.write().unwrap_or_else(std::sync::PoisonError::into_inner) = mgr;
}

/// Go `GetManagerFromStore`: the manager bound to the store.
#[must_use]
pub fn get_manager_from_store() -> Option<std::sync::Arc<dyn Manager>> {
    STORE_MANAGER.read().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
}

fn role_is(m: Option<&dyn Manager>, role: &str) -> bool {
    m.is_some_and(|m| m.role().0 == role)
}

/// Go `AbortGCV2ForUpgrade`: aborts GCV2 work when this TiDB is the dedicated
/// GCV2 worker. Answers true when the caller must terminate after the abort.
///
/// # Errors
///
/// The abort failed.
pub fn abort_gcv2_for_upgrade(mgr: Option<&dyn Manager>) -> Result<bool, ClientError> {
    if !is_gcv2_worker(mgr) {
        return Ok(false);
    }
    mgr.expect("a GCV2 worker has a manager").abort_gcv2()?;
    Ok(true)
}

struct ManagerImpl {
    cli: Box<dyn Client>,
    role: ExternalWorkloadRole,
    meta: KeyspaceMeta,
}

/// Go `NewManager`: a Manager for an enabled external workload config
/// (`None` when it is disabled). `tls_config` is Go's cluster TLS
/// configuration, when the cluster CA is set.
///
/// # Errors
///
/// The keyspace meta is nil, or the controller could not be dialed or pinged.
pub fn new_manager(
    keyspace_meta: Option<&KeyspaceMeta>,
    cfg: &ExternalWorkload,
    tls_config: Option<tonic::transport::ClientTlsConfig>,
) -> Result<Option<Box<dyn Manager>>, ClientError> {
    if !cfg.enable {
        return Ok(None);
    }
    let keyspace_meta = keyspace_meta.ok_or_else(|| {
        ClientError::Other("external workload controller requires a non-nil keyspace meta".to_owned())
    })?;
    let cli = dial_client(keyspace_meta, cfg, tls_config)
        .map_err(|error| ClientError::Other(format!("init external workload client: {error}")))?;
    tidb_logutil::bg_logger().info(
        "external workload manager installed",
        &[
            Field::new("role", Value::Str(cfg.role.0.clone())),
            Field::new("keyspace", Value::Str(keyspace_meta.name.clone())),
            Field::new("keyspace-id", Value::U64(u64::from(keyspace_id(keyspace_meta)))),
        ],
    );
    Ok(Some(new_manager_with_client(cli, cfg.role.clone(), keyspace_meta.clone())))
}

/// Go `keyspaceMeta.GetId()`.
fn keyspace_id(meta: &KeyspaceMeta) -> u32 {
    match meta.keyspace {
        Some(tikv_client::proto::keyspacepb::keyspace_meta::Keyspace::Id(id)) => id,
        _ => 0,
    }
}

/// A Manager over `cli` (Go's `&manager{cli, role, meta}`).
#[must_use]
pub fn new_manager_with_client(cli: Box<dyn Client>, role: ExternalWorkloadRole, meta: KeyspaceMeta) -> Box<dyn Manager> {
    Box::new(ManagerImpl { cli, role, meta })
}

fn dial_client(
    keyspace_meta: &KeyspaceMeta,
    cfg: &ExternalWorkload,
    tls_config: Option<tonic::transport::ClientTlsConfig>,
) -> Result<Box<dyn Client>, ClientError> {
    let cli = client::new(&ClientOption {
        keyspace_id: keyspace_id(keyspace_meta),
        keyspace_name: keyspace_meta.name.clone(),
        tidb_pool: cfg.tidb_pool.clone(),
        controller_addr: cfg.controller_addr.clone(),
        tls_config,
        interceptor: Some(metrics_interceptor),
    })?;
    if let Err(error) = cli.ping(CallContext { timeout: DIAL_TIMEOUT, labels: None }) {
        if let Err(close_error) = cli.close() {
            tidb_logutil::bg_logger().warn(
                "failed to close external workload client after ping failure",
                &[tidb_logutil::zap_error(close_error.to_string())],
            );
        }
        return Err(ClientError::Other(format!("ping external workload controller: {error}")));
    }
    Ok(cli)
}

/// Go `metricsInterceptor`: counts a labelled call.
fn metrics_interceptor(labels: &CallLabels) {
    tidb_metrics::external_workload::EXTERNAL_WORKLOAD_TASK_COUNTER
        .with_label_values(&[labels.worker_type, labels.action])
        .inc();
}

/// Go `withRequestTimeout` and `withMetric`.
fn ctx(labels: Option<(&'static str, &'static str)>) -> CallContext {
    CallContext {
        timeout: REQUEST_TIMEOUT,
        labels: labels.map(|(worker_type, action)| CallLabels { worker_type, action }),
    }
}

/// Go `int64(gcLifeTime.Seconds())`.
fn seconds(duration: Duration) -> i64 {
    duration.as_secs_f64() as i64
}

impl Manager for ManagerImpl {
    fn close(&self) -> Result<(), ClientError> {
        self.cli.close()
    }

    fn role(&self) -> ExternalWorkloadRole {
        self.role.clone()
    }

    fn meta(&self) -> &KeyspaceMeta {
        &self.meta
    }

    fn initialize_gcv2(&self, gc_life_time: Duration) -> Result<(), ClientError> {
        self.cli.register_gcv2(ctx(Some((ROLE_GCV2_WORKER, WORKER_ACTION_INIT))), 0, seconds(gc_life_time))
    }

    fn abort_gcv2(&self) -> Result<(), ClientError> {
        // MaxUint64 asks the controller to recycle all GCV2 tasks.
        self.cli.recycle_gcv2(ctx(Some((ROLE_GCV2_WORKER, WORKER_ACTION_ABORT))), u64::MAX)
    }

    fn register_gcv2(&self, safe_point: u64, gc_life_time: Duration) -> Result<(), ClientError> {
        self.cli.register_gcv2(
            ctx(Some((ROLE_GCV2_WORKER, WORKER_ACTION_REGISTER))),
            safe_point,
            seconds(gc_life_time),
        )
    }

    fn recycle_gcv2(&self, safe_point: u64) -> Result<(), ClientError> {
        self.cli.recycle_gcv2(ctx(Some((ROLE_GCV2_WORKER, WORKER_ACTION_RECYCLE))), safe_point)
    }

    fn update_gc_life_time(&self, gc_life_time: Duration) -> Result<(), ClientError> {
        self.cli.update_gc_life_time(ctx(None), seconds(gc_life_time))
    }

    fn register_ttl_table_info(&self, table_id: i64, ttl_job_enable: bool) -> Result<(), ClientError> {
        self.cli.register_ttl_task(
            ctx(Some((ROLE_TTL_TASK_WORKER, WORKER_ACTION_REGISTER))),
            table_id,
            ttl_job_enable,
        )
    }

    fn delete_ttl_table_info(&self, table_id: i64) -> Result<(), ClientError> {
        self.cli.delete_ttl_table_info(ctx(None), table_id)
    }

    fn recycle_ttl_task(&self, completed_job_create_time: u64) -> Result<(), ClientError> {
        self.cli.recycle_ttl_task(ctx(Some((ROLE_TTL_TASK_WORKER, WORKER_ACTION_RECYCLE))), completed_job_create_time)
    }

    fn update_ttl_job_enable(&self, ttl_job_enable: bool) -> Result<(), ClientError> {
        self.cli.update_ttl_job_enable(ctx(None), ttl_job_enable)
    }

    fn register_auto_analyze(&self, task_id: u64) -> Result<(), ClientError> {
        self.cli.register_auto_analyze(ctx(Some((ROLE_AUTO_ANALYZE_WORKER, WORKER_ACTION_REGISTER))), task_id)
    }

    fn recycle_auto_analyze(&self, task_id: u64) -> Result<(), ClientError> {
        self.cli.recycle_auto_analyze(ctx(Some((ROLE_AUTO_ANALYZE_WORKER, WORKER_ACTION_RECYCLE))), task_id)
    }
}

#[cfg(test)]
mod tests;
