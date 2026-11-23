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

//! Go `pkg/ddl`, the worker side, below the session.
//!
//! Go's layering is `ddl` < `domain` < `session`: the DDL executor builds a
//! job, the owner's worker steps it through the schema states, and a session
//! (a unit test's `testkit` session included) runs on a store that already
//! hosts both. The Rust node grew the other way round, with the worker living
//! in `tidb-server` beside the connection layer, so no session below the
//! server could ever have one.
//!
//! * [`worker`] is `RealClusterDdl`: `DoDDLJob` for the submitting side, the
//!   owner-elected scheduler that steps persisted jobs, the schema-version
//!   syncer and the reorganization backfillers.
//! * [`schema_sync`] is the non-owner's MDL loop (`MDLCheckLoop`): it
//!   acknowledges each loaded schema version to the owner's
//!   `WaitVersionSynced`, held back by the node's [`schema_sync::SchemaPinRegistry`].
//! * [`etcd_ops`] binds the syncers' etcd surface to the real client.
//! * [`embedded_store`] is `mockstore.NewMockStore` over unistore: the store
//!   the bootstrap and every in-process test run the worker on.
//!
//! The executor is never named: the work Go's DDL does through the session
//! pool the domain hands it -- a system session, the restricted SQL of the
//! data checks -- goes through the
//! [`crate::real_tikv_ddl::DdlSessionPool`] the node hands
//! [`RealClusterDdl`].

mod cluster_bdr;
/// Go `pkg/ddl/index.go` distributed-task control.
pub mod index;
pub mod stat;
pub mod ddl_history;
pub mod cluster_ddl;
pub mod ddl_running_jobs;
pub mod job_control;
mod job_ctx;
pub mod job_txn;
mod job_submitter;
pub mod ddl_affinity;
pub mod ddl_delete_range;
pub mod ddl_job_scheduler;
pub mod ddl_reorg_table;
pub mod foreign_key_build;
pub mod placement_delivery;
pub mod real_tikv_ddl;
pub mod tiflash_replica_manager;
pub mod embedded_store;
pub mod etcd_ops;
pub mod owner_mgr;
pub mod schema_sync;
pub mod worker;
pub mod pre_split;
pub mod testutil;
mod index_presplit;

pub use worker::{ClusterDdl, RealClusterDdl};
/// Go `ddl.DefaultAnalyzeCheckInterval`.
pub use cluster_ddl::analyze::DEFAULT_ANALYZE_CHECK_INTERVAL_MILLIS;
/// Go `executor`'s statement methods, over any [`ExecutorStore`].
pub use worker::{executor, ExecutorStore};

/// Go `JobWrapper`: a job, typed arguments and its ID-allocation ownership.
pub use tidb_ddl_jobsubmit::JobSpec as JobWrapper;

/// Go `OnExist`: what creating an object does on a name collision.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OnExist {
    /// Go `OnExistError`: throws an error on name collision.
    #[default]
    Error,
    /// Go `OnExistIgnore`: skips creating the new object.
    Ignore,
    /// Go `OnExistReplace`: replaces the old object by the new object. This
    /// is only supported by VIEWs at the moment; for other object types it
    /// is equivalent to `Error`.
    Replace,
}

/// Go `CreateTableConfig`: the configuration of `CreateTableWithInfo`
/// (Go's `WithOnExist` / `WithIDAllocated` options set its fields).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CreateTableConfig {
    /// Go `OnExist`.
    pub on_exist: OnExist,
    /// Go `IDAllocated`: the job has allocated all IDs for the tables it
    /// affects, so DDL does not allocate them again. Only BR uses it.
    pub id_allocated: bool,
}

/// Go `ddl.StartMode`: the mode a node starts its DDL in. `Upgrade` forces
/// the node to be the DDL owner, and outside `Normal` the masking-policy
/// hooks tolerate a `mysql.tidb_masking_policy` that does not exist yet;
/// Go's other readers -- the executor's `BR` checks -- belong to executor
/// methods this port lacks (D01).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartMode {
    /// Go `Normal` mode, cluster is in normal state.
    Normal,
    /// Go `Bootstrap` mode, cluster is during bootstrap.
    Bootstrap,
    /// Go `Upgrade` mode, cluster is during upgrade, we will force current
    /// node to be the DDL owner, to make sure all upgrade related DDLs are
    /// run on new version TiDB instance.
    Upgrade,
    /// Go `BR` mode, Start DDL from br, with this mode can skip loading
    /// keyspace meta.
    Br,
}

impl StartMode {
    /// Go's string value of the mode.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Bootstrap => "bootstrap",
            Self::Upgrade => "upgrade",
            Self::Br => "br",
        }
    }
}

/// Tests of process-global sysvar hooks must not share those hooks with other
/// simulated nodes. Runs the original test body in its own harness process.
#[cfg(test)]
pub(crate) fn isolate_process_globals() -> bool {
    const MARKER: &str = "TIDB_DDL_ISOLATED_GLOBALS_TEST";
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

/// Go distributed backfill task metadata and executor.
pub mod backfilling_dist_executor;
/// Go distributed backfill scheduler.
pub mod backfilling_dist_scheduler;

/// Go `delayForAsyncCommit` (`pkg/ddl/ddl.go`): without the metadata lock,
/// a DDL step that must not race an async-commit or 1PC transaction still
/// using the previous schema sleeps for the async-commit safe window plus
/// the allowed clock drift, after which such a transaction's `maxCommitTS`
/// has passed and it falls back to a 2PC commit that checks the schema.
pub(crate) fn delay_for_async_commit() {
    if tidb_vardef::is_mdl_enabled(tidb_config_kerneltype::is_next_gen()) {
        return;
    }
    let config = tikv_client::config::get_global_config();
    let duration = config.tikv_client.async_commit.safe_window + config.tikv_client.async_commit.allowed_clock_drift;
    tidb_ddl_logutil::ddl_logger().info(
        "sleep before DDL finishes to make async commit and 1PC safe",
        &[tidb_log::Field::new(
            "duration",
            tidb_log::Value::Duration(i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)),
        )],
    );
    std::thread::sleep(duration);
}

// Go's test binary links `planner/core`, whose `init` sets `expression.BuildSimpleExpr`.
#[cfg(test)]
tidb_planner_init::link!();
