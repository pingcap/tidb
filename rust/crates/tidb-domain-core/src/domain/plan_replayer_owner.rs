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

//! Go `Domain.SetupPlanReplayerHandle` / `StartPlanReplayerHandle` and
//! `SetupDumpFileGCChecker` / `DumpFileGcCheckerLoop`: the capture-task
//! collector, the dump workers and the dump-file GC, over the domain's
//! internal sessions and the global external storage.

use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use tidb_domain::plan_replayer::{
    DumpFileGcChecker, DumpFileStorage, GcStatusHook, PlanReplayerDumpTask, PlanReplayerDumper,
    PlanReplayerError, PlanReplayerHandle, PlanReplayerTaskCollectorHandle,
    PlanReplayerTaskDumpWorker,
};

use super::{Domain, UsageWorkerStop};
use crate::sys_session::DomainSysSessionPool;

/// Go `planReplayerHandleLease`.
const PLAN_REPLAYER_HANDLE_LEASE: Duration = Duration::from_secs(10);

/// Go's dedicated internal sessions (`ses[6]`, the worker sessions, `ses[7]`)
/// as one pooled internal session per call.
#[derive(Clone)]
pub struct PooledSql(pub Arc<DomainSysSessionPool>);

impl PooledSql {
    /// One call on a pooled internal session (Go's `sctx`); a failed call
    /// keeps the session out of the pool, as Go's `WithSession` does.
    fn with_context<T>(
        &self,
        run: impl FnOnce(&dyn crate::sys_session::DomainSysSession) -> tidb_sqlexec::Result<T>,
    ) -> tidb_sqlexec::Result<T> {
        self.0
            .with_session(|session| {
                session.with_session_context(|context| {
                    run(context).map_err(|error| tidb_syssession::SysSessionError::new(error.to_string()))
                })
            })
            .map_err(|error| Box::new(error) as tidb_sqlexec::SqlExecError)
    }
}

/// Go `sctx.GetSQLExecutor()`.
impl tidb_sqlexec::SqlExecutor for PooledSql {
    fn execute(
        &self,
        context: &dyn tidb_sqlexec::ExecutionContext,
        sql: &str,
    ) -> tidb_sqlexec::Result<Vec<Box<dyn tidb_sqlexec::RecordSet>>> {
        self.with_context(|sctx| sctx.sql_executor().execute(context, sql))
    }

    fn execute_internal(
        &self,
        context: &dyn tidb_sqlexec::ExecutionContext,
        sql: &str,
        arguments: &[tidb_sqlescape::SqlArg<'_>],
    ) -> tidb_sqlexec::Result<Option<Box<dyn tidb_sqlexec::RecordSet>>> {
        self.with_context(|sctx| sctx.sql_executor().execute_internal(context, sql, arguments))
    }

    fn execute_stmt(
        &self,
        context: &dyn tidb_sqlexec::ExecutionContext,
        statement: &tidb_ast::Stmt,
    ) -> tidb_sqlexec::Result<Option<Box<dyn tidb_sqlexec::RecordSet>>> {
        self.with_context(|sctx| sctx.sql_executor().execute_stmt(context, statement))
    }
}

/// Go `sctx.GetRestrictedSQLExecutor()`.
impl tidb_sqlexec::RestrictedSqlExecutor for PooledSql {
    fn parse_with_params(
        &self,
        context: &dyn tidb_sqlexec::ExecutionContext,
        sql: &str,
        arguments: &[tidb_sqlescape::SqlArg<'_>],
    ) -> tidb_sqlexec::Result<tidb_ast::Stmt> {
        self.with_context(|sctx| sctx.restricted_sql_executor().parse_with_params(context, sql, arguments))
    }

    fn exec_restricted_stmt(
        &self,
        context: &dyn tidb_sqlexec::ExecutionContext,
        statement: &tidb_ast::Stmt,
        options: &[tidb_sqlexec::OptionFuncAlias],
    ) -> tidb_sqlexec::Result<(Vec<Vec<tidb_datatype::Datum>>, Vec<tidb_resolve::ResultFieldRef>)> {
        self.with_context(|sctx| sctx.restricted_sql_executor().exec_restricted_stmt(context, statement, options))
    }

    fn exec_restricted_sql(
        &self,
        context: &dyn tidb_sqlexec::ExecutionContext,
        options: &[tidb_sqlexec::OptionFuncAlias],
        sql: &str,
        arguments: &[tidb_sqlescape::SqlArg<'_>],
    ) -> tidb_sqlexec::Result<(Vec<Vec<tidb_datatype::Datum>>, Vec<tidb_resolve::ResultFieldRef>)> {
        self.with_context(|sctx| sctx.restricted_sql_executor().exec_restricted_sql(context, options, sql, arguments))
    }
}

/// Go `extstore.GetGlobalExtStorage(ctx)` as the GC checker walks it: read
/// again on every call, as Go's `gcDumpFilesByPath` does.
struct GcStorage;

impl GcStorage {
    fn storage() -> Result<Arc<tidb_planner_extstore::LocalStorage>, PlanReplayerError> {
        tidb_planner_extstore::get_global_ext_storage().map_err(PlanReplayerError::Other)
    }
}

impl DumpFileStorage for GcStorage {
    fn walk_dir(&self, sub_dir: &str) -> Result<Vec<String>, PlanReplayerError> {
        Self::storage()?.walk_dir(sub_dir).map_err(|error| PlanReplayerError::Other(error.to_string()))
    }

    fn delete_file(&self, file_name: &str) -> Result<(), PlanReplayerError> {
        Self::storage()?.delete_file(file_name).map_err(PlanReplayerError::Other)
    }
}

/// The dumper the session layer installs (Go's worker sessions run
/// `DumpPlanReplayerInfo`).
#[derive(Clone)]
pub struct SharedDumper(pub Arc<dyn PlanReplayerDumper + Send + Sync>);

impl PlanReplayerDumper for SharedDumper {
    fn generate_plan_replayer_file(
        &self,
        is_capture: bool,
        is_continues_capture: bool,
        enable_historical_stats_for_capture: bool,
    ) -> Result<String, PlanReplayerError> {
        self.0
            .generate_plan_replayer_file(is_capture, is_continues_capture, enable_historical_stats_for_capture)
    }

    fn dump_plan_replayer_info(&self, task: &PlanReplayerDumpTask) -> Result<(), PlanReplayerError> {
        self.0.dump_plan_replayer_info(task)
    }
}

/// Go `Domain.planReplayerHandle` and its goroutines.
pub struct PlanReplayerRuntime {
    /// Go `GetPlanReplayerHandle()`.
    pub handle: Arc<PlanReplayerHandle<PooledSql>>,
    stop: Arc<UsageWorkerStop>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

impl PlanReplayerRuntime {
    /// Go `SetupPlanReplayerHandle` + `StartPlanReplayerHandle` +
    /// `SetupDumpFileGCChecker` + `DumpFileGcCheckerLoop`.
    pub(super) fn start(domain: &Arc<Domain>, dumper: SharedDumper) -> Self {
        let sql = PooledSql(domain.advanced_sys_session_pool());
        let handle = Arc::new(PlanReplayerHandle::new(PlanReplayerTaskCollectorHandle::new(sql.clone()), 16));
        let stop = Arc::new(UsageWorkerStop {
            stopped: Mutex::new(false),
            wake: std::sync::Condvar::new(),
        });
        let mut threads = Vec::new();
        // Go "PlanReplayerTaskCollectHandle": collect every lease.
        {
            let handle = Arc::clone(&handle);
            let stop = Arc::clone(&stop);
            threads.push(
                tidb_worker_stack::sql_worker_thread("plan-replayer-collect")
                    .spawn(move || {
                        while !stop.wait(PLAN_REPLAYER_HANDLE_LEASE) {
                            if let Err(error) = handle.collector.collect_plan_replayer_task() {
                                tidb_logutil::bg_logger().warn(
                                    "plan replayer handle collect tasks failed",
                                    &[tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string()))],
                                );
                            }
                        }
                    })
                    .expect("plan replayer collector spawns"),
            );
        }
        // Go "PlanReplayerTaskDumpHandle": the workers share the task channel.
        if let Some(receiver) = handle.dump_handle.take_receiver() {
            let receiver = Arc::new(Mutex::new(receiver));
            let workers = tidb_config::config_tree::config::get_global_config()
                .performance
                .plan_replayer_dump_worker_concurrency;
            for index in 0..workers {
                let global_vars = domain.global_vars.clone();
                let receiver = Arc::clone(&receiver);
                let stop = Arc::clone(&stop);
                let mut worker =
                    PlanReplayerTaskDumpWorker::new(sql.clone(), dumper.clone(), handle.dump_handle.get_task_status());
                threads.push(
                    tidb_worker_stack::sql_worker_thread(format!("plan-replayer-dump-{index}"))
                        .spawn(move || loop {
                            if *stop.stopped.lock().unwrap_or_else(std::sync::PoisonError::into_inner) {
                                return;
                            }
                            let task = receiver
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .recv_timeout(Duration::from_millis(200));
                            match task {
                                Ok(task) => {
                                    // Go `vardef.EnableHistoricalStatsForCapture.Load()`,
                                    // the global variable's current value.
                                    worker.set_enable_historical_stats_for_capture(
                                        global_vars
                                            .get(tidb_vardef::tidb_vars::TIDB_ENABLE_HISTORICAL_STATS_FOR_CAPTURE)
                                            .is_ok_and(|value| tidb_vardef::option_values::tidb_opt_on(&value)),
                                    );
                                    worker.handle_task(&task);
                                }
                                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
                            }
                        })
                        .expect("plan replayer dump worker spawns"),
                );
            }
        }
        // Go "dumpFileGcChecker": GC every lease over the three dump dirs.
        {
            // Go `vardef.GetPlanReplayerGCLease()`, which `NewDomain` receives.
            let lease = Duration::from_nanos(u64::try_from(tidb_vardef::get_plan_replayer_gc_lease()).unwrap_or(0));
            let instance_id = domain
                .server_info()
                .map(|syncer| syncer.get_local_server_info().static_info.id)
                .unwrap_or_default();
            let mut checker = DumpFileGcChecker::new(
                chrono::Duration::from_std(lease).unwrap_or(chrono::Duration::minutes(10)),
                vec![
                    tidb_replayer::get_plan_replayer_dir_name().to_owned(),
                    tidb_domain::optimize_trace::get_optimizer_trace_dir_name(&instance_id),
                    tidb_domain::extract::get_extract_task_dir_name().to_owned(),
                ],
            );
            checker.setup_sctx(GcStatusHook { sctx: sql, task_status: handle.dump_handle.get_task_status() });
            let stop = Arc::clone(&stop);
            let storage = GcStorage;
            if !lease.is_zero() {
                threads.push(
                    tidb_worker_stack::sql_worker_thread("dump-file-gc")
                        .spawn(move || {
                            while !stop.wait(lease) {
                                let retention = chrono::Duration::nanoseconds(tidb_vardef::plan_replayer_file_retention_time());
                                checker.gc_dump_files(&storage, chrono::Utc::now(), retention, chrono::Duration::days(7));
                            }
                        })
                        .expect("dump file gc checker spawns"),
                );
            }
        }
        Self { handle, stop, threads }
    }
}

impl Drop for PlanReplayerRuntime {
    fn drop(&mut self) {
        *self.stop.stopped.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.stop.wake.notify_all();
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}

impl Domain {
    /// Go `StartPlanReplayerHandle` (and the dump-file GC loop) with the
    /// session layer's dumper; a second call keeps the first runtime.
    pub fn start_plan_replayer_handle(self: &Arc<Self>, dumper: Arc<dyn PlanReplayerDumper + Send + Sync>) {
        let _ = self.plan_replayer.get_or_init(|| PlanReplayerRuntime::start(self, SharedDumper(dumper)));
    }

    /// Go `GetPlanReplayerHandle()`, absent until started.
    pub fn plan_replayer_handle(&self) -> Option<Arc<PlanReplayerHandle<PooledSql>>> {
        self.plan_replayer.get().map(|runtime| Arc::clone(&runtime.handle))
    }

    /// Go `SetupExtractHandle` setups extract handler; a second call keeps
    /// the first handle.
    pub fn setup_extract_handle(&self, sctx: Box<dyn tidb_domain::extract::ExtractSessionContext + Send>) {
        let _ = self
            .extract_task_handle
            .get_or_init(|| Arc::new(tidb_domain::extract::ExtractHandle::new(sctx)));
    }

    /// Go `GetExtractHandle` returns extract handle, absent until set up.
    pub fn get_extract_handle(&self) -> Option<Arc<tidb_domain::extract::ExtractHandle>> {
        self.extract_task_handle.get().cloned()
    }

    /// A weak domain for the dumper's sessions.
    pub fn weak(self: &Arc<Self>) -> Weak<Self> {
        Arc::downgrade(self)
    }
}
