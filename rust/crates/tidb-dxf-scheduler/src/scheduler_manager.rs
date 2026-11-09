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

//! Go `scheduler_manager.go`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::Duration;

use tidb_dxf::{
    get_max_concurrent_task, Task, TaskBase, TaskState, TaskType, MAX_CONCURRENT_TASK_UPPER_BOUND, TASK_STATE_FAILED,
    TASK_STATE_MODIFYING, TASK_STATE_PENDING, TASK_STATE_RESUMING, TASK_STATE_RUNNING,
};
use tidb_gocontext::{CancelFunc, Context};
use tidb_log::{Field, Value};

use crate::balancer::Balancer;
use crate::interface::{get_cleaner_factory, get_scheduler_factory, BatchCleaner, Cleaner, Error, Param};
use crate::scheduler::{on_task_finished, plain, BaseScheduler, ServerInfoSource};
use crate::{NodeManager, Scheduler, SlotManager, TaskManager};

/// Go `CheckTaskRunningInterval`: the interval of loading tasks. Exported
/// for testing.
pub static CHECK_TASK_RUNNING_INTERVAL: RwLock<Duration> = RwLock::new(Duration::from_secs(3));
/// Go `defaultHistorySubtaskTableGcInterval`.
const DEFAULT_HISTORY_SUBTASK_TABLE_GC_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
/// Go `DefaultCleanUpInterval`: the interval of task cleanup.
pub static DEFAULT_CLEAN_UP_INTERVAL: RwLock<Duration> = RwLock::new(Duration::from_secs(10 * 60));
/// Go's `historySubtaskTableGcInterval` call-failpoint, which may change the
/// interval.
#[cfg(any(test, feature = "failpoints"))]
pub static HISTORY_SUBTASK_TABLE_GC_INTERVAL: crate::scheduler::CallHook<dyn Fn(&mut Duration) + Send + Sync> =
    RwLock::new(None);
/// Go's `beforeGetSchedulableTasks` call-failpoint.
#[cfg(any(test, feature = "failpoints"))]
pub static BEFORE_GET_SCHEDULABLE_TASKS: crate::scheduler::CallHook<dyn Fn() + Send + Sync> = RwLock::new(None);
/// Go's `processCleanupTaskBatch` call-failpoint.
#[cfg(any(test, feature = "failpoints"))]
pub static PROCESS_CLEANUP_TASK_BATCH: crate::scheduler::CallHook<dyn Fn() + Send + Sync> = RwLock::new(None);
/// Go's `WaitCleanUpFinished` call-failpoint.
#[cfg(any(test, feature = "failpoints"))]
pub static WAIT_CLEAN_UP_FINISHED: crate::scheduler::CallHook<dyn Fn() + Send + Sync> = RwLock::new(None);
/// Go `defaultCollectMetricsInterval`: metric scraping mostly happens every
/// 15s, so collecting more often is meaningless.
const DEFAULT_COLLECT_METRICS_INTERVAL: Duration = Duration::from_secs(15);

fn read(duration: &RwLock<Duration>) -> Duration {
    *duration.read().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn str_field(name: &str, value: impl Into<String>) -> Field {
    Field::new(name, Value::Str(value.into()))
}

#[derive(Default)]
struct Schedulers {
    scheduler_map: HashMap<i64, Arc<dyn Scheduler>>,
    /// In task order.
    schedulers: Vec<Arc<dyn Scheduler>>,
}

/// Go `Manager`: runs one scheduler per task; at most a limited number at
/// a time.
pub struct Manager {
    ctx: Context,
    cancel: CancelFunc,
    task_mgr: Arc<dyn TaskManager>,
    wg: Mutex<Vec<JoinHandle<()>>>,
    scheduler_wg: Mutex<Vec<JoinHandle<()>>>,
    slot_mgr: Arc<SlotManager>,
    node_mgr: Arc<NodeManager>,
    balancer: Balancer,
    initialized: AtomicBool,
    /// The server ID: `ip:port` now.
    server_id: String,
    logger: tidb_logutil::Logger,
    /// Buffers a finish signal per concurrent task at the largest
    /// `maxConcurrentTask`, so raising the limit after start drops none.
    finish_ch: (crossbeam_channel::Sender<()>, crossbeam_channel::Receiver<()>),
    mu: RwLock<Schedulers>,
    node_res: tidb_dxf::NodeResource,
    /// Go's `infosync` global: the cluster's server infos.
    server_info: Arc<dyn ServerInfoSource>,
}

impl Manager {
    /// Go `NewManager`; `server_info` stands for Go's `infosync` global.
    #[must_use]
    pub fn new(
        ctx: &Context,
        task_mgr: Arc<dyn TaskManager>,
        server_id: &str,
        node_res: tidb_dxf::NodeResource,
        server_info: Arc<dyn ServerInfoSource>,
    ) -> Arc<Self> {
        let mut logger = tidb_logutil::err_verbose_logger();
        if tidb_intest::IN_TEST {
            logger = logger.with_fields(&[str_field("server-id", server_id)]);
        }
        let (sub_ctx, cancel) = Context::with_cancel(ctx);
        let slot_mgr = Arc::new(SlotManager::new());
        let node_mgr = Arc::new(NodeManager::new(server_id));
        let balancer = Balancer::new(Param {
            task_mgr: Arc::clone(&task_mgr),
            node_mgr: Arc::clone(&node_mgr),
            slot_mgr: Arc::clone(&slot_mgr),
            server_id: server_id.to_owned(),
            allocated_slots: false,
            node_res: None,
            task_runtime: None,
        });
        Arc::new(Self {
            ctx: sub_ctx,
            cancel,
            task_mgr,
            wg: Mutex::new(Vec::new()),
            scheduler_wg: Mutex::new(Vec::new()),
            slot_mgr,
            node_mgr,
            balancer,
            initialized: AtomicBool::new(false),
            server_id: server_id.to_owned(),
            logger,
            finish_ch: crossbeam_channel::bounded(MAX_CONCURRENT_TASK_UPPER_BOUND as usize),
            mu: RwLock::new(Schedulers::default()),
            node_res,
            server_info,
        })
    }

    /// Go `GetRunningTaskCnt` (`main_test.go`).
    #[cfg(any(test, feature = "failpoints"))]
    #[must_use]
    pub fn get_running_task_cnt(&self) -> usize {
        self.get_scheduler_count()
    }

    /// Go `DelRunningTask` (`main_test.go`).
    #[cfg(any(test, feature = "failpoints"))]
    pub fn del_running_task(&self, id: i64) {
        self.del_scheduler(id);
    }

    /// Go `DoClean` (`main_test.go`): runs task cleanup synchronously.
    #[cfg(any(test, feature = "failpoints"))]
    pub fn do_clean(&self) {
        self.drain_clean_task_batches();
    }

    fn get_scheduler_count(&self) -> usize {
        self.mu.read().unwrap_or_else(std::sync::PoisonError::into_inner).scheduler_map.len()
    }

    fn add_scheduler(&self, task_id: i64, scheduler: Arc<dyn Scheduler>) {
        let mut mu = self.mu.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        mu.scheduler_map.insert(task_id, Arc::clone(&scheduler));
        mu.schedulers.push(scheduler);
        mu.schedulers.sort_by(|i, j| i.get_task().compare_task(&j.get_task()));
    }

    fn has_scheduler(&self, task_id: i64) -> bool {
        self.mu.read().unwrap_or_else(std::sync::PoisonError::into_inner).scheduler_map.contains_key(&task_id)
    }

    fn del_scheduler(&self, task_id: i64) {
        let mut mu = self.mu.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        mu.scheduler_map.remove(&task_id);
        if let Some(i) = mu.schedulers.iter().position(|scheduler| scheduler.get_task().base.id == task_id) {
            mu.schedulers.remove(i);
        }
    }

    fn clear_schedulers(&self) {
        let mut mu = self.mu.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        mu.scheduler_map.clear();
        mu.schedulers.clear();
    }

    /// Go `getSchedulers`: a copy of the schedulers, in task order.
    pub(crate) fn get_schedulers(&self) -> Vec<Arc<dyn Scheduler>> {
        self.mu.read().unwrap_or_else(std::sync::PoisonError::into_inner).schedulers.clone()
    }

    fn spawn(self: &Arc<Self>, name: &str, f: impl FnOnce(Arc<Self>) + Send + 'static) {
        let manager = Arc::clone(self);
        let handle = std::thread::Builder::new()
            .name(format!("dxf-scheduler-{name}"))
            .spawn(move || f(manager))
            .expect("the scheduler manager thread spawns");
        self.wg.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push(handle);
    }

    /// Go `Start`: the loops that schedule tasks, clean them up, collect
    /// metrics, and maintain and balance the nodes.
    pub fn start(self: &Arc<Self>) {
        // init cached managed nodes
        self.node_mgr.refresh_nodes(&self.ctx, self.task_mgr.as_ref(), &self.slot_mgr);

        self.spawn("schedule", |sm| sm.schedule_task_loop());
        self.spawn("gc", |sm| sm.gc_subtask_history_table_loop());
        self.spawn("cleanup", |sm| sm.clean_task_loop());
        self.spawn("collect", |sm| sm.collect_loop());
        self.spawn("live-nodes", |sm| {
            let ticker = crossbeam_channel::tick(crate::nodes::nodes_check_interval());
            let done = sm.ctx.done();
            loop {
                crossbeam_channel::select! {
                    recv(done) -> _ => return,
                    recv(ticker) -> _ => sm.node_mgr.maintain_live_nodes(&sm.ctx, sm.server_info.as_ref(), sm.task_mgr.as_ref()),
                }
            }
        });
        self.spawn("refresh-nodes", |sm| {
            let ticker = crossbeam_channel::tick(crate::nodes::nodes_check_interval());
            let done = sm.ctx.done();
            loop {
                crossbeam_channel::select! {
                    recv(done) -> _ => return,
                    recv(ticker) -> _ => sm.node_mgr.refresh_nodes(&sm.ctx, sm.task_mgr.as_ref(), &sm.slot_mgr),
                }
            }
        });
        self.spawn("balance", |sm| sm.balancer.balance_loop(&sm.ctx, &sm));
        self.initialized.store(true, Ordering::SeqCst);
    }

    /// Go `Cancel`: used in test to simulate a node shutdown.
    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    /// Go `Stop`.
    pub fn stop(&self) {
        self.cancel.cancel();
        let schedulers = std::mem::take(&mut *self.scheduler_wg.lock().unwrap_or_else(std::sync::PoisonError::into_inner));
        for handle in schedulers {
            let _ = handle.join();
        }
        let loops = std::mem::take(&mut *self.wg.lock().unwrap_or_else(std::sync::PoisonError::into_inner));
        for handle in loops {
            let _ = handle.join();
        }
        self.clear_schedulers();
        self.initialized.store(false, Ordering::SeqCst);

        // clear existing counters on owner change
        tidb_dxf_dxfmetric::WORKER_COUNT.reset();
        tidb_dxf_dxfmetric::FINISHED_TASK_COUNTER.reset();
    }

    /// Go `Initialized`.
    #[must_use]
    pub fn initialized(&self) -> bool {
        self.initialized.load(Ordering::SeqCst)
    }

    /// Go `scheduleTaskLoop`.
    fn schedule_task_loop(self: &Arc<Self>) {
        self.logger.info("schedule task loop start", &[]);
        let ticker = crossbeam_channel::tick(read(&CHECK_TASK_RUNNING_INTERVAL));
        let done = self.ctx.done();
        let task_changed = tidb_dxf_handle::TASK_CHANGED_CH.1.clone();
        loop {
            crossbeam_channel::select! {
                recv(done) -> _ => {
                    self.logger.info("schedule task loop exits", &[]);
                    return;
                }
                recv(ticker) -> _ => {}
                recv(task_changed) -> _ => {}
            }

            #[cfg(any(test, feature = "failpoints"))]
            crate::scheduler::call_hook(&BEFORE_GET_SCHEDULABLE_TASKS, |hook| hook());
            let Ok(schedulable_tasks) = self.get_schedulable_tasks() else {
                continue;
            };
            let _ = self.start_schedulers(schedulable_tasks);
        }
    }

    /// Go `getSchedulableTasks`: the tasks with no scheduler yet; once the
    /// concurrent task limit is reached, only the tasks that need no
    /// resources (reverting, cancelling, pausing, modifying).
    fn get_schedulable_tasks(&self) -> tidb_dxf_storage::Result<Vec<TaskBase>> {
        let task_cnt = self.get_scheduler_count();
        let max_concurrent_task = get_max_concurrent_task() as usize;
        let tasks = if task_cnt >= max_concurrent_task {
            // when we have reached the limit of concurrent tasks, we only handle
            // tasks in states that don't need resources, e.g. reverting/cancelling/
            // pausing/modifying.
            self.task_mgr.get_top_no_need_resource_tasks(&self.ctx)
        } else {
            self.task_mgr.get_top_unfinished_tasks(&self.ctx)
        }
        .map_err(|err| {
            self.logger.warn("get unfinished tasks failed", &[str_field("error", err.to_string())]);
            err
        })?;

        let mut schedulable_tasks = Vec::with_capacity(tasks.len());
        for task in tasks {
            if self.has_scheduler(task.id) {
                continue;
            }
            // we check it before start scheduler, so no need to check it again.
            // see startScheduler.
            // this should not happen normally, unless user modify system table
            // directly.
            if get_scheduler_factory(&task.tp).is_none() {
                self.logger.warn(
                    "unknown task type",
                    &[
                        Field::new("task-id", Value::I64(task.id)),
                        str_field("task-key", task.key.clone()),
                        str_field("task-type", task.tp.to_string()),
                    ],
                );
                self.fail_task(task.id, &task.state, plain("unknown task type"));
                continue;
            }
            schedulable_tasks.push(task);
        }
        Ok(schedulable_tasks)
    }

    /// Go `startSchedulers`.
    fn start_schedulers(self: &Arc<Self>, schedulable_tasks: Vec<TaskBase>) -> tidb_dxf_storage::Result<()> {
        if schedulable_tasks.is_empty() {
            return Ok(());
        }
        self.slot_mgr.update(&self.ctx, &self.node_mgr, self.task_mgr.as_ref()).map_err(|err| {
            self.logger.warn("update used slot failed", &[str_field("error", err.to_string())]);
            err
        })?;
        for task in schedulable_tasks {
            let mut reserved_exec_id = String::new();
            let mut allocate_slots = true;
            if task.state == TASK_STATE_PENDING || task.state == TASK_STATE_RUNNING || task.state == TASK_STATE_RESUMING {
                if self.get_scheduler_count() >= get_max_concurrent_task() as usize {
                    continue;
                }
                let (exec_id, ok) = self.slot_mgr.can_reserve(&task);
                if !ok {
                    // task of low ranking might be able to be scheduled.
                    continue;
                }
                reserved_exec_id = exec_id;
            } else {
                // reverting/cancelling/pausing/modifying, we don't allocate slots for them.
                allocate_slots = false;
                self.logger.info(
                    "start scheduler without allocating slots",
                    &[
                        Field::new("task-id", Value::I64(task.id)),
                        str_field("task-key", task.key.clone()),
                        str_field("state", task.state.to_string()),
                    ],
                );
            }
            self.start_scheduler(&task, allocate_slots, &reserved_exec_id);
        }
        Ok(())
    }

    /// Go `failTask`.
    fn fail_task(&self, id: i64, curr_state: &TaskState, err: Error) {
        match self.task_mgr.fail_task(&self.ctx, id, curr_state, Some(as_dyn(err.as_ref()))) {
            Err(err2) => self.logger.warn(
                "failed to update task state to failed",
                &[Field::new("task-id", Value::I64(id)), str_field("error", err2.to_string())],
            ),
            Ok(()) => on_task_finished(&TASK_STATE_FAILED, Some(as_dyn(err.as_ref()))),
        }
    }

    /// Go `gcSubtaskHistoryTableLoop`.
    fn gc_subtask_history_table_loop(self: &Arc<Self>) {
        self.logger.info("subtask table gc loop start", &[]);
        #[allow(unused_mut)]
        let mut history_subtask_table_gc_interval = DEFAULT_HISTORY_SUBTASK_TABLE_GC_INTERVAL;
        #[cfg(any(test, feature = "failpoints"))]
        crate::scheduler::call_hook(&HISTORY_SUBTASK_TABLE_GC_INTERVAL, |hook| hook(&mut history_subtask_table_gc_interval));
        let ticker = crossbeam_channel::tick(history_subtask_table_gc_interval);
        let done = self.ctx.done();
        loop {
            crossbeam_channel::select! {
                recv(done) -> _ => {
                    self.logger.info("subtask history table gc loop exits", &[]);
                    return;
                }
                recv(ticker) -> _ => match self.task_mgr.gc_subtasks(&self.ctx) {
                    Err(err) => self.logger.warn("subtask history table gc failed", &[str_field("error", err.to_string())]),
                    Ok(()) => self.logger.info("subtask history table gc success", &[]),
                },
            }
        }
    }

    /// Go `startScheduler`.
    fn start_scheduler(self: &Arc<Self>, basic_task: &TaskBase, allocate_slots: bool, reserved_exec_id: &str) {
        let task = match self.task_mgr.get_task_by_id(&self.ctx, basic_task.id) {
            Ok(task) => task,
            Err(err) => {
                self.logger.error(
                    "get task failed",
                    &[
                        Field::new("task-id", Value::I64(basic_task.id)),
                        str_field("task-key", basic_task.key.clone()),
                        str_field("error", err.to_string()),
                    ],
                );
                return;
            }
        };

        let holder_id = tidb_dxf_dxfutil::gen_holder_id("scheduler", task.base.id);
        let session_provider = TaskManagerSessions(Arc::clone(&self.task_mgr));
        let (task_runtime, release_fn) =
            match tidb_dxf_dxfutil::acquire_task_runtime(&session_provider, &task.base.keyspace, &holder_id) {
                Ok(acquired) => acquired,
                Err(err) => {
                    self.logger.warn(
                        "acquire task runtime failed",
                        &[
                            Field::new("task-id", Value::I64(basic_task.id)),
                            str_field("task-key", basic_task.key.clone()),
                            str_field("error", err.to_string()),
                        ],
                    );
                    return;
                }
            };

        let scheduler_factory = get_scheduler_factory(&task.base.tp).expect("checked in getSchedulableTasks");
        let (task_id, task_key, task_state) = (task.base.id, task.base.key.clone(), task.base.state.clone());
        let scheduler = scheduler_factory(
            &self.ctx,
            task,
            Param {
                task_mgr: Arc::clone(&self.task_mgr),
                node_mgr: Arc::clone(&self.node_mgr),
                slot_mgr: Arc::clone(&self.slot_mgr),
                server_id: self.server_id.clone(),
                allocated_slots: allocate_slots,
                node_res: Some(self.node_res.clone()),
                task_runtime: Some(task_runtime),
            },
        );
        if let Err(err) = scheduler.init() {
            self.logger.error("init scheduler failed", &[str_field("error", err.to_string())]);
            self.fail_task(task_id, &task_state, err);
            release_fn();
            return;
        }
        self.add_scheduler(task_id, Arc::clone(&scheduler));
        if allocate_slots {
            self.slot_mgr.reserve(basic_task, reserved_exec_id);
        }
        self.logger.info("task scheduler started", &[Field::new("task-id", Value::I64(task_id)), str_field("task-key", task_key.clone())]);
        let manager = Arc::clone(self);
        let basic_task = basic_task.clone();
        let reserved_exec_id = reserved_exec_id.to_owned();
        let handle = std::thread::Builder::new()
            .name(format!("dxf-scheduler-task-{task_id}"))
            .spawn(move || {
                scheduler.schedule_task();
                let _ = manager.finish_ch.0.try_send(());
                scheduler.close();
                release_fn();
                manager.del_scheduler(task_id);
                if allocate_slots {
                    manager.slot_mgr.un_reserve(&basic_task, &reserved_exec_id);
                }
                tidb_dxf_handle::notify_task_change();
                manager.logger.info("task scheduler exit", &[Field::new("task-id", Value::I64(task_id)), str_field("task-key", task_key)]);
            })
            .expect("the task scheduler thread spawns");
        self.scheduler_wg.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push(handle);
    }

    /// Go `cleanTaskLoop`.
    fn clean_task_loop(self: &Arc<Self>) {
        self.logger.info("cleanup loop start", &[]);
        self.drain_clean_task_batches();
        let ticker = crossbeam_channel::tick(read(&DEFAULT_CLEAN_UP_INTERVAL));
        let done = self.ctx.done();
        let finish = self.finish_ch.1.clone();
        loop {
            crossbeam_channel::select! {
                recv(done) -> _ => {
                    self.logger.info("cleanup loop exits", &[]);
                    return;
                }
                recv(finish) -> _ => self.drain_clean_task_batches(),
                recv(ticker) -> _ => self.drain_clean_task_batches(),
            }
        }
    }

    /// Go `drainCleanTaskBatches`: bounded batches until one is not fully
    /// handled. The cleanup loop runs serially, so draining needs no overall
    /// bound while every batch is fully handled.
    pub(crate) fn drain_clean_task_batches(&self) {
        while self.process_clean_task_batch() {}
    }

    /// Go `processCleanTaskBatch`: one bounded batch of cleanup tasks (such as
    /// global sort's temporary files on S3); whether every task of it moved to
    /// history.
    pub(crate) fn process_clean_task_batch(&self) -> bool {
        let tasks = match self.task_mgr.get_cleanup_tasks(&self.ctx) {
            Ok(tasks) => tasks,
            Err(err) => {
                self.logger.warn("get cleanup tasks failed", &[str_field("error", err.to_string())]);
                return false;
            }
        };
        if tasks.is_empty() {
            return false;
        }
        // Keep the failpoint name stable for existing integration tests.
        #[cfg(any(test, feature = "failpoints"))]
        crate::scheduler::call_hook(&PROCESS_CLEANUP_TASK_BATCH, |hook| hook());
        self.logger.info("task cleanup starts", &[]);
        let total = tasks.len();
        let transferred_task_count = match self.clean_finished_tasks(tasks) {
            Ok(count) => count,
            Err(err) => {
                self.logger.warn("task cleanup failed", &[str_field("error", err.to_string())]);
                return false;
            }
        };
        #[cfg(any(test, feature = "failpoints"))]
        crate::scheduler::call_hook(&WAIT_CLEAN_UP_FINISHED, |hook| hook());
        let batch_fully_handled = transferred_task_count == total;
        self.logger.info(
            "task cleanup finished",
            &[
                Field::new("transferred-task-count", Value::I64(transferred_task_count as i64)),
                Field::new("total-task-count", Value::I64(total as i64)),
                Field::new("batch-fully-handled", Value::Bool(batch_fully_handled)),
            ],
        );
        batch_fully_handled
    }

    /// Go `cleanFinishedTasks`: runs the cleaners and moves the cleaned tasks
    /// to history; the count of tasks moved, zero when the move failed.
    pub(crate) fn clean_finished_tasks(&self, tasks: Vec<Task>) -> tidb_dxf_storage::Result<usize> {
        struct BatchGroup {
            cleaner: Arc<dyn Cleaner>,
            tasks: Vec<Task>,
        }

        let mut single_cleaner_tasks: Vec<(Task, Arc<dyn Cleaner>)> = Vec::new();
        let mut batch_cleaner_task_groups: Vec<(TaskType, BatchGroup)> = Vec::new();
        let mut cleaned_tasks = Vec::with_capacity(tasks.len());
        let mut first_err: Option<Error> = None;
        for task in tasks {
            self.logger.info("cleanup task", &[Field::new("task-id", Value::I64(task.base.id)), str_field("task-key", task.base.key.clone())]);
            if let Some((_, group)) = batch_cleaner_task_groups.iter_mut().find(|(tp, _)| *tp == task.base.tp) {
                group.tasks.push(task);
                continue;
            }

            let Some(cleaner_factory) = get_cleaner_factory(&task.base.tp) else {
                cleaned_tasks.push(task);
                continue;
            };
            let cleaner = cleaner_factory();
            if cleaner.as_batch_cleaner().is_some() {
                batch_cleaner_task_groups.push((task.base.tp.clone(), BatchGroup { cleaner, tasks: vec![task] }));
                continue;
            }
            single_cleaner_tasks.push((task, cleaner));
        }

        for (mut task, cleaner) in single_cleaner_tasks {
            if let Err(err) = cleaner.clean(&self.ctx, &mut task) {
                // maybe consider continue cleaning other tasks on error later.
                first_err = Some(err);
                break;
            }
            cleaned_tasks.push(task);
        }
        if first_err.is_none() {
            for (_, mut group) in batch_cleaner_task_groups {
                let batch: &dyn BatchCleaner = group.cleaner.as_batch_cleaner().expect("grouped as a batch cleaner");
                if let Err(err) = batch.batch_clean(&self.ctx, &mut group.tasks) {
                    first_err = Some(err);
                    break;
                }
                cleaned_tasks.extend(group.tasks);
            }
        }
        if let Some(err) = first_err {
            // normally ScheduleEventCounter requires a task ID, but since scheduler
            // will delete counters after task finished, we use "-" to indicate
            // it's not related to any specific task.
            tidb_dxf_dxfmetric::SCHEDULE_EVENT_COUNTER
                .with_label_values(&["-", tidb_dxf_dxfmetric::EVENT_CLEANUP_FAILED])
                .inc();
            self.logger.warn("task cleanup failed", &[str_field("error", err.to_string())]);
        }

        #[cfg(any(test, feature = "failpoints"))]
        fail::fail_point!("mockTransferErr", |_| Err(tidb_dxf_storage::StorageError::Other("transfer err".to_owned())));

        self.task_mgr.transfer_tasks2_history(&self.ctx, &cleaned_tasks)?;
        Ok(cleaned_tasks.len())
    }

    /// Go `collectLoop`.
    fn collect_loop(self: &Arc<Self>) {
        self.logger.info("collect loop start", &[]);
        let ticker = crossbeam_channel::tick(DEFAULT_COLLECT_METRICS_INTERVAL);
        let mut const_labels = HashMap::new();
        // we might create multiple domains in the same process in tests, we will
        // add a unique label to avoid conflict.
        if tidb_intest::IN_TEST {
            static NEXT_COLLECTOR: AtomicU64 = AtomicU64::new(1);
            const_labels.insert("server_id".to_owned(), NEXT_COLLECTOR.fetch_add(1, Ordering::Relaxed).to_string());
        }
        let metric_collector = Arc::new(tidb_dxf_dxfmetric::Collector::new(const_labels));
        let registered = prometheus::default_registry().register(Box::new(SharedCollector(Arc::clone(&metric_collector))));
        let done = self.ctx.done();
        loop {
            crossbeam_channel::select! {
                recv(done) -> _ => {
                    self.logger.info("collect loop exits", &[]);
                    break;
                }
                recv(ticker) -> _ => self.collect(&metric_collector),
            }
        }
        if registered.is_ok() {
            let _ = prometheus::default_registry().unregister(Box::new(SharedCollector(metric_collector)));
        }
    }

    /// Go `collect`.
    fn collect(&self, metric_collector: &tidb_dxf_dxfmetric::Collector) {
        let tasks = self.task_mgr.get_all_tasks(&self.ctx).unwrap_or_else(|err| {
            self.logger.warn("get all tasks failed", &[str_field("error", err.to_string())]);
            Vec::new()
        });
        let subtasks = match self.task_mgr.get_all_subtasks(&self.ctx) {
            Ok(subtasks) => subtasks,
            Err(err) => {
                self.logger.warn("get all subtasks failed", &[str_field("error", err.to_string())]);
                return;
            }
        };
        metric_collector.update_info(tasks.clone(), subtasks);

        if tidb_config_kerneltype::is_next_gen() {
            self.collect_worker_metrics(tasks);
        }
    }

    /// Go `collectWorkerMetrics`.
    fn collect_worker_metrics(&self, tasks: Vec<TaskBase>) {
        let manager = match tidb_dxf_storage::get_task_manager() {
            Ok(manager) => manager,
            Err(err) => {
                self.logger.warn("failed to get task manager", &[str_field("error", err.to_string())]);
                return;
            }
        };
        let (node_count, node_cpu) = match tidb_dxf_handle::get_nodes_info(&self.ctx, &manager) {
            Ok(info) => info,
            Err(err) => {
                self.logger.warn("failed to get nodes info", &[str_field("error", err.to_string())]);
                return;
            }
        };
        let mut scheduled_tasks: Vec<TaskBase> = tasks
            .into_iter()
            .filter(|t| t.state == TASK_STATE_RUNNING || t.state == TASK_STATE_MODIFYING)
            .collect();
        scheduled_tasks.sort_by(TaskBase::compare);
        let required_nodes = tidb_dxf_handle::calculate_required_nodes(&scheduled_tasks, node_cpu);
        tidb_dxf_dxfmetric::WORKER_COUNT.with_label_values(&["required"]).set(required_nodes as f64);
        tidb_dxf_dxfmetric::WORKER_COUNT.with_label_values(&["current"]).set(node_count as f64);
    }

    #[cfg(test)]
    pub(crate) fn slot_mgr(&self) -> &Arc<SlotManager> {
        &self.slot_mgr
    }

    #[cfg(test)]
    pub(crate) fn node_mgr(&self) -> &Arc<NodeManager> {
        &self.node_mgr
    }

    #[cfg(test)]
    pub(crate) fn add_scheduler_for_test(&self, task_id: i64, scheduler: Arc<dyn Scheduler>) {
        self.add_scheduler(task_id, scheduler);
    }

    /// Go `MockScheduler`: a scheduler of the task, only used for tests.
    #[must_use]
    pub fn mock_scheduler(&self, task: Task) -> BaseScheduler {
        BaseScheduler::new(
            &self.ctx,
            task,
            Param {
                task_mgr: Arc::clone(&self.task_mgr),
                node_mgr: Arc::clone(&self.node_mgr),
                slot_mgr: Arc::clone(&self.slot_mgr),
                server_id: self.server_id.clone(),
                allocated_slots: false,
                node_res: None,
                task_runtime: None,
            },
        )
    }
}

fn as_dyn<'a>(err: &'a (dyn std::error::Error + Send + Sync + 'static)) -> &'a (dyn std::error::Error + 'static) {
    err
}

/// Go's `TaskManager` as `dxfutil`'s session provider.
struct TaskManagerSessions(Arc<dyn TaskManager>);

impl tidb_dxf_dxfutil::SessionProvider for TaskManagerSessions {
    fn with_new_session(
        &self,
        f: &mut dyn FnMut(&dyn tidb_dxf_storage::SessionContext) -> tidb_dxf_storage::Result<()>,
    ) -> tidb_dxf_storage::Result<()> {
        self.0.with_new_session(f)
    }
}

/// The shared collector, registered with prometheus (Go `metrics.Register`).
struct SharedCollector(Arc<tidb_dxf_dxfmetric::Collector>);

impl prometheus::core::Collector for SharedCollector {
    fn desc(&self) -> Vec<&prometheus::core::Desc> {
        self.0.desc()
    }

    fn collect(&self) -> Vec<prometheus::proto::MetricFamily> {
        self.0.collect()
    }
}

#[cfg(test)]
#[path = "scheduler_manager_nokit_tests.rs"]
mod nokit_tests;
