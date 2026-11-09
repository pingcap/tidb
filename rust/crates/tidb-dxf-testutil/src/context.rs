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

//! Go `context.go`: a DXF cluster of in-process nodes over one store, each
//! running a task executor manager, and the owners a scheduler manager.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::Duration;

use tidb_dxf::{NodeResource, Step, Subtask};
use tidb_dxf_storage::TaskManager;
use tidb_gocontext::Context;
use tidb_gomock::Controller;

const GB: i64 = 1 << 30;

/// Go `tidbNode`.
#[derive(Clone)]
struct TidbNode {
    id: String,
    owner: bool,
    exe_mgr: Arc<tidb_dxf_taskexecutor::Manager>,
    sch_mgr: Option<Arc<tidb_dxf_scheduler::Manager>>,
}

#[derive(Default)]
struct Nodes {
    /// to test network partition, we allow multiple owners
    owner_indices: HashMap<String, usize>,
    node_indices: HashMap<String, usize>,
    nodes: Vec<TidbNode>,
}

/// The server infos the scheduler managers would read; the nodes' exec IDs
/// come from Go's `scheduler.MockServerInfo` instead
/// (`mockTaskExecutorNodes`).
struct NoServerInfo;

impl tidb_dxf_scheduler::ServerInfoSource for NoServerInfo {
    fn get_all_server_info(
        &self,
    ) -> Result<HashMap<String, tidb_domain_serverinfo::serverinfo::ServerInfo>, String> {
        Err("the DXF test context mocks the task executor nodes".to_owned())
    }
}

/// Go `TestDXFContext`.
pub struct TestDxfContext {
    /// Go `Ctx`.
    pub ctx: Context,
    /// Go `TaskMgr`.
    pub task_mgr: Arc<TaskManager>,
    /// Go `MockCtrl`.
    pub mock_ctrl: Arc<Controller>,
    /// Go `TestContext`.
    pub test_context: Arc<TestContext>,

    id_allocator: AtomicI32,
    /// in real case, when node scale in/out, the node might use the same IP
    /// or host name such as using K8S, so we use this to simulate this case.
    node_id_pool: (crossbeam_channel::Sender<String>, crossbeam_channel::Receiver<String>),
    wg: Mutex<Vec<JoinHandle<()>>>,
    mu: RwLock<Nodes>,
    mock_cpu_num: isize,
    restore: Mutex<Vec<Box<dyn FnOnce() + Send>>>,
}

/// Go `NewDXFContextWithRandomNodes`.
#[must_use]
pub fn new_dxf_context_with_random_nodes(task_mgr: Arc<TaskManager>, min_cnt: usize, max_cnt: usize) -> Arc<TestDxfContext> {
    let mut c = new_test_dxf_context0(task_mgr);
    let node_num = rand::random_range(min_cnt..=max_cnt);
    tidb_logutil::bg_logger().info(&format!("dxf context with random node num: {node_num}"), &[]);
    c.init(node_num, 16, true);
    c
}

/// Go `NewTestDXFContext`: `task_mgr` is the store's task manager (Go creates
/// the store and a pool of 10 sessions over it).
#[must_use]
pub fn new_test_dxf_context(task_mgr: Arc<TaskManager>, node_num: usize, cpu_count: isize, reduce_check_interval: bool) -> Arc<TestDxfContext> {
    let mut c = new_test_dxf_context0(task_mgr);
    c.init(node_num, cpu_count, reduce_check_interval);
    c
}

fn new_test_dxf_context0(task_mgr: Arc<TaskManager>) -> Arc<TestDxfContext> {
    Arc::new(TestDxfContext {
        ctx: Context::background(),
        task_mgr,
        mock_ctrl: Controller::new(),
        test_context: Arc::new(TestContext::default()),
        id_allocator: AtomicI32::new(0),
        node_id_pool: crossbeam_channel::bounded(100),
        wg: Mutex::default(),
        mu: RwLock::default(),
        mock_cpu_num: 0,
        restore: Mutex::default(),
    })
}

fn node_resource(cpu: isize) -> NodeResource {
    NodeResource::new(cpu, 32 * GB, 100 * GB as u64)
}

impl TestDxfContext {
    fn init(self: &mut Arc<Self>, node_num: usize, cpu_count: isize, reduce_check_interval: bool) {
        let mut restore: Vec<Box<dyn FnOnce() + Send>> = Vec::new();
        if reduce_check_interval {
            // make test faster
            restore.push(Box::new(reduce_check_interval_for_test()));
        }
        // all nodes are isometric
        Arc::get_mut(self).expect("the context is not shared yet").mock_cpu_num = cpu_count;
        let origin_node_resource = tidb_dxf_storage::get_node_resource();
        tidb_dxf_storage::set_node_resource(node_resource(cpu_count));
        restore.push(Box::new(move || tidb_dxf_storage::set_node_resource(origin_node_resource)));
        fail::cfg("mockNumCpu", &format!("return({cpu_count})")).expect("the failpoint is valid");
        fail::cfg("mockTaskExecutorNodes", "return()").expect("the failpoint is valid");
        restore.push(Box::new(|| {
            fail::remove("mockNumCpu");
            fail::remove("mockTaskExecutorNodes");
        }));
        tidb_dxf_storage::set_task_manager(Some(Arc::clone(&self.task_mgr)));
        *self.restore.lock().unwrap() = restore;

        for _ in 0..node_num {
            let id = self.get_node_id();
            self.scale_out_by(&id, false);
        }
        self.elect_if_needed();
    }

    fn get_node_id(&self) -> String {
        match self.node_id_pool.1.try_recv() {
            Ok(id) => id,
            Err(_) => format!(":{}", 4000 - 1 + self.id_allocator.fetch_add(1, Ordering::SeqCst) + 1),
        }
    }

    fn recycle_node_id(&self, id: &str) {
        let _ = self.node_id_pool.0.try_send(id.to_owned());
    }

    /// Go `ScaleOut`: scales out nodes, and elects an owner if required.
    pub fn scale_out(&self, node_num: usize) {
        for _ in 0..node_num {
            let id = self.get_node_id();
            self.scale_out_by(&id, false);
        }
        self.elect_if_needed();
    }

    /// Go `ScaleOutBy`: scales out a node, as owner if required.
    ///
    /// # Panics
    /// The node's executor manager cannot start.
    pub fn scale_out_by(&self, id: &str, owner: bool) {
        tidb_logutil::bg_logger().info(&format!("scale out node of id = {id}, owner = {owner}"), &[]);
        let exe_mgr = tidb_dxf_taskexecutor::Manager::new(
            &self.ctx,
            id,
            Arc::clone(&self.task_mgr) as Arc<dyn tidb_dxf_taskexecutor::TaskTable>,
            node_resource(self.mock_cpu_num),
        );
        exe_mgr.init_meta().unwrap();
        exe_mgr.start().unwrap();
        let sch_mgr = owner.then(|| self.new_scheduler_manager(id));
        let node = TidbNode { id: id.to_owned(), owner, exe_mgr, sch_mgr };

        {
            let mut mu = self.mu.write().unwrap();
            mu.nodes.push(node);
            let idx = mu.nodes.len() - 1;
            mu.node_indices.insert(id.to_owned(), idx);
            if owner {
                mu.owner_indices.insert(id.to_owned(), idx);
            }
        }

        self.update_live_exec_ids();
    }

    fn new_scheduler_manager(&self, id: &str) -> Arc<tidb_dxf_scheduler::Manager> {
        let sch_mgr = tidb_dxf_scheduler::Manager::new(
            &self.ctx,
            Arc::clone(&self.task_mgr) as Arc<dyn tidb_dxf_scheduler::TaskManager>,
            id,
            node_resource(self.mock_cpu_num),
            Arc::new(NoServerInfo),
        );
        sch_mgr.start();
        sch_mgr
    }

    fn update_live_exec_ids(&self) {
        let mu = self.mu.read().unwrap();
        let exec_ids = mu.nodes.iter().map(|node| node.id.clone()).collect();
        tidb_dxf_scheduler::set_mock_server_info(exec_ids);
    }

    /// Go `ScaleIn`: scales in the last added nodes, and elects a new owner
    /// if required.
    pub fn scale_in(&self, node_num: usize) {
        for _ in 0..node_num {
            let Some(id) = self.mu.read().unwrap().nodes.last().map(|node| node.id.clone()) else {
                return;
            };
            self.scale_in_by(&id);
        }
    }

    /// Go `ScaleInBy`: scales in a node, and elects a new owner if required.
    pub fn scale_in_by(&self, id: &str) {
        let node = {
            let mut mu = self.mu.write().unwrap();
            let Some(&idx) = mu.node_indices.get(id) else {
                tidb_logutil::bg_logger().info(&format!("scale in failed, cannot find node {id}"), &[]);
                return;
            };
            let node = mu.nodes.remove(idx);
            let (node_indices, owner_indices) = mu.nodes.iter().enumerate().fold(
                (HashMap::new(), HashMap::new()),
                |(mut node_indices, mut owner_indices), (i, n)| {
                    node_indices.insert(n.id.clone(), i);
                    if n.owner {
                        owner_indices.insert(n.id.clone(), i);
                    }
                    (node_indices, owner_indices)
                },
            );
            mu.node_indices = node_indices;
            mu.owner_indices = owner_indices;
            self.recycle_node_id(id);
            node
        };

        self.update_live_exec_ids();

        tidb_logutil::bg_logger().info(&format!("scale in node of id = {}, owner = {}", node.id, node.owner), &[]);
        node.exe_mgr.stop();
        if let Some(sch_mgr) = node.sch_mgr.filter(|_| node.owner) {
            sch_mgr.stop();
        }

        self.elect_if_needed();
    }

    /// Go `AsyncChangeOwner`: changes the owner on another thread.
    pub fn async_change_owner(self: &Arc<Self>) {
        let c = Arc::clone(self);
        self.run_with_log(move || c.change_owner());
    }

    /// Go `ChangeOwner`: resigns all current owners and makes a random node
    /// the owner.
    pub fn change_owner(&self) {
        tidb_logutil::bg_logger().info("changing owner", &[]);
        {
            let mut mu = self.mu.write().unwrap();
            if mu.nodes.is_empty() {
                tidb_logutil::bg_logger().info("there no node, cannot change owner", &[]);
                return;
            }
            let owner_indices: Vec<usize> = mu.owner_indices.values().copied().collect();
            for idx in owner_indices {
                if let Some(sch_mgr) = mu.nodes[idx].sch_mgr.take() {
                    sch_mgr.stop();
                }
                mu.nodes[idx].owner = false;
            }
            mu.owner_indices.clear();
        }

        self.elect_if_needed();
    }

    /// Go `AsyncShutdown`: cancels the node's managers here (the caller is a
    /// failpoint), then scales it in on another thread.
    pub fn async_shutdown(self: &Arc<Self>, id: &str) {
        tidb_logutil::bg_logger().info(&format!("shuting down node of id {id}"), &[]);
        let Some(node) = self.get_node(id) else {
            return;
        };
        node.exe_mgr.cancel();
        if node.owner {
            if let Some(sch_mgr) = &node.sch_mgr {
                sch_mgr.cancel();
            }
        }
        let (c, id) = (Arc::clone(self), id.to_owned());
        self.run_with_log(move || c.scale_in_by(&id));
    }

    /// Go `GetRandNodeIDs`: `limit` random node IDs, all of them when
    /// `limit` is larger.
    #[must_use]
    pub fn get_rand_node_ids(&self, limit: usize) -> HashSet<String> {
        let mu = self.mu.read().unwrap();
        let mut ids: Vec<String> = mu.nodes.iter().map(|node| node.id.clone()).collect();
        // Go `rand.Shuffle`.
        for i in (1..ids.len()).rev() {
            let j = rand::random_range(0..=i);
            ids.swap(i, j);
        }
        ids.into_iter().take(limit).collect()
    }

    /// Go `GetNodeIDByIdx`.
    #[must_use]
    pub fn get_node_id_by_idx(&self, idx: usize) -> String {
        self.mu.read().unwrap().nodes[idx].id.clone()
    }

    /// Go `NodeCount`.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.mu.read().unwrap().nodes.len()
    }

    fn get_node(&self, id: &str) -> Option<TidbNode> {
        let mu = self.mu.read().unwrap();
        let node = mu.node_indices.get(id).map(|&idx| mu.nodes[idx].clone());
        if node.is_none() {
            tidb_logutil::bg_logger().info(&format!("cannot find node of id {id}"), &[]);
        }
        node
    }

    fn elect_if_needed(&self) {
        let (owner_id, new_owner_idx) = {
            let mut mu = self.mu.write().unwrap();
            if mu.nodes.is_empty() || !mu.owner_indices.is_empty() {
                return;
            }

            let new_owner_idx = rand::random_range(0..mu.nodes.len());
            let owner_id = mu.nodes[new_owner_idx].id.clone();
            mu.owner_indices.insert(owner_id.clone(), new_owner_idx);
            let sch_mgr = self.new_scheduler_manager(&owner_id);
            let owner_node = &mut mu.nodes[new_owner_idx];
            owner_node.sch_mgr = Some(sch_mgr);
            owner_node.owner = true;
            (owner_id, new_owner_idx)
        };

        tidb_logutil::bg_logger().info(&format!("new owner elected, id = {owner_id}, newOwnerIdx = {new_owner_idx}"), &[]);
    }

    fn run_with_log(&self, f: impl FnOnce() + Send + 'static) {
        let handle = std::thread::spawn(move || {
            if let Err(panic) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
                tidb_logutil::log_recovered_panic("dxf test context", panic.as_ref());
            }
        });
        self.wg.lock().unwrap().push(handle);
    }

    /// Go `WaitAsyncOperations`.
    pub fn wait_async_operations(&self) {
        let handles = std::mem::take(&mut *self.wg.lock().unwrap());
        for handle in handles {
            let _ = handle.join();
        }
    }

    fn close(&self) {
        self.wait_async_operations();
        let mut mu = self.mu.write().unwrap();
        for node in &mu.nodes {
            node.exe_mgr.stop();
            if node.owner {
                if let Some(sch_mgr) = &node.sch_mgr {
                    sch_mgr.stop();
                }
            }
        }
        *mu = Nodes::default();
    }
}

impl Drop for TestDxfContext {
    /// Go's `t.Cleanup`: `ctrl.Finish()` and `close()`, then the restores.
    fn drop(&mut self) {
        self.close();
        if !std::thread::panicking() {
            self.mock_ctrl.finish();
        }
        for restore in std::mem::take(&mut *self.restore.lock().unwrap()).into_iter().rev() {
            restore();
        }
    }
}

/// Go `TestContext`: the variables disttask tests share.
#[derive(Default)]
pub struct TestContext {
    /// taskID/step -> subtask map.
    subtasks_has_run: RwLock<HashMap<String, HashSet<i64>>>,
    /// Go `CallTime`: for plan err handling tests.
    pub call_time: Mutex<i64>,
}

impl TestContext {
    /// Go `CollectSubtask`.
    pub fn collect_subtask(&self, subtask: &Subtask) {
        let key = get_task_step_key(subtask.base.task_id, subtask.base.step);
        self.subtasks_has_run.write().unwrap().entry(key).or_default().insert(subtask.base.id);
    }

    /// Go `CollectedSubtaskCnt`.
    #[must_use]
    pub fn collected_subtask_cnt(&self, task_id: i64, step: Step) -> usize {
        let key = get_task_step_key(task_id, step);
        self.subtasks_has_run.read().unwrap().get(&key).map_or(0, HashSet::len)
    }
}

/// Go `getTaskStepKey`.
fn get_task_step_key(id: i64, step: Step) -> String {
    format!("{id}/{}", step.0)
}

fn set(duration: &RwLock<Duration>, value: Duration) -> Duration {
    std::mem::replace(&mut *duration.write().unwrap(), value)
}

/// Go `ReduceCheckInterval`: the scheduler's and executor's intervals for
/// test, with the restore Go's `t.Cleanup` runs.
#[must_use]
pub fn reduce_check_interval_for_test() -> impl FnOnce() + Send {
    let ms = Duration::from_millis;
    let intervals: Vec<(&'static RwLock<Duration>, Duration)> = vec![
        (&tidb_dxf_scheduler::CHECK_TASK_RUNNING_INTERVAL, ms(100)),
        (&tidb_dxf_scheduler::CHECK_TASK_FINISHED_INTERVAL, ms(100)),
        (&tidb_dxf_scheduler::DEFAULT_CLEAN_UP_INTERVAL, ms(200)),
        (&tidb_dxf_taskexecutor::TASK_CHECK_INTERVAL, ms(10)),
        (&tidb_dxf_taskexecutor::MAX_SUBTASK_CHECK_INTERVAL, ms(10)),
        (&tidb_dxf_taskexecutor::SUBTASK_CHECK_INTERVAL, ms(10)),
        (&tidb_dxf_taskexecutor::DETECT_PARAM_MODIFY_INTERVAL, ms(10)),
    ];
    let backups: Vec<(&'static RwLock<Duration>, Duration)> =
        intervals.into_iter().map(|(interval, value)| (interval, set(interval, value))).collect();
    move || {
        for (interval, backup) in backups {
            set(interval, backup);
        }
    }
}
