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

//! `MemArbitrator` arbitration core and task execution: quota allocation,
//! priority reclaim, GC/heap reclaim, the task queue executor and the
//! control-loop drivers (`run_one_round` / `async_run` / `auto_run` /
//! `stop`). Split out of `arbitrator.rs`; mirrors the arbitration and task
//! sections of Go `pkg/util/memory/arbitrator.go`.

use super::*;

impl MemArbitrator {
    // ----- arbitration core -----

    /// Go `allocateFromArbitrator`.
    pub(crate) fn allocate_from_arbitrator(&self, remain_bytes: i64) -> (bool, i64) {
        let mut reclaimed = 0i64;
        let mut ok = false;
        {
            let _g = self.mu.lock().unwrap();
            let available = self.quota_available();
            if remain_bytes <= available {
                self.do_alloc(remain_bytes);
                reclaimed += remain_bytes;
                ok = true;
            } else if available > 0 {
                self.do_alloc(available);
                reclaimed += available;
            }
        }
        (ok, reclaimed)
    }

    /// Go `doReclaimMemByPriority`.
    pub(crate) fn do_reclaim_mem_by_priority(
        &self,
        target: &Arc<RootPoolEntry>,
        remain_bytes: i64,
    ) {
        let mut under_reclaim = 0i64;

        // Check pool entries already under cancel.
        if self.under_cancel.lock().unwrap().num > 0 {
            let now = self.inner_time();
            let entries: Vec<Arc<RootPoolEntry>> = self
                .under_cancel
                .lock()
                .unwrap()
                .entries
                .values()
                .cloned()
                .collect();
            for entry in entries {
                let mut st = entry.arbitrator_mu.lock().unwrap();
                if st.under_cancel.fail {
                    continue;
                }
                let deadline = st.under_cancel.start_time + DEF_KILL_CANCEL_CHECK_TIMEOUT;
                if now >= deadline {
                    let actions = self.actions.lock().unwrap().clone();
                    (actions.warn)(&format!(
                        "Failed to `CANCEL` root pool due to timeout: uid={} name={}",
                        entry.pool.uid(),
                        entry.pool.name()
                    ));
                    st.under_cancel.fail = true;
                    continue;
                }
                under_reclaim += st.under_cancel.reclaim;
            }
        }

        if under_reclaim >= remain_bytes {
            return;
        }

        let target_prio = target.mem_priority();
        for prio in PRIORITIES {
            if prio >= target_prio {
                break;
            }
            let mut pos = self.entry_map.max_quota_shard_index;
            while pos > self.entry_map.min_quota_shard_index_to_check {
                pos -= 1;
                let entries: Vec<(u64, Arc<RootPoolEntry>)> = self.entry_map.quota_shards
                    [prio as usize][pos]
                    .lock()
                    .unwrap()
                    .entries
                    .iter()
                    .map(|(k, v)| (*k, Arc::clone(v)))
                    .collect();
                for (_uid, entry) in entries {
                    if entry.arbitrator_mu.lock().unwrap().under_cancel.start || entry.not_running()
                    {
                        continue;
                    }
                    let ctx = entry.load_ctx();
                    if ctx_available(&ctx) {
                        let ctx = ctx.unwrap();
                        self.exec_metrics.cancel_priority[prio as usize].fetch_add(1, SeqCst);
                        ctx.stop(ArbitratorStopReason::PriorityCancel);
                        if self.remove_task(&entry) {
                            entry.wind_up(0, ArbitrateResult::Fail);
                        }
                        let quota = entry.arbitrator_mu.lock().unwrap().quota;
                        self.add_under_cancel(&entry, quota, self.inner_time());
                        under_reclaim += quota;
                        if under_reclaim >= remain_bytes {
                            return;
                        }
                    }
                }
            }
        }
    }

    /// Go `allocateFromPrivilegedBudget`.
    pub(crate) fn allocate_from_privileged_budget(
        &self,
        target: &Arc<RootPoolEntry>,
        remain_bytes: i64,
    ) -> (bool, i64) {
        let mut ok = false;
        {
            let mut pe = self.privileged_entry.lock().unwrap();
            match pe.as_ref() {
                Some(e) if Arc::ptr_eq(e, target) => ok = true,
                None if target.ctx.prefer_privilege.load(SeqCst)
                    && target.enter_exec_privileged() =>
                {
                    *pe = Some(Arc::clone(target));
                    ok = true;
                }
                _ => {}
            }
        }
        if !ok {
            return (false, 0);
        }
        self.alloc(remain_bytes);
        (true, remain_bytes)
    }

    /// Go `ableToGC`.
    pub(crate) fn able_to_gc(&self) -> bool {
        let small = self.pool_alloc_stats.read().unwrap().small_pool_limit;
        self.mu_released
            .load(SeqCst)
            .wrapping_sub(self.mu_last_gc.load(SeqCst))
            >= small as u64
    }

    pub(crate) fn gc(&self) {
        self.mu_last_gc.store(self.mu_released.load(SeqCst), SeqCst);
        let actions = self.actions.lock().unwrap().clone();
        if let Some(gc) = &actions.gc {
            gc();
        }
        self.exec_metrics.action_gc.fetch_add(1, SeqCst);
    }

    pub(crate) fn reclaim_heap(&self) {
        self.gc();
        self.refresh_runtime_mem_stats();
    }

    /// Go `tryRuntimeGC`.
    pub(crate) fn try_runtime_gc(&self) -> bool {
        if self.able_to_gc() {
            self.update_tracked_heap_stats();
            self.reclaim_heap();
            return true;
        }
        false
    }

    /// Go `arbitrate`.
    pub(crate) fn arbitrate(&self, target: &Arc<RootPoolEntry>) -> (bool, i64) {
        let mut reclaimed_bytes = 0i64;
        let mut remain_bytes = target.request.quota.load(SeqCst);

        let mut only_privileged_budget = false;
        while remain_bytes > self.heap_available() {
            if !self.try_runtime_gc() {
                only_privileged_budget = true;
                break;
            }
        }

        {
            let mut ok = false;
            if self.exec_mu.lock().unwrap().mode == ArbitratorWorkMode::Priority {
                let (o, reclaimed) = self.allocate_from_privileged_budget(target, remain_bytes);
                ok = o;
                reclaimed_bytes += reclaimed;
                remain_bytes -= reclaimed;
            }
            if ok {
                return (true, reclaimed_bytes);
            } else if only_privileged_budget {
                return (false, reclaimed_bytes);
            }
        }

        loop {
            let (ok, reclaimed) = self.allocate_from_arbitrator(remain_bytes);
            reclaimed_bytes += reclaimed;
            remain_bytes -= reclaimed;
            if ok {
                return (true, reclaimed_bytes);
            }
            if !self.try_runtime_gc() {
                break;
            }
        }

        (false, reclaimed_bytes)
    }

    // ----- task execution -----

    /// Go `doCancelPendingTasks`.
    pub(crate) fn do_cancel_pending_tasks(
        &self,
        prio: ArbitrationPriority,
        wait_averse: bool,
    ) -> i64 {
        let mut cnt = 0i64;
        let reason = if wait_averse {
            ArbitratorStopReason::WaitAverseCancel
        } else {
            ArbitratorStopReason::StandardCancel
        };

        loop {
            let mut batch: Vec<Arc<RootPoolEntry>> = Vec::with_capacity(64);
            {
                let mut tasks = self.tasks.lock().unwrap();
                loop {
                    let front = if wait_averse {
                        tasks.fifo_wait_averse.front()
                    } else {
                        tasks.fifo_by_priority[prio as usize].front()
                    };
                    let Some(entry) = front else { break };
                    if Self::remove_task_impl(&mut tasks, &entry) {
                        batch.push(entry);
                    }
                    if batch.len() == 64 {
                        break;
                    }
                }
            }

            let size = batch.len();
            for entry in &batch {
                let ctx = entry.load_ctx();
                if ctx_available(&ctx) {
                    ctx.unwrap().stop(reason);
                }
                entry.wind_up(0, ArbitrateResult::Fail);
            }
            cnt += size as i64;
            if size != 64 {
                break;
            }
        }
        cnt
    }

    /// Go `doExecuteFirstTask`.
    pub(crate) fn do_execute_first_task(&self) -> bool {
        if self.tasks.lock().unwrap().fifo_tasks.approx_empty() {
            return false;
        }
        let Some(entry) = self.extract_first_task_entry() else {
            return false;
        };

        if entry.arbitrator_mu.lock().unwrap().destroyed {
            if self.remove_task(&entry) {
                entry.wind_up(0, ArbitrateResult::Fail);
            }
            return true;
        }

        let mut exec = false;
        {
            let _g = entry.request.task_mu.lock().unwrap();
            let (ok, reclaimed_bytes) = self.arbitrate(&entry);
            if ok {
                exec = true;
                if self.remove_task(&entry) {
                    if self.exec_mu.lock().unwrap().mode == ArbitratorWorkMode::Priority {
                        // Go reads `taskMu.fifoByPriority.priority`, which
                        // survives task removal.
                        let prio = entry
                            .task_mu
                            .lock()
                            .unwrap()
                            .fifo_priority
                            .unwrap_or(ArbitrationPriority::Medium);
                        self.exec_metrics.task_succ_by_priority[prio as usize].fetch_add(1, SeqCst);
                    }
                    self.entry_map.add_quota(&entry, reclaimed_bytes);
                    entry.wind_up(reclaimed_bytes, ArbitrateResult::Ok);
                } else {
                    self.release(reclaimed_bytes);
                }
            } else {
                self.release(reclaimed_bytes);
                self.update_blocked_at();
                self.do_reclaim_by_work_mode(&entry, reclaimed_bytes);
            }
        }
        exec
    }

    /// Go `doReclaimNonBlockingTasks`.
    pub(crate) fn do_reclaim_non_blocking_tasks(&self) {
        if self.exec_mu.lock().unwrap().mode == ArbitratorWorkMode::Standard {
            for prio in PRIORITIES {
                if self.task_num_by_priority(prio) != 0 {
                    let n = self.do_cancel_pending_tasks(prio, false);
                    self.exec_metrics.cancel_standard.fetch_add(n, SeqCst);
                }
            }
        } else if self.task_num_of_wait_averse() != 0 {
            let n = self.do_cancel_pending_tasks(ArbitrationPriority::High, true);
            self.exec_metrics.cancel_wait_averse.fetch_add(n, SeqCst);
        }
    }

    /// Go `doReclaimByWorkMode`.
    pub(crate) fn do_reclaim_by_work_mode(&self, entry: &Arc<RootPoolEntry>, reclaimed: i64) {
        let wait_averse = entry.ctx.wait_averse.load(SeqCst);
        self.do_reclaim_non_blocking_tasks();
        if wait_averse {
            return;
        }
        if self.exec_mu.lock().unwrap().mode == ArbitratorWorkMode::Priority {
            self.do_reclaim_mem_by_priority(entry, entry.request.quota.load(SeqCst) - reclaimed);
        }
    }

    /// Go `doExecuteCleanupTasks`.
    pub(crate) fn do_execute_cleanup_tasks(&self) {
        loop {
            let entry = self.cleanup_fifo.lock().unwrap().pop_front();
            let Some(entry) = entry else { break };

            {
                let mut pe = self.privileged_entry.lock().unwrap();
                if pe.as_ref().is_some_and(|e| Arc::ptr_eq(e, &entry)) {
                    *pe = None;
                }
            }
            self.delete_under_cancel(&entry);
            self.delete_under_kill(&entry);

            if !entry.state_mu.stop.load(SeqCst) {
                let to_release = entry.state_mu.quota_to_reclaim.swap(0, SeqCst);
                if to_release > 0 {
                    self.release(to_release);
                    self.mu_released.fetch_add(to_release as u64, SeqCst);
                    self.entry_map.add_quota(&entry, -to_release);
                }
            } else {
                let destroyed = entry.arbitrator_mu.lock().unwrap().destroyed;
                if !destroyed {
                    let quota = entry.arbitrator_mu.lock().unwrap().quota;
                    if quota > 0 {
                        self.release(quota);
                        self.mu_released.fetch_add(quota as u64, SeqCst);
                    }
                    self.entry_map.delete(&entry);
                    self.root_pool_num.fetch_add(-1, SeqCst);
                    entry.arbitrator_mu.lock().unwrap().destroyed = true;
                }
                if self.remove_task(&entry) {
                    entry.wind_up(0, ArbitrateResult::Fail);
                }
            }
        }
    }

    /// Go `implicitRun` (disable mode: satisfy every subscription).
    pub(crate) fn implicit_run(&self) {
        if self.tasks.lock().unwrap().fifo_tasks.approx_empty() {
            return;
        }
        loop {
            let Some(entry) = self.front_task_entry() else {
                break;
            };
            if entry.arbitrator_mu.lock().unwrap().destroyed {
                if self.remove_task(&entry) {
                    entry.wind_up(0, ArbitrateResult::Fail);
                }
                continue;
            }
            {
                let _g = entry.request.task_mu.lock().unwrap();
                let quota = entry.request.quota.load(SeqCst);
                if self.remove_task(&entry) {
                    self.alloc(quota);
                    self.entry_map.add_quota(&entry, quota);
                    entry.wind_up(quota, ArbitrateResult::Ok);
                }
            }
        }
    }

    /// Go `runOneRound`: -1 disabled, -2 mem unsafe, >= 0 executed tasks.
    pub fn run_one_round(&self) -> i32 {
        {
            let mut exec = self.exec_mu.lock().unwrap();
            exec.start_time = self.now();
            let t = exec
                .start_time
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            if t != self.approx_unix_time_sec() {
                self.set_unix_time_sec(t);
            }
            let mode = self.work_mode();
            if exec.mode != mode {
                exec.mode = mode;
                if mode == ArbitratorWorkMode::Disable {
                    drop(exec);
                    self.set_mem_safe();
                    self.exec_mu.lock().unwrap().blocked_state = BlockedState::default();
                }
            }
        }

        if !self.cleanup_fifo.lock().unwrap().approx_empty() {
            self.do_execute_cleanup_tasks();
        }

        if self.exec_mu.lock().unwrap().mode == ArbitratorWorkMode::Disable {
            self.implicit_run();
            return -1;
        }

        if !self.handle_mem_issues() {
            return -2;
        }

        let mut task_exec_num = 0;
        while self.do_execute_first_task() {
            task_exec_num += 1;
        }
        task_exec_num
    }

    /// Go `asyncRun`.
    pub(crate) fn async_run(self: &Arc<Self>, duration: Duration) -> bool {
        if self.control_running.load(SeqCst) {
            return false;
        }
        self.control_running.store(true, SeqCst);
        let (finish_tx, finish_rx) = bounded::<()>(0);
        *self.control_mu.lock().unwrap() = Some(finish_rx);

        let m = Arc::clone(self);
        std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(duration);
            while m.control_running.load(SeqCst) {
                let mut sel = Select::new();
                let op_tick = sel.recv(&ticker);
                let _op_notif = sel.recv(&m.notifer.rx);
                let op = sel.select();
                if op.index() == op_tick {
                    let _ = op.recv(&ticker);
                    m.weak_wake();
                } else {
                    let _ = op.recv(&m.notifer.rx);
                    m.notifer.clear();
                    m.run_one_round();
                }
            }
            drop(finish_tx); // close(finishCh)
        });
        true
    }

    /// Go `AutoRun`.
    pub fn auto_run(
        self: &Arc<Self>,
        actions: MemArbitratorActions,
        await_free_pool_alloc_align_size: i64,
        await_free_pool_shard_num: i64,
        task_tick_dur: Duration,
    ) -> bool {
        if self.control_running.load(SeqCst) {
            return false;
        }
        *self.actions.lock().unwrap() = Arc::new(actions);
        self.refresh_runtime_mem_stats();
        self.init_await_free_pool(await_free_pool_alloc_align_size, await_free_pool_shard_num);
        self.async_run(task_tick_dur)
    }

    /// Go `stop` (unexported in the source; the global cleanup path needs
    /// it).
    pub fn stop(&self) -> bool {
        if !self.control_running.load(SeqCst) {
            return false;
        }
        let finish = { self.control_mu.lock().unwrap().clone() };
        self.control_running.store(false, SeqCst);
        self.wake();
        if let Some(rx) = finish {
            let _ = rx.recv(); // wait for close
        }
        self.run_one_round();
        true
    }
}
