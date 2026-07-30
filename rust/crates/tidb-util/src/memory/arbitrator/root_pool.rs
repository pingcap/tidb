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

//! `MemArbitrator` root-pool lifecycle: registering, restarting, resetting
//! and removing root resource pools, plus the arbitrator-only under-kill /
//! under-cancel bookkeeping. Split out of `arbitrator.rs`; mirrors the
//! root-pool section of Go `pkg/util/memory/arbitrator.go`.

use super::*;

impl MemArbitrator {
    // ----- root pool lifecycle -----

    pub(crate) fn get_root_pool_entry(&self, uid: u64) -> Option<Arc<RootPoolEntry>> {
        self.entry_map
            .status_shard(uid)
            .entries
            .read()
            .unwrap()
            .get(&uid)
            .cloned()
    }

    /// Go `FindRootPool`.
    pub fn find_root_pool(&self, uid: u64) -> RootPoolWrap {
        RootPoolWrap {
            entry: self.get_root_pool_entry(uid),
        }
    }

    /// Go `EmplaceRootPool`.
    pub fn emplace_root_pool(&self, uid: u64) -> Result<RootPoolWrap, PoolError> {
        if let Some(e) = self.get_root_pool_entry(uid) {
            return Ok(RootPoolWrap { entry: Some(e) });
        }
        let pool = ResourcePool::new_raw(
            &format!("root-{uid}"),
            uid,
            DEF_MAX_LIMIT,
            1,
            DEF_MAX_UNUSED_BLOCKS_LOCAL,
        );
        let entry = self.add_root_pool(pool)?;
        Ok(RootPoolWrap { entry: Some(entry) })
    }

    /// Go `addRootPool`.
    pub(crate) fn add_root_pool(
        &self,
        pool: Arc<ResourcePool>,
    ) -> Result<Arc<RootPoolEntry>, PoolError> {
        let b = pool.capacity();
        if b != 0 {
            return Err(PoolError(format!(
                "{}: has {} bytes budget left",
                pool.name(),
                b
            )));
        }
        if let Some(up) = pool.upstream() {
            return Err(PoolError(format!(
                "{}: already started with pool {}",
                pool.name(),
                up.name()
            )));
        }
        if pool.reserved() != 0 {
            return Err(PoolError(format!(
                "{}: has {} reserved budget left",
                pool.name(),
                pool.reserved()
            )));
        }

        let name = pool.name().to_string();
        let (entry, ok) = self.entry_map.emplace(pool);
        if !ok {
            return Err(PoolError(format!("{name}: already exists")));
        }
        self.root_pool_num.fetch_add(1, SeqCst);
        Ok(entry)
    }

    /// Go `RestartEntryByContext`.
    pub fn restart_entry_by_context(
        self: &Arc<Self>,
        p: RootPoolWrap,
        ctx: Option<Arc<ArbitrationContext>>,
    ) -> bool {
        let Some(entry) = p.entry else {
            return false;
        };
        let _sg = entry.state_mu.mutex.lock().unwrap();
        if entry.state_mu.stop.load(SeqCst) || entry.exec_state() != EntryExecState::Idle {
            return false;
        }

        // Go locks the pool mutex across the context install; the Rust pool
        // has interior locking, so equivalent exclusion comes from the
        // state_mu lock held here.
        match &ctx {
            Some(c) => {
                if c.wait_averse {
                    entry.ctx.prefer_privilege.store(false, SeqCst);
                    entry
                        .ctx
                        .mem_priority
                        .store(ArbitrationPriority::High as i32, SeqCst);
                } else {
                    entry.ctx.prefer_privilege.store(c.prefer_privilege, SeqCst);
                    entry.ctx.mem_priority.store(c.mem_priority as i32, SeqCst);
                }
                *entry.ctx.cancel_ch.lock().unwrap() = c.cancel_ch.clone();
                entry.ctx.wait_averse.store(c.wait_averse, SeqCst);
            }
            None => {
                *entry.ctx.cancel_ch.lock().unwrap() = None;
                entry.ctx.wait_averse.store(false, SeqCst);
                entry
                    .ctx
                    .mem_priority
                    .store(ArbitrationPriority::Medium as i32, SeqCst);
                entry.ctx.prefer_privilege.store(false, SeqCst);
            }
        }
        entry.ctx.canceled.store(false, SeqCst);
        *entry.ctx.ptr.lock().unwrap() = ctx;

        {
            let mut cache = self.entry_map.context_cache.lock().unwrap();
            if cache.insert(entry.pool.uid(), Arc::clone(&entry)).is_none() {
                self.entry_map.context_cache_num.fetch_add(1, SeqCst);
            }
        }

        let m = Arc::clone(self);
        let entry_cb = Arc::clone(&entry);
        entry.pool.set_out_of_capacity_action(Box::new(
            move |s: OutOfCapacityActionArgs<'_, '_>| {
                let request = s.request;
                let mut pool_ctx = s.pool;
                if m.blocking_allocate(&entry_cb, request, &mut |d| pool_ctx.force_add_cap(d))
                    != ArbitrateResult::Ok
                {
                    return Err(err_arbitrate_fail());
                }
                Ok(())
            },
        ));
        entry.pool.clear_stopped();
        entry.set_exec_state(EntryExecState::Running);
        true
    }

    /// Go `ResetRootPoolByID`.
    pub fn reset_root_pool_by_id(&self, uid: u64, max_mem_consumed: i64, tune: bool) {
        let Some(entry) = self.get_root_pool_entry(uid) else {
            return;
        };
        self.try_to_update_buffer(max_mem_consumed, self.approx_unix_time_sec());
        if tune {
            let small = self.pool_alloc_stats.read().unwrap().small_pool_limit;
            if max_mem_consumed > small {
                self.record_mem_consumed(max_mem_consumed, self.approx_unix_time_sec());
            }
        }
        self.reset_root_pool_entry(&entry);
        self.wake();
    }

    /// Go `resetRootPoolEntry`.
    pub(crate) fn reset_root_pool_entry(&self, entry: &Arc<RootPoolEntry>) -> bool {
        {
            let _g = entry.state_mu.mutex.lock().unwrap();
            if entry.exec_state() == EntryExecState::Idle {
                return false;
            }
            entry.set_exec_state(EntryExecState::Idle);
        }
        let released = entry.pool.stop();
        if released > 0 {
            entry.state_mu.quota_to_reclaim.fetch_add(released, SeqCst);
        }
        self.cleanup_fifo
            .lock()
            .unwrap()
            .push_back(Arc::clone(entry));
        true
    }

    /// Go `RemoveRootPoolByID`.
    pub fn remove_root_pool_by_id(&self, uid: u64) -> bool {
        let Some(entry) = self.get_root_pool_entry(uid) else {
            return false;
        };
        if self.remove_root_pool_entry(&entry) {
            if let Some(ctx) = entry.load_ctx() {
                if let Some(h) = ctx.helper() {
                    h.finish();
                }
            }
            self.wake();
            return true;
        }
        false
    }

    /// Go `removeRootPoolEntry`.
    pub(crate) fn remove_root_pool_entry(&self, entry: &Arc<RootPoolEntry>) -> bool {
        {
            let _g = entry.state_mu.mutex.lock().unwrap();
            if entry.state_mu.stop.swap(true, SeqCst) {
                return false;
            }
            if entry.exec_state() != EntryExecState::Idle {
                entry.set_exec_state(EntryExecState::Idle);
            }
        }
        self.cleanup_fifo
            .lock()
            .unwrap()
            .push_back(Arc::clone(entry));
        entry.pool.stop();
        true
    }

    fn warn_kill_cancel(&self, entry: &Arc<RootPoolEntry>, ctx_reclaim: i64, reason: &str) {
        let actions = self.actions.lock().unwrap().clone();
        (actions.warn)(&format!(
            "{reason}: uid={} name={} reclaimed={}",
            entry.pool.uid(),
            entry.pool.name(),
            ctx_reclaim,
        ));
    }

    // ----- under kill / cancel bookkeeping (arbitrator-only) -----

    pub(crate) fn add_under_kill(
        &self,
        entry: &Arc<RootPoolEntry>,
        memory_used: i64,
        start_time: SystemTime,
    ) {
        let mut st = entry.arbitrator_mu.lock().unwrap();
        if !st.under_kill.start {
            self.under_kill.lock().unwrap().add(entry);
            st.under_kill = EntryKillCancelCtx {
                start: true,
                start_time,
                reclaim: memory_used,
                fail: false,
            };
        }
    }

    pub(crate) fn add_under_cancel(
        &self,
        entry: &Arc<RootPoolEntry>,
        memory_used: i64,
        start_time: SystemTime,
    ) {
        let mut st = entry.arbitrator_mu.lock().unwrap();
        if !st.under_cancel.start {
            self.under_cancel.lock().unwrap().add(entry);
            st.under_cancel = EntryKillCancelCtx {
                start: true,
                start_time,
                reclaim: memory_used,
                fail: false,
            };
        }
    }

    pub(crate) fn delete_under_kill(&self, entry: &Arc<RootPoolEntry>) {
        let reclaim = {
            let mut st = entry.arbitrator_mu.lock().unwrap();
            if !st.under_kill.start {
                return;
            }
            st.under_kill.start = false;
            st.under_kill.reclaim
        };
        self.under_kill.lock().unwrap().delete(entry);
        self.warn_kill_cancel(entry, reclaim, "Finish to `KILL` root pool");
    }

    pub(crate) fn delete_under_cancel(&self, entry: &Arc<RootPoolEntry>) {
        let mut st = entry.arbitrator_mu.lock().unwrap();
        if st.under_cancel.start {
            st.under_cancel.start = false;
            drop(st);
            self.under_cancel.lock().unwrap().delete(entry);
        }
    }
}
