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

//! `MemArbitrator` runtime memory-stats ingestion, avoidance sizing and the
//! await-free pool. Split out of `arbitrator.rs`; mirrors the runtime-stats
//! and await-free-pool sections of Go `pkg/util/memory/arbitrator.go`.

use super::*;

impl MemArbitrator {
    // ----- runtime mem stats & avoidance -----

    pub(crate) fn refresh_runtime_mem_stats(&self) {
        let actions = self.actions.lock().unwrap().clone();
        if let Some(f) = &actions.update_runtime_mem_stats {
            f();
        }
        self.exec_metrics.action_update_stats.fetch_add(1, SeqCst);
    }

    pub(crate) fn try_set_runtime_mem_stats(&self, s: MemStats) -> bool {
        if let Ok(g) = self.hc_mutex.try_lock() {
            self.do_set_runtime_mem_stats(s);
            drop(g);
            true
        } else {
            false
        }
    }

    /// Go `setRuntimeMemStats` — feed fresh runtime stats.
    pub fn set_runtime_mem_stats(&self, s: MemStats) {
        let _g = self.hc_mutex.lock().unwrap();
        self.do_set_runtime_mem_stats(s);
    }

    fn do_set_runtime_mem_stats(&self, s: MemStats) {
        self.hc_heap_alloc.store(s.heap_alloc, SeqCst);
        self.hc_heap_inuse.store(s.heap_inuse, SeqCst);
        self.hc_heap_total_free.store(s.total_free, SeqCst);
        self.hc_mem_off_heap.store(s.mem_off_heap, SeqCst);
        self.hc_mem_inuse
            .store(s.mem_off_heap + s.heap_inuse, SeqCst);

        if s.last_gc > self.hc_last_gc_utime.load(SeqCst) {
            self.hc_last_gc_heap_alloc.store(s.heap_alloc, SeqCst);
            self.hc_last_gc_utime.store(s.last_gc, SeqCst);
        }
        self.update_avoid_size();
    }

    /// Go `updateAvoidSize`.
    pub(crate) fn update_avoid_size(&self) {
        let mut capacity = self.soft_limit_i();
        if *self.mu_soft_limit_mode.lock().unwrap() == SoftLimitMode::Auto {
            let ratio = self.mem_magnif();
            if ratio != 0 {
                let new_cap = calc_ratio(self.limit(), ratio);
                capacity = capacity.min(new_cap);
            }
        }
        let avoid_size = 0i64
            .max(
                self.hc_heap_alloc.load(SeqCst) + self.hc_mem_off_heap.load(SeqCst)
                    - self.heap_tracked.load(SeqCst),
            )
            .max(self.limit() - capacity);
        self.avoid_size.store(avoid_size, SeqCst);

        let af = self.await_free.lock().unwrap().clone();
        let Some(af) = af else { return };
        let delta = self.allocated() - self.limit() + avoid_size;
        if delta > 0 && af.pool.allocated() > 0 {
            let mut reclaimed = 0i64;
            let kick_idx = self.exec_mu.lock().unwrap().await_free_kick_idx;
            let mut final_idx = kick_idx;
            for i in 0..af.shards.len() {
                let idx = (kick_idx + i as u64 + 1) & af.size_mask;
                let b = &af.shards[idx as usize];
                if b.budget.approx_capacity() > 0 {
                    if let Ok(_g) = b.budget.mu.try_lock() {
                        let x = (delta - reclaimed).min(b.budget.capacity.load(SeqCst));
                        b.budget.capacity.fetch_add(-x, SeqCst);
                        reclaimed += x;
                    }
                }
                if reclaimed >= delta {
                    final_idx = idx;
                    break;
                }
            }
            if reclaimed >= delta {
                self.exec_mu.lock().unwrap().await_free_kick_idx = final_idx;
            }
            let mut pool_released = 0i64;
            if reclaimed > 0 {
                pool_released = af.pool.release_pop_budget(reclaimed);
            }
            if pool_released > 0 {
                self.release(pool_released);
                self.exec_metrics
                    .await_free_force_shrink
                    .fetch_add(1, SeqCst);
                self.mu_released.fetch_add(pool_released as u64, SeqCst);
            }
        }
    }

    /// Go `HandleRuntimeStats`.
    pub fn handle_runtime_stats(&self, s: MemStats) {
        self.try_shrink_await_free_pool(DEF_POOL_RESERVED_QUOTA, now_unix_milli());
        self.try_update_tracked_mem_stats(now_unix_milli());
        self.try_set_runtime_mem_stats(s);
        self.execute_tick(now_unix_milli());
        self.weak_wake();
    }

    pub(crate) fn try_update_tracked_mem_stats(&self, utime_milli: i64) -> bool {
        if self.heap_tracked_last_update_milli.load(SeqCst) + DEF_TRACK_MEM_STATS_DUR_MILLI
            <= utime_milli
        {
            self.update_tracked_heap_stats();
            return true;
        }
        false
    }

    /// Go `updateTrackedHeapStats`.
    pub(crate) fn update_tracked_heap_stats(&self) {
        let mut total_tracked_heap = 0i64;
        if self.entry_map.context_cache_num.load(SeqCst) != 0 {
            let mut max_mem_used = 0i64;
            let entries: Vec<Arc<RootPoolEntry>> = self
                .entry_map
                .context_cache
                .lock()
                .unwrap()
                .values()
                .cloned()
                .collect();
            for e in entries {
                if e.not_running() {
                    continue;
                }
                let ctx = e.load_ctx();
                if ctx_available(&ctx) {
                    if let Some(h) = ctx.unwrap().helper() {
                        let mem_used = h.heap_inuse();
                        if mem_used > 0 {
                            total_tracked_heap += mem_used;
                            max_mem_used = max_mem_used.max(mem_used);
                        }
                    }
                }
            }
            if self.buffer_size.load(SeqCst) < max_mem_used {
                self.try_to_update_buffer(max_mem_used, self.approx_unix_time_sec());
            }
        }
        total_tracked_heap += self.await_free_pool_used().tracked_heap;
        self.heap_tracked.store(total_tracked_heap, SeqCst);
        self.heap_tracked_last_update_milli
            .store(now_unix_milli(), SeqCst);
    }

    // ----- await-free pool -----

    /// Go `initAwaitFreePool`.
    pub fn init_await_free_pool(self: &Arc<Self>, alloc_align_size: i64, shard_num: i64) {
        let alloc_align_size = if alloc_align_size <= 0 {
            DEF_AWAIT_FREE_POOL_ALLOC_ALIGN_SIZE
        } else {
            alloc_align_size
        };
        let p = ResourcePool::new_raw("awaitfree-pool", 0, DEF_MAX_LIMIT, alloc_align_size, 0);

        let m = Arc::clone(self);
        let pool_cb = Arc::clone(&p);
        p.set_out_of_capacity_action(Box::new(move |mut s: OutOfCapacityActionArgs<'_, '_>| {
            if m.hc_heap_alloc.load(SeqCst) > m.oom_risk() - s.request
                || m.allocated() > m.limit() - m.out_of_control() - s.request
            {
                m.update_blocked_at();
                m.exec_metrics.await_free_fail.fetch_add(1, SeqCst);
                return Err(err_arbitrate_fail());
            }
            m.alloc(s.request);
            s.pool.force_add_cap(s.request);
            let _ = &pool_cb;
            m.exec_metrics.await_free_succ.fetch_add(1, SeqCst);
            Ok(())
        }));

        let cnt = next_pow2(shard_num as u64);
        let mut shards = Vec::with_capacity(cnt as usize);
        for _ in 0..cnt {
            shards.push(TrackedConcurrentBudget {
                budget: ConcurrentBudget::new(Arc::clone(&p)),
                heap_inuse: AtomicI64::new(0),
            });
        }
        *self.await_free.lock().unwrap() = Some(Arc::new(AwaitFreeState {
            pool: p,
            shards,
            size_mask: cnt - 1,
        }));
    }

    pub(crate) fn await_free_state(&self) -> Option<Arc<AwaitFreeState>> {
        self.await_free.lock().unwrap().clone()
    }

    /// Go `GetAwaitFreeBudgets`: runs `f` with the budget shard for `uid`.
    pub fn with_await_free_budget<R>(
        &self,
        uid: u64,
        f: impl FnOnce(&TrackedConcurrentBudget) -> R,
    ) -> Option<R> {
        let af = self.await_free_state()?;
        let index = shard_index_by_uid(uid, af.size_mask);
        Some(f(&af.shards[index as usize]))
    }

    /// Go `ConsumeQuotaFromAwaitFreePool`.
    pub fn consume_quota_from_await_free_pool(&self, uid: u64, req: i64) -> bool {
        let utime = self.approx_unix_time_sec();
        self.with_await_free_budget(uid, |b| b.budget.consume_quota(utime, req).is_ok())
            .unwrap_or(false)
    }

    /// Go `ReportHeapInuseToAwaitFreePool`.
    pub fn report_heap_inuse_to_await_free_pool(&self, uid: u64, req: i64) {
        self.with_await_free_budget(uid, |b| b.report_heap_inuse(req));
    }

    /// Go `awaitFreePoolCap` (metrics surface).
    pub fn await_free_pool_cap(&self) -> i64 {
        match self.await_free_state() {
            Some(af) => af.pool.capacity(),
            None => 0,
        }
    }

    /// Go `awaitFreePoolUsed`.
    pub(crate) fn await_free_pool_used(&self) -> MemPoolQuotaUsage {
        let mut res = MemPoolQuotaUsage::default();
        if let Some(af) = self.await_free_state() {
            for b in &af.shards {
                let d = b.budget.used.load(SeqCst);
                if d > 0 {
                    res.quota += d;
                }
                let d = b.heap_inuse.load(SeqCst);
                if d > 0 {
                    res.tracked_heap += d;
                }
            }
        }
        *self.await_free_last_usage.lock().unwrap() = res;
        res
    }

    /// Go `approxAwaitFreePoolUsed` (metrics surface).
    pub fn approx_await_free_pool_used(&self) -> MemPoolQuotaUsage {
        *self.await_free_last_usage.lock().unwrap()
    }

    pub(crate) fn try_shrink_await_free_pool(&self, min_remain: i64, utime_milli: i64) -> bool {
        if self.await_free_last_shrink_milli.load(SeqCst) + DEF_AWAIT_FREE_POOL_SHRINK_DUR_MILLI
            <= utime_milli
        {
            self.shrink_await_free_pool(min_remain, utime_milli);
            return true;
        }
        false
    }

    /// Go `shrinkAwaitFreePool`.
    pub(crate) fn shrink_await_free_pool(&self, min_remain: i64, utime_milli: i64) {
        let Some(af) = self.await_free_state() else {
            return;
        };
        if af.pool.allocated() <= 0 {
            return;
        }

        let mut reclaimed = 0i64;
        let align = af.pool.alloc_align_size();

        for b in &af.shards {
            let used = b.budget.used.load(SeqCst);
            if used > 0 {
                if b.budget.approx_capacity() - (used + min_remain) >= align {
                    if let Ok(_g) = b.budget.mu.try_lock() {
                        let used = b.budget.used.load(SeqCst);
                        if used > 0 {
                            let to_reclaim = b.budget.capacity.load(SeqCst) - (used + min_remain);
                            if to_reclaim >= align {
                                b.budget.capacity.fetch_add(-to_reclaim, SeqCst);
                                reclaimed += to_reclaim;
                            }
                        }
                    }
                }
            } else if b.budget.approx_capacity() > 0
                && b.budget.get_last_used_time_sec() * KILO + DEF_AWAIT_FREE_POOL_SHRINK_DUR_MILLI
                    <= utime_milli
            {
                if let Ok(_g) = b.budget.mu.try_lock() {
                    let to_reclaim = b.budget.capacity.load(SeqCst);
                    if b.budget.used.load(SeqCst) <= 0 && to_reclaim > 0 {
                        b.budget.capacity.fetch_add(-to_reclaim, SeqCst);
                        reclaimed += to_reclaim;
                    }
                }
            }
        }

        let mut pool_released = 0i64;
        if reclaimed > 0 {
            pool_released = af.pool.release_pop_budget(reclaimed);
        }
        if pool_released > 0 {
            self.release(pool_released);
            self.mu_released.fetch_add(pool_released as u64, SeqCst);
            self.exec_metrics.await_free_shrink.fetch_add(1, SeqCst);
            self.weak_wake();
        }
        self.await_free_last_shrink_milli.store(utime_milli, SeqCst);
    }
}
