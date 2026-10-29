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

//! `MemArbitrator` self-tuning and memory-risk handling: pool medium
//! capacity, memory magnification, the periodic tick, mem-risk entry/exit,
//! top-N killing and mem-state recording. Split out of `arbitrator.rs`;
//! mirrors the medium-capacity/magnification and mem-risk sections of Go
//! `pkg/util/memory/arbitrator.go`.

use super::*;

impl MemArbitrator {
    // ----- pool medium capacity & magnification -----

    /// Go `updatePoolMediumCapacity`.
    pub(crate) fn update_pool_medium_capacity(&self, utime_milli: i64) {
        const MAX_NUM: i64 = (2 + DEF_REDUNDANCY) as i64;
        const MAX_DUR: i64 = MAX_NUM - DEF_REDUNDANCY as i64;
        {
            let stats = self.pool_alloc_stats.read().unwrap();
            let ts_align = utime_milli / KILO / DEF_UPDATE_MEM_CONSUMED_TIME_ALIGN_SEC;
            let idx1 = (((MAX_NUM + ts_align - 1) % MAX_NUM) as usize).min(3);
            let idx2 = ((ts_align % MAX_NUM) as usize).min(3);

            let mut tar1 = Some(idx1);
            let mut tar2 = Some(idx2);
            {
                let ts = self.pool_alloc_timed_elems[idx1].ts_align.load(SeqCst);
                if ts <= ts_align - MAX_DUR || ts > ts_align {
                    tar1 = None;
                }
            }
            {
                let ts = self.pool_alloc_timed_elems[idx2].ts_align.load(SeqCst);
                if ts <= ts_align - MAX_DUR || ts > ts_align {
                    tar2 = None;
                }
            }

            let g1 = tar1.map(|i| self.pool_alloc_timed_map[i].read().unwrap());
            let g2 = tar2.map(|i| self.pool_alloc_timed_map[i].read().unwrap());
            let mut total = 0u64;
            if let Some(i) = tar1 {
                total += self.pool_alloc_timed_elems[i].num.load(SeqCst);
            }
            if let Some(i) = tar2 {
                total += self.pool_alloc_timed_elems[i].num.load(SeqCst);
            }

            if total != 0 {
                let expect = 1u64.max(total.div_ceil(2));
                let mut cnt = 0u64;
                let mut index = 0usize;
                for i in 0..DEF_SERVERLIMIT_MIN_UNIT_NUM as usize {
                    if let Some(t1) = tar1 {
                        cnt += self.pool_alloc_timed_elems[t1].slot[i].load(SeqCst) as u64;
                    }
                    if let Some(t2) = tar2 {
                        cnt += self.pool_alloc_timed_elems[t2].slot[i].load(SeqCst) as u64;
                    }
                    if cnt >= expect {
                        index = i;
                        break;
                    }
                }
                let res = stats.pool_alloc_unit * (index as i64 + 1);
                self.pool_alloc_medium_quota.store(res, SeqCst);
            }
            drop(g1);
            drop(g2);
        }
        self.try_store_pool_medium_capacity(utime_milli, self.pool_medium_quota());
    }

    /// Go `tryStorePoolMediumCapacity`.
    pub(crate) fn try_store_pool_medium_capacity(&self, utime_milli: i64, capacity: i64) -> bool {
        if capacity == 0 {
            return false;
        }
        let last_state = self.last_mem_state();
        let should = match last_state {
            None => true,
            Some(s) => {
                self.pool_alloc_last_update_milli.load(SeqCst) + DEF_STORE_POOL_MEDIUM_CAP_DUR_MILLI
                    <= utime_milli
                    && s.pool_medium_cap != capacity
            }
        };
        if should {
            let mem_state = match last_state {
                Some(mut s) => {
                    s.pool_medium_cap = capacity;
                    s
                }
                None => RuntimeMemStateV1 {
                    version: 1,
                    pool_medium_cap: capacity,
                    ..Default::default()
                },
            };
            let _ = self.record_mem_state(&mem_state, "new root pool medium cap");
            self.pool_alloc_last_update_milli.store(utime_milli, SeqCst);
            return true;
        }
        false
    }

    pub(crate) fn pool_medium_quota(&self) -> i64 {
        self.pool_alloc_medium_quota.load(SeqCst)
    }

    /// Go `SuggestPoolInitCap`.
    pub fn suggest_pool_init_cap(&self) -> i64 {
        self.pool_medium_quota()
    }

    /// Go `updateMemMagnification`; returns the updated previous profile.
    pub(crate) fn update_mem_magnification(
        &self,
        utime_milli: i64,
    ) -> Option<(i64, i64, i64, i64)> {
        const MAX_NUM: i64 = 2;
        let cur_ts_align = utime_milli / KILO / DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN;
        let mut updated_pre: Option<(i64, i64, i64, i64)> = None;

        let mut profs = self.hc_timed_mem_profile.lock().unwrap();
        let cur_idx = (cur_ts_align % MAX_NUM) as usize;
        if profs[cur_idx].ts_align < cur_ts_align {
            {
                let pre_ts = cur_ts_align - 1;
                let pre_idx = (((MAX_NUM + pre_ts) % MAX_NUM) as usize).min(1);
                let pre = &mut profs[pre_idx];
                pre.ratio = 0;
                if pre.ts_align == pre_ts && pre.quota > 0 {
                    if pre.heap > 0 {
                        pre.ratio = calc_ratio(pre.heap, pre.quota);
                    }
                    updated_pre = Some((pre.start_utime_milli, pre.heap, pre.quota, pre.ratio));
                }
            }

            let mut v = 0i64;
            for ts_align in [cur_ts_align - 2, cur_ts_align - 1] {
                let tar = &profs[(((MAX_NUM + ts_align) % MAX_NUM) as usize).min(1)];
                if tar.ts_align != ts_align || tar.heap >= self.oom_risk() {
                    v = 0;
                    break;
                }
                if tar.ratio <= 0 {
                    break;
                }
                v = v.max(tar.ratio);
            }

            let mut updated = false;
            let mut ori_ratio = 0i64;
            let mut new_ratio = 0i64;

            if v != 0 {
                if let Ok(_g) = self.mem_magnif_mu.try_lock() {
                    ori_ratio = self.mem_magnif();
                    if ori_ratio != 0 && v < ori_ratio - 10 {
                        new_ratio = (ori_ratio + v) / 2;
                        if new_ratio <= KILO {
                            new_ratio = 0;
                        }
                        self.do_set_mem_magnif(new_ratio);
                        updated = true;
                    }
                }
            }

            if updated {
                let actions = self.actions.lock().unwrap().clone();
                (actions.info)(&format!(
                    "Update mem quota magnification ratio: ori={ori_ratio} new={new_ratio}"
                ));
                if let Some(last) = self.last_mem_state() {
                    if new_ratio < last.magnif {
                        let mem_state = RuntimeMemStateV1 {
                            version: 1,
                            magnif: new_ratio,
                            pool_medium_cap: self.pool_medium_quota(),
                            ..Default::default()
                        };
                        let _ = self.record_mem_state(&mem_state, "new magnification ratio");
                    }
                }
            }

            profs[cur_idx] = MemProfile {
                ts_align: cur_ts_align,
                start_utime_milli: utime_milli,
                ..Default::default()
            };
        }

        if profs[cur_idx].ts_align == cur_ts_align {
            let ut = self.hc_last_gc_utime.load(SeqCst);
            if cur_ts_align == ut / 1_000_000_000 / DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN {
                profs[cur_idx].heap = profs[cur_idx]
                    .heap
                    .max(self.hc_last_gc_heap_alloc.load(SeqCst));
            }
            let (blocked_size, utime_sec) = self.last_blocked_at();
            if utime_sec / DEF_UPDATE_MEM_MAGNIF_UTIME_ALIGN == cur_ts_align {
                profs[cur_idx].quota = profs[cur_idx].quota.max(blocked_size);
            }
        }
        updated_pre
    }

    /// Go `executeTick`.
    pub(crate) fn execute_tick(&self, utime_milli: i64) -> bool {
        if self.at_mem_risk() {
            return false;
        }
        if self.tick_last_milli.load(SeqCst) + DEF_TICK_DUR_MILLI > utime_milli {
            return false;
        }
        let _g = self.tick_mu.lock().unwrap();
        self.tick_last_milli.store(utime_milli, SeqCst);

        if let Some((start_ms, heap, quota, ratio)) = self.update_mem_magnification(utime_milli) {
            let actions = self.actions.lock().unwrap().clone();
            (actions.info)(&format!(
                "Mem profile timeline: last-blocked-heap={heap} last-blocked-quota={quota} last-ratio={ratio} start={start_ms}"
            ));
        }
        self.update_pool_medium_capacity(utime_milli);
        let limit = self.digest_limit.load(SeqCst);
        self.shrink_digest_profile(utime_milli / KILO, limit, limit / 2);
        true
    }

    // ----- mem risk -----

    pub(crate) fn is_mem_safe(&self) -> bool {
        self.hc_mem_inuse.load(SeqCst) < self.oom_risk()
    }

    pub(crate) fn is_mem_no_risk(&self) -> bool {
        self.is_mem_safe() && self.hc_heap_alloc.load(SeqCst) < self.mem_risk()
    }

    /// Go `calcMemRisk`.
    pub(crate) fn calc_mem_risk(&self) -> Option<RuntimeMemStateV1> {
        if *self.mu_soft_limit_mode.lock().unwrap() != SoftLimitMode::Auto {
            return None;
        }
        let mut mem_state = RuntimeMemStateV1 {
            version: 1,
            last_risk: LastRisk {
                heap_alloc: self.hc_heap_alloc.load(SeqCst),
                quota_alloc: self.allocated(),
            },
            pool_medium_cap: self.pool_medium_quota(),
            ..Default::default()
        };
        if mem_state.last_risk.quota_alloc == 0
            || mem_state.last_risk.heap_alloc <= mem_state.last_risk.quota_alloc
        {
            return None;
        }
        mem_state.magnif = calc_ratio(
            mem_state.last_risk.heap_alloc,
            mem_state.last_risk.quota_alloc,
        ) + 100;
        if let Some(p) = self.last_mem_state() {
            mem_state.magnif = mem_state.magnif.max(p.magnif);
        }
        Some(mem_state)
    }

    /// Go `handleMemIssues`: returns true if memory state is safe.
    pub(crate) fn handle_mem_issues(&self) -> bool {
        if self.at_mem_risk() {
            let gc_executed = self.try_runtime_gc();
            if !gc_executed {
                self.refresh_runtime_mem_stats();
            }
            if self.is_mem_no_risk() {
                self.update_tracked_heap_stats();
                self.update_avoid_size();
                let actions = self.actions.lock().unwrap().clone();
                (actions.info)("Memory is safe");
                self.set_mem_safe();
                return true;
            }
            self.do_reclaim_non_blocking_tasks();
            self.handle_mem_risk(gc_executed);
            false
        } else if !self.is_mem_safe() {
            self.do_reclaim_non_blocking_tasks();
            self.enter_mem_risk();
            false
        } else {
            true
        }
    }

    /// Go `intoMemRisk`.
    pub(crate) fn enter_mem_risk(&self) {
        let now = self.inner_time();
        {
            let mut risk = self.hc_mem_risk.lock().unwrap();
            risk.start_time = now;
            risk.last_heap_total_free = self.hc_heap_total_free.load(SeqCst);
            risk.last_stats_start_time = now;
        }
        self.hc_mem_risk_start_unix_milli.store(
            now.duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
            SeqCst,
        );
        self.exec_metrics.risk_mem.fetch_add(1, SeqCst);

        {
            let actions = self.actions.lock().unwrap().clone();
            (actions.warn)("Memory inuse reach threshold");
        }
        self.reclaim_heap();

        if let Some(mut mem_state) = self.calc_mem_risk() {
            if mem_state.magnif > DEF_MAX_MAGNIF {
                let actions = self.actions.lock().unwrap().clone();
                (actions.warn)("Memory pressure is abnormally high");
                mem_state.magnif = DEF_MAX_MAGNIF;
            }
            {
                let _g = self.mem_magnif_mu.lock().unwrap();
                self.do_set_mem_magnif(mem_state.magnif);
            }
            if let Err(err) = self.record_mem_state(&mem_state, "oom risk") {
                let actions = self.actions.lock().unwrap().clone();
                (actions.error)(&format!("Failed to save mem-risk: {err}"));
            }
        }

        if self.is_mem_no_risk() {
            self.wake();
        }
    }

    /// Go `handleMemRisk`.
    pub(crate) fn handle_mem_risk(&self, gc_executed: bool) {
        let now = self.inner_time();
        let oom_risk = self.hc_mem_inuse.load(SeqCst) > self.limit();
        let (dur, last_free, risk_start) = {
            let risk = self.hc_mem_risk.lock().unwrap();
            (
                now.duration_since(risk.last_stats_start_time)
                    .unwrap_or(Duration::ZERO),
                risk.last_heap_total_free,
                risk.start_time,
            )
        };
        if !oom_risk && dur < DEF_HEAP_RECLAIM_CHECK_DURATION {
            return;
        }
        let mut heap_use_bps = 0i64;
        if dur > Duration::ZERO {
            let heap_frees = self.hc_heap_total_free.load(SeqCst) - last_free;
            heap_use_bps = (heap_frees as f64 / dur.as_secs_f64()) as i64;
        }
        if oom_risk || mem_hang_risk(heap_use_bps, self.min_heap_free_bps(), now, risk_start) {
            self.enter_oom_risk();
            let mem_to_reclaim = self.hc_mem_inuse.load(SeqCst) - self.mem_risk();
            {
                let actions = self.actions.lock().unwrap().clone();
                (actions.warn)("`OOM RISK`: try to `KILL` running root pool");
            }

            let (new_kill_num, _reclaiming) = self.kill_topn_entry(mem_to_reclaim);
            if new_kill_num != 0 {
                let t = self.inner_time();
                {
                    let mut risk = self.hc_mem_risk.lock().unwrap();
                    risk.start_time = t;
                }
                self.hc_mem_risk_start_unix_milli.store(
                    t.duration_since(SystemTime::UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0),
                    SeqCst,
                );
                let actions = self.actions.lock().unwrap().clone();
                (actions.warn)("Restart runtime memory check");
            } else {
                let under_kill_num = {
                    let uk = self.under_kill.lock().unwrap();
                    uk.entries
                        .values()
                        .filter(|e| !e.arbitrator_mu.lock().unwrap().under_kill.fail)
                        .count()
                };
                if under_kill_num == 0 {
                    let mut force_kill = 0;
                    loop {
                        let Some(entry) = self.front_task_entry() else {
                            break;
                        };
                        let ctx = entry.load_ctx();
                        if ctx_available(&ctx) {
                            let ctx = ctx.unwrap();
                            ctx.stop(ArbitratorStopReason::OomRiskKill);
                            self.exec_metrics.risk_oom_kill[entry.mem_priority() as usize]
                                .fetch_add(1, SeqCst);
                            force_kill += 1;
                            if self.remove_task(&entry) {
                                entry.wind_up(0, ArbitrateResult::Fail);
                            }
                        } else {
                            // Task with unavailable ctx: Go would loop
                            // forever fetching the same front entry; the
                            // source relies on ctx being available here.
                            break;
                        }
                    }
                    let actions = self.actions.lock().unwrap().clone();
                    if force_kill != 0 {
                        (actions.warn)(
                            "No more running root pool can be killed to resolve `OOM RISK`; KILL all awaiting tasks;",
                        );
                    } else {
                        (actions.warn)(
                            "No more running root pool or awaiting task can be terminated to resolve `OOM RISK`",
                        );
                    }
                }
            }
        } else {
            let actions = self.actions.lock().unwrap().clone();
            (actions.warn)("Runtime memory free speed meets require, start re-check");
        }

        if dur >= DEF_HEAP_RECLAIM_CHECK_DURATION {
            let mut risk = self.hc_mem_risk.lock().unwrap();
            risk.last_heap_total_free = self.hc_heap_total_free.load(SeqCst);
            risk.last_stats_start_time = self.inner_time();
        }

        if !gc_executed {
            self.gc();
        }
    }

    /// Go `killTopnEntry`.
    pub(crate) fn kill_topn_entry(&self, required: i64) -> (i32, i64) {
        let mut new_kill_num = 0;
        let mut reclaimed = 0i64;

        if self.under_kill.lock().unwrap().num > 0 {
            let now = self.inner_time();
            let entries: Vec<Arc<RootPoolEntry>> = self
                .under_kill
                .lock()
                .unwrap()
                .entries
                .values()
                .cloned()
                .collect();
            for entry in entries {
                let mut st = entry.arbitrator_mu.lock().unwrap();
                if st.under_kill.fail {
                    continue;
                }
                let deadline = st.under_kill.start_time + DEF_KILL_CANCEL_CHECK_TIMEOUT;
                if now >= deadline {
                    let actions = self.actions.lock().unwrap().clone();
                    (actions.error)(&format!(
                        "Failed to `KILL` root pool due to timeout: uid={} name={}",
                        entry.pool.uid(),
                        entry.pool.name()
                    ));
                    st.under_kill.fail = true;
                    continue;
                }
                reclaimed += st.under_kill.reclaim;
            }
        }

        if reclaimed >= required {
            return (new_kill_num, reclaimed);
        }

        for prio in PRIORITIES {
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
                for (uid, entry) in entries {
                    if entry.arbitrator_mu.lock().unwrap().under_kill.start || entry.not_running() {
                        continue;
                    }
                    let ctx = entry.load_ctx();
                    if ctx_available(&ctx) {
                        let ctx = ctx.unwrap();
                        let memory_used = ctx.helper().map(|h| h.heap_inuse()).unwrap_or(0);
                        if memory_used <= 0 {
                            continue;
                        }
                        self.add_under_kill(&entry, memory_used, self.inner_time());
                        reclaimed += memory_used;
                        ctx.stop(ArbitratorStopReason::OomRiskKill);
                        new_kill_num += 1;
                        self.exec_metrics.risk_oom_kill[prio as usize].fetch_add(1, SeqCst);
                        {
                            let actions = self.actions.lock().unwrap().clone();
                            (actions.warn)(&format!(
                                "Start to `KILL` root pool: uid={uid} mem-used={memory_used}"
                            ));
                        }
                        if self.remove_task(&entry) {
                            let actions = self.actions.lock().unwrap().clone();
                            (actions.warn)(&format!(
                                "Make the mem quota subscription failed: uid={uid}"
                            ));
                            entry.wind_up(0, ArbitrateResult::Fail);
                        }
                        if reclaimed >= required {
                            return (new_kill_num, reclaimed);
                        }
                    }
                }
            }
        }
        (new_kill_num, reclaimed)
    }

    // ----- mem state recording -----

    pub(crate) fn last_mem_state(&self) -> Option<RuntimeMemStateV1> {
        *self.hc_last_mem_state.lock().unwrap()
    }

    /// Go `recordMemState`.
    pub(crate) fn record_mem_state(
        &self,
        s: &RuntimeMemStateV1,
        reason: &str,
    ) -> Result<(), String> {
        let recorder = self.hc_recorder_mu.lock().unwrap();
        *self.hc_last_mem_state.lock().unwrap() = Some(*s);
        if let Err(err) = recorder.store(s) {
            self.exec_metrics.action_record_fail.fetch_add(1, SeqCst);
            return Err(err);
        }
        self.exec_metrics.action_record_succ.fetch_add(1, SeqCst);
        let actions = self.actions.lock().unwrap().clone();
        (actions.info)(&format!("Record mem state: reason={reason} data={s:?}"));
        Ok(())
    }

    /// Go `TaskNumByPattern`.
    pub fn task_num_by_pattern(&self) -> NumByPattern {
        let mut res = NumByPattern::default();
        for p in PRIORITIES {
            res[p as usize] = self.task_num_by_priority(p);
        }
        res[ARBITRATION_WAIT_AVERSE] = self.task_num_of_wait_averse();
        res
    }
}
