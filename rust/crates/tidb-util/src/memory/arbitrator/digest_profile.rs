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

//! `MemArbitrator` digest-profile cache and pool-allocation statistics:
//! the sharded per-digest memory-consumption profiles, their shrink policy,
//! and the timed maps behind `record_mem_consumed` / buffer tuning. Split
//! out of `arbitrator.rs`; mirrors the digest-profile and pool-alloc
//! statistics sections of Go `pkg/util/memory/arbitrator.go`.

use super::*;

impl MemArbitrator {
    // ----- digest profile cache -----

    pub(crate) fn reset_digest_profile_cache(&self, shard_num: u64) {
        let mut shards = Vec::with_capacity(shard_num as usize);
        for _ in 0..shard_num {
            shards.push(Arc::new(DigestShard {
                map: Mutex::new(HashMap::new()),
                num: AtomicI64::new(0),
            }));
        }
        *self.digest_shards.lock().unwrap() = shards;
        self.digest_shards_mask.store(shard_num - 1, SeqCst);
        self.digest_num.store(0, SeqCst);
        self.digest_limit
            .store(DEF_MAX_DIGEST_PROFILE_CACHE_LIMIT, SeqCst);
    }

    /// Go `SetDigestProfileCacheLimit`.
    pub fn set_digest_profile_cache_limit(&self, limit: i64) {
        self.digest_limit.store(limit.clamp(0, DEF_MAX), SeqCst);
    }

    pub(super) fn digest_shard(&self, digest_id: u64) -> Arc<DigestShard> {
        let shards = self.digest_shards.lock().unwrap();
        let mask = self.digest_shards_mask.load(SeqCst);
        Arc::clone(&shards[(digest_id & mask) as usize])
    }

    /// Go `GetDigestProfileCache`.
    pub fn get_digest_profile_cache(&self, digest_id: u64, utime_sec: i64) -> Option<i64> {
        let shard = self.digest_shard(digest_id);
        let pf = shard.map.lock().unwrap().get(&digest_id).cloned()?;
        if utime_sec > pf.last_fetch_utime_sec.load(SeqCst) {
            pf.last_fetch_utime_sec.store(utime_sec, SeqCst);
        }
        Some(pf.max_val.load(SeqCst))
    }

    /// Go `UpdateDigestProfileCache`.
    pub fn update_digest_profile_cache(&self, digest_id: u64, mem_consumed: i64, utime_sec: i64) {
        let shard = self.digest_shard(digest_id);
        let pf = {
            let mut map = shard.map.lock().unwrap();
            match map.get(&digest_id) {
                Some(p) => Arc::clone(p),
                None => {
                    let p = Arc::new(DigestProfile::default());
                    map.insert(digest_id, Arc::clone(&p));
                    shard.num.fetch_add(1, SeqCst);
                    self.digest_num.fetch_add(1, SeqCst);
                    p
                }
            }
        };

        const MAX_NUM: i64 = (2 + DEF_REDUNDANCY) as i64;
        const MAX_DUR: i64 = MAX_NUM - DEF_REDUNDANCY as i64;

        let ts_align = utime_sec / DEF_UPDATE_BUFFER_TIME_ALIGN_SEC;
        let tar_idx = (ts_align % MAX_NUM) as usize;

        {
            let ori_ts = pf.timed_map[tar_idx].read().unwrap().ts_align.load(SeqCst);
            if ori_ts < ts_align && ori_ts != 0 {
                // Exclusive lock on purpose (Go `Lock()`): serialize the
                // reset against concurrent RLock readers.
                #[allow(clippy::readonly_write_lock)]
                let tar = pf.timed_map[tar_idx].write().unwrap();
                let ori_ts = tar.ts_align.load(SeqCst);
                if ori_ts < ts_align && ori_ts != 0 {
                    tar.ts_align.store(0, SeqCst);
                    tar.max_val.store(0, SeqCst);
                }
            }
        }

        let mut clean_next = false;
        {
            let tar = pf.timed_map[tar_idx].read().unwrap();
            let mut update_size = false;

            if tar.ts_align.load(SeqCst) == 0
                && tar
                    .ts_align
                    .compare_exchange(0, ts_align, SeqCst, SeqCst)
                    .is_ok()
            {
                clean_next = true;
            }

            loop {
                let old_val = tar.max_val.load(SeqCst);
                if old_val >= mem_consumed {
                    break;
                }
                if tar
                    .max_val
                    .compare_exchange(old_val, mem_consumed, SeqCst, SeqCst)
                    .is_ok()
                {
                    update_size = true;
                    break;
                }
            }

            if update_size {
                let mut maxv = tar.max_val.load(SeqCst);
                for i in 0..MAX_DUR {
                    let d_idx = ((MAX_NUM + ts_align - i) % MAX_NUM) as usize;
                    let d = pf.timed_map[d_idx].read().unwrap();
                    let ts = d.ts_align.load(SeqCst);
                    if ts > ts_align - MAX_DUR && ts <= ts_align {
                        maxv = maxv.max(d.max_val.load(SeqCst));
                    }
                }
                pf.max_val.store(maxv, SeqCst); // force update
            }
        }

        if utime_sec > pf.last_fetch_utime_sec.load(SeqCst) {
            pf.last_fetch_utime_sec.store(utime_sec, SeqCst);
        }

        if clean_next {
            let d_idx = (((ts_align + 1) % MAX_NUM) as usize).min(3);
            // Exclusive on purpose (Go `Lock()`), as above.
            #[allow(clippy::readonly_write_lock)]
            let d = pf.timed_map[d_idx].write().unwrap();
            let ts = d.ts_align.load(SeqCst);
            if ts < ts_align + 1 && ts != 0 {
                d.ts_align.store(0, SeqCst);
                d.max_val.store(0, SeqCst);
            }
        }
    }

    /// Go `shrinkDigestProfile`.
    pub(crate) fn shrink_digest_profile(&self, utime_sec: i64, limit: i64, shrink_to: i64) -> i64 {
        if self.digest_num.load(SeqCst) <= limit {
            return 0;
        }
        self.exec_metrics.shrink_digest.fetch_add(1, SeqCst);

        let mut shrinked = 0i64;
        let mut val_map = [0i64; DEF_POOL_QUOTA_SHARDS];
        let small_pool_limit = self.pool_alloc_stats.read().unwrap().small_pool_limit;

        let shards: Vec<Arc<DigestShard>> = self.digest_shards.lock().unwrap().clone();
        for d in &shards {
            if d.num.load(SeqCst) == 0 {
                continue;
            }
            let mut dn = 0i64;
            let snapshot: Vec<(u64, Arc<DigestProfile>)> = d
                .map
                .lock()
                .unwrap()
                .iter()
                .map(|(k, v)| (*k, Arc::clone(v)))
                .collect();
            for (k, pf) in snapshot {
                let max_val = pf.max_val.load(SeqCst);
                let timeout = if max_val > small_pool_limit {
                    DEF_DIGEST_PROFILE_MEM_TIMEOUT_SEC
                } else {
                    DEF_DIGEST_PROFILE_SMALL_MEM_TIMEOUT_SEC
                };
                if utime_sec - pf.last_fetch_utime_sec.load(SeqCst) > timeout
                    && d.map.lock().unwrap().remove(&k).is_some()
                {
                    d.num.fetch_add(-1, SeqCst);
                    dn += 1;
                    continue;
                }
                let index = get_quota_shard(max_val, DEF_POOL_QUOTA_SHARDS);
                val_map[index] += 1;
            }
            self.digest_num.fetch_add(-dn, SeqCst);
            shrinked += dn;
        }

        let mut to_shrink = self.digest_num.load(SeqCst) - shrink_to;
        if to_shrink <= 0 {
            return shrinked;
        }

        let mut shrink_max_size = DEF_MAX_LIMIT;
        {
            let mut n = 0i64;
            for (i, v) in val_map.iter().enumerate() {
                n += *v;
                if n >= to_shrink {
                    shrink_max_size = BASE_QUOTA_UNIT * (1 << i);
                    break;
                }
            }
        }

        for d in &shards {
            if d.num.load(SeqCst) == 0 {
                continue;
            }
            let mut dn = 0i64;
            let snapshot: Vec<(u64, Arc<DigestProfile>)> = d
                .map
                .lock()
                .unwrap()
                .iter()
                .map(|(k, v)| (*k, Arc::clone(v)))
                .collect();
            for (k, pf) in snapshot {
                if pf.max_val.load(SeqCst) < shrink_max_size
                    && d.map.lock().unwrap().remove(&k).is_some()
                {
                    d.num.fetch_add(-1, SeqCst);
                    to_shrink -= 1;
                    dn += 1;
                }
                if to_shrink <= 0 {
                    break;
                }
            }
            self.digest_num.fetch_add(-dn, SeqCst);
            shrinked += dn;
            if to_shrink <= 0 {
                break;
            }
        }
        shrinked
    }

    // ----- pool alloc statistics & buffer -----

    /// Go `recordMemConsumed`.
    pub(crate) fn record_mem_consumed(&self, mem_consumed: i64, utime_sec: i64) {
        let stats = self.pool_alloc_stats.read().unwrap();
        const MAX_NUM: i64 = (2 + DEF_REDUNDANCY) as i64;
        let ts_align = utime_sec / DEF_UPDATE_MEM_CONSUMED_TIME_ALIGN_SEC;
        let tar_idx = (ts_align % MAX_NUM) as usize;
        let tar = &self.pool_alloc_timed_elems[tar_idx];

        {
            let ori_ts = tar.ts_align.load(SeqCst);
            if ori_ts < ts_align && ori_ts != 0 {
                let _g = self.pool_alloc_timed_map[tar_idx].write().unwrap();
                let ori_ts = tar.ts_align.load(SeqCst);
                if ori_ts < ts_align && ori_ts != 0 {
                    tar.reset();
                }
            }
        }

        let mut clean_next = false;
        {
            let _g = self.pool_alloc_timed_map[tar_idx].read().unwrap();
            if tar.ts_align.load(SeqCst) == 0
                && tar
                    .ts_align
                    .compare_exchange(0, ts_align, SeqCst, SeqCst)
                    .is_ok()
            {
                clean_next = true;
            }
            let pos = (mem_consumed / stats.pool_alloc_unit).min(DEF_SERVERLIMIT_MIN_UNIT_NUM - 1);
            tar.slot[pos as usize].fetch_add(1, SeqCst);
            tar.num.fetch_add(1, SeqCst);
        }

        if clean_next {
            let d_idx = (((ts_align + 1) % MAX_NUM) as usize).min(3);
            let d = &self.pool_alloc_timed_elems[d_idx];
            let _g = self.pool_alloc_timed_map[d_idx].write().unwrap();
            let v = d.ts_align.load(SeqCst);
            if v < ts_align + 1 && v != 0 {
                d.reset();
            }
        }
    }

    /// Go `tryToUpdateBuffer`.
    pub(crate) fn try_to_update_buffer(&self, mem_consumed: i64, utime_sec: i64) {
        const MAX_NUM: i64 = (2 + DEF_REDUNDANCY) as i64;
        const MAX_DUR: i64 = MAX_NUM - DEF_REDUNDANCY as i64;

        let ts_align = utime_sec / DEF_UPDATE_BUFFER_TIME_ALIGN_SEC;
        let tar_idx = (ts_align % MAX_NUM) as usize;
        let tar = &self.buffer_timed_elems[tar_idx];

        {
            let ori_ts = tar.ts.load(SeqCst);
            if ori_ts < ts_align && ori_ts != 0 {
                let _g = self.buffer_timed_map[tar_idx].write().unwrap();
                let ori_ts = tar.ts.load(SeqCst);
                if ori_ts < ts_align && ori_ts != 0 {
                    tar.ts.store(0, SeqCst);
                    tar.size.store(0, SeqCst);
                    tar.quota.store(0, SeqCst);
                }
            }
        }

        let mut clean_next = false;
        {
            let _g = self.buffer_timed_map[tar_idx].read().unwrap();
            let mut update_size = false;
            let mut mem_consumed = mem_consumed;

            if tar.ts.load(SeqCst) == 0
                && tar.ts.compare_exchange(0, ts_align, SeqCst, SeqCst).is_ok()
            {
                clean_next = true;
            }

            loop {
                let old_val = tar.size.load(SeqCst);
                if old_val >= mem_consumed {
                    break;
                }
                if tar
                    .size
                    .compare_exchange(old_val, mem_consumed, SeqCst, SeqCst)
                    .is_ok()
                {
                    update_size = true;
                    break;
                }
            }

            if update_size {
                for i in 0..MAX_DUR {
                    let d_idx = ((MAX_NUM + ts_align - i) % MAX_NUM) as usize;
                    let d = &self.buffer_timed_elems[d_idx];
                    let ts = d.ts.load(SeqCst);
                    if ts > ts_align - MAX_DUR && ts <= ts_align {
                        mem_consumed = mem_consumed.max(d.size.load(SeqCst));
                    }
                }
                if self.buffer_size.load(SeqCst) != mem_consumed {
                    self.set_buffer_size(mem_consumed);
                }
            }
        }

        if clean_next {
            let d_idx = (((ts_align + 1) % MAX_NUM) as usize).min(3);
            let d = &self.buffer_timed_elems[d_idx];
            let _g = self.buffer_timed_map[d_idx].write().unwrap();
            let v = d.ts.load(SeqCst);
            if v < ts_align + 1 && v != 0 {
                d.ts.store(0, SeqCst);
                d.size.store(0, SeqCst);
                d.quota.store(0, SeqCst);
            }
        }
    }
}
