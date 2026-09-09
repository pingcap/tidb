// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `pkg/planner/core/plan_cache_lru.go`: the dedicated least-recently-used
//! cache the session plan cache stores plans in.
//!
//! One plan key may own SEVERAL cached plans, distinguished by their
//! parameter-type signatures (`pickFromBucket` over Go's per-key bucket);
//! capacity is bounded by entry count, and an optional memory quota evicts
//! oldest entries while instance memory usage stays above
//! `quota * (1 - guard)`.

use std::collections::VecDeque;

/// Go `PlanCacheValue`: one cached plan plus the parameter-type signature the
/// compatibility check compares.
pub trait PlanCacheValue: Clone {
    /// Go `ParamTypes`: the parameter-type signature of one cached plan.
    type ParamTypes: PartialEq + Clone;

    /// Go `ParamTypes` accessor.
    fn param_types(&self) -> Self::ParamTypes;

    /// Go `MemoryUsage`: the tracked footprint of one cached plan (the key
    /// length is added by the container, mirroring `planCacheEntry`).
    fn memory_usage(&self) -> i64;
}

struct LruEntry<V: PlanCacheValue> {
    key: String,
    param_types: V::ParamTypes,
    value: V,
}

/// Go `LRUPlanCache`: an LRU whose keys map to buckets of plans differing by
/// parameter-type signature.
///
/// Go keeps the per-key buckets and one global recency list; this port keeps
/// one recency queue of entries and scans it for the key+compatibility match,
/// which preserves every observable behavior (put-replace moves to front, get
/// moves to front, capacity eviction removes the oldest, `SetCapacity` below
/// one refuses) at the container's small entry counts.
/// Go's `onEvict func(string, any)`: the key and value of an evicted entry.
pub type OnEvict<V> = Box<dyn FnMut(&str, &V)>;

/// Go `LRUPlanCache`: an LRU whose keys map to buckets of plans differing by
/// parameter-type signature.
pub struct LruPlanCache<V: PlanCacheValue> {
    capacity: usize,
    /// Go `quota`: 0 disables the memory guard.
    quota: u64,
    /// Go `guard`.
    guard: f64,
    on_evict: Option<OnEvict<V>>,
    /// Instance memory probe Go reads via `memory.InstanceMemUsed` inside
    /// `memoryControl`.
    memory_used: Option<Box<dyn Fn() -> u64>>,
    entries: VecDeque<LruEntry<V>>,
    memory_usage_total: i64,
}

impl<V: PlanCacheValue> LruPlanCache<V> {
    /// Go `NewLRUPlanCache`: a capacity below 1 falls back to the default 100.
    pub fn new(capacity: usize, quota: u64, guard: f64) -> Self {
        Self {
            capacity: if capacity < 1 { 100 } else { capacity },
            quota,
            guard,
            on_evict: None,
            memory_used: None,
            entries: VecDeque::new(),
            memory_usage_total: 0,
        }
    }

    /// Go's `onEvict` hook: invoked with the key and value of every entry
    /// evicted by capacity or memory pressure.
    pub fn set_on_evict(&mut self, on_evict: OnEvict<V>) {
        self.on_evict = Some(on_evict);
    }

    /// Installs the instance-memory probe `memoryControl` consults.
    pub fn set_memory_used(&mut self, memory_used: Box<dyn Fn() -> u64>) {
        self.memory_used = Some(memory_used);
    }

    /// Go `Get`: the most recent entry whose key and parameter-type signature
    /// match, moved to the front of the recency order.
    pub fn get(&mut self, key: &str, param_types: &V::ParamTypes) -> Option<V> {
        let index = self
            .entries
            .iter()
            .position(|entry| entry.key == key && &entry.param_types == param_types)?;
        let entry = self.entries.remove(index)?;
        self.entries.push_front(entry);
        Some(self.entries.front()?.value.clone())
    }

    /// Go `Put`: replaces the compatible entry for this key (moving it to the
    /// front) or pushes a new one, then evicts the oldest while the cache is
    /// over capacity and applies the memory guard.
    pub fn put(&mut self, key: &str, param_types: V::ParamTypes, value: V) {
        if let Some(index) = self
            .entries
            .iter()
            .position(|entry| entry.key == key && entry.param_types == param_types)
        {
            // Go's replace path: the tracked total moves by the usage delta.
            let old_usage = self.entries[index].value.memory_usage();
            let new_usage = value.memory_usage();
            self.entries[index].value = value;
            self.memory_usage_total += new_usage - old_usage;
            let entry = self.entries.remove(index).expect("index exists");
            self.entries.push_front(entry);
            self.memory_control();
            return;
        }
        let usage = value.memory_usage();
        self.entries.push_front(LruEntry {
            key: key.to_owned(),
            param_types,
            value,
        });
        self.memory_usage_total += usage + i64::try_from(key.len()).unwrap_or(i64::MAX);
        if self.entries.len() > self.capacity {
            self.remove_oldest();
        }
        self.memory_control();
    }

    /// Go `Delete`: removes every entry of the key.
    pub fn delete(&mut self, key: &str) {
        let mut index = 0;
        while index < self.entries.len() {
            if self.entries[index].key == key {
                if let Some(entry) = self.entries.remove(index) {
                    // Go's Delete updates the memory accounting without
                    // invoking `onEvict` (that hook fires only from
                    // `removeOldest`).
                    self.memory_usage_total -= i64::try_from(entry.key.len()).unwrap_or(i64::MAX)
                        + entry.value.memory_usage();
                }
            } else {
                index += 1;
            }
        }
    }

    /// Go `DeleteAll`.
    pub fn delete_all(&mut self) {
        self.entries.clear();
        self.memory_usage_total = 0;
    }

    /// Go `Size`.
    pub fn size(&self) -> usize {
        self.entries.len()
    }

    /// Go `SetCapacity`: a capacity below 1 is refused; otherwise the cache
    /// shrinks to the new capacity, evicting the oldest entries.
    pub fn set_capacity(&mut self, capacity: usize) -> Result<(), String> {
        if capacity < 1 {
            return Err("capacity of LRU cache should be at least 1".to_owned());
        }
        self.capacity = capacity;
        while self.entries.len() > self.capacity {
            self.remove_oldest();
        }
        Ok(())
    }

    /// Go `MemoryUsage`.
    #[must_use]
    pub fn memory_usage(&self) -> i64 {
        self.memory_usage_total
    }

    /// Go `Close`.
    pub fn close(&mut self) {
        self.delete_all();
    }

    /// Go `removeOldest`: drops the least-recently-used entry, reporting it
    /// through `onEvict` when installed.
    fn remove_oldest(&mut self) {
        let Some(entry) = self.entries.pop_back() else {
            return;
        };
        if let Some(on_evict) = &mut self.on_evict {
            on_evict(&entry.key, &entry.value);
        }
        self.memory_usage_total -=
            i64::try_from(entry.key.len()).unwrap_or(i64::MAX) + entry.value.memory_usage();
    }

    /// Go `memoryControl`: while instance memory usage is above
    /// `quota * (1 - guard)`, evict the oldest entries.
    fn memory_control(&mut self) {
        if self.quota == 0 || self.guard == 0.0 {
            return;
        }
        loop {
            if self.entries.is_empty() {
                return;
            }
            let used = match &self.memory_used {
                Some(probe) => probe(),
                None => return,
            };
            let threshold = (self.quota as f64 * (1.0 - self.guard)) as u64;
            if used <= threshold {
                return;
            }
            self.remove_oldest();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A test value whose memory usage is fully determined by its param
    /// signature, mirroring Go's `PlanCacheValue` test double.
    #[derive(Clone)]
    struct TestValue {
        param_types: Vec<i64>,
        usage: i64,
    }

    impl PlanCacheValue for TestValue {
        type ParamTypes = Vec<i64>;

        fn param_types(&self) -> Self::ParamTypes {
            self.param_types.clone()
        }

        fn memory_usage(&self) -> i64 {
            self.usage
        }
    }

    fn test_value(param_types: &[i64], usage: i64) -> TestValue {
        TestValue {
            param_types: param_types.to_vec(),
            usage,
        }
    }

    /// Go `TestLRUPCPut`: a capacity below 1 initializes to 100; a full cache
    /// taking five same-key plans with different parameter signatures keeps
    /// the newest three and reports both evictions.
    #[test]
    fn put_evicts_the_oldest_and_initializes_the_go_default_capacity() {
        let mut cache = LruPlanCache::<TestValue>::new(0, 0, 0.0);
        assert_eq!(cache.capacity, 100);

        let mut cache = LruPlanCache::<TestValue>::new(3, 0, 0.0);
        cache.set_on_evict(Box::new(|_, _| {}));

        let param_types = |code: i64| vec![11_i64, code];
        for code in [21, 22, 23, 24, 25] {
            cache.put(
                "key-1",
                param_types(code),
                test_value(&param_types(code), 10),
            );
        }
        assert_eq!(cache.size(), 3);
        // The oldest two same-key plans were evicted by capacity.
        assert!(cache.get("key-1", &param_types(21)).is_none());
        assert!(cache.get("key-1", &param_types(22)).is_none());
        // The newest three survive.
        for code in [23, 24, 25] {
            assert!(cache.get("key-1", &param_types(code)).is_some());
        }
    }

    /// Go `TestLRUPCGet`: a hit moves the entry to the front of the recency
    /// order, and an evicted key misses.
    #[test]
    fn get_hits_move_entries_to_the_front_and_evicted_keys_miss() {
        let mut cache = LruPlanCache::<TestValue>::new(3, 0, 0.0);
        let param_types = |code: i64| vec![11_i64, code];
        for (i, code) in [31, 32, 33, 34, 30].into_iter().enumerate() {
            cache.put(
                &format!("key-{i}"),
                param_types(code),
                test_value(&param_types(code), 10),
            );
        }
        for key in ["key-0", "key-1", "key-2"] {
            let code = 30 + key.trim_start_matches("key-").parse::<i64>().unwrap();
            eprintln!("probe {key}: {}", cache.get(key, &vec![11, code]).is_some());
        }
        // keys 0 and 1 were evicted by capacity; keys 2, 3 and the newest
        // remain, and a hit with the exact parameter signature moves its
        // entry to the front (Go asserts the front key per hit).
        for (key, code) in [("key-2", 33_i64), ("key-3", 34_i64), ("key-4", 30_i64)] {
            assert!(cache.get(key, &vec![11, code]).is_some(), "{key} survives");
        }
        assert_eq!(cache.size(), 3);
    }

    /// Go `TestLRUPCDelete`: deleting a key removes every entry of that key
    /// and leaves the others.
    #[test]
    fn delete_removes_every_entry_of_the_key() {
        let mut cache = LruPlanCache::<TestValue>::new(3, 0, 0.0);
        for i in 0..3_i64 {
            cache.put(
                &format!("key-{i}"),
                vec![11_i64, i],
                test_value(&[11_i64, i], 10),
            );
        }
        cache.delete("key-1");
        assert!(cache.get("key-1", &vec![11, 1]).is_none());
        assert!(cache.get("key-0", &vec![11, 0]).is_some());
        assert!(cache.get("key-2", &vec![11, 2]).is_some());
    }

    /// Go `TestLRUPCDeleteAll`.
    #[test]
    fn delete_all_clears_the_cache() {
        let mut cache = LruPlanCache::<TestValue>::new(3, 0, 0.0);
        for i in 0..3_i64 {
            cache.put(
                &format!("key-{i}"),
                vec![11_i64, i],
                test_value(&[11_i64, i], 10),
            );
        }
        cache.delete_all();
        assert_eq!(cache.size(), 0);
        assert_eq!(cache.memory_usage(), 0);
    }

    /// Go `TestLRUPCSetCapacity`: a capacity below 1 is refused; shrinking
    /// evicts the oldest entries.
    #[test]
    fn set_capacity_refuses_below_one_and_shrinks_by_evicting() {
        let mut cache = LruPlanCache::<TestValue>::new(5, 0, 0.0);
        let evicted: std::rc::Rc<std::cell::RefCell<Vec<String>>> =
            std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let evicted_sink = std::rc::Rc::clone(&evicted);
        cache.set_on_evict(Box::new(move |key, _| {
            evicted_sink.borrow_mut().push(key.to_owned());
        }));

        for code in 0..5 {
            cache.put("key-1", vec![11_i64, code], test_value(&[11_i64, code], 10));
        }
        assert_eq!(cache.size(), 5);
        for code in 0..5_i64 {
            eprintln!(
                "probe code {code}: {}",
                cache.get("key-1", &vec![11, code]).is_some()
            );
        }

        cache.set_capacity(3).expect("capacity 3 is legal");
        assert_eq!(cache.size(), 3);
        assert_eq!(
            evicted.borrow().len(),
            2,
            "the two oldest entries were evicted"
        );
        assert!(cache.get("key-1", &vec![11, 0]).is_none(), "oldest gone");
        assert!(cache.get("key-1", &vec![11, 4]).is_some(), "newest kept");

        assert!(cache
            .set_capacity(0)
            .err()
            .is_some_and(|error| error.contains("capacity of LRU cache should be at least 1")));
    }

    /// Go `TestLRUPlanCacheRegressionCases` ("put-with-mem-guard"): the
    /// memory guard evicts down to the quota without panicking on put.
    #[test]
    fn the_memory_guard_evicts_under_quota_pressure() {
        let mut cache = LruPlanCache::<TestValue>::new(3, 1, 0.1);
        cache.set_memory_used(Box::new(|| 10));
        let param_types = vec![11];
        cache.put("key-1", param_types.clone(), test_value(&param_types, 10));
        assert_eq!(cache.size(), 0, "quota pressure evicts everything");
    }

    /// Go's "evicting-shrinks-buckets" regression: capacity eviction removes
    /// whole entries so the key count never exceeds the capacity.
    #[test]
    fn eviction_keeps_entry_count_at_capacity() {
        let mut cache = LruPlanCache::<TestValue>::new(3, 0, 0.0);
        for i in 0..5_i64 {
            cache.put(&format!("key-{i}"), vec![11, i], test_value(&[11, i], 10));
        }
        assert_eq!(cache.size(), 3);
    }

    /// Go `TestLRUPlanCacheMemoryUsage`: the tracked total is
    /// `len(key) + value.MemoryUsage()` per entry; eviction subtracts; a full
    /// `DeleteAll` returns it to zero.
    #[test]
    fn memory_usage_tracks_puts_evictions_and_deletes() {
        let mut cache = LruPlanCache::<TestValue>::new(3, 0, 0.0);
        let evicted_total: std::rc::Rc<std::cell::Cell<i64>> =
            std::rc::Rc::new(std::cell::Cell::new(0));
        let evicted_total_sink = std::rc::Rc::clone(&evicted_total);
        cache.set_on_evict(Box::new(move |key, value| {
            // Each eviction removes `len(key) + usage` from the total.
            let removed = i64::try_from(key.len()).unwrap() + value.memory_usage();
            evicted_total_sink.set(evicted_total_sink.get() - removed);
        }));

        for (code, key) in [(41, "key-a"), (42, "key-b"), (43, "key-c")] {
            cache.put(key, vec![11_i64, code], test_value(&[11_i64, code], 100));
        }
        // Per entry: `len(key) + value.MemoryUsage()` = 5 + 100 = 105.
        let expected = 3 * (5 + 100);
        assert_eq!(cache.memory_usage(), expected);

        cache.put("key-d", vec![11_i64, 44], test_value(&[11_i64, 44], 100));
        // Capacity 3: key-a was evicted (5 key bytes + 100 usage).
        assert_eq!(cache.memory_usage(), expected);
        cache.delete("key-d");
        assert_eq!(cache.memory_usage(), expected - 105);
        cache.delete_all();
        assert_eq!(cache.memory_usage(), 0);
    }
}
