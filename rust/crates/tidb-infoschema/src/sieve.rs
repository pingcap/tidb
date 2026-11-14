// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/infoschema/sieve.go`: byte-accounted SIEVE with a stable clock hand.
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

/// Go `sieveStatusHook`; callbacks run under the cache lock.
pub trait SieveStatusHook: std::fmt::Debug + Send + Sync {
    fn on_hit(&self) {}
    fn on_miss(&self) {}
    fn on_evict(&self) {}
    fn on_update(&self, _size: u64, _count: u64) {}
    fn on_update_limit(&self, _limit: u64) {}
}
impl SieveStatusHook for () {}

/// Go `sieveStatusHookImpl`, pkg/infoschema/metrics.go.
#[derive(Debug)]
pub(crate) struct MetricsHook;
impl SieveStatusHook for MetricsHook {
    fn on_hit(&self) {
        tidb_metrics::domain::INFOSCHEMA_V2_CACHE_COUNTER
            .with_label_values(&["hit"])
            .inc();
    }
    fn on_miss(&self) {
        tidb_metrics::domain::INFOSCHEMA_V2_CACHE_COUNTER
            .with_label_values(&["miss"])
            .inc();
    }
    fn on_evict(&self) {
        tidb_metrics::domain::INFOSCHEMA_V2_CACHE_COUNTER
            .with_label_values(&["evict"])
            .inc();
    }
    fn on_update(&self, size: u64, count: u64) {
        tidb_metrics::domain::INFOSCHEMA_V2_CACHE_SIZE.set(size as f64);
        tidb_metrics::domain::INFOSCHEMA_V2_CACHE_COUNT.set(count as f64);
    }
    fn on_update_limit(&self, limit: u64) {
        tidb_metrics::domain::INFOSCHEMA_V2_CACHE_LIMIT.set(limit as f64);
    }
}

/// Go `sieveStatusHook` observations, kept with the cache under its mutex.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Status {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

#[derive(Debug)]
struct Entry<K, V> {
    value: V,
    visited: bool,
    size: u64,
    prev: Option<K>,
    next: Option<K>,
}

#[derive(Debug)]
struct State<K, V> {
    items: HashMap<K, Entry<K, V>>,
    head: Option<K>,
    tail: Option<K>,
    hand: Option<K>,
    size: u64,
    capacity: u64,
    status: Status,
    hook: Arc<dyn SieveStatusHook>,
}

/// Go `Sieve`. Values own their allocations; outstanding readers may retain an
/// Arc after eviction, just as Go callers can retain a returned table.Table.
#[derive(Debug)]
pub struct Sieve<K, V>(Mutex<State<K, V>>);

impl<K: Clone + Eq + Hash, V: Clone> Sieve<K, V> {
    /// Go `newSieve`.
    pub fn new(capacity: u64) -> Self {
        Self(Mutex::new(State {
            items: HashMap::new(),
            head: None,
            tail: None,
            hand: None,
            size: 0,
            capacity,
            status: Status::default(),
            hook: Arc::new(()),
        }))
    }
    /// Go `Sieve.SetStatusHook`.
    pub fn set_status_hook(&self, hook: Arc<dyn SieveStatusHook>) {
        self.0.lock().unwrap().hook = hook;
    }
    /// Go `Sieve.SetCapacity`.
    pub fn set_capacity(&self, capacity: u64) {
        let mut state = self.0.lock().unwrap();
        state.capacity = capacity;
        state.hook.on_update_limit(capacity);
    }
    /// Go `Sieve.SetCapacityAndWaitEvict`.
    pub fn set_capacity_and_wait_evict(&self, capacity: u64) {
        self.set_capacity(capacity);
        loop {
            let mut s = self.0.lock().unwrap();
            if s.size <= s.capacity {
                break;
            }
            for _ in 0..10 {
                if s.size <= s.capacity {
                    break;
                }
                s.evict();
            }
        }
    }
    /// Go `Sieve.Capacity`.
    pub fn capacity(&self) -> u64 {
        self.0.lock().unwrap().capacity
    }
    /// Go `Sieve.Set`; like Go, eviction precedes insertion and is bounded to
    /// ten entries. Replacing an existing value retains its original charge.
    pub fn set(&self, key: K, value: V, size: u64) {
        let mut s = self.0.lock().unwrap();
        if let Some(e) = s.items.get_mut(&key) {
            e.value = value;
            e.visited = true;
            return;
        }
        for _ in 0..10 {
            if s.size <= s.capacity {
                break;
            }
            s.evict();
        }
        let next = s.head.clone();
        if let Some(head) = &next {
            s.items.get_mut(head).unwrap().prev = Some(key.clone());
        } else {
            s.tail = Some(key.clone());
        }
        s.head = Some(key.clone());
        s.size += size;
        s.items.insert(
            key,
            Entry {
                value,
                visited: false,
                size,
                prev: None,
                next,
            },
        );
        s.hook.on_update(s.size, s.items.len() as u64);
    }
    /// Go `Sieve.Get`.
    pub fn get(&self, key: &K) -> Option<V> {
        if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/infoschema/skipGet").is_some() {
            return None;
        }
        let mut s = self.0.lock().unwrap();
        if let Some(e) = s.items.get_mut(key) {
            e.visited = true;
            let value = e.value.clone();
            s.status.hits += 1;
            s.hook.on_hit();
            Some(value)
        } else {
            s.status.misses += 1;
            s.hook.on_miss();
            None
        }
    }
    /// Go `Sieve.Remove`.
    pub fn remove(&self, key: &K) -> bool {
        let mut s = self.0.lock().unwrap();
        if s.hand.as_ref() == Some(key) {
            s.hand = s.items.get(key).and_then(|e| e.prev.clone());
        }
        s.remove_entry(key)
    }
    /// Go `Sieve.Contains`.
    pub fn contains(&self, key: &K) -> bool {
        self.0.lock().unwrap().items.contains_key(key)
    }
    /// Go `Sieve.Peek`.
    pub fn peek(&self, key: &K) -> Option<V> {
        self.0
            .lock()
            .unwrap()
            .items
            .get(key)
            .map(|e| e.value.clone())
    }
    /// Go `Sieve.Size`.
    pub fn size(&self) -> u64 {
        self.0.lock().unwrap().size
    }
    /// Go `Sieve.Len`.
    pub fn len(&self) -> usize {
        self.0.lock().unwrap().items.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Go `Sieve.Purge` / `Close`; no detached worker survives this cache.
    pub fn purge(&self) {
        let mut s = self.0.lock().unwrap();
        s.items.clear();
        s.head = None;
        s.tail = None;
        s.hand = None;
        s.size = 0;
        s.hook.on_update(0, 0);
    }
    /// Go `sieveStatusHook` counters.
    pub fn status(&self) -> Status {
        self.0.lock().unwrap().status
    }
}

impl<K: Clone + Eq + Hash, V> State<K, V> {
    /// Go `Sieve.removeEntry`.
    fn remove_entry(&mut self, key: &K) -> bool {
        let Some(e) = self.items.remove(key) else {
            return false;
        };
        if let Some(prev) = &e.prev {
            self.items.get_mut(prev).unwrap().next = e.next.clone();
        } else {
            self.head = e.next.clone();
        }
        if let Some(next) = &e.next {
            self.items.get_mut(next).unwrap().prev = e.prev.clone();
        } else {
            self.tail = e.prev.clone();
        }
        self.size -= e.size;
        self.hook.on_update(self.size, self.items.len() as u64);
        true
    }
    /// Go `Sieve.evict`.
    fn evict(&mut self) {
        let mut key = self
            .hand
            .clone()
            .or_else(|| self.tail.clone())
            .expect("nonempty SIEVE");
        loop {
            let e = self.items.get_mut(&key).unwrap();
            if !e.visited {
                break;
            }
            e.visited = false;
            key = e.prev.clone().or_else(|| self.tail.clone()).unwrap();
        }
        self.hand = self.items[&key].prev.clone();
        self.remove_entry(&key);
        self.status.evictions += 1;
        self.hook.on_evict();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Go `TestGetAndSet`, pkg/infoschema/sieve_test.go.
    #[test]
    fn test_get_and_set() {
        let cache = Sieve::new(10 * 1024 * 1024);
        for i in 1..=10 {
            cache.set(i, i * 10, 64);
        }
        for i in 1..=10 {
            assert_eq!(cache.get(&i), Some(i * 10));
        }
    }
    /// Go `TestRemove`.
    #[test]
    fn test_remove() {
        let cache = Sieve::new(10 * 1024 * 1024);
        cache.set(1, 10, 64);
        assert_eq!(cache.get(&1), Some(10));
        assert!(cache.remove(&1));
        assert_eq!(cache.get(&1), None);
        assert!(!cache.remove(&-1));
    }
    /// Go `TestSievePolicy`.
    #[test]
    fn test_sieve_policy() {
        let cache = Sieve::new(10 * 64);
        for i in 1..=10 {
            cache.set(i, i, 64);
        }
        for i in 6..=10 {
            assert!(cache.get(&i).is_some());
        }
        for i in 1..=5 {
            cache.set(i * 10, i * 10, 64);
        }
        for i in 6..=10 {
            assert!(cache.get(&i).is_some());
        }
    }
    /// Go `TestContains`.
    #[test]
    fn test_contains() {
        let cache = Sieve::new(10 * 1024 * 1024);
        assert!(!cache.contains(&"hello"));
        cache.set("hello", "world", 64);
        assert!(cache.contains(&"hello"));
    }
    /// Go `TestCacheSize`.
    #[test]
    fn test_cache_size() {
        let cache = Sieve::new(10 * 1024 * 1024);
        assert_eq!(cache.size(), 0);
        cache.set(1, 1, 64);
        assert_eq!(cache.size(), 64);
        cache.set(1, 2, 128);
        assert_eq!(cache.size(), 64);
        assert_eq!(cache.get(&1), Some(2));
        cache.set(2, 2, 64);
        assert_eq!(cache.size(), 128);
    }
    /// Go `TestPurge`.
    #[test]
    fn test_purge() {
        let cache = Sieve::new(10 * 1024 * 1024);
        cache.set(1, 1, 64);
        cache.set(2, 2, 64);
        assert_eq!(cache.len(), 2);
        cache.purge();
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.size(), 0);
    }
}
