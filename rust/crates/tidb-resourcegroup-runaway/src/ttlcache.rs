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

//! The slice of `github.com/jellydator/ttlcache/v3` (v3.0.1) the watch list
//! uses: `WithTTL(NoTTL)`, `WithCapacity`, `WithDisableTouchOnHit`, `Set`,
//! `Get`, `Delete`, `Items`, `DeleteExpired`, `Start` / `Stop` and the
//! insertion / eviction subscriptions.
//!
//! Two differences from the library, both at its edges: the subscription
//! functions run on the caller's thread after the cache lock is released
//! (the library starts a goroutine per event), and `Set` over a key whose
//! item has expired but not yet been cleaned evicts that item first (the
//! library leaves it in its LRU list, where its later eviction deletes the
//! new item's map entry).

use std::collections::{BTreeMap, HashMap};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

/// `ttlcache.EvictionReason`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EvictionReason {
    /// `EvictionReasonDeleted`.
    Deleted,
    /// `EvictionReasonCapacityReached`.
    CapacityReached,
    /// `EvictionReasonExpired`.
    Expired,
}

struct Item<V> {
    value: V,
    /// `Item.expiresAt`; `None` for an item without TTL.
    expires_at: Option<Instant>,
    /// The item's place in the LRU list: larger is more recent.
    lru: u64,
}

impl<V> Item<V> {
    fn is_expired(&self, now: Instant) -> bool {
        self.expires_at.is_some_and(|at| at < now)
    }
}

struct Items<V> {
    values: HashMap<String, Item<V>>,
    /// `items.lru`, from least to most recent.
    lru: BTreeMap<u64, String>,
    next_lru: u64,
    stopped: bool,
}

type InsertionFn<V> = Box<dyn Fn(&V) + Send + Sync>;
type EvictionFn<V> = Box<dyn Fn(EvictionReason, &V) + Send + Sync>;

/// `ttlcache.Cache[string, V]` with `NoTTL` as the default TTL.
pub(crate) struct Cache<V> {
    capacity: usize,
    items: Mutex<Items<V>>,
    /// `items.timerCh`: wakes `Start` when the earliest expiry may move.
    timer: Condvar,
    insertion: Mutex<Vec<InsertionFn<V>>>,
    eviction: Mutex<Vec<EvictionFn<V>>>,
}

/// The events one locked operation produced, delivered after unlocking.
struct Events<V> {
    inserted: Vec<V>,
    evicted: Vec<(EvictionReason, V)>,
}

impl<V: Clone> Cache<V> {
    /// `ttlcache.New(WithTTL(NoTTL), WithCapacity(capacity), WithDisableTouchOnHit())`.
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            items: Mutex::new(Items { values: HashMap::new(), lru: BTreeMap::new(), next_lru: 0, stopped: false }),
            timer: Condvar::new(),
            insertion: Mutex::new(Vec::new()),
            eviction: Mutex::new(Vec::new()),
        }
    }

    /// `OnInsertion`.
    pub(crate) fn on_insertion(&self, callback: impl Fn(&V) + Send + Sync + 'static) {
        self.insertion.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push(Box::new(callback));
    }

    /// `OnEviction`.
    pub(crate) fn on_eviction(&self, callback: impl Fn(EvictionReason, &V) + Send + Sync + 'static) {
        self.eviction.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push(Box::new(callback));
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Items<V>> {
        self.items.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn deliver(&self, events: Events<V>) {
        if !events.inserted.is_empty() {
            let callbacks = self.insertion.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            for value in &events.inserted {
                for callback in callbacks.iter() {
                    callback(value);
                }
            }
        }
        if !events.evicted.is_empty() {
            let callbacks = self.eviction.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            for (reason, value) in &events.evicted {
                for callback in callbacks.iter() {
                    callback(*reason, value);
                }
            }
        }
    }

    fn evict(items: &mut Items<V>, key: &str, reason: EvictionReason, events: &mut Events<V>) {
        if let Some(item) = items.values.remove(key) {
            items.lru.remove(&item.lru);
            events.evicted.push((reason, item.value));
        }
    }

    /// `Set`: `ttl` zero is the default TTL, which is `NoTTL`.
    pub(crate) fn set(&self, key: &str, value: V, ttl: Duration) {
        let mut events = Events { inserted: Vec::new(), evicted: Vec::new() };
        {
            let mut items = self.lock();
            let now = Instant::now();
            let expires_at = (!ttl.is_zero()).then(|| now + ttl);
            items.next_lru += 1;
            let lru = items.next_lru;
            let live = items.values.get(key).is_some_and(|item| !item.is_expired(now));
            if live {
                // update/overwrite an existing item
                let item = items.values.get_mut(key).expect("checked above");
                let old_lru = std::mem::replace(&mut item.lru, lru);
                item.value = value;
                item.expires_at = expires_at;
                items.lru.remove(&old_lru);
                items.lru.insert(lru, key.to_owned());
            } else {
                Self::evict(&mut items, key, EvictionReason::Expired, &mut events);
                if self.capacity != 0 && items.values.len() >= self.capacity {
                    // delete the oldest item
                    if let Some((_, oldest)) = items.lru.first_key_value() {
                        let oldest = oldest.clone();
                        Self::evict(&mut items, &oldest, EvictionReason::CapacityReached, &mut events);
                    }
                }
                events.inserted.push(value.clone());
                items.values.insert(key.to_owned(), Item { value, expires_at, lru });
                items.lru.insert(lru, key.to_owned());
            }
        }
        self.timer.notify_all();
        self.deliver(events);
    }

    /// `Get` without touching: the live item, which becomes the most recent.
    pub(crate) fn get(&self, key: &str) -> Option<V> {
        let mut items = self.lock();
        let now = Instant::now();
        let item = items.values.get(key).filter(|item| !item.is_expired(now))?;
        let (old_lru, value) = (item.lru, item.value.clone());
        items.next_lru += 1;
        let lru = items.next_lru;
        items.lru.remove(&old_lru);
        items.lru.insert(lru, key.to_owned());
        items.values.get_mut(key).expect("present").lru = lru;
        Some(value)
    }

    /// `Delete`.
    pub(crate) fn delete(&self, key: &str) {
        let mut events = Events { inserted: Vec::new(), evicted: Vec::new() };
        Self::evict(&mut self.lock(), key, EvictionReason::Deleted, &mut events);
        self.deliver(events);
    }

    /// `Items`: the live items.
    pub(crate) fn items(&self) -> Vec<V> {
        let items = self.lock();
        let now = Instant::now();
        items.values.values().filter(|item| !item.is_expired(now)).map(|item| item.value.clone()).collect()
    }

    /// `DeleteExpired`.
    pub(crate) fn delete_expired(&self) {
        let mut events = Events { inserted: Vec::new(), evicted: Vec::new() };
        {
            let mut items = self.lock();
            let now = Instant::now();
            let mut expired: Vec<(Instant, String)> = items
                .values
                .iter()
                .filter(|(_, item)| item.is_expired(now))
                .map(|(key, item)| (item.expires_at.expect("expired items have a TTL"), key.clone()))
                .collect();
            expired.sort();
            for (_, key) in expired {
                Self::evict(&mut items, &key, EvictionReason::Expired, &mut events);
            }
        }
        self.deliver(events);
    }

    /// `Start`: deletes expired items as they expire until [`Self::stop`].
    pub(crate) fn start(&self) {
        loop {
            let items = self.lock();
            if items.stopped {
                return;
            }
            // `NoTTL` with nothing to expire waits an hour.
            let wait = items
                .values
                .values()
                .filter_map(|item| item.expires_at)
                .min()
                .map_or(Duration::from_secs(3600), |at| {
                    at.saturating_duration_since(Instant::now()).max(Duration::from_micros(1))
                });
            let (items, timeout) =
                self.timer.wait_timeout(items, wait).unwrap_or_else(std::sync::PoisonError::into_inner);
            if items.stopped {
                return;
            }
            drop(items);
            if timeout.timed_out() {
                self.delete_expired();
            }
        }
    }

    /// `Stop`.
    pub(crate) fn stop(&self) {
        self.lock().stopped = true;
        self.timer.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex as StdMutex};

    #[test]
    fn capacity_evicts_the_least_recently_used_and_get_does_not_touch() {
        let cache = Cache::new(2);
        let evicted = Arc::new(StdMutex::new(Vec::new()));
        let seen = Arc::clone(&evicted);
        cache.on_eviction(move |reason, value: &i32| seen.lock().unwrap().push((reason, *value)));
        cache.set("a", 1, Duration::ZERO);
        cache.set("b", 2, Duration::ZERO);
        // `a` becomes the most recent.
        assert_eq!(cache.get("a"), Some(1));
        cache.set("c", 3, Duration::ZERO);
        assert_eq!(*evicted.lock().unwrap(), [(EvictionReason::CapacityReached, 2)]);
        assert_eq!(cache.get("b"), None);
        // Overwriting is no insertion and no eviction.
        cache.set("a", 10, Duration::ZERO);
        assert_eq!(cache.get("a"), Some(10));
        assert_eq!(evicted.lock().unwrap().len(), 1);
        cache.delete("a");
        assert_eq!(evicted.lock().unwrap()[1], (EvictionReason::Deleted, 10));
    }

    #[test]
    fn expired_items_vanish_from_get_and_items_then_evict() {
        let cache = Arc::new(Cache::new(10));
        let evicted = Arc::new(StdMutex::new(Vec::new()));
        let seen = Arc::clone(&evicted);
        cache.on_eviction(move |reason, value: &i32| seen.lock().unwrap().push((reason, *value)));
        let inserted = Arc::new(StdMutex::new(0));
        let count = Arc::clone(&inserted);
        cache.on_insertion(move |_| *count.lock().unwrap() += 1);
        cache.set("short", 1, Duration::from_millis(30));
        cache.set("forever", 2, Duration::ZERO);
        let cleaner = {
            let cache = Arc::clone(&cache);
            std::thread::spawn(move || cache.start())
        };
        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(cache.get("short"), None);
        assert_eq!(cache.items(), [2]);
        let deadline = Instant::now() + Duration::from_secs(5);
        while evicted.lock().unwrap().is_empty() {
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(*evicted.lock().unwrap(), [(EvictionReason::Expired, 1)]);
        assert_eq!(*inserted.lock().unwrap(), 2);
        cache.stop();
        cleaner.join().unwrap();
    }
}
