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

//! Ristretto store.go and ttl.go. TTL lookup is immediate; reclamation uses
//! completed expiration buckets and runs independently of read traffic.
use crate::Item;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwapOption;
use foldhash::fast::RandomState;
use parking_lot::ReentrantMutex;

// Sharing each item keeps a copy of the shard from cloning unrelated values.
type Items<V> = HashMap<u64, Arc<Item<V>>, RandomState>;

const READS_BEFORE_PUBLISH: usize = 16;
const READS_AFTER_INVALIDATION: usize = 512;

struct LockedShard<V> {
    items: RefCell<Arc<Items<V>>>,
    reads_since_write: Cell<usize>,
    publish_after: Cell<usize>,
}

struct StoreShard<V> {
    locked: ReentrantMutex<LockedShard<V>>,
    published: ArcSwapOption<Items<V>>,
    // A hint only. The ArcSwap guard protects a published map.
    has_snapshot: AtomicBool,
}

impl<V: Clone> StoreShard<V> {
    fn new() -> Self {
        Self {
            locked: ReentrantMutex::new(LockedShard {
                items: RefCell::new(Arc::new(Items::default())),
                reads_since_write: Cell::new(0),
                publish_after: Cell::new(READS_BEFORE_PUBLISH),
            }),
            published: ArcSwapOption::empty(),
            has_snapshot: AtomicBool::new(false),
        }
    }

    fn read<R>(&self, publish: bool, read: impl FnOnce(&Items<V>) -> R) -> R {
        if self.has_snapshot.load(Ordering::Relaxed) {
            let snapshot = self.published.load();
            if let Some(items) = snapshot.as_ref() {
                return read(items);
            }
        }
        // Cloning a value may call back into a lookup on the same shard.
        let locked = self.locked.lock();
        let items = locked.items.borrow();
        let count = locked.reads_since_write.get();
        let publish_after = locked.publish_after.get();
        if publish && count < publish_after {
            locked.reads_since_write.set(count + 1);
            if count + 1 == publish_after {
                self.published.store(Some(Arc::clone(&items)));
                self.has_snapshot.store(true, Ordering::Relaxed);
            }
        }
        read(&items)
    }

    fn write<R>(&self, update: impl FnOnce(&mut Items<V>) -> R) -> R {
        let locked = self.locked.lock();
        if locked.reads_since_write.get() == locked.publish_after.get() {
            self.published.store(None);
            self.has_snapshot.store(false, Ordering::Relaxed);
            // Wait for a longer read streak after a write replaces a snapshot.
            locked.publish_after.set(READS_AFTER_INVALIDATION);
        }
        locked.reads_since_write.set(0);
        // Only copy the shard if an overlapping read still holds its snapshot.
        let mut items = locked.items.borrow_mut();
        update(Arc::make_mut(&mut items))
    }
}

pub(crate) struct Store<V> {
    shards: Vec<StoreShard<V>>,
    expiration: Mutex<HashMap<u128, HashMap<u64, u64>>>,
    bucket_duration: Duration,
}
impl<V: Clone> Store<V> {
    pub fn new(bucket_duration: Duration) -> Self {
        Self {
            shards: (0..256).map(|_| StoreShard::new()).collect(),
            expiration: Mutex::new(HashMap::new()),
            bucket_duration,
        }
    }
    fn shard(&self, key: u64) -> &StoreShard<V> {
        &self.shards[(key % 256) as usize]
    }
    pub fn get(&self, key: u64, conflict: u64) -> Option<V> {
        self.get_with(key, conflict, V::clone)
    }
    pub fn get_with<R>(
        &self,
        key: u64,
        conflict: u64,
        inspect: impl FnOnce(&V) -> R,
    ) -> Option<R> {
        self.shard(key).read(true, |shard| {
            let item = shard.get(&key)?;
            if conflict != 0 && conflict != item.conflict {
                return None;
            }
            if item
                .expiration
                .is_some_and(|expiration| SystemTime::now() > expiration)
            {
                return None;
            }
            item.value.as_ref().map(inspect)
        })
    }
    pub fn expiration(&self, key: u64) -> Option<SystemTime> {
        self.shard(key)
            .read(false, |shard| shard.get(&key).and_then(|i| i.expiration))
    }
    pub fn set(&self, item: Item<V>) {
        self.shard(item.key).write(|shard| {
            let old = shard.get(&item.key);
            if old.is_some_and(|old| item.conflict != 0 && item.conflict != old.conflict) {
                return;
            }
            self.update_expiration(
                item.key,
                item.conflict,
                old.and_then(|i| i.expiration),
                item.expiration,
            );
            shard.insert(item.key, Arc::new(item));
        });
    }
    pub fn update(&self, item: &Item<V>) -> Option<V> {
        self.shard(item.key).write(|shard| {
            let old = shard.get_mut(&item.key)?;
            if item.conflict != 0 && item.conflict != old.conflict {
                return None;
            }
            self.update_expiration(item.key, item.conflict, old.expiration, item.expiration);
            std::mem::replace(Arc::make_mut(old), item.clone()).value
        })
    }
    pub fn del(&self, key: u64, conflict: u64) -> Option<Item<V>> {
        self.shard(key).write(|shard| {
            if shard
                .get(&key)
                .is_some_and(|i| conflict != 0 && conflict != i.conflict)
            {
                return None;
            }
            let item = Arc::unwrap_or_clone(shard.remove(&key)?);
            self.update_expiration(key, conflict, item.expiration, None);
            Some(item)
        })
    }
    fn bucket(&self, time: SystemTime) -> u128 {
        time.duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            / self.bucket_duration.as_nanos()
            + 1
    }
    fn update_expiration(
        &self,
        key: u64,
        conflict: u64,
        old: Option<SystemTime>,
        new: Option<SystemTime>,
    ) {
        if old.is_none() && new.is_none() {
            return;
        }
        let mut expiration = self.expiration.lock().unwrap();
        if let Some(bucket) = old.and_then(|old| expiration.get_mut(&self.bucket(old))) {
            bucket.remove(&key);
        }
        if let Some(new) = new {
            expiration
                .entry(self.bucket(new))
                .or_default()
                .insert(key, conflict);
        }
    }
    pub fn expired_bucket(&self, now: SystemTime) -> HashMap<u64, u64> {
        self.expiration
            .lock()
            .unwrap()
            .remove(&(self.bucket(now) - 1))
            .unwrap_or_default()
    }
    pub fn clear(&self, mut on_evict: impl FnMut(Item<V>)) {
        for shard in &self.shards {
            // Rust releases the lock before invoking application callbacks.
            // Clear is quiesced by Cache; no callback can observe freed values.
            let values = shard.write(std::mem::take);
            for (_, item) in values {
                let mut item = Arc::unwrap_or_clone(item);
                item.cost = 0;
                item.expiration = None;
                on_evict(item);
            }
        }
        self.expiration.lock().unwrap().clear();
    }
    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.read(false, HashMap::len))
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Weak, atomic::AtomicUsize, mpsc};

    struct CountedValue(Arc<AtomicUsize>);

    impl Clone for CountedValue {
        fn clone(&self) -> Self {
            self.0.fetch_add(1, Ordering::Relaxed);
            Self(Arc::clone(&self.0))
        }
    }

    #[test]
    fn a_live_snapshot_preserves_items_without_cloning_unrelated_values() {
        let store = Store::new(Duration::from_secs(5));
        let clones = Arc::new(AtomicUsize::new(0));
        let item = |key| Item {
            key,
            conflict: 0,
            value: Some(CountedValue(Arc::clone(&clones))),
            cost: 1,
            expiration: None,
        };
        store.set(item(1));
        for _ in 1..READS_BEFORE_PUBLISH {
            assert!(store.get(1, 0).is_some());
        }
        assert!(store.shard(1).published.load().is_none());
        assert!(store.get(1, 0).is_some());
        let previous = store.shard(1).published.load();
        store.set(item(257));

        assert!(store.shard(1).published.load().is_none());
        assert!(previous.as_ref().unwrap().contains_key(&1));
        assert!(!previous.as_ref().unwrap().contains_key(&257));
        assert_eq!(clones.load(Ordering::Relaxed), READS_BEFORE_PUBLISH);
        assert_eq!(store.len(), 2);
        for _ in 1..READS_AFTER_INVALIDATION {
            assert!(store.get(257, 0).is_some());
        }
        assert!(store.shard(1).published.load().is_none());
        assert!(store.get(257, 0).is_some());
        assert!(
            store
                .shard(1)
                .published
                .load()
                .as_ref()
                .unwrap()
                .contains_key(&257)
        );
    }

    #[test]
    fn cloning_a_value_can_read_its_shard() {
        struct ReadOnClone(Weak<Store<Self>>);

        impl Clone for ReadOnClone {
            fn clone(&self) -> Self {
                let store = self.0.upgrade().unwrap();
                assert_eq!(store.expiration(1), None);
                Self(self.0.clone())
            }
        }

        let (sender, receiver) = mpsc::channel();
        let lookup = std::thread::spawn(move || {
            let store = Arc::new(Store::new(Duration::from_secs(5)));
            store.set(Item {
                key: 1,
                conflict: 0,
                value: Some(ReadOnClone(Arc::downgrade(&store))),
                cost: 1,
                expiration: None,
            });
            assert!(store.get(1, 0).is_some());
            sender.send(()).unwrap();
        });
        receiver.recv_timeout(Duration::from_secs(5)).unwrap();
        lookup.join().unwrap();
    }
}
