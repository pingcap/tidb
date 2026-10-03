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
use std::collections::HashMap;
use std::sync::{Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(crate) struct Store<V> {
    shards: Vec<RwLock<HashMap<u64, Item<V>>>>,
    expiration: Mutex<HashMap<u128, HashMap<u64, u64>>>,
    bucket_duration: Duration,
}
impl<V: Clone> Store<V> {
    pub fn new(bucket_duration: Duration) -> Self {
        Self {
            shards: (0..256).map(|_| RwLock::new(HashMap::new())).collect(),
            expiration: Mutex::new(HashMap::new()),
            bucket_duration,
        }
    }
    fn shard(&self, key: u64) -> &RwLock<HashMap<u64, Item<V>>> {
        &self.shards[(key % 256) as usize]
    }
    pub fn get(&self, key: u64, conflict: u64) -> Option<V> {
        let shard = self.shard(key).read().unwrap();
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
        item.value.clone()
    }
    pub fn expiration(&self, key: u64) -> Option<SystemTime> {
        self.shard(key)
            .read()
            .unwrap()
            .get(&key)
            .and_then(|i| i.expiration)
    }
    pub fn set(&self, item: Item<V>) {
        let mut shard = self.shard(item.key).write().unwrap();
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
        shard.insert(item.key, item);
    }
    pub fn update(&self, item: &Item<V>) -> Option<V> {
        let mut shard = self.shard(item.key).write().unwrap();
        let old = shard.get_mut(&item.key)?;
        if item.conflict != 0 && item.conflict != old.conflict {
            return None;
        }
        self.update_expiration(item.key, item.conflict, old.expiration, item.expiration);
        let old = std::mem::replace(old, item.clone());
        old.value
    }
    pub fn del(&self, key: u64, conflict: u64) -> Option<Item<V>> {
        let mut shard = self.shard(key).write().unwrap();
        if shard
            .get(&key)
            .is_some_and(|i| conflict != 0 && conflict != i.conflict)
        {
            return None;
        }
        let item = shard.remove(&key)?;
        self.update_expiration(key, conflict, item.expiration, None);
        Some(item)
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
            let values = std::mem::take(&mut *shard.write().unwrap());
            for (_, mut item) in values {
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
            .map(|shard| shard.read().unwrap().len())
            .sum()
    }
}
