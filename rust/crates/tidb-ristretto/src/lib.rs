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

//! Native implementation of the github.com/dgraph-io/ristretto v0.1.1 root
//! package. Independent caches share code, never budgets or worker lifetimes.
//! New keys are published by the single admission worker; resident replacement
//! is immediate. Wait orders writes, not the deliberately lossy read stream.

#![cfg_attr(test, feature(test))]

mod key;
mod metrics;
mod policy;
mod ring;
mod sketch;
mod store;
pub use key::{Key, KeyRef};
pub use metrics::{LifeExpectancy, Metrics};

use crossbeam_channel::{bounded, Receiver, Sender};
use metrics::Metric;
use policy::Policy;
use ring::Ring;
use std::collections::{hash_map::RandomState, HashMap};
use std::fmt;
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc, Mutex, RwLock, RwLockReadGuard,
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime};
use store::Store;

/// Go's 64-bit storeItem charge. Keep configured byte budgets independent of
/// the monomorphized Rust value's inline size; large payloads live behind Arc.
pub const INTERNAL_ITEM_COST: i64 = 56;
const WRITE_BUFFER: usize = 32_768;
const OPEN: u8 = 0;
const CLEARING: u8 = 1;
const CLOSED: u8 = 2;

/// Callback input. None represents Go's missing/delete value; an application
/// can separately store a nil payload using V = Option<T>.
#[derive(Clone, Debug)]
pub struct Item<V> {
    /// Primary hash of the key.
    pub key: u64,
    /// Secondary hash; zero disables the conflict check.
    pub conflict: u64,
    /// Retained value, absent on an empty/delete callback.
    pub value: Option<V>,
    /// Policy cost of the item, or a deferred value cost function.
    pub cost: i64,
    /// Absolute expiration time; None means no TTL.
    pub expiration: Option<SystemTime>,
}
/// Application callbacks run outside store/policy locks. They may enqueue
/// writes, but must not block waiting for the same worker or panic.
pub trait Callback<V>: Send + Sync {
    /// Called before OnExit when an item is evicted or cleared.
    fn on_evict(&self, _item: &Item<V>) {}
    /// Called before OnExit for policy rejection, never for a dropped new write.
    fn on_reject(&self, _item: &Item<V>) {}
    /// Called when a retained value exits through replacement, deletion or callback.
    fn on_exit(&self, _value: V) {}
}
struct NoCallback;
impl<V> Callback<V> for NoCallback {}
type Coster<V> = Arc<dyn Fn(&V) -> i64 + Send + Sync>;
type KeyHasher = Arc<dyn Fn(KeyRef<'_>) -> (u64, u64) + Send + Sync>;

/// Construction parameters of the pinned Go root package.
pub struct Config<V> {
    /// Number of frequency counters before power-of-two rounding.
    pub num_counters: usize,
    /// Total configured policy budget.
    pub max_cost: i64,
    /// Read observations per lossy batch; normally 64.
    pub buffer_items: usize,
    /// Enable policy counters and eviction lifetime measurements.
    pub metrics: bool,
    /// Exclude the source store-item byte charge.
    pub ignore_internal_cost: bool,
    /// Owner callbacks for eviction, rejection and value exit.
    pub callback: Arc<dyn Callback<V>>,
    /// Policy cost of the item, or a deferred value cost function.
    pub cost: Option<Coster<V>>,
    /// Optional custom primary/conflict hashing function.
    pub key_to_hash: Option<KeyHasher>,
}
impl<V> Config<V> {
    /// Construct the source configuration or cache with its required parameters.
    pub fn new(num_counters: usize, max_cost: i64, buffer_items: usize) -> Self {
        Self {
            num_counters,
            max_cost,
            buffer_items,
            metrics: false,
            ignore_internal_cost: false,
            callback: Arc::new(NoCallback),
            cost: None,
            key_to_hash: None,
        }
    }
}
#[derive(Clone, Copy)]
enum Flag {
    New,
    Update,
    Delete,
}
enum Write<V> {
    Item(Flag, Item<V>),
    Wait(Sender<()>),
}
struct Core<V> {
    store: Store<V>,
    policy: Mutex<Policy>,
    ring: Ring,
    sets: Sender<Write<V>>,
    reads: Sender<Vec<u64>>,
    config: Config<V>,
    seed: RandomState,
    metrics: Arc<Metrics>,
    bucket_duration: Duration,
    state: AtomicU8,
    operations: RwLock<()>,
}
impl<V: Clone + Send + Sync + 'static> Core<V> {
    fn exit(&self, value: Option<V>) {
        if let Some(value) = value {
            self.config.callback.on_exit(value);
        }
    }
    fn evict(&self, item: &Item<V>) {
        self.config.callback.on_evict(item);
        self.exit(item.value.clone());
    }
    fn reject(&self, item: &Item<V>) {
        self.config.callback.on_reject(item);
        self.exit(item.value.clone());
    }
    fn hash<K: Key + ?Sized>(&self, key: &K) -> (u64, u64) {
        self.config.key_to_hash.as_ref().map_or_else(
            || key::hash(key.key_ref(), &self.seed),
            |hash| hash(key.key_ref()),
        )
    }
    fn operation(&self) -> Option<RwLockReadGuard<'_, ()>> {
        // Do not block callback reentry behind a clearing writer. The state
        // transition rejects new operations; existing ones finish before join.
        loop {
            if self.state.load(Ordering::Acquire) != OPEN {
                return None;
            }
            if let Ok(guard) = self.operations.try_read() {
                return (self.state.load(Ordering::Acquire) == OPEN).then_some(guard);
            }
            thread::yield_now();
        }
    }
    fn push_reads(&self, keys: Vec<u64>) -> Result<(), Vec<u64>> {
        if keys.is_empty() {
            return Ok(());
        }
        let key = keys[0];
        let count = keys.len() as u64;
        match self.reads.try_send(keys) {
            Ok(()) => {
                self.metrics.add(Metric::KeepGets, key, count);
                Ok(())
            }
            Err(error) => {
                self.metrics.add(Metric::DropGets, key, count);
                Err(error.into_inner())
            }
        }
    }
}
struct Workers {
    writer: Option<JoinHandle<()>>,
    reader: Option<JoinHandle<()>>,
    stop_writer: Sender<()>,
    stop_reader: Sender<()>,
    writer_stop: Receiver<()>,
}
/// Thread-safe cache with joined workers. The worker owns Core, not Cache,
/// so consumer Weak<Cache> callbacks cannot create an ownership cycle.
pub struct Cache<V: Clone + Send + Sync + 'static> {
    core: Arc<Core<V>>,
    writes: Receiver<Write<V>>,
    workers: Mutex<Workers>,
}
impl<V: Clone + Send + Sync + 'static> fmt::Debug for Cache<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cache")
            .field("max_cost", &self.max_cost())
            .finish_non_exhaustive()
    }
}
impl<V: Clone + Send + Sync + 'static> Cache<V> {
    /// Construct the source configuration or cache with its required parameters.
    pub fn new(config: Config<V>) -> Result<Self, String> {
        Self::with_runtime(config, WRITE_BUFFER, Duration::from_secs(5))
    }
    fn with_runtime(
        config: Config<V>,
        write_capacity: usize,
        bucket_duration: Duration,
    ) -> Result<Self, String> {
        if config.num_counters == 0 {
            return Err("NumCounters can't be zero".into());
        }
        if config.max_cost == 0 {
            return Err("MaxCost can't be zero".into());
        }
        if config.buffer_items == 0 {
            return Err("BufferItems can't be zero".into());
        }
        if config.num_counters.checked_next_power_of_two().is_none() {
            return Err("NumCounters overflows the native sketch".into());
        }
        let (sets, writes) = bounded(write_capacity);
        let (reads, read_receiver) = bounded(3);
        let metrics = Arc::new(Metrics::new(config.metrics));
        let core = Arc::new(Core {
            store: Store::new(bucket_duration),
            policy: Mutex::new(Policy::new(
                config.num_counters,
                config.max_cost,
                Arc::clone(&metrics),
            )),
            ring: Ring::new(config.buffer_items),
            sets,
            reads,
            seed: RandomState::new(),
            config,
            metrics,
            bucket_duration,
            state: AtomicU8::new(OPEN),
            operations: RwLock::new(()),
        });
        let (stop_writer, writer_stop) = bounded(0);
        let (stop_reader, reader_stop) = bounded(0);
        let read_core = Arc::clone(&core);
        let reader = thread::Builder::new().name("ristretto-reads".into()).spawn(move || loop {
            crossbeam_channel::select! {
                recv(read_receiver) -> keys => if let Ok(keys) = keys { read_core.policy.lock().unwrap().admit.push(&keys); } else { break; },
                recv(reader_stop) -> _ => break,
            }
        }).map_err(|error| error.to_string())?;
        let writer = match spawn_writer(
            Arc::clone(&core),
            writes.clone(),
            writer_stop.clone(),
            bucket_duration,
        ) {
            Ok(writer) => writer,
            Err(error) => {
                let _ = stop_reader.send(());
                let _ = reader.join();
                return Err(error);
            }
        };
        Ok(Self {
            core,
            writes,
            workers: Mutex::new(Workers {
                writer: Some(writer),
                reader: Some(reader),
                stop_writer,
                stop_reader,
                writer_stop,
            }),
        })
    }
    /// Observe a read and return the current, unexpired resident value.
    pub fn get<K: Key + ?Sized>(&self, key: &K) -> Option<V> {
        let _operation = self.core.operation()?;
        let (key, conflict) = self.core.hash(key);
        self.core.ring.push(key, |keys| self.core.push_reads(keys));
        let value = self.core.store.get(key, conflict);
        self.core.metrics.add(
            if value.is_some() {
                Metric::Hit
            } else {
                Metric::Miss
            },
            key,
            1,
        );
        value
    }
    /// Queue a new value or immediately replace a resident; admission may reject it.
    pub fn set<K: Key + ?Sized>(&self, key: &K, value: V, cost: i64) -> bool {
        self.set_with_ttl(key, value, cost, 0)
    }
    /// Signed nanosecond TTL, like Go time.Duration. Zero never expires;
    /// negative TTL is a no-op and causes no application callbacks.
    pub fn set_with_ttl<K: Key + ?Sized>(
        &self,
        key: &K,
        value: V,
        cost: i64,
        ttl_nanos: i64,
    ) -> bool {
        let Some(_operation) = self.core.operation() else {
            return false;
        };
        if ttl_nanos < 0 {
            return false;
        }
        let expiration =
            (ttl_nanos > 0).then(|| SystemTime::now() + Duration::from_nanos(ttl_nanos as u64));
        let (key, conflict) = self.core.hash(key);
        let item = Item {
            key,
            conflict,
            value: Some(value),
            cost,
            expiration,
        };
        let flag = if let Some(previous) = self.core.store.update(&item) {
            self.core.exit(Some(previous));
            Flag::Update
        } else {
            Flag::New
        };
        if self.core.sets.try_send(Write::Item(flag, item)).is_ok() {
            return true;
        }
        if matches!(flag, Flag::Update) {
            return true;
        }
        self.core.metrics.add(Metric::DropSets, key, 1);
        false
    }
    /// Delete immediately and enqueue an ordered deletion marker.
    pub fn del<K: Key + ?Sized>(&self, key: &K) {
        let Some(_operation) = self.core.operation() else {
            return;
        };
        let (key, conflict) = self.core.hash(key);
        self.core.exit(
            self.core
                .store
                .del(key, conflict)
                .and_then(|item| item.value),
        );
        let _ = self.core.sets.send(Write::Item(
            Flag::Delete,
            Item {
                key,
                conflict,
                value: None,
                cost: 0,
                expiration: None,
            },
        ));
    }
    /// Remaining TTL, zero for a resident without expiration, None for a miss.
    pub fn get_ttl<K: Key + ?Sized>(&self, key: &K) -> Option<Duration> {
        let _operation = self.core.operation()?;
        let (key, conflict) = self.core.hash(key);
        self.core.store.get(key, conflict)?;
        self.core
            .store
            .expiration(key)
            .map_or(Some(Duration::ZERO), |expires| {
                expires.duration_since(SystemTime::now()).ok()
            })
    }
    /// Wait until preceding write messages have been processed.
    pub fn wait(&self) {
        let Some(_operation) = self.core.operation() else {
            return;
        };
        let (sender, receiver) = bounded(0);
        if self.core.sets.send(Write::Wait(sender)).is_ok() {
            let _ = receiver.recv();
        }
    }
    /// Read the current policy budget.
    pub fn max_cost(&self) -> i64 {
        self.core.policy.lock().unwrap().evict.max_cost
    }
    /// Change the budget; eviction occurs at the next new admission.
    pub fn update_max_cost(&self, cost: i64) {
        self.core.policy.lock().unwrap().evict.max_cost = cost;
    }
    /// Read optional policy counters; disabled counters return zero.
    pub fn metrics(&self) -> &Metrics {
        &self.core.metrics
    }
    /// Resident count for owner diagnostics, independent of optional metrics.
    pub fn len(&self) -> usize {
        self.core.store.len()
    }
    /// Whether there are no resident values.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Clear is quiesced while callbacks run; new public operations are declined
    /// during that interval. It does not promise concurrent-clear SQL semantics.
    pub fn clear(&self) {
        self.finish(false);
    }
    /// Stop admission, clear values and join both worker threads.
    pub fn close(&self) {
        self.finish(true);
    }
    fn finish(&self, close: bool) {
        let mut workers = self.workers.lock().unwrap();
        if self.core.state.load(Ordering::Acquire) == CLOSED {
            return;
        }
        self.core.state.store(CLEARING, Ordering::Release);
        let _operations = self.core.operations.write().unwrap();
        if let Some(writer) = workers.writer.take() {
            let _ = workers.stop_writer.send(());
            writer.join().expect("Ristretto write callback panicked");
        }
        for write in self.writes.try_iter() {
            match write {
                Write::Wait(waiter) => {
                    let _ = waiter.send(());
                }
                Write::Item(Flag::Update, _) => (),
                Write::Item(_, item) => self.core.evict(&item),
            }
        }
        self.core.policy.lock().unwrap().clear();
        self.core.store.clear(|item| self.core.evict(&item));
        self.core.metrics.clear();
        if close {
            if let Some(reader) = workers.reader.take() {
                let _ = workers.stop_reader.send(());
                reader.join().expect("Ristretto read worker panicked");
            }
            self.core.state.store(CLOSED, Ordering::Release);
        } else {
            workers.writer = Some(
                spawn_writer(
                    Arc::clone(&self.core),
                    self.writes.clone(),
                    workers.writer_stop.clone(),
                    self.core.bucket_duration,
                )
                .expect("restart Ristretto writer"),
            );
            self.core.state.store(OPEN, Ordering::Release);
        }
    }
}
impl<V: Clone + Send + Sync + 'static> Drop for Cache<V> {
    fn drop(&mut self) {
        self.close();
    }
}

fn spawn_writer<V: Clone + Send + Sync + 'static>(
    core: Arc<Core<V>>,
    writes: Receiver<Write<V>>,
    stop: Receiver<()>,
    bucket_duration: Duration,
) -> Result<JoinHandle<()>, String> {
    thread::Builder::new().name("ristretto-writes".into()).spawn(move || {
        let ticker = crossbeam_channel::tick(bucket_duration / 2);
        let mut admitted = HashMap::<u64, Instant>::new();
        let evict = |item: &Item<V>, admitted: &mut HashMap<u64, Instant>| {
            if let Some(start) = admitted.remove(&item.key) { core.metrics.track_eviction(start.elapsed().as_secs() as i64); }
            core.evict(item);
        };
        loop {
            crossbeam_channel::select! {
                recv(stop) -> _ => break,
                recv(writes) -> write => match write {
                    Ok(Write::Wait(waiter)) => { let _ = waiter.send(()); }
                    Ok(Write::Item(flag, mut item)) => {
                        if item.cost == 0 && !matches!(flag, Flag::Delete) {
                            if let (Some(coster), Some(value)) = (&core.config.cost, &item.value) { item.cost = coster(value); }
                        }
                        if !core.config.ignore_internal_cost { item.cost = item.cost.wrapping_add(INTERNAL_ITEM_COST); }
                        match flag {
                            Flag::New => {
                                let (victims, added) = core.policy.lock().unwrap().add(item.key, item.cost);
                                if added {
                                    core.store.set(item.clone()); core.metrics.add(Metric::KeyAdd, item.key, 1);
                                    if core.metrics.enabled() {
                                        admitted.insert(item.key, Instant::now());
                                        if admitted.len() > 100_000 { let key = *admitted.keys().next().unwrap(); admitted.remove(&key); }
                                    }
                                } else { core.reject(&item); }
                                for (key, cost) in victims {
                                    let mut victim = core.store.del(key, 0).unwrap_or(Item { key, conflict: 0, value: None, cost, expiration: None });
                                    victim.cost = cost; victim.expiration = None;
                                    evict(&victim, &mut admitted);
                                }
                            }
                            Flag::Update => { core.policy.lock().unwrap().evict.update(item.key, item.cost); }
                            Flag::Delete => { core.policy.lock().unwrap().evict.del(item.key); core.exit(core.store.del(item.key, item.conflict).and_then(|item| item.value)); }
                        }
                    }
                    Err(_) => break,
                },
                recv(ticker) -> _ => {
                    let now = SystemTime::now();
                    for (key, conflict) in core.store.expired_bucket(now) {
                        if core.store.expiration(key).is_some_and(|expires| expires > now) { continue; }
                        let cost = { let mut policy = core.policy.lock().unwrap(); let cost = policy.evict.costs.get(&key).copied().unwrap_or(-1); policy.evict.del(key); cost };
                        let value = core.store.del(key, conflict).and_then(|item| item.value);
                        evict(&Item { key, conflict, value, cost, expiration: None }, &mut admitted);
                    }
                }
            }
        }
    }).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod benchmarks;
