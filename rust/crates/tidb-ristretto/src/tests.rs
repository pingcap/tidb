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

//! Original root-package cases from pinned v0.1.1 plus ordering regressions.
//! Wall-clock sleeps are replaced by barriers or short TTL buckets; workload
//! cardinalities and semantic assertions remain recorded in the package receipt.
use super::*;
use crate::sketch::{Sketch, TinyLfu};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

fn config(max: i64) -> Config<i64> {
    let mut c = Config::new(1000, max, 64);
    c.ignore_internal_cost = true;
    c.metrics = true;
    c
}
fn cache(max: i64) -> Cache<i64> {
    Cache::new(config(max)).unwrap()
}
fn item(key: u64, value: i64) -> Item<i64> {
    Item {
        key,
        conflict: 0,
        value: Some(value),
        cost: 1,
        expiration: None,
    }
}
fn eventually(mut check: impl FnMut() -> bool) {
    let until = Instant::now() + Duration::from_secs(5);
    while !check() {
        assert!(Instant::now() < until, "condition did not become true");
        thread::sleep(Duration::from_millis(1));
    }
}
#[derive(Default)]
struct Events {
    evicted: Mutex<Vec<Item<i64>>>,
    rejected: Mutex<Vec<Item<i64>>>,
    exited: Mutex<Vec<i64>>,
}
impl Callback<i64> for Events {
    fn on_evict(&self, item: &Item<i64>) {
        self.evicted.lock().unwrap().push(item.clone());
    }
    fn on_reject(&self, item: &Item<i64>) {
        self.rejected.lock().unwrap().push(item.clone());
    }
    fn on_exit(&self, value: i64) {
        self.exited.lock().unwrap().push(value);
    }
}
fn pause(c: &Cache<i64>) {
    let mut workers = c.workers.lock().unwrap();
    workers.stop_writer.send(()).unwrap();
    workers.writer.take().unwrap().join().unwrap();
}
fn resume(c: &Cache<i64>) {
    let mut workers = c.workers.lock().unwrap();
    workers.writer = Some(
        spawn_writer(
            Arc::clone(&c.core),
            c.writes.clone(),
            workers.writer_stop.clone(),
            Duration::from_secs(5),
        )
        .unwrap(),
    );
}

#[test]
fn test_cache_key_to_hash() {
    let calls = Arc::new(AtomicUsize::new(0));
    let hits = Arc::clone(&calls);
    let mut cfg = config(1000);
    cfg.key_to_hash = Some(Arc::new(move |key| {
        hits.fetch_add(1, AtomicOrdering::Relaxed);
        let KeyRef::Integer(key) = key else { panic!() };
        (key, 0)
    }));
    let c = Cache::new(cfg).unwrap();
    c.set(&1, 1, 1);
    c.wait();
    assert_eq!(c.get(&1), Some(1));
    c.del(&1);
    assert_eq!(calls.load(AtomicOrdering::Relaxed), 3);
}
#[test]
fn test_cache_max_cost() {
    let mut cfg = config(1_000_000);
    cfg.ignore_internal_cost = false;
    cfg.num_counters = 12_960;
    let c = Cache::new(cfg).unwrap();
    thread::scope(|scope| {
        for _ in 0..8 {
            let c = &c;
            scope.spawn(move || {
                for _ in 0..4000 {
                    let key = rand::random_range(0_i64..1296);
                    if c.get(&key).is_none() {
                        c.set(&key, key, if key % 10 == 0 { 6 } else { 1002 });
                    }
                }
            });
        }
    });
    c.wait();
    assert!(
        c.metrics()
            .cost_added()
            .wrapping_sub(c.metrics().cost_evicted())
            <= 1_000_000
    );
}
#[test]
fn test_update_max_cost() {
    let mut cfg = config(10);
    cfg.ignore_internal_cost = false;
    let c = Cache::new(cfg).unwrap();
    c.set(&1, 1, 1);
    c.wait();
    assert_eq!(c.get(&1), None);
    c.update_max_cost(1000);
    assert_eq!(c.max_cost(), 1000);
    c.set(&1, 1, 1);
    c.wait();
    assert_eq!(c.get(&1), Some(1));
}
#[test]
fn test_new_cache() {
    for cfg in [
        Config::<i64>::new(0, 10, 64),
        Config::new(100, 0, 64),
        Config::new(100, 10, 0),
    ] {
        assert!(Cache::new(cfg).is_err());
    }
    assert!(Cache::new(config(10)).is_ok());
}
#[test]
fn test_nil_cache() {
    let absent: Option<Cache<i64>> = None;
    assert_eq!(absent.as_ref().and_then(|c| c.get(&1)), None);
    assert!(!absent.as_ref().is_some_and(|c| c.set(&1, 1, 1)));
}
#[test]
fn test_multiple_close() {
    let c = cache(10);
    c.close();
    c.close();
}
#[test]
fn test_set_after_close() {
    let c = cache(10);
    c.close();
    assert!(!c.set(&1, 1, 1));
}
#[test]
fn test_clear_after_close() {
    let c = cache(10);
    c.close();
    c.clear();
    assert!(!c.set(&1, 1, 1));
}
#[test]
fn test_get_after_close() {
    let c = cache(10);
    c.set(&1, 1, 1);
    c.close();
    assert_eq!(c.get(&1), None);
}
#[test]
fn test_del_after_close() {
    let c = cache(10);
    c.set(&1, 1, 1);
    c.close();
    c.del(&1);
    assert!(c.is_empty());
}
#[test]
fn test_cache_process_items() {
    let events = Arc::new(Events::default());
    let mut cfg = config(10);
    cfg.callback = events.clone();
    cfg.cost = Some(Arc::new(|v| *v));
    let c = Cache::new(cfg).unwrap();
    let mut i = item(1, 1);
    i.cost = 0;
    c.core.sets.send(Write::Item(Flag::New, i.clone())).unwrap();
    c.wait();
    assert_eq!(c.core.policy.lock().unwrap().evict.costs[&1], 1);
    i.value = Some(2);
    c.core
        .sets
        .send(Write::Item(Flag::Update, i.clone()))
        .unwrap();
    c.wait();
    assert_eq!(c.core.policy.lock().unwrap().evict.costs[&1], 2);
    c.del(&1);
    c.wait();
    assert_eq!(c.get(&1), None);
    assert!(!c.core.policy.lock().unwrap().evict.costs.contains_key(&1));
    for (key, cost) in [(2, 3), (3, 3), (4, 3), (5, 5)] {
        c.set(&key, key, cost);
    }
    c.wait();
    assert!(!events.evicted.lock().unwrap().is_empty());
}
#[test]
fn test_cache_get() {
    let c = cache(10);
    c.core.store.set(item(1, 1));
    assert_eq!(c.get(&1), Some(1));
    assert_eq!(c.get(&2), None);
    assert_eq!(c.metrics().ratio(), 0.5);
}
#[test]
fn test_cache_set() {
    let c = cache(10);
    c.set(&1, 1, 1);
    c.wait();
    pause(&c);
    assert!(c.set(&1, 2, 2));
    assert_eq!(c.get(&1), Some(2));
    while c
        .core
        .sets
        .try_send(Write::Item(Flag::Update, item(1, 2)))
        .is_ok()
    {}
    assert!(!c.set(&2, 2, 1));
    assert_eq!(c.metrics().sets_dropped(), 1);
    assert!(c.set(&1, 3, 3));
    assert_eq!(c.get(&1), Some(3));
    resume(&c);
}
#[test]
fn test_cache_internal_cost() {
    let mut cfg = config(10);
    cfg.ignore_internal_cost = false;
    let c = Cache::new(cfg).unwrap();
    c.set(&1, 1, 1);
    c.wait();
    assert_eq!(c.get(&1), None);
}
fn ttl_cache() -> Cache<i64> {
    Cache::with_runtime(config(10), WRITE_BUFFER, Duration::from_millis(20)).unwrap()
}
#[test]
fn test_recache_with_ttl() {
    let c = ttl_cache();
    c.set_with_ttl(&1, 1, 1, 20_000_000);
    c.wait();
    assert_eq!(c.get(&1), Some(1));
    eventually(|| c.get(&1).is_none());
    c.set_with_ttl(&1, 2, 1, 1_000_000_000);
    c.wait();
    assert_eq!(c.get(&1), Some(2));
}
#[test]
fn test_cache_set_with_ttl() {
    let events = Arc::new(Events::default());
    let mut cfg = config(10);
    cfg.callback = events.clone();
    let c = Cache::with_runtime(cfg, WRITE_BUFFER, Duration::from_millis(20)).unwrap();
    assert!(!c.set_with_ttl(&1, 1, 1, -1));
    assert!(c.set_with_ttl(&1, 1, 1, 20_000_000));
    c.wait();
    eventually(|| !events.evicted.lock().unwrap().is_empty());
    assert_eq!(events.evicted.lock().unwrap()[0].key, 1);
    c.set_with_ttl(&2, 1, 1, 20_000_000);
    c.wait();
    c.set_with_ttl(&2, 2, 1, 1_000_000_000);
    c.wait();
    thread::sleep(Duration::from_millis(60));
    assert_eq!(c.get(&2), Some(2));
    c.set(&3, 1, 1);
    c.wait();
    c.set_with_ttl(&3, 2, 1, 20_000_000);
    c.wait();
    eventually(|| c.get(&3).is_none());
}
#[test]
fn test_cache_del() {
    let c = cache(10);
    pause(&c);
    c.set(&1, 1, 1);
    let (tx, rx) = bounded(0);
    thread::scope(|s| {
        s.spawn(|| {
            c.del(&1);
            tx.send(()).unwrap();
        });
        rx.recv().unwrap();
        resume(&c);
    });
    c.wait();
    assert_eq!(c.get(&1), None);
}
#[test]
fn test_cache_del_with_ttl() {
    let c = ttl_cache();
    c.set_with_ttl(&3, 1, 1, 1_000_000_000);
    c.wait();
    c.del(&3);
    assert_eq!(c.get(&3), None);
}
#[test]
fn test_cache_get_ttl() {
    let c = ttl_cache();
    c.set_with_ttl(&1, 1, 1, 1_000_000_000);
    c.wait();
    let ttl = c.get_ttl(&1).unwrap();
    assert!(ttl > Duration::from_millis(500) && ttl <= Duration::from_secs(1));
    c.del(&1);
    assert_eq!(c.get_ttl(&1), None);
    c.set(&2, 2, 1);
    c.wait();
    assert_eq!(c.get_ttl(&2), Some(Duration::ZERO));
    assert_eq!(c.get_ttl(&3), None);
    c.set_with_ttl(&3, 3, 1, 20_000_000);
    c.wait();
    eventually(|| c.get_ttl(&3).is_none());
}
#[test]
fn test_cache_clear() {
    let c = cache(10);
    for i in 0..10 {
        c.set(&i, i, 1);
    }
    c.wait();
    assert_eq!(c.metrics().keys_added(), 10);
    c.clear();
    assert_eq!(c.metrics().keys_added(), 0);
    for i in 0..10 {
        assert_eq!(c.get(&i), None);
    }
    c.set(&1, 1, 1);
    c.wait();
    assert_eq!(c.get(&1), Some(1));
}
#[test]
fn test_cache_metrics() {
    let c = cache(10);
    for i in 0..10 {
        c.set(&i, i, 1);
    }
    c.wait();
    assert_eq!(c.metrics().keys_added(), 10);
}
#[test]
fn test_metrics() {
    assert!(Metrics::new(true).enabled());
    assert_eq!(
        Metrics::new(true).life_expectancy_seconds().unwrap().count,
        0
    );
}
#[test]
fn test_nil_metrics() {
    let m = Metrics::new(false);
    m.add(Metric::Hit, 1, 1);
    assert_eq!(
        [
            m.hits(),
            m.misses(),
            m.keys_added(),
            m.keys_evicted(),
            m.cost_evicted(),
            m.sets_dropped(),
            m.sets_rejected(),
            m.gets_dropped(),
            m.gets_kept()
        ],
        [0; 9]
    );
    assert!(m.life_expectancy_seconds().is_none());
}
#[test]
fn test_metrics_add_get() {
    let m = Metrics::new(true);
    for i in 1..=3 {
        m.add(Metric::Hit, i, i);
    }
    assert_eq!(m.hits(), 6);
}
#[test]
fn test_metrics_ratio() {
    let m = Metrics::new(true);
    assert_eq!(m.ratio(), 0.0);
    for i in 1..=2 {
        m.add(Metric::Hit, i, i);
        m.add(Metric::Miss, i, i);
    }
    assert_eq!(m.ratio(), 0.5);
    assert_eq!(Metrics::new(false).ratio(), 0.0);
}
#[test]
fn test_metrics_string() {
    let m = Metrics::new(true);
    for metric in [
        Metric::Hit,
        Metric::Miss,
        Metric::KeyAdd,
        Metric::KeyUpdate,
        Metric::KeyEvict,
        Metric::CostAdd,
        Metric::CostEvict,
        Metric::DropSets,
        Metric::RejectSets,
        Metric::DropGets,
        Metric::KeepGets,
    ] {
        m.add(metric, 1, 1);
    }
    assert_eq!(
        [
            m.hits(),
            m.misses(),
            m.keys_added(),
            m.keys_updated(),
            m.keys_evicted(),
            m.cost_added(),
            m.cost_evicted(),
            m.sets_dropped(),
            m.sets_rejected(),
            m.gets_dropped(),
            m.gets_kept()
        ],
        [1; 11]
    );
    assert!(m.to_string().contains("hit-ratio: 0.50"));
    assert_eq!(Metrics::new(false).to_string(), "");
}
#[test]
fn test_cache_metrics_clear() {
    let c = cache(10);
    c.set(&1, 1, 1);
    thread::scope(|s| {
        s.spawn(|| {
            for _ in 0..10000 {
                c.get(&1);
            }
        });
        for _ in 0..10 {
            c.clear();
        }
    });
    c.clear();
    assert_eq!(c.metrics().hits(), 0);
}
#[test]
fn test_block_on_clear() {
    let c = cache(10);
    thread::scope(|s| {
        s.spawn(|| {
            for _ in 0..10 {
                c.wait();
            }
        });
        for _ in 0..10 {
            c.clear();
        }
    });
}
#[test]
fn test_drop_updates() {
    for _ in 0..100 {
        let events = Arc::new(Events::default());
        let mut cfg = config(10);
        cfg.callback = events.clone();
        cfg.ignore_internal_cost = false;
        let c = Cache::with_runtime(cfg, 10, Duration::from_secs(5)).unwrap();
        let mut dropped = Vec::new();
        for value in 0..50 {
            if !c.set(&0, value, 1) {
                dropped.push(value);
                thread::sleep(Duration::from_micros(1));
            }
        }
        c.wait();
        c.close();
        for item in events
            .evicted
            .lock()
            .unwrap()
            .iter()
            .chain(events.rejected.lock().unwrap().iter())
        {
            if let Some(value) = item.value {
                assert!(!dropped.contains(&value));
            }
        }
    }
}
struct Allocation {
    live: Arc<AtomicUsize>,
    _data: Vec<u8>,
}
impl Drop for Allocation {
    fn drop(&mut self) {
        self.live.fetch_sub(1, AtomicOrdering::SeqCst);
    }
}
fn calloc_case(ttl: bool) {
    let live = Arc::new(AtomicUsize::new(0));
    let mut cfg = Config::new(104_856, 996_147, 64);
    cfg.metrics = true;
    let c = Cache::with_runtime(cfg, WRITE_BUFFER, Duration::from_secs(1)).unwrap();
    thread::scope(|scope| {
        for _ in 0..8 {
            let live = Arc::clone(&live);
            let c = &c;
            scope.spawn(move || {
                for _ in 0..10000 {
                    let key = rand::random_range(0..10000);
                    live.fetch_add(1, AtomicOrdering::SeqCst);
                    let value = Arc::new(Allocation {
                        live: Arc::clone(&live),
                        _data: vec![0; 256],
                    });
                    c.set_with_ttl(&key, value, 256, if ttl { 1_000_000_000 } else { 0 });
                    if key % 10 == 0 {
                        c.del(&key);
                    }
                }
            });
        }
    });
    c.wait();
    if ttl {
        eventually(|| live.load(AtomicOrdering::SeqCst) == 0);
    } else {
        c.clear();
    }
    assert_eq!(live.load(AtomicOrdering::SeqCst), 0);
}
#[test]
fn test_ristretto_calloc() {
    calloc_case(false);
}
#[test]
fn test_ristretto_calloc_ttl() {
    calloc_case(true);
}
#[test]
fn test_cache_with_ttl() {
    for _ in 0..10 {
        let c = ttl_cache();
        c.set_with_ttl(&1, 1, 1, 20_000_000);
        c.wait();
        assert_eq!(c.get(&1), Some(1));
        eventually(|| c.get(&1).is_none());
    }
}

fn policy() -> Policy {
    Policy::new(100, 10, Arc::new(Metrics::new(true)))
}
#[test]
fn test_policy() {
    assert_eq!(policy().evict.max_cost, 10);
}
#[test]
fn test_policy_metrics() {
    let mut p = policy();
    p.add(1, 1);
    assert_eq!(p.evict.used, 1);
}
#[test]
fn test_policy_process_items() {
    let c = cache(10);
    c.core.push_reads(vec![1, 2, 2]).unwrap();
    eventually(|| c.core.policy.lock().unwrap().admit.estimate(2) == 2);
    assert_eq!(c.core.policy.lock().unwrap().admit.estimate(1), 1);
}
#[test]
fn test_policy_push() {
    let c = cache(10);
    assert!(c.core.push_reads(Vec::new()).is_ok());
    assert!((0..10).any(|_| c.core.push_reads(vec![1, 2, 3, 4, 5]).is_ok()));
}
#[test]
fn test_policy_add() {
    let mut p = Policy::new(1000, 100, Arc::new(Metrics::new(true)));
    assert_eq!(p.add(1, 101), (vec![], false));
    p.evict.add(1, 1);
    p.admit.push(&[1, 2, 3]);
    assert_eq!(p.add(1, 1), (vec![], false));
    assert_eq!(p.add(2, 20), (vec![], true));
    let (victims, added) = p.add(3, 90);
    assert!(!victims.is_empty() && added);
    assert!(!p.add(4, 20).1);
}
#[test]
fn test_policy_has() {
    let mut p = policy();
    p.add(1, 1);
    assert!(p.evict.costs.contains_key(&1));
    assert!(!p.evict.costs.contains_key(&2));
}
#[test]
fn test_policy_del() {
    let mut p = policy();
    p.add(1, 1);
    p.evict.del(1);
    p.evict.del(2);
    assert!(p.evict.costs.is_empty());
}
#[test]
fn test_policy_cap() {
    let mut p = policy();
    p.add(1, 1);
    assert_eq!(p.evict.room_left(0), 9);
}
#[test]
fn test_policy_update() {
    let mut p = policy();
    p.add(1, 1);
    p.evict.update(1, 2);
    assert_eq!(p.evict.costs[&1], 2);
}
#[test]
fn test_policy_cost() {
    let mut p = policy();
    p.add(1, 2);
    assert_eq!(p.evict.costs.get(&1), Some(&2));
    assert!(!p.evict.costs.contains_key(&2));
}
#[test]
fn test_policy_clear() {
    let mut p = policy();
    for i in 1..4 {
        p.add(i, i as i64);
    }
    p.clear();
    assert_eq!(p.evict.room_left(0), 10);
    assert!(p.evict.costs.is_empty());
}
#[test]
fn test_policy_close() {
    let c = cache(10);
    c.close();
    assert!(c.core.reads.send(vec![1]).is_err());
}
#[test]
fn test_push_after_close() {
    let c = cache(10);
    c.close();
    assert!(c.core.push_reads(vec![1, 2]).is_err());
}
#[test]
fn test_add_after_close() {
    let c = cache(10);
    c.close();
    assert!(c.core.policy.lock().unwrap().add(1, 1).1);
}
#[test]
fn test_sampled_lfu_add() {
    let mut p = policy();
    for (k, v) in [(1, 1), (2, 2), (3, 1)] {
        p.evict.add(k, v);
    }
    assert_eq!(p.evict.used, 4);
    assert_eq!(p.evict.costs[&2], 2);
}
#[test]
fn test_sampled_lfu_del() {
    let mut p = policy();
    p.evict.add(1, 1);
    p.evict.add(2, 2);
    p.evict.del(2);
    p.evict.del(4);
    assert_eq!(p.evict.used, 1);
    assert!(!p.evict.costs.contains_key(&2));
}
#[test]
fn test_sampled_lfu_update() {
    let mut p = policy();
    p.evict.add(1, 1);
    assert!(p.evict.update(1, 2));
    assert_eq!(p.evict.used, 2);
    assert!(!p.evict.update(2, 2));
}
#[test]
fn test_sampled_lfu_clear() {
    let mut p = policy();
    p.evict.add(1, 1);
    p.evict.add(2, 2);
    p.evict.clear();
    assert_eq!(p.evict.used, 0);
    assert!(p.evict.costs.is_empty());
}
#[test]
fn test_sampled_lfu_room() {
    let mut p = policy();
    p.evict.max_cost = 16;
    for k in 1..4 {
        p.evict.add(k, k as i64);
    }
    assert_eq!(p.evict.room_left(4), 6);
}
#[test]
fn test_sampled_lfu_sample() {
    let mut p = policy();
    p.evict.add(4, 4);
    p.evict.add(5, 5);
    let mut sample = vec![(1, 1), (2, 2), (3, 3)];
    p.evict.fill_sample(&mut sample);
    assert_eq!(sample.len(), 5);
    assert!(sample.last().unwrap().0 > 3);
    p.evict.fill_sample(&mut sample);
    assert_eq!(sample.len(), 5);
    p.evict.del(5);
    sample.truncate(3);
    p.evict.fill_sample(&mut sample);
    assert_eq!(sample.len(), 4);
}
#[test]
fn test_tiny_lfu_increment() {
    let mut p = TinyLfu::new(4);
    p.push(&[1, 1, 1]);
    assert!(p.door.has(1));
    assert_eq!(p.freq.estimate(1), 2);
    p.increment(1);
    assert!(!p.door.has(1));
    assert_eq!(p.freq.estimate(1), 1);
}
#[test]
fn test_tiny_lfu_estimate() {
    let mut p = TinyLfu::new(8);
    p.push(&[1, 1, 1]);
    assert_eq!(p.estimate(1), 3);
    assert_eq!(p.estimate(2), 0);
}
#[test]
fn test_tiny_lfu_push() {
    let mut p = TinyLfu::new(16);
    p.push(&[1, 2, 2, 3, 3, 3]);
    assert_eq!([p.estimate(1), p.estimate(2), p.estimate(3)], [1, 2, 3]);
    assert_eq!(p.increments, 6);
}
#[test]
fn test_tiny_lfu_clear() {
    let mut p = TinyLfu::new(16);
    p.push(&[1, 3, 3, 3]);
    p.clear();
    assert_eq!(p.increments, 0);
    assert_eq!(p.estimate(3), 0);
}
#[test]
fn test_sketch() {
    assert_eq!(Sketch::new(5).mask, 7);
    assert!(std::panic::catch_unwind(|| Sketch::new(0)).is_err());
}
#[test]
fn test_sketch_increment() {
    let mut p = Sketch::new(16);
    p.seeds = [0, 1, 2, 3];
    for k in [1, 5, 9] {
        p.increment(k);
    }
    assert!(p.rows.iter().any(|row| row != &p.rows[0]));
}
#[test]
fn test_sketch_estimate() {
    let mut p = Sketch::new(16);
    p.increment(1);
    p.increment(1);
    assert_eq!(p.estimate(1), 2);
    assert_eq!(p.estimate(0), 0);
}
#[test]
fn test_sketch_reset() {
    let mut p = Sketch::new(16);
    for _ in 0..4 {
        p.increment(1);
    }
    p.reset();
    assert_eq!(p.estimate(1), 2);
}
#[test]
fn test_sketch_clear() {
    let mut p = Sketch::new(16);
    for k in 0..16 {
        p.increment(k);
    }
    p.clear();
    for k in 0..16 {
        assert_eq!(p.estimate(k), 0);
    }
}
#[test]
fn test_next2_power() {
    let n = ((12_u64 << 30) as f64 * 0.01) as usize;
    let size = n.next_power_of_two();
    assert!(size >= n && size / 2 < n);
}
#[test]
fn test_ring_drain() {
    let ring = Ring::new(1);
    let mut drains = 0;
    for i in 0..100 {
        ring.push(i, |_| {
            drains += 1;
            Ok(())
        });
    }
    assert_eq!(drains, 100);
}
#[test]
fn test_ring_reset() {
    let ring = Ring::new(4);
    let mut rejected = 0;
    for i in 0..100 {
        ring.push(i, |items| {
            assert_eq!(items.len(), 4);
            rejected += 1;
            Err(items)
        });
    }
    assert_eq!(rejected, 25);
}
#[test]
fn test_ring_consumer() {
    let ring = Ring::new(4);
    let mut keys = std::collections::HashSet::new();
    for i in 0..100 {
        ring.push(i, |items| {
            keys.extend(items);
            Ok(())
        });
    }
    assert!(!keys.is_empty() && keys.len() <= 100);
}
#[test]
fn test_store_set_get() {
    let s = Store::new(Duration::from_secs(5));
    s.set(item(1, 2));
    assert_eq!(s.get(1, 0), Some(2));
    s.set(item(1, 3));
    assert_eq!(s.get(1, 0), Some(3));
    s.set(item(2, 2));
    assert_eq!(s.get(2, 0), Some(2));
}
#[test]
fn test_store_del() {
    let s = Store::new(Duration::from_secs(5));
    s.set(item(1, 1));
    s.del(1, 0);
    assert_eq!(s.get(1, 0), None);
    s.del(2, 0);
}
#[test]
fn test_store_clear() {
    let s = Store::new(Duration::from_secs(5));
    for i in 0..1000 {
        s.set(item(i, i as i64));
    }
    let mut exited = 0;
    s.clear(|_| exited += 1);
    assert_eq!(exited, 1000);
    for i in 0..1000 {
        assert_eq!(s.get(i, 0), None);
    }
}
#[test]
fn test_store_update() {
    let s = Store::new(Duration::from_secs(5));
    s.set(item(1, 1));
    for i in 2..=3 {
        assert_eq!(s.update(&item(1, i)), Some(i - 1));
        assert_eq!(s.get(1, 0), Some(i));
    }
    assert_eq!(s.update(&item(2, 2)), None);
    assert_eq!(s.get(2, 0), None);
}
#[test]
fn test_store_collision() {
    let s = Store::new(Duration::from_secs(5));
    s.set(item(1, 1));
    assert_eq!(s.get(1, 1), None);
    let mut i = item(1, 2);
    i.conflict = 1;
    s.set(i.clone());
    assert_eq!(s.get(1, 0), Some(1));
    assert_eq!(s.update(&i), None);
    s.del(1, 1);
    assert_eq!(s.get(1, 0), Some(1));
}
#[test]
fn test_store_expiration() {
    let s = Store::new(Duration::from_secs(5));
    let mut i = item(1, 1);
    i.expiration = Some(SystemTime::now() + Duration::from_secs(1));
    s.set(i.clone());
    assert_eq!(s.get(1, 0), Some(1));
    assert_eq!(s.expiration(1), i.expiration);
    s.del(1, 0);
    assert_eq!(s.expiration(1), None);
    assert_eq!(s.expiration(4_340_958_203_495), None);
}
#[test]
fn test_stress_set_get() {
    let c = cache(100);
    for i in 0..100 {
        c.set(&i, i, 1);
    }
    c.wait();
    thread::scope(|scope| {
        for _ in 0..8 {
            let c = &c;
            scope.spawn(move || {
                for _ in 0..1000 {
                    let key = rand::random_range(0..10);
                    assert_eq!(c.get(&key), Some(key));
                }
            });
        }
    });
    assert_eq!(c.metrics().ratio(), 1.0);
}
#[test]
fn test_stress_hit_ratio() {
    let mut cfg = config(100);
    cfg.ignore_internal_cost = false;
    let c = Cache::new(cfg).unwrap();
    // Same Zipf exponent/support as sim.NewZipfian(1.0001,1,1000).
    let mut cdf = Vec::new();
    let mut sum = 0.0;
    for key in 1..=1000 {
        sum += 1.0 / (key as f64).powf(1.0001);
        cdf.push(sum);
    }
    let mut access = Vec::new();
    let mut frequencies = HashMap::new();
    for _ in 0..10000 {
        let point = rand::random::<f64>() * sum;
        let key = cdf.partition_point(|p| *p < point) as i64 + 1;
        access.push(key);
        *frequencies.entry(key).or_insert(0_u64) += 1;
        if c.get(&key).is_none() {
            c.set(&key, key, 1);
        }
    }
    let mut retained = std::collections::HashSet::new();
    let mut heap = std::collections::BinaryHeap::new();
    let mut hits = 0;
    for key in access {
        if retained.contains(&key) {
            hits += 1;
            continue;
        }
        if heap.len() >= 100 {
            let std::cmp::Reverse((_, key)) = heap.pop().unwrap();
            retained.remove(&key);
        }
        retained.insert(key);
        heap.push(std::cmp::Reverse((frequencies[&key], key)));
    }
    eprintln!(
        "actual: {:.2}, optimal: {:.2}",
        c.metrics().ratio(),
        hits as f64 / 10000.0
    );
}

// Regressions beyond the source suite: callbacks and pending writes must not
// be used to paper over eager publication or dropped-set ownership changes.
#[test]
fn new_publication_replacement_and_clear_callback_order() {
    let events = Arc::new(Events::default());
    let mut cfg = config(10);
    cfg.callback = events.clone();
    let c = Cache::new(cfg).unwrap();
    c.set(&1, 1, 1);
    c.wait();
    pause(&c);
    c.set(&1, 2, 2);
    c.set(&2, 3, 1);
    assert_eq!(c.get(&1), Some(2));
    assert_eq!(c.get(&2), None);
    assert_eq!(*events.exited.lock().unwrap(), vec![1]);
    // Clear drains pending New through OnEvict, skips Update (already resident),
    // then evicts the resident replacement exactly once with zero clear cost.
    c.clear();
    let mut exits = events.exited.lock().unwrap().clone();
    exits.sort();
    assert_eq!(exits, vec![1, 2, 3]);
    let evicted = events.evicted.lock().unwrap();
    assert_eq!(evicted.len(), 2);
    assert_eq!(evicted.iter().find(|i| i.key == 1).unwrap().cost, 0);
}
#[test]
fn frequency_protects_hot_resident_and_reports_rejections() {
    let mut cfg = config(1);
    cfg.buffer_items = 1;
    let c = Cache::new(cfg).unwrap();
    c.set(&1, 1, 1);
    c.wait();
    for _ in 0..10 {
        c.get(&1);
    }
    eventually(|| c.core.policy.lock().unwrap().admit.estimate(1) > 0);
    c.set(&2, 2, 1);
    c.wait();
    assert_eq!(c.get(&1), Some(1));
    assert_eq!(c.get(&2), None);
    assert_eq!(c.metrics().sets_rejected(), 1);
}
#[test]
fn dropped_new_writes_have_no_callbacks() {
    let events = Arc::new(Events::default());
    let mut cfg = config(10);
    cfg.callback = events.clone();
    let c = Cache::with_runtime(cfg, 1, Duration::from_secs(5)).unwrap();
    pause(&c);
    assert!(c.set(&1, 1, 1));
    assert!(!c.set(&2, 2, 1));
    resume(&c);
    c.wait();
    c.close();
    assert!(!events.exited.lock().unwrap().contains(&2));
}
