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

//! Ristretto cache.go policy counters and z.HistogramData lifetime snapshots.
use std::fmt;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};

#[derive(Clone, Copy)]
pub(crate) enum Metric {
    Hit,
    Miss,
    KeyAdd,
    KeyUpdate,
    KeyEvict,
    CostAdd,
    CostEvict,
    DropSets,
    RejectSets,
    DropGets,
    KeepGets,
}
const NAMES: [&str; 11] = [
    "hit",
    "miss",
    "keys-added",
    "keys-updated",
    "keys-evicted",
    "cost-added",
    "cost-evicted",
    "sets-dropped",
    "sets-rejected",
    "gets-dropped",
    "gets-kept",
];
#[repr(align(64))]
struct Counter(AtomicU64);
/// Copy of the lifetime histogram, with strict upper bucket bounds 2^1..2^16.
#[derive(Clone, Debug, PartialEq)]
pub struct LifeExpectancy {
    /// Strict upper bounds of the lifetime buckets.
    pub bounds: Vec<i64>,
    /// Number of evictions in each bucket, including overflow.
    pub count_per_bucket: Vec<i64>,
    /// Total recorded evictions.
    pub count: i64,
    /// Minimum lifetime in seconds; i64::MAX before the first eviction.
    pub min: i64,
    /// Maximum lifetime in seconds.
    pub max: i64,
    /// Sum of recorded lifetimes in seconds.
    pub sum: i64,
}
impl Default for LifeExpectancy {
    fn default() -> Self {
        Self {
            bounds: (1..=16).map(|e| 1 << e).collect(),
            count_per_bucket: vec![0; 17],
            count: 0,
            min: i64::MAX,
            max: 0,
            sum: 0,
        }
    }
}
impl LifeExpectancy {
    fn update(&mut self, seconds: i64) {
        self.min = self.min.min(seconds);
        self.max = self.max.max(seconds);
        self.count += 1;
        self.sum += seconds;
        let bucket = self
            .bounds
            .iter()
            .position(|bound| seconds < *bound)
            .unwrap_or(self.bounds.len());
        self.count_per_bucket[bucket] += 1;
    }
    /// Mean recorded lifetime in seconds, or zero before any eviction.
    pub fn mean(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum as f64 / self.count as f64
        }
    }
}
/// Optional metrics. Disabled counters have Go's nil-metrics zero behavior.
pub struct Metrics {
    counters: Option<Box<[[Counter; 25]; 11]>>,
    life: Mutex<LifeExpectancy>,
}
impl Metrics {
    pub(crate) fn new(enabled: bool) -> Self {
        Self {
            counters: enabled.then(|| {
                Box::new(std::array::from_fn(|_| {
                    std::array::from_fn(|_| Counter(AtomicU64::new(0)))
                }))
            }),
            life: Mutex::new(LifeExpectancy::default()),
        }
    }
    /// Whether counters were enabled at construction.
    pub fn enabled(&self) -> bool {
        self.counters.is_some()
    }
    pub(crate) fn add(&self, metric: Metric, key: u64, value: u64) {
        if let Some(c) = &self.counters {
            c[metric as usize][key as usize % 25]
                .0
                .fetch_add(value, Ordering::Relaxed);
        }
    }
    pub(crate) fn get(&self, metric: Metric) -> u64 {
        self.counters.as_ref().map_or(0, |c| {
            c[metric as usize].iter().fold(0_u64, |sum, c| {
                sum.wrapping_add(c.0.load(Ordering::Relaxed))
            })
        })
    }
    pub(crate) fn track_eviction(&self, seconds: i64) {
        if self.enabled() {
            self.life.lock().unwrap().update(seconds);
        }
    }
    /// Clone the optional eviction lifetime histogram.
    pub fn life_expectancy_seconds(&self) -> Option<LifeExpectancy> {
        self.enabled().then(|| self.life.lock().unwrap().clone())
    }
    /// Successful resident reads.
    pub fn hits(&self) -> u64 {
        self.get(Metric::Hit)
    }
    /// Reads without an unexpired matching resident.
    pub fn misses(&self) -> u64 {
        self.get(Metric::Miss)
    }
    /// New keys accepted by the policy.
    pub fn keys_added(&self) -> u64 {
        self.get(Metric::KeyAdd)
    }
    /// Resident policy cost updates.
    pub fn keys_updated(&self) -> u64 {
        self.get(Metric::KeyUpdate)
    }
    /// Keys removed from the policy.
    pub fn keys_evicted(&self) -> u64 {
        self.get(Metric::KeyEvict)
    }
    /// Wrapping cumulative admitted cost, including replacement deltas.
    pub fn cost_added(&self) -> u64 {
        self.get(Metric::CostAdd)
    }
    /// Cumulative policy cost removed.
    pub fn cost_evicted(&self) -> u64 {
        self.get(Metric::CostEvict)
    }
    /// New writes dropped because the bounded queue was full.
    pub fn sets_dropped(&self) -> u64 {
        self.get(Metric::DropSets)
    }
    /// New writes rejected by a more frequent eviction candidate.
    pub fn sets_rejected(&self) -> u64 {
        self.get(Metric::RejectSets)
    }
    /// Read observations dropped at the policy channel.
    pub fn gets_dropped(&self) -> u64 {
        self.get(Metric::DropGets)
    }
    /// Read observations accepted by the policy channel.
    pub fn gets_kept(&self) -> u64 {
        self.get(Metric::KeepGets)
    }
    /// Successful reads divided by total reads.
    pub fn ratio(&self) -> f64 {
        let all = self.hits().wrapping_add(self.misses());
        if all == 0 {
            0.0
        } else {
            self.hits() as f64 / all as f64
        }
    }
    /// Reset all counters and the lifetime histogram.
    pub fn clear(&self) {
        if let Some(c) = &self.counters {
            for row in c.iter() {
                for c in row {
                    c.0.store(0, Ordering::Relaxed);
                }
            }
        }
        *self.life.lock().unwrap() = LifeExpectancy::default();
    }
}
impl fmt::Display for Metrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Some(counters) = &self.counters else {
            return Ok(());
        };
        for (name, row) in NAMES.iter().zip(counters.iter()) {
            let value = row.iter().fold(0_u64, |sum, c| {
                sum.wrapping_add(c.0.load(Ordering::Relaxed))
            });
            write!(f, "{name}: {value} ")?;
        }
        write!(
            f,
            "gets-total: {} hit-ratio: {:.2}",
            self.hits().wrapping_add(self.misses()),
            self.ratio()
        )
    }
}
