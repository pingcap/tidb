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

//! Ristretto policy.go: TinyLFU admission and sampled LFU cost accounting.
use crate::metrics::{Metric, Metrics};
use crate::sketch::TinyLfu;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) struct SampledLfu {
    pub max_cost: i64,
    pub used: i64,
    pub costs: HashMap<u64, i64>,
    // Native substitute for Go's randomized map iteration. The dense index
    // permits a different bounded sample without scanning the entire cache.
    keys: Vec<u64>,
    positions: HashMap<u64, usize>,
    metrics: Arc<Metrics>,
}
impl SampledLfu {
    pub fn new(max_cost: i64, metrics: Arc<Metrics>) -> Self {
        Self {
            max_cost,
            used: 0,
            costs: HashMap::new(),
            keys: Vec::new(),
            positions: HashMap::new(),
            metrics,
        }
    }
    pub fn add(&mut self, key: u64, cost: i64) {
        if !self.costs.contains_key(&key) {
            self.positions.insert(key, self.keys.len());
            self.keys.push(key);
        }
        self.costs.insert(key, cost);
        self.used += cost;
    }
    pub fn del(&mut self, key: u64) {
        let Some(cost) = self.costs.remove(&key) else {
            return;
        };
        self.used -= cost;
        let position = self.positions.remove(&key).unwrap();
        self.keys.swap_remove(position);
        if let Some(moved) = self.keys.get(position) {
            self.positions.insert(*moved, position);
        }
        self.metrics.add(Metric::CostEvict, key, cost as u64);
        self.metrics.add(Metric::KeyEvict, key, 1);
    }
    pub fn update(&mut self, key: u64, cost: i64) -> bool {
        let Some(previous) = self.costs.get_mut(&key) else {
            return false;
        };
        let difference = cost.wrapping_sub(*previous);
        self.used = self.used.wrapping_add(difference);
        *previous = cost;
        self.metrics.add(Metric::KeyUpdate, key, 1);
        self.metrics.add(Metric::CostAdd, key, difference as u64);
        true
    }
    pub fn room_left(&self, cost: i64) -> i64 {
        self.max_cost.wrapping_sub(self.used.wrapping_add(cost))
    }
    pub fn fill_sample(&self, sample: &mut Vec<(u64, i64)>) {
        if sample.len() >= 5 || self.keys.is_empty() {
            return;
        }
        let start = rand::random_range(0..self.keys.len());
        for i in 0..self.keys.len() {
            let key = self.keys[(start + i) % self.keys.len()];
            sample.push((key, self.costs[&key]));
            if sample.len() >= 5 {
                break;
            }
        }
    }
    pub fn clear(&mut self) {
        self.used = 0;
        self.costs.clear();
        self.keys.clear();
        self.positions.clear();
    }
}

pub(crate) struct Policy {
    pub admit: TinyLfu,
    pub evict: SampledLfu,
    metrics: Arc<Metrics>,
}
impl Policy {
    pub fn new(counters: usize, max_cost: i64, metrics: Arc<Metrics>) -> Self {
        Self {
            admit: TinyLfu::new(counters),
            evict: SampledLfu::new(max_cost, Arc::clone(&metrics)),
            metrics,
        }
    }
    pub fn add(&mut self, key: u64, cost: i64) -> (Vec<(u64, i64)>, bool) {
        if cost > self.evict.max_cost {
            return (Vec::new(), false);
        }
        if self.evict.update(key, cost) {
            return (Vec::new(), false);
        }
        let mut victims = Vec::new();
        let mut sample = Vec::with_capacity(5);
        let incoming_hits = self.admit.estimate(key);
        while self.evict.room_left(cost) < 0 {
            self.evict.fill_sample(&mut sample);
            let Some((index, &(victim, victim_cost))) = sample
                .iter()
                .enumerate()
                .min_by_key(|(_, (key, _))| self.admit.estimate(*key))
            else {
                // A nonpositive capacity updated after construction can leave
                // no victim. Reject instead of indexing an empty sample.
                return (victims, false);
            };
            if incoming_hits < self.admit.estimate(victim) {
                self.metrics.add(Metric::RejectSets, key, 1);
                return (victims, false);
            }
            self.evict.del(victim);
            sample.swap_remove(index);
            victims.push((victim, victim_cost));
        }
        self.evict.add(key, cost);
        self.metrics.add(Metric::CostAdd, key, cost as u64);
        (victims, true)
    }
    pub fn clear(&mut self) {
        self.admit.clear();
        self.evict.clear();
    }
}
