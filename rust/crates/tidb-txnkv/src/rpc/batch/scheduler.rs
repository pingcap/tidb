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

//! Source-shaped BatchCommands grouping and adaptive policy.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::priority_queue::{PriorityItem, PriorityQueue};

pub const HIGH_TASK_PRIORITY: u64 = 10;
pub const BATCH_POLICY_BASIC: &str = "basic";
pub const BATCH_POLICY_STANDARD: &str = "standard";
pub const BATCH_POLICY_POSITIVE: &str = "positive";
pub const BATCH_POLICY_CUSTOM: &str = "custom";
pub const DEFAULT_BATCH_POLICY: &str = BATCH_POLICY_STANDARD;

/// State retained by the caller while an entry is owned by the scheduler.
#[derive(Clone, Debug, Default)]
pub struct BatchEntryState {
    canceled: Arc<AtomicBool>,
    failure: Arc<Mutex<Option<String>>>,
}

impl BatchEntryState {
    pub fn cancel(&self) {
        self.canceled.store(true, Ordering::Release);
    }

    pub fn is_canceled(&self) -> bool {
        self.canceled.load(Ordering::Acquire)
    }

    pub fn failure(&self) -> Option<String> {
        self.failure.lock().expect("batch failure lock").clone()
    }

    fn fail(&self, reason: &str) {
        *self.failure.lock().expect("batch failure lock") = Some(reason.to_owned());
    }
}

/// One opaque request waiting to be assigned a BatchCommands request ID.
#[derive(Debug)]
pub struct BatchEntry<T> {
    payload: T,
    forwarded_host: Option<String>,
    priority: u64,
    state: BatchEntryState,
}

impl<T> BatchEntry<T> {
    pub fn new(payload: T) -> Self {
        Self {
            payload,
            forwarded_host: None,
            priority: 0,
            state: BatchEntryState::default(),
        }
    }

    pub fn with_forwarded_host(mut self, forwarded_host: impl Into<String>) -> Self {
        self.forwarded_host = Some(forwarded_host.into());
        self
    }

    pub const fn with_priority(mut self, priority: u64) -> Self {
        self.priority = priority;
        self
    }

    pub fn state(&self) -> BatchEntryState {
        self.state.clone()
    }

    pub fn payload(&self) -> &T {
        &self.payload
    }

    pub fn forwarded_host(&self) -> Option<&str> {
        self.forwarded_host.as_deref()
    }

    pub const fn priority_value(&self) -> u64 {
        self.priority
    }
}

impl<T> PriorityItem for BatchEntry<T> {
    fn priority(&self) -> u64 {
        self.priority
    }

    fn is_canceled(&self) -> bool {
        self.state.is_canceled()
    }
}

#[derive(Debug)]
pub struct ScheduledEntry<T> {
    request_id: u64,
    entry: BatchEntry<T>,
}

impl<T> ScheduledEntry<T> {
    pub fn request_id(&self) -> u64 {
        self.request_id
    }

    pub fn entry(&self) -> &BatchEntry<T> {
        &self.entry
    }
}

#[derive(Debug)]
pub struct BatchGroup<T> {
    entries: Vec<ScheduledEntry<T>>,
}

impl<T> Default for BatchGroup<T> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl<T> BatchGroup<T> {
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn entries(&self) -> &[ScheduledEntry<T>] {
        &self.entries
    }

    fn push(&mut self, entry: ScheduledEntry<T>) {
        self.entries.push(entry);
    }
}

#[derive(Debug)]
pub struct BatchGroups<T> {
    direct: Option<BatchGroup<T>>,
    forwarded: BTreeMap<String, BatchGroup<T>>,
}

impl<T> Default for BatchGroups<T> {
    fn default() -> Self {
        Self {
            direct: None,
            forwarded: BTreeMap::new(),
        }
    }
}

impl<T> BatchGroups<T> {
    pub fn direct(&self) -> Option<&BatchGroup<T>> {
        self.direct.as_ref()
    }

    pub fn forwarded(&self) -> &BTreeMap<String, BatchGroup<T>> {
        &self.forwarded
    }

    fn push(&mut self, entry: ScheduledEntry<T>) {
        if let Some(host) = entry.entry.forwarded_host.clone() {
            self.forwarded.entry(host).or_default().push(entry);
        } else {
            self.direct
                .get_or_insert_with(BatchGroup::default)
                .push(entry);
        }
    }
}

/// Pure request collector shared by future synchronous and asynchronous batch clients.
#[derive(Debug)]
pub struct BatchScheduler<T> {
    id_alloc: u64,
    entries: PriorityQueue<BatchEntry<T>>,
}

impl<T> Default for BatchScheduler<T> {
    fn default() -> Self {
        Self {
            id_alloc: 0,
            entries: PriorityQueue::new(),
        }
    }
}

impl<T> BatchScheduler<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub const fn id_alloc(&self) -> u64 {
        self.id_alloc
    }

    pub fn push(&mut self, entry: BatchEntry<T>) {
        self.entries.push(entry);
    }

    /// Builds direct and forwarded request groups.
    ///
    /// Normal requests consume `limit`; priorities at or above
    /// `HIGH_TASK_PRIORITY` bypass it. Canceled requests are discarded without
    /// receiving an ID and without consuming concurrency.
    pub fn build_with_limit(&mut self, limit: usize) -> BatchGroups<T> {
        let mut groups = BatchGroups::default();
        let mut normal_count = 0;

        while !self.entries.is_empty() {
            let high_priority = self.entries.highest_priority() >= HIGH_TASK_PRIORITY;
            if !high_priority && normal_count >= limit {
                break;
            }

            let Some(entry) = self.entries.pop() else {
                break;
            };
            if entry.state.is_canceled() {
                continue;
            }
            if entry.priority < HIGH_TASK_PRIORITY {
                normal_count += 1;
            }

            self.id_alloc = self.id_alloc.wrapping_add(1);
            groups.push(ScheduledEntry {
                request_id: self.id_alloc,
                entry,
            });
        }

        groups
    }

    /// Removes canceled requests but preserves valid requests left by a limit.
    pub fn reset(&mut self) {
        self.entries.clean_canceled();
    }

    /// Fails and drains every queued request without invoking a transport callback.
    pub fn cancel_all(&mut self, reason: &str) -> Vec<BatchEntry<T>> {
        let entries = self.entries.drain();
        for entry in &entries {
            entry.state.fail(reason);
        }
        entries
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct BatchPolicyOptions {
    pub version: i32,
    pub max_arrival_intervals: i32,
    pub wait_seconds: f64,
    pub weight: f64,
    pub threshold: f64,
    pub wait_size_rounding: f64,
}

impl BatchPolicyOptions {
    const fn standard() -> Self {
        Self {
            version: 1,
            max_arrival_intervals: 5,
            wait_seconds: 0.0001,
            weight: 0.2,
            threshold: 0.8,
            wait_size_rounding: 0.8,
        }
    }

    const fn positive() -> Self {
        Self {
            wait_seconds: 0.0001,
            ..Self::default_const()
        }
    }

    const fn default_const() -> Self {
        Self {
            version: 0,
            max_arrival_intervals: 0,
            wait_seconds: 0.0,
            weight: 0.0,
            threshold: 0.0,
            wait_size_rounding: 0.0,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BatchTrigger {
    options: BatchPolicyOptions,
    estimated_fetch_probability: f64,
    estimated_arrival_interval: f64,
    max_arrival_interval: f64,
}

impl BatchTrigger {
    pub fn from_policy(policy: &str) -> (Self, bool) {
        let options = match policy {
            BATCH_POLICY_BASIC => Some(BatchPolicyOptions::default()),
            BATCH_POLICY_STANDARD => Some(BatchPolicyOptions::standard()),
            BATCH_POLICY_POSITIVE => Some(BatchPolicyOptions::positive()),
            _ => parse_custom_options(policy),
        };
        let valid = options.is_some();
        let options = options.unwrap_or_else(BatchPolicyOptions::standard);
        (
            Self {
                options,
                estimated_fetch_probability: 0.0,
                estimated_arrival_interval: 0.0,
                max_arrival_interval: 0.0,
            },
            valid,
        )
    }

    pub const fn options(&self) -> BatchPolicyOptions {
        self.options
    }

    pub fn turbo_wait_time(&self) -> Duration {
        Duration::from_secs_f64(self.options.wait_seconds.max(0.0))
    }

    pub fn need_fetch_more(&mut self, request_arrival_interval: Duration) -> bool {
        match self.options.version {
            1 => {
                let mut arrival = request_arrival_interval.as_secs_f64();
                if self.max_arrival_interval == 0.0 {
                    self.max_arrival_interval =
                        self.options.wait_seconds * f64::from(self.options.max_arrival_intervals);
                }
                arrival = arrival.min(self.max_arrival_interval);
                if self.estimated_arrival_interval == 0.0 {
                    self.estimated_arrival_interval = arrival;
                } else {
                    self.estimated_arrival_interval = self.options.weight * arrival
                        + (1.0 - self.options.weight) * self.estimated_arrival_interval;
                }
                self.estimated_arrival_interval < self.options.wait_seconds * self.options.threshold
            }
            2 => {
                let observed = if request_arrival_interval.as_secs_f64() < self.options.wait_seconds
                {
                    1.0
                } else {
                    0.0
                };
                self.estimated_fetch_probability = self.options.weight * observed
                    + (1.0 - self.options.weight) * self.estimated_fetch_probability;
                self.estimated_fetch_probability > self.options.threshold
            }
            _ => true,
        }
    }

    pub fn preferred_batch_wait_size(
        &self,
        average_batch_wait_size: f64,
        default_batch_wait_size: usize,
    ) -> usize {
        if self.options.version == 0 {
            return default_batch_wait_size;
        }
        let whole = average_batch_wait_size.trunc();
        let fractional = average_batch_wait_size.fract();
        let rounded = if fractional >= self.options.wait_size_rounding {
            whole + 1.0
        } else {
            whole
        };
        rounded.max(0.0) as usize
    }

    pub const fn estimated_arrival_interval(&self) -> f64 {
        self.estimated_arrival_interval
    }

    pub const fn max_arrival_interval(&self) -> f64 {
        self.max_arrival_interval
    }
}

fn parse_custom_options(policy: &str) -> Option<BatchPolicyOptions> {
    let raw = policy
        .strip_prefix(BATCH_POLICY_CUSTOM)
        .unwrap_or(policy)
        .trim();
    let body = raw.strip_prefix('{')?.strip_suffix('}')?.trim();
    let mut options = BatchPolicyOptions::default();
    if body.is_empty() {
        return Some(options);
    }

    for member in body.split(',') {
        let (key, value) = member.split_once(':')?;
        let key = key.trim();
        let key = key.strip_prefix('"')?.strip_suffix('"')?;
        let value = value.trim();
        match key {
            "v" => options.version = parse_json_integer(value)?,
            "n" => options.max_arrival_intervals = parse_json_integer(value)?,
            "t" => options.wait_seconds = parse_json_number(value)?,
            "w" => options.weight = parse_json_number(value)?,
            "p" => options.threshold = parse_json_number(value)?,
            "q" => options.wait_size_rounding = parse_json_number(value)?,
            // encoding/json ignores unknown object fields.
            _ => {
                parse_json_number(value)?;
            }
        }
    }
    Some(options)
}

fn parse_json_integer(raw: &str) -> Option<i32> {
    let value = parse_json_number(raw)?;
    if value.fract() != 0.0 || value < f64::from(i32::MIN) || value > f64::from(i32::MAX) {
        return None;
    }
    Some(value as i32)
}

fn parse_json_number(raw: &str) -> Option<f64> {
    let value = raw.parse::<f64>().ok()?;
    value.is_finite().then_some(value)
}
