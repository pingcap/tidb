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
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Map, Value};

use super::observability::{BatchRequestProgress, BatchRequestState};
use super::priority_queue::{PriorityItem, PriorityQueue};

pub const HIGH_TASK_PRIORITY: u64 = 10;
pub const BATCH_POLICY_BASIC: &str = "basic";
pub const BATCH_POLICY_STANDARD: &str = "standard";
pub const BATCH_POLICY_POSITIVE: &str = "positive";
pub const BATCH_POLICY_CUSTOM: &str = "custom";
pub const DEFAULT_BATCH_POLICY: &str = BATCH_POLICY_STANDARD;

/// One cancellation and terminal-delivery authority supplied by the caller.
///
/// The scheduler stores no parallel completion state. The later async adapter
/// implements this trait with the same once-only handle returned to the pull
/// caller, so queue cancellation and terminal failure share one authority.
pub trait BatchEntryCompletion<E>: fmt::Debug + Send + Sync {
    fn is_canceled(&self) -> bool;
    fn fail(&self, error: E);
}

/// One opaque request waiting to be assigned a BatchCommands request ID.
#[derive(Debug)]
pub struct BatchEntry<T, E> {
    payload: T,
    forwarded_host: Option<String>,
    priority: u64,
    completion: Arc<dyn BatchEntryCompletion<E>>,
    progress: Arc<BatchRequestProgress>,
}

impl<T, E> BatchEntry<T, E> {
    pub fn new(payload: T, completion: Arc<dyn BatchEntryCompletion<E>>) -> Self {
        Self {
            payload,
            forwarded_host: None,
            priority: 0,
            completion,
            progress: Arc::new(BatchRequestProgress::new(None)),
        }
    }

    pub fn with_forwarded_host(mut self, forwarded_host: impl Into<String>) -> Self {
        let forwarded_host = forwarded_host.into();
        self.progress.set_forwarded_host(forwarded_host.clone());
        self.forwarded_host = Some(forwarded_host);
        self
    }

    pub const fn with_priority(mut self, priority: u64) -> Self {
        self.priority = priority;
        self
    }

    pub fn completion(&self) -> &dyn BatchEntryCompletion<E> {
        self.completion.as_ref()
    }

    pub fn progress(&self) -> Arc<BatchRequestProgress> {
        Arc::clone(&self.progress)
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

impl<T, E> PriorityItem for BatchEntry<T, E> {
    fn priority(&self) -> u64 {
        self.priority
    }

    fn is_canceled(&self) -> bool {
        self.completion.is_canceled()
    }
}

#[derive(Debug)]
pub struct ScheduledEntry<T, E> {
    request_id: u64,
    entry: BatchEntry<T, E>,
}

impl<T, E> ScheduledEntry<T, E> {
    pub fn request_id(&self) -> u64 {
        self.request_id
    }

    pub fn entry(&self) -> &BatchEntry<T, E> {
        &self.entry
    }
}

#[derive(Debug)]
pub struct BatchGroup<T, E> {
    entries: Vec<ScheduledEntry<T, E>>,
    state: BatchRequestState,
}

impl<T, E> Default for BatchGroup<T, E> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            state: BatchRequestState::default(),
        }
    }
}

impl<T, E> BatchGroup<T, E> {
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn entries(&self) -> &[ScheduledEntry<T, E>] {
        &self.entries
    }

    pub fn state(&self) -> BatchRequestState {
        self.state.clone()
    }

    fn push(&mut self, entry: ScheduledEntry<T, E>, selected_at: Instant) {
        entry.entry.progress.record_batch_selected_at(
            entry.request_id,
            selected_at,
            self.state.clone(),
        );
        self.entries.push(entry);
        self.state.set_batch_size(self.entries.len());
    }
}

#[derive(Debug)]
pub struct BatchGroups<T, E> {
    direct: Option<BatchGroup<T, E>>,
    forwarded: BTreeMap<String, BatchGroup<T, E>>,
}

impl<T, E> Default for BatchGroups<T, E> {
    fn default() -> Self {
        Self {
            direct: None,
            forwarded: BTreeMap::new(),
        }
    }
}

impl<T, E> BatchGroups<T, E> {
    pub fn direct(&self) -> Option<&BatchGroup<T, E>> {
        self.direct.as_ref()
    }

    pub fn forwarded(&self) -> &BTreeMap<String, BatchGroup<T, E>> {
        &self.forwarded
    }

    fn push(&mut self, entry: ScheduledEntry<T, E>, selected_at: Instant) {
        if let Some(host) = entry.entry.forwarded_host.clone() {
            self.forwarded
                .entry(host)
                .or_default()
                .push(entry, selected_at);
        } else {
            self.direct
                .get_or_insert_with(BatchGroup::default)
                .push(entry, selected_at);
        }
    }
}

/// Pure request collector shared by future synchronous and asynchronous batch clients.
#[derive(Debug)]
pub struct BatchScheduler<T, E> {
    id_alloc: u64,
    entries: PriorityQueue<BatchEntry<T, E>>,
}

impl<T, E> Default for BatchScheduler<T, E> {
    fn default() -> Self {
        Self {
            id_alloc: 0,
            entries: PriorityQueue::new(),
        }
    }
}

impl<T, E> BatchScheduler<T, E> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub const fn id_alloc(&self) -> u64 {
        self.id_alloc
    }

    pub fn push(&mut self, entry: BatchEntry<T, E>) {
        self.entries.push(entry);
    }

    /// Builds direct and forwarded request groups.
    ///
    /// Normal requests consume `limit`; priorities at or above
    /// `HIGH_TASK_PRIORITY` bypass it. Canceled requests are discarded without
    /// receiving an ID and without consuming concurrency.
    pub fn build_with_limit(&mut self, limit: usize) -> BatchGroups<T, E> {
        let mut groups = BatchGroups::default();
        let mut normal_count = 0;
        let selected_at = Instant::now();

        while (normal_count < limit && !self.entries.is_empty())
            || self.entries.highest_priority() >= HIGH_TASK_PRIORITY
        {
            let take_count = if limit == 0 { 1 } else { limit };
            for entry in self.entries.take(take_count) {
                if entry.completion.is_canceled() {
                    continue;
                }
                if entry.priority < HIGH_TASK_PRIORITY {
                    normal_count += 1;
                }

                self.id_alloc = self.id_alloc.wrapping_add(1);
                groups.push(
                    ScheduledEntry {
                        request_id: self.id_alloc,
                        entry,
                    },
                    selected_at,
                );
            }
        }

        groups
    }

    /// Removes canceled requests but preserves valid requests left by a limit.
    pub fn reset(&mut self) {
        self.entries.clean_canceled();
    }

    /// Fails and drains every queued request through its sole completion handle.
    pub fn cancel_all(&mut self, error: E) -> Vec<BatchEntry<T, E>>
    where
        E: Clone,
    {
        let entries = self.entries.drain();
        for entry in &entries {
            entry.completion.fail(error.clone());
        }
        entries
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct BatchPolicyOptions {
    pub version: i64,
    pub max_arrival_intervals: i64,
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
        source_effective_wait_time(self.options.wait_seconds)
    }

    pub fn need_fetch_more(&mut self, request_arrival_interval: Duration) -> bool {
        match self.options.version {
            1 => {
                let mut arrival = request_arrival_interval.as_secs_f64();
                if self.max_arrival_interval == 0.0 {
                    self.max_arrival_interval =
                        self.options.wait_seconds * self.options.max_arrival_intervals as f64;
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
    let value: Value = serde_json::from_str(raw).ok()?;
    let body = value.as_object()?;
    let mut options = BatchPolicyOptions::default();
    options.version = parse_json_integer(body, "v")?.unwrap_or_default();
    options.max_arrival_intervals = parse_json_integer(body, "n")?.unwrap_or_default();
    options.wait_seconds = parse_json_number(body, "t")?.unwrap_or_default();
    options.weight = parse_json_number(body, "w")?.unwrap_or_default();
    options.threshold = parse_json_number(body, "p")?.unwrap_or_default();
    options.wait_size_rounding = parse_json_number(body, "q")?.unwrap_or_default();
    Some(options)
}

fn parse_json_integer(body: &Map<String, Value>, key: &str) -> Option<Option<i64>> {
    let Some(value) = body.get(key) else {
        return Some(None);
    };
    if value.is_null() {
        return Some(None);
    }
    value.as_i64().map(Some)
}

fn parse_json_number(body: &Map<String, Value>, key: &str) -> Option<Option<f64>> {
    let Some(value) = body.get(key) else {
        return Some(None);
    };
    if value.is_null() {
        return Some(None);
    }
    value.as_f64().map(Some)
}

fn source_effective_wait_time(seconds: f64) -> Duration {
    // Go stores this as a signed nanosecond duration. Rust's Duration cannot
    // represent the source-valid negative or overflowing cases; both make the
    // source timer fire immediately, so retain the raw seconds in options and
    // project only their effective wait here.
    let nanoseconds = seconds * 1_000_000_000.0;
    if nanoseconds <= 0.0 || nanoseconds >= i64::MAX as f64 || !nanoseconds.is_finite() {
        return Duration::ZERO;
    }
    Duration::from_nanos(nanoseconds as u64)
}
