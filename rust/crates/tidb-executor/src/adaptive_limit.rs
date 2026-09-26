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

//! Statement-local admission feedback for Go's `exec.AdaptiveLimitController`.
//!
//! The two budgets use different units: outer rows bound an ordered index
//! join's prefetched outer input, while lookup handles bound double-read table
//! tasks. The direct lookup form has only the latter. Rust drives these stages
//! from one pull thread, so executor call sites use `try_reserve_*` and drain
//! already-started work when the window is full; the blocking reservation
//! methods preserve the controller contract for independent producers.

use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

const YIELD_WINDOW_SIZE: usize = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    IndexJoin,
    DirectIndexLookup,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    Outer,
    Lookup,
}

#[derive(Clone, Copy, Debug, Default)]
struct YieldWindow {
    inputs: [u64; YIELD_WINDOW_SIZE],
    outputs: [u64; YIELD_WINDOW_SIZE],
    next: usize,
}

impl YieldWindow {
    fn add(&mut self, input: u64, output: u64) {
        self.inputs[self.next] = input;
        self.outputs[self.next] = output;
        self.next = (self.next + 1) % YIELD_WINDOW_SIZE;
    }

    fn totals(self) -> (u64, u64) {
        self.inputs.into_iter().zip(self.outputs).fold(
            (0, 0),
            |(inputs, outputs), (input, output)| {
                (inputs.saturating_add(input), outputs.saturating_add(output))
            },
        )
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct AdmissionBlockStats {
    blocked_since: Option<Instant>,
    waiters: usize,
    blocked_time: Duration,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct AdaptiveLimitSnapshot {
    pub(crate) demand_rows: u64,
    pub(crate) output_rows: u64,
    pub(crate) outer_fetched: u64,
    pub(crate) outer_consumed: u64,
    pub(crate) outer_reserved: u64,
    pub(crate) outer_window: u64,
    pub(crate) outer_outstanding_at_stop: u64,
    pub(crate) lookup_reserved: u64,
    pub(crate) lookup_handles: u64,
    pub(crate) lookup_rows: u64,
    pub(crate) lookup_window: u64,
    pub(crate) lookup_batch_size: u64,
    pub(crate) lookup_physical_window: u64,
    pub(crate) lookup_outstanding_at_stop: u64,
    pub(crate) outer_admission_blocked: Duration,
    pub(crate) lookup_admission_blocked: Duration,
    pub(crate) stopped: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AdaptiveLimitRuntimeKind {
    DirectLookup,
    IndexJoin,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AdaptiveLimitRuntimeSnapshot {
    pub(crate) kind: AdaptiveLimitRuntimeKind,
    pub(crate) snapshot: AdaptiveLimitSnapshot,
}

pub(crate) type AdaptiveLimitRuntimeSink = Arc<Mutex<Option<AdaptiveLimitRuntimeSnapshot>>>;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct AdaptiveLimitConfig {
    pub(crate) demand_rows: u64,
    pub(crate) initial_outer_window: u64,
    pub(crate) max_outer_window: u64,
    pub(crate) initial_lookup_window: u64,
    pub(crate) max_lookup_window: u64,
    pub(crate) initial_lookup_batch_size: u64,
    pub(crate) max_lookup_batch_size: u64,
}

#[derive(Clone, Copy)]
struct Bounds {
    mode: Mode,
    demand_rows: u64,
    initial_outer_window: u64,
    max_outer_window: u64,
    initial_lookup_window: u64,
    max_lookup_window: u64,
    initial_lookup_batch_size: u64,
    max_lookup_batch_size: u64,
}

#[derive(Default)]
struct State {
    output_rows: u64,
    outer_fetched: u64,
    outer_consumed: u64,
    outer_reserved: u64,
    outer_outstanding_at_stop: u64,
    outer_admission_blocked: AdmissionBlockStats,
    pending_outer_output: u64,
    recent_outer_yield: YieldWindow,
    outer_no_output_rows: u64,
    outer_window: u64,
    outer_growth_barrier: u64,
    lookup_reserved: u64,
    lookup_handles: u64,
    lookup_rows: u64,
    lookup_outstanding_at_stop: u64,
    lookup_admission_blocked: AdmissionBlockStats,
    recent_lookup_yield: YieldWindow,
    lookup_no_output_rows: u64,
    lookup_in_no_output_phase: bool,
    lookup_window: u64,
    lookup_batch_size: u64,
    lookup_growth_progress: u64,
    stopped: bool,
}

impl State {
    fn new(bounds: Bounds) -> Self {
        let mut state = Self {
            outer_window: bounds.initial_outer_window,
            lookup_window: bounds.initial_lookup_window,
            lookup_batch_size: bounds.initial_lookup_batch_size,
            ..Self::default()
        };
        if bounds.demand_rows == 0 {
            state.stopped = true;
            state.outer_window = 0;
            state.lookup_window = 0;
            state.lookup_batch_size = 0;
        }
        state
    }
}

/// One controller is shared by the LIMIT and the eligible lookup/join stages
/// of a single executor tree. It is reset only after that tree's producers
/// have exited.
pub(crate) struct AdaptiveLimitController {
    bounds: Bounds,
    state: Mutex<State>,
    changed: Condvar,
}

/// A native pull executor's time waiting for admission capacity. Independent
/// waiters are counted as a union, like Go's blocked-time accounting.
pub(crate) struct AdmissionWait {
    controller: Arc<AdaptiveLimitController>,
    stage: Stage,
}

impl Drop for AdmissionWait {
    fn drop(&mut self) {
        let mut state = self.controller.lock();
        end_blocked(&mut state, self.stage, Instant::now());
    }
}

impl AdaptiveLimitController {
    pub(crate) fn for_index_join(config: AdaptiveLimitConfig) -> Arc<Self> {
        Self::new(config, Mode::IndexJoin)
    }

    pub(crate) fn for_direct_lookup(config: AdaptiveLimitConfig) -> Arc<Self> {
        Self::new(config, Mode::DirectIndexLookup)
    }

    fn new(config: AdaptiveLimitConfig, mode: Mode) -> Arc<Self> {
        let mut initial_outer_window = config.initial_outer_window;
        let mut max_outer_window = config.max_outer_window;
        let mut initial_lookup_window = config.initial_lookup_window;
        let mut max_lookup_window = config.max_lookup_window;
        (initial_outer_window, max_outer_window) =
            normalize_window(initial_outer_window, max_outer_window);
        (initial_lookup_window, max_lookup_window) =
            normalize_window(initial_lookup_window, max_lookup_window);
        let max_lookup_batch_size = config.max_lookup_batch_size.max(1).min(max_lookup_window);
        let initial_lookup_batch_size = config
            .initial_lookup_batch_size
            .max(initial_lookup_window.min(max_lookup_batch_size))
            .min(max_lookup_batch_size);
        if config.demand_rows > 0 {
            initial_outer_window = initial_outer_window.min(config.demand_rows);
            initial_lookup_window = initial_lookup_window.min(config.demand_rows);
        }
        if mode == Mode::DirectIndexLookup {
            initial_outer_window = 0;
            max_outer_window = 0;
        }
        let bounds = Bounds {
            mode,
            demand_rows: config.demand_rows,
            initial_outer_window,
            max_outer_window,
            initial_lookup_window,
            max_lookup_window,
            initial_lookup_batch_size,
            max_lookup_batch_size,
        };
        Arc::new(Self {
            bounds,
            state: Mutex::new(State::new(bounds)),
            changed: Condvar::new(),
        })
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Reuses immutable bounds for a new Open/Next/Close lifecycle.
    pub(crate) fn reset(&self) {
        *self.lock() = State::new(self.bounds);
        self.changed.notify_all();
    }

    /// Nonblocking outer reservation for Rust's single pull-thread executor.
    /// `None` means the caller must consume a pending task before reading more.
    pub(crate) fn try_reserve_outer(&self, max_rows: usize) -> Option<usize> {
        if self.bounds.mode == Mode::DirectIndexLookup {
            return None;
        }
        self.try_reserve(Stage::Outer, max_rows)
    }

    /// Nonblocking lookup reservation. A full budget means the caller should
    /// drain the oldest in-flight lookup window and try again.
    pub(crate) fn try_reserve_lookup(&self, max_handles: usize) -> Option<usize> {
        self.try_reserve(Stage::Lookup, max_handles)
    }

    fn try_reserve(&self, stage: Stage, max_units: usize) -> Option<usize> {
        if max_units == 0 {
            return Some(0);
        }
        let mut state = self.lock();
        if state.stopped {
            return None;
        }
        let (window, outstanding) = outstanding(&state, stage, self.bounds);
        if outstanding >= window {
            return None;
        }
        let mut units = (max_units as u64).min(window - outstanding);
        match stage {
            Stage::Outer => state.outer_reserved = state.outer_reserved.saturating_add(units),
            Stage::Lookup => {
                units = units.min(state.lookup_batch_size);
                state.lookup_reserved = state.lookup_reserved.saturating_add(units);
            }
        }
        Some(units as usize)
    }

    /// Blocking counterpart of Go's ReserveOuter, for an independent producer.
    /// The pull executor uses `try_reserve_outer` to avoid waiting on the same
    /// thread that must consume the task which frees capacity.
    pub(crate) fn reserve_outer(&self, max_rows: usize) -> Option<usize> {
        if self.bounds.mode == Mode::DirectIndexLookup {
            return None;
        }
        self.reserve(Stage::Outer, max_rows)
    }

    pub(crate) fn reserve_lookup(&self, max_handles: usize) -> Option<usize> {
        self.reserve(Stage::Lookup, max_handles)
    }

    fn reserve(&self, stage: Stage, max_units: usize) -> Option<usize> {
        if max_units == 0 {
            return Some(0);
        }
        let mut state = self.lock();
        let mut waiting = false;
        loop {
            if state.stopped {
                if waiting {
                    end_blocked(&mut state, stage, Instant::now());
                }
                return None;
            }
            let (window, outstanding) = outstanding(&state, stage, self.bounds);
            if outstanding < window {
                if waiting {
                    end_blocked(&mut state, stage, Instant::now());
                }
                let mut units = (max_units as u64).min(window - outstanding);
                match stage {
                    Stage::Outer => {
                        state.outer_reserved = state.outer_reserved.saturating_add(units);
                    }
                    Stage::Lookup => {
                        units = units.min(state.lookup_batch_size);
                        state.lookup_reserved = state.lookup_reserved.saturating_add(units);
                    }
                }
                return Some(units as usize);
            }
            if !waiting {
                begin_blocked(&mut state, stage, Instant::now());
                waiting = true;
            }
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    pub(crate) fn begin_outer_wait(self: &Arc<Self>) -> AdmissionWait {
        begin_wait(self, Stage::Outer)
    }

    pub(crate) fn begin_lookup_wait(self: &Arc<Self>) -> AdmissionWait {
        begin_wait(self, Stage::Lookup)
    }

    /// Settles an outer reservation with the number of rows actually fetched.
    pub(crate) fn commit_outer(&self, reserved: usize, fetched: usize) {
        let mut state = self.lock();
        let released = (reserved as u64).min(state.outer_reserved);
        state.outer_reserved -= released;
        state.outer_fetched = state
            .outer_fetched
            .saturating_add((fetched as u64).min(released));
        self.changed.notify_all();
    }

    /// Pairs fully consumed outer rows with all final rows produced for them.
    pub(crate) fn observe_join_progress(&self, consumed_rows: usize, output_rows: usize) {
        if consumed_rows == 0 && output_rows == 0 {
            return;
        }
        let mut state = self.lock();
        let previous_consumed = state.outer_consumed;
        if consumed_rows > 0 {
            state.outer_consumed = state
                .outer_consumed
                .saturating_add(consumed_rows as u64)
                .min(state.outer_fetched);
        }
        let consumed = state.outer_consumed - previous_consumed;
        if output_rows > 0 {
            state.output_rows = state.output_rows.saturating_add(output_rows as u64);
        }
        if state.output_rows >= self.bounds.demand_rows {
            self.stop_locked(&mut state);
            return;
        }
        if consumed == 0 {
            state.pending_outer_output = state
                .pending_outer_output
                .saturating_add(output_rows as u64);
            return;
        }
        let paired_output = state
            .pending_outer_output
            .saturating_add(output_rows as u64);
        state.pending_outer_output = 0;
        if paired_output > 0 {
            state.recent_outer_yield.add(consumed, paired_output);
            state.outer_no_output_rows = 0;
            self.recompute_outer_window(&mut state);
            self.recompute_lookup_window(&mut state);
        } else {
            if state.outer_no_output_rows == 0 {
                state.recent_outer_yield = YieldWindow::default();
            }
            state.outer_no_output_rows = state.outer_no_output_rows.saturating_add(consumed);
            self.grow_outer_window_if_drained(&mut state);
        }
        self.changed.notify_all();
    }

    /// Completes one result-ordered lookup task and learns its handle-to-row yield.
    pub(crate) fn complete_lookup(&self, reserved: usize, handles: usize, rows: usize) {
        if reserved == 0 {
            return;
        }
        let mut state = self.lock();
        if state.stopped || reserved as u64 > state.lookup_reserved {
            return;
        }
        state.lookup_reserved -= reserved as u64;
        state.lookup_handles = state.lookup_handles.saturating_add(handles as u64);
        state.lookup_rows = state.lookup_rows.saturating_add(rows as u64);
        if self.bounds.mode == Mode::DirectIndexLookup {
            state.output_rows = state.output_rows.saturating_add(rows as u64);
            if state.output_rows >= self.bounds.demand_rows {
                self.stop_locked(&mut state);
                return;
            }
        }
        if rows > 0 {
            state.recent_lookup_yield.add(handles as u64, rows as u64);
            state.lookup_no_output_rows = 0;
            state.lookup_in_no_output_phase = false;
            self.recompute_lookup_window(&mut state);
        } else {
            if !state.lookup_in_no_output_phase {
                state.recent_lookup_yield = YieldWindow::default();
            }
            state.lookup_in_no_output_phase = true;
            state.lookup_no_output_rows = state
                .lookup_no_output_rows
                .saturating_add((handles as u64).max(1));
            self.grow_lookup_window_if_drained(&mut state);
        }
        self.changed.notify_all();
    }

    /// Releases work that failed or was never dispatched, without learning from it.
    pub(crate) fn abort_lookup(&self, handles: usize) {
        if handles == 0 {
            return;
        }
        let mut state = self.lock();
        state.lookup_reserved -= (handles as u64).min(state.lookup_reserved);
        self.changed.notify_all();
    }

    pub(crate) fn suggested_batch_size(&self, ceiling: usize) -> usize {
        if ceiling == 0 {
            return 1;
        }
        self.lock().lookup_batch_size.min(ceiling as u64).max(1) as usize
    }

    /// Stops future admission and wakes independent blocked producers.
    pub(crate) fn stop(&self) {
        let mut state = self.lock();
        self.stop_locked(&mut state);
    }

    fn stop_locked(&self, state: &mut State) {
        if state.stopped {
            return;
        }
        state.stopped = true;
        let now = Instant::now();
        finish_blocked(&mut state.outer_admission_blocked, now);
        finish_blocked(&mut state.lookup_admission_blocked, now);
        state.outer_outstanding_at_stop = (state.outer_fetched
            - state.outer_fetched.min(state.outer_consumed))
        .saturating_add(state.outer_reserved);
        state.lookup_outstanding_at_stop = state.lookup_reserved;
        state.outer_reserved = 0;
        state.lookup_reserved = 0;
        state.outer_window = 0;
        state.lookup_window = 0;
        state.lookup_batch_size = 0;
        self.changed.notify_all();
    }

    pub(crate) fn snapshot(&self) -> AdaptiveLimitSnapshot {
        let state = self.lock();
        AdaptiveLimitSnapshot {
            demand_rows: self.bounds.demand_rows,
            output_rows: state.output_rows,
            outer_fetched: state.outer_fetched,
            outer_consumed: state.outer_consumed,
            outer_reserved: state.outer_reserved,
            outer_window: state.outer_window,
            outer_outstanding_at_stop: state.outer_outstanding_at_stop,
            lookup_reserved: state.lookup_reserved,
            lookup_handles: state.lookup_handles,
            lookup_rows: state.lookup_rows,
            lookup_window: state.lookup_window,
            lookup_batch_size: state.lookup_batch_size,
            lookup_physical_window: lookup_physical_window(&state, self.bounds),
            lookup_outstanding_at_stop: state.lookup_outstanding_at_stop,
            outer_admission_blocked: blocked_time(state.outer_admission_blocked),
            lookup_admission_blocked: blocked_time(state.lookup_admission_blocked),
            stopped: state.stopped,
        }
    }

    fn recompute_outer_window(&self, state: &mut State) {
        let remaining_output = self.bounds.demand_rows - state.output_rows;
        let mut estimated = divide_round_up(
            remaining_output.saturating_mul(state.outer_consumed),
            state.output_rows,
        );
        let (recent_input, recent_output) = state.recent_outer_yield.totals();
        if recent_output > 0 {
            estimated = estimated.max(divide_round_up(
                remaining_output.saturating_mul(recent_input),
                recent_output,
            ));
        }
        let target = add_headroom(estimated, remaining_output, self.bounds.demand_rows);
        let (window, grew) = adjust_window(
            target,
            state.outer_window,
            1,
            self.bounds.max_outer_window,
            state.outer_consumed > state.outer_growth_barrier,
        );
        state.outer_window = window;
        if grew {
            state.outer_growth_barrier = state.outer_fetched;
        }
    }

    fn recompute_lookup_window(&self, state: &mut State) {
        if self.bounds.mode == Mode::DirectIndexLookup {
            self.recompute_direct_lookup_window(state);
            return;
        }
        if state.lookup_rows == 0 || state.lookup_in_no_output_phase {
            return;
        }
        let lookup_buffered = state.lookup_rows - state.lookup_rows.min(state.outer_consumed);
        let outer_buffered = state.outer_fetched - state.outer_fetched.min(state.outer_consumed);
        let buffered = lookup_buffered.max(outer_buffered);
        let remaining_outer = state.outer_window - state.outer_window.min(buffered);
        let mut target = divide_round_up(
            remaining_outer.saturating_mul(state.lookup_handles),
            state.lookup_rows,
        );
        let (recent_handles, recent_rows) = state.recent_lookup_yield.totals();
        if recent_rows > 0 {
            target = target.max(divide_round_up(
                remaining_outer.saturating_mul(recent_handles),
                recent_rows,
            ));
        }
        let (window, grew) = adjust_window(
            target,
            state.lookup_window,
            self.bounds.initial_lookup_window,
            self.bounds.max_lookup_window,
            state.lookup_handles > state.lookup_growth_progress,
        );
        state.lookup_window = window;
        if grew {
            state.lookup_growth_progress = state.lookup_handles;
        }
    }

    fn recompute_direct_lookup_window(&self, state: &mut State) {
        if state.lookup_rows == 0 || state.lookup_in_no_output_phase {
            return;
        }
        let remaining_output = self
            .bounds
            .demand_rows
            .saturating_sub(state.output_rows.min(self.bounds.demand_rows));
        let mut estimate = divide_round_up(
            remaining_output.saturating_mul(state.lookup_handles),
            state.lookup_rows,
        );
        let (recent_handles, recent_rows) = state.recent_lookup_yield.totals();
        if recent_rows > 0 {
            estimate = estimate.max(divide_round_up(
                remaining_output.saturating_mul(recent_handles),
                recent_rows,
            ));
        }
        let target = add_headroom(estimate, remaining_output, self.bounds.demand_rows);
        let (window, grew) = adjust_window(
            target,
            state.lookup_window,
            self.bounds.initial_lookup_window,
            self.bounds.max_lookup_window,
            state.lookup_handles > state.lookup_growth_progress,
        );
        state.lookup_window = window;
        if grew {
            state.lookup_growth_progress = state.lookup_handles;
        }
    }

    fn grow_outer_window_if_drained(&self, state: &mut State) {
        let outstanding = state.outer_fetched - state.outer_fetched.min(state.outer_consumed)
            + state.outer_reserved;
        if outstanding != 0 || state.outer_no_output_rows < state.outer_window {
            return;
        }
        let next = grow_window(state.outer_window, self.bounds.max_outer_window);
        if next > state.outer_window {
            state.outer_growth_barrier = state.outer_fetched;
        }
        state.outer_window = next;
        state.outer_no_output_rows = 0;
        if !state.lookup_in_no_output_phase {
            self.recompute_lookup_window(state);
        }
    }

    fn grow_lookup_window_if_drained(&self, state: &mut State) {
        if state.lookup_reserved != 0 || state.lookup_no_output_rows < state.lookup_window {
            return;
        }
        let next = grow_window(state.lookup_window, self.bounds.max_lookup_window);
        if next > state.lookup_window {
            state.lookup_growth_progress = state.lookup_handles;
        }
        state.lookup_window = next;
        state.lookup_batch_size =
            grow_window(state.lookup_batch_size, self.bounds.max_lookup_batch_size);
        state.lookup_no_output_rows = 0;
    }
}

fn begin_wait(controller: &Arc<AdaptiveLimitController>, stage: Stage) -> AdmissionWait {
    let mut state = controller.lock();
    begin_blocked(&mut state, stage, Instant::now());
    drop(state);
    AdmissionWait {
        controller: Arc::clone(controller),
        stage,
    }
}

fn outstanding(state: &State, stage: Stage, bounds: Bounds) -> (u64, u64) {
    match stage {
        Stage::Outer => (
            state.outer_window,
            state
                .outer_fetched
                .saturating_sub(state.outer_consumed.min(state.outer_fetched))
                .saturating_add(state.outer_reserved),
        ),
        Stage::Lookup => (lookup_physical_window(state, bounds), state.lookup_reserved),
    }
}

fn lookup_physical_window(state: &State, bounds: Bounds) -> u64 {
    if state.lookup_window == 0 || state.lookup_batch_size == 0 {
        return 0;
    }
    divide_round_up(state.lookup_window, state.lookup_batch_size)
        .saturating_mul(state.lookup_batch_size)
        .min(bounds.max_lookup_window)
}

fn begin_blocked(state: &mut State, stage: Stage, now: Instant) {
    let stats = match stage {
        Stage::Outer => &mut state.outer_admission_blocked,
        Stage::Lookup => &mut state.lookup_admission_blocked,
    };
    if stats.waiters == 0 {
        stats.blocked_since = Some(now);
    }
    stats.waiters += 1;
}

fn end_blocked(state: &mut State, stage: Stage, now: Instant) {
    let stats = match stage {
        Stage::Outer => &mut state.outer_admission_blocked,
        Stage::Lookup => &mut state.lookup_admission_blocked,
    };
    if stats.waiters == 0 {
        return;
    }
    stats.waiters -= 1;
    if stats.waiters == 0 {
        if let Some(started) = stats.blocked_since.take() {
            stats.blocked_time = stats
                .blocked_time
                .saturating_add(now.saturating_duration_since(started));
        }
    }
}

fn finish_blocked(stats: &mut AdmissionBlockStats, now: Instant) {
    if stats.waiters == 0 {
        return;
    }
    if let Some(started) = stats.blocked_since.take() {
        stats.blocked_time = stats
            .blocked_time
            .saturating_add(now.saturating_duration_since(started));
    }
    stats.waiters = 0;
}

fn blocked_time(stats: AdmissionBlockStats) -> Duration {
    match (stats.waiters, stats.blocked_since) {
        (0, _) | (_, None) => stats.blocked_time,
        (_, Some(started)) => stats.blocked_time.saturating_add(started.elapsed()),
    }
}

fn normalize_window(initial: u64, maximum: u64) -> (u64, u64) {
    let initial = initial.max(1);
    (initial, maximum.max(initial))
}

fn grow_window(window: u64, maximum: u64) -> u64 {
    window.saturating_mul(2).min(maximum)
}

fn add_headroom(estimated: u64, remaining: u64, demand: u64) -> u64 {
    if remaining <= demand / 4 {
        estimated
    } else if remaining <= demand / 2 {
        divide_round_up(estimated.saturating_mul(9), 8)
    } else {
        divide_round_up(estimated.saturating_mul(5), 4)
    }
}

fn adjust_window(
    target: u64,
    current: u64,
    minimum: u64,
    maximum: u64,
    can_grow: bool,
) -> (u64, bool) {
    let target = target.max(minimum).min(maximum);
    if target > current {
        if !can_grow {
            return (current, false);
        }
        (target.min(grow_window(current, maximum)), true)
    } else {
        (target, false)
    }
}

fn divide_round_up(value: u64, divisor: u64) -> u64 {
    if divisor == 0 {
        return 0;
    }
    value / divisor + u64::from(value % divisor != 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    fn make_controller(demand: u64, outer: u64, lookup: u64) -> Arc<AdaptiveLimitController> {
        AdaptiveLimitController::for_index_join(AdaptiveLimitConfig {
            demand_rows: demand,
            initial_outer_window: outer,
            max_outer_window: 100_000,
            initial_lookup_window: lookup,
            max_lookup_window: 100_000,
            initial_lookup_batch_size: lookup,
            max_lookup_batch_size: 100_000,
        })
    }

    #[test]
    fn uses_current_execution_yield_and_tapers_the_tail() {
        let controller = make_controller(1000, 32, 32);
        let reserved = controller.reserve_outer(25_000).unwrap();
        controller.commit_outer(reserved, reserved);
        controller.observe_join_progress(32, 32);
        assert_eq!(controller.snapshot().outer_window, 64);

        let tail = make_controller(1000, 1024, 1024);
        let reserved = tail.reserve_outer(1024).unwrap();
        tail.commit_outer(reserved, reserved);
        tail.observe_join_progress(999, 999);
        assert_eq!(tail.snapshot().outer_window, 1);

        let middle = make_controller(1000, 500, 500);
        let reserved = middle.reserve_outer(500).unwrap();
        middle.commit_outer(reserved, reserved);
        middle.observe_join_progress(500, 500);
        assert_eq!(middle.snapshot().outer_window, 563);
    }

    #[test]
    fn grows_only_after_new_progress() {
        let controller = make_controller(1000, 32, 32);
        let reserved = controller.reserve_outer(32).unwrap();
        controller.commit_outer(reserved, reserved);
        controller.observe_join_progress(1, 1);
        assert_eq!(controller.snapshot().outer_window, 64);
        controller.observe_join_progress(1, 1);
        controller.observe_join_progress(1, 1);
        assert_eq!(controller.snapshot().outer_window, 64);
        let reserved = controller.reserve_outer(32).unwrap();
        controller.commit_outer(reserved, reserved);
        controller.observe_join_progress(29, 29);
        assert_eq!(controller.snapshot().outer_window, 64);
        controller.observe_join_progress(1, 1);
        assert_eq!(controller.snapshot().outer_window, 128);
    }

    #[test]
    fn pairs_output_with_completed_outer_rows() {
        let controller = make_controller(1000, 32, 32);
        let reserved = controller.reserve_outer(32).unwrap();
        controller.commit_outer(reserved, reserved);
        for _ in 0..4 {
            controller.observe_join_progress(0, 8);
        }
        assert_eq!(controller.snapshot().outer_window, 32);
        controller.observe_join_progress(1, 0);
        let snapshot = controller.snapshot();
        assert_eq!(snapshot.outer_consumed, 1);
        assert_eq!(snapshot.outer_window, 39);
    }

    #[test]
    fn grows_after_a_fully_drained_zero_output_window() {
        let controller = make_controller(1000, 32, 32);
        for expected in [32, 64, 128] {
            let reserved = controller.reserve_outer(25_000).unwrap();
            assert_eq!(reserved as u64, expected);
            controller.commit_outer(reserved, reserved);
            controller.observe_join_progress(reserved, 0);
        }
        assert_eq!(controller.snapshot().outer_window, 256);
    }

    #[test]
    fn stop_wakes_reservers_and_reset_restores_bounds() {
        let controller = make_controller(1000, 32, 32);
        let reserved = controller.reserve_outer(32).unwrap();
        controller.commit_outer(reserved, reserved);
        let waiter = Arc::clone(&controller);
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let thread = thread::spawn(move || {
            started_tx.send(()).unwrap();
            done_tx.send(waiter.reserve_outer(32)).unwrap();
        });
        started_rx.recv().unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while controller.lock().outer_admission_blocked.waiters == 0 {
            assert!(Instant::now() < deadline, "reservation did not block");
            thread::yield_now();
        }
        controller.stop();
        assert_eq!(done_rx.recv_timeout(Duration::from_secs(1)).unwrap(), None);
        thread.join().unwrap();
        let stopped = controller.snapshot();
        assert!(stopped.stopped);
        assert_eq!(stopped.outer_outstanding_at_stop, reserved as u64);
        assert_eq!(stopped.outer_window, 0);
        assert!(stopped.outer_admission_blocked > Duration::ZERO);
        controller.reset();
        let reset = controller.snapshot();
        assert!(!reset.stopped);
        assert_eq!(reset.outer_fetched, 0);
        assert_eq!(reset.outer_window, 32);
        assert_eq!(reset.outer_admission_blocked, Duration::ZERO);
    }

    #[test]
    fn lookup_budget_rounds_to_task_size_and_grows_on_empty_yield() {
        let controller = AdaptiveLimitController::for_index_join(AdaptiveLimitConfig {
            demand_rows: 1,
            initial_outer_window: 1,
            max_outer_window: 100_000,
            initial_lookup_window: 1,
            max_lookup_window: 100_000,
            initial_lookup_batch_size: 1024,
            max_lookup_batch_size: 20_000,
        });
        let snapshot = controller.snapshot();
        assert_eq!(snapshot.lookup_window, 1);
        assert_eq!(snapshot.lookup_batch_size, 1024);
        assert_eq!(snapshot.lookup_physical_window, 1024);
        let reserved = controller.reserve_lookup(20_000).unwrap();
        assert_eq!(reserved, 1024);
        controller.complete_lookup(reserved, reserved, 0);
        let snapshot = controller.snapshot();
        assert_eq!(snapshot.lookup_window, 2);
        assert_eq!(snapshot.lookup_batch_size, 2048);
        assert_eq!(snapshot.lookup_physical_window, 2048);

        let capped = AdaptiveLimitController::for_index_join(AdaptiveLimitConfig {
            demand_rows: 1,
            initial_outer_window: 1,
            max_outer_window: 500,
            initial_lookup_window: 1,
            max_lookup_window: 500,
            initial_lookup_batch_size: 1024,
            max_lookup_batch_size: 20_000,
        });
        assert_eq!(capped.snapshot().lookup_physical_window, 500);
        assert_eq!(capped.snapshot().lookup_batch_size, 500);

        let selective = make_controller(1000, 64, 32);
        let reserved = selective.reserve_lookup(32).unwrap();
        selective.complete_lookup(reserved, reserved, reserved);
        let reserved = selective.reserve_lookup(32).unwrap();
        selective.complete_lookup(reserved, reserved, 4);
        let snapshot = selective.snapshot();
        assert_eq!(snapshot.lookup_window, 50);
        assert_eq!(snapshot.lookup_batch_size, 32);
        assert_eq!(snapshot.lookup_physical_window, 64);
        assert!(
            snapshot.lookup_physical_window - snapshot.lookup_window < snapshot.lookup_batch_size
        );
    }

    #[test]
    fn direct_lookup_uses_lookup_yield_and_stops_at_demand() {
        let direct = AdaptiveLimitController::for_direct_lookup(AdaptiveLimitConfig {
            demand_rows: 1000,
            initial_lookup_window: 32,
            max_lookup_window: 100_000,
            initial_lookup_batch_size: 32,
            max_lookup_batch_size: 100_000,
            ..AdaptiveLimitConfig::default()
        });
        assert_eq!(direct.snapshot().outer_window, 0);
        assert_eq!(direct.try_reserve_outer(32), None);
        let reserved = direct.reserve_lookup(32).unwrap();
        direct.complete_lookup(reserved, reserved, 1);
        assert_eq!(direct.snapshot().output_rows, 1);
        assert!(direct.snapshot().lookup_window > 32);

        let at_demand = AdaptiveLimitController::for_direct_lookup(AdaptiveLimitConfig {
            demand_rows: 2,
            initial_lookup_window: 2,
            max_lookup_window: 32,
            initial_lookup_batch_size: 2,
            max_lookup_batch_size: 32,
            ..AdaptiveLimitConfig::default()
        });
        let reserved = at_demand.reserve_lookup(2).unwrap();
        at_demand.complete_lookup(reserved, reserved, 2);
        let snapshot = at_demand.snapshot();
        assert_eq!(snapshot.output_rows, 2);
        assert!(snapshot.stopped);
        assert_eq!(snapshot.lookup_window, 0);
        assert_eq!(snapshot.lookup_batch_size, 0);
    }

    #[test]
    fn direct_lookup_grows_after_empty_tasks() {
        let controller = AdaptiveLimitController::for_direct_lookup(AdaptiveLimitConfig {
            demand_rows: 1000,
            initial_lookup_window: 32,
            max_lookup_window: 100_000,
            initial_lookup_batch_size: 32,
            max_lookup_batch_size: 100_000,
            ..AdaptiveLimitConfig::default()
        });
        for expected in [64, 128, 256] {
            let reserved = controller.reserve_lookup(1000).unwrap();
            controller.complete_lookup(reserved, reserved, 0);
            assert_eq!(controller.snapshot().lookup_window, expected);
        }
    }

    #[test]
    fn overlapping_lookup_waits_count_as_one_interval() {
        let controller = AdaptiveLimitController::for_index_join(AdaptiveLimitConfig {
            demand_rows: 1000,
            initial_outer_window: 1,
            max_outer_window: 1,
            initial_lookup_window: 1,
            max_lookup_window: 1,
            initial_lookup_batch_size: 1,
            max_lookup_batch_size: 1,
        });
        let first = controller.reserve_lookup(1).unwrap();
        let started = Instant::now();
        let mut threads = Vec::new();
        for _ in 0..2 {
            let controller = Arc::clone(&controller);
            threads.push(thread::spawn(move || controller.reserve_lookup(1)));
        }
        let deadline = Instant::now() + Duration::from_secs(1);
        while controller.lock().lookup_admission_blocked.waiters < 2 {
            assert!(
                Instant::now() < deadline,
                "lookup reservations did not block"
            );
            thread::yield_now();
        }
        controller.abort_lookup(first);
        let one = threads.remove(0).join().unwrap().unwrap();
        controller.abort_lookup(one);
        let two = threads.remove(0).join().unwrap().unwrap();
        controller.abort_lookup(two);
        let snapshot = controller.snapshot();
        assert!(snapshot.lookup_admission_blocked > Duration::ZERO);
        assert!(snapshot.lookup_admission_blocked <= started.elapsed() + Duration::from_millis(50));
        assert_eq!(snapshot.lookup_reserved, 0);
    }
}
