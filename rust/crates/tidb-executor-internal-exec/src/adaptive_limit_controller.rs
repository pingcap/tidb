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

//! Go `pkg/executor/internal/exec/adaptive_limit_controller.go` (0b505ecc58b,
//! 33bdc22d43d, cd9601b3f2f): statement-local admission control for an
//! early-stop ordered LIMIT scan.

use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender};
use tidb_gocontext::{Context, CtxError};

/// Go `adaptiveYieldWindowSize`.
const ADAPTIVE_YIELD_WINDOW_SIZE: usize = 4;

/// Go `adaptiveLimitMode`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum AdaptiveLimitMode {
    IndexJoin,
    DirectIndexLookup,
    IndexJoinOuterOnly,
}

/// Go `adaptiveAdmissionStage`: both the admitted unit and the accounting
/// path: outer admission counts rows, while lookup admission counts handles.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum AdaptiveAdmissionStage {
    OuterRows,
    LookupHandles,
}

/// Go `adaptiveYieldWindow`: a small recent sample alongside the
/// controller's cumulative counters, so an ordered scan reacts when the
/// selectivity near its current position differs from earlier input.
#[derive(Clone, Copy, Default, Debug)]
struct AdaptiveYieldWindow {
    /// Recent input counts: outer rows for join feedback or index handles for
    /// lookup feedback.
    inputs: [u64; ADAPTIVE_YIELD_WINDOW_SIZE],
    /// The output count paired with each `inputs` sample.
    outputs: [u64; ADAPTIVE_YIELD_WINDOW_SIZE],
    /// The ring-buffer slot replaced by the next sample.
    next: usize,
}

impl AdaptiveYieldWindow {
    fn add(&mut self, input: u64, output: u64) {
        self.inputs[self.next] = input;
        self.outputs[self.next] = output;
        self.next = (self.next + 1) % ADAPTIVE_YIELD_WINDOW_SIZE;
    }

    fn totals(&self) -> (u64, u64) {
        let mut input = 0;
        let mut output = 0;
        for i in 0..ADAPTIVE_YIELD_WINDOW_SIZE {
            input = saturating_add(input, self.inputs[i]);
            output = saturating_add(output, self.outputs[i]);
        }
        (input, output)
    }
}

/// Go `admissionBlockStats`: the union of admission-blocked intervals for one
/// stage. Concurrent waiters overlap in wall-clock time and are counted once.
#[derive(Clone, Copy, Default, Debug)]
struct AdmissionBlockStats {
    /// The start of the current blocked interval, `None` when no reserver is
    /// waiting.
    blocked_since: Option<Instant>,
    /// The number of reservers currently blocked by admission.
    waiters: usize,
    /// Completed blocked intervals.
    blocked_time: Duration,
}

/// Go `AdaptiveLimitSnapshot`: a point-in-time view of an
/// [`AdaptiveLimitController`]. Outer fields are measured in outer rows.
/// Lookup reservation, handle, window, and batch fields are measured in
/// handles; `lookup_rows` is measured in table rows. `output_rows` counts
/// final output observed by the join or, for a direct IndexLookUp, when a
/// lookup task is fully consumed. The admission-blocked fields are wall-clock
/// durations with overlapping waits in the same stage counted once.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct AdaptiveLimitSnapshot {
    pub output_rows: u64,
    pub outer_fetched: u64,
    pub outer_consumed: u64,
    pub outer_reserved: u64,
    pub outer_window: u64,
    pub outer_outstanding_at_stop: u64,
    pub lookup_reserved: u64,
    pub lookup_handles: u64,
    pub lookup_rows: u64,
    pub lookup_window: u64,
    pub lookup_batch_size: u64,
    pub lookup_physical_window: u64,
    pub lookup_outstanding_at_stop: u64,
    pub outer_admission_blocked: Duration,
    pub lookup_admission_blocked: Duration,
    pub has_lookup_stage: bool,
    pub stopped: bool,
}

/// Go `AdaptiveLimitConfig`: the immutable bounds of one statement-local
/// controller. Outer windows are measured in outer rows; lookup windows and
/// batches in index handles.
#[derive(Clone, Copy, Default, Debug)]
pub struct AdaptiveLimitConfig {
    /// The final-row demand derived from LIMIT offset plus count.
    pub demand_rows: u64,
    pub initial_outer_window: u64,
    pub max_outer_window: u64,
    pub initial_lookup_window: u64,
    pub max_lookup_window: u64,
    /// The starting number of handles assigned to one lookup task.
    pub initial_lookup_batch_size: u64,
    pub max_lookup_batch_size: u64,
    /// Optional floors for lookup growth once an admitted window returns no
    /// rows.
    pub no_output_lookup_batch_size: u64,
    pub no_output_lookup_window: u64,
}

/// The mutable state Go's `AdaptiveLimitController` guards with `mu`.
struct State {
    /// Wakes outer reservers after capacity or lifecycle changes; its single
    /// buffered signal coalesces duplicate notifications.
    outer_changed: (Sender<()>, Receiver<()>),
    /// Go `stopCh`: `Stop` drops the sender to wake blocked producers, and it
    /// stays `None` until `Reset`.
    stop_tx: Option<Sender<()>>,
    stop_rx: Receiver<()>,
    lookup_changed: (Sender<()>, Receiver<()>),
    lookup_admission_blocked: AdmissionBlockStats,
    outer_admission_blocked: AdmissionBlockStats,
    recent_lookup_yield: AdaptiveYieldWindow,
    recent_outer_yield: AdaptiveYieldWindow,
    /// `outer_fetched` at the latest productive growth, preventing another
    /// growth decision until consumption passes that frontier.
    outer_growth_barrier: u64,
    max_outer_window: u64,
    /// Final rows observed before their outer rows are reported consumed.
    pending_outer_output: u64,
    outer_reserved: u64,
    outer_window: u64,
    lookup_handles: u64,
    lookup_rows: u64,
    outer_consumed: u64,
    lookup_outstanding_at_stop: u64,
    demand_rows: u64,
    outer_fetched: u64,
    initial_outer_window: u64,
    lookup_reserved: u64,
    outer_outstanding_at_stop: u64,
    /// Outer rows consumed in the current zero-output phase.
    outer_no_output_rows: u64,
    initial_lookup_window: u64,
    max_lookup_window: u64,
    lookup_window: u64,
    initial_lookup_batch_size: u64,
    max_lookup_batch_size: u64,
    no_output_lookup_batch_size: u64,
    no_output_lookup_window: u64,
    /// The execution-task size in index handles, independent of the logical
    /// lookup window.
    lookup_batch_size: u64,
    /// A temporary batch increase during sustained Join misses while the outer
    /// lookup continues to produce rows.
    join_lookup_batch_size: u64,
    /// `lookup_handles` at the latest growth decision.
    lookup_growth_progress: u64,
    /// Handles consumed in the current zero-output phase.
    lookup_no_output_rows: u64,
    output_rows: u64,
    /// A zero-output window has triggered growth in this phase. Single reads
    /// also use it to coalesce small grants.
    outer_in_no_output_growth: bool,
    /// Suppresses productive lookup-yield adjustment until a subsequent lookup
    /// task returns rows.
    lookup_in_no_output_phase: bool,
    mode: AdaptiveLimitMode,
}

/// Go `AdaptiveLimitController`: bounds speculative work for an early-stop
/// LIMIT. It is owned by one executor tree and learns only from the current
/// execution.
///
/// It keeps two independent admission lifecycles:
///
/// ```text
/// outer rows:     reserve_outer -> commit_outer -> observe_join_progress
/// lookup handles: reserve_lookup -> lookup task -> complete_lookup / abort_lookup
/// ```
///
/// An IndexJoin uses outer admission to bound the rows it consumes to
/// produce final rows. A double-read outer input also uses lookup admission
/// to bound the index handles used to produce outer rows. A direct
/// IndexLookUp uses only lookup admission. Every reservation must eventually
/// be committed, completed, aborted, or cleared by `stop`.
pub struct AdaptiveLimitController {
    mode: AdaptiveLimitMode,
    mu: Mutex<State>,
}

impl std::fmt::Debug for AdaptiveLimitController {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AdaptiveLimitController")
            .field("mode", &self.mode)
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

fn signal_channel() -> (Sender<()>, Receiver<()>) {
    crossbeam_channel::bounded(1)
}

fn stop_channel() -> (Option<Sender<()>>, Receiver<()>) {
    let (tx, rx) = crossbeam_channel::bounded(0);
    (Some(tx), rx)
}

impl AdaptiveLimitController {
    /// Go `NewAdaptiveLimitController`.
    pub fn new(config: AdaptiveLimitConfig) -> Self {
        Self::with_mode(config, AdaptiveLimitMode::IndexJoin)
    }

    /// Go `NewAdaptiveLimitLookupController`: a controller for a direct
    /// ordered IndexLookUp under LIMIT, which has no Join outer stage.
    pub fn new_lookup(config: AdaptiveLimitConfig) -> Self {
        Self::with_mode(config, AdaptiveLimitMode::DirectIndexLookup)
    }

    /// Go `NewAdaptiveLimitOuterJoinController`: admits only outer rows for an
    /// IndexJoin whose outer input is a single read.
    pub fn new_outer_join(config: AdaptiveLimitConfig) -> Self {
        Self::with_mode(config, AdaptiveLimitMode::IndexJoinOuterOnly)
    }

    /// Go `HasLookupStage`.
    pub fn has_lookup_stage(&self) -> bool {
        self.mode != AdaptiveLimitMode::IndexJoinOuterOnly
    }

    /// Go `newAdaptiveLimitController`.
    fn with_mode(config: AdaptiveLimitConfig, mode: AdaptiveLimitMode) -> Self {
        let demand_rows = config.demand_rows;
        let (mut initial_outer_window, max_outer_window) =
            normalize_adaptive_window(config.initial_outer_window, config.max_outer_window);
        let (mut initial_lookup_window, max_lookup_window) =
            normalize_adaptive_window(config.initial_lookup_window, config.max_lookup_window);
        let max_lookup_batch_size = config.max_lookup_batch_size.max(1).min(max_lookup_window);
        let initial_lookup_batch_size = config
            .initial_lookup_batch_size
            .max(initial_lookup_window.min(max_lookup_batch_size))
            .min(max_lookup_batch_size);
        if demand_rows > 0 && initial_outer_window > demand_rows {
            initial_outer_window = demand_rows;
        }
        if demand_rows > 0 && initial_lookup_window > demand_rows {
            initial_lookup_window = demand_rows;
        }
        let (stop_tx, stop_rx) = stop_channel();
        let mut state = State {
            outer_changed: signal_channel(),
            stop_tx,
            stop_rx,
            lookup_changed: signal_channel(),
            lookup_admission_blocked: AdmissionBlockStats::default(),
            outer_admission_blocked: AdmissionBlockStats::default(),
            recent_lookup_yield: AdaptiveYieldWindow::default(),
            recent_outer_yield: AdaptiveYieldWindow::default(),
            outer_growth_barrier: 0,
            max_outer_window,
            pending_outer_output: 0,
            outer_reserved: 0,
            outer_window: initial_outer_window,
            lookup_handles: 0,
            lookup_rows: 0,
            outer_consumed: 0,
            lookup_outstanding_at_stop: 0,
            demand_rows,
            outer_fetched: 0,
            initial_outer_window,
            lookup_reserved: 0,
            outer_outstanding_at_stop: 0,
            outer_no_output_rows: 0,
            initial_lookup_window,
            max_lookup_window,
            lookup_window: initial_lookup_window,
            initial_lookup_batch_size,
            max_lookup_batch_size,
            no_output_lookup_batch_size: config.no_output_lookup_batch_size.min(max_lookup_batch_size),
            no_output_lookup_window: config.no_output_lookup_window.min(max_lookup_window),
            lookup_batch_size: initial_lookup_batch_size,
            join_lookup_batch_size: 0,
            lookup_growth_progress: 0,
            lookup_no_output_rows: 0,
            output_rows: 0,
            outer_in_no_output_growth: false,
            lookup_in_no_output_phase: false,
            mode,
        };
        if mode == AdaptiveLimitMode::DirectIndexLookup {
            state.initial_outer_window = 0;
            state.max_outer_window = 0;
            state.outer_window = 0;
        }
        if mode == AdaptiveLimitMode::IndexJoinOuterOnly {
            state.initial_lookup_window = 0;
            state.max_lookup_window = 0;
            state.lookup_window = 0;
            state.initial_lookup_batch_size = 0;
            state.max_lookup_batch_size = 0;
            state.lookup_batch_size = 0;
        }
        if demand_rows == 0 {
            state.stop();
        }
        Self { mode, mu: Mutex::new(state) }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.mu.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Go `Reset`: prepares the controller for another Open/Next/Close
    /// lifecycle. Callers must ensure all producers from the previous
    /// lifecycle have exited.
    pub fn reset(&self) {
        let mut c = self.lock();
        c.output_rows = 0;
        c.outer_fetched = 0;
        c.outer_consumed = 0;
        c.outer_reserved = 0;
        c.outer_outstanding_at_stop = 0;
        c.outer_admission_blocked = AdmissionBlockStats::default();
        c.pending_outer_output = 0;
        c.recent_outer_yield = AdaptiveYieldWindow::default();
        c.outer_no_output_rows = 0;
        c.outer_in_no_output_growth = false;
        c.outer_window = c.initial_outer_window;
        c.outer_growth_barrier = 0;
        c.lookup_reserved = 0;
        c.lookup_handles = 0;
        c.lookup_rows = 0;
        c.recent_lookup_yield = AdaptiveYieldWindow::default();
        c.lookup_no_output_rows = 0;
        c.lookup_in_no_output_phase = false;
        c.lookup_window = c.initial_lookup_window;
        c.lookup_batch_size = c.initial_lookup_batch_size;
        c.join_lookup_batch_size = 0;
        c.lookup_growth_progress = 0;
        c.lookup_outstanding_at_stop = 0;
        c.lookup_admission_blocked = AdmissionBlockStats::default();
        c.outer_changed = signal_channel();
        c.lookup_changed = signal_channel();
        (c.stop_tx, c.stop_rx) = stop_channel();
        if c.demand_rows == 0 {
            c.stop();
        }
    }

    /// Go `ReserveOuter`: waits until up to `max_rows` can be admitted to the
    /// join outer pipeline. `Ok((_, false))` after LIMIT completion; context
    /// cancellation is the error.
    pub fn reserve_outer(&self, ctx: &Context, max_rows: usize) -> Result<(usize, bool), CtxError> {
        if self.mode == AdaptiveLimitMode::DirectIndexLookup {
            return Ok((0, false));
        }
        self.reserve(ctx, max_rows, AdaptiveAdmissionStage::OuterRows)
    }

    /// Go `ReserveLookup`: bounds handles admitted to the double-read table
    /// lookup stage.
    pub fn reserve_lookup(&self, ctx: &Context, max_handles: usize) -> Result<(usize, bool), CtxError> {
        if !self.has_lookup_stage() {
            return match ctx.err() {
                Some(error) => Err(error),
                None => Ok((0, false)),
            };
        }
        self.reserve(ctx, max_handles, AdaptiveAdmissionStage::LookupHandles)
    }

    fn reserve(&self, ctx: &Context, max_units: usize, stage: AdaptiveAdmissionStage) -> Result<(usize, bool), CtxError> {
        if let Some(error) = ctx.err() {
            return Err(error);
        }
        if max_units == 0 {
            return Ok((0, true));
        }
        let mut waiting = false;
        loop {
            let mut c = self.lock();
            if let Some(error) = ctx.err() {
                if waiting {
                    c.end_admission_blocked(stage, Instant::now());
                }
                return Err(error);
            }
            if c.stop_tx.is_none() {
                if waiting {
                    c.end_admission_blocked(stage, Instant::now());
                }
                return Ok((0, false));
            }
            let window;
            let outstanding;
            let changed;
            let mut min_units = 1u64;
            if stage == AdaptiveAdmissionStage::OuterRows {
                window = c.outer_window;
                // Fetched-but-unconsumed rows and uncommitted reservations
                // both consume outer admission capacity.
                outstanding = c.outer_buffered_rows() + c.outer_reserved;
                changed = c.outer_changed.1.clone();
                // An in-flight task can free enough room for a full batch.
                // With no task to consume, admit a partial batch to guarantee
                // progress.
                if c.mode == AdaptiveLimitMode::IndexJoinOuterOnly && c.outer_in_no_output_growth && outstanding != 0 {
                    min_units = max_units as u64;
                }
            } else {
                window = c.lookup_physical_window();
                outstanding = c.lookup_reserved;
                changed = c.lookup_changed.1.clone();
            }
            if outstanding < window && window - outstanding >= min_units {
                if waiting {
                    c.end_admission_blocked(stage, Instant::now());
                }
                let mut units = (max_units as u64).min(window - outstanding);
                if stage == AdaptiveAdmissionStage::OuterRows {
                    c.outer_reserved += units;
                } else {
                    units = units.min(c.lookup_execution_batch_size());
                    c.lookup_reserved += units;
                }
                return Ok((units as usize, true));
            }
            if !waiting {
                c.begin_admission_blocked(stage, Instant::now());
                waiting = true;
            }
            let stop_rx = c.stop_rx.clone();
            drop(c);

            let done = ctx.done();
            crossbeam_channel::select! {
                recv(done) -> _ => {
                    self.lock().end_admission_blocked(stage, Instant::now());
                    return Err(ctx.err().unwrap_or(CtxError::Canceled));
                }
                recv(stop_rx) -> _ => {
                    self.lock().end_admission_blocked(stage, Instant::now());
                    if let Some(error) = ctx.err() {
                        return Err(error);
                    }
                    return Ok((0, false));
                }
                recv(changed) -> _ => {}
            }
        }
    }

    /// Go `CommitOuter`: converts an outer reservation into rows actually
    /// fetched.
    pub fn commit_outer(&self, reserved: usize, fetched: usize) {
        let mut c = self.lock();
        let released = (reserved as u64).min(c.outer_reserved);
        c.outer_reserved -= released;
        c.outer_fetched += (fetched as u64).min(released);
        notify(&c.outer_changed);
    }

    /// Go `ObserveJoinProgress`: updates input consumption and output
    /// production under one lock. Output produced before an outer row is
    /// fully consumed is retained until it can be paired with completed
    /// outer-row input.
    pub fn observe_join_progress(&self, consumed_rows: usize, output_rows: usize) {
        if consumed_rows == 0 && output_rows == 0 {
            return;
        }
        let mut c = self.lock();
        let previous_consumed = c.outer_consumed;
        if consumed_rows > 0 {
            c.outer_consumed = c.outer_fetched.min(c.outer_consumed + consumed_rows as u64);
        }
        let consumed = c.outer_consumed - previous_consumed;
        if output_rows > 0 {
            c.output_rows = saturating_add(c.output_rows, output_rows as u64);
        }
        if c.output_rows >= c.demand_rows {
            c.stop();
            return;
        }
        if output_rows > 0 && c.join_lookup_batch_size != 0 {
            // A match ends the temporary lookup batch increase even when the
            // current outer row still has matches to consume.
            c.join_lookup_batch_size = 0;
            if consumed == 0 {
                // Batch rounding can change lookup capacity without a yield
                // sample.
                notify(&c.lookup_changed);
            }
        }
        if consumed == 0 {
            if output_rows > 0 {
                c.pending_outer_output = saturating_add(c.pending_outer_output, output_rows as u64);
            }
            return;
        }
        let mut paired_output = c.pending_outer_output;
        if output_rows > 0 {
            paired_output = saturating_add(paired_output, output_rows as u64);
        }
        c.pending_outer_output = 0;
        if paired_output > 0 {
            c.recent_outer_yield.add(consumed, paired_output);
            c.outer_no_output_rows = 0;
            c.outer_in_no_output_growth = false;
            c.recompute_outer_window();
            c.recompute_lookup_window();
        } else {
            if c.outer_no_output_rows == 0 {
                c.recent_outer_yield = AdaptiveYieldWindow::default();
            }
            c.outer_no_output_rows = saturating_add(c.outer_no_output_rows, consumed);
            c.grow_outer_window_on_no_output();
        }
        notify(&c.outer_changed);
        notify(&c.lookup_changed);
    }

    /// Go `CompleteLookup`: releases a fully consumed lookup task and learns
    /// its handle-to-row yield. `reserved` is admission accounting; `handles`
    /// is the task's actual input. Callers report tasks in result-consumption
    /// order.
    pub fn complete_lookup(&self, reserved: usize, handles: usize, rows: usize) {
        if reserved == 0 {
            return;
        }
        let mut c = self.lock();
        if c.stop_tx.is_none() || reserved as u64 > c.lookup_reserved {
            return;
        }
        c.lookup_reserved -= reserved as u64;
        c.lookup_handles = saturating_add(c.lookup_handles, handles as u64);
        c.lookup_rows = saturating_add(c.lookup_rows, rows as u64);
        if c.mode == AdaptiveLimitMode::DirectIndexLookup {
            c.output_rows = saturating_add(c.output_rows, rows as u64);
            if c.output_rows >= c.demand_rows {
                c.stop();
                return;
            }
        }
        if rows > 0 {
            c.recent_lookup_yield.add(handles as u64, rows as u64);
            c.lookup_no_output_rows = 0;
            c.lookup_in_no_output_phase = false;
            c.recompute_lookup_window();
        } else {
            if !c.lookup_in_no_output_phase {
                c.recent_lookup_yield = AdaptiveYieldWindow::default();
            }
            c.lookup_in_no_output_phase = true;
            c.join_lookup_batch_size = 0;
            c.lookup_no_output_rows = saturating_add(c.lookup_no_output_rows, handles.max(1) as u64);
            c.grow_lookup_window_on_no_output();
        }
        notify(&c.lookup_changed);
    }

    /// Go `AbortLookup`: releases lookup admission without using the task as
    /// a yield sample (extraction errors, cancellation, undispatched work).
    pub fn abort_lookup(&self, handles: usize) {
        if handles == 0 {
            return;
        }
        let mut c = self.lock();
        c.lookup_reserved -= (handles as u64).min(c.lookup_reserved);
        notify(&c.lookup_changed);
    }

    /// Go `SuggestedBatchSize`: the current execution batch bounded by the
    /// caller's configured batch ceiling.
    pub fn suggested_batch_size(&self, ceiling: usize) -> usize {
        if ceiling < 1 {
            return 1;
        }
        let batch_size = self.lock().lookup_execution_batch_size();
        (batch_size.min(ceiling as u64) as usize).max(1).min(ceiling)
    }

    /// Go `Stop`: prevents future admission and wakes all blocked producers.
    pub fn stop(&self) {
        self.lock().stop();
    }

    /// Go `Snapshot`: the current counters for tests and diagnostics.
    pub fn snapshot(&self) -> AdaptiveLimitSnapshot {
        let c = self.lock();
        AdaptiveLimitSnapshot {
            has_lookup_stage: self.has_lookup_stage(),
            output_rows: c.output_rows,
            outer_fetched: c.outer_fetched,
            outer_consumed: c.outer_consumed,
            outer_reserved: c.outer_reserved,
            outer_window: c.outer_window,
            outer_outstanding_at_stop: c.outer_outstanding_at_stop,
            lookup_reserved: c.lookup_reserved,
            lookup_handles: c.lookup_handles,
            lookup_rows: c.lookup_rows,
            lookup_window: c.lookup_window,
            lookup_batch_size: c.lookup_execution_batch_size(),
            lookup_physical_window: c.lookup_physical_window(),
            lookup_outstanding_at_stop: c.lookup_outstanding_at_stop,
            outer_admission_blocked: c.admission_blocked_time(AdaptiveAdmissionStage::OuterRows),
            lookup_admission_blocked: c.admission_blocked_time(AdaptiveAdmissionStage::LookupHandles),
            stopped: c.stop_tx.is_none(),
        }
    }
}

impl State {
    fn block_stats(&mut self, stage: AdaptiveAdmissionStage) -> &mut AdmissionBlockStats {
        match stage {
            AdaptiveAdmissionStage::OuterRows => &mut self.outer_admission_blocked,
            AdaptiveAdmissionStage::LookupHandles => &mut self.lookup_admission_blocked,
        }
    }

    /// Go `beginAdmissionBlockedLocked`.
    fn begin_admission_blocked(&mut self, stage: AdaptiveAdmissionStage, now: Instant) {
        let stats = self.block_stats(stage);
        if stats.waiters == 0 {
            stats.blocked_since = Some(now);
        }
        stats.waiters += 1;
    }

    /// Go `endAdmissionBlockedLocked`.
    fn end_admission_blocked(&mut self, stage: AdaptiveAdmissionStage, now: Instant) {
        let stats = self.block_stats(stage);
        if stats.waiters == 0 {
            return;
        }
        stats.waiters -= 1;
        if stats.waiters == 0 {
            stats.blocked_time += since(stats.blocked_since.take(), now);
        }
    }

    /// Go `finishAdmissionBlockedLocked`.
    fn finish_admission_blocked(&mut self, stage: AdaptiveAdmissionStage, now: Instant) {
        let stats = self.block_stats(stage);
        if stats.waiters == 0 {
            return;
        }
        stats.blocked_time += since(stats.blocked_since.take(), now);
        stats.waiters = 0;
    }

    /// Go `recomputeOuterWindowLocked`.
    fn recompute_outer_window(&mut self) {
        // Estimate the input needed for the remaining LIMIT from cumulative
        // yield. The recent estimate can detect a low-yield region hidden by
        // earlier productive input; taking the larger estimate is
        // conservative for progress.
        let remaining_output = self.demand_rows - self.output_rows;
        let mut estimated_input =
            divide_and_round_up(saturating_multiply(remaining_output, self.outer_consumed), self.output_rows);
        let (recent_consumed, recent_output) = self.recent_outer_yield.totals();
        if recent_output > 0 {
            let recent_estimate =
                divide_and_round_up(saturating_multiply(remaining_output, recent_consumed), recent_output);
            estimated_input = estimated_input.max(recent_estimate);
        }
        let target = add_adaptive_window_headroom(estimated_input, remaining_output, self.demand_rows);
        let grew;
        (self.outer_window, grew) = adjust_adaptive_window(
            target,
            self.outer_window,
            1,
            self.max_outer_window,
            self.outer_consumed > self.outer_growth_barrier,
        );
        if grew {
            self.outer_growth_barrier = self.outer_fetched;
        }
    }

    /// Go `recomputeLookupWindowLocked`.
    fn recompute_lookup_window(&mut self) {
        if self.lookup_rows == 0 || self.lookup_in_no_output_phase {
            return;
        }
        let remaining_rows = if self.mode == AdaptiveLimitMode::DirectIndexLookup {
            self.demand_rows - self.demand_rows.min(self.output_rows)
        } else {
            let lookup_buffered = self.lookup_rows - self.lookup_rows.min(self.outer_consumed);
            // Use the larger visible buffer so rows already held by either
            // stage can satisfy the outer window without admitting the same
            // demand twice.
            let buffered_rows = lookup_buffered.max(self.outer_buffered_rows());
            self.outer_window - self.outer_window.min(buffered_rows)
        };
        let mut target =
            divide_and_round_up(saturating_multiply(remaining_rows, self.lookup_handles), self.lookup_rows);
        let (recent_handles, recent_rows) = self.recent_lookup_yield.totals();
        if recent_rows > 0 {
            let recent_estimate = divide_and_round_up(saturating_multiply(remaining_rows, recent_handles), recent_rows);
            target = target.max(recent_estimate);
        }
        // The Join path already includes headroom in its outer window.
        if self.mode == AdaptiveLimitMode::DirectIndexLookup {
            target = add_adaptive_window_headroom(target, remaining_rows, self.demand_rows);
        }
        // Only the logical window changes here. The execution batch is tracked
        // separately so shrinking the window does not cause tiny lookup RPCs.
        let grew;
        (self.lookup_window, grew) = adjust_adaptive_window(
            target,
            self.lookup_window,
            self.initial_lookup_window,
            self.max_lookup_window,
            self.lookup_handles > self.lookup_growth_progress,
        );
        if grew {
            self.lookup_growth_progress = self.lookup_handles;
        }
    }

    /// Go `outerBufferedRowsLocked`.
    fn outer_buffered_rows(&self) -> u64 {
        self.outer_fetched - self.outer_fetched.min(self.outer_consumed)
    }

    /// Go `growOuterWindowOnNoOutputLocked`.
    fn grow_outer_window_on_no_output(&mut self) {
        if self.outer_no_output_rows < self.outer_window {
            return;
        }
        let outstanding = self.outer_buffered_rows() + self.outer_reserved;
        if self.mode == AdaptiveLimitMode::IndexJoinOuterOnly {
            // Retry the first empty window at its original size in case
            // matches begin in the next batch. After that, use newly consumed
            // input to grow even when the producer keeps the window partially
            // occupied.
            if self.output_rows == 0 && self.outer_consumed == self.initial_outer_window && outstanding == 0 {
                self.outer_no_output_rows = 0;
                return;
            }
        } else {
            // If lookup is delivering input, consumed rows already prove that
            // the join is making progress through a low-yield interval.
            // Refilling the window must not prevent growth.
            let lookup_producing = self.lookup_rows > 0 && !self.lookup_in_no_output_phase;
            if outstanding != 0 && !lookup_producing {
                return;
            }
            if lookup_producing && self.outer_in_no_output_growth {
                self.join_lookup_batch_size =
                    grow_adaptive_window(self.lookup_execution_batch_size(), self.max_lookup_batch_size);
            }
        }
        self.outer_in_no_output_growth = true;
        let next_window = grow_adaptive_window(self.outer_window, self.max_outer_window);
        if next_window > self.outer_window {
            self.outer_growth_barrier = self.outer_fetched;
        }
        self.outer_window = next_window;
        self.outer_no_output_rows = 0;
        if !self.lookup_in_no_output_phase {
            self.recompute_lookup_window();
        }
    }

    /// Go `growLookupWindowOnNoOutputLocked`.
    fn grow_lookup_window_on_no_output(&mut self) {
        // Drain the initial probe before opening the fast-growth window. Once
        // fast growth is active, each window of completed zero-output work is
        // enough evidence to grow even if the producer keeps tasks in flight.
        let can_grow_with_in_flight = self.no_output_lookup_batch_size > self.initial_lookup_batch_size
            && self.lookup_batch_size >= self.no_output_lookup_batch_size;
        if self.lookup_no_output_rows < self.lookup_window || (self.lookup_reserved != 0 && !can_grow_with_in_flight) {
            return;
        }
        let next_window =
            grow_adaptive_window(self.lookup_window, self.max_lookup_window).max(self.no_output_lookup_window);
        if next_window > self.lookup_window {
            self.lookup_growth_progress = self.lookup_handles;
        }
        self.lookup_window = next_window;
        self.lookup_batch_size =
            grow_adaptive_window(self.lookup_batch_size, self.max_lookup_batch_size).max(self.no_output_lookup_batch_size);
        self.lookup_no_output_rows = 0;
    }

    /// Go `lookupPhysicalWindowLocked`: the logical handle budget rounded up
    /// to whole execution batches. Unless the configured maximum truncates
    /// it, physical slack is less than one batch.
    fn lookup_physical_window(&self) -> u64 {
        let batch_size = self.lookup_execution_batch_size();
        if self.lookup_window == 0 || batch_size == 0 {
            return 0;
        }
        let batch_count = divide_and_round_up(self.lookup_window, batch_size);
        saturating_multiply(batch_count, batch_size).min(self.max_lookup_window)
    }

    /// Go `lookupExecutionBatchSizeLocked`.
    fn lookup_execution_batch_size(&self) -> u64 {
        self.lookup_batch_size.max(self.join_lookup_batch_size)
    }

    /// Go `stopLocked`.
    fn stop(&mut self) {
        if self.stop_tx.is_none() {
            return;
        }
        let now = Instant::now();
        self.finish_admission_blocked(AdaptiveAdmissionStage::OuterRows, now);
        self.finish_admission_blocked(AdaptiveAdmissionStage::LookupHandles, now);
        self.outer_outstanding_at_stop = saturating_add(self.outer_buffered_rows(), self.outer_reserved);
        self.lookup_outstanding_at_stop = self.lookup_reserved;
        self.lookup_reserved = 0;
        self.outer_reserved = 0;
        self.outer_window = 0;
        self.lookup_window = 0;
        self.lookup_batch_size = 0;
        self.join_lookup_batch_size = 0;
        // Go `close(c.stopCh)`: dropping the only sender disconnects every
        // receiver.
        self.stop_tx = None;
    }

    /// Go `admissionBlockedTimeLocked`.
    fn admission_blocked_time(&self, stage: AdaptiveAdmissionStage) -> Duration {
        let stats = match stage {
            AdaptiveAdmissionStage::OuterRows => &self.outer_admission_blocked,
            AdaptiveAdmissionStage::LookupHandles => &self.lookup_admission_blocked,
        };
        if stats.waiters == 0 {
            return stats.blocked_time;
        }
        stats.blocked_time + since(stats.blocked_since, Instant::now())
    }
}

fn since(start: Option<Instant>, now: Instant) -> Duration {
    start.map_or(Duration::ZERO, |start| now.saturating_duration_since(start))
}

/// Go `notifyLocked`: a non-blocking send into a one-slot channel.
fn notify(channel: &(Sender<()>, Receiver<()>)) {
    let _ = channel.0.try_send(());
}

/// Go `normalizeAdaptiveWindow`.
fn normalize_adaptive_window(initial: u64, maximum: u64) -> (u64, u64) {
    let initial = initial.max(1);
    (initial, maximum.max(initial))
}

/// Go `growAdaptiveWindow`.
fn grow_adaptive_window(window: u64, maximum: u64) -> u64 {
    saturating_multiply(window, 2).min(maximum)
}

/// Go `addAdaptiveWindowHeadroom`: more headroom early in the statement,
/// tapering as LIMIT completion approaches to reduce tail over-admission.
fn add_adaptive_window_headroom(estimated_input: u64, remaining_output: u64, demand_rows: u64) -> u64 {
    if remaining_output <= demand_rows / 4 {
        estimated_input
    } else if remaining_output <= demand_rows / 2 {
        divide_and_round_up(saturating_multiply(estimated_input, 9), 8)
    } else {
        divide_and_round_up(saturating_multiply(estimated_input, 5), 4)
    }
}

/// Go `adjustAdaptiveWindow`: shrink immediately near LIMIT completion.
/// Growth requires new input progress and is capped at 2x so transient
/// feedback cannot open the window without bound in one adjustment.
fn adjust_adaptive_window(target: u64, current: u64, minimum: u64, maximum: u64, can_grow: bool) -> (u64, bool) {
    let target = target.max(minimum).min(maximum);
    if target > current {
        if !can_grow {
            return (current, false);
        }
        return (target.min(grow_adaptive_window(current, maximum)), true);
    }
    (target, false)
}

/// Go `divideAndRoundUp`.
fn divide_and_round_up(value: u64, divisor: u64) -> u64 {
    if divisor == 0 {
        return 0;
    }
    value / divisor + u64::from(value % divisor != 0)
}

/// Go `saturatingAdd`.
fn saturating_add(left: u64, right: u64) -> u64 {
    left.saturating_add(right)
}

/// Go `saturatingMultiply`.
fn saturating_multiply(left: u64, right: u64) -> u64 {
    left.saturating_mul(right)
}

#[cfg(test)]
#[path = "adaptive_limit_controller_test.rs"]
mod tests;
