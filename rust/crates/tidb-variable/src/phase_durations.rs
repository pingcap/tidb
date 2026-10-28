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

//! The planning-phase durations Go keeps on `SessionVars`
//! (`pkg/sessionctx/variable/session.go`): `RewritePhaseInfo`,
//! `DurationOptimizer` and `DurationWaitTS`, which the slow log prints as
//! `Rewrite_time`, `Optimize_time` (with its `Opt_*` breakdown) and
//! `Wait_TS`.
//!
//! They are SESSION values, as in Go: the planner and the transaction layer
//! overwrite them while a statement runs, and nothing but
//! `ResetContextOfStmt` (which zeroes `DurationWaitTS`) and
//! `buildLogicalPlan` (which resets `RewritePhaseInfo`) clears them. A
//! statement that is never optimized -- a plan-cache hit, a `SET` -- shows
//! what the previous optimized statement left, exactly as Go's slow log does.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Go `RewritePhaseInfo`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RewritePhaseInfo {
    /// Go `DurationRewrite`.
    pub duration_rewrite: Duration,
    /// Go `DurationPreprocessSubQuery`.
    pub duration_preprocess_sub_query: Duration,
    /// Go `PreprocessSubQueries`.
    pub preprocess_sub_queries: u64,
}

/// Go `DurationOptimizer`: the optimizer's phase durations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DurationOptimizer {
    /// Go `Total`.
    pub total: Duration,
    /// Go `LogicalOpt`.
    pub logical_opt: Duration,
    /// Go `PhysicalOpt`.
    pub physical_opt: Duration,
    /// Go `BindingMatch`.
    pub binding_match: Duration,
    /// Go `StatsSyncWait`.
    pub stats_sync_wait: Duration,
    /// Go `StatsDerive`.
    pub stats_derive: Duration,
}

/// The values [`SessionPhaseDurations`] holds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PhaseDurations {
    /// Go `SessionVars.RewritePhaseInfo`.
    pub rewrite_phase_info: RewritePhaseInfo,
    /// Go `SessionVars.DurationOptimizer`.
    pub duration_optimizer: DurationOptimizer,
    /// Go `SessionVars.DurationWaitTS`.
    pub duration_wait_ts: Duration,
}

#[derive(Debug, Default)]
struct State {
    durations: PhaseDurations,
    /// Go `optimize`'s `beginOpt` while `optimizeStarted`: set when the
    /// statement's first optimization round begins, spent when its plan is
    /// ready.
    begin_optimize: Option<Instant>,
    /// Go `physicalOptimize`'s `begin`, taken before `RecursiveDeriveStats`.
    /// This port derives statistics at the end of the logical pass and runs
    /// the physical search in a separate call, so the start is handed from
    /// one to the other.
    begin_physical_opt: Option<Instant>,
}

/// A session's [`PhaseDurations`], shared with every statement context and
/// snapshot the session's statements use.
#[derive(Clone, Debug, Default)]
pub struct SessionPhaseDurations(Arc<Mutex<State>>);

impl SessionPhaseDurations {
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The values in force.
    #[must_use]
    pub fn snapshot(&self) -> PhaseDurations {
        self.state().durations
    }

    /// Go `ResetContextOfStmt`: `vars.DurationWaitTS = 0`. The optimizer
    /// clocks a failed statement left running are abandoned too, as Go's
    /// `optimize` frame ends with the statement.
    pub fn reset_for_statement(&self) {
        let mut state = self.state();
        state.durations.duration_wait_ts = Duration::ZERO;
        state.begin_optimize = None;
        state.begin_physical_opt = None;
    }

    /// Go `buildLogicalPlan`: `RewritePhaseInfo.Reset()` before the build.
    pub fn reset_rewrite_phase_info(&self) {
        self.state().durations.rewrite_phase_info = RewritePhaseInfo::default();
    }

    /// Go `buildLogicalPlan`: `RewritePhaseInfo.DurationRewrite =
    /// time.Since(beginRewrite)` after `builder.Build`.
    pub fn set_duration_rewrite(&self, duration: Duration) {
        self.state().durations.rewrite_phase_info.duration_rewrite = duration;
    }

    /// Go `EvalSubqueryFirstRow`'s deferred accounting: one more
    /// pre-processed subquery and the time it ran.
    pub fn add_preprocess_sub_query(&self, duration: Duration) {
        let mut state = self.state();
        let info = &mut state.durations.rewrite_phase_info;
        info.preprocess_sub_queries += 1;
        info.duration_preprocess_sub_query += duration;
    }

    /// Go `buildAndOptimizeLogicalPlanRound`: `if !*optimizeStarted {
    /// *beginOpt = time.Now() }` before the first `DoOptimize`.
    pub fn begin_optimize(&self) {
        self.state().begin_optimize.get_or_insert_with(Instant::now);
    }

    /// Go `optimize`'s deferred `DurationOptimizer.Total =
    /// time.Since(beginOpt)` when the optimization started.
    pub fn finish_optimize(&self) {
        let mut state = self.state();
        if let Some(begin) = state.begin_optimize.take() {
            state.durations.duration_optimizer.total = begin.elapsed();
        }
    }

    /// Go `logicalOptimize`'s deferred `DurationOptimizer.LogicalOpt`.
    pub fn set_logical_opt(&self, duration: Duration) {
        self.state().durations.duration_optimizer.logical_opt = duration;
    }

    /// Go `physicalOptimize`: `DurationOptimizer.StatsDerive =
    /// time.Since(begin)` after `RecursiveDeriveStats`, and `begin` handed to
    /// the physical search that follows.
    pub fn finish_stats_derive(&self, begin_physical_opt: Instant) {
        let mut state = self.state();
        state.durations.duration_optimizer.stats_derive = begin_physical_opt.elapsed();
        state.begin_physical_opt = Some(begin_physical_opt);
    }

    /// The start [`Self::finish_stats_derive`] handed over, if the physical
    /// search now beginning follows a statistics derivation.
    #[must_use]
    pub fn take_begin_physical_opt(&self) -> Option<Instant> {
        self.state().begin_physical_opt.take()
    }

    /// Go `physicalOptimize`'s deferred `DurationOptimizer.PhysicalOpt`.
    pub fn set_physical_opt(&self, duration: Duration) {
        self.state().durations.duration_optimizer.physical_opt = duration;
    }

    /// Go `matchSQLBindingCore`'s deferred `DurationOptimizer.BindingMatch`.
    pub fn set_binding_match(&self, duration: Duration) {
        self.state().durations.duration_optimizer.binding_match = duration;
    }

    /// Go `SyncWaitStatsLoad`'s deferred `DurationOptimizer.StatsSyncWait`.
    pub fn set_stats_sync_wait(&self, duration: Duration) {
        self.state().durations.duration_optimizer.stats_sync_wait = duration;
    }

    /// Go `LazyTxn.Wait`: `DurationWaitTS = time.Since(begin)` once the
    /// pending transaction is valid.
    pub fn set_duration_wait_ts(&self, duration: Duration) {
        self.state().durations.duration_wait_ts = duration;
    }

    /// Go's isolation providers: `DurationWaitTS += time.Since(start)` after
    /// a statement or for-update timestamp future is waited.
    pub fn add_duration_wait_ts(&self, duration: Duration) {
        self.state().durations.duration_wait_ts += duration;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn optimize_total_is_only_recorded_for_a_started_optimization() {
        let durations = SessionPhaseDurations::default();
        durations.finish_optimize();
        assert_eq!(
            durations.snapshot().duration_optimizer.total,
            Duration::ZERO
        );
        durations.begin_optimize();
        std::thread::sleep(Duration::from_millis(2));
        // A later round does not move the start.
        durations.begin_optimize();
        durations.finish_optimize();
        let total = durations.snapshot().duration_optimizer.total;
        assert!(total >= Duration::from_millis(2), "{total:?}");
        // The value outlives the statement until another optimization.
        durations.reset_for_statement();
        durations.finish_optimize();
        assert_eq!(durations.snapshot().duration_optimizer.total, total);
    }

    #[test]
    fn rewrite_reset_clears_subquery_counts_and_wait_ts_resets_per_statement() {
        let durations = SessionPhaseDurations::default();
        durations.add_preprocess_sub_query(Duration::from_nanos(5));
        durations.add_preprocess_sub_query(Duration::from_nanos(7));
        durations.set_duration_wait_ts(Duration::from_nanos(3));
        durations.add_duration_wait_ts(Duration::from_nanos(4));
        let snapshot = durations.snapshot();
        assert_eq!(snapshot.rewrite_phase_info.preprocess_sub_queries, 2);
        assert_eq!(
            snapshot.rewrite_phase_info.duration_preprocess_sub_query,
            Duration::from_nanos(12)
        );
        assert_eq!(snapshot.duration_wait_ts, Duration::from_nanos(7));
        durations.reset_rewrite_phase_info();
        durations.reset_for_statement();
        let snapshot = durations.snapshot();
        assert_eq!(snapshot.rewrite_phase_info, RewritePhaseInfo::default());
        assert_eq!(snapshot.duration_wait_ts, Duration::ZERO);
    }

    #[test]
    fn physical_opt_start_is_handed_over_once() {
        let durations = SessionPhaseDurations::default();
        let begin = Instant::now();
        durations.finish_stats_derive(begin);
        assert_eq!(durations.take_begin_physical_opt(), Some(begin));
        assert_eq!(durations.take_begin_physical_opt(), None);
    }
}
