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

//! SEED port of the dependency-closed decisions in `pkg/executor/adapter.go`.
//!
//! `ExecStmt` itself (`pkg/executor/adapter.go:356-390`) is a handle onto a
//! live session: `sessionctx.Context`, `infoschema.InfoSchema`, `base.Plan`,
//! `ast.StmtNode`, `plannercore.PlanCacheStmt`. None of those have a Rust seam
//! in this crate, so the struct is not reproduced. What *is* dependency-closed
//! is the bookkeeping it drives, and that is what this module ports:
//!
//! * `ExecStmt.LogSlowQuery`'s gating cascade
//!   (`pkg/executor/adapter.go:1948-1981`) and its post-write metric routing
//!   (`:2002-2020`) — pure decisions over already-ported inputs.
//! * `ExecStmt.SummaryStmt`'s trigger conditions
//!   (`pkg/executor/adapter.go:2189-2225`).
//! * `ExecStmt.FinishExecuteStmt`'s accounting
//!   (`pkg/executor/adapter.go:1659-1756`): retry-count accumulation, the
//!   scan-detail SLI/keys-examined rules, and the fair-locking counters.
//! * The phase-duration ledger: `resetPhaseDurations`
//!   (`pkg/executor/adapter.go:1533`) and `observePhaseDurations`'s
//!   `duration > 0` filter (`:1609`).
//! * `formatSQL` / `QueryReplacer` (`pkg/executor/adapter.go:1544, 1581`).
//!
//! The gating inputs are supplied as already-evaluated booleans/durations
//! rather than being read from a session, because that read is the boundary.

use core::time::Duration;

// ---------------------------------------------------------------------------
// LogSlowQuery
// ---------------------------------------------------------------------------

/// Everything `LogSlowQuery` consults before deciding to emit a slow log.
///
/// Go: `pkg/executor/adapter.go:1948-1981`.
#[derive(Clone, Copy, Debug, Default)]
pub struct SlowLogGate {
    /// Go: `stmtCtx.WriteSlowLog`. When set, the whole threshold/rules block is
    /// skipped and the log is written unconditionally (subject to rate
    /// limiting), but `matchRules` stays false.
    pub write_slow_log: bool,
    /// Go: `log.GetLevel() <= zapcore.DebugLevel || trace.IsEnabled()`.
    pub force: bool,
    /// Go: `cfg.Instance.EnableSlowLog.Load()`.
    pub enable_slow_log: bool,
    /// Go: `len(sessVars.SlowLogRules.EffectiveFields) != 0`.
    pub has_effective_fields: bool,
    /// Go: `ShouldWriteSlowLog(globalRules, sessVars, slowItems)`. Consulted
    /// only when `has_effective_fields` is set; see
    /// [`crate::slow_log_match::should_write_slow_log`] for the already-ported
    /// rule evaluation this stands for.
    pub rules_matched: bool,
    /// Go: `sessVars.GetTotalCostDuration()`.
    pub total_cost: Duration,
    /// Go: `time.Duration(atomic.LoadUint64(&cfg.Instance.SlowThreshold)) * time.Millisecond`.
    pub slow_threshold: Duration,
    /// Go: `vardef.GlobalSlowLogRateLimiter.Allow()`.
    pub rate_limiter_allow: bool,
}

/// What `LogSlowQuery` does for a given gate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlowLogAction {
    /// Go: one of the two early `return`s before the rate limiter — the
    /// statement is neither slow nor forced.
    Skip,
    /// Go: `!vardef.GlobalSlowLogRateLimiter.Allow()` — an info line is logged
    /// about the drop and nothing else happens.
    SkipRateLimited,
    /// Go: the slow log line is written.
    Write {
        /// Go's `matchRules`. When false the function returns right after
        /// writing the line, so the histograms, the slow-query counters, and
        /// `domain.LogSlowQuery` are all skipped.
        match_rules: bool,
    },
}

/// Decides whether `LogSlowQuery` writes, and whether the write also feeds the
/// slow-query metrics and the domain's slow-query recorder.
///
/// Go: `ExecStmt.LogSlowQuery` at `pkg/executor/adapter.go:1948`.
///
/// Three behaviours worth naming, all preserved:
///
/// * `stmtCtx.WriteSlowLog` bypasses every threshold check but leaves
///   `matchRules` false, so such a statement is logged without being counted;
/// * `force` (debug logging or tracing) overrides both the instance switch and
///   the threshold/rules verdict, again without setting `matchRules`;
/// * the rate limiter is checked *after* the gate, so a forced or
///   `WriteSlowLog` statement can still be dropped by it.
#[must_use]
pub fn decide_log_slow_query(gate: SlowLogGate) -> SlowLogAction {
    let mut match_rules = false;
    if !gate.write_slow_log {
        if !gate.enable_slow_log && !gate.force {
            return SlowLogAction::Skip;
        }
        match_rules = if gate.has_effective_fields {
            gate.rules_matched
        } else {
            gate.total_cost >= gate.slow_threshold
        };
        if !match_rules && !gate.force {
            return SlowLogAction::Skip;
        }
    }
    if !gate.rate_limiter_allow {
        return SlowLogAction::SkipRateLimited;
    }
    SlowLogAction::Write { match_rules }
}

/// The slow-query metric family a statement is charged to.
///
/// Go: `pkg/executor/adapter.go:2007-2020`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlowQueryMetricFamily {
    /// Go: the `*Internal` observers, taken when `sessVars.InRestrictedSQL`.
    Internal,
    /// Go: the `*General` observers.
    General,
}

/// The metric updates a logged slow query performs.
///
/// Go: `pkg/executor/adapter.go:2007-2020`. Only the general branch computes
/// the coprocessor MVCC ratio, and only when a scan detail exists with a
/// non-zero processed-key count (which is also what keeps the division safe).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SlowQueryMetrics {
    /// Which observer family is charged.
    pub family: SlowQueryMetricFamily,
    /// Go: `CopMVCCRatioHistogramGeneral.Observe(TotalKeys / ProcessedKeys)`.
    pub cop_mvcc_ratio: Option<f64>,
}

/// Routes a logged slow query's metrics.
///
/// Go: `pkg/executor/adapter.go:2007-2020`. `scan_detail` carries
/// `(TotalKeys, ProcessedKeys)`; `None` models Go's nil `ExecDetail.ScanDetail`.
#[must_use]
pub fn slow_query_metrics(
    in_restricted_sql: bool,
    scan_detail: Option<(i64, i64)>,
) -> SlowQueryMetrics {
    if in_restricted_sql {
        return SlowQueryMetrics {
            family: SlowQueryMetricFamily::Internal,
            cop_mvcc_ratio: None,
        };
    }
    let cop_mvcc_ratio = match scan_detail {
        Some((total_keys, processed_keys)) if processed_keys != 0 =>
        {
            #[expect(
                clippy::cast_precision_loss,
                reason = "Go computes float64(TotalKeys) / float64(ProcessedKeys)"
            )]
            Some(total_keys as f64 / processed_keys as f64)
        }
        _ => None,
    };
    SlowQueryMetrics {
        family: SlowQueryMetricFamily::General,
        cop_mvcc_ratio,
    }
}

// boundary: `PrepareSlowLogItemsForRules`, `SetSlowLogItems`,
// `sessVars.SlowLogFormat`, and `domain.LogSlowQuery` all read a live
// `SessionVars`. The formatting half is already ported as
// `crate::slow_log_format`; wiring it needs the session seam.

// ---------------------------------------------------------------------------
// SummaryStmt
// ---------------------------------------------------------------------------

/// The statement kinds `SummaryStmt` special-cases.
///
/// Go: `pkg/executor/adapter.go:2204, 2217`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SummaryStmtKind {
    /// Go: `*ast.PrepareStmt` — skipped; `EXECUTE` is recorded instead.
    Prepare,
    /// Go: `*ast.CommitStmt` — recorded, but only when a previous statement
    /// digest exists to attribute it to.
    Commit,
    /// Any other statement.
    Other,
}

/// Everything `SummaryStmt` consults before recording.
///
/// Go: `pkg/executor/adapter.go:2189-2225`.
#[derive(Clone, Debug)]
pub struct SummaryGate {
    /// Go: `sessVars.InRestrictedSQL`.
    pub in_restricted_sql: bool,
    /// Go: `sessVars.User.Username`, empty when `sessVars.User == nil`.
    pub user_name: String,
    /// Go: `sessVars.InExplainExplore`.
    pub in_explain_explore: bool,
    /// Go: `stmtsummaryv2.Enabled()`.
    pub summary_enabled: bool,
    /// Go: `stmtsummaryv2.EnabledInternal()`.
    pub summary_internal_enabled: bool,
    /// The statement's kind.
    pub stmt_kind: SummaryStmtKind,
    /// Go: `sessVars.GetPrevStmtDigest()`.
    pub prev_stmt_digest: String,
}

/// What `SummaryStmt` does for a given gate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SummaryAction {
    /// Go: summary disabled for this statement — `SetPrevStmtDigest("")` then
    /// return. Note this is the *only* skip path that clears the digest.
    ClearPrevDigestAndSkip,
    /// Go: `*ast.PrepareStmt` returns without touching the previous digest.
    SkipPrepare,
    /// Go: a `COMMIT` seen before any digest was recorded is ignored, again
    /// without touching the previous digest.
    SkipCommitWithoutPrevDigest,
    /// Go: the statement is recorded via `stmtsummaryv2.Add`, and the previous
    /// statement digest is replaced by this statement's digest.
    Record {
        /// Go's `isInternalSQL`, stored on `StmtExecInfo.IsInternal`.
        is_internal: bool,
        /// Go: the previous statement digest a `COMMIT` is attributed to;
        /// `None` for every other statement kind.
        attributed_prev_digest: Option<String>,
    },
}

/// Whether a statement counts as internal for statement-summary purposes.
///
/// Go: `pkg/executor/adapter.go:2198`. A statement with no user name counts as
/// internal just like restricted SQL, but `EXPLAIN EXPLORE` forces it back to
/// user-visible so its inner statements land in the summary.
#[must_use]
pub fn is_internal_sql(in_restricted_sql: bool, user_name: &str, in_explain_explore: bool) -> bool {
    (in_restricted_sql || user_name.is_empty()) && !in_explain_explore
}

/// Decides whether `SummaryStmt` records this statement.
///
/// Go: `ExecStmt.SummaryStmt` at `pkg/executor/adapter.go:2189`.
#[must_use]
pub fn decide_summary_stmt(gate: &SummaryGate) -> SummaryAction {
    let is_internal = is_internal_sql(
        gate.in_restricted_sql,
        &gate.user_name,
        gate.in_explain_explore,
    );
    if !gate.summary_enabled || (is_internal && !gate.summary_internal_enabled) {
        return SummaryAction::ClearPrevDigestAndSkip;
    }
    if gate.stmt_kind == SummaryStmtKind::Prepare {
        return SummaryAction::SkipPrepare;
    }
    let attributed_prev_digest = if gate.stmt_kind == SummaryStmtKind::Commit {
        if gate.prev_stmt_digest.is_empty() {
            return SummaryAction::SkipCommitWithoutPrevDigest;
        }
        Some(gate.prev_stmt_digest.clone())
    } else {
        None
    };
    SummaryAction::Record {
        is_internal,
        attributed_prev_digest,
    }
}

// boundary: the `StmtExecInfo` population (`pkg/executor/adapter.go:2258-2302`)
// reads ~30 session/stmt-context fields (`stmtCtx.SQLDigest`,
// `stmtCtx.CopTasksSummary`, `sessVars.MemTracker`, `GetPlanDigest`,
// `keyspace.GetKeyspaceNameBySettings`, `calculateStatementTotalRUV2`, ...).
// The summary sink itself lives in the `tidb-stmtsummary` crate; only the
// trigger conditions above are dependency-closed today.

// ---------------------------------------------------------------------------
// FinishExecuteStmt accounting
// ---------------------------------------------------------------------------

/// The scan-detail driven accounting `FinishExecuteStmt` performs.
///
/// Go: `pkg/executor/adapter.go:1678-1687`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ScanKeyAccounting {
    /// Go: `GetTxnWriteThroughputSLI().AddReadKeys(processedKeys)`, done only
    /// for statements that affected at least one row.
    pub sli_read_keys: i64,
    /// Go: `sessVars.KeysExamined += uint64(processedKeys)`, done regardless of
    /// affected rows.
    pub keys_examined_delta: u64,
}

/// Computes the read-key accounting for a finished statement.
///
/// Go: `pkg/executor/adapter.go:1678-1687`. A non-positive processed-key count
/// contributes nothing at all; a positive one always advances
/// `KeysExamined` but only feeds the write-throughput SLI when the statement
/// affected rows, since that SLI tracks reads performed by write statements.
#[must_use]
pub fn scan_key_accounting(processed_keys: i64, affected_rows: u64) -> ScanKeyAccounting {
    if processed_keys <= 0 {
        return ScanKeyAccounting::default();
    }
    ScanKeyAccounting {
        sli_read_keys: if affected_rows > 0 { processed_keys } else { 0 },
        #[expect(
            clippy::cast_sign_loss,
            reason = "Go converts a checked-positive int64 with uint64(processedKeys)"
        )]
        keys_examined_delta: processed_keys as u64,
    }
}

/// The fair-locking counters a finished statement bumps.
///
/// Go: `pkg/executor/adapter.go:1734-1753`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FairLockingCounters {
    /// Go: `FairLockingStmtUsedCount`.
    pub stmt_used: bool,
    /// Go: `FairLockingStmtEffectiveCount` — only ever set when `stmt_used` is.
    pub stmt_effective: bool,
    /// Go: `FairLockingTxnUsedCount`, gated on a non-nil commit detail.
    pub txn_used: bool,
    /// Go: `FairLockingTxnEffectiveCount`, likewise.
    pub txn_effective: bool,
}

/// The lock-keys detail fields the fair-locking counters read.
///
/// Go: `util.LockKeysDetails`.
#[derive(Clone, Copy, Debug, Default)]
pub struct LockKeysCounts {
    /// Go: `AggressiveLockNewCount`.
    pub aggressive_lock_new: i32,
    /// Go: `AggressiveLockDerivedCount`.
    pub aggressive_lock_derived: i32,
    /// Go: `LockedWithConflictCount`.
    pub locked_with_conflict: i32,
}

/// Computes the fair-locking counter updates for a finished statement.
///
/// Go: `pkg/executor/adapter.go:1734-1753`. The transaction-level counters are
/// bumped only when the statement carried a commit detail, i.e. only on the
/// statement that actually committed the transaction.
#[must_use]
pub fn fair_locking_counters(
    lock_keys: Option<LockKeysCounts>,
    has_commit_detail: bool,
    txn_fair_locking_used: bool,
    txn_fair_locking_effective: bool,
) -> FairLockingCounters {
    let mut counters = FairLockingCounters::default();
    if let Some(lock_keys) = lock_keys {
        if lock_keys.aggressive_lock_new > 0 || lock_keys.aggressive_lock_derived > 0 {
            counters.stmt_used = true;
            counters.stmt_effective =
                lock_keys.locked_with_conflict > 0 || lock_keys.aggressive_lock_derived > 0;
        }
    }
    if has_commit_detail {
        counters.txn_used = txn_fair_locking_used;
        counters.txn_effective = txn_fair_locking_effective;
    }
    counters
}

// boundary: the rest of `FinishExecuteStmt` (`pkg/executor/adapter.go:1659`)
// is session mutation and metric emission through
// `checkPlanReplayerCapture`, `RuntimeStatsColl.RegisterStats`,
// `GetTxnWriteThroughputSLI`, `finalizeStatementRUV2Metrics`,
// `updateNetworkTrafficStatsAndMetrics`, `observeStmtFinishedForTopProfiling`,
// `UpdatePlanCacheRuntimeInfo`, `updatePrevStmt`, `recordLastQueryInfo`, and
// `Ctx.ReportUsageStats`.

// ---------------------------------------------------------------------------
// Phase durations
// ---------------------------------------------------------------------------

/// One phase's two-part duration ledger.
///
/// Go: the `[2]time.Duration` fields on `ExecStmt`
/// (`pkg/executor/adapter.go:381-384`). Index 0 accumulates within the current
/// pessimistic-retry iteration; index 1 accumulates the iterations that were
/// abandoned after a lock conflict.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PhaseDuration {
    /// Go: index 0 — the final (current) iteration.
    pub final_iteration: Duration,
    /// Go: index 1 — the durations rolled up from abandoned iterations.
    pub locking_iterations: Duration,
}

impl PhaseDuration {
    /// Rolls the current iteration into the locking total and resets it.
    ///
    /// Go: the per-field body of `resetPhaseDurations`
    /// (`pkg/executor/adapter.go:1533`).
    pub fn reset(&mut self) {
        self.locking_iterations = self.locking_iterations.saturating_add(self.final_iteration);
        self.final_iteration = Duration::ZERO;
    }
}

/// The four phase ledgers an `ExecStmt` keeps.
///
/// Go: `pkg/executor/adapter.go:381-384`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PhaseDurations {
    /// Go: `phaseBuildDurations`.
    pub build: PhaseDuration,
    /// Go: `phaseOpenDurations`.
    pub open: PhaseDuration,
    /// Go: `phaseNextDurations`.
    pub next: PhaseDuration,
    /// Go: `phaseLockDurations`.
    pub lock: PhaseDuration,
}

impl PhaseDurations {
    /// Go: `ExecStmt.resetPhaseDurations` (`pkg/executor/adapter.go:1533`),
    /// called when a pessimistic lock error sends the statement back around the
    /// retry loop.
    pub fn reset(&mut self) {
        self.build.reset();
        self.open.reset();
        self.next.reset();
        self.lock.reset();
    }

    /// The phase observations `observePhaseDurations` emits, in Go's order.
    ///
    /// Go: `pkg/executor/adapter.go:1609-1628`. Zero durations are filtered
    /// out, so an unused phase never touches its histogram.
    #[must_use]
    pub fn observations(&self) -> Vec<(&'static str, Duration)> {
        [
            ("build_final", self.build.final_iteration),
            ("build_locking", self.build.locking_iterations),
            ("open_final", self.open.final_iteration),
            ("open_locking", self.open.locking_iterations),
            ("next_final", self.next.final_iteration),
            ("next_locking", self.next.locking_iterations),
            ("lock_final", self.lock.final_iteration),
            ("lock_locking", self.lock.locking_iterations),
        ]
        .into_iter()
        .filter(|(_, duration)| *duration > Duration::ZERO)
        .collect()
    }
}

// boundary: `getPhaseDurationObserver` (`pkg/executor/adapter.go:1596`) maps a
// phase name to a Prometheus observer through
// `executor_metrics.PhaseDurationObserverMap[Internal]`, falling back to
// `ExecUnknown`. The phase-name constants above are the map keys; the registry
// itself is not a seam this crate owns.

// ---------------------------------------------------------------------------
// formatSQL
// ---------------------------------------------------------------------------

/// Replaces the characters that would break a grepped log line.
///
/// Go: `QueryReplacer` at `pkg/executor/adapter.go:1544`, a
/// `strings.NewReplacer("\r", " ", "\n", " ", "\t", " ")`.
#[must_use]
pub fn query_replace(sql: &str) -> String {
    sql.replace(['\r', '\n', '\t'], " ")
}

/// Truncates a SQL text for logging and flattens its whitespace.
///
/// Go: `formatSQL` at `pkg/executor/adapter.go:1581`, with the max length taken
/// from `vardef.QueryLogMaxLen`. A non-positive limit disables truncation; when
/// the text is longer than the limit it is cut and the *original* length is
/// appended as `(len:N)`.
///
/// Narrowing: Go truncates by byte index, which can split a multi-byte
/// character. This port cuts at the nearest character boundary at or below the
/// limit so the result stays valid UTF-8; the appended length is still the
/// original byte length, matching Go's `len(sql)`.
#[must_use]
pub fn format_sql(sql: &str, max_query_len: i32) -> String {
    if max_query_len <= 0 {
        return query_replace(sql);
    }
    let length = sql.len();
    #[expect(
        clippy::cast_sign_loss,
        reason = "max_query_len is checked positive above"
    )]
    let limit = max_query_len as usize;
    if length <= limit {
        return query_replace(sql);
    }
    let mut cut = limit;
    while cut > 0 && !sql.is_char_boundary(cut) {
        cut -= 1;
    }
    query_replace(&format!("{}(len:{length})", &sql[..cut]))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: Duration = Duration::from_millis(1);

    fn threshold_gate() -> SlowLogGate {
        SlowLogGate {
            enable_slow_log: true,
            rate_limiter_allow: true,
            slow_threshold: 300 * MS,
            ..SlowLogGate::default()
        }
    }

    #[test]
    fn disabled_slow_log_skips_unless_forced() {
        let gate = SlowLogGate {
            enable_slow_log: false,
            total_cost: 10 * MS.saturating_mul(1000),
            ..threshold_gate()
        };
        assert_eq!(decide_log_slow_query(gate), SlowLogAction::Skip);
        let forced = SlowLogGate {
            force: true,
            ..gate
        };
        assert_eq!(
            decide_log_slow_query(forced),
            SlowLogAction::Write { match_rules: true }
        );
    }

    #[test]
    fn threshold_is_inclusive() {
        let gate = SlowLogGate {
            total_cost: 300 * MS,
            ..threshold_gate()
        };
        assert_eq!(
            decide_log_slow_query(gate),
            SlowLogAction::Write { match_rules: true }
        );
        let below = SlowLogGate {
            total_cost: 299 * MS,
            ..threshold_gate()
        };
        assert_eq!(decide_log_slow_query(below), SlowLogAction::Skip);
    }

    #[test]
    fn effective_fields_replace_the_threshold_entirely() {
        // A statement far under the threshold is logged when the rules match.
        let gate = SlowLogGate {
            has_effective_fields: true,
            rules_matched: true,
            total_cost: MS,
            ..threshold_gate()
        };
        assert_eq!(
            decide_log_slow_query(gate),
            SlowLogAction::Write { match_rules: true }
        );
        // And a statement far over it is not, when the rules do not match.
        let gate = SlowLogGate {
            has_effective_fields: true,
            rules_matched: false,
            total_cost: 10 * MS.saturating_mul(1000),
            ..threshold_gate()
        };
        assert_eq!(decide_log_slow_query(gate), SlowLogAction::Skip);
    }

    #[test]
    fn forced_write_below_threshold_does_not_match_rules() {
        // `force` bypasses the skip but leaves `matchRules` false, so the
        // histograms and `domain.LogSlowQuery` are not reached.
        let gate = SlowLogGate {
            force: true,
            total_cost: MS,
            ..threshold_gate()
        };
        assert_eq!(
            decide_log_slow_query(gate),
            SlowLogAction::Write { match_rules: false }
        );
    }

    #[test]
    fn write_slow_log_flag_bypasses_gate_without_matching_rules() {
        let gate = SlowLogGate {
            write_slow_log: true,
            enable_slow_log: false,
            total_cost: Duration::ZERO,
            ..threshold_gate()
        };
        assert_eq!(
            decide_log_slow_query(gate),
            SlowLogAction::Write { match_rules: false }
        );
    }

    #[test]
    fn rate_limiter_is_checked_after_the_gate() {
        let gate = SlowLogGate {
            write_slow_log: true,
            rate_limiter_allow: false,
            ..threshold_gate()
        };
        assert_eq!(decide_log_slow_query(gate), SlowLogAction::SkipRateLimited);
        // A statement that never passed the gate is reported as Skip, not as
        // rate limited.
        let gate = SlowLogGate {
            rate_limiter_allow: false,
            total_cost: MS,
            ..threshold_gate()
        };
        assert_eq!(decide_log_slow_query(gate), SlowLogAction::Skip);
    }

    #[test]
    fn internal_slow_query_has_no_mvcc_ratio() {
        let metrics = slow_query_metrics(true, Some((100, 10)));
        assert_eq!(metrics.family, SlowQueryMetricFamily::Internal);
        assert!(metrics.cop_mvcc_ratio.is_none());
    }

    #[test]
    fn general_slow_query_computes_mvcc_ratio() {
        let metrics = slow_query_metrics(false, Some((100, 10)));
        assert_eq!(metrics.family, SlowQueryMetricFamily::General);
        assert!((metrics.cop_mvcc_ratio.expect("ratio") - 10.0).abs() < f64::EPSILON);
    }

    #[test]
    fn zero_processed_keys_skips_the_mvcc_ratio() {
        assert!(slow_query_metrics(false, Some((100, 0)))
            .cop_mvcc_ratio
            .is_none());
        assert!(slow_query_metrics(false, None).cop_mvcc_ratio.is_none());
    }

    fn summary_gate() -> SummaryGate {
        SummaryGate {
            in_restricted_sql: false,
            user_name: "root".to_owned(),
            in_explain_explore: false,
            summary_enabled: true,
            summary_internal_enabled: false,
            stmt_kind: SummaryStmtKind::Other,
            prev_stmt_digest: String::new(),
        }
    }

    #[test]
    fn missing_user_counts_as_internal() {
        assert!(is_internal_sql(false, "", false));
        assert!(is_internal_sql(true, "root", false));
        assert!(!is_internal_sql(false, "root", false));
    }

    #[test]
    fn explain_explore_forces_internal_sql_back_to_user_visible() {
        assert!(!is_internal_sql(true, "", true));
    }

    #[test]
    fn disabled_summary_clears_prev_digest() {
        let gate = SummaryGate {
            summary_enabled: false,
            ..summary_gate()
        };
        assert_eq!(
            decide_summary_stmt(&gate),
            SummaryAction::ClearPrevDigestAndSkip
        );
    }

    #[test]
    fn internal_sql_needs_the_internal_switch() {
        let gate = SummaryGate {
            in_restricted_sql: true,
            ..summary_gate()
        };
        assert_eq!(
            decide_summary_stmt(&gate),
            SummaryAction::ClearPrevDigestAndSkip
        );
        let enabled = SummaryGate {
            summary_internal_enabled: true,
            ..gate
        };
        assert_eq!(
            decide_summary_stmt(&enabled),
            SummaryAction::Record {
                is_internal: true,
                attributed_prev_digest: None,
            }
        );
    }

    #[test]
    fn prepare_is_skipped_without_clearing_the_digest() {
        let gate = SummaryGate {
            stmt_kind: SummaryStmtKind::Prepare,
            prev_stmt_digest: "abc".to_owned(),
            ..summary_gate()
        };
        assert_eq!(decide_summary_stmt(&gate), SummaryAction::SkipPrepare);
    }

    #[test]
    fn commit_without_prev_digest_is_ignored() {
        let gate = SummaryGate {
            stmt_kind: SummaryStmtKind::Commit,
            ..summary_gate()
        };
        assert_eq!(
            decide_summary_stmt(&gate),
            SummaryAction::SkipCommitWithoutPrevDigest
        );
    }

    #[test]
    fn commit_is_attributed_to_the_previous_statement() {
        let gate = SummaryGate {
            stmt_kind: SummaryStmtKind::Commit,
            prev_stmt_digest: "abc".to_owned(),
            ..summary_gate()
        };
        assert_eq!(
            decide_summary_stmt(&gate),
            SummaryAction::Record {
                is_internal: false,
                attributed_prev_digest: Some("abc".to_owned()),
            }
        );
    }

    #[test]
    fn non_positive_processed_keys_account_for_nothing() {
        assert_eq!(scan_key_accounting(0, 5), ScanKeyAccounting::default());
        assert_eq!(scan_key_accounting(-1, 5), ScanKeyAccounting::default());
    }

    #[test]
    fn read_keys_feed_the_sli_only_for_row_affecting_statements() {
        assert_eq!(
            scan_key_accounting(7, 0),
            ScanKeyAccounting {
                sli_read_keys: 0,
                keys_examined_delta: 7,
            }
        );
        assert_eq!(
            scan_key_accounting(7, 1),
            ScanKeyAccounting {
                sli_read_keys: 7,
                keys_examined_delta: 7,
            }
        );
    }

    #[test]
    fn fair_locking_stmt_counters_need_an_aggressive_lock() {
        let counters = fair_locking_counters(Some(LockKeysCounts::default()), false, true, true);
        assert_eq!(counters, FairLockingCounters::default());
    }

    #[test]
    fn derived_locks_are_both_used_and_effective() {
        let counters = fair_locking_counters(
            Some(LockKeysCounts {
                aggressive_lock_derived: 1,
                ..LockKeysCounts::default()
            }),
            false,
            false,
            false,
        );
        assert!(counters.stmt_used);
        assert!(counters.stmt_effective);
    }

    #[test]
    fn new_locks_without_conflict_are_used_but_not_effective() {
        let counters = fair_locking_counters(
            Some(LockKeysCounts {
                aggressive_lock_new: 1,
                ..LockKeysCounts::default()
            }),
            false,
            false,
            false,
        );
        assert!(counters.stmt_used);
        assert!(!counters.stmt_effective);
    }

    #[test]
    fn txn_fair_locking_counters_require_a_commit_detail() {
        let without = fair_locking_counters(None, false, true, true);
        assert!(!without.txn_used);
        assert!(!without.txn_effective);
        let with = fair_locking_counters(None, true, true, true);
        assert!(with.txn_used);
        assert!(with.txn_effective);
    }

    #[test]
    fn reset_rolls_the_current_iteration_into_the_locking_total() {
        let mut durations = PhaseDurations {
            build: PhaseDuration {
                final_iteration: 5 * MS,
                locking_iterations: 2 * MS,
            },
            ..PhaseDurations::default()
        };
        durations.reset();
        assert_eq!(durations.build.final_iteration, Duration::ZERO);
        assert_eq!(durations.build.locking_iterations, 7 * MS);
        durations.reset();
        assert_eq!(durations.build.locking_iterations, 7 * MS);
    }

    #[test]
    fn only_non_zero_phases_are_observed() {
        let durations = PhaseDurations {
            build: PhaseDuration {
                final_iteration: MS,
                locking_iterations: Duration::ZERO,
            },
            next: PhaseDuration {
                final_iteration: Duration::ZERO,
                locking_iterations: 2 * MS,
            },
            ..PhaseDurations::default()
        };
        assert_eq!(
            durations.observations(),
            vec![("build_final", MS), ("next_locking", 2 * MS)]
        );
    }

    #[test]
    fn format_sql_flattens_whitespace() {
        assert_eq!(format_sql("select\n1\tfrom\rt", 0), "select 1 from t");
    }

    #[test]
    fn non_positive_limit_disables_truncation() {
        let sql = "select 1";
        assert_eq!(format_sql(sql, -1), sql);
        assert_eq!(format_sql(sql, 0), sql);
    }

    #[test]
    fn over_limit_sql_is_truncated_and_annotated_with_original_length() {
        assert_eq!(format_sql("select 1 from t", 6), "select(len:15)");
    }

    #[test]
    fn exactly_at_limit_is_not_truncated() {
        assert_eq!(format_sql("select", 6), "select");
    }
}
