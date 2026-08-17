// Copyright 2021 PingCAP, Inc.
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

//! Transaction write-throughput SLI accounting.
//!
//! Transcreation of the whole Go package `pkg/util/sli` (`pkg/util/sli/sli.go`,
//! the package's only production file). Every exported Go symbol of that
//! package is present here:
//!
//! | Go (`pkg/util/sli/sli.go`) | Rust |
//! | --- | --- |
//! | `TxnWriteThroughputSLI` (L26) | [`TxnWriteThroughputSli`] |
//! | `FinishExecuteStmt` (L36) | [`TxnWriteThroughputSli::finish_execute_stmt`] |
//! | `AddReadKeys` (L61) | [`TxnWriteThroughputSli::add_read_keys`] |
//! | `AddTxnWriteSize` (L66) | [`TxnWriteThroughputSli::add_txn_write_size`] |
//! | `reportMetric` (L71) | [`TxnWriteThroughputSli::report_metric`] |
//! | `SetInvalid` (L81) | [`TxnWriteThroughputSli::set_invalid`] |
//! | `IsInvalid` (L88) | [`TxnWriteThroughputSli::is_invalid`] |
//! | `smallTxnAffectRow` / `smallTxnSize` (L93) | [`SMALL_TXN_AFFECT_ROW`] / [`SMALL_TXN_SIZE`] |
//! | `IsSmallTxn` (L98) | [`TxnWriteThroughputSli::is_small_txn`] |
//! | `Reset` (L103) | [`TxnWriteThroughputSli::reset`] |
//! | `String` (L113) | `impl Display for TxnWriteThroughputSli` |
//!
//! The SLI is a plain per-session accumulator: statements report how long they
//! took and how many rows they touched, the KV layer reports how many bytes and
//! keys were written, and when the transaction ends the collected numbers are
//! published as one of two histogram observations. Nothing here reaches into
//! the executor or the plan; it only adds and compares numbers.
//!
//! Tests below are TRANSCREATED from Go `TestTxnWriteThroughputSLI` in
//! `pkg/executor/executor_failpoint_test.go` (L595-L684). The Go package
//! `pkg/util/sli` has no `*_test.go` of its own; that executor test is the
//! upstream test that exercises this type, and its `require.Equal` fixtures on
//! `IsInvalid`/`IsSmallTxn`/`String()` are reproduced verbatim. The Go test
//! drives the accumulator through a testkit session, so the write sizes and key
//! counts that the session's KV layer feeds into `AddTxnWriteSize` are supplied
//! here as literals taken from the Go fixture strings (29 bytes per single-row
//! `insert`, 19 bytes per deleted row, matching every `writeSize` the Go test
//! asserts).

use std::fmt;
use std::time::Duration;

use crate::exec_details::format_go_duration;

/// Go: `smallTxnAffectRow` in `pkg/util/sli/sli.go` (L94).
pub const SMALL_TXN_AFFECT_ROW: u64 = 20;

/// Go: `smallTxnSize` in `pkg/util/sli/sli.go` (L95) — 1MB.
pub const SMALL_TXN_SIZE: i64 = 1024 * 1024;

/// One histogram observation published by
/// [`TxnWriteThroughputSli::report_metric`].
///
/// Go publishes into process-global Prometheus histograms
/// (`metrics.SmallTxnWriteDuration.Observe` and
/// `metrics.TxnWriteThroughput.Observe`, `pkg/util/sli/sli.go` L71-L79).
///
/// boundary: `metrics.SmallTxnWriteDuration` / `metrics.TxnWriteThroughput`
/// (`pkg/metrics/sli.go` L24-L46) — this crate has no Prometheus registry, so
/// the observation is returned to the caller instead of being written into a
/// global collector. Which histogram is selected and the exact observed float
/// are preserved; the registration/exposition side of the Go metric is not
/// modelled here.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SliObservation {
    /// `metrics.SmallTxnWriteDuration.Observe(t.writeTime.Seconds())`.
    SmallTxnWriteDuration(f64),
    /// `metrics.TxnWriteThroughput.Observe(writeSize / writeTime.Seconds())`.
    TxnWriteThroughput(f64),
}

/// Go: `TxnWriteThroughputSLI` (`pkg/util/sli/sli.go` L26-L33).
///
/// Reports transaction write throughput metrics for SLI.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TxnWriteThroughputSli {
    invalid: bool,
    affect_row: u64,
    write_size: i64,
    read_keys: i64,
    write_keys: i64,
    write_time: Duration,
    /// Stands in for `failpoint.Inject("CheckTxnWriteThroughput", ...)`
    /// (`pkg/util/sli/sli.go` L52-L54), which makes `FinishExecuteStmt` return
    /// before the end-of-transaction `Reset` so a test can inspect the
    /// accumulated numbers.
    ///
    /// boundary: `failpoint.Inject` — there is no global failpoint registry in
    /// this crate, so the switch is per-instance and must be set explicitly via
    /// [`TxnWriteThroughputSli::set_check_txn_write_throughput_failpoint`]. Its
    /// effect on `finish_execute_stmt` is identical to Go's.
    check_txn_write_throughput_failpoint: bool,
}

impl TxnWriteThroughputSli {
    /// A fresh accumulator, equivalent to Go's zero value.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Enables or disables the stand-in for the `CheckTxnWriteThroughput`
    /// failpoint. See [`TxnWriteThroughputSli::check_txn_write_throughput_failpoint`].
    pub fn set_check_txn_write_throughput_failpoint(&mut self, enabled: bool) {
        self.check_txn_write_throughput_failpoint = enabled;
    }

    /// Go: `FinishExecuteStmt` (`pkg/util/sli/sli.go` L36-L59).
    ///
    /// Records the cost of a write statement that affected rows, and, once the
    /// statement leaves the transaction (`in_txn == false`, i.e. the
    /// transaction just finished), publishes the metric and resets.
    ///
    /// Returns the observation Go would have pushed into Prometheus, or `None`
    /// when Go published nothing (still in a transaction, or the accumulated
    /// data is invalid).
    pub fn finish_execute_stmt(
        &mut self,
        cost: Duration,
        affect_row: u64,
        in_txn: bool,
    ) -> Option<SliObservation> {
        if affect_row > 0 {
            self.write_time += cost;
            self.affect_row += affect_row;
        }

        // Currently not in transaction means the last transaction is finish,
        // should report metrics and reset data.
        if in_txn {
            return None;
        }
        if affect_row == 0 {
            // AffectRows is 0 when statement is commit.
            self.write_time += cost;
        }
        // Report metrics after commit this transaction.
        let observation = self.report_metric();

        // Skip reset for test.
        if self.check_txn_write_throughput_failpoint {
            return observation;
        }

        // Reset for next transaction.
        self.reset();
        observation
    }

    /// Go: `AddReadKeys` (`pkg/util/sli/sli.go` L61-L63).
    pub fn add_read_keys(&mut self, read_keys: i64) {
        self.read_keys += read_keys;
    }

    /// Go: `AddTxnWriteSize` (`pkg/util/sli/sli.go` L66-L69).
    pub fn add_txn_write_size(&mut self, size: i64, keys: i64) {
        self.write_size += size;
        self.write_keys += keys;
    }

    /// Go: `reportMetric` (`pkg/util/sli/sli.go` L71-L79).
    #[must_use]
    pub fn report_metric(&self) -> Option<SliObservation> {
        if self.is_invalid() {
            return None;
        }
        let write_secs = self.write_time.as_secs_f64();
        if self.is_small_txn() {
            Some(SliObservation::SmallTxnWriteDuration(write_secs))
        } else {
            // `is_invalid` already rejected `write_time == 0`, so this division
            // matches Go's finite result.
            #[expect(clippy::cast_precision_loss, reason = "Go: float64(t.writeSize)")]
            Some(SliObservation::TxnWriteThroughput(
                self.write_size as f64 / write_secs,
            ))
        }
    }

    /// Go: `SetInvalid` (`pkg/util/sli/sli.go` L81-L83).
    pub fn set_invalid(&mut self) {
        self.invalid = true;
    }

    /// Go: `IsInvalid` (`pkg/util/sli/sli.go` L88-L90).
    ///
    /// The transaction cannot report SLI metrics when it contains an
    /// `insert|replace into ... select ... from ...` statement, or when the
    /// write statements read more keys than they wrote.
    #[must_use]
    pub fn is_invalid(&self) -> bool {
        self.invalid
            || self.read_keys > self.write_keys
            || self.write_size == 0
            || self.write_time.is_zero()
    }

    /// Go: `IsSmallTxn` (`pkg/util/sli/sli.go` L98-L100).
    #[must_use]
    pub fn is_small_txn(&self) -> bool {
        self.affect_row <= SMALL_TXN_AFFECT_ROW && self.write_size <= SMALL_TXN_SIZE
    }

    /// Go: `Reset` (`pkg/util/sli/sli.go` L103-L110).
    pub fn reset(&mut self) {
        self.invalid = false;
        self.affect_row = 0;
        self.write_size = 0;
        self.read_keys = 0;
        self.write_keys = 0;
        self.write_time = Duration::ZERO;
    }

    /// Accumulated affected rows, for callers that report session state.
    #[must_use]
    pub fn affect_row(&self) -> u64 {
        self.affect_row
    }

    /// Accumulated written bytes.
    #[must_use]
    pub fn write_size(&self) -> i64 {
        self.write_size
    }

    /// Accumulated read keys.
    #[must_use]
    pub fn read_keys(&self) -> i64 {
        self.read_keys
    }

    /// Accumulated written keys.
    #[must_use]
    pub fn write_keys(&self) -> i64 {
        self.write_keys
    }

    /// Accumulated write time.
    #[must_use]
    pub fn write_time(&self) -> Duration {
        self.write_time
    }
}

/// Go: `String` (`pkg/util/sli/sli.go` L113-L116).
impl fmt::Display for TxnWriteThroughputSli {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid: {}, affectRow: {}, writeSize: {}, readKeys: {}, writeKeys: {}, writeTime: {}",
            self.invalid,
            self.affect_row,
            self.write_size,
            self.read_keys,
            self.write_keys,
            format_go_duration(self.write_time),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND: Duration = Duration::from_secs(1);

    /// Row byte costs the Go testkit session feeds into `AddTxnWriteSize`,
    /// derived from the `writeSize` numbers asserted in Go
    /// `TestTxnWriteThroughputSLI`: 29 bytes per inserted/replaced row
    /// (58 = 29*2, 609 = 29*21, 116 = 29*4, 29 = 29*1) and 19 bytes per
    /// deleted row (76 = 19*4).
    const INSERT_ROW_SIZE: i64 = 29;
    const DELETE_ROW_SIZE: i64 = 19;

    /// Go: `mustExec` closure (`executor_failpoint_test.go` L610-L613) — the
    /// session hands the KV write footprint to the SLI, then finishes the
    /// statement with a one-second cost.
    fn must_exec(
        sli: &mut TxnWriteThroughputSli,
        write_size: i64,
        write_keys: i64,
        read_keys: i64,
        affect_row: u64,
        in_txn: bool,
    ) {
        sli.add_txn_write_size(write_size, write_keys);
        sli.add_read_keys(read_keys);
        sli.finish_execute_stmt(SECOND, affect_row, in_txn);
    }

    /// Go: `TestTxnWriteThroughputSLI` L629-L635 — `insert into t values
    /// (1,3),(2,4)` in a small autocommit transaction.
    #[test]
    fn insert_in_small_txn() {
        let mut sli = TxnWriteThroughputSli::new();
        sli.set_check_txn_write_throughput_failpoint(true);

        must_exec(&mut sli, 2 * INSERT_ROW_SIZE, 2, 0, 2, false);

        assert!(!sli.is_invalid());
        assert!(sli.is_small_txn());
        assert_eq!(
            sli.to_string(),
            "invalid: false, affectRow: 2, writeSize: 58, readKeys: 0, writeKeys: 2, writeTime: 1s"
        );
    }

    /// Go: `TestTxnWriteThroughputSLI` L637-L642 — `insert into t select b, a
    /// from t`, which the session marks invalid via `SetInvalid` and which also
    /// records the keys it read.
    #[test]
    fn insert_select_from_is_invalid() {
        let mut sli = TxnWriteThroughputSli::new();
        sli.set_check_txn_write_throughput_failpoint(true);

        sli.set_invalid();
        must_exec(&mut sli, 2 * INSERT_ROW_SIZE, 2, 2, 2, false);

        assert!(sli.is_invalid());
        assert!(sli.is_small_txn());
        assert_eq!(
            sli.to_string(),
            "invalid: true, affectRow: 2, writeSize: 58, readKeys: 2, writeKeys: 2, writeTime: 1s"
        );
    }

    /// Go: `TestTxnWriteThroughputSLI` L644-L647 — `delete from t`.
    #[test]
    fn delete_rows() {
        let mut sli = TxnWriteThroughputSli::new();
        sli.set_check_txn_write_throughput_failpoint(true);

        must_exec(&mut sli, 4 * DELETE_ROW_SIZE, 4, 4, 4, false);

        assert_eq!(
            sli.to_string(),
            "invalid: false, affectRow: 4, writeSize: 76, readKeys: 4, writeKeys: 4, writeTime: 1s"
        );
    }

    /// Go: `TestTxnWriteThroughputSLI` L649-L663 — an explicit transaction with
    /// 21 single-row inserts plus two zero-affect-row `select`s, which must not
    /// contribute to `writeTime`.
    #[test]
    fn insert_not_in_small_txn() {
        let mut sli = TxnWriteThroughputSli::new();
        sli.set_check_txn_write_throughput_failpoint(true);

        // `begin`: affects 0 rows and stays in the transaction.
        must_exec(&mut sli, 0, 0, 0, 0, true);
        for _ in 0..20 {
            must_exec(&mut sli, INSERT_ROW_SIZE, 1, 0, 1, true);
            assert!(sli.is_small_txn());
        }
        // The statement which affect rows is 0 shouldn't record into time.
        must_exec(&mut sli, 0, 0, 0, 0, true);
        must_exec(&mut sli, 0, 0, 0, 0, true);
        must_exec(&mut sli, INSERT_ROW_SIZE, 1, 0, 1, true);
        assert!(!sli.is_small_txn());

        // `commit`: affects 0 rows and leaves the transaction, so its own cost
        // is still charged and the metric is reported.
        let observation = sli.finish_execute_stmt(SECOND, 0, false);

        assert!(!sli.is_invalid());
        assert_eq!(
            sli.to_string(),
            "invalid: false, affectRow: 21, writeSize: 609, readKeys: 0, writeKeys: 21, \
             writeTime: 22s"
        );
        assert_eq!(
            observation,
            Some(SliObservation::TxnWriteThroughput(609.0 / 22.0))
        );
    }

    /// Go: `TestTxnWriteThroughputSLI` L665-L674 — a transaction containing
    /// `replace ... select ... from ...`, which explicitly marks the SLI
    /// invalid even though its read keys do not exceed its write keys.
    #[test]
    fn replace_select_from_marks_invalid() {
        let mut sli = TxnWriteThroughputSli::new();
        sli.set_check_txn_write_throughput_failpoint(true);

        must_exec(&mut sli, 0, 0, 0, 0, true); // begin
        must_exec(&mut sli, 2 * INSERT_ROW_SIZE, 2, 0, 2, true);
        sli.set_invalid();
        must_exec(&mut sli, 2 * INSERT_ROW_SIZE, 2, 0, 2, true);
        let observation = sli.finish_execute_stmt(SECOND, 0, false); // commit

        assert!(sli.is_invalid());
        assert_eq!(
            sli.to_string(),
            "invalid: true, affectRow: 4, writeSize: 116, readKeys: 0, writeKeys: 4, writeTime: 3s"
        );
        assert_eq!(observation, None);
    }

    /// Go: `TestTxnWriteThroughputSLI` L676-L682 — with the failpoint disabled,
    /// a failed `commit` still ends the transaction, so the accumulator is
    /// reported and reset.
    #[test]
    fn failed_commit_resets_when_failpoint_disabled() {
        let mut sli = TxnWriteThroughputSli::new();

        must_exec(&mut sli, 0, 0, 0, 0, true); // begin
        must_exec(&mut sli, 2 * INSERT_ROW_SIZE, 2, 0, 2, true);
        sli.finish_execute_stmt(SECOND, 0, false); // commit, which errored

        assert_eq!(
            sli.to_string(),
            "invalid: false, affectRow: 0, writeSize: 0, readKeys: 0, writeKeys: 0, writeTime: 0s"
        );
    }

    /// Go: `TestTxnWriteThroughputSLI` L684-L688 — after the failed
    /// transaction the accumulator is clean, so the next transaction reports
    /// only its own numbers.
    #[test]
    fn next_txn_after_reset_is_clean() {
        let mut sli = TxnWriteThroughputSli::new();
        sli.set_check_txn_write_throughput_failpoint(true);

        must_exec(&mut sli, 0, 0, 0, 0, true); // begin
        must_exec(&mut sli, INSERT_ROW_SIZE, 1, 0, 1, true);
        let observation = sli.finish_execute_stmt(SECOND, 0, false); // commit

        assert_eq!(
            sli.to_string(),
            "invalid: false, affectRow: 1, writeSize: 29, readKeys: 0, writeKeys: 1, writeTime: 2s"
        );
        assert_eq!(
            observation,
            Some(SliObservation::SmallTxnWriteDuration(2.0))
        );
    }

    /// Go: `TestTxnWriteThroughputSLI` L690-L692 — explicit `Reset`.
    #[test]
    fn reset_clears_every_field() {
        let mut sli = TxnWriteThroughputSli::new();
        sli.set_check_txn_write_throughput_failpoint(true);
        sli.set_invalid();
        must_exec(&mut sli, INSERT_ROW_SIZE, 1, 3, 1, false);

        sli.reset();

        assert_eq!(
            sli.to_string(),
            "invalid: false, affectRow: 0, writeSize: 0, readKeys: 0, writeKeys: 0, writeTime: 0s"
        );
        assert!(!sli.is_invalid_flag_set());
    }

    impl TxnWriteThroughputSli {
        fn is_invalid_flag_set(&self) -> bool {
            self.invalid
        }
    }

    /// `IsInvalid` rejects a transaction whose read keys exceed its write keys,
    /// whose write size is zero, or whose write time is zero
    /// (`pkg/util/sli/sli.go` L88-L90).
    #[test]
    fn is_invalid_covers_every_disqualifier() {
        let mut sli = TxnWriteThroughputSli::new();
        assert!(sli.is_invalid()); // zero write size and zero write time

        sli.add_txn_write_size(10, 1);
        assert!(sli.is_invalid()); // zero write time

        sli.finish_execute_stmt(SECOND, 1, true);
        assert!(!sli.is_invalid());

        sli.add_read_keys(2);
        assert!(sli.is_invalid()); // read keys exceed write keys

        sli.add_txn_write_size(0, 2);
        assert!(!sli.is_invalid());

        sli.set_invalid();
        assert!(sli.is_invalid());
    }

    /// `IsSmallTxn` boundaries (`pkg/util/sli/sli.go` L98-L100): both limits are
    /// inclusive.
    #[test]
    fn is_small_txn_boundaries() {
        let mut sli = TxnWriteThroughputSli::new();
        sli.set_check_txn_write_throughput_failpoint(true);

        sli.add_txn_write_size(SMALL_TXN_SIZE, 1);
        sli.finish_execute_stmt(SECOND, SMALL_TXN_AFFECT_ROW, true);
        assert!(sli.is_small_txn());

        sli.add_txn_write_size(1, 0);
        assert!(!sli.is_small_txn());

        sli.reset();
        sli.finish_execute_stmt(SECOND, SMALL_TXN_AFFECT_ROW + 1, true);
        assert!(!sli.is_small_txn());
    }

    /// `reportMetric` picks the small-transaction histogram when the
    /// transaction is small (`pkg/util/sli/sli.go` L71-L79).
    #[test]
    fn report_metric_selects_histogram() {
        let mut sli = TxnWriteThroughputSli::new();
        sli.set_check_txn_write_throughput_failpoint(true);
        sli.add_txn_write_size(100, 1);
        sli.finish_execute_stmt(Duration::from_secs(2), 1, true);
        assert_eq!(
            sli.report_metric(),
            Some(SliObservation::SmallTxnWriteDuration(2.0))
        );

        sli.finish_execute_stmt(Duration::from_secs(2), SMALL_TXN_AFFECT_ROW, true);
        assert_eq!(
            sli.report_metric(),
            Some(SliObservation::TxnWriteThroughput(100.0 / 4.0))
        );
    }
}
