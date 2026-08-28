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

//! Ports of the `pkg/ddl/executor_nokit_test.go` family (part6 items 306–313
//! of the package's `func Test*`/`func Benchmark*` declarations, sorted by
//! file and line), read from `origin/master`.
//!
//! The Go tests exercise unexported executor helpers: the kill-flag plumbing
//! (`isSessionDone`, `convertKillFlag`, `waitPendingTableThreshold`) and the
//! DDL-cancel error classifier, plus the CREATE-TABLE job merger. The kill
//! plumbing and the classifier are not transcreated in this workspace; the
//! job-merger family (`buildQueryStringFromJobs`, `mergeCreateTableJobs`,
//! `mergeCreateTableJobsOfSameSchema`, `isUndroppableTable`) IS transcreated
//! — in the `tidb-exec` crate (`src/ddl_job_merge.rs`), which depends on this
//! crate and therefore cannot be exercised from here; its upstream ports live
//! beside it and are verified out-of-gate (see the b105 receipt). Each Go
//! test's disposition is recorded below; nothing is approximated.

use tidb_util::sqlkiller::{KillSignal, SqlKiller};

/// The expressible INPUT half of
/// TestIsSessionDoneHandlesWrappedQueryInterrupted
/// (pkg/ddl/executor_nokit_test.go:36): Go sends
/// `sqlkiller.QueryInterrupted` through the session's SQLKiller and requires
/// `isSessionDone` to report `(done=true, killed=1)`. This tier transcreates
/// the killer itself (tidb-util::sqlkiller) but not the executor wrapper, so
/// the port pins the signal round-trip the wrapper must read: sending
/// `QueryInterrupted` makes the signal observable, and its Go numeric value
/// is 1 — the `killed` count `isSessionDone` returns.
#[test]
fn is_session_done_input_signal_roundtrip_reads_query_interrupted_as_one() {
    let killer = SqlKiller::default();
    assert!(
        killer.get_kill_signal().is_none(),
        "a fresh session has no kill signal"
    );
    killer.send_kill_signal(KillSignal::QueryInterrupted);
    // Go `sqlkiller.QueryInterrupted` == 1; `isSessionDone` returns exactly
    // this value as its `killed` result (pkg/ddl/executor_nokit_test.go:36).
    assert_eq!(killer.get_kill_signal(), Some(KillSignal::QueryInterrupted));
    assert_eq!(KillSignal::QueryInterrupted as u32, 1);
}

// The OUTPUT half of the same Go test: `isSessionDone(sctx)` -> (true, 1)
// for a killed session (and (false, 0) otherwise).
//
// go-parity-gap: `isSessionDone` (pkg/ddl/executor.go) is not transcreated.
#[test]
#[ignore = "go-parity-gap: isSessionDone (pkg/ddl/executor.go) is not transcreated"]
fn is_session_done_maps_a_killed_session_to_done_and_one() {
    // Contract (pkg/ddl/executor_nokit_test.go:36-43): after
    // SQLKiller.SendKillSignal(QueryInterrupted), done==true and killed==1.
}

// --- TestConvertKillFlag (pkg/ddl/executor_nokit_test.go:45) ---
//
// Go requires `convertKillFlag(0)` to be a no-op error and
// `convertKillFlag(1)` to equal `exeerrors.ErrQueryInterrupted`.
//
// go-parity-gap: `convertKillFlag` is not transcreated; this tier carries no
// DDL-statement kill-flag conversion.
#[test]
#[ignore = "go-parity-gap: convertKillFlag (pkg/ddl/executor.go) is not transcreated"]
fn convert_kill_flag_maps_one_to_query_interrupted() {
    // Contract (pkg/ddl/executor_nokit_test.go:45-49): flag 0 -> Ok;
    // flag 1 -> ErrQueryInterrupted.
}

// --- TestWaitPendingTableThresholdAbortsOnKill
//     (pkg/ddl/executor_nokit_test.go:51) ---
//
// Go requires `(&executor{}).waitPendingTableThreshold` on a killed session
// to return `finished=true, forceCheck=false, killed=1`, and the killed flag
// to convert to `ErrQueryInterrupted`.
//
// go-parity-gap: the pending-table-threshold wait loop is not transcreated.
#[test]
#[ignore = "go-parity-gap: executor.waitPendingTableThreshold is not transcreated"]
fn wait_pending_table_threshold_aborts_on_kill() {
    // Contract (pkg/ddl/executor_nokit_test.go:51-60): with
    // QueryInterrupted armed, the wait reports finished with killed=1 and no
    // forced check.
}

// --- TestIsRetryableDDLCancelErr (pkg/ddl/executor_nokit_test.go:62) ---
//
// Go requires `isRetryableDDLCancelErr` to accept only the transient
// "mock failed admin command on ddl jobs" processJobs failure and to reject
// `ErrCancelFinishedDDLJob`, `ErrCannotCancelDDLJob` and `ErrDDLJobNotFound`
// — wrapped or not — as non-retryable.
//
// go-parity-gap: the classifier (and the DDL job-table admin path whose
// failures it judges) is not transcreated.
#[test]
#[ignore = "go-parity-gap: isRetryableDDLCancelErr (pkg/ddl/executor.go) is not transcreated"]
fn is_retryable_ddl_cancel_err_classifies_only_transient_failures() {
    // Contract (pkg/ddl/executor_nokit_test.go:62-81): finished/cannot-cancel/
    // not-found are terminal (wrapped or not); a transient processJobs error
    // is retryable.
}

// --- TestBuildQueryStringFromJobs / TestMergeCreateTableJobsOfSameSchema /
//     TestMergeCreateTableJobs / TestIsUndroppableTable
//     (pkg/ddl/executor_nokit_test.go:83 / :126 / :146 / :279) ---
//
// Already transcreated WITH their upstream tests in `tidb-exec`
// (`src/ddl_job_merge.rs`: `build_query_string_from_jobs`,
// `merge_create_table_jobs_of_same_schema`, `merge_create_table_jobs`,
// `is_undroppable_table` plus a `tests` module mirroring every Go case,
// including the max-batch-size-8 split). `tidb-exec` depends on THIS crate,
// so the tests cannot be mirrored here without a dependency cycle; they run
// under `-p tidb-exec`, which the b105 receipt records as an out-of-gate
// verification. No duplicate Rust test is registered for them in this gate.
