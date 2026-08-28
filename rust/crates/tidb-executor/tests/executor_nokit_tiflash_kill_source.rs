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

//! Port ledger for the kill/interrupt trio of
//! `pkg/ddl/executor_nokit_test.go` (`pkg/ddl.part6` batch b105, items
//! 306-308 of the pkg/ddl enumeration).
//!
//! The three Go tests exercise the batch
//! `ALTER DATABASE ... SET TIFLASH REPLICA` abort path: `isSessionDone`
//! samples the session's SQLKiller, `convertKillFlag` maps the sampled flag
//! onto `exeerrors.ErrQueryInterrupted`, and
//! `executor.waitPendingTableThreshold` polls pending-TiFlash-table counts
//! until it can proceed, is killed, or times out into a forced schema sync.
//! The TiFlash-replica batch path is not transcreated in this workspace.
//! The killer PRIMITIVE the first test relies on DOES exist -- the session
//! memory quota's `StatementCancellation` sends `KillSignal::QueryInterrupted`
//! (crates/tidb-executor/src/mem_quota.rs:256) -- but no DDL carrier samples
//! it through Go's `isSessionDone` shape.

/// GO PORT of `pkg/ddl/executor_nokit_test.go:36
/// TestIsSessionDoneHandlesWrappedQueryInterrupted`.
///
/// Re-derived contract (pkg/ddl/executor.go:461-471): after
/// `SQLKiller.SendKillSignal(sqlkiller.QueryInterrupted)`, `isSessionDone`
/// answers `(done=true, killed=1)` -- the sentinel comparison
/// `exeerrors.ErrQueryInterrupted.Equal(HandleSignal())` collapses to the
/// flag, and the kill value handed upward is the literal 1. Without a
/// signal it answers `(false, 0)`.
#[test]
#[ignore = "go-parity-gap: isSessionDone (pkg/ddl/executor.go:461-471) and the DDL TiFlash batch path that calls it are not transcreated"]
fn is_session_done_reports_the_wrapped_query_interrupted_signal() {}

/// GO PORT of `pkg/ddl/executor_nokit_test.go:45 TestConvertKillFlag`.
///
/// Re-derived contract (pkg/ddl/executor.go:473-481): `convertKillFlag(0)`
/// returns nil -- a zero kill value is the failpoint-only abort and must NOT
/// surface as an error -- and `convertKillFlag(1)` returns
/// `exeerrors.ErrQueryInterrupted` (matched with testify's Error equality,
/// so any wrapped instance of the sentinel counts).
#[test]
#[ignore = "go-parity-gap: convertKillFlag (pkg/ddl/executor.go:473-481) belongs to the untranscreated batch ALTER ... SET TIFLASH REPLICA path"]
fn convert_kill_flag_maps_only_a_nonzero_flag_to_query_interrupted() {}

/// GO PORT of `pkg/ddl/executor_nokit_test.go:51
/// TestWaitPendingTableThresholdAbortsOnKill`.
///
/// Re-derived contract (pkg/ddl/executor.go:483-520, called with the SQL
/// killer pre-armed): with a pending count at/over the threshold the poller
/// would keep sleeping, but the kill signal makes the FIRST iteration
/// return `finished=true` with `forceCheck=false` and `killed=1`, and the
/// caller's `convertKillFlag(killed)` then yields
/// `exeerrors.ErrQueryInterrupted`. The kill short-circuits before any
/// wait: the test's argument list is `(sctx, schemaID=1, tableID=1,
/// originVersion=0, pendingCount=0, threshold=1)`.
#[test]
#[ignore = "go-parity-gap: executor.waitPendingTableThreshold (pkg/ddl/executor.go:483-520) and its getPendingTiFlashTableCount feed are not transcreated"]
fn wait_pending_table_threshold_aborts_before_waiting_when_killed() {}
