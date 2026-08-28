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

//! Gap tests for the TopSQL/TopRU statement-profiling hooks of Go
//! `pkg/executor/adapter_internal_test.go`. Only the global state toggles are
//! transcreated (`tidb-util::topsql_state`: `enable_top_sql`/`enable_top_ru`
//! /`top_ru_enabled`); the `ExecStmt` hooks
//! (`pkg/executor/adapter.go:2407 observeStmtBeginForTopProfiling`,
//! `:2501 observeStmtFinishedForTopProfiling`), the mock TopSQL collector and
//! the `stmtstats.StatementStats` ledger they feed are all unported, so each
//! Go contract below is documented rather than executed.

/// Go `pkg/executor/adapter_internal_test.go:253::TestObserveStmtBeginOnTopProfiling`:
/// with TopRU enabled and a mock collector installed via
/// `topsql.SetupTopProfilingForTest`, `observeStmtBeginForTopProfiling`
/// registers the statement: the collector later maps the statement's SQL
/// digest to `sc.SQLDigest()`'s normalized SQL and the plan digest to
/// `sc.SetPlanDigest`'s normalized plan
/// (`TableReader(table:t)->Selection(eq(test.t.a, ?))`).
#[test]
#[ignore = "go-parity-gap: observeStmtBeginForTopProfiling (pkg/executor/adapter.go:2407), the mock TopSQL collector and the plan-digest plumbing are unported; only state toggles exist (tidb-util::topsql_state)"]
fn observe_stmt_begin_on_top_profiling_registers_sql_and_plan() {}

/// Go `pkg/executor/adapter_internal_test.go:288::TestObserveStmtBeginOnTopProfilingRUV2Wiring`,
/// two subtests: with a mock domain whose resource-group controller reports
/// `RUVersionV2`, begin-profiling writes into the session's statement stats a
/// single `stmtstats.RUKey` entry (user + binary SQL/plan digests) with
/// `ExecCount == 1` and `TotalRU == RUV2Metrics.TotalRU(weights, 0, 0)` for 3
/// plan-count units; with a nil domain the default RU version yields
/// `ExecCount == 1` and `TotalRU == 0`.
#[test]
#[ignore = "go-parity-gap: stmtstats.StatementStats ledger, the resource-group RUVersionV2 controller and domain binding (adapter_internal_test.go:288) are unported"]
fn observe_stmt_begin_on_top_profiling_ru_v2_wiring() {}

/// Go `pkg/executor/adapter_internal_test.go:333::TestObserveStmtFinishedOnTopProfiling`.
/// Flow: begin(TopRU on) -> TopRU disabled -> finish -> re-enable -> stats
/// read before any new begin. The finished statement reports `ExecCount == 1`
/// with `TotalRU == 0` (its RU exec context was cleared at finish while
/// profiling was off), and a later `ru.Merge(5)` must not leak a positive
/// delta -- i.e. the stale baseline is really gone
/// (`pkg/executor/adapter.go:2501`).
#[test]
#[ignore = "go-parity-gap: observeStmtFinishedForTopProfiling (pkg/executor/adapter.go:2501) and util.RUDetails context tracking are unported"]
fn observe_stmt_finished_on_top_profiling_clears_stale_exec_context() {}

/// Go `pkg/executor/adapter_internal_test.go:368::TestObserveStmtFinishedOnTopProfilingDoes`.
/// Flow: begin(on) -> disable -> finish -> begin(off, same key) -> re-enable
/// -> finish(on). The begin that ran while TopRU was disabled must not
/// create or reuse an RU exec context, so the final stats still hold exactly
/// `ExecCount == 1` (from the first begin) and no RU delta from the stale
/// baseline merged in between.
#[test]
#[ignore = "go-parity-gap: same unported surface as the begin/finish hooks (pkg/executor/adapter.go:2407/:2501) plus the TopRU toggle windows they key on"]
fn observe_stmt_finished_on_top_profiling_does_not_reuse_stale_baseline() {}

/// Go `pkg/executor/adapter_internal_test.go:407::TestObserveStmtFinishedOnTopProfilingKeeps`:
/// with TopSQL enabled but TopRU off, begin+finish still records duration
/// stats: `ExecCount == 1`, `DurationCount == 1`, `SumDurationNs > 0` (the
/// session start time was backdated one second) and the stored
/// `OutPacketBytes == 123` surfaced as `NetworkOutBytes`.
#[test]
#[ignore = "go-parity-gap: TopSQL-only finish stats (OutPacketBytes/StartTime accounting in pkg/executor/adapter.go:2501) are unported"]
fn observe_stmt_finished_on_top_profiling_keeps_top_sql_only_stats() {}

/// Go `pkg/executor/adapter_internal_test.go:433::TestObserveStmtFinishedOnTopProfilingIgnores`:
/// a Go context whose `util.RUDetailsCtxKey` value is not a `*util.RUDetails`
/// (here the string "bad-type") must not panic at finish; the stats still
/// show `ExecCount == 1` with a zero RU delta, keeping TopRU sampling stable.
#[test]
#[ignore = "go-parity-gap: no Go-context RUDetails type-assertion path exists here (the hook itself, pkg/executor/adapter.go:2501, is unported)"]
fn observe_stmt_finished_on_top_profiling_ignores_bad_ru_details_type() {}
