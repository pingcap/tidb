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

//! Gap tests for Go `pkg/executor/adapter_test.go` (slow-log rules/items,
//! RU-v2 finish sync, format-SQL, max-execution-time and the L5 rows
//! counters). The rule parser and match core ARE transcreated, but in the
//! sibling crate `tidb-exec` (`slow_log_rules.rs`, `slow_log_match.rs`,
//! `ruv2_metrics.rs`, `adapter.rs::format_sql`,
//! `insert_rows_col_multiply.rs`, `delete_rows_col_multiply.rs`), which this
//! crate does not depend on; the full-SQL halves additionally need a live
//! session stack this tier does not model.

/// Go `pkg/executor/adapter_test.go:82::TestFormatSQL`: `FormatSQL`
/// (`pkg/executor/adapter.go:1577`) returns the SQL unchanged while
/// `tidb_query_log_max_len` is 0, and truncates to `prefix(len:N)` when it is
/// 5: `FormatSQL("a"*20)` with max len 5 renders `aaaaa(len:20)`.
#[test]
#[ignore = "go-parity-gap: FormatSQL is transcreated as tidb-exec::adapter::format_sql (tidb-exec/src/adapter.rs:504), a sibling crate this one does not depend on"]
fn format_sql_clamps_to_query_log_max_len() {}

/// Go `pkg/executor/adapter_test.go:93::TestContextCancelWhenReadFromCopIterator`:
/// a context canceled between the coprocessor receive boundary (failpoint
/// `pkg/store/copr/CtxCancelBeforeReceive`) and result draining makes an
/// internal `select * from test.t` fail with `context.Canceled`.
#[test]
#[ignore = "go-parity-gap: needs a mock TiKV store, an internal session and the cop receive failpoint; none exist on this tier"]
fn context_cancel_while_reading_cop_iterator_surfaces_context_canceled() {}

/// Go `pkg/executor/adapter_test.go:128::TestPrepareAndCompleteSlowLogItemsForRules`:
/// with session rules requiring only conn_id/db/succ/process_time,
/// `PrepareSlowLogItemsForRules` (`pkg/executor/adapter_slow_log.go:92`)
/// fills only required fields (ExecRetryCount and MemMax stay zero until
/// `CompleteSlowLogItemsForRules`, `:178`), skipping the fill entirely when
/// `NeedUpdateEffectiveFields` is false; the prepared items then match rules
/// for conn_id 123, db `testdb`, succ true, process_time 2s, backoff 1ms,
/// process_keys 20001, total_keys 10000, and
/// `cop_mvcc_read_amplification` (total_keys/processed_keys,
/// `pkg/sessionctx/variable/slow_log.go:227`) matches a 0.49 threshold but
/// not 0.5; `Wait_time` has no rule accessor at all; completion sets
/// ExecRetryCount 2, MemMax 1000, DiskMax 2000, Succ and the KV/PD wait
/// fields from the context's `util.ExecDetails`.
#[test]
#[ignore = "go-parity-gap: PrepareSlowLogItemsForRules/CompleteSlowLogItemsForRules (pkg/executor/adapter_slow_log.go:92/:178) and SlowQueryLogItems are unported; tidb-exec carries only the rule parser/matcher"]
fn prepare_and_complete_slow_log_items_fill_only_rule_fields() {}

/// Go `pkg/executor/adapter_test.go:206::TestShouldWriteSlowLog` (7
/// subtests): with empty rules nothing is written; a session rule on
/// `resource_group:testRG` matches the base items; a session miss falls back
/// to global `conn_id`/`resource_group` rules, global `unsetConnID` rules
/// match any connection, OR-combined rule groups match when any group holds
/// (`Conn_id:N,...` or `Succ:false`), complex multi-field groups
/// (exec_retry_count/session_alias/pd_total/kv_total/db) evaluate per field,
/// and `cop_mvcc_read_amplification:10` matches items whose scan detail
/// ratio is 10 (100 total/10 processed) while `10.01` does not. Each subtest
/// also pins the `show variables` / `@@SESSION` / `@@GLOBAL` renderings of
/// `tidb_slow_log_rules` (lower-cased, connection-id expanded).
#[test]
#[ignore = "go-parity-gap: ShouldWriteSlowLog needs the session sysvar stack (set/show tidb_slow_log_rules); the pure matcher lives in tidb-exec::slow_log_match::should_write_slow_log, outside this crate"]
fn should_write_slow_log_matches_session_then_global_rules() {}

/// Go `pkg/executor/adapter_test.go:402::TestWriteSlowLog`: with the zap
/// observer swapped in as the slow-query logger, a statement is logged
/// exactly once (`metrics.SlowQueryCounter` +1, warn-level entry containing
/// the SQL) when `tidb_slow_log_threshold=0`, not logged at the default 300ms
/// threshold, and logged under `tidb_slow_log_threshold=5000` once session
/// or global `tidb_slow_log_rules="Succ:true"` matches.
#[test]
#[ignore = "go-parity-gap: needs a live session, the slow-query zap logger swap and metrics.SlowQueryCounter; none modeled on this tier"]
fn write_slow_log_emits_one_entry_when_threshold_or_rules_admit() {}

/// Go `pkg/executor/adapter_test.go:458::TestFinishExecuteStmtSyncsTiDBRUV2FromRUDetails`
/// (3 subtests): `FinishExecuteStmt` (`pkg/executor/adapter.go:1659`) drains
/// the context's `util.RUDetails` into the session's `RUV2Metrics`
/// (TiKV read/write RPC counts 5/7, storage processed keys 11, commit
/// details write keys 3 / size 66), reports `tikvRUV2=23456`,
/// `tidbRUV2=expected.CalculateRUValues(...)`, `tiflashRU=345+67` through the
/// DistSQL reporter under `rg1`; a bypass-enabled metrics object skips final
/// reporting entirely; and 64 concurrent finishes against a mutating
/// `ExecDetails` read the network-traffic stats without data races.
#[test]
#[ignore = "go-parity-gap: FinishExecuteStmt RU-v2 sync (pkg/executor/adapter.go:1659) over util.RUDetails/CommitDetails and the DistSQL reporter seam are unported (metrics live in tidb-exec::ruv2_metrics)"]
fn finish_execute_stmt_syncs_ruv2_metrics_from_ru_details() {}

/// Go `pkg/executor/adapter_test.go:649::TestSlowLogMaxPerSec`: the GLOBAL-only
/// `tidb_slow_log_max_per_sec` variable rejects session sets (1238/1229
/// errors), rejects non-integer strings (1232), truncates -1 and 1234567 to
/// the [0, 1000000] clamp with warning 1292, and re-arms
/// `vardef.GlobalSlowLogRateLimiter` (`pkg/sessionctx/vardef/tidb_vars.go:1887`)
/// so 2 allows then a deny at rate 2 and unlimited allows at rate 0.
#[test]
#[ignore = "go-parity-gap: the sysvar/validation stack and vardef.GlobalSlowLogRateLimiter live in the session tier; no SET GLOBAL surface on this crate"]
fn slow_log_max_per_sec_is_a_clamped_global_rate_limit() {}

/// Go `pkg/executor/adapter_test.go:806::TestMaxExecutionTimeIncludesTSOWaitTime`
/// (3 timed cases): with failpoint `injectTSOWaitDelay` stalling the TSO wait
/// by 50/150/300ms, a range SELECT honors `max_execution_time` including the
/// TSO wait (50/150ms pass under a 500ms budget, 300ms exceeds a 50ms
/// budget with `maximum statement execution time exceeded`), and
/// `ShowProcess` elapsed time includes the injected wait.
#[test]
#[ignore = "go-parity-gap: needs a live session, TSO-wait failpoint injection and statement kill scheduling; unmodeled on this tier"]
fn max_execution_time_includes_tso_wait_time() {}

/// Go `pkg/executor/adapter_test.go:892::TestInsertRowsColMultiplyRUV2SQLPath`:
/// each INSERT's context-scoped `RUV2Metrics.ExecutorL5InsertRows` counts
/// rows*columns per statement (2 rows x 3 cols = 6; 2 rows x 2 listed cols =
/// 4; insert-select 2x2 = 4), and batched DML accumulates across the
/// statement even when a duplicate-key error aborts the tail batch
/// (batch of 2, 4 rows -> 12 total across attempt + duplicate re-encode of
/// the failed row set, surviving rows order-pinned).
#[test]
#[ignore = "go-parity-gap: needs INSERT execution with RUV2 context plumbing; the pure counter lives in tidb-exec::insert_rows_col_multiply::rows_col_multiply (tidb-exec/src/insert_rows_col_multiply.rs:23)"]
fn insert_rows_col_multiply_counts_rows_times_columns_per_statement() {}

/// Go `pkg/executor/adapter_test.go:934::TestDMLRowsColMultiplyRUV2SQLPath`:
/// REPLACE/UPDATE/DELETE and multi-table deletes/updates accumulate
/// `ExecutorL5InsertRows` by affected rows times column count (replace 2x3 =
/// 6; update of 2 rows x 3 cols = 6; delete 1x3 = 3; two-table delete 2x2 =
/// 4; left-join update touches only the matched rows = 2; inner join with a
/// duplicated side counts each updated row once = 2).
#[test]
#[ignore = "go-parity-gap: needs DML execution with RUV2 context plumbing; the accumulator lives in tidb-exec::delete_rows_col_multiply::add_delete_rows_col_multiply (tidb-exec/src/delete_rows_col_multiply.rs:27)"]
fn dml_rows_col_multiply_accumulates_affected_rows_times_columns() {}
