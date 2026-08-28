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

//! Gap tests for the prepared-statement Go tests of
//! `pkg/executor/prepared_test.go` whose contracts need sessions,
//! isolation-read engines, global counters, charset-bound prepare APIs, or
//! wrong-type execute errors this tier does not model. The runnable halves
//! of `TestPlanCacheWithDifferentVariableTypes` and `TestParameterPushDown`
//! are ported in `tests_prepared_param_types_source` and
//! `tests_prepared_parameter_pushdown_source`.

/// Go `pkg/executor/prepared_test.go:165::TestPrepareStmtAfterIsolationReadChange`:
/// one PREPARE, two EXECUTEs under different `tidb_isolation_read_engines`
/// (`tikv` then `tiflash`), each explained through `explain for connection`,
/// pinning that the statement re-plans per execute onto the requested engine
/// (`cop[tikv]` then `cop[tiflash]`) while `PreparedStmts[1]` keeps
/// normalized SQL `select * from \`t\`` and an empty `NormalizedPlan`. Needs
/// a live domain (TiFlash-replica metadata override), isolation-read engine
/// gating, and the process/EXPLAIN-for-connection surface.
#[test]
#[ignore = "go-parity-gap: isolation-read engine gating (pkg/planner/core/rule_partition_processor or plan builder engine selection), TiFlash replica metadata, and explain-for-connection have no Rust counterpart on this tier"]
fn prepare_stmt_replans_after_an_isolation_read_change() {}

/// Go `pkg/executor/prepared_test.go:210::TestMaxPreparedStmtCount`: with
/// `@@global.max_prepared_stmt_count = 2`, the third PREPARE fails with
/// `variable.ErrMaxPreparedStmtCountReached`
/// (pkg/sessionctx/variable/session.go:2970, error
/// pkg/sessionctx/variable/error.go:35), reading the process-wide
/// `variable.PreparedStmtCount` atomic. This tier has no prepared-statement
/// registry or global counter.
#[test]
#[ignore = "go-parity-gap: the process-wide PreparedStmtCount atomic and max_prepared_stmt_count enforcement (pkg/sessionctx/variable/session.go:2970) are unported"]
fn max_prepared_stmt_count_rejects_the_third_prepare() {}

/// Go `pkg/executor/prepared_test.go:225::TestExecuteWithWrongType`:
/// `update t3 set c1 = 2 where c2 in (?, ?)` over
/// `c2 decimal(32,30)` must ERROR when an execute binds `'aa'`
/// (truncated/wrong-value cast error), and the error must not poison the
/// cached plan — a later execute with numeric values succeeds, on the same
/// and on a freshly prepared statement. Probed on this tier: binding
/// `Decimal('0.0')`/`String('aa')` and running the bound update returns
/// `updated 0` WITHOUT any error — the IN comparison evaluates the
/// non-castable string as a non-match instead of Go's cast error, so the
/// Go contract cannot be pinned here.
#[test]
#[ignore = "go-parity-gap: comparing an uncastable string against a decimal IN-list must error (ErrTruncatedWrongValue) but this tier's comparison yields no-match silently; the wrong-type execute error contract is unportable"]
fn execute_with_wrong_parameter_type_errors_without_poisoning_the_cache() {}

/// Go `pkg/executor/prepared_test.go:246::TestIssue58870`: under
/// `set names GBK`, INSERT statements containing raw GBK bytes
/// (`\xB2\xE2`) prepare and execute through the session API
/// (`Session.PrepareStmt` + `ExecutePreparedStmt`), including a
/// no-result-set execute. Rust's driver takes UTF-8 `&str` SQL, so raw GBK
/// byte sequences cannot even be represented in the parse input.
#[test]
#[ignore = "go-parity-gap: GBK-encoded SQL text (raw \\xB2\\xE2 literals) cannot be represented in this tier's UTF-8 parse input; the session PrepareStmt/ExecutePreparedStmt API pair is also unported"]
fn prepare_and_execute_insert_with_gbk_bytes() {}
