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

//! Port ledger for `pkg/planner/core/casetest/instanceplancache/builtin_func_test.go`
//! (`pkg/planner.part5`, items 241–248 of all `Test*`/`Benchmark*`
//! declarations under `pkg/planner/` on `origin/master`, sorted by file path
//! then line).
//!
//! Family contract being pinned: with
//! `set global tidb_enable_instance_plan_cache=1` (sessionctx/variable/
//! sysvar.go:1712-1719), TEN concurrent sessions each run 100 rounds of
//! prepare/execute over randomly chosen table shapes, and every cached-plan
//! execution must return exactly the same rows as a fresh one for the SAME
//! parameter values — i.e. the instance plan cache
//! (planner/core/plan_cache_utils.go:538 `PlanCacheValue`) must never leak
//! parameters of one execution into another through the shared plan.
//!
//! Every item here is an honest gap port: this crate has no SQL session,
//! no mock store (`testkit.CreateMockStore`), no PREPARE/EXECUTE protocol,
//! and no instance plan cache carrier at all — `PlanCacheValue` lives only
//! in Go (plan_cache_utils.go:538-572). Recorded as gaps, not approximated.

/// GO PORT of `builtin_func_test.go:26 TestBuiltinInIntSig`.
///
/// Re-derived contract: four tables (plain `a int`, indexed, primary key,
/// unique key) seeded 0..99; ten goroutines ×100 iterations pick two values
/// v1 ∈ [0,50) and v2 ∈ [50,100), pick one of the four tables, re-PREPARE
/// `select a from <t> where a in (?, ?)` each round and EXECUTE with
/// `@p1=v1 @p2=v2`; the sorted result must be exactly [`v1`, `v2`] rendered
/// as decimal strings (lexical min/max compare is irrelevant because
/// v1 < v2 numerically and both are ≤2 digits). Pins that a plan prepared
/// against ANY of the four access shapes returns live parameter results —
/// never stale constants captured at prepare time.
#[test]
#[ignore = "go-parity-gap: needs mock store + session PREPARE/EXECUTE protocol + instance plan cache, none carried in this crate"]
fn builtin_in_int_sig_prepared_execute_matches_sorted_rows() {}

/// GO PORT of `builtin_func_test.go:65 TestBuiltinInStringSig`.
///
/// Re-derived contract: same workload shape as the int sig (:26) over
/// `varchar(20)` columns; values are bound as quoted string literals
/// (`set @p1='%v'`) so the IN-list comparisons are string-typed, and every
/// cached execution must return exactly the two bound values sorted.
#[test]
#[ignore = "go-parity-gap: needs mock store + session PREPARE/EXECUTE + instance plan cache"]
fn builtin_in_string_sig_prepared_execute_matches_sorted_rows() {}

/// GO PORT of `builtin_func_test.go:104 TestBuiltinInRealSig`.
///
/// Re-derived contract: rows inserted as `<i>.1` literals into `real`
/// columns; parameters bound as `<v>.1` strings so TiDB converts them to
/// float in the IN-list; every cached execution returns exactly the two
/// values formatted `<int-part>.1`, proving REAL-typed param coercion is
/// reproducible across plan-cache hits.
#[test]
#[ignore = "go-parity-gap: needs mock store + session PREPARE/EXECUTE + instance plan cache"]
fn builtin_in_real_sig_prepared_execute_matches_sorted_rows() {}

/// GO PORT of `builtin_func_test.go:143 TestBuiltinInDecimalSig`.
///
/// Re-derived contract: `decimal(10,2)` rows inserted as `<i>.10`; params
/// bound as `<v>.10` strings; the DECIMAL scale fix-up applied at bind time
/// must produce identical results whether or not the plan was served from
/// the instance cache.
#[test]
#[ignore = "go-parity-gap: needs mock store + session PREPARE/EXECUTE + instance plan cache"]
fn builtin_in_decimal_sig_prepared_execute_matches_sorted_rows() {}

/// GO PORT of `builtin_func_test.go:182 TestBuiltinInTimeSig`.
///
/// Re-derived contract: `datetime` rows '2000-01-01 00:{10..49}:00';
/// parameters are datetime strings drawn from minutes 10..29 and 30..49;
/// string→DATETIME conversion of the IN-list bound values must be stable
/// under plan-cache reuse.
#[test]
#[ignore = "go-parity-gap: needs mock store + session PREPARE/EXECUTE + instance plan cache"]
fn builtin_in_time_sig_prepared_execute_matches_sorted_rows() {}

/// GO PORT of `builtin_func_test.go:222 TestBuiltinRealIsTrueFalse`.
///
/// Re-derived contract: table t(real) holds {1.1, 2.2}; each round prepares
/// either `select a from t where (a-?) is true` (expected result: the OTHER
/// value, since a-p≈a≠0 keeps truthiness true only for the row whose value
/// does not cancel the parameter) or `(a-?) is false` (expected: the bound
/// value's own row when the subtraction cancels to falsy zero for it).
/// Ten workers ×100 randomized rounds pin that `IsTrue`/`IsFalse` truthiness
/// over float parameters is preserved verbatim on every cached execution.
#[test]
#[ignore = "go-parity-gap: needs mock store + session PREPARE/EXECUTE + instance plan cache"]
fn builtin_real_is_true_false_truthiness_survives_cached_plans() {}

/// GO PORT of `builtin_func_test.go:257 TestBuiltinDecimalIsTrueFalse`.
///
/// Re-derived contract: identical truthiness matrix to :222 over
/// `decimal(10,2)` values {1.10, 2.20}; pins exact-decimal cancellation
/// semantics (not float rounding) under the instance plan cache.
#[test]
#[ignore = "go-parity-gap: needs mock store + session PREPARE/EXECUTE + instance plan cache"]
fn builtin_decimal_is_true_false_truthiness_survives_cached_plans() {}

/// GO PORT of `builtin_func_test.go:292 TestBuiltinIntIsTrueFalse`.
///
/// Re-derived contract: same truthiness matrix over int values {1, 2};
/// integer arithmetic a-? cannot leave a fractional residue, so only the
/// exact cancellation row flips truthiness; results must match plain
/// executions for every plan-cache hit.
#[test]
#[ignore = "go-parity-gap: needs mock store + session PREPARE/EXECUTE + instance plan cache"]
fn builtin_int_is_true_false_truthiness_survives_cached_plans() {}
