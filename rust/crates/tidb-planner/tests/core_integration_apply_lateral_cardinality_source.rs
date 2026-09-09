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

//! Port ledger for `pkg/planner/core/integration_test.go` Apply/LATERAL
//! cardinality and correlated-order items (`pkg/planner.part11`, Go items
//! 622–625 on `origin/master`).
//!
//! Family contract: LATERAL/correlated execution planning metrics —
//! estimated-vs-actual Apply row counts from `explain analyze`, Apply-cache
//! on/off decisions recorded in operator info, and order-provisioning of
//! index scans through correlated equalities — measured over analyzed
//! mock-store tables with `insert into mysql.opt_rule_blacklist
//! value("decorrelate")` keeping subqueries as Applies.
//!
//! All four items are honest gap ports: this crate has no cost model driver,
//! no explain-analyze executor and no apply cache; nothing was approximated
//! to simulate Go behavior.

/// GO PORT of `pkg/planner/core/integration_test.go:1773
/// TestLateralJoinCardinality`.
///
/// Re-derived contract: a derived table executed once per outer row already
/// accounts for its correlated predicates in its own stats, so the Apply/join
/// estimate must equal the PRODUCT of outer rows × per-execution inner rows —
/// never rescaled by correlated-column NDV selectivity (:1773-1841):
/// lateral scalar MIN aggregate → exactly 600 (one row/outer) actual AND
/// equal estimate (:1822-1824); lateral ORDER BY+LIMIT 3 → 1800 (=600×3,
/// :1827-1830); unbounded GROUP BY k2 → 30000 (=30 keys × 20 matches × …,
/// every matched tl_inner row once, :1835-1838). Fixture: tl_inner 30 groups
/// × 50 rows analyzed all columns; tl_outer 600 rows over the same 30 keys
/// (:1780-1812); est/act extracted from explain-analyze Apply rows (:1816).
#[test]
#[ignore = "go-parity-gap: needs lateral join cardinality + explain analyze est/act pipeline"]
fn lateral_join_cardinality_is_outer_times_per_execution_rows() {}

/// GO PORT of `pkg/planner/core/integration_test.go:1843
/// TestApplyCacheEnabledByOuterRowCount`.
///
/// Re-derived contract: the Apply cache decision is taken from how often the
/// SAME correlated VALUE recurs among ROWS REACHING the Apply — not from its
/// output row count (:1843-1927). With identical 40-rows-per-outer lateral
/// payloads: tac_uniq (50 distinct keys, one outer row each) → info contains
/// "cache:OFF" (:1902-1903); tac_rep (10 rows per key) → "cache:ON"
/// (:1905-1907); joining tac_uniq to tac_fan which duplicates each key ten
/// times flips it to "cache:ON" even though the correlated column is that
/// table's PRIMARY KEY (:1912-1923). Explain-analyze Apply row column 5
/// carries the exec info (:1877-1884).
#[test]
#[ignore = "go-parity-gap: needs apply-cache costing keyed on outer-row value repetition"]
fn apply_cache_uses_reaching_row_values_not_apply_row_count() {}

/// GO PORT of `pkg/planner/core/integration_test.go:1929
/// TestCorColEqProvidesIndexOrder`.
///
/// Re-derived contract: an `index_col = correlated_col` access condition pins
/// its index position to ONE value per execution, so later index columns are
/// ordered and an early-stop plan replaces TopN sorting (:1929-2017):
/// requireEarlyStopPlan asserts keep order:true scan + Limit(cop[tikv],
/// count:1) + root Limit(count:1) and NO TopN operator anywhere (:1956-1967).
/// Holds for MIN over clustered PK (k1,k2) with cor-eq on k1 (results 10,10,
/// 30,30,50 :1976-1978), for secondary ik(k1,v,k2) with literal v=1 equality
/// (NULL-padded results :1980-1983), and for explicit ORDER BY..LIMIT 1
/// inside the subquery (:1985-1990). A correlated RANGE predicate (t2.c <
/// t1.c after b-eq) does NOT pin order → TopN must SURVIVE (:1994-2012).
#[test]
#[ignore = "go-parity-gap: needs cor-col order provisioning + early-stop limit push into cop paths"]
fn cor_col_equality_provides_index_order_and_limit_one_plans() {}

/// GO PORT of `pkg/planner/core/integration_test.go:2019
/// TestExplainAnalyzeDMLCommit` (issue #37373).
///
/// Re-derived contract: while commit is paused by failpoint
/// github.com/pingcap/tidb/pkg/session/mockSleepBeforeTxnCommit=return(500)
/// (:2026-2031), executing `explain analyze delete from t` must still
/// complete without error only AFTER the commit finishes — no interference
/// between explain-analyze execution and in-flight txn commit; the following
/// plain select observes the deleted state (empty result, :2036).
#[test]
#[ignore = "go-parity-gap: needs DML explain-analyze execution + txn-commit failpoint hook"]
fn explain_analyze_dml_waits_for_paused_commit_finish() {}
