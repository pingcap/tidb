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

//! Port for `pkg/planner/core/tests/subquery/subquery_test.go:23
//! TestCollateSubQuery` — item 1178 of `pkg/planner.part20` (all 1278
//! `Test*`/`Benchmark*` declarations under `pkg/planner/` on `origin/master`,
//! sorted by file then line, chunked by 60). The package's
//! `main_test.go:25 TestMain` (item 1177) is bootstrap-only and is recorded
//! as skipped-reason in the batch receipt.

/// GO PORT of `pkg/planner/core/tests/subquery/subquery_test.go:23
/// TestCollateSubQuery`.
///
/// Contract: `t(id int, col varchar(100), key ix(col))` and
/// `t1(id varchar(100))`, both `CHARSET=utf8mb4 COLLATE=utf8mb4_bin`
/// (:27-28). The plan for
/// `select * from t use index(ix) where col in (select cast(id as char)
/// from t1)` is the SAME seven-row tree under three different
/// `collation_connection` values — utf8mb4_bin default (:38-41),
/// `utf8_bin` (:42-44), and `latin1_bin` (:45-47): an IndexHashJoin whose
/// build side aggregates the subquery with
/// `group by:cast(test.t1.id, var_string(100))` and whose probe side does
/// `IndexRangeScan ... range: decided by [eq(test.t.col, Column)]` (:29-41).
/// Collation-connection changes must not re-shape the subquery-to-join
/// rewrite when both columns are binary collation.
///
/// go-parity-gap: explain-format plan goldens over a live session with
/// collation-connection variables need the session/explain stack.
#[test]
#[ignore = "go-parity-gap: collation-driven explain goldens need the session + explain stack"]
fn collate_subquery_cast_plan_independent_of_collation_connection() {}
