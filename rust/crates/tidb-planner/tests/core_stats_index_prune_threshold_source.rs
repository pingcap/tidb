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

//! Documentary gap port for `pkg/planner/core/stats_test.go`
//! (`pkg/planner.part15` items 867–868 on `origin/master`; package
//! `core_test`, exercising index-prune scoring inside logical optimization).
//!
//! | Go function (`stats_test.go`) | Rust test |
//! | --- | --- |
//! | `:40 TestPruneIndexesByWhereAndOrder` | [`prune_indexes_by_where_and_order_thresholds`] |
//! | `:172 TestIndexChoiceFromPruning` | [`index_choice_from_pruning_keeps_plans_threshold_invariant`] |

/// GO PORT of `pkg/planner/core/stats_test.go:40 TestPruneIndexesByWhereAndOrder`.
///
/// Re-derived contract: table `t1(a..f)` with all 64 `(a|b)-prefixed`
/// secondary indexes is built in a live mock store (DDL at :47-77). The
/// helper `getDataSourceFromQuery` (:241-334) parses, applies SET_VAR hints
/// through `hint.ParseStmtHints` with the hint-updatable checker, builds the
/// plan and runs `core.LogicalOptimizeTest` with
/// FlagCollectPredicateColumnsPoint; `findDataSource` (:336-344) locates the
/// DataSource. Subtests: `threshold_20` (:98) keeps strictly FEWER paths than
/// both threshold_100 and the baseline while every remaining path stays
/// well-formed; `threshold_10` (:120) prunes further still;
/// `threshold_hint` (:141) shows `SET_VAR(tidb_opt_index_prune_threshold=10)`
/// matching the session-variable effect; `no_interesting_columns` (:162)
/// restores ALL paths when the query has no predicate columns.
#[test]
#[ignore = "go-parity-gap: needs live optimize pipeline, tidb_opt_index_prune_threshold plumbing and AccessPath construction"]
fn prune_indexes_by_where_and_order_thresholds() {}

/// GO PORT of `pkg/planner/core/stats_test.go:172 TestIndexChoiceFromPruning`.
///
/// Re-derived contract: eleven SQL shapes — equality/in combinations,
/// or-branches, window functions over order-by, plain min(), multi-column
/// ordering, and two left joins between t2 and the wide-index t1 — capture
/// `explain format='plan_tree'` rows per threshold (plansAtNegative :221,
/// plansAt10 :227-233) and require IDENTICAL rows pairwise (:236): pruning
/// may shrink the candidate set but never changes the CHOSEN plan here.
#[test]
#[ignore = "go-parity-gap: plan_tree explain output comparison requires the unported optimizer stack"]
fn index_choice_from_pruning_keeps_plans_threshold_invariant() {}
