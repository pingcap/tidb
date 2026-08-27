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

//! Documentary gap ports for `pkg/planner/core/rule/collect_column_stats_usage_test.go`
//! (`pkg/planner.part15` items 841–842 on `origin/master`).
//!
//! Both tests drive parsed SQL through `coretestsdk.CreatePlannerSuiteElems`,
//! `core.Preprocess`, `PlanBuilder.Build` and `core.LogicalOptimizeTest`, then
//! interrogate `rule.CollectColumnStatsUsage(lp)` over the infoschema; the
//! Rust workspace has none of that SQL→plan pipeline yet. Recorded as gaps,
//! not approximations.
//!
//! | Go function (`collect_column_stats_usage_test.go`) | Rust test |
//! | --- | --- |
//! | `:151 TestCollectPredicateColumns` | [`collect_predicate_columns_across_operator_kinds`] |
//! | `:343 TestCollectHistNeededColumns` | [`collect_hist_needed_columns_full_meta_split`] |

/// GO PORT of `collect_column_stats_usage_test.go:151 TestCollectPredicateColumns`.
///
/// Re-derived contract: the 27-case SQL table (:152-341) runs every query
/// through Preprocess + Build + LogicalOptimizeTest and checks
/// `rule.CollectColumnStatsUsage`'s predicate-column set TWICE per case,
/// before and after logical optimization (:336-339); each case must collect
/// exactly its listed fully-qualified columns via
/// `checkColumnStatsUsageForPredicates` (:85-89). Pins per-operator
/// contributions across the table span: DataSource filters (`select * from t
/// where a > 2`, :159), projection remapping of `a+b`, aggregation
/// group-by/having args, union-all children, window partition/order keys
/// (`avg(b) over(partition by a)`, :194), semi/anti joins and applies
/// (`> all/> any/scalar sum/count(*), exists/not exists/in/not in`),
/// Sort/TopN keys, filtered union-all views (:279), CTE seed/recursive
/// assignments (`with cte(x, y) as (select a + 1, b from t …)`, :284), and
/// the partitioned table reporting TABLE ids under BOTH prune modes
/// (`pruneMode: "static"` :304, `"dynamic"` :310).
#[test]
#[ignore = "go-parity-gap: needs the SQL build/optimize pipeline plus rule.CollectColumnStatsUsage over an infoschema"]
fn collect_predicate_columns_across_operator_kinds() {}

/// GO PORT of `collect_column_stats_usage_test.go:343 TestCollectHistNeededColumns`.
///
/// Re-derived contract: with failpoint `forceDynamicPrune=return(true)`
/// enabled (:344), eleven cases (:345-405) assert via
/// `checkColumnStatsUsageForStatsLoad` (:96-111) BOTH the full/meta split of
/// StatsLoadItems (equality/in-list predicates need META only, everything
/// else FULL) AND the expanded-partition map: `pt1` under static mode
/// expands to `{"pt1": ["pt1.p1", "pt1.p2"]}` (:388-392) while dynamic mode
/// leaves it empty (:396-399). `_tidb_rowid > 1` collects nothing (:364);
/// flags are masked with `^ (FlagJoinReOrder | FlagPruneColumnsAgain)`
/// because hist-needed collection must precede join reorder (:428).
#[test]
#[ignore = "go-parity-gap: needs CollectColumnStatsUsage's full/meta halves, failpoint plumbing and partition expansion"]
fn collect_hist_needed_columns_full_meta_split() {}
