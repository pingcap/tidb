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

//! Port ledger for `pkg/planner/core/integration_test.go` range-memory
//! fallback and correlated-column access-condition items
//! (`pkg/planner.part11`, Go items 619–621 and 626 on `origin/master`).
//!
//! Family contract: `tidb_opt_range_max_size` bounds the memory spent
//! building index ranges; exceeding it falls back to coarser ranges with
//! Warning 1105 "Memory capacity of N bytes for 'tidb_opt_range_max_size'
//! exceeded when building ranges. Less accurate ranges such as full range are
//! chosen". Correlated equalities/ranges appended via
//! SplitCorColAccessCondFromFilters are EXEMPT from that budget (both when
//! first attached and each time they are rebuilt per outer value), because a
//! rebuild-time fallback would return WRONG query results.
//!
//! All four items are honest gap ports: range construction against mock-store
//! sessions plus prepared-plan-cache round-trips do not exist in this crate;
//! nothing was approximated to simulate Go behavior.

/// GO PORT of `pkg/planner/core/integration_test.go:1508
/// TestPlanCacheForIndexRangeFallback`.
///
/// Re-derived contract: over t(a,b,c; index idx_a_b) with
/// tidb_opt_range_max_size=1330 (exactly five ["x","x"] point pairs):
/// literal IN with 5 short strings keeps IndexRangeScan showing all five
/// ranges (:1518-1520); 5×10-char strings exceed the budget → TableFullScan +
/// the Warning 1105 (:1521-1527). PREPARE/EXECUTE of the same shapes shows
/// the cache asymmetry (:1534-1552): first execute builds within budget
/// (no warnings) and caches; oversized parameters still answer from a plan
/// whose REBUILT ranges ignore the memory limit (last_plan_from_cache=1,
/// explain-for-connection shows IndexRangeScan with all five long ranges,
/// :1540-1549); a 5-in-list × 2-columns statement that fell back is NOT
/// cached (last_plan_from_cache=0) and warns additionally "skip prepared
/// plan-cache: in-list is too long" (:1544-1548).
#[test]
#[ignore = "go-parity-gap: needs range-memory accounting + prepared plan cache + explain-for-connection"]
fn plan_cache_range_fallback_never_limits_cached_rebuild() {}

/// GO PORT of `pkg/planner/core/integration_test.go:2039
/// TestPlanCacheForIndexJoinRangeFallback`.
///
/// Re-derived contract: same budget mechanism for INDEX-JOIN inner ranges:
/// tidb_opt_range_max_size=1260 fits [? a,? a]…[? c,? c] so inl_join keeps
/// "range: decided by [eq(test.t1.a, test.t2.d) in(test.t1.b, a, b, c)]"
/// without warning (:2050-2053); long in-lists drop to
/// "range: decided by [eq(test.t1.a, test.t2.d)]" WITH the overflow warning
/// (:2054-2058); cached-plan rebuild again ignores the limit — the executed
/// cached plan regains the full in-list decided-by text (:2060-2072); the
/// five-item-in-list join plan stays uncached with both warnings
/// ("skip prepared plan-cache: in-list is too long", :2073-2080).
#[test]
#[ignore = "go-parity-gap: needs index-join range building + prepared plan cache interaction"]
fn plan_cache_index_join_range_fallback_matches_literal_semantics() {}

/// GO PORT of `pkg/planner/core/integration_test.go:1554
/// TestCorColRangeWithRangeMaxSize`.
///
/// Re-derived contract (decorrelate blacklisted so EXISTS stays an Apply):
/// correlated-column handling overrides tiny range budgets (:1580-1615):
/// with tidb_opt_range_max_size=1000 the b>=2 predicate CANNOT extend the
/// three cor-col point ranges ([1..,3..,5..] × b-inf), so it remains as
/// Selection ge(test.t2.b, 2) while IndexRangeScan keeps "range: decided by
/// [in(test.t2.a, 1, 3, 5) eq(test.t2.b, test.t1.a)]" AND the overflow
/// warning fires exactly once for the pre-fallback build; executing the
/// query returns row "2" correctly — proving rebuilt-per-value ranges never
/// fall back. With max_size=1, a PK correlated equality still yields
/// TableRangeScan "range: decided by [eq(test.t3.a, test.t1.a)]" and rows
/// "2","4" (the single-byte budget would reject even one point pair).
#[test]
#[ignore = "go-parity-gap: needs SplitCorColAccessCondFromFilters + opt-rule blacklist + apply execution"]
fn cor_col_access_conditions_bypass_range_memory_limit() {}

/// GO PORT of `pkg/planner/core/integration_test.go:1604
/// TestCorColRangePredicateAccess`.
///
/// Re-derived contract (decorrelate blacklisted, NO_DECORRELATE hints):
/// LT/GT/LE/GE predicates between inner column and OUTER correlated column
/// become index ACCESS conditions rendered inside "range: decided by"
/// (`lt(test.t1.a, test.t1.a)` :1625-1626, `gt` :1655-1656, `le` :
/// 1677-1678, `ge` :1702-1703) on clustered-PK secondaries — not mere
/// post-scan filters. Reversed operand order (outer < inner) recognizes the
/// same access condition (:1729-1734). Result semantics across fixture
/// stages pin NULL three-valued logic: empty result before duplicate keys;
/// "10 1","20 2" after inserting (10,1),(20,2) for LT/GT unions; self-match
/// inclusive LE/GE lists all rows; tables containing a NULL-pK-less row
/// (NULL,1) contribute no matches for either strict arm but DO appear in
/// LE/GE through self/inclusive matches on other rows.
#[test]
#[ignore = "go-parity-gap: needs cor-col range predicates feeding access conditions + executor"]
fn cor_col_range_predicates_become_index_access_conditions() {}
