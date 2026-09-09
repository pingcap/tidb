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

//! Documentary gap ports for `pkg/planner/core/exhaust_physical_plans_test.go`
//! index-join lookup-filter items (`pkg/planner.part10` items 560-561 on
//! `origin/master`) and `pkg/planner/core/explain_ru_test.go` (items 562-563).
//!
//! The exhaust items drive `getIdxLookupFilters`/
//! `buildRangeForIndexJoinLookUpFilters` over a hand-built join fixture
//! (`prepareForAnalyzeLookUpFilters` :61-186: outer/inner schemas of longlong
//! and varchar columns with prefix-index lens {unspecified,…,2,…,2}) and pin
//! the exact chosen ranges / idxOff2KeyOff / accesses / remained /
//! compareFilters stringifications per case. The crate's task stack refuses
//! the index-join runtime property by name (`src/task` header) and carries no
//! stringified filter rendering, so both stay recorded gaps.

/// GO PORT of
/// `pkg/planner/core/exhaust_physical_plans_test.go:227
/// TestIndexJoinAnalyzeLookUpFilters`.
///
/// Contract table (:227-358): non-continuous join keys without pushed filters
/// keep `[[NULL,NULL]]` with idxOff2KeyOff `[0 -1 -1 -1 -1]`; a pushed eq
/// filter NOT aligned to keys yields empty everything; continuous keys plus
/// `a = 1` produce `[[1 NULL,1 NULL]]` and accesses `eq(Column#1, 1)`
/// (:271-283); correlated otherConds become BOTH accesses and
/// lastColManager compareFilters, while plain int addition avoids an extra
/// cast in either; prefix indexes split gt/lt pairs into accesses + remained
/// with half-truncated literal bounds (`"a"`→open `(1 NULL "a",`); IN lists
/// explode into the cross product of point ranges across key columns
/// (:315-333); a range-only pushover on a non-key column widens to
/// `[(NULL 1,NULL +inf]]`.
#[test]
#[ignore = "go-parity-gap: index-join inner lookup-filter construction over built joins is unported (task-stack refuses IndexJoinProp)"]
fn index_join_analyze_lookup_filters_table_pins_ranges_and_offsets() {}

/// GO PORT of
/// `pkg/planner/core/exhaust_physical_plans_test.go:368
/// TestRangeFallbackForAnalyzeLookUpFilters`.
///
/// Contract (:368-…): driving the SAME cases under shrinking
/// `tidb_opt_range_max_size` budgets must progressively DEGRADE the built
/// ranges — full cross-product ranges, then truncated per-key ranges, then
/// empty — with each step gated on a `'tidb_opt_range_max_size' exceeded when
/// building ranges` warning visible in stmtCtx warnings
/// (checkRangeFallbackAndReset resets handlers between steps :359-366). Range
/// memory-budget fallback handling does not exist in this crate yet.
#[test]
#[ignore = "go-parity-gap: tidb_opt_range_max_size budget/fallback handler lives outside this crate"]
fn range_fallback_for_analyze_lookup_filters_degrades_by_budget() {}

/// GO PORT of `pkg/planner/core/explain_ru_test.go:28 TestExplainAnalyzeRUFormat`.
///
/// Contract (:28-66): over t(a int), every SQL in the explain-analyze-ru suite
/// book replays its string-matrix rows exactly through
/// `explain analyze format = 'ru'` — the RU formatter output surface.
#[test]
#[ignore = "go-parity-gap: RU cost accounting and analyze-format execution need the store"]
fn explain_analyze_ru_format_rows_match_suite_book() {}

/// GO PORT of `pkg/planner/core/explain_ru_test.go:67
/// TestExplainAnalyzeRUFormatEndToEndMonotonicity`.
///
/// Contract subtests (:140-260): TableReader cumRU strictly increases as rows
/// accumulate across 20 insert batches; scan RU is attributed ONLY to the
/// owning Reader (TableFullScan selfRU/cumRU are zero); Selection selfRU grows
/// with input rows AND with condition count; Sort selfRU grows with input
/// rows; TopN and Limit selfRU grow with retained-row counts. Each pins that
/// per-operator RU attribution stays monotone end-to-end.
#[test]
#[ignore = "go-parity-gap: end-to-end RU attribution needs executed plans against a real mock store"]
fn explain_analyze_ru_monotonicity_legs_hold_end_to_end() {}
