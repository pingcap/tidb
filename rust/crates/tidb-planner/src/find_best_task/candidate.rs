// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go access-path comparison shared by ordinary scans and IndexJoin.

use tidb_planner_util::column_length::{compare_col2_len, Col2Len};

/// Go candidatePath and AccessPath fields read by skyline comparison.
#[derive(Clone, Debug, Default)]
pub struct CandidateMetrics {
    /// Access predicate columns and prefix lengths.
    pub access_columns: Col2Len,
    /// Access plus index-filter columns and prefix lengths.
    pub index_columns: Col2Len,
    /// Whether the index covers the required output and filters.
    pub single_scan: bool,
    /// Integer/common-handle table path.
    pub table_path: bool,
    /// MV index paths cannot eliminate another candidate.
    pub multi_valued: bool,
    /// IndexMerge paths have separately estimated partial paths.
    pub partial_paths: bool,
    /// Global indexes win the global/local comparison when applicable.
    pub global: bool,
    /// Whether this candidate matches the required physical property.
    pub matches_property: bool,
    /// Go isCandidatesPseudo after table/index analyzed-state lookup.
    pub pseudo: bool,
    /// Go isFullIndexMatch, including its DNF rules.
    pub full_index_match: bool,
    /// Go equalPredicateCount; IndexJoin uses the number of used columns.
    pub eq_or_in_count: usize,
    /// Go `len(candidatePath.path.Ranges)`, read by `compareEqOrIn`.
    pub range_count: usize,
    /// Estimated rows after access predicates, before index filtering.
    pub count_after_access: f64,
    /// Estimated rows after index filtering.
    pub count_after_index: f64,
    /// Lower bound on access cardinality.
    pub min_count_after_access: f64,
    /// Worst-case access cardinality.
    pub max_count_after_access: f64,
    /// Go `indexJoinCols`: how many index columns the IndexJoin's access
    /// conditions use; zero for an ordinary DataSource candidate.
    pub index_join_cols: usize,
    /// Go `countAfterAccess4IndexJoin`: the access count per probe, used only
    /// by index join candidate comparison.
    pub count_after_access_4_index_join: f64,
    /// Go `countAfterAccess4IndexJoinOK`: whether that count is stable
    /// enough for skyline pruning.
    pub count_after_access_4_index_join_ok: bool,
}

impl CandidateMetrics {
    /// Go `candidatePath.getCountAfterAccess4SkylinePruning`.
    fn get_count_after_access_4_skyline_pruning(&self) -> Option<f64> {
        if self.index_join_cols > 0 {
            return self
                .count_after_access_4_index_join_ok
                .then_some(self.count_after_access_4_index_join);
        }
        Some(self.count_after_access)
    }
}

pub(super) fn equality_facts(
    access_conds: &[tidb_expr::expression::Expression],
    eq_or_in_count: usize,
    is_dnf_cond: bool,
    min_access_conds_for_dnf_cond: i64,
) -> (usize, bool) {
    use tidb_expr::expression::Expression;
    fn only_equal(expression: &Expression) -> bool {
        let Expression::ScalarFunction(function) = expression else {
            return false;
        };
        match function.func_name.lowercase() {
            "eq" | "in" => true,
            "and" | "or" => function.args.iter().all(only_equal),
            _ => false,
        }
    }
    let equal_dnf = is_dnf_cond && access_conds.iter().all(only_equal);
    let minimum_dnf = min_access_conds_for_dnf_cond.max(0) as usize;
    let eq_or_in_count = if !is_dnf_cond || access_conds.is_empty() {
        eq_or_in_count
    } else if equal_dnf {
        minimum_dnf
    } else {
        minimum_dnf.saturating_sub(1)
    };
    (eq_or_in_count, equal_dnf)
}

/// Go `ExtractCol2Len` over index columns: each index column the conditions
/// read, with its index prefix length.
fn extract<'a>(
    conditions: impl Iterator<Item = &'a tidb_expr::expression::Expression>,
    columns: impl Iterator<Item = &'a (tidb_expr::column::Column, i64)> + Clone,
) -> Col2Len {
    use tidb_expr::expression::Expression;
    fn visit<'a>(
        expression: &Expression,
        columns: impl Iterator<Item = &'a (tidb_expr::column::Column, i64)> + Clone,
        result: &mut std::collections::BTreeMap<i64, i64>,
    ) {
        match expression {
            Expression::Column(column) => {
                // Go `extractCol2LenFromExpr`: `EqualByExprAndID`.
                if let Some((_, length)) = columns
                    .clone()
                    .find(|(key, _)| column.equal_by_expr_and_id_column(key))
                {
                    result.insert(column.unique_id, *length);
                }
            }
            Expression::ScalarFunction(function) => {
                for arg in &function.args {
                    visit(arg, columns.clone(), result);
                }
            }
            _ => {}
        }
    }
    let mut result = std::collections::BTreeMap::new();
    for condition in conditions {
        visit(condition, columns.clone(), &mut result);
    }
    Col2Len::from_pairs(result)
}

/// Go `AccessPath.GetCol2LenFromAccessConds`: a table path's access columns
/// at full length, an index path's at their index prefix lengths.
pub(crate) fn get_col2_len_from_access_conds(path: &tidb_planner_util::path::AccessPath) -> Col2Len {
    if path.is_table_path() {
        return Col2Len::from_pairs(path.access_conds.iter().flat_map(|condition| {
            tidb_expr::simple_expr::extract_columns(condition)
                .into_iter()
                .map(|column| (column.unique_id, -1))
        }));
    }
    extract(path.access_conds.iter(), path.idx_cols.iter())
}

/// Go `getIndexCandidate`: read both coverage maps and estimate stages from
/// the logically filled path. Missing facts must not eliminate a competitor.
pub(crate) fn get_index_candidate(
    source: &tidb_planner_core_operator_logicalop::DataSource,
    index: &tidb_model::GoShared<tidb_model::IndexInfo>,
    path: &tidb_planner_util::path::AccessPath,
    single_scan: bool,
    matches_property: bool,
) -> Option<CandidateMetrics> {
    let estimate = path.row_estimate?;
    let count_after_index = path.count_after_index?;
    let access_columns = extract(path.access_conds.iter(), path.idx_cols.iter());
    let index_columns = extract(
        path.access_conds.iter().chain(&path.index_filters),
        path.full_idx_cols.iter().flatten().flatten(),
    );
    let (eq_or_in_count, equal_dnf) = equality_facts(
        &path.access_conds,
        path.eq_or_in_cond_count,
        path.is_dnf_cond,
        path.min_access_conds_for_dnf_cond,
    );
    let minimum_dnf = path.min_access_conds_for_dnf_cond.max(0) as usize;
    let full_index_match = if equal_dnf {
        minimum_dnf >= index.read().columns.len()
    } else {
        path.eq_or_in_cond_count > 0 && index_columns.len() >= index.read().columns.len()
    };
    let pseudo = !has_stats_for_estimation(
        source,
        index.read().id,
        path.idx_cols.iter().map(|(column, _)| column),
        &access_columns,
    );
    Some(CandidateMetrics {
        access_columns,
        index_columns,
        single_scan,
        multi_valued: index.read().mv_index,
        global: index.read().global,
        matches_property,
        pseudo,
        full_index_match,
        eq_or_in_count,
        range_count: path.ranges.len(),
        count_after_access: estimate.est,
        count_after_index,
        min_count_after_access: estimate.min_est,
        max_count_after_access: estimate.max_est,
        ..Default::default()
    })
}

/// Go `hasStatsForEstimation`: the index has statistics, or every column its
/// access conditions reference does. In the latter case its row count is
/// estimated from the column statistics (exponential backoff for several
/// columns), so it is compared on its estimates rather than on the
/// missing-statistics heuristics.
pub(crate) fn has_stats_for_estimation<'a>(
    source: &tidb_planner_core_operator_logicalop::DataSource,
    index_id: i64,
    idx_cols: impl IntoIterator<Item = &'a tidb_expr::column::Column>,
    access_columns: &Col2Len,
) -> bool {
    if source.index_has_analyzed(index_id) {
        return true;
    }
    if access_columns.is_empty() {
        return false;
    }
    let mut found = 0;
    for column in idx_cols {
        if !access_columns.contains_key(column.unique_id) {
            continue;
        }
        if !source.column_has_analyzed(column.id) {
            return false;
        }
        found += 1;
    }
    found == access_columns.len()
}

/// Go compareCandidates's result and its statement-context fix-control read.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CandidateComparison {
    /// 1 means left wins, -1 right wins, 0 means no skyline winner.
    pub ordering: i8,
    /// Whether the winner lacks analyzed statistics.
    pub winner_is_pseudo: bool,
    /// Caller must record relevant optimizer fix 45132 when true.
    pub used_row_count_ratio: bool,
}

fn compare_bool(left: bool, right: bool) -> i8 {
    i8::from(left) - i8::from(right)
}

fn risk_ratio(path: &CandidateMetrics) -> f64 {
    if path.max_count_after_access > path.count_after_access && path.count_after_access > 0.0 {
        path.max_count_after_access / path.count_after_access
    } else {
        0.0
    }
}

fn compare_risk(left: &CandidateMetrics, right: &CandidateMetrics) -> i8 {
    let wins = |a: &CandidateMetrics, b: &CandidateMetrics| {
        risk_ratio(a) < risk_ratio(b)
            && (a.count_after_access <= b.count_after_access
                || (a.count_after_access + a.max_count_after_access
                    < b.count_after_access + b.max_count_after_access
                    && a.min_count_after_access > 0.0
                    && (a.min_count_after_access <= b.min_count_after_access
                        || a.count_after_index <= b.count_after_index)))
    };
    if wins(left, right) {
        1
    } else if wins(right, left) {
        -1
    } else {
        0
    }
}

/// Go `isBroadIndexLookup`: a non-covering index that may read so much of
/// the table that a lookup per row can cost more than scanning the table --
/// either its own estimate reads at least `cost.SelectionFactor` (the
/// default selectivity) of `table_rows`, or it has more ranges than
/// `range_max_count` (`tidb_opt_range_max_count`; zero disables that check).
/// The estimate of an index without statistics is unreliable, but it still
/// scales with the number of ranges: a single-column IN list of 900 values
/// is estimated to read 90% of the table. See pingcap/tidb#72561.
pub(crate) fn is_broad_index_lookup(c: &CandidateMetrics, table_rows: f64, range_max_count: i64) -> bool {
    if c.table_path || c.single_scan {
        return false;
    }
    if range_max_count > 0 && c.range_count as i64 > range_max_count {
        return true;
    }
    c.count_after_access > 0.0
        && c.count_after_access >= table_rows * tidb_planner_core_cost::SELECTION_FACTOR
}

/// Go `canPseudoWinnerPrune`: false when the winner is a broad index lookup
/// relative to the table path loser, so both paths are left for cost
/// comparison.
fn can_pseudo_winner_prune(winner: &CandidateMetrics, loser: &CandidateMetrics, range_max_count: i64) -> bool {
    !loser.table_path || !is_broad_index_lookup(winner, loser.count_after_access, range_max_count)
}

fn compare_pseudo(left: &CandidateMetrics, right: &CandidateMetrics, eq: i8) -> i8 {
    if !left.pseudo && left.eq_or_in_count > 0 && eq >= 0 {
        return 1;
    }
    if !right.pseudo && right.eq_or_in_count > 0 && eq <= 0 {
        return -1;
    }
    // Keep an index without statistics if it has more equal/IN predicates,
    // and either at least 2 of them or a full index match.
    if left.pseudo && eq > 0 && (left.eq_or_in_count > 1 || left.full_index_match) {
        return 1;
    }
    if right.pseudo && eq < 0 && (right.eq_or_in_count > 1 || right.full_index_match) {
        return -1;
    }
    0
}

/// Go `compareEqOrIn` (master #70768): the candidate with more equality/IN
/// conditions wins, unless its range count exceeds `range_max_count` ("More
/// equality/IN predicates can produce many seeks. Do not give that path an
/// advantage on predicate count alone"); zero disables the threshold.
fn compare_eq_or_in(left: &CandidateMetrics, right: &CandidateMetrics, range_max_count: i64) -> i8 {
    if left.partial_paths || right.partial_paths {
        return 0;
    }
    let too_many = |candidate: &CandidateMetrics| {
        range_max_count > 0 && i64::try_from(candidate.range_count).unwrap_or(i64::MAX) > range_max_count
    };
    if left.eq_or_in_count > right.eq_or_in_count {
        return if too_many(left) { 0 } else { 1 };
    }
    if left.eq_or_in_count < right.eq_or_in_count {
        return if too_many(right) { 0 } else { -1 };
    }
    0
}

/// Go `compareCandidates`. `row_count_ratio_threshold` is the current value of
/// optimizer fix 45132 (Go default 1000); zero disables that comparison.
pub fn compare_candidates(
    left: &CandidateMetrics,
    right: &CandidateMetrics,
    table_pseudo: bool,
    expected_count: f64,
    row_count_ratio_threshold: f64,
    range_max_count: i64,
) -> CandidateComparison {
    if left.multi_valued || right.multi_valued {
        return CandidateComparison::default();
    }
    let matched = compare_bool(left.matches_property, right.matches_property);
    let global = if left.table_path || right.table_path || left.partial_paths || right.partial_paths
    {
        0
    } else {
        compare_bool(left.global, right.global)
    };
    let (access, comparable_access) = compare_col2_len(&left.access_columns, &right.access_columns);
    let scan = compare_bool(left.single_scan, right.single_scan);
    let (scan, comparable_scan) = if scan == 0 && !left.single_scan {
        compare_col2_len(&left.index_columns, &right.index_columns)
    } else {
        (scan, true)
    };
    let risk = compare_risk(left, right);
    let eq = compare_eq_or_in(left, right, range_max_count);
    let predicates = access + risk + eq;
    let total = access + scan + matched + global;
    let result = |ordering, used_row_count_ratio| CandidateComparison {
        ordering,
        used_row_count_ratio,
        winner_is_pseudo: if ordering > 0 {
            left.pseudo
        } else if ordering < 0 {
            right.pseudo
        } else {
            false
        },
    };
    if (left.pseudo || right.pseudo)
        && !table_pseudo
        && (left.eq_or_in_count > 0 || right.eq_or_in_count > 0)
    {
        let mut pseudo = compare_pseudo(left, right, eq);
        // A broad index lookup must not prune a table path on predicate
        // counts alone.
        if (pseudo > 0 && !can_pseudo_winner_prune(left, right, range_max_count))
            || (pseudo < 0 && !can_pseudo_winner_prune(right, left, range_max_count))
        {
            pseudo = 0;
        }
        // Predicate counts alone cannot justify pruning an index on different
        // columns: the winner must also strictly dominate the other path's
        // access conditions.
        if (pseudo > 0 && comparable_access && access > 0 && total >= 0)
            || (pseudo < 0 && comparable_access && access < 0 && total <= 0)
        {
            return result(pseudo, false);
        }
    }
    // For IndexJoin inner candidates, the access count is safe for this
    // strong rule only when the runtime join key has a stable NDV; otherwise
    // the IndexJoin-specific NDV/cost comparison decides.
    let ratio_counts = left
        .get_count_after_access_4_skyline_pruning()
        .zip(right.get_count_after_access_4_skyline_pruning())
        .filter(|(left_count, right_count)| {
            *left_count > 100.0
                && *right_count > 100.0
                && !left.partial_paths
                && !right.partial_paths
                && expected_count == f64::MAX
        });
    let uses_ratio = ratio_counts.is_some();
    if uses_ratio {
        // Go reads the fix control's threshold here.
        tidb_variable::relevant_opt::record_relevant_opt_fix(tidb_fixcontrol::FIX_45132);
    }
    if let Some((left_count, right_count)) = ratio_counts.filter(|_| row_count_ratio_threshold > 0.0) {
        if left_count / right_count > row_count_ratio_threshold && risk <= 0 {
            return result(-1, true);
        }
        if right_count / left_count > row_count_ratio_threshold && risk >= 0 {
            return result(1, true);
        }
    }
    let left_did_not_lose = predicates >= 0 && scan >= 0 && matched >= 0 && global >= 0;
    let right_did_not_lose = predicates <= 0 && scan <= 0 && matched <= 0 && global <= 0;
    if !comparable_access || !comparable_scan {
        if risk > 0 && left_did_not_lose && total >= 0 && predicates > 1 {
            return result(1, uses_ratio);
        }
        if risk < 0 && right_did_not_lose && total <= 0 && predicates < -1 {
            return result(-1, uses_ratio);
        }
        return result(0, uses_ratio);
    }
    if left_did_not_lose && total > 0 {
        return result(1, uses_ratio);
    }
    if right_did_not_lose && total < 0 {
        return result(-1, uses_ratio);
    }
    result(0, uses_ratio)
}

/// Inserts a candidate using Go skylinePruning's reverse traversal.
/// The callback returns None for retained TiFlash paths, which Go skips.
/// Returns whether the candidate survived, whether a pseudo winner was seen,
/// and whether fix45132 was read.
pub fn insert_skyline_candidate<T>(
    candidates: &mut Vec<T>,
    current: T,
    metrics: impl Fn(&T) -> Option<&CandidateMetrics>,
    table_pseudo: bool,
    expected_count: f64,
    row_count_ratio_threshold: f64,
    range_max_count: i64,
    table_row_count: f64,
) -> (bool, bool, bool) {
    let mut missing_stats = false;
    let mut used_ratio = false;
    for index in (0..candidates.len()).rev() {
        let (Some(left), Some(right)) = (metrics(&candidates[index]), metrics(&current)) else {
            continue;
        };
        let result = compare_candidates(
            left,
            right,
            table_pseudo,
            expected_count,
            row_count_ratio_threshold,
            range_max_count,
        );
        // `idx_missing_stats` removes full scan paths later, so a broad
        // index lookup must not set it, even if it won against another index.
        let winner = if result.ordering == 1 { left } else { right };
        missing_stats |= result.winner_is_pseudo && !is_broad_index_lookup(winner, table_row_count, range_max_count);
        used_ratio |= result.used_row_count_ratio;
        if result.ordering > 0 {
            return (false, missing_stats, used_ratio);
        }
        if result.ordering < 0 {
            candidates.remove(index);
        }
    }
    candidates.push(current);
    (true, missing_stats, used_ratio)
}

#[cfg(test)]
mod skyline_tests {
    use super::*;

    /// Go master `TestRangeCount`'s `testRangeCountComparison` (#70768).
    #[test]
    fn range_count_comparison() {
        for (name, threshold, lhs_ranges, rhs_ranges, lhs_count, rhs_count, want) in [
            ("disabled", 0, 1001, 1, 3, 2, 1),
            ("below threshold", 1000, 999, 1, 3, 2, 1),
            ("at threshold", 1000, 1000, 1, 3, 2, 1),
            ("above threshold", 1000, 1001, 1, 3, 2, 0),
            ("both above threshold", 1000, 1001, 1002, 3, 2, 0),
            ("only weaker candidate above threshold", 1000, 1, 1001, 3, 2, 1),
            ("equal predicate counts", 1000, 1001, 1, 2, 2, 0),
            ("single range", 1, 1, 1, 3, 2, 1),
        ] {
            let mut lhs = CandidateMetrics { range_count: lhs_ranges, eq_or_in_count: lhs_count, ..Default::default() };
            let rhs = CandidateMetrics { range_count: rhs_ranges, eq_or_in_count: rhs_count, ..Default::default() };
            assert_eq!(compare_eq_or_in(&lhs, &rhs, threshold), want, "{name}");
            assert_eq!(compare_eq_or_in(&rhs, &lhs, threshold), -want, "{name}");
            // IndexMerge's predicate counts remain incomparable, even with the
            // threshold disabled.
            lhs.partial_paths = true;
            assert_eq!(compare_eq_or_in(&lhs, &rhs, threshold), 0, "{name}");
            assert_eq!(compare_eq_or_in(&rhs, &lhs, threshold), 0, "{name}");
        }
    }

    /// Go master `TestRangeCount`'s `testRangeCountSkyline` (#70768).
    #[test]
    fn range_count_skyline() {
        let candidate = |id: i64, eq_count: usize, range_count: usize, max_count: f64, pseudo: bool| CandidateMetrics {
            access_columns: tidb_planner_util::column_length::Col2Len::from_pairs([(id, -1)]),
            single_scan: true,
            eq_or_in_count: eq_count,
            range_count,
            count_after_access: 10.0,
            max_count_after_access: max_count,
            pseudo,
            ..Default::default()
        };
        for pseudo in [false, true] {
            let (lhs, rhs) = (candidate(1, 3, 1001, 10.0, pseudo), candidate(2, 2, 1, 20.0, pseudo));
            for (threshold, want) in [(0, 1), (1001, 1), (1000, 0)] {
                let result = compare_candidates(&lhs, &rhs, pseudo, 0.0, 1000.0, threshold).ordering;
                assert_eq!(result, want, "pseudo={pseudo} threshold={threshold}");
                let result = compare_candidates(&rhs, &lhs, pseudo, 0.0, 1000.0, threshold).ordering;
                assert_eq!(result, -want, "reversed: pseudo={pseudo} threshold={threshold}");
            }
        }
        // "Missing statistics and equality/IN counts cannot break the tie
        // between incomparable access conditions, regardless of the
        // range-count threshold."
        let (lhs, rhs) = (candidate(1, 3, 1001, 10.0, true), candidate(2, 2, 1, 10.0, false));
        for threshold in [0, 1001, 1000] {
            assert_eq!(compare_candidates(&lhs, &rhs, false, 0.0, 1000.0, threshold).ordering, 0, "{threshold}");
            assert_eq!(compare_candidates(&rhs, &lhs, false, 0.0, 1000.0, threshold).ordering, 0, "{threshold}");
        }
    }

    /// Go master `TestRangeCount`'s `testMissingStatsIndexVsFullScan`: the
    /// missing-statistics index with 2 equal predicates wins over a full
    /// table scan, independent of `tidb_opt_prefer_range_scan` (which
    /// `compareCandidates` no longer reads).
    #[test]
    fn missing_stats_index_vs_full_scan() {
        let idx = CandidateMetrics {
            access_columns: tidb_planner_util::column_length::Col2Len::from_pairs([(1, -1), (2, -1)]),
            eq_or_in_count: 2,
            range_count: 1,
            count_after_access: 10.0,
            pseudo: true,
            ..Default::default()
        };
        let tbl = CandidateMetrics {
            table_path: true,
            single_scan: true,
            range_count: 1,
            count_after_access: 1000.0,
            ..Default::default()
        };
        let result = compare_candidates(&idx, &tbl, false, 0.0, 1000.0, 0);
        assert_eq!(result.ordering, 1);
        assert!(result.winner_is_pseudo);
        assert_eq!(compare_candidates(&tbl, &idx, false, 0.0, 1000.0, 0).ordering, -1);

        // A non-covering missing-statistics index estimated to read most of
        // the table must not prune the full table scan (pingcap/tidb#72561).
        let mut broad = CandidateMetrics { range_count: 900, count_after_access: 900.0, ..idx.clone() };
        let result = compare_candidates(&broad, &tbl, false, 0.0, 1000.0, 0);
        assert_eq!(result.ordering, 0);
        assert!(!result.winner_is_pseudo);
        assert_eq!(compare_candidates(&tbl, &broad, false, 0.0, 1000.0, 0).ordering, 0);
        // The threshold is the default selectivity (0.8) of the table.
        broad.count_after_access = 800.0;
        assert_eq!(compare_candidates(&broad, &tbl, false, 0.0, 1000.0, 0).ordering, 0);
        broad.count_after_access = 799.0;
        let result = compare_candidates(&broad, &tbl, false, 0.0, 1000.0, 0);
        assert_eq!(result.ordering, 1);
        assert!(result.winner_is_pseudo);
        // A covering index has no table lookups, so it may still prune the
        // full table scan.
        broad.count_after_access = 900.0;
        broad.single_scan = true;
        let result = compare_candidates(&broad, &tbl, false, 0.0, 1000.0, 0);
        assert_eq!(result.ordering, 1);
        assert!(result.winner_is_pseudo);

        // A non-covering index with more ranges than
        // tidb_opt_range_max_count is a broad lookup, even if its estimate is
        // small. Zero disables the range count check.
        broad.single_scan = false;
        broad.count_after_access = 10.0;
        assert!(!is_broad_index_lookup(&broad, 1000.0, 1000));
        assert!(is_broad_index_lookup(&broad, 1000.0, 899));
        assert!(!is_broad_index_lookup(&broad, 1000.0, 0));
        assert!(can_pseudo_winner_prune(&broad, &tbl, 1000));
        assert!(!can_pseudo_winner_prune(&broad, &tbl, 899));
        // Against another index, the winner may always prune.
        assert!(can_pseudo_winner_prune(&broad, &idx, 899));
        let result = compare_candidates(&broad, &tbl, false, 0.0, 1000.0, 899);
        assert_eq!(result.ordering, 0);
        assert!(!result.winner_is_pseudo);
    }

    #[test]
    fn index_facts_keep_full_key_filters_and_dnf_equality_counts() {
        use tidb_planner_util::path::AccessPath;
        use tidb_datatype::{Datum, FieldType, FieldTypeCode};
        use tidb_expr::{
            column::Column, constant::Constant, expression::Expression,
            scalar_function::ScalarFunction,
        };
        let ty = FieldType::new(FieldTypeCode::LongLong);
        let a = Column::new(1, ty.clone());
        let c = Column::new(3, ty.clone());
        let func = |name: &str, args| {
            tidb_expr::new_function_base(
                &tidb_exprctx::NoColumns,
                name,
                ty.clone(),
                args,
            ).unwrap()
        };
        let eq = |column: &Column| {
            func(
                "eq",
                vec![
                    Expression::Column(column.clone()),
                    Expression::Constant(Constant::new(Datum::Int(1), ty.clone())),
                ],
            )
        };
        let index = tidb_model::GoShared::new(tidb_model::IndexInfo {
            id: 9,
            columns: vec![tidb_model::IndexColumn::default(); 3].into(),
            ..tidb_model::IndexInfo::default()
        });
        let source = tidb_planner_core_operator_logicalop::DataSource::default();
        let mut path = AccessPath {
            row_estimate: Some(tidb_stats::RowEstimate::new(
                100.0, 20.0, 200.0,
            )),
            // The unresolved middle key cannot erase a later index filter.
            full_idx_cols: Some(vec![Some((a.clone(), -1)), None, Some((c.clone(), 4))]),
            idx_cols: vec![(a.clone(), -1)],
            access_conds: vec![eq(&a)],
            eq_or_in_cond_count: 1,
            index_filters: vec![eq(&c)],
            count_after_index: Some(8.0),
            ..Default::default()
        };
        let facts = get_index_candidate(&source, &index, &path, false, false).unwrap();
        assert_eq!(facts.access_columns, Col2Len::from_pairs([(1, -1)]));
        assert_eq!(facts.index_columns, Col2Len::from_pairs([(1, -1), (3, 4)]));
        assert_eq!(
            (facts.count_after_access, facts.count_after_index),
            (100.0, 8.0)
        );
        assert_eq!(
            (facts.min_count_after_access, facts.max_count_after_access),
            (20.0, 200.0)
        );
        assert!(!facts.full_index_match);
        let two_keys = tidb_model::GoShared::new(index.read().clone_like_go());
        {
            let mut info = two_keys.write();
            let len = info.columns.len();
            info.columns.delete_go(2, len);
        }
        assert!(
            get_index_candidate(&source, &two_keys, &path, false, false)
                .unwrap()
                .full_index_match,
            "non-DNF full match includes index filters, as in Go"
        );

        path.is_dnf_cond = true;
        path.min_access_conds_for_dnf_cond = 2;
        path.eq_or_in_cond_count = 0;
        path.access_conds = vec![func(
            "or",
            vec![
                func("and", vec![eq(&a), eq(&c)]),
                func("and", vec![eq(&a), eq(&c)]),
            ],
        )];
        let facts = get_index_candidate(&source, &two_keys, &path, false, false).unwrap();
        assert_eq!(facts.eq_or_in_count, 2);
        assert!(facts.full_index_match);
        path.access_conds.push(func("not", vec![eq(&c)]));
        let facts = get_index_candidate(&source, &two_keys, &path, false, false).unwrap();
        assert_eq!(facts.eq_or_in_count, 1);
        assert!(!facts.full_index_match);
        path.count_after_index = None;
        assert!(
            get_index_candidate(&source, &index, &path, false, false).is_none(),
            "unavailable filter estimates cannot justify eliminating a candidate"
        );
    }

    fn path(columns: &[i64]) -> CandidateMetrics {
        CandidateMetrics {
            access_columns: Col2Len::from_pairs(columns.iter().map(|id| (*id, -1))),
            index_columns: Col2Len::from_pairs(columns.iter().map(|id| (*id, -1))),
            eq_or_in_count: columns.len(),
            count_after_access: 1.0,
            count_after_index: 1.0,
            min_count_after_access: 1.0,
            max_count_after_access: 1.0,
            ..Default::default()
        }
    }

    #[test]
    fn superset_removes_all_dominated_paths_in_either_order() {
        for order in [
            vec![vec![1], vec![2], vec![1, 2]],
            vec![vec![1, 2], vec![2], vec![1]],
        ] {
            let mut candidates = Vec::new();
            for columns in order {
                insert_skyline_candidate(
                    &mut candidates,
                    path(&columns),
                    |path| Some(path),
                    false,
                    f64::MAX,
                    1000.0,
                    tidb_vardef::defaults::DEF_TIDB_OPT_RANGE_MAX_COUNT,
                    0.0,
                );
            }
            assert_eq!(candidates.len(), 1);
            assert_eq!(candidates[0].eq_or_in_count, 2);
        }
    }

    #[test]
    fn incomparable_paths_and_property_tradeoff_survive_for_costing() {
        let mut candidates = vec![path(&[1])];
        insert_skyline_candidate(
            &mut candidates,
            path(&[2]),
            |path| Some(path),
            false,
            f64::MAX,
            1000.0,
            tidb_vardef::defaults::DEF_TIDB_OPT_RANGE_MAX_COUNT,
            0.0,
        );
        assert_eq!(candidates.len(), 2);
        for candidate in &mut candidates {
            candidate.matches_property = true;
        }
        insert_skyline_candidate(
            &mut candidates,
            path(&[1, 2]),
            |path| Some(path),
            false,
            f64::MAX,
            1000.0,
            tidb_vardef::defaults::DEF_TIDB_OPT_RANGE_MAX_COUNT,
            0.0,
        );
        assert_eq!(candidates.len(), 3);
    }

    #[test]
    fn skipped_tiflash_candidate_is_preserved() {
        let mut candidates = vec![None, Some(path(&[1]))];
        insert_skyline_candidate(
            &mut candidates,
            Some(path(&[1, 2])),
            Option::as_ref,
            false,
            f64::MAX,
            1000.0,
            tidb_vardef::defaults::DEF_TIDB_OPT_RANGE_MAX_COUNT,
            0.0,
        );
        assert_eq!(candidates.len(), 2);
        assert!(candidates[0].is_none());
        assert_eq!(candidates[1].as_ref().unwrap().eq_or_in_count, 2);
    }
}

/// Go `buildPrunedRangeAccessPath` (Go master 1c0793ea576): a path over the
/// same index whose ranges use only its first `cutoff` index columns, so the
/// dropped IN-lists become filters. `Ok(None)` when the cutoff leaves nothing
/// to prune or the ranges are empty; the error is the ranger's.
pub(super) fn build_pruned_range_access_path(
    ds: &tidb_planner_core_operator_logicalop::DataSource,
    original: &tidb_planner_util::path::AccessPath,
    cutoff: usize,
    ctx: &super::dispatch::DispatchContext<'_>,
) -> Result<Option<tidb_planner_util::path::AccessPath>, String> {
    if cutoff >= original.idx_cols.len() || cutoff < 1 {
        return Ok(None);
    }
    let Some(index) = original.index.clone() else {
        return Ok(None);
    };
    let derivation = ctx.access_path_derivation_context();
    let (columns, lengths): (Vec<_>, Vec<_>) = original.idx_cols[..cutoff].iter().cloned().unzip();
    let detached = derivation
        .detach_index_range(&ds.pushed_down_conds, &columns, &lengths)
        .map_err(|error| error.to_string())?;
    if detached.ranges.is_empty() {
        return Ok(None);
    }
    // The hint and plan-cache state carry over, so the pruned alternative
    // obeys the same constraints as the path it was derived from.
    let mut pruned = original.clone();
    pruned.ranges = detached.ranges;
    pruned.access_conds = detached.access_conds;
    pruned.eq_cond_count = detached.eq_cond_count;
    pruned.eq_or_in_cond_count = detached.eq_or_in_count;
    pruned.is_dnf_cond = detached.is_dnf_cond;
    pruned.min_access_conds_for_dnf_cond = 0;
    pruned.correlated_access_count = 0;
    pruned.const_col_ids = Vec::new();
    pruned.const_cols = (0..original.idx_cols.len())
        .map(|i| detached.column_values.get(i).is_some_and(Option::is_some))
        .collect();
    // Go `splitIndexFilterConditions`: the remaining conditions the index
    // covers are index filters, the rest table filters.
    let (index_filters, table_filters): (Vec<_>, Vec<_>) =
        detached.remained_conds.into_iter().partition(|condition| {
            tidb_planner_core_operator_logicalop::data_source::index_covers_condition(
                ds,
                &index,
                condition,
                derivation.opt_prefix_index_single_scan,
            )
        });
    pruned.index_filters = index_filters;
    pruned.table_filters = table_filters;
    // As in `detachCondAndBuildRangeForPath`, the estimate reads the index
    // statistics over the ranges projected to the declared index columns.
    let count = crate::access_path::index_merge::estimate_index_ranges(
        ds,
        &index,
        &pruned.ranges,
        &pruned.idx_cols,
        &derivation,
    )
    .map_err(|error| error.to_string())?;
    // The pruned ranges are a superset of the original ranges, so they
    // cannot return fewer rows. Bound the estimate by the original path's
    // CountAfterAccess, which has already been through
    // adjustCountAfterAccess; adjusting again would charge the selection
    // factor penalty twice.
    let count_after_access = count.est.max(original.count_after_access().unwrap_or(0.0));
    pruned.row_estimate = Some(tidb_stats::RowEstimate::new(
        count_after_access,
        count.min_est,
        count.max_est.max(count_after_access),
    ));
    pruned.count_after_index = Some(if pruned.index_filters.is_empty() {
        count_after_access
    } else {
        (count_after_access * crate::access_path::ordinary::filter_selectivity(ds, &pruned.index_filters, &derivation))
            .max(ds.base.base.stats_info().map_or(0.0, |stats| stats.row_count()))
    });
    Ok(Some(pruned))
}

/// Go `prunedRangeMaxRowsPerSavedSeek`: how many rows a pruned-range
/// alternative may scan, at its upper-bound row estimate, for each seek it
/// saves. The cost model charges a seek as roughly 30 rows
/// (`rangeSeekCostVer2`), so this allows about three times the break-even
/// scan before the alternative is rejected regardless of its estimated cost.
const PRUNED_RANGE_MAX_ROWS_PER_SAVED_SEEK: f64 = 100.0;

/// Go `appendPrunedRangeCandidates`: for each index candidate that survived
/// skyline pruning, alternatives that build ranges on a shorter prefix of its
/// EqOrIn columns, trading seeks for scanned rows. The derived paths go
/// through the same task conversion and cost comparison as every other
/// candidate. Eligibility: a strong equality prefix (EqCondCount > 1) or at
/// least 2 IN-list columns.
pub(super) fn append_pruned_range_candidates(
    ds: &tidb_planner_core_operator_logicalop::DataSource,
    candidates: &[&tidb_planner_util::path::AccessPath],
    ctx: &super::dispatch::DispatchContext<'_>,
) -> Vec<tidb_planner_util::path::AccessPath> {
    let mut derived = Vec::new();
    // Pruned candidates are index scans, which TiFlash and TableSample don't
    // support.
    if ds.prefer_store_type & tidb_planner_core_operator_logicalop::data_source::PREFER_TIFLASH != 0
        || ds.sample_info.is_some()
    {
        return derived;
    }
    for &path in candidates {
        let Some(index) = path.index.as_ref() else {
            continue;
        };
        if !path.partial_index_paths.is_empty()
            || path.is_table_path()
            || path.is_dnf_cond
            || index.read().mv_index
            || path.eq_or_in_cond_count <= path.eq_cond_count
            || path.ranges.len() <= 1
        {
            continue;
        }
        // Count actual IN-list columns using ConstCols: a column in the
        // EqOrIn prefix that is not constant is an IN-list column. This
        // handles `a IN (1,2) AND b = 1 AND c IN (1,2)`, where EqCondCount =
        // 0 but b is an equality.
        let in_list_count = (0..path.eq_or_in_cond_count)
            .filter(|&i| i < path.const_cols.len() && !const_col(path, i))
            .count();
        if (path.eq_cond_count <= 1 && in_list_count < 2) || !index_has_analyzed_stats(ds, index) {
            continue;
        }
        // Full-key ranges on a unique index read at most one row each and
        // can become a [Batch]PointGet; shortening them saves no meaningful
        // work.
        if index.read().unique && ranges_cover_all_index_columns(path, index) {
            continue;
        }
        for cutoff in path.eq_cond_count.max(1)..path.eq_or_in_cond_count {
            // The pruned path is only an optional alternative: if building it
            // fails, log it and keep the original path.
            let pruned = match build_pruned_range_access_path(ds, path, cutoff, ctx) {
                Ok(pruned) => pruned,
                Err(error) => {
                    // Go `logSkippedPrunedRangePath`.
                    tracing::debug!(index = %index.read().name.original(), cutoff, %error, "skip pruned-range alternative");
                    continue;
                }
            };
            let Some(pruned) = pruned.filter(|pruned| pruned.ranges.len() < path.ranges.len()) else {
                continue;
            };
            // Safeguard: the dropped columns may be selective, and the cost
            // comparison trusts the row estimate. Reject the alternative if,
            // at its upper-bound estimate, it scans too many rows for the
            // seeks it saves.
            let saved_seeks = (path.ranges.len() - pruned.ranges.len()) as f64;
            if pruned.row_estimate.is_some_and(|estimate| {
                estimate.max_est > saved_seeks * PRUNED_RANGE_MAX_ROWS_PER_SAVED_SEEK
            }) {
                continue;
            }
            // A required partial order the pruned ranges no longer match is
            // refused where the path loop matches it (Go checks
            // `partialOrderMatchResult` here).
            derived.push(pruned);
        }
    }
    derived
}

/// Go `ConstCols[i]`: the ranger's value or `markConstCol`'s mark.
fn const_col(path: &tidb_planner_util::path::AccessPath, i: usize) -> bool {
    path.const_cols.get(i).copied().unwrap_or(false)
        || path.idx_cols.get(i).is_some_and(|(column, _)| path.const_col_ids.contains(&column.unique_id))
}

/// Go `rangesCoverAllIndexColumns`: whether every range of `path` fixes all
/// columns of its index.
fn ranges_cover_all_index_columns(
    path: &tidb_planner_util::path::AccessPath,
    index: &tidb_model::GoShared<tidb_model::IndexInfo>,
) -> bool {
    let columns = index.read().columns.len();
    path.ranges.iter().all(|range| range.low_val.len() == columns)
}

/// Go `indexHasAnalyzedStats`: the row estimates that decide between the
/// original and the pruned ranges come from the index histogram, and are
/// not reliable enough to trade seeks for scanned rows without it.
fn index_has_analyzed_stats(
    ds: &tidb_planner_core_operator_logicalop::DataSource,
    index: &tidb_model::GoShared<tidb_model::IndexInfo>,
) -> bool {
    let Some(table) = ds.statistic_table.as_ref().filter(|table| !table.hist_coll.pseudo) else {
        return false;
    };
    table.hist_coll.get_index(index.read().id).is_some_and(|stats| {
        stats.read().unwrap_or_else(std::sync::PoisonError::into_inner).is_analyzed()
    })
}
