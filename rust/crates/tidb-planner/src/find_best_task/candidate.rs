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

use crate::column_length::{compare_col2_len, Col2Len};

/// Facts used by Go `derivePathStatsAndTryHeuristics`, before cost comparison.
#[derive(Clone, Debug, Default)]
pub(crate) struct HeuristicPath {
    pub range_count: usize,
    pub only_points: bool,
    pub unique: bool,
    pub single_scan: bool,
    pub table_filter_count: usize,
    pub access_columns: Col2Len,
}

/// Select the unique point path, unless a covering range strictly refines it
/// and needs fewer than twice as many ranges (Go `stats.go:663`).
pub(crate) fn choose_heuristic_path(paths: &[HeuristicPath]) -> Option<usize> {
    let mut unique = Vec::new();
    let mut single = Vec::new();
    for (index, path) in paths.iter().enumerate() {
        if path.range_count == 0 || (path.only_points && path.unique && path.single_scan) {
            return Some(index);
        }
        if path.only_points {
            if path.unique {
                unique.push(index);
            }
        } else if path.single_scan {
            single.push(index);
        }
    }
    let best = unique
        .iter()
        .copied()
        .min_by_key(|&index| (paths[index].range_count, paths[index].table_filter_count))?;
    let refined = single
        .into_iter()
        .filter(|&index| {
            unique.iter().any(|&unique| {
                compare_col2_len(&paths[index].access_columns, &paths[unique].access_columns)
                    == (1, true)
            })
        })
        .min_by_key(|&index| paths[index].range_count);
    Some(
        refined
            .filter(|&index| paths[index].range_count < 2 * paths[best].range_count)
            .unwrap_or(best),
    )
}

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
    /// Estimated rows after access predicates, before index filtering.
    pub count_after_access: f64,
    /// Estimated rows after index filtering.
    pub count_after_index: f64,
    /// Lower bound on access cardinality.
    pub min_count_after_access: f64,
    /// Worst-case access cardinality.
    pub max_count_after_access: f64,
}

pub(super) fn equality_facts(detached: &crate::ranger::detacher::DetachRangeResult) -> (usize, bool) {
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
    let equal_dnf = detached.is_dnf_cond && detached.access_conds.iter().all(only_equal);
    let minimum_dnf = detached.min_access_conds_for_dnf_cond.max(0) as usize;
    let eq_or_in_count = if !detached.is_dnf_cond || detached.access_conds.is_empty() {
        detached.eq_or_in_count
    } else if equal_dnf {
        minimum_dnf
    } else {
        minimum_dnf.saturating_sub(1)
    };
    (eq_or_in_count, equal_dnf)
}

/// Go `getIndexCandidate`: read both coverage maps and estimate stages from
/// the logically filled path. Missing facts must not eliminate a competitor.
pub(crate) fn index_candidate_metrics(
    source: &crate::logical::DataSource,
    index: &crate::plan_builder::catalog::SourceIndex,
    path: &crate::access_path::IndexPathState,
    single_scan: bool,
    matches_property: bool,
) -> Option<CandidateMetrics> {
    use tidb_expr::expression::Expression;
    let filled = path.filled.as_ref()?;
    let estimate = path.row_estimate?;
    let count_after_index = filled.count_after_index?;
    fn extract<'a>(
        conditions: impl Iterator<Item = &'a Expression>,
        columns: impl Iterator<Item = &'a (tidb_expr::column::Column, i64)> + Clone,
    ) -> Col2Len {
        fn visit<'a>(
            expression: &Expression,
            columns: impl Iterator<Item = &'a (tidb_expr::column::Column, i64)> + Clone,
            result: &mut std::collections::BTreeMap<i64, i64>,
        ) {
            match expression {
                Expression::Column(column) => {
                    if let Some((_, length)) = columns
                        .clone()
                        .find(|(key, _)| key.unique_id == column.unique_id)
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
    let access_columns = extract(filled.detached.access_conds.iter(), filled.columns.iter());
    let index_columns = extract(
        filled
            .detached
            .access_conds
            .iter()
            .chain(&filled.index_filters),
        filled.full_columns.iter().flatten(),
    );
    let detached = &filled.detached;
    let (eq_or_in_count, equal_dnf) = equality_facts(detached);
    let minimum_dnf = detached.min_access_conds_for_dnf_cond.max(0) as usize;
    let full_index_match = if equal_dnf {
        minimum_dnf >= index.columns.len()
    } else {
        detached.eq_or_in_count > 0 && index_columns.len() >= index.columns.len()
    };
    Some(CandidateMetrics {
        access_columns,
        index_columns,
        single_scan,
        multi_valued: index.is_multi_valued,
        global: index.global,
        matches_property,
        pseudo: !source.analyzed_index_ids.contains(&index.id),
        full_index_match,
        eq_or_in_count,
        count_after_access: estimate.est,
        count_after_index,
        min_count_after_access: estimate.min_est,
        max_count_after_access: estimate.max_est,
        ..Default::default()
    })
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

fn compare_pseudo(
    left: &CandidateMetrics,
    right: &CandidateMetrics,
    eq: i8,
    prefer_range: bool,
) -> i8 {
    if !left.pseudo && left.eq_or_in_count > 0 && eq >= 0 {
        return 1;
    }
    if !right.pseudo && right.eq_or_in_count > 0 && eq <= 0 {
        return -1;
    }
    if prefer_range {
        if left.pseudo && eq > 0 && (left.eq_or_in_count > 1 || left.full_index_match) {
            return 1;
        }
        if right.pseudo && eq < 0 && (right.eq_or_in_count > 1 || right.full_index_match) {
            return -1;
        }
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
    prefer_range: bool,
    row_count_ratio_threshold: f64,
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
    let eq = if left.partial_paths || right.partial_paths {
        0
    } else {
        compare_bool(
            left.eq_or_in_count > right.eq_or_in_count,
            left.eq_or_in_count < right.eq_or_in_count,
        )
    };
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
        let pseudo = compare_pseudo(left, right, eq, prefer_range);
        if (pseudo > 0 && total >= 0) || (pseudo < 0 && total <= 0) {
            return result(pseudo, false);
        }
    }
    let uses_ratio = left.count_after_access > 100.0
        && right.count_after_access > 100.0
        && !left.partial_paths
        && !right.partial_paths
        && expected_count == f64::MAX;
    if uses_ratio && row_count_ratio_threshold > 0.0 {
        if left.count_after_access / right.count_after_access > row_count_ratio_threshold
            && risk <= 0
        {
            return result(-1, true);
        }
        if right.count_after_access / left.count_after_access > row_count_ratio_threshold
            && risk >= 0
        {
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
    prefer_range: bool,
    row_count_ratio_threshold: f64,
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
            prefer_range,
            row_count_ratio_threshold,
        );
        missing_stats |= result.winner_is_pseudo;
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

    #[test]
    fn point_heuristics_refine_unique_reads_only_below_double_range_count() {
        let full = HeuristicPath {
            range_count: 1,
            single_scan: true,
            ..Default::default()
        };
        let unique = HeuristicPath {
            range_count: 2,
            only_points: true,
            unique: true,
            table_filter_count: 1,
            access_columns: Col2Len::from_pairs([(1, -1)]),
            ..Default::default()
        };
        // Go chooses unique points before costing, even for a tiny table.
        assert_eq!(
            choose_heuristic_path(&[full.clone(), unique.clone()]),
            Some(1)
        );
        let mut refined = HeuristicPath {
            range_count: 3,
            single_scan: true,
            access_columns: Col2Len::from_pairs([(1, -1), (2, -1)]),
            ..Default::default()
        };
        assert_eq!(
            choose_heuristic_path(&[full.clone(), unique.clone(), refined.clone()]),
            Some(2)
        );
        refined.range_count = 4;
        assert_eq!(choose_heuristic_path(&[full, unique, refined]), Some(1));
    }

    #[test]
    fn index_facts_keep_full_key_filters_and_dnf_equality_counts() {
        use crate::access_path::{IndexPathState, ordinary::FilledIndexPath};
        use crate::plan_builder::catalog::{SourceIndex, SourceIndexColumn};
        use tidb_datatype::{Datum, FieldType, FieldTypeCode};
        use tidb_expr::{
            column::Column, constant::Constant, expression::Expression,
            scalar_function::ScalarFunction,
        };
        let ty = FieldType::new(FieldTypeCode::LongLong);
        let a = Column::new(1, ty.clone());
        let c = Column::new(3, ty.clone());
        let func = |name: &str, args| {
            Expression::ScalarFunction(ScalarFunction::new(
                tidb_ast::CiString::new(name),
                ty.clone(),
                args,
            ))
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
        let index = SourceIndex {
            id: 9,
            columns: vec![SourceIndexColumn::default(); 3],
            ..Default::default()
        };
        let source = crate::logical::DataSource::default();
        let mut path = IndexPathState {
            row_estimate: Some(crate::cardinality::row_count_column::RowEstimate::new(
                100.0, 20.0, 200.0,
            )),
            filled: Some(FilledIndexPath {
                columns: vec![(a.clone(), -1)],
                // The unresolved middle key cannot erase a later index filter.
                full_columns: vec![Some((a.clone(), -1)), None, Some((c.clone(), 4))],
                detached: crate::ranger::detacher::DetachRangeResult {
                    access_conds: vec![eq(&a)],
                    eq_or_in_count: 1,
                    ..Default::default()
                },
                index_filters: vec![eq(&c)],
                table_filters: Vec::new(),
                count_after_index: Some(8.0),
            }),
            ..Default::default()
        };
        let facts = index_candidate_metrics(&source, &index, &path, false, false).unwrap();
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
        let mut two_keys = index.clone();
        two_keys.columns.truncate(2);
        assert!(
            index_candidate_metrics(&source, &two_keys, &path, false, false)
                .unwrap()
                .full_index_match,
            "non-DNF full match includes index filters, as in Go"
        );

        let detached = &mut path.filled.as_mut().unwrap().detached;
        detached.is_dnf_cond = true;
        detached.min_access_conds_for_dnf_cond = 2;
        detached.eq_or_in_count = 0;
        detached.access_conds = vec![func(
            "or",
            vec![
                func("and", vec![eq(&a), eq(&c)]),
                func("and", vec![eq(&a), eq(&c)]),
            ],
        )];
        let facts = index_candidate_metrics(&source, &two_keys, &path, false, false).unwrap();
        assert_eq!(facts.eq_or_in_count, 2);
        assert!(facts.full_index_match);
        path.filled
            .as_mut()
            .unwrap()
            .detached
            .access_conds
            .push(func("not", vec![eq(&c)]));
        let facts = index_candidate_metrics(&source, &two_keys, &path, false, false).unwrap();
        assert_eq!(facts.eq_or_in_count, 1);
        assert!(!facts.full_index_match);
        path.filled.as_mut().unwrap().count_after_index = None;
        assert!(
            index_candidate_metrics(&source, &index, &path, false, false).is_none(),
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
                    false,
                    1000.0,
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
            false,
            1000.0,
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
            false,
            1000.0,
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
            false,
            1000.0,
        );
        assert_eq!(candidates.len(), 2);
        assert!(candidates[0].is_none());
        assert_eq!(candidates[1].as_ref().unwrap().eq_or_in_count, 2);
    }
}
