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
