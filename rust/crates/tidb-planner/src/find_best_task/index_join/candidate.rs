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

//! Go IndexJoin column maps and incomparable-path tie breaker.

use super::path::IndexJoinPathRanges;
use crate::column_length::Col2Len;
use crate::find_best_task::candidate::{compare_candidates, CandidateMetrics};
use tidb_expr::{column::Column, expression::Expression};

/// Go `ExtractCol2Len` followed by `getIndexCandidateForIndexJoin`'s dynamic
/// prefix insertion. Borrowed expressions avoid copying predicate trees merely
/// to combine AccessConds and IndexFilters.
pub fn index_join_column_map<'a>(
    conditions: impl IntoIterator<Item = &'a Expression>,
    columns: &[Column],
    lengths: &[i64],
    lookup_columns: usize,
) -> Col2Len {
    fn extract(
        expression: &Expression,
        columns: &[Column],
        lengths: &[i64],
        pairs: &mut std::collections::BTreeMap<i64, i64>,
    ) {
        match expression {
            Expression::Column(column) => {
                if let Some(offset) = columns
                    .iter()
                    .position(|candidate| candidate.equal_by_expr_and_id(expression))
                {
                    pairs.insert(column.unique_id, lengths[offset]);
                }
            }
            Expression::ScalarFunction(function) => {
                for arg in &function.args {
                    extract(arg, columns, lengths, pairs);
                }
            }
            _ => {}
        }
    }
    let mut pairs = std::collections::BTreeMap::new();
    for expression in conditions {
        extract(expression, columns, lengths, &mut pairs);
    }
    for index in 0..lookup_columns {
        pairs.insert(columns[index].unique_id, lengths[index]);
    }
    Col2Len::from_pairs(pairs)
}

/// Go `isNDVClose`.
pub fn ndv_is_close(left: f64, right: f64) -> bool {
    if left == 0.0 || right == 0.0 {
        return left == right;
    }
    let min = left.min(right);
    let max = left.max(right);
    let difference = (left - right).abs();
    max <= 20.0 || (difference < 200.0 && min >= 20.0) || difference / max < 0.2
}

/// Go `indexJoinPathCmp4UnComparableOnes`, used ONLY after skyline has no
/// winner. NDVs describe the equality prefix in original table statistics.
pub fn prefer_incomparable(
    current: &IndexJoinPathRanges,
    current_ndv: f64,
    best: &IndexJoinPathRanges,
    best_ndv: f64,
) -> bool {
    if !ndv_is_close(current_ndv, best_ndv) {
        return current_ndv > best_ndv;
    }
    if current.used_columns() != best.used_columns() {
        return current.used_columns() > best.used_columns();
    }
    let current_keys = current
        .index_to_key
        .iter()
        .filter(|offset| **offset >= 0)
        .count();
    let best_keys = best
        .index_to_key
        .iter()
        .filter(|offset| **offset >= 0)
        .count();
    if current_keys != best_keys {
        return current_keys > best_keys;
    }
    current_ndv > best_ndv
}

/// Borrowed candidate facts; no physical plan or path definition is copied.
#[derive(Clone, Copy)]
pub struct IndexJoinCandidate<'a> {
    /// Built lookup ranges and key mapping.
    pub ranges: &'a IndexJoinPathRanges,
    /// Existing access-path facts, augmented by the lookup prefix's columns.
    pub metrics: &'a CandidateMetrics,
    /// Equality-prefix NDV from the original table profile (zero for pseudo).
    pub equality_ndv: f64,
}

/// Go `indexJoinPathCompare`: skyline first, then IndexJoin-specific NDV,
/// prefix-width and key-coverage ordering. Returns (current wins, fix45132 read).
pub fn prefer_index_join_path(
    current: Option<IndexJoinCandidate<'_>>,
    best: Option<IndexJoinCandidate<'_>>,
    table_pseudo: bool,
    prefer_range: bool,
    row_count_ratio_threshold: f64,
) -> (bool, bool) {
    let Some(current) = current.filter(|candidate| !candidate.ranges.ranges.is_empty()) else {
        return (false, false);
    };
    let Some(best) = best else {
        return (true, false);
    };
    // Go deliberately uses an empty property, not the enclosing join's LIMIT.
    let comparison = compare_candidates(
        current.metrics,
        best.metrics,
        table_pseudo,
        f64::MAX,
        prefer_range,
        row_count_ratio_threshold,
    );
    let wins = if comparison.ordering == 0 {
        prefer_incomparable(
            current.ranges,
            current.equality_ndv,
            best.ranges,
            best.equality_ndv,
        )
    } else {
        comparison.ordering > 0
    };
    (wins, comparison.used_row_count_ratio)
}
