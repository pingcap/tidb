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

//! Full-join row-count estimation from `pkg/planner/cardinality/join.go`.
//!
//! The Go entrypoint receives a `PlanContext`, expression columns, schemas, and
//! `StatsInfo` objects.  Those owners do not exist in the seed planner yet, so
//! this leaf keeps the arithmetic at a typed statistics boundary.  The caller
//! supplies the already-estimated NDV and matched-key lengths; the future
//! planner can adapt its real column/statistics owners without changing the
//! join formula.

/// NDV and matched-prefix information for one join-key side.
///
/// `key_len` is the number of source join keys. `matched_len` is the number of
/// columns covered by a matching GroupNDV (or the source-shaped fallback
/// estimate). Keeping both values is necessary because Go uses the key length
/// to select equi versus non-equi keys, but uses the matched length only in the
/// 0.9 correlation adjustment.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct JoinKeyEstimate {
    /// Estimated number of distinct values for this key tuple.
    pub ndv: f64,
    /// Number of keys matched by the source GroupNDV lookup.
    pub matched_len: usize,
    /// Number of join keys in the source slice.
    pub key_len: usize,
}

impl JoinKeyEstimate {
    /// Creates a source-shaped join-key estimate.
    #[must_use]
    pub const fn new(ndv: f64, matched_len: usize, key_len: usize) -> Self {
        Self {
            ndv,
            matched_len,
            key_len,
        }
    }

    /// Creates an empty key tuple, equivalent to a nil Go key slice.
    #[must_use]
    pub const fn empty() -> Self {
        Self::new(1.0, 1, 0)
    }
}

/// Source-shaped inputs for [`estimate_full_join_row_count`].
///
/// The `join_reorder_threshold` is the integer session variable
/// `TiDBOptJoinReorderThreshold`; a non-positive value disables the 0.9
/// correlation adjustment exactly as in Go. `non_equi_*` are the NA-key
/// estimates used when both equi-key slices are empty.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FullJoinRowCountInput {
    /// Estimated rows on the left side.
    pub left_row_count: f64,
    /// Estimated rows on the right side.
    pub right_row_count: f64,
    /// Whether the join has no join predicates.
    pub is_cartesian: bool,
    /// Equi-join keys and their NDV estimate on the left.
    pub left_join_keys: JoinKeyEstimate,
    /// Equi-join keys and their NDV estimate on the right.
    pub right_join_keys: JoinKeyEstimate,
    /// Null-aware/non-equi keys and their NDV estimate on the left.
    pub left_non_equi_keys: JoinKeyEstimate,
    /// Null-aware/non-equi keys and their NDV estimate on the right.
    pub right_non_equi_keys: JoinKeyEstimate,
    /// `TiDBOptJoinReorderThreshold` from the source session variables.
    pub join_reorder_threshold: i32,
}

// Keep Go's floating-point max behavior at the source boundary. In
// particular, a NaN NDV must not be silently discarded by Rust's primitive
// `max`, and the signed-zero result is observable when an estimate is zero.
fn go_max(x: f64, y: f64) -> f64 {
    if x.is_nan() || y.is_nan() {
        return f64::NAN;
    }
    if x == 0.0 && x == y {
        return if x.is_sign_negative() { y } else { x };
    }
    if x > y {
        x
    } else {
        y
    }
}

/// Estimates the row count of a full join.
///
/// This is the arithmetic body of Go's `EstimateFullJoinRowCount`: Cartesian
/// products bypass NDV estimation; if either equi-key side is non-empty, both
/// equi-key estimates are used, otherwise both non-equi estimates are used.
/// The denominator is the larger key NDV, and enabling join reorder applies
/// one `0.9` factor per remaining left-side equi key.
#[must_use]
pub fn estimate_full_join_row_count(input: &FullJoinRowCountInput) -> f64 {
    if input.is_cartesian {
        return input.left_row_count * input.right_row_count;
    }

    let use_equi_keys = input.left_join_keys.key_len > 0 || input.right_join_keys.key_len > 0;
    let (left_keys, right_keys) = if use_equi_keys {
        (input.left_join_keys, input.right_join_keys)
    } else {
        (input.left_non_equi_keys, input.right_non_equi_keys)
    };

    let count =
        input.left_row_count * input.right_row_count / go_max(left_keys.ndv, right_keys.ndv);
    if input.join_reorder_threshold <= 0 {
        return count;
    }

    // This deliberately uses the left equi-key length, even when the
    // non-equi fallback supplied the NDVs: that is the exact source expression
    // `len(leftJoinKeys)-max(leftColCnt,rightColCnt)`.
    let remaining_keys = input.left_join_keys.key_len as f64
        - go_max(
            input.left_join_keys.matched_len as f64,
            input.right_join_keys.matched_len as f64,
        );
    count * 0.9_f64.powf(remaining_keys)
}
