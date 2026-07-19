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

//! Exact signed-integer table ranges for one clustered `BIGINT` handle.
//!
//! TiDB's `BuildTableRange` intersects ordered CNF access conditions and
//! converts open integer endpoints to concrete signed table-handle bounds.
//! This module ports that bounded behavior for the six comparisons already
//! admitted by the read-only planner. Closed integer intervals eliminate
//! separate infinity and exclusivity cases: strict bounds use the adjacent
//! integer when it exists and become empty at `i64::MIN` or `i64::MAX`.

use crate::physical_selection::{BigIntComparison, ComparisonOp, ComparisonOperand};

/// One nonempty inclusive interval in signed TiKV table-handle order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SignedBigIntRange {
    start: i64,
    end: i64,
}

impl SignedBigIntRange {
    /// Constructs a nonempty closed interval, or returns `None` for reversed
    /// endpoints.
    #[must_use]
    pub const fn new(start: i64, end: i64) -> Option<Self> {
        if start <= end {
            Some(Self { start, end })
        } else {
            None
        }
    }

    /// Returns the full signed clustered-handle interval.
    #[must_use]
    pub const fn full() -> Self {
        Self {
            start: i64::MIN,
            end: i64::MAX,
        }
    }

    /// Returns the inclusive lower handle.
    #[must_use]
    pub const fn start(self) -> i64 {
        self.start
    }

    /// Returns the inclusive upper handle.
    #[must_use]
    pub const fn end(self) -> i64 {
        self.end
    }

    fn intersection(self, other: Self) -> Option<Self> {
        Self::new(self.start.max(other.start), self.end.min(other.end))
    }
}

/// Exact access ranges plus conditions that still require Selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusteredSignedBigIntRangeResult {
    ranges: Vec<SignedBigIntRange>,
    access_condition_indices: Vec<usize>,
    residual_conditions: Vec<BigIntComparison>,
}

impl ClusteredSignedBigIntRangeResult {
    /// Returns sorted, nonempty, nonoverlapping inclusive handle ranges.
    #[must_use]
    pub fn ranges(&self) -> &[SignedBigIntRange] {
        &self.ranges
    }

    /// Returns source-order indices of comparisons detached into ranges.
    #[must_use]
    pub fn access_condition_indices(&self) -> &[usize] {
        &self.access_condition_indices
    }

    /// Returns source-order conditions not targeting the clustered handle.
    ///
    /// Callers must retain these conditions in the physical Selection. Access
    /// conditions are exact over the non-null signed handle and need not be
    /// evaluated a second time.
    #[must_use]
    pub fn residual_conditions(&self) -> &[BigIntComparison] {
        &self.residual_conditions
    }
}

/// Detaches exact clustered-handle comparisons and intersects them as ordered
/// `AND` conditions.
///
/// Every comparison targeting another scan input remains a residual condition
/// in its original order. Literal-left comparisons are canonicalized by
/// reversing their operator, so `7 < id` and `id > 7` produce identical
/// ranges. An absent access condition yields the full signed range, while a
/// contradiction yields no ranges.
#[must_use]
pub fn detach_clustered_signed_bigint_ranges(
    conditions: &[BigIntComparison],
    clustered_input_offset: u32,
) -> ClusteredSignedBigIntRangeResult {
    let mut ranges = vec![SignedBigIntRange::full()];
    let mut access_condition_indices = Vec::new();
    let mut residual_conditions = Vec::new();

    for (index, condition) in conditions.iter().copied().enumerate() {
        if condition.input_offset() != clustered_input_offset {
            residual_conditions.push(condition);
            continue;
        }

        access_condition_indices.push(index);
        let (op, value) = canonical_comparison(condition);
        ranges = intersect_ranges(&ranges, &ranges_for_comparison(op, value));
    }

    ClusteredSignedBigIntRangeResult {
        ranges,
        access_condition_indices,
        residual_conditions,
    }
}

fn canonical_comparison(condition: BigIntComparison) -> (ComparisonOp, i64) {
    match (condition.lhs(), condition.rhs()) {
        (ComparisonOperand::InputOffset(_), ComparisonOperand::Int(value)) => {
            (condition.op(), value)
        }
        (ComparisonOperand::Int(value), ComparisonOperand::InputOffset(_)) => {
            (reverse(condition.op()), value)
        }
        _ => unreachable!("BigIntComparison validates one input and one literal"),
    }
}

const fn reverse(op: ComparisonOp) -> ComparisonOp {
    match op {
        ComparisonOp::Lt => ComparisonOp::Gt,
        ComparisonOp::Le => ComparisonOp::Ge,
        ComparisonOp::Gt => ComparisonOp::Lt,
        ComparisonOp::Ge => ComparisonOp::Le,
        ComparisonOp::Eq => ComparisonOp::Eq,
        ComparisonOp::Ne => ComparisonOp::Ne,
    }
}

fn ranges_for_comparison(op: ComparisonOp, value: i64) -> Vec<SignedBigIntRange> {
    match op {
        ComparisonOp::Eq => vec![SignedBigIntRange {
            start: value,
            end: value,
        }],
        ComparisonOp::Ne => {
            let mut ranges = Vec::with_capacity(2);
            if let Some(end) = value.checked_sub(1) {
                ranges.push(SignedBigIntRange {
                    start: i64::MIN,
                    end,
                });
            }
            if let Some(start) = value.checked_add(1) {
                ranges.push(SignedBigIntRange {
                    start,
                    end: i64::MAX,
                });
            }
            ranges
        }
        ComparisonOp::Lt => value.checked_sub(1).map_or_else(Vec::new, |end| {
            vec![SignedBigIntRange {
                start: i64::MIN,
                end,
            }]
        }),
        ComparisonOp::Le => vec![SignedBigIntRange {
            start: i64::MIN,
            end: value,
        }],
        ComparisonOp::Gt => value.checked_add(1).map_or_else(Vec::new, |start| {
            vec![SignedBigIntRange {
                start,
                end: i64::MAX,
            }]
        }),
        ComparisonOp::Ge => vec![SignedBigIntRange {
            start: value,
            end: i64::MAX,
        }],
    }
}

fn intersect_ranges(
    left: &[SignedBigIntRange],
    right: &[SignedBigIntRange],
) -> Vec<SignedBigIntRange> {
    let mut intersections = Vec::with_capacity(left.len().saturating_add(right.len()));
    let mut left_index = 0;
    let mut right_index = 0;

    while left_index < left.len() && right_index < right.len() {
        let left_range = left[left_index];
        let right_range = right[right_index];
        if let Some(intersection) = left_range.intersection(right_range) {
            intersections.push(intersection);
        }

        if left_range.end < right_range.end {
            left_index += 1;
        } else {
            right_index += 1;
        }
    }

    normalize_ranges(intersections)
}

fn normalize_ranges(ranges: Vec<SignedBigIntRange>) -> Vec<SignedBigIntRange> {
    let mut normalized: Vec<SignedBigIntRange> = Vec::with_capacity(ranges.len());
    for range in ranges {
        let Some(previous) = normalized.last_mut() else {
            normalized.push(range);
            continue;
        };
        let touches_previous = previous
            .end
            .checked_add(1)
            .is_some_and(|next| range.start <= next);
        if range.start <= previous.end || touches_previous {
            previous.end = previous.end.max(range.end);
        } else {
            normalized.push(range);
        }
    }
    normalized
}
