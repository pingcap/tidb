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

//! Dependency-closed index-range fast-path policy from
//! `pkg/planner/cardinality/row_count_index.go`.
//!
//! TiDB can skip expensive histogram estimation only for a true full range
//! that includes NULLs, and only for ordinary (non-partial, non-MV) indexes.
//! The executor's statistics boundary calls this policy before it queues an
//! evicted index histogram; the leaf carries only the normalized range and
//! index metadata needed for that decision.

/// The normalized kind of a range endpoint needed by the source fast path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RangeBoundKind {
    /// SQL NULL, which is the inclusive low endpoint for a full index range.
    Null,
    /// The first non-NULL value; this excludes NULL rows.
    MinNotNull,
    /// Positive infinity used as the inclusive high endpoint.
    MaxValue,
    /// Any ordinary scalar value.
    Value,
}

/// A normalized index range shape. Values themselves remain owned by the
/// caller because the fast-path decision only inspects endpoint kinds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexRangeShape {
    low: Vec<RangeBoundKind>,
    high: Vec<RangeBoundKind>,
    low_exclude: bool,
    high_exclude: bool,
}

impl IndexRangeShape {
    /// Creates a range shape from endpoint kinds and inclusivity flags.
    #[must_use]
    pub fn new(
        low: impl IntoIterator<Item = RangeBoundKind>,
        high: impl IntoIterator<Item = RangeBoundKind>,
        low_exclude: bool,
        high_exclude: bool,
    ) -> Self {
        Self {
            low: low.into_iter().collect(),
            high: high.into_iter().collect(),
            low_exclude,
            high_exclude,
        }
    }

    /// Returns the low endpoint kinds in index-column order.
    #[must_use]
    pub fn low(&self) -> &[RangeBoundKind] {
        &self.low
    }

    /// Returns the high endpoint kinds in index-column order.
    #[must_use]
    pub fn high(&self) -> &[RangeBoundKind] {
        &self.high
    }

    /// Returns whether the low endpoint is exclusive.
    #[must_use]
    pub const fn low_exclude(&self) -> bool {
        self.low_exclude
    }

    /// Returns whether the high endpoint is exclusive.
    #[must_use]
    pub const fn high_exclude(&self) -> bool {
        self.high_exclude
    }
}

/// The index metadata used by `canSkipIndexEstimation`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IndexRangePolicy {
    /// A non-empty partial-index predicate means the index does not cover all
    /// table rows, even when the key range itself is full.
    pub has_condition: bool,
    /// Multi-valued indexes can contain multiple entries per row and therefore
    /// cannot use the realtime-row-count fast path.
    pub is_multi_value: bool,
}

/// Returns whether a single range covers every value, including NULLs.
///
/// This mirrors `isFullRangeIncludingNulls`: the range must have matching,
/// non-empty endpoint widths; both endpoints must be inclusive; every low
/// endpoint must be `NULL`; and every high endpoint must be `MaxValue`.
#[must_use]
pub fn is_full_range_including_nulls(range: &IndexRangeShape) -> bool {
    if range.low.is_empty() || range.low.len() != range.high.len() {
        return false;
    }
    if range.low_exclude || range.high_exclude {
        return false;
    }
    range.low.iter().all(|kind| *kind == RangeBoundKind::Null)
        && range
            .high
            .iter()
            .all(|kind| *kind == RangeBoundKind::MaxValue)
}

/// Returns whether index-row estimation may use the realtime-count fast path.
///
/// Go's caller is `GetRowCountByIndexRanges`
/// (`pkg/planner/cardinality/row_count_index.go:53`), which short-circuits
/// histogram estimation when a range covers the whole index including NULLs.
#[must_use]
pub fn can_skip_index_estimation(policy: IndexRangePolicy, ranges: &[IndexRangeShape]) -> bool {
    can_skip_full_ranges(policy, ranges.iter().map(is_full_range_including_nulls))
}

fn can_skip_full_ranges(
    policy: IndexRangePolicy,
    mut full_ranges: impl Iterator<Item = bool>,
) -> bool {
    !policy.has_condition && !policy.is_multi_value && full_ranges.any(|full| full)
}

/// The same fast-path policy over borrowed datum bounds. Both storage and
/// planner ranges can use it without allocating normalized endpoint vectors.
pub fn can_skip_datum_index_estimation<'a>(
    policy: IndexRangePolicy,
    ranges: impl IntoIterator<
        Item = (
            &'a [tidb_datatype::Datum],
            &'a [tidb_datatype::Datum],
            bool,
            bool,
        ),
    >,
) -> bool {
    can_skip_full_ranges(
        policy,
        ranges
            .into_iter()
            .map(|(low, high, low_exclude, high_exclude)| {
                !low.is_empty()
                    && low.len() == high.len()
                    && !low_exclude
                    && !high_exclude
                    && low.iter().all(tidb_datatype::Datum::is_null)
                    && high
                        .iter()
                        .all(|value| matches!(value, tidb_datatype::Datum::MaxValue))
            }),
    )
}
