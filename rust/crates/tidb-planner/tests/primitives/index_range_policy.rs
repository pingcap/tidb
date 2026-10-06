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

//! Direct source-contract tests for the index row-count fast-path predicate.

use tidb_planner::cardinality::index_range_policy::{
    can_skip_index_estimation, is_full_range_including_nulls, IndexRangePolicy, IndexRangeShape,
    RangeBoundKind,
};

fn full_range() -> IndexRangeShape {
    IndexRangeShape::new(
        [RangeBoundKind::Null],
        [RangeBoundKind::MaxValue],
        false,
        false,
    )
}

/// Directly preserves the full-range/null-boundary cases in
/// `TestCanSkipIndexEstimation` (`pkg/planner/cardinality/selectivity_test.go:541`).
#[test]
fn full_range_including_nulls_is_the_only_fast_path_range() {
    assert!(is_full_range_including_nulls(&full_range()));
    assert!(can_skip_index_estimation(
        IndexRangePolicy::default(),
        &[full_range()]
    ));

    let composite_full = IndexRangeShape::new(
        [RangeBoundKind::Null, RangeBoundKind::Null],
        [RangeBoundKind::MaxValue, RangeBoundKind::MaxValue],
        false,
        false,
    );
    assert!(is_full_range_including_nulls(&composite_full));
    let bounded_placeholder = IndexRangeShape::new(
        [RangeBoundKind::Value],
        [RangeBoundKind::Value],
        false,
        false,
    );
    assert!(can_skip_index_estimation(
        IndexRangePolicy::default(),
        &[bounded_placeholder, composite_full]
    ));

    let not_null = IndexRangeShape::new(
        [RangeBoundKind::MinNotNull],
        [RangeBoundKind::MaxValue],
        false,
        false,
    );
    assert!(!is_full_range_including_nulls(&not_null));
    assert!(!can_skip_index_estimation(
        IndexRangePolicy::default(),
        &[not_null]
    ));

    let bounded = IndexRangeShape::new(
        [RangeBoundKind::Value],
        [RangeBoundKind::Value],
        false,
        false,
    );
    assert!(!is_full_range_including_nulls(&bounded));
    assert!(!can_skip_index_estimation(
        IndexRangePolicy::default(),
        &[bounded]
    ));

    let exclusive_null = IndexRangeShape::new(
        [RangeBoundKind::Null],
        [RangeBoundKind::MaxValue],
        true,
        false,
    );
    assert!(!is_full_range_including_nulls(&exclusive_null));
    assert!(!can_skip_index_estimation(
        IndexRangePolicy::default(),
        &[exclusive_null]
    ));
}

#[test]
fn index_metadata_disables_the_full_range_fast_path() {
    assert!(!can_skip_index_estimation(
        IndexRangePolicy {
            has_condition: true,
            is_multi_value: false,
        },
        &[full_range()]
    ));
    assert!(!can_skip_index_estimation(
        IndexRangePolicy {
            has_condition: false,
            is_multi_value: true,
        },
        &[full_range()]
    ));
}

#[test]
fn malformed_or_empty_ranges_never_claim_full_coverage() {
    let empty = IndexRangeShape::new([], [], false, false);
    assert!(!is_full_range_including_nulls(&empty));

    let mismatched = IndexRangeShape::new(
        [RangeBoundKind::Null, RangeBoundKind::Null],
        [RangeBoundKind::MaxValue],
        false,
        false,
    );
    assert!(!is_full_range_including_nulls(&mismatched));

    let exclusive_high = IndexRangeShape::new(
        [RangeBoundKind::Null],
        [RangeBoundKind::MaxValue],
        false,
        true,
    );
    assert!(!is_full_range_including_nulls(&exclusive_high));
}
