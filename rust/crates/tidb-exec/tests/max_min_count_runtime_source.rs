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

//! Regression coverage for Go `pkg/executor/aggfuncs/max_min_count.go`.
//!
//! MAX_COUNT/MIN_COUNT return the number of non-NULL rows tied at the selected
//! extreme. The pair state is deliberately separate from `MaxMinState`: the
//! value is needed while updating/merging, but the SQL result is always an
//! integer count and is zero for an empty or all-NULL group.

use tidb_datatype::{Collation, Datum};
use tidb_exec::aggregate::runtime::{fold_values, MaxMinCountSlidingState, MaxMinCountState};
use tidb_planner::aggregation_descriptor::AggregateKind;

fn fold(kind: AggregateKind, values: &[Datum]) -> i64 {
    match fold_values(kind, false, values, 4).expect("max/min count folds") {
        Datum::Int(value) => value,
        other => panic!("expected count result, got {other:?}"),
    }
}

#[test]
fn max_min_count_counts_only_rows_tied_at_the_extreme() {
    let values = [
        Datum::Int(0),
        Datum::Int(0),
        Datum::Int(1),
        Datum::Int(4),
        Datum::Int(4),
        Datum::Int(4),
        Datum::Null,
    ];
    assert_eq!(fold(AggregateKind::MaxCount, &values), 3);
    assert_eq!(fold(AggregateKind::MinCount, &values), 2);
    assert_eq!(fold(AggregateKind::MaxCount, &[]), 0);
    assert_eq!(fold(AggregateKind::MinCount, &[Datum::Null]), 0);
}

#[test]
fn max_min_count_preserves_typed_comparison_domains() {
    let unsigned = [Datum::UInt(1), Datum::UInt(9), Datum::UInt(9), Datum::UInt(2)];
    assert_eq!(fold(AggregateKind::MaxCount, &unsigned), 2);
    assert_eq!(fold(AggregateKind::MinCount, &unsigned), 1);

    let decimals = [
        Datum::Decimal(tidb_datatype::Decimal::from_int(3)),
        Datum::Decimal(tidb_datatype::Decimal::from_int(1)),
        Datum::Decimal(tidb_datatype::Decimal::from_int(1)),
    ];
    assert_eq!(fold(AggregateKind::MaxCount, &decimals), 1);
    assert_eq!(fold(AggregateKind::MinCount, &decimals), 2);

    let strings = [
        Datum::new_collation_string(b"B", Collation::Utf8Mb4GeneralCi),
        Datum::new_collation_string(b"a", Collation::Utf8Mb4GeneralCi),
        Datum::new_collation_string(b"b", Collation::Utf8Mb4GeneralCi),
        Datum::new_collation_string(b"A", Collation::Utf8Mb4GeneralCi),
    ];
    assert_eq!(fold(AggregateKind::MaxCount, &strings), 2);
    assert_eq!(fold(AggregateKind::MinCount, &strings), 2);
}

#[test]
fn max_min_count_merges_winner_and_tie_count() {
    let mut left = MaxMinCountState::new(AggregateKind::MaxCount).unwrap();
    left.update(&Datum::Int(4)).unwrap();
    left.update(&Datum::Int(4)).unwrap();
    left.update(&Datum::Int(2)).unwrap();

    let mut right = MaxMinCountState::new(AggregateKind::MaxCount).unwrap();
    right.update(&Datum::Int(4)).unwrap();
    right.update(&Datum::Int(1)).unwrap();
    left.merge_from(&right).unwrap();
    assert_eq!(left.result(), 3);

    let mut minimum = MaxMinCountState::new(AggregateKind::MinCount).unwrap();
    minimum.update(&Datum::Int(2)).unwrap();
    minimum.update(&Datum::Int(0)).unwrap();
    minimum.update(&Datum::Int(0)).unwrap();
    assert!(minimum
        .merge_from(&left)
        .is_err(), "different max/min count kinds must not merge");
    minimum.reset();
    assert_eq!(minimum.result(), 0);
}

#[test]
fn max_min_count_sliding_state_keeps_equal_indices_through_expiry() {
    // Source: pkg/executor/aggfuncs/func_max_min_count.go sliding deque and
    // TestMaxMinCountSlidingWindow. Equal extrema stay grouped so expiring
    // one occurrence leaves the remaining ties visible.
    let mut max = MaxMinCountSlidingState::new(AggregateKind::MaxCount).unwrap();
    max.update(
        0,
        &[
            Datum::Int(1),
            Datum::Int(1),
            Datum::Int(2),
            Datum::Int(2),
        ],
    )
    .unwrap();
    assert_eq!(max.result(), 2);
    assert!(!max.is_null());

    // Go Slide enqueues incoming rows first, then removes rows at the old
    // frame boundary. The new frame is [2, 5], whose max is tied twice.
    max.slide(
        4,
        &[Datum::Null, Datum::Int(2)],
        Some(1),
    )
    .unwrap();
    assert_eq!(max.result(), 3);

    let mut min = MaxMinCountSlidingState::new(AggregateKind::MinCount).unwrap();
    min.update(0, &[Datum::Null, Datum::Int(3), Datum::Int(1)])
        .unwrap();
    assert_eq!(min.result(), 1);
    min.slide(3, &[Datum::Int(1), Datum::Int(1)], Some(1))
        .unwrap();
    assert_eq!(min.result(), 3);
}

#[test]
fn max_min_count_sliding_state_resets_empty_and_rejects_mixed_domains() {
    let mut state = MaxMinCountSlidingState::new(AggregateKind::MaxCount).unwrap();
    state.update(10, &[Datum::Null]).unwrap();
    assert!(state.is_null());
    assert_eq!(state.result(), 0);

    state
        .slide(11, &[Datum::Int(4), Datum::Int(4)], None)
        .unwrap();
    assert_eq!(state.result(), 2);
    state.reset();
    assert!(state.is_null());
    assert_eq!(state.result(), 0);

    let err = state
        .update(
            0,
            &[
                Datum::new_collation_string(b"a", Collation::Utf8Mb4GeneralCi),
                Datum::new_collation_string(b"b", Collation::Utf8Mb4Bin),
            ],
        )
        .expect_err("mixed string collations must stay a typed-domain error");
    assert!(matches!(
        err,
        tidb_exec::ExecError::Unsupported("MAX/MIN string collation mismatch")
    ));
}
