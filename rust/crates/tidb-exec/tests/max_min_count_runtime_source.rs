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
use tidb_exec::aggregate::runtime::{fold_values, MaxMinCountState};
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
