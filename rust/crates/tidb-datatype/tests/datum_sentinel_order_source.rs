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

//! Direct source vectors for Datum range-sentinel ordering and representation.

use std::cmp::Ordering;

use tidb_datatype::{Datum, DatumKind};

fn signed(ordering: Ordering) -> i8 {
    match ordering {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

/// Exact source table: `pkg/types/compare_test.go::TestCompareDatum`.
#[test]
fn test_compare_datum() {
    let rows = [
        (Datum::max_value(), Datum::new_string("00:00:00"), 1),
        (Datum::min_not_null(), Datum::new_string("00:00:00"), -1),
        (Datum::default(), Datum::new_string("00:00:00"), -1),
        (Datum::default(), Datum::default(), 0),
        (Datum::min_not_null(), Datum::min_not_null(), 0),
        (Datum::max_value(), Datum::max_value(), 0),
        (Datum::default(), Datum::min_not_null(), -1),
        (Datum::min_not_null(), Datum::max_value(), -1),
    ];

    for (index, (left, right, expected)) in rows.into_iter().enumerate() {
        let forward = left
            .compare_sentinel_order(&right)
            .expect("every source row contains NULL or a range sentinel");
        assert_eq!(signed(forward), expected, "source row {index}");

        let reverse = right
            .compare_sentinel_order(&left)
            .expect("every reversed source row contains NULL or a range sentinel");
        assert_eq!(signed(reverse), -expected, "reversed source row {index}");
    }
}

/// Source anchors: `KindMinNotNull`, `KindMaxValue`, `SetMinNotNull`,
/// `MinNotNullDatum`, and `MaxValueDatum` in `pkg/types/datum.go`.
#[test]
fn sentinel_constructors_kinds_and_setter_share_the_datum_authority() {
    assert_eq!(Datum::min_not_null().kind(), DatumKind::MinNotNull);
    assert_eq!(Datum::max_value().kind(), DatumKind::MaxValue);
    assert!(Datum::min_not_null().is_min_not_null());
    assert!(Datum::max_value().is_max_value());
    assert!(Datum::min_not_null().sql_string().is_err());
    assert!(Datum::max_value().sql_string().is_err());
    assert_eq!(Datum::min_not_null().label(), "SKIP:15");
    assert_eq!(Datum::max_value().label(), "SKIP:16");
    assert_eq!(
        Datum::min_not_null().sql_string().unwrap_err().to_string(),
        "cannot convert <nil>(type <nil>) to string"
    );

    let mut value = Datum::new_string("payload that must be discarded");
    value.set_min_not_null();
    assert_eq!(value, Datum::min_not_null());
    assert_eq!(value.as_raw_bytes(), None);
    assert_eq!(value.collation(), None);
}

/// Ordinary datum comparison is still context/collation-owned. This guard
/// prevents the sentinel slice from growing an invented scalar total order.
#[test]
fn ordinary_pairs_are_left_to_the_context_aware_comparer() {
    assert_eq!(
        Datum::new_int(1).compare_sentinel_order(&Datum::new_int(2)),
        None
    );
}
