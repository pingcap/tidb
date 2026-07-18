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

#![allow(missing_docs)]

use tidb_datatype::{Collation, Datum, Decimal};
use tidb_exec::aggregate::runtime::{fold_values, MaxMinState};
use tidb_planner::aggregation_descriptor::AggregateKind;

fn assert_update_vector(kind: AggregateKind, values: &[Datum], expected: Datum) {
    let mut state = MaxMinState::new(kind).unwrap();
    for value in values {
        state.update(value).unwrap();
    }
    assert_eq!(state.result(), expected);
}

fn assert_merge_vector(kind: AggregateKind, values: &[Datum], expected: Datum) {
    let split = values.len() / 2;
    let mut destination = MaxMinState::new(kind).unwrap();
    for value in &values[..split] {
        destination.update(value).unwrap();
    }
    let mut source = MaxMinState::new(kind).unwrap();
    for value in &values[split..] {
        source.update(value).unwrap();
    }
    destination.merge_from(&source).unwrap();
    assert_eq!(destination.result(), expected);
}

#[test]
fn max_min_update_vectors_cover_the_complete_executor_datum_domain() {
    let vectors = [
        (
            vec![Datum::Null, Datum::Int(0), Datum::Int(4), Datum::Int(2)],
            Datum::Int(4),
            Datum::Int(0),
        ),
        (
            vec![Datum::Null, Datum::UInt(0), Datum::UInt(u64::MAX)],
            Datum::UInt(u64::MAX),
            Datum::UInt(0),
        ),
        (
            vec![Datum::Real(0.0), Datum::Real(4.5), Datum::Real(2.5)],
            Datum::Real(4.5),
            Datum::Real(0.0),
        ),
        (
            vec![
                Datum::Decimal(Decimal::from_int(0)),
                Datum::Decimal(Decimal::from_int(4)),
                Datum::Decimal(Decimal::from_int(2)),
            ],
            Datum::Decimal(Decimal::from_int(4)),
            Datum::Decimal(Decimal::from_int(0)),
        ),
        (
            vec![
                Datum::new_string("0"),
                Datum::new_string("4"),
                Datum::new_string("2"),
            ],
            Datum::new_string("4"),
            Datum::new_string("0"),
        ),
        (
            vec![
                Datum::new_bytes(b"0".to_vec()),
                Datum::new_bytes(b"4".to_vec()),
                Datum::new_bytes(b"2".to_vec()),
            ],
            Datum::new_bytes(b"4".to_vec()),
            Datum::new_bytes(b"0".to_vec()),
        ),
    ];
    for (values, maximum, minimum) in vectors {
        assert_update_vector(AggregateKind::Max, &values, maximum.clone());
        assert_merge_vector(AggregateKind::Max, &values, maximum);
        assert_update_vector(AggregateKind::Min, &values, minimum.clone());
        assert_merge_vector(AggregateKind::Min, &values, minimum);
    }
    assert_update_vector(AggregateKind::Max, &[Datum::Null], Datum::Null);
    assert_update_vector(AggregateKind::Min, &[], Datum::Null);
}

#[test]
fn max_min_merge_preserves_null_identity_and_strict_winner_direction() {
    for (kind, left, right, expected) in [
        (AggregateKind::Max, None, None, Datum::Null),
        (AggregateKind::Max, None, Some(4), Datum::Int(4)),
        (AggregateKind::Max, Some(4), Some(2), Datum::Int(4)),
        (AggregateKind::Max, Some(2), Some(4), Datum::Int(4)),
        (AggregateKind::Min, None, Some(0), Datum::Int(0)),
        (AggregateKind::Min, Some(0), Some(2), Datum::Int(0)),
        (AggregateKind::Min, Some(2), Some(0), Datum::Int(0)),
    ] {
        let mut destination = MaxMinState::new(kind).unwrap();
        if let Some(value) = left {
            destination.update(&Datum::Int(value)).unwrap();
        }
        let mut source = MaxMinState::new(kind).unwrap();
        if let Some(value) = right {
            source.update(&Datum::Int(value)).unwrap();
        }
        destination.merge_from(&source).unwrap();
        assert_eq!(destination.result(), expected);
    }
}

#[test]
fn reset_and_live_fold_use_the_same_partial_state() {
    let values = [Datum::Null, Datum::Int(0), Datum::Int(4), Datum::Int(2)];
    assert_eq!(
        fold_values(AggregateKind::Max, false, &values, 4).unwrap(),
        Datum::Int(4)
    );
    assert_eq!(
        fold_values(AggregateKind::Min, false, &values, 4).unwrap(),
        Datum::Int(0)
    );

    let mut state = MaxMinState::new(AggregateKind::Max).unwrap();
    state.update(&Datum::Int(9)).unwrap();
    state.reset();
    assert_eq!(state.result(), Datum::Null);
}

#[test]
fn max_min_rejects_untyped_or_non_scalar_comparison_domains() {
    let mut state = MaxMinState::new(AggregateKind::Max).unwrap();
    state.update(&Datum::Int(1)).unwrap();
    assert!(state.update(&Datum::UInt(2)).is_err());
    assert!(fold_values(
        AggregateKind::Max,
        false,
        &[Datum::Int(1), Datum::UInt(2)],
        4,
    )
    .is_err());

    let mut destination = MaxMinState::new(AggregateKind::Max).unwrap();
    destination.update(&Datum::Int(1)).unwrap();
    let mut mixed_source = MaxMinState::new(AggregateKind::Max).unwrap();
    mixed_source.update(&Datum::UInt(2)).unwrap();
    assert!(destination.merge_from(&mixed_source).is_err());

    for sentinel in [Datum::min_not_null(), Datum::max_value()] {
        let mut state = MaxMinState::new(AggregateKind::Max).unwrap();
        assert!(state.update(&sentinel).is_err());
    }

    let mut state = MaxMinState::new(AggregateKind::Max).unwrap();
    assert!(state.update(&Datum::Real(f64::NAN)).is_err());

    let mut maximum = MaxMinState::new(AggregateKind::Max).unwrap();
    maximum.update(&Datum::Int(1)).unwrap();
    let mut minimum = MaxMinState::new(AggregateKind::Min).unwrap();
    minimum.update(&Datum::Int(2)).unwrap();
    assert!(maximum.merge_from(&minimum).is_err());
}

#[test]
fn max_min_uses_the_typed_string_collation() {
    let binary = |value: &str| Datum::new_collation_string(value, Collation::Utf8Mb4Bin);
    let general_ci = |value: &str| Datum::new_collation_string(value, Collation::Utf8Mb4GeneralCi);

    // utf8mb4_bin is PAD SPACE: the equal second value must not replace the
    // first retained value merely because its raw byte sequence is longer.
    assert_update_vector(
        AggregateKind::Max,
        &[binary("a"), binary("a ")],
        binary("a"),
    );

    // Case-insensitive collation order differs from raw byte order here.
    assert_update_vector(
        AggregateKind::Max,
        &[general_ci("Z"), general_ci("a")],
        general_ci("Z"),
    );

    let mut state = MaxMinState::new(AggregateKind::Max).unwrap();
    state.update(&binary("a")).unwrap();
    assert!(state.update(&general_ci("b")).is_err());
}
