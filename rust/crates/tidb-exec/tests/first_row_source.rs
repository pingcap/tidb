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

//! Source-backed tests for the canonical `FIRST_ROW` partial state.

use tidb_datatype::{Collation, Datum, DatumKind};
use tidb_exec::first_row::{
    fold_first_row, FirstRowSpillKind, FirstRowSpillSerializer, FirstRowState, FirstRowWireError,
};

#[test]
fn first_physical_row_and_merge_match_source() {
    // Source: pkg/executor/aggfuncs/func_first_row.go, all Update/Merge pairs.
    // Direct Go coverage: func_first_row_test.go:27.
    let mut destination = FirstRowState::new();
    assert_eq!(destination.update(&[]), 0);
    assert_eq!(destination.update(&[Datum::Int(0), Datum::Int(2)]), 0);
    assert_eq!(destination.update(&[Datum::Int(99)]), 0);
    assert_eq!(destination.result(), Datum::Int(0));

    let mut source = FirstRowState::new();
    source.update(&[Datum::Int(2)]);
    destination.merge_from(&source);
    assert_eq!(destination.result(), Datum::Int(0));

    let mut empty_destination = FirstRowState::new();
    empty_destination.merge_from(&source);
    assert_eq!(empty_destination.result(), Datum::Int(2));

    let mut null_source = FirstRowState::new();
    null_source.update(&[Datum::Null, Datum::Int(7)]);
    assert!(null_source.got_first_row());
    assert!(null_source.is_null());
    assert_eq!(null_source.value(), &Datum::Null);
    empty_destination.reset();
    empty_destination.merge_from(&null_source);
    assert!(empty_destination.got_first_row());
    assert_eq!(empty_destination.result(), Datum::Null);

    // Go copies the whole source struct whenever the destination is unseen,
    // even when the source is itself unseen and contains inconsistent fixture
    // fields. Flags and payload therefore remain independently observable.
    let unseen_source = FirstRowState::from_parts(true, false, Datum::Int(321));
    let mut unseen_destination = FirstRowState::new();
    unseen_destination.merge_from(&unseen_source);
    assert!(unseen_destination.is_null());
    assert!(!unseen_destination.got_first_row());
    assert_eq!(unseen_destination.value(), &Datum::Int(321));
    assert_eq!(unseen_destination.result(), Datum::Null);
}

#[test]
fn reset_and_live_fold_preserve_seen_separately_from_value() {
    let inconsistent = FirstRowState::from_parts(true, false, Datum::Int(123));
    assert!(!inconsistent.got_first_row());
    assert_eq!(inconsistent.value(), &Datum::Int(123));
    assert_eq!(inconsistent.result(), Datum::Null);

    let mut state = FirstRowState::from_parts(false, true, Datum::Int(5));
    state.reset();
    assert!(!state.got_first_row());
    assert!(!state.is_null());
    assert_eq!(state.value(), &Datum::Int(0));

    let retained = Datum::new_string("stale until overwritten");
    let mut string_state = FirstRowState::from_parts(false, true, retained.clone());
    string_state.reset();
    assert!(!string_state.got_first_row());
    assert!(!string_state.is_null());
    assert_eq!(string_state.value(), &retained);
    assert_eq!(string_state.result(), Datum::Null);
    assert_eq!(string_state.update(&[Datum::new_string("next")]), 4);
    assert_eq!(string_state.result(), Datum::new_string("next"));

    assert_eq!(fold_first_row(&[]), Datum::Null);
    assert_eq!(fold_first_row(&[Datum::Null, Datum::Int(4)]), Datum::Null);
    assert_eq!(
        fold_first_row(&[Datum::UInt(9), Datum::UInt(4)]),
        Datum::UInt(9)
    );
}

#[test]
fn source_spill_layout_has_no_rust_only_type_or_collation_tags() {
    let mut serializer = FirstRowSpillSerializer::new();
    let int_state = FirstRowState::from_parts(true, false, Datum::Int(-123));
    let mut int_wire = vec![1, 0];
    int_wire.extend_from_slice(&(-123_i64).to_ne_bytes());
    assert_eq!(
        serializer
            .serialize(&int_state, FirstRowSpillKind::Int)
            .unwrap(),
        int_wire
    );
    assert_eq!(
        FirstRowState::deserialize(&int_wire, FirstRowSpillKind::Int).unwrap(),
        int_state
    );

    // A live Rust NULL has no typed payload. The externally selected Go
    // helper supplies its independently serialized zero value.
    let null_state = FirstRowState::from_parts(true, true, Datum::Null);
    let mut null_int_wire = vec![1, 1];
    null_int_wire.extend_from_slice(&0_i64.to_ne_bytes());
    assert_eq!(
        serializer
            .serialize(&null_state, FirstRowSpillKind::Int)
            .unwrap(),
        null_int_wire
    );
    let decoded_null = FirstRowState::deserialize(&null_int_wire, FirstRowSpillKind::Int).unwrap();
    assert!(decoded_null.is_null());
    assert!(decoded_null.got_first_row());
    assert_eq!(decoded_null.value(), &Datum::Int(0));
    assert_eq!(decoded_null.result(), Datum::Null);

    let float_state = FirstRowState::from_parts(false, true, Datum::Real(-1.1));
    let mut float_wire = vec![0, 1];
    float_wire.extend_from_slice(&(-1.1_f64).to_ne_bytes());
    assert_eq!(
        serializer
            .serialize(&float_state, FirstRowSpillKind::Float64)
            .unwrap(),
        float_wire
    );
    let decoded = FirstRowState::deserialize(&float_wire, FirstRowSpillKind::Float64).unwrap();
    assert_eq!(decoded.is_null(), float_state.is_null());
    assert_eq!(decoded.got_first_row(), float_state.got_first_row());
    assert_eq!(
        decoded.value().as_real().unwrap().to_bits(),
        (-1.1_f64).to_bits()
    );

    let payload = vec![0xff, 0, b'x'];
    let string_state = FirstRowState::from_parts(
        true,
        false,
        Datum::new_collation_string(payload.clone(), Collation::Utf8UnicodeCi),
    );
    let mut string_wire = vec![1, 0];
    string_wire.extend_from_slice(&payload.len().to_ne_bytes());
    string_wire.extend_from_slice(&payload);
    let string_kind = FirstRowSpillKind::String(Collation::Utf8UnicodeCi);
    assert_eq!(
        serializer.serialize(&string_state, string_kind).unwrap(),
        string_wire
    );
    assert_eq!(
        FirstRowState::deserialize(&string_wire, string_kind).unwrap(),
        string_state
    );

    // Go's collation is external field metadata, not bytes in the spill row.
    let other_collation = FirstRowSpillKind::String(Collation::Binary);
    let decoded = FirstRowState::deserialize(&string_wire, other_collation).unwrap();
    assert_eq!(decoded.value().as_raw_bytes(), Some(payload.as_slice()));
    assert_eq!(decoded.value().collation(), Some(Collation::Binary));
}

#[test]
fn source_spill_exact_int_float64_and_string_fixtures_round_trip() {
    // Exact representable fixture values from spill_helper_test.go:986,1076,1167.
    let cases = [
        (
            FirstRowSpillKind::Int,
            FirstRowState::from_parts(true, false, Datum::Int(-123)),
        ),
        (
            FirstRowSpillKind::Int,
            FirstRowState::from_parts(false, false, Datum::Int(0)),
        ),
        (
            FirstRowSpillKind::Int,
            FirstRowState::from_parts(true, true, Datum::Int(123)),
        ),
        (
            FirstRowSpillKind::String(Collation::DEFAULT),
            FirstRowState::from_parts(true, false, Datum::new_string(Vec::<u8>::new())),
        ),
        (
            FirstRowSpillKind::String(Collation::DEFAULT),
            FirstRowState::from_parts(
                false,
                false,
                // spill_helper_test.go getLongString repeats by doubling ten
                // times, hence 2^10 copies of the exact source seed.
                Datum::new_string("平352p凯额6辰c".repeat(1 << 10)),
            ),
        ),
        (
            FirstRowSpillKind::Float64,
            FirstRowState::from_parts(true, false, Datum::Real(-1.1)),
        ),
        (
            FirstRowSpillKind::Float64,
            FirstRowState::from_parts(false, false, Datum::Real(0.0)),
        ),
        (
            FirstRowSpillKind::Float64,
            FirstRowState::from_parts(true, true, Datum::Real(1.1)),
        ),
    ];
    let mut serializer = FirstRowSpillSerializer::new();
    let initial_capacity = serializer.capacity();
    for (kind, state) in cases {
        let bytes = serializer.serialize(&state, kind).unwrap().to_vec();
        assert_eq!(FirstRowState::deserialize(&bytes, kind).unwrap(), state);
    }
    assert!(serializer.capacity() >= initial_capacity);
    assert!(serializer.capacity() >= "平352p凯额6辰c".len() * (1 << 10));
}

#[test]
fn string_memory_delta_is_charged_only_for_the_winning_row() {
    // Directly representable branch of func_first_row_test.go:52.
    let mut state = FirstRowState::new();
    assert_eq!(state.update(&[Datum::new_string("first")]), 5);
    assert_eq!(state.update(&[Datum::new_string("ignored")]), 0);
    assert_eq!(state.result(), Datum::new_string("first"));
}

#[test]
fn malformed_spill_rows_fail_closed() {
    assert_eq!(
        FirstRowState::deserialize(&[], FirstRowSpillKind::Int),
        Err(FirstRowWireError::Truncated)
    );
    assert_eq!(
        FirstRowState::deserialize(&[2, 0, 0], FirstRowSpillKind::Int),
        Err(FirstRowWireError::InvalidBool(2))
    );
    assert_eq!(
        FirstRowSpillSerializer::new()
            .serialize(
                &FirstRowState::from_parts(false, true, Datum::UInt(7)),
                FirstRowSpillKind::Int,
            )
            .unwrap_err(),
        FirstRowWireError::DatumKindMismatch {
            expected: FirstRowSpillKind::Int,
            actual: DatumKind::UInt,
        }
    );

    let state = FirstRowState::from_parts(false, true, Datum::Int(7));
    let mut serializer = FirstRowSpillSerializer::new();
    let mut bytes = serializer
        .serialize(&state, FirstRowSpillKind::Int)
        .unwrap()
        .to_vec();
    bytes.push(0);
    assert_eq!(
        FirstRowState::deserialize(&bytes, FirstRowSpillKind::Int),
        Err(FirstRowWireError::TrailingBytes(1))
    );
}
