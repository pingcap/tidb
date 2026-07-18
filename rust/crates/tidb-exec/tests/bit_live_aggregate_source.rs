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

//! Source-derived live bit aggregate and spill tests.

use tidb_datatype::Datum;
use tidb_exec::aggregate::runtime::fold_values;
use tidb_exec::bit_agg::{fold_bit_values, BitAggregate, BitAggregateKind};
use tidb_exec::{Database, Outcome, ResultSet};
use tidb_planner::aggregation_descriptor::AggregateKind;

#[test]
fn live_fold_preserves_source_identities_nulls_and_raw_integer_bits() {
    // Source: pkg/executor/aggfuncs/func_bitfuncs.go:73-182.
    assert_eq!(
        fold_bit_values(AggregateKind::BitAnd, &[]).unwrap(),
        Datum::UInt(u64::MAX)
    );
    assert_eq!(
        fold_bit_values(AggregateKind::BitOr, &[Datum::Null]).unwrap(),
        Datum::UInt(0)
    );
    assert_eq!(
        fold_bit_values(
            AggregateKind::BitXor,
            &[Datum::Int(-1), Datum::UInt(u64::MAX), Datum::Null]
        )
        .unwrap(),
        Datum::UInt(0)
    );
    assert!(fold_bit_values(AggregateKind::BitOr, &[Datum::Real(1.0)]).is_err());
}

#[test]
fn shared_aggregate_runtime_dispatches_all_bit_kinds_to_the_canonical_state() {
    // Database::compute_aggregate enters through aggregate::runtime::fold_values.
    // Exercise that shared route, not only the leaf helper. Window dispatch is
    // a separate executor boundary and remains outside this slice.
    let values = [Datum::Int(1), Datum::Int(2), Datum::Int(4), Datum::Null];
    assert_eq!(
        fold_values(AggregateKind::BitAnd, false, &values, 4).unwrap(),
        Datum::UInt(0)
    );
    assert_eq!(
        fold_values(AggregateKind::BitOr, false, &values, 4).unwrap(),
        Datum::UInt(7)
    );
    assert_eq!(
        fold_values(AggregateKind::BitXor, false, &values, 4).unwrap(),
        Datum::UInt(7)
    );
}

#[test]
fn database_aggregate_path_consumes_the_canonical_bit_state() {
    let mut database = Database::new();
    let mut execute =
        |sql: &str| database.run(&tidb_parser::parse(sql).expect("bit aggregate SQL parses"));
    assert_eq!(execute("create table bit_live (v int)"), Ok(Outcome::Done));
    assert_eq!(
        execute("insert into bit_live values (1),(2),(4),(null)"),
        Ok(Outcome::Done)
    );
    assert_eq!(
        execute("select bit_and(v), bit_or(v), bit_xor(v) from bit_live"),
        Ok(Outcome::Rows(ResultSet {
            rows: vec![vec![Datum::UInt(0), Datum::UInt(7), Datum::UInt(7)]],
            ordered: false,
        }))
    );
}

#[test]
fn xor_slide_removes_departing_values_then_adds_arriving_values() {
    // Source: pkg/executor/aggfuncs/func_bitfuncs.go:112-143.
    let mut xor = BitAggregate::new(BitAggregateKind::Xor);
    xor.update(&[Datum::Int(1), Datum::Int(2), Datum::Int(3)])
        .unwrap();
    xor.slide_xor(&[Datum::Int(1), Datum::Null], &[Datum::Int(4), Datum::Null])
        .unwrap();
    assert_eq!(xor.value(), 5);

    let mut bit_or = BitAggregate::new(BitAggregateKind::Or);
    assert!(bit_or
        .slide_xor(&[Datum::Int(1)], &[Datum::Int(2)])
        .is_err());

    // Source processes every departing row before the first arriving row.
    // A departing EvalInt error therefore stops before any arriving mutation.
    let mut ordered = BitAggregate::new(BitAggregateKind::Xor);
    ordered.update(&[Datum::Int(3)]).unwrap();
    assert!(ordered
        .slide_xor(&[Datum::Int(1), Datum::Real(2.0)], &[Datum::Int(4)])
        .is_err());
    assert_eq!(ordered.value(), 2);
}

#[test]
fn partial_result_spill_round_trips_source_native_eight_byte_shape() {
    // Direct Go coverage:
    // pkg/executor/aggfuncs/spill_helper_test.go:801
    // (TestPartialResult4BitFunc).
    for expected in [0_u64, 1, 2, u64::MAX] {
        let mut state = BitAggregate::new(BitAggregateKind::Or);
        state.update(&[Datum::UInt(expected)]).unwrap();
        let serialized = state.serialize_partial();
        assert_eq!(serialized.len(), 8);
        assert_eq!(serialized, expected.to_ne_bytes());
        let decoded = BitAggregate::deserialize_partial(BitAggregateKind::Or, &serialized).unwrap();
        assert_eq!(decoded.value(), expected);
    }
    assert!(BitAggregate::deserialize_partial(BitAggregateKind::Or, &[0; 7]).is_err());

    // Go DeserializeUint64 advances one value in a larger row buffer rather
    // than requiring the value to consume the complete buffer.
    let mut with_trailing_byte = 2_u64.to_ne_bytes().to_vec();
    with_trailing_byte.push(0xff);
    assert_eq!(
        BitAggregate::deserialize_partial(BitAggregateKind::Or, &with_trailing_byte)
            .unwrap()
            .value(),
        2
    );
}
