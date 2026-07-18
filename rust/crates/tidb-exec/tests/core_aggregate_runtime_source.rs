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

use tidb_datatype::{Datum, Decimal};
use tidb_exec::aggregate::runtime::spill::{
    deserialize_avg_float64, deserialize_decimal_pair, deserialize_sum_float64, SpillSerializer,
};
use tidb_exec::aggregate::runtime::{
    fold_values, AvgFloat64State, AvgState, CountDistinctIntState, CountState, SumDecimalState,
    SumFloat64State, SumInt64State, SumState, SumUint64State,
};
use tidb_planner::aggregation_descriptor::AggregateKind;

#[test]
fn count_original_partial_merge_slide_and_distinct_share_one_runtime() {
    // func_count_test.go:43,51 and generated typed rows at 63/78/109;
    // func_count_test.go:115 and generated memory rows at 153.
    let mut partial = CountState::new();
    for value in [Datum::Null, Datum::Int(1), Datum::Real(2.0)] {
        partial.update(&value);
    }
    let mut state = CountState::new();
    state.add_partial(3);
    state.merge_from(&partial);
    assert_eq!(state.result(), 5);
    state.slide(&[Datum::Int(1)], &[Datum::Null, Datum::Int(9)]);
    assert_eq!(state.result(), 5);
    assert_eq!(CountState::partial_state_size(), 8);

    let mut distinct = CountDistinctIntState::new();
    distinct.update(&[None, Some(1), Some(1), Some(2)]);
    let mut other = CountDistinctIntState::new();
    other.update(&[Some(2), Some(3)]);
    distinct.merge_from(&other);
    assert_eq!(distinct.result(), 3);
}

#[test]
fn sum_typed_update_merge_empty_and_sliding_vectors_match_source() {
    // func_sum_test.go:33/44, 50/60, 66/83, 89, and 133.
    let mut signed = SumInt64State::new();
    signed
        .update(&[None, Some(0), Some(1), Some(2), Some(3), Some(4)])
        .unwrap();
    let mut signed_source = SumInt64State::new();
    signed_source.update(&[Some(9)]).unwrap();
    signed.merge_from(&signed_source).unwrap();
    assert_eq!(signed.result(), Some(19));
    let mut signed_slide = SumInt64State::new();
    signed_slide.update(&[Some(i64::MAX - 1)]).unwrap();
    signed_slide
        .slide(&[Some(i64::MAX - 1)], &[Some(2)])
        .unwrap();
    assert_eq!(signed_slide.result(), Some(2));

    let mut unsigned = SumUint64State::new();
    unsigned.update(&[Some(u64::MAX - 1)]).unwrap();
    unsigned.slide(&[Some(u64::MAX - 1)], &[Some(2)]).unwrap();
    assert_eq!(unsigned.result(), Some(2));

    let mut real = SumFloat64State::new();
    real.update(&[None, Some(0.0), Some(1.0), Some(2.0), Some(3.0), Some(4.0)]);
    assert_eq!(real.result(), Some(10.0));
    let mut real_source = SumFloat64State::new();
    real_source.update(&[Some(9.0)]);
    real.merge_from(&real_source);
    assert_eq!(real.result(), Some(19.0));

    let mut decimal = SumDecimalState::new();
    decimal.update_one(&Decimal::from_int(10));
    decimal.update_one(&Decimal::from_int(9));
    assert_eq!(decimal.result(), Some(Decimal::from_int(19)));
}

#[test]
fn canonical_sum_and_avg_states_fold_the_executor_datum_domain() {
    let mut sum = SumState::new();
    for value in [Datum::Null, Datum::Int(0), Datum::Int(1), Datum::Int(2)] {
        sum.update(&value).unwrap();
    }
    assert_eq!(sum.result(), Some(Datum::Int(3)));

    // func_avg_test.go:27,37,48: source values 0..4 and merge values 2..4.
    let mut avg = AvgState::new();
    for value in 0..5 {
        avg.update(&Datum::Int(value)).unwrap();
    }
    let mut avg_source = AvgState::new();
    for value in 2..5 {
        avg_source.update(&Datum::Int(value)).unwrap();
    }
    avg.merge_from(&avg_source).unwrap();
    assert_eq!(
        avg.result(4).unwrap(),
        Datum::Decimal(Decimal::from_literal("2.3750"))
    );

    let mut avg_real = AvgFloat64State::new();
    avg_real.update(&[Some(0.0), Some(1.0), Some(2.0), Some(3.0), Some(4.0)]);
    assert_eq!(avg_real.result(), Some(2.0));
}

#[test]
fn canonical_sql_sum_avg_and_decimal_distinct_use_decimal_value_semantics() {
    let signed = [Datum::Int(i64::MAX), Datum::Int(1)];
    assert_eq!(
        fold_values(AggregateKind::Sum, false, &signed, 4).unwrap(),
        Datum::Decimal(Decimal::from_literal("9223372036854775808"))
    );
    assert_eq!(
        fold_values(AggregateKind::Avg, false, &signed, 4).unwrap(),
        Datum::Decimal(Decimal::from_literal("4611686018427387904.0000"))
    );
    assert!(fold_values(AggregateKind::SumInt, false, &signed, 4).is_err());

    let unsigned = [Datum::UInt(u64::MAX), Datum::UInt(1)];
    assert_eq!(
        fold_values(AggregateKind::Sum, false, &unsigned, 4).unwrap(),
        Datum::Decimal(Decimal::from_literal("18446744073709551616"))
    );

    let equivalent_decimals = [
        Datum::Decimal(Decimal::from_literal("1.0")),
        Datum::Decimal(Decimal::from_literal("1.00")),
    ];
    assert_eq!(
        fold_values(AggregateKind::Count, true, &equivalent_decimals, 4).unwrap(),
        Datum::Int(1)
    );
    assert_eq!(
        fold_values(AggregateKind::Sum, true, &equivalent_decimals, 4).unwrap(),
        Datum::Decimal(Decimal::from_literal("1.0"))
    );
}

#[test]
fn avg_and_sum_spill_pairs_round_trip_all_original_vectors() {
    // spill_helper_test.go:568,613,658,703.
    let mut serializer = SpillSerializer::new();
    for (value, count) in [(0_i64, 0_i64), (12_345, 123), (87_654, -123)] {
        let decimal = Decimal::from_int(value);
        let bytes = serializer.serialize_decimal_pair(&decimal, count).to_vec();
        assert_eq!(deserialize_decimal_pair(&bytes).unwrap(), (decimal, count));
    }

    for (value, count) in [(0.0, 0), (123.123, 123), (-123.123, -123)] {
        let avg = AvgFloat64State::from_parts(value, count);
        let bytes = serializer.serialize_avg_float64(avg).to_vec();
        assert_eq!(deserialize_avg_float64(&bytes).unwrap(), avg);
        let sum = SumFloat64State::from_parts(value, count);
        let bytes = serializer.serialize_sum_float64(sum).to_vec();
        assert_eq!(deserialize_sum_float64(&bytes).unwrap(), sum);
    }
}

#[test]
fn source_benchmark_shapes_do_not_use_a_second_state_family() {
    // func_avg_test.go:65 and func_count_test.go:173 use 50,000 rows.
    let mut count = CountState::new();
    let mut avg = AvgFloat64State::new();
    for value in 0..50_000 {
        count.update(&Datum::Int(value));
        avg.update(&[Some(value as f64)]);
    }
    assert_eq!(count.result(), 50_000);
    assert_eq!(avg.result(), Some(24_999.5));
}
