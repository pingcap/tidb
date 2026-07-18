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

//! Source-backed tests for bitwise aggregate state.

use tidb_datatype::Datum;
use tidb_exec::bit_agg::{BitAggregate, BitAggregateKind};

#[test]
fn bit_aggregate_vectors_match_source() {
    // Source: pkg/executor/aggfuncs/func_bitfuncs.go:73-109.
    // Direct Go coverage: pkg/executor/aggfuncs/func_bitfuncs_test.go:25
    // (TestMergePartialResult4BitFuncs), with source generated values 0..4.
    let values = [0, 1, 2, 3, 4].map(Datum::Int);

    let mut and = BitAggregate::new(BitAggregateKind::And);
    and.update(&values).unwrap();
    assert_eq!(and.value(), 0);

    let mut or = BitAggregate::new(BitAggregateKind::Or);
    or.update(&values).unwrap();
    assert_eq!(or.value(), 7);

    let mut xor = BitAggregate::new(BitAggregateKind::Xor);
    xor.update(&values).unwrap();
    assert_eq!(xor.value(), 4);
}

#[test]
fn bit_aggregate_merge_null_and_reset_match_source() {
    let mut and = BitAggregate::new(BitAggregateKind::And);
    and.update(&[Datum::Null, Datum::Int(-1), Datum::Int(7)])
        .unwrap();
    assert_eq!(and.value(), 7);
    let mut and_source = BitAggregate::new(BitAggregateKind::And);
    and_source.update(&[Datum::Int(3)]).unwrap();
    and.merge_from(&and_source).unwrap();
    assert_eq!(and.value(), 3);
    and.reset();
    assert_eq!(and.value(), u64::MAX);

    let mut or = BitAggregate::new(BitAggregateKind::Or);
    or.update(&[Datum::Null]).unwrap();
    assert_eq!(or.value(), 0);
    let mut or_source = BitAggregate::new(BitAggregateKind::Or);
    or_source.update(&[Datum::UInt(5)]).unwrap();
    or.merge_from(&or_source).unwrap();
    assert_eq!(or.value(), 5);
    or.reset();
    assert_eq!(or.value(), 0);

    let mut xor = BitAggregate::new(BitAggregateKind::Xor);
    xor.update(&[Datum::Int(-1), Datum::Int(-1), Datum::Null])
        .unwrap();
    assert_eq!(xor.value(), 0);
    xor.reset();
    assert_eq!(xor.value(), 0);

    // Concrete Rust typing replaces Go's untyped PartialResult cast. Refuse
    // to silently combine partials belonging to different operations.
    assert!(and.merge_from(&or).is_err());
}

#[test]
fn bit_aggregate_partial_state_size_matches_source() {
    // Source: pkg/executor/aggfuncs/func_bitfuncs.go:24-34.
    // Direct Go coverage: pkg/executor/aggfuncs/func_bitfuncs_test.go:36
    // (TestMemBitFunc). The executor allocator/tracker remains outside this
    // shared uint64 owner.
    assert_eq!(
        BitAggregate::partial_state_size(),
        std::mem::size_of::<u64>()
    );
    assert_eq!(BitAggregate::partial_state_size(), 8);
}
