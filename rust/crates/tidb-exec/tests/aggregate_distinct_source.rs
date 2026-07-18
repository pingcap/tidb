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

//! Direct translation of `pkg/expression/aggregation/util_test.go`.

#[path = "../src/aggregate_distinct.rs"]
mod aggregate_distinct;

use aggregate_distinct::DistinctChecker;
use tidb_datatype::{Collation, Datum, Decimal};

/// Source: `pkg/expression/aggregation/util_test.go::TestDistinct`.
#[test]
fn test_distinct() {
    let mut checker = DistinctChecker::new();
    let cases = [
        (vec![Datum::Int(1), Datum::Int(1)], true),
        (vec![Datum::Int(1), Datum::Int(1)], false),
        (vec![Datum::Int(1), Datum::Int(2)], true),
        (vec![Datum::Int(1), Datum::Int(2)], false),
        (vec![Datum::Int(1), Datum::Null], true),
        (vec![Datum::Int(1), Datum::Null], false),
    ];

    for (values, expected) in cases {
        assert_eq!(checker.check(&values), expected);
    }
}

/// `codec.EncodeValue` keys strings by raw bytes rather than by collation
/// metadata and uses length-delimited values, so tuple boundaries cannot
/// alias one another.
#[test]
fn source_value_key_preserves_raw_bytes_and_tuple_boundaries() {
    let mut checker = DistinctChecker::new();
    assert!(checker.check(&[Datum::new_collation_string("a", Collation::Utf8Mb4Bin,)]));
    assert!(!checker.check(&[Datum::new_collation_string(
        "a",
        Collation::Utf8Mb4GeneralCi,
    )]));

    assert!(checker.check(&[Datum::new_bytes("ab"), Datum::new_bytes("c")]));
    assert!(checker.check(&[Datum::new_bytes("a"), Datum::new_bytes("bc")]));

    assert!(checker.check(&[Datum::Decimal(Decimal::from_literal("1.0"))]));
    assert!(!checker.check(&[Datum::Decimal(Decimal::from_literal("1.00"))]));
}
