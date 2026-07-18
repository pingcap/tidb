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

//! Source-derived live `FIRST_ROW` folding tests for campaign 03 routing.

use tidb_datatype::{Collation, Datum, Decimal};
use tidb_exec::first_row::fold_first_row;

#[test]
fn first_row_live_fold_covers_every_current_scalar_family() {
    let first_values = [
        Datum::Int(-7),
        Datum::UInt(u64::MAX),
        Datum::Decimal(Decimal::from_literal("12.340")),
        Datum::Real(-0.25),
        Datum::new_collation_string(b"first".to_vec(), Collation::Utf8UnicodeCi),
        Datum::new_bytes(vec![0xff, 0]),
    ];
    for first in first_values {
        assert_eq!(
            fold_first_row(&[first.clone(), Datum::Null, Datum::Int(99)]),
            first
        );
    }
}

#[test]
fn first_row_live_fold_distinguishes_empty_null_and_non_null() {
    assert_eq!(fold_first_row(&[]), Datum::Null);
    assert_eq!(fold_first_row(&[Datum::Null]), Datum::Null);
    assert_eq!(
        fold_first_row(&[Datum::Null, Datum::new_string("later")]),
        Datum::Null
    );
    assert_eq!(
        fold_first_row(&[Datum::new_string("first"), Datum::Null]),
        Datum::new_string("first")
    );
}
