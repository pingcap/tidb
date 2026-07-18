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

//! Focused tests for translated `pkg/expression/builtin_compare.go` behavior.

use super::e;
use crate::{apply_binary, Datum, Decimal};
use tidb_ast::BinaryOp;

#[test]
fn compare_source_vector_promotes_real_and_decimal() {
    // pkg/expression/builtin_compare_test.go:80 TestCompare
    // `realVal` is a Go float64 while `decimalVal` is a MyDecimal.  The
    // compare function therefore selects the ETReal signature and returns a
    // boolean datum, rather than exposing either operand's numeric value.
    let decimal = Datum::new_decimal(Decimal::from_literal("123.123"));
    assert_eq!(
        apply_binary(BinaryOp::Lt, Datum::Real(1.1), decimal),
        Ok(Datum::Int(1))
    );
}

#[test]
fn greatest_least_source_vectors_preserve_mixed_integer_result_domain() {
    // pkg/expression/builtin_compare_test.go:286 TestGreatestLeastFunc
    assert_eq!(
        e("greatest(-9223372036854775808, 9223372036854775809)"),
        "DEC:9223372036854775809"
    );
    assert_eq!(
        e("least(-9223372036854775808, 9223372036854775809)"),
        "DEC:-9223372036854775808"
    );
    assert_eq!(
        e("greatest(cast(9223372036854775808 as unsigned), cast(9223372036854775809 as unsigned))"),
        "UINT:9223372036854775809"
    );
    assert_eq!(
        e("least(cast(9223372036854775808 as unsigned), cast(9223372036854775809 as unsigned))"),
        "UINT:9223372036854775808"
    );
}

#[test]
fn greatest_least_source_vector_stringifies_mixed_arguments() {
    // pkg/expression/builtin_compare_test.go:286 TestGreatestLeastFunc
    // Go's aggregateType selects the string signature when any argument is a
    // string, so the numeric 12 is compared as the text "12".
    assert_eq!(e("greatest('123a', 'b', 'c', 12)"), "STR:c");
    assert_eq!(e("least('123a', 'b', 'c', 12)"), "STR:12");
}
