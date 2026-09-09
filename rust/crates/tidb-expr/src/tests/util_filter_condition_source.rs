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

//! GO PORT of `pkg/expression/util_test.go:333` `TestFilter` (batch part11).
//!
//! Go's test drives `Filter(result, conditions, isLogicOrFunction)`
//! (`util.go:Filter`, a predicate-append loop returning the KEPT conditions in
//! input order) over three built expressions and requires exactly the one
//! `or` condition to survive. This crate carries only the in-place twin of
//! that function, `expr_util::extract::filter_out_in_place`
//! (`util.go:FilterOutInPlace`), whose `filtered` half returns exactly the
//! conditions `Filter` would have appended — same predicate, same elements.
//! The port therefore expresses Go's expectation through the sibling carrier
//! and additionally checks its complement. With a single matching condition
//! the reverse-order collection inside `FilterOutInPlace` cannot change the
//! outcome, so no ordering assumption is smuggled in.

use crate::column::Column;
use crate::constant::Constant;
use crate::expression::Expression;
use crate::scalar_function::ScalarFunction;
use tidb_ast::CiString;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};

fn int_type() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
}

/// Go `newColumn(id)` / `newLonglong(v)` under this crate's builderless
/// convention (`expr_util::tests` header documents why).
fn col(id: i64) -> Expression {
    let mut column = Column::new(id, int_type());
    column.index = id;
    Expression::Column(column)
}

fn longlong_const(v: i64) -> Expression {
    Expression::Constant(Constant::new(Datum::Int(v), int_type()))
}

fn func(name: &str, args: Vec<Expression>) -> Expression {
    Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), int_type(), args))
}

/// Go's `isLogicOrFunction` predicate (`util_test.go:394`).
fn is_logic_or(e: &Expression) -> bool {
    matches!(e, Expression::ScalarFunction(f) if f.func_name.lowercase() == "or")
}

#[test]
fn test_filter_keeps_exactly_the_predicate_matches() {
    // Same three conditions as util_test.go:334-338:
    // eq(a,b), eq(b,c), or(1,a).
    let conditions = vec![
        func("eq", vec![col(0), col(1)]),
        func("eq", vec![col(1), col(2)]),
        func("or", vec![longlong_const(1), col(0)]),
    ];

    // `filtered` carries what Go's Filter would append into `result`.
    let (remained, filtered) =
        crate::expr_util::extract::filter_out_in_place(conditions, &is_logic_or);

    assert_eq!(filtered.len(), 1);
    let Expression::ScalarFunction(kept) = &filtered[0] else {
        panic!("the kept condition is a scalar function")
    };
    assert_eq!(kept.func_name.lowercase(), "or");

    // The complement half (sibling-carrier extra): both eq conditions stay.
    assert_eq!(remained.len(), 2);
    for expr in &remained {
        let Expression::ScalarFunction(f) = expr else {
            panic!("a kept-in-place condition is a scalar function")
        };
        assert_eq!(f.func_name.lowercase(), "eq");
    }
}
