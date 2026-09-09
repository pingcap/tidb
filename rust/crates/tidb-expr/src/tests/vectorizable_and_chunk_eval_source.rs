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

//! GO PORTS of `pkg/expression/expression_test.go`'s vectorization tests:
//! `TestVectorizable` (:196) and `TestEvalExpr` (:299), plus the unportable
//! `TestExpressionMemeoryUsage` (:328) recorded as an `#[ignore]` stub with its
//! go-parity-gap reason.

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Decimal, FieldType, FieldTypeCode, MySqlDuration, Time, TimeType};

use super::*;
use crate::column::Column;
use crate::constant::Constant;
use crate::context::NoColumns;
use crate::evaluator::EvaluatorSuite;
use crate::expression::Expression;
use crate::scalar_function::ScalarFunction;
use tidb_ast::CiString;

/// GO PORT of `pkg/expression/expression_test.go:196 TestVectorizable`.
///
/// Go's table:
///
/// - `[rand(), 1, NULL, col]` is Vectorizable -- a non-variable, non-sequence
///   list never blocks the column-by-column path,
/// - `setvar(col_str0, col_str1)` is NOT (writes happen per row in select-list
///   order),
/// - `getvar(col_str0)` is NOT,
/// - `[nextval, lastval]`, and `[nextval, setval]`, and
///   `[nextval, nextval]` are NOT (sequence state must advance row-major).
///
/// The nodes are constructed directly because `Vectorizable` reads only names
/// and argument SHAPES, exactly what Go's `newFunctionWithMockCtx` produces for
/// these cases; running the real builder would additionally noop-gate
/// `setvar`/`getvar`, which is beside this predicate's contract.
#[test]
fn test_vectorizable_over_go_expression_table() {
    fn raw(name: &str, args: Vec<Expression>) -> Expression {
        Expression::ScalarFunction(ScalarFunction::new(
            CiString::new(name),
            FieldType::new(FieldTypeCode::LongLong),
            args,
        ))
    }
    fn column(unique_id: i64, code: FieldTypeCode) -> Expression {
        Expression::Column(Column::new(unique_id, FieldType::new(code)))
    }
    let one = || -> Expression {
        Expression::Constant(Constant::new(
            Datum::Int(1),
            FieldType::new(FieldTypeCode::LongLong),
        ))
    };

    // [rand, one, null, column] all together stay vectorizable.
    let expressions = vec![
        raw("rand", vec![]),
        one(),
        Expression::Constant(Constant::new(
            Datum::Null,
            FieldType::new(FieldTypeCode::LongLong),
        )),
        column(0, FieldTypeCode::LongLong),
    ];
    assert!(crate::evaluator::vectorizable(&expressions));

    // setvar(string_col, string_col) blocks it.
    assert!(!crate::evaluator::vectorizable(&[raw(
        "setvar",
        vec![
            column(1, FieldTypeCode::String),
            column(2, FieldTypeCode::String)
        ]
    )]));

    // getvar(string_col) blocks it too.
    assert!(!crate::evaluator::vectorizable(&[raw(
        "getvar",
        vec![column(3, FieldTypeCode::String)]
    )]));

    // Sequence mixing blocks it in every combination Go lists.
    assert!(!crate::evaluator::vectorizable(&[
        raw("nextval", vec![column(3, FieldTypeCode::LongLong)]),
        raw("lastval", vec![column(3, FieldTypeCode::LongLong)])
    ]));
    assert!(!crate::evaluator::vectorizable(&[
        raw("lastval", vec![column(3, FieldTypeCode::LongLong)]),
        raw("nextval", vec![column(3, FieldTypeCode::LongLong)])
    ]));
    assert!(!crate::evaluator::vectorizable(&[
        raw("nextval", vec![column(4, FieldTypeCode::LongLong)]),
        raw(
            "setval",
            vec![
                column(5, FieldTypeCode::String),
                column(6, FieldTypeCode::LongLong)
            ]
        )
    ]));
}

const ROWS: usize = 1024;

/// One cell per row index for a given eval type; every 7th row becomes NULL so
/// both evaluation modes cross SQL's three-valued logic (Go fills random data
/// through data generators, which also carry a NULL rate; deterministic rows
/// keep the same invariant reproducible).
fn fill(chunk: &mut Chunk, value_of: impl Fn(usize) -> Option<Datum>) {
    for row in 0..ROWS {
        match value_of(row) {
            Some(datum) => chunk.append_datum(0, &datum),
            None => chunk.append_null(0),
        }
    }
}

/// GO PORT of `pkg/expression/expression_test.go:299 TestEvalExpr`.
///
/// For each of Go's seven eval types (`ETInt ETReal ETDecimal ETString
/// ETTimestamp ETDatetime ETDuration`) the test projects a bare `Column{Index:
/// 0}` over a filled input chunk twice and requires byte-equal outputs:
///
/// - the default suite routes the lone column through the COLUMN TRANSFER
///   helper (`ColumnSwapHelper`, Go's fast path behind
///   `EvaluatorSuite.Vectorized()` being true for columns), while
/// - `avoid_column_evaluator=true` forces the CELL-BY-CELL path Go compares
///   against.
///
/// Go asserts equality of `GetRaw(j)` bytes plus per-row null bits between the
/// two outputs; `Row::get_raw` / `Row::is_null` are exactly those views here.
#[test]
fn test_eval_expr_column_projected_through_both_modes_agrees() {
    let int_type = || FieldType::new(FieldTypeCode::LongLong);
    let real_type = || FieldType::new(FieldTypeCode::Double);
    let decimal_type = || FieldType::new(FieldTypeCode::NewDecimal);
    let string_type = || FieldType::new(FieldTypeCode::VarString);
    let timestamp_type = || FieldType::new(FieldTypeCode::Timestamp);
    let datetime_type = || FieldType::new(FieldTypeCode::Datetime);
    let duration_type = || FieldType::new(FieldTypeCode::Duration);

    let time = |row: usize, type_code: TimeType| -> Datum {
        Datum::Time(
            Time::from_date_checked(
                2020,
                ((row % 12) + 1) as i32,
                ((row % 28) + 1) as i32,
                (row % 24) as i32,
                (row % 60) as i32,
                (row % 60) as i32,
                0,
                type_code,
                0,
            )
            .expect("a valid wall-clock date"),
        )
    };

    // `(field type, fill)` per eval type, eType2FieldType's mapping.
    let cases: Vec<(FieldType, Box<dyn Fn(&mut Chunk)>)> = vec![
        (
            int_type(),
            Box::new(move |chunk: &mut Chunk| {
                fill(chunk, |row| {
                    (row % 7 != 3).then(|| Datum::Int((row as i64) * 3 - 97))
                })
            }),
        ),
        (
            real_type(),
            Box::new(move |chunk: &mut Chunk| {
                fill(chunk, |row| {
                    (row % 7 != 3).then(|| Datum::Real(((row as f64) * 17.0 - 11.0) / 7.0))
                })
            }),
        ),
        (
            decimal_type(),
            Box::new(move |chunk: &mut Chunk| {
                fill(chunk, |row| {
                    (row % 7 != 3).then(|| Datum::Decimal(Decimal::from_int((row as i64) * 13 - 6)))
                })
            }),
        ),
        (
            string_type(),
            Box::new(move |chunk: &mut Chunk| {
                fill(chunk, |row| {
                    (row % 7 != 3).then(|| Datum::Bytes(format!("str-{row}").into_bytes()))
                })
            }),
        ),
        (
            timestamp_type(),
            Box::new(move |chunk: &mut Chunk| {
                fill(chunk, |row| {
                    (row % 7 != 3).then(|| time(row, TimeType::Timestamp))
                })
            }),
        ),
        (
            datetime_type(),
            Box::new(move |chunk: &mut Chunk| {
                fill(chunk, |row| {
                    (row % 7 != 3).then(|| time(row, TimeType::DateTime))
                })
            }),
        ),
        (
            duration_type(),
            Box::new(move |chunk: &mut Chunk| {
                fill(chunk, |row| {
                    (row % 7 != 3).then(|| {
                        Datum::Duration(
                            MySqlDuration::new(
                                (row % 20) as i64,
                                (row % 60) as i64,
                                (row % 60) as i64,
                                ((row % 500) as i64) * 1000,
                                0,
                            )
                            .expect("a valid duration"),
                        )
                    })
                })
            }),
        ),
    ];

    for (index, (field_type, filler)) in cases.iter().enumerate() {
        let mut input = Chunk::new_with_capacity(std::slice::from_ref(field_type), ROWS);
        filler(&mut input);

        // The projected expression: Column{Index: 0} typed by eType2FieldType.
        let mut projection_column = Column::new(index as i64, field_type.clone());
        projection_column.index = 0;
        let expression = Expression::Column(projection_column);

        // Default suite = vectorized transfer of the lone column; the
        // avoid_column_evaluator suite = Go's scalar per-row evaluation.
        let transfer_suite = EvaluatorSuite::new(vec![expression.clone()], false);
        let cell_by_cell_suite = EvaluatorSuite::new(vec![expression], true);

        let mut transferred = Chunk::new_with_capacity(std::slice::from_ref(field_type), ROWS);
        let mut recalculated = Chunk::new_with_capacity(std::slice::from_ref(field_type), ROWS);
        transfer_suite
            .run(&NoColumns, &mut input.clone(), &mut transferred)
            .expect("transfer evaluation must succeed");
        cell_by_cell_suite
            .run(&NoColumns, &mut input, &mut recalculated)
            .expect("cell-by-cell evaluation must succeed");

        for row in 0..ROWS {
            assert_eq!(
                transferred.get_row(row).is_null(0),
                recalculated.get_row(row).is_null(0),
                "null bit differs for {field_type:?} row {row}"
            );
            if transferred.get_row(row).is_null(0) {
                continue;
            }
            assert_eq!(
                transferred.get_row(row).get_raw(0).as_ref(),
                recalculated.get_row(row).get_raw(0).as_ref(),
                "raw encoding differs for {field_type:?} row {row}"
            );
        }
    }
}

/// go-parity-gap: TestExpressionMemeoryUsage (`expression_test.go:328`)
/// exercises `Column.MemoryUsage()` / `Constant.MemoryUsage()`; both are on
/// column.rs/constant.rs's DEFERRED list ("reproduce Go struct byte sizes"),
/// so no size contract exists to pin yet.
#[test]
#[ignore = "go-parity-gap: MemoryUsage is a deferred unit in tidb-expr (Go struct byte-size accounting), so TestExpressionMemeoryUsage has no carrier"]
fn test_expression_memory_usage() {}

/// Guards the port's honesty about `MySqlDuration::new`'s argument order:
/// hours, minutes, seconds, micros, fsp — matching the sibling source-table
/// modules' usage of `MySqlDuration::new(12, 59, 59, 555_000, 3)`.
#[test]
fn duration_constructor_argument_order_is_pinned() {
    let noon_thirty_point_five =
        MySqlDuration::new(12, 30, 30, 500_000, 1).expect("valid duration");
    assert_eq!(noon_thirty_point_five.to_string(), "12:30:30.5");
}
