// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

use super::*;
use crate::{column::Column as ExprColumn, NoColumns};
use tidb_datatype::FieldTypeFlags;

fn column(index: i64, field: &FieldType) -> Expression {
    let mut column = ExprColumn::new(index + 1, field.clone());
    column.index = index;
    Expression::Column(column)
}

fn numeric(value: i64, field: &FieldType) -> Datum {
    match field.eval_type() {
        EvalType::Int if field.is_unsigned() => Datum::UInt(value.unsigned_abs()),
        EvalType::Int => Datum::Int(value),
        EvalType::Real => Datum::Real(value as f64 / 10.0),
        EvalType::Decimal => Datum::Decimal(Decimal::parse_mysql(&format!("{}.25", value)).0),
        _ => unreachable!(),
    }
}

/// Go `TestVectorizedBuiltinArithmeticFunc`, `vecBuiltinArithmeticCases`:
/// all six operations, their three domains and four integer flag combinations.
#[test]
fn test_vectorized_builtin_arithmetic_func() {
    for code in [
        FieldTypeCode::LongLong,
        FieldTypeCode::Double,
        FieldTypeCode::NewDecimal,
    ] {
        for unsigned in [[false, false], [false, true], [true, false], [true, true]] {
            let fields = unsigned.map(|unsigned| {
                let mut field = FieldType::new(code);
                if unsigned {
                    field.add_flags(FieldTypeFlags::UNSIGNED);
                }
                if code == FieldTypeCode::NewDecimal {
                    field.set_decimal(2);
                    field.set_flen(20);
                }
                field
            });
            for name in ["plus", "minus", "mul", "div", "intdiv", "mod"] {
                let expression = crate::new_function::new_function_base(
                    &NoColumns,
                    name,
                    FieldType::new(code),
                    vec![column(0, &fields[0]), column(1, &fields[1])],
                )
                .unwrap();
                let result_type = expression.static_type().unwrap();
                let mut input = Chunk::new_chunk_with_capacity(&fields, 1024);
                for i in 0..1024 {
                    // Go uses range generators, keeping unsigned subtraction
                    // nonnegative and divisors nonzero in the agreement cases.
                    for j in 0..2 {
                        let number = if j == 0 {
                            500 + i as i64 % 101
                        } else {
                            1 + i as i64 % 97
                        };
                        let value = if (i + j * 3) % 11 == 0 {
                            Datum::Null
                        } else {
                            numeric(number, &fields[j])
                        };
                        input.append_datum(j, &value);
                    }
                }
                for selection in [None, Some(vec![1023, 0, 7, 7, 11, 200]), Some(vec![])] {
                    input.set_sel(selection);
                    let mut result = Column::default();
                    eval_into(
                        &expression,
                        result_type.eval_type(),
                        &NoColumns,
                        &input,
                        &mut result,
                    )
                    .unwrap();
                    for row in 0..input.num_rows() {
                        let actual =
                            value(&result, row, result_type.eval_type(), Some(result_type));
                        let expected = expression.eval(&NoColumns, input.get_row(row)).unwrap();
                        assert_eq!(actual, expected, "{name} {code:?} {unsigned:?} row {row}");
                    }
                }
            }
        }
    }
}

#[test]
fn vectorized_arithmetic_overflow_leaves_go_partial_results() {
    use crate::EvalErrorSql;

    struct SubtractionMode(bool);
    impl Columns for SubtractionMode {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn no_unsigned_subtraction(&self) -> bool {
            self.0
        }
    }

    let min = 1_u64 << 63;
    let max = min - 1;
    let integer = FieldTypeCode::LongLong;
    let real = FieldTypeCode::Double;
    let unsigned = [true, false];
    let signed = [false, false];
    let bits = f64::to_bits;
    let nan = 0x7ff8_0000_0000_1234;
    // Go's VecEvalInt/VecEvalReal results, including the failing row and the
    // unevaluated operand cells after it.
    let cases: [(_, _, _, bool, &[[u64; 2]], Option<usize>, _, &[(u64, bool)]); 8] = [
        (
            "plus",
            integer,
            signed,
            false,
            &[[5, 3], [max, 2], [4, 1], [max, 2], [8, 2]],
            Some(1),
            "BIGINT value is out of range in '(Column#1 + Column#2)'",
            &[(8, false), (2, true), (5, false), (2, false), (2, false)],
        ),
        (
            "minus",
            integer,
            signed,
            false,
            &[[5, 3], [0, min], [4, 1], [max, min], [8, 2]],
            None,
            "BIGINT value is out of range in '(Column#1 - Column#2)'",
            &[
                (2, false),
                (min, false),
                (3, false),
                (min, false),
                (2, false),
            ],
        ),
        (
            "minus",
            integer,
            unsigned,
            true,
            &[[5, 3], [min, 0], [8, 2]],
            None,
            "BIGINT value is out of range in '(Column#1 - Column#2)'",
            &[(2, false), (0, false), (2, false)],
        ),
        (
            "mul",
            integer,
            signed,
            false,
            &[[5, 3], [min, u64::MAX], [8, 2]],
            None,
            "BIGINT value is out of range in '(Column#1 * Column#2)'",
            &[(15, false), (min, true), (8, false)],
        ),
        (
            "mul",
            integer,
            [true, true],
            false,
            &[[5, 3], [min, 2], [8, 2]],
            None,
            "BIGINT UNSIGNED value is out of range in '(Column#1 * Column#2)'",
            &[(15, false), (min, false), (8, false)],
        ),
        (
            "unaryminus",
            integer,
            unsigned,
            false,
            &[[5, 0], [min, 0], [min + 1, 0], [8, 0]],
            None,
            "BIGINT value is out of range in '-9223372036854775809'",
            &[
                (u64::MAX - 4, false),
                (min, false),
                (min + 1, false),
                (8, false),
            ],
        ),
        (
            "plus",
            real,
            signed,
            false,
            &[
                [bits(5.0), bits(3.0)],
                [nan, bits(2.0)],
                [bits(1.0), bits(2.0)],
                [nan, bits(2.0)],
                [bits(8.0), bits(2.0)],
            ],
            Some(1),
            "DOUBLE value is out of range in '(Column#1 + Column#2)'",
            &[
                (bits(8.0), false),
                (nan, true),
                (bits(3.0), false),
                (nan, false),
                (bits(8.0), false),
            ],
        ),
        (
            "mul",
            real,
            signed,
            false,
            &[
                [bits(5.0), bits(3.0)],
                [nan, bits(2.0)],
                [bits(f64::MAX), bits(2.0)],
                [bits(8.0), bits(2.0)],
            ],
            None,
            "DOUBLE value is out of range in '(Column#1 * Column#2)'",
            &[
                (bits(15.0), false),
                (nan, false),
                (bits(f64::INFINITY), false),
                (bits(8.0), false),
            ],
        ),
    ];
    for (name, code, unsigned, force_signed, rows, null_row, message, expected) in cases {
        let fields = unsigned.map(|flag| FieldType::new(code).with_unsigned(flag));
        let ctx = &SubtractionMode(force_signed);
        let mut args = vec![column(0, &fields[0])];
        if name != "unaryminus" {
            args.push(column(1, &fields[1]));
        }
        let expression =
            crate::new_function::new_function_base(ctx, name, FieldType::new(code), args).unwrap();
        let mut input = Chunk::new_chunk_with_capacity(&fields, rows.len());
        for &[left, right] in rows {
            input.append_uint64(0, left);
            input.append_uint64(1, right);
        }
        if let Some(row) = null_row {
            input.column_mut(1).set_null(row, true);
        }
        let mut result = Column::default();
        let domain = fields[0].eval_type();
        let error = eval_into(&expression, domain, ctx, &input, &mut result)
            .unwrap_err()
            .to_sql_error();
        assert_eq!(error.code, 1690, "{name}");
        assert_eq!(error.message, message, "{name}");
        for (row, &(bits, is_null)) in expected.iter().enumerate() {
            assert_eq!(result.get_uint64(row), bits, "{name} row {row}");
            assert_eq!(result.is_null(row), is_null, "{name} row {row}");
        }
        if name == "unaryminus" {
            input.set_sel(Some(vec![0, 2, 3, 1]));
            let error = eval_into(&expression, domain, ctx, &input, &mut result)
                .unwrap_err()
                .to_sql_error();
            assert_eq!(error.code, 1690);
            assert_eq!(error.message, message);
            for (row, bits) in [u64::MAX - 4, min + 1, 8, min].into_iter().enumerate() {
                assert_eq!(result.get_uint64(row), bits, "selected unaryminus row {row}");
                assert!(!result.is_null(row));
            }
        }
    }
}

/// Go `TestVectorizedDecimalErrOverflow`: the native 81-digit decimal range
/// is independent of the SQL result type's maximum declared precision.
#[test]
fn test_vectorized_decimal_err_overflow() {
    let mut field = FieldType::new(FieldTypeCode::NewDecimal);
    field.set_flen(65);
    field.set_decimal(0);
    for (name, right) in [
        ("plus", "81e79"),
        ("minus", "-81e79"),
        ("mul", "81e79"),
        ("div", "0.1"),
    ] {
        let expression = crate::new_function::new_function_base(
            &NoColumns,
            name,
            field.clone(),
            vec![column(0, &field), column(1, &field)],
        )
        .unwrap();
        let mut input = Chunk::new_chunk_with_capacity(&[field.clone(), field.clone()], 1);
        input.append_datum(0, &Datum::Decimal(Decimal::parse_mysql("81e79").0));
        input.append_datum(1, &Datum::Decimal(Decimal::parse_mysql(right).0));
        let error = eval_into(
            &expression,
            EvalType::Decimal,
            &NoColumns,
            &input,
            &mut Column::default(),
        )
        .unwrap_err();
        assert!(
            matches!(
                error,
                EvalError::DataOutOfRange {
                    value: "DECIMAL",
                    ..
                }
            ),
            "{name}: {error:?}"
        );
    }
}

fn types() -> Vec<FieldType> {
    [
        FieldTypeCode::LongLong,
        FieldTypeCode::Double,
        FieldTypeCode::NewDecimal,
        FieldTypeCode::VarString,
        FieldTypeCode::Datetime,
        FieldTypeCode::Duration,
        FieldTypeCode::Json,
    ]
    .map(|code| {
        let mut field = FieldType::new(code);
        field.set_decimal(0);
        if code == FieldTypeCode::VarString {
            field.set_charset_name("utf8mb4");
            field.set_collation_name("utf8mb4_bin");
        }
        field
    })
    .to_vec()
}

/// Go's vec agreement tests read a duration column's `GoDurations()` and
/// compare `val.Duration` only (a duration cell has no fsp), and a string
/// column's `GetString(i)`.
fn go_vec(value: Datum) -> Datum {
    match value {
        Datum::Duration(duration) => {
            Datum::Duration(tidb_datatype::MySqlDuration::from_raw_parts(duration.nanoseconds(), 0))
        }
        // `GetString(i)` against the row's string: bytes, no collation.
        Datum::String(value) => Datum::Bytes(value.bytes().to_vec()),
        value => value,
    }
}

fn sample(field: &FieldType, i: usize) -> Datum {
    use tidb_datatype::{BinaryJSON, MySqlDuration, Time, TimeType};
    if i % 7 == 0 {
        return Datum::Null;
    }
    match field.eval_type() {
        EvalType::Int => Datum::Int(i as i64 % 3 - 1),
        EvalType::Real => Datum::Real(i as f64 / 7.0),
        EvalType::Decimal => Datum::Decimal(Decimal::from_int(i as i64)),
        EvalType::String => Datum::new_string_datum(format!("item-{}", i % 13)),
        EvalType::Datetime => Datum::Time(
            Time::from_date_checked(
                2020,
                1,
                1 + (i % 28) as i32,
                0,
                0,
                0,
                0,
                // Go's vec test generators stamp the field's own time type.
                match field.code() {
                    FieldTypeCode::Date => TimeType::Date,
                    FieldTypeCode::Timestamp => TimeType::Timestamp,
                    _ => TimeType::DateTime,
                },
                0,
            )
            .unwrap(),
        ),
        EvalType::Duration => {
            Datum::Duration(MySqlDuration::new((i % 24) as i64, 0, 0, 0, 0).unwrap())
        }
        EvalType::Json => Datum::Json(BinaryJSON::parse(&format!("[{}]", i % 9)).unwrap()),
        EvalType::VectorFloat32 => {
            Datum::VectorFloat32(tidb_datatype::VectorFloat32::must_create(vec![
                i as f32, 2.0, 3.0,
            ]))
        }
        _ => unreachable!(),
    }
}

fn agreement(name: &str, fields: &[FieldType]) {
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        name,
        FieldType::new(FieldTypeCode::LongLong),
        fields
            .iter()
            .enumerate()
            .map(|(i, ft)| column(i as i64, ft))
            .collect(),
    )
    .unwrap();
    let Expression::ScalarFunction(function) = &expression else {
        panic!("unfolded signature");
    };
    assert!(has_kernel(function), "{name}: missing Go vector kernel");
    let field = expression.static_type().unwrap();
    let mut input = Chunk::new_chunk_with_capacity(fields, 128);
    if fields.is_empty() {
        input.set_num_virtual_rows(128);
    }
    for i in 0..128 {
        for (j, field) in fields.iter().enumerate() {
            input.append_datum(j, &sample(field, i + j * 3));
        }
    }
    let mut result = Column::default();
    for selection in [None, Some(vec![127, 3, 0, 7, 7]), Some(vec![])] {
        input.set_sel(selection);
        eval_into(
            &expression,
            field.eval_type(),
            &NoColumns,
            &input,
            &mut result,
        )
        .unwrap();
        for row in 0..input.num_rows() {
            assert_eq!(
                go_vec(value(&result, row, field.eval_type(), Some(field))),
                go_vec(expression.eval(&NoColumns, input.get_row(row)).unwrap()),
                "{name} {fields:?} row {row}"
            );
        }
    }
}

/// Go `TestVectorizedGeneratedBuiltinCompareFunc` and
/// `TestVectorizedBuiltinCompareFunc`: all comparison domains and NULL equality.
#[test]
fn test_vectorized_generated_builtin_compare_func() {
    for field in types() {
        for name in ["eq", "ne", "lt", "le", "gt", "ge", "nulleq"] {
            agreement(name, &[field.clone(), field.clone()]);
        }
    }
}

#[test]
fn vectorized_real_comparisons_match_go_nan_and_null_rules() {
    let real = FieldType::new(FieldTypeCode::Double);
    let integer = FieldType::new(FieldTypeCode::LongLong);
    let nan_a = f64::from_bits(0x7ff8_0000_0000_1234);
    let nan_b = f64::from_bits(0xfff8_0000_0000_5678);
    let mut input = Chunk::new_chunk_with_capacity(&[real.clone(), real.clone()], 8);
    for [left, right] in [
        [Some(nan_a), Some(nan_b)],
        [Some(nan_a), Some(3.0)],
        [Some(3.0), Some(nan_b)],
        [Some(-0.0), Some(0.0)],
        [Some(f64::NEG_INFINITY), Some(f64::INFINITY)],
        [Some(8.0), None],
        [None, Some(8.0)],
        [None, None],
    ] {
        for (index, value) in [left, right].into_iter().enumerate() {
            if let Some(value) = value {
                input.append_float64(index, value);
            } else {
                input.append_null(index);
            }
        }
    }
    for (name, expected) in [
        ("eq", [1, 0, 0, 1, 0, -1, -1, -1]),
        ("ne", [0, 1, 1, 0, 1, -1, -1, -1]),
        ("lt", [0, 1, 0, 0, 1, -1, -1, -1]),
        ("le", [1, 1, 0, 1, 1, -1, -1, -1]),
        ("gt", [0, 0, 1, 0, 0, -1, -1, -1]),
        ("ge", [1, 0, 1, 1, 0, -1, -1, -1]),
        ("nulleq", [1, 0, 0, 1, 0, 0, 0, 1]),
        ("in", [1, 0, 0, 1, 0, -1, -1, -1]),
        ("in_constant", [0, 0, 0, 1, 0, 0, -1, -1]),
    ] {
        let (function, arguments) = if name == "in_constant" {
            let mut arguments = vec![column(0, &real)];
            for value in [nan_a, 0.0] {
                arguments.push(Expression::Constant(crate::constant::Constant::new(
                    Datum::Real(value),
                    real.clone(),
                )));
            }
            ("in", arguments)
        } else {
            (name, vec![column(0, &real), column(1, &real)])
        };
        let expression =
            crate::new_function::new_function_base(&NoColumns, function, integer.clone(), arguments)
                .unwrap();
        let mut result = Column::default();
        eval_into(&expression, EvalType::Int, &NoColumns, &input, &mut result).unwrap();
        for (row, want) in expected.into_iter().enumerate() {
            assert_eq!(result.is_null(row), want == -1, "{name} row {row}");
            if want != -1 {
                assert_eq!(result.get_int64(row), want, "{name} row {row}");
            }
        }
    }
    for (name, infinity) in [("greatest", f64::INFINITY), ("least", f64::NEG_INFINITY)] {
        let expression = crate::new_function::new_function_base(
            &NoColumns,
            name,
            real.clone(),
            vec![column(0, &real), column(1, &real)],
        )
        .unwrap();
        let mut result = Column::default();
        eval_into(&expression, EvalType::Real, &NoColumns, &input, &mut result).unwrap();
        for (row, want) in [nan_a, nan_a, 3.0, -0.0, infinity].into_iter().enumerate() {
            assert_eq!(result.get_uint64(row), want.to_bits(), "{name} row {row}");
        }
        assert!((5..8).all(|row| result.is_null(row)));
    }
}

/// Go `TestVectorizedBuiltinOtherFuncGenerated`: IN over typed columns.
#[test]
fn test_vectorized_builtin_other_func_generated() {
    for field in types() {
        agreement("in", &vec![field; 5]);
    }
}

/// Go `TestVectorizedBuiltinControlFuncGenerated`: IF, IFNULL and both CASE
/// arities over every generated return domain; Coalesce from compare cases.
#[test]
fn test_vectorized_builtin_control_func_generated() {
    let int = FieldType::new(FieldTypeCode::LongLong);
    for field in types() {
        agreement("if", &[int.clone(), field.clone(), field.clone()]);
        agreement("ifnull", &[field.clone(), field.clone()]);
        agreement("coalesce", &vec![field.clone(); 4]);
        for else_branch in [false, true] {
            let mut fields = vec![int.clone(), field.clone(), int.clone(), field.clone()];
            if else_branch {
                fields.push(field.clone());
            }
            agreement("case", &fields);
        }
    }
}

#[test]
fn vectorized_control_fallback_preserves_go_warnings_and_partial_rows() {
    use crate::{EvalErrorSql, builtin::ColumnsBuildContext};
    use std::cell::RefCell;

    #[derive(Default)]
    struct Warnings(RefCell<Vec<(u16, String)>>);
    impl Columns for Warnings {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn append_warning(&self, code: u16, message: &str) {
            self.0.borrow_mut().push((code, message.to_owned()));
        }

        fn warning_count(&self) -> usize {
            self.0.borrow().len()
        }

        fn truncate_warnings(&self, bookmark: usize) {
            self.0.borrow_mut().truncate(bookmark);
        }
    }

    let integer = FieldType::new(FieldTypeCode::LongLong);
    let text = text_field();
    let fields = [
        integer.clone(),
        integer.clone(),
        text.clone(),
        integer.clone(),
    ];
    for name in ["if", "ifnull", "case", "coalesce"] {
        for (mode, selected_error) in [
            ("warning", false),
            ("hidden-error", false),
            ("selected-error", true),
        ] {
            let ctx = Warnings::default();
            let mut input = Chunk::new_chunk_with_capacity(&fields, 4);
            for (first, value, text, negated) in [
                (Some(1), 10, "ignored", i64::MIN),
                (None, 11, "22tail", 11),
                (
                    None,
                    12,
                    "32tail",
                    if selected_error { i64::MIN } else { 4 },
                ),
                (Some(-1), 13, "ignored-again", i64::MIN),
            ] {
                if let Some(first) = first {
                    input.append_int64(0, first);
                } else {
                    input.append_null(0);
                }
                input.append_int64(1, value);
                input.append_string(2, text);
                input.append_int64(3, negated);
            }
            let branch = if mode == "warning" {
                crate::wrap_cast::build_cast_to(
                    &ColumnsBuildContext(&ctx),
                    column(2, &text),
                    integer.clone(),
                )
                .unwrap()
            } else {
                crate::new_function::new_function_base(
                    &ctx,
                    "unaryminus",
                    integer.clone(),
                    vec![column(3, &integer)],
                )
                .unwrap()
            };
            let mut args = vec![column(0, &integer)];
            let predicate = matches!(name, "if" | "case");
            if predicate {
                args.push(column(1, &integer));
            }
            args.push(branch);
            let expression =
                crate::new_function::new_function_base(&ctx, name, integer.clone(), args).unwrap();
            let mut result = Column::default();
            let status = eval_into(&expression, EvalType::Int, &ctx, &input, &mut result);
            let (first, last) = if predicate { (10, 13) } else { (1, -1) };
            let expected = match mode {
                "warning" => {
                    status.unwrap();
                    assert_eq!(
                        *ctx.0.borrow(),
                        [
                            (
                                1292,
                                "Truncated incorrect INTEGER value: '22tail'".to_owned()
                            ),
                            (
                                1292,
                                "Truncated incorrect INTEGER value: '32tail'".to_owned()
                            ),
                        ],
                        "{name}"
                    );
                    [(first, false), (22, false), (32, false), (last, false)]
                }
                "hidden-error" => {
                    status.unwrap();
                    [(first, false), (-11, false), (-4, false), (last, false)]
                }
                _ => {
                    let error = status.unwrap_err().to_sql_error();
                    assert_eq!(error.code, 1690, "{name}");
                    assert_eq!(
                        error.message, "BIGINT value is out of range in '--9223372036854775808'",
                        "{name}",
                    );
                    if name == "coalesce" {
                        [(first, false), (-11, false), (0, true), (last, false)]
                    } else {
                        [(first, false), (-11, false), (0, false), (0, false)]
                    }
                }
            };
            if mode != "warning" {
                assert!(ctx.0.borrow().is_empty(), "{name} {mode}");
            }
            for (row, (bits, is_null)) in expected.into_iter().enumerate() {
                assert_eq!(result.get_int64(row), bits, "{name} {mode} row {row}");
                assert_eq!(result.is_null(row), is_null, "{name} {mode} row {row}");
            }
        }
    }
}

/// Go `TestVectorizedBuiltinOpFunc`: nullable logical truth tables, bitwise
/// operators, and typed IS NULL/IS TRUE/IS FALSE/NOT predicates.
#[test]
fn test_vectorized_builtin_op_func() {
    let int = FieldType::new(FieldTypeCode::LongLong);
    for name in [
        "and",
        "or",
        "xor",
        "bitand",
        "bitor",
        "bitxor",
        "leftshift",
        "rightshift",
    ] {
        agreement(name, &[int.clone(), int.clone()]);
    }
    for field in types() {
        agreement("isnull", &[field.clone()]);
        if matches!(
            field.eval_type(),
            EvalType::Int | EvalType::Real | EvalType::Decimal
        ) {
            for name in ["not", "istrue", "isfalse", "istrue_with_null", "unaryminus"] {
                agreement(name, &[field.clone()]);
            }
        }
    }
}

#[test]
fn vectorized_integer_cast_overflow_preserves_go_result_cells() {
    use crate::{ErrorLevel, EvalErrorSql, builtin::ColumnsBuildContext};
    use std::cell::RefCell;

    struct CastContext {
        level: ErrorLevel,
        warnings: RefCell<Vec<(u16, String)>>,
    }
    impl Columns for CastContext {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn truncate_level(&self) -> ErrorLevel {
            self.level
        }

        fn append_warning(&self, code: u16, message: &str) {
            self.warnings.borrow_mut().push((code, message.to_owned()));
        }
    }

    let integer = FieldType::new(FieldTypeCode::LongLong);
    let real = FieldType::new(FieldTypeCode::Double);
    let mut decimal = FieldType::new(FieldTypeCode::NewDecimal);
    decimal.set_flen(31);
    decimal.set_decimal(3);
    let literal = ["1.500", "-2.500", "9223372036854775807.500", "0", "9.5"];
    let float = [1.5, -2.5, f64::INFINITY, 0.0, 8.5];
    for source in [real, decimal] {
        let is_decimal = source.eval_type() == EvalType::Decimal;
        let mut input = Chunk::new_chunk_with_capacity(&[source.clone()], 5);
        for row in 0..5 {
            if row == 3 {
                input.append_null(0);
            } else if is_decimal {
                let decimal = tidb_datatype::MyDecimal::from_string(literal[row].as_bytes()).0;
                input.append_my_decimal(0, &decimal);
            } else {
                input.append_float64(0, float[row]);
            }
        }
        let error_message = if is_decimal {
            "%s value is out of range in '%s'"
        } else {
            "constant +Inf overflows bigint"
        };
        for level in [ErrorLevel::Error, ErrorLevel::Warn] {
            let ctx = CastContext {
                level,
                warnings: RefCell::default(),
            };
            let expression = crate::wrap_cast::build_cast_to(
                &ColumnsBuildContext(&ctx),
                column(0, &source),
                integer.clone(),
            )
            .unwrap();
            let mut result = Column::default();
            let status = eval_into(&expression, EvalType::Int, &ctx, &input, &mut result);
            let last = if level == ErrorLevel::Error {
                let error = status.unwrap_err().to_sql_error();
                assert_eq!(error.code, 1690);
                assert_eq!(error.message, error_message);
                assert!(ctx.warnings.borrow().is_empty());
                0
            } else {
                status.unwrap();
                let warnings = ctx.warnings.borrow();
                if is_decimal {
                    let field = |kind, value| format!("%!s({kind}={value:0128})");
                    let words = [9, 223_372_036, 854_775_807, 500_000_000, 0, 0, 0, 0, 0]
                        .map(|word| field("int32", word))
                        .join(" ");
                    let want = format!(
                        "Truncated incorrect DECIMAL value: '{{{} {} {} %!s(bool=false) [{words}]}}'",
                        field("int8", 19),
                        field("int8", 3),
                        field("int8", 3),
                    );
                    assert_eq!(*warnings, [(1292, want)]);
                    10
                } else {
                    assert_eq!(*warnings, [(1690, error_message.to_owned())]);
                    8
                }
            };
            for (row, expected) in [2, if is_decimal { -3 } else { -2 }, i64::MAX, 0, last]
                .into_iter()
                .enumerate()
            {
                assert_eq!(
                    result.get_int64(row),
                    expected,
                    "{source:?} {level:?} row {row}"
                );
                assert_eq!(
                    result.is_null(row),
                    row == 3,
                    "{source:?} {level:?} row {row}"
                );
            }
        }
    }
}

/// Go `TestVectorizedCastStringAsDecimalWithUnsignedFlagInUnion`.
#[test]
fn test_vectorized_cast_string_as_decimal_with_unsigned_flag_in_union() {
    let source = FieldType::new(FieldTypeCode::VarString);
    let mut target = FieldType::new(FieldTypeCode::NewDecimal);
    target.add_flags(FieldTypeFlags::UNSIGNED);
    target.set_flen(20);
    target.set_decimal(4);
    let expression = crate::builtin_cast::build_cast_function_with_check(
        &crate::builtin::ColumnsBuildContext(&NoColumns),
        column(0, &source),
        target.clone(),
        true,
        false,
    )
    .unwrap();
    let mut input = Chunk::new_chunk_with_capacity(&[source], 128);
    for i in 0..128 {
        input.append_bytes(0, format!("{}.125", i as i64 - 64).as_bytes());
    }
    let mut result = Column::default();
    eval_into(
        &expression,
        EvalType::Decimal,
        &NoColumns,
        &input,
        &mut result,
    )
    .unwrap();
    for i in 0..128 {
        assert_eq!(
            value(&result, i, EvalType::Decimal, Some(&target)),
            expression.eval(&NoColumns, input.get_row(i)).unwrap()
        );
    }
}

#[test]
fn vectorized_math_overflow_leaves_go_partial_results() {
    use crate::EvalErrorSql;

    let integer = FieldType::new(FieldTypeCode::LongLong);
    let abs = crate::new_function::new_function_base(
        &NoColumns,
        "abs",
        integer.clone(),
        vec![column(0, &integer)],
    )
    .unwrap();
    let mut input = Chunk::new_chunk_with_capacity(&[integer.clone()], 4);
    for value in [-3, i64::MIN, -9] {
        input.append_int64(0, value);
    }
    input.append_null(0);
    let mut result = Column::default();
    let error = eval_into(&abs, EvalType::Int, &NoColumns, &input, &mut result)
        .unwrap_err()
        .to_sql_error();
    assert_eq!(error.code, 1690);
    assert_eq!(
        error.message,
        "BIGINT value is out of range in 'abs(-9223372036854775808)'"
    );
    assert_eq!(
        [result.get_int64(0), result.get_int64(1), result.get_int64(2)],
        [3, i64::MIN, -9]
    );
    assert!(result.is_null(3));

    let mut decimal = FieldType::new(FieldTypeCode::NewDecimal);
    decimal.set_flen(12);
    decimal.set_decimal(3);
    let mut input = Chunk::new_chunk_with_capacity(&[decimal.clone()], 4);
    for literal in ["-1.200", "9223372036854775808.200", "9.200"] {
        input.append_my_decimal(0, &Decimal::from_literal(literal).to_chunk_my_decimal_lossy());
    }
    input.append_null(0);
    for (name, first) in [("ceil", -1), ("floor", -2)] {
        let expression = crate::new_function::new_function_base(
            &NoColumns,
            name,
            integer.clone(),
            vec![column(0, &decimal)],
        )
        .unwrap();
        let error = eval_into(&expression, EvalType::Int, &NoColumns, &input, &mut result)
            .unwrap_err()
            .to_sql_error();
        assert_eq!(error.code, 1690);
        assert_eq!(error.message, "%s value is out of range in '%s'");
        assert_eq!(
            [result.get_int64(0), result.get_int64(1), result.get_int64(2)],
            [first, i64::MAX, 0]
        );
        assert!(result.is_null(3));
    }
}

/// Go `TestVectorizedBuiltinMathFunc`: nullable input columns for each math
/// signature; the existing scalar tests pin the per-cell transcendental math.
#[test]
fn test_vectorized_builtin_math_func() {
    let int = FieldType::new(FieldTypeCode::LongLong);
    let real = FieldType::new(FieldTypeCode::Double);
    let dec = FieldType::new(FieldTypeCode::NewDecimal);
    for field in [&int, &real, &dec] {
        for name in ["abs", "ceil", "floor", "round"] {
            agreement(name, &[field.clone()]);
        }
        for name in ["round", "truncate"] {
            agreement(name, &[field.clone(), int.clone()]);
        }
    }
    for name in [
        "sqrt", "sin", "cos", "tan", "atan", "degrees", "radians", "sign", "exp", "acos", "asin",
        "cot", "log", "log2", "log10",
    ] {
        agreement(name, &[real.clone()]);
    }
    for name in ["pow", "atan2"] {
        agreement(name, &[real.clone(), real.clone()]);
    }
    agreement("pi", &[]);
}

/// Go `TestVectorizedBuiltinStringFunc` and generated FIELD cases.
#[test]
fn test_vectorized_builtin_string_func() {
    let text = types()
        .into_iter()
        .find(|f| f.eval_type() == EvalType::String)
        .unwrap();
    let int = FieldType::new(FieldTypeCode::LongLong);
    for name in [
        "length",
        "char_length",
        "bit_length",
        "upper",
        "lower",
        "reverse",
        "hex",
        "unhex",
        "quote",
        "ascii",
        "ord",
        "ltrim",
        "rtrim",
        "trim",
        "to_base64",
        "from_base64",
    ] {
        agreement(name, &[text.clone()]);
    }
    for name in ["left", "right", "repeat", "substring"] {
        agreement(name, &[text.clone(), int.clone()]);
    }
    for name in [
        "strcmp",
        "find_in_set",
        "instr",
        "locate",
        "concat",
        "concat_ws",
    ] {
        agreement(name, &[text.clone(), text.clone()]);
    }
    for name in ["lpad", "rpad"] {
        agreement(name, &[text.clone(), int.clone(), text.clone()]);
    }
    for name in ["replace", "translate", "field"] {
        agreement(name, &vec![text.clone(); 3]);
    }
    agreement("elt", &[int.clone(), text.clone(), text.clone()]);
    agreement(
        "substring_index",
        &[text.clone(), text.clone(), int.clone()],
    );
    agreement("locate", &[text.clone(), text.clone(), int.clone()]);
    for name in ["space", "bin", "oct"] {
        agreement(name, &[int.clone()]);
    }
}

#[test]
fn vectorized_format_matches_go_signs_rounding_and_locale_warnings() {
    #[derive(Default)]
    struct Warnings(std::cell::RefCell<Vec<(u16, String)>>);
    impl Columns for Warnings {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn append_warning(&self, code: u16, message: &str) {
            self.0.borrow_mut().push((code, message.to_owned()));
        }
    }

    let real = FieldType::new(FieldTypeCode::Double);
    let integer = FieldType::new(FieldTypeCode::LongLong);
    let text = FieldType::new(FieldTypeCode::VarString)
        .with_collation(tidb_charset::Collation::Utf8Mb4Bin);
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        "format",
        text.clone(),
        vec![column(0, &real), column(1, &integer), column(2, &text)],
    )
    .unwrap();
    let mut input = Chunk::new_chunk_with_capacity(&[real, integer, text], 5);
    for (value, locale) in [
        (Some(f64::NEG_INFINITY), Some("en_US")),
        (Some(-0.0), Some("dE_dE")),
        (Some(999.995), Some("de_DE")),
        (Some(1.005), None),
        (None, Some("unrecognized")),
    ] {
        if let Some(value) = value {
            input.append_float64(0, value);
        } else {
            input.append_null(0);
        }
        input.append_int64(1, 2);
        if let Some(locale) = locale {
            input.append_bytes(2, locale.as_bytes());
        } else {
            input.append_null(2);
        }
    }
    input.set_sel(Some(vec![4, 0, 2, 1, 3, 0]));
    let context = Warnings::default();
    let mut result = Column::default();
    eval_into(&expression, EvalType::String, &context, &input, &mut result).unwrap();
    assert!(result.is_null(0));
    for (row, expected) in ["0.00", "1.000,00", "-0,00", "1.01", "0.00"]
        .into_iter()
        .enumerate()
    {
        assert_eq!(result.get_raw(row + 1).as_ref(), expected.as_bytes());
    }
    assert_eq!(*context.0.borrow(), [(1649, "Unknown locale: 'NULL'".to_owned())]);
}

#[test]
fn vectorized_json_contains_path_short_circuits_in_source_order() {
    let json = FieldType::new(FieldTypeCode::Json);
    let text = FieldType::new(FieldTypeCode::VarString)
        .with_collation(tidb_charset::Collation::Utf8Mb4Bin);
    let fields = [json, text.clone(), text.clone(), text];
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        "json_contains_path",
        FieldType::new(FieldTypeCode::LongLong),
        fields
            .iter()
            .enumerate()
            .map(|(i, field)| column(i as i64, field))
            .collect(),
    )
    .unwrap();
    let mut input = Chunk::new_chunk_with_capacity(&fields, 5);
    let json = tidb_datatype::BinaryJSON::parse(r#"{"a":[1,2],"z":"unrelated"}"#).unwrap();
    let mut payload = json.value().to_vec();
    *payload.last_mut().unwrap() = 0xff;
    let value = tidb_datatype::BinaryJSON::from_encoded_parts(json.type_code(), payload);
    for args in [
        [Some("OnE"), Some("$.a"), None],
        [Some("ALL"), Some("$.missing"), Some("$[")],
        [Some("one"), Some("$.missing"), None],
        [Some("bad"), Some("$"), Some("$")],
        [None, Some("$"), Some("$")],
    ] {
        input.append_json(0, &value);
        for (i, arg) in args.into_iter().enumerate() {
            if let Some(arg) = arg {
                input.append_bytes(i + 1, arg.as_bytes());
            } else {
                input.append_null(i + 1);
            }
        }
    }
    for (row, want) in [Datum::Int(1), Datum::Int(0), Datum::Null].into_iter().enumerate() {
        assert_eq!(expression.eval(&NoColumns, input.get_row(row)).unwrap(), want);
    }
    let mut result = Column::default();
    let err = eval_into(&expression, EvalType::Int, &NoColumns, &input, &mut result).unwrap_err();
    assert!(matches!(
        err,
        EvalError::Json(crate::JsonError::BadOneOrAllArg { .. })
    ));
    assert_eq!(result.rows(), 5);
    assert_eq!(result.get_int64(0), 1);
    assert_eq!(result.get_int64(1), 0);
    assert!(result.is_null(2));
    assert!(result.is_null(4));

    let mut input = Chunk::new_chunk_with_capacity(&fields, 1);
    let deep = format!("{}0{}", r#"{"child":"#.repeat(100), "}".repeat(100));
    input.append_json(0, &tidb_datatype::BinaryJSON::parse(&deep).unwrap());
    input.append_bytes(1, b"One");
    input.append_bytes(2, b"$[0 to 0]");
    input.append_null(3);
    eval_into(&expression, EvalType::Int, &NoColumns, &input, &mut result).unwrap();
    assert_eq!(result.get_int64(0), 1);
}

#[test]
fn literal_json_contains_path_reports_only_reached_path_errors() {
    let json = FieldType::new(FieldTypeCode::Json);
    let text = FieldType::new(FieldTypeCode::VarString);
    let literal = |value: &str| {
        Expression::Constant(crate::constant::Constant::new(
            Datum::new_string_datum(value),
            text.clone(),
        ))
    };
    let mut input = Chunk::new_chunk_with_capacity(&[json.clone()], 2);
    input.append_null(0);
    input.append_json(0, &tidb_datatype::BinaryJSON::parse(r#"{"n":1}"#).unwrap());
    for (mode, first, expected) in [
        ("ONE", "$.n", Some(1)),
        ("ALL", "$.missing", Some(0)),
        ("ALL", "$.n", None),
    ] {
        let expr = crate::new_function::new_function_base(
            &NoColumns,
            "json_contains_path",
            FieldType::new(FieldTypeCode::LongLong),
            vec![column(0, &json), literal(mode), literal(first), literal("$[")],
        ).unwrap();
        assert_eq!(expr.eval(&NoColumns, input.get_row(0)).unwrap(), Datum::Null);
        let scalar = expr.eval(&NoColumns, input.get_row(1));
        let mut result = Column::default();
        let vector = eval_into(&expr, EvalType::Int, &NoColumns, &input, &mut result);
        assert!(result.is_null(0));
        if let Some(expected) = expected {
            assert_eq!(scalar.unwrap(), Datum::Int(expected));
            vector.unwrap();
            assert_eq!(result.get_int64(1), expected);
        } else {
            assert!(matches!(scalar, Err(EvalError::Json(crate::JsonError::InvalidPath(_)))));
            assert!(matches!(vector, Err(EvalError::Json(crate::JsonError::InvalidPath(_)))));
        }
    }
}

#[test]
fn vectorized_json_extract_remembers_the_layout_of_a_wide_match() {
    let json = FieldType::new(FieldTypeCode::Json);
    let text = FieldType::new(FieldTypeCode::VarString)
        .with_collation(tidb_charset::Collation::Utf8Mb4Bin);
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        "json_extract",
        json.clone(),
        vec![column(0, &json), column(1, &text)],
    )
    .unwrap();
    let mut input = Chunk::new_chunk_with_capacity(&[json, text], 1);
    let payload = "界".repeat(Column::MIN_RETAINED_CELL_BYTES);
    let document = tidb_datatype::BinaryJSON::parse(&format!(r#"{{"a":"{payload}"}}"#)).unwrap();
    input.append_json(0, &document);
    input.append_bytes(1, b"$.a");

    let mut result = Column::default();
    eval_into(&expression, EvalType::Json, &NoColumns, &input, &mut result).unwrap();
    let extracted = result.get_json(0);
    assert!(extracted.known_layout().is_some_and(|layout| layout.is_canonical()));
    assert_eq!(extracted.as_string(), Some(payload.as_bytes()));

    let Datum::Json(scalar) = expression.eval(&NoColumns, input.get_row(0)).unwrap() else {
        panic!("expected the selected JSON string");
    };
    assert!(scalar.known_layout().is_some_and(|layout| layout.is_canonical()));
    {
        let raw = input.get_row(0).get_raw(0);
        let start = raw.windows(3).position(|bytes| bytes == "界".as_bytes()).unwrap();
        assert_eq!(scalar.as_string().unwrap().as_ptr(), raw.as_ptr().wrapping_add(start));
    }
    input.reset();
    assert_eq!(scalar.as_string(), Some(payload.as_bytes()));
}

#[test]
fn vectorized_json_length_merges_nulls_before_a_path_error() {
    let doc = FieldType::new(FieldTypeCode::Json);
    let text = FieldType::new(FieldTypeCode::VarString)
        .with_collation(tidb_charset::Collation::Utf8Mb4Bin);
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        "json_length",
        FieldType::new(FieldTypeCode::LongLong),
        vec![column(0, &doc), column(1, &text)],
    )
    .unwrap();
    let mut input = Chunk::new_chunk_with_capacity(&[doc, text], 5);
    let value = tidb_datatype::BinaryJSON::parse(r#"{"a":[1,2],"z":"unrelated"}"#).unwrap();
    let mut payload = value.value().to_vec();
    *payload.last_mut().unwrap() = 0xff;
    let value = tidb_datatype::BinaryJSON::from_encoded_parts(value.type_code(), payload);
    for (row, path) in [Some("$.a"), Some("$"), Some("$.a[*]"), None, Some("$")]
        .into_iter()
        .enumerate()
    {
        if row == 1 {
            input.append_null(0);
        } else {
            input.append_datum(0, &Datum::Json(value.clone()));
        }
        if let Some(path) = path {
            input.append_bytes(1, path.as_bytes());
        } else {
            input.append_null(1);
        }
    }
    let mut result = Column::default();
    assert_eq!(expression.eval(&NoColumns, input.get_row(0)).unwrap(), Datum::Int(2));
    let error = eval_into(&expression, EvalType::Int, &NoColumns, &input, &mut result)
        .unwrap_err();
    assert!(matches!(
        error,
        EvalError::Json(crate::JsonError::InvalidPathMultipleSelection)
    ));
    assert_eq!(result.rows(), 5);
    assert_eq!(result.get_int64(0), 2);
    assert!(result.is_null(1));
    assert!(result.is_null(3));
}

#[test]
fn vectorized_json_unquote_preserves_bytes_and_keeps_rows_before_an_error() {
    let text = FieldType::new(FieldTypeCode::VarString)
        .with_collation(tidb_charset::Collation::Utf8Mb4Bin);
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        "json_unquote",
        text.clone(),
        vec![column(0, &text)],
    )
    .unwrap();
    let mut input = Chunk::new_chunk_with_capacity(std::slice::from_ref(&text), 5);
    input.append_bytes(0, b"\xffplain");
    input.append_bytes(0, b"\"\xe2\x82\\n\"");
    input.append_null(0);
    input.append_bytes(0, b"\"invalid\\q\"");
    input.append_bytes(0, b"after");
    input.set_sel(Some(vec![2, 1, 0, 1, 3, 4]));
    let mut result = Column::default();
    let error = eval_into(&expression, EvalType::String, &NoColumns, &input, &mut result)
        .unwrap_err();
    assert!(matches!(error, EvalError::Json(crate::JsonError::InvalidText)));
    assert_eq!(result.rows(), 4);
    assert!(result.is_null(0));
    assert_eq!(result.get_raw(1).as_ref(), b"\xe2\x82\n");
    assert_eq!(result.get_raw(2).as_ref(), b"\xffplain");
    assert_eq!(result.get_raw(3).as_ref(), b"\xe2\x82\n");
    for (row, bytes) in [(1, &b"\xe2\x82\n"[..]), (2, &b"\xffplain"[..])] {
        let scalar = expression.eval(&NoColumns, input.get_row(row)).unwrap();
        assert_eq!(scalar.as_raw_bytes(), Some(bytes));
    }
}

#[test]
fn vectorized_search_uses_go_lowercase_and_rune_offsets() {
    let integer = FieldType::new(FieldTypeCode::LongLong);
    use tidb_charset::Collation::{Binary, Utf8Mb4Bin, Utf8Mb4UnicodeCi};
    for (collation, unpositioned, positioned) in [
        (Binary, [0, 1, 0, 3, 2, 1, 7], [0, 1, 0, 3, 2, 2, 7]),
        (Utf8Mb4Bin, [0, 1, 0, 2, 2, 1, 7], [0, 0, 1, 2, 2, 2, 7]),
        (
            Utf8Mb4UnicodeCi,
            [0, 1, 1, 2, 0, 1, 7],
            [0, 1, 1, 2, 0, 2, 7],
        ),
    ] {
        let text = FieldType::new(FieldTypeCode::VarString).with_collation(collation);
        let fields = [text.clone(), text.clone(), integer.clone()];
        let mut input = Chunk::new_chunk_with_capacity(&fields, 8);
        for (needle, hay, start) in [
            ("ß".as_bytes(), &b"aSSb"[..], 1),
            (&b"\xff"[..], &b"\xffx"[..], 1),
            ("�".as_bytes(), &b"\xffx"[..], 1),
            (&b"x"[..], "éx".as_bytes(), 2),
            (&b"\xa9"[..], "éx".as_bytes(), 1),
            (&b""[..], "é".as_bytes(), 2),
            (&b"aaab"[..], &b"aaaaaaaaab"[..], 2),
        ] {
            input.append_bytes(0, needle);
            input.append_bytes(1, hay);
            input.append_int64(2, start);
        }
        input.append_null(0);
        input.append_bytes(1, b"x");
        input.append_int64(2, 1);
        let mut result = Column::default();
        for (name, args, expected) in [
            (
                "locate",
                vec![column(0, &text), column(1, &text)],
                unpositioned,
            ),
            (
                "instr",
                vec![column(1, &text), column(0, &text)],
                unpositioned,
            ),
            (
                "locate",
                vec![column(0, &text), column(1, &text), column(2, &integer)],
                positioned,
            ),
        ] {
            let expression =
                crate::new_function::new_function_base(&NoColumns, name, integer.clone(), args)
                    .unwrap();
            input.set_sel(Some(vec![7, 2, 4, 0, 1, 3, 5, 6, 4]));
            eval_into(&expression, EvalType::Int, &NoColumns, &input, &mut result).unwrap();
            assert!(result.is_null(0));
            for (row, index) in [2, 4, 0, 1, 3, 5, 6, 4].into_iter().enumerate() {
                assert!(!result.is_null(row + 1));
                assert_eq!(
                    result.get_int64(row + 1),
                    expected[index],
                    "{name} {collation:?} row {index}"
                );
            }
        }
    }
}

#[test]
fn vectorized_field_uses_int64_carriers_for_signed_and_unsigned_arguments() {
    let signed = FieldType::new(FieldTypeCode::LongLong);
    let mut unsigned = signed.clone();
    unsigned.add_flags(FieldTypeFlags::UNSIGNED);
    let fields = [signed.clone(), unsigned, signed.clone()];
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        "field",
        signed,
        fields
            .iter()
            .enumerate()
            .map(|(i, field)| column(i as i64, field))
            .collect(),
    )
    .unwrap();
    let mut input = Chunk::new_chunk_with_capacity(&fields, 4);
    for (row, first) in [Some(u64::MAX), None, Some(7), Some(u64::MAX)]
        .into_iter()
        .enumerate()
    {
        if row == 3 {
            input.append_null(0);
        } else {
            input.append_int64(0, -1);
        }
        if let Some(first) = first {
            input.append_uint64(1, first);
        } else {
            input.append_null(1);
        }
        input.append_int64(2, if row == 2 { 1 } else { -1 });
    }
    let mut result = Column::default();
    for (selection, expected) in [
        (None, [1, 2, 0, 0]),
        (Some(vec![3, 0, 1, 0]), [0, 1, 2, 1]),
    ] {
        input.set_sel(selection);
        eval_into(&expression, EvalType::Int, &NoColumns, &input, &mut result).unwrap();
        for (row, index) in expected.into_iter().enumerate() {
            assert!(!result.is_null(row));
            assert_eq!(result.get_int64(row), index, "row {row}");
            assert_eq!(
                expression.eval(&NoColumns, input.get_row(row)).unwrap(),
                Datum::Int(index),
                "scalar row {row}"
            );
        }
    }
}

/// Go `TestVectorizedBuiltinCompareFunc`: extrema and INTERVAL typed columns.
#[test]
fn test_vectorized_builtin_compare_func() {
    for field in types()
        .into_iter()
        .filter(|f| f.eval_type() != EvalType::Json)
    {
        for name in ["greatest", "least"] {
            for count in 2..6 {
                agreement(name, &vec![field.clone(); count]);
            }
        }
    }
    for field in [
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::Double),
    ] {
        agreement("interval", &vec![field; 4]);
    }
}

/// Go `TestVectorizedBuiltinJSONFunc`: nullable document and scalar columns.
#[test]
fn test_vectorized_builtin_json_func() {
    let json = FieldType::new(FieldTypeCode::Json);
    let string = types().remove(3);
    for name in [
        "json_type",
        "json_unquote",
        "json_depth",
        "json_keys",
        "json_pretty",
        "json_storage_free",
        "json_storage_size",
        "json_length",
    ] {
        agreement(name, &[json.clone()]);
    }
    for name in [
        "json_merge",
        "json_merge_preserve",
        "json_merge_patch",
        "json_contains",
        "json_overlaps",
    ] {
        agreement(name, &[json.clone(), json.clone()]);
    }
    agreement("json_quote", &[string]);
    agreement("json_array", &[json.clone(), json]);
}

#[test]
fn vectorized_json_depth_reads_structure_without_validating_text() {
    use tidb_datatype::{BinaryJSON, JSON_TYPE_CODE_STRING};

    let field = FieldType::new(FieldTypeCode::Json);
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        "json_depth",
        FieldType::new(FieldTypeCode::LongLong),
        vec![column(0, &field)],
    ).unwrap();
    let string = BinaryJSON::from_encoded_parts(JSON_TYPE_CODE_STRING, [1, 0xff]);
    let array = BinaryJSON::parse(r#"[["a"]]"#).unwrap();
    let mut bytes = array.value().to_vec();
    *bytes.last_mut().unwrap() = 0xff;
    let array = BinaryJSON::from_encoded_parts(array.type_code(), bytes);

    let mut input = Chunk::new_chunk_with_capacity(&[field], 3);
    input.append_json(0, &string);
    input.append_json(0, &array);
    input.append_null(0);
    input.set_sel(Some(vec![1, 2, 0, 1]));
    let mut result = Column::default();
    eval_into(&expression, EvalType::Int, &NoColumns, &input, &mut result).unwrap();
    assert_eq!(result.get_int64(0), 3);
    assert!(result.is_null(1));
    assert_eq!(result.get_int64(2), 1);
    assert_eq!(result.get_int64(3), 3);
}

#[test]
fn json_keys_uses_selected_key_entries_on_both_sql_paths() {
    use tidb_datatype::BinaryJSON;

    let field = FieldType::new(FieldTypeCode::Json);
    let document = BinaryJSON::parse(r#"{"a":{"key":"value"},"z":"unrelated"}"#).unwrap();
    let mut bytes = document.value().to_vec();
    *bytes.last_mut().unwrap() = 0xff;
    let raw = BinaryJSON::from_encoded_parts(document.type_code(), bytes);
    let mut input = Chunk::new_chunk_with_capacity(&[field.clone()], 3);
    input.append_json(0, &raw);
    input.append_json(0, &BinaryJSON::parse("[1]").unwrap());
    input.append_null(0);
    input.set_sel(Some(vec![2, 0, 1, 0]));

    for (path, keys) in [(None, r#"["a","z"]"#), (Some("$.a"), r#"["key"]"#)] {
        let mut args = vec![column(0, &field)];
        if let Some(path) = path {
            args.push(Expression::Constant(crate::constant::Constant::new(
                Datum::new_string_datum(path),
                FieldType::new(FieldTypeCode::VarString),
            )));
        }
        let expression = crate::new_function::new_function_base(
            &NoColumns, "json_keys", field.clone(), args,
        ).unwrap();
        let keys = BinaryJSON::parse(keys).unwrap();
        let mut result = Column::default();
        eval_into(&expression, EvalType::Json, &NoColumns, &input, &mut result).unwrap();
        for row in [0, 2] {
            assert_eq!(expression.eval(&NoColumns, input.get_row(row)).unwrap(), Datum::Null);
            assert!(result.is_null(row));
        }
        for row in [1, 3] {
            assert_eq!(expression.eval(&NoColumns, input.get_row(row)).unwrap(), Datum::Json(keys.clone()));
            assert_eq!(result.get_json(row), keys);
        }
    }
}

#[derive(Default)]
struct InformationContext {
    database_reads: std::cell::Cell<usize>,
    session: crate::test_identity::TestSession,
}

impl Columns for InformationContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn current_database(&self) -> Option<String> {
        self.database_reads.set(self.database_reads.get() + 1);
        Some("test".into())
    }
    fn get_optional_prop_provider(
        &self,
        key: tidb_exprctx::OptionalEvalPropKey,
    ) -> Option<std::sync::Arc<dyn tidb_exprctx::DynOptionalEvalPropProvider>> {
        self.session.props().get(key)
    }
}

/// Go `TestVectorizedBuiltinInfoFunc`: one snapshot per batch and
/// `builtinLastInsertIDWithIDSig.vecEvalInt`'s last-non-NULL single write.
#[test]
fn test_vectorized_builtin_info_func() {
    let ctx = InformationContext::default();
    let int = FieldType::new(FieldTypeCode::LongLong);
    let mut input = Chunk::new_chunk_with_capacity(&[int.clone()], 4);
    for value in [Datum::Int(3), Datum::Null, Datum::Int(9), Datum::Null] {
        input.append_datum(0, &value);
    }
    let expression = crate::new_function::new_function_base(
        &ctx,
        "last_insert_id",
        int.clone(),
        vec![column(0, &int)],
    )
    .unwrap();
    let mut result = Column::default();
    eval_into(&expression, EvalType::Int, &ctx, &input, &mut result).unwrap();
    assert_eq!(*ctx.session.last_insert_id_calls.lock().unwrap(), [9]);
    assert_eq!(result.get_int64(0), 3);
    assert!(result.is_null(1));
    let database = crate::new_function::new_function_base(
        &ctx,
        "database",
        FieldType::new(FieldTypeCode::VarString),
        vec![],
    )
    .unwrap();
    ctx.database_reads.set(0);
    eval_into(&database, EvalType::String, &ctx, &input, &mut result).unwrap();
    assert_eq!(ctx.database_reads.get(), 1);
    for i in 0..4 {
        assert_eq!(result.get_bytes(i).as_ref(), b"test");
    }
}

/// Go `TestVectorizedBuiltinLikeFunc`.
#[test]
fn test_vectorized_builtin_like_func() {
    let string = types().remove(3);
    let int = FieldType::new(FieldTypeCode::LongLong);
    agreement("like", &[string.clone(), string, int]);
}

/// Go `TestVectorizedBuiltinVecFunc`.
#[test]
fn test_vectorized_builtin_vec_func() {
    let field = FieldType::new(FieldTypeCode::VectorFloat32);
    for name in [
        "vec_dims",
        "vec_l2_norm",
        "vec_as_text",
        "vec_l1_distance",
        "vec_l2_distance",
        "vec_negative_inner_product",
        "vec_cosine_distance",
    ] {
        agreement(
            name,
            &vec![
                field.clone();
                if matches!(name, "vec_dims" | "vec_l2_norm" | "vec_as_text") {
                    1
                } else {
                    2
                }
            ],
        );
    }
}

#[test]
fn vec_from_text_reads_selected_rows_in_order() {
    let string = FieldType::new(FieldTypeCode::VarString);
    let vector = FieldType::new(FieldTypeCode::VectorFloat32);
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        "vec_from_text",
        vector,
        vec![column(0, &string)],
    )
    .unwrap();
    let mut input = Chunk::new_chunk_with_capacity(&[string], 4);
    input.append_bytes(0, b"[1,2]");
    input.append_bytes(0, b"[invalid]");
    input.append_bytes(0, b"[3,4]");
    input.append_null(0);
    input.set_sel(Some(vec![2, 3, 0, 2]));
    let mut output = Column::default();
    eval_into(&expression, EvalType::VectorFloat32, &NoColumns, &input, &mut output).unwrap();
    assert_eq!(output.rows(), 4);
    assert_eq!(output.get_vector_float32(0).elements(), [3.0, 4.0]);
    assert!(output.is_null(1));
    assert_eq!(output.get_vector_float32(2).elements(), [1.0, 2.0]);
    assert_eq!(output.get_vector_float32(3).elements(), [3.0, 4.0]);
}

/// Go `TestVectorizedBuiltinEncryptionFunc`: nullable byte strings and keys.
#[test]
fn test_vectorized_builtin_encryption_func() {
    let string = types().remove(3);
    let int = FieldType::new(FieldTypeCode::LongLong);
    for name in [
        "md5",
        "sha1",
        "sm3",
        "compress",
        "uncompress",
        "uncompressed_length",
    ] {
        agreement(name, &[string.clone()]);
    }
    for name in ["aes_encrypt", "aes_decrypt", "encode", "decode"] {
        agreement(name, &[string.clone(), string.clone()]);
    }
    agreement("sha2", &[string, int]);
}

#[test]
fn vectorized_calendar_parts_match_go_zero_date_rules() {
    use crate::{ErrorLevel, EvalErrorSql};
    use std::cell::RefCell;
    use tidb_datatype::{CoreTime, DateModes, Time, TimeType};

    #[derive(Default)]
    struct CalendarContext {
        no_zero_dates: bool,
        strict: bool,
        warnings: RefCell<Vec<(u16, String)>>,
    }
    impl Columns for CalendarContext {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn date_modes(&self) -> DateModes {
            DateModes {
                no_zero_date: self.no_zero_dates,
                no_zero_in_date: self.no_zero_dates,
                ..DateModes::default()
            }
        }
        fn truncate_level(&self) -> ErrorLevel {
            if self.strict {
                ErrorLevel::Error
            } else {
                ErrorLevel::Warn
            }
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.warnings.borrow_mut().push((code, message.to_owned()));
        }
    }

    let mut field = FieldType::new(FieldTypeCode::Datetime);
    field.set_decimal(6);
    let mut input = Chunk::new_chunk_with_capacity(&[field.clone()], 7);
    for (row, core) in [
        CoreTime::from_date(2024, 2, 29, 23, 59, 59, 123_456),
        CoreTime::from_date(2019, 2, 0, 0, 0, 0, 0),
        CoreTime::from_date(0, 1, 1, 0, 0, 0, 0),
        CoreTime::from_date(2006, 0, 20, 1, 2, 3, 4),
        CoreTime::default(),
        CoreTime::default(),
        CoreTime::from_date(2019, 2, 31, 23, 0, 0, 0),
    ]
    .into_iter()
    .enumerate()
    {
        if row == 5 {
            input.append_null(0);
        } else {
            input.append_time(0, Time::new(core, TimeType::DateTime, 6).unwrap());
        }
    }
    let cases: [(&str, [i64; 7], &[usize]); 11] = [
        ("year", [2024, 2019, 0, 2006, 0, -1, 2019], &[]),
        ("month", [2, 2, 1, 0, 0, -1, 2], &[]),
        ("dayofmonth", [29, 0, 1, 20, 0, -1, 31], &[]),
        ("quarter", [1, 1, 1, 0, 0, -1, 1], &[]),
        ("dayofweek", [5, -1, 7, -1, -1, -1, 1], &[1, 3, 4]),
        ("dayofyear", [60, -1, 1, -1, -1, -1, 62], &[1, 3, 4]),
        ("weekday", [3, 3, 5, 1, -1, -1, 6], &[4]),
        ("week", [8, 0, 1, 0, -1, -1, 9], &[4]),
        ("yearweek", [202408, -1, 1, -1, -1, -1, 201909], &[1, 3, 4]),
        ("to_days", [739310, 737455, 1, 732665, -1, -1, 737486], &[4]),
        (
            "to_seconds",
            [
                63876470399,
                63716112000,
                86400,
                63302259723,
                -1,
                -1,
                63718873200,
            ],
            &[4],
        ),
    ];
    let warning = |row| {
        let time = match row {
            1 => "2019-02-00 00:00:00.000000",
            3 => "2006-00-20 01:02:03.000004",
            4 => "0000-00-00 00:00:00.000000",
            _ => unreachable!(),
        };
        (1292, format!("Incorrect datetime value: '{time}'"))
    };
    for (name, expected, warned) in cases {
        let ctx = CalendarContext::default();
        let expression = crate::new_function::new_function_base(
            &ctx,
            name,
            FieldType::new(FieldTypeCode::LongLong),
            vec![column(0, &field)],
        )
        .unwrap();
        let mut result = Column::default();
        eval_into(&expression, EvalType::Int, &ctx, &input, &mut result).unwrap();
        for (row, want) in expected.into_iter().enumerate() {
            assert_eq!(result.is_null(row), want == -1, "{name} row {row}");
            if want != -1 {
                assert_eq!(result.get_int64(row), want, "{name} row {row}");
            }
        }
        assert_eq!(
            *ctx.warnings.borrow(),
            warned.iter().copied().map(warning).collect::<Vec<_>>(),
            "{name}"
        );
    }

    for (name, expected) in [
        (
            "date",
            ["2024-02-29", "", "0000-01-01", "", "", "", "2019-02-31"],
        ),
        (
            "last_day",
            ["2024-02-29", "", "0000-01-31", "", "", "", "2019-02-28"],
        ),
    ] {
        let ctx = CalendarContext {
            no_zero_dates: true,
            ..CalendarContext::default()
        };
        let expression = crate::new_function::new_function_base(
            &ctx,
            name,
            FieldType::new(FieldTypeCode::Date),
            vec![column(0, &field)],
        )
        .unwrap();
        let mut result = Column::default();
        eval_into(&expression, EvalType::Datetime, &ctx, &input, &mut result).unwrap();
        for (row, want) in expected.into_iter().enumerate() {
            assert_eq!(result.is_null(row), want.is_empty(), "{name} row {row}");
            if !want.is_empty() {
                assert_eq!(result.get_time(row).to_string(), want, "{name} row {row}");
            }
        }
        assert_eq!(
            *ctx.warnings.borrow(),
            [warning(1), warning(3), warning(4)],
            "{name}"
        );
    }

    let ctx = CalendarContext {
        no_zero_dates: true,
        strict: true,
        ..CalendarContext::default()
    };
    let expression = crate::new_function::new_function_base(
        &ctx,
        "dayofweek",
        FieldType::new(FieldTypeCode::LongLong),
        vec![column(0, &field)],
    )
    .unwrap();
    let mut result = Column::default();
    let error = eval_into(&expression, EvalType::Int, &ctx, &input, &mut result)
        .unwrap_err()
        .to_sql_error();
    assert_eq!((error.code, error.message), warning(1));
    for (row, expected) in [5, 0, 0, 0, 0, 0, 0].into_iter().enumerate() {
        assert_eq!(result.get_int64(row), expected, "strict row {row}");
        assert_eq!(result.is_null(row), row == 5, "strict row {row}");
    }
}

#[test]
fn vectorized_duration_parts_keep_go_microseconds_at_time_boundary() {
    let mut field = FieldType::new(FieldTypeCode::Duration);
    field.set_decimal(6);
    let mut input = Chunk::new_chunk_with_capacity(&[field.clone()], 4);
    for value in [
        -3_020_399_999_999_000,
        -1_234_567_000,
        3_020_399_999_999_000,
    ] {
        input.append_int64(0, value);
    }
    input.append_null(0);
    for (name, values) in [
        ("hour", [838, 0, 838]),
        ("minute", [59, 0, 59]),
        ("second", [59, 1, 59]),
        ("microsecond", [999_999, 234_567, 999_999]),
        ("time_to_sec", [-3_020_399, -1, 3_020_399]),
    ] {
        let expression = crate::new_function::new_function_base(
            &NoColumns,
            name,
            FieldType::new(FieldTypeCode::LongLong),
            vec![column(0, &field)],
        )
        .unwrap();
        let mut result = Column::default();
        eval_into(&expression, EvalType::Int, &NoColumns, &input, &mut result).unwrap();
        for (row, want) in values.into_iter().enumerate() {
            assert!(!result.is_null(row), "{name} row {row}");
            assert_eq!(result.get_int64(row), want, "{name} row {row}");
        }
        assert!(result.is_null(3), "{name}");
    }
}

/// Go `TestVectorizedBuiltinTimeFunc`: date and duration extraction kernels.
#[test]
fn test_vectorized_builtin_time_func() {
    let date = types().remove(4);
    let duration = types().remove(5);
    let int = FieldType::new(FieldTypeCode::LongLong);
    let real = FieldType::new(FieldTypeCode::Double);
    for name in [
        "month",
        "year",
        "dayofmonth",
        "dayofweek",
        "dayofyear",
        "week",
        "weekofyear",
        "yearweek",
        "weekday",
        "quarter",
        "dayname",
        "monthname",
        "last_day",
        "date",
        "to_days",
        "to_seconds",
        "unix_timestamp",
    ] {
        agreement(name, &[date.clone()]);
    }
    for name in ["hour", "minute", "second", "microsecond", "time_to_sec"] {
        agreement(name, &[duration.clone()]);
    }
    for name in ["week", "yearweek"] {
        agreement(name, &[date.clone(), int.clone()]);
    }
    agreement("datediff", &[date.clone(), date]);
    agreement("from_days", &[int.clone()]);
    agreement("makedate", &[int.clone(), int.clone()]);
    agreement("maketime", &[int.clone(), int, real.clone()]);
    agreement("sec_to_time", &[real]);
}

/// Go `TestVectorizedBuiltinMiscellaneousFunc`: typed delegation and addresses.
#[test]
fn test_vectorized_builtin_miscellaneous_func() {
    for field in types() {
        agreement("any_value", &[field]);
    }
    let string = types().remove(3);
    let int = FieldType::new(FieldTypeCode::LongLong);
    for name in [
        "is_ipv4",
        "is_ipv6",
        "is_ipv4_compat",
        "is_ipv4_mapped",
        "is_uuid",
        "inet6_ntoa",
    ] {
        agreement(name, &[string.clone()]);
    }
    for name in ["inet_ntoa", "vitess_hash"] {
        agreement(name, &[int.clone()]);
    }
}

/// Go `TestVectorizedBuiltinOtherFunc`: BIT_COUNT's typed column kernel.
#[test]
fn test_vectorized_builtin_other_func() {
    agreement("bit_count", &[FieldType::new(FieldTypeCode::LongLong)]);
}

/// Go `TestVectorizedBuiltinTimeFunc`: ADD/SUB DATE argument domains and units.
#[test]
fn test_vectorized_builtin_date_arithmetic() {
    use crate::constant::Constant;
    // Go's test context has a statement timestamp: a DURATION date with a
    // non-clock unit converts through it (`Duration.ConvertToTime`).
    struct StmtClock;
    impl Columns for StmtClock {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn now(&self) -> Option<(i64, u32, i32)> {
            Some((1_700_000_000, 123_456_000, 0))
        }
    }
    let dates = [
        FieldTypeCode::Date,
        FieldTypeCode::Datetime,
        FieldTypeCode::Duration,
    ];
    let amounts = [
        FieldTypeCode::LongLong,
        FieldTypeCode::Double,
        FieldTypeCode::NewDecimal,
        FieldTypeCode::VarString,
    ];
    for date_code in dates {
        let mut date = FieldType::new(date_code);
        date.set_decimal(0);
        for amount_code in amounts {
            let mut amount = FieldType::new(amount_code);
            amount.set_decimal(0);
            for unit in ["DAY", "HOUR", "SECOND", "MINUTE_SECOND"] {
                for name in ["date_add", "date_sub"] {
                    let expression = crate::new_function::new_function_base(
                        &NoColumns,
                        name,
                        FieldType::new(FieldTypeCode::Datetime),
                        vec![
                            column(0, &date),
                            column(1, &amount),
                            Expression::Constant(Constant::new(
                                Datum::new_string_datum(unit),
                                FieldType::new(FieldTypeCode::VarString),
                            )),
                        ],
                    )
                    .unwrap();
                    let result_field = expression.static_type().unwrap();
                    let mut input =
                        Chunk::new_chunk_with_capacity(&[date.clone(), amount.clone()], 16);
                    for row in 0..16 {
                        input.append_datum(0, &sample(&date, row));
                        let value = match amount.eval_type() {
                            EvalType::Int => Datum::Int(2),
                            EvalType::Real => Datum::Real(2.0),
                            EvalType::Decimal => Datum::Decimal(Decimal::from_int(2)),
                            _ => Datum::new_string_datum("2"),
                        };
                        input.append_datum(1, &value);
                    }
                    let mut result = Column::default();
                    eval_into(
                        &expression,
                        result_field.eval_type(),
                        &StmtClock,
                        &input,
                        &mut result,
                    )
                    .unwrap();
                    for row in 0..input.num_rows() {
                        assert_eq!(
                            go_vec(value(&result, row, result_field.eval_type(), Some(result_field))),
                            go_vec(expression.eval(&StmtClock, input.get_row(row)).unwrap()),
                            "{name} {date_code:?} {amount_code:?} {unit} row {row}"
                        );
                    }
                }
            }
        }
    }
}

/// Go `TestVectorizedTimeFormatEmptyFormatReturnsNull`.
#[test]
fn test_vectorized_time_format_empty_format_returns_null() {
    let fields = [types().remove(5), types().remove(3)];
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        "time_format",
        fields[1].clone(),
        vec![column(0, &fields[0]), column(1, &fields[1])],
    )
    .unwrap();
    let mut input = Chunk::new_chunk_with_capacity(&fields, 2);
    for (hour, minute, second, mask) in [(12, 34, 56, ""), (1, 2, 3, "%H:%i:%s")] {
        input.append_datum(
            0,
            &Datum::Duration(
                tidb_datatype::MySqlDuration::new(hour, minute, second, 0, 0).unwrap(),
            ),
        );
        input.append_bytes(1, mask.as_bytes());
    }
    let mut result = Column::default();
    eval_into(
        &expression,
        EvalType::String,
        &NoColumns,
        &input,
        &mut result,
    )
    .unwrap();
    assert!(result.is_null(0));
    assert_eq!(result.get_bytes(1).as_ref(), b"01:02:03");
}

/// Go `TestVectorizedBuiltinCastFunc`: the 7 by 7 generated cast domains,
/// with result/error agreement on each nullable source cell.
#[test]
fn test_vectorized_builtin_cast_func() {
    for source in types() {
        for mut target in types() {
            if target.eval_type() == EvalType::Decimal {
                target.set_flen(65);
                target.set_decimal(6);
            }
            let expression = crate::builtin_cast::build_cast_function_with_check(
                &crate::builtin::ColumnsBuildContext(&NoColumns),
                column(0, &source),
                target.clone(),
                false,
                false,
            )
            .unwrap();
            for row in 0..16 {
                let mut input = Chunk::new_chunk_with_capacity(&[source.clone()], 1);
                input.append_datum(0, &sample(&source, row));
                let mut result = Column::default();
                let actual = eval_into(
                    &expression,
                    target.eval_type(),
                    &NoColumns,
                    &input,
                    &mut result,
                )
                .map(|()| value(&result, 0, target.eval_type(), Some(&target)));
                let expected = expression.eval(&NoColumns, input.get_row(0));
                match (actual, expected) {
                    (Ok(actual), Ok(expected)) => assert_eq!(
                        actual,
                        expected,
                        "{:?} to {:?}, row {row}",
                        source.eval_type(),
                        target.eval_type()
                    ),
                    (Err(actual), Err(expected)) => {
                        use crate::eval_error_sql::EvalErrorSql;
                        assert_eq!(actual.to_sql_error().code, expected.to_sql_error().code);
                    }
                    (actual, expected) => panic!(
                        "{:?} to {:?}, row {row}: {actual:?} vs {expected:?}",
                        source.eval_type(),
                        target.eval_type()
                    ),
                }
            }
        }
    }
}

/// Go `TestVectorizedBuiltinInfoFunc`: BENCHMARK repeats its entire argument
/// column, and only a positive signed constant count enables its vector kernel.
#[test]
fn test_vectorized_builtin_info_benchmark() {
    use crate::constant::Constant;
    let ctx = InformationContext::default();
    let int = FieldType::new(FieldTypeCode::LongLong);
    let mut input = Chunk::new_chunk_with_capacity(&[int.clone()], 3);
    for value in [1, 2, 9] {
        input.append_int64(0, value);
    }
    let effect = crate::new_function::new_function_base(
        &ctx,
        "last_insert_id",
        int.clone(),
        vec![column(0, &int)],
    )
    .unwrap();
    let expression = crate::new_function::new_function_base(
        &ctx,
        "benchmark",
        int.clone(),
        vec![
            Expression::Constant(Constant::new(Datum::Int(3), int)),
            effect,
        ],
    )
    .unwrap();
    let mut result = Column::default();
    eval_into(&expression, EvalType::Int, &ctx, &input, &mut result).unwrap();
    assert_eq!(*ctx.session.last_insert_id_calls.lock().unwrap(), [9, 9, 9]);
    for i in 0..3 {
        assert_eq!(result.get_int64(i), 0);
        assert!(!result.is_null(i));
    }
}

/// Go `TestVectorizedBuiltinLikeFunc` includes ILIKE's constant escape path.
#[test]
fn test_vectorized_builtin_ilike_func() {
    use crate::constant::Constant;
    let string = types().remove(3);
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        "ilike",
        FieldType::new(FieldTypeCode::LongLong),
        vec![
            column(0, &string),
            column(1, &string),
            Expression::Constant(Constant::new(
                Datum::Int(i64::from(b'\\')),
                FieldType::new(FieldTypeCode::LongLong),
            )),
        ],
    )
    .unwrap();
    let mut input = Chunk::new_chunk_with_capacity(&[string.clone(), string], 4);
    for (text, pattern) in [
        ("ABC", "a%"),
        ("abc_", "ABC\\_"),
        ("你好A", "你好a"),
        ("Abc", "D%"),
    ] {
        input.append_bytes(0, text.as_bytes());
        input.append_bytes(1, pattern.as_bytes());
    }
    let mut result = Column::default();
    eval_into(&expression, EvalType::Int, &NoColumns, &input, &mut result).unwrap();
    for row in 0..4 {
        assert_eq!(
            Datum::Int(result.get_int64(row)),
            expression.eval(&NoColumns, input.get_row(row)).unwrap()
        );
    }
}

#[test]
fn vectorized_string_integer_reads_mutably_shared_cells() {
    let mut string = FieldType::new(FieldTypeCode::VarString);
    string.set_charset_name("utf8mb4");
    string.set_collation_name("utf8mb4_bin");
    let fields = [string.clone()];
    let mut input = Chunk::new_chunk_with_capacity(&fields, 2);
    input.append_bytes(0, "🌿".repeat(2048).as_bytes());
    input.append_null(0);
    let mut alias = tidb_chunk::mutrow::MutRow::mut_row_from_types(&fields);
    alias.shallow_copy_partial_row(0, &mut input, 0);
    assert!(input.column(0).has_mutably_shared_bytes());

    let mut result = Column::default();
    for (name, expected) in [
        ("length", 8192),
        ("bit_length", 65536),
        ("char_length", 2048),
        ("ascii", 0xf0),
        ("ord", 0xf09f8cbf),
    ] {
        let expression = crate::new_function::new_function_base(
            &NoColumns,
            name,
            FieldType::new(FieldTypeCode::LongLong),
            vec![column(0, &string)],
        )
        .unwrap();
        eval_into(&expression, EvalType::Int, &NoColumns, &input, &mut result).unwrap();
        assert_eq!(result.get_int64(0), expected, "{name}");
        assert!(result.is_null(1), "{name}");
    }
}

#[test]
fn vectorized_quote_replaces_each_invalid_utf8_byte() {
    let string = FieldType::new(FieldTypeCode::VarString);
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        "quote",
        string.clone(),
        vec![column(0, &string)],
    )
    .unwrap();
    let mut input = Chunk::new_chunk_with_capacity(std::slice::from_ref(&string), 3);
    input.append_bytes(0, b"\xf0\x9f\x92'\x00");
    input.append_null(0);
    input.append_bytes(0, b"");
    input.set_sel(Some(vec![1, 0, 2, 0]));

    let mut result = Column::default();
    eval_into(&expression, EvalType::String, &NoColumns, &input, &mut result).unwrap();
    for (row, expected) in ["NULL", "'���\\'\\0'", "''", "'���\\'\\0'"]
        .into_iter()
        .enumerate()
    {
        assert!(!result.is_null(row));
        assert_eq!(result.get_bytes(row).as_ref(), expected.as_bytes());
    }
}

#[test]
fn vectorized_make_set_uses_only_the_64_bit_mask_and_keeps_raw_bytes() {
    let text = types().remove(3);
    let mut fields = vec![text.clone(); 67];
    fields[0] = FieldType::new(FieldTypeCode::LongLong);
    let mut input = Chunk::new_chunk_with_capacity(&fields, 1);
    let mask = i64::MIN | 1;
    input.append_int64(0, mask);
    let mut values = vec![Datum::Int(mask)];
    for col in 1..fields.len() {
        let bytes: &[u8] = match col {
            1 => b"\xff",
            64 => b"\xe2\x82",
            _ => b"ignored",
        };
        input.append_bytes(col, bytes);
        values.push(Datum::new_string_datum(bytes));
    }
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        "make_set",
        text,
        fields
            .iter()
            .enumerate()
            .map(|(i, f)| column(i as i64, f))
            .collect(),
    )
    .unwrap();
    let mut result = Column::default();
    eval_into(
        &expression,
        EvalType::String,
        &NoColumns,
        &input,
        &mut result,
    )
    .unwrap();
    assert_eq!(result.get_raw(0), b"\xff,\xe2\x82");
    assert_eq!(
        crate::string_fn::make_set(&values).unwrap().go_bytes(),
        b"\xff,\xe2\x82"
    );
}

#[test]
fn vectorized_export_set_keeps_raw_bytes_and_the_signed_bit_test() {
    let text = types().remove(3);
    let integer = FieldType::new(FieldTypeCode::LongLong);
    let fields = [
        integer.clone(),
        text.clone(),
        text.clone(),
        text.clone(),
        integer,
    ];
    let mut input = Chunk::new_chunk_with_capacity(&fields, 3);
    for (mask, on, off, separator, count) in [
        (5, Some(&b"\xff"[..]), &b"\xe2\x82"[..], &b";"[..], 3),
        (i64::MIN, Some(&b"1"[..]), &b"0"[..], &b""[..], 64),
        (0, None, &b"0"[..], &b""[..], 0),
    ] {
        input.append_int64(0, mask);
        match on {
            Some(value) => input.append_bytes(1, value),
            None => input.append_null(1),
        }
        input.append_bytes(2, off);
        input.append_bytes(3, separator);
        input.append_int64(4, count);
    }
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        "export_set",
        text,
        fields
            .iter()
            .enumerate()
            .map(|(i, f)| column(i as i64, f))
            .collect(),
    )
    .unwrap();
    let mut result = Column::default();
    eval_into(
        &expression,
        EvalType::String,
        &NoColumns,
        &input,
        &mut result,
    )
    .unwrap();
    assert_eq!(result.get_raw(0), b"\xff;\xe2\x82;\xff");
    assert_eq!(result.get_raw(1), &[b'0'; 64]);
    assert!(result.is_null(2));
    let scalar = crate::string_fn::export_set(&[
        Datum::Int(5),
        Datum::new_string_datum(b"\xff".as_slice()),
        Datum::new_string_datum(b"\xe2\x82".as_slice()),
        Datum::new_string_datum(";"),
        Datum::Int(3),
    ])
    .unwrap();
    assert_eq!(scalar.go_bytes(), b"\xff;\xe2\x82;\xff");
}

#[test]
fn vectorized_translate_maps_literal_and_row_runes_in_selection_order() {
    let text = types().remove(3);
    let fields = [text.clone(), text.clone(), text.clone()];
    let mut input = Chunk::new_chunk_with_capacity(&fields, 4);
    let from = "a好a�🦀X";
    let to = "🦀éqZ";
    input.append_bytes(0, &["a好🦀X".as_bytes(), b"\xe2\x82"].concat());
    input.append_bytes(1, from.as_bytes());
    input.append_bytes(2, to.as_bytes());
    input.append_bytes(0, "a好🦀X".as_bytes());
    input.append_bytes(1, "好".as_bytes());
    input.append_bytes(2, b"");
    input.append_bytes(0, b"aX");
    input.append_bytes(1, b"");
    input.append_bytes(2, b"");
    input.append_null(0);
    input.append_bytes(1, b"a");
    input.append_bytes(2, b"b");
    input.set_sel(Some(vec![3, 0, 1, 2, 0]));

    let literal = |s: &str| {
        Expression::Constant(crate::constant::Constant::new(
            Datum::new_string_datum(s),
            text.clone(),
        ))
    };
    for (mapping, expected) in [
        ([column(1, &text), column(2, &text)], ["a🦀X", "aX"]),
        ([literal(from), column(2, &text)], ["", ""]),
        ([literal(from), literal(to)], ["🦀é", "🦀"]),
    ] {
        let [from, to] = mapping;
        let expression = crate::new_function::new_function_base(
            &NoColumns,
            "translate",
            text.clone(),
            vec![column(0, &text), from, to],
        )
        .unwrap();
        let mut result = Column::default();
        eval_into(
            &expression,
            EvalType::String,
            &NoColumns,
            &input,
            &mut result,
        )
        .unwrap();
        assert!(result.is_null(0));
        for (row, expected) in ["🦀éZZ", expected[0], expected[1], "🦀éZZ"]
            .into_iter()
            .enumerate()
        {
            assert!(!result.is_null(row + 1));
            assert_eq!(result.get_raw(row + 1), expected.as_bytes());
        }
    }
}

#[test]
fn vectorized_insert_preserves_noops_and_checks_only_edited_bytes() {
    #[derive(Default)]
    struct Warnings(std::cell::RefCell<Vec<u16>>);
    impl Columns for Warnings {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn max_allowed_packet(&self) -> u64 {
            8
        }

        fn append_warning(&self, code: u16, _: &str) {
            self.0.borrow_mut().push(code);
        }
    }

    let ctx = Warnings::default();
    let text = types().remove(3);
    let integer = FieldType::new(FieldTypeCode::LongLong);
    let fields = [text.clone(), integer.clone(), integer, text.clone()];
    let mut input = Chunk::new_chunk_with_capacity(&fields, 5);
    for (source, position, length, replacement) in [
        (&b"abcdefghi"[..], 0, 1, Some(&b"xyz"[..])),
        (&b"abcdefghi"[..], 1, 1, Some(&b"xyz"[..])),
        (&b"a\xe2\x82z"[..], 4, 1, Some(&b"\xff"[..])),
        (&b"\xc3\xa9\xe2\x82"[..], 4, 1, Some(&b"xyz"[..])),
        (&b"abcdefghi"[..], 0, 1, None),
    ] {
        input.append_bytes(0, source);
        input.append_int64(1, position);
        input.append_int64(2, length);
        match replacement {
            Some(bytes) => input.append_bytes(3, bytes),
            None => input.append_null(3),
        }
    }
    input.set_sel(Some(vec![4, 3, 2, 0, 1, 2]));
    let expression = crate::new_function::new_function_base(
        &ctx,
        "insert_func",
        text,
        fields
            .iter()
            .enumerate()
            .map(|(i, f)| column(i as i64, f))
            .collect(),
    )
    .unwrap();
    let mut result = Column::default();
    eval_into(&expression, EvalType::String, &ctx, &input, &mut result).unwrap();
    assert_eq!(ctx.0.into_inner(), [1301]);
    assert!(result.is_null(0));
    assert_eq!(result.get_raw(1), b"\xc3\xa9\xe2\x82");
    assert_eq!(result.get_raw(2), b"a\xef\xbf\xbd\xef\xbf\xbd\xff");
    assert_eq!(result.get_raw(3), b"abcdefghi");
    assert!(result.is_null(4));
    assert_eq!(result.get_raw(5), result.get_raw(2));
}

#[test]
fn vectorized_pad_matches_go_packet_order_and_malformed_utf8() {
    #[derive(Default)]
    struct Warnings(std::cell::RefCell<Vec<u16>>);
    impl Columns for Warnings {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn max_allowed_packet(&self) -> u64 {
            1 << 26
        }

        fn append_warning(&self, code: u16, _: &str) {
            self.0.borrow_mut().push(code);
        }
    }

    let string = types().remove(3);
    let mut binary = string.clone();
    binary.set_charset_name("binary");
    binary.set_collation_name("binary");
    binary.add_flags(FieldTypeFlags::BINARY);
    for pad in [string.clone(), binary] {
        let fields = [
            string.clone(),
            FieldType::new(FieldTypeCode::LongLong),
            pad.clone(),
        ];
        let mut input = Chunk::new_chunk_with_capacity(&fields, 7);
        for (source, target, pad) in [
            (Some(&b"\xc2"[..]), 3, Some(&b"\x80\xc2"[..])),
            (None, -1, Some(&b"."[..])),
            (Some(&b"x"[..]), -1, None),
            (Some(&b"x"[..]), (1 << 62) + 1, None),
            (Some(&b"x"[..]), 4_194_305, Some(&b""[..])),
            (Some(&b"x"[..]), 2, Some(&b""[..])),
            (Some(&b"\xc2"[..]), 9, Some(&b"\x80\xc2"[..])),
        ] {
            for (col, bytes) in [(0, source), (2, pad)] {
                match bytes {
                    Some(bytes) => input.append_bytes(col, bytes),
                    None => input.append_null(col),
                }
            }
            input.append_int64(1, target);
        }
        input.set_sel(Some(vec![4, 2, 1, 0, 5, 3, 0, 6]));
        for op in ["lpad", "rpad"] {
            let ctx = Warnings::default();
            let expression = crate::new_function::new_function_base(
                &ctx,
                op,
                string.clone(),
                fields
                    .iter()
                    .enumerate()
                    .map(|(i, f)| column(i as i64, f))
                    .collect(),
            )
            .unwrap();
            let mut result = Column::default();
            eval_into(&expression, EvalType::String, &ctx, &input, &mut result).unwrap();
            let binary = pad.is_binary_string();
            assert_eq!(
                ctx.0.into_inner(),
                if binary { vec![1301, 1301] } else { vec![1301] }
            );
            assert_eq!(result.is_null(0), !binary);
            for row in [1, 2, 5] {
                assert!(result.is_null(row));
            }
            let expected: &[u8] = match (binary, op) {
                (true, "lpad") => b"\x80\xc2\xc2",
                (true, _) => b"\xc2\x80\xc2",
                (false, "lpad") => "�\u{80}�".as_bytes(),
                (false, _) => "\u{80}\u{80}�".as_bytes(),
            };
            for row in [3, 6] {
                assert_eq!(result.get_raw(row), expected);
            }
            assert!(!result.is_null(4));
            assert!(result.get_raw(4).is_empty());
            let extended: &[u8] = match (binary, op) {
                (true, "lpad") => b"\x80\xc2\x80\xc2\x80\xc2\x80\xc2\xc2",
                (true, _) => b"\xc2\x80\xc2\x80\xc2\x80\xc2\x80\xc2",
                (false, "lpad") => "�\u{80}\u{80}\u{80}\u{80}�\0\0�".as_bytes(),
                (false, _) => "\u{80}\u{80}\u{80}\u{80}\u{80}�\0\0\0".as_bytes(),
            };
            assert_eq!(result.get_raw(7), extended);
        }
    }
}

#[test]
fn vectorized_space_checks_packet_limit_for_each_selected_row() {
    #[derive(Default)]
    struct PacketLimit(std::cell::RefCell<Vec<u16>>);
    impl Columns for PacketLimit {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn max_allowed_packet(&self) -> u64 {
            6
        }

        fn truncate_level(&self) -> crate::ErrorLevel {
            crate::ErrorLevel::Warn
        }

        fn append_warning(&self, code: u16, _: &str) {
            self.0.borrow_mut().push(code);
        }
    }

    let ctx = PacketLimit::default();
    let integer = FieldType::new(FieldTypeCode::LongLong);
    let mut input = Chunk::new_chunk_with_capacity(std::slice::from_ref(&integer), 4);
    input.append_int64(0, -1);
    input.append_int64(0, 6);
    input.append_int64(0, 7);
    input.append_null(0);
    input.set_sel(Some(vec![2, 1, 0, 3, 2]));
    let expression = crate::new_function::new_function_base(
        &ctx,
        "space",
        FieldType::new(FieldTypeCode::VarString),
        vec![column(0, &integer)],
    )
    .unwrap();
    let mut result = Column::default();
    eval_into(&expression, EvalType::String, &ctx, &input, &mut result).unwrap();
    for row in [0, 3, 4] {
        assert!(result.is_null(row));
    }
    for (row, expected) in [(1, "      "), (2, "")] {
        assert!(!result.is_null(row));
        assert_eq!(result.get_bytes(row).as_ref(), expected.as_bytes());
    }
    assert_eq!(ctx.0.into_inner(), [1301, 1301]);
}

#[test]
fn vectorized_concat_checks_packets_in_argument_order() {
    #[derive(Default)]
    struct PacketLimit(std::cell::RefCell<Vec<(u16, String)>>);
    impl Columns for PacketLimit {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn max_allowed_packet(&self) -> u64 {
            3
        }

        fn truncate_level(&self) -> crate::ErrorLevel {
            crate::ErrorLevel::Warn
        }

        fn append_warning(&self, code: u16, message: &str) {
            self.0.borrow_mut().push((code, message.to_owned()));
        }
    }

    let ctx = PacketLimit::default();
    let mut string = types().remove(3);
    string.set_flen(32);
    let repeat = crate::new_function::new_function_base(
        &ctx,
        "repeat",
        string.clone(),
        vec![
            column(1, &string),
            Expression::Constant(crate::constant::Constant::new(
                Datum::Int(2),
                FieldType::new(FieldTypeCode::LongLong),
            )),
        ],
    )
    .unwrap();
    let concat = crate::new_function::new_function_base(
        &ctx,
        "concat",
        string.clone(),
        vec![column(0, &string), repeat, column(2, &string)],
    )
    .unwrap();
    let mut input = Chunk::new_chunk_with_capacity(&[string.clone(), string.clone(), string], 6);
    for (first, second, third) in [
        (Some("xxxx"), "b", ""),
        (Some("a"), "zz", ""),
        (None, "zz", ""),
        (Some("a"), "b", ""),
        (Some("aa"), "b", ""),
        (Some(""), "b", "c"),
    ] {
        match first {
            Some(text) => input.append_bytes(0, text.as_bytes()),
            None => input.append_null(0),
        }
        input.append_bytes(1, second.as_bytes());
        input.append_bytes(2, third.as_bytes());
    }
    let mut result = Column::default();
    eval_into(&concat, EvalType::String, &ctx, &input, &mut result).unwrap();
    for row in [0, 1, 2, 4] {
        assert!(result.is_null(row));
    }
    assert_eq!(result.get_bytes(3).as_ref(), b"abb");
    assert_eq!(result.get_bytes(5).as_ref(), b"bbc");
    let warnings = ctx.0.into_inner();
    assert_eq!(warnings.len(), 4);
    for ((code, message), op) in warnings.iter().zip(["concat", "repeat", "repeat", "concat"]) {
        assert_eq!(*code, 1301);
        assert_eq!(
            message,
            &format!("Result of {op}() was larger than max_allowed_packet (3) - truncated")
        );
    }
}

/// Go `TestVectorizedBuiltinStringFunc`, `builtinConcatWSSig.vecEvalString`:
/// retain the source's row-index condition when accounting for separators.
#[test]
fn test_vectorized_concat_ws_packet_accounting() {
    struct PacketLimit;
    impl Columns for PacketLimit {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn max_allowed_packet(&self) -> u64 {
            3
        }
    }
    let ctx = PacketLimit;
    let string = types().remove(3);
    let fields = vec![string.clone(); 3];
    let expression = crate::new_function::new_function_base(
        &ctx,
        "concat_ws",
        string.clone(),
        (0..3).map(|i| column(i, &string)).collect(),
    )
    .unwrap();
    let mut input = Chunk::new_chunk_with_capacity(&fields, 3);
    for _ in 0..3 {
        input.append_bytes(0, b",,");
        input.append_bytes(1, b"a");
        input.append_bytes(2, b"b");
    }
    let mut result = Column::default();
    eval_into(&expression, EvalType::String, &ctx, &input, &mut result).unwrap();
    assert_eq!(result.get_bytes(0).as_ref(), b"a,,b");
    assert_eq!(result.get_bytes(1).as_ref(), b"a,,b");
    assert!(result.is_null(2));
}

/// Go `TestVectorizedBuiltinMathFuncForRand`: the constant-seed signature
/// advances one captured generator, which its Clone also shares.
#[test]
fn test_vectorized_builtin_math_func_for_rand() {
    use crate::constant::Constant;
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        "rand",
        FieldType::new(FieldTypeCode::Double),
        vec![Expression::Constant(Constant::new(
            Datum::Int(1),
            FieldType::new(FieldTypeCode::LongLong),
        ))],
    )
    .unwrap();
    let copy = expression.clone();
    let expected = crate::MysqlRng::new_with_seed(1);
    let mut input = Chunk::new_chunk_with_capacity(&[], 16);
    input.set_num_virtual_rows(16);
    let mut result = Column::default();
    for expression in [&expression, &copy, &expression] {
        eval_into(expression, EvalType::Real, &NoColumns, &input, &mut result).unwrap();
        for row in 0..16 {
            assert_eq!(result.get_float64(row), expected.gen());
        }
    }
    agreement("rand", &[FieldType::new(FieldTypeCode::LongLong)]);
}

/// Go `builtinConvSig.vectorized` deliberately remains false for hybrid arguments.
#[test]
fn conv_keeps_go_non_vectorized_signature() {
    let int = FieldType::new(FieldTypeCode::LongLong);
    let string = FieldType::new(FieldTypeCode::VarString);
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        "conv",
        string.clone(),
        vec![column(0, &string), column(1, &int), column(2, &int)],
    )
    .unwrap();
    assert!(!crate::chunk_executor::vectorized(&expression));
}

/// Go `builtinInIntSig.vecEvalInt`: hash probes merge the input's NULL bitmap.
#[test]
fn in_constant_hash_set_preserves_input_nulls_and_first_sign_flag() {
    let field = FieldType::new(FieldTypeCode::LongLong);
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        "in",
        field.clone(),
        vec![
            column(0, &field),
            Expression::Constant(crate::constant::Constant::new(Datum::UInt(u64::MAX), {
                let mut ty = field.clone();
                ty.add_flags(FieldTypeFlags::UNSIGNED);
                ty
            })),
            Expression::Constant(crate::constant::Constant::new(
                Datum::Int(-1),
                field.clone(),
            )),
        ],
    )
    .unwrap();
    let mut input = Chunk::new_chunk_with_capacity(&[field], 3);
    for v in [Datum::Null, Datum::Int(-1), Datum::Int(1)] {
        input.append_datum(0, &v);
    }
    let mut result = Column::default();
    eval_into(&expression, EvalType::Int, &NoColumns, &input, &mut result).unwrap();
    assert!(result.is_null(0));
    assert_eq!(result.get_int64(1), 0);
    assert_eq!(result.get_int64(2), 0);
}

/// Go `TestVectorizedBuiltinTimeFunc`: the dedicated literal signatures retain
/// the parsed value and precision, and evaluate a full virtual input chunk.
#[test]
fn test_vectorized_builtin_time_literal_func() {
    for (name, text) in [
        ("'tidb`.(dateliteral", "2024-01-02"),
        ("'tidb`.(timeliteral", "12:34:56.120"),
        ("'tidb`.(timestampliteral", "2024-01-02 12:34:56.120"),
    ] {
        let field = FieldType::new(FieldTypeCode::VarString);
        let expression = crate::new_function::new_function_base(
            &NoColumns,
            name,
            field.clone(),
            vec![Expression::Constant(crate::constant::Constant::new(
                Datum::new_string_datum(text),
                field,
            ))],
        )
        .unwrap();
        assert!(crate::chunk_executor::vectorized(&expression));
        let Expression::ScalarFunction(function) = &expression else {
            panic!("unfolded literal signature")
        };
        assert!(function.args.is_empty());
        let target = expression.static_type().unwrap();
        let expected = expression.eval(&NoColumns, Row::empty()).unwrap();
        let mut input = Chunk::new_chunk_with_capacity(&[], 128);
        input.set_num_virtual_rows(128);
        let mut result = Column::default();
        eval_into(
            &expression,
            target.eval_type(),
            &NoColumns,
            &input,
            &mut result,
        )
        .unwrap();
        for i in 0..128 {
            assert_eq!(
                value(&result, i, target.eval_type(), Some(target)),
                expected
            );
        }
    }
}

/// Go `builtinUTCTimestampWithArgSig.vecEvalTime` merges its FSP NULL bitmap,
/// unlike `evalTime`, and the duration clock kernels read one timestamp per batch.
#[test]
fn vectorized_clock_nulls_and_snapshot_follow_go() {
    struct Clock(std::cell::Cell<usize>);
    impl Columns for Clock {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn now(&self) -> Option<(i64, u32, i32)> {
            self.0.set(self.0.get() + 1);
            Some((1_700_000_000, 123_456_789, 0))
        }
    }
    let context = Clock(std::cell::Cell::new(0));
    let int = FieldType::new(FieldTypeCode::LongLong);
    let mut input = Chunk::new_chunk_with_capacity(&[int.clone()], 3);
    for datum in [Datum::Null, Datum::Int(0), Datum::Int(3)] {
        input.append_datum(0, &datum);
    }
    for name in ["utc_timestamp", "utc_time", "current_time"] {
        let expression = crate::new_function::new_function_base(
            &context,
            name,
            int.clone(),
            vec![column(0, &int)],
        )
        .unwrap();
        let field = expression.static_type().unwrap();
        context.0.set(0);
        let mut result = Column::default();
        eval_into(
            &expression,
            field.eval_type(),
            &context,
            &input,
            &mut result,
        )
        .unwrap();
        assert_eq!(result.is_null(0), name != "current_time", "{name}");
        if name != "utc_timestamp" {
            assert_eq!(context.0.get(), 1, "{name}");
        }
        for i in 1..3 {
            assert_eq!(
                go_vec(value(&result, i, field.eval_type(), Some(field))),
                go_vec(expression.eval(&context, input.get_row(i)).unwrap()),
                "{name} {i}"
            );
        }
        if name == "utc_timestamp" {
            assert!(!expression
                .eval(&context, input.get_row(0))
                .unwrap()
                .is_null());
        }
    }
}

/// Go `testVectorizedBuiltinFunc`: deterministic source-case generators,
/// complete columns, duplicate/reordered selection, and empty input.
fn agreement_rows(name: &str, fields: &[FieldType], rows: &[Vec<Datum>]) {
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        name,
        FieldType::new(FieldTypeCode::LongLong),
        fields
            .iter()
            .enumerate()
            .map(|(i, field)| {
                if matches!(name, "extract" | "timestampadd") && i == 0 {
                    Expression::Constant(crate::constant::Constant::new(
                        rows[0][i].clone(),
                        field.clone(),
                    ))
                } else {
                    column(i as i64, field)
                }
            })
            .collect(),
    )
    .unwrap();
    assert!(crate::chunk_executor::vectorized(&expression), "{name}");
    let field = expression.static_type().unwrap();
    let mut input = Chunk::new_chunk_with_capacity(fields, 128);
    for i in 0..128 {
        for (j, value) in rows[i % rows.len()].iter().enumerate() {
            input.append_datum(j, value);
        }
    }
    let mut result = Column::default();
    for selection in [None, Some(vec![127, 0, 3, 3]), Some(vec![])] {
        input.set_sel(selection);
        eval_into(
            &expression,
            field.eval_type(),
            &NoColumns,
            &input,
            &mut result,
        )
        .unwrap_or_else(|error| panic!("{name}: {error:?}"));
        for i in 0..input.num_rows() {
            assert_eq!(
                go_vec(value(&result, i, field.eval_type(), Some(field))),
                go_vec(expression.eval(&NoColumns, input.get_row(i)).unwrap()),
                "{name} row {i}"
            );
        }
    }
}

/// Go `vecBuiltinJSONCases` path and document generators.
#[test]
fn test_vectorized_builtin_json_func_paths() {
    let json = FieldType::new(FieldTypeCode::Json);
    let string = types().remove(3);
    let document = Datum::Json(tidb_datatype::BinaryJSON::parse(r#"{"a":{"c":3},"b":2}"#).unwrap());
    let child = Datum::Json(tidb_datatype::BinaryJSON::parse(r#"{"b":2}"#).unwrap());
    let text = |s: &str| Datum::new_string_datum(s);
    for name in ["json_keys", "json_extract", "json_length", "json_remove"] {
        agreement_rows(
            name,
            &[json.clone(), string.clone()],
            &[
                vec![document.clone(), text("$.a")],
                vec![document.clone(), text("$.missing")],
                vec![Datum::Null, text("$.a")],
            ],
        );
    }
    for name in [
        "json_set",
        "json_insert",
        "json_replace",
        "json_array_append",
    ] {
        agreement_rows(
            name,
            &[
                json.clone(),
                string.clone(),
                json.clone(),
                string.clone(),
                json.clone(),
            ],
            &[
                vec![
                    document.clone(),
                    text("$.a"),
                    child.clone(),
                    text("$.x"),
                    child.clone(),
                ],
                vec![
                    Datum::Null,
                    text("$.a"),
                    child.clone(),
                    text("$.x"),
                    Datum::Null,
                ],
            ],
        );
    }
    let array =
        Datum::Json(tidb_datatype::BinaryJSON::parse(r#"["a",{"b":[1,2]},[3,4]]"#).unwrap());
    agreement_rows(
        "json_array_insert",
        &[json.clone(), string.clone(), json.clone()],
        &[vec![array, text("$[1]"), child.clone()]],
    );
    agreement_rows(
        "json_contains",
        &[json.clone(), json.clone(), string.clone()],
        &[vec![document.clone(), child.clone(), text("$.a")]],
    );
    agreement_rows(
        "json_contains_path",
        &[json.clone(), string.clone(), string.clone(), string.clone()],
        &[
            vec![document.clone(), text("one"), text("$.a"), text("$.x")],
            vec![document.clone(), text("all"), text("$.a"), text("$.x")],
        ],
    );
    agreement_rows(
        "json_object",
        &[string.clone(), json.clone(), string.clone(), json.clone()],
        &[vec![text("a"), child.clone(), text("b"), Datum::Null]],
    );
    let search = Datum::Json(
        tidb_datatype::BinaryJSON::parse(r#"["abc",[{"k":"10"},"def"],{"x":"abc"}]"#).unwrap(),
    );
    agreement_rows(
        "json_search",
        &[json, string.clone(), string],
        &[
            vec![search.clone(), text("one"), text("abc")],
            vec![search, text("all"), text("%a%")],
        ],
    );
}

/// Go `TestInDecimal`: equal decimals with different stored fraction widths.
#[test]
fn test_in_decimal() {
    let decimal = FieldType::new(FieldTypeCode::NewDecimal);
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        "in",
        FieldType::new(FieldTypeCode::LongLong),
        vec![column(0, &decimal), column(1, &decimal)],
    )
    .unwrap();
    let mut input = Chunk::new_chunk_with_capacity(&[decimal.clone(), decimal], 1024);
    for i in 0..1024 {
        let text = format!("{}.{}", i % 1000, i * 1973 + 1);
        input.append_datum(0, &Datum::Decimal(Decimal::parse_mysql(&text).0));
        input.append_datum(
            1,
            &Datum::Decimal(Decimal::parse_mysql(&format!("{text}00")).0),
        );
    }
    let mut result = Column::default();
    eval_into(&expression, EvalType::Int, &NoColumns, &input, &mut result).unwrap();
    for i in 0..1024 {
        assert!(!result.is_null(i));
        assert_eq!(result.get_int64(i), 1);
    }
}

/// Go `TestGetParamVec`: typed indices, string conversion and out-of-range error.
#[test]
fn test_get_param_vec() {
    struct Params;
    impl Columns for Params {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn get_param_value(&self, index: usize) -> Result<Datum, EvalError> {
            [Datum::Int(123), Datum::new_string_datum("abc")]
                .get(index)
                .cloned()
                .ok_or(EvalError::ParamIndexExceedParamCounts)
        }
    }
    let int = FieldType::new(FieldTypeCode::LongLong);
    let expression = crate::new_function::new_function_base(
        &Params,
        "getparam",
        int.clone(),
        vec![column(0, &int)],
    )
    .unwrap();
    let mut input = Chunk::new_chunk_with_capacity(&[int], 3);
    input.append_datum(0, &Datum::Int(0));
    input.append_datum(0, &Datum::Int(1));
    let mut result = Column::default();
    eval_into(&expression, EvalType::String, &Params, &input, &mut result).unwrap();
    assert_eq!(result.get_bytes(0).as_ref(), b"123");
    assert_eq!(result.get_bytes(1).as_ref(), b"abc");
    input.append_datum(0, &Datum::Int(2));
    assert!(eval_into(&expression, EvalType::String, &Params, &input, &mut result).is_err());
}

/// Go `TestVectorizedBuiltinStringFunc2`: multibyte/binary signatures,
/// FORMAT, INSERT, EXPORT_SET, MAKE_SET, and FIELD's three domains.
#[test]
fn test_vectorized_builtin_string_func2() {
    let text = |s: &str| Datum::new_string_datum(s);
    let int = FieldType::new(FieldTypeCode::LongLong);
    let string = types().remove(3);
    let mut binary = string.clone();
    binary.set_charset_name("binary");
    binary.set_collation_name("binary");
    binary.add_flags(FieldTypeFlags::BINARY);
    for field in [string.clone(), binary] {
        for name in ["upper", "lower", "reverse", "char_length"] {
            agreement_rows(
                name,
                &[field.clone()],
                &[vec![text(" Ab中ß ")], vec![Datum::Null]],
            );
        }
        for name in ["left", "right", "substring"] {
            agreement_rows(
                name,
                &[field.clone(), int.clone()],
                &[
                    vec![text("a中😀z"), Datum::Int(2)],
                    vec![text("abc"), Datum::Int(-1)],
                    vec![text("abc"), Datum::Null],
                ],
            );
        }
        agreement_rows(
            "substring",
            &[field.clone(), int.clone(), int.clone()],
            &[
                vec![text("a中😀z"), Datum::Int(2), Datum::Int(2)],
                vec![text("abc"), Datum::Int(2), Datum::Int(i64::MAX)],
                vec![text("abc"), Datum::Null, Datum::Int(2)],
                vec![text("abc"), Datum::Int(2), Datum::Null],
            ],
        );
        // Go `ast.InsertFunc`.
        agreement_rows(
            "insert_func",
            &[field.clone(), int.clone(), int.clone(), field.clone()],
            &[vec![
                text("a中😀z"),
                Datum::Int(2),
                Datum::Int(2),
                text("new"),
            ]],
        );
        for name in ["lpad", "rpad"] {
            agreement_rows(
                name,
                &[field.clone(), int.clone(), field.clone()],
                &[vec![text("中"), Datum::Int(5), text("😀!")]],
            );
        }
        for name in ["instr", "locate"] {
            agreement_rows(
                name,
                &[field.clone(), field.clone()],
                &[vec![text("中"), text("a中😀z")]],
            );
        }
    }
    for count in 3..=5 {
        let fields = [
            int.clone(),
            string.clone(),
            string.clone(),
            string.clone(),
            int.clone(),
        ];
        let row = [
            Datum::Int(10),
            text("Y"),
            text("N"),
            text(","),
            Datum::Int(8),
        ];
        agreement_rows("export_set", &fields[..count], &[row[..count].to_vec()]);
    }
    agreement_rows(
        "make_set",
        &[int.clone(), string.clone(), string.clone()],
        &[
            vec![Datum::Int(3), text("one"), text("two")],
            vec![Datum::Int(3), Datum::Null, text("two")],
        ],
    );
    agreement_rows(
        "format",
        &[string.clone(), int.clone()],
        &[vec![text("12345.6789"), Datum::Int(2)]],
    );
    agreement_rows(
        "format",
        &[string.clone(), int.clone(), string.clone()],
        &[vec![text("12345.6789"), Datum::Int(2), text("en_US")]],
    );
    agreement_rows(
        "trim",
        &[string.clone(), string.clone()],
        &[vec![text("xyzhelloxyz"), text("xyz")]],
    );
    for field in [int, FieldType::new(FieldTypeCode::Double), string] {
        agreement("field", &vec![field; 4]);
    }
}

#[test]
fn substring_index_large_counts_find_the_requested_delimiter() {
    use crate::string_fn::substring_index_byte_slice;

    let dense = b"x|".repeat(1024);
    for count in [511, 512, 700, 1024] {
        let bytes = count as usize * 2;
        assert_eq!(substring_index_byte_slice(&dense, b"|", count, false), &dense[..bytes - 1]);
        assert_eq!(substring_index_byte_slice(&dense, b"|", -count, false), &dense[dense.len() - bytes + 2..]);
    }

    let sparse = b"abcdefg|".repeat(64);
    assert_eq!(substring_index_byte_slice(&sparse, b"|", 200, false), sparse);
    assert_eq!(substring_index_byte_slice(&sparse, b"|", -200, false), sparse);
}

/// Go `vecBuiltinTimeCases` and generated add/sub/time-diff cases: typed
/// temporal combinations, unit/format columns and nullable argument rows.
#[test]
fn test_vectorized_builtin_time_func_formats_and_arithmetic() {
    let date = types().remove(4);
    let duration = types().remove(5);
    let string = types().remove(3);
    let int = FieldType::new(FieldTypeCode::LongLong);
    let text = |s: &str| Datum::new_string_datum(s);
    let datetime = sample(&date, 1);
    let time = sample(&duration, 1);
    for name in ["period_add", "period_diff"] {
        agreement_rows(
            name,
            &[int.clone(), int.clone()],
            &[
                vec![Datum::Int(200801), Datum::Int(2)],
                vec![Datum::Null, Datum::Int(0)],
            ],
        );
    }
    agreement_rows(
        "date_format",
        &[date.clone(), string.clone()],
        &[
            vec![datetime.clone(), text("%Y-%m-%d %H:%i:%s")],
            vec![Datum::Null, text("%Y")],
        ],
    );
    agreement_rows(
        "time_format",
        &[duration.clone(), string.clone()],
        &[vec![time.clone(), text("%H:%i:%s")]],
    );
    agreement_rows(
        "get_format",
        &[string.clone(), string.clone()],
        &[
            vec![text("DATE"), text("USA")],
            vec![text("DATETIME"), text("ISO")],
        ],
    );
    for (field, datum) in [
        (date.clone(), datetime.clone()),
        (duration.clone(), time.clone()),
    ] {
        agreement_rows(
            "extract",
            &[string.clone(), field],
            &[
                vec![text("HOUR"), datum.clone()],
                vec![text("SECOND"), datum],
            ],
        );
    }
    agreement_rows(
        "timestampdiff",
        &[string.clone(), date.clone(), date.clone()],
        &[vec![text("DAY"), datetime.clone(), sample(&date, 2)]],
    );
    agreement_rows(
        "timestampadd",
        &[string.clone(), int.clone(), date.clone()],
        &[vec![text("DAY"), Datum::Int(3), datetime.clone()]],
    );
    agreement_rows(
        "convert_tz",
        &[date.clone(), string.clone(), string.clone()],
        &[vec![datetime.clone(), text("+00:00"), text("+08:00")]],
    );
    for (first_field, first) in [
        (date.clone(), datetime.clone()),
        (duration.clone(), time.clone()),
        (string.clone(), text("2020-01-02 01:02:03")),
    ] {
        for (second_field, second) in [
            (duration.clone(), time.clone()),
            (string.clone(), text("01:02:03")),
        ] {
            for name in ["addtime", "subtime", "timediff"] {
                agreement_rows(
                    name,
                    &[first_field.clone(), second_field.clone()],
                    &[
                        vec![first.clone(), second.clone()],
                        vec![Datum::Null, second.clone()],
                        vec![first.clone(), Datum::Null],
                    ],
                );
            }
        }
    }
    for formats in [
        vec![text("2020-01-02"), text("%Y-%m-%d")],
        vec![text("2020-01-02 01:02:03"), text("%Y-%m-%d %H:%i:%s")],
        vec![text("01:02:03"), text("%H:%i:%s")],
    ] {
        agreement_rows("str_to_date", &[string.clone(), string.clone()], &[formats]);
    }
    agreement_rows(
        "timestamp",
        &[string.clone()],
        &[vec![text("2020-01-02 01:02:03")]],
    );
    agreement_rows(
        "timestamp",
        &[string.clone(), string.clone()],
        &[vec![text("2020-01-02"), text("01:02:03")]],
    );
    agreement_rows("time", &[string.clone()], &[vec![text("01:02:03.123456")]]);
    agreement_rows(
        "from_unixtime",
        &[int.clone()],
        &[vec![Datum::Int(1_700_000_000)]],
    );
    agreement_rows(
        "from_unixtime",
        &[int, string],
        &[vec![Datum::Int(1_700_000_000), text("%Y-%m-%d")]],
    );
}

/// Go `builtinExtractDatetimeFromStringSig.vectorized` is disabled explicitly.
#[test]
fn extract_from_string_keeps_go_non_vectorized_signature() {
    let string = types().remove(3);
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        "extract",
        FieldType::new(FieldTypeCode::LongLong),
        vec![
            Expression::Constant(crate::constant::Constant::new(
                Datum::new_string_datum("DAY_SECOND"),
                string.clone(),
            )),
            column(1, &string),
        ],
    )
    .unwrap();
    assert!(!crate::chunk_executor::vectorized(&expression));
}

/// Runs `name` over `rows` (one column per field) through its Go vector
/// kernel, answering the result column's cells.
fn vec_cells(name: &str, fields: &[FieldType], rows: &[Vec<Datum>]) -> (Expression, Vec<Datum>) {
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        name,
        FieldType::new(FieldTypeCode::LongLong),
        fields
            .iter()
            .enumerate()
            .map(|(i, ft)| column(i as i64, ft))
            .collect(),
    )
    .unwrap();
    let Expression::ScalarFunction(function) = &expression else {
        panic!("unfolded signature");
    };
    assert!(has_kernel(function), "{name}: missing Go vector kernel");
    let field = expression.static_type().unwrap().clone();
    let mut input = Chunk::new_chunk_with_capacity(fields, rows.len());
    for row in rows {
        for (j, value) in row.iter().enumerate() {
            input.append_datum(j, value);
        }
    }
    let mut result = Column::default();
    eval_into(&expression, field.eval_type(), &NoColumns, &input, &mut result).unwrap();
    let cells = (0..rows.len())
        .map(|i| go_vec(value(&result, i, field.eval_type(), Some(&field))))
        .collect();
    (expression, cells)
}

fn text_field() -> FieldType {
    let mut field = FieldType::new(FieldTypeCode::VarString);
    field.set_charset_name("utf8mb4");
    field.set_collation_name("utf8mb4_bin");
    field
}

fn row_of(values: &[Datum]) -> Chunk {
    let fields: Vec<_> = values
        .iter()
        .map(|value| match value {
            Datum::Int(_) => FieldType::new(FieldTypeCode::LongLong),
            _ => text_field(),
        })
        .collect();
    let mut chunk = Chunk::new_chunk_with_capacity(&fields, 1);
    for (j, value) in values.iter().enumerate() {
        chunk.append_datum(j, value);
    }
    chunk
}

/// Go `builtinInetAtonSig.vecEvalInt` answers NULL for a malformed address
/// where `evalInt` answers `errWrongValueForType` (1411).
#[test]
fn inet_aton_vector_kernel_nulls_what_the_row_form_refuses() {
    let text = text_field();
    let rows: Vec<Vec<Datum>> = ["", "0.0.0.256", "127.0.0.1", "123.2.1."]
        .iter()
        .map(|s| vec![Datum::new_string_datum(*s)])
        .collect();
    let (expression, cells) = vec_cells("inet_aton", &[text], &rows);
    assert_eq!(cells[0], Datum::Null);
    assert_eq!(cells[1], Datum::Null);
    assert_eq!(cells[3], Datum::Null);
    assert!(matches!(cells[2], Datum::UInt(2130706433) | Datum::Int(2130706433)), "{:?}", cells[2]);
    let row = row_of(&[Datum::new_string_datum("0.0.0.256")]);
    assert!(matches!(
        expression.eval(&NoColumns, row.get_row(0)),
        Err(EvalError::WrongValueForType { .. })
    ));
}

/// Go `builtinInet6AtonSig.vecEvalString`: an empty or unparseable address is
/// NULL; `evalString` answers `errWrongValueForType`.
#[test]
fn inet6_aton_vector_kernel_nulls_what_the_row_form_refuses() {
    let rows: Vec<Vec<Datum>> = ["", "zzz", "::1"]
        .iter()
        .map(|s| vec![Datum::new_string_datum(*s)])
        .collect();
    let (expression, cells) = vec_cells("inet6_aton", &[text_field()], &rows);
    assert_eq!(cells[0], Datum::Null);
    assert_eq!(cells[1], Datum::Null);
    let mut loopback = [0u8; 16];
    loopback[15] = 1;
    assert_eq!(cells[2], Datum::Bytes(loopback.to_vec()));
    let row = row_of(&[Datum::new_string_datum("zzz")]);
    assert!(matches!(
        expression.eval(&NoColumns, row.get_row(0)),
        Err(EvalError::WrongValueForType { .. })
    ));
}

/// Go `builtinPasswordSig.vecEvalString` hashes a NULL to "" where
/// `evalString` answers NULL.
#[test]
fn password_vector_kernel_hashes_null_to_the_empty_string() {
    let (expression, cells) = vec_cells("password", &[text_field()], &[vec![Datum::Null]]);
    assert_eq!(cells[0], Datum::Bytes(Vec::new()));
    let row = row_of(&[Datum::Null]);
    assert_eq!(expression.eval(&NoColumns, row.get_row(0)).unwrap(), Datum::Null);
}

/// Go `builtinRepeatSig.vecEvalString`: past the packet check, a cell longer
/// than `flen / num` is NULL -- a check `evalString` does not have.
#[test]
fn repeat_vector_kernel_nulls_a_cell_past_flen_over_count() {
    let int = FieldType::new(FieldTypeCode::LongLong);
    let count = 20_000_000;
    let (expression, cells) = vec_cells(
        "repeat",
        &[text_field(), int],
        &[vec![Datum::new_string_datum("a"), Datum::Int(count)]],
    );
    let flen = expression.static_type().unwrap().flen();
    assert!(flen / count < 1, "the fixture must cross flen/num: flen {flen}");
    assert_eq!(cells[0], Datum::Null);
    let row = row_of(&[Datum::new_string_datum("a"), Datum::Int(count)]);
    let value = expression.eval(&NoColumns, row.get_row(0)).unwrap();
    assert_eq!(
        crate::arg_eval_type::eval_string(&value).unwrap().map(|bytes| bytes.len()),
        Some(count as usize)
    );
}

/// Go `builtinGreatestStringSig.vecEvalString` keeps the running value only
/// when it compares strictly ahead, so of two equal arguments it answers the
/// later; `evalString` keeps the earlier.
#[test]
fn greatest_string_vector_kernel_answers_the_later_of_equal_arguments() {
    let (expression, cells) = vec_cells(
        "greatest",
        &[text_field(), text_field()],
        &[vec![Datum::new_string_datum("a"), Datum::new_string_datum("a ")]],
    );
    assert_eq!(cells[0], Datum::Bytes(b"a ".to_vec()));
    let row = row_of(&[Datum::new_string_datum("a"), Datum::new_string_datum("a ")]);
    assert_eq!(
        expression.eval(&NoColumns, row.get_row(0)).unwrap(),
        Datum::new_string_datum("a")
    );
}

fn datetime_field() -> FieldType {
    let mut field = FieldType::new(FieldTypeCode::Datetime);
    field.set_decimal(0);
    field
}

/// A DATETIME with a zero month, which `ALLOW_INVALID_DATES`-free reads keep
/// (Go `IgnoreZeroInDate`).
fn zero_month_datetime() -> Datum {
    Datum::Time(
        tidb_datatype::Time::new(
            tidb_datatype::CoreTime::from_date(2020, 0, 10, 0, 0, 0, 0),
            tidb_datatype::TimeType::DateTime,
            0,
        )
        .unwrap(),
    )
}

/// Go `builtin{WeekDay,WeekWithMode,YearWeekWithMode,WeekOfYear}Sig.vecEvalInt`
/// reject only a zero date: a zero-month date computes on (`Weekday`
/// normalizing 2020-00-10 to 2019-12-10, a Tuesday; `Week` answering 0),
/// where the row forms warn and answer NULL.
#[test]
fn week_vector_kernels_compute_on_an_invalid_zero_date() {
    let int = FieldType::new(FieldTypeCode::LongLong);
    let date = zero_month_datetime();
    for (name, fields, row, vector) in [
        ("weekday", vec![datetime_field()], vec![date.clone()], Datum::Int(1)),
        ("weekofyear", vec![datetime_field()], vec![date.clone()], Datum::Int(0)),
        ("week", vec![datetime_field(), int.clone()], vec![date.clone(), Datum::Int(0)], Datum::Int(0)),
    ] {
        let (expression, cells) = vec_cells(name, &fields, &[row.clone()]);
        assert_eq!(cells[0], vector, "{name} vector");
        let mut chunk = Chunk::new_chunk_with_capacity(&fields, 1);
        for (j, value) in row.iter().enumerate() {
            chunk.append_datum(j, value);
        }
        assert_eq!(
            expression.eval(&NoColumns, chunk.get_row(0)).unwrap(),
            Datum::Null,
            "{name} row"
        );
    }
}

/// Go `builtinStrToDateDateSig`: under `NO_ZERO_DATE` a zero component is
/// rejected naming the INPUT (1411, `ErrWrongValueForType`) in `evalTime` but
/// naming the parsed TIME (1292, `ErrWrongValue`) in `vecEvalTime`.
#[test]
fn str_to_date_vector_kernel_names_the_parsed_time() {
    struct Warnings(std::cell::RefCell<Vec<(u16, String)>>);
    impl crate::Columns for Warnings {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.0.borrow_mut().push((code, message.to_owned()));
        }
    }
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        "str_to_date",
        FieldType::new(FieldTypeCode::LongLong),
        vec![
            column(0, &text_field()),
            Expression::Constant(crate::constant::Constant::new(Datum::new_string_datum("%Y"), text_field())),
        ],
    )
    .unwrap();
    let Expression::ScalarFunction(function) = &expression else {
        panic!("unfolded signature");
    };
    assert_eq!(function.pb_code(), tidb_proto::tipb::ScalarFuncSig::StrToDateDate);
    let field = expression.static_type().unwrap().clone();
    let mut input = Chunk::new_chunk_with_capacity(&[text_field()], 1);
    input.append_datum(0, &Datum::new_string_datum("2020"));

    let vector = Warnings(Default::default());
    let mut result = Column::default();
    eval_into(&expression, field.eval_type(), &vector, &input, &mut result).unwrap();
    assert!(result.is_null(0));
    assert_eq!(
        *vector.0.borrow(),
        [(1292, "Incorrect datetime value: '2020-00-00 00:00:00'".to_owned())]
    );

    let row = Warnings(Default::default());
    assert_eq!(expression.eval(&row, input.get_row(0)).unwrap(), Datum::Null);
    assert_eq!(
        *row.0.borrow(),
        [(1411, "Incorrect datetime value: '2020' for function str_to_date".to_owned())]
    );
}

/// Go `builtinCastStringAsTimeSig`: a `strconv` failure inside `ParseTime`
/// ('2020-01-0a') is `ErrIncorrectDatetimeValue` (8034), a warning, in
/// `vecEvalTime` but the raw strconv error (1105) in `evalTime` (testkit
/// probe with `tidb_enable_vectorized_expression` on and off).
#[test]
fn cast_string_as_time_vector_kernel_warns_where_the_row_form_fails() {
    struct Warnings(std::cell::RefCell<Vec<(u16, String)>>);
    impl crate::Columns for Warnings {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.0.borrow_mut().push((code, message.to_owned()));
        }
    }
    let target = datetime_field();
    let expression = crate::builtin_cast::build_cast_function_with_check(
        &crate::builtin::ColumnsBuildContext(&NoColumns),
        column(0, &text_field()),
        target.clone(),
        false,
        false,
    )
    .unwrap();
    let mut input = Chunk::new_chunk_with_capacity(&[text_field()], 1);
    input.append_datum(0, &Datum::new_string_datum("2020-01-0a"));

    let vector = Warnings(Default::default());
    let mut result = Column::default();
    eval_into(&expression, EvalType::Datetime, &vector, &input, &mut result).unwrap();
    assert!(result.is_null(0));
    assert_eq!(
        *vector.0.borrow(),
        [(8034, "Incorrect datetime value: '2020-01-0a'".to_owned())]
    );

    let row = Warnings(Default::default());
    assert_eq!(
        expression.eval(&row, input.get_row(0)),
        Err(EvalError::Raw("strconv.Atoi: parsing \"0a\": invalid syntax".to_owned()))
    );
    assert!(row.0.borrow().is_empty());
}

/// Go `builtinCastJSONAsDurationSig`: a JSON value of no temporal or string
/// kind warns `Truncated incorrect time value` (`TypeStr(TypeDuration)`) in
/// `vecEvalDuration` and `Truncated incorrect TIME value` in `evalDuration`.
#[test]
fn cast_json_as_duration_vector_kernel_names_the_type_in_lowercase() {
    struct Warnings(std::cell::RefCell<Vec<(u16, String)>>);
    impl crate::Columns for Warnings {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.0.borrow_mut().push((code, message.to_owned()));
        }
    }
    let json = FieldType::new(FieldTypeCode::Json);
    let mut target = FieldType::new(FieldTypeCode::Duration);
    target.set_decimal(0);
    let expression = crate::builtin_cast::build_cast_function_with_check(
        &crate::builtin::ColumnsBuildContext(&NoColumns),
        column(0, &json),
        target,
        false,
        false,
    )
    .unwrap();
    let mut input = Chunk::new_chunk_with_capacity(&[json], 1);
    input.append_datum(0, &Datum::Json(tidb_datatype::BinaryJSON::parse("[1]").unwrap()));

    let vector = Warnings(Default::default());
    let mut result = Column::default();
    eval_into(&expression, EvalType::Duration, &vector, &input, &mut result).unwrap();
    assert!(result.is_null(0));
    let messages = |w: &Warnings| w.0.borrow().iter().map(|(_, m)| m.clone()).collect::<Vec<_>>();
    assert_eq!(messages(&vector), ["Truncated incorrect time value: '[1]'"]);

    let row = Warnings(Default::default());
    assert_eq!(expression.eval(&row, input.get_row(0)).unwrap(), Datum::Null);
    assert_eq!(messages(&row), ["Truncated incorrect TIME value: '[1]'"]);
}
