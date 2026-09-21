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

//! Value-level ports of `pkg/expression/builtin_arithmetic_test.go`.
//!
//! Each case reaches the builtin through the SAME door Go's tests use:
//! `funcs[name].getFunction(ctx, datumsToConstants(...))` followed by one
//! evaluation over an empty row. `datums_to_constants` mirrors Go's
//! `evaluator_test.go:33` `kindToFieldType` exactly, because the SIGNATURE
//! selection (signed int / unsigned int / real / decimal) reads those field
//! types, not the datum kinds alone.

use super::*;
use tidb_datatype::{FieldType, FieldTypeCode as C, FieldTypeFlags};

use crate::expression::Expression;

/// Go `datumsToConstants` (`evaluator_test.go:79`) over `kindToFieldType`
/// (`:33`): one constant per datum, typed by its kind.
fn datums_to_constants(datums: &[Datum]) -> Vec<Expression> {
    datums
        .iter()
        .map(|d| {
            let ft = match d {
                Datum::Null => FieldType::new(C::Null),
                Datum::Int(_) => FieldType::new(C::LongLong),
                Datum::UInt(_) => {
                    FieldType::new(C::LongLong).with_added_flags(FieldTypeFlags::UNSIGNED)
                }
                Datum::Float32(_) | Datum::Real(_) => FieldType::new(C::Double),
                Datum::String(_) | Datum::Bytes(_) => FieldType::new(C::VarString),
                // Go's table continues past what the signed-numeric tables
                // needed: DECIMAL, DURATION and SET map their own types.
                Datum::Decimal(_) => FieldType::new(C::NewDecimal),
                Datum::Duration(_) => FieldType::new(C::Duration),
                Datum::Set(_, _) => FieldType::new(C::Set),
                other => panic!("kindToFieldType has no test mapping for {other:?}"),
            };
            Expression::Constant(crate::constant::Constant::new(d.clone(), ft))
        })
        .collect()
}

/// One evaluation of `name(args)` over an empty row, Go's
/// `getFunction` + `evalBuiltinFunc(sig, ctx, chunk.Row{})`. The return type
/// comes from the crate's own arithmetic inference -- Go's `getFunction`
/// computes exactly that type and the result is coerced into it.
fn go_eval(name: &str, args: &[Datum]) -> Result<Datum, crate::EvalError> {
    let exprs = datums_to_constants(args);
    assert_eq!(exprs.len(), 2, "binary arithmetic only");
    let ret_type = crate::builtin_arithmetic::infer_arithmetic_type(name, &exprs[0], &exprs[1])
        .unwrap_or_else(|| panic!("{name}: no inferred type"));
    let function =
        crate::scalar_function::ScalarFunction::new(tidb_ast::CiString::new(name), ret_type, exprs);
    let cols = crate::context::ZonedNoColumns(tidb_datatype::SessionTimeZone::utc());
    let empty = tidb_chunk::chunk::Chunk::new_with_capacity(&[], 1);
    function.eval(&cols, empty.get_row(0))
}

/// Go `testutil.DatumEqual` (`pkg/testkit/testutil/require.go:30`): the two
/// datums COMPARE equal under the binary collation -- kind-agnostic for
/// numbers, which is why Go's tables may write `float64(1)` where the
/// signature produced a DECIMAL.
fn datum_equal(actual: &Datum, expected: &Datum) -> bool {
    if actual.is_null() || expected.is_null() {
        return actual.is_null() && expected.is_null();
    }
    actual
        .compare(expected, tidb_datatype::Collation::Binary)
        .map(|ordering| ordering == std::cmp::Ordering::Equal)
        .unwrap_or(false)
}

fn i(v: i64) -> Datum {
    Datum::Int(v)
}

fn u(v: u64) -> Datum {
    Datum::UInt(v)
}

fn r(v: f64) -> Datum {
    Datum::Real(v)
}

/// Go `TestArithmeticMinus` (`builtin_arithmetic_test.go:192`): signed ints
/// subtract in INT, floats subtract in REAL, any NULL makes NULL.
#[test]
fn go_test_arithmetic_minus() {
    assert_eq!(go_eval("minus", &[i(12), i(1)]).unwrap(), i(11));
    assert_eq!(
        go_eval("minus", &[r(1.01001), r(-0.01)]).unwrap(),
        r(1.02001)
    );
    for args in [
        vec![Datum::Null, r(-0.11101)],
        vec![r(1.01), Datum::Null],
        vec![Datum::Null, Datum::Null],
    ] {
        assert_eq!(go_eval("minus", &args).unwrap(), Datum::Null);
    }
}

/// Go `TestArithmeticMultiply` (`builtin_arithmetic_test.go:277`): INT*INT
/// overflow reports 1690 with Go's message shape; NULL propagates.
#[test]
fn go_test_arithmetic_multiply() {
    let cases: &[(Vec<Datum>, Datum, Option<&str>)] = &[
        (vec![i(11), i(11)], i(121), None),
        (
            vec![i(-1), i(i64::MIN)],
            Datum::Null,
            Some("BIGINT value is out of range in '(-1 * -9223372036854775808)'"),
        ),
        (
            vec![i(i64::MIN), i(-1)],
            Datum::Null,
            Some("BIGINT value is out of range in '(-9223372036854775808 * -1)'"),
        ),
        (vec![u(11), u(11)], u(121), None),
        (vec![r(11.0), r(11.0)], r(121.0), None),
        (vec![Datum::Null, r(-0.11101)], Datum::Null, None),
        (vec![r(1.01), Datum::Null], Datum::Null, None),
        (vec![Datum::Null, Datum::Null], Datum::Null, None),
    ];
    for (args, expected, error_part) in cases {
        match go_eval("mul", args) {
            Ok(value) => {
                assert!(error_part.is_none(), "{args:?}: unexpected success");
                assert!(datum_equal(&value, expected), "{args:?}: {value:?}");
            }
            Err(error) => {
                // Go's 1690 message: "BIGINT value is out of range in '(expr)'".
                // This tier carries it as DataOutOfRange { value, expression }.
                let text = match &error {
                    crate::EvalError::DataOutOfRange { value, expression } => {
                        format!("{value} value is out of range in '{expression}'")
                    }
                    other => format!("{other:?}"),
                };
                assert!(
                    text.contains(error_part.unwrap()),
                    "{args:?}: {text} does not contain {error_part:?}"
                );
            }
        }
    }
}

/// Go `TestArithmeticDivide` (`builtin_arithmetic_test.go:325`): division is
/// always REAL/DECIMAL-valued; zero divisors answer NULL.
#[test]
fn go_test_arithmetic_divide() {
    // (args, expected-label) -- labels sidestep Real formatting noise while
    // keeping the value comparison exact where Go compares exact literals.
    let cases: &[(Vec<Datum>, Datum)] = &[
        (vec![r(11.111_111_1), r(11.1)], r(1.001_001)),
        (vec![r(11.111_111_1), r(0.0)], Datum::Null),
        (vec![i(11), i(11)], r(1.0)),
        (vec![i(11), i(2)], r(5.5)),
        (vec![i(11), i(0)], Datum::Null),
        (vec![u(11), u(11)], r(1.0)),
        (vec![u(11), u(2)], r(5.5)),
        (vec![u(11), u(0)], Datum::Null),
        (vec![Datum::Null, r(-0.11101)], Datum::Null),
        (vec![r(1.01), Datum::Null], Datum::Null),
        (vec![Datum::Null, Datum::Null], Datum::Null),
    ];
    for (args, expected) in cases {
        let value = go_eval("div", args).unwrap_or_else(|error| panic!("{args:?}: {error:?}"));
        assert!(
            datum_equal(&value, expected),
            "{args:?}: {value:?} != {expected:?}"
        );
    }
}

/// Go `TestArithmeticIntDivide` (`builtin_arithmetic_test.go:409`): truncating
/// division over ints and decimals, NULL on zero divisor, and Go's 1690
/// overflow messages for extreme operands.
#[test]
fn go_test_arithmetic_int_divide() {
    let cases: &[(Vec<Datum>, Datum, Option<&str>)] = &[
        (vec![i(13), i(11)], i(1), None),
        (vec![i(-13), i(11)], i(-1), None),
        (vec![i(13), i(-11)], i(-1), None),
        (vec![i(-13), i(-11)], i(1), None),
        (vec![i(33), i(11)], i(3), None),
        (vec![i(-33), i(11)], i(-3), None),
        (vec![i(33), i(-11)], i(-3), None),
        (vec![i(-33), i(-11)], i(3), None),
        (vec![i(11), i(0)], Datum::Null, None),
        (vec![i(-11), i(0)], Datum::Null, None),
        (vec![r(11.01), r(1.1)], i(10), None),
        (vec![r(-11.01), r(1.1)], i(-10), None),
        (vec![r(11.01), r(-1.1)], i(-10), None),
        (vec![r(-11.01), r(-1.1)], i(10), None),
        (vec![Datum::Null, r(-0.11101)], Datum::Null, None),
        (vec![r(1.01), Datum::Null], Datum::Null, None),
        (vec![Datum::Null, i(-1001)], Datum::Null, None),
        (vec![i(101), Datum::Null], Datum::Null, None),
        (vec![Datum::Null, Datum::Null], Datum::Null, None),
        (
            vec![r(123_456_789_100_000.0), r(-0.00001)],
            Datum::Null,
            Some("BIGINT value is out of range in '(123456789100000 DIV -0.00001)'"),
        ),
        (
            vec![i(i64::MIN), r(-1.0)],
            Datum::Null,
            Some("BIGINT value is out of range in '(-9223372036854775808 DIV -1)'"),
        ),
        (vec![u(1), r(-2.0)], i(0), None),
        (
            vec![u(1), r(-1.0)],
            Datum::Null,
            Some("BIGINT UNSIGNED value is out of range in '(1 DIV -1)'"),
        ),
        // Go stamps the decimal DIV result unsigned when either operand is
        // unsigned, then reads the exact quotient through ToUint. This value
        // is above i64::MAX but still valid in BIGINT UNSIGNED.
        (
            vec![u(u64::MAX), Datum::Decimal(Decimal::from_literal("1.5"))],
            u(12_297_829_382_473_034_410),
            None,
        ),
        // A negative quotient remains an unsigned overflow, while a quotient
        // truncated from (-1, 0] is the source's special zero result.
        (
            vec![u(1), Datum::Decimal(Decimal::from_literal("-0.5"))],
            Datum::Null,
            Some("BIGINT UNSIGNED value is out of range in '(1 DIV -0.5)'"),
        ),
        (
            vec![u(1), Datum::Decimal(Decimal::from_literal("-2.0"))],
            u(0),
            None,
        ),
        (
            vec![u(u64::MAX), Datum::Decimal(Decimal::from_literal("0.5"))],
            Datum::Null,
            Some("BIGINT UNSIGNED value is out of range in '(18446744073709551615 DIV 0.5)'"),
        ),
    ];
    for (args, expected, error_part) in cases {
        match go_eval("intdiv", args) {
            Ok(value) => {
                assert!(error_part.is_none(), "{args:?}: unexpected success");
                assert!(datum_equal(&value, expected), "{args:?}: {value:?}");
            }
            Err(error) => {
                // Go's 1690 message: "BIGINT value is out of range in '(expr)'".
                // This tier carries it as DataOutOfRange { value, expression }.
                let text = match &error {
                    crate::EvalError::DataOutOfRange { value, expression } => {
                        format!("{value} value is out of range in '{expression}'")
                    }
                    other => format!("{other:?}"),
                };
                assert!(
                    text.contains(error_part.unwrap()),
                    "{args:?}: {text} does not contain {error_part:?}"
                );
            }
        }
    }
}

/// Go `TestArithmeticMod` (`builtin_arithmetic_test.go:510`): the sign follows
/// the DIVIDEND, zero divisors answer NULL, and mixed-sign combinations route
/// through the unsigned-aware signatures.
#[test]
fn go_test_arithmetic_mod() {
    let cases: &[(Vec<Datum>, Datum)] = &[
        (vec![i(13), i(11)], i(2)),
        (vec![i(13), i(0)], Datum::Null),
        (vec![u(13), i(0)], Datum::Null),
        (vec![i(13), u(0)], Datum::Null),
        (vec![u((i64::MAX as u64) + 1), i(i64::MIN)], i(0)),
        (vec![i(-22), u(10)], i(-2)),
        (vec![i(i64::MIN), u(3)], i(-2)),
        (vec![i(-13), i(11)], i(-2)),
        (vec![i(13), i(-11)], i(2)),
        (vec![i(-13), i(-11)], i(-2)),
    ];
    for (args, expected) in cases {
        let value = go_eval("mod", args).unwrap();
        assert!(
            datum_equal(&value, expected),
            "{args:?}: {value:?} != {expected:?}"
        );
    }
}

/// Go `TestArithmeticMod`'s four HYBRID-type rows (`builtin_arithmetic_test.go`):
/// a string rides StrToInt to its integer prefix on one side, stays REAL when
/// BOTH sides are strings (MySQL's rule), and Enum/Set/Duration operands
/// evaluate through their numeric values.
#[test]
fn go_test_arithmetic_mod_hybrid_operand_kinds() {
    let cases: &[(Vec<Datum>, Datum)] = &[
        (
            vec![Datum::new_string("1231".to_string()), i(12)],
            i(7), // StrToInt("1231") % 12
        ),
        (
            vec![
                Datum::new_string("1231".to_string()),
                Datum::new_string("12".to_string()),
            ],
            r(7.0), // both strings force the REAL signature
        ),
        (
            vec![
                Datum::Duration(tidb_datatype::MySqlDuration::new(12, 34, 56, 0, 0).unwrap()),
                i(122),
            ],
            i(114), // duration reads as its packed clock 123456
        ),
        (
            vec![
                Datum::Set(
                    tidb_datatype::MysqlSet::new("abc", 7),
                    tidb_datatype::Collation::Utf8Mb4Bin,
                ),
                Datum::new_string("12".to_string()),
            ],
            r(7.0), // Set numeric value rides the REAL signature beside a string
        ),
    ];
    for (args, expected) in cases {
        let value = go_eval("mod", args).unwrap();
        assert!(
            datum_equal(&value, expected),
            "{args:?}: {value:?} != {expected:?}"
        );
    }
}

/// Captured from the pinned Go arithmetic builder and both typed evaluators.
#[test]
fn hybrid_arithmetic_uses_go_typed_numeric_arguments() {
    use crate::scalar_function::{try_eval_numeric_batch, ScalarFunction};
    use tidb_datatype::{BinaryLiteral, Collation, MysqlEnum, MysqlSet};
    struct Strict;
    impl crate::Columns for Strict {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn truncate_level(&self) -> crate::ErrorLevel {
            crate::ErrorLevel::Error
        }
    }
    let cases = [
        ("literal_unsigned", "max", ["error"; 6]),
        (
            "literal_signed",
            "min",
            [
                "error",
                "9223372036854775807",
                "error",
                "-2.0000",
                "0",
                "-1",
            ],
        ),
        ("enum", "int", ["3", "1", "2", "2", "2", "0"]),
        ("set", "int", ["4", "2", "3", "3", "3", "0"]),
        (
            "set_precision",
            "int",
            [
                "9007199254740992",
                "9007199254740991",
                "9007199254740992",
                "9007199254740992",
                "9007199254740992",
                "0",
            ],
        ),
        ("set_overflow", "int", ["error"; 6]),
        ("bit_signed", "int", ["0", "-2", "-1", "-1.0000", "-1", "0"]),
        (
            "bit_unsigned",
            "int",
            [
                "error",
                "18446744073709551614",
                "18446744073709551615",
                "18446744073709551615.0000",
                "18446744073709551615",
                "0",
            ],
        ),
        (
            "literal_signed",
            "int",
            ["0", "-2", "-1", "18446744073709551615.0000", "-1", "0"],
        ),
        (
            "literal_unsigned",
            "int",
            [
                "error",
                "18446744073709551614",
                "18446744073709551615",
                "18446744073709551615.0000",
                "18446744073709551615",
                "0",
            ],
        ),
        ("literal_wide", "int", ["error"; 6]),
        (
            "set_precision",
            "real",
            [
                "9007199254740994",
                "9007199254740990",
                "13510798882111488",
                "6004799503160661",
                "6004799503160661",
                "0.5",
            ],
        ),
        (
            "set_precision",
            "decimal",
            [
                "9007199254740994",
                "9007199254740990",
                "13510798882111488",
                "6004799503160661",
                "6004799503160661",
                "0.5",
            ],
        ),
        (
            "bit_signed",
            "real",
            ["0.5", "-2.5", "-1.5", "-0.6666666666666666", "0", "-1"],
        ),
        (
            "bit_signed",
            "decimal",
            ["0.5", "-2.5", "-1.5", "-0.6667", "0", "-1.0"],
        ),
        (
            "literal_signed",
            "real",
            [
                "18446744073709552000",
                "18446744073709552000",
                "27670116110564327000",
                "12297829382473034000",
                "error",
                "1",
            ],
        ),
        (
            "literal_signed",
            "decimal",
            [
                "18446744073709551616.5",
                "18446744073709551613.5",
                "27670116110564327422.5",
                "12297829382473034410.0000",
                "error",
                "0.0",
            ],
        ),
        (
            "literal_unsigned",
            "real",
            [
                "18446744073709552000",
                "18446744073709552000",
                "27670116110564327000",
                "12297829382473034000",
                "12297829382473034410",
                "1",
            ],
        ),
        (
            "literal_unsigned",
            "decimal",
            [
                "18446744073709551616.5",
                "18446744073709551613.5",
                "27670116110564327422.5",
                "12297829382473034410.0000",
                "12297829382473034410",
                "0.0",
            ],
        ),
    ];
    let mut mismatches = Vec::new();
    for (name, rhs, expected) in cases {
        for (op_index, op) in ["plus", "minus", "mul", "div", "intdiv", "mod"]
            .into_iter()
            .enumerate()
        {
            if rhs == "max" && op != "mul" {
                continue;
            }
            for vectorized in [false, true] {
                let mut bytes = vec![255; 8];
                if name == "literal_wide" {
                    bytes.insert(0, 1);
                }
                let mut field = FieldType::new(match name {
                    "enum" => C::Enum,
                    "set" | "set_precision" | "set_overflow" => C::Set,
                    "bit_signed" | "bit_unsigned" => C::Bit,
                    _ => C::VarString,
                });
                let value = match name {
                    "enum" => Datum::Enum(MysqlEnum::new("999", 2), Collation::Binary),
                    "set" | "set_precision" | "set_overflow" => Datum::Set(
                        MysqlSet::new(
                            "999",
                            match name {
                                "set_precision" => 9007199254740993,
                                "set_overflow" => u64::MAX,
                                _ => 3,
                            },
                        ),
                        Collation::Binary,
                    ),
                    "bit_signed" | "bit_unsigned" => {
                        field.set_flen(64);
                        Datum::Bit(BinaryLiteral::from(bytes))
                    }
                    _ => {
                        field.set_charset_name("binary");
                        field.set_collation_name("binary");
                        Datum::BinaryLiteral(BinaryLiteral::from(bytes))
                    }
                };
                if matches!(name, "bit_unsigned" | "literal_unsigned" | "literal_wide") {
                    field.add_flags(FieldTypeFlags::UNSIGNED);
                }
                let column = |index, field| {
                    let mut c = crate::column::Column::new(index + 1, field);
                    c.index = index;
                    Expression::Column(c)
                };
                let left = if name.starts_with("literal") {
                    Expression::Constant(crate::constant::Constant::new(
                        value.clone(),
                        field.clone(),
                    ))
                } else {
                    column(0, field.clone())
                };
                let mut integer = FieldType::new(match rhs {
                    "real" | "max" => C::Double,
                    "decimal" => C::NewDecimal,
                    _ => C::LongLong,
                });
                let right_value = match rhs {
                    "real" => Datum::Real(1.5),
                    "max" => Datum::Real(f64::MAX),
                    "decimal" => {
                        integer.set_flen(2);
                        integer.set_decimal(1);
                        Datum::Decimal(Decimal::from_literal("1.5"))
                    }
                    "min" => Datum::Int(i64::MIN),
                    _ => Datum::Int(1),
                };
                let right = column(1, integer.clone());
                let output =
                    crate::builtin_arithmetic::infer_arithmetic_type(op, &left, &right).unwrap();
                let expr = Expression::ScalarFunction(ScalarFunction::new(
                    tidb_ast::CiString::new(op),
                    output,
                    vec![left, right],
                ));
                let mut input = tidb_chunk::chunk::Chunk::new_with_capacity(&[field, integer], 1);
                input.append_datum(0, &value);
                input.append_datum(1, &right_value);
                let result = if vectorized {
                    try_eval_numeric_batch(&expr, &Strict, &input).and_then(|batch| {
                        Ok(batch
                            .expect("hybrid numeric batch must be supported")
                            .remove(0))
                    })
                } else {
                    expr.eval(&Strict, input.get_row(0))
                };
                let actual = result
                    .map(|value| value.sql_string().unwrap())
                    .map_err(|error| match error {
                        crate::EvalError::Conversion(error) => {
                            let error = error.to_sql_error();
                            (error.code, error.message)
                        }
                        crate::EvalError::DataOutOfRange { value, expression } => (
                            1690,
                            format!("{value} value is out of range in '{expression}'"),
                        ),
                        other => (0, format!("{other:?}")),
                    });
                let wanted = if expected[op_index] != "error" {
                    Ok(expected[op_index].to_owned())
                } else {
                    Err(match name {
                    "literal_signed" if rhs == "min" => (1690, format!("BIGINT value is out of range in '(-1 {} Column#2)'", if op == "plus" { "+" } else { "*" })),
                    "literal_signed" => (1690, format!("BIGINT value is out of range in '(18446744073709551615 DIV {})'", if vectorized { "1.5" } else if rhs == "real" { "cast(Column#2, decimal(65,0) BINARY)" } else { "Column#2" })),
                    "set_overflow" => (1690, "constant 1.8446744073709552e+19 overflows bigint".to_owned()),
                    "literal_wide" => (1292, "Truncated incorrect BINARY value: '0x01ffffffffffffffff'".to_owned()),
                    "literal_unsigned" if rhs == "max" => (1690, "DOUBLE value is out of range in '(1.8446744073709552e+19 * Column#2)'".to_owned()),
                    "literal_unsigned" => (1690, "BIGINT UNSIGNED value is out of range in '(18446744073709551615 + Column#2)'".to_owned()),
                    _ => (1690, "BIGINT UNSIGNED value is out of range in '(Column#1 + Column#2)'".to_owned()),
                })
                };
                if actual != wanted {
                    mismatches.push(format!(
                        "{name}/{rhs}/{op}/{vectorized}: {actual:?}; want {wanted:?}"
                    ));
                }
            }
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}

#[test]
fn hybrid_arithmetic_preserves_null_and_selection_order() {
    use crate::scalar_function::{try_eval_numeric_batch, ScalarFunction};
    let field = FieldType::new(C::Set);
    let column = |index| {
        let mut column = crate::column::Column::new(index + 1, field.clone());
        column.index = index;
        Expression::Column(column)
    };
    let datum = |number| {
        Datum::Set(
            tidb_datatype::MysqlSet::new("999", number),
            tidb_datatype::Collation::Binary,
        )
    };
    for (op, expected) in [
        ("plus", "3"),
        ("minus", "-1"),
        ("mul", "2"),
        ("div", "0.5"),
        ("intdiv", "0"),
        ("mod", "1"),
    ] {
        for vectorized in [false, true] {
            for selected in [false, true] {
                let left = column(0);
                let right = column(1);
                let output =
                    crate::builtin_arithmetic::infer_arithmetic_type(op, &left, &right).unwrap();
                let expression = Expression::ScalarFunction(ScalarFunction::new(
                    tidb_ast::CiString::new(op),
                    output,
                    vec![left, right],
                ));
                let mut input =
                    tidb_chunk::chunk::Chunk::new_with_capacity(&[field.clone(), field.clone()], 2);
                input.append_null(0);
                input.append_datum(1, &datum(u64::MAX));
                if selected {
                    input.append_datum(0, &datum(1));
                    input.append_datum(1, &datum(2));
                    input.set_sel(Some(vec![1]));
                }
                let result = if vectorized {
                    try_eval_numeric_batch(&expression, &crate::NoColumns, &input)
                        .map(|batch| batch.unwrap().remove(0))
                } else {
                    expression.eval(&crate::NoColumns, input.get_row(0))
                };
                if selected {
                    assert_eq!(
                        result.unwrap().sql_string().unwrap(),
                        expected,
                        "{op}/{vectorized}"
                    );
                } else if vectorized || matches!(op, "plus" | "mod") {
                    let crate::EvalError::Conversion(error) = result.unwrap_err() else {
                        panic!("expected SET conversion overflow")
                    };
                    assert_eq!(
                        (
                            error.to_sql_error().code,
                            error.to_sql_error().message.as_str()
                        ),
                        (1690, "constant 1.8446744073709552e+19 overflows bigint")
                    );
                } else {
                    assert_eq!(result.unwrap(), Datum::Null, "{op}");
                }
            }
        }
    }
}
