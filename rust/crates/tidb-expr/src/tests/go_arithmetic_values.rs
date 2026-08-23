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
                Datum::UInt(_) => FieldType::new(C::LongLong)
                    .with_added_flags(FieldTypeFlags::UNSIGNED),
                Datum::Float32(_) | Datum::Real(_) => FieldType::new(C::Double),
                Datum::String(_) | Datum::Bytes(_) => FieldType::new(C::VarString),
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
    let ret_type = crate::builtin_arithmetic::infer_arithmetic_type(
        name,
        &exprs[0],
        &exprs[1],
    )
    .unwrap_or_else(|| panic!("{name}: no inferred type"));
    let function = crate::scalar_function::ScalarFunction::new(
        tidb_ast::CiString::new(name),
        ret_type,
        exprs,
    );
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
        let value =
            go_eval("div", args).unwrap_or_else(|error| panic!("{args:?}: {error:?}"));
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
