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

//! `pkg/expression/builtin_arithmetic.go`: the arithmetic function classes'
//! result-type derivation (`getFunction` on `arithmeticPlus/Minus/Multiply/
//! Divide/IntDivide/ModFunctionClass`), transcreated as
//! [`infer_arithmetic_type`].
//!
//! Go's classes pick a signature (Int/Real/Decimal) from the operands'
//! numeric-context types and derive the result `FieldType` (type code,
//! flen/decimal, unsigned flag). The value evaluation itself is carried by the
//! shared Datum operator semantics (`ops.rs`), which dispatch on the operand
//! kinds exactly as the per-signature builtins do.
//!
//! DEFERRED (documented): the vector-float32 signatures; the SQL-mode
//! `NO_UNSIGNED_SUBTRACTION` check in minus (assumed unset -- the default);
//! `div_precision_increment` is fixed at its default (4) until the session
//! variable is wired; and `newReturnFieldTypeForBaseBuiltinFunc`'s
//! boolean-function flag (none of these six are boolean functions).

use crate::expression::Expression;
use tidb_datatype::{
    Datum, EvalType, FieldType, FieldTypeBuilder, FieldTypeCode, FieldTypeFlags, MAX_DECIMAL_WIDTH,
    UNSPECIFIED_LENGTH,
};

/// Go `mysql.MaxIntWidth` (`pkg/parser/mysql/const.go` = 20).
const MAX_INT_WIDTH: i64 = 20;
/// Go `mysql.MaxRealWidth` (`pkg/parser/mysql/const.go` = 23).
const MAX_REAL_WIDTH: i64 = 23;
/// Go `vardef.DefDivPrecisionIncrement` (= 4), the `div_precision_increment`
/// default; the session variable is not yet wired.
const DEF_DIV_PRECISION_INCREMENT: i64 = 4;

/// Go `isConstantBinaryLiteral`: a constant whose value is a binary literal
/// (like `0x1234`) with a binary string type.
fn is_constant_binary_literal(expr: &Expression) -> bool {
    let Some(ft) = expr.static_type() else {
        return false;
    };
    // Go types.IsBinaryStr: a string-family type whose collation is binary.
    let is_binary_str = ft.collation_name() == "binary"
        && matches!(
            ft.code(),
            FieldTypeCode::Varchar
                | FieldTypeCode::VarString
                | FieldTypeCode::String
                | FieldTypeCode::Blob
                | FieldTypeCode::TinyBlob
                | FieldTypeCode::MediumBlob
                | FieldTypeCode::LongBlob
        );
    if !is_binary_str {
        return false;
    }
    match expr {
        Expression::Constant(c) => {
            matches!(c.value, Datum::BinaryLiteral(_))
        }
        _ => false,
    }
}

/// Go `numericContextResultType`: the numeric evaluation type an operand takes
/// in an arithmetic context.
fn numeric_context_result_type(expr: &Expression) -> EvalType {
    let Some(ft) = expr.static_type() else {
        // A missing type cannot happen in Go (RetType is always set); treat it
        // as real, the most general numeric context.
        return EvalType::Real;
    };
    if ft.code().is_type_temporal() {
        if ft.decimal() > 0 {
            return EvalType::Decimal;
        }
        return EvalType::Int;
    }
    // Constant binary literals (0x1234) and BIT operands act as integers.
    if is_constant_binary_literal(expr) || ft.code() == FieldTypeCode::Bit {
        return EvalType::Int;
    }
    let mut eval_tp = EvalType::Real;
    if !ft.is_hybrid() {
        eval_tp = ft.eval_type();
        if eval_tp != EvalType::Decimal && eval_tp != EvalType::Int {
            eval_tp = EvalType::Real;
        }
    }
    eval_tp
}

/// Go `newReturnFieldTypeForBaseBuiltinFunc`, restricted to the numeric arms
/// this module (and the sibling `builtin_compare`/`builtin_op` modules) needs:
/// the base result type for an Int/Real/Decimal signature. The Go function's
/// trailing `booleanFunctions` check is applied by the callers that need it
/// (none of the arithmetic classes are boolean functions).
pub(crate) fn new_return_field_type(ret: EvalType) -> FieldType {
    let mut ft = match ret {
        EvalType::Int => FieldTypeBuilder::new()
            .with_code(FieldTypeCode::LongLong)
            .add_flags(FieldTypeFlags::BINARY)
            .flen_set(MAX_INT_WIDTH)
            .build(),
        EvalType::Real => FieldTypeBuilder::new()
            .with_code(FieldTypeCode::Double)
            .add_flags(FieldTypeFlags::BINARY)
            .flen_set(MAX_REAL_WIDTH)
            .decimal_set(UNSPECIFIED_LENGTH)
            .build(),
        EvalType::Decimal => FieldTypeBuilder::new()
            .with_code(FieldTypeCode::NewDecimal)
            .add_flags(FieldTypeFlags::BINARY)
            .flen_set(11)
            .build(),
        other => unreachable!("arithmetic result type is never {other:?}"),
    };
    // A binary-flagged non-JSON result carries the binary charset/collation.
    ft.set_charset_name("binary");
    ft.set_collation_name("binary");
    ft
}

/// Go `setFlenDecimal4RealOrDecimal`.
fn set_flen_decimal4_real_or_decimal(
    ret: &mut FieldType,
    a: &FieldType,
    b: &FieldType,
    is_real: bool,
    is_multiply: bool,
) {
    if a.decimal() != UNSPECIFIED_LENGTH && b.decimal() != UNSPECIFIED_LENGTH {
        ret.set_decimal_under_limit(a.decimal() + b.decimal());
        if !is_multiply {
            ret.set_decimal_under_limit(a.decimal().max(b.decimal()));
        }
        if !is_real && ret.decimal() > tidb_datatype::MAX_DECIMAL_SCALE {
            ret.set_decimal(tidb_datatype::MAX_DECIMAL_SCALE);
        }
        if a.flen() == UNSPECIFIED_LENGTH || b.flen() == UNSPECIFIED_LENGTH {
            ret.set_flen(UNSPECIFIED_LENGTH);
            return;
        }
        if is_multiply {
            let digits_int = a.flen() - a.decimal() + b.flen() - b.decimal();
            ret.set_flen_under_limit(digits_int + ret.decimal());
        } else {
            let digits_int = (a.flen() - a.decimal()).max(b.flen() - b.decimal());
            ret.set_flen_under_limit(digits_int + ret.decimal() + 1);
        }
        if is_real {
            ret.set_flen(ret.flen().min(MAX_REAL_WIDTH));
            return;
        }
        ret.set_flen_under_limit(ret.flen().min(MAX_DECIMAL_WIDTH));
        return;
    }
    if is_real {
        ret.set_flen(UNSPECIFIED_LENGTH);
        ret.set_decimal(UNSPECIFIED_LENGTH);
    } else {
        ret.set_flen(MAX_DECIMAL_WIDTH);
        ret.set_decimal(tidb_datatype::MAX_DECIMAL_SCALE);
    }
}

/// Go `types.DecimalLength2Precision`.
fn decimal_length2_precision(mut length: i64, scale: i64, has_unsigned_flag: bool) -> i64 {
    if scale > 0 {
        length -= 1;
    }
    if has_unsigned_flag || length > 0 {
        length -= 1;
    }
    length
}

/// Go `types.Precision2LengthNoTruncation`.
fn precision2_length_no_truncation(mut length: i64, scale: i64, has_unsigned_flag: bool) -> i64 {
    if scale > 0 {
        length += 1;
    }
    if has_unsigned_flag || length > 0 {
        length += 1;
    }
    length
}

/// Go `arithmeticDivideFunctionClass.setType4DivReal`.
fn set_type4_div_real(ret: &mut FieldType) {
    ret.set_decimal(UNSPECIFIED_LENGTH);
    ret.set_flen(MAX_REAL_WIDTH);
}

/// Go `arithmeticDivideFunctionClass.setType4DivDecimal`.
fn set_type4_div_decimal(ret: &mut FieldType, a: &FieldType, b: &FieldType, div_prec_inc: i64) {
    // Go UnspecifiedFsp == UnspecifiedLength == -1.
    let deca = if a.decimal() == UNSPECIFIED_LENGTH {
        0
    } else {
        a.decimal()
    };
    let decb = if b.decimal() == UNSPECIFIED_LENGTH {
        0
    } else {
        b.decimal()
    };
    ret.set_decimal_under_limit(deca + div_prec_inc);
    if a.flen() == UNSPECIFIED_LENGTH {
        ret.set_flen(MAX_DECIMAL_WIDTH);
        return;
    }
    let a_prec = decimal_length2_precision(a.flen(), a.decimal(), a.is_unsigned());
    ret.set_flen_under_limit(a_prec + decb + div_prec_inc);
    let no_trunc = precision2_length_no_truncation(ret.flen(), ret.decimal(), ret.is_unsigned());
    ret.set_flen_under_limit(no_trunc);
}

/// Go `arithmeticModFunctionClass.setType4ModRealOrDecimal`.
fn set_type4_mod_real_or_decimal(
    ret: &mut FieldType,
    a: &FieldType,
    b: &FieldType,
    is_decimal: bool,
) {
    if a.decimal() == UNSPECIFIED_LENGTH || b.decimal() == UNSPECIFIED_LENGTH {
        ret.set_decimal(UNSPECIFIED_LENGTH);
    } else {
        ret.set_decimal_under_limit(a.decimal().max(b.decimal()));
    }
    if a.flen() == UNSPECIFIED_LENGTH || b.flen() == UNSPECIFIED_LENGTH {
        ret.set_flen(UNSPECIFIED_LENGTH);
    } else {
        ret.set_flen(a.flen().max(b.flen()));
        if is_decimal {
            ret.set_flen_under_limit(ret.flen());
            return;
        }
        ret.set_flen(ret.flen().min(MAX_REAL_WIDTH));
    }
}

fn unsigned(ft: Option<&FieldType>) -> bool {
    ft.is_some_and(FieldType::is_unsigned)
}

/// The result `FieldType` the Go arithmetic function classes derive in
/// `getFunction`, for the operator scalar-function `name`
/// (`plus`/`minus`/`mul`/`div`/`intdiv`/`mod`). Returns `None` for any other
/// function name.
#[must_use]
pub fn infer_arithmetic_type(name: &str, lhs: &Expression, rhs: &Expression) -> Option<FieldType> {
    let lhs_tp = numeric_context_result_type(lhs);
    let rhs_tp = numeric_context_result_type(rhs);
    let a = lhs.static_type();
    let b = rhs.static_type();
    let real_or_decimal = |is_multiply: bool| -> FieldType {
        if lhs_tp == EvalType::Real || rhs_tp == EvalType::Real {
            let mut ret = new_return_field_type(EvalType::Real);
            if let (Some(a), Some(b)) = (a, b) {
                set_flen_decimal4_real_or_decimal(&mut ret, a, b, true, is_multiply);
            }
            ret
        } else {
            let mut ret = new_return_field_type(EvalType::Decimal);
            if let (Some(a), Some(b)) = (a, b) {
                set_flen_decimal4_real_or_decimal(&mut ret, a, b, false, is_multiply);
            }
            ret
        }
    };
    let int_result = lhs_tp != EvalType::Real
        && rhs_tp != EvalType::Real
        && lhs_tp != EvalType::Decimal
        && rhs_tp != EvalType::Decimal;

    Some(match name {
        "plus" | "minus" => {
            if int_result {
                let mut ret = new_return_field_type(EvalType::Int);
                // Minus additionally consults NO_UNSIGNED_SUBTRACTION (assumed
                // unset, the default), making the branches identical here.
                if unsigned(a) || unsigned(b) {
                    ret.add_flags(FieldTypeFlags::UNSIGNED);
                }
                ret
            } else {
                real_or_decimal(false)
            }
        }
        "mul" => {
            if int_result {
                let mut ret = new_return_field_type(EvalType::Int);
                if unsigned(a) || unsigned(b) {
                    ret.add_flags(FieldTypeFlags::UNSIGNED);
                }
                ret
            } else {
                real_or_decimal(true)
            }
        }
        "div" => {
            // `/` never yields Int: Real if either side is real, else Decimal.
            if lhs_tp == EvalType::Real || rhs_tp == EvalType::Real {
                let mut ret = new_return_field_type(EvalType::Real);
                set_type4_div_real(&mut ret);
                ret
            } else {
                let mut ret = new_return_field_type(EvalType::Decimal);
                if let (Some(a), Some(b)) = (a, b) {
                    set_type4_div_decimal(&mut ret, a, b, DEF_DIV_PRECISION_INCREMENT);
                }
                if unsigned(a) {
                    // Note: Go sets no flag here; division result keeps default.
                }
                ret
            }
        }
        "intdiv" => {
            // `DIV` always yields Int; unsigned if either side is unsigned.
            let mut ret = new_return_field_type(EvalType::Int);
            if unsigned(a) || unsigned(b) {
                ret.add_flags(FieldTypeFlags::UNSIGNED);
            }
            ret
        }
        "mod" => {
            if lhs_tp == EvalType::Real || rhs_tp == EvalType::Real {
                let mut ret = new_return_field_type(EvalType::Real);
                if let (Some(a), Some(b)) = (a, b) {
                    set_type4_mod_real_or_decimal(&mut ret, a, b, false);
                }
                if unsigned(a) {
                    ret.add_flags(FieldTypeFlags::UNSIGNED);
                }
                ret
            } else if lhs_tp == EvalType::Decimal || rhs_tp == EvalType::Decimal {
                let mut ret = new_return_field_type(EvalType::Decimal);
                if let (Some(a), Some(b)) = (a, b) {
                    set_type4_mod_real_or_decimal(&mut ret, a, b, true);
                }
                if unsigned(a) {
                    ret.add_flags(FieldTypeFlags::UNSIGNED);
                }
                ret
            } else {
                // Mod's int result is unsigned iff the LHS is unsigned.
                let mut ret = new_return_field_type(EvalType::Int);
                if unsigned(a) {
                    ret.add_flags(FieldTypeFlags::UNSIGNED);
                }
                ret
            }
        }
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constant::Constant;

    fn int_expr(v: i64) -> Expression {
        Expression::Constant(Constant::new(
            Datum::Int(v),
            FieldTypeBuilder::new()
                .with_code(FieldTypeCode::LongLong)
                .flen_set(MAX_INT_WIDTH)
                .decimal_set(0)
                .build(),
        ))
    }

    fn real_expr(v: f64) -> Expression {
        Expression::Constant(Constant::new(
            Datum::Real(v),
            FieldTypeBuilder::new()
                .with_code(FieldTypeCode::Double)
                .flen_set(MAX_REAL_WIDTH)
                .decimal_set(UNSPECIFIED_LENGTH)
                .build(),
        ))
    }

    #[test]
    fn int_plus_int_is_longlong() {
        let ret = infer_arithmetic_type("plus", &int_expr(1), &int_expr(2)).unwrap();
        assert_eq!(ret.code(), FieldTypeCode::LongLong);
        assert!(!ret.is_unsigned());
    }

    #[test]
    fn real_operand_promotes_to_double() {
        for op in ["plus", "minus", "mul", "mod"] {
            let ret = infer_arithmetic_type(op, &real_expr(1.5), &int_expr(1)).unwrap();
            assert_eq!(ret.code(), FieldTypeCode::Double, "op {op}");
        }
    }

    #[test]
    fn div_is_decimal_for_ints_real_for_reals() {
        let dec = infer_arithmetic_type("div", &int_expr(1), &int_expr(2)).unwrap();
        assert_eq!(dec.code(), FieldTypeCode::NewDecimal);
        // int/int: decimal = 0 + div_precision_increment(4).
        assert_eq!(dec.decimal(), 4);

        let real = infer_arithmetic_type("div", &real_expr(1.0), &int_expr(2)).unwrap();
        assert_eq!(real.code(), FieldTypeCode::Double);
        assert_eq!(real.decimal(), UNSPECIFIED_LENGTH);
        assert_eq!(real.flen(), MAX_REAL_WIDTH);
    }

    #[test]
    fn intdiv_is_always_int() {
        let ret = infer_arithmetic_type("intdiv", &real_expr(1.5), &int_expr(2)).unwrap();
        assert_eq!(ret.code(), FieldTypeCode::LongLong);
    }

    #[test]
    fn unsigned_propagation() {
        let mut u_ft = FieldTypeBuilder::new()
            .with_code(FieldTypeCode::LongLong)
            .add_flags(FieldTypeFlags::UNSIGNED)
            .flen_set(MAX_INT_WIDTH)
            .decimal_set(0)
            .build();
        u_ft.set_flen(MAX_INT_WIDTH);
        let u = Expression::Constant(Constant::new(Datum::UInt(1), u_ft));

        // plus/mul/intdiv: unsigned if either side is unsigned.
        for op in ["plus", "mul", "intdiv"] {
            let ret = infer_arithmetic_type(op, &int_expr(1), &u).unwrap();
            assert!(ret.is_unsigned(), "op {op}");
        }
        // mod: unsigned iff the LHS is unsigned.
        assert!(!infer_arithmetic_type("mod", &int_expr(1), &u)
            .unwrap()
            .is_unsigned());
        assert!(infer_arithmetic_type("mod", &u, &int_expr(1))
            .unwrap()
            .is_unsigned());
    }

    #[test]
    fn non_arithmetic_name_is_none() {
        assert!(infer_arithmetic_type("eq", &int_expr(1), &int_expr(1)).is_none());
    }
}
