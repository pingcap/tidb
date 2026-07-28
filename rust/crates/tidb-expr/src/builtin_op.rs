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

//! `pkg/expression/builtin_op.go`: the logic/bit/unary operators' result-type
//! derivation (`getFunction` on the `logicAnd/logicOr/logicXor`,
//! `bitAnd/bitOr/bitXor`, `leftShift/rightShift`, `unaryNot`, `unaryMinus` and
//! `bitNeg` function classes), transcreated as [`infer_op_type`] (binary) and
//! [`infer_unary_op_type`] (unary).
//!
//! What Go does per class:
//! - logic `and`/`or`/`xor`: base ETInt return type, then `SetFlen(1)`; the
//!   names are in the `booleanFunctions` map, so `mysql.IsBooleanFlag` is
//!   added by `newReturnFieldTypeForBaseBuiltinFunc`.
//! - `bitand`/`bitor`/`bitxor`/`leftshift`/`rightshift`: base ETInt return
//!   type plus `AddFlag(mysql.UnsignedFlag)`; not boolean functions.
//! - `not` (`unaryNotFunctionClass`): base ETInt, `SetFlen(1)`, boolean.
//! - `bitneg` (`bitNegFunctionClass`): base ETInt plus `UnsignedFlag`.
//! - `unaryminus`: its own type logic; only the plain int and real paths are
//!   ported (see [`infer_unary_op_type`]).
//!
//! DEFERRED (documented): `wrapWithIsTrue` on logic and/or arguments mutates
//! the *args* (istrue_with_null wrappers), not the result type, and is not
//! ported; `unaryMinusFunctionClass`'s int-overflow-to-decimal promotion
//! (`typeInfer` on a constant `-(max int + 1)`), its decimal/temporal arms,
//! and the column-arg flen reservation are deferred -- those paths return
//! `None` here so the rewriter keeps its placeholder.

use crate::builtin_arithmetic::new_return_field_type;
use crate::expression::Expression;
use tidb_datatype::{EvalType, FieldType, FieldTypeFlags};

/// The result `FieldType` Go's logic/bit function classes derive in
/// `getFunction`, for the binary operator scalar-function `name`
/// (`and`/`or`/`xor`, `bitand`/`bitor`/`bitxor`, `leftshift`/`rightshift`).
/// Returns `None` for any other function name.
#[must_use]
pub fn infer_op_type(name: &str) -> Option<FieldType> {
    match name {
        "and" | "or" | "xor" => {
            let mut ret = new_return_field_type(EvalType::Int);
            // logicAnd/logicOr/logicXor getFunction: SetFlen(1).
            ret.set_flen(1);
            // ast.LogicAnd/LogicOr/LogicXor are in Go's booleanFunctions map.
            ret.add_flags(FieldTypeFlags::IS_BOOLEAN);
            Some(ret)
        }
        "bitand" | "bitor" | "bitxor" | "leftshift" | "rightshift" => {
            let mut ret = new_return_field_type(EvalType::Int);
            // bitAnd/bitOr/bitXor/leftShift/rightShift getFunction:
            // AddFlag(mysql.UnsignedFlag).
            ret.add_flags(FieldTypeFlags::UNSIGNED);
            Some(ret)
        }
        _ => None,
    }
}

/// The result `FieldType` Go's unary operator function classes derive in
/// `getFunction`, for the unary scalar-function `name`
/// (`not`/`bitneg`/`unaryminus`).
///
/// `unaryminus` ports only the simple paths: an ETInt argument (without the
/// constant-int-overflow promotion to decimal) keeps ETInt with decimal 0, and
/// an ETReal argument keeps ETReal; both then reserve one extra display digit
/// (`SetFlenUnderLimit(argFlen + 1)`, the non-column arm). The decimal,
/// temporal, and overflow arms are deferred and return `None`.
///
/// Returns `None` for any other function name (including `unaryplus`, whose Go
/// class returns the argument unchanged rather than building a function).
#[must_use]
pub fn infer_unary_op_type(name: &str, arg: &Expression) -> Option<FieldType> {
    match name {
        "not" => {
            let mut ret = new_return_field_type(EvalType::Int);
            // unaryNotFunctionClass getFunction: SetFlen(1).
            ret.set_flen(1);
            // ast.UnaryNot is in Go's booleanFunctions map.
            ret.add_flags(FieldTypeFlags::IS_BOOLEAN);
            Some(ret)
        }
        "bitneg" => {
            let mut ret = new_return_field_type(EvalType::Int);
            // bitNegFunctionClass getFunction: AddFlag(mysql.UnsignedFlag).
            ret.add_flags(FieldTypeFlags::UNSIGNED);
            Some(ret)
        }
        "unaryminus" => {
            let arg_ft = arg.static_type()?;
            let eval_type = arg_ft.eval_type();
            let mut ret = match eval_type {
                // DEFERRED: the constant `intOverflow` check (typeInfer) that
                // promotes `-(literal beyond max int64)` to decimal.
                EvalType::Int => {
                    let mut ret = new_return_field_type(EvalType::Int);
                    ret.set_decimal(0);
                    ret
                }
                EvalType::Real => new_return_field_type(EvalType::Real),
                // Go's default arm: a decimal argument -- and a temporal one,
                // which it converts through decimal -- keeps the decimal
                // domain with the argument's own scale; everything else
                // (a string, JSON) negates as a real.
                other => {
                    let as_decimal = other == EvalType::Decimal
                        || matches!(
                            arg_ft.code(),
                            tidb_datatype::FieldTypeCode::Date
                                | tidb_datatype::FieldTypeCode::NewDate
                                | tidb_datatype::FieldTypeCode::Datetime
                                | tidb_datatype::FieldTypeCode::Timestamp
                                | tidb_datatype::FieldTypeCode::Duration
                        );
                    if as_decimal {
                        let mut ret = new_return_field_type(EvalType::Decimal);
                        ret.set_decimal_under_limit(arg_ft.decimal());
                        ret
                    } else {
                        new_return_field_type(EvalType::Real)
                    }
                }
            };
            // Go reserves a digit for the sign, except over a column whose
            // declared width already covers it.
            let is_column = matches!(arg, Expression::Column(_) | Expression::CorrelatedColumn(_));
            let column_keeps_width = is_column
                && (eval_type == EvalType::Decimal
                    || (eval_type == EvalType::Int
                        && arg_ft.flags() & FieldTypeFlags::UNSIGNED == 0));
            if column_keeps_width {
                ret.set_flen_under_limit(arg_ft.flen());
            } else {
                ret.set_flen_under_limit(arg_ft.flen() + 1);
            }
            Some(ret)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constant::Constant;
    use tidb_datatype::{Datum, FieldTypeBuilder, FieldTypeCode, UNSPECIFIED_LENGTH};

    fn int_expr(v: i64) -> Expression {
        Expression::Constant(Constant::new(
            Datum::Int(v),
            FieldTypeBuilder::new()
                .with_code(FieldTypeCode::LongLong)
                .flen_set(20)
                .decimal_set(0)
                .build(),
        ))
    }

    fn real_expr(v: f64) -> Expression {
        Expression::Constant(Constant::new(
            Datum::Real(v),
            FieldTypeBuilder::new()
                .with_code(FieldTypeCode::Double)
                .flen_set(23)
                .decimal_set(UNSPECIFIED_LENGTH)
                .build(),
        ))
    }

    #[test]
    fn logic_ops_are_boolean_flen1() {
        for name in ["and", "or", "xor"] {
            let ret = infer_op_type(name).unwrap();
            assert_eq!(ret.code(), FieldTypeCode::LongLong, "{name}");
            assert_eq!(ret.flen(), 1, "{name}");
            assert!(!ret.is_unsigned(), "{name}");
            assert_ne!(ret.flags() & FieldTypeFlags::IS_BOOLEAN, 0, "{name}");
        }
    }

    #[test]
    fn bit_ops_are_unsigned_longlong() {
        for name in ["bitand", "bitor", "bitxor", "leftshift", "rightshift"] {
            let ret = infer_op_type(name).unwrap();
            assert_eq!(ret.code(), FieldTypeCode::LongLong, "{name}");
            assert!(ret.is_unsigned(), "{name}");
            // Base ETInt flen (MaxIntWidth) is kept; no SetFlen(1).
            assert_eq!(ret.flen(), 20, "{name}");
            assert_eq!(ret.flags() & FieldTypeFlags::IS_BOOLEAN, 0, "{name}");
        }
    }

    #[test]
    fn unary_not_is_boolean_flen1() {
        let ret = infer_unary_op_type("not", &int_expr(1)).unwrap();
        assert_eq!(ret.code(), FieldTypeCode::LongLong);
        assert_eq!(ret.flen(), 1);
        assert_ne!(ret.flags() & FieldTypeFlags::IS_BOOLEAN, 0);
    }

    #[test]
    fn bitneg_is_unsigned() {
        let ret = infer_unary_op_type("bitneg", &int_expr(1)).unwrap();
        assert_eq!(ret.code(), FieldTypeCode::LongLong);
        assert!(ret.is_unsigned());
    }

    #[test]
    fn unary_minus_int_and_real_paths() {
        let int_ret = infer_unary_op_type("unaryminus", &int_expr(1)).unwrap();
        assert_eq!(int_ret.code(), FieldTypeCode::LongLong);
        assert_eq!(int_ret.decimal(), 0);
        // Sign digit reserved: arg flen 20 + 1.
        assert_eq!(int_ret.flen(), 21);

        let real_ret = infer_unary_op_type("unaryminus", &real_expr(1.5)).unwrap();
        assert_eq!(real_ret.code(), FieldTypeCode::Double);
        assert_eq!(real_ret.flen(), 24);
    }

    #[test]
    fn non_op_names_are_none() {
        assert!(infer_op_type("eq").is_none());
        assert!(infer_op_type("plus").is_none());
        assert!(infer_unary_op_type("unaryplus", &int_expr(1)).is_none());
    }
}
