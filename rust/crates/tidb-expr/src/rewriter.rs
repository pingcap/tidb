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

//! The expression rewriter: builds a planner [`Expression`] from a parsed AST
//! [`Expr`] (Go's `expression_rewriter.go`).
//!
//! This is the bridge from a parsed SQL expression to the evaluable expression
//! tree. It is a SEED: literals become [`Constant`]s and operators become
//! [`ScalarFunction`]s (named so [`ScalarFunction::eval`] dispatches them),
//! which is enough for constant/operator expressions such as `1 + 1` or
//! `2 * 3 - 1`.
//!
//! DEFERRED (documented): column references (need schema/name resolution), the
//! full literal domain (decimal/hex/bit/charset strings, unsigned promotion of
//! large integers), function calls, subqueries, and the result-type inference
//! that Go performs while rewriting for forms other than the arithmetic,
//! comparison, logic, bit and unary operators (which consult the transcreated
//! `builtin_arithmetic`/`builtin_compare`/`builtin_op` function classes);
//! uncovered forms keep a LongLong placeholder ret type (evaluation dispatches
//! on operand kinds, not on this type).

use crate::column::Column;
use crate::constant::Constant;
use crate::expression::{Expression, ScalarFunction};
use crate::scalar_function::{binary_op_name, unary_op_name};
use crate::EvalError;
use tidb_ast::{BinaryOp, CiString, Expr, IsTarget, UnaryOp};
use tidb_datatype::{Datum, FieldType, FieldTypeCode};

/// Resolves a dotted column path to an output column, standing in for the
/// schema/name resolution Go's `expression_rewriter` performs against the
/// plan's schema (`resolveColumn`).
pub trait ColumnResolver {
    /// Resolves `path` (e.g. `["t", "a"]` or `["a"]`) to
    /// `(row index, result type, unique id)`, or `None` when unknown.
    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)>;
}

/// A resolver that knows no columns (for constant-only expressions).
pub struct NoResolver;

impl ColumnResolver for NoResolver {
    fn resolve(&self, _path: &[String]) -> Option<(usize, FieldType, i64)> {
        None
    }
}

/// Go `types.SetBinChsClnFlag`: the binary charset/collation plus the binary
/// flag every non-string literal type carries.
fn set_binary_charset(ft: &mut FieldType) {
    ft.set_charset_name("binary");
    ft.set_collation_name("binary");
    ft.add_flags(tidb_datatype::FieldTypeFlags::BINARY);
}

/// Go `types.DefaultTypeForValue` for a `*MyDecimal`: the printed length plus
/// one for the decimal point, and the literal's own fractional digits.
fn decimal_literal_type(value: &tidb_datatype::Decimal) -> FieldType {
    let mut ft = FieldType::new(FieldTypeCode::NewDecimal);
    ft.set_flen_under_limit(value.to_string().chars().count() as i64);
    ft.set_decimal_under_limit(i64::from(value.scale()));
    ft.set_flen_under_limit(ft.flen() + 1);
    set_binary_charset(&mut ft);
    ft.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    ft
}

/// Go `types.DefaultTypeForValue` for a `BitLiteral`/`HexLiteral`: a binary
/// `VarString` three bytes wide per literal byte. Only the hex form is
/// unsigned, which is what makes `0x41 + 0` read the bytes as a number.
fn binary_literal_type(byte_len: usize, unsigned: bool) -> FieldType {
    let mut ft = FieldType::new(FieldTypeCode::VarString);
    ft.set_flen(byte_len as i64 * 3);
    ft.set_decimal(0);
    set_binary_charset(&mut ft);
    ft.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    if unsigned {
        ft.add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
    }
    ft
}

/// The result type of a builtin this rewriter is willing to build.
///
/// A chunk cell is sized from its column's type, so a wrong static type is a
/// panic rather than a wrong answer -- which is why this rewriter builds ONLY
/// the functions whose result type Go fixes to one thing, and refuses the
/// rest instead of falling back to a placeholder. Go's own per-class type
/// inference (`getFunction` on each `functionClass`) is the full version of
/// this table; the deferred names are listed with it.
///
/// NOT BUILT here, and refused (each needs more than a fixed result type):
/// `CURRENT_USER`/`USER` need a session user, which this tier's session does
/// not carry; `NOW` needs the statement clock the resolver does not supply
/// in the chunk path yet;
/// `CAST`/`CONVERT` take a target type, not a value, argument;
/// `GROUP_CONCAT` is an aggregate; `DATE_ADD`-family take an `Expr::Interval`
/// argument that is not an expression at all.
fn builtin_return_type(name: &str, args: &[Expression]) -> Option<FieldType> {
    let text = || {
        let mut ft = FieldType::new(FieldTypeCode::VarString);
        ft.set_decimal(tidb_datatype::UNSPECIFIED_LENGTH);
        ft
    };
    let int = || FieldType::new(FieldTypeCode::LongLong);
    Some(match name {
        // String in, string out.
        "concat" | "concat_ws" | "upper" | "ucase" | "lower" | "lcase" | "trim" | "ltrim"
        | "rtrim" | "reverse" | "left" | "right" | "substring" | "substr" | "mid" | "replace"
        | "repeat" | "lpad" | "rpad" | "space" | "hex" | "unhex" | "md5" | "elt" => text(),
        // The date/time family. Every value this crate produces for them is
        // a formatted string or an integer -- see `time_fn`'s own doc for
        // why there is no `Time` value domain here -- so the result types
        // are the string and integer ones rather than Go's temporal types.
        // The VALUES match TiDB; the reported column type is the documented
        // divergence, the same one the temporal casts carry.
        "now" | "current_timestamp" | "utc_timestamp" | "curdate" | "current_date" | "utc_date"
        | "curtime" | "current_time" | "utc_time" | "monthname" | "dayname" | "last_day"
        | "sec_to_time" | "maketime" | "makedate" | "from_days" => text(),
        "month" | "day" | "dayofmonth" | "dayofweek" | "dayofyear" | "weekday" | "quarter"
        | "week" | "weekofyear" | "yearweek" | "year" | "hour" | "minute" | "second"
        | "microsecond" | "time_to_sec" | "to_days" | "period_add" | "period_diff"
        | "unix_timestamp" | "datediff" => int(),
        // Go reads these from `SessionVars`; each returns a string.
        "database" | "schema" | "version" => text(),
        // String in, number out.
        "length" | "octet_length" | "char_length" | "character_length" | "bit_length" | "ascii"
        | "instr" | "locate" | "position" | "find_in_set" | "strcmp" | "field" => int(),
        // Go `likeFunctionClass`: a one-digit boolean.
        "like" | "ilike" => {
            let mut ft = int();
            ft.set_flen(1);
            ft.add_flags(tidb_datatype::FieldTypeFlags::IS_BOOLEAN);
            ft
        }
        // Go aggregates the branch types of these (`aggregateType`). Only a
        // set of branches that already agree is built here; a mixed set is
        // refused rather than guessed, because the guess sizes a chunk cell.
        "case_when" | "if" | "ifnull" | "coalesce" | "nullif" => {
            // A NULL branch carries no type of its own -- Go's `aggregateType`
            // ignores it -- so only the typed branches have to agree.
            let branches = args
                .iter()
                .filter_map(Expression::static_type)
                .filter(|ft| ft.code() != FieldTypeCode::Null);
            let typed: Vec<&FieldType> = branches.collect();
            let first = (*typed.first()?).clone();
            // Go `types.AggFieldType` merges the string family to VarString,
            // which is what lets `IFNULL(varchar_column, 'literal')` -- a
            // Varchar branch and a VarString branch -- have one type. Other
            // mixtures are refused rather than guessed, since the result type
            // sizes a chunk cell.
            if typed
                .iter()
                .all(|ft| ft.eval_type() == tidb_datatype::EvalType::String)
            {
                if typed.iter().any(|ft| ft.code() != first.code()) {
                    text()
                } else {
                    first
                }
            } else {
                if typed.iter().any(|ft| ft.code() != first.code()) {
                    return None;
                }
                first
            }
        }
        _ => return None,
    })
}

/// The function name and result type a `CAST(expr AS type)` becomes.
///
/// Go picks one `builtinCast*As*Sig` per target type; the name here carries
/// that choice, so evaluation never has to re-derive the target from a result
/// type that may not describe it (the temporal targets produce a string
/// value in this crate -- see below -- so their type genuinely cannot).
///
/// `TIME` and `JSON` targets, and the `ARRAY` modifier, are refused here for
/// the same reason `cast::eval_cast` refuses them -- there is no value domain
/// for them in this crate yet.
fn cast_target(cast_type: &tidb_ast::CastType) -> Option<(&'static str, FieldType)> {
    use tidb_ast::CastType;
    let name = match cast_type {
        CastType::Signed => "cast_signed",
        CastType::Unsigned => "cast_unsigned",
        CastType::Char { .. } => "cast_char",
        CastType::Binary { .. } => "cast_binary",
        CastType::Decimal { .. } => "cast_decimal",
        CastType::Date => "cast_date",
        CastType::DateTime { .. } => "cast_datetime",
        CastType::Year => "cast_year",
        CastType::Double | CastType::Float => "cast_double",
        CastType::Time { .. } | CastType::Json => return None,
    };
    let ft = match cast_type {
        CastType::Signed => FieldType::new(FieldTypeCode::LongLong),
        CastType::Unsigned => {
            let mut ft = FieldType::new(FieldTypeCode::LongLong);
            ft.add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
            ft
        }
        CastType::Char { len, .. } => {
            let mut ft = FieldType::new(FieldTypeCode::VarString);
            if let Some(len) = len {
                ft.set_flen(i64::from(*len));
            }
            ft
        }
        CastType::Binary { len } => {
            let mut ft = FieldType::new(FieldTypeCode::VarString);
            ft.set_charset_name("binary");
            ft.set_collation_name("binary");
            ft.add_flags(tidb_datatype::FieldTypeFlags::BINARY);
            if let Some(len) = len {
                ft.set_flen(i64::from(*len));
            }
            ft
        }
        CastType::Decimal { flen, scale } => {
            let mut ft = FieldType::new(FieldTypeCode::NewDecimal);
            ft.set_flen(i64::from(*flen));
            ft.set_decimal(i64::from(*scale));
            ft
        }
        // DOCUMENTED DIVERGENCE: `cast::eval_cast` produces a FORMATTED
        // STRING for a temporal target (see its own doc -- this crate has no
        // `Time` value in the cast path), so the chunk column that holds the
        // result has to be a string column. The VALUE matches TiDB exactly;
        // the reported column TYPE is `VarString` where TiDB says `DATE` or
        // `DATETIME`. Typing it as Go does would put a string into a
        // fixed-width temporal cell, which panics rather than mistyping.
        CastType::Date | CastType::DateTime { .. } => FieldType::new(FieldTypeCode::VarString),
        // Likewise, the year cast yields an integer value here.
        CastType::Year => FieldType::new(FieldTypeCode::LongLong),
        CastType::Double | CastType::Float => FieldType::new(FieldTypeCode::Double),
        CastType::Time { .. } | CastType::Json => return None,
    };
    Some((name, ft))
}

fn constant(datum: Datum, code: FieldTypeCode) -> Expression {
    Expression::Constant(Constant::new(datum, FieldType::new(code)))
}

fn scalar(name: &str, args: Vec<Expression>) -> Expression {
    // The result type is a placeholder: operator evaluation dispatches on the
    // operand datum kinds, not on this type. Faithful type inference is deferred.
    Expression::ScalarFunction(ScalarFunction::new(
        CiString::new(name),
        FieldType::new(FieldTypeCode::LongLong),
        args,
    ))
}

/// Go `expression_rewriter`: rewrite a parsed AST [`Expr`] into an evaluable
/// [`Expression`].
///
/// Supports integer/float/string/boolean/NULL literals, unary and binary
/// operators, and parentheses. Returns [`EvalError::Unsupported`] for forms not
/// yet handled (column references, function calls, other literal kinds).
pub fn rewrite_expr(expr: &Expr) -> Result<Expression, EvalError> {
    rewrite_expr_resolved(expr, &NoResolver)
}

/// [`rewrite_expr`] with column resolution: `Expr::Column` paths are bound
/// through `resolver` into [`Expression::Column`] nodes (index + result type).
pub fn rewrite_expr_resolved(
    expr: &Expr,
    resolver: &impl ColumnResolver,
) -> Result<Expression, EvalError> {
    if let Expr::Column(path) = expr {
        let (index, ret_type, unique_id) = resolver
            .resolve(path)
            .ok_or(EvalError::Unsupported("unresolved column reference"))?;
        let mut col = Column::new(unique_id, ret_type);
        col.index = index as i64;
        return Ok(Expression::Column(col));
    }
    rewrite_leaf(expr, resolver)
}

fn rewrite_leaf(expr: &Expr, resolver: &impl ColumnResolver) -> Result<Expression, EvalError> {
    match expr {
        Expr::Int(text) => {
            let value: i64 = text
                .parse()
                .map_err(|_| EvalError::Unsupported("integer literal outside the i64 domain"))?;
            Ok(constant(Datum::Int(value), FieldTypeCode::LongLong))
        }
        Expr::Float(value) => Ok(constant(Datum::Real(*value), FieldTypeCode::Double)),
        Expr::Bool(value) => Ok(constant(
            Datum::Int(i64::from(*value)),
            FieldTypeCode::LongLong,
        )),
        Expr::Null => Ok(constant(Datum::Null, FieldTypeCode::Null)),
        Expr::String(text) => {
            let mut datum = Datum::Null;
            datum.set_bytes(text.clone().into_bytes());
            Ok(Expression::Constant(Constant::new(
                datum,
                FieldType::new(FieldTypeCode::VarString),
            )))
        }
        // Go's parser folds a decimal literal into a `*MyDecimal` value whose
        // type `DefaultTypeForValue` derives from the printed literal.
        Expr::Decimal(text) => {
            let value = tidb_datatype::Decimal::from_literal(text);
            let ft = decimal_literal_type(&value);
            Ok(Expression::Constant(Constant::new(
                Datum::Decimal(value),
                ft,
            )))
        }
        // `0x41` / `x'4142'`: Go keeps the raw bytes as a `HexLiteral`, which
        // prints as a string but converts to a number by its byte value.
        Expr::Hex(digits) => {
            let literal = tidb_datatype::parse_hex_str(&format!("0x{digits}"))
                .map_err(|_| EvalError::Unsupported("malformed hexadecimal literal"))?;
            let ft = binary_literal_type(literal.as_bytes().len(), true);
            Ok(Expression::Constant(Constant::new(
                Datum::BinaryLiteral(literal),
                ft,
            )))
        }
        // `b'1010'`: the same shape as a hex literal, but signed.
        Expr::Bit(digits) => {
            let literal = tidb_datatype::parse_bit_str(&format!("0b{digits}"))
                .map_err(|_| EvalError::Unsupported("malformed bit literal"))?;
            let ft = binary_literal_type(literal.as_bytes().len(), false);
            Ok(Expression::Constant(Constant::new(
                Datum::BinaryLiteral(literal),
                ft,
            )))
        }
        Expr::Paren(inner) => rewrite_expr_resolved(inner, resolver),
        // Go's `in` builtin takes the tested value as args[0] and the list as
        // the remaining arguments; `NOT IN` wraps it in a unary NOT, which
        // keeps NULL as NULL exactly as MySQL requires.
        Expr::In { expr, list, not } => {
            let mut args = Vec::with_capacity(list.len() + 1);
            args.push(rewrite_expr_resolved(expr, resolver)?);
            for item in list {
                args.push(rewrite_expr_resolved(item, resolver)?);
            }
            let mut ret_type = FieldType::new(FieldTypeCode::LongLong);
            ret_type.set_flen(1);
            let call = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("in"),
                ret_type.clone(),
                args,
            ));
            if *not {
                return Ok(Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new(unary_op_name(UnaryOp::Not)),
                    ret_type,
                    vec![call],
                )));
            }
            Ok(call)
        }
        // Go rewrites `x IS <target>` into the isnull/istrue/isfalse builtin,
        // wrapping `IS NOT` in a unary NOT. These return 0/1 and never NULL,
        // so the wrapping NOT is exact.
        Expr::Is { expr, target, not } => {
            let arg = rewrite_expr_resolved(expr, resolver)?;
            let name = match target {
                // `IS UNKNOWN` is `IS NULL` (Go maps both to isnull).
                IsTarget::Null | IsTarget::Unknown => "isnull",
                IsTarget::True => "istrue",
                IsTarget::False => "isfalse",
            };
            // Go's result is a one-digit integer (`flen` 1, boolean-flagged).
            let mut ret_type = FieldType::new(FieldTypeCode::LongLong);
            ret_type.set_flen(1);
            let call = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(name),
                ret_type.clone(),
                vec![arg],
            ));
            if *not {
                return Ok(Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new(unary_op_name(UnaryOp::Not)),
                    ret_type,
                    vec![call],
                )));
            }
            Ok(call)
        }
        Expr::Unary(op, inner) => {
            let arg = rewrite_expr_resolved(inner, resolver)?;
            let name = unary_op_name(*op);
            // not/bitneg/unaryminus result types come from the transcreated
            // builtin_op function classes; anything uncovered (unaryplus, the
            // deferred unaryminus arms) keeps the LongLong placeholder.
            if let Some(ret_type) = crate::builtin_op::infer_unary_op_type(name, &arg) {
                return Ok(Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new(name),
                    ret_type,
                    vec![arg],
                )));
            }
            Ok(scalar(name, vec![arg]))
        }
        Expr::Binary(op, lhs, rhs) => {
            let left = rewrite_expr_resolved(lhs, resolver)?;
            let right = rewrite_expr_resolved(rhs, resolver)?;
            let name = binary_op_name(*op);
            // Result types come from the transcreated function classes:
            // builtin_arithmetic (plus/minus/mul/div/intdiv/mod),
            // builtin_compare (eq/nulleq/ne/lt/le/gt/ge) and builtin_op
            // (logic and bit operators). Anything still uncovered keeps the
            // LongLong placeholder.
            if let Some(ret_type) =
                crate::builtin_arithmetic::infer_arithmetic_type(name, &left, &right)
                    .or_else(|| crate::builtin_compare::infer_compare_type(name))
                    .or_else(|| crate::builtin_op::infer_op_type(name))
            {
                return Ok(Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new(name),
                    ret_type,
                    vec![left, right],
                )));
            }
            Ok(scalar(name, vec![left, right]))
        }
        // Go `expressionRewriter.betweenToExpression`: `x BETWEEN l AND h`
        // is `x >= l AND x <= h`, and the negated form is `x < l OR x > h` --
        // built from the comparison operators, so it inherits their types.
        Expr::Between {
            expr,
            low,
            high,
            not,
        } => {
            let value = rewrite_expr_resolved(expr, resolver)?;
            let low = rewrite_expr_resolved(low, resolver)?;
            let high = rewrite_expr_resolved(high, resolver)?;
            let (lower_op, upper_op, joiner) = if *not {
                (BinaryOp::Lt, BinaryOp::Gt, "or")
            } else {
                (BinaryOp::Ge, BinaryOp::Le, "and")
            };
            let compare = |op: BinaryOp, left: Expression, right: Expression| {
                let name = binary_op_name(op);
                let ret_type = crate::builtin_compare::infer_compare_type(name)
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
                Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new(name),
                    ret_type,
                    vec![left, right],
                ))
            };
            let lower = compare(lower_op, value.clone(), low);
            let upper = compare(upper_op, value, high);
            let mut ret_type = FieldType::new(FieldTypeCode::LongLong);
            ret_type.set_flen(1);
            Ok(Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(joiner),
                ret_type,
                vec![lower, upper],
            )))
        }
        // Go builds `like(expr, pattern, escape)`, whose third argument is
        // the escape byte as an integer; `NOT LIKE` wraps it in a unary NOT.
        Expr::Like {
            expr,
            pattern,
            not,
            ilike,
            escape,
        } => {
            let name = if *ilike { "ilike" } else { "like" };
            let args = vec![
                rewrite_expr_resolved(expr, resolver)?,
                rewrite_expr_resolved(pattern, resolver)?,
                // Go defaults the escape to `\\` when none was written.
                constant(
                    Datum::Int(i64::from(escape.unwrap_or(b'\\'))),
                    FieldTypeCode::LongLong,
                ),
            ];
            let ret_type =
                builtin_return_type(name, &args).expect("the like builtin has a fixed result type");
            let call = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(name),
                ret_type.clone(),
                args,
            ));
            if *not {
                return Ok(Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new(unary_op_name(UnaryOp::Not)),
                    ret_type,
                    vec![call],
                )));
            }
            Ok(call)
        }
        // Go `caseWhenFunctionClass`: the arguments are the flattened
        // `cond, result, cond, result, ..., else` list, and the simple form
        // (`CASE value WHEN ...`) becomes an equality per branch.
        Expr::Case {
            value,
            when_clauses,
            else_clause,
        } => {
            let compare_value = match value {
                Some(value) => Some(rewrite_expr_resolved(value, resolver)?),
                None => None,
            };
            let mut args = Vec::with_capacity(when_clauses.len() * 2 + 1);
            for (condition, result) in when_clauses {
                let condition = rewrite_expr_resolved(condition, resolver)?;
                let condition = match &compare_value {
                    Some(value) => {
                        let name = binary_op_name(BinaryOp::Eq);
                        let ret_type = crate::builtin_compare::infer_compare_type(name)
                            .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
                        Expression::ScalarFunction(ScalarFunction::new(
                            CiString::new(name),
                            ret_type,
                            vec![value.clone(), condition],
                        ))
                    }
                    None => condition,
                };
                args.push(condition);
                args.push(rewrite_expr_resolved(result, resolver)?);
            }
            if let Some(else_clause) = else_clause {
                args.push(rewrite_expr_resolved(else_clause, resolver)?);
            }
            // The result type comes from the branches, which are every other
            // argument plus the trailing ELSE.
            let branches: Vec<Expression> = args
                .iter()
                .skip(1)
                .step_by(2)
                .chain(if args.len() % 2 == 1 {
                    args.last()
                } else {
                    None
                })
                .cloned()
                .collect();
            let ret_type = builtin_return_type("case_when", &branches).ok_or(
                EvalError::Unsupported("a CASE whose branches have different types"),
            )?;
            Ok(Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("case_when"),
                ret_type,
                args,
            )))
        }
        // An ordinary builtin call: every argument is evaluated eagerly and
        // the shared `eval_func_values` implementation runs it.
        Expr::Func { name, args, .. } => {
            let lowered = name.to_ascii_lowercase();
            let rewritten: Vec<Expression> = args
                .iter()
                .map(|arg| rewrite_expr_resolved(arg, resolver))
                .collect::<Result<_, _>>()?;
            let ret_type = builtin_return_type(&lowered, &rewritten).ok_or(
                EvalError::Unsupported("this builtin is not yet built for chunk evaluation"),
            )?;
            Ok(Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(&lowered),
                ret_type,
                rewritten,
            )))
        }
        // Go builds one `builtinCast*As*Sig` per target type, so the cast
        // node becomes a one-argument function whose RESULT type carries the
        // target -- `CONVERT(x, t)` and `BINARY x` are the same node.
        Expr::Cast(cast) => {
            if cast.array {
                return Err(EvalError::Unsupported(
                    "a CAST with the ARRAY modifier is not supported yet",
                ));
            }
            let (name, ret_type) = cast_target(&cast.cast_type).ok_or(EvalError::Unsupported(
                "this CAST target type has no value domain yet",
            ))?;
            let arg = rewrite_expr_resolved(&cast.expr, resolver)?;
            Ok(Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(name),
                ret_type,
                vec![arg],
            )))
        }
        _ => Err(EvalError::Unsupported(
            "expression form is not yet supported by the rewriter",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::NoColumns;
    use tidb_ast::{BinaryOp, UnaryOp};
    use tidb_chunk::chunk::Chunk;

    // Evaluates a rewritten expression over an empty (column-less) row.
    fn eval_const(expr: &Expr) -> Datum {
        let rewritten = rewrite_expr(expr).unwrap();
        let chunk = Chunk::new_empty(&[]);
        // A column-less chunk still yields a virtual row for evaluation.
        let mut c = chunk;
        c.set_num_virtual_rows(1);
        rewritten.eval(&NoColumns, c.get_row(0)).unwrap()
    }

    /// `IS NULL` / `IS TRUE` / `IS FALSE` (and their `IS NOT` forms) return
    /// 0 or 1 and never NULL, which is what makes the `IS NOT` wrapping NOT
    /// exact. `IS UNKNOWN` is `IS NULL`.
    /// Go's `in` builtin is three-valued: a match is 1, no match with a NULL
    /// anywhere is NULL, otherwise 0. `NOT IN` is a unary NOT over it, so
    /// NULL stays NULL rather than becoming true.
    #[test]
    fn rewrite_and_eval_in_lists() {
        let int = |text: &str| Box::new(Expr::Int(text.to_owned()));
        let in_list = |expr: Box<Expr>, list: Vec<Expr>, not: bool| Expr::In { expr, list, not };

        // 2 IN (1, 2, 3) -> 1; 5 IN (1, 2) -> 0.
        assert_eq!(
            eval_const(&in_list(int("2"), vec![*int("1"), *int("2")], false)),
            Datum::Int(1)
        );
        assert_eq!(
            eval_const(&in_list(int("5"), vec![*int("1"), *int("2")], false)),
            Datum::Int(0)
        );
        // A NULL in the list turns a non-match into NULL, but not a match.
        assert_eq!(
            eval_const(&in_list(int("5"), vec![*int("1"), Expr::Null], false)),
            Datum::Null
        );
        assert_eq!(
            eval_const(&in_list(int("1"), vec![*int("1"), Expr::Null], false)),
            Datum::Int(1)
        );
        // A NULL tested value is always NULL.
        assert_eq!(
            eval_const(&in_list(Box::new(Expr::Null), vec![*int("1")], false)),
            Datum::Null
        );
        // NOT IN negates, and NULL stays NULL.
        assert_eq!(
            eval_const(&in_list(int("5"), vec![*int("1")], true)),
            Datum::Int(1)
        );
        assert_eq!(
            eval_const(&in_list(int("5"), vec![*int("1"), Expr::Null], true)),
            Datum::Null
        );
    }

    #[test]
    fn rewrite_and_eval_is_predicates() {
        let is = |expr: Expr, target: IsTarget, not: bool| Expr::Is {
            expr: Box::new(expr),
            target,
            not,
        };
        let null = || Expr::Null;
        let int = |text: &str| Expr::Int(text.to_owned());

        for (expr, want) in [
            (is(null(), IsTarget::Null, false), 1),
            (is(int("1"), IsTarget::Null, false), 0),
            (is(null(), IsTarget::Null, true), 0),
            (is(int("1"), IsTarget::Null, true), 1),
            (is(null(), IsTarget::Unknown, false), 1),
            (is(int("2"), IsTarget::True, false), 1),
            (is(int("0"), IsTarget::True, false), 0),
            (is(null(), IsTarget::True, false), 0),
            (is(int("0"), IsTarget::False, false), 1),
            (is(int("2"), IsTarget::False, false), 0),
            (is(null(), IsTarget::False, false), 0),
            // NULL is neither true nor false, so both IS NOT forms hold.
            (is(null(), IsTarget::True, true), 1),
            (is(null(), IsTarget::False, true), 1),
        ] {
            assert_eq!(eval_const(&expr), Datum::Int(want), "{expr:?}");
        }
    }

    #[test]
    fn rewrite_and_eval_arithmetic() {
        // 1 + 1
        let one = || Box::new(Expr::Int("1".to_owned()));
        let plus = Expr::Binary(BinaryOp::Plus, one(), one());
        assert_eq!(eval_const(&plus), Datum::Int(2));

        // 2 * 3 - 1  ==  (2*3) - 1  == 5
        let two = Box::new(Expr::Int("2".to_owned()));
        let three = Box::new(Expr::Int("3".to_owned()));
        let mul = Box::new(Expr::Binary(BinaryOp::Mul, two, three));
        let minus = Expr::Binary(BinaryOp::Minus, mul, one());
        assert_eq!(eval_const(&minus), Datum::Int(5));

        // -(1 + 1) == -2, through a paren
        let paren = Box::new(Expr::Paren(Box::new(Expr::Binary(
            BinaryOp::Plus,
            one(),
            one(),
        ))));
        let neg = Expr::Unary(UnaryOp::Minus, paren);
        assert_eq!(eval_const(&neg), Datum::Int(-2));
    }

    #[test]
    fn rewrite_literals() {
        assert_eq!(eval_const(&Expr::Null), Datum::Null);
        assert_eq!(eval_const(&Expr::Bool(true)), Datum::Int(1));
        match eval_const(&Expr::Float(1.5)) {
            Datum::Real(f) => assert_eq!(f, 1.5),
            other => panic!("expected real, got {other:?}"),
        }
    }

    #[test]
    fn rewrite_infers_compare_and_op_ret_types() {
        use tidb_datatype::{FieldTypeCode, FieldTypeFlags};

        let one = || Box::new(Expr::Int("1".to_owned()));
        let two = || Box::new(Expr::Int("2".to_owned()));

        // 1 < 2: comparison ret type is LongLong with flen 1 (boolean).
        let lt = rewrite_expr(&Expr::Binary(BinaryOp::Lt, one(), two())).unwrap();
        let Expression::ScalarFunction(f) = &lt else {
            panic!("expected a scalar function");
        };
        let ret = f.ret_type.as_ref().unwrap();
        assert_eq!(ret.code(), FieldTypeCode::LongLong);
        assert_eq!(ret.flen(), 1);
        assert_ne!(ret.flags() & FieldTypeFlags::IS_BOOLEAN, 0);

        // 1 AND 2: logic ret type is also flen 1.
        let and = rewrite_expr(&Expr::Binary(BinaryOp::LogicAnd, one(), two())).unwrap();
        let Expression::ScalarFunction(f) = &and else {
            panic!("expected a scalar function");
        };
        assert_eq!(f.ret_type.as_ref().unwrap().flen(), 1);

        // 1 & 2: bit ops are unsigned LongLong.
        let band = rewrite_expr(&Expr::Binary(BinaryOp::BitAnd, one(), two())).unwrap();
        let Expression::ScalarFunction(f) = &band else {
            panic!("expected a scalar function");
        };
        assert!(f.ret_type.as_ref().unwrap().is_unsigned());

        // NOT 1: flen 1; ~1: unsigned.
        let not = rewrite_expr(&Expr::Unary(UnaryOp::Not, one())).unwrap();
        let Expression::ScalarFunction(f) = &not else {
            panic!("expected a scalar function");
        };
        assert_eq!(f.ret_type.as_ref().unwrap().flen(), 1);

        let neg = rewrite_expr(&Expr::Unary(UnaryOp::BitNeg, one())).unwrap();
        let Expression::ScalarFunction(f) = &neg else {
            panic!("expected a scalar function");
        };
        assert!(f.ret_type.as_ref().unwrap().is_unsigned());
    }

    #[test]
    fn unsupported_form_errors() {
        // A column reference is not yet handled.
        let col = Expr::Column(vec!["a".to_owned()]);
        assert!(rewrite_expr(&col).is_err());
    }
}

#[cfg(test)]
mod literal_tests {
    use super::*;

    fn rewrite(sql_expr: &str) -> Expression {
        let stmt = tidb_parser::parse(&format!("SELECT {sql_expr}")).expect("parses");
        let tidb_ast::Stmt::Query(query) = stmt else {
            panic!("expected a query")
        };
        let tidb_ast::QueryStmt::Select(select) = &*query else {
            panic!("expected a SELECT")
        };
        let tidb_ast::SelectField::Expr { expr, .. } = &select.fields.fields()[0] else {
            panic!("expected an expression field")
        };
        rewrite_expr_resolved(expr, &NoResolver).expect("rewrites")
    }

    fn constant_of(expr: &Expression) -> (&Datum, &FieldType) {
        let Expression::Constant(constant) = expr else {
            panic!("expected a constant, got {expr:?}")
        };
        (
            &constant.value,
            constant.ret_type.as_ref().expect("a literal has a type"),
        )
    }

    /// Captured from TiDB (`SELECT 1.5` etc., reading the result field's own
    /// type/flen/decimal/flag): a decimal literal is a `NewDecimal` whose
    /// flen is the printed length plus one, with the binary charset and the
    /// not-null flag.
    #[test]
    fn decimal_literal_type_matches_tidb() {
        for (text, flen, decimal, printed) in [
            ("1.5", 4, 1, "1.5"),
            ("0.10", 5, 2, "0.10"),
            ("2.750", 6, 3, "2.750"),
        ] {
            let expr = rewrite(text);
            let (value, ft) = constant_of(&expr);
            assert_eq!(ft.code(), FieldTypeCode::NewDecimal, "{text}");
            assert_eq!(ft.flen(), flen, "{text} flen");
            assert_eq!(ft.decimal(), decimal, "{text} decimal");
            assert_eq!(ft.charset_name(), "binary", "{text} charset");
            assert!(ft.flags() & tidb_datatype::FieldTypeFlags::NOT_NULL != 0);
            assert!(ft.flags() & tidb_datatype::FieldTypeFlags::BINARY != 0);
            let Datum::Decimal(value) = value else {
                panic!("expected a decimal datum for {text}")
            };
            assert_eq!(value.to_string(), printed, "{text} value");
        }
    }

    /// Captured from TiDB: `0x41` and `x'4142'` are unsigned binary
    /// `VarString`s three bytes wide per literal byte, printing as the bytes
    /// themselves; `b'1010'` is the same but signed.
    #[test]
    fn binary_literal_types_match_tidb() {
        for (text, bytes, flen, unsigned) in [
            ("0x41", &b"A"[..], 3, true),
            ("x'4142'", &b"AB"[..], 6, true),
            ("b'1010'", &b"\n"[..], 3, false),
        ] {
            let expr = rewrite(text);
            let (value, ft) = constant_of(&expr);
            assert_eq!(ft.code(), FieldTypeCode::VarString, "{text}");
            assert_eq!(ft.flen(), flen, "{text} flen");
            assert_eq!(ft.decimal(), 0, "{text} decimal");
            assert_eq!(
                ft.flags() & tidb_datatype::FieldTypeFlags::UNSIGNED != 0,
                unsigned,
                "{text} unsigned"
            );
            let Datum::BinaryLiteral(literal) = value else {
                panic!("expected a binary literal datum for {text}")
            };
            assert_eq!(literal.as_bytes(), bytes, "{text} bytes");
        }
    }
}
