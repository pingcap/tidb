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

/// Go `mysql.DefaultDecimal` (`parser/mysql/const.go`): the value a decimal
/// literal too large for a `MyDecimal` saturates to.
const DEFAULT_DECIMAL_LITERAL: &str =
    "99999999999999999999999999999999999999999999999999999999999999999";

pub(crate) mod control_type;
pub(crate) mod result_type;

pub use control_type::{infer_type4_control_funcs, set_numeric_len_from_args};
pub use result_type::go_result_type_code;
use result_type::{
    binary_literal_type, builtin_return_type, decimal_literal_type, int_literal_type,
    returns_binary_string, set_binary_charset,
};

/// Resolves a dotted column path to an output column, standing in for the
/// schema/name resolution Go's `expression_rewriter` performs against the
/// plan's schema (`resolveColumn`).
pub trait ColumnResolver {
    /// Resolves `path` (e.g. `["t", "a"]` or `["a"]`) to
    /// `(row index, result type, unique id)`, or `None` when unknown.
    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)>;

    /// Resolves a `DEFAULT(column)` leaf to the statement-scoped constant its
    /// DML planner prepared. A default implementation keeps ordinary query
    /// resolvers unaware of write-only metadata; write resolvers override it
    /// after materializing defaults in Go's planning order.
    fn resolve_default(&self, _path: &[String]) -> Option<Expression> {
        None
    }

    /// The session `time_zone` the rewrite runs under -- Go's
    /// `ctx.Location()`, which `getFunction` reaches while BUILDING the
    /// expression and which the `TIMESTAMP 'lit'` fold both normalizes an
    /// explicit `+HH:MM` offset into and applies a fractional-second carry
    /// in (see `crate::time_literal`).
    ///
    /// There is NO default on purpose: Go has no session-less rewrite, so
    /// every implementor must decide what zone its callers' statements run
    /// in rather than silently inheriting a hardcode -- which is exactly the
    /// dropped-Context bug this accessor closes.
    fn time_zone(&self) -> tidb_datatype::SessionTimeZone;
}

/// A resolver that knows no columns but folds in a REAL session zone: what
/// [`NoResolver`] should be wherever the caller has a statement context to
/// take the zone from. `resolve` answering `None` is the constant test the
/// callers rely on; only the zone differs.
pub struct ZonedNoResolver(pub tidb_datatype::SessionTimeZone);

impl ColumnResolver for ZonedNoResolver {
    fn resolve(&self, _path: &[String]) -> Option<(usize, FieldType, i64)> {
        None
    }

    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        self.0.clone()
    }
}

/// A resolver that knows no columns (for constant-only expressions).
pub struct NoResolver;

impl ColumnResolver for NoResolver {
    fn resolve(&self, _path: &[String]) -> Option<(usize, FieldType, i64)> {
        None
    }

    /// UTC: the zone the pre-accessor code hardcoded, kept so the
    /// constant-only callers (DDL partition bounds, column defaults, index
    /// ranges) answer exactly what they answered before. Each such site that
    /// Go runs under a real session should migrate to a session-aware
    /// resolver; until then this is a named, greppable stand-in rather than
    /// a buried literal.
    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        tidb_datatype::SessionTimeZone::utc()
    }
}

/// The function name and result type a `CAST(expr AS type)` becomes.
///
/// Go picks one `builtinCast*As*Sig` per target type; the name here carries
/// that choice, so evaluation never has to re-derive the target from a result
/// type that may not describe it. JSON retains its native domain; public
/// DATE/DATETIME results remain strings until the differential protocol owns
/// native temporal cells.
///
/// The `ARRAY` modifier is outside this AST's cast surface. `TIME` remains
/// refused; JSON retains its native result domain.
/// The literal text a typed temporal literal wraps.
///
/// Go's `getFunction` asserts the argument is a `*Constant` and PANICS
/// otherwise, because its parser only ever puts a string literal there. The
/// ODBC spelling `{ts <expr>}` accepts a full expression, so the argument is
/// rewritten and folded first; anything that does not fold to a constant is a
/// boundary this tier reports instead of crashing on.
fn literal_text(expr: &Expr, resolver: &impl ColumnResolver) -> Result<String, EvalError> {
    let built = rewrite_expr_resolved(expr, resolver)?;
    let Expression::Constant(constant) = built else {
        return Err(EvalError::Unsupported(
            "a temporal literal whose argument is not constant",
        ));
    };
    constant
        .value
        .sql_string()
        .map_err(|_| EvalError::Unsupported("invalid UTF-8 in a temporal literal"))
}

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
        CastType::Json => "cast_json",
        CastType::Time { .. } | CastType::Vector { .. } => return None,
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
            set_binary_charset(&mut ft);
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
        CastType::Date => FieldType::new(FieldTypeCode::VarString),
        CastType::DateTime { fsp } => {
            let decimal = i64::from(fsp.unwrap_or(0));
            let mut ft = FieldType::new(FieldTypeCode::VarString);
            ft.set_flen(if decimal > 0 { 20 + decimal } else { 19 });
            ft.set_decimal(decimal);
            ft
        }
        // Likewise, the year cast yields an integer value here.
        CastType::Year => FieldType::new(FieldTypeCode::LongLong),
        CastType::Double | CastType::Float => FieldType::new(FieldTypeCode::Double),
        CastType::Json => FieldType::new(FieldTypeCode::Json),
        CastType::Time { .. } | CastType::Vector { .. } => return None,
    };
    Some((name, ft))
}

pub(crate) fn builtin_cast_lockdown_result_type_anchor(
    cast_type: &tidb_ast::CastType,
) -> Option<(&'static str, FieldType)> {
    cast_target(cast_type)
}

/// A string literal constant, used where Go's builder supplies a default
/// argument (`TRIM`'s implicit space).
/// A SQL character-string literal.
///
/// The datum is a COLLATION-TAGGED string, never a `Datum::Bytes`: Go's
/// `ast.NewValueExpr` stamps a string literal with the connection charset, and
/// `Datum.ConvertTo` reads that collation to decide whether a write into a
/// column DECODES from binary or merely VALIDATES against the column's
/// charset. A binary-tagged literal takes the decode branch, which inverts the
/// non-UTF-8 write path -- a `CHARSET gbk` column would then accept `😉`
/// (whose UTF-8 bytes happen to form legal GBK pairs) and reject `一列`.
fn constant_string(text: &str) -> Expression {
    let mut field_type = FieldType::new(FieldTypeCode::VarString);
    // Go `types.DefaultTypeForValue`'s `case string` (`pkg/types/field_type.go`):
    // `tp.SetFlen(len(x))`, the literal's BYTE length -- Go's own TODO there
    // says it should arguably be three times that, and it is not. Without it a
    // literal is unsized, and an unsized argument is what makes
    // `CONCAT('T','i','DB')` report `MaxBlobWidth - 1` instead of Go's 4.
    field_type.set_flen(text.len() as i64);
    let datum = Datum::new_collation_string(text.as_bytes().to_vec(), field_type.collation());
    Expression::Constant(Constant::new(datum, field_type))
}

/// Go `HandleBinaryLiteral`, applied where Go applies it: as each builtin's
/// arguments are built.
///
/// A non-legacy-charset argument (in practice `gbk` or `gb18030`) of a
/// binary-aware function, or of any function whose result is the binary
/// charset, is wrapped with the implicit `to_binary` call that performs the
/// UTF-8 -> charset transcode. See `crate::convert_charset` for why that is
/// the only place the bytes ever change.
fn wrap_binary_literals(
    name: &str,
    result_charset: &str,
    args: Vec<Expression>,
) -> Vec<Expression> {
    let prop = crate::convert_charset::func_prop(name);
    if prop == crate::convert_charset::FuncProp::None {
        return args;
    }
    args.into_iter()
        .map(|arg| {
            let Some(arg_type) = arg.static_type() else {
                return arg;
            };
            if !crate::convert_charset::needs_to_binary(
                prop,
                arg_type.charset_name(),
                result_charset,
            ) {
                return arg;
            }
            let mut ret_type = FieldType::new(FieldTypeCode::VarString);
            set_binary_charset(&mut ret_type);
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("to_binary"),
                ret_type,
                vec![arg],
            ))
        })
        .collect()
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
    if let Expr::Default(Some(path)) = expr {
        return resolver
            .resolve_default(path)
            .ok_or(EvalError::Unsupported("unresolved DEFAULT column"));
    }
    if let Expr::Column(path) = expr {
        let (index, ret_type, unique_id) = resolver
            .resolve(path)
            .ok_or(EvalError::Unsupported("unresolved column reference"))?;
        let mut col = Column::new(unique_id, ret_type);
        col.index = index as i64;
        return Ok(Expression::Column(col));
    }
    let mut built = rewrite_leaf(expr, resolver)?;
    derive_tree_collation(&mut built)?;
    crate::constant_fold::derive_constant_null_flag(&mut built);
    Ok(built)
}

/// Runs Go's collation derivation over a freshly built expression tree,
/// bottom up (`pkg/expression/collation.go` `deriveCollation`, applied by
/// `newBaseBuiltinFuncWithTp` as each function is constructed).
///
/// Go derives while BUILDING, which is naturally bottom-up; this rewriter
/// builds several nested functions inside a single arm (`NOT IN` is a `not`
/// over an `in`, `NOT LIKE` a `not` over a `like`), so the derivation is a
/// walk instead of a per-construction-site call. A node that already carries a
/// derived coercibility is left alone, which makes the walk idempotent -- the
/// recursion in [`rewrite_expr_resolved`] runs it once per level, and only the
/// first visit does work.
pub fn derive_tree_collation(expr: &mut Expression) -> Result<(), EvalError> {
    let Expression::ScalarFunction(func) = expr else {
        return Ok(());
    };
    for arg in &mut func.args {
        derive_tree_collation(arg)?;
    }
    if func.collation.has_coercibility() {
        return Ok(());
    }
    let ret_type = func
        .ret_type
        .as_ref()
        .map_or(tidb_datatype::EvalType::Int, FieldType::eval_type);
    let name = func.func_name.lowercase().to_owned();
    let ec = crate::collation_derive::derive_collation(&name, &func.args, ret_type)?;
    crate::collation_derive::apply_derived_collation(expr, &ec);
    // The builtins whose `getFunction` calls `types.SetBinChsClnFlag(bf.tp)`
    // AFTER `newBaseBuiltinFuncWithTp` has built `bf.tp` from the generically
    // derived collation -- so the forced binary charset/collation is always
    // the LAST word for them, regardless of what `deriveCollation`'s generic
    // (non-string-in-string-out) default arm computed from the connection
    // charset. `derive_tree_collation` runs that generic derivation as a
    // bottom-up walk over an already-built tree, i.e. strictly after
    // `builtin_return_type` gave these names their forced binary type, so
    // without this re-assertion the generic pass would silently overwrite it
    // back to a character type -- and a `VARBINARY` result reported as
    // `utf8mb4` makes `CHAR_LENGTH` count characters where TiDB counts bytes.
    if let Expression::ScalarFunction(func) = expr {
        if returns_binary_string(func.func_name.lowercase()) {
            if let Some(ft) = func.ret_type.as_mut() {
                set_binary_charset(ft);
            }
        }
    }
    Ok(())
}

fn rewrite_leaf(expr: &Expr, resolver: &impl ColumnResolver) -> Result<Expression, EvalError> {
    match expr {
        // Go's `ast.NewValueExpr` hands the scanned literal to
        // `types.NewDatum`, whose int64/uint64 split puts a literal above
        // `math.MaxInt64` in `KindUint64` -- the signedness lives in the datum
        // kind, so every consumer reads it from the one value rather than from
        // a parallel flag. `Datum::UInt` is that kind here. A literal wider
        // than u64 never reaches this arm: the lexer already turns it into a
        // `DecLit` (`toInt` -> `toDecimal` on `strconv.ErrRange`).
        Expr::Int(text) => {
            let (datum, unsigned, printed_len) = match text.parse::<i64>() {
                Ok(value) => (Datum::Int(value), false, value.to_string().len()),
                Err(_) => {
                    let value: u64 = text.parse().map_err(|_| {
                        EvalError::Unsupported("integer literal outside the u64 domain")
                    })?;
                    (Datum::UInt(value), true, value.to_string().len())
                }
            };
            Ok(Expression::Constant(Constant::new(
                datum,
                int_literal_type(printed_len, unsigned),
            )))
        }
        Expr::Float(value) => Ok(constant(Datum::Real(*value), FieldTypeCode::Double)),
        // A `TRUE`/`FALSE` literal is `0`/`1` typed `bigint(1)` with
        // `IsBooleanFlag` (Go `DefaultTypeForValue` -> `KindMysqlBool`), so
        // `JSON_ARRAY(true)` is `[true]`, not `[1]`.
        Expr::Bool(value) => {
            let mut ft = FieldType::new(FieldTypeCode::LongLong);
            ft.set_flen(1);
            ft.add_flags(tidb_datatype::FieldTypeFlags::IS_BOOLEAN);
            Ok(Expression::Constant(Constant::new(
                Datum::Int(i64::from(*value)),
                ft,
            )))
        }
        Expr::Null => Ok(constant(Datum::Null, FieldTypeCode::Null)),
        Expr::String(text) => Ok(constant_string(text)),
        // Go's parser folds a decimal literal into a `*MyDecimal` value whose
        // type `DefaultTypeForValue` derives from the printed literal.
        //
        // `parse_mysql` is `MyDecimal.FromString`, which is exactly what
        // `ast.NewDecimal` (`types/parser_driver/value_expr.go`) calls, and
        // the distinction from `from_literal` is load-bearing: `FromString`
        // honours the FIXED WORD BUFFER, so a literal with more fraction
        // digits than a `MyDecimal` can hold comes back TRUNCATED with
        // `ErrTruncated`, which Go swallows. `from_literal` kept all 91
        // digits of `expression/issues`'s
        // `select 0.000...0`, and the chunk cell that value was appended to
        // holds 30 -- an unrepresentable value reaching a fixed cell, which
        // panicked. TiDB records that statement's value as 72 fraction
        // digits, which is what the word buffer leaves for a zero integer
        // part.
        Expr::Decimal(text) => {
            let (value, err) = tidb_datatype::Decimal::parse_mysql(text);
            // Go's other disposition: a value the buffer cannot hold AT ALL
            // (`ErrDataOutOfRange`) becomes `mysql.DefaultDecimal`, the
            // 65-nine saturation value, rather than the partial parse.
            let value = if matches!(err, Some(tidb_datatype::DecimalParseError::Overflow)) {
                tidb_datatype::Decimal::parse_mysql(DEFAULT_DECIMAL_LITERAL).0
            } else {
                value
            };
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
        Expr::Bit(value) => {
            let literal = tidb_datatype::BinaryLiteral::from(value.as_bytes());
            let ft = binary_literal_type(value.as_bytes().len(), false);
            Ok(Expression::Constant(Constant::new(
                Datum::BinaryLiteral(literal),
                ft,
            )))
        }
        // Go `parseCharsetIntroducer` retains the binary literal Datum and
        // annotates only its FieldType. Keeping the wrapper at this boundary
        // lets DDL constant evaluation remain byte-authoritative while live
        // expression consumers still see the explicit charset/collation and
        // UnderScoreCharset/Binary flags.
        Expr::CharsetBinary { charset, value } => {
            let mut rewritten = rewrite_leaf(value, resolver)?;
            let lower = charset.to_ascii_lowercase();
            let collation = tidb_datatype::get_default_collation(&lower)
                .map_err(|_| EvalError::Unsupported("unknown character introducer"))?;
            let Expression::Constant(constant) = &mut rewritten else {
                return Err(EvalError::Unsupported(
                    "character introducer requires a literal",
                ));
            };
            let field_type = constant.ret_type.as_mut().ok_or(EvalError::Unsupported(
                "character-introduced literal has no type",
            ))?;
            field_type.set_charset_name(lower);
            field_type.set_collation_name(&collation);
            field_type.add_flags(tidb_datatype::FieldTypeFlags::UNDERSCORE_CHARSET);
            if collation == "binary" {
                field_type.add_flags(tidb_datatype::FieldTypeFlags::BINARY);
            }
            Ok(rewritten)
        }
        // The inline `@name := expr` assignment expression: Go's
        // `builtinSetVar*Sig`, whose whole point is the SIDE EFFECT on the
        // session, performed once per row. The name is a build-time token, so
        // it rides as a constant argument, and the result type comes from the
        // value -- which is also the type the assignment stores.
        //
        // A bare `@name` READ has no arm here on purpose: its result type is
        // the type of the value the session currently holds (Go picks one of
        // its typed `GetVar` signatures from `GetUserVarType` at build time),
        // and the session encodes that choice in the function NAME before the
        // rewriter runs -- see `getvar_*` in [`builtin_return_type`].
        Expr::Assign { name, value } => {
            let args = vec![
                constant_string(name),
                rewrite_expr_resolved(value, resolver)?,
            ];
            let ret_type = builtin_return_type("setvar", &args)
                .ok_or(EvalError::Unsupported("setvar has no result type"))?;
            Ok(Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("setvar"),
                ret_type,
                args,
            )))
        }
        Expr::Paren(inner) => rewrite_expr_resolved(inner, resolver),
        // Go `expression_rewriter`'s `case *ast.SetCollationExpr`: the
        // collation is written onto the argument's own result type and its
        // coercibility is raised to EXPLICIT, so it outranks every column and
        // literal in the enclosing aggregation. A collation that does not
        // belong to the value's charset is error 1253, which is what makes
        // `SELECT 'a' COLLATE latin1_bin` fail (a bare literal is utf8mb4).
        Expr::Collate { expr, collation } => {
            let mut arg = rewrite_expr_resolved(expr, resolver)?;
            let name = collation.to_ascii_lowercase();
            let Some(collation) = tidb_datatype::Collation::from_name(&name) else {
                return Err(EvalError::UnknownCollation(name));
            };
            let arg_charset = arg
                .static_type()
                .map_or(String::new(), |ft| ft.charset_name().to_owned());
            if !crate::collation_derive::collation_matches_charset(collation, &arg_charset) {
                return Err(EvalError::CollationCharsetMismatch {
                    collation: name,
                    charset: arg_charset,
                });
            }
            crate::collation_derive::set_explicit_collation(&mut arg, collation);
            Ok(arg)
        }
        // A charset introducer (`_binary'a'`, `_latin1'x'`): Go's parser gives
        // the literal that charset and its default collation. `_binary` is the
        // one this tier's value domain can represent exactly -- a byte string,
        // NO PAD -- and it is also the only introducer with SQL-visible
        // comparison semantics of its own here.
        Expr::CharsetString { charset, value } if charset.eq_ignore_ascii_case("binary") => {
            let mut datum = Datum::Null;
            datum.set_bytes(value.clone().into_bytes());
            let mut ft = FieldType::new(FieldTypeCode::VarString);
            set_binary_charset(&mut ft);
            Ok(Expression::Constant(Constant::new(datum, ft)))
        }
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
            // `ast.In` is in Go's `booleanFunctions` map, so the result carries
            // `IsBooleanFlag`; a `JSON_ARRAY(x IN (...))` element is JSON
            // `true`/`false`, not `1`/`0`. The `NOT IN` wrapper reuses this type.
            ret_type.add_flags(tidb_datatype::FieldTypeFlags::IS_BOOLEAN);
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
            // Go's result is a one-digit integer (`flen` 1, boolean-flagged):
            // `ast.IsNull`, `ast.IsTruthWithNull` and `ast.IsFalsity` are all in
            // the `booleanFunctions` map, so a `JSON_ARRAY(x IS NULL)` element is
            // JSON `true`/`false`.
            let mut ret_type = FieldType::new(FieldTypeCode::LongLong);
            ret_type.set_flen(1);
            ret_type.add_flags(tidb_datatype::FieldTypeFlags::IS_BOOLEAN);
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
            // Go `unaryOpToExpression`: `case opcode.Plus: return` -- the
            // expression `(+ a)` IS `a`, so no function is built at all. That
            // is also the only reason `+ a` needs no return-type rule: there
            // is nothing whose type could disagree with `a`'s.
            if matches!(op, UnaryOp::Plus) {
                return Ok(arg);
            }
            let name = unary_op_name(*op);
            // not/bitneg/unaryminus result types come from the transcreated
            // builtin_op function classes; anything uncovered (the deferred
            // unaryminus arms) keeps the LongLong placeholder.
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
            // The joining `AND`/`OR` is a `booleanFunctions` name, so the whole
            // `BETWEEN` result is boolean-flagged: `JSON_ARRAY(x BETWEEN l AND h)`
            // is `[true]`/`[false]`, not `[1]`/`[0]`.
            let mut ret_type = FieldType::new(FieldTypeCode::LongLong);
            ret_type.set_flen(1);
            ret_type.add_flags(tidb_datatype::FieldTypeFlags::IS_BOOLEAN);
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
        // Go builds `regexp(expr, pattern)`; `NOT REGEXP`/`NOT RLIKE` wraps
        // it in a unary NOT, the same shape `Expr::Like` above builds.
        Expr::Regexp { expr, pattern, not } => {
            let args = vec![
                rewrite_expr_resolved(expr, resolver)?,
                rewrite_expr_resolved(pattern, resolver)?,
            ];
            let ret_type = builtin_return_type("regexp", &args)
                .expect("the regexp builtin has a fixed result type");
            let call = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("regexp"),
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
        // `EXTRACT(unit FROM value)` is sugar for the SAME single-argument
        // function `unit` already names, exactly as the AST evaluator treats
        // it (see `crate::eval_in`'s own `Expr::Extract` arm) — so it is
        // rewritten into that builtin call and needs no chunk machinery of
        // its own. This includes a composite unit (`HOUR_MINUTE`,
        // `DAY_SECOND`, ...): `time_fn::dispatch` names a function for those
        // too (`time_fn::calendar::extract_composite`), and
        // `builtin_return_type` below types them the same `int()` as every
        // other EXTRACT unit.
        Expr::Extract { unit, value } => rewrite_expr_resolved(
            &Expr::Func {
                name: unit.clone(),
                args: vec![(**value).clone()],
                origin_position: 0,
            },
            resolver,
        ),
        // `TIMESTAMPDIFF(unit, a, b)`'s unit is a dedicated AST field rather
        // than an argument expression (see `tidb_ast::Expr::TimestampDiff`),
        // but the shared implementation `time_fn::dispatch` already takes the
        // unit as its first VALUE — so the unit becomes a constant argument
        // and the one implementation runs unchanged.
        // `TIMESTAMPADD(unit, n, datetime)` is the same shape as
        // `TIMESTAMPDIFF` below: the unit is a dedicated AST field, and
        // `builtinTimestampAddSig.evalString` reads it as its first VALUE
        // (`b.args[0].EvalString`), so a constant argument reproduces Go's
        // own argument list exactly.
        Expr::TimestampAdd {
            unit,
            interval,
            expr,
        } => {
            let args = vec![
                constant_string(unit),
                rewrite_expr_resolved(interval, resolver)?,
                rewrite_expr_resolved(expr, resolver)?,
            ];
            let ret_type = builtin_return_type("timestampadd", &args).ok_or(
                EvalError::Unsupported("this builtin is not yet built for chunk evaluation"),
            )?;
            Ok(Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("timestampadd"),
                ret_type,
                args,
            )))
        }
        Expr::TimestampDiff { unit, expr1, expr2 } => {
            let args = vec![
                constant_string(unit),
                rewrite_expr_resolved(expr1, resolver)?,
                rewrite_expr_resolved(expr2, resolver)?,
            ];
            Ok(Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("timestampdiff"),
                FieldType::new(FieldTypeCode::LongLong),
                args,
            )))
        }
        // An ordinary builtin call: every argument is evaluated eagerly and
        // the shared `eval_func_values` implementation runs it.
        Expr::Func { name, args, .. } => {
            let lowered = name.to_ascii_lowercase();
            // `NEXTVAL(s)` / `LASTVAL(s)` / `SETVAL(s, n)` name a SEQUENCE, but
            // the grammar has no place for a table name inside an expression,
            // so the parser produces a COLUMN reference. Go's expression
            // rewriter special-cases exactly these three
            // (`pkg/planner/core/expression_rewriter.go` -> `ast.NextVal` and
            // friends) and reinterprets the reference as a name path; without
            // that, `select nextval(seq)` fails resolving a column called
            // `seq`. The path travels as a string constant so the evaluated
            // node needs no resolver of its own.
            if matches!(lowered.as_str(), "nextval" | "lastval" | "setval") {
                if let Some(Expr::Column(path)) = args.first() {
                    let mut rewritten = vec![Expression::Constant(crate::constant::Constant::new(
                        Datum::Bytes(
                            path.join(&crate::func::SEQUENCE_PATH_SEPARATOR.to_string())
                                .into_bytes(),
                        ),
                        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString),
                    ))];
                    for arg in &args[1..] {
                        rewritten.push(rewrite_expr_resolved(arg, resolver)?);
                    }
                    let ret_type =
                        builtin_return_type(&lowered, &rewritten).ok_or(EvalError::Unsupported(
                            "this builtin is not yet built for chunk evaluation",
                        ))?;
                    return Ok(Expression::ScalarFunction(ScalarFunction::new(
                        CiString::new(&lowered),
                        ret_type,
                        rewritten,
                    )));
                }
            }
            // The `DATE_ADD` family's second argument is an `Expr::Interval`
            // — a value AND a unit keyword — not an expression the generic
            // argument loop below can rewrite. The unit is a build-time
            // choice exactly like a cast's target type, so it travels in the
            // FUNCTION NAME (`date_add_month`) and the node keeps two
            // ordinary child expressions; `ScalarFunction::eval` then calls
            // the same `time_fn::calendar::date_add` the row path uses.
            // `ADDDATE`/`SUBDATE` are the same shape and the same evaluation
            // (the parser already normalized their bare-number form to an
            // `INTERVAL n DAY`), so they map onto the same two names.
            if matches!(
                lowered.as_str(),
                "date_add" | "date_sub" | "adddate" | "subdate"
            ) {
                if let [date, Expr::Interval { value, unit }] = args.as_slice() {
                    let subtract = lowered == "date_sub" || lowered == "subdate";
                    let unit = unit.to_ascii_uppercase();
                    // Only the units `date_add` itself implements are built;
                    // `QUARTER` still parses but is refused here rather than
                    // deferred to a runtime error. The composite units
                    // (`HOUR_MINUTE`, `DAY_SECOND`, `YEAR_MONTH`, ...) ARE
                    // built — `time_fn::calendar::date_add` handles them via
                    // `composite_spec`.
                    if !matches!(
                        unit.as_str(),
                        "DAY"
                            | "WEEK"
                            | "MONTH"
                            | "YEAR"
                            | "HOUR"
                            | "MINUTE"
                            | "SECOND"
                            | "YEAR_MONTH"
                            | "DAY_HOUR"
                            | "DAY_MINUTE"
                            | "DAY_SECOND"
                            | "DAY_MICROSECOND"
                            | "HOUR_MINUTE"
                            | "HOUR_SECOND"
                            | "HOUR_MICROSECOND"
                            | "MINUTE_SECOND"
                            | "MINUTE_MICROSECOND"
                            | "SECOND_MICROSECOND"
                    ) {
                        return Err(EvalError::Unsupported(
                            "this INTERVAL unit is not yet built for chunk evaluation",
                        ));
                    }
                    let name = format!(
                        "{}_{}",
                        if subtract { "date_sub" } else { "date_add" },
                        unit.to_ascii_lowercase()
                    );
                    let args = vec![
                        rewrite_expr_resolved(date, resolver)?,
                        rewrite_expr_resolved(value, resolver)?,
                    ];
                    let ret_type =
                        builtin_return_type(&name, &args).ok_or(EvalError::Unsupported(
                            "this builtin is not yet built for chunk evaluation",
                        ))?;
                    return Ok(Expression::ScalarFunction(ScalarFunction::new(
                        CiString::new(&name),
                        ret_type,
                        args,
                    )));
                }
            }
            let rewritten: Vec<Expression> = args
                .iter()
                .map(|arg| rewrite_expr_resolved(arg, resolver))
                .collect::<Result<_, _>>()?;
            let ret_type = builtin_return_type(&lowered, &rewritten).ok_or(
                EvalError::Unsupported("this builtin is not yet built for chunk evaluation"),
            )?;
            let rewritten = wrap_binary_literals(&lowered, ret_type.charset_name(), rewritten);
            Ok(Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(&lowered),
                ret_type,
                rewritten,
            )))
        }
        // Go builds one `builtinCast*As*Sig` per target type, so the cast
        // node becomes a one-argument function whose RESULT type carries the
        // target -- `CONVERT(x, t)` and `BINARY x` are the same node.
        // `DATE 'lit'` / `TIMESTAMP 'lit'` (and `{d ...}` / `{ts ...}`) share
        // the `Cast` node with `CAST(x AS DATE)` but are a DIFFERENT function
        // class in Go, with their own regex gate, their own hard errors and
        // their own fractional precision. Folding them here -- where Go also
        // resolves them, in `getFunction` -- is what keeps them from being
        // lowered into the never-failing, fraction-dropping cast below.
        // See `crate::time_literal`.
        Expr::Cast(cast)
            if matches!(
                cast.style,
                tidb_ast::CastStyle::DateLiteral | tidb_ast::CastStyle::TimestampLiteral
            ) =>
        {
            let text = literal_text(&cast.expr, resolver)?;
            let zone = resolver.time_zone();
            let (time, ret_type) = match cast.style {
                tidb_ast::CastStyle::DateLiteral => {
                    crate::time_literal::date_literal(&text, &zone)?
                }
                _ => crate::time_literal::timestamp_literal(&text, &zone)?,
            };
            // The folded constant carries Go's OWN result type -- `TypeDate`
            // or `TypeDatetime` with the literal's fsp -- over the same
            // `Datum::Time` cell a `date`/`datetime` COLUMN reads out of a
            // chunk row (`tidb_chunk::row::Row::get_datum`). It used to fold
            // to a `VarString`, which is why every consumer that asks Go's
            // question "is any argument temporal?" answered no for a literal
            // and yes for a column: `GREATEST(date 'lit', 19910101, ...)`
            // compared as TEXT and printed `20050505` where TiDB prints
            // `2005-05-05`, and `GREATEST(date 'lit', timestamp 'lit')`
            // dropped the widened `00:00:00`. Typing it here fixes them all
            // at once, in the same place Go sets the type
            // (`dateLiteralFunctionClass.getFunction`), rather than teaching
            // each consumer about literals.
            Ok(Expression::Constant(Constant::new(
                Datum::Time(time),
                ret_type,
            )))
        }
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
            // `CAST(x AS BINARY)` is Go's `funcPropAuto` binary-result arm:
            // a gbk-charset argument transcodes on the way in, which is why
            // `HEX(CAST(gbk_col AS BINARY))` reports the GBK bytes.
            let args = wrap_binary_literals("cast", ret_type.charset_name(), vec![arg]);
            let charset_name = ret_type.charset_name().to_owned();
            let collation_name = ret_type.collation_name().to_owned();
            let mut node = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(name),
                ret_type,
                args,
            ));
            // `cast_target` already computes the FULL correct target type
            // per cast kind (including `set_binary_charset` for `AS
            // BINARY`), mirroring the dedicated `types.SetBinChsClnFlag`
            // override Go's cast builtins apply after their own generic
            // collation derivation. `derive_tree_collation`'s later walk has
            // no `cast_*` case of its own, so without marking this node
            // derived here it would fall into the generic default arm and,
            // for a string result, overwrite this target charset with the
            // connection charset -- silently undoing `CAST(x AS BINARY)`.
            crate::collation_derive::apply_derived_collation(
                &mut node,
                &crate::expr_collation::ExprCollation {
                    coer: crate::expr_collation::Coercibility::IMPLICIT,
                    repe: crate::expr_collation::Repertoire::UNICODE,
                    charset: charset_name,
                    collation: collation_name,
                },
            );
            Ok(node)
        }
        // `CONVERT(expr USING charset)` -- Go `convertFunctionClass`. The
        // target charset is a build-time keyword, so it becomes a constant
        // argument of the one-argument `convert_using` signature and the
        // RESULT type carries the charset the value is retagged with.
        Expr::ConvertUsing { expr, charset } => {
            let charset = charset.to_ascii_lowercase();
            if !tidb_datatype::is_supported_encoding(&charset) {
                return Err(EvalError::Unsupported("unknown character set"));
            }
            let mut ret_type = FieldType::new(FieldTypeCode::VarString);
            ret_type.set_decimal(tidb_datatype::UNSPECIFIED_LENGTH);
            if charset == "binary" {
                set_binary_charset(&mut ret_type);
            } else {
                let target = tidb_datatype::Charset::from_name(&charset)
                    .expect("a supported encoding is a registered charset");
                ret_type.set_charset_name(target.name());
                ret_type.set_collation_name(target.default_collation().name());
            }
            let args = vec![
                rewrite_expr_resolved(expr, resolver)?,
                constant_string(&charset),
            ];
            let charset_name = ret_type.charset_name().to_owned();
            let collation_name = ret_type.collation_name().to_owned();
            let mut node = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("convert_using"),
                ret_type,
                args,
            ));
            // Go's `convertFunctionClass.getFunction` STAMPS the target
            // charset on the result type instead of aggregating it from the
            // arguments -- `ast.Convert` is not one of `deriveCollation`'s
            // aggregating arms. Marking the node derived here keeps
            // `derive_tree_collation` from replacing `gbk` with the
            // connection charset, which is what makes the following
            // `HEX`/`LENGTH` wrap see a non-legacy charset at all.
            crate::collation_derive::apply_derived_collation(
                &mut node,
                &crate::expr_collation::ExprCollation {
                    coer: crate::expr_collation::Coercibility::IMPLICIT,
                    repe: crate::expr_collation::Repertoire::UNICODE,
                    charset: charset_name,
                    collation: collation_name,
                },
            );
            Ok(node)
        }
        // Go `weightStringFunctionClass`: the `AS CHAR(n)`/`AS BINARY(n)`
        // clause is not syntax sugar over a cast -- it becomes the second and
        // third ARGUMENTS of the builtin, which `verifyArgs` requires to be
        // constants, and only then does `getFunction` pick the padding. Both
        // spellings are rebuilt here as those constants so one evaluator arm
        // reads them.
        Expr::WeightString { expr, as_type } => {
            let mut args = vec![rewrite_expr_resolved(expr, resolver)?];
            if let Some((kind, length)) = as_type {
                args.push(constant_string(match kind {
                    tidb_ast::WeightStringType::Char => "CHAR",
                    tidb_ast::WeightStringType::Binary => "BINARY",
                }));
                args.push(Expression::Constant(Constant::new(
                    Datum::Int(i64::try_from(*length).unwrap_or(i64::MAX)),
                    FieldType::new(FieldTypeCode::LongLong),
                )));
            }
            let mut ret_type = FieldType::new(FieldTypeCode::VarString);
            ret_type.set_decimal(tidb_datatype::UNSPECIFIED_LENGTH);
            set_binary_charset(&mut ret_type);
            Ok(Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("weight_string"),
                ret_type,
                args,
            )))
        }
        // Go `trimFunctionClass`: the direction picks one of the three
        // signatures, and a bare `TRIM(x)` strips spaces from both ends.
        Expr::Trim {
            expr,
            remstr,
            direction,
        } => {
            let name = match direction.unwrap_or(tidb_ast::TrimDirection::Both) {
                tidb_ast::TrimDirection::Both => "trim",
                tidb_ast::TrimDirection::Leading => "ltrim_with",
                tidb_ast::TrimDirection::Trailing => "rtrim_with",
            };
            let mut args = vec![rewrite_expr_resolved(expr, resolver)?];
            match remstr {
                Some(remstr) => args.push(rewrite_expr_resolved(remstr, resolver)?),
                None => args.push(constant_string(" ")),
            }
            let mut ret_type = FieldType::new(FieldTypeCode::VarString);
            ret_type.set_decimal(tidb_datatype::UNSPECIFIED_LENGTH);
            Ok(Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(name),
                ret_type,
                args,
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

    /// The flen each of these builtins reports is the CLIENT-VISIBLE
    /// `ColumnLength` in the result-set metadata, so a wrong one is
    /// wire-observable even when the VALUE is right. Each expectation is the
    /// `bf.tp.SetFlen(...)` line of the owning Go `getFunction`.
    #[test]
    fn builtin_result_flens_match_go_getfunction() {
        let str_arg = |text: &str| Expr::String(text.to_owned());
        let int_arg = |text: &str| Expr::Int(text.to_owned());
        let call = |name: &str, args: Vec<Expr>| Expr::Func {
            name: name.to_owned(),
            args,
            origin_position: 0,
        };
        let flen = |expr: &Expr| rewrite_expr(expr).unwrap().static_type().unwrap().flen();

        let uuid = "6ccd780c-baba-1026-9564-5b8c656024db";
        for (expr, expected, why) in [
            // `md5FunctionClass` (`builtin_encryption.go:592`) -- 32 hex
            // digits. Its SHA siblings already carried 40 and 128; MD5 was the
            // odd one out, lumped into the UNSIZED string family.
            (call("MD5", vec![str_arg("a")]), 32, "md5"),
            (call("SHA1", vec![str_arg("a")]), 40, "sha1"),
            (
                call("SHA2", vec![str_arg("a"), int_arg("256")]),
                128,
                "sha2",
            ),
            // `passwordFunctionClass` (`:487`) -- `mysql.PWDHashLen + 1`, the
            // 40 hex digits plus the leading `*`.
            (call("PASSWORD", vec![str_arg("x")]), 41, "password"),
            // `uncompressedLengthFunctionClass` (`:972`).
            (
                call("UNCOMPRESSED_LENGTH", vec![str_arg("x")]),
                10,
                "uncompressed_length",
            ),
            // `uncompressFunctionClass` (`:911`) -- `mysql.MaxBlobWidth`.
            (
                call("UNCOMPRESS", vec![str_arg("x")]),
                16_777_216,
                "uncompress",
            ),
            // `uuidToBinFunctionClass` (`builtin_miscellaneous.go:1822`) and
            // `binToUUIDFunctionClass` (`:1901`).
            (call("UUID_TO_BIN", vec![str_arg(uuid)]), 16, "uuid_to_bin"),
            (call("BIN_TO_UUID", vec![str_arg("x")]), 32, "bin_to_uuid"),
            // `uuidTimestampFunctionClass` (`:1683`).
            (
                call("UUID_TIMESTAMP", vec![str_arg("x")]),
                18,
                "uuid_timestamp",
            ),
            // `tidbShardFunctionClass` (`:1988`) is 4 (a hash taken mod 256);
            // `vitessHashFunctionClass` (`:1766`) is 20 (a full 64-bit digest).
            (call("TIDB_SHARD", vec![int_arg("1")]), 4, "tidb_shard"),
            (call("VITESS_HASH", vec![int_arg("1")]), 20, "vitess_hash"),
            // The `SessionVars` readers, every one of them `SetFlen(64)` in
            // `builtin_info.go`.
            (call("DATABASE", vec![]), 64, "database"),
            (call("SCHEMA", vec![]), 64, "schema"),
            (call("VERSION", vec![]), 64, "version"),
            (call("CURRENT_USER", vec![]), 64, "current_user"),
            (call("CURRENT_ROLE", vec![]), 64, "current_role"),
            (call("USER", vec![]), 64, "user"),
            (call("SESSION_USER", vec![]), 64, "session_user"),
            (call("SYSTEM_USER", vec![]), 64, "system_user"),
            // `encodeFunctionClass`/`decodeFunctionClass` (`:439`, `:389`)
            // copy argument 0's flen -- the stream cipher is
            // length-preserving. The argument here is `MD5(...)` rather than a
            // string LITERAL because this rewriter's `constant_string` leaves a
            // literal's flen unspecified where Go's `DefaultTypeForValue` sizes
            // it from the text; that gap is its own (pre-existing) divergence,
            // and pinning it here would assert the wrong number for ENCODE.
            (
                call(
                    "ENCODE",
                    vec![call("MD5", vec![str_arg("abc")]), str_arg("k")],
                ),
                32,
                "encode",
            ),
            (
                call(
                    "DECODE",
                    vec![call("MD5", vec![str_arg("abc")]), str_arg("k")],
                ),
                32,
                "decode",
            ),
            // `NAME_CONST` clones argument 1's WHOLE type (`:1259`), so it
            // reports the literal's own one-digit width, not a fixed one.
            (
                call("NAME_CONST", vec![str_arg("a"), int_arg("5")]),
                1,
                "name_const",
            ),
        ] {
            assert_eq!(flen(&expr), expected, "{why}");
        }

        // `AES_ENCRYPT`/`AES_DECRYPT` stay REFUSED on purpose: the ported body
        // is `aes-128-ecb` only, while Go picks the cipher from the
        // `block_encryption_mode` session variable, which this gate cannot
        // see. A refusal beats a silently wrong ciphertext.
        assert!(rewrite_expr(&call("AES_ENCRYPT", vec![str_arg("a"), str_arg("k")])).is_err());
        assert!(rewrite_expr(&call("AES_DECRYPT", vec![str_arg("a"), str_arg("k")])).is_err());
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

    /// Captured from TiDB (`SELECT <literal>`, reading the result field's own
    /// type/flen/flag): a literal above `math.MaxInt64` is `KindUint64` with
    /// `UnsignedFlag` set and a flen of its printed digits, and one wider
    /// than `math.MaxUint64` never reaches this arm at all -- the lexer has
    /// already made it a decimal literal, which is why `SELECT
    /// 18446744073709551616` reports `tp=decimal`.
    #[test]
    fn integer_literal_signedness_matches_tidb() {
        for (text, datum, flen, unsigned) in [
            ("0", Datum::Int(0), 1, false),
            ("007", Datum::Int(7), 1, false),
            ("9223372036854775807", Datum::Int(i64::MAX), 19, false),
            (
                "9223372036854775808",
                Datum::UInt(9_223_372_036_854_775_808),
                19,
                true,
            ),
            ("18446744073709551615", Datum::UInt(u64::MAX), 20, true),
        ] {
            let expr = rewrite(text);
            let (value, ft) = constant_of(&expr);
            assert_eq!(*value, datum, "{text} value");
            assert_eq!(ft.code(), FieldTypeCode::LongLong, "{text} type");
            assert_eq!(ft.flen(), flen, "{text} flen");
            assert_eq!(ft.decimal(), 0, "{text} decimal");
            assert_eq!(ft.charset_name(), "binary", "{text} charset");
            assert!(ft.flags() & tidb_datatype::FieldTypeFlags::NOT_NULL != 0);
            assert_eq!(
                ft.flags() & tidb_datatype::FieldTypeFlags::UNSIGNED != 0,
                unsigned,
                "{text} unsigned"
            );
        }
        // The mixed-sign comparison rules Go applies to the pair, captured
        // whole: `18446744073709551615 = -1` is FALSE (not the two-complement
        // bit identity), and `-1 < 18446744073709551615` is TRUE.
        for (text, want) in [
            ("18446744073709551615 = -1", Datum::Int(0)),
            ("-1 < 18446744073709551615", Datum::Int(1)),
            ("9223372036854775808 > 9223372036854775807", Datum::Int(1)),
            ("18446744073709551615 - 1", Datum::UInt(u64::MAX - 1)),
            ("9223372036854775808 + 1", Datum::UInt(1 << 63 | 1)),
        ] {
            let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
            chunk.set_num_virtual_rows(1);
            let got = rewrite(text)
                .eval(&crate::context::NoColumns, chunk.get_row(0))
                .expect(text);
            assert_eq!(got, want, "{text}");
        }
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

/// The chunk evaluator's builtin gate is [`builtin_return_type`]: a name it
/// does not type is refused outright, however complete the value
/// implementation behind it is. These tests pin the RESULT TYPE each newly
/// gated builtin reports against the `getFunction` that fixes it in
/// `pkg/expression/builtin_*.go`, and the VALUES against a `goeval` capture --
/// a builtin that returns the right string with the wrong flen, charset, or
/// unsigned flag is a latent bug, not a passing port.
#[cfg(test)]
mod builtin_type_tests {
    use super::result_type::base64_needed_encoded_length;
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

    fn ret_type(sql_expr: &str) -> FieldType {
        match rewrite(sql_expr) {
            Expression::ScalarFunction(f) => f.ret_type.expect("a builtin call has a result type"),
            other => panic!("expected a scalar function for {sql_expr}, got {other:?}"),
        }
    }

    fn eval(sql_expr: &str) -> Datum {
        let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
        chunk.set_num_virtual_rows(1);
        rewrite(sql_expr)
            .eval(&crate::context::NoColumns, chunk.get_row(0))
            .expect("evaluates")
    }

    fn text_datum(value: &str) -> Datum {
        Datum::new_string(value.as_bytes().to_vec())
    }

    fn is_unsigned(ft: &FieldType) -> bool {
        ft.flags() & tidb_datatype::FieldTypeFlags::UNSIGNED != 0
    }

    /// The integer-returning builtins, with the flen each `getFunction`
    /// fixes. `INET_ATON` and `LAST_INSERT_ID` are the UNSIGNED ones -- that
    /// flag is what makes `INET_ATON('255.255.255.255')` report 4294967295
    /// and `LAST_INSERT_ID(-1)` report 18446744073709551615 rather than a
    /// negative number.
    #[test]
    fn int_returning_builtins_carry_gos_flen_and_sign() {
        // Go's `newBaseBuiltinFuncWithTp` gives an `ETInt` result
        // `mysql.MaxIntWidth` when the class sets no flen of its own, which is
        // the same default `FieldType::new(LongLong)` carries.
        let default_int_width = 20;
        for (expr, flen, unsigned) in [
            // `ordFunctionClass` / `uuidVersionFunctionClass`: flen 10.
            ("ord('a')", 10, false),
            ("uuid_version('x')", 10, false),
            // The one-digit boolean family.
            ("is_ipv4('1.2.3.4')", 1, false),
            ("is_ipv4_compat('x')", 1, false),
            ("is_ipv4_mapped('x')", 1, false),
            ("is_ipv6('::1')", 1, false),
            ("is_uuid('x')", 1, false),
            ("isnull(null)", 1, false),
            // `bitCountFunctionClass`: flen 2 (at most 64 set bits).
            ("bit_count(255)", 2, false),
            // `inetAtonFunctionClass`: flen 21 AND unsigned.
            ("inet_aton('1.2.3.4')", 21, true),
            // No flen of their own.
            ("interval(1, 2, 3)", default_int_width, false),
            ("to_seconds('2009-11-29')", default_int_width, false),
            // `ROW_COUNT()` is SIGNED -- a failed statement reports -1.
            ("row_count()", default_int_width, false),
            ("last_insert_id()", default_int_width, true),
            ("last_insert_id(5)", default_int_width, true),
        ] {
            let ft = ret_type(expr);
            assert_eq!(ft.code(), FieldTypeCode::LongLong, "{expr} type");
            assert_eq!(ft.flen(), flen, "{expr} flen");
            assert_eq!(is_unsigned(&ft), unsigned, "{expr} unsigned");
        }
    }

    /// The string-returning builtins. The hash family and `INET_NTOA` are
    /// explicitly NOT binary -- Go stamps them with the CONNECTION charset --
    /// while `INET6_ATON` returns raw address bytes and IS binary, the same
    /// distinction `UNHEX` carries.
    #[test]
    fn string_returning_builtins_carry_gos_flen_and_charset() {
        let unspecified = tidb_datatype::UNSPECIFIED_LENGTH;
        for (expr, flen, binary) in [
            // `sha1FunctionClass` flen 40, `sha2FunctionClass` flen 128.
            ("sha('x')", 40, false),
            ("sha1('x')", 40, false),
            ("sha2('x', 256)", 128, false),
            // `inetNtoaFunctionClass` flen 93; `inet6NtoaFunctionClass` 117.
            ("inet_ntoa(0)", 93, false),
            ("inet6_ntoa('x')", 117, false),
            // `inet6AtonFunctionClass` flen 16, binary charset.
            ("inet6_aton('::1')", 16, true),
            // No fixed flen.
            ("format_bytes(0)", unspecified, false),
            ("format_nano_time(0)", unspecified, false),
            ("json_search('[\"a\"]', 'one', 'a')", unspecified, false),
        ] {
            let ft = ret_type(expr);
            assert_eq!(ft.code(), FieldTypeCode::VarString, "{expr} type");
            assert_eq!(ft.flen(), flen, "{expr} flen");
            assert_eq!(
                ft.charset_name() == "binary",
                binary,
                "{expr} charset is {:?}",
                ft.charset_name()
            );
        }
    }

    /// `TO_BASE64`'s flen is derived from its ARGUMENT's via
    /// `base64NeededEncodedLength`, and stays unspecified when the argument's
    /// is. A 3-byte literal encodes to 4 characters; a line break appears
    /// once the encoding exceeds 76 characters.
    #[test]
    fn to_base64_flen_follows_the_argument() {
        assert_eq!(base64_needed_encoded_length(0), 0);
        assert_eq!(base64_needed_encoded_length(1), 4);
        assert_eq!(base64_needed_encoded_length(3), 4);
        assert_eq!(base64_needed_encoded_length(57), 76);
        // 58 bytes encode to 80 characters, which crosses one line break.
        assert_eq!(base64_needed_encoded_length(58), 81);
        assert_eq!(
            base64_needed_encoded_length(tidb_datatype::UNSPECIFIED_LENGTH),
            tidb_datatype::UNSPECIFIED_LENGTH
        );
        // Go's overflow sentinel is -1, which is `UNSPECIFIED_LENGTH` itself.
        assert_eq!(
            base64_needed_encoded_length(6_827_690_988_321_067_804),
            tidb_datatype::UNSPECIFIED_LENGTH
        );
        // A string LITERAL is as wide as its own bytes (Go
        // `DefaultTypeForValue`'s `SetFlen(len(x))`), so three bytes derive
        // the four characters they encode to. `TO_BASE64` is
        // connection-charset text, never binary.
        let ft = ret_type("to_base64('abc')");
        assert_eq!(ft.code(), FieldTypeCode::VarString);
        assert_eq!(ft.flen(), 4);
        assert_ne!(ft.charset_name(), "binary");
    }

    /// A string literal is as wide as its own bytes, which is Go's
    /// `types.DefaultTypeForValue` (`SetFlen(len(x))` -- BYTES, so a
    /// multi-byte character counts more than once). Asserted end to end
    /// through the rewriter rather than on hand-built `FieldType`s, because
    /// the literal rule and the per-builtin sum have to meet somewhere and
    /// this is the only test that walks the join.
    ///
    /// Both numbers are Go's own, from `createTestCase4StrFuncs`.
    #[test]
    fn a_string_literal_is_as_wide_as_its_bytes() {
        assert_eq!(ret_type("concat('T', 'i', 'DB')").flen(), 4);
        assert_eq!(ret_type("concat_ws('-', 'T', 'i', 'DB')").flen(), 6);
        // BYTES, not characters: Go's own TODO beside `SetFlen(len(x))` says
        // this arguably ought to be tripled and is not.
        assert_eq!(ret_type("concat('é')").flen(), 2);
    }

    /// `TIME_FORMAT`'s flen is `(format_flen + 1) / 2 * 11`, an upper bound
    /// on how far one specifier can expand. The five-byte format literal
    /// therefore bounds the result at `(5 + 1) / 2 * 11`.
    #[test]
    fn time_format_flen_follows_the_format_argument() {
        let ft = ret_type("time_format('23:00:00', '%H %k')");
        assert_eq!(ft.code(), FieldTypeCode::VarString);
        assert_eq!(ft.flen(), 33);
    }

    /// `ANY_VALUE` is the identity on the whole `FieldType`, not merely the
    /// value: Go copies the argument's type over the builder's
    /// (`*bf.tp = *ft`). This is also what unblocks it in the planner -- the
    /// `ONLY_FULL_GROUP_BY` exemption already recognized `ANY_VALUE`, but the
    /// chunk evaluator refused it, so the exemption could only be observed as
    /// "some error other than 1055/8123".
    #[test]
    fn any_value_reports_its_argument_type() {
        assert_eq!(ret_type("any_value(1)").code(), FieldTypeCode::LongLong);
        assert_eq!(
            ret_type("any_value(3.14)").code(),
            FieldTypeCode::NewDecimal
        );
        assert_eq!(ret_type("any_value('x')").code(), FieldTypeCode::VarString);
        assert_eq!(eval("any_value(1234)"), Datum::Int(1234));
        assert_eq!(eval("any_value(null)"), Datum::Null);
    }

    /// Edge cases the result corpus does not carry, captured from Go with
    /// `rust/difftests/goeval`. Every one of these runs through the CHUNK
    /// evaluator (`rewrite` + `ScalarFunction::eval`), which is the path the
    /// result gate refused before these builtins were typed.
    #[test]
    fn go_captured_edge_case_values() {
        // NULL propagates through every one of them.
        for expr in [
            "ord(null)",
            "sha(null)",
            "sha1(null)",
            "sha2('x', null)",
            "sha2(null, 224)",
            "inet_aton(null)",
            "inet_ntoa(null)",
            "is_ipv4(null)",
            "is_ipv6(null)",
            "is_uuid(null)",
            "uuid_version(null)",
            "to_base64(null)",
            "format_bytes(null)",
            "format_nano_time(null)",
            "to_seconds(null)",
        ] {
            assert_eq!(eval(expr), Datum::Null, "{expr}");
        }
        // `ORD('')` is 0, not NULL -- the empty string has no first code
        // point but the signature still returns an integer.
        assert_eq!(eval("ord('')"), Datum::Int(0));
        // A multi-byte first character contributes its whole UTF-8 encoding
        // read as a big-endian number: 0xE4BDA0 = 14990752.
        assert_eq!(eval("ord('你好')"), Datum::Int(14_990_752));
        // `TO_BASE64('')` is the empty string, NOT NULL.
        assert_eq!(eval("to_base64('')"), text_datum(""));
        // `SHA('')` still hashes: SHA-1 of zero bytes.
        assert_eq!(
            eval("sha('')"),
            text_datum("da39a3ee5e6b4b0d3255bfef95601890afd80709")
        );
        // `SHA2`'s length argument: 0 means 256, and any value outside
        // {0, 224, 256, 384, 512} yields NULL rather than an error.
        assert_eq!(eval("sha2('pingcap', 0)"), eval("sha2('pingcap', 256)"));
        assert_eq!(eval("sha2('x', 255)"), Datum::Null);
        // `BIT_COUNT(-1)` counts the two's-complement bits: all 64.
        assert_eq!(eval("bit_count(-1)"), Datum::Int(64));
        // `INTERVAL` with a NULL first argument is -1, not NULL: Go's
        // `builtinIntervalRealSig` reports "below every bucket".
        assert_eq!(eval("interval(null, 1, 2)"), Datum::Int(-1));
        // `INET_NTOA` refuses an out-of-range address with NULL.
        assert_eq!(eval("inet_ntoa(-1)"), Datum::Null);
        assert_eq!(eval("inet_ntoa(0)"), text_datum("0.0.0.0"));
        assert_eq!(eval("inet_ntoa(4294967295)"), text_datum("255.255.255.255"));
        // A dotted-quad prefix is left-extended, so `'127'` is 127.
        assert_eq!(eval("inet_aton('127')"), Datum::UInt(127));
        assert_eq!(eval("isnull(null)"), Datum::Int(1));
        assert_eq!(eval("isnull(0)"), Datum::Int(0));
        // The zero date has no seconds count.
        assert_eq!(eval("to_seconds('0000-00-00')"), Datum::Null);
        assert_eq!(eval("to_seconds(950501)"), Datum::Int(62_966_505_600));
        assert_eq!(eval("format_bytes(0)"), text_datum("0 bytes"));
        assert_eq!(
            eval("time_format('23:00:00', '%H %k')"),
            text_datum("23 23")
        );
    }
}
