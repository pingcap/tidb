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

//! Go-shaped AST-to-planner expression rewriting and constant-fold ordering.

use crate::aggregation::wrap_cast::{
    wrap_with_cast_as_decimal, wrap_with_cast_as_duration, wrap_with_cast_as_int,
    wrap_with_cast_as_json, wrap_with_cast_as_real, wrap_with_cast_as_string,
    wrap_with_cast_as_time,
};
use tidb_datatype::CollationCollator;
use crate::column::Column;
use crate::constant::Constant;
use crate::constant_fold::ConstantFoldMode;
use crate::context::JsonError;
use crate::expression::{Expression, ScalarFunction};
use crate::scalar_function::{binary_op_name, unary_op_name};
use crate::EvalError;
use tidb_ast::{BinaryOp, CiString, Expr, GetFormatSelector, IsTarget, UnaryOp};
use tidb_datatype::{
    Datum, EvalType, FieldType, FieldTypeCode,
};

/// Go `mysql.DefaultDecimal` (`parser/mysql/const.go`): the value a decimal
/// literal too large for a `MyDecimal` saturates to.
const DEFAULT_DECIMAL_LITERAL: &str =
    "99999999999999999999999999999999999999999999999999999999999999999";

pub(crate) mod control_type;
mod fold_mode;
pub(crate) mod signature_args;
pub(crate) mod result_type;

pub use control_type::{infer_type4_control_funcs, set_numeric_len_from_args};
use fold_mode::FoldModeResolver;
pub(crate) use result_type::adjust_ret_ft_for_cast_string;
pub(crate) use result_type::builtin_return_type;
use result_type::{
    binary_literal_type, cast_target, decimal_literal_type, int_literal_type,
    returns_binary_string, set_binary_charset, validate_cast_type, validate_name_const_args,
};

/// Resolves a dotted column path to an output column, standing in for the
/// schema/name resolution Go's `expression_rewriter` performs against the
/// plan's schema (`resolveColumn`).
pub trait ColumnResolver {
    /// Builds GROUPING using the current query block's Expand metadata.
    fn rewrite_grouping(&self, _args: &[Expression]) -> Result<Expression, EvalError> {
        Err(EvalError::InvalidGroupFuncUse)
    }

    /// Resolves `path` (e.g. `["t", "a"]` or `["a"]`) to
    /// `(row index, result type, unique id)`, or `None` when unknown.
    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)>;

    /// Go's session user-variable owner: declared types are resolved at build time.
    fn user_vars(&self) -> Option<&crate::user_vars::UserVars> {
        None
    }

    /// The current typed value of a prepared marker. Its absolute order is
    /// retained in the compiled expression, independently of this snapshot.
    fn param_value(&self, _order: usize) -> Result<Datum, EvalError> {
        Err(EvalError::Unsupported("unbound prepared parameter"))
    }

    /// Resolves `path` to a COMPLETE output column rather than the three
    /// fields [`Self::resolve`] reports.
    ///
    /// Go's `expressionRewriter.toColumn` hands back `schema.Columns[idx]`
    /// itself, so the built expression keeps that column's `ID`, `OrigName`,
    /// `IsHidden`, `VirtualExpr` and derived collation. A resolver that owns
    /// real [`Column`] values (one built over a
    /// [`Schema`](crate::schema::Schema), as
    /// [`crate::simple_expr::build_simple_expr`] does) should override this so
    /// none of that identity is dropped on the way into the tree.
    ///
    /// The default combines `(index, type, unique id)` with the optional
    /// source name. Overrides return all metadata in one lookup; rewriting
    /// does not resolve the name again after receiving a complete column.
    fn resolve_column(&self, path: &[String]) -> Option<Column> {
        let (index, ret_type, unique_id) = self.resolve(path)?;
        let mut col = Column::new(unique_id, ret_type);
        col.index = index as i64;
        col.orig_name = self.orig_name(path).unwrap_or_default();
        Some(col)
    }

    /// Resolves a column-shaped AST leaf to the complete expression Go's
    /// plan-aware rewriter produces. Ordinary scopes return a plain column;
    /// a nested-query scope may override this with a `CorrelatedColumn` that
    /// retains the outer binding identity.
    fn resolve_expression(&self, path: &[String]) -> Option<Expression> {
        self.resolve_column(path).map(Expression::Column)
    }

    /// Go `clauseMsg` (`planbuilder.go:132`): the clause an unknown-column
    /// error names. Go's plan-aware rewriter reads it from `er.clause()`; the
    /// default is Go's `expressionClause` spelling, which is what a resolver
    /// without a statement clause reports.
    fn clause_message(&self) -> &'static str {
        "expression"
    }

    /// Resolves the Go `Column.OrigName` carried by a source column. The
    /// planner uses this metadata for EXPLAIN and diagnostics after a column
    /// crosses a Projection or aggregate boundary; execution still binds by
    /// index and unique id. Resolvers without source-name metadata keep the
    /// empty default.
    fn orig_name(&self, _path: &[String]) -> Option<String> {
        None
    }

    /// Resolves a planner-owned column-shaped value to the constant it
    /// represents. Go uses this for an evaluated scalar subquery: its
    /// `Constant` keeps `SubqueryRefID` for EXPLAIN, while estimation and
    /// execution consume the ordinary datum.
    fn resolve_constant(&self, _path: &[String]) -> Option<Expression> {
        None
    }

    /// Whether any column path in this resolver can currently materialize as
    /// a planner-owned constant. Consumers that only need to substitute such
    /// columns can use this to keep an ordinary AST borrowed instead of
    /// cloning it speculatively.
    fn has_resolved_constants(&self) -> bool {
        false
    }

    /// Resolves a `DEFAULT(column)` leaf to the statement-scoped constant its
    /// DML planner prepared. A default implementation keeps ordinary query
    /// resolvers unaware of write-only metadata; write resolvers override it
    /// after materializing defaults in Go's planning order.
    fn resolve_default(&self, _path: &[String]) -> Option<Expression> {
        None
    }

    /// Go `evalDefaultExprWithPlanCtx`: `DEFAULT(column)` inside a query,
    /// resolved through the plan's name scopes with Go's own errors (1052,
    /// 1054, 1364). `None` leaves the leaf to [`Self::resolve_default`].
    fn resolve_default_in_query(&self, _path: &[String]) -> Option<Result<Expression, EvalError>> {
        None
    }

    /// Resolves `VALUES(column)` to Go's `NewValuesFunc`. Go's rewriter builds
    /// it in every scope over the column the current schema names
    /// (`FindFieldName`, else 1054); an INSERT's ON DUPLICATE KEY UPDATE
    /// resolves `insertPlan.TableColNames` instead, which its resolver
    /// overrides this with. Outside that scope the function reads an empty
    /// `CurrInsertValues` and answers NULL.
    fn resolve_values(&self, path: &[String]) -> Option<Expression> {
        let (index, field_type, _) = self.resolve(path)?;
        Some(Expression::ScalarFunction(ScalarFunction::new_values(
            index, field_type,
        )))
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

    /// The statement's temporal SQL-mode bits. Typed `DATE`/`TIMESTAMP`
    /// literals are built under the same session context as ordinary casts;
    /// a resolver with no session uses TiDB's shipped default mode.
    fn date_modes(&self) -> tidb_datatype::DateModes {
        tidb_datatype::DateModes::TIDB_DEFAULT_SQL_MODE
    }

    /// Go's `BuildContext.GetCharsetInfo`: the connection charset/collation
    /// stamped onto ordinary string literals and string results with no
    /// stronger collation source.
    fn connection_charset_info(&self) -> (&str, &str) {
        crate::collation_derive::connection_charset_info()
    }

    /// Go `GetDefaultCollationForUTF8MB4`: the collation a `_utf8mb4'...'`
    /// literal takes (`adjustUTF8MB4Collation`). A resolver bound to a live
    /// statement reads the session variable through it.
    fn default_collation_for_utf8mb4(&self) -> String {
        self.comparison_context()
            .and_then(|ctx| ctx.sysvar(None, "default_collation_for_utf8mb4"))
            .and_then(|value| {
                value
                    .as_raw_bytes()
                    .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
            })
            .unwrap_or_else(|| "utf8mb4_bin".to_owned())
    }

    /// Length of the process identity returned by `TIDB_VERSION()`.
    /// Go reads the process globals while building this function and uses the
    /// same rendered value to set its result width.
    fn tidb_info_len(&self) -> usize {
        tidb_printer::get_tidb_info().len()
    }

    /// The third argument Go supplies to `like()` when SQL omitted `ESCAPE`.
    /// Session-aware resolvers override this for `NO_BACKSLASH_ESCAPES`.
    fn like_default_escape(&self) -> u8 {
        b'\\'
    }

    /// Whether integer subtraction is forced onto the signed result domain.
    fn no_unsigned_subtraction(&self) -> bool {
        false
    }

    /// The live statement context Go's comparison builder uses while
    /// refining an integer column against a non-integer constant. A planner
    /// resolver can expose it so conversion diagnostics are raised once at
    /// build time; context-free resolvers retain the historical no-warning
    /// structural rewrite.
    fn comparison_context(&self) -> Option<&dyn crate::context::Columns> {
        None
    }

    /// The statement's `div_precision_increment` used to build `/` metadata.
    fn div_precision_increment(&self) -> u32 {
        4
    }

    /// Go `SessionVars.CurrentDB`, used to qualify an unknown function name.
    fn current_database(&self) -> Option<String> {
        None
    }

    /// Go `expressionRewriter.allowBuildCastArray`: only a functional
    /// index's generated expression may build `CAST(... AS ... ARRAY)`.
    fn allow_build_cast_array(&self) -> bool {
        false
    }

    /// Construction mode inherited from the current AST parent.
    fn fold_mode(&self) -> ConstantFoldMode {
        ConstantFoldMode::Normal
    }

    /// Folds the node just built in its statement context and selected mode.
    fn fold_constant(&self, expression: &mut Expression, mode: ConstantFoldMode) {
        if mode != ConstantFoldMode::Disabled {
            crate::constant_fold::derive_constant_null_flag(expression);
        }
    }

    /// Evaluates a column-free expression in the same context used to build
    /// it. Ranger endpoints must not lose parameters, warnings or session
    /// settings by reconstructing a sessionless evaluation context.
    /// Session-backed resolvers override this; the standalone default carries
    /// only the explicitly supplied timezone.
    fn eval_constant(&self, expression: &Expression) -> Result<Datum, EvalError> {
        // The fold context COLLECTS the warnings the expression raises
        // (go appends them to StmtCtx.warnings): ADDDATE('abc', INTERVAL 1
        // DAY) warns `Incorrect datetime value: 'abc'` even on an empty
        // table. The statement driver drains the stash into the warning
        // buffer right after planning.
        let ctx = crate::constant_fold::FoldWarningContext::new(self.time_zone());
        crate::eval_expression_once(expression, &ctx)
    }
}

// Keep a borrowed resolver's complete context, including trait objects, when
// passing it through the rewriter's statically dispatched recursive helpers.
impl<T: ColumnResolver + ?Sized> ColumnResolver for &T {
    fn user_vars(&self) -> Option<&crate::user_vars::UserVars> {
        (**self).user_vars()
    }
    fn allow_build_cast_array(&self) -> bool {
        (**self).allow_build_cast_array()
    }

    fn rewrite_grouping(&self, args: &[Expression]) -> Result<Expression, EvalError> {
        (**self).rewrite_grouping(args)
    }

    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        (**self).resolve(path)
    }
    fn resolve_column(&self, path: &[String]) -> Option<Column> {
        (**self).resolve_column(path)
    }
    fn resolve_expression(&self, path: &[String]) -> Option<Expression> {
        (**self).resolve_expression(path)
    }
    fn clause_message(&self) -> &'static str {
        (**self).clause_message()
    }
    fn param_value(&self, order: usize) -> Result<Datum, EvalError> {
        (**self).param_value(order)
    }
    fn orig_name(&self, path: &[String]) -> Option<String> {
        (**self).orig_name(path)
    }
    fn resolve_constant(&self, path: &[String]) -> Option<Expression> {
        (**self).resolve_constant(path)
    }
    fn has_resolved_constants(&self) -> bool {
        (**self).has_resolved_constants()
    }
    fn resolve_default(&self, path: &[String]) -> Option<Expression> {
        (**self).resolve_default(path)
    }
    fn resolve_default_in_query(&self, path: &[String]) -> Option<Result<Expression, EvalError>> {
        (**self).resolve_default_in_query(path)
    }
    fn resolve_values(&self, path: &[String]) -> Option<Expression> {
        (**self).resolve_values(path)
    }
    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        (**self).time_zone()
    }
    fn date_modes(&self) -> tidb_datatype::DateModes {
        (**self).date_modes()
    }
    fn connection_charset_info(&self) -> (&str, &str) {
        (**self).connection_charset_info()
    }
    fn tidb_info_len(&self) -> usize {
        (**self).tidb_info_len()
    }
    fn like_default_escape(&self) -> u8 {
        (**self).like_default_escape()
    }
    fn no_unsigned_subtraction(&self) -> bool {
        (**self).no_unsigned_subtraction()
    }
    fn div_precision_increment(&self) -> u32 {
        (**self).div_precision_increment()
    }
    fn current_database(&self) -> Option<String> {
        (**self).current_database()
    }
    fn fold_mode(&self) -> ConstantFoldMode {
        (**self).fold_mode()
    }
    fn fold_constant(&self, expression: &mut Expression, mode: ConstantFoldMode) {
        (**self).fold_constant(expression, mode);
    }
    fn eval_constant(&self, expression: &Expression) -> Result<Datum, EvalError> {
        (**self).eval_constant(expression)
    }
}

/// A no-column resolver carrying a real session zone and `LIKE` escape.
pub struct ZonedNoResolver {
    zone: tidb_datatype::SessionTimeZone,
    like_default_escape: u8,
    /// The session's `GetCharsetInfo`, when the caller evaluates under one.
    connection_charset: Option<(String, String)>,
}

impl ZonedNoResolver {
    /// Builds a no-column resolver with the historic implicit backslash
    /// `LIKE` escape.
    #[must_use]
    pub fn new(zone: tidb_datatype::SessionTimeZone) -> Self {
        Self {
            zone,
            like_default_escape: b'\\',
            connection_charset: None,
        }
    }

    /// Builds a no-column resolver from the live statement's zone and
    /// implicit `LIKE` escape.
    #[must_use]
    pub fn with_like_default_escape(zone: tidb_datatype::SessionTimeZone, escape: u8) -> Self {
        Self {
            zone,
            like_default_escape: escape,
            connection_charset: None,
        }
    }

    /// Stamps string literals with the session's connection charset and
    /// collation (Go `BuildContext.GetCharsetInfo`), as an expression Go
    /// evaluates under the session context (`EvalSimpleAst`,
    /// `ParseSimpleExpr`) does.
    #[must_use]
    pub fn with_connection_charset(mut self, charset: &str, collation: &str) -> Self {
        self.connection_charset = Some((charset.to_owned(), collation.to_owned()));
        self
    }
}

impl ColumnResolver for ZonedNoResolver {
    fn resolve(&self, _path: &[String]) -> Option<(usize, FieldType, i64)> {
        None
    }

    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        self.zone.clone()
    }

    fn like_default_escape(&self) -> u8 {
        self.like_default_escape
    }

    fn connection_charset_info(&self) -> (&str, &str) {
        match &self.connection_charset {
            Some((charset, collation)) => (charset, collation),
            None => crate::collation_derive::connection_charset_info(),
        }
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
/// type that may not describe it. JSON and DATE/DATETIME retain their native
/// datum domains.
///
/// The `ARRAY` modifier is outside this AST's cast surface.
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

#[cfg(test)]
pub(crate) fn builtin_cast_result_type(
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
///
/// The `funcPropAuto` arm's other direction -- a BINARY argument of a
/// function whose derived charset is not binary -- decodes through an
/// implicit `from_binary`, but only at the positions the function class
/// declares `ETString` ([`string_declared_args`]): an integer argument also
/// carries the binary charset and is never wrapped.
pub(crate) fn wrap_binary_literals(
    name: &str,
    result_charset: &str,
    result_collation: &str,
    ret_eval_type: tidb_datatype::EvalType,
    args: Vec<Expression>,
    fold: impl Fn(&mut Expression),
) -> Vec<Expression> {
    let prop = crate::convert_charset::func_prop(name);
    if prop == crate::convert_charset::FuncProp::None {
        return args;
    }
    let declared_string = if prop == crate::convert_charset::FuncProp::Auto {
        string_declared_args(name, &args, ret_eval_type)
    } else {
        Vec::new()
    };
    args.into_iter()
        .enumerate()
        .map(|(position, arg)| {
            let Some(arg_type) = arg.static_type() else {
                return arg;
            };
            if crate::convert_charset::needs_to_binary(
                prop,
                arg_type.charset_name(),
                result_charset,
            ) {
                return build_to_binary(arg, &fold);
            }
            if declared_string.get(position).copied().unwrap_or(false) {
                let mut wrapped = wrap_implicit_from_binary(arg, result_charset, result_collation);
                if matches!(&wrapped, Expression::ScalarFunction(function)
                    if function.func_name.lowercase() == "from_binary")
                {
                    // Go `BuildFromBinaryFunction` folds the new node.
                    fold(&mut wrapped);
                }
                return wrapped;
            }
            arg
        })
        .collect()
}

/// The argument positions a `funcPropAuto` function class declares
/// `ETString` -- the only ones `newBaseBuiltinFuncWithTp` /
/// `newBaseBuiltinFuncWithFieldTypes` hand to `HandleBinaryLiteral`.
/// Comparisons and CAST take their own builders.
fn string_declared_args(
    name: &str,
    args: &[Expression],
    ret_eval_type: tidb_datatype::EvalType,
) -> Vec<bool> {
    use tidb_datatype::EvalType;
    let count = args.len();
    let at = |positions: &[usize]| {
        (0..count)
            .map(|index| positions.contains(&index))
            .collect::<Vec<_>>()
    };
    let string_result = ret_eval_type == EvalType::String;
    match name {
        "concat" | "concat_ws" | "find_in_set" | "instr" | "replace" | "strcmp" | "crc32"
        | "regexp" | "regexp_like" => vec![true; count],
        // `exportSetFunctionClass`: bits and number_of_bits are integers.
        "export_set" => at(&[1, 2, 3]),
        // `fieldFunctionClass`: ETString only when every argument is a
        // string type (`types.IsStringType`).
        "field" => vec![
            args.iter()
                .all(|arg| arg.static_type().is_some_and(FieldType::is_string));
            count
        ],
        "insert_func" => at(&[0, 3]),
        "lpad" | "rpad" => at(&[0, 2]),
        "locate" | "position" | "substring_index" | "trim" | "like" | "ilike" => at(&[0, 1]),
        "make_set" | "elt" => (0..count).map(|index| index > 0).collect(),
        "regexp_instr" => at(&[0, 1, 5]),
        "regexp_substr" => at(&[0, 1, 4]),
        "regexp_replace" => at(&[0, 1, 2, 5]),
        // The control functions declare their value arguments with the
        // result type.
        "if" => (0..count).map(|index| index > 0 && string_result).collect(),
        "ifnull" => vec![string_result; count],
        "case" => (0..count)
            .map(|index| string_result && (index % 2 == 1 || index + 1 == count))
            .collect(),
        _ => vec![false; count],
    }
}

/// Go `BuildToBinaryFunction`, which folds the new node before its parent
/// inspects argument constancy. `tidbToBinaryFunctionClass` clones the
/// argument's type into a binary VAR_STRING.
fn build_to_binary(arg: Expression, fold: &dyn Fn(&mut Expression)) -> Expression {
    let mut ret_type = arg
        .static_type()
        .cloned()
        .unwrap_or_else(|| FieldType::new(FieldTypeCode::VarString));
    ret_type.set_code(FieldTypeCode::VarString);
    set_binary_charset(&mut ret_type);
    let mut wrapped = Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("to_binary"),
        ret_type,
        vec![arg],
    ));
    fold(&mut wrapped);
    wrapped
}

/// `HandleBinaryLiteral`'s `from_binary` arm for an IMPLICIT conversion: a
/// binary-charset argument of a function whose derived charset is not binary
/// decodes into it, and an undecodable value is an error, not a warning.
pub(crate) fn wrap_implicit_from_binary(
    arg: Expression,
    result_charset: &str,
    result_collation: &str,
) -> Expression {
    let Some(arg_type) = arg.static_type() else {
        return arg;
    };
    // Go reaches `HandleBinaryLiteral` after `WrapWithCastAsString`, so a
    // non-string argument is a connection-charset string by then.
    if arg_type.eval_type() != tidb_datatype::EvalType::String
        || arg_type.charset_name() != "binary"
        || result_charset == "binary"
        || arg_type.code() == FieldTypeCode::Null
    {
        return arg;
    }
    let mut ret_type = arg_type.clone();
    ret_type.set_charset_name(result_charset);
    ret_type.set_collation_name(result_collation);
    let (coer, repe) = (
        crate::collation_derive::coercibility_of(&arg),
        crate::collation_derive::repertoire_of(&arg),
    );
    let mut wrapped =
        Expression::ScalarFunction(ScalarFunction::new_from_binary(arg, ret_type, false));
    pin_result_collation(&mut wrapped, coer, repe);
    wrapped
}

/// Go `BuildCastFunctionWithCheck`'s string arm: a BIT source sizes the
/// string target in BYTES, `(flen + 7) / 8`, before the cast class reads it.
pub(crate) fn size_string_cast_for_bit_source(target: &mut FieldType, source: &Expression) {
    if let Some(bits) = source
        .static_type()
        .filter(|source| source.code() == FieldTypeCode::Bit)
        .map(FieldType::flen)
    {
        target.set_flen((bits + 7) / 8);
    }
}

/// Marks a node built with a fixed result charset as already derived, with
/// that charset and the given coercibility.
fn pin_result_collation(
    node: &mut Expression,
    coer: crate::expr_collation::Coercibility,
    repe: crate::expr_collation::Repertoire,
) {
    let Some(ret_type) = node.static_type() else {
        return;
    };
    let ec = crate::expr_collation::ExprCollation {
        coer,
        repe,
        charset: ret_type.charset_name().to_owned(),
        collation: ret_type.collation_name().to_owned(),
    };
    crate::collation_derive::apply_derived_collation(node, &ec);
}

/// Go `castAsStringFunctionClass.getFunction`'s argument handling for a
/// cast to `target` (a string type): a BIT argument cast to a non-binary
/// charset is first cast to a binary string, and a string argument then
/// meets `HandleBinaryLiteral` with the cast's own charset as an EXPLICIT
/// cast -- so a binary argument decodes through `from_binary`, which warns
/// rather than errors on bytes the target charset cannot hold. ENUM and SET
/// (and BIT into binary) take the early return untouched.
pub(crate) fn wrap_cast_as_string_arg(
    arg: Expression,
    target: &FieldType,
    fold: &dyn Fn(&mut Expression),
) -> Result<Expression, EvalError> {
    let Some((hybrid, bit)) = arg
        .static_type()
        .map(|arg_type| (arg_type.is_hybrid(), arg_type.code() == FieldTypeCode::Bit))
    else {
        return Ok(arg);
    };
    let mut arg = arg;
    if hybrid {
        if !bit || target.charset_name() == "binary" {
            return Ok(arg);
        }
        let mut binary = FieldType::new(FieldTypeCode::String);
        set_binary_charset(&mut binary);
        binary.add_flags(tidb_datatype::FieldTypeFlags::BINARY);
        arg = crate::simple_expr::build_cast_function(arg, binary, false)?;
        // A cast's result charset is its target's; keep the later
        // tree-collation walk from re-deriving it.
        pin_result_collation(
            &mut arg,
            crate::expr_collation::Coercibility::IMPLICIT,
            crate::expr_collation::Repertoire::UNICODE,
        );
        fold(&mut arg);
    }
    let Some(arg_type) = arg.static_type().cloned() else {
        return Ok(arg);
    };
    if arg_type.eval_type() != tidb_datatype::EvalType::String {
        return Ok(arg);
    }
    let (arg_charset, result_charset) = (arg_type.charset_name(), target.charset_name());
    // `HandleBinaryLiteral`'s `funcPropAuto` arm for `ast.Cast`.
    if arg_charset != "binary" && result_charset == "binary" {
        if crate::convert_charset::is_legacy_charset(arg_charset) {
            return Ok(arg);
        }
        return Ok(build_to_binary(arg, fold));
    }
    if arg_charset == "binary"
        && result_charset != "binary"
        && arg_type.code() != FieldTypeCode::Null
    {
        let mut ret_type = arg_type.clone();
        ret_type.set_charset_name(result_charset);
        ret_type.set_collation_name(target.collation_name());
        // Go keeps the argument's coercibility in `collationInfo` while
        // `bf.tp = c.tp` makes the result the target charset.
        let (coer, repe) = (
            crate::collation_derive::coercibility_of(&arg),
            crate::collation_derive::repertoire_of(&arg),
        );
        let mut wrapped =
            Expression::ScalarFunction(ScalarFunction::new_from_binary(arg, ret_type, true));
        pin_result_collation(&mut wrapped, coer, repe);
        // Go `BuildFromBinaryFunction` folds the new node.
        fold(&mut wrapped);
        return Ok(wrapped);
    }
    Ok(arg)
}

/// Reapplies the result-charset rules whose Go function classes override the
/// generic collation derivation after `newBaseBuiltinFuncWithTp` returns.
pub(crate) fn restore_function_result_charset(expr: &mut Expression) -> Result<(), EvalError> {
    let Expression::ScalarFunction(func) = expr else {
        return Ok(());
    };
    if returns_binary_string(func.func_name.lowercase()) {
        if let Some(field_type) = func.ret_type.as_mut() {
            set_binary_charset(field_type);
        }
    }
    result_type::restore_char_result_charset(func)
}

/// Applies the two `ETReal` argument declarations of Go's
/// `powFunctionClass.getFunction` while building the expression tree.
/// Go `timeFormatFunctionClass.getFunction` builds through
/// `newBaseBuiltinFuncWithTp(..., types.ETString, types.ETDuration,
/// types.ETString)`: the time argument is cast to TIME
/// (`WrapWithCastAsDuration`), so a number that is no valid time warns
/// through that cast and reaches the function as NULL.
fn wrap_time_format_arguments(
    name: &str,
    args: Vec<Expression>,
    resolver: &impl ColumnResolver,
) -> Result<Vec<Expression>, EvalError> {
    if name != "time_format" {
        return Ok(args);
    }
    let mut args = args.into_iter();
    let Some(first) = args.next() else {
        return Ok(Vec::new());
    };
    let is_duration = first
        .static_type()
        .is_some_and(|field_type| field_type.code() == FieldTypeCode::Duration);
    let mut first = wrap_with_cast_as_duration(first)?;
    if !is_duration {
        // `BuildCastFunction` folds the cast it built.
        derive_tree_collation_with_connection(&mut first, resolver.connection_charset_info())?;
        resolver.fold_constant(&mut first, ConstantFoldMode::Normal);
    }
    Ok(std::iter::once(first).chain(args).collect())
}

/// Go's clock function classes (`nowFunctionClass`, `sysDateFunctionClass`,
/// `currentTimeFunctionClass`, `utcTimeFunctionClass`,
/// `utcTimestampFunctionClass`) declare the optional precision `ETInt` and
/// read it back with `getFspByIntArg` while BUILDING: a constant precision is
/// cast to INT, folded, and range-checked before anything is evaluated.
fn wrap_clock_fsp_argument(
    name: &str,
    args: Vec<Expression>,
    resolver: &impl ColumnResolver,
) -> Result<Vec<Expression>, EvalError> {
    if !matches!(
        name,
        "now"
            | "current_timestamp"
            | "localtime"
            | "localtimestamp"
            | "sysdate"
            | "curtime"
            | "current_time"
            | "utc_time"
            | "utc_timestamp"
    ) {
        return Ok(args);
    }
    let args = args
        .into_iter()
        .map(|arg| {
            let is_int = arg
                .static_type()
                .is_some_and(|field_type| field_type.eval_type() == EvalType::Int);
            let mut arg = wrap_with_cast_as_int(arg, None)?;
            if !is_int {
                // `BuildCastFunction` folds the cast it built.
                derive_tree_collation_with_connection(
                    &mut arg,
                    resolver.connection_charset_info(),
                )?;
                resolver.fold_constant(&mut arg, ConstantFoldMode::Normal);
            }
            Ok(arg)
        })
        .collect::<Result<Vec<_>, EvalError>>()?;
    result_type::get_fsp_by_int_arg(&args, name)?;
    Ok(args)
}

fn wrap_power_arguments(
    name: &str,
    args: Vec<Expression>,
    resolver: &impl ColumnResolver,
) -> Result<Vec<Expression>, EvalError> {
    if !matches!(name, "pow" | "power") {
        return Ok(args);
    }
    args.into_iter()
        .map(|arg| {
            if arg
                .static_type()
                .is_some_and(|field_type| field_type.eval_type() == tidb_datatype::EvalType::Real)
            {
                return Ok(arg);
            }
            let ret_type = arg.static_type().map_or_else(
                || crate::builtin_arithmetic::new_return_field_type(tidb_datatype::EvalType::Real),
                |source| crate::builtin_arithmetic::real_argument_type(source).into_owned(),
            );
            if let Expression::Constant(constant) = &arg {
                // Immutable integer literals need no runtime conversion.
                // Markers use the context-aware cast builder below.
                let real = match constant.literal_value() {
                    Some(Datum::Int(value)) => Some(*value as f64),
                    Some(Datum::UInt(value)) => Some(*value as f64),
                    _ => None,
                };
                if let Some(value) = real {
                    return Ok(Expression::Constant(Constant::new(
                        Datum::Real(value),
                        ret_type,
                    )));
                }
            }
            let mut cast = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("cast_double"),
                ret_type,
                vec![arg],
            ));
            // WrapWithCastAsReal calls BuildCastFunction, which folds its
            // newly constructed cast even under a try/disabled-fold parent.
            // Parameter results keep their deferred expression for rebinding.
            derive_tree_collation_with_connection(&mut cast, resolver.connection_charset_info())?;
            resolver.fold_constant(&mut cast, ConstantFoldMode::Normal);
            Ok(cast)
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

/// Go `wrapWithIsTrue(ctx, true, arg, false)`: an integer argument passes
/// through, anything else is wrapped in `istrue_with_null`, whose
/// `getFunction` reads a string, time or JSON argument as a double (the cast
/// is built, and folded over a constant, like any `WrapWithCastAsReal`).
fn wrap_with_is_true_with_null(
    argument: Expression,
    resolver: &impl ColumnResolver,
) -> Result<Expression, EvalError> {
    let eval_type = argument
        .static_type()
        .map_or(EvalType::Int, FieldType::eval_type);
    if eval_type == EvalType::Int {
        return Ok(argument);
    }
    let wrapped = if matches!(
        eval_type,
        EvalType::String
            | EvalType::Timestamp
            | EvalType::Datetime
            | EvalType::Duration
            | EvalType::Json
    ) {
        // GO folds the wrap-time cast bottom-up (WrapWithCastAsReal evaluates
        // a constant argument eagerly), so `istrue_with_null(cast(', ',
        // double))` collapses to `0`. Fold the cast first: the wrapper fold
        // requires all-constant arguments.
        let mut cast = crate::aggregation::wrap_cast::wrap_with_cast_as_real(argument)?;
        resolver.fold_constant(&mut cast, ConstantFoldMode::Normal);
        cast
    } else {
        argument
    };
    let mut call = Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("istrue_with_null"),
        crate::expr_util::builder::tiny_int_type(),
        vec![wrapped],
    ));
    resolver.fold_constant(&mut call, ConstantFoldMode::Normal);
    Ok(call)
}

/// A hex/bit literal keeps its raw-bytes semantics in arithmetic: Go's
/// `numericContextResultType` reads `IsBinaryLiteral` as an integer, so the
/// operand gets no cast and `b'1111...' + 0` must stay INT:-1. ENUM and SET
/// columns are NOT exempt: `WrapWithCastAsReal` casts them like any other
/// operand (`e + i` plans as `plus(cast(e, double), cast(i, double))`).
fn is_hybrid_argument(expression: &Expression) -> bool {
    matches!(
        expression,
        Expression::Constant(constant)
            if matches!(constant.value, tidb_datatype::Datum::BinaryLiteral(_))
    )
}

/// Whether the node is a cast scalar function that GO's BuildCastFunction
/// would have folded at construction: only a freshly-built cast needs the
/// construction-time fold.
/// `newBaseBuiltinFuncWithTp` for a builtin with an integer result whose
/// `string_positions` are declared `ETString`: the collation is derived first,
/// each string argument is `WrapWithCastAsString`-ed (folded at Go's
/// `BuildCastFunction`), and `HandleBinaryLiteral` then sees the cast
/// arguments.
fn wrap_declared_string_args(
    name: &str,
    args: Vec<Expression>,
    string_positions: &[usize],
    resolver: &impl ColumnResolver,
) -> Result<Vec<Expression>, EvalError> {
    let connection = resolver.connection_charset_info();
    let derived = crate::collation_derive::derive_collation_with_connection(
        name,
        &args,
        tidb_datatype::EvalType::Int,
        connection,
    )?;
    let args = args
        .into_iter()
        .enumerate()
        .map(|(position, arg)| {
            if !string_positions.contains(&position) {
                return Ok(arg);
            }
            let mut wrapped = wrap_with_cast_as_string(arg, connection)?;
            if is_newly_built_cast(&wrapped) {
                resolver.fold_constant(&mut wrapped, ConstantFoldMode::Normal);
            }
            Ok(wrapped)
        })
        .collect::<Result<Vec<_>, EvalError>>()?;
    Ok(wrap_binary_literals(
        name,
        &derived.charset,
        &derived.collation,
        tidb_datatype::EvalType::Int,
        args,
        |expression| resolver.fold_constant(expression, ConstantFoldMode::Normal),
    ))
}

/// Whether `expression` is a cast `BuildCastFunctionWithCheck` just built and
/// closed with its `FoldConstant`. Go leaves a cast whose eval type is JSON
/// unfolded ("we may reset the flag of the field type of CastAsJson later"),
/// so `json_contains(j, cast("1", json BINARY))` keeps the cast in a plan.
fn is_newly_built_cast(expression: &Expression) -> bool {
    matches!(
        expression,
        Expression::ScalarFunction(function)
            if function.func_name.lowercase().starts_with("cast_")
                && function
                    .ret_type
                    .as_ref()
                    .is_none_or(|tp| tp.eval_type() != EvalType::Json)
    )
}

fn binary_expression(
    op: BinaryOp,
    left: Expression,
    right: Expression,
    resolver: &impl ColumnResolver,
) -> Result<Expression, EvalError> {
    let name = binary_op_name(op);
    let ret_type = crate::builtin_arithmetic::infer_arithmetic_type_with_context(
        name,
        &left,
        &right,
        resolver.no_unsigned_subtraction(),
        resolver.div_precision_increment(),
    )
    .or_else(|| crate::builtin_compare::infer_compare_type(name))
    .or_else(|| crate::builtin_op::infer_op_type(name));
    match ret_type {
        Some(mut ret_type) => {
            let mut args = vec![left, right];
            // `intdiv` keeps its virtual operand casts (see
            // `numeric_operand_domain`) until its diagnostics read real ones.
            if matches!(
                name,
                "bitand" | "bitor" | "bitxor" | "leftshift" | "rightshift"
            ) {
                args = signature_args::wrap_builtin_arguments(
                    name,
                    args,
                    resolver.connection_charset_info(),
                    &|argument| resolver.fold_constant(argument, ConstantFoldMode::Normal),
                    Some(&ret_type),
                )?;
            }
            if matches!(name, "plus" | "minus" | "mul" | "div" | "mod") {
                match ret_type.eval_type() {
                    tidb_datatype::EvalType::Decimal => {
                        for argument in &mut args {
                            crate::builtin_compare::wrap_integer_operand_as_decimal(argument);
                            // Go `WrapWithCastAsDecimal` also casts a temporal
                            // or string operand: `d + dt` plans as
                            // `plus(d, cast(dt, decimal(19,0)))`.
                            if !is_hybrid_argument(argument)
                                && argument.static_type().map(FieldType::eval_type)
                                    != Some(EvalType::Decimal)
                            {
                                *argument =
                                    crate::aggregation::wrap_cast::wrap_with_cast_as_decimal(
                                        std::mem::replace(
                                            argument,
                                            Expression::Constant(Constant::new_null()),
                                        ),
                                    )?;
                                if is_newly_built_cast(argument) {
                                    resolver.fold_constant(argument, ConstantFoldMode::Normal);
                                }
                            }
                        }
                    }
                    // GO's arithmeticFunctionClass.getFunction coerces BOTH
                    // operands to the arithmetic domain through
                    // newBaseBuiltinFuncWithTp: either real -> [ETReal,
                    // ETReal] (q17 renders `div(Column#, cast(Column#,
                    // double BINARY))`), either decimal -> [ETDecimal,
                    // ETDecimal], else [ETInt, ETInt] -- which is why
                    // q72's `d_date + 5` renders the date operand as
                    // `cast(d_date, bigint BINARY)`.
                    tidb_datatype::EvalType::Real => {
                        for argument in &mut args {
                            // GO's WrapWithCastAsReal early-returns for a
                            // hybrid argument (hex/bit literals keep their
                            // raw-bytes semantics in arithmetic), so a hybrid
                            // operand never gets a cast node.
                            if is_hybrid_argument(argument) {
                                continue;
                            }
                            // The placeholder is replaced by the wrap result
                            // before it can ever be read.
                            *argument = crate::aggregation::wrap_cast::wrap_with_cast_as_real(
                                std::mem::replace(
                                    argument,
                                    Expression::Constant(Constant::new_null()),
                                ),
                            )?;
                            // GO's BuildCastFunction folds its newly-built
                            // cast over a strict constant at the same
                            // boundary; a wrap that early-returned leaves the
                            // argument untouched and needs no fold.
                            if is_newly_built_cast(argument) {
                                resolver.fold_constant(argument, ConstantFoldMode::Normal);
                            }
                        }
                    }
                    tidb_datatype::EvalType::Decimal => {
                        for argument in &mut args {
                            if is_hybrid_argument(argument) {
                                continue;
                            }
                            *argument = crate::aggregation::wrap_cast::wrap_with_cast_as_decimal(
                                std::mem::replace(
                                    argument,
                                    Expression::Constant(Constant::new_null()),
                                ),
                            )?;
                            if is_newly_built_cast(argument) {
                                resolver.fold_constant(argument, ConstantFoldMode::Normal);
                            }
                        }
                    }
                    tidb_datatype::EvalType::Int => {
                        for argument in &mut args {
                            if is_hybrid_argument(argument) {
                                continue;
                            }
                            *argument = crate::aggregation::wrap_cast::wrap_with_cast_as_int(
                                std::mem::replace(
                                    argument,
                                    Expression::Constant(Constant::new_null()),
                                ),
                                None,
                            )?;
                            if is_newly_built_cast(argument) {
                                resolver.fold_constant(argument, ConstantFoldMode::Normal);
                            }
                        }
                    }
                    _ => {}
                }
            }
            // GO `logicAnd/logicOr/logicXorFunctionClass.getFunction`
            // (builtin_op.go:65/128/191): every argument runs through
            // `wrapWithIsTrue(ctx, true, arg, false)` — an ETInt argument
            // passes through unwrapped (wrapForInt=false); anything else is
            // wrapped in `istrue_with_null`, whose getFunction prices a
            // string/time/json argument through ETReal — the base builder
            // emits `cast(arg, double BINARY)` (q84's `||` chains render
            // `or(istrue_with_null(cast(coalesce(..), double BINARY)), 0)`).
            if matches!(name, "and" | "or" | "xor") {
                for argument in &mut args {
                    let operand = std::mem::replace(argument, Expression::Constant(Constant::new_null()));
                    *argument = wrap_with_is_true_with_null(operand, resolver)?;
                }
            }
            // Go `adjustNullFlagForReturnType` runs on the arguments AFTER the
            // implicit casts wrapped them.
            result_type::stamp_builtin_return_flags(name, &args, &mut ret_type);
            if crate::builtin_compare::infer_compare_type(name).is_some() {
                if let Some(ctx) = resolver.comparison_context() {
                    let mut expression = Expression::ScalarFunction(ScalarFunction::new(
                        CiString::new(name),
                        ret_type.clone(),
                        args,
                    ));
                    crate::builtin_compare::refine_comparison_dyn(&mut expression, ctx)?;
                    // Go `NewFunction` folds the comparison it built: an
                    // always-false refinement (`refineArgs` returning `0, 1`
                    // for `int_not_null = 0.12`) becomes the constant 0. This
                    // tier settles a string comparison's collation after
                    // construction, so only a comparison of two plain INT
                    // constants -- which no collation can change -- folds
                    // here.
                    let integer_constants = matches!(
                        &expression,
                        Expression::ScalarFunction(function)
                            if function.args.iter().all(|argument| matches!(
                                argument,
                                Expression::Constant(constant)
                                    if constant.param_marker.is_none()
                                        && constant.deferred_expr.is_none()
                                        && matches!(constant.value, tidb_datatype::Datum::Int(_))
                            ))
                    );
                    if integer_constants {
                        resolver.fold_constant(&mut expression, ConstantFoldMode::Normal);
                    }
                    return Ok(expression);
                } else {
                    crate::builtin_compare::refine_integer_comparison_for_rewrite(name, &mut args);
                }
                crate::builtin_compare::prepare_json_comparison_args(&mut args);
                crate::builtin_compare::wrap_comparison_arguments_with_fold(
                    &mut args,
                    resolver.connection_charset_info(),
                    |argument| resolver.fold_constant(argument, ConstantFoldMode::Normal),
                )?;
            }
            let mut function = ScalarFunction::new(CiString::new(name), ret_type, args);
            function.prepare_numeric_arguments(
                resolver.comparison_context().unwrap_or(&crate::NoColumns),
            );
            Ok(Expression::ScalarFunction(function))
        }
        None => Ok(scalar(name, vec![left, right])),
    }
}

/// Go `ResolveType4Between` (`pkg/expression/builtin_compare.go:395`), which
/// resolves one comparison domain across all three BETWEEN operands before
/// the two binary comparisons are built.
fn resolve_type4_between(args: [&Expression; 3]) -> EvalType {
    let eval_type = |expr: &Expression| {
        expr.static_type()
            .map_or(EvalType::String, FieldType::eval_type)
    };
    let base_cmp_type = |lhs: EvalType, rhs: EvalType| {
        if lhs.is_string_kind() && rhs.is_string_kind() {
            EvalType::String
        } else if lhs == EvalType::Int && rhs == EvalType::Int {
            EvalType::Int
        } else if (lhs == EvalType::Decimal && rhs == EvalType::String)
            || (lhs == EvalType::String && rhs == EvalType::Decimal)
        {
            EvalType::Real
        } else if (lhs == EvalType::Int || lhs == EvalType::Decimal)
            && (rhs == EvalType::Int || rhs == EvalType::Decimal)
        {
            EvalType::Decimal
        } else {
            EvalType::Real
        }
    };

    let mut cmp_type = eval_type(args[0]);
    for arg in &args[1..] {
        cmp_type = base_cmp_type(cmp_type, eval_type(arg));
    }
    if cmp_type == EvalType::String {
        if args[0]
            .static_type()
            .is_some_and(|field_type| field_type.code() == FieldTypeCode::Duration)
        {
            cmp_type = EvalType::Duration;
        } else if args.iter().any(|arg| {
            arg.static_type()
                .is_some_and(|field_type| field_type.code().is_type_temporal())
        }) {
            cmp_type = EvalType::Datetime;
        }
    }
    if args.iter().all(|arg| {
        arg.static_type()
            .is_some_and(|field_type| field_type.eval_type() == EvalType::Int)
            || matches!(
                arg,
                Expression::Constant(constant)
                    if matches!(&constant.value, Datum::BinaryLiteral(_))
            )
    }) {
        return EvalType::Int;
    }
    cmp_type
}

/// Go `newBaseBuiltinFuncWithFieldTypes`' implicit cast for one CASE branch:
/// every THEN/ELSE argument is wrapped in the MERGED control type. Decimal,
/// datetime, timestamp, and duration targets take the FULL merged type
/// (`flen`/`decimal`), which is what turns a folded `0` into `0.0000`; the
/// other families use the same `WrapWithCastAs*` helpers Go calls.
///
/// The flag says whether a cast was built: Go folds only a cast it just built
/// (`BuildCastFunctionWithCheck`), never an argument it passes through.
fn wrap_case_branch(
    branch: Expression,
    target: &FieldType,
    connection: (&str, &str),
) -> Result<(Expression, bool), EvalError> {
    // Go `FieldType.Equal`: every flag but UNSIGNED is ignored, so a branch
    // that merely differs in NOT NULL is passed through uncast.
    let already = branch
        .static_type()
        .is_some_and(|source| source.equal(target));
    let was_cast = is_newly_built_cast(&branch);
    let wrapped = match target.eval_type() {
        EvalType::Int => wrap_with_cast_as_int(branch, Some(target))?,
        EvalType::Real => wrap_with_cast_as_real(branch)?,
        EvalType::Decimal | EvalType::Datetime | EvalType::Timestamp | EvalType::Duration => {
            if already {
                return Ok((branch, false));
            }
            crate::simple_expr::build_cast_function(branch, target.clone(), false)?
        }
        EvalType::String => wrap_with_cast_as_string(branch, connection)?,
        EvalType::Json => crate::aggregation::wrap_cast::wrap_with_cast_as_json(branch)?,
        EvalType::VectorFloat32 => return Ok((branch, false)),
    };
    let built = is_newly_built_cast(&wrapped) && !was_cast;
    Ok((wrapped, built))
}

/// Applies Go's `wrapExpWithCast` to all three BETWEEN operands. The common
/// type is deliberately chosen once, rather than allowing the lower and upper
/// comparisons to infer independently from their two arms.
fn wrap_between_arguments(
    args: [&Expression; 3],
    cmp_type: EvalType,
    connection: (&str, &str),
) -> Result<[Expression; 3], EvalError> {
    let [expr, low, high] = args;
    match cmp_type {
        EvalType::Int => Ok([
            wrap_with_cast_as_int(expr.clone(), None)?,
            wrap_with_cast_as_int(low.clone(), None)?,
            wrap_with_cast_as_int(high.clone(), None)?,
        ]),
        EvalType::Real => Ok([
            wrap_with_cast_as_real(expr.clone())?,
            wrap_with_cast_as_real(low.clone())?,
            wrap_with_cast_as_real(high.clone())?,
        ]),
        EvalType::Decimal => Ok([
            wrap_with_cast_as_decimal(expr.clone())?,
            wrap_with_cast_as_decimal(low.clone())?,
            wrap_with_cast_as_decimal(high.clone())?,
        ]),
        EvalType::String => Ok([
            wrap_with_cast_as_string(expr.clone(), connection)?,
            wrap_with_cast_as_string(low.clone(), connection)?,
            wrap_with_cast_as_string(high.clone(), connection)?,
        ]),
        EvalType::Duration => Ok([
            wrap_with_cast_as_duration(expr.clone())?,
            wrap_with_cast_as_duration(low.clone())?,
            wrap_with_cast_as_duration(high.clone())?,
        ]),
        EvalType::Datetime => {
            let target = FieldType::new(FieldTypeCode::Datetime);
            Ok([
                wrap_with_cast_as_time(expr.clone(), target.clone())?,
                wrap_with_cast_as_time(low.clone(), target.clone())?,
                wrap_with_cast_as_time(high.clone(), target)?,
            ])
        }
        // ResolveType4Between returns one of the six cases above for all
        // currently supported scalar field types. Preserve the source shape
        // if a future type reaches this narrow adapter before its Go cast is
        // transcreated.
        EvalType::Timestamp | EvalType::Json | EvalType::VectorFloat32 => {
            Ok([expr.clone(), low.clone(), high.clone()])
        }
    }
}

fn row_len(expr: &Expr) -> usize {
    match expr {
        Expr::Row(items) => items.len(),
        _ => 1,
    }
}

/// Go `expressionRewriter.constructBinaryOpFunction`: scalar comparisons stay
/// scalar; same-arity row comparisons are expanded into AND/OR trees.
fn rewrite_comparison(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    resolver: &impl ColumnResolver,
) -> Result<Expression, EvalError> {
    match (left, right) {
        (Expr::Row(left), Expr::Row(right)) => rewrite_row_comparison(op, left, right, resolver),
        // go's ctxStack holds BUILT expressions by the time
        // `constructBinaryOpFunction` runs: one side a row, the other a
        // folded multi-column scalar subquery's `row` function
        // (`(a,b) = (SELECT a,b ...)` with an uncorrelated subquery) is a
        // row comparison over the built sides (oracle g-subq: empty result).
        (Expr::Row(left), other) | (other, Expr::Row(left)) => {
            let built = rewrite_expr_resolved(other, resolver)?;
            let (row_on_left, right_args) = match &built {
                Expression::ScalarFunction(function)
                    if function.func_name.lowercase() == "row"
                        && function.get_args().len() == left.len() =>
                {
                    (true, function.get_args().to_vec())
                }
                _ => return Err(EvalError::OperandColumns(left.len())),
            };
            let mut left_args = Vec::with_capacity(left.len());
            for element in left {
                left_args.push(rewrite_expr_resolved(element, resolver)?);
            }
            // Keep the operator's orientation: the row's element stays on the
            // side it was written on.
            let (lhs, rhs): (&Vec<Expression>, &Vec<Expression>) = if row_on_left {
                (&left_args, &right_args)
            } else {
                (&right_args, &left_args)
            };
            row_comparison_of_built(op, lhs, rhs, resolver)
        }
        _ => binary_expression(
            op,
            rewrite_expr_resolved(left, resolver)?,
            rewrite_expr_resolved(right, resolver)?,
            resolver,
        ),
    }
}

/// go `constructBinaryOpFunction`'s row arms over BUILT expressions:
/// equality and NULL-safe equality are CNF, inequality is DNF, and ordering
/// is lexicographic DNF whose prefix comparisons use the STRICT form.
fn row_comparison_of_built(
    op: BinaryOp,
    left: &[Expression],
    right: &[Expression],
    resolver: &impl ColumnResolver,
) -> Result<Expression, EvalError> {
    let pair = |op: BinaryOp, i: usize| -> Result<Expression, EvalError> {
        binary_expression(op, left[i].clone(), right[i].clone(), resolver)
    };
    if matches!(op, BinaryOp::Eq | BinaryOp::Ne | BinaryOp::NullEq) {
        let join = if op == BinaryOp::Ne {
            BinaryOp::LogicOr
        } else {
            BinaryOp::LogicAnd
        };
        let first = pair(op, 0)?;
        return (1..left.len()).try_fold(first, |condition, i| {
            binary_expression(join, condition, pair(op, i)?, resolver)
        });
    }
    // Lexicographic DNF: positions before `i` are equal, position `i` decides.
    let mut alternatives = Vec::with_capacity(left.len());
    for i in 0..left.len() {
        let mut conj = Vec::with_capacity(i + 1);
        for j in 0..i {
            conj.push(pair(BinaryOp::Eq, j)?);
        }
        let decider = if i + 1 < left.len() {
            match op {
                BinaryOp::Ge => BinaryOp::Gt,
                BinaryOp::Le => BinaryOp::Lt,
                _ => op,
            }
        } else {
            op
        };
        conj.push(pair(decider, i)?);
        let first = conj.remove(0);
        alternatives.push(conj.into_iter().try_fold(first, |condition, term| {
            binary_expression(BinaryOp::LogicAnd, condition, term, resolver)
        })?);
    }
    let first = alternatives.remove(0);
    alternatives.into_iter().try_fold(first, |condition, term| {
        binary_expression(BinaryOp::LogicOr, condition, term, resolver)
    })
}

fn compose_comparisons(
    join: BinaryOp,
    comparisons: impl IntoIterator<Item = Result<Expression, EvalError>>,
    resolver: &impl ColumnResolver,
) -> Result<Expression, EvalError> {
    let mut comparisons = comparisons.into_iter();
    let first = comparisons
        .next()
        .ok_or(EvalError::Unsupported("a row expression with no columns"))??;
    comparisons.try_fold(first, |condition, comparison| {
        binary_expression(join, condition, comparison?, resolver)
    })
}

/// Go `constructBinaryOpFunction` for row comparisons. Equality and NULL-safe
/// equality are CNF, inequality is DNF, and ordering is lexicographic DNF.
fn rewrite_row_comparison(
    op: BinaryOp,
    left: &[Expr],
    right: &[Expr],
    resolver: &impl ColumnResolver,
) -> Result<Expression, EvalError> {
    if left.len() != right.len() {
        return Err(EvalError::OperandColumns(left.len()));
    }
    if matches!(op, BinaryOp::Eq | BinaryOp::Ne | BinaryOp::NullEq) {
        let join = if op == BinaryOp::Ne {
            BinaryOp::LogicOr
        } else {
            BinaryOp::LogicAnd
        };
        return compose_comparisons(
            join,
            left.iter()
                .zip(right)
                .map(|(left, right)| rewrite_comparison(op, left, right, resolver)),
            resolver,
        );
    }

    let comparisons = (0..left.len()).map(|index| {
        let mut prefix = (0..index)
            .map(|prefix| rewrite_comparison(BinaryOp::Eq, &left[prefix], &right[prefix], resolver))
            .collect::<Vec<_>>();
        let current_op = if index + 1 < left.len() {
            match op {
                BinaryOp::Ge => BinaryOp::Gt,
                BinaryOp::Le => BinaryOp::Lt,
                _ => op,
            }
        } else {
            op
        };
        prefix.push(rewrite_comparison(
            current_op,
            &left[index],
            &right[index],
            resolver,
        ));
        compose_comparisons(BinaryOp::LogicAnd, prefix, resolver)
    });
    compose_comparisons(BinaryOp::LogicOr, comparisons, resolver)
}

/// Go `expression.CheckArgsNotMultiColumnRow` at a rewriter step: an
/// operator that takes one value refuses a row (`ErrOperandColumns`, 1241).
fn ensure_single_column(args: &[&Expression]) -> Result<(), EvalError> {
    if args
        .iter()
        .any(|arg| crate::expr_util::get_row_len(arg) != 1)
    {
        return Err(EvalError::OperandColumns(1));
    }
    Ok(())
}

/// Rewrites a parsed AST expression without a column scope.
pub fn rewrite_expr(expr: &Expr) -> Result<Expression, EvalError> {
    rewrite_expr_resolved(expr, &NoResolver)
}

/// Go BuildGetVarFunction and the get*Var function classes.
fn user_variable_read_type(declared: &FieldType) -> (&'static str, FieldType) {
    use tidb_datatype::EvalType;
    let (kind, code) = match declared.eval_type() {
        EvalType::Int if declared.is_unsigned() => ("uint", FieldTypeCode::LongLong),
        EvalType::Int => ("int", FieldTypeCode::LongLong),
        EvalType::Real => ("real", FieldTypeCode::Double),
        EvalType::Decimal => ("decimal", FieldTypeCode::NewDecimal),
        EvalType::Datetime => ("time", declared.code()),
        _ => ("string", FieldTypeCode::VarString),
    };
    let mut result = FieldType::new(code);
    result.set_flen_under_limit(declared.flen());
    result.set_decimal(tidb_datatype::UNSPECIFIED_LENGTH);
    match declared.eval_type() {
        EvalType::Int => {
            result.set_raw_flags(declared.raw_flags());
            result.set_decimal(0);
        }
        EvalType::Datetime => {
            result.set_decimal(declared.decimal());
        }
        _ if kind == "string" => {
            if !declared.charset_name().is_empty() {
                result.set_charset_name(declared.charset_name());
                result.set_collation_name(declared.collation_name());
            }
        }
        _ => {}
    }
    (kind, result)
}

/// [`rewrite_expr`] with column resolution: `Expr::Column` paths are bound
/// through `resolver` into [`Expression::Column`] nodes (index + result type).
pub fn rewrite_expr_resolved(
    expr: &Expr,
    resolver: &impl ColumnResolver,
) -> Result<Expression, EvalError> {
    // Go's rewriter walks arbitrarily nested expressions because a
    // goroutine's stack grows on demand. This is that semantics for a Rust
    // thread: when fewer than 128 KB remain, the walk continues on a fresh
    // 4 MB segment instead of aborting the process. The red zone covers the
    // deepest single step below ([`rewrite_leaf`]'s split keeps one level
    // under ~30 KB in a debug build); the check itself is a few instructions
    // on the non-growing path.
    stacker::maybe_grow(128 * 1024, 4 * 1024 * 1024, || {
        rewrite_expr_resolved_inner(expr, resolver)
    })
}

fn rewrite_expr_resolved_inner(
    expr: &Expr,
    resolver: &impl ColumnResolver,
) -> Result<Expression, EvalError> {
    if let Expr::Default(Some(path)) = expr {
        if let Some(result) = resolver.resolve_default_in_query(path) {
            return result;
        }
        return resolver
            .resolve_default(path)
            // Go `expression_rewriter.go:1835`, with neither a plan nor a
            // source table to read the named column's default from.
            .ok_or(EvalError::Unsupported(
                "Unsupported expr *ast.DefaultExpr when source table not provided",
            ));
    }
    if let Expr::Column(path) = expr {
        if let Some(constant) = resolver.resolve_constant(path) {
            return Ok(constant);
        }
        return resolver.resolve_expression(path).ok_or_else(|| {
            EvalError::UnknownColumnInClause(path.join("."), resolver.clause_message())
        });
    }
    let mut built = rewrite_leaf(expr, resolver)?;
    // Go stamps every builtin's return type as it is constructed (binary on a
    // non-string result, NOT NULL by `adjustNullFlagForReturnType`); the arms
    // above that build their own `ScalarFunction` are covered here, on the
    // node this call produced -- its operands were stamped by their own
    // rewrites.
    if let Expression::ScalarFunction(function) = &mut built {
        if let Some(ret_type) = function.ret_type.as_mut() {
            result_type::stamp_builtin_return_flags(
                &function.func_name.lowercase().to_owned(),
                &function.args,
                ret_type,
            );
        }
    }
    derive_tree_collation_with_connection(&mut built, resolver.connection_charset_info())?;
    // Go FuncCastExpr calls BuildCastFunctionWithCheck directly, outside
    // newFunctionWithInit's try/disabled-fold counters. JSON casts alone
    // stay unfolded because their evaluation flags can still change.
    let fold_mode = match expr {
        Expr::Cast(cast) if matches!(cast.cast_type, tidb_ast::CastType::Json) => {
            ConstantFoldMode::Disabled
        }
        Expr::Cast(_) => ConstantFoldMode::Normal,
        _ => resolver.fold_mode(),
    };
    resolver.fold_constant(&mut built, fold_mode);
    prepare_in_string_hash_sets(&mut built);
    Ok(built)
}

/// The cast's target type with its character set settled: `BINARY`, a named
/// charset with its default collation, or the connection's.
fn resolved_cast_target(
    cast: &tidb_ast::CastExpr,
    resolver: &impl ColumnResolver,
) -> Result<(&'static str, FieldType), EvalError> {
    let (name, mut ret_type) = cast_target(&cast.cast_type).ok_or(EvalError::Unsupported(
        "this CAST target type has no value domain yet",
    ))?;
    // `BINARY expr` (Go `expr_prefix_parser.go`) casts to `TypeString` with
    // the binary charset and no length, where `CAST(expr AS BINARY)` is
    // `TypeVarString`.
    if matches!(cast.style, tidb_ast::CastStyle::BinaryOperator) {
        ret_type = FieldType::new(FieldTypeCode::String);
        set_binary_charset(&mut ret_type);
    }
    if let tidb_ast::CastType::Char { charset, .. } = &cast.cast_type {
        match charset.as_deref() {
            Some("BINARY" | "binary") => set_binary_charset(&mut ret_type),
            Some(charset) => {
                let charset = tidb_datatype::Charset::from_name(charset)
                    .ok_or(EvalError::Unsupported("unknown CAST character set"))?;
                ret_type.set_charset_name(charset.name());
                ret_type.set_collation_name(charset.default_collation().name());
            }
            None => {
                let (charset, collation) = resolver.connection_charset_info();
                ret_type.set_charset_name(charset);
                ret_type.set_collation_name(collation);
            }
        }
    }
    Ok((name, ret_type))
}

/// Go `castAsArrayFunctionClass` (`builtin_cast.go`), reached through the
/// rewriter's `allowBuildCastArray` gate: `CAST(json AS <type> ARRAY)` builds
/// a JSON array of `<type>` values, the key part of a multi-valued index.
fn rewrite_cast_array(
    cast: &tidb_ast::CastExpr,
    resolver: &impl ColumnResolver,
) -> Result<Expression, EvalError> {
    if !resolver.allow_build_cast_array() {
        return Err(EvalError::NotImplemented(
            "Use of CAST( .. AS .. ARRAY) outside of functional index in CREATE(non-SELECT)/ALTER TABLE or in general expressions"
                .into(),
        ));
    }
    validate_cast_type(cast)?;
    let (_, element) = resolved_cast_target(cast, resolver)?;
    let arg = rewrite_expr_resolved(&cast.expr, resolver)?;
    // `verifyArgs`: "args[0].GetType(ctx).EvalType() != types.ETJson".
    if arg.static_type().map(FieldType::eval_type) != Some(tidb_datatype::EvalType::Json) {
        return Err(EvalError::Json(JsonError::InvalidTypeForJson {
            argument: 1,
            function: "cast_as_array",
        }));
    }
    if matches!(
        element.code(),
        FieldTypeCode::Year | FieldTypeCode::Json | FieldTypeCode::Float | FieldTypeCode::NewDecimal
    ) {
        return Err(EvalError::NotImplemented(
            format!("CAST-ing data to array of {}", element.source_string()).into(),
        ));
    }
    if element.eval_type() == tidb_datatype::EvalType::String {
        if !matches!(element.charset_name(), "utf8mb4" | "binary") {
            return Err(EvalError::NotImplemented(
                "specifying charset for multi-valued index".into(),
            ));
        }
        if element.flen() == tidb_datatype::UNSPECIFIED_LENGTH {
            return Err(EvalError::NotImplemented(
                "CAST-ing data to array of char/binary BLOBs with unspecified length".into(),
            ));
        }
    }
    Ok(Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("cast_array"),
        element.with_array(true),
        vec![arg],
    )))
}

/// Go `BuildJSONSumCrc32FunctionWithCheck` (`builtin_json.go`), the
/// rewriter's `*ast.JSONSumCrc32Expr` arm: `JSON_SUM_CRC32(json AS <type>
/// ARRAY)` is a `BIGINT` that sums the CRC32 of the array's elements, each
/// converted to `<type>`. It is not a `CAST(... AS ... ARRAY)`: no
/// functional-index context is needed to build it.
fn rewrite_json_sum_crc32(
    cast: &tidb_ast::CastExpr,
    resolver: &impl ColumnResolver,
) -> Result<Expression, EvalError> {
    validate_cast_type(cast)?;
    let (_, element) = resolved_cast_target(cast, resolver)?;
    let arg = rewrite_expr_resolved(&cast.expr, resolver)?;
    let mut array_type = element.clone().with_array(true);
    // "If source argument's nullable, then target type should be nullable".
    if !arg
        .static_type()
        .is_some_and(|ft| ft.has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL))
    {
        array_type.del_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    }
    // `jsonSumCRC32FunctionClass.verifyArgs`.
    if arg.static_type().map(FieldType::eval_type) != Some(tidb_datatype::EvalType::Json) {
        return Err(EvalError::Json(JsonError::InvalidTypeForJson {
            argument: 1,
            function: "JSON_SUM_CRC32",
        }));
    }
    if matches!(
        element.code(),
        FieldTypeCode::Year
            | FieldTypeCode::Json
            | FieldTypeCode::Float
            | FieldTypeCode::NewDecimal
    ) {
        return Err(EvalError::NotImplemented(
            format!(
                "calculating json_sum_crc32 on array of {}",
                element.source_string()
            )
            .into(),
        ));
    }
    if element.eval_type() == tidb_datatype::EvalType::String {
        if !matches!(element.charset_name(), "utf8mb4" | "binary") {
            return Err(EvalError::NotImplemented("unsupported charset".into()));
        }
        if element.flen() == tidb_datatype::UNSPECIFIED_LENGTH {
            return Err(EvalError::NotImplemented(
                "calculating json_sum_crc32 on array of char/binary BLOBs with unspecified length"
                    .into(),
            ));
        }
    }
    let mut ret_type = FieldType::new(FieldTypeCode::LongLong);
    result_type::set_binary_charset(&mut ret_type);
    let mut node = Expression::ScalarFunction(ScalarFunction::new_json_sum_crc32(
        arg, ret_type, array_type,
    ));
    // `jsonSumFunction.SetCoercibility(CoercibilityNumeric)` and
    // `SetRepertoire(ASCII)`.
    crate::collation_derive::apply_derived_collation(
        &mut node,
        &crate::expr_collation::ExprCollation {
            coer: crate::expr_collation::Coercibility::NUMERIC,
            repe: crate::expr_collation::Repertoire::ASCII,
            charset: "binary".to_owned(),
            collation: "binary".to_owned(),
        },
    );
    Ok(node)
}

fn prepare_in_string_hash_sets(expr: &mut Expression) {
    let Expression::ScalarFunction(function) = expr else {
        return;
    };
    for argument in &mut function.args {
        prepare_in_string_hash_sets(argument);
    }
    function.prepare_in_string_hash_set();
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
    derive_tree_collation_with_connection(expr, crate::collation_derive::connection_charset_info())
}

fn derive_tree_collation_with_connection(
    expr: &mut Expression,
    connection: (&str, &str),
) -> Result<(), EvalError> {
    let Expression::ScalarFunction(func) = expr else {
        return Ok(());
    };
    for arg in &mut func.args {
        derive_tree_collation_with_connection(arg, connection)?;
    }
    if func.collation.has_coercibility() {
        return Ok(());
    }
    let ret_type = func
        .ret_type
        .as_ref()
        .map_or(tidb_datatype::EvalType::Int, FieldType::eval_type);
    let name = func.func_name.lowercase().to_owned();
    let ec = crate::collation_derive::derive_collation_with_connection(
        &name, &func.args, ret_type, connection,
    )?;
    crate::collation_derive::apply_derived_collation(expr, &ec);
    // Go `regexpSubstr/InStr/ReplaceFunctionClass.getFunction`: a call whose
    // derived charset and collation are both `binary` is refused when it is
    // built (`regexp_like` is not).
    if matches!(
        name.as_str(),
        "regexp_substr" | "regexp_instr" | "regexp_replace"
    ) && ec.charset == "binary"
        && ec.collation == "binary"
    {
        return Err(crate::regexp::err_regexp(crate::regexp::BINARY_COLLATE_ERR));
    }
    // Some getFunction implementations overwrite generic derivation with a
    // fixed result charset. Reapply those source-owned result rules last.
    restore_function_result_charset(expr)?;
    Ok(())
}

/// The dispatcher over `Expr`'s shapes, holding only the two that a NESTED
/// predicate walks through -- `(...)` and a binary operator -- and handing
/// every other shape to one of the three functions below.
///
/// The split is about STACK, not structure. A debug build gives one frame a
/// slot for every match arm's locals at once, so the single 33-arm match this
/// was carried a 174 KB frame; this function recurses once per level of a
/// nested predicate, so `... on ((c=1 and ((a=3 and a=3) or (a=4 and a=4)))
/// or (a=2 and a=2))` -- eight levels -- overflowed a 2 MB stack while
/// PLANNING and aborted the process. Measured after the split: 25 KB per
/// level, and that same predicate nests fifteen deep before it runs out.
///
/// Go has no equivalent limit: a goroutine's stack grows on demand, so its
/// planner walks an arbitrarily nested expression. Anything that shrinks
/// these frames therefore moves this tier toward Go rather than merely
/// tuning it.
fn rewrite_leaf(expr: &Expr, resolver: &impl ColumnResolver) -> Result<Expression, EvalError> {
    match expr {
        Expr::Paren(inner) => rewrite_expr_resolved(inner, resolver),
        Expr::Binary(op, lhs, rhs) => {
            if matches!(
                op,
                BinaryOp::Eq
                    | BinaryOp::Ne
                    | BinaryOp::NullEq
                    | BinaryOp::Lt
                    | BinaryOp::Le
                    | BinaryOp::Gt
                    | BinaryOp::Ge
            ) && (matches!(lhs.as_ref(), Expr::Row(_)) || matches!(rhs.as_ref(), Expr::Row(_)))
            {
                return rewrite_comparison(*op, lhs, rhs, resolver);
            }
            let mode = if matches!(op, BinaryOp::LogicAnd | BinaryOp::LogicOr) {
                ConstantFoldMode::Try
            } else {
                ConstantFoldMode::Normal
            };
            let child_resolver = FoldModeResolver::new(resolver, mode);
            let resolver = &child_resolver;
            let left = rewrite_expr_resolved(lhs, resolver)?;
            let right = rewrite_expr_resolved(rhs, resolver)?;
            // Go `binaryOpToExpression`: only a comparison takes a row, and
            // `constructBinaryOpFunction` compares rows of one arity element
            // by element (a folded scalar subquery is a built row).
            if matches!(
                op,
                BinaryOp::Eq
                    | BinaryOp::Ne
                    | BinaryOp::NullEq
                    | BinaryOp::Lt
                    | BinaryOp::Le
                    | BinaryOp::Gt
                    | BinaryOp::Ge
            ) {
                let (l_len, r_len) = (
                    crate::expr_util::get_row_len(&left),
                    crate::expr_util::get_row_len(&right),
                );
                if l_len != r_len {
                    return Err(EvalError::OperandColumns(l_len));
                }
                if let (Expression::ScalarFunction(l_row), Expression::ScalarFunction(r_row)) =
                    (&left, &right)
                {
                    if l_len != 1 {
                        return row_comparison_of_built(
                            *op,
                            l_row.get_args(),
                            r_row.get_args(),
                            resolver,
                        );
                    }
                }
            } else {
                ensure_single_column(&[&left, &right])?;
            }
            // Result types come from the transcreated function classes:
            // builtin_arithmetic (plus/minus/mul/div/intdiv/mod),
            // builtin_compare (eq/nulleq/ne/lt/le/gt/ge) and builtin_op
            // (logic and bit operators). Anything still uncovered keeps the
            // LongLong placeholder.
            let built = binary_expression(*op, left, right, resolver)?;
            Ok(built)
        }
        Expr::Int(_)
        | Expr::Float(_)
        | Expr::Bool(_)
        | Expr::Null
        | Expr::ParamMarker { .. }
        | Expr::String(_)
        | Expr::RawString(_)
        | Expr::Decimal(_)
        | Expr::Hex(_)
        | Expr::Bit(_)
        | Expr::CharsetBinary { .. }
        | Expr::Assign { .. }
        | Expr::UserVar(_)
        | Expr::Collate { .. }
        | Expr::CharsetString { .. } => rewrite_leaf_literal(expr, resolver),
        Expr::In { .. }
        | Expr::Between { .. }
        | Expr::Like { .. }
        | Expr::Regexp { .. }
        | Expr::Case { .. }
        | Expr::MemberOf { .. }
        | Expr::Is { .. }
        | Expr::Unary(..) => rewrite_leaf_compound(expr, resolver),
        _ => rewrite_leaf_call(expr, resolver),
    }
}

/// [`rewrite_leaf`]'s arms for LITERALS and the single-value wrappers, in a function of their own.
///
/// The split is about STACK, not structure. A debug build gives one frame a
/// slot for every arm's locals at once, so a single 33-arm match over `Expr`
/// carried a 174 KB frame -- and `rewrite_leaf` recurses once per level of a
/// nested predicate, which made eight levels of `AND`/`OR` overflow a 2 MB
/// stack while planning. Each group pays only its own arms now, and the
/// dispatcher that recurses pays almost nothing.
#[inline(never)]
fn rewrite_leaf_literal(
    expr: &Expr,
    resolver: &impl ColumnResolver,
) -> Result<Expression, EvalError> {
    match expr {
        Expr::ParamMarker {
            order,
            in_execute,
            value,
            ..
        } => {
            // Go ParamMarkerExpression reads the marker's current Datum at
            // construction; runtime Constant evaluation reads this execution's
            // parameter context. Standalone resolvers can supply an uninstalled
            // marker directly without changing its absolute order.
            let datum = match (in_execute, value.as_ref().and_then(|value| value.downcast_ref::<Datum>())) {
                (true, Some(value)) => value.clone(),
                _ => resolver.param_value(*order)?,
            };
            let value = crate::expr_util::ParamMarkerValue {
                datum,
                order: i64::try_from(*order)
                    .map_err(|_| EvalError::Unsupported("prepared parameter index overflow"))?,
            };
            Ok(Expression::Constant(
                crate::expr_util::param_marker_expression(&value, true, false, None),
            ))
        }
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
        Expr::Float(value) => Ok(Expression::Constant(Constant::new(
            Datum::Real(*value),
            result_type::float_literal_type(*value),
        ))),
        // A `TRUE`/`FALSE` literal is `0`/`1` typed `bigint(1)` with
        // `IsBooleanFlag` (Go `DefaultTypeForValue` -> `KindMysqlBool`), so
        // `JSON_ARRAY(true)` is `[true]`, not `[1]`.
        Expr::Bool(value) => Ok(Expression::Constant(Constant::new(
            Datum::Int(i64::from(*value)),
            result_type::bool_literal_type(),
        ))),
        Expr::Null => Ok(Expression::Constant(Constant::new(
            Datum::Null,
            result_type::null_literal_type(),
        ))),
        // Go `ast.NewValueExpr(value, "", "")`, a value the parser built
        // rather than a SQL literal (`SET NAMES`' charset): no introducer
        // and no connection charset.
        Expr::RawString(text) => Ok(constant_string(text)),
        Expr::String(text) => {
            // The literal's exact bytes: a client byte that is not UTF-8
            // travels in the text as its `raw_bytes` escape.
            let bytes = tidb_hack::raw_bytes::str_to_bytes(text).into_owned();
            let (charset, collation) = resolver.connection_charset_info();
            let mut field_type = FieldType::new(FieldTypeCode::VarString);
            field_type.set_flen(bytes.len() as i64);
            field_type.set_charset_name(charset);
            field_type.set_collation_name(collation);
            // Go `DefaultTypeForValue` adds NotNull for every non-nil value.
            field_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
            field_type.set_decimal(-1);
            let datum = Datum::new_collation_string(bytes, field_type.collation());
            Ok(Expression::Constant(Constant::new(datum, field_type)))
        }
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
        Expr::UserVar(name) => {
            use crate::user_vars::UserVarsReader;
            let name = tidb_hack::go_to_lower(name);
            let declared = resolver
                .user_vars()
                .and_then(|vars| vars.get_user_var_type(&name))
                .unwrap_or_else(|| {
                    let mut field = FieldType::new(FieldTypeCode::VarString);
                    field.set_flen(65_535);
                    field
                });
            let (kind, ret_type) = user_variable_read_type(&declared);
            let mut function = ScalarFunction::new(
                CiString::new(format!("getvar_{kind}")),
                ret_type.clone(),
                vec![constant_string(&name)],
            );
            function
                .collation
                .set_coercibility(crate::expr_collation::Coercibility::IMPLICIT);
            function
                .collation
                .set_repertoire(crate::expr_collation::Repertoire::UNICODE);
            function
                .collation
                .set_charset_and_collation(ret_type.charset_name(), ret_type.collation_name());
            Ok(Expression::ScalarFunction(function))
        }
        // Go rewriteUserVariable publishes the RHS type while building the
        // plan, even if no rows execute or the RHS later evaluates to NULL.
        Expr::Assign { name, value } => {
            let name = tidb_hack::go_to_lower(name);
            let value = rewrite_expr_resolved(value, resolver)?;
            let declared = value
                .static_type()
                .cloned()
                .ok_or(EvalError::Unsupported("user variable RHS has no type"))?;
            let args = vec![constant_string(&name), value];
            let ret_type = builtin_return_type("setvar", &args)
                .ok_or(EvalError::Unsupported("setvar has no result type"))?;
            if let Some(vars) = resolver.user_vars() {
                vars.set_user_var_type(&name, declared);
            }
            Ok(Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("setvar"),
                ret_type,
                args,
            )))
        }
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
            let is_json = arg
                .static_type()
                .is_some_and(|ft| ft.code() == FieldTypeCode::Json);
            // go's FieldType for every NON-string result (numerics,
            // temporals) carries Charset 'binary', so `LOCATE(...) COLLATE
            // utf8mb4_general_ci` is 1253 against 'binary' even though the
            // signature's placeholder type left the charset at its default.
            // A JSON value's charset is always utf8mb4.
            let arg_charset = arg
                .static_type()
                .map(|ft| {
                    if is_json {
                        "utf8mb4".to_owned()
                    } else if ft.is_string()
                        || matches!(ft.code(), FieldTypeCode::Enum | FieldTypeCode::Set)
                    {
                        // An ENUM/SET column keeps its declared charset too.
                        ft.charset_name().to_owned()
                    } else {
                        "binary".to_owned()
                    }
                })
                .unwrap_or_default();
            if !crate::collation_derive::collation_matches_charset(collation, &arg_charset) {
                return Err(EvalError::CollationCharsetMismatch {
                    collation: name,
                    charset: arg_charset,
                });
            }
            // Go: a COLUMN (or JSON) argument is wrapped in a CAST so the
            // column's own FieldType stays untouched -- which is what makes
            // `GROUP BY a COLLATE utf8mb4_bin` group by the cast rather than
            // by `a` under its declared collation. Constants and scalar
            // functions take the collation in place.
            if matches!(arg, Expression::Column(_)) || is_json {
                if arg
                    .static_type()
                    .is_some_and(|ft| matches!(ft.code(), FieldTypeCode::Enum | FieldTypeCode::Set))
                {
                    return Err(EvalError::NotImplemented(
                        "use collate clause for enum or set".into(),
                    ));
                }
                let mut expr_type = if is_json {
                    let mut long_blob = FieldType::new(FieldTypeCode::LongBlob);
                    long_blob.set_charset_name("utf8mb4");
                    long_blob
                } else {
                    arg.static_type()
                        .cloned()
                        .unwrap_or_else(|| FieldType::new(FieldTypeCode::VarString))
                };
                expr_type.set_collation_name(collation.name());
                arg = crate::simple_expr::build_cast_function(arg, expr_type, false)?;
            }
            crate::collation_derive::set_explicit_collation(&mut arg, collation);
            Ok(arg)
        }
        // A charset introducer (`_binary'a'`, `_latin1'x'`): Go's parser gives
        // the literal that charset and its legacy default collation, and marks
        // the result as explicitly introduced.
        Expr::CharsetString { charset, value } => {
            let charset = charset.to_ascii_lowercase();
            let mut collation_name = tidb_datatype::get_default_collation_legacy(&charset)
                .map_err(|_| EvalError::Unsupported("unknown character introducer"))?;
            // Go `adjustUTF8MB4Collation`: a `_utf8mb4` introducer takes the
            // session's `default_collation_for_utf8mb4`.
            if charset == "utf8mb4" {
                collation_name = resolver.default_collation_for_utf8mb4();
            }
            let collation = tidb_datatype::Collation::from_name(&collation_name)
                .ok_or(EvalError::Unsupported("unknown character introducer"))?;
            let bytes = tidb_hack::raw_bytes::str_to_bytes(value).into_owned();
            let mut ft = FieldType::new(FieldTypeCode::VarString);
            ft.set_flen(bytes.len() as i64);
            ft.set_charset_name(charset);
            ft.set_collation_name(collation_name);
            // Go `DefaultTypeForValue` adds NotNull for every non-nil value,
            // an introduced literal included: a view body is stored with its
            // strings as `_UTF8MB4'..'`, and its columns stay NOT NULL.
            ft.add_flags(
                tidb_datatype::FieldTypeFlags::UNDERSCORE_CHARSET
                    | tidb_datatype::FieldTypeFlags::NOT_NULL,
            );
            let datum = if collation == tidb_datatype::Collation::Binary {
                ft.add_flags(tidb_datatype::FieldTypeFlags::BINARY);
                Datum::Bytes(bytes)
            } else {
                Datum::new_collation_string(bytes, collation)
            };
            Ok(Expression::Constant(Constant::new(datum, ft)))
        }
        _ => unreachable!("dispatched by rewrite_leaf"),
    }
}

/// [`rewrite_leaf`]'s arms for the COMPOUND predicates, in a function of their own.
///
/// The split is about STACK, not structure. A debug build gives one frame a
/// slot for every arm's locals at once, so a single 33-arm match over `Expr`
/// carried a 174 KB frame -- and `rewrite_leaf` recurses once per level of a
/// nested predicate, which made eight levels of `AND`/`OR` overflow a 2 MB
/// stack while planning. Each group pays only its own arms now, and the
/// dispatcher that recurses pays almost nothing.
#[inline(never)]
fn rewrite_leaf_compound(
    expr: &Expr,
    resolver: &impl ColumnResolver,
) -> Result<Expression, EvalError> {
    match expr {
        // Go's `in` builtin takes the tested value as args[0] and the list as
        // the remaining arguments; `NOT IN` wraps it in a unary NOT, which
        // keeps NULL as NULL exactly as MySQL requires.
        Expr::In { expr, list, not } => {
            let expected_columns = row_len(expr);
            if list
                .iter()
                .any(|candidate| row_len(candidate) != expected_columns)
            {
                return Err(EvalError::OperandColumns(expected_columns));
            }
            if let Expr::Row(left) = expr.as_ref() {
                let mut comparisons = list.iter().map(|right| {
                    let Expr::Row(right) = right else {
                        return Err(EvalError::OperandColumns(left.len()));
                    };
                    rewrite_row_comparison(BinaryOp::Eq, left, right, resolver)
                });
                let first = comparisons.next().ok_or(EvalError::Unsupported(
                    "an IN expression with no candidates",
                ))??;
                let call = comparisons.try_fold(first, |condition, comparison| {
                    binary_expression(BinaryOp::LogicOr, condition, comparison?, resolver)
                })?;
                if *not {
                    let ret_type = call
                        .static_type()
                        .cloned()
                        .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
                    return Ok(Expression::ScalarFunction(ScalarFunction::new(
                        CiString::new(unary_op_name(UnaryOp::Not)),
                        ret_type,
                        vec![call],
                    )));
                }
                return Ok(call);
            }
            let mut args = Vec::with_capacity(list.len() + 1);
            args.push(rewrite_expr_resolved(expr, resolver)?);
            for item in list {
                args.push(rewrite_expr_resolved(item, resolver)?);
            }
            let ctx = resolver.comparison_context().unwrap_or(&crate::NoColumns);
            crate::builtin_compare::refine_integer_in_arguments(&mut args, ctx)?;
            let left = &args[0];
            let left_type = left.static_type();
            if left_type.is_some_and(|field| field.code() == FieldTypeCode::Null) {
                return Ok(Expression::Constant(Constant::new_null()));
            }
            fn operand(expression: &Expression) -> crate::builtin_compare::CmpOperand<'_> {
                crate::builtin_compare::CmpOperand {
                    field_type: expression.static_type().expect("typed IN argument"),
                    is_constant: matches!(expression, Expression::Constant(_)),
                    is_column: matches!(expression, Expression::Column(_)),
                }
            }
            let all_same_type = left_type.is_some_and(|field| {
                args[1..].iter().all(|argument| {
                    argument.static_type().is_some_and(|other| {
                        other.code() == FieldTypeCode::Null
                            || crate::builtin_compare::get_accurate_cmp_type(
                                operand(left),
                                operand(argument),
                            ) == field.eval_type()
                    })
                })
            });
            if !all_same_type || list.len() == 1 {
                // Go `deriveCollationForIn` + `castCollationForIn`: one
                // collation over the whole IN, and each string member not
                // already in it cast to it (EXPLICIT) before the equalities
                // are built, so `a IN ('b' COLLATE utf8mb4_general_ci, 'A', 3)`
                // compares `a` with 'A' under general_ci.
                let derived = crate::collation_derive::check_and_derive_collation_from_exprs(
                    "IN",
                    EvalType::Int,
                    &args,
                )?;
                if tidb_datatype::new_collation_enabled() {
                    for index in 1..args.len() {
                        let compares_as_string = crate::builtin_compare::get_accurate_cmp_type(
                            operand(&args[0]),
                            operand(&args[index]),
                        ) == EvalType::String;
                        if let Some(mut cast) = crate::simple_expr::build_cast_collation_function(
                            &args[index],
                            &derived,
                            compares_as_string,
                            &|expression| {
                                resolver.fold_constant(expression, ConstantFoldMode::Normal);
                            },
                        )? {
                            crate::collation_derive::set_coercibility(
                                &mut cast,
                                crate::expr_collation::Coercibility::EXPLICIT,
                            );
                            args[index] = cast;
                        }
                    }
                }
                let left = &args[0];
                let mut equalities = Vec::with_capacity(list.len());
                for right in &args[1..] {
                    equalities.push(binary_expression(
                        BinaryOp::Eq,
                        left.clone(),
                        right.clone(),
                        resolver,
                    )?);
                }
                let call = crate::simple_expr::compose_dnf_condition(equalities).ok_or(
                    EvalError::Unsupported("an IN expression with no candidates"),
                )?;
                return if *not {
                    Ok(Expression::ScalarFunction(ScalarFunction::new(
                        CiString::new(unary_op_name(UnaryOp::Not)),
                        call.static_type()
                            .cloned()
                            .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong)),
                        vec![call],
                    )))
                } else {
                    Ok(call)
                };
            }
            // Go `buildHashMapForConstArgs` keeps the first occurrence of each
            // strict constant in the list (and one NULL), keyed the way its
            // signature hashes: `InInt` by the evaluated int64 -- so -1 and
            // 18446744073709551615 are one entry and the later is dropped --
            // `InString` by the collation key of the IN's collation, every
            // other signature by value. args[0] is the tested expression;
            // only the LIST entries (args[1..]) take part. Prepared `?`
            // markers and deferred subqueries evaluate per execution: never
            // fold them, and never seed the seen set from them.
            #[derive(PartialEq)]
            enum InKey {
                Int(i64),
                Bytes(Vec<u8>),
                Value(tidb_datatype::Datum),
            }
            let left_eval_type = args[0].static_type().map(FieldType::eval_type);
            let string_collation = (left_eval_type == Some(EvalType::String))
                .then(|| {
                    crate::collation_derive::check_and_derive_collation_from_exprs(
                        "IN",
                        EvalType::Int,
                        &args,
                    )
                    .ok()
                    .and_then(|derived| tidb_datatype::Collation::from_name(&derived.collation))
                })
                .flatten();
            let key_of = |value: &tidb_datatype::Datum| -> InKey {
                match (left_eval_type, value) {
                    (_, Datum::Null) => InKey::Value(Datum::Null),
                    (Some(EvalType::Int), Datum::Int(int)) => InKey::Int(*int),
                    (Some(EvalType::Int), Datum::UInt(uint)) => InKey::Int(*uint as i64),
                    (Some(EvalType::String), value) => {
                        match (string_collation, value.as_raw_bytes()) {
                            (Some(collation), Some(bytes)) => InKey::Bytes(collation.key(bytes)),
                            _ => InKey::Value(value.clone()),
                        }
                    }
                    (_, value) => InKey::Value(value.clone()),
                }
            };
            let mut deduplicated = Vec::with_capacity(args.len());
            let mut seen_keys: Vec<InKey> = Vec::new();
            for (position, argument) in args.into_iter().enumerate() {
                let strict_constant = match &argument {
                    Expression::Constant(constant)
                        if position > 0
                            && constant.param_marker.is_none()
                            && constant.deferred_expr.is_none() =>
                    {
                        Some(key_of(&constant.value))
                    }
                    _ => None,
                };
                if let Some(key) = strict_constant {
                    if seen_keys.contains(&key) {
                        continue;
                    }
                    seen_keys.push(key);
                }
                deduplicated.push(argument);
            }
            let mut args = deduplicated;
            // Go `inFunctionClass.verifyArgs`: a negative integer constant can
            // never equal a BIT value, so it leaves the list -- and a list it
            // empties is the arity error (`id in (-1, -2)` is 1582).
            if args[0]
                .static_type()
                .is_some_and(|field| field.code() == FieldTypeCode::Bit)
            {
                // Dropping a mutable member freezes this parameter's value.
                let ctx = resolver.comparison_context().unwrap_or(&crate::NoColumns);
                let over_optimized = crate::expr_util::maybe_over_optimized_4_plan_cache(
                    ctx.use_plan_cache(),
                    &args,
                );
                let mut position = 0;
                args.retain(|argument| {
                    position += 1;
                    let negative = match argument {
                        Expression::Constant(constant) if position > 1 => match constant.value {
                            Datum::Int(value) if value < 0 => Some(value),
                            _ => None,
                        },
                        _ => None,
                    };
                    if let (Some(value), true) = (negative, over_optimized) {
                        ctx.set_skip_plan_cache(&format!("Bit Column in ({value})"));
                    }
                    negative.is_none()
                });
                if args.len() < 2 {
                    return Err(EvalError::WrongParameterCount("in"));
                }
            }
            // Go `inFunctionClass.getFunction` (`builtin_other.go:89`) coerces
            // every argument to the FIRST argument's eval type through
            // `newBaseBuiltinFuncWithTp` (`builtin.go:184`). The wrap replaces
            // the argument inside the shared backing array, so the built
            // `in()` node itself carries the coerced arguments, and each cast
            // over a strict constant folds at Go's `BuildCastFunction`
            // boundary -- which is why EXPLAIN renders a date column's string
            // IN-list as bare `datetime(6)` constants rather than quoted
            // strings (the q83 shape).
            let in_eval_type = args
                .first()
                .and_then(|expression| expression.static_type())
                .map(|field_type| field_type.eval_type());
            let connection = resolver.connection_charset_info();
            let mut coerced = Vec::with_capacity(args.len());
            for (position, argument) in args.into_iter().enumerate() {
                let mut coerced_argument = match in_eval_type {
                    Some(EvalType::Int) => wrap_with_cast_as_int(argument, None)?,
                    Some(EvalType::Real) => wrap_with_cast_as_real(argument)?,
                    Some(EvalType::Decimal) => wrap_with_cast_as_decimal(argument)?,
                    Some(EvalType::String) => wrap_with_cast_as_string(argument, connection)?,
                    Some(EvalType::Datetime) => {
                        wrap_with_cast_as_time(argument, FieldType::new(FieldTypeCode::Datetime))?
                    }
                    Some(EvalType::Timestamp) => {
                        wrap_with_cast_as_time(argument, FieldType::new(FieldTypeCode::Timestamp))?
                    }
                    Some(EvalType::Duration) => wrap_with_cast_as_duration(argument)?,
                    // Go `inFunctionClass` then `DisableParseJSONFlag4Expr`
                    // on every list argument: a string stays a JSON string.
                    Some(EvalType::Json) => {
                        let mut wrapped = wrap_with_cast_as_json(argument)?;
                        if position > 0 {
                            crate::expr_util::predicates::disable_parse_json_flag_4_expr(
                                &mut wrapped,
                            );
                        }
                        wrapped
                    }
                    _ => argument,
                };
                // Go `BuildCastFunctionWithCheck` folds each newly-built cast
                // except a JSON result. A fold over a non-cast node (the
                // early-return arms) leaves it untouched, matching Go, whose
                // `FoldConstant` only runs where a cast was actually built.
                if matches!(
                    &coerced_argument,
                    Expression::ScalarFunction(function)
                        if function.func_name.lowercase().starts_with("cast_")
                            && function
                                .ret_type
                                .as_ref()
                                .is_some_and(|tp| tp.eval_type() != EvalType::Json)
                ) {
                    crate::constant_fold::fold_constant_in_mode(
                        &mut coerced_argument,
                        ctx,
                        ConstantFoldMode::Normal,
                    );
                }
                coerced.push(coerced_argument);
            }
            let args = coerced;
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
            let was_cast = [&value, &low, &high].map(is_newly_built_cast);
            let [mut value, mut low, mut high] = wrap_between_arguments(
                [&value, &low, &high],
                resolve_type4_between([&value, &low, &high]),
                resolver.connection_charset_info(),
            )?;
            // Go `BuildCastFunction` folds the cast it just built, so a
            // constant operand is converted (and warns) once, not once per
            // bound comparison that shares it.
            for (operand, was_cast) in [&mut value, &mut low, &mut high].into_iter().zip(was_cast) {
                if is_newly_built_cast(operand) && !was_cast {
                    resolver.fold_constant(operand, ConstantFoldMode::Normal);
                }
            }
            // Go `betweenToExpression`: one collation over all three
            // operands, each cast to it, so `a BETWEEN binary_col AND
            // ci_col` compares both bounds as binary instead of picking a
            // collation per comparison.
            let derived = crate::collation_derive::check_and_derive_collation_from_exprs(
                "BETWEEN",
                EvalType::Int,
                &[value.clone(), low.clone(), high.clone()],
            )?;
            let accurate = |left: &Expression, right: &Expression| match (
                left.static_type(),
                right.static_type(),
            ) {
                (Some(left_type), Some(right_type)) => {
                    crate::builtin_compare::get_accurate_cmp_type(
                        crate::builtin_compare::CmpOperand {
                            field_type: left_type,
                            is_constant: matches!(left, Expression::Constant(_)),
                            is_column: matches!(left, Expression::Column(_)),
                        },
                        crate::builtin_compare::CmpOperand {
                            field_type: right_type,
                            is_constant: matches!(right, Expression::Constant(_)),
                            is_column: matches!(right, Expression::Column(_)),
                        },
                    )
                }
                _ => EvalType::Int,
            };
            let enum_or_set_real_type_is_str =
                accurate(&value, &low) != EvalType::Int && accurate(&value, &high) != EvalType::Int;
            let cast = |operand: Expression| -> Result<Expression, EvalError> {
                Ok(crate::simple_expr::build_cast_collation_function(
                    &operand,
                    &derived,
                    enum_or_set_real_type_is_str,
                    &|expression| {
                        resolver.fold_constant(expression, ConstantFoldMode::Normal);
                    },
                )?
                .unwrap_or(operand))
            };
            let (value, low, high) = (cast(value)?, cast(low)?, cast(high)?);
            let (lower_op, upper_op, joiner) = if *not {
                (BinaryOp::Lt, BinaryOp::Gt, "or")
            } else {
                (BinaryOp::Ge, BinaryOp::Le, "and")
            };
            let compare = binary_expression;
            let mut lower = compare(lower_op, value.clone(), low, resolver)?;
            let mut upper = compare(upper_op, value, high, resolver)?;
            // Go builds each bound with `NewFunction`, which settles its
            // collation and folds it on the spot; the joining AND then folds
            // over two constants, so `'a' BETWEEN 'g' AND 'f'` is a constant
            // false and the scan becomes a TableDual.
            for bound in [&mut lower, &mut upper] {
                derive_tree_collation_with_connection(bound, resolver.connection_charset_info())?;
                resolver.fold_constant(bound, resolver.fold_mode());
            }
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
                // Go supplies the session-selected implicit escape when none
                // was written. Explicit `ESCAPE` always wins.
                constant(
                    Datum::Int(i64::from(
                        escape.unwrap_or_else(|| resolver.like_default_escape()),
                    )),
                    FieldTypeCode::LongLong,
                ),
            ];
            // `likeFunctionClass`: ETString, ETString, ETInt.
            let args = wrap_declared_string_args(name, args, &[0, 1], resolver)?;
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
            // `regexpLikeFunctionClass`: both arguments are ETString.
            let args = wrap_declared_string_args("regexp", args, &[0, 1], resolver)?;
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
            let child_resolver = FoldModeResolver::new(resolver, ConstantFoldMode::Try);
            let compare_value = match value {
                Some(value) => Some(rewrite_expr_resolved(value, &child_resolver)?),
                None => None,
            };
            let mut args = Vec::with_capacity(when_clauses.len() * 2 + 1);
            for (condition, result) in when_clauses {
                let condition = rewrite_expr_resolved(condition, &child_resolver)?;
                let condition = match &compare_value {
                    // Go `caseToExpression` builds each WHEN with
                    // `constructBinaryOpFunction(value, when, EQ)`: the
                    // ordinary comparison, with its argument coercion (a
                    // JSON operand against a string, for one).
                    Some(value) => {
                        binary_expression(BinaryOp::Eq, value.clone(), condition, &child_resolver)?
                    }
                    None => condition,
                };
                // Go `caseWhenFunctionClass.getFunction`: every WHEN
                // condition runs through `wrapWithIsTrue(ctx, true, arg, false)`.
                args.push(wrap_with_is_true_with_null(condition, &child_resolver)?);
                args.push(rewrite_expr_resolved(result, &child_resolver)?);
            }
            if let Some(else_clause) = else_clause {
                args.push(rewrite_expr_resolved(else_clause, &child_resolver)?);
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
            let ret_type = builtin_return_type("case", &branches).ok_or(EvalError::Unsupported(
                "a CASE whose branches have different types",
            ))?;
            // Go's `caseWhenFunctionClass.getFunction` builds the signature
            // with the merged type as every result argument's expected type,
            // so the branches are cast to it before the function is built.
            let connection = resolver.connection_charset_info();
            let mut result_indexes: Vec<usize> = (1..args.len()).step_by(2).collect();
            if args.len() % 2 == 1 {
                // The trailing ELSE is a result branch too.
                result_indexes.push(args.len() - 1);
            }
            for index in result_indexes {
                let branch = args[index].clone();
                let (mut wrapped, built) = wrap_case_branch(branch, &ret_type, connection)?;
                // Go `BuildCastFunctionWithCheck` (`builtin_cast.go:2655`)
                // folds the constant cast it just built, which is why a
                // constant branch renders as the cast's own type
                // (`0.0000`). Rust's builders defer that fold so conversion
                // diagnostics stay with the live statement context; this is
                // that context, and a non-constant branch is untouched.
                if let (true, Some(context)) = (built, resolver.comparison_context()) {
                    crate::constant_fold::fold_constant_in_mode(
                        &mut wrapped,
                        context,
                        ConstantFoldMode::Normal,
                    );
                }
                args[index] = wrapped;
            }
            Ok(Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("case"),
                ret_type,
                args,
            )))
        }
        // Go's parser represents `candidate MEMBER OF (document)` as a
        // two-argument JSON_MEMBER_OF call; only the restore syntax is infix.
        Expr::MemberOf { expr, array } => {
            let args = vec![
                rewrite_expr_resolved(expr, resolver)?,
                rewrite_expr_resolved(array, resolver)?,
            ];
            crate::builtin_ext::json::verify_json_function_args("json_memberof", &args)?;
            // Go's function name is `ast.JSONMemberOf` = "json_memberof",
            // which EXPLAIN prints and TiKV push-down recognises.
            let ret_type =
                builtin_return_type("json_memberof", &args).expect("JSON_MEMBEROF is registered");
            signature_args::build_call(
                "json_memberof",
                ret_type,
                args,
                resolver.connection_charset_info(),
                &|expression| resolver.fold_constant(expression, ConstantFoldMode::Normal),
            )
        }
        // Go rewrites `x IS <target>` into the isnull/istrue/isfalse builtin,
        // wrapping `IS NOT` in a unary NOT. These return 0/1 and never NULL,
        // so the wrapping NOT is exact.
        Expr::Is { expr, target, not } => {
            let arg = rewrite_expr_resolved(expr, resolver)?;
            // Go `isNullToExpression` / `isTrueToScalarFunc`.
            ensure_single_column(&[&arg])?;
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
            let mut call = signature_args::build_call(
                name,
                ret_type.clone(),
                vec![arg],
                resolver.connection_charset_info(),
                &|expression| resolver.fold_constant(expression, ConstantFoldMode::Normal),
            )?;
            if *not {
                // Go's `notToExpression` builds the operator through
                // `newFunction` first, which folds it BEFORE the `not` is
                // built over it -- so `a IS NOT NULL` over a NOT NULL column
                // is `not(0)`, a folded constant that is itself NOT NULL.
                if let Expression::ScalarFunction(function) = &mut call {
                    if let Some(ret_type) = function.ret_type.as_mut() {
                        result_type::stamp_builtin_return_flags(name, &function.args, ret_type);
                    }
                }
                resolver.fold_constant(&mut call, resolver.fold_mode());
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
            // Go `unaryOpToExpression`.
            ensure_single_column(&[&arg])?;
            let name = unary_op_name(*op);
            // not/bitneg/unaryminus result types come from the transcreated
            // builtin_op function classes; anything uncovered (the deferred
            // unaryminus arms) keeps the LongLong placeholder.
            if let Some(ret_type) = crate::builtin_op::infer_unary_op_type(name, &arg) {
                return signature_args::build_call(
                    name,
                    ret_type,
                    vec![arg],
                    resolver.connection_charset_info(),
                    &|expression| resolver.fold_constant(expression, ConstantFoldMode::Normal),
                );
            }
            Ok(scalar(name, vec![arg]))
        }
        _ => unreachable!("dispatched by rewrite_leaf"),
    }
}

/// Go `setExprRetType`: overwrites the result type of a node (the caller
/// owns a clone, so a shared column definition is not touched).
fn set_expression_ret_type(expression: &mut Expression, ret_type: FieldType) {
    match expression {
        Expression::Column(column) => column.ret_type = Some(ret_type),
        Expression::CorrelatedColumn(column) => column.column.ret_type = Some(ret_type),
        Expression::Constant(constant) => constant.ret_type = Some(ret_type),
        Expression::ScalarFunction(function) => function.ret_type = Some(ret_type),
    }
}

/// Go `newFunctionImpl` for an already-built argument list: the build-time
/// argument handling, the result type, and the signature casts of the builtin
/// `lowered`. NULLIF re-enters here to build the `if` it is rewritten to.
fn finish_function_call(
    lowered: &str,
    mut rewritten: Vec<Expression>,
    resolver: &impl ColumnResolver,
) -> Result<Expression, EvalError> {
    let lowered = lowered.to_owned();
    // Go's rewriter turns `NULLIF(a, b)` into `IF(a = b, NULL, a)` before any
    // function class sees it (`expression_rewriter.go`'s `ast.Nullif` arm):
    // the comparison is built over clones, the NULL keeps TypeNull so the IF
    // takes its type from the value branch, and that branch loses
    // NotNullFlag (and its charset when it is not a string).
    if lowered == "nullif" && rewritten.len() == 2 {
        let second = rewritten.pop().expect("two arguments");
        let first = rewritten.pop().expect("two arguments");
        let condition = binary_expression(BinaryOp::Eq, first.clone(), second, resolver)?;
        let mut value = first;
        if let Some(mut value_type) = value.static_type().cloned() {
            value_type.del_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
            if !value_type.eval_type().is_string_kind() {
                value_type.set_charset_name("binary");
                value_type.set_collation_name("binary");
            }
            set_expression_ret_type(&mut value, value_type);
        }
        let null = Expression::Constant(Constant::new(
            Datum::Null,
            FieldType::new(FieldTypeCode::Null),
        ));
        return finish_function_call("if", vec![condition, null, value], resolver);
    }
    // Go `newFunctionImpl`: an untyped NULL argument takes the last
    // typed argument's type before the function class sees it
    // (control functions and ROW infer their own).
    if !matches!(lowered.as_str(), "if" | "ifnull" | "nullif" | "row") {
        crate::new_function::type_infer_for_null(&mut rewritten);
    }
    let rewritten = wrap_power_arguments(&lowered, rewritten, resolver)?;
    let rewritten = wrap_time_format_arguments(&lowered, rewritten, resolver)?;
    let rewritten = wrap_clock_fsp_argument(&lowered, rewritten, resolver)?;
    // Go's JSON function classes check their arguments' TYPES in
    // `verifyArgs`; a malformed constant path is only an evaluation
    // error (`EXPLAIN` succeeds, an unevaluated call never fails).
    crate::builtin_ext::json::verify_json_function_args(&lowered, &rewritten)?;
    let mut ret_type = builtin_return_type(&lowered, &rewritten).ok_or_else(|| {
        crate::builtin_registry::unresolved_error(&lowered, resolver.current_database())
    })?;
    // Go `ifFunctionClass` / `ifNullFunctionClass` build with
    // `newBaseBuiltinFuncWithFieldTypes`, the merged type as each
    // value argument's expected type: the values are cast to it
    // exactly as CASE's branches are (`wrap_case_branch`).
    let rewritten = if matches!(lowered.as_str(), "if" | "ifnull") {
        let first_value = usize::from(lowered == "if");
        let connection = resolver.connection_charset_info();
        rewritten
            .into_iter()
            .enumerate()
            .map(|(index, arg)| {
                if index < first_value {
                    // Go `ifFunctionClass.getFunction`:
                    // `wrapWithIsTrue(ctx, true, args[0], false)`.
                    return wrap_with_is_true_with_null(arg, resolver);
                }
                let (mut wrapped, built) = wrap_case_branch(arg, &ret_type, connection)?;
                if let (true, Some(context)) = (built, resolver.comparison_context()) {
                    crate::constant_fold::fold_constant_in_mode(
                        &mut wrapped,
                        context,
                        ConstantFoldMode::Normal,
                    );
                }
                Ok(wrapped)
            })
            .collect::<Result<Vec<_>, EvalError>>()?
    } else if lowered == "coalesce" {
        // Go `coalesceFunctionClass` builds with
        // `newBaseBuiltinFuncWithTp`: every argument is cast to the
        // merged EVAL type only, keeping its own length and scale, so
        // an unfolded COALESCE answers the chosen argument as it is
        // (a folded one is padded by the constant's `adjustDecimal`).
        let connection = resolver.connection_charset_info();
        rewritten
            .into_iter()
            .map(|arg| {
                let was_cast = is_newly_built_cast(&arg);
                let mut wrapped = match ret_type.eval_type() {
                    EvalType::Int => wrap_with_cast_as_int(arg, None)?,
                    EvalType::Real => wrap_with_cast_as_real(arg)?,
                    EvalType::Decimal => wrap_with_cast_as_decimal(arg)?,
                    EvalType::String => wrap_with_cast_as_string(arg, connection)?,
                    EvalType::Datetime => {
                        crate::aggregation::wrap_cast::wrap_with_cast_as_time(
                            arg,
                            FieldType::new(tidb_datatype::FieldTypeCode::Datetime),
                        )?
                    }
                    EvalType::Timestamp => {
                        crate::aggregation::wrap_cast::wrap_with_cast_as_time(
                            arg,
                            FieldType::new(tidb_datatype::FieldTypeCode::Timestamp),
                        )?
                    }
                    EvalType::Duration => wrap_with_cast_as_duration(arg)?,
                    EvalType::Json => wrap_with_cast_as_json(arg)?,
                    EvalType::VectorFloat32 => arg,
                };
                if is_newly_built_cast(&wrapped) && !was_cast {
                    if let Some(context) = resolver.comparison_context() {
                        crate::constant_fold::fold_constant_in_mode(
                            &mut wrapped,
                            context,
                            ConstantFoldMode::Normal,
                        );
                    }
                }
                Ok(wrapped)
            })
            .collect::<Result<Vec<_>, EvalError>>()?
    } else {
        rewritten
    };
    if lowered == "tidb_version" {
        ret_type.set_flen(i64::try_from(resolver.tidb_info_len()).unwrap_or(i64::MAX));
    }
    // Go derives the function collation before applying
    // `HandleBinaryLiteral`. In particular, CONCAT with a binary
    // operand has a binary result, so a non-legacy argument such as
    // GBK must be wrapped with `to_binary` before the node is built.
    // The tree-level derivation below still stamps the complete
    // `ExprCollation` (including coercibility and repertoire), but
    // this early charset/collation result is needed for the
    // construction-time conversion decision itself.
    let derived = crate::collation_derive::derive_collation_with_connection(
        &lowered,
        &rewritten,
        ret_type.eval_type(),
        resolver.connection_charset_info(),
    )?;
    ret_type.set_charset_name(derived.charset.clone());
    ret_type.set_collation_name(derived.collation.clone());
    let rewritten = wrap_binary_literals(
        &lowered,
        &derived.charset,
        &derived.collation,
        ret_type.eval_type(),
        rewritten,
        |expression| resolver.fold_constant(expression, ConstantFoldMode::Normal),
    );
    signature_args::build_call(
        &lowered,
        ret_type,
        rewritten,
        resolver.connection_charset_info(),
        &|expression| resolver.fold_constant(expression, ConstantFoldMode::Normal),
    )
}

/// [`rewrite_leaf`]'s arms for CALLS and casts -- and the catch-all, in a function of their own.
///
/// The split is about STACK, not structure. A debug build gives one frame a
/// slot for every arm's locals at once, so a single 33-arm match over `Expr`
/// carried a 174 KB frame -- and `rewrite_leaf` recurses once per level of a
/// nested predicate, which made eight levels of `AND`/`OR` overflow a 2 MB
/// stack while planning. Each group pays only its own arms now, and the
/// dispatcher that recurses pays almost nothing.
#[inline(never)]
fn rewrite_leaf_call(expr: &Expr, resolver: &impl ColumnResolver) -> Result<Expression, EvalError> {
    match expr {
        // Go's parser keeps `EXTRACT(unit FROM value)` as a two-argument
        // `extract` call: `parseExtractFunc` builds `FuncCallExpr{FnName:
        // ast.Extract, Args: [TimeUnitExpr, value]}` and the rewriter turns
        // the unit into a VARCHAR constant (`expression_rewriter.go:1838`).
        // EXTRACT's evaluator selects the value's datetime/duration signature
        // from this unit and the second argument's static type.
        Expr::Extract { unit, value } => {
            let unit = constant(Datum::new_string(unit.clone()), FieldTypeCode::VarString);
            let value = rewrite_expr_resolved(value, resolver)?;
            Ok(scalar("extract", vec![unit, value]))
        }
        // The first GET_FORMAT argument is grammar, not an expression. Go's
        // parser turns it into the signature's first ETString constant and
        // collapses TIMESTAMP into DATETIME before evaluation.
        Expr::GetFormat { selector, expr } => {
            let selector = match selector {
                GetFormatSelector::Date => "DATE",
                GetFormatSelector::Time => "TIME",
                GetFormatSelector::Datetime => "DATETIME",
            };
            let args = vec![
                constant_string(selector),
                rewrite_expr_resolved(expr, resolver)?,
            ];
            let ret_type = builtin_return_type("get_format", &args).ok_or(
                EvalError::Unsupported("this builtin is not yet built for chunk evaluation"),
            )?;
            Ok(Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("get_format"),
                ret_type,
                args,
            )))
        }
        // Go parses POSITION with its own `substr IN str` grammar into an
        // ordinary `position(substr, str)` call (`parsePositionFunc`), which
        // `funcs` builds with `locateFunctionClass` -- through the same
        // argument handling as any call, so a gbk argument meeting a binary
        // literal is transcoded (`HandleBinaryLiteral`).
        Expr::Position { substr, str } => rewrite_expr_resolved(
            &Expr::Func {
                name: "position".to_owned(),
                args: vec![(**substr).clone(), (**str).clone()],
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
            if lowered == "values" {
                if let [Expr::Column(path)] = args.as_slice() {
                    if let Some(values) = resolver.resolve_values(path) {
                        return Ok(values);
                    }
                }
            }
            if lowered == "grouping" {
                let args = args
                    .iter()
                    .map(|arg| rewrite_expr_resolved(arg, resolver))
                    .collect::<Result<Vec<_>, _>>()?;
                return resolver.rewrite_grouping(&args);
            }
            // Go resolves every FuncCallExpr through its function class
            // before a physical operator can suppress row evaluation (for
            // example an empty table). Keep DATE_ADD's Rust interval node
            // aligned with Go's three internal arguments.
            let arity_count = if matches!(
                lowered.as_str(),
                "date_add" | "date_sub" | "adddate" | "subdate"
            ) && matches!(args.as_slice(), [_, Expr::Interval { .. }])
            {
                3
            } else {
                args.len()
            };
            crate::builtin_registry::verify_args_by_count(&lowered, arity_count)?;
            // Go `tidbDecodeSQLDigestsFunctionClass.getFunction` checks the
            // PROCESS privilege while building, before any row is read.
            if lowered == "tidb_decode_sql_digests"
                && resolver
                    .comparison_context()
                    .is_some_and(|ctx| !ctx.has_process_privilege())
            {
                return Err(crate::builtin_ext::misc::process_privilege_denied());
            }
            let child_resolver = FoldModeResolver::for_function(resolver, &lowered);
            if lowered == "name_const" {
                validate_name_const_args(args)?;
            }
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
                        rewritten.push(rewrite_expr_resolved(arg, &child_resolver)?);
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
            // -- a value AND a unit keyword. Go's parser hands the function
            // class three arguments, the unit as an `ast.TimeUnitExpr`, and
            // the rewriter turns that into a VARCHAR string constant
            // (`expression_rewriter.go`, `case *ast.TimeUnitExpr`):
            // `date_add(d, n, "MONTH")`. `ADDDATE`/`SUBDATE` keep their own
            // names (the parser already normalized their bare-number form to
            // an `INTERVAL n DAY`).
            if matches!(
                lowered.as_str(),
                "date_add" | "date_sub" | "adddate" | "subdate"
            ) {
                if let [date, Expr::Interval { value, unit }] = args.as_slice() {
                    let unit = unit.to_ascii_uppercase();
                    // Only the units `date_add` itself implements are built.
                    // The composite units
                    // (`HOUR_MINUTE`, `DAY_SECOND`, `YEAR_MONTH`, ...) ARE
                    // built — `time_fn::calendar::date_add` handles them via
                    // `composite_spec`.
                    if !matches!(
                        unit.as_str(),
                        "DAY"
                            | "WEEK"
                            | "MONTH"
                            | "QUARTER"
                            | "YEAR"
                            | "HOUR"
                            | "MINUTE"
                            | "SECOND"
                            | "MICROSECOND"
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
                    let args = vec![
                        rewrite_expr_resolved(date, &child_resolver)?,
                        rewrite_expr_resolved(value, &child_resolver)?,
                        constant(Datum::new_string(unit), FieldTypeCode::Varchar),
                    ];
                    let ret_type =
                        builtin_return_type(&lowered, &args).ok_or(EvalError::Unsupported(
                            "this builtin is not yet built for chunk evaluation",
                        ))?;
                    return Ok(Expression::ScalarFunction(ScalarFunction::new(
                        CiString::new(&lowered),
                        ret_type,
                        args,
                    )));
                }
            }
            // The parser keeps CHAR(... USING charset)'s validated charset as
            // RawString so it cannot be mistaken for a SQL string literal.
            // Go appends the same value as the function's final Constant.
            if lowered == "char_func" {
                let rewritten = args
                    .iter()
                    .enumerate()
                    .map(|(index, arg)| match (index + 1 == args.len(), arg) {
                        (true, Expr::RawString(charset)) => Ok(constant_string(charset)),
                        _ => rewrite_expr_resolved(arg, &child_resolver),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                // go's charFunctionClass.getFunction validates the charset
                // argument at build time: an unknown name fails the plan with
                // the bare 1105 `Unknown charset <name>`, AFTER the argument
                // casts' own truncation warnings reached the statement buffer
                // (oracle: CHAR('中文测试', 'ünïcödé') keeps the 1292 row
                // beside the 1105).
                if let Some(Expression::Constant(constant)) = rewritten.last() {
                    if !constant.value.is_null() {
                        if let Ok(charset) = constant.value.sql_string() {
                            let lowered_charset = charset.to_ascii_lowercase();
                            if tidb_datatype::get_default_collation(&lowered_charset).is_err() {
                                // go's build-time arg casts warn BEFORE the
                                // charset check fails the plan (oracle:
                                // CHAR('中文测试', 'ünïcödé') keeps the 1292
                                // truncation row beside the 1105). The plan
                                // fails here, so the constant code-point casts'
                                // warnings stash for the error door's drain --
                                // the successful path re-warns at evaluation,
                                // which is why this branch stashes instead.
                                let stash = crate::constant_fold::FoldWarningContext::new(
                                    resolver.time_zone(),
                                );
                                for code_point in &rewritten[..rewritten.len() - 1] {
                                    if let Expression::Constant(constant) = code_point {
                                        let _ = crate::cast::report_int_truncation(
                                            &constant.value,
                                            &stash,
                                        );
                                    }
                                }
                                return Err(EvalError::Unsupported(Box::leak(
                                    format!("Unknown charset {lowered_charset}").into_boxed_str(),
                                )));
                            }
                        }
                    }
                }
                let ret_type = builtin_return_type(&lowered, &rewritten).ok_or(
                    EvalError::Unsupported("this builtin is not yet built for chunk evaluation"),
                )?;
                return signature_args::build_call(
                    &lowered,
                    ret_type,
                    rewritten,
                    resolver.connection_charset_info(),
                    &|expression| resolver.fold_constant(expression, ConstantFoldMode::Normal),
                );
            }
            let rewritten: Vec<Expression> = args
                .iter()
                .map(|arg| rewrite_expr_resolved(arg, &child_resolver))
                .collect::<Result<_, _>>()?;
            finish_function_call(&lowered, rewritten, resolver)
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
                tidb_ast::CastStyle::DateLiteral
                    | tidb_ast::CastStyle::TimeLiteral
                    | tidb_ast::CastStyle::TimestampLiteral
            ) =>
        {
            let text = literal_text(&cast.expr, resolver)?;
            let zone = resolver.time_zone();
            let modes = resolver.date_modes();
            if cast.style == tidb_ast::CastStyle::TimeLiteral {
                let (duration, mut ret_type) =
                    crate::time_literal::time_literal(&text, &zone, modes)?;
                // Go folds the literal call (`foldConstant`), and a non-null
                // folded value carries the NotNull flag.
                ret_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
                return Ok(Expression::Constant(Constant::new(
                    Datum::Duration(duration),
                    ret_type,
                )));
            }
            let (time, mut ret_type) = match cast.style {
                tidb_ast::CastStyle::DateLiteral => {
                    crate::time_literal::date_literal(&text, &zone, modes)?
                }
                _ => crate::time_literal::timestamp_literal(&text, &zone, modes)?,
            };
            ret_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
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
            if cast.style == tidb_ast::CastStyle::JsonSumCrc32 {
                return rewrite_json_sum_crc32(cast, resolver);
            }
            if cast.array {
                return rewrite_cast_array(cast, resolver);
            }
            validate_cast_type(cast)?;
            let (name, ret_type) = resolved_cast_target(cast, resolver)?;
            let arg = rewrite_expr_resolved(&cast.expr, resolver)?;
            // Go `Leave(*ast.FuncCastExpr)`.
            ensure_single_column(&[&arg])?;
            // `CAST(x AS BINARY)` is Go's `funcPropAuto` binary-result arm:
            // a gbk-charset argument transcodes on the way in, which is why
            // `HEX(CAST(gbk_col AS BINARY))` reports the GBK bytes; a binary
            // argument cast to a character set decodes through `from_binary`.
            let mut ret_type = ret_type;
            let args = if ret_type.eval_type() == tidb_datatype::EvalType::String {
                size_string_cast_for_bit_source(&mut ret_type, &arg);
                let arg = wrap_cast_as_string_arg(arg, &ret_type, &|expression| {
                    resolver.fold_constant(expression, ConstantFoldMode::Normal)
                })?;
                if let Some(arg_type) = arg.static_type() {
                    adjust_ret_ft_for_cast_string(&mut ret_type, arg_type);
                }
                vec![arg]
            } else {
                let mut arg = arg;
                // Go `castAsRealFunctionClass`: a DECIMAL argument's result
                // scale widens through `PropagateType(ETReal)`.
                if ret_type.eval_type() == tidb_datatype::EvalType::Real
                    && arg
                        .static_type()
                        .is_some_and(|field| field.eval_type() == tidb_datatype::EvalType::Decimal)
                {
                    crate::expression::propagate_type(&mut arg, tidb_datatype::EvalType::Real);
                }
                vec![arg]
            };
            let charset_name = ret_type.charset_name().to_owned();
            let collation_name = ret_type.collation_name().to_owned();
            // Go's rewriter overrides the coercibility of the CAST it builds:
            // IMPLICIT for a string target (ASCII repertoire only for the
            // ascii charset), NUMERIC/ASCII for any other. A CAST AS DATE in
            // a UNION branch therefore yields to the string branch's
            // collation instead of dragging the result to binary.
            let (coer, repe) = if ret_type.eval_type() == tidb_datatype::EvalType::String {
                let repe = if charset_name == "ascii" {
                    crate::expr_collation::Repertoire::ASCII
                } else {
                    crate::expr_collation::Repertoire::UNICODE
                };
                (crate::expr_collation::Coercibility::IMPLICIT, repe)
            } else {
                (
                    crate::expr_collation::Coercibility::NUMERIC,
                    crate::expr_collation::Repertoire::ASCII,
                )
            };
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
                    coer,
                    repe,
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
            // Go `convertFunctionClass.getFunction`: `bf.tp.SetFlen(mysql.MaxBlobWidth)`.
            ret_type.set_flen(16_777_216);
            result_type::promote_wide_string_result(&mut ret_type);
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
        // Go's parser builds `trim(str)`, `trim(str, remstr)` or
        // `trim(str, remstr, direction)` (`parseTrimFunc`): the direction is
        // an integer constant (`TrimBothDefault` 0, `TrimBoth` 1,
        // `TrimLeading` 2, `TrimTrailing` 3), the removal string defaults to
        // a space when only a direction is written, and `trimFunctionClass`
        // picks the signature by argument count.
        Expr::Trim {
            expr,
            remstr,
            direction,
        } => {
            let mut args = vec![rewrite_expr_resolved(expr, resolver)?];
            match (remstr, direction) {
                (Some(remstr), _) => args.push(rewrite_expr_resolved(remstr, resolver)?),
                (None, Some(_)) => args.push(constant_string(" ")),
                (None, None) => {}
            }
            if let Some(direction) = direction {
                let code = match direction {
                    tidb_ast::TrimDirection::Both => 1,
                    tidb_ast::TrimDirection::Leading => 2,
                    tidb_ast::TrimDirection::Trailing => 3,
                };
                args.push(Expression::Constant(Constant::new(
                    Datum::Int(code),
                    FieldType::new(FieldTypeCode::Tiny),
                )));
            }
            finish_function_call("trim", args, resolver)
        }
        Expr::Row(items) => {
            let args = items
                .iter()
                .map(|item| rewrite_expr_resolved(item, resolver))
                .collect::<Result<Vec<_>, _>>()?;
            // Go rowToScalarFunc uses the first element's type. The planner
            // expands this row constructor when comparing subquery operands.
            let ret_type = args
                .first()
                .and_then(Expression::static_type)
                .cloned()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::Null));
            Ok(Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("row"),
                ret_type,
                args,
            )))
        }
        // Go `expressionRewriter.Enter`: an aggregate that the plan's
        // aggregation did not map (nested in another aggregate's argument)
        // is `ErrInvalidGroupFuncUse` (1111), before its arguments are seen.
        Expr::Aggregate { .. } | Expr::GroupConcat { .. } => Err(EvalError::InvalidGroupFuncUse),
        // Go `matchAgainstToBuiltin`: a modifier the native FTS builtin cannot
        // carry through pushdown is refused outright; the default mode builds
        // the native function, which only a fulltext index can evaluate.
        Expr::MatchAgainst { modifier, .. } => {
            if modifier.is_boolean_mode() || modifier.with_query_expansion() {
                Err(EvalError::NotImplemented(
                    "MATCH...AGAINST with this modifier on the native FTS path (modifier is not carried through pushdown to TiFlash)"
                        .into(),
                ))
            } else {
                Err(EvalError::Unsupported(
                    "cannot use 'MATCH ... AGAINST' outside of fulltext index",
                ))
            }
        }
        // Go's preprocessor (`preprocess.go`, `*ast.FuncCallExpr` with a
        // schema) answers every qualified call with `ErrFunctionNotExists`,
        // naming the lowercased `schema.function`.
        Expr::GenericFuncCall { schema, name, .. } => Err(EvalError::FunctionNotExists(format!(
            "{}.{}",
            tidb_mysql::to_lowercase(schema),
            tidb_mysql::to_lowercase(name)
        ))),
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

    struct PreparedValues(Vec<Datum>);

    impl crate::Columns for PreparedValues {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn param_value(&self, order: usize) -> Result<Datum, EvalError> {
            self.0
                .get(order)
                .cloned()
                .ok_or(EvalError::Unsupported("unbound prepared parameter"))
        }
    }

    impl ColumnResolver for PreparedValues {
        fn resolve(&self, _: &[String]) -> Option<(usize, FieldType, i64)> {
            None
        }

        fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
            tidb_datatype::SessionTimeZone::utc()
        }

        fn param_value(&self, order: usize) -> Result<Datum, EvalError> {
            crate::Columns::param_value(self, order)
        }

        fn fold_constant(&self, expression: &mut Expression, mode: ConstantFoldMode) {
            crate::fold_constant_in_mode(expression, self, mode);
        }
    }

    #[test]
    fn explicit_cast_owns_folding_independently_of_parent_mode() {
        let context = PreparedValues(Vec::new());
        for mode in [
            ConstantFoldMode::Normal,
            ConstantFoldMode::Try,
            ConstantFoldMode::Disabled,
        ] {
            let resolver = FoldModeResolver::new(&context, mode);
            let integer = crate::simple_expr::parse_simple_expr(
                &resolver,
                "CAST('2' AS SIGNED)",
                &crate::simple_expr::BuildOptions::new(),
            )
            .unwrap();
            assert!(
                matches!(integer, Expression::Constant(ref constant) if constant.value == Datum::Int(2)),
                "{mode:?}: {integer:?}"
            );
            let json = crate::simple_expr::parse_simple_expr(
                &resolver,
                "CAST('2' AS JSON)",
                &crate::simple_expr::BuildOptions::new(),
            )
            .unwrap();
            assert!(
                matches!(json, Expression::ScalarFunction(ref function) if function.func_name.lowercase() == "cast_json"),
                "{mode:?}: {json:?}"
            );
        }
    }

    #[test]
    fn prepared_rewriter_retains_distinct_markers_with_equal_initial_values() {
        let ctx = PreparedValues(vec![Datum::Int(2), Datum::Int(2)]);
        let mut compiled = crate::simple_expr::parse_simple_expr(
            &ctx,
            "? - ?",
            &crate::simple_expr::BuildOptions::new(),
        )
        .unwrap();
        let Expression::Constant(planned) = &compiled else {
            panic!("missing parameter-dependent planning value");
        };
        assert_eq!(planned.value, Datum::Int(0));
        assert!(planned.literal_value().is_none());
        let Some(Expression::ScalarFunction(function)) = planned.deferred_expr.as_deref() else {
            panic!("parameter-dependent expression was discarded");
        };
        for (order, arg) in function.args.iter().enumerate() {
            let Expression::Constant(constant) = arg else {
                panic!("expected a marker");
            };
            assert_eq!(constant.param_marker.unwrap().order, order as i64);
            assert_eq!(constant.value, Datum::Int(2));
        }
        let hash = compiled.hash_code().to_vec();
        for (values, expected) in [(vec![2, 2], 0), (vec![9, 2], 7), (vec![2, 9], -7)] {
            let ctx = PreparedValues(values.into_iter().map(Datum::Int).collect());
            assert_eq!(
                crate::eval_expression_once(&compiled, &ctx).unwrap(),
                Datum::Int(expected),
            );
        }
        assert_eq!(compiled.hash_code(), hash);
    }

    #[test]
    fn prepared_rewriter_preserves_markers_under_lazy_nested_scopes() {
        let planning = PreparedValues(vec![
            Datum::Int(1),
            Datum::Int(10),
            Datum::Int(3),
            Datum::Int(40),
            Datum::Int(2),
        ]);
        let compiled = crate::simple_expr::parse_simple_expr(
            &planning,
            "IF(?, ? - ?, ? + ?)",
            &crate::simple_expr::BuildOptions::new(),
        )
        .unwrap();
        for (condition, expected) in [(Datum::Int(1), 7), (Datum::Int(0), 42), (Datum::Null, 42)] {
            let ctx = PreparedValues(vec![
                condition,
                Datum::Int(10),
                Datum::Int(3),
                Datum::Int(40),
                Datum::Int(2),
            ]);
            assert_eq!(
                crate::eval_expression_once(&compiled, &ctx).unwrap(),
                Datum::Int(expected),
            );
        }
        assert!(crate::simple_expr::parse_simple_expr(
            &PreparedValues(vec![]),
            "?",
            &crate::simple_expr::BuildOptions::new(),
        )
        .is_err());
    }

    #[test]
    fn prepared_rewriter_keeps_parameter_dependent_implicit_real_casts() {
        let planning = PreparedValues(vec![Datum::Int(2), Datum::Int(3)]);
        let compiled = crate::simple_expr::parse_simple_expr(
            &planning,
            "POW(?, ?)",
            &crate::simple_expr::BuildOptions::new(),
        )
        .unwrap();
        for (values, expected) in [([2, 3], 8.0), ([3, 2], 9.0), ([5, 0], 1.0)] {
            let context = PreparedValues(values.into_iter().map(Datum::Int).collect());
            assert_eq!(
                crate::eval_expression_once(&compiled, &context).unwrap(),
                Datum::Real(expected),
            );
        }
    }

    #[test]
    fn prepared_rewriter_keeps_changing_string_in_members() {
        let text = |value: &str| Datum::new_string(value.to_owned());
        let planning = PreparedValues(vec![text("a"), text("a"), text("a")]);
        let compiled = crate::simple_expr::parse_simple_expr(
            &planning,
            "? IN (?, ?, 'literal')",
            &crate::simple_expr::BuildOptions::new(),
        )
        .unwrap();
        for (values, expected) in [
            (vec![text("a"), text("a"), text("a")], Datum::Int(1)),
            (vec![text("b"), text("a"), text("b")], Datum::Int(1)),
            (vec![text("a"), text("b"), text("c")], Datum::Int(0)),
            (vec![text("a"), text("b"), Datum::Null], Datum::Null),
        ] {
            assert_eq!(
                crate::eval_expression_once(&compiled, &PreparedValues(values)).unwrap(),
                expected,
            );
        }
    }

    #[test]
    fn prepared_nested_planning_values_determine_parent_result_types() {
        let text = |value: &str| Datum::new_string(value.to_owned());
        // Live Go PREPARE/EXECUTE, plus builtin_time.go's getExpressionFsp,
        // strToDateFunctionClass.getRetTp and convertTzFunctionClass.getDecimal.
        for (sql, values, replay, code, flen, decimal, expected, replay_expected) in [
            (
                "TIME(CONCAT(?, '.1'))",
                vec![text("12:34:56")],
                vec![text("01:02:03")],
                FieldTypeCode::Duration,
                12,
                1,
                "12:34:56.1",
                "01:02:03.1",
            ),
            (
                "STR_TO_DATE(?, CONCAT('%Y', ?))",
                vec![text("2026-09-08"), text("-%m-%d")],
                vec![text("2025/02/03"), text("/%m/%d")],
                FieldTypeCode::Date,
                10,
                0,
                "2026-09-08",
                "2025-02-03",
            ),
            (
                "CONVERT_TZ(CONCAT(?, '.123'), '+00:00', '+00:00')",
                vec![text("2026-09-08 12:34:56")],
                vec![text("2025-02-03 01:02:03")],
                FieldTypeCode::Datetime,
                23,
                3,
                "2026-09-08 12:34:56.123",
                "2025-02-03 01:02:03.123",
            ),
        ] {
            let context = PreparedValues(values);
            let compiled = crate::simple_expr::parse_simple_expr(
                &context,
                sql,
                &crate::simple_expr::BuildOptions::new(),
            )
            .unwrap();
            let actual = compiled.static_type().unwrap();
            assert_eq!(
                (actual.code(), actual.flen(), actual.decimal()),
                (code, flen, decimal),
                "{sql}",
            );
            assert_eq!(
                crate::eval_expression_once(&compiled, &context)
                    .unwrap()
                    .sql_string()
                    .unwrap(),
                expected,
                "{sql}",
            );
            assert_eq!(
                crate::eval_expression_once(&compiled, &PreparedValues(replay))
                    .unwrap()
                    .sql_string()
                    .unwrap(),
                replay_expected,
                "replayed {sql}",
            );
        }
    }

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
            (call("SM3", vec![str_arg("a")]), 40, "sm3"),
            (
                call("SHA2", vec![str_arg("a"), int_arg("256")]),
                128,
                "sha2",
            ),
            // `passwordFunctionClass` (`:487`) -- `mysql.PWDHashLen + 1`, the
            // 40 hex digits plus the leading `*`.
            (call("PASSWORD", vec![str_arg("x")]), 41, "password"),
            // `compressFunctionClass`: zlib's upper bound for five input
            // bytes, 5 + 13 here because all shifted terms are zero.
            (call("COMPRESS", vec![str_arg("hello")]), 18, "compress"),
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

        for name in ["AES_ENCRYPT", "AES_DECRYPT"] {
            let built = rewrite_expr(&call(name, vec![str_arg("a"), str_arg("k")])).unwrap();
            assert_eq!(
                built.static_type().unwrap().charset_name(),
                "binary",
                "{name}"
            );
        }
    }

    /// `HandleBinaryLiteral` runs after collation derivation in Go. A BINARY
    /// CONCAT result therefore encodes the GBK argument before appending the
    /// raw suffix; deriving only after construction would retain UTF-8 bytes.
    #[test]
    fn concat_derives_binary_before_wrapping_gbk_string() {
        struct GbkResolver;

        impl ColumnResolver for GbkResolver {
            fn resolve(&self, _path: &[String]) -> Option<(usize, FieldType, i64)> {
                None
            }

            fn connection_charset_info(&self) -> (&str, &str) {
                ("gbk", "gbk_bin")
            }

            fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
                tidb_datatype::SessionTimeZone::utc()
            }
        }

        let expression = Expr::Func {
            name: "CONCAT".to_owned(),
            args: vec![
                Expr::String("中文".to_owned()),
                Expr::Hex("D2BB".to_owned()),
            ],
            origin_position: 0,
        };
        let rewritten = rewrite_expr_resolved(&expression, &GbkResolver).unwrap();
        let mut chunk = Chunk::new_empty(&[]);
        chunk.set_num_virtual_rows(1);
        let value = rewritten
            .eval(&NoColumns, chunk.get_row(0))
            .expect("mixed-charset CONCAT evaluates");

        assert_eq!(
            value.as_raw_bytes(),
            Some(&[0xd6, 0xd0, 0xce, 0xc4, 0xd2, 0xbb][..])
        );
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

    /// Go `inFunctionClass.getFunction` coerces every argument to the FIRST
    /// argument's eval type (`newBaseBuiltinFuncWithTp`, `builtin.go:184`) and
    /// folds each strict-constant cast (`BuildCastFunctionWithCheck` ends in
    /// `FoldConstant`). A DATE column's string IN-list therefore becomes bare
    /// `datetime(6)` constants in the built node — EXPLAIN renders
    /// `in(col, 1998-01-02 00:00:00.000000, ...)` (the q83 shape), not quoted
    /// strings, and the tested column itself is not wrapped (DATE is already
    /// in the DATETIME comparison domain).
    #[test]
    fn in_list_coerces_string_candidates_to_the_column_eval_type() {
        struct DateColumnResolver;

        impl ColumnResolver for DateColumnResolver {
            fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
                (path.last().is_some_and(|name| name == "d_date"))
                    .then(|| (0, FieldType::new(FieldTypeCode::Date), 1))
            }

            fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
                tidb_datatype::SessionTimeZone::utc()
            }
        }

        let expression = Expr::In {
            expr: Box::new(Expr::Column(vec![
                "tpcds50".to_owned(),
                "date_dim".to_owned(),
                "d_date".to_owned(),
            ])),
            list: vec![
                Expr::String("1998-01-02".to_owned()),
                Expr::String("1998-10-15".to_owned()),
                Expr::String("1998-11-10".to_owned()),
            ],
            not: false,
        };
        let rewritten = rewrite_expr_resolved(&expression, &DateColumnResolver).unwrap();
        let Expression::ScalarFunction(function) = &rewritten else {
            panic!("expected the in() node, got {rewritten:?}")
        };
        assert_eq!(function.func_name.lowercase(), "in");
        assert_eq!(function.args.len(), 4, "column + three candidates");
        assert!(
            matches!(function.args[0], Expression::Column(_)),
            "the tested column stays a column: {:?}",
            function.args[0]
        );
        let expected = [
            "1998-01-02 00:00:00.000000",
            "1998-10-15 00:00:00.000000",
            "1998-11-10 00:00:00.000000",
        ];
        for (candidate, want) in function.args[1..].iter().zip(expected) {
            let Expression::Constant(constant) = candidate else {
                panic!("expected a folded constant candidate, got {candidate:?}")
            };
            let Datum::Time(time) = &constant.value else {
                panic!("expected a datetime constant, got {:?}", constant.value)
            };
            // `WrapWithCastAsTime` raises a string source to `MaxFsp`.
            assert_eq!(time.fsp(), 6);
            // Rendered bare, like Go's `Time.String()` in EXPLAIN.
            assert_eq!(time.to_string(), want);
        }
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
    fn decimal_arithmetic_casts_an_integer_column_to_decimal() {
        struct ArithmeticColumns;

        impl ColumnResolver for ArithmeticColumns {
            fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
                let (index, mut field_type) = match path {
                    [name] if name == "price" => (0, FieldType::new(FieldTypeCode::NewDecimal)),
                    [name] if name == "qty" => (1, FieldType::new(FieldTypeCode::Long)),
                    _ => return None,
                };
                if index == 0 {
                    field_type.set_flen(10);
                    field_type.set_decimal(2);
                } else {
                    field_type.set_flen(10);
                    field_type.set_decimal(0);
                }
                Some((index, field_type, (index + 1) as i64))
            }

            fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
                tidb_datatype::SessionTimeZone::utc()
            }
        }

        let expression = Expr::Binary(
            BinaryOp::Mul,
            Box::new(Expr::Column(vec!["price".to_owned()])),
            Box::new(Expr::Column(vec!["qty".to_owned()])),
        );
        let Expression::ScalarFunction(multiply) =
            rewrite_expr_resolved(&expression, &ArithmeticColumns).unwrap()
        else {
            panic!("DECIMAL multiplication was not rewritten as a scalar function");
        };
        let Expression::ScalarFunction(cast) = &multiply.args[1] else {
            panic!("integer multiplication operand was not cast to DECIMAL");
        };
        assert_eq!(cast.func_name.lowercase(), "cast_decimal");
        let target = cast.ret_type.as_ref().unwrap();
        assert_eq!(target.code(), FieldTypeCode::NewDecimal);
        assert_eq!(target.flen(), 10);
        assert_eq!(target.decimal(), 0);
        assert_ne!(target.flags() & tidb_datatype::FieldTypeFlags::BINARY, 0);
    }

    #[test]
    fn power_wraps_integer_expressions_as_real() {
        let minus = Expr::Binary(
            BinaryOp::Minus,
            Box::new(Expr::Binary(
                BinaryOp::Minus,
                Box::new(Expr::Int("5".to_owned())),
                Box::new(Expr::Int("1".to_owned())),
            )),
            Box::new(Expr::Int("2".to_owned())),
        );
        let power = Expr::Func {
            name: "POWER".to_owned(),
            args: vec![minus, Expr::Int("2".to_owned())],
            origin_position: 0,
        };
        let rewritten = rewrite_expr(&power).unwrap();
        let Expression::ScalarFunction(power_call) = &rewritten else {
            panic!("POWER was not rewritten as a scalar function");
        };
        assert_eq!(power_call.func_name.lowercase(), "power");
        let Expression::ScalarFunction(cast) = &power_call.args[0] else {
            panic!("POWER's integer expression was not cast to real");
        };
        assert_eq!(cast.func_name.lowercase(), "cast_double");
        let Expression::Constant(exponent) = &power_call.args[1] else {
            panic!("POWER's integer literal was not folded to a real constant");
        };
        assert_eq!(exponent.value, Datum::Real(2.0));
        assert_eq!(eval_const(&power), Datum::Real(4.0));
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
    fn nested_planner_column_keeps_its_scalar_subquery_constant() {
        struct PlannerConstant;

        impl ColumnResolver for PlannerConstant {
            fn resolve(&self, _path: &[String]) -> Option<(usize, FieldType, i64)> {
                None
            }

            fn resolve_constant(&self, path: &[String]) -> Option<Expression> {
                if path != ["scalar"] {
                    return None;
                }
                let mut constant =
                    Constant::new(Datum::Int(2), FieldType::new(FieldTypeCode::LongLong));
                constant.subquery_ref_id = 7;
                Some(Expression::Constant(constant))
            }

            fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
                tidb_datatype::SessionTimeZone::utc()
            }
        }

        let expression = Expr::Binary(
            BinaryOp::Gt,
            Box::new(Expr::Int("3".to_owned())),
            Box::new(Expr::Column(vec!["scalar".to_owned()])),
        );
        let Expression::ScalarFunction(comparison) =
            rewrite_expr_resolved(&expression, &PlannerConstant).unwrap()
        else {
            panic!("expected a comparison");
        };
        let Expression::Constant(constant) = &comparison.args[1] else {
            panic!("planner-owned scalar was not rewritten to a constant");
        };
        assert_eq!(constant.value, Datum::Int(2));
        assert_eq!(constant.subquery_ref_id, 7);
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

    #[test]
    fn json_result_batch_signatures_keep_source_types_and_widths() {
        for (sql, code, flen) in [
            (
                "json_search('[\"x\"]','one','x')",
                FieldTypeCode::Json,
                16_777_216,
            ),
            ("json_type('{}')", FieldTypeCode::VarString, 51),
            ("json_quote('abc')", FieldTypeCode::VarString, 20),
            ("json_unquote('\"x\"')", FieldTypeCode::VarString, 3),
            (
                "json_unquote(json_array(1))",
                FieldTypeCode::LongBlob,
                4_294_967_295,
            ),
            ("json_pretty('{}')", FieldTypeCode::LongBlob, 67_108_864),
        ] {
            let field = ret_type(sql);
            assert_eq!((field.code(), field.flen()), (code, flen), "{sql}");
        }
        assert_eq!(
            eval("json_array(json_search('[\"x\"]','one','x'))"),
            Datum::new_json(tidb_datatype::BinaryJSON::parse("[\"$[0]\"]").unwrap())
        );
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
            ("tidb_decode_plan('')", unspecified, false),
            ("tidb_decode_binary_plan('')", unspecified, false),
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
        let encoded = tidb_plancodec::compress(b"0\t1\t0\t1\teq(a, 1)\n");
        assert_eq!(
            eval(&format!("tidb_decode_plan('{encoded}')")),
            text_datum(
                "\tid       \ttask\testRows\toperator info\n\tSelection\troot\t1      \teq(a, 1)"
            )
        );
        assert_eq!(
            eval("tidb_decode_plan('malformed')"),
            text_datum("malformed")
        );
        let binary_discarded = tidb_plancodec::BINARY_PLAN_DISCARDED_ENCODED.as_str();
        assert_eq!(
            eval(&format!("tidb_decode_binary_plan('{binary_discarded}')")),
            text_datum("(plan discarded because too long)")
        );
        assert_eq!(
            eval("time_format('23:00:00', '%H %k')"),
            text_datum("23 23")
        );
    }
}
