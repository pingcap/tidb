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

//! What Go's `expression.BuildSimpleExpr` builds against: the column scope and
//! session reads of its `BuildContext` and `BuildOptions` (a
//! [`ColumnResolver`]), and the function variable itself. The AST rewrite
//! behind it is the planner's `expressionRewriter`
//! (`tidb_planner::expression_rewriter::leave`), which installs itself here
//! as Go's `core_init.go` sets `expression.BuildSimpleExpr`.

use std::sync::OnceLock;

use crate::column::Column;
use crate::constant_fold::ConstantFoldMode;
use crate::expression::Expression;
use crate::grouping::GroupingMode;
use crate::EvalError;
use tidb_ast::Expr;
use tidb_datatype::{Datum, FieldType};

/// The rewrite `expression.BuildSimpleExpr` runs.
pub type RewriteExprFunc = fn(&Expr, &dyn ColumnResolver) -> Result<Expression, EvalError>;

static REWRITE_EXPR_FUNC: OnceLock<RewriteExprFunc> = OnceLock::new();

/// Go `expression.BuildSimpleExpr = buildSimpleExpr` (`core_init.go`).
pub fn set_rewrite_expr_func(rewrite: RewriteExprFunc) {
    let _ = REWRITE_EXPR_FUNC.set(rewrite);
}

/// Rewrites a parsed AST expression without a column scope.
pub fn rewrite_expr(expr: &Expr) -> Result<Expression, EvalError> {
    rewrite_expr_resolved(expr, &NoResolver)
}

/// Go `expression.BuildSimpleExpr`: `Expr::Column` paths are bound through
/// `resolver`. Panics, as Go's nil function variable does, when no planner
/// installed the rewrite (`tidb_planner::init`).
pub fn rewrite_expr_resolved(
    expr: &Expr,
    resolver: &dyn ColumnResolver,
) -> Result<Expression, EvalError> {
    #[cfg(test)]
    let rewrite = REWRITE_EXPR_FUNC.get_or_init(|| crate::planner_expression_rewriter::build_simple_expr);
    #[cfg(not(test))]
    let rewrite = REWRITE_EXPR_FUNC
        .get()
        .expect("expression.BuildSimpleExpr is not installed: call tidb_planner::init()");
    rewrite(expr, resolver)
}

/// What Go's `ast.Grouping` rewrite takes from `planCtx.rollExpand`: the
/// Expand's grouping-id column (`GID`), its `GroupingMode`, and
/// `GenerateGroupingMarks` over the GROUP BY columns the GROUPING arguments
/// resolve to (`ResolveGroupingFuncArgsInGroupBy`).
pub struct RollExpandGrouping {
    /// Go `rollExpand.GID`.
    pub gid: Column,
    /// Go `rollExpand.GroupingMode`.
    pub grouping_mode: GroupingMode,
    /// Go `rollExpand.GenerateGroupingMarks(resolvedCols)`.
    pub grouping_marks: Vec<std::collections::BTreeSet<u64>>,
}

/// Go `expressionRewriter.planCtx` as `matchAgainstToExpression` reads it:
/// the plan builder's clause and node stack, the statement's alternative
/// logical plan signals and the info schema.
pub trait MatchAgainstPlanCtx {
    /// Go `inDirectMatchBooleanContext` for the MATCH `node`: it is under
    /// WHERE, HAVING or ON, and every ancestor is AND, OR, NOT or parentheses.
    fn in_direct_match_boolean_context(&self, node: &Expr) -> bool;
    /// Go `SessionVars.EnableAlternativeLogicalPlans`.
    fn enable_alternative_logical_plans(&self) -> bool;
    /// Go `StmtCtx.AlternativeLogicalPlanFTSLikeFallback`.
    fn alternative_logical_plan_fts_like_fallback(&self) -> bool;
    /// Go `PlanBuilder.MarkPredicateMatch`.
    fn mark_predicate_match(&self);
    /// Go `PlanBuilder.MarkNonViableFTSMatch`.
    fn mark_non_viable_fts_match(&self);
    /// Go `ftsNativeViable`'s test of one matched column: its table has an
    /// available TiFlash replica and a public FULLTEXT index on the column.
    fn fts_native_viable_column(&self, path: &[String]) -> bool;
}

/// Resolves a dotted column path to an output column, standing in for the
/// schema/name resolution Go's `expression_rewriter` performs against the
/// plan's schema (`resolveColumn`).
pub trait ColumnResolver {
    /// Go `er.planCtx` for a MATCH AGAINST; a rewrite outside the planner
    /// has none.
    fn match_against_plan_ctx(&self) -> Option<&dyn MatchAgainstPlanCtx> {
        None
    }

    /// Go `planCtx.rollExpand` as the rewriter's `ast.Grouping` arm reads it
    /// for the rewritten GROUPING arguments `args`. Outside a ROLLUP block it
    /// is nil, which is `ErrInvalidGroupFuncUse`.
    fn roll_expand_grouping(&self, _args: &[Expression]) -> Result<RollExpandGrouping, EvalError> {
        Err(EvalError::InvalidGroupFuncUse)
    }

    /// Resolves `path` (e.g. `["t", "a"]` or `["a"]`) to
    /// `(row index, result type, unique id)`, or `None` when unknown.
    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)>;

    /// Go's session user-variable owner: declared types are resolved at build time.
    fn user_vars(&self) -> Option<&tidb_variable::user_vars::UserVars> {
        None
    }

    /// Go `er.planCtx.builder.ctx.BuiltinFunctionUsageInc`. A rewrite with no
    /// plan builder behind it (Go's nil `planCtx`) counts nothing.
    fn builtin_function_usage_inc(&self, _scalar_func_sig_name: &str) {}

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

    /// The name Go's `toColumn` quotes for a column that resolved to nothing:
    /// `v.String()`, the reference as written.
    fn unknown_column_name(&self, path: &[String]) -> String {
        path.join(".")
    }

    /// Go `toColumn`'s answer for a column no scope resolves: by default
    /// `ErrUnknownColumn` over [`Self::unknown_column_name`] in
    /// [`Self::clause_message`].
    fn unknown_column_error(&self, path: &[String]) -> EvalError {
        EvalError::UnknownColumnInClause(self.unknown_column_name(path), self.clause_message())
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

    /// Resolves `VALUES(column)` to the offset and type Go's rewriter hands
    /// `NewValuesFunc`. Go's rewriter builds it in every scope over the
    /// column the current schema names (`FindFieldName`, else 1054); an
    /// INSERT's ON DUPLICATE KEY UPDATE resolves `insertPlan.TableColNames`
    /// instead, which its resolver overrides this with. Outside that scope
    /// the function reads an empty `CurrInsertValues` and answers NULL.
    fn resolve_values(&self, path: &[String]) -> Option<(usize, FieldType)> {
        let (index, field_type, _) = self.resolve(path)?;
        Some((index, field_type))
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
    fn comparison_context(&self) -> Option<&dyn tidb_exprctx::context::Columns> {
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
    fn match_against_plan_ctx(&self) -> Option<&dyn MatchAgainstPlanCtx> {
        (**self).match_against_plan_ctx()
    }
    fn user_vars(&self) -> Option<&tidb_variable::user_vars::UserVars> {
        (**self).user_vars()
    }
    fn builtin_function_usage_inc(&self, scalar_func_sig_name: &str) {
        (**self).builtin_function_usage_inc(scalar_func_sig_name);
    }
    fn allow_build_cast_array(&self) -> bool {
        (**self).allow_build_cast_array()
    }

    fn roll_expand_grouping(&self, args: &[Expression]) -> Result<RollExpandGrouping, EvalError> {
        (**self).roll_expand_grouping(args)
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
    fn unknown_column_name(&self, path: &[String]) -> String {
        (**self).unknown_column_name(path)
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
    fn resolve_values(&self, path: &[String]) -> Option<(usize, FieldType)> {
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

/// Go `EvalSimpleAst`'s build context: a rewrite over no schema that builds
/// and folds each node in the evaluation context itself, so the fold reads
/// the SQL mode and raises its warnings where the evaluation would.
pub struct ContextResolver<'a>(pub &'a dyn tidb_exprctx::context::Columns);

impl ColumnResolver for ContextResolver<'_> {
    fn user_vars(&self) -> Option<&tidb_variable::user_vars::UserVars> {
        self.0.user_vars()
    }

    fn param_value(&self, order: usize) -> Result<Datum, EvalError> {
        self.0.param_value(order)
    }

    fn resolve(&self, _path: &[String]) -> Option<(usize, FieldType, i64)> {
        None
    }

    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        self.0.time_zone()
    }

    fn date_modes(&self) -> tidb_datatype::DateModes {
        self.0.date_modes()
    }

    fn connection_charset_info(&self) -> (&str, &str) {
        self.0.connection_charset_info()
    }

    fn like_default_escape(&self) -> u8 {
        self.0.like_default_escape()
    }

    fn no_unsigned_subtraction(&self) -> bool {
        self.0.no_unsigned_subtraction()
    }

    fn comparison_context(&self) -> Option<&dyn tidb_exprctx::context::Columns> {
        Some(self.0)
    }

    fn div_precision_increment(&self) -> u32 {
        self.0.div_precision_increment()
    }

    fn current_database(&self) -> Option<String> {
        self.0.current_database()
    }

    fn fold_constant(&self, expression: &mut Expression, mode: ConstantFoldMode) {
        if mode != ConstantFoldMode::Disabled {
            crate::constant_fold::derive_constant_null_flag(expression);
        }
        crate::constant_fold::fold_constant_in_mode(expression, self.0, mode);
    }

    fn eval_constant(&self, expression: &Expression) -> Result<Datum, EvalError> {
        crate::eval_expression_once(expression, self.0)
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
