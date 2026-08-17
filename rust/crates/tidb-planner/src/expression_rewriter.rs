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

//! The PLAN-AWARE half of expression rewriting: subqueries become plan nodes.
//!
//! Go sources:
//! * `pkg/planner/core/expression_rewriter.go` — `expressionRewriter`,
//!   `exprRewriterPlanCtx`, and every `handle*Subquery` member.
//! * `pkg/planner/core/logical_plan_builder.go` — `buildSemiJoin`
//!   (line 5761), `buildSemiApply` (5718), `buildApplyWithJoinType` (5697),
//!   `buildMaxOneRow` (5754) and `buildDistinct` (1966), which are the plan
//!   constructors those members call.
//! * `pkg/planner/util/coreusage/correlated_misc.go` —
//!   `ExtractCorrelatedCols4LogicalPlan` and `ExtractCorColumnsBySchema`.
//! * `pkg/planner/core/planbuilder.go` — `clauseCode` (line 114) and
//!   `subQueryCtx` (line 196).
//!
//! # What is here and what is NOT
//!
//! `pkg/planner/core/expression_rewriter.go` is two rewriters wearing one
//! struct. The EXPRESSION half — `ast.ExprNode` to `expression.Expression`,
//! i.e. `Leave`'s `binaryOpToExpression` / `caseToExpression` /
//! `inToExpression` / `funcCallToExpression` / `betweenToExpression` and their
//! ~40 kin — is ALREADY PORTED in [`tidb_expr::rewriter`], and is REUSED, not
//! restated: nothing in this module rebuilds an AST node into an
//! `Expression`.
//!
//! What only exists once a PLAN TREE exists is here:
//!
//! * `EXISTS`, `IN`, `= ANY` / `!= ALL`, the ordered quantifiers and scalar
//!   subqueries, each of which turns the outer plan into a
//!   [`LogicalApply`]/[`LogicalJoin`] over the inner plan;
//! * correlated-column resolution against the outer plans' schemas
//!   (`toColumn`'s `outerSchemas` walk), which is what MAKES a
//!   [`CorrelatedColumn`];
//! * the plan-carrying rewriter state, `exprRewriterPlanCtx`.
//!
//! SEED of `pkg/planner/core/expression_rewriter.go`: the subquery-to-plan
//! surface lands complete, and the AST-driven `Enter`/`Leave` driver does not
//! — see the boundaries below.
//!
//! # Restructuring, named
//!
//! Go mutates `planCtx.plan` in place. Here every handler TAKES the outer plan
//! by value and RETURNS the replacement, which is the same rule the logical
//! tree's own rewrites follow (`fn(self, ...) -> LogicalPlan`) and which lets
//! the outer plan be MOVED into the new apply's child list instead of cloned.
//! [`ExprRewriterPlanCtx`] therefore carries the scopes and the clause, not
//! the plan.
//!
//! Go reports errors by assigning `er.err` and returning; every handler here
//! returns `Result`, so an error cannot be read past.
//!
//! # Boundaries, by exact Go symbol
//!
//! Each is a symbol whose dependency is genuinely absent from the workspace,
//! not a body that was skipped. The dependency-closed part of every one of
//! them is still answered.
//!
//! * `expressionRewriter.Enter` / `Leave` / `buildSubquery`
//!   (`expression_rewriter.go:535`, `1672`, `485`). These walk `ast.Node` and
//!   call `PlanBuilder.buildResultSetNode` to PLAN the subquery. Neither
//!   `ast.Node` nor `PlanBuilder` is transcreated, so this module takes the
//!   already-built inner plan `np` as an argument — exactly what `buildSubquery`
//!   returns — together with the `hint_flags` it also returns. Everything
//!   downstream of that call is ported.
//! * `DoOptimize` / `EvalSubqueryFirstRow` (`optimizer.go`, `common_plans.go`).
//!   `handleScalarSubquery` and `handleExistSubquery` have a second path that
//!   OPTIMIZES and EXECUTES an uncorrelated subquery at plan time and folds the
//!   result to a constant. There is no optimizer driver in this crate (see the
//!   crate header) and no executor handle, so that path is
//!   [`ScalarSubqueryOutcome::EvaluateSeparately`]: the handler reports that the
//!   subquery is separately evaluable and hands back the inner plan rather than
//!   silently building an apply Go would not build.
//! * `setIsInApplyForCTE` (`logical_plan_builder.go:5735`). It sets
//!   `CTEClass.IsInApply` on a `*CTEClass` SHARED between the producer and every
//!   consumer. The logical tree deliberately holds no shared handle on a child
//!   edge, so there is nothing here to write through; the call site is marked.
//! * `hint.TableHintInfo` / `SetPreferredJoinTypeAndOrder`
//!   (`logical_plan_builder.go:5714`, `5844`). Join and aggregation hint
//!   preference is a `hint.TableHintInfo` field copy; the hint info struct is
//!   not transcreated. [`RewriterHints`] carries the three fields the ported
//!   bodies actually read (`PreferAggType`, `PreferAggToCop`,
//!   `PreferJoinType`'s rewrite bit) so the semantics survive.
//! * `expression.CheckAndDeriveCollationFromExprs` and
//!   `collate.CompatibleCollate` (`expression/collation.go`). The compare-subquery
//!   collation check and the IN-to-join collation guard are inputs to this
//!   module's decisions rather than its work: the IN-to-join guard is the
//!   `collations_compatible` argument of
//!   [`ExpressionRewriter::handle_in_subquery`], and the compare-subquery
//!   refusal is a marked `// boundary:` in
//!   [`ExpressionRewriter::handle_compare_subquery`]'s body.
//!
//! # Narrowings, by name
//!
//! * `ExtractCorColumnsBySchema`'s `corCol.Data = resultCorCols[idx].Data`
//!   ALIASES one `*types.Datum` between the outer apply and every inner
//!   reference, which is how the apply loop publishes the current outer row.
//!   Rust has no such alias on an owned tree;
//!   [`extract_cor_columns_by_schema`] returns the resolved columns and the
//!   binding is the apply executor's job (`tidb-executor`'s driver already
//!   binds by column identity — see this module's harvest note).
//! * `ScalarSubQueryExpr` and `ScalarSubqueryEvalCtx` (`common_plans.go`)
//!   belong to the separately-evaluated path above and are not modelled.
//! * `er.disableFoldCounter`, `er.preprocess`, `er.astNodeStack`,
//!   `er.sourceTable`, `er.windowMap`, `er.insertPlan`, `er.rollExpand` are
//!   `Enter`/`Leave` state for the AST half and have no reader here.
//!
//! # Harvested rather than written fresh
//!
//! `tidb-executor`'s `driver/subquery.rs`, `driver/decorrelate_exists.rs` and
//! `driver/correlated_agg_decorrelate.rs` carry Go-faithful decorrelation
//! written against the driver's `QueryStmt` rather than a plan tree. What moved
//! onto the IR here: their two-scope correlated-reference test (inner scope
//! first, then outer — [`ExpressionRewriter::to_column`] below is the same rule against
//! `outer_schemas`), their `SubqueryUse` split of what the outer expression
//! asks of a subquery ([`SubQueryCtx`] is Go's own spelling of it), and their
//! `EXISTS`-to-semi-join shape. What is written fresh: everything that needs a
//! plan node — the apply/join constructors, the quantifier aggregation plans,
//! and `popExistsSubPlan`.

use tidb_datatype::{FieldName, FieldType, FieldTypeCode, FieldTypeFlags, QualifiedColumnName};
use tidb_expr::aggregation::AggFuncDesc;
use tidb_expr::column::{Column, CorrelatedColumn};
use tidb_expr::constant::Constant;
use tidb_expr::expr_util::builder::{FunctionBuilder, RealFunctionBuilder};
use tidb_expr::expr_util::extract::set_expr_column_in_operand;
use tidb_expr::expr_util::normal_form::split_cnf_items;
use tidb_expr::expr_util::predicates::{get_func_arg, get_row_len};
use tidb_expr::expr_util::substitute::SubstituteOptions;
use tidb_expr::expression::Expression;
use tidb_expr::find_field_name;
use tidb_expr::schema::{merge_schema, Schema};
use tidb_expr::simple_expr::{compose_cnf_condition, compose_dnf_condition, extract_columns};
use tidb_expr::Columns;

use crate::find_best_task::LogicalJoinType;
use crate::logical::aggregation::LogicalAggregation;
use crate::logical::apply::LogicalApply;
use crate::logical::join::LogicalJoin;
use crate::logical::limit::LogicalLimit;
use crate::logical::max_one_row::LogicalMaxOneRow;
use crate::logical::projection::LogicalProjection;
use crate::logical::table_dual::LogicalTableDual;
use crate::logical::{BaseLogicalPlan, LogicalPlan};
use crate::plan_base::PlanIdAllocator;

// ***** planbuilder.go: the two small enums the rewriter is keyed on *****

/// Go `clauseCode` (`planbuilder.go:114`): which clause the column is in.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ClauseCode {
    /// Go `unknowClause` (the source's own spelling).
    #[default]
    Unknow,
    /// Go `fieldList`.
    FieldList,
    /// Go `havingClause`.
    Having,
    /// Go `onClause`.
    On,
    /// Go `orderByClause`.
    OrderBy,
    /// Go `whereClause`.
    Where,
    /// Go `groupByClause`.
    GroupBy,
    /// Go `showStatement`.
    ShowStatement,
    /// Go `globalOrderByClause`.
    GlobalOrderBy,
    /// Go `expressionClause`.
    Expression,
    /// Go `windowOrderByClause`.
    WindowOrderBy,
    /// Go `partitionByClause`.
    PartitionBy,
}

impl ClauseCode {
    /// Go `clauseMsg` (`planbuilder.go:132`): the text an error names it by.
    #[must_use]
    pub const fn message(self) -> &'static str {
        match self {
            Self::Unknow => "",
            Self::FieldList => "field list",
            Self::Having => "having clause",
            Self::On => "on clause",
            Self::OrderBy => "order clause",
            Self::Where => "where clause",
            Self::GroupBy => "group statement",
            Self::ShowStatement => "show statement",
            Self::GlobalOrderBy => "global ORDER clause",
            Self::Expression => "expression",
            Self::WindowOrderBy => "window order by",
            Self::PartitionBy => "window partition by",
        }
    }
}

/// Go `subQueryCtx` (`planbuilder.go:196`): which subquery form is being
/// built, which `isNoDecorrelate` and the hint diagnostics read.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SubQueryCtx {
    /// Go `notHandlingSubquery`.
    #[default]
    NotHandlingSubquery,
    /// Go `handlingExistsSubquery`.
    Exists,
    /// Go `handlingCompareSubquery`.
    Compare,
    /// Go `handlingInSubquery`.
    In,
    /// Go `handlingScalarSubquery`.
    Scalar,
}

/// Go `hint.HintFlagSemiJoinRewrite` (`util/hint/hint.go:138`).
pub const HINT_FLAG_SEMI_JOIN_REWRITE: u64 = 1 << 0;
/// Go `hint.HintFlagNoDecorrelate` (`util/hint/hint.go:139`).
pub const HINT_FLAG_NO_DECORRELATE: u64 = 1 << 1;
/// Go `h.PreferRewriteSemiJoin`, the `PreferJoinType` bit `buildSemiJoin`
/// sets when the semi join must be rewritten to an inner join plus aggregate.
pub const PREFER_REWRITE_SEMI_JOIN: u32 = 1 << 12;

// ***** allocators and the ambient context *****

/// Go `SessionVars.AllocPlanColumnID()`: the source of a plan column's
/// `UniqueID`.
///
/// The plan-id counter already lives on [`PlanIdAllocator`]; column ids are a
/// separate Go counter with a wider type, so they get their own here.
#[derive(Debug, Default)]
pub struct ColumnIdAllocator {
    next: std::sync::atomic::AtomicI64,
}

impl ColumnIdAllocator {
    /// A fresh allocator whose first [`Self::alloc`] returns `1`.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            next: std::sync::atomic::AtomicI64::new(0),
        }
    }

    /// Go `PlanColumnID.Add(1)`.
    pub fn alloc(&self) -> i64 {
        self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1
    }
}

/// The session switches the ported bodies branch on, each named for its Go
/// `SessionVars` field.
#[derive(Clone, Copy, Debug, Default)]
pub struct RewriterSessionFlags {
    /// Go `GetAllowInSubqToJoinAndAgg()`.
    pub allow_in_subq_to_join_and_agg: bool,
    /// Go `EnableCorrelateSubquery`.
    pub enable_correlate_subquery: bool,
    /// Go `EnableAlternativeLogicalPlans`.
    pub enable_alternative_logical_plans: bool,
    /// Go `EnableNoDecorrelateInSelect`.
    pub enable_no_decorrelate_in_select: bool,
    /// Go `EnableSemiJoinRewrite`.
    pub enable_semi_join_rewrite: bool,
    /// Go `PlanBuilder.disableSubQueryPreprocessing`.
    pub disable_subquery_preprocessing: bool,
}

/// The slice of `hint.TableHintInfo` the ported bodies read; see the module
/// header's boundary note.
#[derive(Clone, Copy, Debug, Default)]
pub struct RewriterHints {
    /// Go `TableHintInfo.PreferAggType`, copied onto every aggregation this
    /// module builds.
    pub prefer_agg_type: u32,
    /// Go `TableHintInfo.PreferAggToCop`.
    pub prefer_agg_to_cop: bool,
}

/// Everything the rewriter needs from its surroundings: the expression
/// builder's context, the two id counters, and the session state.
pub struct RewriterEnv<'a, C: Columns> {
    /// Go `b.ctx.GetExprCtx()` / `er.sctx`.
    pub ctx: &'a C,
    /// Go `PlanID`.
    pub plan_ids: &'a PlanIdAllocator,
    /// Go `PlanColumnID`.
    pub column_ids: &'a ColumnIdAllocator,
    /// Go `b.getSelectOffset()`.
    pub select_offset: i32,
    /// Go `SessionVars`, narrowed.
    pub flags: RewriterSessionFlags,
    /// Go `b.TableHints()`, narrowed.
    pub hints: RewriterHints,
}

impl<C: Columns> RewriterEnv<'_, C> {
    fn base(&self, tp: &str) -> BaseLogicalPlan {
        BaseLogicalPlan::new(self.plan_ids, tp, self.select_offset)
    }

    fn builder(&self) -> RealFunctionBuilder<'_, C> {
        RealFunctionBuilder::new(self.ctx)
    }

    /// A fresh plan column of `ret_type`, Go's
    /// `&expression.Column{UniqueID: AllocPlanColumnID(), RetType: ...}`.
    fn new_plan_column(&self, ret_type: FieldType) -> Column {
        Column::new(self.column_ids.alloc(), ret_type)
    }
}

/// Go `exprRewriterPlanCtx` (`expression_rewriter.go:335`), minus the plan
/// itself — see the module header's restructuring note.
#[derive(Clone, Debug, Default)]
pub struct ExprRewriterPlanCtx {
    /// Go `curClause`.
    pub cur_clause: ClauseCode,
    /// Go `PlanBuilder.outerSchemas`, outermost first, which is what makes a
    /// column reference correlated.
    pub outer_schemas: Vec<Schema>,
    /// Go `PlanBuilder.outerNames`, index-parallel to `outer_schemas`.
    pub outer_names: Vec<Vec<FieldName>>,
    /// Go `PlanBuilder.inUpdateStmt || b.inDeleteStmt`, read only by
    /// [`should_remap_redundant_base_column`].
    pub in_dml_stmt: bool,
}

/// What went wrong, in the shape Go assigns to `er.err`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RewriteError {
    /// Go `expression.ErrOperandColumns.GenWithStackByArgs(n)`.
    OperandColumns(usize),
    /// Go `plannererrors.ErrAmbiguous.GenWithStackByArgs(name, clause)`.
    Ambiguous(String, &'static str),
    /// Go `plannererrors.ErrUnknownColumn.GenWithStackByArgs(name, clause)`.
    UnknownColumn(String, &'static str),
    /// Go `errors.New("We don't support <=> all or <=> any now")`.
    NullEqQuantifierUnsupported,
    /// Go's `expression.NewFunction` error, kept verbatim.
    FunctionBuild(String),
    /// Go's `aggregation.NewAggFuncDesc` error.
    AggFuncBuild(String),
    /// The outer plan produced no schema, which Go reaches by dereferencing
    /// nil; see [`LogicalPlan::schema`].
    MissingSchema,
}

impl std::fmt::Display for RewriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OperandColumns(n) => write!(f, "Operand should contain {n} column(s)"),
            Self::Ambiguous(name, clause) => {
                write!(f, "Column '{name}' in {clause} is ambiguous")
            }
            Self::UnknownColumn(name, clause) => {
                write!(f, "Unknown column '{name}' in '{clause}'")
            }
            Self::NullEqQuantifierUnsupported => {
                write!(f, "We don't support <=> all or <=> any now")
            }
            Self::FunctionBuild(reason) | Self::AggFuncBuild(reason) => write!(f, "{reason}"),
            Self::MissingSchema => write!(f, "plan produces no schema"),
        }
    }
}

impl std::error::Error for RewriteError {}

// ***** coreusage/correlated_misc.go *****

/// Go `coreusage.ExtractCorrelatedCols4LogicalPlan(p)`
/// (`correlated_misc.go:38`): every node's own correlated columns, whole
/// subtree.
///
/// Go recurses; this walks an explicit stack, per the tree's depth-safety rule.
#[must_use]
pub fn extract_correlated_cols_4_logical_plan(plan: &LogicalPlan) -> Vec<CorrelatedColumn> {
    let mut result = Vec::new();
    let mut stack = vec![plan];
    while let Some(node) = stack.pop() {
        result.extend(node.extract_correlated_cols());
        stack.extend(node.children().iter());
    }
    result
}

/// Go `coreusage.ExtractCorColumnsBySchema(corCols, schema, true)`
/// (`correlated_misc.go:64`): the correlated columns `schema` RESOLVES,
/// deduplicated by schema position and index-resolved.
///
/// Go also aliases the `*types.Datum` binding cell; see the module header.
#[must_use]
pub fn extract_cor_columns_by_schema(
    cor_cols: &[CorrelatedColumn],
    schema: &Schema,
) -> Vec<CorrelatedColumn> {
    let mut slots: Vec<Option<CorrelatedColumn>> = vec![None; schema.len()];
    for cor_col in cor_cols {
        let idx = schema.column_index(&cor_col.column);
        if idx < 0 {
            continue;
        }
        let idx = usize::try_from(idx).expect("column_index is non-negative here");
        if slots[idx].is_none() {
            let mut column = schema.columns[idx].clone();
            // Go's `resolveIndex` pass: `corCol.Index = schema.ColumnIndex(...)`.
            column.index = i64::try_from(idx).expect("schema length fits in i64");
            slots[idx] = Some(CorrelatedColumn { column, data: None });
        }
    }
    slots.into_iter().flatten().collect()
}

/// Go `ExtractCorColumnsBySchema4LogicalPlan(np, outerSchema)`
/// (`correlated_misc.go:57`): which of the inner plan's correlated columns the
/// IMMEDIATE outer plan supplies. A non-empty result is what makes a subquery
/// correlated at this level.
#[must_use]
pub fn extract_cor_columns_by_schema_4_logical_plan(
    plan: &LogicalPlan,
    outer_schema: &Schema,
) -> Vec<CorrelatedColumn> {
    extract_cor_columns_by_schema(&extract_correlated_cols_4_logical_plan(plan), outer_schema)
}

// ***** small plan predicates *****

/// Go `hasCTEConsumerInSubPlan(p)` (`expression_rewriter.go:1645`).
///
/// Go recurses; this walks an explicit stack.
#[must_use]
pub fn has_cte_consumer_in_sub_plan(plan: &LogicalPlan) -> bool {
    let mut stack = vec![plan];
    while let Some(node) = stack.pop() {
        if matches!(node, LogicalPlan::CTE(_)) {
            return true;
        }
        stack.extend(node.children().iter());
    }
    false
}

/// Go `hasLimit(plan)` (`expression_rewriter.go:3354`), which gates the
/// `LIMIT 1` an un-decorrelated `EXISTS` gets.
///
/// Go recurses; this walks an explicit stack.
#[must_use]
pub fn has_limit(plan: &LogicalPlan) -> bool {
    let mut stack = vec![plan];
    while let Some(node) = stack.pop() {
        if matches!(node, LogicalPlan::Limit(_)) {
            return true;
        }
        stack.extend(node.children().iter());
    }
    false
}

/// Go `expression.ExprNotNull(ctx, expr)` (`expression/expression.go:335`):
/// a constant is not null when its value is not, everything else answers from
/// the `NotNull` type flag.
///
/// Go's own comment notes the `ScalarFunction` answer is only as good as the
/// flag maintenance behind it; that is reproduced, not improved on.
#[must_use]
pub fn expr_not_null(expr: &Expression) -> bool {
    if let Expression::Constant(constant) = expr {
        return !constant.value.is_null();
    }
    expr.static_type()
        .is_some_and(|ty| ty.has_flag(FieldTypeFlags::NOT_NULL))
}

/// Go `expression.Expression.Decorrelate(schema)`: a correlated column the
/// outer schema supplies becomes a plain column, so a semi join's `ON` clause
/// references the outer child rather than reaching past it.
#[must_use]
pub fn decorrelate_expr(expr: &Expression, schema: &Schema) -> Expression {
    match expr {
        Expression::CorrelatedColumn(cor) => cor.decorrelate(schema),
        Expression::ScalarFunction(func) => {
            let mut rebuilt = func.clone();
            rebuilt.args = func
                .args
                .iter()
                .map(|arg| decorrelate_expr(arg, schema))
                .collect();
            Expression::ScalarFunction(rebuilt)
        }
        other => other.clone(),
    }
}

/// Go `types.NewFieldType(mysql.TypeTiny)`, the result type every condition
/// this module builds is asked for.
fn tiny() -> FieldType {
    FieldType::new(FieldTypeCode::Tiny)
}

/// Sets a schema-producing operator's own schema and names.
///
/// [`LogicalPlan::set_output_names`] forwards to `children[0]` (Go's
/// `BaseLogicalPlan` default); a join/apply/aggregation/projection is a
/// `logicalSchemaProducer` and OVERRIDES that, so it writes its own base.
fn set_own_schema(plan: &mut LogicalPlan, schema: Schema, names: Vec<FieldName>) {
    let base = plan.base_mut();
    base.base.set_schema(Some(schema));
    base.base.set_output_names(names);
}

/// Go `p.Children()[0]`, taking ownership.
fn take_first_child(plan: &mut LogicalPlan) -> Option<LogicalPlan> {
    let children = plan.base_mut().children_mut();
    if children.is_empty() {
        return None;
    }
    Some(children.remove(0))
}

// ***** the rewriter *****

/// Go `expressionRewriter` (`expression_rewriter.go:352`), narrowed to the
/// plan-carrying half — see the module header.
pub struct ExpressionRewriter<'a, C: Columns> {
    /// Go `ctxStack`.
    pub ctx_stack: Vec<Expression>,
    /// Go `ctxNameStk`, index-parallel to `ctx_stack`.
    pub ctx_name_stk: Vec<FieldName>,
    /// Go `schema`: the CURRENT block's schema, which `toColumn` resolves
    /// against before it reaches for an outer one.
    pub schema: Option<Schema>,
    /// Go `names`, index-parallel to `schema`'s columns.
    pub names: Vec<FieldName>,
    /// Go `asScalar`: the caller needs a value, not just a filter. Go's own
    /// note applies — this CHANGES during a rewrite.
    pub as_scalar: bool,
    /// Go `planCtx`.
    pub plan_ctx: ExprRewriterPlanCtx,
    /// Go's `StmtCtx.SetHintWarning` sink.
    pub hint_warnings: Vec<String>,
    /// Go's ambient session/builder state.
    pub env: RewriterEnv<'a, C>,
}

/// What [`ExpressionRewriter::handle_scalar_subquery`] and
/// [`ExpressionRewriter::handle_exist_subquery`] decided.
#[derive(Debug)]
// Go returns the outer plan and the inner plan as two independent values; the
// size gap between one plan and two is inherent to that and boxing either arm
// would only move an owned tree behind a pointer the callers must then unwrap.
#[allow(clippy::large_enum_variant)]
pub enum ScalarSubqueryOutcome {
    /// The subquery became an apply above the outer plan, and the expression
    /// that reads its result was pushed on the ctx stack.
    Applied(LogicalPlan),
    /// Go's second path: the subquery is uncorrelated and preprocessing is on,
    /// so Go OPTIMIZES and RUNS it here and folds the row to a constant. That
    /// needs `DoOptimize`/`EvalSubqueryFirstRow`, which are boundaries; the
    /// outer plan is returned untouched together with the inner plan to
    /// evaluate.
    EvaluateSeparately {
        /// The untouched outer plan.
        outer: LogicalPlan,
        /// The inner plan Go would optimize and run.
        inner: LogicalPlan,
    },
}

/// The already-rewritten pieces of `ast.CompareSubqueryExpr` that
/// [`ExpressionRewriter::handle_compare_subquery`] needs; the AST walk that
/// produces them is a boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompareOp {
    /// Go `opcode.EQ`.
    Eq,
    /// Go `opcode.NE`.
    Ne,
    /// Go `opcode.NullEQ`.
    NullEq,
    /// Go `opcode.LT`.
    Lt,
    /// Go `opcode.LE`.
    Le,
    /// Go `opcode.GT`.
    Gt,
    /// Go `opcode.GE`.
    Ge,
}

impl CompareOp {
    /// Go `opcode.Op.String()`, which is the function name
    /// `handleOtherComparableSubq` builds the comparison with.
    #[must_use]
    pub const fn func_name(self) -> &'static str {
        match self {
            Self::Eq => "eq",
            Self::Ne => "ne",
            Self::NullEq => "nulleq",
            Self::Lt => "lt",
            Self::Le => "le",
            Self::Gt => "gt",
            Self::Ge => "ge",
        }
    }
}

impl<'a, C: Columns> ExpressionRewriter<'a, C> {
    /// A rewriter over `env`, with an empty stack.
    #[must_use]
    pub fn new(env: RewriterEnv<'a, C>) -> Self {
        Self {
            ctx_stack: Vec::new(),
            ctx_name_stk: Vec::new(),
            schema: None,
            names: Vec::new(),
            as_scalar: false,
            plan_ctx: ExprRewriterPlanCtx::default(),
            hint_warnings: Vec::new(),
            env,
        }
    }

    /// Go `ctxStackLen()` (`expression_rewriter.go:386`).
    #[must_use]
    pub fn ctx_stack_len(&self) -> usize {
        self.ctx_stack.len()
    }

    /// Go `ctxStackPop(num)` (`expression_rewriter.go:390`).
    pub fn ctx_stack_pop(&mut self, num: usize) {
        let keep = self.ctx_stack.len().saturating_sub(num);
        self.ctx_stack.truncate(keep);
        self.ctx_name_stk.truncate(keep);
    }

    /// Go `ctxStackAppend(col, name)` (`expression_rewriter.go:396`).
    pub fn ctx_stack_append(&mut self, expr: Expression, name: FieldName) {
        self.ctx_stack.push(expr);
        self.ctx_name_stk.push(name);
    }

    /// Go `er.clause()` (`expression_rewriter.go:3061`).
    #[must_use]
    pub const fn clause(&self) -> ClauseCode {
        self.plan_ctx.cur_clause
    }

    fn new_function(
        &self,
        name: &str,
        ret_type: FieldType,
        args: Vec<Expression>,
    ) -> Result<Expression, RewriteError> {
        self.env
            .builder()
            .new_function(name, Some(ret_type), args)
            .map_err(|err| RewriteError::FunctionBuild(err.to_string()))
    }

    fn new_agg_func(
        &self,
        name: &str,
        args: Vec<Expression>,
        has_distinct: bool,
    ) -> Result<AggFuncDesc, RewriteError> {
        AggFuncDesc::new(self.env.ctx, name, args, has_distinct)
            .map_err(|err| RewriteError::AggFuncBuild(format!("{err:?}")))
    }

    /// Go `constructBinaryOpFunction(l, r, op)`
    /// (`expression_rewriter.go:413`), which turns a ROW comparison into the
    /// scalar chain that implements it.
    ///
    /// 1. two scalars: `l op r`;
    /// 2. mismatched widths: `ErrOperandColumns`;
    /// 3. `=`/`!=`/`<=>`: the conjunction of the per-position comparisons;
    /// 4. anything ordered: the lexicographic DNF, whose prefix comparisons
    ///    are the STRICT form even when the operator is `>=`/`<=` — the only
    ///    difference `>=` makes is in the last position.
    ///
    /// # Errors
    ///
    /// [`RewriteError::OperandColumns`] on a width mismatch, or the builder's
    /// error.
    pub fn construct_binary_op_function(
        &self,
        l: &Expression,
        r: &Expression,
        op: &str,
    ) -> Result<Expression, RewriteError> {
        let (l_len, r_len) = (get_row_len(l), get_row_len(r));
        if l_len == 1 && r_len == 1 {
            return self.new_function(op, tiny(), vec![l.clone(), r.clone()]);
        }
        if r_len != l_len {
            return Err(RewriteError::OperandColumns(l_len));
        }
        let arg = |expr: &Expression, i: usize| -> Expression {
            get_func_arg(expr, i)
                .cloned()
                .unwrap_or_else(|| expr.clone())
        };
        match op {
            "eq" | "ne" | "nulleq" => {
                let mut funcs = Vec::with_capacity(l_len);
                for i in 0..l_len {
                    funcs.push(self.construct_binary_op_function(&arg(l, i), &arg(r, i), op)?);
                }
                Ok(compose_cnf_condition(funcs).expect("l_len >= 2 here"))
            }
            _ => {
                // Go's `larger`: the strict operator the prefix positions use.
                let strict = match op {
                    "ge" => "gt",
                    "le" => "lt",
                    other => other,
                };
                let mut dnf = Vec::with_capacity(l_len);
                for i in 0..l_len {
                    // Positions before `i` are equal; position `i` decides.
                    let decider = if i + 1 == l_len { op } else { strict };
                    let mut conj = Vec::with_capacity(i + 1);
                    for j in 0..i {
                        conj.push(self.new_function("eq", tiny(), vec![arg(l, j), arg(r, j)])?);
                    }
                    conj.push(self.new_function(decider, tiny(), vec![arg(l, i), arg(r, i)])?);
                    dnf.push(compose_cnf_condition(conj).expect("conj is non-empty"));
                }
                Ok(compose_dnf_condition(dnf).expect("dnf is non-empty"))
            }
        }
    }

    // ***** logical_plan_builder.go: the plan constructors *****

    /// Go `PlanBuilder.buildMaxOneRow(p)`
    /// (`logical_plan_builder.go:5754`). Go's comment is kept: the query block
    /// is the CHILD's, not the builder's.
    #[must_use]
    pub fn build_max_one_row(&self, child: LogicalPlan) -> LogicalPlan {
        let offset = child.base().base.query_block_offset();
        let base = BaseLogicalPlan::new(self.env.plan_ids, LogicalMaxOneRow::TYPE, offset);
        let mut plan = LogicalPlan::MaxOneRow(LogicalMaxOneRow::new(base));
        plan.set_children(vec![child]);
        plan
    }

    /// Go `PlanBuilder.buildDistinct(child, length)`
    /// (`logical_plan_builder.go:1966`): the `first_row` aggregation that
    /// de-duplicates the inner side of an `IN`-to-join rewrite.
    ///
    /// Go's last loop is load-bearing and reproduced: `first_row`'s result type
    /// is NOT always its argument's, so the output schema's types are reset
    /// from the descriptors.
    ///
    /// # Errors
    ///
    /// The aggregate descriptor's error.
    pub fn build_distinct(
        &self,
        child: LogicalPlan,
        length: usize,
    ) -> Result<LogicalPlan, RewriteError> {
        let child_schema = child.schema().ok_or(RewriteError::MissingSchema)?.clone();
        let child_names = child.output_names().to_vec();
        let group_by_items = child_schema.columns[..length.min(child_schema.len())]
            .iter()
            .cloned()
            .map(Expression::Column)
            .collect();
        let mut agg_funcs = Vec::with_capacity(child_schema.len());
        for col in &child_schema.columns {
            agg_funcs.push(self.new_agg_func(
                "firstrow",
                vec![Expression::Column(col.clone())],
                false,
            )?);
        }
        let offset = child.base().base.query_block_offset();
        let base = BaseLogicalPlan::new(self.env.plan_ids, LogicalAggregation::TYPE, offset);
        let mut agg = LogicalAggregation::new(base, agg_funcs, group_by_items);
        agg.prefer_agg_type = self.env.hints.prefer_agg_type;
        agg.prefer_agg_to_cop = self.env.hints.prefer_agg_to_cop;
        let mut schema = child_schema;
        for (i, col) in schema.columns.iter_mut().enumerate() {
            col.ret_type = Some(agg.agg_funcs[i].ret_type().clone());
        }
        let mut plan = LogicalPlan::Aggregation(agg);
        plan.set_children(vec![child]);
        set_own_schema(&mut plan, schema, child_names);
        Ok(plan)
    }

    /// Go `PlanBuilder.buildApplyWithJoinType(outer, inner, tp, mark)`
    /// (`logical_plan_builder.go:5697`): the apply a SCALAR subquery becomes.
    ///
    /// Go's `tp` can only be `InnerJoin` or `LeftOuterJoin`; the `LeftOuterJoin`
    /// branch RESETS the not-null flag on every inner column, because an outer
    /// row with no inner match reads them as NULL.
    ///
    /// `// boundary:` Go `setIsInApplyForCTE` — see the module header.
    ///
    /// # Errors
    ///
    /// [`RewriteError::MissingSchema`] when either side produces no schema.
    pub fn build_apply_with_join_type(
        &self,
        outer: LogicalPlan,
        inner: LogicalPlan,
        tp: LogicalJoinType,
        mark_no_decorrelate: bool,
    ) -> Result<LogicalPlan, RewriteError> {
        let outer_schema = outer.schema().ok_or(RewriteError::MissingSchema)?;
        let inner_schema = inner.schema().ok_or(RewriteError::MissingSchema)?;
        let outer_len = outer_schema.len();
        let mut schema = merge_schema(Some(outer_schema), Some(inner_schema))
            .ok_or(RewriteError::MissingSchema)?;
        if tp == LogicalJoinType::LeftOuter {
            // Go `util.ResetNotNullFlag(ap.Schema(), outerLen, ap.Schema().Len())`.
            for col in &mut schema.columns[outer_len..] {
                if let Some(ty) = col.ret_type.as_mut() {
                    let mut cloned = ty.clone();
                    cloned.del_flags(FieldTypeFlags::NOT_NULL);
                    *ty = cloned;
                }
            }
        }
        let mut names = outer.output_names().to_vec();
        names.resize(schema.len(), FieldName::default());
        let base = self.env.base(LogicalApply::TYPE);
        let mut apply = LogicalApply::new(base, tp);
        apply.no_decorrelate = mark_no_decorrelate;
        let mut plan = LogicalPlan::Apply(apply);
        plan.set_children(vec![outer, inner]);
        set_own_schema(&mut plan, schema, names);
        Ok(plan)
    }

    /// Go `PlanBuilder.buildSemiJoin(outer, inner, onCondition, asScalar, not,
    /// forceRewrite)` (`logical_plan_builder.go:5761`).
    ///
    /// The join type is the whole point, and it is a two-by-two:
    ///
    /// | | `not = false` | `not = true` |
    /// |---|---|---|
    /// | `as_scalar = false` | `SemiJoin` | `AntiSemiJoin` |
    /// | `as_scalar = true` | `LeftOuterSemiJoin` | `AntiLeftOuterSemiJoin` |
    ///
    /// The `as_scalar` row APPENDS a `Tiny` column carrying the match answer,
    /// because the parent expression reads a value, not a filtered row.
    ///
    /// Every `ON` condition is decorrelated against the outer schema first, so
    /// a reference the outer side supplies becomes a plain column.
    ///
    /// # Errors
    ///
    /// [`RewriteError::MissingSchema`] when either side produces no schema.
    pub fn build_semi_join(
        &mut self,
        outer: LogicalPlan,
        inner: LogicalPlan,
        on_condition: &[Expression],
        as_scalar: bool,
        not: bool,
        force_rewrite: bool,
    ) -> Result<LogicalPlan, RewriteError> {
        let outer_schema = outer.schema().ok_or(RewriteError::MissingSchema)?.clone();
        let inner_schema = inner.schema().ok_or(RewriteError::MissingSchema)?.clone();
        let on_condition: Vec<Expression> = on_condition
            .iter()
            .map(|expr| decorrelate_expr(expr, &outer_schema))
            .collect();

        let join_type = match (as_scalar, not) {
            (false, false) => LogicalJoinType::Semi,
            (false, true) => LogicalJoinType::AntiSemi,
            (true, false) => LogicalJoinType::LeftOuterSemi,
            (true, true) => LogicalJoinType::AntiLeftOuterSemi,
        };
        let base = self.env.base(LogicalJoin::TYPE);
        let mut join = LogicalJoin::new(base, join_type);
        {
            let builder = self.env.builder();
            let opts = SubstituteOptions::new(&builder);
            join.attach_on_conds(&on_condition, &outer_schema, &inner_schema, &opts);
        }
        if force_rewrite || self.env.flags.enable_semi_join_rewrite {
            join.prefer_join_type |= PREFER_REWRITE_SEMI_JOIN;
        }

        let mut names = outer.output_names().to_vec();
        let mut schema = outer_schema;
        if as_scalar {
            schema.append([self.env.new_plan_column(tiny())]);
            names.push(FieldName::default());
        }
        let mut plan = LogicalPlan::Join(join);
        plan.set_children(vec![outer, inner]);
        set_own_schema(&mut plan, schema, names);
        Ok(plan)
    }

    /// Go `PlanBuilder.buildSemiApply(outer, inner, condition, asScalar, not,
    /// considerRewrite, markNoDecorrelate)` (`logical_plan_builder.go:5718`):
    /// [`Self::build_semi_join`] re-typed as an apply.
    ///
    /// Go builds the join and then RE-WRAPS it (`&LogicalApply{LogicalJoin:
    /// *join}` plus `SetTP(TypeApply)`), which is exactly what happens here —
    /// the join's schema, names, conditions and children all carry over.
    ///
    /// `// boundary:` Go `setIsInApplyForCTE` — see the module header.
    ///
    /// # Errors
    ///
    /// See [`Self::build_semi_join`].
    #[allow(clippy::too_many_arguments)]
    pub fn build_semi_apply(
        &mut self,
        outer: LogicalPlan,
        inner: LogicalPlan,
        condition: &[Expression],
        as_scalar: bool,
        not: bool,
        consider_rewrite: bool,
        mark_no_decorrelate: bool,
    ) -> Result<LogicalPlan, RewriteError> {
        let join =
            self.build_semi_join(outer, inner, condition, as_scalar, not, consider_rewrite)?;
        let LogicalPlan::Join(mut join) = join else {
            unreachable!("build_semi_join returns a join");
        };
        join.base.base.set_tp(LogicalApply::TYPE);
        Ok(LogicalPlan::Apply(LogicalApply {
            join,
            cor_cols: Vec::new(),
            no_decorrelate: mark_no_decorrelate,
            is_lateral: false,
        }))
    }

    // ***** expression_rewriter.go: the subquery handlers *****

    /// Go `buildSemiApplyFromEqualSubq(np, planCtx, l, r, not,
    /// markNoDecorrelate)` (`expression_rewriter.go:776`): the shape
    /// `a = ANY (subq)` and `a != ALL (subq)` share with `a [NOT] IN (subq)`.
    ///
    /// The `InOperand` marking is the load-bearing part and Go's own comment
    /// says why: for an anti/left-outer-semi join the `=` is NOT an ordinary
    /// column equality, because a NULL on either side must stay NULL-aware.
    /// Marking is SKIPPED when both sides are provably not null, which lets
    /// later rules treat the condition as a plain equality.
    ///
    /// # Errors
    ///
    /// The builder's error, or [`Self::build_semi_apply`]'s.
    pub fn build_semi_apply_from_equal_subq(
        &mut self,
        outer: LogicalPlan,
        np: LogicalPlan,
        l: &Expression,
        r: &Expression,
        not: bool,
        mark_no_decorrelate: bool,
    ) -> Result<LogicalPlan, RewriteError> {
        let mut l = l.clone();
        let mut r = r.clone();
        if self.as_scalar || not {
            if get_row_len(&r) == 1 {
                if !expr_not_null(&l) || !expr_not_null(&r) {
                    r = mark_in_operand(r);
                    l = set_expr_column_in_operand(l);
                }
            } else if let Expression::ScalarFunction(row_func) = r.clone() {
                let mut args = Vec::with_capacity(row_func.args.len());
                let mut modified = false;
                for (i, r_arg) in row_func.args.iter().enumerate() {
                    let l_arg = get_func_arg(&l, i);
                    let l_not_null = l_arg.is_some_and(expr_not_null);
                    if !l_not_null || !expr_not_null(r_arg) {
                        args.push(mark_in_operand(r_arg.clone()));
                        modified = true;
                    } else {
                        args.push(r_arg.clone());
                    }
                }
                if modified {
                    let ret_type = args[0].static_type().cloned().unwrap_or_else(tiny);
                    r = self.new_function("row", ret_type, args)?;
                    l = set_expr_column_in_operand(l);
                }
            }
        }
        let condition = self.construct_binary_op_function(&l, &r, "eq")?;
        let as_scalar = self.as_scalar;
        self.build_semi_apply(
            outer,
            np,
            &[condition],
            as_scalar,
            not,
            false,
            mark_no_decorrelate,
        )
    }

    /// Go `handleCompareSubquery(ctx, planCtx, v)`
    /// (`expression_rewriter.go:822`), from the point where `v.L` has been
    /// rewritten (it is on the ctx stack as `lexpr`) and `v.R`'s subquery has
    /// been planned into `np`.
    ///
    /// Which plan a quantified comparison becomes:
    ///
    /// * `= ANY` is `IN`, and `!= ALL` is `NOT IN`: both go to
    ///   [`Self::build_semi_apply_from_equal_subq`];
    /// * `= ALL` and `!= ANY` each need to know whether the inner side has
    ///   more than one distinct value, so they build their own aggregation —
    ///   [`Self::handle_eq_all`] and [`Self::handle_ne_any`];
    /// * `<=>` is refused, as in Go;
    /// * every ordered comparison becomes a comparison against `MIN` or `MAX`
    ///   ([`Self::handle_other_comparable_subq`]), and which one it is
    ///   depends on BOTH the operator and the quantifier: `< ALL` and `> ANY`
    ///   need the extreme that makes the predicate hardest to satisfy.
    ///
    /// Only `= ANY` and `!= ALL` accept a ROW on the left; every other form
    /// requires one column on each side.
    ///
    /// `// boundary:` Go `expression.CheckAndDeriveCollationFromExprs(er.sctx,
    /// op, ETInt, lexpr, rexpr)` — the collation-compatibility refusal between
    /// the two sides. `pkg/expression/collation.go` is not reachable from this
    /// crate's dependency set; a caller that has it should apply it before
    /// calling.
    ///
    /// # Errors
    ///
    /// [`RewriteError::OperandColumns`] on an arity mismatch,
    /// [`RewriteError::NullEqQuantifierUnsupported`] for `<=>`, or a builder
    /// error.
    pub fn handle_compare_subquery(
        &mut self,
        outer: LogicalPlan,
        lexpr: &Expression,
        np: LogicalPlan,
        op: CompareOp,
        all: bool,
        hint_flags: u64,
    ) -> Result<LogicalPlan, RewriteError> {
        let outer_schema = outer.schema().ok_or(RewriteError::MissingSchema)?.clone();
        let np_schema = np.schema().ok_or(RewriteError::MissingSchema)?.clone();
        let cor_cols = extract_cor_columns_by_schema_4_logical_plan(&np, &outer_schema);
        let no_decorrelate = is_no_decorrelate(
            self.plan_ctx.cur_clause,
            self.env.flags,
            &cor_cols,
            hint_flags,
            SubQueryCtx::Compare,
            &mut self.hint_warnings,
        );

        // Go: only `(a,b,c) = any (...)` and `(a,b,c) != all (...)` may be rows.
        let can_multi_col = (!all && op == CompareOp::Eq) || (all && op == CompareOp::Ne);
        let l_len = get_row_len(lexpr);
        if !can_multi_col && (l_len != 1 || np_schema.len() != 1) {
            return Err(RewriteError::OperandColumns(1));
        }
        if l_len != np_schema.len() {
            return Err(RewriteError::OperandColumns(l_len));
        }
        let rexpr = self.row_of_schema(&np_schema)?;

        let mut plan = match op {
            CompareOp::Eq if all => self.handle_eq_all(outer, lexpr, &rexpr, np, no_decorrelate)?,
            CompareOp::Eq => {
                // `a = any(subq)` is `a in (subq)`.
                self.as_scalar = true;
                self.build_semi_apply_from_equal_subq(
                    outer,
                    np,
                    lexpr,
                    &rexpr,
                    false,
                    no_decorrelate,
                )?
            }
            CompareOp::Ne if all => {
                // `a != all(subq)` is `a not in (subq)`.
                self.as_scalar = true;
                self.build_semi_apply_from_equal_subq(
                    outer,
                    np,
                    lexpr,
                    &rexpr,
                    true,
                    no_decorrelate,
                )?
            }
            CompareOp::Ne => self.handle_ne_any(outer, lexpr, &rexpr, np, no_decorrelate)?,
            CompareOp::NullEq => return Err(RewriteError::NullEqQuantifierUnsupported),
            _ => {
                // Go: `< all` and `> any` need MIN; `> all` and `< any` need MAX.
                let use_min = (matches!(op, CompareOp::Lt | CompareOp::Le) && all)
                    || (matches!(op, CompareOp::Gt | CompareOp::Ge) && !all);
                self.handle_other_comparable_subq(
                    outer,
                    lexpr,
                    &rexpr,
                    np,
                    use_min,
                    op.func_name(),
                    all,
                    no_decorrelate,
                )?
            }
        };
        self.ctx_stack_pop(1);
        if self.as_scalar {
            self.push_last_schema_column(&mut plan)?;
        }
        Ok(plan)
    }

    /// Go `handleOtherComparableSubq(planCtx, lexpr, rexpr, np, useMin,
    /// cmpFunc, all, markNoDecorrelate)` (`expression_rewriter.go:927`):
    /// `t.id < any (select s.id from s)` becomes `t.id < max(s.id)`.
    ///
    /// # Errors
    ///
    /// The aggregate descriptor's or the builder's error.
    #[allow(clippy::too_many_arguments)]
    pub fn handle_other_comparable_subq(
        &mut self,
        outer: LogicalPlan,
        lexpr: &Expression,
        rexpr: &Expression,
        np: LogicalPlan,
        use_min: bool,
        cmp_func: &str,
        all: bool,
        mark_no_decorrelate: bool,
    ) -> Result<LogicalPlan, RewriteError> {
        let func_name = if use_min { "min" } else { "max" };
        let func_max_or_min = self.new_agg_func(func_name, vec![rexpr.clone()], false)?;
        let col_max_or_min = self.env.new_plan_column(func_max_or_min.ret_type().clone());

        let base = self.env.base(LogicalAggregation::TYPE);
        let mut agg = LogicalAggregation::new(base, vec![func_max_or_min], Vec::new());
        agg.prefer_agg_type = self.env.hints.prefer_agg_type;
        agg.prefer_agg_to_cop = self.env.hints.prefer_agg_to_cop;
        let cond = self.new_function(
            cmp_func,
            tiny(),
            vec![lexpr.clone(), Expression::Column(col_max_or_min.clone())],
        )?;
        let mut plan4_agg = LogicalPlan::Aggregation(agg);
        plan4_agg.set_children(vec![np]);
        set_own_schema(
            &mut plan4_agg,
            Schema::new(vec![col_max_or_min]),
            vec![FieldName::default()],
        );
        self.build_quantifier_plan(
            outer,
            plan4_agg,
            cond,
            lexpr,
            rexpr,
            all,
            mark_no_decorrelate,
        )
    }

    /// Go `buildQuantifierPlan(planCtx, plan4Agg, cond, lexpr, rexpr, all,
    /// markNoDecorrelate)` (`expression_rewriter.go:964`): the three-valued
    /// logic a quantified comparison needs, spelled as extra aggregates.
    ///
    /// Two extra aggregates go on `plan4_agg`: `sum(inner IS NULL)`, which
    /// answers "did the inner side contain a NULL", and `count(1)`, which
    /// answers "was the inner side empty". They are what make the SQL answer
    /// correct rather than merely plausible:
    ///
    /// * `ALL` over an inner side containing NULL is NULL, not true, so the
    ///   condition is ANDed with `if(hasNull, NULL, 1)`; an EMPTY inner side
    ///   makes `ALL` true unconditionally, and a NULL outer key against a
    ///   non-empty inner side is NULL.
    /// * `ANY` is the dual: ORed with `if(hasNull, NULL, 0)`, false on an
    ///   empty inner side, NULL for a NULL outer key.
    ///
    /// When the caller does not need a value (`as_scalar` false) the condition
    /// rides the semi apply's `ON` clause. When it does, the apply is an INNER
    /// one and a projection appends the condition as an extra output column.
    ///
    /// # Errors
    ///
    /// The aggregate descriptor's or the builder's error.
    #[allow(clippy::too_many_arguments)]
    pub fn build_quantifier_plan(
        &mut self,
        outer: LogicalPlan,
        mut plan4_agg: LogicalPlan,
        cond: Expression,
        lexpr: &Expression,
        rexpr: &Expression,
        all: bool,
        mark_no_decorrelate: bool,
    ) -> Result<LogicalPlan, RewriteError> {
        let inner_is_null = self.new_function("isnull", tiny(), vec![rexpr.clone()])?;
        let outer_is_null = self.new_function("isnull", tiny(), vec![lexpr.clone()])?;

        let func_sum = self.new_agg_func("sum", vec![inner_is_null], false)?;
        let col_sum = self.env.new_plan_column(func_sum.ret_type().clone());
        let func_count = self.new_agg_func("count", vec![one()], false)?;
        let col_count = self.env.new_plan_column(func_count.ret_type().clone());
        {
            let LogicalPlan::Aggregation(agg) = &mut plan4_agg else {
                unreachable!("build_quantifier_plan is only called on an aggregation");
            };
            agg.agg_funcs.push(func_sum);
            agg.agg_funcs.push(func_count);
        }
        {
            let base = plan4_agg.base_mut();
            let mut schema = base.base.schema().cloned().unwrap_or_default();
            schema.append([col_sum.clone(), col_count.clone()]);
            base.base.set_schema(Some(schema));
        }

        let inner_has_null =
            self.new_function("ne", tiny(), vec![Expression::Column(col_sum), zero()])?;
        let cond = if all {
            // Go: `t.id < all(s)` is `t.id < min(s.id) and if(sum(s.id is null) != 0, null, true)`.
            let inner_null_checker =
                self.new_function("if", tiny(), vec![inner_has_null, null(), one()])?;
            let cond = compose_cnf_condition(vec![cond, inner_null_checker])
                .expect("two conditions compose");
            let empty_checker =
                self.new_function("eq", tiny(), vec![Expression::Column(col_count), zero()])?;
            let outer_null_checker =
                self.new_function("if", tiny(), vec![outer_is_null, null(), zero()])?;
            compose_dnf_condition(vec![cond, empty_checker, outer_null_checker])
                .expect("three conditions compose")
        } else {
            // Go: `t.id < any(s)` is `t.id < max(s.id) or if(sum(s.id is null) != 0, null, false)`.
            let inner_null_checker =
                self.new_function("if", tiny(), vec![inner_has_null, null(), zero()])?;
            let cond = compose_dnf_condition(vec![cond, inner_null_checker])
                .expect("two conditions compose");
            let empty_checker =
                self.new_function("ne", tiny(), vec![Expression::Column(col_count), zero()])?;
            let outer_null_checker =
                self.new_function("if", tiny(), vec![outer_is_null, null(), one()])?;
            compose_cnf_condition(vec![cond, empty_checker, outer_null_checker])
                .expect("three conditions compose")
        };

        if !self.as_scalar {
            // Go: a semi apply with no aux column cannot tell false from null,
            // and does not need to, so the condition is just a join predicate.
            return self.build_semi_apply(
                outer,
                plan4_agg,
                &[cond],
                false,
                false,
                false,
                mark_no_decorrelate,
            );
        }
        let outer_schema_len = outer.schema().ok_or(RewriteError::MissingSchema)?.len();
        let join = self.build_apply_with_join_type(
            outer,
            plan4_agg,
            LogicalJoinType::Inner,
            mark_no_decorrelate,
        )?;
        let join_schema = join.schema().ok_or(RewriteError::MissingSchema)?.clone();
        let mut proj_schema = Schema::new(join_schema.columns[..outer_schema_len].to_vec());
        let mut names = join.output_names()[..outer_schema_len].to_vec();
        let mut exprs: Vec<Expression> = proj_schema
            .columns
            .iter()
            .cloned()
            .map(Expression::Column)
            .collect();
        let cond_type = cond.static_type().cloned().unwrap_or_else(tiny);
        exprs.push(cond);
        proj_schema.append([self.env.new_plan_column(cond_type)]);
        names.push(FieldName::default());

        let base = self.env.base(LogicalProjection::TYPE);
        let mut proj = LogicalPlan::Projection(LogicalProjection::new(base, exprs));
        proj.set_children(vec![join]);
        set_own_schema(&mut proj, proj_schema, names);
        Ok(proj)
    }

    /// Go `handleNEAny(planCtx, lexpr, rexpr, np, markNoDecorrelate)`
    /// (`expression_rewriter.go:1050`): `t.id != any (select s.id from s)`
    /// becomes `t.id != max(s.id) or count(distinct s.id) > 1`.
    ///
    /// Go's reasoning, kept: if the inner side holds two DIFFERENT values then
    /// one of them necessarily differs from `t.id`, so the distinct count
    /// settles the answer without comparing every row. `MAX` rather than an
    /// arbitrary row because `MAX` skips NULLs.
    ///
    /// # Errors
    ///
    /// The aggregate descriptor's or the builder's error.
    pub fn handle_ne_any(
        &mut self,
        outer: LogicalPlan,
        lexpr: &Expression,
        rexpr: &Expression,
        np: LogicalPlan,
        mark_no_decorrelate: bool,
    ) -> Result<LogicalPlan, RewriteError> {
        let (plan4_agg, max_col, count_col) = self.build_max_and_distinct_count_agg(rexpr, np)?;
        let gt_func =
            self.new_function("gt", tiny(), vec![Expression::Column(count_col), one()])?;
        let ne_cond = self.new_function(
            "ne",
            tiny(),
            vec![lexpr.clone(), Expression::Column(max_col)],
        )?;
        let cond = compose_dnf_condition(vec![gt_func, ne_cond]).expect("two conditions compose");
        self.build_quantifier_plan(
            outer,
            plan4_agg,
            cond,
            lexpr,
            rexpr,
            false,
            mark_no_decorrelate,
        )
    }

    /// Go `handleEQAll(planCtx, lexpr, rexpr, np, markNoDecorrelate)`
    /// (`expression_rewriter.go:1093`): `t.id = all (select s.id from s)`
    /// becomes `t.id = max(s.id) and count(distinct s.id) <= 1`.
    ///
    /// The dual of [`Self::handle_ne_any`]: `= ALL` can only hold when the
    /// inner side has at most one distinct value.
    ///
    /// # Errors
    ///
    /// The aggregate descriptor's or the builder's error.
    pub fn handle_eq_all(
        &mut self,
        outer: LogicalPlan,
        lexpr: &Expression,
        rexpr: &Expression,
        np: LogicalPlan,
        mark_no_decorrelate: bool,
    ) -> Result<LogicalPlan, RewriteError> {
        let (plan4_agg, max_col, count_col) = self.build_max_and_distinct_count_agg(rexpr, np)?;
        let le_func =
            self.new_function("le", tiny(), vec![Expression::Column(count_col), one()])?;
        let eq_cond = self.new_function(
            "eq",
            tiny(),
            vec![lexpr.clone(), Expression::Column(max_col)],
        )?;
        let cond = compose_cnf_condition(vec![le_func, eq_cond]).expect("two conditions compose");
        self.build_quantifier_plan(
            outer,
            plan4_agg,
            cond,
            lexpr,
            rexpr,
            true,
            mark_no_decorrelate,
        )
    }

    /// The `max(rexpr), count(distinct rexpr)` aggregation both `!= ANY` and
    /// `= ALL` build over the inner plan.
    fn build_max_and_distinct_count_agg(
        &mut self,
        rexpr: &Expression,
        np: LogicalPlan,
    ) -> Result<(LogicalPlan, Column, Column), RewriteError> {
        let max_func = self.new_agg_func("max", vec![rexpr.clone()], false)?;
        let count_func = self.new_agg_func("count", vec![rexpr.clone()], true)?;
        let max_col = self.env.new_plan_column(max_func.ret_type().clone());
        let count_col = self.env.new_plan_column(count_func.ret_type().clone());
        let base = self.env.base(LogicalAggregation::TYPE);
        let mut agg = LogicalAggregation::new(base, vec![max_func, count_func], Vec::new());
        agg.prefer_agg_type = self.env.hints.prefer_agg_type;
        agg.prefer_agg_to_cop = self.env.hints.prefer_agg_to_cop;
        let mut plan = LogicalPlan::Aggregation(agg);
        plan.set_children(vec![np]);
        set_own_schema(
            &mut plan,
            Schema::new(vec![max_col.clone(), count_col.clone()]),
            vec![FieldName::default(), FieldName::default()],
        );
        Ok((plan, max_col, count_col))
    }

    /// Go `handleExistSubquery(ctx, planCtx, v)`
    /// (`expression_rewriter.go:1136`), from the point where the subquery has
    /// been planned into `np`.
    ///
    /// `EXISTS` cares only whether a row comes back, which is why
    /// [`pop_exists_sub_plan`] can throw away a projection, a sort, or a whole
    /// grouping-free aggregation first, and why an UN-decorrelated `EXISTS`
    /// gets a `LIMIT 1` so the inner run can stop at the first row.
    ///
    /// The result is a semi apply with NO join condition — the inner plan's
    /// correlated columns are the only link — anti when the source said `NOT
    /// EXISTS`, and left-outer-semi when the caller needs a value.
    ///
    /// # Errors
    ///
    /// See [`Self::build_semi_apply`].
    pub fn handle_exist_subquery(
        &mut self,
        outer: LogicalPlan,
        np: LogicalPlan,
        not: bool,
        hint_flags: u64,
    ) -> Result<ScalarSubqueryOutcome, RewriteError> {
        let outer_schema = outer.schema().ok_or(RewriteError::MissingSchema)?.clone();
        let cor_cols = extract_cor_columns_by_schema_4_logical_plan(&np, &outer_schema);
        let mut no_decorrelate = is_no_decorrelate(
            self.plan_ctx.cur_clause,
            self.env.flags,
            &cor_cols,
            hint_flags,
            SubQueryCtx::Exists,
            &mut self.hint_warnings,
        );
        let semi_join_rewrite_hint = hint_flags & HINT_FLAG_SEMI_JOIN_REWRITE > 0;
        // Go: keeping a correlated subquery as an Apply preserves the index
        // lookups the correlate alternative round wants. The SEMI_JOIN_REWRITE
        // hint is excluded because it explicitly asks for decorrelation.
        if !no_decorrelate
            && !cor_cols.is_empty()
            && !semi_join_rewrite_hint
            && self.env.flags.enable_correlate_subquery
        {
            no_decorrelate = true;
        }
        let mut np = np;
        if no_decorrelate && !has_limit(&np) {
            np = self.build_limit_one(np);
        }
        np = pop_exists_sub_plan(np, self.env.plan_ids);

        let mut semi_join_rewrite = semi_join_rewrite_hint;
        if semi_join_rewrite && hint_flags & HINT_FLAG_NO_DECORRELATE > 0 {
            self.hint_warnings.push(
                "NO_DECORRELATE() and SEMI_JOIN_REWRITE() are in conflict. Both will be ineffective."
                    .to_owned(),
            );
            no_decorrelate = false;
            semi_join_rewrite = false;
        }

        if !self.must_build_apply(&np) {
            return Ok(ScalarSubqueryOutcome::EvaluateSeparately { outer, inner: np });
        }
        let as_scalar = self.as_scalar;
        let mut plan = self.build_semi_apply(
            outer,
            np,
            &[],
            as_scalar,
            not,
            semi_join_rewrite,
            no_decorrelate,
        )?;
        if as_scalar {
            self.push_last_schema_column(&mut plan)?;
        }
        Ok(ScalarSubqueryOutcome::Applied(plan))
    }

    /// Go `handleInSubquery(ctx, planCtx, v)`
    /// (`expression_rewriter.go:1284`), from the point where `v.Expr` has been
    /// rewritten into `lexpr` and `v.Sel` planned into `np`.
    ///
    /// There are two plans an `IN` subquery can become, and the choice is not
    /// cosmetic:
    ///
    /// * `outer JOIN DISTINCT(inner) ON outer_col = inner_col` — an INNER join
    ///   with the inner side de-duplicated, which the join reorderer can then
    ///   move. Legal only for a plain `IN` (never `NOT IN`), with no value
    ///   needed, no correlated column at this level, and compatible collations.
    ///   The `DISTINCT` must be taken on the SAME expression the join compares
    ///   on — Go materialises a coerced right-hand side into a projection
    ///   first, because de-duplicating raw values would let two values that
    ///   compare equal after an implicit cast multiply the outer rows.
    /// * otherwise a semi apply, anti when `NOT IN`, left-outer-semi when a
    ///   value is needed, with the `InOperand` marking that keeps `=`
    ///   NULL-aware.
    ///
    /// `collations_compatible` is Go's
    /// `collate.CompatibleCollate(lt.GetCollate(), rt.GetCollate())` — see the
    /// module header.
    ///
    /// `treat_in_subquery_as_exists_for_filter` is Go
    /// `er.canTreatInSubqueryAsExistsForFilter(planCtx)`
    /// (`expression_rewriter.go:691`), which reads `er.astNodeStack` to decide
    /// whether the `IN` sits in a direct boolean context. `// boundary:` that
    /// state is the AST walk's and does not exist here, so the answer is an
    /// argument.
    ///
    /// # Errors
    ///
    /// [`RewriteError::OperandColumns`] on an arity mismatch, or a builder
    /// error.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub fn handle_in_subquery(
        &mut self,
        outer: LogicalPlan,
        lexpr: &Expression,
        np: LogicalPlan,
        not: bool,
        as_scalar: bool,
        hint_flags: u64,
        collations_compatible: bool,
        treat_in_subquery_as_exists_for_filter: bool,
    ) -> Result<LogicalPlan, RewriteError> {
        let outer_schema = outer.schema().ok_or(RewriteError::MissingSchema)?.clone();
        let np_schema = np.schema().ok_or(RewriteError::MissingSchema)?.clone();
        let l_len = get_row_len(lexpr);
        if l_len != np_schema.len() {
            return Err(RewriteError::OperandColumns(l_len));
        }
        let mut lexpr = lexpr.clone();
        // Go: an anti/left-outer-semi join cannot treat `in` as a plain column
        // equality, so the inner operand is marked. A filter-context `in` that
        // stays a plain semi join does not need the marking.
        let mark_in_operand = not || (as_scalar && !treat_in_subquery_as_exists_for_filter);
        let rexpr = if np_schema.len() == 1 {
            let col = Expression::Column(np_schema.columns[0].clone());
            if mark_in_operand && (!expr_not_null(&lexpr) || !expr_not_null(&col)) {
                lexpr = set_expr_column_in_operand(lexpr);
                mark_in_operand_expr(col)
            } else {
                col
            }
        } else {
            let mut args = Vec::with_capacity(np_schema.len());
            let mut l_args: Vec<Expression> = (0..l_len)
                .map(|i| {
                    get_func_arg(&lexpr, i)
                        .cloned()
                        .unwrap_or_else(|| lexpr.clone())
                })
                .collect();
            for (i, col) in np_schema.columns.iter().enumerate() {
                let col = Expression::Column(col.clone());
                if mark_in_operand && (!expr_not_null(&l_args[i]) || !expr_not_null(&col)) {
                    l_args[i] = set_expr_column_in_operand(l_args[i].clone());
                    args.push(mark_in_operand_expr(col));
                } else {
                    args.push(col);
                }
            }
            if let Expression::ScalarFunction(row_func) = &mut lexpr {
                row_func.args = l_args;
            }
            let ret_type = args[0].static_type().cloned().unwrap_or_else(tiny);
            self.new_function("row", ret_type, args)?
        };
        let check_condition = self.construct_binary_op_function(&lexpr, &rexpr, "eq")?;

        let cor_cols = extract_cor_columns_by_schema_4_logical_plan(&np, &outer_schema);
        let mut no_decorrelate = is_no_decorrelate(
            self.plan_ctx.cur_clause,
            self.env.flags,
            &cor_cols,
            hint_flags,
            SubQueryCtx::In,
            &mut self.hint_warnings,
        );
        if !no_decorrelate
            && !cor_cols.is_empty()
            && !not
            && self.env.flags.enable_correlate_subquery
        {
            no_decorrelate = true;
        }

        let can_rewrite_to_join_agg = self.env.flags.allow_in_subq_to_join_and_agg
            && !not
            && !as_scalar
            && cor_cols.is_empty()
            && collations_compatible;

        let mut plan = if can_rewrite_to_join_agg && !self.env.flags.enable_correlate_subquery {
            self.build_in_subquery_join_agg(outer, np, &lexpr, check_condition, l_len)?
        } else {
            let semi_rewrite = hint_flags & HINT_FLAG_SEMI_JOIN_REWRITE > 0;
            let mut plan = self.build_semi_apply(
                outer,
                np,
                &split_cnf_items(&check_condition),
                as_scalar,
                not,
                semi_rewrite,
                no_decorrelate,
            )?;
            // Go: mark an uncorrelated semi join so `CorrelateSolver` can turn
            // it into a correlated apply in the alternative round.
            if cor_cols.is_empty() && !not && self.env.flags.enable_correlate_subquery {
                if let LogicalPlan::Apply(apply) = &mut plan {
                    apply.join.prefer_correlate = true;
                }
            }
            plan
        };

        self.ctx_stack_pop(1);
        if as_scalar {
            self.push_last_schema_column(&mut plan)?;
        }
        Ok(plan)
    }

    /// The `outer JOIN DISTINCT(inner)` half of [`Self::handle_in_subquery`].
    fn build_in_subquery_join_agg(
        &mut self,
        outer: LogicalPlan,
        np: LogicalPlan,
        lexpr: &Expression,
        check_condition: Expression,
        l_len: usize,
    ) -> Result<LogicalPlan, RewriteError> {
        let np_schema = np.schema().ok_or(RewriteError::MissingSchema)?.clone();
        let mut distinct_child = np;
        let mut distinct_len = np_schema.len();
        let mut join_condition = check_condition.clone();

        // Go: when `=` injected a cast on the right, DISTINCT must run on the
        // CAST value, not the raw one, or two raw values that become equal
        // after the cast would multiply the outer rows.
        if l_len == 1 {
            if let Expression::ScalarFunction(eq_cond) = &check_condition {
                if eq_cond.func_name.lowercase() == "eq" && eq_cond.args.len() == 2 {
                    let rhs = &eq_cond.args[1];
                    if !matches!(rhs, Expression::Column(_)) && expr_from_schema(rhs, &np_schema) {
                        let ret_type = rhs.static_type().cloned().unwrap_or_else(tiny);
                        let proj_col = self.env.new_plan_column(ret_type);
                        let base = self.env.base(LogicalProjection::TYPE);
                        let mut proj = LogicalPlan::Projection(LogicalProjection::new(
                            base,
                            vec![rhs.clone()],
                        ));
                        proj.set_children(vec![distinct_child]);
                        set_own_schema(
                            &mut proj,
                            Schema::new(vec![proj_col.clone()]),
                            vec![FieldName::default()],
                        );
                        distinct_child = proj;
                        distinct_len = 1;
                        join_condition = self.construct_binary_op_function(
                            lexpr,
                            &Expression::Column(proj_col),
                            "eq",
                        )?;
                    }
                }
            }
        }

        let agg = self.build_distinct(distinct_child, distinct_len)?;
        let outer_schema = outer.schema().ok_or(RewriteError::MissingSchema)?.clone();
        let agg_schema = agg.schema().ok_or(RewriteError::MissingSchema)?.clone();
        let schema = merge_schema(Some(&outer_schema), Some(&agg_schema))
            .ok_or(RewriteError::MissingSchema)?;
        let mut names = outer.output_names().to_vec();
        names.extend(agg.output_names().iter().cloned());
        names.resize(schema.len(), FieldName::default());

        let base = self.env.base(LogicalJoin::TYPE);
        let mut join = LogicalJoin::new(base, LogicalJoinType::Inner);
        {
            let builder = self.env.builder();
            let opts = SubstituteOptions::new(&builder);
            join.attach_on_conds(
                &split_cnf_items(&join_condition),
                &outer_schema,
                &agg_schema,
                &opts,
            );
        }
        // Go: inherit the left join's FullSchema/FullNames so a USING/NATURAL
        // name stays resolvable above the rewrite.
        if let LogicalPlan::Join(left) = &outer {
            if left.full_schema.is_some() {
                join.full_schema = left.full_schema.clone();
                join.full_names = left.full_names.clone();
            }
        }
        let mut plan = LogicalPlan::Join(join);
        plan.set_children(vec![outer, agg]);
        set_own_schema(&mut plan, schema, names);
        Ok(plan)
    }

    /// Go `handleScalarSubquery(ctx, planCtx, v)`
    /// (`expression_rewriter.go:1527`), from the point where the subquery has
    /// been planned into `np`.
    ///
    /// A scalar subquery is wrapped in a `MaxOneRow` — SQL requires it to
    /// return at most one row, and the guard is what turns a second row into
    /// an error rather than a silently-wrong value — and then applied with a
    /// LEFT OUTER join, so an outer row with no inner match reads NULL.
    ///
    /// A subquery producing several columns (a row subquery) is read back as a
    /// `row(...)` over the inner schema rather than as a single column.
    ///
    /// # Errors
    ///
    /// See [`Self::build_apply_with_join_type`].
    pub fn handle_scalar_subquery(
        &mut self,
        outer: LogicalPlan,
        np: LogicalPlan,
        hint_flags: u64,
    ) -> Result<ScalarSubqueryOutcome, RewriteError> {
        let outer_schema = outer.schema().ok_or(RewriteError::MissingSchema)?.clone();
        let np = self.build_max_one_row(np);
        let cor_cols = extract_cor_columns_by_schema_4_logical_plan(&np, &outer_schema);
        let no_decorrelate = is_no_decorrelate(
            self.plan_ctx.cur_clause,
            self.env.flags,
            &cor_cols,
            hint_flags,
            SubQueryCtx::Scalar,
            &mut self.hint_warnings,
        );

        if !self.must_build_apply(&np) {
            return Ok(ScalarSubqueryOutcome::EvaluateSeparately { outer, inner: np });
        }
        let np_schema = np.schema().ok_or(RewriteError::MissingSchema)?.clone();
        let mut plan =
            self.build_apply_with_join_type(outer, np, LogicalJoinType::LeftOuter, no_decorrelate)?;
        if np_schema.len() > 1 {
            let row = self.row_of_schema(&np_schema)?;
            self.ctx_stack_append(row, FieldName::default());
        } else {
            self.push_last_schema_column(&mut plan)?;
        }
        Ok(ScalarSubqueryOutcome::Applied(plan))
    }

    /// Go `toColumn(v)` (`expression_rewriter.go:3083`): resolve a column name
    /// against the CURRENT block, then against a `USING`/`NATURAL` join's full
    /// schema, then against each enclosing block OUTWARDS.
    ///
    /// The last step is what makes a subquery correlated: a name the inner
    /// block cannot resolve but an enclosing one can becomes a
    /// [`CorrelatedColumn`] over the OUTER schema's column, and the apply that
    /// wraps the subquery is what later binds it.
    ///
    /// `plan` is `planCtx.plan`, needed only for the natural-join lookups.
    ///
    /// # Errors
    ///
    /// [`RewriteError::Ambiguous`] when the name matches twice in one scope,
    /// [`RewriteError::UnknownColumn`] when no scope resolves it or the match
    /// is a hidden column.
    pub fn to_column(
        &mut self,
        plan: &LogicalPlan,
        v: &QualifiedColumnName,
    ) -> Result<(), RewriteError> {
        let clause = self.clause();
        let idx = find_field_name(&self.names, v).map_err(|_| {
            RewriteError::Ambiguous(v.column.original.clone(), ClauseCode::FieldList.message())
        })?;
        if let Some(idx) = idx {
            let schema = self.schema.as_ref().ok_or(RewriteError::MissingSchema)?;
            let column = schema.columns[idx].clone();
            let name = self.names[idx].clone();
            if column.is_hidden {
                return Err(RewriteError::UnknownColumn(
                    v.column.original.clone(),
                    clause.message(),
                ));
            }
            let (column, name) = self.remap_redundant(plan, clause, column, name);
            self.ctx_stack_append(Expression::Column(column), name);
            return Ok(());
        }

        if let Some((col, name)) = find_field_name_from_natural_using_join(plan, v)? {
            let (column, name) = self.remap_redundant(plan, clause, col.clone(), name.clone());
            self.ctx_stack_append(Expression::Column(column), name);
            return Ok(());
        }

        // Go walks `outerSchemas` from the INNERMOST enclosing block outwards.
        for i in (0..self.plan_ctx.outer_schemas.len()).rev() {
            let outer_names = &self.plan_ctx.outer_names[i];
            let idx = find_field_name(outer_names, v).map_err(|_| {
                RewriteError::Ambiguous(v.column.original.clone(), ClauseCode::FieldList.message())
            })?;
            if let Some(idx) = idx {
                let column = self.plan_ctx.outer_schemas[i].columns[idx].clone();
                let name = outer_names[idx].clone();
                self.ctx_stack_append(
                    Expression::CorrelatedColumn(CorrelatedColumn { column, data: None }),
                    name,
                );
                return Ok(());
            }
        }
        Err(RewriteError::UnknownColumn(
            v.column.original.clone(),
            clause.message(),
        ))
    }

    fn remap_redundant(
        &self,
        plan: &LogicalPlan,
        clause: ClauseCode,
        column: Column,
        name: FieldName,
    ) -> (Column, FieldName) {
        if !should_remap_redundant_base_column(self.plan_ctx.in_dml_stmt, clause, &name) {
            return (column, name);
        }
        // Go: `JOIN ... USING`/`NATURAL` keeps the redundant side in FullSchema
        // for name resolution, but the executable join only outputs the
        // canonical column, so carrying the redundant one further would fail
        // `ResolveIndices` later.
        match resolve_redundant_column_from_natural_using_join_plan(plan, &column) {
            Some((mapped_col, mapped_name)) => (mapped_col.clone(), mapped_name.clone()),
            None => (column, name),
        }
    }

    /// Go's repeated `er.ctxStackAppend(plan.Schema().Columns[len-1],
    /// plan.OutputNames()[len-1])`: the aux column a left-outer-semi apply or
    /// a quantifier projection appended.
    fn push_last_schema_column(&mut self, plan: &mut LogicalPlan) -> Result<(), RewriteError> {
        let schema = plan.schema().ok_or(RewriteError::MissingSchema)?;
        let last = schema
            .len()
            .checked_sub(1)
            .ok_or(RewriteError::MissingSchema)?;
        let column = schema.columns[last].clone();
        let name = plan.output_names().get(last).cloned().unwrap_or_default();
        self.ctx_stack_append(Expression::Column(column), name);
        Ok(())
    }

    /// Go's `np.Schema().Len() == 1 ? Columns[0] : row(Columns...)`.
    fn row_of_schema(&self, schema: &Schema) -> Result<Expression, RewriteError> {
        if schema.len() == 1 {
            return Ok(Expression::Column(schema.columns[0].clone()));
        }
        let args: Vec<Expression> = schema
            .columns
            .iter()
            .cloned()
            .map(Expression::Column)
            .collect();
        let ret_type = args
            .first()
            .and_then(|arg| arg.static_type().cloned())
            .unwrap_or_else(tiny);
        self.new_function("row", ret_type, args)
    }

    /// Go's shared guard `b.disableSubQueryPreprocessing ||
    /// len(ExtractCorrelatedCols4LogicalPlan(np)) > 0 ||
    /// hasCTEConsumerInSubPlan(np)`: whether the subquery MUST become an apply
    /// rather than being evaluated on its own.
    fn must_build_apply(&self, np: &LogicalPlan) -> bool {
        self.env.flags.disable_subquery_preprocessing
            || !extract_correlated_cols_4_logical_plan(np).is_empty()
            || has_cte_consumer_in_sub_plan(np)
    }

    /// Go `b.buildLimit(np, &ast.Limit{Count: 1}, np.QueryBlockOffset())`, the
    /// one shape `handleExistSubquery` needs.
    fn build_limit_one(&self, child: LogicalPlan) -> LogicalPlan {
        let offset = child.base().base.query_block_offset();
        let base = BaseLogicalPlan::new(self.env.plan_ids, LogicalLimit::TYPE, offset);
        let mut plan = LogicalPlan::Limit(LogicalLimit::new(base, 0, 1));
        plan.set_children(vec![child]);
        plan
    }
}

/// Go's `rColCopy := *rCol; rColCopy.InOperand = true`.
fn mark_in_operand(expr: Expression) -> Expression {
    mark_in_operand_expr(expr)
}

fn mark_in_operand_expr(expr: Expression) -> Expression {
    match expr {
        Expression::Column(mut col) => {
            col.in_operand = true;
            Expression::Column(col)
        }
        other => other,
    }
}

/// Go `expression.ExprFromSchema(expr, schema)`: every column `expr` reads is
/// produced by `schema`.
#[must_use]
pub fn expr_from_schema(expr: &Expression, schema: &Schema) -> bool {
    extract_columns(expr).iter().all(|col| schema.contains(col))
}

/// Go `expression.NewOne()`.
fn one() -> Expression {
    Expression::Constant(Constant::new_one())
}

/// Go `expression.NewZero()`.
fn zero() -> Expression {
    Expression::Constant(Constant::new_zero())
}

/// Go `expression.NewNull()`.
fn null() -> Expression {
    Expression::Constant(Constant::new_null())
}

/// Go `isNoDecorrelate(planCtx, corCols, hintFlags, sCtx)`
/// (`expression_rewriter.go:1497`).
///
/// The hint alone is not enough: with NO correlated columns the hint is
/// INAPPLICABLE and is dropped with a warning, and with correlated columns the
/// `tidb_opt_enable_no_decorrelate_in_select` variable can turn it on for a
/// scalar or `EXISTS` subquery sitting in the SELECT list. The
/// `SEMI_JOIN_REWRITE` + `EXISTS` combination is excluded from that variable
/// path because the two would otherwise cancel each other out later.
#[must_use]
pub fn is_no_decorrelate(
    cur_clause: ClauseCode,
    flags: RewriterSessionFlags,
    cor_cols: &[CorrelatedColumn],
    hint_flags: u64,
    subquery_ctx: SubQueryCtx,
    hint_warnings: &mut Vec<String>,
) -> bool {
    let mut no_decorrelate = hint_flags & HINT_FLAG_NO_DECORRELATE > 0;
    if cor_cols.is_empty() {
        if no_decorrelate {
            hint_warnings.push(
                "NO_DECORRELATE() is inapplicable because there are no correlated columns."
                    .to_owned(),
            );
            no_decorrelate = false;
        }
        return no_decorrelate;
    }
    let semi_join_rewrite = hint_flags & HINT_FLAG_SEMI_JOIN_REWRITE > 0;
    if semi_join_rewrite && subquery_ctx == SubQueryCtx::Exists {
        return no_decorrelate;
    }
    let valid_subq_type =
        subquery_ctx == SubQueryCtx::Scalar || subquery_ctx == SubQueryCtx::Exists;
    if valid_subq_type
        && cur_clause == ClauseCode::FieldList
        && !no_decorrelate
        && flags.enable_no_decorrelate_in_select
    {
        no_decorrelate = true;
    }
    no_decorrelate
}

/// Go `expressionRewriter.popExistsSubPlan(planCtx, p)`
/// (`expression_rewriter.go:1262`): strip what `EXISTS` cannot observe.
///
/// A projection or a sort above an `EXISTS` subquery changes nothing about
/// whether a row exists, so it goes. An aggregation with NO `GROUP BY` always
/// produces exactly one row, so the whole subtree collapses to a one-row dual;
/// with a `GROUP BY` it is only the grouping that goes.
#[must_use]
pub fn pop_exists_sub_plan(mut plan: LogicalPlan, plan_ids: &PlanIdAllocator) -> LogicalPlan {
    loop {
        match &plan {
            LogicalPlan::Projection(_) | LogicalPlan::Sort(_) => {
                match take_first_child(&mut plan) {
                    Some(child) => plan = child,
                    None => return plan,
                }
            }
            LogicalPlan::Aggregation(agg) => {
                if agg.group_by_items.is_empty() {
                    let offset = agg.base.base.query_block_offset();
                    let base = BaseLogicalPlan::new(plan_ids, LogicalTableDual::TYPE, offset);
                    return LogicalPlan::TableDual(LogicalTableDual { base, row_count: 1 });
                }
                match take_first_child(&mut plan) {
                    Some(child) => plan = child,
                    None => return plan,
                }
            }
            _ => return plan,
        }
    }
}

/// Go `shouldRemapRedundantBaseColumn(planCtx, clause, name)`
/// (`expression_rewriter.go:3068`).
#[must_use]
pub fn should_remap_redundant_base_column(
    in_dml_stmt: bool,
    clause: ClauseCode,
    name: &FieldName,
) -> bool {
    if clause != ClauseCode::Where && clause != ClauseCode::Having {
        return false;
    }
    // Go: UPDATE/DELETE build the JOIN schema in merged (non-coalesced) order,
    // so the coalesced remap does not apply there.
    if in_dml_stmt {
        return false;
    }
    name.redundant && !name.names.original_table.lower.is_empty()
}

/// Go `findFieldNameFromNaturalUsingJoin(p, v)`
/// (`expression_rewriter.go:3168`): resolve a name against a `USING`/`NATURAL`
/// join's FULL schema, which keeps the redundant side name resolution needs.
///
/// Go recurses through the identity-preserving unary operators; this loops.
///
/// # Errors
///
/// [`RewriteError::Ambiguous`] when the full name slice matches twice.
pub fn find_field_name_from_natural_using_join<'p>(
    plan: &'p LogicalPlan,
    column: &QualifiedColumnName,
) -> Result<Option<(&'p Column, &'p FieldName)>, RewriteError> {
    let mut node = plan;
    loop {
        match node {
            LogicalPlan::Limit(_)
            | LogicalPlan::Selection(_)
            | LogicalPlan::TopN(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::MaxOneRow(_) => match node.children().first() {
                Some(child) => node = child,
                None => return Ok(None),
            },
            LogicalPlan::Join(join) => {
                return lookup_full_names(join.full_schema.as_ref(), &join.full_names, column);
            }
            LogicalPlan::Apply(apply) => {
                // Go: an apply with no FullSchema is a transparent wrapper, so
                // resolution continues into the OUTER (left) child.
                if apply.join.full_schema.is_none() {
                    match node.children().first() {
                        Some(child) => node = child,
                        None => return Ok(None),
                    }
                } else {
                    return lookup_full_names(
                        apply.join.full_schema.as_ref(),
                        &apply.join.full_names,
                        column,
                    );
                }
            }
            _ => return Ok(None),
        }
    }
}

fn lookup_full_names<'p>(
    full_schema: Option<&'p Schema>,
    full_names: &'p [FieldName],
    column: &QualifiedColumnName,
) -> Result<Option<(&'p Column, &'p FieldName)>, RewriteError> {
    let Some(schema) = full_schema else {
        return Ok(None);
    };
    match find_field_name(full_names, column) {
        Err(_) => Err(RewriteError::Ambiguous(
            column.column.original.clone(),
            ClauseCode::FieldList.message(),
        )),
        Ok(Some(idx)) => Ok(Some((&schema.columns[idx], &full_names[idx]))),
        Ok(None) => Ok(None),
    }
}

/// Go `resolveRedundantColumnFromNaturalUsingJoinPlan(p, col)`
/// (`expression_rewriter.go:3200`): map a redundant `USING`/`NATURAL` column
/// to the canonical output column the executable join actually produces.
///
/// Only an INNER join defines the mapping; when a join contains the column but
/// has no mapping for it, the walk continues into its children, so a nested
/// join can still answer. Go recurses; this walks an explicit stack.
#[must_use]
pub fn resolve_redundant_column_from_natural_using_join_plan<'p>(
    plan: &'p LogicalPlan,
    col: &Column,
) -> Option<(&'p Column, &'p FieldName)> {
    let mut stack = vec![plan];
    while let Some(node) = stack.pop() {
        match node {
            LogicalPlan::Limit(_)
            | LogicalPlan::Selection(_)
            | LogicalPlan::TopN(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::MaxOneRow(_) => {
                // These preserve their child's column identity.
                stack.extend(node.children().first());
            }
            LogicalPlan::Join(join) => {
                if let Some(found) = resolve_in_join(node, join, col) {
                    return Some(found);
                }
                stack.extend(node.children().iter());
            }
            LogicalPlan::Apply(apply) => {
                if let Some(found) = resolve_in_join(node, &apply.join, col) {
                    return Some(found);
                }
                stack.extend(node.children().iter());
            }
            _ => {}
        }
    }
    None
}

fn resolve_in_join<'p>(
    node: &'p LogicalPlan,
    join: &LogicalJoin,
    col: &Column,
) -> Option<(&'p Column, &'p FieldName)> {
    // Go: remapping is only defined for inner `JOIN ... USING`/`NATURAL`.
    if join.join_type != LogicalJoinType::Inner {
        return None;
    }
    let full_schema = join.full_schema.as_ref()?;
    if !full_schema.contains(col) {
        return None;
    }
    let self_schema = node.schema()?;
    let (column, name) = join.resolve_redundant_column(col, self_schema, node.output_names())?;
    Some((column, name?))
}

#[cfg(test)]
mod tests;
