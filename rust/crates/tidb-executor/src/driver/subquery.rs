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

//! Subqueries: Go's `expressionRewriter` subquery handling plus the Apply the
//! correlated ones need, reduced to the one shape this tier plans.
//!
//! Two paths, split by whether the subquery reads an outer column. An
//! UNCORRELATED subquery is planned and run once, and its result folds back
//! into the enclosing AST as a literal ([`fold_select_subqueries`]) -- Go's own
//! constant-folding of a scalar subquery, reached here through the AST because
//! this tier executes SQL text. A CORRELATED one cannot be folded, so it is
//! LIFTED out of the expression ([`extract_correlated_subquery`]) and re-run
//! per outer row with the outer columns bound ([`run_correlated_subquery`]),
//! which is Go's Apply operator with a nested loop for its join.
//!
//! [`extract_and_hoist_subquery`] is the aggregate-query variant of the same
//! lift: an aggregate query's Apply column has to be appended AFTER the
//! aggregation, so the select field records where it reads from
//! ([`OutputSlot`]) instead of being rewritten in place.

use std::any::Any;
use std::borrow::Cow;

use super::*;
use crate::plan_trace::{GoLogicalPlanColumns, GoLogicalQuerySourceColumns};
use tidb_ast::{Join, JoinType, Visitable, Visitor};

/// Go's default filter-context branch of `handleInSubquery`:
///
/// ```text
/// outer_key IN (SELECT inner_key ...)
///     => outer INNER JOIN (SELECT DISTINCT inner_key ...) ON outer_key = inner_key
/// ```
///
/// The rewrite is deliberately bounded by the same semantic gates that make
/// this an ordinary equality join: a top-level WHERE conjunct, one
/// uncorrelated scalar output, and compatible collations. Positive `IN`
/// becomes an inner join to a DISTINCT relation. `NOT IN` becomes correlated
/// `NOT EXISTS` only when both scalar keys are statically `NOT NULL`, which is
/// the branch where the two predicates have identical NULL semantics. Other
/// correlated, nullable, row-valued, scalar-context, and LIMIT-bearing forms
/// retain the existing Apply/fold path; moving DISTINCT below a subquery LIMIT
/// would change which rows are deduplicated.
pub(crate) fn rewrite_filter_in_subqueries(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Option<tidb_ast::SelectStmt>, DriverError> {
    let (Some(from), Some(where_clause)) = (&select.from, &select.where_clause) else {
        return Ok(None);
    };
    let mut conjuncts = Vec::new();
    collect_filter_conjuncts(where_clause, &mut conjuncts);
    let has_candidate = conjuncts.iter().any(|conjunct| {
        matches!(
            conjunct,
            tidb_ast::Expr::InSubquery {
                expr,
                subquery,
                ..
            } if !matches!(expr.as_ref(), tidb_ast::Expr::Row(_))
                && matches!(subquery.as_ref(), QueryStmt::Select(inner)
                    if inner.limit.is_none()
                        && matches!(inner.fields.fields(), [SelectField::Expr { .. }]))
        )
    });
    if !has_candidate {
        return Ok(None);
    }
    let outer = select_outer_scope(select, catalog, current_db, ctx);
    let mut residual = Vec::with_capacity(conjuncts.len());
    let mut rewritten_from = from.clone();
    let mut rewritten = 0usize;

    for conjunct in conjuncts {
        if let tidb_ast::Expr::InSubquery {
            expr: lhs,
            subquery,
            not: true,
        } = &conjunct
        {
            if let Some(not_exists) =
                rewrite_non_null_not_in(lhs, subquery, &outer, catalog, current_db, ctx)?
            {
                residual.push(not_exists);
                rewritten += 1;
                continue;
            }
        }
        let tidb_ast::Expr::InSubquery {
            expr: lhs,
            subquery,
            not: false,
        } = &conjunct
        else {
            residual.push(conjunct);
            continue;
        };
        if matches!(lhs.as_ref(), tidb_ast::Expr::Row(_)) {
            residual.push(conjunct);
            continue;
        }
        let QueryStmt::Select(inner) = subquery.as_ref() else {
            residual.push(conjunct);
            continue;
        };
        if inner.limit.is_some() {
            residual.push(conjunct);
            continue;
        }
        let [SelectField::Expr { .. }] = inner.fields.fields() else {
            residual.push(conjunct);
            continue;
        };
        let mut correlated = Vec::new();
        collect_correlated_columns_query(
            subquery,
            &outer,
            catalog,
            current_db,
            &mut correlated,
            ctx,
        );
        if !correlated.is_empty() {
            residual.push(conjunct);
            continue;
        }

        // Optimize the subquery in its own query block before the outer IN
        // adds DISTINCT. Go first rewrites nested filter-context IN predicates
        // in that block, then decorrelates its scalar aggregations. Doing
        // either step after the enclosing semi join adds duplicate elimination
        // makes the synthetic DISTINCT incorrectly block aggregation pull-up.
        let rewritten_inner_in = rewrite_filter_in_subqueries(inner, catalog, current_db, ctx)?;
        let inner = rewritten_inner_in.as_ref().unwrap_or(inner);
        let decorrelated_inner =
            super::correlated_agg_decorrelate::rewrite(inner, catalog, current_db, ctx);
        let inner = decorrelated_inner.as_ref().unwrap_or(inner);

        let Ok(lhs_expression) = rewrite_expr_resolved(lhs, &ScopeResolver { scope: &outer })
        else {
            residual.push(conjunct);
            continue;
        };
        let Some(lhs_type) = lhs_expression.static_type() else {
            residual.push(conjunct);
            continue;
        };
        let inner_columns = plan_select_meta_stmt(inner, catalog, current_db, ctx)?;
        let [(_, inner_type)] = inner_columns.as_slice() else {
            residual.push(conjunct);
            continue;
        };
        if !tidb_datatype::compatible_collate(
            lhs_type.collation_name(),
            inner_type.collation_name(),
        ) {
            residual.push(conjunct);
            continue;
        }

        // The DISTINCT below dedups in the INNER column's own domain, while
        // the join predicate compares in the domain Go's `GetAccurateCmpType`
        // picks for the pair. When those differ the rewrite is unsound on its
        // own: `0 in (select c1 from t0)` over a `blob` compares as REAL, so
        // `'gO'` and `'W'` are two distinct keys that are both `0`, and each
        // outer row matched BOTH -- `select hex(t0.c1) from t0 where 0 in
        // (select t0.c1 from t0)` answered four rows where TiDB answers two.
        //
        // Go carries the same repair and says why (`expression_rewriter.go`,
        // `handleInSubquery`): "DISTINCT must be applied on the same
        // comparison domain as the join predicate ... deduplicating raw inner
        // values is insufficient". It projects the coerced key below the
        // duplicate elimination so both the DISTINCT and the join read
        // exactly that expression.
        let key_cast = match comparison_key_cast(&lhs_expression, &lhs_type, inner_type) {
            KeyCast::None => None,
            KeyCast::To(cast_type) => Some(cast_type),
            // A coercion this tier cannot spell as a `CAST` leaves the
            // predicate to the Apply path, whose IN evaluation needs no
            // duplicate elimination to be correct.
            KeyCast::Inexpressible => {
                residual.push(conjunct);
                continue;
            }
        };
        let relation_alias = fresh_in_subquery_alias(&outer, rewritten, "relation");
        let key_alias = format!("__in_subquery_key_{rewritten}");
        let mut distinct = (*inner).clone();
        // Go builds a second duplicate-elimination aggregation above the
        // subquery, then AggregationEliminator removes it when the subquery's
        // output already contains its complete GROUP BY tuple. Preserve that
        // logical boundary in the AST adapter by omitting the redundant
        // DISTINCT up front. A projected subset of the group tuple is not
        // unique and still keeps duplicate elimination.
        distinct.distinct = !grouped_output_is_unique(inner);
        distinct.all = false;
        let [SelectField::Expr { expr, alias }] = distinct.fields.fields_mut() else {
            unreachable!("the one-field shape was checked above");
        };
        if let Some(cast_type) = key_cast {
            *expr = tidb_ast::Expr::Cast(tidb_ast::CastExpr {
                expr: Box::new(expr.clone()),
                cast_type,
                style: tidb_ast::CastStyle::Cast,
                array: false,
            });
            // A group tuple that is unique in the inner column's own domain
            // says nothing about the PROJECTED key, so the duplicate
            // elimination Go's `AggregationEliminator` would have removed has
            // to stay -- it is the only thing that stops the collapse.
            distinct.distinct = true;
        }
        *alias = Some(key_alias.clone());

        rewritten_from = Join {
            left: JoinNode::Join(Box::new(rewritten_from)),
            right: Some(JoinNode::Derived {
                subquery: tidb_ast::NodeBox::new(QueryStmt::Select(Box::new(distinct))),
                alias: Some(relation_alias.clone()),
                lateral: false,
                column_names: Vec::new(),
            }),
            tp: JoinType::Cross,
            straight: false,
            on: Some(tidb_ast::Expr::Binary(
                tidb_ast::BinaryOp::Eq,
                Box::new((**lhs).clone()),
                Box::new(tidb_ast::Expr::Column(vec![relation_alias, key_alias])),
            )),
            using: Vec::new(),
            natural: false,
            explicit_parens: false,
        };
        rewritten += 1;
    }

    if rewritten == 0 {
        return Ok(None);
    }
    let mut result = select.clone();
    if !expand_unqualified_wildcards(&mut result, &outer) {
        return Ok(None);
    }
    result.from = Some(rewritten_from);
    result.where_clause = combine_filter_conjuncts(residual);
    Ok(Some(result))
}

/// Expands an unqualified `*` against `scope`, which must be the scope of the
/// `FROM` the statement had BEFORE a rewrite added a relation to it.
///
/// Go makes this cut by ORDERING: `buildSelect` calls
/// `unfoldWildStar(p, sel.Fields.Fields)` (`logical_plan_builder.go:4351`)
/// and only afterwards `buildSelection` (`:4436`), which is where
/// `handleInSubquery` replaces the plan with the join it synthesizes. So `*`
/// is already a column list by the time the join exists, even though the join
/// takes `MergeSchema(plan, agg)` for its own schema.
///
/// This tier rewrites the AST rather than the plan, so the two steps happen in
/// the other order and a surviving `*` resolves against the REWRITTEN `FROM`:
/// `select * from t1 where 1 in (select b from t2)` answered
/// `a, __in_subquery_key_0` where TiDB answers `a`, the synthesized join key
/// having become an output column. Expanding here restores Go's order.
///
/// Only an unqualified `*` is affected -- `t1.*` names a relation the rewrite
/// does not touch. Returns `false` when a column cannot be given a qualified
/// path, which leaves the caller to decline its rewrite rather than emit a
/// statement whose `*` means something new.
pub(crate) fn expand_unqualified_wildcards(
    select: &mut tidb_ast::SelectStmt,
    scope: &FromScope,
) -> bool {
    let unqualified =
        |field: &SelectField| matches!(field, SelectField::Wildcard(path) if path.last().is_none());
    if !select.fields.fields().iter().any(unqualified) {
        return true;
    }
    let mut expanded = Vec::with_capacity(select.fields.fields().len());
    for field in select.fields.fields() {
        if !unqualified(field) {
            expanded.push(field.clone());
            continue;
        }
        for (offset, _, _) in scope.star_columns() {
            let Some(path) = scope.qualified_path(offset) else {
                return false;
            };
            expanded.push(SelectField::Expr {
                expr: tidb_ast::Expr::Column(path),
                alias: None,
            });
        }
    }
    select.fields = expanded.into();
    true
}

/// What the inner key must be cast to before duplicate elimination, so that
/// DISTINCT runs in the same domain as the join predicate.
enum KeyCast {
    /// The comparison already runs in the inner column's own domain.
    None,
    /// Project `CAST(inner AS ...)` and dedup on that.
    To(tidb_ast::CastType),
    /// The domain differs and this tier has no `CAST` target for it.
    Inexpressible,
}

/// Go `GetAccurateCmpType(lhs, rhs)` applied to the `IN`'s two keys, reduced
/// to the cast the projected key needs.
///
/// The inner side is the subquery's output COLUMN, which is Go's
/// `np.Schema().Columns[0]` -- never a constant.
fn comparison_key_cast(
    lhs: &tidb_expr::expression::Expression,
    lhs_type: &tidb_datatype::FieldType,
    inner_type: &tidb_datatype::FieldType,
) -> KeyCast {
    use tidb_datatype::EvalType;
    use tidb_expr::builtin_compare::{get_accurate_cmp_type, CmpOperand};
    let cmp_type = get_accurate_cmp_type(
        CmpOperand {
            field_type: lhs_type,
            is_constant: matches!(lhs, tidb_expr::expression::Expression::Constant(_)),
            is_column: matches!(lhs, tidb_expr::expression::Expression::Column(_)),
        },
        CmpOperand::column(inner_type),
    );
    if cmp_type == inner_type.eval_type() {
        return KeyCast::None;
    }
    // A STRING-domain inner key coerced into a NUMERIC comparison domain
    // stays on the Apply/fold path, whose `IN` evaluation coerces every list
    // value per outer row. Projecting the coerced key below the duplicate
    // elimination -- Go's repair (`expression_rewriter.go`, handleInSubquery,
    // "DISTINCT must be applied on the same comparison domain ...") -- keeps
    // the ANSWER right but moves the coercion from per-comparison to
    // once-per-inner-row, and a string-to-number coercion is observable:
    // each one raises 1292 `Truncated incorrect DOUBLE value` through the
    // statement's truncate policy (`builtin_cast.go` CastStringAsRealSig ->
    // types.StrToFloat). Go reaches the same per-comparison coercions only
    // because its vectorized `IN` re-evaluates every arg per chunk; this
    // tier's join shape cannot reproduce that surface, so it declines the
    // rewrite exactly where the existing `Inexpressible` arm already does --
    // the fold needs no duplicate elimination to be correct.
    let numeric = |eval_type: tidb_datatype::EvalType| {
        matches!(
            eval_type,
            tidb_datatype::EvalType::Int
                | tidb_datatype::EvalType::Real
                | tidb_datatype::EvalType::Decimal
        )
    };
    if inner_type.eval_type() == tidb_datatype::EvalType::String && numeric(cmp_type) {
        return KeyCast::Inexpressible;
    }
    match cmp_type {
        // Go `WrapWithCastAsReal`: `TypeDouble`, no length of its own.
        EvalType::Real => KeyCast::To(tidb_ast::CastType::Double),
        // Go `WrapWithCastAsInt` keeps only the source's UNSIGNED flag, and
        // every source that reaches an INT comparison without already being
        // one is a hybrid, whose value is an unsigned ordinal or literal.
        EvalType::Int => KeyCast::To(if inner_type.is_unsigned() || inner_type.is_hybrid() {
            tidb_ast::CastType::Unsigned
        } else {
            tidb_ast::CastType::Signed
        }),
        // Go `WrapWithCastAsDecimal`.
        EvalType::Decimal => {
            let (flen, scale) = decimal_cast_shape(inner_type);
            KeyCast::To(tidb_ast::CastType::Decimal { flen, scale })
        }
        _ => KeyCast::Inexpressible,
    }
}

/// Go `WrapWithCastAsDecimal`'s target `flen`/`decimal`, without its
/// constant-folding refinement (the inner key is a column, never a constant).
fn decimal_cast_shape(source: &tidb_datatype::FieldType) -> (u32, u32) {
    use tidb_datatype::{EvalType, FieldTypeCode, MAX_DECIMAL_WIDTH, UNSPECIFIED_LENGTH};
    // Go `mysql.MaxIntWidth`, `getFixedLen`'s widest integer display width.
    const MAX_INT_WIDTH: i64 = 20;
    let (mut flen, mut scale) = (source.flen(), source.decimal());
    if source.eval_type() == EvalType::Int {
        // `minimalDecimalLenForHoldingInteger`.
        flen = match source.code() {
            FieldTypeCode::Tiny => 3,
            FieldTypeCode::Short => 5,
            FieldTypeCode::Int24 => 8,
            FieldTypeCode::Long => 10,
            FieldTypeCode::LongLong => 20,
            FieldTypeCode::Year => 4,
            _ => MAX_INT_WIDTH,
        };
        scale = 0;
    }
    if flen == UNSPECIFIED_LENGTH || flen > MAX_DECIMAL_WIDTH {
        flen = MAX_DECIMAL_WIDTH;
    }
    let cap = u32::try_from(MAX_DECIMAL_WIDTH).unwrap_or(65);
    (
        u32::try_from(flen).unwrap_or(cap),
        u32::try_from(scale).unwrap_or(0),
    )
}

fn grouped_output_is_unique(select: &tidb_ast::SelectStmt) -> bool {
    if select.group_by.is_empty() {
        return false;
    }
    let outputs = select
        .fields
        .fields()
        .iter()
        .map(|field| match field {
            SelectField::Expr { expr, .. } => Some(expr),
            SelectField::Wildcard(_) => None,
        })
        .collect::<Option<Vec<_>>>();
    let Some(outputs) = outputs else {
        return false;
    };
    select
        .group_by
        .iter()
        .all(|group| outputs.iter().any(|output| *output == &group.expr))
}

/// Go's `handleInSubquery` can use an anti-semi join for `NOT IN`. The null
/// aware form carries extra semantics, so this direct `NOT EXISTS` lowering
/// is restricted to two proven non-null scalar columns.
fn rewrite_non_null_not_in(
    lhs: &tidb_ast::Expr,
    subquery: &QueryStmt,
    outer: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Option<tidb_ast::Expr>, DriverError> {
    if !matches!(lhs, tidb_ast::Expr::Column(_)) {
        return Ok(None);
    }
    let Ok(lhs_expression) = rewrite_expr_resolved(lhs, &ScopeResolver { scope: outer }) else {
        return Ok(None);
    };
    if lhs_expression
        .static_type()
        .is_none_or(|field_type| !field_type.has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL))
    {
        return Ok(None);
    }

    let QueryStmt::Select(inner) = subquery else {
        return Ok(None);
    };
    if inner.kind != tidb_ast::SelectStatementKind::Select
        || inner.with.is_some()
        || !inner.hints.is_empty()
        || inner.from.is_none()
        || inner.distinct
        || !inner.group_by.is_empty()
        || inner.rollup
        || inner.having.is_some()
        || !inner.windows.is_empty()
        || inner.limit.is_some()
        || inner.lock.is_some()
        || inner.into_outfile.is_some()
    {
        return Ok(None);
    }
    let [SelectField::Expr {
        expr: inner_key @ tidb_ast::Expr::Column(_),
        ..
    }] = inner.fields.fields()
    else {
        return Ok(None);
    };
    if inner.where_clause.as_ref().is_some_and(expr_has_subquery) {
        return Ok(None);
    }
    let output = plan_select_meta_stmt(inner, catalog, current_db, ctx)?;
    let [(_, inner_type)] = output.as_slice() else {
        return Ok(None);
    };
    if !inner_type.has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL)
        || !tidb_datatype::compatible_collate(
            lhs_expression
                .static_type()
                .expect("the outer type was checked")
                .collation_name(),
            inner_type.collation_name(),
        )
    {
        return Ok(None);
    }

    let equality = tidb_ast::Expr::Binary(
        tidb_ast::BinaryOp::Eq,
        Box::new(lhs.clone()),
        Box::new(inner_key.clone()),
    );
    let mut rewritten = (**inner).clone();
    let mut predicates = Vec::new();
    if let Some(predicate) = rewritten.where_clause.take() {
        collect_filter_conjuncts(&predicate, &mut predicates);
    }
    predicates.push(equality);
    rewritten.where_clause = combine_filter_conjuncts(predicates);
    Ok(Some(tidb_ast::Expr::Exists {
        subquery: tidb_ast::NodeBox::new(QueryStmt::Select(Box::new(rewritten))),
        not: true,
    }))
}

fn collect_filter_conjuncts(expr: &tidb_ast::Expr, out: &mut Vec<tidb_ast::Expr>) {
    match expr {
        tidb_ast::Expr::Paren(inner) => collect_filter_conjuncts(inner, out),
        tidb_ast::Expr::Binary(tidb_ast::BinaryOp::LogicAnd, left, right) => {
            collect_filter_conjuncts(left, out);
            collect_filter_conjuncts(right, out);
        }
        other => out.push(other.clone()),
    }
}

fn combine_filter_conjuncts(mut conjuncts: Vec<tidb_ast::Expr>) -> Option<tidb_ast::Expr> {
    let first = conjuncts.pop()?;
    Some(conjuncts.into_iter().rev().fold(first, |right, left| {
        tidb_ast::Expr::Binary(
            tidb_ast::BinaryOp::LogicAnd,
            Box::new(left),
            Box::new(right),
        )
    }))
}

fn fresh_in_subquery_alias(outer: &FromScope, ordinal: usize, kind: &str) -> String {
    let mut suffix = ordinal;
    loop {
        let candidate = format!("__in_subquery_{kind}_{suffix}");
        if outer
            .tables
            .iter()
            .all(|table| !table.name.eq_ignore_ascii_case(&candidate))
        {
            return candidate;
        }
        suffix += 1;
    }
}
/// The type of the column an Apply appends for a correlated scalar subquery.
///
/// Go infers it statically from the subquery's select field, where a
/// correlated reference is a `CorrelatedColumn` carrying the OUTER column's
/// own `RetType`. Here the query is planned once with every correlated column
/// bound to a stand-in value, which reaches the same field type without
/// depending on any outer row -- and it must, because the appended column's
/// width is fixed before the first inner run (a `SUM` is a 40-byte decimal,
/// not an 8-byte integer).
///
/// The stand-in is [`probe_datum`] of the outer column's own type, NOT a bare
/// NULL, for the reason `build_lateral_join` already states for the `LATERAL`
/// shape: a NULL erases the type it stood for, so `select t.a from t t1 limit
/// 1` infers `NULL`, whose chunk column is variable-length, and the first
/// inner run then appends an 8-byte integer into a zero-width cell and panics.
/// Both Apply shapes now settle their inner column the one way.
///
/// `outer` is the scope the correlated columns bind against. Falling back to
/// `LongLong` matches what the rest of the seed does for an uninferred
/// expression.
pub(crate) fn subquery_result_type(
    correlated: &CorrelatedSubquery,
    outer: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Option<FieldType> {
    let resolver = ScopeResolver { scope: outer };
    let probes: Vec<(Vec<String>, Datum)> = correlated
        .columns
        .iter()
        .map(|path| {
            let datum = resolver.resolve(path).map_or(Datum::Null, |(_, ft, _)| {
                crate::driver::from::probe_datum(&ft)
            });
            (path.clone(), datum)
        })
        .collect();
    let typed = bind_subquery_columns_query(&correlated.query, &probes).ok()?;
    let columns = match &typed {
        QueryStmt::Select(select) => plan_select_meta_stmt(select, catalog, current_db, ctx),
        QueryStmt::SetOpr(set_opr) => plan_set_opr_meta_stmt(set_opr, catalog, current_db, ctx),
    }
    .ok()?;
    columns.first().map(|(_, ft)| ft.clone())
}

/// A short description of a driver error, for the executor-level error the
/// apply callback must return.
///
/// Borrowed when the reason is fixed text, owned when the refusal built it per
/// call -- so a refusal that names what it saw keeps saying so here instead of
/// being flattened to the generic phrase.
pub(crate) fn driver_error_text(error: &DriverError) -> Cow<'static, str> {
    match error {
        DriverError::SubqueryReturnsMoreThanOneRow => {
            Cow::Borrowed("Subquery returns more than 1 row")
        }
        DriverError::Unsupported(text) => text.clone(),
        _ => Cow::Borrowed("the correlated subquery failed"),
    }
}

/// What the outer expression asks of a correlated subquery's result.
///
/// Go builds a different plan for each: `handleScalarSubquery` for a scalar
/// read, and a semi join (`LogicalJoin` with `SemiJoin`/`AntiSemiJoin`/
/// `LeftOuterSemiJoin`) for `EXISTS`, `IN` and `ANY`/`ALL`. Here they all ride
/// one Apply, because the join's answer for one outer row is exactly what
/// running the inner query for that row and folding the result yields.
pub(crate) enum SubqueryKind {
    /// A scalar read: the one value the subquery selects, NULL if no row.
    Scalar,
    /// `[NOT] EXISTS`.
    Exists { not: bool },
    /// `lhs [NOT] IN (subquery)`. `lhs` belongs to the OUTER scope and is
    /// evaluated per outer row against that row's inner result.
    In { lhs: tidb_ast::Expr, not: bool },
    /// `lhs <op> ANY|ALL (subquery)`.
    Compare {
        op: tidb_ast::BinaryOp,
        lhs: tidb_ast::Expr,
        all: bool,
    },
}

/// A correlated subquery found in an outer expression: the subquery itself and
/// what its result is asked for.
pub(crate) struct CorrelatedSubquery {
    pub(crate) query: QueryStmt,
    pub(crate) kind: SubqueryKind,
    pub(crate) columns: Vec<Vec<String>>,
}

/// Whether `expr` references a column of the OUTER scope, which is what makes
/// a subquery correlated (Go's `ExtractCorrelatedCols4LogicalPlan`).
///
/// A reference is correlated when the inner query's own `FROM` cannot resolve
/// it but the outer scope can -- the same two-scope test Go's name resolver
/// applies when it binds a column to an outer plan's schema.
/// [`collect_correlated_columns`], widened to a `QueryStmt`: a set operation's
/// correlated columns are the union of what each of its terms references,
/// since every term is re-run per outer row exactly like a lone `SELECT` is.
/// A statement-level `ORDER BY`/`LIMIT` names an output column or position
/// (see `sort_rows_by_output`), never an outer one, so it contributes nothing
/// here.
pub(crate) fn collect_correlated_columns_query(
    query: &QueryStmt,
    outer: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    found: &mut Vec<Vec<String>>,
    ctx: &crate::StmtContext,
) {
    match query {
        QueryStmt::Select(select) => {
            collect_correlated_columns(select, outer, catalog, current_db, found, ctx)
        }
        QueryStmt::SetOpr(set_opr) => {
            for term in &set_opr.terms {
                match &term.body {
                    tidb_ast::SetOprTermBody::Select(select) => {
                        collect_correlated_columns(select, outer, catalog, current_db, found, ctx)
                    }
                    tidb_ast::SetOprTermBody::Nested(nested) => collect_correlated_columns_query(
                        &QueryStmt::SetOpr(nested.clone()),
                        outer,
                        catalog,
                        current_db,
                        found,
                        ctx,
                    ),
                }
            }
        }
    }
}

fn collect_correlated_columns(
    select: &tidb_ast::SelectStmt,
    outer: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    found: &mut Vec<Vec<String>>,
    ctx: &crate::StmtContext,
) {
    let inner = match &select.from {
        None => FromScope::for_statement(ctx),
        Some(join) => {
            let mut trace = PlanTrace::planning();
            match build_join(
                join,
                catalog,
                current_db,
                ctx,
                Some(&mut trace),
                None,
                crate::driver::leaf_demand::FromDemand::none(),
                &tidb_planner::physical_property::PhysicalProperty::default(),
                None,
            ) {
                Ok((_, scope, _)) => scope,
                // An unresolvable inner FROM is reported by the inner run itself.
                Err(_) => FromScope::for_statement(ctx),
            }
        }
    };
    let mut visit = |expr: &tidb_ast::Expr| {
        collect_outer_columns(expr, &inner, outer, found);
    };
    for field in select.fields.fields() {
        if let SelectField::Expr { expr, .. } = field {
            visit(expr);
        }
    }
    if let Some(where_clause) = &select.where_clause {
        visit(where_clause);
    }
    if let Some(having) = &select.having {
        visit(having);
    }
    for item in &select.group_by {
        visit(&item.expr);
    }
    for item in &select.order_by {
        visit(&item.expr);
    }
}

/// Records every column reference in `expr` that the inner scope cannot
/// resolve but the outer scope can.
fn collect_outer_columns(
    expr: &tidb_ast::Expr,
    inner: &FromScope,
    outer: &FromScope,
    found: &mut Vec<Vec<String>>,
) {
    struct Collector<'a> {
        inner: &'a FromScope,
        outer: &'a FromScope,
        found: &'a mut Vec<Vec<String>>,
    }

    impl Visitor for Collector<'_> {
        fn enter(&mut self, node: &mut dyn Any) -> bool {
            let Some(expr) = node.downcast_mut::<tidb_ast::Expr>() else {
                return false;
            };
            if matches!(
                expr,
                tidb_ast::Expr::Subquery(_)
                    | tidb_ast::Expr::Exists { .. }
                    | tidb_ast::Expr::InSubquery { .. }
                    | tidb_ast::Expr::CompareSubquery { .. }
            ) {
                return true;
            }
            let tidb_ast::Expr::Column(path) = expr else {
                return false;
            };
            let inner_resolver = ScopeResolver { scope: self.inner };
            let outer_resolver = ScopeResolver { scope: self.outer };
            if inner_resolver.resolve(path).is_none()
                && outer_resolver.resolve(path).is_some()
                && !self.found.contains(path)
            {
                self.found.push(path.clone());
            }
            true
        }

        fn leave(&mut self, _node: &mut dyn Any) -> bool {
            true
        }
    }

    let mut walked = expr.clone();
    walked.accept(&mut Collector {
        inner,
        outer,
        found,
    });
}

/// Correlated columns referenced by one expression, using the same lexical
/// inner-then-outer resolution as [`collect_correlated_columns_query`].
pub(crate) fn collect_correlated_columns_expr(
    expr: &tidb_ast::Expr,
    inner: &FromScope,
    outer: &FromScope,
) -> Vec<Vec<String>> {
    let mut found = Vec::new();
    collect_outer_columns(expr, inner, outer, &mut found);
    found
}

/// Replaces each correlated column reference with the literal for the outer
/// row's value, which is this port's equivalent of Go's apply loop writing
/// `*col.Data` before re-running the inner plan.
fn bind_correlated_columns(
    expr: &tidb_ast::Expr,
    bindings: &[(Vec<String>, Datum)],
) -> Result<tidb_ast::Expr, DriverError> {
    struct Binder<'a> {
        bindings: &'a [(Vec<String>, Datum)],
        error: Option<DriverError>,
    }

    impl Visitor for Binder<'_> {
        fn enter(&mut self, node: &mut dyn Any) -> bool {
            if self.error.is_some() {
                return true;
            }
            let Some(expr) = node.downcast_mut::<tidb_ast::Expr>() else {
                return false;
            };
            // A nested query owns a new lexical scope. Its correlated names
            // are bound when that query becomes its own Apply, never by
            // walking through it from this expression.
            if matches!(
                expr,
                tidb_ast::Expr::Subquery(_)
                    | tidb_ast::Expr::Exists { .. }
                    | tidb_ast::Expr::InSubquery { .. }
                    | tidb_ast::Expr::CompareSubquery { .. }
            ) {
                return true;
            }
            let tidb_ast::Expr::Column(path) = expr else {
                return false;
            };
            let Some((_, value)) = self
                .bindings
                .iter()
                .find(|(bound, _)| paths_match(bound, path))
            else {
                return true;
            };
            match datum_to_literal(value) {
                Ok(literal) => *expr = literal,
                Err(error) => self.error = Some(error),
            }
            true
        }

        fn leave(&mut self, _node: &mut dyn Any) -> bool {
            true
        }
    }

    let mut bound = expr.clone();
    let mut binder = Binder {
        bindings,
        error: None,
    };
    bound.accept(&mut binder);
    if let Some(error) = binder.error {
        return Err(error);
    }
    Ok(bound)
}

/// Whether a bound path and a reference name the same column.
///
/// The comparison is the WHOLE path, because every binding was recorded from
/// the very references it is substituted into ([`collect_outer_columns`] pushes
/// each path as written), so an unqualified outer reference is bound under its
/// unqualified path and needs no suffix rule. Matching on the last name alone
/// would instead bind the INNER query's own same-named column: in
/// `SELECT id FROM emp WHERE emp.dept_id = dept.id AND emp.id = 10`, the outer
/// binding for `dept.id` would swallow `emp.id` and the selected `id`, and the
/// subquery would return NULL for every outer row.
fn paths_match(bound: &[String], candidate: &[String]) -> bool {
    bound.len() == candidate.len()
        && bound
            .iter()
            .zip(candidate)
            .all(|(a, b)| a.eq_ignore_ascii_case(b))
}

/// Substitutes `bindings` for the correlated column references in every clause
/// of `select`.
fn bind_subquery_columns(
    select: &tidb_ast::SelectStmt,
    bindings: &[(Vec<String>, Datum)],
) -> Result<tidb_ast::SelectStmt, DriverError> {
    let mut bound = select.clone();
    for field in bound.fields.fields_mut() {
        if let SelectField::Expr { expr, .. } = field {
            *expr = bind_correlated_columns(expr, bindings)?;
        }
    }
    if let Some(where_clause) = &bound.where_clause {
        bound.where_clause = Some(bind_correlated_columns(where_clause, bindings)?);
    }
    if let Some(having) = &bound.having {
        bound.having = Some(bind_correlated_columns(having, bindings)?);
    }
    for item in &mut bound.group_by {
        item.expr = bind_correlated_columns(&item.expr, bindings)?;
    }
    for item in &mut bound.order_by {
        item.expr = bind_correlated_columns(&item.expr, bindings)?;
    }
    Ok(bound)
}

/// [`bind_subquery_columns`], widened to a `QueryStmt`: every term of a set
/// operation gets the same substitution, since each is re-run per outer row.
pub(crate) fn bind_subquery_columns_query(
    query: &QueryStmt,
    bindings: &[(Vec<String>, Datum)],
) -> Result<QueryStmt, DriverError> {
    Ok(match query {
        QueryStmt::Select(select) => {
            QueryStmt::Select(Box::new(bind_subquery_columns(select, bindings)?))
        }
        QueryStmt::SetOpr(set_opr) => {
            let mut bound = (**set_opr).clone();
            for term in &mut bound.terms {
                term.body = match &term.body {
                    tidb_ast::SetOprTermBody::Select(select) => tidb_ast::SetOprTermBody::Select(
                        Box::new(bind_subquery_columns(select, bindings)?),
                    ),
                    tidb_ast::SetOprTermBody::Nested(nested) => {
                        let QueryStmt::SetOpr(nested) = bind_subquery_columns_query(
                            &QueryStmt::SetOpr(nested.clone()),
                            bindings,
                        )?
                        else {
                            unreachable!("SetOpr input binds to SetOpr output")
                        };
                        tidb_ast::SetOprTermBody::Nested(nested)
                    }
                };
            }
            QueryStmt::SetOpr(Box::new(bound))
        }
    })
}

/// Resolves the outer-row indexes represented by correlated column paths.
pub(crate) fn correlated_path_indices(
    paths: &[Vec<String>],
    outer_scope: &FromScope,
) -> Result<Vec<usize>, DriverError> {
    paths
        .iter()
        .map(|path| {
            let resolver = ScopeResolver { scope: outer_scope };
            let (index, _, _) = resolver
                .resolve(path)
                .ok_or(DriverError::unsupported("unresolved correlated column"))?;
            Ok(index)
        })
        .collect()
}

/// Resolves the cache-key columns for a correlated subquery.
pub(crate) fn correlated_column_indices(
    correlated: &CorrelatedSubquery,
    outer_scope: &FromScope,
) -> Result<Vec<usize>, DriverError> {
    let mut paths = correlated.columns.clone();
    match &correlated.kind {
        // The Apply result for a semi/anti-semi comparison depends on both
        // the inner query's correlated columns AND the outer operand it tests
        // against that result. Caching only the former makes `(a, b) IN (...)`
        // reuse `b`'s answer for a later row with the same `a`.
        SubqueryKind::In { lhs, .. } => {
            for path in super::only_full_group_by::bare_columns(lhs) {
                if !paths.contains(&path) {
                    paths.push(path);
                }
            }
        }
        SubqueryKind::Compare { lhs, .. } => {
            for path in super::only_full_group_by::bare_columns(lhs) {
                if !paths.contains(&path) {
                    paths.push(path);
                }
            }
        }
        SubqueryKind::Scalar | SubqueryKind::Exists { .. } => {}
    }
    paths
        .iter()
        .map(|path| {
            let resolver = ScopeResolver { scope: outer_scope };
            // Aggregation output is one flat row with no table qualifiers,
            // so a qualified correlated reference falls back to its final
            // name there. Plain source scopes resolve the qualified path.
            let (index, _, _) = resolver
                .resolve(path)
                .or_else(|| {
                    let name = path.last()?;
                    resolver.resolve(std::slice::from_ref(name))
                })
                .ok_or(DriverError::unsupported("unresolved correlated column"))?;
            Ok(index)
        })
        .collect()
}

/// Binds every correlated column in a query and runs it for one outer row.
pub(crate) fn run_correlated_subquery(
    correlated: &CorrelatedSubquery,
    outer_values: &[Datum],
    outer_scope: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Datum, DriverError> {
    let indices = correlated_column_indices(correlated, outer_scope)?;
    let mut bindings = Vec::with_capacity(correlated.columns.len());
    for (path, index) in correlated.columns.iter().zip(indices) {
        let value = outer_values
            .get(index)
            .cloned()
            .ok_or(DriverError::unsupported("correlated column out of range"))?;
        bindings.push((path.clone(), value));
    }

    let bound = bind_subquery_columns_query(&correlated.query, &bindings)?;
    let (_, rows) = run_query_stmt(&bound, catalog, current_db, ctx)?;
    match &correlated.kind {
        // EXISTS folds to 1/0 per outer row.
        SubqueryKind::Exists { not } => Ok(Datum::Int(i64::from(!rows.is_empty() != *not))),
        SubqueryKind::Scalar => match rows.len() {
            0 => Ok(Datum::Null),
            1 => {
                let [value] = rows[0].as_slice() else {
                    return Err(DriverError::unsupported(
                        "a scalar subquery selecting several columns is not supported yet",
                    ));
                };
                Ok(value.clone())
            }
            _ => Err(DriverError::SubqueryReturnsMoreThanOneRow),
        },
        // The semi-join shapes: this outer row's inner result becomes a value
        // list, and the test is evaluated over it exactly as the uncorrelated
        // fold evaluates its own folded list -- same `IN`, same comparisons,
        // so the same three-valued answers.
        SubqueryKind::In { lhs, not } => {
            let list = subquery_value_list(&rows, true)?;
            let test = in_list_expr(lhs.clone(), list, *not);
            eval_expr_on_row(&test, outer_scope, outer_values, ctx)
        }
        SubqueryKind::Compare { op, lhs, all } => {
            let list = subquery_value_list(&rows, false)?;
            let test = any_all_expr(*op, lhs.clone(), *all, list);
            eval_expr_on_row(&test, outer_scope, outer_values, ctx)
        }
    }
}

/// A subquery result's rows as the literals a value list needs. A
/// multi-column row becomes an AST row constructor, which the existing `IN`
/// evaluator compares component by component.
fn subquery_value_list(
    rows: &[Vec<Datum>],
    allow_rows: bool,
) -> Result<Vec<tidb_ast::Expr>, DriverError> {
    let mut list = Vec::with_capacity(rows.len());
    for row in rows {
        let values = row
            .iter()
            .map(|value| {
                // Go keeps a subquery result as a runtime column of the
                // semi-join. This driver materializes that relation, so a
                // bare literal would let constant folding collapse the whole
                // predicate and run its coercions once instead of once per
                // outer row. ANY_VALUE is Go's non-foldable scalar identity:
                // it preserves the exact datum/type while retaining the
                // runtime-column property of the source plan.
                datum_to_literal(value).map(|literal| tidb_ast::Expr::Func {
                    name: "any_value".to_owned(),
                    args: vec![literal],
                    origin_position: 0,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        list.push(match values.as_slice() {
            [value] => value.clone(),
            _ if allow_rows => tidb_ast::Expr::Row(values),
            _ => {
                return Err(DriverError::unsupported(
                    "an ANY/ALL subquery selecting several columns is not supported yet",
                ))
            }
        });
    }
    Ok(list)
}

/// `lhs [NOT] IN (list)`, with the empty list written as the constant it is.
///
/// `x IN ()` is not sayable in SQL: an empty subquery result makes `IN` false
/// and `NOT IN` true for every x INCLUDING NULL, because MySQL evaluates the
/// semi join, which finds no row to match. The non-empty case keeps the
/// ordinary `IN`, whose NULL rules are the three-valued ones (an unmatched x
/// against a list holding NULL is NULL, not false).
fn in_list_expr(lhs: tidb_ast::Expr, list: Vec<tidb_ast::Expr>, not: bool) -> tidb_ast::Expr {
    if list.is_empty() {
        return tidb_ast::Expr::Int(i64::from(not).to_string());
    }
    tidb_ast::Expr::In {
        expr: Box::new(lhs),
        list,
        not,
    }
}

/// `lhs <op> ANY|ALL (list)` as the OR/AND chain it is defined to be.
///
/// Go's `buildSemiApply` for a comparison subquery builds the same disjunction
/// (`ANY`) or conjunction (`ALL`) of per-value comparisons, which is where the
/// three-valued behaviour comes from: `20 > ANY (25, NULL)` is
/// `false OR NULL` = NULL, while `20 > ALL (25, NULL)` is `false AND NULL` =
/// false. An empty list has no comparison at all, so `ALL` is vacuously TRUE
/// and `ANY` is FALSE -- both for a NULL `lhs` too.
fn any_all_expr(
    op: tidb_ast::BinaryOp,
    lhs: tidb_ast::Expr,
    all: bool,
    list: Vec<tidb_ast::Expr>,
) -> tidb_ast::Expr {
    use tidb_ast::{BinaryOp, Expr};
    let compare = |value: Expr| Expr::Binary(op, Box::new(lhs.clone()), Box::new(value));
    let mut values = list.into_iter();
    let Some(first) = values.next() else {
        return Expr::Int(i64::from(all).to_string());
    };
    let combine = if all {
        BinaryOp::LogicAnd
    } else {
        BinaryOp::LogicOr
    };
    values.fold(compare(first), |acc, value| {
        Expr::Binary(combine, Box::new(acc), Box::new(compare(value)))
    })
}

/// Evaluates an expression over the OUTER scope's columns for one outer row.
///
/// The semi-join folds keep their left operand in the outer scope rather than
/// binding it to a literal, so the comparison runs through the very same
/// expression evaluator the uncorrelated path uses.
fn eval_expr_on_row(
    expr: &tidb_ast::Expr,
    scope: &FromScope,
    values: &[Datum],
    ctx: &crate::StmtContext,
) -> Result<Datum, DriverError> {
    let types: Vec<FieldType> = scope.column_list().into_iter().map(|(_, ft)| ft).collect();
    let rewritten = rewrite_expr_resolved(expr, &ScopeResolver { scope })
        .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
    let chunk = row_chunk(values, &types)?;
    rewritten
        .eval(ctx, chunk.get_row(0))
        .map_err(|e| DriverError::Exec(ExecError::Eval(e)))
}

/// Finds the one correlated subquery in `expr`, replacing it with a reference
/// to the column an [`ApplyExec`] will append at `index`.
///
/// Go's rewriter does the same substitution: after building the Apply, the
/// subquery expression becomes the Apply schema's last column.
pub(crate) fn extract_correlated_subquery(
    expr: &tidb_ast::Expr,
    outer: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    index: usize,
    found: &mut Option<CorrelatedSubquery>,
    ctx: &crate::StmtContext,
) -> Result<tidb_ast::Expr, DriverError> {
    struct Extractor<'a> {
        outer: &'a FromScope,
        catalog: &'a Catalog,
        current_db: &'a str,
        ctx: &'a crate::StmtContext,
        index: usize,
        found: Option<CorrelatedSubquery>,
    }

    impl Visitor for Extractor<'_> {
        fn enter(&mut self, node: &mut dyn Any) -> bool {
            if self.found.is_some() {
                return true;
            }
            let Some(expr) = node.downcast_mut::<tidb_ast::Expr>() else {
                return false;
            };
            let (query, kind, operand_has_subquery) = match expr {
                tidb_ast::Expr::Subquery(query) => ((**query).clone(), SubqueryKind::Scalar, false),
                tidb_ast::Expr::Exists { subquery, not } => (
                    (**subquery).clone(),
                    SubqueryKind::Exists { not: *not },
                    false,
                ),
                tidb_ast::Expr::InSubquery {
                    expr: lhs,
                    subquery,
                    not,
                } => (
                    (**subquery).clone(),
                    SubqueryKind::In {
                        lhs: (**lhs).clone(),
                        not: *not,
                    },
                    expr_has_subquery(lhs),
                ),
                tidb_ast::Expr::CompareSubquery {
                    op,
                    left,
                    all,
                    subquery,
                } => (
                    (**subquery).clone(),
                    SubqueryKind::Compare {
                        op: *op,
                        lhs: (**left).clone(),
                        all: *all,
                    },
                    expr_has_subquery(left),
                ),
                _ => return false,
            };

            let mut columns = Vec::new();
            collect_correlated_columns_query(
                &query,
                self.outer,
                self.catalog,
                self.current_db,
                &mut columns,
                self.ctx,
            );
            // Never descend into a subquery body: names there belong to the
            // inner scope. An uncorrelated subquery is owned by the folding
            // pass, while a correlated one becomes this pass's one Apply.
            if columns.is_empty() {
                return true;
            }
            if operand_has_subquery {
                // Go's handleInSubquery/handleCompareSubquery rewrites the
                // left operand before building the right-hand subquery. Let
                // the visitor descend so this pass extracts that operand;
                // the caller's next pass then extracts this subquery against
                // the already-widened Apply scope.
                return false;
            }
            // Go `isPhysicalPlanCacheable`: a plan containing a
            // `PhysicalApply` is refused by the prepared plan cache, because
            // a per-outer-row executor cannot be reused across parameter
            // sets. This is the one place the driver decides an Apply IS the
            // plan, so it is the one place that can say so.
            self.ctx.report_planned_apply();
            self.found = Some(CorrelatedSubquery {
                query,
                kind,
                columns,
            });
            *expr = tidb_ast::Expr::Column(vec![format!("__apply_{}", self.index)]);
            true
        }

        fn leave(&mut self, _node: &mut dyn Any) -> bool {
            true
        }
    }

    if found.is_some() {
        return Ok(expr.clone());
    }
    let mut rewritten = expr.clone();
    let mut extractor = Extractor {
        outer,
        catalog,
        current_db,
        ctx,
        index,
        found: None,
    };
    rewritten.accept(&mut extractor);
    *found = extractor.found;
    Ok(rewritten)
}

/// The scope a subquery inside `select` sees as its OUTER scope: `select`'s
/// own `FROM` tables. An unresolvable `FROM` yields an empty scope, and the
/// error surfaces when the query itself is built.
pub(crate) fn select_outer_scope(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> FromScope {
    let empty = || FromScope::for_statement(ctx);
    match &select.from {
        None => empty(),
        Some(join) => {
            let mut trace = PlanTrace::planning();
            match build_join(
                join,
                catalog,
                current_db,
                ctx,
                Some(&mut trace),
                None,
                crate::driver::leaf_demand::FromDemand::none(),
                &tidb_planner::physical_property::PhysicalProperty::default(),
                None,
            ) {
                Ok((_, scope, _)) => scope,
                Err(_) => empty(),
            }
        }
    }
}

/// Whether any clause of `select` contains a subquery, so the fold pass runs
/// only when it has something to do. The pass folds each uncorrelated node
/// and leaves each correlated node intact for the Apply path.
pub(crate) fn select_has_subquery(select: &tidb_ast::SelectStmt) -> bool {
    let fields = select.fields.fields().iter().any(|field| match field {
        SelectField::Expr { expr, .. } => expr_has_subquery(expr),
        SelectField::Wildcard(_) => false,
    });
    fields
        || select.where_clause.as_ref().is_some_and(expr_has_subquery)
        || select.having.as_ref().is_some_and(expr_has_subquery)
        || select
            .order_by
            .iter()
            .any(|item| expr_has_subquery(&item.expr))
        || select
            .group_by
            .iter()
            .any(|item| expr_has_subquery(&item.expr))
}

/// Whether `expr` contains a subquery in a position the fold pass walks.
pub(crate) fn expr_has_subquery(expr: &tidb_ast::Expr) -> bool {
    use tidb_ast::Expr;
    match expr {
        Expr::Subquery(_)
        | Expr::Exists { .. }
        | Expr::InSubquery { .. }
        | Expr::CompareSubquery { .. } => true,
        Expr::Paren(inner) | Expr::Unary(_, inner) | Expr::Is { expr: inner, .. } => {
            expr_has_subquery(inner)
        }
        Expr::Binary(_, lhs, rhs) => expr_has_subquery(lhs) || expr_has_subquery(rhs),
        Expr::In { expr, list, .. } => {
            expr_has_subquery(expr) || list.iter().any(expr_has_subquery)
        }
        // The forms below must stay in step with `fold_subqueries`' own walk:
        // this predicate is the GATE that decides whether the fold pass runs
        // at all, so a form the fold could handle but this cannot is a
        // subquery that never gets folded and fails to plan instead.
        Expr::Case {
            value,
            when_clauses,
            else_clause,
        } => {
            value.as_deref().is_some_and(expr_has_subquery)
                || when_clauses.iter().any(|(condition, result)| {
                    expr_has_subquery(condition) || expr_has_subquery(result)
                })
                || else_clause.as_deref().is_some_and(expr_has_subquery)
        }
        Expr::Func { args, .. } | Expr::Aggregate { args, .. } => {
            args.iter().any(expr_has_subquery)
        }
        _ => false,
    }
}

/// Folds every subquery in `select`'s clauses, returning the rewritten copy.
pub(crate) fn fold_select_subqueries(
    select: &tidb_ast::SelectStmt,
    outer: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<tidb_ast::SelectStmt, DriverError> {
    let mut folded = select.clone();
    for field in folded.fields.fields_mut() {
        if let SelectField::Expr { expr, .. } = field {
            *expr = fold_subqueries(expr, outer, catalog, current_db, ctx)?;
        }
    }
    if let Some(where_clause) = &folded.where_clause {
        folded.where_clause = Some(fold_subqueries(
            where_clause,
            outer,
            catalog,
            current_db,
            ctx,
        )?);
    }
    if let Some(having) = &folded.having {
        folded.having = Some(fold_subqueries(having, outer, catalog, current_db, ctx)?);
    }
    for item in &mut folded.order_by {
        item.expr = fold_subqueries(&item.expr, outer, catalog, current_db, ctx)?;
    }
    for item in &mut folded.group_by {
        item.expr = fold_subqueries(&item.expr, outer, catalog, current_db, ctx)?;
    }
    Ok(folded)
}

pub(crate) struct PlannedSelectSubqueries {
    pub(crate) select: tidb_ast::SelectStmt,
    pub(crate) columns: Vec<crate::driver::from::PlanColumn>,
}

/// Plan-column IDs allocated while Go builds one logical SELECT block before
/// it rewrites the block's scalar subqueries. Rust builds executors in a
/// different order, so retaining this small receipt is necessary to preserve
/// Go's statement-wide EXPLAIN identities.
fn go_table_source_column_count(
    table: &tidb_ast::TableRef,
    catalog: &Catalog,
    current_db: &str,
) -> usize {
    let Ok((database, name)) = super::split_table_path(&table.name, current_db) else {
        return 0;
    };
    let Some(entry) = catalog.get_in(database, name) else {
        return 0;
    };
    match entry {
        TableEntry::Kv(table) => {
            let needs_extra_handle =
                table.pk_handle_offset().is_none() && table.common_handle_offsets().is_empty();
            table.logical_data_source_column_count() + usize::from(needs_extra_handle) + 1
        }
        // Go's virtual/system sources do not append TiKV's hidden handle and
        // commit-ts columns.
        _ => entry.column_list().len(),
    }
}

fn reserve_go_join_source_columns(
    node: &tidb_ast::JoinNode,
    catalog: &Catalog,
    current_db: &str,
    trace: &PlanTrace,
    query_sources: &mut Vec<GoLogicalQuerySourceColumns>,
) -> usize {
    match node {
        tidb_ast::JoinNode::Table(table) => {
            let Ok((database, name)) = super::split_table_path(&table.name, current_db) else {
                return 0;
            };
            let Some(entry) = catalog.get_in(database, name) else {
                return 0;
            };
            let TableEntry::View(view) = entry else {
                return go_table_source_column_count(table, catalog, current_db);
            };
            let Ok(tidb_ast::Stmt::Query(query)) = tidb_parser::parse(&view.select_sql) else {
                return entry.column_list().len();
            };
            let QueryStmt::Select(select) = &*query else {
                return entry.column_list().len();
            };
            let columns = build_go_logical_plan_columns(select, catalog, database, trace);
            query_sources.push(GoLogicalQuerySourceColumns {
                query: (*query).clone(),
                columns,
            });
            0
        }
        tidb_ast::JoinNode::Derived { subquery, .. } => {
            go_query_source_column_count(subquery, catalog, current_db)
        }
        tidb_ast::JoinNode::Join(join) => {
            reserve_go_join_source_columns(&join.left, catalog, current_db, trace, query_sources)
                + join.right.as_ref().map_or(0, |right| {
                    reserve_go_join_source_columns(right, catalog, current_db, trace, query_sources)
                })
        }
    }
}

fn go_join_source_column_count(
    node: &tidb_ast::JoinNode,
    catalog: &Catalog,
    current_db: &str,
) -> usize {
    match node {
        tidb_ast::JoinNode::Table(table) => {
            go_table_source_column_count(table, catalog, current_db)
        }
        tidb_ast::JoinNode::Derived { subquery, .. } => {
            go_query_source_column_count(subquery, catalog, current_db)
        }
        tidb_ast::JoinNode::Join(join) => {
            go_join_source_column_count(&join.left, catalog, current_db)
                + join.right.as_ref().map_or(0, |right| {
                    go_join_source_column_count(right, catalog, current_db)
                })
        }
    }
}

fn query_source_has_subquery(query: &tidb_ast::QueryStmt) -> bool {
    match query {
        QueryStmt::Select(select) => {
            select_has_subquery(select)
                || select.from.as_ref().is_some_and(|join| {
                    join_source_has_subquery(&join.left)
                        || join.right.as_ref().is_some_and(join_source_has_subquery)
                })
        }
        QueryStmt::SetOpr(set_opr) => set_opr.terms.iter().any(|term| match &term.body {
            tidb_ast::SetOprTermBody::Select(select) => {
                select_has_subquery(select)
                    || select.from.as_ref().is_some_and(|join| {
                        join_source_has_subquery(&join.left)
                            || join.right.as_ref().is_some_and(join_source_has_subquery)
                    })
            }
            tidb_ast::SetOprTermBody::Nested(nested) => {
                query_source_has_subquery(&QueryStmt::SetOpr(nested.clone()))
            }
        }),
    }
}

fn join_source_has_subquery(node: &tidb_ast::JoinNode) -> bool {
    match node {
        tidb_ast::JoinNode::Table(_) => false,
        tidb_ast::JoinNode::Derived { subquery, .. } => query_source_has_subquery(subquery),
        tidb_ast::JoinNode::Join(join) => {
            join_source_has_subquery(&join.left)
                || join.right.as_ref().is_some_and(join_source_has_subquery)
        }
    }
}

fn go_query_source_column_count(
    query: &tidb_ast::QueryStmt,
    catalog: &Catalog,
    current_db: &str,
) -> usize {
    match query {
        QueryStmt::Select(select) => select.from.as_ref().map_or(0, |join| {
            go_join_source_column_count(&join.left, catalog, current_db)
                + join.right.as_ref().map_or(0, |right| {
                    go_join_source_column_count(right, catalog, current_db)
                })
        }),
        QueryStmt::SetOpr(set_opr) => set_opr
            .terms
            .iter()
            .map(|term| match &term.body {
                tidb_ast::SetOprTermBody::Select(select) => {
                    select.from.as_ref().map_or(0, |join| {
                        go_join_source_column_count(&join.left, catalog, current_db)
                            + join.right.as_ref().map_or(0, |right| {
                                go_join_source_column_count(right, catalog, current_db)
                            })
                    })
                }
                tidb_ast::SetOprTermBody::Nested(nested) => nested
                    .terms
                    .iter()
                    .map(|term| match &term.body {
                        tidb_ast::SetOprTermBody::Select(select) => {
                            select.from.as_ref().map_or(0, |join| {
                                go_join_source_column_count(&join.left, catalog, current_db)
                                    + join.right.as_ref().map_or(0, |right| {
                                        go_join_source_column_count(right, catalog, current_db)
                                    })
                            })
                        }
                        tidb_ast::SetOprTermBody::Nested(_) => 0,
                    })
                    .sum(),
            })
            .sum(),
    }
}

fn collect_pre_resolved_subquery_sources(
    expr: &tidb_ast::Expr,
    catalog: &Catalog,
    current_db: &str,
) -> usize {
    use tidb_ast::Expr;
    match expr {
        Expr::Subquery(query)
        | Expr::Exists {
            subquery: query, ..
        } => go_query_source_column_count(query, catalog, current_db),
        Expr::InSubquery { expr, subquery, .. } => {
            collect_pre_resolved_subquery_sources(expr, catalog, current_db)
                + go_query_source_column_count(subquery, catalog, current_db)
        }
        Expr::CompareSubquery { left, subquery, .. } => {
            collect_pre_resolved_subquery_sources(left, catalog, current_db)
                + go_query_source_column_count(subquery, catalog, current_db)
        }
        Expr::Paren(inner) | Expr::Unary(_, inner) | Expr::Is { expr: inner, .. } => {
            collect_pre_resolved_subquery_sources(inner, catalog, current_db)
        }
        Expr::Binary(_, left, right) => {
            collect_pre_resolved_subquery_sources(left, catalog, current_db)
                + collect_pre_resolved_subquery_sources(right, catalog, current_db)
        }
        Expr::In { expr, list, .. } => {
            collect_pre_resolved_subquery_sources(expr, catalog, current_db)
                + list
                    .iter()
                    .map(|item| collect_pre_resolved_subquery_sources(item, catalog, current_db))
                    .sum::<usize>()
        }
        Expr::Case {
            value,
            when_clauses,
            else_clause,
        } => {
            value.as_deref().map_or(0, |value| {
                collect_pre_resolved_subquery_sources(value, catalog, current_db)
            }) + when_clauses
                .iter()
                .map(|(condition, result)| {
                    collect_pre_resolved_subquery_sources(condition, catalog, current_db)
                        + collect_pre_resolved_subquery_sources(result, catalog, current_db)
                })
                .sum::<usize>()
                + else_clause.as_deref().map_or(0, |value| {
                    collect_pre_resolved_subquery_sources(value, catalog, current_db)
                })
        }
        Expr::Func { args, .. } | Expr::Aggregate { args, .. } => args
            .iter()
            .map(|arg| collect_pre_resolved_subquery_sources(arg, catalog, current_db))
            .sum(),
        _ => 0,
    }
}

fn collect_unique_aggregates(expr: &tidb_ast::Expr, aggregates: &mut Vec<tidb_ast::Expr>) {
    use tidb_ast::Expr;
    match expr {
        Expr::Aggregate { .. } => {
            if !aggregates.contains(expr) {
                aggregates.push(expr.clone());
            }
        }
        // An aggregate in another query block owns that block's allocator
        // stage and is collected by its recursive `run_select_traced` call.
        Expr::Subquery(_)
        | Expr::Exists { .. }
        | Expr::InSubquery { .. }
        | Expr::CompareSubquery { .. } => {}
        Expr::Paren(inner) | Expr::Unary(_, inner) | Expr::Is { expr: inner, .. } => {
            collect_unique_aggregates(inner, aggregates);
        }
        Expr::Binary(_, left, right) => {
            collect_unique_aggregates(left, aggregates);
            collect_unique_aggregates(right, aggregates);
        }
        Expr::In { expr, list, .. } => {
            collect_unique_aggregates(expr, aggregates);
            for item in list {
                collect_unique_aggregates(item, aggregates);
            }
        }
        Expr::Case {
            value,
            when_clauses,
            else_clause,
        } => {
            if let Some(value) = value {
                collect_unique_aggregates(value, aggregates);
            }
            for (condition, result) in when_clauses {
                collect_unique_aggregates(condition, aggregates);
                collect_unique_aggregates(result, aggregates);
            }
            if let Some(value) = else_clause {
                collect_unique_aggregates(value, aggregates);
            }
        }
        Expr::Func { args, .. } => {
            for arg in args {
                collect_unique_aggregates(arg, aggregates);
            }
        }
        _ => {}
    }
}

fn select_unique_aggregates(select: &tidb_ast::SelectStmt) -> Vec<tidb_ast::Expr> {
    let mut aggregates = Vec::new();
    for field in select.fields.fields() {
        if let SelectField::Expr { expr, .. } = field {
            collect_unique_aggregates(expr, &mut aggregates);
        }
    }
    if let Some(having) = &select.having {
        collect_unique_aggregates(having, &mut aggregates);
    }
    for item in &select.order_by {
        collect_unique_aggregates(&item.expr, &mut aggregates);
    }
    aggregates
}

fn projection_allocates_column(expr: &tidb_ast::Expr) -> bool {
    !matches!(
        expr,
        tidb_ast::Expr::Column(_) | tidb_ast::Expr::Aggregate { .. }
    )
}

/// Mirrors the ID-producing part of Go `PlanBuilder.buildSelect` up through
/// the logical Projection. `resolveHavingAndOrderBy` builds subquery source
/// schemas once before the aggregation; the later expression rewrite builds
/// them again, which is why both stages must consume the same allocator.
fn build_go_logical_plan_columns(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    trace: &PlanTrace,
) -> GoLogicalPlanColumns {
    let mut query_sources = Vec::new();
    let source_columns = select.from.as_ref().map_or(0, |join| {
        reserve_go_join_source_columns(&join.left, catalog, current_db, trace, &mut query_sources)
            + join.right.as_ref().map_or(0, |right| {
                reserve_go_join_source_columns(
                    right,
                    catalog,
                    current_db,
                    trace,
                    &mut query_sources,
                )
            })
    });
    trace.reserve_plan_column_ids(source_columns);

    let pre_resolved_sources = select.having.as_ref().map_or(0, |expr| {
        collect_pre_resolved_subquery_sources(expr, catalog, current_db)
    }) + select
        .order_by
        .iter()
        .map(|item| collect_pre_resolved_subquery_sources(&item.expr, catalog, current_db))
        .sum::<usize>();
    trace.reserve_plan_column_ids(pre_resolved_sources);

    let finish_after_source = select.from.as_ref().is_some_and(|join| {
        join_source_has_subquery(&join.left)
            || join.right.as_ref().is_some_and(join_source_has_subquery)
    });
    let finish_after_subqueries = select.where_clause.as_ref().is_some_and(expr_has_subquery);
    let mut pending_aggregates = select_unique_aggregates(select);
    let aggregate_ids = if finish_after_source || finish_after_subqueries {
        Vec::new()
    } else {
        pending_aggregates
            .drain(..)
            .map(|aggregate| (aggregate, trace.alloc_plan_column_id()))
            .collect::<Vec<_>>()
    };

    let all_projection_ids_pending = finish_after_source || finish_after_subqueries;
    if !all_projection_ids_pending {
        for field in select.fields.fields() {
            if let SelectField::Expr { expr, .. } = field {
                if !expr_has_subquery(expr) && projection_allocates_column(expr) {
                    trace.alloc_plan_column_id();
                }
            }
        }
    }
    GoLogicalPlanColumns {
        aggregate_ids,
        pending_aggregates,
        all_projection_ids_pending,
        finish_after_source,
        query_sources,
    }
}

pub(crate) fn reserve_go_logical_plan_columns(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    trace: &PlanTrace,
) -> GoLogicalPlanColumns {
    let mut columns = trace
        .take_pre_reserved_query_source()
        .unwrap_or_else(|| build_go_logical_plan_columns(select, catalog, current_db, trace));
    trace.push_query_source_frame(std::mem::take(&mut columns.query_sources));
    columns
}

impl GoLogicalPlanColumns {
    /// Completes the logical/physical IDs Go allocates after scalar-subquery
    /// rewrite and records the retained Projection's exact column mapping.
    pub(crate) fn finish_after_subqueries(
        &mut self,
        select: &tidb_ast::SelectStmt,
        trace: &mut PlanTrace,
    ) {
        if self.finish_after_source {
            return;
        }
        self.finish(select, trace);
    }

    pub(crate) fn finish_after_source(
        mut self,
        select: &tidb_ast::SelectStmt,
        trace: &mut PlanTrace,
    ) {
        if self.finish_after_source {
            self.finish(select, trace);
        }
    }

    fn finish(&mut self, select: &tidb_ast::SelectStmt, trace: &mut PlanTrace) {
        self.aggregate_ids.extend(
            self.pending_aggregates
                .drain(..)
                .map(|aggregate| (aggregate, trace.alloc_plan_column_id())),
        );

        for field in select.fields.fields() {
            if let SelectField::Expr { expr, .. } = field {
                let pending = self.all_projection_ids_pending || expr_has_subquery(expr);
                if pending && projection_allocates_column(expr) {
                    trace.alloc_plan_column_id();
                }
            }
        }
        self.all_projection_ids_pending = false;

        let having_has_subquery = select.having.as_ref().is_some_and(expr_has_subquery);
        if !having_has_subquery || self.aggregate_ids.is_empty() {
            return;
        }

        // Go's physical HashAgg creates its restored output schema before
        // `InjectProjBelowAgg`; the retained final Projection is allocated
        // next, and only then are its child input columns allocated.
        trace.reserve_plan_column_ids(self.aggregate_ids.len());
        let mapping = select
            .fields
            .fields()
            .iter()
            .map(|field| match field {
                SelectField::Expr {
                    expr: aggregate @ tidb_ast::Expr::Aggregate { .. },
                    ..
                } => self
                    .aggregate_ids
                    .iter()
                    .find(|(candidate, _)| candidate == aggregate)
                    .map(|(_, input)| (*input, trace.alloc_plan_column_id())),
                _ => None,
            })
            .collect();
        trace.set_aggregation_projection_mapping(mapping);
    }
}

fn nested_query_source_physical_column_count(
    query: &tidb_ast::QueryStmt,
    catalog: &Catalog,
    current_db: &str,
) -> usize {
    let QueryStmt::Select(select) = query else {
        return 0;
    };
    let nested = select.from.as_ref().map_or(0, |join| {
        nested_join_source_physical_column_count(&join.left, catalog, current_db)
            + join.right.as_ref().map_or(0, |right| {
                nested_join_source_physical_column_count(right, catalog, current_db)
            })
    });
    let aggregates = select_unique_aggregates(select);
    if aggregates.is_empty() {
        return nested;
    }
    let max_min_only = aggregates.iter().all(|aggregate| {
        matches!(
            aggregate,
            tidb_ast::Expr::Aggregate { name, .. }
                if name.eq_ignore_ascii_case("max") || name.eq_ignore_ascii_case("min")
        )
    });
    if max_min_only || select.group_by.is_empty() {
        return nested;
    }
    let partial_outputs = aggregates
        .iter()
        .map(|aggregate| match aggregate {
            tidb_ast::Expr::Aggregate { name, .. } if name.eq_ignore_ascii_case("avg") => 2,
            _ => 1,
        })
        .sum::<usize>();
    nested + partial_outputs
}

fn nested_join_source_physical_column_count(
    node: &tidb_ast::JoinNode,
    catalog: &Catalog,
    current_db: &str,
) -> usize {
    match node {
        tidb_ast::JoinNode::Table(table) => {
            let Ok((database, name)) = super::split_table_path(&table.name, current_db) else {
                return 0;
            };
            let Some(TableEntry::View(view)) = catalog.get_in(database, name) else {
                return 0;
            };
            let Ok(tidb_ast::Stmt::Query(query)) = tidb_parser::parse(&view.select_sql) else {
                return 0;
            };
            nested_query_source_physical_column_count(&query, catalog, database)
        }
        tidb_ast::JoinNode::Derived { .. } => 0,
        tidb_ast::JoinNode::Join(join) => {
            nested_join_source_physical_column_count(&join.left, catalog, current_db)
                + join.right.as_ref().map_or(0, |right| {
                    nested_join_source_physical_column_count(right, catalog, current_db)
                })
        }
    }
}

fn scalar_subquery_physical_column_count(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
) -> usize {
    let nested = select.from.as_ref().map_or(0, |join| {
        nested_join_source_physical_column_count(&join.left, catalog, current_db)
            + join.right.as_ref().map_or(0, |right| {
                nested_join_source_physical_column_count(right, catalog, current_db)
            })
    });
    let aggregates = select_unique_aggregates(select);
    if aggregates.is_empty() {
        return nested;
    }
    let computed_arguments = aggregates
        .iter()
        .filter_map(|aggregate| match aggregate {
            tidb_ast::Expr::Aggregate { args, .. } => Some(args),
            _ => None,
        })
        .flatten()
        .filter(|arg| {
            !matches!(
                arg,
                tidb_ast::Expr::Column(_)
                    | tidb_ast::Expr::Int(_)
                    | tidb_ast::Expr::Decimal(_)
                    | tidb_ast::Expr::Float(_)
                    | tidb_ast::Expr::Hex(_)
                    | tidb_ast::Expr::Bit(_)
                    | tidb_ast::Expr::String(_)
                    | tidb_ast::Expr::RawString(_)
                    | tidb_ast::Expr::CharsetString { .. }
                    | tidb_ast::Expr::CharsetBinary { .. }
                    | tidb_ast::Expr::Null
                    | tidb_ast::Expr::Bool(_)
            )
        })
        .count();
    let single_base_table = select.from.as_ref().is_some_and(|join| {
        join.right.is_none() && matches!(join.left, tidb_ast::JoinNode::Table(_))
    });
    let max_min_only = aggregates.iter().all(|aggregate| {
        matches!(
            aggregate,
            tidb_ast::Expr::Aggregate { name, .. }
                if name.eq_ignore_ascii_case("max") || name.eq_ignore_ascii_case("min")
        )
    });
    if single_base_table && !max_min_only {
        let partial_outputs = aggregates
            .iter()
            .map(|aggregate| match aggregate {
                tidb_ast::Expr::Aggregate { name, .. } if name.eq_ignore_ascii_case("avg") => 2,
                _ => 1,
            })
            .sum::<usize>();
        nested + computed_arguments + 2 * partial_outputs
    } else if computed_arguments > 0 {
        nested + computed_arguments + aggregates.len()
    } else {
        nested
    }
}

/// Go's plain-EXPLAIN branch of `handleScalarSubquery` and
/// `handleExistSubquery`. Each uncorrelated child is optimized with the same
/// plan-only trace, retained as a separate `ScalarSubQuery` root, and replaced
/// by a typed non-row column. Unlike [`fold_select_subqueries`], this path does
/// not evaluate a child or turn its result into a literal.
pub(crate) fn plan_select_subqueries(
    select: &tidb_ast::SelectStmt,
    outer: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    trace: &mut PlanTrace,
) -> Result<PlannedSelectSubqueries, DriverError> {
    let mut planner = PlanOnlySubqueries {
        outer,
        catalog,
        current_db,
        ctx,
        trace,
        columns: Vec::new(),
    };
    let mut planned = select.clone();
    for field in planned.fields.fields_mut() {
        if let SelectField::Expr { expr, .. } = field {
            *expr = planner.plan_expr(expr)?;
        }
    }
    if let Some(where_clause) = &planned.where_clause {
        planned.where_clause = Some(planner.plan_expr(where_clause)?);
    }
    if let Some(having) = &planned.having {
        planned.having = Some(planner.plan_expr(having)?);
    }
    for item in &mut planned.order_by {
        item.expr = planner.plan_expr(&item.expr)?;
    }
    for item in &mut planned.group_by {
        item.expr = planner.plan_expr(&item.expr)?;
    }
    Ok(PlannedSelectSubqueries {
        select: planned,
        columns: planner.columns,
    })
}

struct PlanOnlySubqueries<'a> {
    outer: &'a FromScope,
    catalog: &'a Catalog,
    current_db: &'a str,
    ctx: &'a crate::StmtContext,
    trace: &'a mut PlanTrace,
    columns: Vec<crate::driver::from::PlanColumn>,
}

impl PlanOnlySubqueries<'_> {
    fn is_correlated(&self, query: &tidb_ast::QueryStmt) -> bool {
        let mut columns = Vec::new();
        collect_correlated_columns_query(
            query,
            self.outer,
            self.catalog,
            self.current_db,
            &mut columns,
            self.ctx,
        );
        !columns.is_empty()
    }

    fn plan_query(
        &mut self,
        query: &tidb_ast::QueryStmt,
    ) -> Result<Vec<(String, FieldType)>, DriverError> {
        let (columns, _) = match query {
            QueryStmt::Select(select) => run_select_traced(
                select,
                self.catalog,
                self.current_db,
                self.ctx,
                Some(self.trace),
                &tidb_planner::physical_property::PhysicalProperty::default(),
                false,
            )?,
            QueryStmt::SetOpr(set_opr) => run_set_opr_traced(
                set_opr,
                self.catalog,
                self.current_db,
                self.ctx,
                Some(self.trace),
            )?,
        };
        if let QueryStmt::Select(select) = query {
            self.trace
                .reserve_plan_column_ids(scalar_subquery_physical_column_count(
                    select,
                    self.catalog,
                    self.current_db,
                ));
        }
        Ok(columns)
    }

    fn register_scalar(
        &mut self,
        output: Vec<(String, FieldType)>,
        max_one_row: bool,
        values: Option<Vec<Datum>>,
    ) -> Result<tidb_ast::Expr, DriverError> {
        if output.is_empty() {
            return Err(DriverError::unsupported(
                "a scalar subquery must expose at least one column",
            ));
        }
        let ids = self.trace.scalar_subquery(output.len(), max_one_row);
        let mut expressions = Vec::with_capacity(output.len());
        for (index, (id, (_, field_type))) in ids.into_iter().zip(output).enumerate() {
            let name = format!("ScalarQueryCol#{id}");
            self.columns.push(crate::driver::from::PlanColumn {
                name: name.clone(),
                field_type,
                unique_id: -id,
                value: values.as_ref().and_then(|row| row.get(index)).cloned(),
            });
            expressions.push(tidb_ast::Expr::Column(vec![
                crate::driver::from::SCALAR_QUERY_SCOPE.to_owned(),
                name,
            ]));
        }
        Ok(if expressions.len() == 1 {
            expressions.pop().expect("one scalar expression")
        } else {
            tidb_ast::Expr::Row(expressions)
        })
    }

    fn plan_expr(&mut self, expr: &tidb_ast::Expr) -> Result<tidb_ast::Expr, DriverError> {
        use tidb_ast::Expr;
        match expr {
            Expr::Subquery(query) if !self.is_correlated(query) => {
                let output = self.plan_query(query)?;
                let values = match run_subquery(query, self.catalog, self.current_db, self.ctx)?
                    .as_slice()
                {
                    [] => vec![Datum::Null; output.len()],
                    [row] => row.clone(),
                    _ => return Err(DriverError::SubqueryReturnsMoreThanOneRow),
                };
                self.register_scalar(output, true, Some(values))
            }
            Expr::Exists { subquery, not } if !self.is_correlated(subquery) => {
                let mut output = self.plan_query(subquery)?;
                output.truncate(1);
                let scalar = self.register_scalar(output, false, None)?;
                Ok(if *not {
                    Expr::Unary(tidb_ast::UnaryOp::NotKeyword, Box::new(scalar))
                } else {
                    scalar
                })
            }
            // Go lowers IN/ANY/ALL into joins or Apply nodes in the enclosing
            // logical plan. They must remain visible to that path rather than
            // becoming independent ScalarSubQuery roots.
            Expr::InSubquery {
                expr,
                subquery,
                not,
            } => Ok(Expr::InSubquery {
                expr: Box::new(self.plan_expr(expr)?),
                subquery: subquery.clone(),
                not: *not,
            }),
            Expr::CompareSubquery {
                op,
                left,
                all,
                subquery,
            } => Ok(Expr::CompareSubquery {
                op: *op,
                left: Box::new(self.plan_expr(left)?),
                all: *all,
                subquery: subquery.clone(),
            }),
            Expr::Subquery(_) | Expr::Exists { .. } => Ok(expr.clone()),
            Expr::Case {
                value,
                when_clauses,
                else_clause,
            } => Ok(Expr::Case {
                value: value
                    .as_deref()
                    .map(|value| self.plan_expr(value).map(Box::new))
                    .transpose()?,
                when_clauses: when_clauses
                    .iter()
                    .map(|(condition, result)| {
                        Ok((self.plan_expr(condition)?, self.plan_expr(result)?))
                    })
                    .collect::<Result<_, DriverError>>()?,
                else_clause: else_clause
                    .as_deref()
                    .map(|value| self.plan_expr(value).map(Box::new))
                    .transpose()?,
            }),
            Expr::Func {
                name,
                args,
                origin_position,
            } => Ok(Expr::Func {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|arg| self.plan_expr(arg))
                    .collect::<Result<_, _>>()?,
                origin_position: *origin_position,
            }),
            Expr::Aggregate {
                name,
                distinct,
                args,
            } => Ok(Expr::Aggregate {
                name: name.clone(),
                distinct: *distinct,
                args: args
                    .iter()
                    .map(|arg| self.plan_expr(arg))
                    .collect::<Result<_, _>>()?,
            }),
            Expr::Paren(inner) => Ok(Expr::Paren(Box::new(self.plan_expr(inner)?))),
            Expr::Unary(op, inner) => Ok(Expr::Unary(*op, Box::new(self.plan_expr(inner)?))),
            Expr::Binary(op, lhs, rhs) => Ok(Expr::Binary(
                *op,
                Box::new(self.plan_expr(lhs)?),
                Box::new(self.plan_expr(rhs)?),
            )),
            Expr::Is { expr, target, not } => Ok(Expr::Is {
                expr: Box::new(self.plan_expr(expr)?),
                target: *target,
                not: *not,
            }),
            Expr::In { expr, list, not } => Ok(Expr::In {
                expr: Box::new(self.plan_expr(expr)?),
                list: list
                    .iter()
                    .map(|item| self.plan_expr(item))
                    .collect::<Result<_, _>>()?,
                not: *not,
            }),
            other => Ok(other.clone()),
        }
    }
}

/// Replaces every uncorrelated subquery in `expr` with the value it produces.
///
/// This is Go's `handleScalarSubquery` path for a subquery with no correlated
/// columns: the subquery is planned and run on the spot
/// (`EvalSubqueryFirstRow`) and its result folded into a `Constant`, so the
/// outer statement plans against ordinary literals. Go's `buildMaxOneRow`
/// wrapper is the "more than one row" check below; a subquery producing no
/// rows yields NULL.
///
/// `EXISTS` folds to 1 or 0, `x IN (subquery)` folds to `x IN (values)` and
/// `x <op> ANY|ALL (subquery)` to the OR/AND chain of comparisons, all of
/// which evaluate identically for an uncorrelated subquery -- including the
/// NULL rules, since the folded list is compared by the same code.
///
/// DEFERRED (documented): CORRELATED subqueries, which Go turns into an Apply
/// operator rather than folding, and which this leaves for the Apply path
/// rather than silently evaluating the inner query against the wrong row; and
/// row constructors (a subquery selecting several columns).
pub(crate) fn fold_subqueries(
    expr: &tidb_ast::Expr,
    outer: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<tidb_ast::Expr, DriverError> {
    use tidb_ast::Expr;
    // A subquery reading the OUTER query's columns has no single value to fold
    // to: it is the Apply path's job, one run per outer row.
    let correlated_right = |query: &tidb_ast::QueryStmt| {
        let mut columns = Vec::new();
        collect_correlated_columns_query(query, outer, catalog, current_db, &mut columns, ctx);
        !columns.is_empty()
    };
    match expr {
        Expr::Subquery(query)
        | Expr::Exists {
            subquery: query, ..
        } if correlated_right(query) => {
            return Ok(expr.clone());
        }
        Expr::InSubquery {
            expr: lhs,
            subquery,
            not,
        } if correlated_right(subquery) => {
            return Ok(Expr::InSubquery {
                expr: Box::new(fold_subqueries(lhs, outer, catalog, current_db, ctx)?),
                subquery: subquery.clone(),
                not: *not,
            });
        }
        Expr::CompareSubquery {
            op,
            left,
            all,
            subquery,
        } if correlated_right(subquery) => {
            return Ok(Expr::CompareSubquery {
                op: *op,
                left: Box::new(fold_subqueries(left, outer, catalog, current_db, ctx)?),
                all: *all,
                subquery: subquery.clone(),
            });
        }
        _ => {}
    }
    Ok(match expr {
        Expr::Subquery(query) => {
            let rows = run_subquery(query, catalog, current_db, ctx)?;
            match rows.len() {
                // Go: a scalar subquery with no rows is NULL.
                0 => Expr::Null,
                1 => {
                    let row = &rows[0];
                    let [value] = row.as_slice() else {
                        return Err(DriverError::unsupported(
                            "a scalar subquery selecting several columns is not supported yet",
                        ));
                    };
                    datum_to_literal(value)?
                }
                // Go's buildMaxOneRow raises ER_SUBQUERY_NO_1_ROW here.
                _ => return Err(DriverError::SubqueryReturnsMoreThanOneRow),
            }
        }
        Expr::Exists { subquery, not } => {
            let rows = run_subquery(subquery, catalog, current_db, ctx)?;
            let exists = !rows.is_empty();
            Expr::Int(i64::from(exists != *not).to_string())
        }
        Expr::InSubquery {
            expr,
            subquery,
            not,
        } => {
            let rows = run_subquery(subquery, catalog, current_db, ctx)?;
            let list = subquery_value_list(&rows, true)?;
            in_list_expr(
                fold_subqueries(expr, outer, catalog, current_db, ctx)?,
                list,
                *not,
            )
        }
        Expr::CompareSubquery {
            op,
            left,
            all,
            subquery,
        } => {
            let rows = run_subquery(subquery, catalog, current_db, ctx)?;
            let list = subquery_value_list(&rows, false)?;
            any_all_expr(
                *op,
                fold_subqueries(left, outer, catalog, current_db, ctx)?,
                *all,
                list,
            )
        }
        // Walk the child-bearing forms. A form missing from this list carries
        // its subquery past the fold and into the rewriter, which knows no
        // subqueries at all: the statement then FAILS to plan (never answers
        // wrongly), which is what makes adding a form here purely a matter of
        // reach rather than of correctness.
        Expr::Case {
            value,
            when_clauses,
            else_clause,
        } => Expr::Case {
            value: match value {
                Some(value) => Some(Box::new(fold_subqueries(
                    value, outer, catalog, current_db, ctx,
                )?)),
                None => None,
            },
            when_clauses: when_clauses
                .iter()
                .map(|(condition, result)| {
                    Ok((
                        fold_subqueries(condition, outer, catalog, current_db, ctx)?,
                        fold_subqueries(result, outer, catalog, current_db, ctx)?,
                    ))
                })
                .collect::<Result<_, DriverError>>()?,
            else_clause: match else_clause {
                Some(else_clause) => Some(Box::new(fold_subqueries(
                    else_clause,
                    outer,
                    catalog,
                    current_db,
                    ctx,
                )?)),
                None => None,
            },
        },
        Expr::Func {
            name,
            args,
            origin_position,
        } => Expr::Func {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| fold_subqueries(arg, outer, catalog, current_db, ctx))
                .collect::<Result<_, _>>()?,
            origin_position: *origin_position,
        },
        // An aggregate's own argument: an UNCORRELATED subquery in it is a
        // constant and folds here, which is the only reason `SUM((SELECT MAX(id)
        // FROM d))` can run at all -- a CORRELATED one has to run once per
        // SOURCE row, below the aggregation, and `driver::agg_build` refuses it
        // by name after this fold has had its chance.
        Expr::Aggregate {
            name,
            distinct,
            args,
        } => Expr::Aggregate {
            name: name.clone(),
            distinct: *distinct,
            args: args
                .iter()
                .map(|arg| fold_subqueries(arg, outer, catalog, current_db, ctx))
                .collect::<Result<_, _>>()?,
        },
        Expr::Paren(inner) => Expr::Paren(Box::new(fold_subqueries(
            inner, outer, catalog, current_db, ctx,
        )?)),
        Expr::Unary(op, inner) => Expr::Unary(
            *op,
            Box::new(fold_subqueries(inner, outer, catalog, current_db, ctx)?),
        ),
        Expr::Binary(op, lhs, rhs) => Expr::Binary(
            *op,
            Box::new(fold_subqueries(lhs, outer, catalog, current_db, ctx)?),
            Box::new(fold_subqueries(rhs, outer, catalog, current_db, ctx)?),
        ),
        Expr::Is { expr, target, not } => Expr::Is {
            expr: Box::new(fold_subqueries(expr, outer, catalog, current_db, ctx)?),
            target: *target,
            not: *not,
        },
        Expr::In { expr, list, not } => Expr::In {
            expr: Box::new(fold_subqueries(expr, outer, catalog, current_db, ctx)?),
            list: list
                .iter()
                .map(|item| fold_subqueries(item, outer, catalog, current_db, ctx))
                .collect::<Result<_, _>>()?,
            not: *not,
        },
        other => other.clone(),
    })
}

/// Runs an uncorrelated subquery against the catalog.
///
/// A correlated subquery references a column of the OUTER query, which this
/// resolver cannot see -- so it fails to resolve here and the error surfaces
/// rather than the subquery being evaluated against the wrong scope.
fn run_subquery(
    query: &tidb_ast::QueryStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Vec<Vec<Datum>>, DriverError> {
    run_query_stmt(query, catalog, current_db, ctx).map(|(_, rows)| rows)
}

/// Where one select field of an aggregate query reads its value from.
pub(crate) enum OutputSlot {
    /// An aggregation output column, by index.
    Agg(usize),
    /// An expression over the aggregation's (+ Apply's) output columns, by
    /// index into `post_agg_exprs` -- a select field that CONTAINS a
    /// correlated subquery alongside aggregates/columns, e.g.
    /// `SUM(v) + (SELECT ...)`.
    Expr(usize),
    /// The column the n-th window call appends above the aggregation.
    Window(usize),
}

/// Extracts the one correlated subquery in a post-aggregation expression (a
/// select field, `HAVING`, or an `ORDER BY` item), hoists any aggregate calls
/// left in the remainder into `agg_funcs`/`names`/`types`, and returns the
/// resulting expression: aggregates and grouped columns become output column
/// references (Go's `havingWindowAndOrderbyExprResolver`), and the subquery
/// becomes a `__apply_N` placeholder column reference that the caller's
/// Apply (built once every correlated subquery in the statement is known)
/// makes real. `EXISTS`, `IN` and `ANY`/`ALL` ride the same placeholder,
/// because the Apply appends whatever [`run_correlated_subquery`] folds.
///
/// Returns `(expr, true)` when a correlated subquery was found and hoisted,
/// `(expr, false)` otherwise (uncorrelated, or no subquery at all).
#[allow(clippy::too_many_arguments)]
pub(crate) fn extract_and_hoist_subquery(
    expr: &tidb_ast::Expr,
    outer: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    applies: &mut Vec<(CorrelatedSubquery, String, FieldType)>,
    agg_funcs: &mut Vec<AggFunc>,
    names: &mut Vec<String>,
    types: &mut Vec<FieldType>,
    grouping_specs: &mut Vec<GroupingSpec>,
    group_by_exprs: &[String],
    resolver: &ScopeResolver<'_>,
    ctx: &crate::StmtContext,
) -> Result<(tidb_ast::Expr, bool), DriverError> {
    // No subquery anywhere in the expression, so there is nothing to hoist
    // out of the way of a per-group Apply: the caller decides how (or
    // whether) to run the aggregate hoist itself, exactly as it did before
    // this function existed.
    if !expr_has_subquery(expr) {
        return Ok((expr.clone(), false));
    }
    let mut rewritten = expr.clone();
    let mut found_any = false;
    while expr_has_subquery(&rewritten) {
        let index = applies.len();
        let mut found = None;
        rewritten = extract_correlated_subquery(
            &rewritten, outer, catalog, current_db, index, &mut found, ctx,
        )?;
        let Some(correlated) = found else {
            // Uncorrelated, or a shape the extraction does not own: leave it
            // to the fold pass or its existing named refusal.
            break;
        };
        let value_type = if matches!(correlated.kind, SubqueryKind::Scalar) {
            subquery_result_type(&correlated, outer, catalog, current_db, ctx)
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong))
        } else {
            FieldType::new(FieldTypeCode::LongLong)
        };
        applies.push((correlated, format!("__apply_{index}"), value_type));
        found_any = true;
    }
    if !found_any {
        return Ok((rewritten, false));
    }
    let hoisted = substitute_aggregates(
        &rewritten,
        agg_funcs,
        names,
        types,
        grouping_specs,
        group_by_exprs,
        resolver,
        ctx.div_precision_increment(),
    )?;
    Ok((hoisted, true))
}
