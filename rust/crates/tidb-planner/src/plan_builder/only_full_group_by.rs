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

//! `sql_mode=ONLY_FULL_GROUP_BY`: a grouped query may only report values a
//! group actually has.
//!
//! Go sources, by symbol:
//!
//! | Rust | Go `logical_plan_builder.go` |
//! | --- | --- |
//! | [`PlanBuilder::check_only_full_group_by`] | `checkOnlyFullGroupBy` (:3731) |
//! | [`PlanBuilder::check_only_full_group_by_with_group_clause`] | `checkOnlyFullGroupByWithGroupClause` (:3781) |
//! | [`PlanBuilder::check_only_full_group_by_without_group_clause`] | `checkOnlyFullGroupByWithOutGroupClause` (:3870) |
//! | [`PlanBuilder::check_col_func_depend`] | `checkColFuncDepend` (:3574) |
//! | [`build_where_func_depend`] | `buildWhereFuncDepend` (:3521) |
//! | [`build_join_func_depend`] | `buildJoinFuncDepend` (:3538) |
//! | [`extract_single_value_col_names_from_where`] | `extractSingeValueColNamesFromWhere` (:3748) |
//! | [`inner_from_parentheses_and_unary_plus`] | `getInnerFromParenthesesAndUnaryPlus` |
//! | [`PlanBuilder::check_order_by_in_distinct_ast`] | `checkOrderByInDistinct`'s AST half (:2445) |
//!
//! # Harvest
//!
//! `tidb-executor`'s `driver/only_full_group_by.rs` is the whole rule already
//! transcreated and validated against captured TiDB behaviour, and the bodies
//! below are it. What CHANGED is only the scope binding: the driver keys a
//! column on an offset into its own `FromScope`, and here the same offset is an
//! index into the plan's [`FieldName`] slice — which is precisely what Go's
//! `expression.FindFieldName(p.OutputNames(), ...)` returns. Every structural
//! decision (what is exempt, what a bare column is, the clause labels, the
//! error selection order) is the driver's, unchanged.
//!
//! The one place the two diverge is the JUSTIFICATION test. The driver reaches
//! for `tidb-funcdep`'s strict closure, which is Go's NEW checker
//! (`@@tidb_enable_new_only_full_group_by_check`, default OFF). Go's DEFAULT
//! checker — the one `buildSelect` (`:4370`) actually calls, gated on
//! `!OptimizerEnableNewOnlyFullGroupByCheck` — is `checkColFuncDepend`, the
//! "some unique index is wholly determined" test, and THAT is what is ported
//! here, against [`super::catalog::SourceIndex`]. The two agree on every case
//! a candidate key covers and differ only on the generated-column and
//! promoted-nullable-key relaxations, which are the new checker's alone.
//!
//! # Narrowings, by exact blocking Go symbol
//!
//! * `b.tblInfoFromCol(x.Left, lCol)` (`:3554`), which `buildJoinFuncDepend`
//!   uses to decide which side of an `ON` equality is the left one. It calls
//!   `ExtractTableList` over the resolve context, an explicitly dropped
//!   narrowing of [`super`]. [`build_join_func_depend`] instead keeps the
//!   equality in BOTH directions for an inner join and, for an outer join,
//!   uses the join node's own left/right subtree membership of the written
//!   table qualifier. An unqualified column on an outer join contributes
//!   nothing rather than the wrong direction.
//! * `ErrExprLoc`'s `ORDER BY` half over `sel.OrderBy.Items` when the item is a
//!   `*ast.PositionExpr` — a positional ORDER BY names a select field that was
//!   already checked in its own right, which is why
//!   [`names_a_select_field`] and the `Expr::Int` arm skip it.
//! * `AuxiliaryColInAgg` (`:3355`). Go marks the columns
//!   `resolveCorrelatedAggregates` appends so this checker ignores them; here
//!   the checker runs over the ORIGINAL select list, BEFORE any auxiliary
//!   field is appended (Go's `:4370` call site is likewise before
//!   `resolveHavingAndOrderBy`), so there is nothing to mark.

use std::collections::{HashMap, HashSet};

use tidb_ast::{BinaryOp, Expr, Join, JoinNode, JoinType, SelectField, SelectStmt, UnaryOp};
use tidb_datatype::{FieldName, FieldTypeFlags};

use super::catalog::TableSource;
use super::{find_field_name, PlanBuilder};
use crate::plan_base::PlanError;
use tidb_expr::Columns;

/// The clause an offending expression was written in, as Go names it in the
/// error and as it decides which error is reported.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Clause {
    Select,
    OrderBy,
}

impl Clause {
    const fn label(self) -> &'static str {
        match self {
            Self::Select => "SELECT list",
            Self::OrderBy => "ORDER BY",
        }
    }
}

/// Go `ErrExprLoc`: an expression that reports a column the grouping does not
/// justify.
struct Offender {
    /// The column's index into the plan's output names.
    offset: usize,
    /// The column's bare written name, which the no-`GROUP BY` error quotes.
    name: String,
    /// The offending expression's 1-based position in its clause.
    position: usize,
    clause: Clause,
}

// ***** the errors, by Go symbol and MySQL code *****

/// Go `plannererrors.ErrFieldNotInGroupBy` (1055).
fn err_field_not_in_group_by(position: usize, clause: Clause, column: &str) -> PlanError {
    PlanError::internal(format!(
        "Expression #{position} of {} is not in GROUP BY clause and contains nonaggregated \
         column '{column}' which is not functionally dependent on columns in GROUP BY clause; \
         this is incompatible with sql_mode=only_full_group_by",
        clause.label()
    ))
}

/// Go `plannererrors.ErrMixOfGroupFuncAndFields` (8123).
fn err_field_not_in_aggregated_query(position: usize, column: &str) -> PlanError {
    PlanError::internal(format!(
        "In aggregated query without GROUP BY, expression #{position} of SELECT list contains \
         nonaggregated column '{column}'; this is incompatible with sql_mode=only_full_group_by"
    ))
}

/// Go `plannererrors.ErrAggregateOrderNonAggQuery` (3029).
fn err_aggregate_order_non_agg_query(position: usize) -> PlanError {
    PlanError::internal(format!(
        "Expression #{position} of ORDER BY contains aggregate function and applies to the \
         result of a non-aggregated query"
    ))
}

/// Go `plannererrors.ErrFieldInOrderNotSelect` (3065).
fn err_field_in_order_not_select(position: usize, column: &str) -> PlanError {
    PlanError::internal(format!(
        "Expression #{position} of ORDER BY clause is not in SELECT list, references column \
         '{column}' which is not in SELECT list; this is incompatible with DISTINCT"
    ))
}

/// Go `plannererrors.ErrAggregateInOrderNotSelect` (3066).
fn err_aggregate_in_order_not_select(position: usize) -> PlanError {
    PlanError::internal(format!(
        "Expression #{position} of ORDER BY clause is not in SELECT list, contains aggregate \
         function; this is incompatible with DISTINCT"
    ))
}

impl<S: TableSource, C: Columns> PlanBuilder<'_, S, C> {
    /// Go `checkOnlyFullGroupBy(p, sel)` (`logical_plan_builder.go:3731`), over
    /// the plan's output names.
    ///
    /// `buildSelect` gates this call on `SQLMode.HasOnlyFullGroupBy()` and
    /// `sel.From != nil`; both are tested here so the body is safe to call
    /// unconditionally.
    ///
    /// # Errors
    ///
    /// 1055, 8123 or 3029, exactly as Go selects between them.
    pub fn check_only_full_group_by(
        &self,
        select: &SelectStmt,
        group_by: &[Expr],
        names: &[FieldName],
    ) -> Result<(), PlanError> {
        if !self.only_full_group_by || select.from.is_none() {
            return Ok(());
        }
        if group_by.is_empty() {
            self.check_only_full_group_by_without_group_clause(select, names)
        } else {
            self.check_only_full_group_by_with_group_clause(select, group_by, names)
        }
    }

    /// Go `checkOnlyFullGroupByWithGroupClause(p, sel)`
    /// (`logical_plan_builder.go:3781`).
    ///
    /// # Errors
    ///
    /// 1055 for the first select-list or ORDER BY expression the grouping does
    /// not justify.
    pub fn check_only_full_group_by_with_group_clause(
        &self,
        select: &SelectStmt,
        group_by: &[Expr],
        names: &[FieldName],
    ) -> Result<(), PlanError> {
        // `:3783` the group-by items that ARE a column pin that column; the
        // rest stand as whole expressions.
        //
        // `group_by` is the RESOLVED item list, not the written one: Go runs
        // this check after `resolveGbyExprs` (`:4360` then `:4370`), so
        // `GROUP BY 1` and `GROUP BY x` over `SELECT dept AS x` both pin
        // `dept` and the select list is justified.
        let mut pinned: HashSet<usize> = HashSet::new();
        let mut group_exprs: Vec<&Expr> = Vec::new();
        for item in group_by {
            let expr = inner_from_parentheses_and_unary_plus(item);
            match column_offset(expr, names) {
                Some(offset) => {
                    pinned.insert(offset);
                }
                None => group_exprs.push(expr),
            }
        }
        // `:3793` "MySQL permits a nonaggregate column not named in a GROUP BY
        // clause ... provided that this column is limited to a single value."
        if let Some(where_clause) = &select.where_clause {
            for column in extract_single_value_col_names_from_where(where_clause) {
                pinned.extend(column_offset(column, names));
            }
        }

        let mut offenders = Vec::new();
        for (index, field) in select.fields.fields().iter().enumerate() {
            if let SelectField::Expr { expr, .. } = field {
                collect_offenders(
                    expr,
                    index + 1,
                    Clause::Select,
                    &group_exprs,
                    &pinned,
                    names,
                    &mut offenders,
                );
            }
        }
        // `:3820` ORDER BY. An item that merely names a select field was
        // checked as that field.
        for (index, item) in select.order_by.iter().enumerate() {
            if names_a_select_field(&item.expr, select.fields.fields()) {
                continue;
            }
            collect_offenders(
                &item.expr,
                index + 1,
                Clause::OrderBy,
                &group_exprs,
                &pinned,
                names,
                &mut offenders,
            );
        }
        if offenders.is_empty() {
            return Ok(());
        }

        // `:3843` the functional-dependency relaxation. Only built when
        // something actually needs justifying, as Go does.
        let where_depend = select
            .where_clause
            .as_ref()
            .map(|where_clause| build_where_func_depend(where_clause, names))
            .unwrap_or_default();
        let join_depend = select
            .from
            .as_ref()
            .map(|from| build_join_func_depend(from, names))
            .unwrap_or_default();
        for offender in offenders {
            if self.check_col_func_depend(
                offender.offset,
                names,
                &pinned,
                &where_depend,
                &join_depend,
            ) {
                continue;
            }
            return Err(err_field_not_in_group_by(
                offender.position,
                offender.clause,
                &qualified_name(offender.offset, names),
            ));
        }
        Ok(())
    }

    /// Go `checkOnlyFullGroupByWithOutGroupClause(p, sel)`
    /// (`logical_plan_builder.go:3870`): with no `GROUP BY`, an aggregate
    /// anywhere makes the whole query aggregated, and then no bare column may
    /// be reported at all.
    ///
    /// # Errors
    ///
    /// 3029 when an ORDER BY aggregate applies to a non-aggregated select list
    /// — which Go reports BEFORE 8123 and regardless of whether the select
    /// list aggregates — else 8123.
    pub fn check_only_full_group_by_without_group_clause(
        &self,
        select: &SelectStmt,
        names: &[FieldName],
    ) -> Result<(), PlanError> {
        let no_pins = HashSet::new();
        let mut offenders = Vec::new();
        let mut has_aggregate = false;
        for (index, field) in select.fields.fields().iter().enumerate() {
            if let SelectField::Expr { expr, .. } = field {
                has_aggregate |= aggregates_anywhere(expr);
                collect_offenders(
                    expr,
                    index + 1,
                    Clause::Select,
                    &[],
                    &no_pins,
                    names,
                    &mut offenders,
                );
            }
        }
        if !offenders.is_empty() {
            if let Some(index) = select
                .order_by
                .iter()
                .position(|item| aggregates_anywhere(&item.expr))
            {
                return Err(err_aggregate_order_non_agg_query(index + 1));
            }
        }
        // A query with no aggregate and no `GROUP BY` is not a grouped query
        // at all, so nothing needs justifying.
        if !has_aggregate {
            return Ok(());
        }
        match offenders.first() {
            Some(offender) => Err(err_field_not_in_aggregated_query(
                offender.position,
                &offender.name,
            )),
            None => Ok(()),
        }
    }

    /// Go `checkColFuncDepend(p, name, tblInfo, gbyOrSingleValueColNames,
    /// whereDependNames, joinDependNames)` (`logical_plan_builder.go:3574`):
    /// whether SOME unique index of the column's table is wholly determined,
    /// which makes every other column of that table determined too.
    ///
    /// A determined index column is one that is pinned outright, or that an
    /// equality in `WHERE` / a join `ON` ties to a pinned column. Every column
    /// of the index must additionally be `NOT NULL`, because a NULL-bearing
    /// unique index is not a key.
    #[must_use]
    pub fn check_col_func_depend(
        &self,
        offset: usize,
        names: &[FieldName],
        pinned: &HashSet<usize>,
        where_depend: &HashMap<usize, usize>,
        join_depend: &HashMap<usize, usize>,
    ) -> bool {
        let Some(name) = names.get(offset) else {
            return false;
        };
        let Some(table) = self.source.find_table(
            &name.names.database.original,
            &name.names.original_table.original,
        ) else {
            // Go reaches `tblInfoFromCol`'s nil arm and reports the column;
            // a derived table or a CTE has no `TableInfo` and so no index.
            return false;
        };
        // The column must be justified through a key OF ITS OWN TABLE, which
        // is what the qualifier on each probe below enforces.
        let determined = |column_name: &str| {
            let path = vec![
                name.names.table.original.clone(),
                column_name.to_ascii_lowercase(),
            ];
            let Some(index) = find_field_name(names, &path) else {
                return false;
            };
            if pinned.contains(&index) {
                return true;
            }
            [where_depend, join_depend]
                .into_iter()
                .any(|map| map.get(&index).is_some_and(|other| pinned.contains(other)))
        };

        for index in &table.indexes {
            if !index.unique {
                continue;
            }
            let whole_index_determined = index.columns.iter().all(|index_column| {
                table.column_at(index_column.offset).is_some_and(|column| {
                    column.ret_type.flags() & FieldTypeFlags::NOT_NULL != 0
                        && determined(&column.name)
                })
            });
            if whole_index_determined {
                return true;
            }
        }
        // `:3623` the PRIMARY KEY, which `pk_is_handle` carries as a column
        // flag rather than as an index.
        let mut has_primary_field = false;
        let mut primary_determined = true;
        for column in &table.columns {
            if !column.is_primary_key {
                continue;
            }
            has_primary_field = true;
            if !determined(&column.name) {
                primary_determined = false;
                break;
            }
        }
        has_primary_field && primary_determined
    }

    /// Go `checkOrderByInDistinct(byItem, idx, expr, p, originalExprs, length)`
    /// (`logical_plan_builder.go:2445`), on the AST rather than on the built
    /// expressions.
    ///
    /// Go compares the BUILT `expression.Expression` against `projExprs` and
    /// against the projection's schema columns. Doing so here would need the
    /// ORDER BY item built against the post-projection plan, which is exactly
    /// what [`PlanBuilder::build_sort`] does one step later — so this runs on
    /// the written AST instead, which is `tidb-executor`'s
    /// `driver/only_full_group_by.rs:403` `check_order_by_in_distinct` and
    /// which that file's header records as agreeing with every captured case.
    /// The one thing lost is an ORDER BY item that is a DIFFERENT expression
    /// with the same built form; that is the permissive side.
    ///
    /// # Errors
    ///
    /// 3065 for a column the select list does not report, 3066 for an
    /// aggregate it does not report.
    pub fn check_order_by_in_distinct_ast(
        &self,
        select: &SelectStmt,
        names: &[FieldName],
    ) -> Result<(), PlanError> {
        if !self.only_full_group_by || !select.distinct || select.order_by.is_empty() {
            return Ok(());
        }
        let fields = select.fields.fields();
        // A wildcard's expansion is not written in the select list, so a field
        // list containing one cannot be compared item by item.
        if fields
            .iter()
            .any(|field| matches!(field, SelectField::Wildcard(_)))
        {
            return Ok(());
        }

        let mut reported_offsets: HashSet<usize> = HashSet::new();
        let mut reported_exprs: Vec<&Expr> = Vec::new();
        let mut reported_names: Vec<&str> = Vec::new();
        for field in fields {
            let SelectField::Expr { expr, alias } = field else {
                continue;
            };
            reported_offsets.extend(column_offset(expr, names));
            reported_exprs.push(inner_from_parentheses_and_unary_plus(expr));
            match alias {
                Some(alias) => reported_names.push(alias),
                None => {
                    if let Expr::Column(path) = inner_from_parentheses_and_unary_plus(expr) {
                        reported_names.extend(path.last().map(String::as_str));
                    }
                }
            }
        }
        // A select-list alias is visible in ORDER BY only as the WHOLE item:
        // `ORDER BY v` resolves to the field aliased `v`, while `ORDER BY v+1`
        // resolves `v` against the TABLE.
        let names_a_reported_field = |path: &[String]| {
            matches!(path, [name]
                if reported_names.iter().any(|reported| reported.eq_ignore_ascii_case(name)))
        };

        for (index, item) in select.order_by.iter().enumerate() {
            let expr = inner_from_parentheses_and_unary_plus(&item.expr);
            // `ORDER BY <n>` names a select field by ordinal.
            if matches!(expr, Expr::Int(_)) {
                continue;
            }
            let whole_item_is_reported = match expr {
                Expr::Column(path) => {
                    column_offset(expr, names)
                        .is_some_and(|offset| reported_offsets.contains(&offset))
                        || names_a_reported_field(path)
                }
                other => reported_exprs.contains(&other),
            };
            if whole_item_is_reported {
                continue;
            }
            for path in bare_columns(expr) {
                // A name this scope cannot resolve is the column resolver's to
                // report, with its own error.
                let Some(offset) = find_field_name(names, &path) else {
                    continue;
                };
                if reported_offsets.contains(&offset) {
                    continue;
                }
                return Err(err_field_in_order_not_select(
                    index + 1,
                    &qualified_name(offset, names),
                ));
            }
            // An aggregate reads no bare column of its own, so it reaches here
            // with nothing collected.
            if is_exempt(expr) {
                return Err(err_aggregate_in_order_not_select(index + 1));
            }
        }
        Ok(())
    }
}

/// Go `buildWhereFuncDepend(p, where)` (`logical_plan_builder.go:3521`): the
/// `col = col` equalities in the top-level `AND` chain, in both directions.
#[must_use]
pub fn build_where_func_depend(where_clause: &Expr, names: &[FieldName]) -> HashMap<usize, usize> {
    let mut depend = HashMap::new();
    for cond in split_where(where_clause) {
        if let Some((left, right)) = func_depend_col(cond, names) {
            depend.insert(left, right);
            depend.insert(right, left);
        }
    }
    depend
}

/// Go `buildJoinFuncDepend(p, from)` (`logical_plan_builder.go:3538`).
///
/// An INNER/CROSS join's `ON` equality holds in both directions; an outer
/// join's holds only from the NULL-supplying side towards the preserved one,
/// because the preserved side's row may pair with an all-NULL other side. See
/// this module's narrowing for how the sides are decided without
/// `tblInfoFromCol`.
#[must_use]
pub fn build_join_func_depend(join: &Join, names: &[FieldName]) -> HashMap<usize, usize> {
    let Some(on) = &join.on else {
        return HashMap::new();
    };
    let mut depend = HashMap::new();
    for cond in split_where(on) {
        let Some((mut left, mut right)) = func_depend_col(cond, names) else {
            continue;
        };
        // Go swaps when `tblInfoFromCol(x.Left, lCol) == nil`, i.e. when the
        // first operand is NOT from the join's left subtree.
        match join_side_of(join, left, names) {
            Some(false) => std::mem::swap(&mut left, &mut right),
            Some(true) => {}
            // Neither side claims the column, so no direction is known and Go's
            // swap decision cannot be reproduced; contributing nothing is the
            // conservative arm.
            None => continue,
        }
        match join.tp {
            JoinType::Left => {
                depend.insert(right, left);
            }
            JoinType::Right => {
                depend.insert(left, right);
            }
            // Go's `ast.CrossJoin` covers the plain comma/INNER join.
            _ => {
                depend.insert(left, right);
                depend.insert(right, left);
            }
        }
    }
    depend
}

/// Whether the column at `offset` is written against the join's LEFT subtree
/// (`Some(true)`), its right (`Some(false)`), or neither (`None`).
fn join_side_of(join: &Join, offset: usize, names: &[FieldName]) -> Option<bool> {
    let name = names.get(offset)?;
    let table = name.names.table.lower.as_str();
    let mut left_tables = Vec::new();
    collect_table_names(&join.left, &mut left_tables);
    if left_tables.iter().any(|found| found == table) {
        return Some(true);
    }
    let mut right_tables = Vec::new();
    if let Some(right) = &join.right {
        collect_table_names(right, &mut right_tables);
    }
    right_tables
        .iter()
        .any(|found| found == table)
        .then_some(false)
}

fn collect_table_names(node: &JoinNode, out: &mut Vec<String>) {
    match node {
        JoinNode::Table(table_ref) => {
            let visible = table_ref
                .alias
                .clone()
                .or_else(|| table_ref.name.last().cloned())
                .unwrap_or_default();
            out.push(visible.to_ascii_lowercase());
        }
        JoinNode::Derived { alias, .. } => {
            out.extend(alias.as_ref().map(|alias| alias.to_ascii_lowercase()));
        }
        JoinNode::Join(join) => {
            collect_table_names(&join.left, out);
            if let Some(right) = &join.right {
                collect_table_names(right, out);
            }
        }
    }
}

/// Go `buildFuncDependCol(p, cond)`: the two column offsets of a `col = col`.
fn func_depend_col(cond: &Expr, names: &[FieldName]) -> Option<(usize, usize)> {
    let Expr::Binary(BinaryOp::Eq, left, right) = cond else {
        return None;
    };
    Some((column_offset(left, names)?, column_offset(right, names)?))
}

/// Go `splitWhere(where)`: the top-level `AND` chain, parentheses transparent.
fn split_where(where_clause: &Expr) -> Vec<&Expr> {
    let mut out = Vec::new();
    let mut stack = vec![where_clause];
    while let Some(expr) = stack.pop() {
        match expr {
            Expr::Paren(inner) => stack.push(inner),
            Expr::Binary(BinaryOp::LogicAnd, left, right) => {
                stack.push(right);
                stack.push(left);
            }
            other => out.push(other),
        }
    }
    out
}

/// Go `extractSingeValueColNamesFromWhere(p, where, ...)`
/// (`logical_plan_builder.go:3748`): the columns a `WHERE` fixes to one value.
///
/// A deliberately shallow rule, not a constant folder: only the top-level `AND`
/// chain, and only `col = <literal>` (or a signed literal) in either operand
/// order.
#[must_use]
pub fn extract_single_value_col_names_from_where(where_clause: &Expr) -> Vec<&Expr> {
    let mut found = Vec::new();
    for cond in split_where(where_clause) {
        let Expr::Binary(BinaryOp::Eq, left, right) = cond else {
            continue;
        };
        if is_literal(right) {
            found.push(&**left);
        }
        if is_literal(left) {
            found.push(&**right);
        }
    }
    found
}

/// A literal, or a literal behind a unary sign — the two shapes Go's
/// single-value extraction accepts on the other side of the equality.
fn is_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Unary(_, inner) => matches!(
            &**inner,
            Expr::Int(_) | Expr::Float(_) | Expr::Decimal(_) | Expr::String(_) | Expr::Bool(_)
        ),
        Expr::Int(_)
        | Expr::Float(_)
        | Expr::Decimal(_)
        | Expr::String(_)
        | Expr::Bool(_)
        | Expr::Null => true,
        _ => false,
    }
}

/// Records every column `expr` reports that the grouping does not justify.
///
/// Go's `checkExprInGroupByOrIsSingleValue`: an aggregate is exempt outright,
/// so are `ANY_VALUE()` and `GROUPING()`; a non-column expression written
/// exactly as a `GROUP BY` item is exempt as a whole; otherwise every column
/// the expression reads OUTSIDE an aggregate must be pinned.
fn collect_offenders(
    expr: &Expr,
    position: usize,
    clause: Clause,
    group_exprs: &[&Expr],
    pinned: &HashSet<usize>,
    names: &[FieldName],
    out: &mut Vec<Offender>,
) {
    let expr = inner_from_parentheses_and_unary_plus(expr);
    if is_exempt(expr) {
        return;
    }
    if !matches!(expr, Expr::Column(_)) && group_exprs.contains(&expr) {
        return;
    }
    for path in bare_columns(expr) {
        let Some(offset) = find_field_name(names, &path) else {
            // An unresolvable name is reported by the ordinary column
            // resolver, with its own error; this rule has nothing to say.
            continue;
        };
        if pinned.contains(&offset) {
            continue;
        }
        out.push(Offender {
            offset,
            name: path.last().cloned().unwrap_or_default(),
            position,
            clause,
        });
    }
}

/// The output-name index a column expression resolves to.
fn column_offset(expr: &Expr, names: &[FieldName]) -> Option<usize> {
    match inner_from_parentheses_and_unary_plus(expr) {
        Expr::Column(path) => find_field_name(names, path),
        _ => None,
    }
}

/// The column at `offset` as Go's `ErrFieldNotInGroupBy` qualifies it:
/// `db.tbl.col`, dropping the parts the source does not have.
fn qualified_name(offset: usize, names: &[FieldName]) -> String {
    let Some(name) = names.get(offset) else {
        return String::new();
    };
    [
        name.names.database.original.as_str(),
        name.names.table.original.as_str(),
        name.names.column.original.as_str(),
    ]
    .into_iter()
    .filter(|part| !part.is_empty())
    .collect::<Vec<_>>()
    .join(".")
}

/// Go `getInnerFromParenthesesAndUnaryPlus(expr)`: parentheses and a unary `+`
/// are pure notation, so `(k)` and `+k` are the column `k`.
#[must_use]
pub fn inner_from_parentheses_and_unary_plus(expr: &Expr) -> &Expr {
    match expr {
        Expr::Paren(inner) | Expr::Unary(UnaryOp::Plus, inner) => {
            inner_from_parentheses_and_unary_plus(inner)
        }
        other => other,
    }
}

/// Whether the expression carries no obligation of its own: an aggregate
/// collapses its whole group, `ANY_VALUE()` says the caller accepts an
/// arbitrary row, and `GROUPING()` is checked by its own rule.
#[must_use]
pub fn is_exempt(expr: &Expr) -> bool {
    match expr {
        Expr::Aggregate { .. } | Expr::GroupConcat { .. } => true,
        Expr::Func { name, .. } => {
            name.eq_ignore_ascii_case("any_value") || name.eq_ignore_ascii_case("grouping")
        }
        _ => false,
    }
}

/// Whether `expr` contains an aggregate anywhere, which is what makes a
/// `GROUP BY`-less query a grouped one (Go's `hasAggFuncOrAnyValue`).
#[must_use]
pub fn aggregates_anywhere(expr: &Expr) -> bool {
    let mut found = false;
    super::aggregation::walk_exprs(expr, &mut |node| {
        if is_exempt(node) {
            found = true;
            return true;
        }
        false
    });
    found
}

/// Every column `expr` reads OUTSIDE an aggregate, a window call or an
/// `ANY_VALUE()`, which are the subtrees that answer for their own columns.
#[must_use]
pub fn bare_columns(expr: &Expr) -> Vec<Vec<String>> {
    let mut found = Vec::new();
    collect_bare_columns(expr, &mut found);
    found
}

fn collect_bare_columns(expr: &Expr, out: &mut Vec<Vec<String>>) {
    super::aggregation::walk_exprs(expr, &mut |node| match node {
        Expr::Column(path) => {
            out.push(path.clone());
            true
        }
        // A window call's own frame reads whole partitions, so its columns are
        // not this expression's to justify.
        Expr::Window { .. } => true,
        // A subquery has its OWN scope. The outer names it correlates to go
        // unchecked here, which is the permissive side and Go's too on this
        // path.
        Expr::Subquery(_) | Expr::Exists { .. } => true,
        Expr::InSubquery { expr, .. } | Expr::CompareSubquery { left: expr, .. } => {
            collect_bare_columns(expr, out);
            true
        }
        other => is_exempt(other),
    });
}

/// Whether an ORDER BY item merely names a select field, which was already
/// checked in its own right (Go skips such an item explicitly).
fn names_a_select_field(expr: &Expr, fields: &[SelectField]) -> bool {
    let Expr::Column(path) = inner_from_parentheses_and_unary_plus(expr) else {
        return false;
    };
    let [name] = path.as_slice() else {
        return false;
    };
    fields.iter().any(|field| match field {
        SelectField::Expr { expr, alias } => match alias {
            Some(alias) => alias.eq_ignore_ascii_case(name),
            None => matches!(expr, Expr::Column(path)
                if path.last().is_some_and(|last| last.eq_ignore_ascii_case(name))),
        },
        SelectField::Wildcard(_) => false,
    })
}
