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

//! Go `PlanBuilder.buildDDL`'s `*ast.CreateViewStmt` arm
//! (`planbuilder.go:5443`), and the AST expansion `buildSelect` makes for it
//! under `capFlag & canExpandAST`.
//!
//! | Rust | Go |
//! | --- | --- |
//! | [`PlanBuilder::build_create_view`] | `buildDDL`'s `*ast.CreateViewStmt` arm |
//! | [`check_for_user_variables`] | `checkForUserVariables` (`planbuilder.go:5291`) |
//! | [`find_stmt_as_view_schema`] | `findStmtAsViewSchema` (`planbuilder.go:6219`) |
//! | [`adjust_overlong_view_colname`] | `adjustOverlongViewColname` (`planbuilder.go:6209`) |
//! | [`PlanBuilder::add_alias_name`] | `addAliasName` (`logical_plan_builder.go:4141`) |
//! | [`PlanBuilder::record_expanded_select`] | the AST writes of `buildSelect` (`:4348-4364`), `resolveGbyExprs` and `resolveHavingAndOrderBy` |
//!
//! # The expansion is Go's in-place AST mutation
//!
//! Go builds a view body once and stores the SAME `*ast.SelectStmt` it built
//! (`ddl.BuildViewInfo` restores `s.Select`), so whatever `buildSelect` wrote
//! into the AST is the stored definition: every `*` unfolded into qualified
//! columns, every field aliased, GROUP BY positions and aliases replaced by
//! the select field's expression, and each column inside an aggregate of
//! HAVING, ORDER BY or a window function replaced by the select field it
//! resolves to.
//!
//! This builder reads the AST through shared references and never mutates it
//! (the marker spec's rule 5), so each `SelectStmt` build RECORDS the writes
//! Go would make, keyed by the statement's `QueryBlockOffset`, which the
//! statement's `QBHintHandler` walk assigned and every clone of the node
//! keeps. [`apply_expanded_selects`] then makes the writes on the statement
//! the caller owns. The key is the node's identity, not its position in the
//! build: a recursive CTE builds clones of its terms and a HAVING is built
//! from a scratch copy, and both still land on the node they came from. A
//! body built twice records twice; Go's second build reads its first build's
//! writes, which are idempotent, so the last record is the same.
//!
//! A view read inside the body (`BuildDataSourceFromView`) parses its own
//! statement, which Go expands and throws away; its records are discarded
//! with it (see [`super::from`]).

use std::collections::BTreeMap;

use tidb_ast::{CreateViewStmt, Expr, QueryStmt, SelectField, SelectStmt, SetOprTermBody};
use tidb_datatype::FieldName;
use tidb_expr::Columns;
use tidb_hack::GoToLower;

use super::aggregation::{
    has_window_flag, is_aggregate_call, resolve_from_select_fields, visit_exprs,
};
use tidb_infoschema::table_source::TableSource;
use super::marker::{MarkerKind, PlanMarker};
use super::{
    find_field_name, lower_name_matches, snapshot_schema_and_names, PlanBuilder, ProjectionField,
};
use crate::logical::LogicalPlan;
use crate::plan_base::PlanError;

/// The writes Go's `buildSelect` makes into one `*ast.SelectStmt` while it
/// builds a `CREATE VIEW` body.
#[derive(Clone, Debug)]
pub struct ExpandedSelect {
    /// `sel.Fields.Fields` after `unfoldWildStar` and `addAliasName`: Go keeps
    /// this list (`originalFields`) when `canExpandAST` is set.
    pub fields: Vec<SelectField>,
    /// Each `sel.GroupBy.Items[i].Expr` after `gbyResolver`.
    pub group_by: Vec<Expr>,
    /// `sel.Having.Expr` after `havingWindowAndOrderbyExprResolver`.
    pub having: Option<Expr>,
    /// Each `sel.OrderBy.Items[i].Expr` after the same resolver.
    pub order_by: Vec<Expr>,
    /// `sel.WindowSpecs` after `resolveWindowFunction`.
    pub windows: Vec<(String, tidb_ast::WindowDef)>,
}

/// Go `checkForUserVariables` (`planbuilder.go:5291`): any `ast.VariableExpr`
/// -- a user or system variable read, or an inline `@v := ...` assignment --
/// refuses the view.
#[must_use]
pub fn check_for_user_variables(query: &QueryStmt) -> bool {
    struct UserVariableChecker {
        has_user_variables: bool,
    }
    impl tidb_ast::Visitor for UserVariableChecker {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(expr) = node.downcast_mut::<Expr>() {
                if matches!(
                    expr,
                    Expr::UserVar(_) | Expr::SysVar { .. } | Expr::Assign { .. }
                ) {
                    self.has_user_variables = true;
                    return true;
                }
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    let mut query = query.clone();
    let mut checker = UserVariableChecker {
        has_user_variables: false,
    };
    tidb_ast::Visitable::accept(&mut query, &mut checker);
    checker.has_user_variables
}

/// Go `findStmtAsViewSchema`: the first `SelectStmt`, through the first term
/// of every set operation, is the one whose fields name the view's columns.
pub fn find_stmt_as_view_schema(query: &mut QueryStmt) -> Option<&mut SelectStmt> {
    match query {
        QueryStmt::Select(select) => Some(select),
        QueryStmt::SetOpr(set_opr) => first_select_of_set_opr(set_opr),
    }
}

fn first_select_of_set_opr(set_opr: &mut tidb_ast::SetOprStmt) -> Option<&mut SelectStmt> {
    match &mut set_opr.terms.first_mut()?.body {
        SetOprTermBody::Select(select) => Some(select),
        SetOprTermBody::Nested(nested) => first_select_of_set_opr(nested),
    }
}

/// Go `adjustOverlongViewColname`: an output column whose name is longer than
/// `mysql.MaxColumnNameLength` bytes is named `name_exp_<offset>`.
pub fn adjust_overlong_view_colname(plan: &mut LogicalPlan) {
    let mut names = plan.output_names().to_vec();
    let mut changed = false;
    for (index, name) in names.iter_mut().enumerate() {
        if name.names.column.original.go_to_lower().len() > tidb_mysql::consts::MaxColumnNameLength
        {
            name.names.column =
                tidb_datatype::IdentifierMetadata::new(format!("name_exp_{}", index + 1));
            changed = true;
        }
    }
    if changed {
        plan.base_mut().base.set_output_names(names);
    }
}

/// Makes the recorded [`ExpandedSelect`] writes on every `SelectStmt` of
/// `query` whose `QueryBlockOffset` has one. The visitor enters a statement
/// before its children, so a subquery inside a rewritten clause is rewritten
/// in turn.
pub fn apply_expanded_selects(query: &mut QueryStmt, expanded: &BTreeMap<i32, ExpandedSelect>) {
    struct Applier<'a> {
        expanded: &'a BTreeMap<i32, ExpandedSelect>,
    }
    impl tidb_ast::Visitor for Applier<'_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            let Some(select) = node.downcast_mut::<SelectStmt>() else {
                return false;
            };
            let Some(expanded) = self.expanded.get(&select.query_block_offset) else {
                return false;
            };
            select.fields = tidb_ast::SelectFieldList::from(expanded.fields.clone());
            for (item, expr) in select.group_by.iter_mut().zip(&expanded.group_by) {
                item.expr = expr.clone();
            }
            select.having = expanded.having.clone();
            for (item, expr) in select.order_by.iter_mut().zip(&expanded.order_by) {
                item.expr = expr.clone();
            }
            select.windows = expanded.windows.clone();
            false
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    tidb_ast::Visitable::accept(query, &mut Applier { expanded });
}

/// Go `ast.ColumnName.Match(b)` with `written` as the receiver: each part
/// `written` carries equals `b`'s, and the names agree. `resolved` is
/// `(DBName, TblName, ColName)`, any of the first two possibly empty.
fn column_name_match(written: &[String], resolved: (&str, &str, &str)) -> bool {
    let (schema, table, column) = match written {
        [column] => ("", "", column.as_str()),
        [table, column] => ("", table.as_str(), column.as_str()),
        [schema, table, column] => (schema.as_str(), table.as_str(), column.as_str()),
        _ => return false,
    };
    (schema.is_empty() || lower_name_matches(&resolved.0.go_to_lower(), schema))
        && (table.is_empty() || lower_name_matches(&resolved.1.go_to_lower(), table))
        && lower_name_matches(&resolved.2.go_to_lower(), column)
}

/// Go `havingWindowAndOrderbyExprResolver.resolveFromPlan`'s name: the
/// `FieldName` a column reference resolves to in `plan`, looking through the
/// operators that keep their child's columns and into a join's `FullNames`,
/// and remapping a redundant `USING`/`NATURAL` column to the canonical one.
/// `None` is Go's `-1`, a hidden column included.
fn resolve_from_plan_name(plan: &LogicalPlan, path: &[String]) -> Option<FieldName> {
    let (schema, names) = snapshot_schema_and_names(plan);
    let (column, name, full) = match find_field_name(&names, path) {
        Some(index) => (
            schema.columns.get(index)?.clone(),
            names.get(index)?.clone(),
            None,
        ),
        None => match plan {
            LogicalPlan::Limit(_)
            | LogicalPlan::Selection(_)
            | LogicalPlan::TopN(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::MaxOneRow(_) => {
                return resolve_from_plan_name(plan.children().first()?, path);
            }
            LogicalPlan::Join(join) if !join.full_names.is_empty() => {
                let full_schema = join.full_schema.as_ref()?;
                let index = find_field_name(&join.full_names, path)?;
                (
                    full_schema.columns.get(index)?.clone(),
                    join.full_names.get(index)?.clone(),
                    Some(join),
                )
            }
            LogicalPlan::Apply(apply) if !apply.join.full_names.is_empty() => {
                let full_schema = apply.join.full_schema.as_ref()?;
                let index = find_field_name(&apply.join.full_names, path)?;
                (
                    full_schema.columns.get(index)?.clone(),
                    apply.join.full_names.get(index)?.clone(),
                    Some(&apply.join),
                )
            }
            _ => return None,
        },
    };
    if column.is_hidden {
        return None;
    }
    let join = full.or(match plan {
        LogicalPlan::Join(join) => Some(join),
        LogicalPlan::Apply(apply) => Some(&apply.join),
        _ => None,
    });
    if let Some(join) = join {
        if name.redundant && !name.names.original_table.original.is_empty() {
            if let Some((_, Some(mapped))) = join.resolve_redundant_column(&column, &schema, &names)
            {
                return Some(mapped.clone());
            }
        }
    }
    Some(name)
}

/// Go's `inAggFunc` arm of `havingWindowAndOrderbyExprResolver.Leave`: a
/// column inside an aggregate resolves through the plan first
/// (`resolveFieldsFirst` is false there) and is REPLACED by the select field
/// it resolves to, `return a.selectFields[index].Expr, true`. A plan column
/// takes the first select field that is that column (`c.Name.Match`), or else
/// the auxiliary field `resolveFromPlan` appends, its fully qualified name; a
/// name only the select list knows takes that field's expression when the
/// clause falls back to the select list (`select_list_fallback`: HAVING and
/// ORDER BY, not the select list or a window specification); a column of an
/// outer query stays. Subqueries are another context and are skipped.
fn expand_aggregate_columns<N: tidb_ast::Visitable>(
    node: &mut N,
    fields: &[ProjectionField],
    plan: &LogicalPlan,
    select_list_fallback: bool,
) {
    struct Expander<'a> {
        fields: &'a [ProjectionField],
        plan: &'a LogicalPlan,
        select_list_fallback: bool,
    }
    impl tidb_ast::Visitor for Expander<'_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if node.is::<QueryStmt>() {
                return true;
            }
            let Some(expr) = node.downcast_mut::<Expr>() else {
                return false;
            };
            if !is_aggregate_call(expr) {
                return false;
            }
            let (fields, plan) = (self.fields, self.plan);
            let select_list_fallback = self.select_list_fallback;
            visit_exprs(expr, &mut |inner| {
                let Expr::Column(path) = inner else {
                    return false;
                };
                if let Some(name) = resolve_from_plan_name(plan, path) {
                    let resolved = (
                        name.names.database.original.as_str(),
                        name.names.table.original.as_str(),
                        name.names.column.original.as_str(),
                    );
                    let written = fields.iter().find_map(|field| match &field.expr {
                        Expr::Column(own) if column_name_match(own, resolved) => {
                            Some(field.expr.clone())
                        }
                        _ => None,
                    });
                    *inner = written.unwrap_or_else(|| {
                        Expr::Column(
                            [resolved.0, resolved.1, resolved.2]
                                .into_iter()
                                .filter(|part| !part.is_empty())
                                .map(str::to_owned)
                                .collect(),
                        )
                    });
                } else if select_list_fallback {
                    if let Ok(Some(index)) = resolve_from_select_fields(path, fields, false) {
                        *inner = fields[index].expr.clone();
                    }
                }
                true
            });
            true
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    tidb_ast::Visitable::accept(
        node,
        &mut Expander {
            fields,
            plan,
            select_list_fallback,
        },
    );
}

impl<S: TableSource, C: Columns> PlanBuilder<'_, S, C> {
    /// Go `buildDDL`'s `*ast.CreateViewStmt` arm: build the body with
    /// `isCreateView`, `canExpandAST` and `renameView` set, leave the expanded
    /// body in `create.query` for `ddl.BuildViewInfo` to restore, and name the
    /// view's columns (`v.Cols`) from the plan when the statement did not.
    ///
    /// `view_schema` is `v.ViewName.Schema` after `preprocess` qualified it.
    /// The visit infos Go appends for `CREATE VIEW` / `DROP` / `SUPER` stay
    /// with the session's DDL privilege check.
    ///
    /// # Errors
    ///
    /// `ErrViewSelectVariable`, the body's own build error, and
    /// `ErrViewWrongList` when a column list does not match the body.
    pub fn build_create_view(
        &mut self,
        view_schema: &str,
        create: &mut CreateViewStmt,
    ) -> Result<LogicalPlan, PlanError> {
        if check_for_user_variables(&create.query) {
            return Err(PlanError::from(
                tidb_util::dbterror::ERR_VIEW_SELECT_VARIABLE.generate_with_stack_by_args(&[]),
            ));
        }
        let view_name = create.name.last().cloned().unwrap_or_default();
        // Go numbers every query block when the statement's `QBHintHandler`
        // walks it, before the planner runs; the numbers are the keys of the
        // expansion. The handler's warnings are the build's to report.
        let _ = tidb_hint::QBHintHandler::build_query(&mut create.query);
        if let Some(stmt) = find_stmt_as_view_schema(&mut create.query) {
            stmt.as_view_schema = true;
        }

        let outer_is_create_view = std::mem::replace(&mut self.is_create_view, true);
        let outer_can_expand_ast = std::mem::replace(&mut self.can_expand_ast, true);
        let outer_renaming_view = self.renaming_view_name.replace((
            tidb_mysql::to_lowercase(view_schema),
            tidb_mysql::to_lowercase(&view_name),
        ));
        let outer_expanded = std::mem::take(&mut self.expanded_selects);
        let outer_correlated_aggs = std::mem::take(&mut self.expanded_correlated_aggs);
        let node = tidb_resolve::NodeW::new((*create.query).clone());
        let built = self.build_query_node(&node, false);
        let expanded = std::mem::replace(&mut self.expanded_selects, outer_expanded);
        self.expanded_correlated_aggs = outer_correlated_aggs;
        self.is_create_view = outer_is_create_view;
        self.can_expand_ast = outer_can_expand_ast;
        self.renaming_view_name = outer_renaming_view;
        let mut plan = built?;
        apply_expanded_selects(&mut create.query, &expanded);

        let schema_len = plan.schema().map_or(0, |schema| schema.columns.len());
        if create.columns.is_empty() {
            adjust_overlong_view_colname(&mut plan);
            create.columns = plan
                .output_names()
                .iter()
                .map(|name| name.names.column.original.clone())
                .collect();
        }
        if create.columns.len() != schema_len {
            return Err(PlanError::from(
                tidb_util::dbterror::ERR_VIEW_WRONG_LIST.generate_with_stack_by_args(&[]),
            ));
        }
        Ok(plan)
    }

    /// Go `addAliasName(ctx, selectStmt, p)` (`logical_plan_builder.go:4141`):
    /// every select field gains an explicit alias, so that a view's column
    /// names are fixed at creation. A field that IS a column
    /// (`*ast.ColumnNameExpr`, parentheses not looked through) takes its
    /// alias or written name; any other field its alias, or the name
    /// `buildProjectionFieldNameFromExpressions` gives its source text.
    ///
    /// Under `AsViewSchema` an anonymous field -- neither a column nor
    /// aliased -- whose name an earlier field already took is renamed
    /// `Name_exp_<name>`, then `Name_exp_<n>_<name>` (issue 29326).
    ///
    /// `fields` is `unfoldWildStar`'s output: a field a wildcard expanded has
    /// no [`ProjectionField::default_name`], and each other field is the next
    /// written expression of `select`.
    pub fn add_alias_name(&self, select: &SelectStmt, fields: &mut [ProjectionField]) {
        let mut written = select
            .fields
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| matches!(field, SelectField::Expr { .. }))
            .map(|(index, _)| index);
        let mut out_names = Vec::with_capacity(fields.len());
        let mut anonymous = Vec::with_capacity(fields.len());
        for field in fields.iter() {
            let written_index = if field.default_name.is_some() {
                written.next()
            } else {
                None
            };
            let expr = self.written_expr(&field.expr);
            let name = match (&expr, &field.alias) {
                (_, Some(alias)) => alias.clone(),
                (Expr::Column(path), None) => path.last().cloned().unwrap_or_default(),
                (expr, None) => match written_index {
                    Some(index) => super::field_name::expression_field_display_name(
                        &select.fields,
                        index,
                        expr,
                    ),
                    None => expr.restore(),
                },
            };
            anonymous.push(!matches!(expr, Expr::Column(_)) && field.alias.is_none());
            out_names.push(name);
        }

        // "dedupMap should be inited with all non-anonymous fields before
        // renaming other duplicated anonymous fields."
        let mut dedup: BTreeMap<String, usize> = BTreeMap::new();
        for (name, anonymous) in out_names.iter().zip(&anonymous) {
            if !anonymous {
                dedup.insert(name.go_to_lower(), 0);
            }
        }
        if select.as_view_schema {
            for (name, anonymous) in out_names.iter_mut().zip(&anonymous) {
                if !anonymous {
                    continue;
                }
                let old = name.clone();
                let key = old.go_to_lower();
                match dedup.get(&key).copied() {
                    Some(dup) => {
                        *name = if dup == 0 {
                            format!("Name_exp_{old}")
                        } else {
                            format!("Name_exp_{dup}_{old}")
                        };
                        dedup.insert(key, dup + 1);
                    }
                    None => {
                        dedup.insert(key, 0);
                    }
                }
            }
        }
        for (field, name) in fields.iter_mut().zip(out_names) {
            field.alias = Some(name);
        }
    }

    /// Records the writes Go's `buildSelect` makes into `select` (see this
    /// module's header): the aliased field list, the resolved GROUP BY items,
    /// and the HAVING and ORDER BY clauses with their aggregates' columns
    /// replaced. `fields` is the list right after [`Self::add_alias_name`],
    /// `gby_exprs` is `resolveGbyExprs`' result, and `plan` is the FROM plan
    /// `resolveFromPlan` searches. A statement no hint handler numbered has no
    /// identity to record under.
    pub(super) fn record_expanded_select(
        &mut self,
        select: &SelectStmt,
        fields: &[ProjectionField],
        gby_exprs: &[Expr],
        plan: &LogicalPlan,
    ) {
        if select.query_block_offset == 0 {
            return;
        }
        let fields: Vec<ProjectionField> = fields
            .iter()
            .map(|field| ProjectionField {
                expr: self.written_expr(&field.expr),
                ..field.clone()
            })
            .collect();
        let fields = fields.as_slice();
        let mut having = select
            .having
            .as_ref()
            .map(|having| self.written_expr(having));
        if let Some(having) = having.as_mut() {
            expand_aggregate_columns(having, fields, plan, true);
        }
        // `resolveHavingAndOrderBy` takes the ORDER BY items without a window
        // call, and `resolveWindowFunction` the others, both in ORDER BY's
        // clause.
        let order_by = select
            .order_by
            .iter()
            .map(|item| {
                let mut expr = self.written_expr(&item.expr);
                expand_aggregate_columns(&mut expr, fields, plan, true);
                expr
            })
            .collect();
        // `resolveWindowFunction` over the select list: each field with a
        // window call, and each named window, in the field-list clause.
        let mut windows = select.windows.clone();
        for (_, window) in &mut windows {
            expand_aggregate_columns(window, fields, plan, false);
        }
        let expanded = ExpandedSelect {
            fields: fields
                .iter()
                .map(|field| {
                    let mut expr = field.expr.clone();
                    if has_window_flag(&expr) {
                        expand_aggregate_columns(&mut expr, fields, plan, false);
                    }
                    SelectField::Expr {
                        expr,
                        alias: field.alias.clone(),
                    }
                })
                .collect(),
            windows,
            group_by: gby_exprs
                .iter()
                .map(|expr| self.written_expr(expr))
                .collect(),
            having,
            order_by,
        };
        self.expanded_selects
            .insert(select.query_block_offset, expanded);
    }

    /// `expr` as Go's AST still has it: each aggregate the correlated
    /// aggregate resolver replaced by a `MarkerKind::CorrelatedAgg` marker is
    /// put back (Go maps the aggregate node, `correlatedAggMapper`, and
    /// leaves the AST alone).
    fn written_expr(&self, expr: &Expr) -> Expr {
        let mut expr = expr.clone();
        if self.expanded_correlated_aggs.is_empty() {
            return expr;
        }
        visit_exprs(&mut expr, &mut |node| {
            let Some(marker) = PlanMarker::from_expr(node) else {
                return false;
            };
            if marker.kind != MarkerKind::CorrelatedAgg {
                return false;
            }
            if let Some(aggregate) = self.expanded_correlated_aggs.get(&marker.index) {
                *node = aggregate.clone();
            }
            true
        });
        expr
    }
}
