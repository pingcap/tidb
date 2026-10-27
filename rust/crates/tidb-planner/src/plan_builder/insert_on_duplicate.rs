// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `buildInsert`'s `ON DUPLICATE KEY UPDATE` half
//! (`planbuilder.go:4111-4405`, `Insert.ResolveOnDuplicate`,
//! `rewriteInsertOnDuplicateUpdate`).
//!
//! An assignment's value is rewritten over `Insert.Schema4OnDuplicate`: the
//! target table's columns (the row already stored), then -- for `INSERT ...
//! SELECT` -- the source columns the SELECT did not output but the
//! assignments name, which `buildSelectPlanOfInsert` appends to the SELECT's
//! fields, then the SELECT's outputs placed at the target column each one
//! fills (the row the insert would have written). `FindFieldName` matches a
//! name against all of them at once, so `a = b` over `t1(a, b)` filled from
//! `t2(a, b)` is ambiguous, while `VALUES(col)` reads the target's columns
//! only.

use std::collections::BTreeMap;

use tidb_ast::{Expr, InsertStmt, QueryStmt, SelectField, SelectStmt};
use tidb_datatype::{Datum, FieldName, FieldType, FieldTypeCode};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::Columns;
use tidb_hack::go_to_lower;

use super::catalog::TableSource;
use super::insert_values::InsertTarget;
use super::marker::MarkerKind;
use super::{ClauseCode, PlanBuilder};
use crate::logical::{LogicalPlan, LogicalTableDual};
use crate::plan_base::PlanError;

/// What the planner knows of an `INSERT ... SELECT`'s source once it is
/// built: the names of the (possibly extended) SELECT's outputs, and how many
/// of them fill the insert.
#[derive(Clone, Debug, Default)]
pub struct InsertSelectOutput {
    /// Go `selectPlan.OutputNames()`.
    pub names: Vec<FieldName>,
    /// Go `actualColLen`: the SELECT's own width when it was extended for the
    /// assignments; `None` when it was not.
    pub actual_col_len: Option<usize>,
}

/// One `ON DUPLICATE KEY UPDATE` assignment (Go `expression.Assignment`).
#[derive(Clone, Debug)]
pub struct OnDuplicateAssign {
    /// The target column's position in [`InsertTarget::columns`].
    pub offset: usize,
    /// The value, over `Schema4OnDuplicate`.
    pub expr: Expression,
    /// Go `Assignment.LazyErr`: a scalar subquery's 1242 raised while the
    /// assignment was built, which only a row meeting a duplicate reports.
    pub lazy_error: Option<PlanError>,
}

/// The column references of an assignment value, outside subqueries: plain
/// references and `VALUES(col)` arguments.
#[derive(Default)]
struct References {
    columns: Vec<Vec<String>>,
    values: Vec<Vec<String>>,
}

fn collect_references(expr: &Expr, references: &mut References) {
    use tidb_ast::Visitable;
    struct Collector<'a> {
        references: &'a mut References,
    }
    impl tidb_ast::Visitor for Collector<'_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if node.is::<QueryStmt>() {
                return true;
            }
            let Some(expr) = node.downcast_mut::<Expr>() else {
                return false;
            };
            match expr {
                Expr::Subquery(_) => true,
                Expr::Func { name, args, .. } if name.eq_ignore_ascii_case("values") => {
                    if let Some(Expr::Column(path)) = args.first() {
                        self.references.values.push(path.clone());
                    }
                    true
                }
                // A subquery the planning already folded is its constant.
                Expr::Column(_) if super::marker::PlanMarker::from_expr(expr).is_some() => true,
                Expr::Column(path) => {
                    self.references.columns.push(path.clone());
                    true
                }
                _ => false,
            }
        }
        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    let mut expr = expr.clone();
    expr.accept(&mut Collector { references });
}

/// Go `detectSelectAgg`: GROUP BY, or an aggregate in a field, HAVING or
/// ORDER BY.
fn detect_select_agg(select: &SelectStmt) -> bool {
    if !select.group_by.is_empty() {
        return true;
    }
    select.fields.fields().iter().any(|field| match field {
        SelectField::Expr { expr, .. } => has_agg(expr),
        SelectField::Wildcard(_) => false,
    }) || select.having.as_ref().is_some_and(has_agg)
        || select.order_by.iter().any(|item| has_agg(&item.expr))
}

/// Go `ast.HasAggFlag`: an aggregate outside any subquery.
fn has_agg(expr: &Expr) -> bool {
    use tidb_ast::Visitable;
    struct Finder(bool);
    impl tidb_ast::Visitor for Finder {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if node.is::<QueryStmt>() {
                return true;
            }
            match node.downcast_mut::<Expr>() {
                Some(Expr::Subquery(_)) => true,
                Some(Expr::Aggregate { .. } | Expr::GroupConcat { .. }) => {
                    self.0 = true;
                    true
                }
                _ => false,
            }
        }
        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            !self.0
        }
    }
    let mut finder = Finder(false);
    let mut expr = expr.clone();
    expr.accept(&mut finder);
    finder.0
}

/// Go's loose field match in `buildSelectPlanOfInsert`: a column field whose
/// qualifiers do not conflict with the reference's.
fn matches_select_field(select: &SelectStmt, path: &[String]) -> bool {
    let split = |path: &[String]| -> (String, String, String) {
        match path {
            [col] => (String::new(), String::new(), go_to_lower(col)),
            [tbl, col] => (String::new(), go_to_lower(tbl), go_to_lower(col)),
            [db, tbl, col, ..] => (go_to_lower(db), go_to_lower(tbl), go_to_lower(col)),
            [] => Default::default(),
        }
    };
    let (db, tbl, col) = split(path);
    select.fields.fields().iter().any(|field| {
        let SelectField::Expr {
            expr: Expr::Column(field_path),
            ..
        } = field
        else {
            return false;
        };
        let (field_db, field_tbl, field_col) = split(field_path);
        (db.is_empty() || field_db.is_empty() || db == field_db)
            && (tbl.is_empty() || field_tbl.is_empty() || tbl == field_tbl)
            && col == field_col
    })
}

impl<S: TableSource, C: Columns> PlanBuilder<'_, S, C> {
    /// Go `buildSelectPlanOfInsert`'s extension: an `INSERT ... SELECT` (a
    /// plain SELECT, no aggregation, no wildcard) whose ON DUPLICATE
    /// assignments name a column that is neither the target's nor a SELECT
    /// output gets that column appended as a SELECT field, so the assignment
    /// can read it (`insert into a select x from b on duplicate key update
    /// a.x = b.y`). Returns the extended query and the original field count,
    /// or `None` when nothing is appended. Go appends in map order; this
    /// keeps first-reference order.
    pub fn extend_insert_select(
        &self,
        insert: &InsertStmt,
        target: &InsertTarget,
    ) -> Option<(QueryStmt, usize)> {
        if insert.on_duplicate.is_empty() {
            return None;
        }
        let QueryStmt::Select(select) = insert.source.as_deref()? else {
            return None;
        };
        if detect_select_agg(select)
            || select
                .fields
                .fields()
                .iter()
                .any(|field| matches!(field, SelectField::Wildcard(_)))
        {
            return None;
        }
        let (_, target_names) = self.insert_mock_schema(target);
        let mut references = References::default();
        for assignment in &insert.on_duplicate {
            collect_references(&assignment.value, &mut references);
        }
        let mut seen: Vec<Vec<String>> = Vec::new();
        let mut appended = Vec::new();
        for path in references.values.iter().chain(&references.columns) {
            if seen.contains(path) {
                continue;
            }
            seen.push(path.clone());
            let in_target = super::find_field_name(&target_names, path).is_some();
            if in_target || matches_select_field(select, path) {
                continue;
            }
            appended.push(path.clone());
        }
        if appended.is_empty() {
            return None;
        }
        let actual_col_len = select.fields.fields().len();
        let mut select = select.as_ref().clone();
        for path in appended {
            select.fields.push(SelectField::Expr {
                expr: Expr::Column(path),
                alias: None,
            });
        }
        Some((QueryStmt::Select(Box::new(select)), actual_col_len))
    }

    /// Go `Insert.ResolveOnDuplicate` with `rewriteInsertOnDuplicateUpdate`:
    /// every assignment is rewritten once, at plan time, whether or not an
    /// inserted row eventually conflicts. `select` is the built source of an
    /// `INSERT ... SELECT`, with its output `schema`.
    pub fn build_on_duplicate(
        &mut self,
        insert: &InsertStmt,
        target: &InsertTarget,
        select: Option<(&Schema, &InsertSelectOutput)>,
    ) -> Result<Vec<OnDuplicateAssign>, PlanError> {
        let (table_schema, table_names) = self.insert_mock_schema(target);
        let target_len = table_names.len();
        // Go `Schema4OnDuplicate` / `Names4OnDuplicate`.
        let mut schema = table_schema.clone();
        let mut names = table_names.clone();
        if let Some((select_schema, output)) = select {
            let selected = &select_schema.columns;
            let filled = output.actual_col_len.unwrap_or(selected.len());
            for (position, column) in selected.iter().enumerate().skip(filled) {
                schema.columns.push(column.clone());
                names.push(output.names.get(position).cloned().unwrap_or_default());
            }
            let mut new_row: Vec<Option<(Column, FieldName)>> = vec![None; target_len];
            for (position, column) in selected.iter().enumerate().take(filled) {
                let Some(&offset) = target.offsets.get(position) else {
                    continue;
                };
                let Some(slot) = new_row.get_mut(offset) else {
                    continue;
                };
                let mut column = column.clone();
                column.ret_type = Some(target.columns[offset].1.clone());
                *slot = Some((
                    column,
                    output.names.get(position).cloned().unwrap_or_default(),
                ));
            }
            for slot in new_row {
                match slot {
                    Some((column, name)) => {
                        schema.columns.push(column);
                        names.push(name);
                    }
                    None => {
                        schema.columns.push(Column::new(
                            self.column_ids.alloc(),
                            FieldType::new(FieldTypeCode::Null),
                        ));
                        names.push(FieldName::default());
                    }
                }
            }
        }
        let mut assigns = Vec::with_capacity(insert.on_duplicate.len());
        for assignment in &insert.on_duplicate {
            let Some(offset) =
                Self::resolve_on_duplicate_target(assignment, target, &names[..target_len])?
            else {
                continue;
            };
            // A bare DEFAULT is the assigned column's own default.
            let value = match &assignment.value {
                Expr::Default(None) => Expr::Default(Some(assignment.col.clone())),
                value => value.clone(),
            };
            let (expr, lazy_error) =
                match self.rewrite_on_duplicate_value(&value, &schema, &names, target_len) {
                    Ok(expr) => (expr, None),
                    // Go: only ErrSubqueryMoreThan1Row is kept for the row
                    // that meets a duplicate (issue 29260).
                    Err(error) if error.equal(&tidb_util::dbterror::exeerrors::ERR_SUBQUERY_MORE_THAN1_ROW) => (
                        Expression::Constant(tidb_expr::constant::Constant::new(
                            Datum::Null,
                            FieldType::new(FieldTypeCode::Null),
                        )),
                        Some(error),
                    ),
                    Err(error) => return Err(error),
                };
            assigns.push(OnDuplicateAssign {
                offset,
                expr,
                lazy_error,
            });
        }
        Ok(assigns)
    }

    /// The first half of Go `ResolveOnDuplicate` for one assignment: the
    /// assigned column against the target's columns, and a generated column's
    /// DEFAULT-only rule (`IsDefaultExprSameColumn`). `None` is an assignment
    /// of DEFAULT to a generated column, which Go drops.
    fn resolve_on_duplicate_target(
        assignment: &tidb_ast::Assignment,
        target: &InsertTarget,
        target_names: &[FieldName],
    ) -> Result<Option<usize>, PlanError> {
        let offset = super::find_field_name(target_names, &assignment.col).ok_or_else(|| {
            PlanError::from(tidb_error::plannererrors::ERR_UNKNOWN_COLUMN.generate_with_stack_by_args(&[tidb_hack::go_to_lower(assignment.col.join(".")).into(), "field list".into()]))
        })?;
        if target.generated[offset] {
            let same = match &assignment.value {
                Expr::Default(None) => true,
                Expr::Default(Some(path)) => {
                    super::find_field_name(target_names, path) == Some(offset)
                }
                _ => false,
            };
            if same {
                return Ok(None);
            }
            return Err(PlanError::from(tidb_error::plannererrors::ERR_BAD_GENERATED_COLUMN.generate_with_stack_by_args(&[(target.columns[offset].0.clone()).into(), (target.table.clone()).into()])));
        }
        Ok(Some(offset))
    }

    /// Go `Insert.buildOnDuplicateUpdateColumns`: the columns the resolved
    /// ON DUPLICATE assignments write (a dropped generated DEFAULT is none),
    /// which decide the foreign key triggers.
    pub fn on_duplicate_columns(
        &self,
        insert: &InsertStmt,
        target: &InsertTarget,
    ) -> Result<Vec<String>, PlanError> {
        let (_, names) = self.insert_mock_schema(target);
        let mut columns = Vec::new();
        for assignment in &insert.on_duplicate {
            if let Some(offset) = Self::resolve_on_duplicate_target(assignment, target, &names)? {
                columns.push(target.columns[offset].0.clone());
            }
        }
        Ok(columns)
    }

    /// Go `rewriteInsertOnDuplicateUpdate`: the value over
    /// `Schema4OnDuplicate`, `VALUES(col)` reading the target's columns only.
    fn rewrite_on_duplicate_value(
        &mut self,
        value: &Expr,
        schema: &Schema,
        names: &[FieldName],
        target_len: usize,
    ) -> Result<Expression, PlanError> {
        // `VALUES(col)` names a target column.
        let mut references = References::default();
        collect_references(value, &mut references);
        for path in &references.values {
            if super::find_field_name(&names[..target_len], path).is_none() {
                return Err(PlanError::from(tidb_error::plannererrors::ERR_UNKNOWN_COLUMN.generate_with_stack_by_args(&[tidb_hack::go_to_lower(path.join(".")).into(), "field list".into()])));
            }
        }
        let saved_clause = self.cur_clause;
        self.cur_clause = ClauseCode::FieldList;
        let result = (|| {
            let mut dual = LogicalTableDual::new(self.base(LogicalTableDual::TYPE), 0);
            dual.base.base.set_schema(Some(schema.clone()));
            dual.base.base.set_output_names(names.to_vec());
            let mut scratch = Self::clause_scratch(value);
            let (plan, _) =
                self.lower_scalar_subqueries(LogicalPlan::TableDual(dual), &mut scratch)?;
            if !matches!(plan, LogicalPlan::TableDual(_)) {
                return Err(PlanError::internal(
                    "Insert's SET operation or VALUES_LIST doesn't support complex subqueries now",
                ));
            }
            let (plan_schema, plan_names) = super::snapshot_schema_and_names(&plan);
            let mut markers = BTreeMap::new();
            markers.insert(MarkerKind::Column, plan_schema.columns.clone());
            self.values_scope = Some((target_len, schema.clone(), names.to_vec()));
            let rewritten = self.rewrite_scalar(&scratch, &plan_schema, &plan_names, &markers);
            self.values_scope = None;
            let rewritten = rewritten?;
            super::check_not_multi_column_row(&rewritten)?;
            Ok(rewritten)
        })();
        self.cur_clause = saved_clause;
        result
    }
}
