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

//! Go `buildInsert`'s target and value lists (`planbuilder.go:4111-4405`):
//! the columns the statement names (`getAffectCols`), and each `VALUES` /
//! `SET` expression rewritten over `mockTablePlan` (`buildValuesListOfInsert`,
//! `getInsertColExpr`).

use std::collections::BTreeMap;

use tidb_ast::{Expr, InsertStmt};
use tidb_datatype::{FieldName, FieldNameMetadata, FieldType, FieldTypeCode, IdentifierMetadata};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_exprctx::Columns;
use tidb_hack::go_to_lower;

use tidb_infoschema::table_source::TableSource;
use super::marker::MarkerKind;
use super::{ClauseCode, PlanBuilder};
use tidb_planner_core_operator_logicalop::{LogicalPlan, LogicalTableDual};
use tidb_planner_core_base::PlanError;

/// The columns of an INSERT's target the statement writes, resolved as Go's
/// `getAffectCols` does over the table's visible columns.
#[derive(Clone, Debug)]
pub struct InsertTarget {
    /// The target's schema.
    pub database: String,
    /// The target's stored name.
    pub table: String,
    /// The visible columns in table order, then `_tidb_rowid` when the
    /// statement names it (Go gives that slot a synthetic `TypeLonglong`
    /// column, `NewExtraHandleColInfo`).
    pub columns: Vec<(String, FieldType)>,
    /// Whether each of [`Self::columns`] is a generated column.
    pub generated: Vec<bool>,
    /// For each column the statement lists (all of them when it lists none),
    /// its position in [`Self::columns`].
    pub offsets: Vec<usize>,
    /// Where `_tidb_rowid` sits in [`Self::columns`], when the statement
    /// named it.
    pub extra_handle_offset: Option<usize>,
}

/// Go `Insert.Lists`: the expressions of each `VALUES` list, in the order of
/// [`InsertTarget::offsets`]; a generated column's own `DEFAULT` has none.
#[derive(Clone, Debug, Default)]
pub struct InsertValues {
    /// One entry per `VALUES` list.
    pub lists: Vec<Vec<Option<Expression>>>,
}

impl<S: TableSource, C: Columns> PlanBuilder<'_, S, C> {
    /// Go `getAffectCols` over `buildInsert`'s target: the columns a
    /// statement writes, by the names it lists or all visible ones.
    ///
    /// `_tidb_rowid` among the named columns is the extra handle, which a
    /// heap table alone has: a table with a real handle has no such column,
    /// so the name falls through to the ordinary `ErrUnknownColumn`.
    pub fn resolve_insert_target(&self, insert: &InsertStmt) -> Result<InsertTarget, PlanError> {
        let (database, name) = match insert.table.as_slice() {
            [_] if self.source.current_database().is_empty() => {
                return Err(PlanError::from(
                    tidb_plannererrors::ERR_NO_DB.generate_with_stack_by_args(&[]),
                ))
            }
            [name] => (self.source.current_database(), name.as_str()),
            [database, name] => (database.as_str(), name.as_str()),
            _ => return Err(PlanError::internal("an INSERT target has no name")),
        };
        let table = self.source.find_table(database, name).ok_or_else(|| {
            PlanError::from(
                tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                    .fast_generate_by_args(&[database.into(), name.into()]),
            )
        })?;
        let mut columns: Vec<(String, FieldType)> = Vec::new();
        let mut generated = Vec::new();
        for column in table
            .columns
            .iter()
            .filter(|column| {
                let column = column.read();
                !column.hidden && column.state == tidb_model::SchemaState::PUBLIC
            })
        {
            columns.push((column.read().name.original().to_owned(), column.read().field_type.clone()));
            generated.push(column.read().is_generated());
        }
        let named: Vec<String> = if insert.set_syntax {
            insert
                .set_columns
                .iter()
                .map(|path| path.last().cloned().unwrap_or_default())
                .collect()
        } else {
            insert.columns.clone()
        };
        let names_a_column = insert.set_syntax || insert.columns_specified;
        let heap = !table.pk_is_handle && !table.is_common_handle;
        let extra_handle_offset = (names_a_column
            && heap
            && !table.is_memory_table
            && named
                .iter()
                .any(|name| go_to_lower(name) == tidb_model::column::EXTRA_HANDLE_NAME))
        .then(|| {
            columns.push((
                tidb_model::column::EXTRA_HANDLE_NAME.to_owned(),
                FieldType::new(FieldTypeCode::LongLong),
            ));
            generated.push(false);
            columns.len() - 1
        });
        let offsets = if names_a_column {
            named
                .iter()
                .map(|name| {
                    columns
                        .iter()
                        .position(|(candidate, _)| go_to_lower(candidate) == go_to_lower(name))
                        .ok_or_else(|| {
                            PlanError::from(
                                tidb_plannererrors::ERR_UNKNOWN_COLUMN
                                    .generate_with_stack_by_args(&[
                                        tidb_hack::go_to_lower(name.clone()).into(),
                                        "field list".into(),
                                    ]),
                            )
                        })
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            (0..columns.len()).collect()
        };
        Ok(InsertTarget {
            database: table.db_name.clone(),
            table: table.table_name.clone(),
            columns,
            generated,
            offsets,
            extra_handle_offset,
        })
    }

    /// Go `resolveGeneratedColumns`: each generated column's expression
    /// (`GeneratedExpr.Clone()`) rewritten over `mockTablePlan`, whose schema
    /// is the table's own, hidden and write-only columns included -- so a
    /// `Column` indexes the table's complete stored row. The answer has one
    /// entry per table column, `None` where the column is not generated.
    pub fn resolve_generated_columns(
        &mut self,
        target: &InsertTarget,
    ) -> Result<Vec<Option<Expression>>, PlanError> {
        let table = self
            .source
            .find_table(&target.database, &target.table)
            .ok_or_else(|| {
                PlanError::from(
                    tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS.fast_generate_by_args(&[
                        (&target.database).into(),
                        (&target.table).into(),
                    ]),
                )
            })?
            .clone();
        if !table.columns.iter().any(|column| column.read().is_generated()) {
            return Ok(Vec::new());
        }
        let mut schema = Schema::default();
        let mut names = Vec::with_capacity(table.columns.len());
        for column in &table.columns {
            let mut built = Column::new(self.column_ids.alloc(), column.read().field_type.clone());
            built.orig_name = format!("{}.{}.{}", table.db_name, table.table_name, column.read().name);
            schema.columns.push(built);
            names.push(FieldName::new(FieldNameMetadata {
                original_table: IdentifierMetadata::new(table.table_name.as_str()),
                original_column: IdentifierMetadata::new(column.read().name.original()),
                database: IdentifierMetadata::new(table.db_name.as_str()),
                table: IdentifierMetadata::new(table.table_name.as_str()),
                column: IdentifierMetadata::new(column.read().name.original()),
            }));
        }
        let saved_clause = self.cur_clause;
        let saved_array = self.allow_build_cast_array.replace(true);
        let mut resolved = Vec::with_capacity(table.columns.len());
        let mut failure = None;
        for column in &table.columns {
            if !column.read().is_generated() {
                resolved.push(None);
                continue;
            }
            let Some(expr) = &column.generated_expr else {
                failure = Some(PlanError::internal(
                    "a generated column has no expression to build",
                ));
                break;
            };
            let mut markers = BTreeMap::new();
            markers.insert(MarkerKind::Column, schema.columns.clone());
            match self.rewrite_scalar(&Self::clause_scratch(expr), &schema, &names, &markers) {
                Ok(expression) => resolved.push(Some(expression)),
                Err(error) => {
                    failure = Some(error);
                    break;
                }
            }
        }
        self.allow_build_cast_array.set(saved_array);
        self.cur_clause = saved_clause;
        match failure {
            Some(error) => Err(error),
            None => Ok(resolved),
        }
    }

    /// Go `buildSelectPlanOfInsert`'s checks once the source is built: its
    /// width must equal the number of target columns, and only then is a
    /// generated target refused (`insert into t (b) select * from t` over a
    /// generated `b` is a count error, `insert into t select * from t` the
    /// generated-column one).
    pub fn check_insert_select(
        &self,
        target: &InsertTarget,
        select_width: usize,
    ) -> Result<(), PlanError> {
        if select_width != target.offsets.len() {
            return Err(PlanError::from(
                tidb_plannererrors::ERR_WRONG_VALUE_COUNT_ON_ROW
                    .generate_with_stack_by_args(&[1.into()]),
            ));
        }
        for &offset in &target.offsets {
            if target.generated[offset] {
                return Err(PlanError::from(
                    tidb_plannererrors::ERR_BAD_GENERATED_COLUMN
                        .generate_with_stack_by_args(&[
                            (target.columns[offset].0.clone()).into(),
                            (target.table.clone()).into(),
                        ]),
                ));
            }
        }
        Ok(())
    }

    /// Go `buildValuesListOfInsert`: the arity checks and every value
    /// rewritten as `getInsertColExpr` does. An uncorrelated scalar subquery
    /// is evaluated here, while the INSERT is planned.
    pub fn build_insert_values(
        &mut self,
        insert: &InsertStmt,
        target: &InsertTarget,
    ) -> Result<InsertValues, PlanError> {
        let names_a_column = insert.set_syntax || insert.columns_specified;
        let table = self
            .source
            .find_table(&target.database, &target.table)
            .ok_or_else(|| {
                PlanError::from(
                    tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS.fast_generate_by_args(&[
                        (&target.database).into(),
                        (&target.table).into(),
                    ]),
                )
            })?
            .clone();
        let (schema, names) = self.insert_mock_schema(target);
        let mut values = InsertValues::default();
        let mut previous_width = target.offsets.len();
        for (index, row) in insert.rows.iter().enumerate() {
            // The first list answers to the column list, every later one to
            // the list before it; the first check is skipped when both the
            // column list and the first list are empty (`INSERT t VALUES ()`).
            let width = row.len();
            if (index > 0 || names_a_column || width > 0) && width != previous_width {
                return Err(PlanError::from(
                    tidb_plannererrors::ERR_WRONG_VALUE_COUNT_ON_ROW
                        .generate_with_stack_by_args(&[(index + 1).into()]),
                ));
            }
            previous_width = width;
            let mut list = Vec::with_capacity(width);
            for (position, value) in row.iter().enumerate() {
                let offset = target.offsets[position];
                list.push(self.get_insert_col_expr(
                    target,
                    &table,
                    offset,
                    value,
                    &schema,
                    &names,
                )?);
            }
            values.lists.push(list);
        }
        Ok(values)
    }

    /// Go `TableInfo2SchemaAndNames`, as `mockTablePlan` carries it.
    pub(super) fn insert_mock_schema(&self, target: &InsertTarget) -> (Schema, Vec<FieldName>) {
        let mut schema = Schema::default();
        let mut names = Vec::with_capacity(target.columns.len());
        for (name, field_type) in &target.columns {
            let mut column = Column::new(self.column_ids.alloc(), field_type.clone());
            column.orig_name = format!("{}.{}.{name}", target.database, target.table);
            schema.columns.push(column);
            names.push(FieldName::new(FieldNameMetadata {
                original_table: IdentifierMetadata::new(target.table.as_str()),
                original_column: IdentifierMetadata::new(name.as_str()),
                database: IdentifierMetadata::new(target.database.as_str()),
                table: IdentifierMetadata::new(target.table.as_str()),
                column: IdentifierMetadata::new(name.as_str()),
            }));
        }
        (schema, names)
    }

    /// Go `getInsertColExpr`.
    fn get_insert_col_expr(
        &mut self,
        target: &InsertTarget,
        table: &tidb_infoschema::table_source::SourceTable,
        offset: usize,
        value: &Expr,
        schema: &Schema,
        names: &[FieldName],
    ) -> Result<Option<Expression>, PlanError> {
        let (column_name, _) = &target.columns[offset];
        let generated = target.generated[offset];
        let expression = match value {
            Expr::Default(path) => {
                let mut reference = column_name.clone();
                if let Some(path) = path {
                    let written = path
                        .last()
                        .ok_or_else(|| PlanError::internal("DEFAULT() needs a column"))?;
                    let found = table
                        .columns
                        .iter()
                        .find(|column| go_to_lower(column.read().name.original()) == go_to_lower(written))
                        .ok_or_else(|| {
                            PlanError::from(
                                tidb_plannererrors::ERR_UNKNOWN_COLUMN
                                    .generate_with_stack_by_args(&[
                                        tidb_hack::go_to_lower(written.clone()).into(),
                                        "field list".into(),
                                    ]),
                            )
                        })?;
                    // `DEFAULT(generated column)` only for the same column.
                    let same = go_to_lower(found.read().name.original()) == go_to_lower(column_name);
                    if !same && (generated || found.read().is_generated()) {
                        return Err(PlanError::from(
                            tidb_plannererrors::ERR_BAD_GENERATED_COLUMN
                                .generate_with_stack_by_args(&[
                                    (column_name.clone()).into(),
                                    (target.table.clone()).into(),
                                ]),
                        ));
                    }
                    if same && generated {
                        return Ok(None);
                    }
                    reference = found.read().name.original().to_owned();
                } else if generated {
                    // Only DEFAULT for a generated column is accepted: its
                    // expression fills the value.
                    return Ok(None);
                }
                let evaluator = self.insert_default_evaluator.ok_or_else(|| {
                    PlanError::internal("an INSERT default needs the default evaluator")
                })?;
                evaluator(&target.database, &target.table, &reference).map_err(PlanError::eval)?
            }
            _ => {
                let saved_clause = self.cur_clause;
                self.cur_clause = ClauseCode::FieldList;
                let result = self.rewrite_insert_value(value, schema, names);
                self.cur_clause = saved_clause;
                result?
            }
        };
        // Go: a generated column written explicitly accepts only DEFAULT.
        if generated {
            return Err(PlanError::from(
                tidb_plannererrors::ERR_BAD_GENERATED_COLUMN.generate_with_stack_by_args(&[
                    (column_name.clone()).into(),
                    (target.table.clone()).into(),
                ]),
            ));
        }
        Ok(Some(expression))
    }

    /// The default arm of `getInsertColExpr`: the value rewritten over
    /// `mockTablePlan` -- or over a fresh, column-less dual when the value IS
    /// a subquery ("subquery in insert values should not reference upper
    /// scope") -- and refused when the rewrite built anything but the dual.
    fn rewrite_insert_value(
        &mut self,
        value: &Expr,
        schema: &Schema,
        names: &[FieldName],
    ) -> Result<Expression, PlanError> {
        let mut dual = LogicalTableDual::new(self.base(LogicalTableDual::TYPE), 0);
        let (dual_schema, dual_names) = if matches!(value, Expr::Subquery(_)) {
            (Schema::default(), Vec::new())
        } else {
            (schema.clone(), names.to_vec())
        };
        dual.base.base.set_schema(Some(dual_schema));
        dual.base.base.set_output_names(dual_names);
        let mut scratch = Self::clause_scratch(value);
        let (plan, _) = self.lower_scalar_subqueries(LogicalPlan::TableDual(dual), &mut scratch)?;
        if !matches!(plan, LogicalPlan::TableDual(_)) {
            return Err(PlanError::internal(
                "Insert's SET operation or VALUES_LIST doesn't support complex subqueries now",
            ));
        }
        let (plan_schema, plan_names) = super::snapshot_schema_and_names(&plan);
        let mut markers = BTreeMap::new();
        markers.insert(MarkerKind::Column, plan_schema.columns.clone());
        let rewritten = self.rewrite_scalar(&scratch, &plan_schema, &plan_names, &markers)?;
        super::check_not_multi_column_row(&rewritten)?;
        Ok(rewritten)
    }
}
