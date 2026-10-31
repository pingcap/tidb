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

//! Go `buildOrderedList` (`pkg/planner/core/point_get_plan.go`): the SET list
//! of the fast point UPDATE plan (`tryUpdatePointPlan`). The fast plan reads
//! through a bare point plan, so there is no logical read to lower the
//! assignments against: each value is rewritten over the point plan's own
//! schema and cast to its target column.

use std::collections::BTreeMap;

use tidb_ast::{Expr, TableRef};
use tidb_datatype::{FieldName, FieldNameMetadata, FieldType, FieldTypeCode};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_exprctx::Columns;

use tidb_infoschema::table_source::TableSource;
use super::marker::MarkerKind;
use super::PlanBuilder;
use tidb_planner_core_base::PlanError;

/// Go `buildOrderedList`'s `expression.BuildCastFunction` of an assignment to
/// its target column.
///
/// An unsigned numeric target is left alone: `CAST(-1 AS UNSIGNED)` would
/// wrap where the assignment's `table.CastValue` must decide under the
/// statement's sql_mode. An ENUM or SET target of an integer value casts by
/// member index (`EnumSetAsIntFlag`). The cast folds as Go's
/// `BuildCastFunctionWithCheck` does (not for a JSON target), so a constant
/// assignment converts, and warns, once for the statement.
pub fn cast_point_update_assignment(
    expression: Expression,
    target: &FieldType,
    ctx: &dyn Columns,
) -> Result<Expression, tidb_exprctx::EvalError> {
    let mut cast_to = target.clone();
    let unsigned_numeric = cast_to.has_flag(tidb_datatype::FieldTypeFlags::UNSIGNED)
        && matches!(
            cast_to.eval_type(),
            tidb_datatype::EvalType::Int
                | tidb_datatype::EvalType::Decimal
                | tidb_datatype::EvalType::Real
        );
    if unsigned_numeric {
        return Ok(expression);
    }
    if matches!(cast_to.code(), FieldTypeCode::Enum | FieldTypeCode::Set)
        && expression
            .static_type()
            .is_some_and(|source| source.eval_type() == tidb_datatype::EvalType::Int)
    {
        cast_to.add_flags(tidb_datatype::FieldTypeFlags::ENUM_SET_AS_INT);
    }
    let json = cast_to.eval_type() == tidb_datatype::EvalType::Json;
    let mut cast = tidb_expr::simple_expr::build_cast_function(expression, cast_to, false)?;
    if !json {
        tidb_expr::fold_constant_in_mode(&mut cast, ctx, tidb_expr::ConstantFoldMode::Normal);
    }
    Ok(cast)
}

impl<S: TableSource, C: Columns> PlanBuilder<'_, S, C> {
    /// The ordered list of a fast point UPDATE of `table_ref`: for each
    /// assignment, `value` (None: no expression, a generated column's own
    /// DEFAULT) rewritten over `schema` (the point plan's output, the table's
    /// columns in order and then the hidden handle) and cast to the target
    /// column named by the last element of `target`.
    pub fn build_point_ordered_list(
        &self,
        table_ref: &TableRef,
        schema: &Schema,
        assignments: &[(Vec<String>, Option<Expr>)],
    ) -> Result<Vec<Option<Expression>>, PlanError> {
        let (database, name) = match table_ref.name.as_slice() {
            [name] => (self.source.current_database(), name.as_str()),
            [database, name] => (database.as_str(), name.as_str()),
            _ => return Err(PlanError::internal("a fast UPDATE names no table")),
        };
        let table = self
            .source
            .find_table(database, name)
            .ok_or_else(|| PlanError::internal("a fast UPDATE's table is not in the catalog"))?;
        let visible = table_ref.alias.as_deref().unwrap_or(name);
        let field = |column: &str| {
            FieldName::new(FieldNameMetadata {
                original_table: tidb_datatype::IdentifierMetadata::new(name),
                original_column: tidb_datatype::IdentifierMetadata::new(column),
                database: tidb_datatype::IdentifierMetadata::new(database),
                table: tidb_datatype::IdentifierMetadata::new(visible),
                column: tidb_datatype::IdentifierMetadata::new(column),
            })
        };
        let mut names: Vec<FieldName> = table
            .columns
            .iter()
            .map(|column| field(column.read().name.original()))
            .collect();
        if schema.columns.len() > names.len() {
            names.push(field(tidb_model::column::EXTRA_HANDLE_NAME));
        }
        if names.len() != schema.columns.len() {
            return Err(PlanError::internal(
                "a fast UPDATE's point plan does not match its table",
            ));
        }
        let mut markers = BTreeMap::new();
        markers.insert(MarkerKind::Column, schema.columns.clone());
        assignments
            .iter()
            .map(|(target, value)| {
                let Some(value) = value else {
                    return Ok(None);
                };
                let rewritten =
                    self.rewrite_scalar(&Self::clause_scratch(value), schema, &names, &markers)?;
                super::check_not_multi_column_row(&rewritten)?;
                let target_type = target.last().and_then(|column| {
                    table
                        .columns
                        .iter()
                        .find(|candidate| candidate.read().name.original().eq_ignore_ascii_case(column))
                        .map(|candidate| candidate.read().field_type.clone())
                });
                match target_type {
                    Some(target_type) => {
                        cast_point_update_assignment(rewritten, &target_type, self.ctx)
                            .map(Some)
                            .map_err(PlanError::eval)
                    }
                    // The hidden handle has no column metadata: it casts to
                    // the BIGINT NOT NULL it is built as.
                    None => cast_point_update_assignment(
                        rewritten,
                        &FieldType::new(FieldTypeCode::LongLong)
                            .with_flags(tidb_datatype::FieldTypeFlags::NOT_NULL),
                        self.ctx,
                    )
                    .map(Some)
                    .map_err(PlanError::eval),
                }
            })
            .collect()
    }
}
