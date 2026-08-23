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

use super::*;

/// One row expression after Go's expression rewriter has inserted the Apply
/// operators required by correlated subqueries.
pub(super) struct DmlExpression {
    expression: Expression,
    field_types: Vec<FieldType>,
    applies: Vec<(CorrelatedSubquery, FromScope)>,
}

impl DmlExpression {
    pub(super) fn build(
        expr: &tidb_ast::Expr,
        scope: FromScope,
        catalog: &Catalog,
        current_db: &str,
        ctx: &crate::StmtContext,
    ) -> Result<Self, DriverError> {
        Self::build_with_prepared_defaults(expr, scope, catalog, current_db, ctx, &[])
    }

    pub(super) fn build_with_prepared_defaults(
        expr: &tidb_ast::Expr,
        mut scope: FromScope,
        catalog: &Catalog,
        current_db: &str,
        ctx: &crate::StmtContext,
        defaults: &[super::defaults::PreparedNamedDefault],
    ) -> Result<Self, DriverError> {
        let mut rewritten = fold_subqueries(expr, &scope, catalog, current_db, ctx)?;
        let mut applies = Vec::new();
        while expr_has_subquery(&rewritten) {
            let index = scope.width();
            let mut found = None;
            rewritten = extract_correlated_subquery(
                &rewritten, &scope, catalog, current_db, index, &mut found, ctx,
            )?;
            let Some(correlated) = found else {
                break;
            };
            let value_type = if matches!(correlated.kind, SubqueryKind::Scalar) {
                subquery_result_type(&correlated, &scope, catalog, current_db, ctx)
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong))
            } else {
                FieldType::new(FieldTypeCode::LongLong)
            };
            applies.push((correlated, scope.clone()));
            scope.tables.push(FromTable {
                name: String::new(),
                database: None,
                columns: vec![(format!("__apply_{index}"), value_type)],
                offset: index,
                func_deps: Default::default(),
                physical: None,
            });
        }
        let expression =
            rewrite_with_prepared_defaults(&rewritten, &ScopeResolver { scope: &scope }, defaults)?;
        let field_types = scope
            .column_list()
            .into_iter()
            .map(|(_, field_type)| field_type)
            .collect();
        Ok(Self {
            expression,
            field_types,
            applies,
        })
    }

    pub(super) fn eval(
        &self,
        row: &[Datum],
        catalog: &Catalog,
        current_db: &str,
        ctx: &crate::StmtContext,
    ) -> Result<Datum, DriverError> {
        let base_width = self.field_types.len() - self.applies.len();
        let mut values = row.iter().take(base_width).cloned().collect::<Vec<_>>();
        for (correlated, scope) in &self.applies {
            values.push(run_correlated_subquery(
                correlated, &values, scope, catalog, current_db, ctx,
            )?);
        }
        let chunk = row_chunk(&values, &self.field_types)?;
        self.expression
            .eval(ctx, chunk.get_row(0))
            .map_err(|error| DriverError::Exec(ExecError::Eval(error)))
    }
}

pub(super) enum UpdateExpression {
    Scalar(Expression),
    Applied(DmlExpression),
}

impl UpdateExpression {
    pub(super) fn scalar(expression: Expression) -> Self {
        Self::Scalar(expression)
    }

    pub(super) fn applied(expression: DmlExpression) -> Self {
        Self::Applied(expression)
    }

    pub(super) fn eval(
        &self,
        row: &[Datum],
        scalar_row: tidb_chunk::row::Row<'_>,
        catalog: &Catalog,
        current_db: &str,
        ctx: &crate::StmtContext,
    ) -> Result<Datum, DriverError> {
        match self {
            Self::Scalar(expression) => expression
                .eval(ctx, scalar_row)
                .map_err(|error| DriverError::Exec(ExecError::Eval(error))),
            Self::Applied(expression) => expression.eval(row, catalog, current_db, ctx),
        }
    }
}

pub(super) fn dml_table_scope(
    table_ref: &tidb_ast::TableRef,
    database: &str,
    name: &str,
    columns: Vec<(String, FieldType)>,
    ctx: &crate::StmtContext,
) -> FromScope {
    let mut scope = PlanTrace::single_table_scope(
        table_ref.alias.as_deref().unwrap_or(name),
        table_ref.alias.is_none().then(|| database.to_owned()),
        columns,
    );
    let statement = FromScope::for_statement(ctx);
    scope.constant_context = statement.constant_context;
    scope.zone = statement.zone;
    scope.tidb_info_len = statement.tidb_info_len;
    scope.like_default_escape = statement.like_default_escape;
    scope.no_unsigned_subtraction = statement.no_unsigned_subtraction;
    scope.div_precision_increment = statement.div_precision_increment;
    scope
}
