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

//! Go `executor.ResolveColumn`-style name resolution for the write paths:
//! unqualified and `t.`-qualified column names against one table's schema.

use super::*;
use tidb_hack::GoCaseCompare;

/// Resolves unqualified/`t.`-qualified column names against one table's schema
/// (case-insensitive, as in MySQL).
pub(crate) struct TableResolver<'a> {
    pub(crate) table_name: &'a str,
    /// The table's database, which a `db.t.c` reference must name (Go
    /// `FindFieldName` matches `name.DBName` even through an alias).
    pub(crate) database: Option<&'a str>,
    pub(crate) columns: &'a [(String, FieldType)],
    pub(crate) constant_context: crate::StmtContext,
    /// The statement's session `time_zone` (see [`ColumnResolver::time_zone`]),
    /// taken from the write's `StmtContext` at each build site.
    pub(crate) zone: tidb_datatype::SessionTimeZone,
    pub(crate) no_unsigned_subtraction: bool,
    pub(crate) div_precision_increment: u32,
    /// Go `clauseMsg` for this resolver's statement clause: the write lists
    /// (`INSERT VALUES`, `ON DUPLICATE KEY UPDATE`, `UPDATE ... SET`) name
    /// `field list`, while a site whose unresolved columns a DIFFERENT clause
    /// names keeps the generic `expression` spelling.
    pub(crate) clause_message: &'static str,
    /// The constants the INSERT's planning folded its uncorrelated scalar
    /// subqueries to, which the values reach as `#const#N` markers (Go's
    /// rewriter pushes the `Constant` itself).
    pub(crate) subquery_constants: &'a [Expression],
}

impl ColumnResolver for TableResolver<'_> {
    fn user_vars(&self) -> Option<&tidb_variable::user_vars::UserVars> {
        self.constant_context.user_vars()
    }

    fn resolve_constant(&self, path: &[String]) -> Option<Expression> {
        let [name] = path else {
            return None;
        };
        let marker = tidb_planner::plan_builder::marker::PlanMarker::decode(name)?;
        (marker.kind == tidb_planner::plan_builder::marker::MarkerKind::Constant)
            .then(|| self.subquery_constants.get(marker.index).cloned())
            .flatten()
    }

    fn has_resolved_constants(&self) -> bool {
        !self.subquery_constants.is_empty()
    }

    fn clause_message(&self) -> &'static str {
        self.clause_message
    }

    fn param_value(&self, order: usize) -> Result<Datum, tidb_expr::EvalError> {
        tidb_expr::Columns::param_value(&self.constant_context, order)
    }

    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        self.zone.clone()
    }

    fn date_modes(&self) -> tidb_datatype::DateModes {
        tidb_expr::Columns::date_modes(&self.constant_context)
    }

    fn connection_charset_info(&self) -> (&str, &str) {
        self.constant_context.connection_charset_info()
    }

    fn like_default_escape(&self) -> u8 {
        self.constant_context.like_default_escape()
    }

    fn no_unsigned_subtraction(&self) -> bool {
        self.no_unsigned_subtraction
    }

    fn div_precision_increment(&self) -> u32 {
        self.div_precision_increment
    }

    fn current_database(&self) -> Option<String> {
        tidb_expr::Columns::current_database(&self.constant_context)
    }

    fn fold_constant(&self, expression: &mut Expression, mode: tidb_expr::ConstantFoldMode) {
        tidb_expr::fold_constant_in_mode(expression, &self.constant_context, mode);
    }

    fn eval_constant(&self, expression: &Expression) -> Result<Datum, tidb_expr::EvalError> {
        tidb_expr::eval_expression_once(expression, &self.constant_context)
    }

    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        let (qualifier, name) = match path {
            [name] => (None, name),
            [table, name] => (Some(table), name),
            [schema, table, name] => {
                if !self
                    .database
                    .is_some_and(|database| database.go_lower_eq(schema))
                {
                    return None;
                }
                (Some(table), name)
            }
            _ => return None,
        };
        if let Some(q) = qualifier {
            if !q.go_lower_eq(self.table_name) {
                return None;
            }
        }
        self.columns
            .iter()
            .position(|(n, _)| n.go_lower_eq(name))
            .map(|i| (i, self.columns[i].1.clone(), (i + 1) as i64))
    }
}
