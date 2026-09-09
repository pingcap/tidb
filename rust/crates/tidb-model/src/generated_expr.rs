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

//! Parsing and validating expressions persisted in table metadata.
//!
//! Go stores generated-column and partial-index expressions as text, then
//! reparses them as the first field of a synthetic `SELECT`. Name resolution
//! at this boundary validates column names without replacing the name-based
//! AST, which remains stable when a table's physical column offsets change.

use std::fmt;

use tidb_ast::{Expr, QueryStmt, SelectField, Stmt, Visitable, Visitor};

use crate::TableInfo;

/// Parses a metadata expression as the first projection of a synthetic
/// `SELECT`.
///
/// This is Go `pkg/util/generatedexpr.ParseExpression`. In particular, a
/// comma in `expression` starts a second projection; only the first one is
/// returned, matching the source package rather than inventing a stricter
/// single-expression grammar.
pub fn parse_expression(expression: &str) -> Result<Expr, tidb_parser::ParseError> {
    let sql = format!("select {expression}");
    let statements = tidb_parser::parse_multi(&sql)?;
    let Some(Stmt::Query(query)) = statements.into_iter().next() else {
        return Err(expression_shape_error(&sql));
    };
    let QueryStmt::Select(select) = &*query else {
        return Err(expression_shape_error(&sql));
    };
    let Some(SelectField::Expr { expr, .. }) = select.fields.fields().first() else {
        return Err(expression_shape_error(&sql));
    };
    Ok(expr.clone())
}

fn expression_shape_error(sql: &str) -> tidb_parser::ParseError {
    tidb_parser::ParseError {
        message: "metadata expression did not produce one SELECT expression".to_owned(),
        offset: sql.len(),
        near_offset: sql.len(),
        errno: None,
    }
}

/// The first column reference that a table cannot resolve.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolveNameError {
    /// Column spelling from the expression.
    pub column: String,
    /// Table spelling from the metadata.
    pub table: String,
}

impl fmt::Display for ResolveNameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "can't find column {} in {}",
            self.column, self.table
        )
    }
}

impl std::error::Error for ResolveNameError {}

/// Validates every column reference against `table` and returns the unchanged
/// name-based expression.
///
/// This is Go `pkg/util/generatedexpr.SimpleResolveName`. Column matching is
/// case-insensitive and ignores optional schema/table qualifiers, exactly as
/// the source visitor checks only `ColumnName.Name`.
pub fn simple_resolve_name(
    mut expression: Expr,
    table: &TableInfo,
) -> Result<Expr, ResolveNameError> {
    let mut resolver = NameResolver { table, error: None };
    if expression.accept(&mut resolver) {
        Ok(expression)
    } else {
        Err(resolver
            .error
            .expect("name resolution stops only after recording an error"))
    }
}

struct NameResolver<'a> {
    table: &'a TableInfo,
    error: Option<ResolveNameError>,
}

impl Visitor for NameResolver<'_> {
    fn enter(&mut self, _node: &mut dyn std::any::Any) -> bool {
        false
    }

    fn leave(&mut self, node: &mut dyn std::any::Any) -> bool {
        let Some(Expr::Column(path)) = node.downcast_ref::<Expr>() else {
            return true;
        };
        let name = path.last().cloned().unwrap_or_default();
        let lowercase_name = tidb_ast::CiString::new(&name).lowercase().to_owned();
        let found = self
            .table
            .columns
            .iter_deref()
            .any(|column| column.read().name.lowercase() == lowercase_name);
        if found {
            return true;
        }
        self.error = Some(ResolveNameError {
            column: name,
            table: self.table.name.original().to_owned(),
        });
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Go TestParseExpression.
    #[test]
    fn parse_expression_matches_go_test() {
        let parsed = parse_expression("json_extract(a, '$.a')").unwrap();
        let Expr::Func { name, .. } = parsed else {
            panic!("JSON_EXTRACT parses as a function call")
        };
        assert_eq!(name, "json_extract");
    }
}
