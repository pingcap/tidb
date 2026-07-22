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

//! Automatic result metadata for the dependency-closed query boundary.
//!
//! The Go executor receives a planner-owned schema before it constructs a
//! result set.  Until Rust has a catalog-backed schema resolver, the one
//! source-shaped query boundary that can be closed safely is a plain,
//! table-less `SELECT`: the parser already owns the select list and the
//! result-field resolver owns literal/operator/function metadata.  This leaf
//! deliberately rejects every query shape that would require relation or set
//! metadata rather than guessing from runtime values.

use std::fmt;

use tidb_ast::{QueryStmt, SelectStatementKind, Stmt};
use tidb_datatype::Collation;
use tidb_parser::ParseError;
use tidb_protocol::ColumnInfo;

use crate::result_field_resolver::{
    resolve_select_fields, ResolvedResultField, ResultFieldResolveError,
};
use crate::result_metadata::{
    col_names_to_result_fields, columns_from_adapted_fields, AdaptedResultField,
};

/// The source-shaped metadata generated for one table-less `SELECT`.
#[derive(Clone, Debug, PartialEq)]
pub struct AutomaticResultResponse {
    /// Resolver output before the source adapter projects it into field names.
    pub resolved_fields: Vec<ResolvedResultField>,
    /// Source-shaped fields after `colNames2ResultFields` compatibility rules.
    pub adapted_fields: Vec<AdaptedResultField>,
    /// Protocol columns produced by `ConvertColumnInfo`.
    pub columns: Vec<ColumnInfo>,
}

/// Explicit query-shape failures at the automatic result-metadata boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AutomaticResultResponseError {
    /// The SQL did not parse.
    Parse(ParseError),
    /// The statement does not produce a query result set.
    NonQueryStatement,
    /// A set operation needs the planner's merged output schema.
    SetOperationRequiresSchema,
    /// `TABLE`/standalone `VALUES` are not plain select lists.
    NonPlainSelect,
    /// A relation in `FROM` needs catalog-backed columns and types.
    FromRequiresSchema,
    /// A CTE has relation scope even if the outer list has no direct `FROM`.
    WithRequiresSchema,
    /// The expression resolver could not derive source-backed metadata.
    Resolve(ResultFieldResolveError),
}

impl fmt::Display for AutomaticResultResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse(error) => write!(f, "result SQL parse failed: {error:?}"),
            Self::NonQueryStatement => f.write_str("automatic result metadata requires a query"),
            Self::SetOperationRequiresSchema => {
                f.write_str("automatic result metadata for set operations requires schema")
            }
            Self::NonPlainSelect => {
                f.write_str("automatic result metadata requires a plain SELECT")
            }
            Self::FromRequiresSchema => {
                f.write_str("automatic result metadata for FROM queries requires schema")
            }
            Self::WithRequiresSchema => {
                f.write_str("automatic result metadata for WITH queries requires schema")
            }
            Self::Resolve(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for AutomaticResultResponseError {}

impl From<ResultFieldResolveError> for AutomaticResultResponseError {
    fn from(error: ResultFieldResolveError) -> Self {
        Self::Resolve(error)
    }
}

/// Resolves and converts a plain, table-less `SELECT` into protocol columns.
///
/// `default_db` is passed through the same adapter fallback used by Go when a
/// field has a table but no explicit database.  No value inspection, catalog
/// lookup, session charset conversion, or wire framing happens here.
pub fn derive_tableless_select_result(
    sql: &str,
    default_collation: Collation,
    default_db: &str,
) -> Result<AutomaticResultResponse, AutomaticResultResponseError> {
    let statement = tidb_parser::parse(sql).map_err(AutomaticResultResponseError::Parse)?;
    let Stmt::Query(query) = statement else {
        return Err(AutomaticResultResponseError::NonQueryStatement);
    };
    let QueryStmt::Select(select) = query.into_inner() else {
        return Err(AutomaticResultResponseError::SetOperationRequiresSchema);
    };
    if select.kind != SelectStatementKind::Select {
        return Err(AutomaticResultResponseError::NonPlainSelect);
    }
    if select.from.is_some() {
        return Err(AutomaticResultResponseError::FromRequiresSchema);
    }
    if select.with.is_some() {
        return Err(AutomaticResultResponseError::WithRequiresSchema);
    }

    let resolved_fields = resolve_select_fields(&select.fields, default_collation)?;
    let schema = resolved_fields
        .iter()
        .map(|field| field.field_type.clone())
        .collect::<Vec<_>>();
    let names = resolved_fields
        .iter()
        .map(|field| field.names.clone())
        .collect::<Vec<_>>();
    let adapted_fields = col_names_to_result_fields(&schema, &names, default_db);
    let columns = columns_from_adapted_fields(&adapted_fields);

    Ok(AutomaticResultResponse {
        resolved_fields,
        adapted_fields,
        columns,
    })
}

/// Resolves a query's result columns at the automatic metadata boundary.
///
/// This name is intentionally query-oriented for callers such as the server
/// command dispatcher.  It has the same strict table-less `SELECT` contract
/// as [`derive_tableless_select_columns`].
pub fn resolve_query_result_columns(
    sql: &str,
    default_collation: Collation,
    default_db: &str,
) -> Result<Vec<ColumnInfo>, AutomaticResultResponseError> {
    derive_tableless_select_columns(sql, default_collation, default_db)
}

/// Convenience wrapper returning only the protocol columns.
pub fn derive_tableless_select_columns(
    sql: &str,
    default_collation: Collation,
    default_db: &str,
) -> Result<Vec<ColumnInfo>, AutomaticResultResponseError> {
    Ok(derive_tableless_select_result(sql, default_collation, default_db)?.columns)
}
