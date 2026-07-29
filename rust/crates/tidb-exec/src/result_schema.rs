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

//! Explicit catalog-to-result-field resolution for the single-table path.
//!
//! Go's executor resolves a `ResultField` from the planner schema and output
//! names.  This leaf makes the missing boundary explicit: the catalog supplies
//! the table identity, ordered columns, and authoritative type metadata; the
//! query supplies a plain single-table projection. It intentionally
//! does not claim that the planner descriptor is carried into execution, infer
//! a column type from a runtime value, or silently flatten a join/derived table
//! into one schema.

use std::fmt;

use tidb_ast::{Expr, JoinNode, SelectField, SelectStmt};

use crate::result_field_resolver::{ResolvedResultField, ResultFieldResolveError};
use crate::result_metadata::{FieldNameMetadata, IdentifierMetadata, ResultFieldTypeMetadata};

/// One ordered catalog column visible to a single-table projection.
#[derive(Clone, Debug, PartialEq)]
pub struct CatalogColumn {
    /// The declared/original column name.
    pub name: String,
    /// The source-authoritative return metadata for this column.
    pub field_type: ResultFieldTypeMetadata,
}

impl CatalogColumn {
    /// Creates a catalog column from its source name and type metadata.
    pub fn new(name: impl Into<String>, field_type: ResultFieldTypeMetadata) -> Self {
        Self {
            name: name.into(),
            field_type,
        }
    }
}

/// A catalog snapshot sufficient for the connected single-table result path.
#[derive(Clone, Debug, PartialEq)]
pub struct CatalogTableSchema {
    /// Database/schema name. Empty means the caller has no default schema.
    pub database: String,
    /// Declared table name (never the query alias).
    pub table: String,
    /// Columns in declaration order.
    pub columns: Vec<CatalogColumn>,
}

impl CatalogTableSchema {
    /// Creates a table schema snapshot.
    pub fn new(
        database: impl Into<String>,
        table: impl Into<String>,
        columns: Vec<CatalogColumn>,
    ) -> Self {
        Self {
            database: database.into(),
            table: table.into(),
            columns,
        }
    }

    fn column(&self, name: &str) -> Option<&CatalogColumn> {
        self.columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(name))
    }
}

/// Explicit failures while resolving a query against one catalog table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CatalogSchemaError {
    /// The query did not contain a single plain table source.
    FromRequired,
    /// A join, derived table, or other multi-source shape is outside this
    /// leaf's schema boundary.
    JoinRequiresRelationResolver,
    /// The query's table name does not match the supplied catalog snapshot.
    MissingTable {
        /// The requested table path.
        table: String,
    },
    /// The query refers to a table qualifier that is not this table or alias.
    UnknownQualifier {
        /// The unresolved qualifier path.
        qualifier: String,
    },
    /// The query refers to a column absent from the snapshot.
    MissingColumn {
        /// The requested column name.
        column: String,
    },
    /// More than one catalog field remained usable after source-shaped name
    /// resolution.
    AmbiguousColumn {
        /// The original expression error text.
        message: String,
    },
    /// A field shape other than a direct column or wildcard needs expression
    /// typing and planner semantics that do not belong to this leaf.
    UnsupportedExpression {
        /// Debug-shaped expression text retained for diagnostics.
        expression: String,
    },
    /// The AST carried an invalid identifier path.
    InvalidIdentifierPath {
        /// The invalid identifier segments.
        path: Vec<String>,
    },
    /// The catalog's stored column/type vectors are not aligned.
    InvalidCatalogShape {
        /// The affected table name.
        table: String,
    },
    /// The declared type/charset is not represented by the current metadata
    /// registry and must not be guessed into a different wire type.
    UnsupportedColumnType {
        /// The declared type or charset spelling.
        type_name: String,
    },
    /// Errors from the existing expression-field adapter are kept visible
    /// instead of being converted to a guessed type.
    ResultField(ResultFieldResolveError),
}

impl fmt::Display for CatalogSchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FromRequired => {
                f.write_str("catalog result fields require a single table source")
            }
            Self::JoinRequiresRelationResolver => {
                f.write_str("catalog result fields do not resolve joins or derived tables")
            }
            Self::MissingTable { table } => write!(f, "catalog table not found: {table}"),
            Self::UnknownQualifier { qualifier } => {
                write!(f, "unknown table qualifier: {qualifier}")
            }
            Self::MissingColumn { column } => write!(f, "catalog column not found: {column}"),
            Self::AmbiguousColumn { message } => f.write_str(message),
            Self::UnsupportedExpression { expression } => {
                write!(f, "unsupported catalog result expression: {expression}")
            }
            Self::InvalidIdentifierPath { path } => {
                write!(f, "invalid identifier path: {path:?}")
            }
            Self::InvalidCatalogShape { table } => {
                write!(f, "catalog schema vectors are misaligned for table {table}")
            }
            Self::UnsupportedColumnType { type_name } => {
                write!(f, "unsupported catalog column type: {type_name}")
            }
            Self::ResultField(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for CatalogSchemaError {}

impl From<ResultFieldResolveError> for CatalogSchemaError {
    fn from(error: ResultFieldResolveError) -> Self {
        Self::ResultField(error)
    }
}

/// Resolves a plain single-table `SELECT` list against an explicit catalog
/// schema. Wildcards expand in catalog declaration order; direct columns may
/// be unqualified, table-qualified, or schema-qualified, and aliases change
/// only the display name. Every other expression shape is rejected.
pub fn resolve_catalog_select_fields(
    select: &SelectStmt,
    schema: &CatalogTableSchema,
) -> Result<Vec<ResolvedResultField>, CatalogSchemaError> {
    let Some(from) = &select.from else {
        return Err(CatalogSchemaError::FromRequired);
    };
    let JoinNode::Table(table_ref) = &from.left else {
        return Err(CatalogSchemaError::JoinRequiresRelationResolver);
    };
    if from.right.is_some() {
        return Err(CatalogSchemaError::JoinRequiresRelationResolver);
    }
    validate_table_ref(
        table_ref.name.as_slice(),
        table_ref.alias.as_deref(),
        schema,
    )?;
    let qualifier = table_ref.alias.as_deref().unwrap_or(&schema.table);

    let mut resolved = Vec::new();
    for field in &select.fields {
        match field {
            SelectField::Wildcard(path) => {
                validate_wildcard(path, schema, qualifier)?;
                resolved.extend(
                    schema
                        .columns
                        .iter()
                        .map(|column| resolved_column(schema, qualifier, column, None)),
                );
            }
            SelectField::Expr { expr, alias } => {
                let Expr::Column(path) = expr else {
                    return Err(CatalogSchemaError::UnsupportedExpression {
                        expression: format!("{expr:?}"),
                    });
                };
                let column_name = column_name(path)?;
                let column = schema.column(column_name).ok_or_else(|| {
                    CatalogSchemaError::MissingColumn {
                        column: column_name.to_owned(),
                    }
                })?;
                validate_column_qualifier(path, schema, qualifier)?;
                resolved.push(resolved_column(
                    schema,
                    qualifier,
                    column,
                    alias.as_deref().filter(|value| !value.is_empty()),
                ));
            }
        }
    }
    Ok(resolved)
}

fn validate_table_ref(
    path: &[String],
    alias: Option<&str>,
    schema: &CatalogTableSchema,
) -> Result<(), CatalogSchemaError> {
    let (database, table) = match path {
        [table] => (None, table.as_str()),
        [database, table] => (Some(database.as_str()), table.as_str()),
        _ => {
            return Err(CatalogSchemaError::InvalidIdentifierPath {
                path: path.to_vec(),
            })
        }
    };
    if !table.eq_ignore_ascii_case(&schema.table)
        || database.is_some_and(|value| !value.eq_ignore_ascii_case(&schema.database))
    {
        return Err(CatalogSchemaError::MissingTable {
            table: path.join("."),
        });
    }
    if alias.is_some_and(|value| value.is_empty()) {
        return Err(CatalogSchemaError::InvalidIdentifierPath {
            path: path.to_vec(),
        });
    }
    Ok(())
}

fn validate_wildcard(
    path: &[String],
    schema: &CatalogTableSchema,
    qualifier: &str,
) -> Result<(), CatalogSchemaError> {
    match path {
        [] => Ok(()),
        [table] if table.eq_ignore_ascii_case(qualifier) => Ok(()),
        [database, table]
            if database.eq_ignore_ascii_case(&schema.database)
                && (table.eq_ignore_ascii_case(qualifier)
                    || table.eq_ignore_ascii_case(&schema.table)) =>
        {
            Ok(())
        }
        _ => Err(CatalogSchemaError::UnknownQualifier {
            qualifier: path.join("."),
        }),
    }
}

fn column_name(path: &[String]) -> Result<&str, CatalogSchemaError> {
    match path {
        [column] | [_, column] | [_, _, column] => Ok(column),
        _ => Err(CatalogSchemaError::InvalidIdentifierPath {
            path: path.to_vec(),
        }),
    }
}

fn validate_column_qualifier(
    path: &[String],
    schema: &CatalogTableSchema,
    qualifier: &str,
) -> Result<(), CatalogSchemaError> {
    match path {
        [_] => Ok(()),
        [table, _] if table.eq_ignore_ascii_case(qualifier) => Ok(()),
        [database, table, _]
            if database.eq_ignore_ascii_case(&schema.database)
                && (table.eq_ignore_ascii_case(qualifier)
                    || table.eq_ignore_ascii_case(&schema.table)) =>
        {
            Ok(())
        }
        _ => Err(CatalogSchemaError::UnknownQualifier {
            qualifier: path[..path.len() - 1].join("."),
        }),
    }
}

fn resolved_column(
    schema: &CatalogTableSchema,
    qualifier: &str,
    column: &CatalogColumn,
    alias: Option<&str>,
) -> ResolvedResultField {
    let column_name = IdentifierMetadata::new(column.name.clone());
    ResolvedResultField {
        names: FieldNameMetadata {
            original_table: IdentifierMetadata::new(schema.table.clone()),
            original_column: column_name.clone(),
            database: IdentifierMetadata::new(schema.database.clone()),
            table: IdentifierMetadata::new(qualifier),
            column: IdentifierMetadata::new(alias.unwrap_or(&column.name)),
        },
        field_type: column.field_type.clone(),
    }
}
