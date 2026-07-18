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

//! Planner-shaped projection metadata over an already-resolved join schema.
//!
//! Go's `pkg/planner/core/logical_plan_builder.go::buildProjection` keeps
//! projection output names separate from the child join schema. For a direct
//! column, `buildProjectionField` preserves the source table/database and
//! original column while applying the select-list alias to the display name;
//! wildcard expansion copies the planner-visible names in declaration order.
//!
//! This leaf ports only that name/type projection contract. It accepts
//! wildcards and direct column references whose types already came from a
//! [`JoinOutputMetadata`] child schema. Expressions and derived-table output
//! are rejected instead of being evaluated or assigned guessed types.
//! Qualified columns/wildcards resolve against the planner `FullSchema`, so a
//! redundant USING-side field remains addressable without changing bare `*`.
//! `Session::resolve_query_result_columns` consumes this leaf for bounded
//! LEFT/USING automatic responses; the relation executor still owns
//! expression evaluation and row production through `Database::project_row`.

use std::fmt;

use tidb_ast::{Expr, SelectField};
use tidb_datatype::{FieldName, QualifiedColumnName};
use tidb_expr::find_field_name;

use crate::result_field_resolver::ResolvedResultField;
use crate::result_metadata::IdentifierMetadata;
use crate::result_schema_join_output::{JoinOutputField, JoinOutputMetadata};

/// A projection failure at the planner/result-schema boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinProjectionError {
    /// An identifier path has more components than TiDB's column resolver
    /// accepts (`column`, `table.column`, or `db.table.column`).
    InvalidIdentifierPath {
        /// The unresolved identifier path.
        path: Vec<String>,
    },
    /// A qualified wildcard/column uses no visible relation qualifier.
    UnknownQualifier {
        /// The unresolved qualifier path.
        qualifier: String,
    },
    /// A column does not occur in the planner-visible join output.
    MissingColumn {
        /// The optional relation qualifier.
        qualifier: Option<String>,
        /// The requested display column name.
        column: String,
    },
    /// An unqualified name occurs in more than one visible child field.
    AmbiguousColumn {
        /// The requested display column name.
        column: String,
    },
    /// An expression needs planner typing and executor evaluation.
    UnsupportedExpression {
        /// Debug-shaped expression text retained for diagnostics.
        expression: String,
    },
}

impl fmt::Display for JoinProjectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidIdentifierPath { path } => {
                write!(f, "invalid join projection identifier path: {path:?}")
            }
            Self::UnknownQualifier { qualifier } => {
                write!(f, "unknown join projection qualifier: {qualifier}")
            }
            Self::MissingColumn { qualifier, column } => match qualifier {
                Some(qualifier) => {
                    write!(f, "join projection column not found: {qualifier}.{column}")
                }
                None => write!(f, "join projection column not found: {column}"),
            },
            Self::AmbiguousColumn { column } => {
                write!(f, "ambiguous join projection column: {column}")
            }
            Self::UnsupportedExpression { expression } => write!(
                f,
                "join projection expression requires planner typing: {expression}"
            ),
        }
    }
}

impl std::error::Error for JoinProjectionError {}

/// Projects source-shaped result fields from planner-visible join metadata.
///
/// The accepted subset is deliberately the same direct-column/wildcard
/// boundary as `buildProjectionField` after expression rewriting has already
/// produced a child column: `*`, `t.*`, `db.t.*`, and direct columns with an
/// optional alias. The returned fields retain source type/nullability metadata
/// and change only the display column name for an alias.
pub fn project_join_output_fields(
    fields: &[SelectField],
    metadata: &JoinOutputMetadata,
) -> Result<Vec<ResolvedResultField>, JoinProjectionError> {
    let mut projected = Vec::new();
    for field in fields {
        match field {
            SelectField::Wildcard(path) => {
                let fields = if path.is_empty() {
                    &metadata.fields
                } else {
                    &metadata.full_fields
                };
                let matches = wildcard_matches(path, fields)?;
                projected.extend(matches.into_iter().map(|field| field.field.clone()));
            }
            SelectField::Expr { expr, alias } => {
                let Expr::Column(path) = expr else {
                    return Err(JoinProjectionError::UnsupportedExpression {
                        expression: format!("{expr:?}"),
                    });
                };
                let fields = if path.len() == 1 {
                    &metadata.fields
                } else {
                    &metadata.full_fields
                };
                let field = resolve_column(path, fields)?;
                projected.push(projected_field(field, alias.as_deref()));
            }
        }
    }
    Ok(projected)
}

fn projected_field(field: &JoinOutputField, alias: Option<&str>) -> ResolvedResultField {
    let mut projected = field.field.clone();
    if let Some(alias) = alias.filter(|value| !value.is_empty()) {
        projected.names.column = IdentifierMetadata::new(alias);
    }
    projected
}

fn wildcard_matches<'a>(
    path: &[String],
    fields: &'a [JoinOutputField],
) -> Result<Vec<&'a JoinOutputField>, JoinProjectionError> {
    match path {
        [] => Ok(fields.iter().collect()),
        [qualifier] => {
            let matches = fields
                .iter()
                .filter(|field| {
                    field
                        .field
                        .names
                        .table
                        .lower
                        .eq_ignore_ascii_case(qualifier)
                })
                .collect::<Vec<_>>();
            if matches.is_empty() {
                return Err(JoinProjectionError::UnknownQualifier {
                    qualifier: qualifier.clone(),
                });
            }
            Ok(matches)
        }
        [database, qualifier] => {
            let matches = fields
                .iter()
                .filter(|field| {
                    field
                        .field
                        .names
                        .database
                        .lower
                        .eq_ignore_ascii_case(database)
                        && (field
                            .field
                            .names
                            .table
                            .lower
                            .eq_ignore_ascii_case(qualifier)
                            || field
                                .field
                                .names
                                .original_table
                                .lower
                                .eq_ignore_ascii_case(qualifier))
                })
                .collect::<Vec<_>>();
            if matches.is_empty() {
                return Err(JoinProjectionError::UnknownQualifier {
                    qualifier: path.join("."),
                });
            }
            Ok(matches)
        }
        _ => Err(JoinProjectionError::InvalidIdentifierPath {
            path: path.to_vec(),
        }),
    }
}

fn resolve_column<'a>(
    path: &[String],
    fields: &'a [JoinOutputField],
) -> Result<&'a JoinOutputField, JoinProjectionError> {
    match path {
        [column] => resolve_qualified_column(None, None, column, fields),
        [qualifier, column] => resolve_qualified_column(Some(qualifier), None, column, fields),
        [database, qualifier, column] => {
            resolve_qualified_column(Some(qualifier), Some(database), column, fields)
        }
        _ => Err(JoinProjectionError::InvalidIdentifierPath {
            path: path.to_vec(),
        }),
    }
}

fn resolve_qualified_column<'a>(
    qualifier: Option<&str>,
    database: Option<&str>,
    column: &str,
    fields: &'a [JoinOutputField],
) -> Result<&'a JoinOutputField, JoinProjectionError> {
    if let Some(qualifier) = qualifier {
        let visible_qualifier = fields.iter().any(|field| match database {
            None => field
                .field
                .names
                .table
                .lower
                .eq_ignore_ascii_case(qualifier),
            Some(database) => {
                field
                    .field
                    .names
                    .database
                    .lower
                    .eq_ignore_ascii_case(database)
                    && (field
                        .field
                        .names
                        .table
                        .lower
                        .eq_ignore_ascii_case(qualifier)
                        || field
                            .field
                            .names
                            .original_table
                            .lower
                            .eq_ignore_ascii_case(qualifier))
            }
        });
        if !visible_qualifier {
            return Err(JoinProjectionError::UnknownQualifier {
                qualifier: database
                    .map(|database| format!("{database}.{qualifier}"))
                    .unwrap_or_else(|| qualifier.to_owned()),
            });
        }
    }

    let names = fields
        .iter()
        .map(|field| FieldName {
            names: field.field.names.clone(),
            hidden: false,
            not_explicit_usable: false,
            redundant: false,
        })
        .collect::<Vec<_>>();
    let qualified = QualifiedColumnName::new(
        database.unwrap_or_default(),
        qualifier.unwrap_or_default(),
        column,
    );

    match find_field_name(&names, &qualified) {
        Ok(Some(index)) => Ok(&fields[index]),
        Ok(None) => {
            if let Some(qualifier) = qualifier {
                return Err(JoinProjectionError::MissingColumn {
                    qualifier: Some(qualifier.to_owned()),
                    column: column.to_owned(),
                });
            }
            Err(JoinProjectionError::MissingColumn {
                qualifier: None,
                column: column.to_owned(),
            })
        }
        Err(_) => Err(JoinProjectionError::AmbiguousColumn {
            column: column.to_owned(),
        }),
    }
}
