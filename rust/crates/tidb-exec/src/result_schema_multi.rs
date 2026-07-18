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

//! Source-shaped result-field binding for a bounded multi-relation `SELECT`.
//!
//! Go's `pkg/executor/adapter.go` does not derive result fields from row
//! values. The planner has already resolved every output column against a
//! relation schema before `colNames2ResultFields` copies names and return
//! types into the wire-facing `ResultField`. This leaf models that boundary
//! for explicit inner/left joins and comma-separated table lists when the
//! caller supplies authoritative [`CatalogTableSchema`] snapshots.
//!
//! Relation execution is deliberately not part of this module. In particular,
//! an outer join's null-extension, `ON`/`USING` predicate semantics, and
//! `USING` column coalescing require the planner/executor relation contract and
//! are reported as remaining integration work rather than guessed here.

use std::fmt;

use tidb_ast::{Expr, Join, JoinNode, JoinType, SelectField, SelectStmt, TableRef};

use crate::result_field_resolver::ResolvedResultField;
use crate::result_metadata::{FieldNameMetadata, IdentifierMetadata};
use crate::result_schema::{CatalogColumn, CatalogTableSchema};

/// A relation-binding failure kept separate from the single-table resolver's
/// error type so callers can distinguish an ambiguous source/column from a
/// missing catalog snapshot without changing that existing API.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CatalogRelationSchemaError {
    /// The caller supplied no catalog snapshots for a query with a `FROM`.
    EmptyCatalog,
    /// A table path is not representable by the current source schema model.
    InvalidIdentifierPath {
        /// The invalid path segments.
        path: Vec<String>,
    },
    /// A `FROM` table was absent from the authoritative snapshots.
    MissingTable {
        /// The requested table path.
        table: String,
    },
    /// More than one supplied snapshot matches an unqualified table path.
    AmbiguousTable {
        /// The requested table path.
        table: String,
    },
    /// Two `FROM` relations expose the same visible qualifier.
    DuplicateQualifier {
        /// The duplicate alias/table qualifier.
        qualifier: String,
    },
    /// A projection qualifier does not identify a visible relation.
    UnknownQualifier {
        /// The unresolved qualifier path.
        qualifier: String,
    },
    /// A schema-qualified original table name maps to multiple aliased
    /// relations in the same `FROM` tree.
    AmbiguousQualifier {
        /// The ambiguous qualifier path.
        qualifier: String,
    },
    /// An unqualified projection matches columns in more than one relation.
    AmbiguousColumn {
        /// The requested column.
        column: String,
        /// Visible relation qualifiers that contain it, in `FROM` order.
        qualifiers: Vec<String>,
    },
    /// A projection refers to a column absent from its resolved relation(s).
    MissingColumn {
        /// The optional relation qualifier.
        qualifier: Option<String>,
        /// The requested column.
        column: String,
    },
    /// Derived tables are not catalog snapshots and need a planner output
    /// schema before they can participate in result-field derivation.
    DerivedTable,
    /// A join shape outside this leaf's explicit inner/left/comma boundary.
    UnsupportedJoin {
        /// The reason this relation shape is not accepted.
        reason: &'static str,
    },
    /// A table option that changes relation semantics was not silently
    /// ignored.
    UnsupportedTableOption {
        /// The affected relation.
        table: String,
        /// The unsupported option name.
        option: &'static str,
    },
    /// A select-list item requires expression typing/planner metadata.
    UnsupportedExpression {
        /// Debug-shaped expression text retained for diagnostics.
        expression: String,
    },
}

impl fmt::Display for CatalogRelationSchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyCatalog => f.write_str("catalog relation resolver requires table schemas"),
            Self::InvalidIdentifierPath { path } => {
                write!(f, "invalid relation identifier path: {path:?}")
            }
            Self::MissingTable { table } => write!(f, "catalog table not found: {table}"),
            Self::AmbiguousTable { table } => {
                write!(f, "catalog table path is ambiguous: {table}")
            }
            Self::DuplicateQualifier { qualifier } => {
                write!(f, "duplicate table qualifier: {qualifier}")
            }
            Self::UnknownQualifier { qualifier } => {
                write!(f, "unknown table qualifier: {qualifier}")
            }
            Self::AmbiguousQualifier { qualifier } => {
                write!(f, "ambiguous table qualifier: {qualifier}")
            }
            Self::AmbiguousColumn { column, qualifiers } => {
                write!(f, "ambiguous column {column} in {qualifiers:?}")
            }
            Self::MissingColumn { qualifier, column } => match qualifier {
                Some(qualifier) => write!(f, "catalog column not found: {qualifier}.{column}"),
                None => write!(f, "catalog column not found: {column}"),
            },
            Self::DerivedTable => {
                f.write_str("derived-table result fields require planner output schema")
            }
            Self::UnsupportedJoin { reason } => {
                write!(f, "unsupported catalog relation join: {reason}")
            }
            Self::UnsupportedTableOption { table, option } => {
                write!(f, "unsupported catalog table option {option} on {table}")
            }
            Self::UnsupportedExpression { expression } => {
                write!(f, "unsupported catalog result expression: {expression}")
            }
        }
    }
}

impl std::error::Error for CatalogRelationSchemaError {}

/// Resolves result fields for a plain multi-relation `SELECT`.
///
/// `schemas` is the authoritative catalog snapshot set. A table reference is
/// matched case-insensitively by its one- or two-part name. A one-part name
/// must match exactly one supplied snapshot; callers that need two schemas
/// with the same table name should provide distinct database-qualified names.
/// A query alias becomes the visible two-part qualifier while the original
/// table remains available only in a three-part `db.table.column` path, which
/// mirrors the single-table resolver's source-shaped alias contract.
pub fn resolve_catalog_relation_select_fields(
    select: &SelectStmt,
    schemas: &[CatalogTableSchema],
) -> Result<Vec<ResolvedResultField>, CatalogRelationSchemaError> {
    let Some(from) = &select.from else {
        return Err(CatalogRelationSchemaError::UnsupportedJoin {
            reason: "a relation resolver requires FROM",
        });
    };
    if schemas.is_empty() {
        return Err(CatalogRelationSchemaError::EmptyCatalog);
    }

    let mut relations = Vec::new();
    collect_join(from, schemas, &mut relations)?;
    if relations.is_empty() {
        return Err(CatalogRelationSchemaError::EmptyCatalog);
    }
    ensure_unique_qualifiers(&relations)?;

    select
        .fields
        .iter()
        .try_fold(Vec::new(), |mut resolved, field| {
            match field {
                SelectField::Wildcard(path) => {
                    let selected = wildcard_relations(path, &relations)?;
                    for relation in selected {
                        resolved.extend(
                            relation
                                .schema
                                .columns
                                .iter()
                                .map(|column| resolve_column(relation, column, None)),
                        );
                    }
                }
                SelectField::Expr { expr, alias } => {
                    let Expr::Column(path) = expr else {
                        return Err(CatalogRelationSchemaError::UnsupportedExpression {
                            expression: format!("{expr:?}"),
                        });
                    };
                    let (relation, column) = resolve_column_path(path, &relations)?;
                    resolved.push(resolve_column(
                        relation,
                        column,
                        alias.as_deref().filter(|value| !value.is_empty()),
                    ));
                }
            }
            Ok(resolved)
        })
}

/// A table source paired with the qualifier visible to a query.
#[derive(Clone, Debug)]
struct RelationBinding<'a> {
    schema: &'a CatalogTableSchema,
    qualifier: String,
}

fn collect_relation_node<'a>(
    node: &JoinNode,
    schemas: &'a [CatalogTableSchema],
    out: &mut Vec<RelationBinding<'a>>,
) -> Result<(), CatalogRelationSchemaError> {
    match node {
        JoinNode::Table(table) => out.push(resolve_table(table, schemas)?),
        JoinNode::Derived { .. } => return Err(CatalogRelationSchemaError::DerivedTable),
        JoinNode::Join(join) => collect_join(join, schemas, out)?,
    }
    Ok(())
}

fn collect_join<'a>(
    join: &Join,
    schemas: &'a [CatalogTableSchema],
    out: &mut Vec<RelationBinding<'a>>,
) -> Result<(), CatalogRelationSchemaError> {
    // `right == None` is the parser's wrapper around one table factor. Its
    // other fields must stay empty; accepting an attached condition would
    // silently lose relation semantics.
    let Some(right) = &join.right else {
        if join.straight || join.natural || join.on.is_some() || !join.using.is_empty() {
            return Err(CatalogRelationSchemaError::UnsupportedJoin {
                reason: "malformed single-relation join wrapper",
            });
        }
        return collect_relation_node(&join.left, schemas, out);
    };

    match join.tp {
        JoinType::Cross | JoinType::Left => {}
        JoinType::Right => {
            return Err(CatalogRelationSchemaError::UnsupportedJoin {
                reason: "RIGHT OUTER JOIN is outside the bounded source leaf",
            });
        }
    }
    if join.straight {
        return Err(CatalogRelationSchemaError::UnsupportedJoin {
            reason: "STRAIGHT_JOIN is outside the bounded source leaf",
        });
    }
    if join.natural {
        return Err(CatalogRelationSchemaError::UnsupportedJoin {
            reason: "NATURAL JOIN needs planner column-coalescing semantics",
        });
    }
    // `ON`/`USING` are intentionally not evaluated here: this leaf only
    // copies planner-visible output names/types, while predicate and outer
    // null-extension semantics belong to the relation executor. We still
    // traverse both sides so every source has an authoritative snapshot.
    collect_relation_node(&join.left, schemas, out)?;
    collect_relation_node(right, schemas, out)
}

fn resolve_table<'a>(
    table: &TableRef,
    schemas: &'a [CatalogTableSchema],
) -> Result<RelationBinding<'a>, CatalogRelationSchemaError> {
    if !table.partitions.is_empty() {
        return Err(CatalogRelationSchemaError::UnsupportedTableOption {
            table: table.name.join("."),
            option: "PARTITION",
        });
    }
    if table.as_of.is_some() {
        return Err(CatalogRelationSchemaError::UnsupportedTableOption {
            table: table.name.join("."),
            option: "AS OF TIMESTAMP",
        });
    }
    if !table.hints.is_empty() {
        return Err(CatalogRelationSchemaError::UnsupportedTableOption {
            table: table.name.join("."),
            option: "INDEX HINT",
        });
    }
    if table.sample.is_some() {
        return Err(CatalogRelationSchemaError::UnsupportedTableOption {
            table: table.name.join("."),
            option: "TABLESAMPLE",
        });
    }
    let candidates = match table.name.as_slice() {
        [table_name] => schemas
            .iter()
            .filter(|schema| schema.table.eq_ignore_ascii_case(table_name))
            .collect::<Vec<_>>(),
        [database, table_name] => schemas
            .iter()
            .filter(|schema| {
                schema.database.eq_ignore_ascii_case(database)
                    && schema.table.eq_ignore_ascii_case(table_name)
            })
            .collect::<Vec<_>>(),
        _ => {
            return Err(CatalogRelationSchemaError::InvalidIdentifierPath {
                path: table.name.clone(),
            })
        }
    };
    let schema = match candidates.as_slice() {
        [] => {
            return Err(CatalogRelationSchemaError::MissingTable {
                table: table.name.join("."),
            })
        }
        [schema] => *schema,
        _ => {
            return Err(CatalogRelationSchemaError::AmbiguousTable {
                table: table.name.join("."),
            })
        }
    };
    if table.alias.as_deref().is_some_and(str::is_empty) {
        return Err(CatalogRelationSchemaError::InvalidIdentifierPath {
            path: table.name.clone(),
        });
    }
    let qualifier = table
        .alias
        .as_deref()
        .filter(|alias| !alias.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| schema.table.clone());
    Ok(RelationBinding { schema, qualifier })
}

fn ensure_unique_qualifiers(
    relations: &[RelationBinding<'_>],
) -> Result<(), CatalogRelationSchemaError> {
    for (index, relation) in relations.iter().enumerate() {
        if relations[..index]
            .iter()
            .any(|other| other.qualifier.eq_ignore_ascii_case(&relation.qualifier))
        {
            return Err(CatalogRelationSchemaError::DuplicateQualifier {
                qualifier: relation.qualifier.clone(),
            });
        }
    }
    Ok(())
}

fn wildcard_relations<'a, 's>(
    path: &[String],
    relations: &'a [RelationBinding<'s>],
) -> Result<Vec<&'a RelationBinding<'s>>, CatalogRelationSchemaError> {
    match path {
        [] => Ok(relations.iter().collect()),
        [qualifier] => relation_by_qualifier(relations, qualifier).map(|relation| vec![relation]),
        [database, qualifier] => {
            qualified_relation(relations, database, qualifier, &path.join("."))
                .map(|relation| vec![relation])
        }
        _ => Err(CatalogRelationSchemaError::InvalidIdentifierPath {
            path: path.to_vec(),
        }),
    }
}

fn relation_by_qualifier<'a, 's>(
    relations: &'a [RelationBinding<'s>],
    qualifier: &str,
) -> Result<&'a RelationBinding<'s>, CatalogRelationSchemaError> {
    relations
        .iter()
        .find(|relation| relation.qualifier.eq_ignore_ascii_case(qualifier))
        .ok_or_else(|| CatalogRelationSchemaError::UnknownQualifier {
            qualifier: qualifier.to_owned(),
        })
}

fn resolve_column_path<'a, 's>(
    path: &[String],
    relations: &'a [RelationBinding<'s>],
) -> Result<(&'a RelationBinding<'s>, &'s CatalogColumn), CatalogRelationSchemaError> {
    let column_name = match path {
        [column] | [_, column] | [_, _, column] => column,
        _ => {
            return Err(CatalogRelationSchemaError::InvalidIdentifierPath {
                path: path.to_vec(),
            })
        }
    };
    match path {
        [column] => {
            let matches = relations
                .iter()
                .filter_map(|relation| {
                    relation
                        .schema
                        .columns
                        .iter()
                        .find(|candidate| candidate.name.eq_ignore_ascii_case(column))
                        .map(|candidate| (relation, candidate))
                })
                .collect::<Vec<_>>();
            match matches.as_slice() {
                [] => Err(CatalogRelationSchemaError::MissingColumn {
                    qualifier: None,
                    column: (*column).clone(),
                }),
                [(relation, candidate)] => Ok((*relation, *candidate)),
                _ => Err(CatalogRelationSchemaError::AmbiguousColumn {
                    column: (*column).clone(),
                    qualifiers: matches
                        .iter()
                        .map(|(relation, _)| relation.qualifier.clone())
                        .collect(),
                }),
            }
        }
        [qualifier, _] => {
            let relation = relation_by_qualifier(relations, qualifier)?;
            let column = relation
                .schema
                .columns
                .iter()
                .find(|candidate| candidate.name.eq_ignore_ascii_case(column_name))
                .ok_or_else(|| CatalogRelationSchemaError::MissingColumn {
                    qualifier: Some((*qualifier).clone()),
                    column: (*column_name).clone(),
                })?;
            Ok((relation, column))
        }
        [database, qualifier, _] => {
            let relation = qualified_relation(
                relations,
                database,
                qualifier,
                &format!("{database}.{qualifier}"),
            )?;
            let column = relation
                .schema
                .columns
                .iter()
                .find(|candidate| candidate.name.eq_ignore_ascii_case(column_name))
                .ok_or_else(|| CatalogRelationSchemaError::MissingColumn {
                    qualifier: Some(format!("{database}.{qualifier}")),
                    column: (*column_name).clone(),
                })?;
            Ok((relation, column))
        }
        _ => unreachable!("column path length validated above"),
    }
}

fn qualified_relation<'a, 's>(
    relations: &'a [RelationBinding<'s>],
    database: &str,
    qualifier: &str,
    display: &str,
) -> Result<&'a RelationBinding<'s>, CatalogRelationSchemaError> {
    let matches = relations
        .iter()
        .filter(|relation| {
            relation.schema.database.eq_ignore_ascii_case(database)
                && (relation.qualifier.eq_ignore_ascii_case(qualifier)
                    || relation.schema.table.eq_ignore_ascii_case(qualifier))
        })
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [] => Err(CatalogRelationSchemaError::UnknownQualifier {
            qualifier: display.to_owned(),
        }),
        [relation] => Ok(*relation),
        _ => Err(CatalogRelationSchemaError::AmbiguousQualifier {
            qualifier: display.to_owned(),
        }),
    }
}

fn resolve_column(
    relation: &RelationBinding<'_>,
    column: &CatalogColumn,
    alias: Option<&str>,
) -> ResolvedResultField {
    let original_column = IdentifierMetadata::new(column.name.clone());
    ResolvedResultField {
        names: FieldNameMetadata {
            original_table: IdentifierMetadata::new(relation.schema.table.clone()),
            original_column: original_column.clone(),
            database: IdentifierMetadata::new(relation.schema.database.clone()),
            table: IdentifierMetadata::new(relation.qualifier.clone()),
            column: IdentifierMetadata::new(alias.unwrap_or(&column.name)),
        },
        field_type: column.field_type.clone(),
    }
}
