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
//! query supplies a plain single-table projection. In addition to direct
//! columns, one bounded `COUNT(t.a) AS c` path validates its qualified argument
//! through the shared source-shaped field-name resolver before independently
//! consuming COUNT's already-translated fixed result type. It intentionally
//! does not claim that the planner descriptor is carried into execution, infer
//! a column type from a runtime value, or silently flatten a join/derived table
//! into one schema.

use std::fmt;

use tidb_ast::{ColumnType, Expr, JoinNode, SelectField, SelectStatementKind, SelectStmt};
use tidb_datatype::{
    Charset, Collation, FieldName, FieldNameMetadata as DatatypeFieldNameMetadata, FieldTypeCode,
    IdentifierMetadata as DatatypeIdentifierMetadata, QualifiedColumnName,
};
use tidb_expr::find_field_name;

use crate::result_field_resolver::{
    resolve_select_fields, ResolvedResultField, ResultFieldResolveError,
};
use crate::result_metadata::{
    FieldNameMetadata, IdentifierMetadata, ResultFieldTypeMetadata, UNSIGNED_FLAG,
};

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

    /// Builds a result-field schema from the executor's authoritative table
    /// snapshot. The stored `ColumnType` values remain the only source of
    /// field type, width, unsignedness, and collation metadata; row values are
    /// never inspected here.
    pub(crate) fn from_columns(
        database: impl Into<String>,
        table: impl Into<String>,
        names: &[String],
        types: &[ColumnType],
    ) -> Result<Self, CatalogSchemaError> {
        let table = table.into();
        if names.len() != types.len() {
            return Err(CatalogSchemaError::InvalidCatalogShape {
                table: table.clone(),
            });
        }
        let columns = names
            .iter()
            .zip(types)
            .map(|(name, column_type)| {
                column_type_metadata(column_type)
                    .map(|field_type| CatalogColumn::new(name, field_type))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self::new(database, table, columns))
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
    /// The bounded catalog-backed COUNT projection did not preserve the
    /// source-proven single-table/single-qualified-column shape.
    UnsupportedCountColumnShape,
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
            Self::UnsupportedCountColumnShape => f.write_str(
                "catalog COUNT metadata requires one plain qualified column over one table",
            ),
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

fn column_type_metadata(ty: &ColumnType) -> Result<ResultFieldTypeMetadata, CatalogSchemaError> {
    let name = ty.name.to_ascii_uppercase();
    let code = match name.as_str() {
        "TINYINT" => FieldTypeCode::Tiny,
        "SMALLINT" => FieldTypeCode::Short,
        "MEDIUMINT" => FieldTypeCode::Int24,
        "INT" | "INTEGER" => FieldTypeCode::Long,
        "BIGINT" => FieldTypeCode::LongLong,
        "FLOAT" => FieldTypeCode::Float,
        "DOUBLE" | "REAL" => FieldTypeCode::Double,
        "DECIMAL" | "NUMERIC" => FieldTypeCode::NewDecimal,
        "DATE" => FieldTypeCode::Date,
        "TIME" => FieldTypeCode::Duration,
        "DATETIME" => FieldTypeCode::Datetime,
        "TIMESTAMP" => FieldTypeCode::Timestamp,
        "YEAR" => FieldTypeCode::Year,
        "CHAR" | "BINARY" => FieldTypeCode::String,
        "VARCHAR" | "VARBINARY" => FieldTypeCode::VarString,
        "TINYTEXT" | "TINYBLOB" => FieldTypeCode::TinyBlob,
        "TEXT" | "BLOB" => FieldTypeCode::Blob,
        "MEDIUMTEXT" | "MEDIUMBLOB" => FieldTypeCode::MediumBlob,
        "LONGTEXT" | "LONGBLOB" => FieldTypeCode::LongBlob,
        "BIT" => FieldTypeCode::Bit,
        "JSON" => FieldTypeCode::Json,
        "ENUM" => FieldTypeCode::Enum,
        "SET" => FieldTypeCode::Set,
        "GEOMETRY" | "POINT" | "LINESTRING" | "POLYGON" | "MULTIPOINT" | "MULTILINESTRING"
        | "MULTIPOLYGON" | "GEOMETRYCOLLECTION" => FieldTypeCode::Geometry,
        "VECTOR" => FieldTypeCode::VectorFloat32,
        _ => {
            return Err(CatalogSchemaError::UnsupportedColumnType {
                type_name: ty.name.clone(),
            });
        }
    };

    let collation = if ty.binary || matches!(name.as_str(), "BINARY" | "VARBINARY") {
        Collation::Binary
    } else if let Some(charset) = ty.charset.as_deref() {
        Charset::from_name(charset)
            .map(Charset::default_collation)
            .ok_or_else(|| CatalogSchemaError::UnsupportedColumnType {
                type_name: format!("{} CHARACTER SET {charset}", ty.name),
            })?
    } else {
        Collation::DEFAULT
    };
    let flen = ty
        .args
        .first()
        .and_then(|arg| arg.as_text_lossy().parse::<u32>().ok());
    let decimal = ty
        .args
        .get(1)
        .and_then(|arg| arg.as_text_lossy().parse::<u8>().ok());
    Ok(ResultFieldTypeMetadata {
        code,
        flags: u16::from(ty.unsigned) * UNSIGNED_FLAG,
        flen,
        decimal,
        collation,
    })
}

impl std::error::Error for CatalogSchemaError {}

impl From<ResultFieldResolveError> for CatalogSchemaError {
    fn from(error: ResultFieldResolveError) -> Self {
        Self::ResultField(error)
    }
}

/// Returns whether `select` is the dependency-closed catalog COUNT slice.
///
/// This is deliberately a complete positive capability test rather than an
/// aggregate-purity classification. It admits only `SELECT COUNT(t.a) AS c
/// FROM table [AS t]`: one ordinary unqualified table, one non-distinct COUNT,
/// one table-qualified direct column, one explicit nonempty output alias, and
/// no clause that changes grouping, filtering, ordering, locking, or statement
/// state. The catalog resolver below separately proves that the bound column
/// exists. Database-qualified tables remain closed until the catalog key owns
/// a database identity instead of only the final table-name segment.
pub(crate) fn is_bounded_catalog_count_column_select(select: &SelectStmt) -> bool {
    if select.kind != SelectStatementKind::Select
        || select.is_in_braces
        || select.with.is_some()
        || !select.hints.is_empty()
        || select.calc_found_rows
        || select.distinct
        || select.all
        || !select.values.is_empty()
        || select.fields.len() != 1
        || select.where_clause.is_some()
        || !select.group_by.is_empty()
        || select.rollup
        || select.having.is_some()
        || !select.windows.is_empty()
        || !select.order_by.is_empty()
        || select.limit.is_some()
        || select.lock.is_some()
        || select.into_outfile.is_some()
    {
        return false;
    }

    let Some(from) = &select.from else {
        return false;
    };
    let JoinNode::Table(table_ref) = &from.left else {
        return false;
    };
    if from.right.is_some()
        || from.on.is_some()
        || !from.using.is_empty()
        || from.natural
        || from.straight
        || !table_ref.partitions.is_empty()
        || table_ref.as_of.is_some()
        || !table_ref.hints.is_empty()
        || table_ref.sample.is_some()
        || table_ref.name.len() != 1
        || table_ref.alias.as_ref().is_some_and(String::is_empty)
    {
        return false;
    }

    let SelectField::Expr { expr, alias } = &select.fields[0] else {
        return false;
    };
    if alias.as_ref().is_none_or(String::is_empty) {
        return false;
    }
    let Expr::Aggregate {
        name,
        distinct,
        args,
    } = expr
    else {
        return false;
    };
    if !name.eq_ignore_ascii_case("COUNT") || *distinct || args.len() != 1 {
        return false;
    }
    let Expr::Column(path) = &args[0] else {
        return false;
    };
    let [argument_qualifier, _] = path.as_slice() else {
        return false;
    };
    let table_name = table_ref.name.last().expect("checked non-empty table path");
    let table_qualifier = table_ref.alias.as_ref().unwrap_or(table_name);
    argument_qualifier.eq_ignore_ascii_case(table_qualifier)
}

/// Binds the qualified argument of the bounded catalog COUNT projection and
/// then delegates COUNT return typing/naming to the existing result-field
/// resolver. Binding happens first, mirroring the order of Go's
/// `FindFieldName` before `NewAggFuncDesc`/`typeInfer4Count`; no input column
/// type or runtime Datum is allowed to manufacture the aggregate's fixed
/// metadata. This bounded adapter does not pretend that Rust's independent
/// `AggFuncDesc` identity model is carried into the existing executor runtime.
pub(crate) fn resolve_catalog_count_column_select_field(
    select: &SelectStmt,
    schema: &CatalogTableSchema,
    default_collation: Collation,
) -> Result<ResolvedResultField, CatalogSchemaError> {
    if !is_bounded_catalog_count_column_select(select) {
        return Err(CatalogSchemaError::UnsupportedCountColumnShape);
    }
    let from = select.from.as_ref().expect("bounded COUNT requires FROM");
    let JoinNode::Table(table_ref) = &from.left else {
        unreachable!("bounded COUNT requires a table")
    };
    validate_table_ref(
        table_ref.name.as_slice(),
        table_ref.alias.as_deref(),
        schema,
    )?;

    let SelectField::Expr { expr, .. } = &select.fields[0] else {
        unreachable!("bounded COUNT requires one expression")
    };
    let Expr::Aggregate { args, .. } = expr else {
        unreachable!("bounded COUNT requires one aggregate")
    };
    let Expr::Column(path) = &args[0] else {
        unreachable!("bounded COUNT requires one column")
    };
    let [qualifier, column_name] = path.as_slice() else {
        unreachable!("bounded COUNT requires a qualified column")
    };
    let visible_table = table_ref.alias.as_deref().unwrap_or(&schema.table);
    let field_names = schema
        .columns
        .iter()
        .map(|column| {
            FieldName::new(DatatypeFieldNameMetadata {
                original_table: DatatypeIdentifierMetadata::new(schema.table.clone()),
                original_column: DatatypeIdentifierMetadata::new(column.name.clone()),
                database: DatatypeIdentifierMetadata::new(schema.database.clone()),
                table: DatatypeIdentifierMetadata::new(visible_table),
                column: DatatypeIdentifierMetadata::new(column.name.clone()),
            })
        })
        .collect::<Vec<_>>();
    let bound_column = QualifiedColumnName::new("", qualifier, column_name);
    match find_field_name(&field_names, &bound_column) {
        Ok(Some(_)) => {}
        Ok(None) => {
            return Err(CatalogSchemaError::MissingColumn {
                column: column_name.clone(),
            })
        }
        Err(error) => {
            return Err(CatalogSchemaError::AmbiguousColumn {
                message: error.to_string(),
            })
        }
    }

    let mut fields = resolve_select_fields(&select.fields, default_collation)?;
    Ok(fields
        .pop()
        .expect("bounded COUNT resolver returns exactly one field"))
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
