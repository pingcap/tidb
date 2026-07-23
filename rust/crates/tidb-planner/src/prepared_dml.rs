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

//! Prepared signed-`BIGINT` INSERT and point UPDATE templates.
//!
//! These are the only write shapes the configured node admits. A template owns
//! no SQL text and no storage: binding positional non-NULL `MYSQL_TYPE_LONGLONG`
//! values yields a storage-neutral [`ConfiguredPreparedWrite`] that the executor
//! lowers into encoded mutations. Every unsupported INSERT/UPDATE feature is
//! rejected here, before a timestamp or any TiKV work exists.

use std::{error::Error, fmt};

use tidb_ast::{
    BinaryOp, DeleteKind, DeleteStmt, DmlStmt, Expr, InsertStmt, Stmt, TableRef, UpdateKind,
    UpdateStmt,
};

use crate::{
    configured_catalog::{ConfiguredCatalog, ConfiguredTableLookupError},
    read_only_scan::{fold_identifier, ConfiguredColumnKind, ConfiguredTable},
};

/// Maximum `VALUES` rows admitted by one prepared INSERT template.
///
/// One row is one transaction mutation, so this checked process limit stays
/// well inside the transaction coordinator's own mutation bound.
pub const MAX_PREPARED_INSERT_ROWS: usize = 128;

/// Why a parsed DML statement cannot become a configured prepared template.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PreparedWritePlanError {
    /// The statement's table name did not resolve to one configured entry.
    Catalog(ConfiguredTableLookupError),
    /// A qualified name has more parts than `schema.table`.
    MalformedTableName(String),
    /// The statement uses a DML feature outside the configured write boundary.
    Unsupported(UnsupportedPreparedWrite),
    /// The statement names a column outside the configured table.
    UnknownColumn(String),
    /// An INSERT column list must name every configured column exactly once.
    InsertColumnCoverage {
        /// Number of configured columns on the resolved table.
        configured: usize,
        /// Number of columns named by the statement.
        named: usize,
    },
    /// One INSERT column is named more than once.
    DuplicateInsertColumn(String),
    /// The `VALUES` row count is zero or above the checked process limit.
    InsertRowCount {
        /// Number of `VALUES` rows written by the statement.
        rows: usize,
        /// Maximum admitted rows.
        limit: usize,
    },
    /// One `VALUES` row has a different arity than the column list.
    InsertRowArity {
        /// Zero-based source position of the offending row.
        row: usize,
        /// Number of expressions in that row.
        values: usize,
        /// Number of named columns.
        columns: usize,
    },
    /// An admitted value position is not a parameter marker at its exact
    /// left-to-right source position.
    MarkerPosition {
        /// Position required by left-to-right source order.
        expected: usize,
        /// Position actually carried by the parsed marker, if it was one.
        found: Option<usize>,
    },
    /// UPDATE writes exactly one configured stored column.
    UpdateAssignmentCount(usize),
    /// UPDATE cannot move a row: its clustered handle is not assignable.
    UpdateClusteredHandle(String),
    /// The `SET` expression is neither `?` nor `<same column> + ?`.
    UpdateAssignmentShape,
    /// A point UPDATE/DELETE requires exactly one clustered-primary-key
    /// equality against a marker in `WHERE`.
    PointHandlePredicate,
}

impl fmt::Display for PreparedWritePlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Catalog(error) => write!(formatter, "prepared write table rejected: {error}"),
            Self::MalformedTableName(name) => {
                write!(formatter, "unsupported qualified table name: {name}")
            }
            Self::Unsupported(feature) => {
                write!(formatter, "unsupported prepared write feature: {feature:?}")
            }
            Self::UnknownColumn(column) => write!(formatter, "unknown column: {column}"),
            Self::InsertColumnCoverage { configured, named } => write!(
                formatter,
                "prepared INSERT must name all {configured} configured columns, found {named}"
            ),
            Self::DuplicateInsertColumn(column) => {
                write!(formatter, "prepared INSERT names column {column} twice")
            }
            Self::InsertRowCount { rows, limit } => write!(
                formatter,
                "prepared INSERT admits 1 to {limit} VALUES rows, found {rows}"
            ),
            Self::InsertRowArity {
                row,
                values,
                columns,
            } => write!(
                formatter,
                "prepared INSERT row {row} has {values} values for {columns} columns"
            ),
            Self::MarkerPosition { expected, found } => match found {
                Some(found) => write!(
                    formatter,
                    "prepared write requires marker position {expected}, found {found}"
                ),
                None => write!(
                    formatter,
                    "prepared write requires a parameter marker at position {expected}"
                ),
            },
            Self::UpdateAssignmentCount(count) => write!(
                formatter,
                "prepared UPDATE requires exactly one assignment, found {count}"
            ),
            Self::UpdateClusteredHandle(column) => write!(
                formatter,
                "prepared UPDATE cannot assign clustered primary key {column}"
            ),
            Self::UpdateAssignmentShape => formatter.write_str(
                "prepared UPDATE assigns either ? or the same column plus ? to a stored column",
            ),
            Self::PointHandlePredicate => formatter.write_str(
                "prepared point write requires one clustered primary-key equality against a marker",
            ),
        }
    }
}

impl Error for PreparedWritePlanError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Catalog(error) => Some(error),
            _ => None,
        }
    }
}

/// A parsed DML feature with no owner in the configured write boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnsupportedPreparedWrite {
    /// `REPLACE INTO`.
    Replace,
    /// `INSERT IGNORE` / `UPDATE IGNORE`.
    Ignore,
    /// `ON DUPLICATE KEY UPDATE`.
    OnDuplicateKey,
    /// `INSERT ... SET col = value`.
    SetSyntax,
    /// `INSERT ... SELECT`.
    InsertSelect,
    /// An explicit `PARTITION (...)` clause.
    Partition,
    /// Optimizer hints.
    Hint,
    /// A DML `RETURNING` result projection.
    Returning,
    /// An INSERT row alias or column alias list.
    InsertRowAlias,
    /// An INSERT with no explicit column list.
    MissingInsertColumns,
    /// A multi-table UPDATE.
    MultiTableUpdate,
    /// A table alias on a prepared write target.
    TableAlias,
    /// `AS OF TIMESTAMP` on a write target.
    AsOfTimestamp,
    /// `ORDER BY` on UPDATE.
    OrderBy,
    /// `LIMIT` on UPDATE.
    Limit,
    /// An UPDATE with no `WHERE` clause.
    MissingWhere,
    /// The prepared statement is not DML at all.
    NonDmlStatement,
    /// A multi-table `DELETE` (single-table point DELETE is supported).
    MultiTableDelete,
    /// A `WITH ... <DML>` common-table-expression prefix.
    CommonTableExpression,
    /// `LOAD DATA`, `IMPORT INTO`, or a non-transactional `BATCH` wrapper.
    UnsupportedDmlStatement,
}

/// One positional value bound into a prepared write, before storage encoding.
///
/// The planner stays storage- and charset-agnostic: it carries a signed integer
/// or raw string bytes and lets `tidb-exec` map each to its target column's
/// codec value. This mirrors Go binding `param.BinaryParam` values into an
/// execution plan without the planning layer knowing the row encoding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PreparedBindValue {
    /// A signed integer parameter, already sign-extended to 64 bits.
    Int(i64),
    /// Raw string/bytes parameter content (a `CHAR`/`VARCHAR` value).
    Bytes(Vec<u8>),
}

/// Why bound execute values cannot produce a typed write command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PreparedWriteBindError {
    /// The execute packet supplied a different number of values.
    ParameterCount {
        /// Number of markers owned by the template.
        expected: usize,
        /// Number of values supplied by the caller.
        found: usize,
    },
    /// A position that requires a signed integer received string bytes.
    NonIntegerParameter {
        /// Zero-based marker position within this statement.
        position: usize,
    },
    /// A position that requires string bytes (a `CHAR` assignment) received an
    /// integer.
    NonStringParameter {
        /// Zero-based marker position within this statement.
        position: usize,
    },
}

impl fmt::Display for PreparedWriteBindError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ParameterCount { expected, found } => write!(
                formatter,
                "prepared write requires exactly {expected} signed BIGINT parameters, found {found}"
            ),
            Self::NonIntegerParameter { position } => write!(
                formatter,
                "prepared write parameter {position} requires a signed integer value"
            ),
            Self::NonStringParameter { position } => write!(
                formatter,
                "prepared write parameter {position} requires a string value"
            ),
        }
    }
}

impl Error for PreparedWriteBindError {}

/// One admitted prepared write template.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfiguredPreparedWriteTemplate {
    /// `INSERT INTO t (<all configured columns>) VALUES (?, ...)[, (?, ...)]`.
    Insert(ConfiguredPreparedInsertTemplate),
    /// `UPDATE t SET <stored column> = ? | <same column> + ? WHERE <handle> = ?`.
    Update(ConfiguredPreparedUpdateTemplate),
    /// `DELETE FROM t WHERE <handle> = ?`.
    Delete(ConfiguredPreparedDeleteTemplate),
}

impl ConfiguredPreparedWriteTemplate {
    /// Number of positional markers the execute packet must supply.
    #[must_use]
    pub fn parameter_count(&self) -> usize {
        match self {
            Self::Insert(template) => template.parameter_count(),
            Self::Update(template) => template.parameter_count(),
            Self::Delete(template) => template.parameter_count(),
        }
    }

    /// The resolved configured target table.
    #[must_use]
    pub const fn table(&self) -> &ConfiguredTable {
        match self {
            Self::Insert(template) => &template.table,
            Self::Update(template) => &template.table,
            Self::Delete(template) => &template.table,
        }
    }

    /// Binds positional execute values. INSERT carries each value through to the
    /// codec (which validates it against its column type); UPDATE requires signed
    /// integers for its assignment and clustered handle; DELETE requires the
    /// signed clustered handle.
    pub fn bind(
        &self,
        params: &[PreparedBindValue],
    ) -> Result<ConfiguredPreparedWrite, PreparedWriteBindError> {
        match self {
            Self::Insert(template) => template.bind(params),
            Self::Update(template) => template.bind(params),
            Self::Delete(template) => template.bind(params),
        }
    }
}

/// Extracts the signed integer a position requires, rejecting string bytes.
fn expect_integer(
    value: &PreparedBindValue,
    position: usize,
) -> Result<i64, PreparedWriteBindError> {
    match value {
        PreparedBindValue::Int(value) => Ok(*value),
        PreparedBindValue::Bytes(_) => {
            Err(PreparedWriteBindError::NonIntegerParameter { position })
        }
    }
}

/// Extracts the raw string bytes a position requires, rejecting an integer.
fn expect_bytes(
    value: &PreparedBindValue,
    position: usize,
) -> Result<Vec<u8>, PreparedWriteBindError> {
    match value {
        PreparedBindValue::Bytes(bytes) => Ok(bytes.clone()),
        PreparedBindValue::Int(_) => Err(PreparedWriteBindError::NonStringParameter { position }),
    }
}

/// A validated multi-row INSERT template over the configured columns.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfiguredPreparedInsertTemplate {
    table: ConfiguredTable,
    /// Configured column index for each named INSERT column, in source order.
    columns: Vec<usize>,
    rows: usize,
}

impl ConfiguredPreparedInsertTemplate {
    /// Number of positional markers across every `VALUES` row.
    #[must_use]
    pub const fn parameter_count(&self) -> usize {
        self.rows * self.columns.len()
    }

    /// Number of `VALUES` rows this template inserts.
    #[must_use]
    pub const fn rows(&self) -> usize {
        self.rows
    }

    /// Configured column indices in statement order.
    #[must_use]
    pub fn columns(&self) -> &[usize] {
        &self.columns
    }

    fn bind(
        &self,
        params: &[PreparedBindValue],
    ) -> Result<ConfiguredPreparedWrite, PreparedWriteBindError> {
        let expected = self.parameter_count();
        if params.len() != expected {
            return Err(PreparedWriteBindError::ParameterCount {
                expected,
                found: params.len(),
            });
        }
        let rows = params
            .chunks_exact(self.columns.len())
            .map(|values| ConfiguredInsertRow {
                values: self
                    .columns
                    .iter()
                    .zip(values)
                    .map(|(column_index, value)| (*column_index, value.clone()))
                    .collect(),
            })
            .collect();
        Ok(ConfiguredPreparedWrite::InsertRows {
            table: self.table.clone(),
            rows,
        })
    }
}

/// A validated point UPDATE template against the clustered handle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfiguredPreparedUpdateTemplate {
    table: ConfiguredTable,
    column_index: usize,
    assignment: PreparedAssignmentShape,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PreparedAssignmentShape {
    /// `SET int_column = ?`.
    SetInt,
    /// `SET int_column = int_column + ?`.
    Add,
    /// `SET char_column = ?`.
    SetBytes,
}

impl ConfiguredPreparedUpdateTemplate {
    /// A point UPDATE binds the assigned value and then the handle.
    #[must_use]
    pub const fn parameter_count(&self) -> usize {
        2
    }

    /// Configured index of the assigned stored column.
    #[must_use]
    pub const fn column_index(&self) -> usize {
        self.column_index
    }

    fn bind(
        &self,
        params: &[PreparedBindValue],
    ) -> Result<ConfiguredPreparedWrite, PreparedWriteBindError> {
        let [value, handle] = params else {
            return Err(PreparedWriteBindError::ParameterCount {
                expected: 2,
                found: params.len(),
            });
        };
        // The clustered handle at position 1 is always a signed integer; the
        // assigned value at position 0 follows the target column's type, resolved
        // to a shape at lowering time.
        let handle = expect_integer(handle, 1)?;
        let assignment = match self.assignment {
            PreparedAssignmentShape::SetInt => ConfiguredAssignment::Set(expect_integer(value, 0)?),
            PreparedAssignmentShape::Add => ConfiguredAssignment::Add(expect_integer(value, 0)?),
            PreparedAssignmentShape::SetBytes => {
                ConfiguredAssignment::SetBytes(expect_bytes(value, 0)?)
            }
        };
        Ok(ConfiguredPreparedWrite::UpdatePoint {
            table: self.table.clone(),
            handle,
            column_index: self.column_index,
            assignment,
        })
    }
}

/// A validated point DELETE template against the clustered handle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfiguredPreparedDeleteTemplate {
    table: ConfiguredTable,
}

impl ConfiguredPreparedDeleteTemplate {
    /// A point DELETE binds only the clustered handle.
    #[must_use]
    pub const fn parameter_count(&self) -> usize {
        1
    }

    fn bind(
        &self,
        params: &[PreparedBindValue],
    ) -> Result<ConfiguredPreparedWrite, PreparedWriteBindError> {
        let [handle] = params else {
            return Err(PreparedWriteBindError::ParameterCount {
                expected: 1,
                found: params.len(),
            });
        };
        // The clustered handle binds at position 0 and targets an integer PK,
        // so a string parameter is a type error.
        let handle = expect_integer(handle, 0)?;
        Ok(ConfiguredPreparedWrite::DeletePoint {
            table: self.table.clone(),
            handle,
        })
    }
}

/// One fully bound INSERT row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfiguredInsertRow {
    values: Vec<(usize, PreparedBindValue)>,
}

impl ConfiguredInsertRow {
    /// Bound `(configured column index, value)` pairs in statement order.
    #[must_use]
    pub fn values(&self) -> &[(usize, PreparedBindValue)] {
        &self.values
    }
}

/// One bound UPDATE assignment, typed to the target column.
///
/// `SET int_col = ?` and `SET int_col = int_col + ?` carry a signed integer;
/// `SET char_col = ?` carries the raw string bytes. Arithmetic (`Add`) is only
/// ever an integer operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfiguredAssignment {
    /// Replace an integer column's stored value.
    Set(i64),
    /// Add to an integer column's stored value with checked signed arithmetic.
    Add(i64),
    /// Replace a `CHAR` column's stored value with the bound raw bytes.
    SetBytes(Vec<u8>),
}

/// A storage-neutral bound write command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfiguredPreparedWrite {
    /// Insert one or more complete configured rows.
    InsertRows {
        /// Resolved target table.
        table: ConfiguredTable,
        /// Bound rows in statement order.
        rows: Vec<ConfiguredInsertRow>,
    },
    /// Update one stored column of exactly one clustered handle.
    UpdatePoint {
        /// Resolved target table.
        table: ConfiguredTable,
        /// Bound clustered primary-key handle.
        handle: i64,
        /// Configured index of the assigned stored column.
        column_index: usize,
        /// Bound assignment.
        assignment: ConfiguredAssignment,
    },
    /// Delete exactly one clustered handle's row.
    DeletePoint {
        /// Resolved target table.
        table: ConfiguredTable,
        /// Bound clustered primary-key handle.
        handle: i64,
    },
}

/// Lowers any parsed statement into one configured prepared write template.
///
/// This is the single admission seam for prepared DML: everything the bounded
/// write boundary does not own is rejected here, before a prepared handle
/// exists.
pub fn lower_prepared_write(
    statement: &Stmt,
    catalog: &ConfiguredCatalog,
) -> Result<ConfiguredPreparedWriteTemplate, PreparedWritePlanError> {
    let Stmt::Dml(dml) = statement else {
        return Err(unsupported(UnsupportedPreparedWrite::NonDmlStatement));
    };
    match dml.as_ref() {
        DmlStmt::Insert(insert) => {
            lower_prepared_insert(insert, catalog).map(ConfiguredPreparedWriteTemplate::Insert)
        }
        DmlStmt::Update(update) => {
            lower_prepared_update(update, catalog).map(ConfiguredPreparedWriteTemplate::Update)
        }
        DmlStmt::Delete(delete) => {
            lower_prepared_delete(delete, catalog).map(ConfiguredPreparedWriteTemplate::Delete)
        }
        DmlStmt::With { .. } => Err(unsupported(UnsupportedPreparedWrite::CommonTableExpression)),
        DmlStmt::ImportInto(_)
        | DmlStmt::LoadData(_)
        | DmlStmt::Batch(_)
        | DmlStmt::DistributeTable(_)
        | DmlStmt::Call(_) => Err(unsupported(
            UnsupportedPreparedWrite::UnsupportedDmlStatement,
        )),
    }
}

/// Lowers a parsed INSERT into one configured prepared template.
pub fn lower_prepared_insert(
    statement: &InsertStmt,
    catalog: &ConfiguredCatalog,
) -> Result<ConfiguredPreparedInsertTemplate, PreparedWritePlanError> {
    if statement.replace {
        return Err(unsupported(UnsupportedPreparedWrite::Replace));
    }
    if statement.ignore {
        return Err(unsupported(UnsupportedPreparedWrite::Ignore));
    }
    if !statement.on_duplicate.is_empty() {
        return Err(unsupported(UnsupportedPreparedWrite::OnDuplicateKey));
    }
    if statement.set_syntax || !statement.set_columns.is_empty() {
        return Err(unsupported(UnsupportedPreparedWrite::SetSyntax));
    }
    if statement.source.is_some() {
        return Err(unsupported(UnsupportedPreparedWrite::InsertSelect));
    }
    if !statement.partitions.is_empty() {
        return Err(unsupported(UnsupportedPreparedWrite::Partition));
    }
    if !statement.hints.is_empty() {
        return Err(unsupported(UnsupportedPreparedWrite::Hint));
    }
    if !statement.returning.is_empty() {
        return Err(unsupported(UnsupportedPreparedWrite::Returning));
    }
    if statement.row_alias.is_some() || !statement.column_aliases.is_empty() {
        return Err(unsupported(UnsupportedPreparedWrite::InsertRowAlias));
    }
    if statement.columns.is_empty() {
        return Err(unsupported(UnsupportedPreparedWrite::MissingInsertColumns));
    }

    let table = resolve_write_table(&statement.table, catalog)?;
    let columns = resolve_insert_columns(&statement.columns, table)?;

    let rows = statement.rows.len();
    if rows == 0 || rows > MAX_PREPARED_INSERT_ROWS {
        return Err(PreparedWritePlanError::InsertRowCount {
            rows,
            limit: MAX_PREPARED_INSERT_ROWS,
        });
    }

    let mut position = 0;
    for (row_index, row) in statement.rows.iter().enumerate() {
        if row.len() != columns.len() {
            return Err(PreparedWritePlanError::InsertRowArity {
                row: row_index,
                values: row.len(),
                columns: columns.len(),
            });
        }
        for value in row {
            expect_marker(value, position)?;
            position += 1;
        }
    }

    Ok(ConfiguredPreparedInsertTemplate {
        table: table.clone(),
        columns,
        rows,
    })
}

/// Lowers a parsed UPDATE into one configured prepared point template.
pub fn lower_prepared_update(
    statement: &UpdateStmt,
    catalog: &ConfiguredCatalog,
) -> Result<ConfiguredPreparedUpdateTemplate, PreparedWritePlanError> {
    if statement.ignore {
        return Err(unsupported(UnsupportedPreparedWrite::Ignore));
    }
    if !statement.hints.is_empty() {
        return Err(unsupported(UnsupportedPreparedWrite::Hint));
    }
    if !statement.order_by.is_empty() {
        return Err(unsupported(UnsupportedPreparedWrite::OrderBy));
    }
    if statement.limit.is_some() {
        return Err(unsupported(UnsupportedPreparedWrite::Limit));
    }
    if !statement.returning.is_empty() {
        return Err(unsupported(UnsupportedPreparedWrite::Returning));
    }
    let UpdateKind::Single(table_ref) = &statement.kind else {
        return Err(unsupported(UnsupportedPreparedWrite::MultiTableUpdate));
    };
    validate_write_table_ref(table_ref)?;
    let table = resolve_write_table(&table_ref.name, catalog)?;

    let [assignment] = statement.assignments.as_slice() else {
        return Err(PreparedWritePlanError::UpdateAssignmentCount(
            statement.assignments.len(),
        ));
    };
    let (column_index, column) = resolve_write_column(&assignment.col, table_ref, table)?;
    if column.kind() == ConfiguredColumnKind::ClusteredPrimaryKey {
        return Err(PreparedWritePlanError::UpdateClusteredHandle(
            column.name().to_owned(),
        ));
    }

    let shape = match &assignment.value {
        Expr::ParamMarker { order, .. } => {
            expect_position(*order, 0)?;
            // The set value's type follows the target column: an integer column
            // binds a signed integer, a CHAR column raw string bytes.
            if column.scalar_type().integer_range().is_some() {
                PreparedAssignmentShape::SetInt
            } else {
                PreparedAssignmentShape::SetBytes
            }
        }
        Expr::Binary(BinaryOp::Plus, left, right) => {
            // `column + ?` is signed integer arithmetic; a non-integer column has
            // no such assignment in this bounded write path.
            if column.scalar_type().integer_range().is_none() {
                return Err(PreparedWritePlanError::UpdateAssignmentShape);
            }
            let Expr::Column(path) = left.as_ref() else {
                return Err(PreparedWritePlanError::UpdateAssignmentShape);
            };
            let (addend_index, _) = resolve_write_column(path, table_ref, table)?;
            if addend_index != column_index {
                return Err(PreparedWritePlanError::UpdateAssignmentShape);
            }
            expect_marker(right, 0)?;
            PreparedAssignmentShape::Add
        }
        _ => return Err(PreparedWritePlanError::UpdateAssignmentShape),
    };

    let Some(predicate) = &statement.where_clause else {
        return Err(unsupported(UnsupportedPreparedWrite::MissingWhere));
    };
    // The UPDATE binds its assigned value at position 0, so the handle marker
    // is position 1.
    validate_handle_equality(predicate, table_ref, table, 1)?;

    Ok(ConfiguredPreparedUpdateTemplate {
        table: table.clone(),
        column_index,
        assignment: shape,
    })
}

/// Lowers a parsed DELETE into one configured prepared template.
///
/// Mirrors Go `pkg/executor/delete.go`'s single-table point delete: exactly one
/// clustered-primary-key equality against a marker, deleting that one row.
pub fn lower_prepared_delete(
    statement: &DeleteStmt,
    catalog: &ConfiguredCatalog,
) -> Result<ConfiguredPreparedDeleteTemplate, PreparedWritePlanError> {
    if statement.ignore {
        return Err(unsupported(UnsupportedPreparedWrite::Ignore));
    }
    if !statement.hints.is_empty() {
        return Err(unsupported(UnsupportedPreparedWrite::Hint));
    }
    if !statement.order_by.is_empty() {
        return Err(unsupported(UnsupportedPreparedWrite::OrderBy));
    }
    if statement.limit.is_some() {
        return Err(unsupported(UnsupportedPreparedWrite::Limit));
    }
    if !statement.returning.is_empty() {
        return Err(unsupported(UnsupportedPreparedWrite::Returning));
    }
    let DeleteKind::Single(table_ref) = &statement.kind else {
        return Err(unsupported(UnsupportedPreparedWrite::MultiTableDelete));
    };
    validate_write_table_ref(table_ref)?;
    let table = resolve_write_table(&table_ref.name, catalog)?;

    let Some(predicate) = &statement.where_clause else {
        return Err(unsupported(UnsupportedPreparedWrite::MissingWhere));
    };
    // The DELETE binds only the clustered handle, at position 0.
    validate_handle_equality(predicate, table_ref, table, 0)?;

    Ok(ConfiguredPreparedDeleteTemplate {
        table: table.clone(),
    })
}

fn validate_handle_equality(
    predicate: &Expr,
    table_ref: &TableRef,
    table: &ConfiguredTable,
    handle_position: usize,
) -> Result<(), PreparedWritePlanError> {
    let Expr::Binary(BinaryOp::Eq, left, right) = predicate else {
        return Err(PreparedWritePlanError::PointHandlePredicate);
    };
    let (path, marker) = match (left.as_ref(), right.as_ref()) {
        (Expr::Column(path), marker) => (path, marker),
        (marker, Expr::Column(path)) => (path, marker),
        _ => return Err(PreparedWritePlanError::PointHandlePredicate),
    };
    let (_, column) = resolve_write_column(path, table_ref, table)?;
    if column.kind() != ConfiguredColumnKind::ClusteredPrimaryKey {
        return Err(PreparedWritePlanError::PointHandlePredicate);
    }
    expect_marker(marker, handle_position)
}

fn resolve_write_table<'a>(
    name: &[String],
    catalog: &'a ConfiguredCatalog,
) -> Result<&'a ConfiguredTable, PreparedWritePlanError> {
    let (schema, table) = match name {
        [table] => (None, table.as_str()),
        [schema, table] => (Some(schema.as_str()), table.as_str()),
        _ => {
            return Err(PreparedWritePlanError::MalformedTableName(name.join(".")));
        }
    };
    catalog
        .resolve_table(schema, table)
        .map_err(PreparedWritePlanError::Catalog)
}

fn validate_write_table_ref(table_ref: &TableRef) -> Result<(), PreparedWritePlanError> {
    if !table_ref.partitions.is_empty() {
        return Err(unsupported(UnsupportedPreparedWrite::Partition));
    }
    if table_ref
        .alias
        .as_deref()
        .is_some_and(|alias| !alias.is_empty())
    {
        return Err(unsupported(UnsupportedPreparedWrite::TableAlias));
    }
    if table_ref.as_of.is_some() {
        return Err(unsupported(UnsupportedPreparedWrite::AsOfTimestamp));
    }
    if !table_ref.hints.is_empty() {
        return Err(unsupported(UnsupportedPreparedWrite::Hint));
    }
    Ok(())
}

fn resolve_insert_columns(
    names: &[String],
    table: &ConfiguredTable,
) -> Result<Vec<usize>, PreparedWritePlanError> {
    // Every configured column is `NOT NULL` with no default, so a prepared
    // INSERT must supply all of them exactly once; order may differ from the
    // catalog because MySQL binds by the written column list.
    if names.len() != table.columns().len() {
        return Err(PreparedWritePlanError::InsertColumnCoverage {
            configured: table.columns().len(),
            named: names.len(),
        });
    }
    let mut columns = Vec::with_capacity(names.len());
    for name in names {
        let index = configured_column_index(name, table)?;
        if columns.contains(&index) {
            return Err(PreparedWritePlanError::DuplicateInsertColumn(name.clone()));
        }
        columns.push(index);
    }
    Ok(columns)
}

fn resolve_write_column<'a>(
    path: &[String],
    table_ref: &TableRef,
    table: &'a ConfiguredTable,
) -> Result<(usize, &'a crate::read_only_scan::ConfiguredColumn), PreparedWritePlanError> {
    let column_name = match path {
        [column] => column.as_str(),
        [qualifier, column] if identifier_eq(qualifier, visible_table_name(table_ref, table)) => {
            column.as_str()
        }
        [schema, table_name, column]
            if identifier_eq(schema, table.schema())
                && identifier_eq(table_name, table.table()) =>
        {
            column.as_str()
        }
        _ => return Err(PreparedWritePlanError::UnknownColumn(path.join("."))),
    };
    let index = configured_column_index(column_name, table)?;
    Ok((index, &table.columns()[index]))
}

fn visible_table_name<'a>(table_ref: &'a TableRef, table: &'a ConfiguredTable) -> &'a str {
    table_ref
        .alias
        .as_deref()
        .filter(|alias| !alias.is_empty())
        .unwrap_or_else(|| table.table())
}

fn configured_column_index(
    name: &str,
    table: &ConfiguredTable,
) -> Result<usize, PreparedWritePlanError> {
    table
        .columns()
        .iter()
        .position(|column| identifier_eq(column.name(), name))
        .ok_or_else(|| PreparedWritePlanError::UnknownColumn(name.to_owned()))
}

fn expect_marker(expr: &Expr, expected: usize) -> Result<(), PreparedWritePlanError> {
    let Expr::ParamMarker { order, .. } = expr else {
        return Err(PreparedWritePlanError::MarkerPosition {
            expected,
            found: None,
        });
    };
    expect_position(*order, expected)
}

fn expect_position(position: usize, expected: usize) -> Result<(), PreparedWritePlanError> {
    if position == expected {
        return Ok(());
    }
    Err(PreparedWritePlanError::MarkerPosition {
        expected,
        found: Some(position),
    })
}

fn identifier_eq(left: &str, right: &str) -> bool {
    fold_identifier(left) == fold_identifier(right)
}

const fn unsupported(feature: UnsupportedPreparedWrite) -> PreparedWritePlanError {
    PreparedWritePlanError::Unsupported(feature)
}
