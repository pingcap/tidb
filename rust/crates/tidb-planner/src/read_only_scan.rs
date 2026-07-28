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

//! Narrow SQL-to-TiKV scan/Selection lowering for the read-only node.
//!
//! This boundary intentionally accepts exactly one configured,
//! non-partitioned table with signed `BIGINT` columns and exactly one clustered
//! primary key. It
//! validates the complete parsed query envelope before entering the existing
//! `LogicalDataSource -> TableAccessPath -> PhysicalTableReaderPlan` path.
//! Ordinary signed-`BIGINT` comparisons joined by `AND` are bound once. Exact
//! clustered-primary-key comparisons become logical table-handle ranges;
//! stored-column comparisons remain in the physical TiKV Selection. Every
//! other predicate shape fails closed.

#[path = "configured_catalog.rs"]
pub mod configured_catalog;

use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt,
};

use tidb_ast::{
    BinaryOp, Expr, JoinNode, JoinType, OrderItem, QueryStmt, SelectField, SelectStatementKind,
    SelectStmt, Stmt, TableRef, UnaryOp,
};
use tidb_datatype::{Collation, FieldType, FieldTypeCode};

use crate::{
    access_path::{
        DataSourceAccessPath, PointGetAdmission, ResolvedTableDescriptor, ResolvedTableScanKind,
        TableAccessPath, TableScanExplainIdSuffix,
    },
    aggregation_descriptor::AggregateKind,
    configured_order_limit_contract::ConfiguredOrderDirection,
    index_task::{ScanReadTask, ScanReadTaskRejection},
    logical_data_source::LogicalDataSource,
    logical_data_source_task::IndexTaskProperty,
    physical_selection::{
        BigIntComparison, ComparisonOp, ComparisonOperand, PhysicalSelectionError,
        PhysicalSelectionPlan,
    },
    physical_table_scan::PhysicalTableScanPlan,
    scan_pushdown::{ScanColumnInfo, TiKvTableScanSpec},
    signed_bigint_ranger::{detach_clustered_signed_bigint_ranges, SignedBigIntRange},
    task_type::TaskType,
};

const MYSQL_TYPE_LONG: i32 = 3;
const MYSQL_TYPE_LONGLONG: i32 = 8;
const BINARY_COLLATION_ID: i32 = 63;
const NOT_NULL_FLAG: i32 = 1;
const PRI_KEY_FLAG: i32 = 1 << 1;
const PHYSICAL_PLAN_ID: i32 = 1;

const MYSQL_TYPE_STRING: i32 = 254;
/// MySQL `TypeDouble`, per Go `mysql.TypeDouble`.
const MYSQL_TYPE_DOUBLE: i32 = 5;
/// `mysql.UnsignedFlag`, set on the coprocessor/result `ColumnInfo.Flag` for an
/// unsigned integer column so both TiKV and the client decode it unsigned.
const UNSIGNED_FLAG: i32 = 1 << 5;
/// `SUM(<integer>)` is an exact `DECIMAL` whose flen is the argument's flen plus
/// this extension, per Go `typeInfer4Sum` (`arg.Flen + 21`).
const SUM_DECIMAL_FLEN_EXTENSION: u32 = 21;
/// MySQL's maximum `DECIMAL` precision; `typeInfer4Sum` clamps the result flen to
/// it (`SetFlenUnderLimit` -> `mysql.MaxDecimalWidth`).
const MAX_DECIMAL_WIDTH: u32 = 65;
/// `utf8mb4_bin` collation id, negated because TiDB rewrites new-collation ids
/// to negative in coprocessor `ColumnInfo` (`collate.RewriteNewCollationIDIfNeeded`).
const UTF8MB4_BIN_COPROCESSOR_COLLATION_ID: i32 = -46;
/// `utf8mb4_bin` collation id as the client result column carries it: positive,
/// per Go `mysql.CharsetNameToID("utf8mb4") = UTF8MB4DefaultCollationID = 46`.
const UTF8MB4_BIN_RESULT_COLLATION_ID: i32 = 46;
/// utf8mb4's max byte width, used by Go `ConvertColumnInfo` to scale a string
/// column's reported length.
const UTF8MB4_MAX_BYTES_PER_CHAR: i32 = 4;

/// The stored type of a configured column.
///
/// For the integer types, the persisted rowcodec bytes are chosen by the
/// value's own compact width, not the column type, so an `Int` and a `BigInt`
/// holding the same value store identically. `Char` stores raw string bytes
/// (no restored-collation data at the default `utf8mb4_bin`). `UnsignedBigInt`
/// and `Double` widen the read path beyond signed integers to the other
/// scalar shapes the coprocessor chunk decoder
/// (`tidb_codec::decode_column_datums`) and `tidb-tablecodec`'s row/index
/// codec both already support end to end; any type outside this set (decimal,
/// temporal, JSON, enum/set, vector) must stay refused at config time rather
/// than admitted with a guessed decode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfiguredScalarType {
    /// Signed 64-bit `BIGINT`.
    BigInt,
    /// Unsigned 64-bit `BIGINT UNSIGNED`.
    UnsignedBigInt,
    /// Signed 32-bit `INT`.
    Int,
    /// 64-bit `DOUBLE`.
    Double,
    /// Fixed-length `CHAR(max_length)` at the default `utf8mb4_bin` collation.
    Char {
        /// Declared character length.
        max_length: u32,
    },
}

impl ConfiguredScalarType {
    /// Inclusive signed value range an integer column admits, or `None` for a
    /// non-integer, unsigned, or floating-point type.
    ///
    /// The integer ranges are exactly Go's `ConvertIntToInt` bounds
    /// (`pkg/types/convert.go`): a value outside the range is an overflow, not
    /// a silent truncation.
    #[must_use]
    pub const fn integer_range(self) -> Option<(i64, i64)> {
        match self {
            Self::BigInt => Some((i64::MIN, i64::MAX)),
            Self::Int => Some((i32::MIN as i64, i32::MAX as i64)),
            Self::UnsignedBigInt | Self::Double | Self::Char { .. } => None,
        }
    }

    /// MySQL protocol/coprocessor type code.
    const fn type_code(self) -> i32 {
        match self {
            Self::BigInt | Self::UnsignedBigInt => MYSQL_TYPE_LONGLONG,
            Self::Int => MYSQL_TYPE_LONG,
            Self::Double => MYSQL_TYPE_DOUBLE,
            Self::Char { .. } => MYSQL_TYPE_STRING,
        }
    }

    /// Coprocessor `ColumnInfo.Flag` bits beyond not-null/primary-key, per Go
    /// `mysql.UnsignedFlag`.
    const fn extra_flag(self) -> i32 {
        match self {
            Self::UnsignedBigInt => UNSIGNED_FLAG,
            Self::BigInt | Self::Int | Self::Double | Self::Char { .. } => 0,
        }
    }

    /// Coprocessor/protocol collation id for this column.
    ///
    /// Integer and floating-point columns carry the binary collation; a
    /// `Char` carries the negated `utf8mb4_bin` id per TiDB's new-collation
    /// sign convention. The `Char` sign is taken from the Go source and is
    /// not yet exercised against real TiKV — the string read path that would
    /// send it is not wired.
    const fn collation_id(self) -> i32 {
        match self {
            Self::BigInt | Self::UnsignedBigInt | Self::Int | Self::Double => BINARY_COLLATION_ID,
            Self::Char { .. } => UTF8MB4_BIN_COPROCESSOR_COLLATION_ID,
        }
    }

    /// Displayed column length.
    const fn column_len(self) -> i32 {
        match self {
            Self::BigInt | Self::UnsignedBigInt => 20,
            Self::Int => 11,
            Self::Double => 22,
            Self::Char { max_length } => max_length as i32,
        }
    }

    /// MySQL type code sent to the client in the result column definition.
    ///
    /// `BIGINT`/`BIGINT UNSIGNED` are `LONGLONG`, `INT` is `LONG`, `DOUBLE` is
    /// `DOUBLE`, `CHAR` is `STRING` — each with a matching cell in the binary
    /// result encoder, so the value is type-faithful (`DumpBinaryRow` dumps
    /// `TypeLong` as a 4-byte `Uint32`, `TypeDouble` as 8 raw bytes).
    #[must_use]
    pub const fn result_type_code(self) -> i32 {
        self.type_code()
    }

    /// Positive result-column charset id, per Go `mysql.CharsetNameToID`
    /// (`"binary" -> 63`, `"utf8mb4" -> 46`).
    ///
    /// This is intentionally NOT [`Self::collation_id`]: the coprocessor
    /// `ColumnInfo` negates a new collation (`-46`), but the client result
    /// column carries the positive id.
    #[must_use]
    pub const fn result_charset_id(self) -> i32 {
        match self {
            Self::BigInt | Self::UnsignedBigInt | Self::Int | Self::Double => BINARY_COLLATION_ID,
            Self::Char { .. } => UTF8MB4_BIN_RESULT_COLLATION_ID,
        }
    }

    /// Result-column length, per Go `column.ConvertColumnInfo`: a string
    /// multiplies its declared length by the charset's max byte width (utf8mb4
    /// is 4), so `CHAR(120)` reports 480.
    #[must_use]
    pub const fn result_column_length(self) -> i32 {
        match self {
            Self::BigInt | Self::UnsignedBigInt => 20,
            Self::Int => 11,
            Self::Double => 22,
            Self::Char { max_length } => max_length as i32 * UTF8MB4_MAX_BYTES_PER_CHAR,
        }
    }

    /// Returns whether this column stores its bytes as an unsigned MySQL
    /// integer, per Go `mysql.HasUnsignedFlag`.
    #[must_use]
    pub const fn is_unsigned(self) -> bool {
        matches!(self, Self::UnsignedBigInt)
    }

    /// Returns the [`FieldType`] the real-TiKV coprocessor chunk decoder
    /// (`tidb_codec::decode_column_datums`) must use to decode this column's
    /// result bytes.
    ///
    /// This is the single source of truth the executor boundary
    /// (`RealTiKvReadSession::execute_plan`) consults instead of assuming
    /// every projected column is a signed `BIGINT`: driving decode from the
    /// wrong `FieldTypeCode` either corrupts a `Char`/`Double`/unsigned
    /// column's value or, for a width mismatch, fails the row outright.
    #[must_use]
    pub fn chunk_field_type(self) -> FieldType {
        match self {
            Self::BigInt => FieldType::new(FieldTypeCode::LongLong),
            Self::UnsignedBigInt => FieldType::new(FieldTypeCode::LongLong).with_unsigned(true),
            Self::Int => FieldType::new(FieldTypeCode::Long),
            Self::Double => FieldType::new(FieldTypeCode::Double),
            Self::Char { max_length } => FieldType::new(FieldTypeCode::String)
                .with_collation(Collation::Utf8Mb4Bin)
                .with_flen(i64::from(max_length)),
        }
    }
}

/// The two signed-`BIGINT` storage roles admitted by the read-only catalog.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfiguredColumnKind {
    /// The table's sole signed integer row handle.
    ClusteredPrimaryKey,
    /// A signed stored value that cannot be `NULL`.
    StoredNotNull,
}

/// One source-ordered column in the configured read-only catalog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfiguredColumn {
    name: String,
    id: i64,
    kind: ConfiguredColumnKind,
    scalar_type: ConfiguredScalarType,
}

impl ConfiguredColumn {
    /// Configures the table's signed `BIGINT PRIMARY KEY CLUSTERED` column.
    #[must_use]
    pub fn clustered_primary_key(name: impl Into<String>, id: i64) -> Self {
        Self {
            name: name.into(),
            id,
            kind: ConfiguredColumnKind::ClusteredPrimaryKey,
            scalar_type: ConfiguredScalarType::BigInt,
        }
    }

    /// Configures one signed stored `BIGINT NOT NULL` column.
    #[must_use]
    pub fn stored_not_null(name: impl Into<String>, id: i64) -> Self {
        Self {
            name: name.into(),
            id,
            kind: ConfiguredColumnKind::StoredNotNull,
            scalar_type: ConfiguredScalarType::BigInt,
        }
    }

    /// Configures one signed stored `INT NOT NULL` column.
    ///
    /// The persisted bytes match a `BIGINT` of the same value; only the wire
    /// metadata and the admitted value range differ.
    #[must_use]
    pub fn stored_int_not_null(name: impl Into<String>, id: i64) -> Self {
        Self {
            name: name.into(),
            id,
            kind: ConfiguredColumnKind::StoredNotNull,
            scalar_type: ConfiguredScalarType::Int,
        }
    }

    /// Configures one stored `CHAR(max_length) NOT NULL` column at the default
    /// `utf8mb4_bin` collation.
    #[must_use]
    pub fn stored_char_not_null(name: impl Into<String>, id: i64, max_length: u32) -> Self {
        Self {
            name: name.into(),
            id,
            kind: ConfiguredColumnKind::StoredNotNull,
            scalar_type: ConfiguredScalarType::Char { max_length },
        }
    }

    /// Configures one unsigned stored `BIGINT UNSIGNED NOT NULL` column.
    #[must_use]
    pub fn stored_unsigned_bigint_not_null(name: impl Into<String>, id: i64) -> Self {
        Self {
            name: name.into(),
            id,
            kind: ConfiguredColumnKind::StoredNotNull,
            scalar_type: ConfiguredScalarType::UnsignedBigInt,
        }
    }

    /// Configures one stored `DOUBLE NOT NULL` column.
    #[must_use]
    pub fn stored_double_not_null(name: impl Into<String>, id: i64) -> Self {
        Self {
            name: name.into(),
            id,
            kind: ConfiguredColumnKind::StoredNotNull,
            scalar_type: ConfiguredScalarType::Double,
        }
    }

    /// Returns the source catalog name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the stable TiDB column identity.
    #[must_use]
    pub const fn id(&self) -> i64 {
        self.id
    }

    /// Returns the column's unambiguous storage role.
    #[must_use]
    pub const fn kind(&self) -> ConfiguredColumnKind {
        self.kind
    }

    /// Returns the column's signed integer domain.
    #[must_use]
    pub const fn scalar_type(&self) -> ConfiguredScalarType {
        self.scalar_type
    }

    fn scan_column(&self) -> ScanColumnInfo {
        let (flag, pk_handle) = match self.kind {
            ConfiguredColumnKind::ClusteredPrimaryKey => (NOT_NULL_FLAG | PRI_KEY_FLAG, true),
            ConfiguredColumnKind::StoredNotNull => (NOT_NULL_FLAG, false),
        };
        let flag = flag | self.scalar_type.extra_flag();
        ScanColumnInfo {
            column_id: self.id,
            tp: self.scalar_type.type_code(),
            collation: self.scalar_type.collation_id(),
            column_len: self.scalar_type.column_len(),
            decimal: 0,
            flag,
            pk_handle,
            ..ScanColumnInfo::default()
        }
    }
}

/// One configured secondary index over a single stored column.
///
/// Scoped to the non-unique single-column shape the deployable node maintains
/// today (sysbench's `k` index). A unique index (whose handle lives in the value
/// and whose write path enforces distinctness) and a multi-column index are
/// deliberately not represented; the write path fails closed on `unique`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConfiguredIndex {
    index_id: i64,
    column_id: i64,
    unique: bool,
}

impl ConfiguredIndex {
    /// Configures one non-unique secondary index over the column `column_id`.
    #[must_use]
    pub const fn non_unique(index_id: i64, column_id: i64) -> Self {
        Self {
            index_id,
            column_id,
            unique: false,
        }
    }

    /// Returns the physical index ID used in TiKV index keys.
    #[must_use]
    pub const fn index_id(&self) -> i64 {
        self.index_id
    }

    /// Returns the stable identity of the single indexed column.
    #[must_use]
    pub const fn column_id(&self) -> i64 {
        self.column_id
    }

    /// Returns whether the index enforces uniqueness (never true yet).
    #[must_use]
    pub const fn is_unique(&self) -> bool {
        self.unique
    }
}

/// The complete catalog input admitted by the first read-only SQL node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfiguredTable {
    schema: String,
    table: String,
    table_id: i64,
    columns: Vec<ConfiguredColumn>,
    indexes: Vec<ConfiguredIndex>,
}

impl ConfiguredTable {
    /// Configures one non-partitioned table with source-ordered signed columns.
    #[must_use]
    pub fn new(
        schema: impl Into<String>,
        table: impl Into<String>,
        table_id: i64,
        columns: impl IntoIterator<Item = ConfiguredColumn>,
    ) -> Self {
        Self {
            schema: schema.into(),
            table: table.into(),
            table_id,
            columns: columns.into_iter().collect(),
            indexes: Vec::new(),
        }
    }

    /// Adds the configured secondary indexes, returning the extended table.
    ///
    /// A builder so the 40-plus existing `new` call sites stay unchanged: an
    /// index-free table simply omits it.
    #[must_use]
    pub fn with_indexes(mut self, indexes: impl IntoIterator<Item = ConfiguredIndex>) -> Self {
        self.indexes.extend(indexes);
        self
    }

    /// Returns the configured secondary indexes in source order.
    #[must_use]
    pub fn indexes(&self) -> &[ConfiguredIndex] {
        &self.indexes
    }

    /// Returns the configured schema name.
    #[must_use]
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Returns the configured table name.
    #[must_use]
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Returns the physical table ID used by TiKV record keys.
    #[must_use]
    pub const fn table_id(&self) -> i64 {
        self.table_id
    }

    /// Returns columns in configured source order.
    #[must_use]
    pub fn columns(&self) -> &[ConfiguredColumn] {
        &self.columns
    }

    /// Validates the complete bounded table descriptor without parsing SQL.
    ///
    /// Multi-table catalog construction uses this same admission seam as the
    /// original single-table lowering, so the two paths cannot disagree about
    /// physical identities or column shape.
    pub fn validate(&self) -> Result<(), ReadOnlyScanError> {
        if self.schema.is_empty() {
            return Err(ReadOnlyScanError::InvalidConfiguration("empty schema name"));
        }
        if self.table.is_empty() {
            return Err(ReadOnlyScanError::InvalidConfiguration("empty table name"));
        }
        if self.table_id <= 0 {
            return Err(ReadOnlyScanError::InvalidConfiguration(
                "table ID must be positive",
            ));
        }
        let mut names = HashSet::with_capacity(self.columns.len());
        let mut ids = HashSet::with_capacity(self.columns.len());
        let mut primary_keys = 0;
        for column in &self.columns {
            if column.name.is_empty() {
                return Err(ReadOnlyScanError::InvalidConfiguration(
                    "column names must be nonempty",
                ));
            }
            if column.id <= 0 {
                return Err(ReadOnlyScanError::InvalidConfiguration(
                    "column IDs must be positive",
                ));
            }
            if !names.insert(fold_identifier(&column.name)) {
                return Err(ReadOnlyScanError::InvalidConfiguration(
                    "column names must be unique",
                ));
            }
            if !ids.insert(column.id) {
                return Err(ReadOnlyScanError::InvalidConfiguration(
                    "column IDs must be unique",
                ));
            }
            if column.kind == ConfiguredColumnKind::ClusteredPrimaryKey {
                primary_keys += 1;
            }
        }
        if primary_keys != 1 {
            return Err(ReadOnlyScanError::InvalidConfiguration(
                "exactly one clustered primary key is required",
            ));
        }
        Ok(())
    }
}

/// One direct projection after catalog resolution and alias preservation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedProjectionColumn {
    output_name: String,
    source_name: String,
    scan_column: ScanColumnInfo,
    kind: ConfiguredColumnKind,
    scalar_type: ConfiguredScalarType,
}

impl ResolvedProjectionColumn {
    fn new(output_name: String, column: &ConfiguredColumn) -> Self {
        Self {
            output_name,
            source_name: column.name.clone(),
            scan_column: column.scan_column(),
            kind: column.kind,
            scalar_type: column.scalar_type,
        }
    }

    /// Returns the MySQL-visible alias or source name.
    #[must_use]
    pub fn output_name(&self) -> &str {
        &self.output_name
    }

    /// Returns the original configured catalog name.
    #[must_use]
    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    /// Returns the exact TiKV scan descriptor.
    #[must_use]
    pub const fn scan_column(&self) -> &ScanColumnInfo {
        &self.scan_column
    }

    /// Returns the resolved source column's storage role.
    #[must_use]
    pub const fn kind(&self) -> ConfiguredColumnKind {
        self.kind
    }

    /// Returns the resolved column's stored type, for result metadata.
    #[must_use]
    pub const fn scalar_type(&self) -> ConfiguredScalarType {
        self.scalar_type
    }
}

/// A parsed query feature outside the first deployable read-only boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnsupportedReadOnlyFeature {
    /// The statement is not a query.
    WriteOrNonQueryStatement,
    /// The query is a set operation.
    SetOperation,
    /// The query uses `TABLE`, `VALUES`, or a parenthesized query envelope.
    QueryForm,
    /// A common table expression is present.
    CommonTableExpression,
    /// An optimizer hint or select modifier is present.
    SelectModifier,
    /// The query does not name a table.
    MissingTable,
    /// More than one table or a nested join tree is present.
    Join,
    /// Grouping or `HAVING` is present.
    Grouping,
    /// A window definition is present.
    Window,
    /// Ordering or TopN planning is required.
    Ordering,
    /// A limit is present.
    Limit,
    /// A locking read is requested.
    LockingRead,
    /// `INTO OUTFILE` is present.
    IntoOutfile,
    /// A partition selection is present.
    Partition,
    /// A stale-read timestamp is present.
    StaleRead,
    /// A table index hint is present.
    IndexHint,
    /// `TABLESAMPLE` is present.
    TableSample,
    /// A wildcard projection is present.
    Wildcard,
    /// An aggregate expression is present.
    Aggregate,
    /// A subquery or derived table is present.
    Subquery,
    /// The projection is not a direct column reference.
    ProjectionExpression,
}

/// A predicate shape outside the bounded signed-`BIGINT` Selection contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnsupportedReadOnlyPredicate {
    /// A boolean operator other than `AND` would require a scalar boolean PB
    /// expression rather than another source Selection condition.
    BooleanOperator,
    /// The predicate root is not one of the six ordinary comparisons.
    ComparisonOperator,
    /// A comparison operand is neither a configured column nor an integer
    /// literal (with optional parentheses and unary sign).
    Operand,
    /// Both operands have the same role; this boundary requires exactly one
    /// configured column and one integer literal.
    ColumnIntegerPair,
    /// An integer literal does not fit the configured signed `BIGINT` domain.
    IntegerOutOfRange,
}

/// Why a parsed prepared point-read template cannot enter the configured
/// read-only planner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PreparedPlanError {
    /// The ordinary configured SQL envelope rejected the statement before it
    /// could become a typed prepared template.
    ReadOnly(ReadOnlyScanError),
    /// The statement's table name did not resolve to exactly one configured
    /// catalog entry.
    Catalog(configured_catalog::ConfiguredTableLookupError),
    /// A `WHERE` comparison was not the clustered primary key against a
    /// parameter marker (in either operand order).
    PrimaryKeyComparison,
    /// The parameter markers did not cover statement positions `0..N` exactly
    /// once. Carries the offending position.
    MarkerPosition(usize),
}

impl fmt::Display for PreparedPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadOnly(error) => write!(formatter, "prepared point read rejected: {error}"),
            Self::Catalog(error) => write!(formatter, "prepared point-read table rejected: {error}"),
            Self::PrimaryKeyComparison => formatter.write_str(
                "prepared read requires each WHERE comparison to be the clustered primary key against a parameter marker",
            ),
            Self::MarkerPosition(position) => write!(
                formatter,
                "prepared read requires parameter markers at contiguous positions 0..N, found out-of-range or duplicate position {position}"
            ),
        }
    }
}

impl Error for PreparedPlanError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ReadOnly(error) => Some(error),
            Self::Catalog(error) => Some(error),
            Self::PrimaryKeyComparison | Self::MarkerPosition(_) => None,
        }
    }
}

/// Why a typed prepared point-read template cannot bind its execute values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PreparedBindError {
    /// This bounded template owns exactly one non-null signed `BIGINT`.
    ParameterCount(usize),
    /// The shared read-only planner rejected the fully typed bound plan.
    ReadOnly(ReadOnlyScanError),
}

impl fmt::Display for PreparedBindError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ParameterCount(count) => write!(
                formatter,
                "prepared point read requires exactly one signed BIGINT parameter, found {count}"
            ),
            Self::ReadOnly(error) => {
                write!(formatter, "prepared point-read binding rejected: {error}")
            }
        }
    }
}

impl Error for PreparedBindError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ReadOnly(error) => Some(error),
            Self::ParameterCount(_) => None,
        }
    }
}

/// Why a SQL statement cannot become the first read-only table scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadOnlyScanError {
    /// SQL parsing failed.
    Parse(String),
    /// The configured catalog entry violates the milestone contract.
    InvalidConfiguration(&'static str),
    /// A parsed feature has no owner in this milestone.
    Unsupported(UnsupportedReadOnlyFeature),
    /// A `WHERE` clause is present but is outside the bounded signed-`BIGINT`
    /// Selection grammar.
    UnsupportedPredicate(UnsupportedReadOnlyPredicate),
    /// The resolved predicate violated the physical Selection contract.
    PhysicalSelection(PhysicalSelectionError),
    /// The query names a table other than the configured table.
    UnknownTable(String),
    /// The query names a column outside the configured table.
    UnknownColumn(String),
    /// A structured bound input names no configured source column.
    InvalidColumnIndex {
        /// Invalid zero-based configured column index.
        index: usize,
        /// Number of configured source columns available on the table.
        column_count: usize,
    },
    /// The existing datasource task builder rejected the validated path.
    PlannerRejected(ScanReadTaskRejection),
    /// The datasource returned a non-reader task after all admissions passed.
    UnexpectedPlannerTask,
}

impl fmt::Display for ReadOnlyScanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse(message) => write!(formatter, "SQL parse error: {message}"),
            Self::InvalidConfiguration(message) => {
                write!(
                    formatter,
                    "invalid read-only table configuration: {message}"
                )
            }
            Self::Unsupported(feature) => {
                write!(formatter, "unsupported read-only SQL feature: {feature:?}")
            }
            Self::UnsupportedPredicate(predicate) => {
                write!(
                    formatter,
                    "unsupported read-only WHERE predicate: {predicate:?}"
                )
            }
            Self::PhysicalSelection(error) => {
                write!(formatter, "invalid physical Selection: {error}")
            }
            Self::UnknownTable(table) => write!(formatter, "unknown table: {table}"),
            Self::UnknownColumn(column) => write!(formatter, "unknown column: {column}"),
            Self::InvalidColumnIndex {
                index,
                column_count,
            } => write!(
                formatter,
                "configured column index {index} is outside column count {column_count}"
            ),
            Self::PlannerRejected(reason) => {
                write!(
                    formatter,
                    "read-only table path rejected by planner: {reason:?}"
                )
            }
            Self::UnexpectedPlannerTask => {
                formatter.write_str("read-only table path did not produce a table reader")
            }
        }
    }
}

impl Error for ReadOnlyScanError {}

/// One validated direct projection, clustered-handle ranges, and optional
/// residual signed-`BIGINT` Selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadOnlyScanPlan {
    reader: crate::physical_table_reader::PhysicalTableReaderPlan,
    projected_columns: Vec<ResolvedProjectionColumn>,
    projection_output_offsets: Vec<u32>,
    handle_ranges: Vec<SignedBigIntRange>,
    selection: Option<PhysicalSelectionPlan>,
}

/// One already-bound signed-`BIGINT` column-versus-literal comparison.
///
/// The variants retain operand order exactly. `ColumnLeft` represents
/// `column <op> value`; `LiteralLeft` represents `value <op> column`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BoundBigIntComparison {
    /// The configured column is the left operand.
    ColumnLeft {
        /// Zero-based source column index in [`ConfiguredTable::columns`].
        column_index: usize,
        /// Typed ordinary comparison operator.
        op: ComparisonOp,
        /// Signed integer literal on the right.
        value: i64,
    },
    /// The signed integer literal is the left operand.
    LiteralLeft {
        /// Signed integer literal on the left.
        value: i64,
        /// Typed ordinary comparison operator.
        op: ComparisonOp,
        /// Zero-based source column index in [`ConfiguredTable::columns`].
        column_index: usize,
    },
}

impl BoundBigIntComparison {
    fn into_unbound(self) -> UnboundComparison {
        match self {
            Self::ColumnLeft {
                column_index,
                op,
                value,
            } => UnboundComparison {
                op,
                lhs: UnboundComparisonOperand::Column(column_index),
                rhs: UnboundComparisonOperand::Int(value),
            },
            Self::LiteralLeft {
                value,
                op,
                column_index,
            } => UnboundComparison {
                op,
                lhs: UnboundComparisonOperand::Int(value),
                rhs: UnboundComparisonOperand::Column(column_index),
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UnboundComparisonOperand {
    Column(usize),
    Int(i64),
    ParamMarker(usize),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct UnboundComparison {
    op: ComparisonOp,
    lhs: UnboundComparisonOperand,
    rhs: UnboundComparisonOperand,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct UnboundProjection {
    column_index: usize,
    output_name: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ValidatedReadOnlySelect {
    projections: Vec<UnboundProjection>,
    comparisons: Vec<UnboundComparison>,
    order_by: Vec<PreparedOrderColumn>,
    distinct: bool,
    aggregate: Option<PreparedAggregate>,
}

/// One planner-resolved `ORDER BY` column for a prepared read.
///
/// An `ORDER BY` without a `LIMIT` is a SQL-layer sort (Go `executor.SortExec`),
/// not a coprocessor `TopN` whose heap needs a bound. The prepared read
/// therefore sorts its already-projected output rows after the scan. Each key
/// is an offset into that output row, a direction, and the projected column's
/// scalar type — the executor uses the scalar type to compare signed integers
/// numerically and `CHAR` columns under their `utf8mb4_bin` collation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PreparedOrderColumn {
    output_offset: usize,
    direction: ConfiguredOrderDirection,
    scalar_type: ConfiguredScalarType,
}

impl PreparedOrderColumn {
    /// Creates a resolved order column from an output-row offset, its direction,
    /// and the projected column's scalar type.
    #[must_use]
    pub const fn new(
        output_offset: usize,
        direction: ConfiguredOrderDirection,
        scalar_type: ConfiguredScalarType,
    ) -> Self {
        Self {
            output_offset,
            direction,
            scalar_type,
        }
    }

    /// Returns the zero-based offset of this key in the projected output row.
    #[must_use]
    pub const fn output_offset(&self) -> usize {
        self.output_offset
    }

    /// Returns this key's independent ordering direction.
    #[must_use]
    pub const fn direction(&self) -> ConfiguredOrderDirection {
        self.direction
    }

    /// Returns the projected column's scalar type, selecting the comparison.
    #[must_use]
    pub const fn scalar_type(&self) -> ConfiguredScalarType {
        self.scalar_type
    }
}

/// A single-column aggregate the prepared read evaluates over its scan output.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreparedAggregateKind {
    /// `SUM(<signed integer column>)`, whose result is an exact `DECIMAL`.
    Sum,
}

/// One planner-resolved single-column aggregate for a prepared read.
///
/// The prepared read has no `GROUP BY`, so a `SUM` collapses the whole scan to
/// one output row. The scan still projects the summed column; the executor folds
/// that column's values into the single result row (Go `AggFuncSum`: an integer
/// argument promotes to an exact `DECIMAL`, an empty set yields `NULL`). The
/// result column metadata is the aggregate's own type — `DECIMAL(flen, 0)` per
/// Go `typeInfer4Sum` — not the summed column's.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedAggregate {
    kind: PreparedAggregateKind,
    source_offset: usize,
    output_name: String,
    result_column_length: u32,
    result_decimals: u8,
}

impl PreparedAggregate {
    /// Creates a resolved aggregate over the scan output column at
    /// `source_offset`, carrying its result column's display name and type.
    #[must_use]
    pub fn new(
        kind: PreparedAggregateKind,
        source_offset: usize,
        output_name: String,
        result_column_length: u32,
        result_decimals: u8,
    ) -> Self {
        Self {
            kind,
            source_offset,
            output_name,
            result_column_length,
            result_decimals,
        }
    }

    /// Returns which aggregate function to fold.
    #[must_use]
    pub const fn kind(&self) -> PreparedAggregateKind {
        self.kind
    }

    /// Returns the offset of the summed column in the scan output row.
    #[must_use]
    pub const fn source_offset(&self) -> usize {
        self.source_offset
    }

    /// Returns the result column's display name (e.g. `SUM(k)`).
    #[must_use]
    pub fn output_name(&self) -> &str {
        &self.output_name
    }

    /// Returns the result `DECIMAL` column's advertised flen.
    #[must_use]
    pub const fn result_column_length(&self) -> u32 {
        self.result_column_length
    }

    /// Returns the result `DECIMAL` column's scale (`0` for an integer `SUM`).
    #[must_use]
    pub const fn result_decimals(&self) -> u8 {
        self.result_decimals
    }
}

/// A validated prepared template for the configured signed-`BIGINT`
/// clustered-primary-key read shape: any number of clustered-handle
/// comparisons against parameter markers, from a single equality (a point read)
/// to a `>=`/`<=` pair (a `BETWEEN` range read).
///
/// The template owns no untyped SQL text and no execution state. Binding the
/// typed values constructs the same [`ReadOnlyScanPlan`] lowering used by
/// literal `COM_QUERY` reads, whose ranger already folds the comparisons into
/// closed handle ranges.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfiguredPreparedPointReadTemplate {
    table: ConfiguredTable,
    projections: Vec<UnboundProjection>,
    comparisons: Vec<PreparedReadComparison>,
    parameter_count: usize,
    order_by: Vec<PreparedOrderColumn>,
    distinct: bool,
    aggregate: Option<PreparedAggregate>,
}

/// One clustered-handle comparison against a parameter marker, retaining the
/// operator, the operand order, and the marker's statement position.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PreparedReadComparison {
    op: ComparisonOp,
    column_index: usize,
    position: usize,
    column_on_left: bool,
}

impl ConfiguredPreparedPointReadTemplate {
    /// Number of positional markers the execute packet must supply.
    #[must_use]
    pub const fn parameter_count(&self) -> usize {
        self.parameter_count
    }

    /// Returns the resolved `ORDER BY` columns, empty when the read is unordered.
    ///
    /// These are parameter-independent, so they live on the template rather than
    /// the bound plan: the executor applies the SQL-layer sort to the scan's
    /// projected output rows after [`Self::bind`] opens the storage path.
    #[must_use]
    pub fn order_by(&self) -> &[PreparedOrderColumn] {
        &self.order_by
    }

    /// Returns whether the read is `SELECT DISTINCT`.
    ///
    /// Like the order, this is parameter-independent: the executor dedups the
    /// projected output rows by their whole-tuple identity after the scan (and
    /// after the optional sort), so it lives on the template, not the plan.
    #[must_use]
    pub const fn is_distinct(&self) -> bool {
        self.distinct
    }

    /// Returns the single-column aggregate to fold over the scan output, if any.
    ///
    /// Parameter-independent like the order and distinct flag: the scan projects
    /// the summed column, and the executor collapses those values into one
    /// result row whose type is the aggregate's own (a `DECIMAL` for an integer
    /// `SUM`), not the summed column's.
    #[must_use]
    pub const fn aggregate(&self) -> Option<&PreparedAggregate> {
        self.aggregate.as_ref()
    }

    /// Binds the non-null signed `BIGINT` execute parameters before the shared
    /// configured read-only scan lowering opens any storage-facing path. Each
    /// comparison substitutes its marker with the value at its statement
    /// position, preserving operator and operand order for the ranger.
    pub fn bind(&self, params: &[i64]) -> Result<ReadOnlyScanPlan, PreparedBindError> {
        if params.len() != self.parameter_count {
            return Err(PreparedBindError::ParameterCount(params.len()));
        }
        let comparisons = self
            .comparisons
            .iter()
            .map(|comparison| {
                let value = params[comparison.position];
                let column = UnboundComparisonOperand::Column(comparison.column_index);
                let literal = UnboundComparisonOperand::Int(value);
                let (lhs, rhs) = if comparison.column_on_left {
                    (column, literal)
                } else {
                    (literal, column)
                };
                UnboundComparison {
                    op: comparison.op,
                    lhs,
                    rhs,
                }
            })
            .collect();
        ReadOnlyScanPlan::lower_validated(
            &self.table,
            ValidatedReadOnlySelect {
                projections: self.projections.clone(),
                comparisons,
                // The plan is sort-free, dedup-free, and aggregate-free: ORDER BY
                // / DISTINCT / SUM are applied by the executor over the projected
                // output rows, keyed by the template's own resolved metadata.
                order_by: Vec::new(),
                distinct: false,
                aggregate: None,
            },
        )
        .map_err(PreparedBindError::ReadOnly)
    }
}

/// Lowers an already-parsed prepared statement into one typed configured
/// point-read template.
///
/// This accepts only `SELECT <configured columns> FROM <configured table>
/// WHERE <clustered primary key> = ?` (or its operand-reversed equivalent).
/// It never formats execute values into SQL text; callers must pass values to
/// [`ConfiguredPreparedPointReadTemplate::bind`].
pub fn lower_prepared_point_read(
    statement: &SelectStmt,
    catalog: &configured_catalog::ConfiguredCatalog,
) -> Result<ConfiguredPreparedPointReadTemplate, PreparedPlanError> {
    let table = resolve_prepared_table(statement, catalog)?;
    let validated = validate_select(statement, table).map_err(PreparedPlanError::ReadOnly)?;
    let comparisons = validated
        .comparisons
        .iter()
        .map(|comparison| classify_prepared_pk_comparison(comparison, table))
        .collect::<Result<Vec<_>, _>>()?;
    let parameter_count = validate_prepared_marker_positions(&comparisons)?;
    Ok(ConfiguredPreparedPointReadTemplate {
        table: table.clone(),
        projections: validated.projections,
        comparisons,
        parameter_count,
        order_by: validated.order_by,
        distinct: validated.distinct,
        aggregate: validated.aggregate,
    })
}

/// Classifies one validated comparison as a clustered-primary-key handle
/// against a parameter marker, in either operand order. Any comparison
/// operator is admitted; the ranger folds them into closed handle ranges.
fn classify_prepared_pk_comparison(
    comparison: &UnboundComparison,
    table: &ConfiguredTable,
) -> Result<PreparedReadComparison, PreparedPlanError> {
    let (column_index, position, column_on_left) = match (comparison.lhs, comparison.rhs) {
        (
            UnboundComparisonOperand::Column(column_index),
            UnboundComparisonOperand::ParamMarker(position),
        ) => (column_index, position, true),
        (
            UnboundComparisonOperand::ParamMarker(position),
            UnboundComparisonOperand::Column(column_index),
        ) => (column_index, position, false),
        _ => return Err(PreparedPlanError::PrimaryKeyComparison),
    };
    match table.columns.get(column_index) {
        Some(column) if column.kind == ConfiguredColumnKind::ClusteredPrimaryKey => {
            Ok(PreparedReadComparison {
                op: comparison.op,
                column_index,
                position,
                column_on_left,
            })
        }
        Some(_) | None => Err(PreparedPlanError::PrimaryKeyComparison),
    }
}

/// The markers must cover statement positions `0..N` exactly once, matching the
/// `N` execute values the packet supplies.
fn validate_prepared_marker_positions(
    comparisons: &[PreparedReadComparison],
) -> Result<usize, PreparedPlanError> {
    let count = comparisons.len();
    let mut seen = vec![false; count];
    for comparison in comparisons {
        let position = comparison.position;
        if position >= count || seen[position] {
            return Err(PreparedPlanError::MarkerPosition(position));
        }
        seen[position] = true;
    }
    Ok(count)
}

fn resolve_prepared_table<'a>(
    statement: &SelectStmt,
    catalog: &'a configured_catalog::ConfiguredCatalog,
) -> Result<&'a ConfiguredTable, PreparedPlanError> {
    let Some(from) = &statement.from else {
        return Err(PreparedPlanError::ReadOnly(ReadOnlyScanError::Unsupported(
            UnsupportedReadOnlyFeature::MissingTable,
        )));
    };
    let JoinNode::Table(table_ref) = &from.left else {
        return Err(PreparedPlanError::ReadOnly(ReadOnlyScanError::Unsupported(
            UnsupportedReadOnlyFeature::Subquery,
        )));
    };
    let (schema, table) = match table_ref.name.as_slice() {
        [table] => (None, table.as_str()),
        [schema, table] => (Some(schema.as_str()), table.as_str()),
        _ => {
            return Err(PreparedPlanError::ReadOnly(
                ReadOnlyScanError::UnknownTable(table_ref.name.join(".")),
            ));
        }
    };
    catalog
        .resolve_table(schema, table)
        .map_err(PreparedPlanError::Catalog)
}

impl ReadOnlyScanPlan {
    /// Parses and lowers one direct-column `SELECT` against `table`, including
    /// the bounded signed-`BIGINT` `WHERE` grammar.
    pub fn lower(sql: &str, table: &ConfiguredTable) -> Result<Self, ReadOnlyScanError> {
        table.validate()?;
        let statement = tidb_parser::parse(sql).map_err(|error| {
            ReadOnlyScanError::Parse(format!("{} at byte {}", error.message, error.offset))
        })?;
        let select = match statement {
            Stmt::Query(query) => match query.into_inner() {
                QueryStmt::Select(select) => select,
                QueryStmt::SetOpr(_) => {
                    return Err(ReadOnlyScanError::Unsupported(
                        UnsupportedReadOnlyFeature::SetOperation,
                    ));
                }
            },
            Stmt::Dml(_) | Stmt::Ddl(_) | Stmt::Admin(_) | Stmt::Session(_) => {
                return Err(ReadOnlyScanError::Unsupported(
                    UnsupportedReadOnlyFeature::WriteOrNonQueryStatement,
                ));
            }
        };

        let validated = validate_select(&select, table)?;
        Self::lower_validated(table, validated)
    }

    /// Lowers one already-bound configured relation without parsing SQL.
    ///
    /// `projected_column_indices` are zero-based indices into
    /// [`ConfiguredTable::columns`] and remain in caller order, including
    /// duplicates. `comparisons` must already be local to this relation and
    /// retain exact column/literal operand order. Both inputs enter the same
    /// range-detachment, residual Selection, scan-column, and physical-reader
    /// lowering core used by [`Self::lower`].
    pub fn lower_bound_relation(
        table: &ConfiguredTable,
        projected_column_indices: &[usize],
        comparisons: &[BoundBigIntComparison],
    ) -> Result<Self, ReadOnlyScanError> {
        let validated = ValidatedReadOnlySelect {
            projections: projected_column_indices
                .iter()
                .map(|column_index| UnboundProjection {
                    column_index: *column_index,
                    output_name: None,
                })
                .collect(),
            comparisons: comparisons
                .iter()
                .copied()
                .map(BoundBigIntComparison::into_unbound)
                .collect(),
            order_by: Vec::new(),
            distinct: false,
            aggregate: None,
        };
        Self::lower_validated(table, validated)
    }

    fn lower_validated(
        table: &ConfiguredTable,
        validated: ValidatedReadOnlySelect,
    ) -> Result<Self, ReadOnlyScanError> {
        table.validate()?;
        // The scan plan carries no ordering or dedup: ORDER BY / DISTINCT are
        // SQL-layer stages the prepared read executor applies over the projected
        // output rows. The literal COM_QUERY lowering runs neither, so it must
        // reject them rather than silently return unsorted or duplicated rows.
        if !validated.order_by.is_empty() {
            return unsupported(UnsupportedReadOnlyFeature::Ordering);
        }
        if validated.distinct {
            return unsupported(UnsupportedReadOnlyFeature::SelectModifier);
        }
        if validated.aggregate.is_some() {
            return unsupported(UnsupportedReadOnlyFeature::Aggregate);
        }
        let projected_columns = validated
            .projections
            .into_iter()
            .map(|projection| {
                let column = configured_column(table, projection.column_index)?;
                let output_name = projection
                    .output_name
                    .unwrap_or_else(|| column.name.clone());
                Ok(ResolvedProjectionColumn::new(output_name, column))
            })
            .collect::<Result<Vec<_>, ReadOnlyScanError>>()?;
        let mut scan_columns = projected_columns
            .iter()
            .map(|column| column.scan_column.clone())
            .collect::<Vec<_>>();
        let projection_output_offsets = (0..projected_columns.len())
            .map(|offset| {
                u32::try_from(offset).map_err(|_| {
                    ReadOnlyScanError::InvalidConfiguration(
                        "projected column count exceeds TiKV output-offset capacity",
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut scan_offsets = HashMap::with_capacity(table.columns.len());
        for (offset, column) in scan_columns.iter().enumerate() {
            let offset = u32::try_from(offset).map_err(|_| {
                ReadOnlyScanError::InvalidConfiguration(
                    "scan column count exceeds TiKV input-offset capacity",
                )
            })?;
            scan_offsets.entry(column.column_id).or_insert(offset);
        }
        let comparisons = validated
            .comparisons
            .into_iter()
            .map(|comparison| {
                BigIntComparison::new(
                    comparison.op,
                    bind_comparison_operand(
                        comparison.lhs,
                        table,
                        &mut scan_columns,
                        &mut scan_offsets,
                    )?,
                    bind_comparison_operand(
                        comparison.rhs,
                        table,
                        &mut scan_columns,
                        &mut scan_offsets,
                    )?,
                )
                .map_err(ReadOnlyScanError::PhysicalSelection)
            })
            .collect::<Result<Vec<_>, ReadOnlyScanError>>()?;
        let clustered_column_id = table
            .columns
            .iter()
            .find(|column| column.kind == ConfiguredColumnKind::ClusteredPrimaryKey)
            .expect("ConfiguredTable::validate requires one clustered primary key")
            .id;
        let (handle_ranges, residual_comparisons) =
            if let Some(clustered_input_offset) = scan_offsets.get(&clustered_column_id) {
                let detached =
                    detach_clustered_signed_bigint_ranges(&comparisons, *clustered_input_offset);
                (
                    detached.ranges().to_vec(),
                    detached.residual_conditions().to_vec(),
                )
            } else {
                (vec![SignedBigIntRange::full()], comparisons)
            };
        let selection = if residual_comparisons.is_empty() {
            None
        } else {
            Some(
                PhysicalSelectionPlan::from_bigint_conditions(residual_comparisons)
                    .map_err(ReadOnlyScanError::PhysicalSelection)?,
            )
        };
        let pushdown = TiKvTableScanSpec::new(table.table_id, scan_columns);
        let descriptor = ResolvedTableDescriptor::new(
            table.table_id,
            false,
            ResolvedTableScanKind::Full,
            TableScanExplainIdSuffix::IncludePlanId,
        );
        let path = TableAccessPath::from_source_table_scan(
            descriptor,
            pushdown,
            PointGetAdmission::NotEligible,
            f64::MAX,
        )
        .map_err(|_| ReadOnlyScanError::UnexpectedPlannerTask)?;
        let source =
            LogicalDataSource::new(PHYSICAL_PLAN_ID, 0, [DataSourceAccessPath::Table(path)]);

        let task = source.build_scan_read_task(IndexTaskProperty::new(TaskType::Root));
        let reader = match task {
            ScanReadTask::TableReader(reader) => reader,
            ScanReadTask::Invalid(reason) => {
                return Err(ReadOnlyScanError::PlannerRejected(reason));
            }
            ScanReadTask::Index(_) | ScanReadTask::TableDual(_) => {
                return Err(ReadOnlyScanError::UnexpectedPlannerTask);
            }
        };
        Ok(Self {
            reader,
            projected_columns,
            projection_output_offsets,
            handle_ranges,
            selection,
        })
    }

    /// Returns the exact physical table scan accepted by DAG lowering.
    #[must_use]
    pub fn table_scan(&self) -> &PhysicalTableScanPlan {
        self.reader
            .table_scan_plan()
            .expect("ReadOnlyScanPlan always owns a planner-built table reader")
    }

    /// Returns the physical table identity used for TiKV routing.
    #[must_use]
    pub fn table_id(&self) -> i64 {
        self.table_scan().pushdown().table_id
    }

    /// Returns resolved source/output metadata in projection order.
    #[must_use]
    pub fn projected_columns(&self) -> &[ResolvedProjectionColumn] {
        &self.projected_columns
    }

    /// Returns the exact TableScan result offsets exposed to the MySQL row.
    /// Predicate-only input columns are deliberately absent.
    #[must_use]
    pub fn projection_output_offsets(&self) -> &[u32] {
        &self.projection_output_offsets
    }

    /// Returns normalized inclusive clustered signed-handle ranges.
    ///
    /// DistSQL owns conversion to physical TiDB record-key bytes. An empty
    /// slice is an exact contradiction and must short-circuit before TSO or
    /// transport work.
    #[must_use]
    pub fn handle_ranges(&self) -> &[SignedBigIntRange] {
        &self.handle_ranges
    }

    /// Returns whether exact clustered-handle predicates are contradictory.
    #[must_use]
    pub fn is_contradiction(&self) -> bool {
        self.handle_ranges.is_empty()
    }

    /// Returns the physical signed-`BIGINT` Selection above the table scan,
    /// when the query has residual non-handle conditions.
    #[must_use]
    pub const fn selection(&self) -> Option<&PhysicalSelectionPlan> {
        self.selection.as_ref()
    }
}

fn bind_comparison_operand(
    operand: UnboundComparisonOperand,
    table: &ConfiguredTable,
    scan_columns: &mut Vec<ScanColumnInfo>,
    scan_offsets: &mut HashMap<i64, u32>,
) -> Result<ComparisonOperand, ReadOnlyScanError> {
    match operand {
        UnboundComparisonOperand::Int(value) => Ok(ComparisonOperand::Int(value)),
        UnboundComparisonOperand::Column(column_index) => {
            let column = configured_column(table, column_index)?;
            let offset = match scan_offsets.get(&column.id) {
                Some(offset) => *offset,
                None => {
                    let offset = u32::try_from(scan_columns.len()).map_err(|_| {
                        ReadOnlyScanError::InvalidConfiguration(
                            "scan column count exceeds TiKV input-offset capacity",
                        )
                    })?;
                    scan_columns.push(column.scan_column());
                    scan_offsets.insert(column.id, offset);
                    offset
                }
            };
            Ok(ComparisonOperand::InputOffset(offset))
        }
        UnboundComparisonOperand::ParamMarker(_) => {
            unsupported_predicate(UnsupportedReadOnlyPredicate::Operand)
        }
    }
}

fn configured_column(
    table: &ConfiguredTable,
    column_index: usize,
) -> Result<&ConfiguredColumn, ReadOnlyScanError> {
    table
        .columns
        .get(column_index)
        .ok_or(ReadOnlyScanError::InvalidColumnIndex {
            index: column_index,
            column_count: table.columns.len(),
        })
}

fn validate_select(
    select: &SelectStmt,
    table: &ConfiguredTable,
) -> Result<ValidatedReadOnlySelect, ReadOnlyScanError> {
    if select.kind != SelectStatementKind::Select
        || select.is_in_braces
        || !select.values.is_empty()
    {
        return unsupported(UnsupportedReadOnlyFeature::QueryForm);
    }
    if select.with.is_some() {
        return unsupported(UnsupportedReadOnlyFeature::CommonTableExpression);
    }
    if !select.hints.is_empty() || select.calc_found_rows || select.all {
        return unsupported(UnsupportedReadOnlyFeature::SelectModifier);
    }

    let from = select.from.as_ref().ok_or(ReadOnlyScanError::Unsupported(
        UnsupportedReadOnlyFeature::MissingTable,
    ))?;
    if from.right.is_some()
        || from.tp != JoinType::Cross
        || from.straight
        || from.on.is_some()
        || !from.using.is_empty()
        || from.natural
    {
        return unsupported(UnsupportedReadOnlyFeature::Join);
    }
    let table_ref = match &from.left {
        JoinNode::Table(table_ref) => table_ref,
        JoinNode::Derived { .. } => return unsupported(UnsupportedReadOnlyFeature::Subquery),
        JoinNode::Join(_) => return unsupported(UnsupportedReadOnlyFeature::Join),
    };
    validate_table_ref(table_ref, table)?;

    let comparisons = select
        .where_clause
        .as_ref()
        .map(|predicate| bind_where_comparisons(predicate, table_ref, table))
        .transpose()?
        .unwrap_or_default();
    if !select.group_by.is_empty() || select.rollup || select.having.is_some() {
        return unsupported(UnsupportedReadOnlyFeature::Grouping);
    }
    if !select.windows.is_empty() {
        return unsupported(UnsupportedReadOnlyFeature::Window);
    }
    if select.limit.is_some() {
        return unsupported(UnsupportedReadOnlyFeature::Limit);
    }
    if select.lock.is_some() {
        return unsupported(UnsupportedReadOnlyFeature::LockingRead);
    }
    if select.into_outfile.is_some() {
        return unsupported(UnsupportedReadOnlyFeature::IntoOutfile);
    }

    if let Some((projections, aggregate)) = resolve_prepared_aggregate(select, table_ref, table)? {
        // An aggregate collapses the whole scan to one row; an ORDER BY or
        // DISTINCT over that single row is a different plan shape this narrow
        // read does not own, so both fail closed rather than being ignored.
        if !select.order_by.is_empty() || select.distinct {
            return unsupported(UnsupportedReadOnlyFeature::Aggregate);
        }
        return Ok(ValidatedReadOnlySelect {
            projections,
            comparisons,
            order_by: Vec::new(),
            distinct: false,
            aggregate: Some(aggregate),
        });
    }

    let mut columns = Vec::with_capacity(select.fields.len());
    for field in &select.fields {
        let (path, alias) = match field {
            SelectField::Wildcard(_) => return unsupported(UnsupportedReadOnlyFeature::Wildcard),
            SelectField::Expr {
                expr: Expr::Column(path),
                alias,
            } => (path, alias),
            SelectField::Expr {
                expr: Expr::Aggregate { .. } | Expr::GroupConcat { .. },
                ..
            } => return unsupported(UnsupportedReadOnlyFeature::Aggregate),
            SelectField::Expr {
                expr:
                    Expr::Subquery(_)
                    | Expr::Exists { .. }
                    | Expr::InSubquery { .. }
                    | Expr::CompareSubquery { .. },
                ..
            } => return unsupported(UnsupportedReadOnlyFeature::Subquery),
            SelectField::Expr { .. } => {
                return unsupported(UnsupportedReadOnlyFeature::ProjectionExpression);
            }
        };
        let (column_index, column) = resolve_column_path(path, table_ref, table)?;
        let output_name = alias
            .as_deref()
            .filter(|alias| !alias.is_empty())
            .unwrap_or(&column.name)
            .to_owned();
        columns.push(UnboundProjection {
            column_index,
            output_name: Some(output_name),
        });
    }
    let order_by = resolve_prepared_order_by(&select.order_by, table_ref, table, &columns)?;
    Ok(ValidatedReadOnlySelect {
        projections: columns,
        comparisons,
        order_by,
        distinct: select.distinct,
        aggregate: None,
    })
}

/// Resolves the one supported aggregate shape: a single `SUM(<integer column>)`
/// field.
///
/// Returns `None` when no field is an aggregate, so the caller treats the query
/// as an ordinary column projection. Returns `Some((scan projections, aggregate))`
/// for the supported shape: the scan projects the summed column, and the
/// aggregate carries the `DECIMAL` result metadata (Go `typeInfer4Sum`). Any
/// other aggregate shape — more than one field, `DISTINCT`, a non-column or
/// non-integer argument, or a function other than `SUM` — fails closed, since a
/// wrong aggregate is a silent correctness bug rather than a missing feature.
fn resolve_prepared_aggregate(
    select: &SelectStmt,
    table_ref: &TableRef,
    table: &ConfiguredTable,
) -> Result<Option<(Vec<UnboundProjection>, PreparedAggregate)>, ReadOnlyScanError> {
    let has_aggregate = select.fields.iter().any(|field| {
        matches!(
            field,
            SelectField::Expr {
                expr: Expr::Aggregate { .. },
                ..
            }
        )
    });
    if !has_aggregate {
        return Ok(None);
    }

    let [SelectField::Expr {
        expr: Expr::Aggregate {
            name,
            distinct,
            args,
        },
        alias,
    }] = &select.fields[..]
    else {
        return unsupported(UnsupportedReadOnlyFeature::Aggregate);
    };
    if *distinct || AggregateKind::from_name(name) != Some(AggregateKind::Sum) {
        return unsupported(UnsupportedReadOnlyFeature::Aggregate);
    }
    let [Expr::Column(path)] = args.as_slice() else {
        return unsupported(UnsupportedReadOnlyFeature::Aggregate);
    };

    let (column_index, column) = resolve_column_path(path, table_ref, table)?;
    // Go `typeInfer4Sum` returns a DECIMAL only for an integer (or decimal)
    // argument; a string argument would be a DOUBLE result, a type path this
    // narrow read does not own yet.
    let arg_flen = match column.scalar_type() {
        ConfiguredScalarType::Int
        | ConfiguredScalarType::BigInt
        | ConfiguredScalarType::UnsignedBigInt => {
            column.scalar_type().result_column_length() as u32
        }
        // Go `typeInfer4Sum` returns a DOUBLE (not DECIMAL) for a floating-point
        // argument, a result shape this DECIMAL-only path does not own yet.
        ConfiguredScalarType::Double | ConfiguredScalarType::Char { .. } => {
            return unsupported(UnsupportedReadOnlyFeature::Aggregate)
        }
    };
    let output_name = alias
        .as_deref()
        .filter(|alias| !alias.is_empty())
        .map_or_else(|| format!("{}({})", name, column.name), str::to_owned);
    let result_column_length = (arg_flen + SUM_DECIMAL_FLEN_EXTENSION).min(MAX_DECIMAL_WIDTH);
    let aggregate = PreparedAggregate::new(
        PreparedAggregateKind::Sum,
        0,
        output_name,
        result_column_length,
        0,
    );
    let projections = vec![UnboundProjection {
        column_index,
        output_name: None,
    }];
    Ok(Some((projections, aggregate)))
}

/// Resolves each `ORDER BY` item to a projected output column.
///
/// The prepared read sorts its already-projected output rows, so a key must be
/// a bare column reference that the `SELECT` list also projects; its offset is
/// that column's position in the output row. Anything the narrow read cannot
/// honor over projected rows — an expression, a positional ordinal, a `COLLATE`
/// clause, or a column absent from the projection — fails closed as an
/// unsupported ordering rather than silently changing the result order.
fn resolve_prepared_order_by(
    order_by: &[OrderItem],
    table_ref: &TableRef,
    table: &ConfiguredTable,
    projections: &[UnboundProjection],
) -> Result<Vec<PreparedOrderColumn>, ReadOnlyScanError> {
    order_by
        .iter()
        .map(|item| {
            let path = match &item.expr {
                Expr::Column(path) => path,
                _ => return unsupported(UnsupportedReadOnlyFeature::Ordering),
            };
            let (column_index, column) = resolve_column_path(path, table_ref, table)?;
            let output_offset = projections
                .iter()
                .position(|projection| projection.column_index == column_index)
                .ok_or(ReadOnlyScanError::Unsupported(
                    UnsupportedReadOnlyFeature::Ordering,
                ))?;
            Ok(PreparedOrderColumn::new(
                output_offset,
                ConfiguredOrderDirection::from_descending(item.desc),
                column.scalar_type(),
            ))
        })
        .collect()
}

fn bind_where_comparisons(
    predicate: &Expr,
    table_ref: &TableRef,
    table: &ConfiguredTable,
) -> Result<Vec<UnboundComparison>, ReadOnlyScanError> {
    let mut comparisons = Vec::new();
    flatten_and_bind(predicate, table_ref, table, &mut comparisons)?;
    Ok(comparisons)
}

fn flatten_and_bind(
    predicate: &Expr,
    table_ref: &TableRef,
    table: &ConfiguredTable,
    comparisons: &mut Vec<UnboundComparison>,
) -> Result<(), ReadOnlyScanError> {
    match strip_parens(predicate) {
        Expr::Binary(BinaryOp::LogicAnd, left, right) => {
            flatten_and_bind(left, table_ref, table, comparisons)?;
            flatten_and_bind(right, table_ref, table, comparisons)
        }
        Expr::Binary(BinaryOp::LogicOr | BinaryOp::LogicXor, _, _) => {
            unsupported_predicate(UnsupportedReadOnlyPredicate::BooleanOperator)
        }
        Expr::Binary(operator, left, right) => {
            let op = comparison_op(*operator).ok_or(ReadOnlyScanError::UnsupportedPredicate(
                UnsupportedReadOnlyPredicate::ComparisonOperator,
            ))?;
            let lhs = bind_unbound_operand(left, table_ref, table)?;
            let rhs = bind_unbound_operand(right, table_ref, table)?;
            push_validated_comparison(op, lhs, rhs, comparisons)
        }
        // `x BETWEEN low AND high` rewrites to `x >= low AND x <= high`, exactly
        // as TiDB's expression rewrite unfolds a non-negated `BetweenExpr`. A
        // `NOT BETWEEN` unfolds to `x < low OR x > high`, and `OR` is
        // unsupported here, so it is rejected the same way.
        Expr::Between {
            expr,
            low,
            high,
            not,
        } => {
            if *not {
                return unsupported_predicate(UnsupportedReadOnlyPredicate::BooleanOperator);
            }
            let tested = bind_unbound_operand(expr, table_ref, table)?;
            let low = bind_unbound_operand(low, table_ref, table)?;
            let high = bind_unbound_operand(high, table_ref, table)?;
            push_validated_comparison(ComparisonOp::Ge, tested, low, comparisons)?;
            push_validated_comparison(ComparisonOp::Le, tested, high, comparisons)
        }
        _ => unsupported_predicate(UnsupportedReadOnlyPredicate::ComparisonOperator),
    }
}

/// Validates one comparison is a column-against-integer (literal or marker)
/// pair and appends it. Shared by the binary-operator and `BETWEEN` grammars.
fn push_validated_comparison(
    op: ComparisonOp,
    lhs: UnboundComparisonOperand,
    rhs: UnboundComparisonOperand,
    comparisons: &mut Vec<UnboundComparison>,
) -> Result<(), ReadOnlyScanError> {
    if !matches!(
        (lhs, rhs),
        (
            UnboundComparisonOperand::Column(_),
            UnboundComparisonOperand::Int(_)
        ) | (
            UnboundComparisonOperand::Int(_),
            UnboundComparisonOperand::Column(_)
        ) | (
            UnboundComparisonOperand::Column(_),
            UnboundComparisonOperand::ParamMarker(_)
        ) | (
            UnboundComparisonOperand::ParamMarker(_),
            UnboundComparisonOperand::Column(_)
        )
    ) {
        return unsupported_predicate(UnsupportedReadOnlyPredicate::ColumnIntegerPair);
    }
    comparisons.push(UnboundComparison { op, lhs, rhs });
    Ok(())
}

fn comparison_op(operator: BinaryOp) -> Option<ComparisonOp> {
    match operator {
        BinaryOp::Lt => Some(ComparisonOp::Lt),
        BinaryOp::Le => Some(ComparisonOp::Le),
        BinaryOp::Gt => Some(ComparisonOp::Gt),
        BinaryOp::Ge => Some(ComparisonOp::Ge),
        BinaryOp::Eq => Some(ComparisonOp::Eq),
        BinaryOp::Ne => Some(ComparisonOp::Ne),
        _ => None,
    }
}

fn bind_unbound_operand(
    operand: &Expr,
    table_ref: &TableRef,
    table: &ConfiguredTable,
) -> Result<UnboundComparisonOperand, ReadOnlyScanError> {
    match strip_parens(operand) {
        Expr::Column(path) => {
            let (column_index, _) = resolve_column_path(path, table_ref, table)?;
            Ok(UnboundComparisonOperand::Column(column_index))
        }
        Expr::ParamMarker { order, .. } => Ok(UnboundComparisonOperand::ParamMarker(*order)),
        literal if is_integer_literal_shape(literal) => parse_signed_integer(literal)
            .map(UnboundComparisonOperand::Int)
            .ok_or(ReadOnlyScanError::UnsupportedPredicate(
                UnsupportedReadOnlyPredicate::IntegerOutOfRange,
            )),
        _ => unsupported_predicate(UnsupportedReadOnlyPredicate::Operand),
    }
}

fn is_integer_literal_shape(expr: &Expr) -> bool {
    match expr {
        Expr::Int(_) => true,
        Expr::Paren(inner) | Expr::Unary(UnaryOp::Plus | UnaryOp::Minus, inner) => {
            is_integer_literal_shape(inner)
        }
        _ => false,
    }
}

fn parse_signed_integer(expr: &Expr) -> Option<i64> {
    fn parse(expr: &Expr) -> Option<i128> {
        match expr {
            Expr::Int(value) => value.parse::<i128>().ok(),
            Expr::Paren(inner) | Expr::Unary(UnaryOp::Plus, inner) => parse(inner),
            Expr::Unary(UnaryOp::Minus, inner) => parse(inner)?.checked_neg(),
            _ => None,
        }
    }

    i64::try_from(parse(expr)?).ok()
}

fn strip_parens(mut expr: &Expr) -> &Expr {
    while let Expr::Paren(inner) = expr {
        expr = inner;
    }
    expr
}

fn validate_table_ref(
    table_ref: &TableRef,
    table: &ConfiguredTable,
) -> Result<(), ReadOnlyScanError> {
    if !table_ref.partitions.is_empty() {
        return unsupported(UnsupportedReadOnlyFeature::Partition);
    }
    if table_ref.as_of.is_some() {
        return unsupported(UnsupportedReadOnlyFeature::StaleRead);
    }
    if !table_ref.hints.is_empty() {
        return unsupported(UnsupportedReadOnlyFeature::IndexHint);
    }
    if table_ref.sample.is_some() {
        return unsupported(UnsupportedReadOnlyFeature::TableSample);
    }

    let matches = match table_ref.name.as_slice() {
        [name] => identifier_eq(name, &table.table),
        [schema, name] => identifier_eq(schema, &table.schema) && identifier_eq(name, &table.table),
        _ => false,
    };
    if matches {
        Ok(())
    } else {
        Err(ReadOnlyScanError::UnknownTable(table_ref.name.join(".")))
    }
}

fn resolve_column_path<'a>(
    path: &[String],
    table_ref: &TableRef,
    table: &'a ConfiguredTable,
) -> Result<(usize, &'a ConfiguredColumn), ReadOnlyScanError> {
    let alias = table_ref.alias.as_deref().filter(|alias| !alias.is_empty());
    let column_name = match path {
        [column] => Some(column.as_str()),
        [qualifier, column] => {
            let visible_table = alias.unwrap_or(&table.table);
            identifier_eq(qualifier, visible_table).then_some(column.as_str())
        }
        [schema, table_name, column] if alias.is_none() => (identifier_eq(schema, &table.schema)
            && identifier_eq(table_name, &table.table))
        .then_some(column.as_str()),
        _ => None,
    };
    column_name
        .and_then(|name| {
            table
                .columns
                .iter()
                .enumerate()
                .find(|(_, column)| identifier_eq(name, &column.name))
        })
        .ok_or_else(|| ReadOnlyScanError::UnknownColumn(path.join(".")))
}

pub(crate) fn fold_identifier(identifier: &str) -> String {
    identifier.to_lowercase()
}

fn identifier_eq(left: &str, right: &str) -> bool {
    fold_identifier(left) == fold_identifier(right)
}

fn unsupported<T>(feature: UnsupportedReadOnlyFeature) -> Result<T, ReadOnlyScanError> {
    Err(ReadOnlyScanError::Unsupported(feature))
}

fn unsupported_predicate<T>(
    predicate: UnsupportedReadOnlyPredicate,
) -> Result<T, ReadOnlyScanError> {
    Err(ReadOnlyScanError::UnsupportedPredicate(predicate))
}
