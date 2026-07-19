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

//! Narrow SQL-to-TiKV table-scan lowering for the first read-only node.
//!
//! This boundary intentionally accepts exactly one configured,
//! non-partitioned table with signed `BIGINT` columns and exactly one clustered
//! primary key. It
//! validates the complete parsed query envelope before entering the existing
//! `LogicalDataSource -> TableAccessPath -> PhysicalTableReaderPlan` path.

use std::{collections::HashSet, error::Error, fmt};

use tidb_ast::{
    Expr, JoinNode, JoinType, QueryStmt, SelectField, SelectStatementKind, SelectStmt, Stmt,
    TableRef,
};

use crate::{
    access_path::{
        DataSourceAccessPath, PointGetAdmission, ResolvedTableDescriptor, ResolvedTableScanKind,
        TableAccessPath, TableScanExplainIdSuffix,
    },
    index_task::{ScanReadTask, ScanReadTaskRejection},
    logical_data_source::LogicalDataSource,
    logical_data_source_task::IndexTaskProperty,
    physical_table_scan::PhysicalTableScanPlan,
    scan_pushdown::{ScanColumnInfo, TiKvTableScanSpec},
    task_type::TaskType,
};

const MYSQL_TYPE_LONGLONG: i32 = 8;
const BINARY_COLLATION_ID: i32 = 63;
const NOT_NULL_FLAG: i32 = 1;
const PRI_KEY_FLAG: i32 = 1 << 1;
const PHYSICAL_PLAN_ID: i32 = 1;

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
}

impl ConfiguredColumn {
    /// Configures the table's signed `BIGINT PRIMARY KEY CLUSTERED` column.
    #[must_use]
    pub fn clustered_primary_key(name: impl Into<String>, id: i64) -> Self {
        Self {
            name: name.into(),
            id,
            kind: ConfiguredColumnKind::ClusteredPrimaryKey,
        }
    }

    /// Configures one signed stored `BIGINT NOT NULL` column.
    #[must_use]
    pub fn stored_not_null(name: impl Into<String>, id: i64) -> Self {
        Self {
            name: name.into(),
            id,
            kind: ConfiguredColumnKind::StoredNotNull,
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

    fn scan_column(&self) -> ScanColumnInfo {
        let (flag, pk_handle) = match self.kind {
            ConfiguredColumnKind::ClusteredPrimaryKey => (NOT_NULL_FLAG | PRI_KEY_FLAG, true),
            ConfiguredColumnKind::StoredNotNull => (NOT_NULL_FLAG, false),
        };
        ScanColumnInfo {
            column_id: self.id,
            tp: MYSQL_TYPE_LONGLONG,
            collation: BINARY_COLLATION_ID,
            column_len: 20,
            decimal: 0,
            flag,
            pk_handle,
            ..ScanColumnInfo::default()
        }
    }
}

/// The complete catalog input admitted by the first read-only SQL node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfiguredTable {
    schema: String,
    table: String,
    table_id: i64,
    columns: Vec<ConfiguredColumn>,
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
        }
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

    fn validate(&self) -> Result<(), ReadOnlyScanError> {
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
            if !names.insert(column.name.to_ascii_lowercase()) {
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
}

impl ResolvedProjectionColumn {
    fn new(output_name: String, column: &ConfiguredColumn) -> Self {
        Self {
            output_name,
            source_name: column.name.clone(),
            scan_column: column.scan_column(),
            kind: column.kind,
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
    /// The `WHERE` clause requires predicate/range planning.
    Predicate,
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

/// Why a SQL statement cannot become the first read-only table scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadOnlyScanError {
    /// SQL parsing failed.
    Parse(String),
    /// The configured catalog entry violates the milestone contract.
    InvalidConfiguration(&'static str),
    /// A parsed feature has no owner in this milestone.
    Unsupported(UnsupportedReadOnlyFeature),
    /// The query names a table other than the configured table.
    UnknownTable(String),
    /// The query names a column outside the configured table.
    UnknownColumn(String),
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
            Self::UnknownTable(table) => write!(formatter, "unknown table: {table}"),
            Self::UnknownColumn(column) => write!(formatter, "unknown column: {column}"),
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

/// One validated direct projection lowered to the existing physical scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadOnlyScanPlan {
    reader: crate::physical_table_reader::PhysicalTableReaderPlan,
    projected_columns: Vec<ResolvedProjectionColumn>,
}

impl ReadOnlyScanPlan {
    /// Parses and lowers one direct-column `SELECT` against `table`.
    pub fn lower(sql: &str, table: &ConfiguredTable) -> Result<Self, ReadOnlyScanError> {
        table.validate()?;
        let statement = tidb_parser::parse(sql).map_err(|error| {
            ReadOnlyScanError::Parse(format!("{} at byte {}", error.message, error.offset))
        })?;
        let select = match statement {
            Stmt::Query(query) => match *query {
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

        let projected_columns = validate_select(&select, table)?;
        let scan_columns = projected_columns
            .iter()
            .map(|column| column.scan_column.clone())
            .collect();
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
}

fn validate_select(
    select: &SelectStmt,
    table: &ConfiguredTable,
) -> Result<Vec<ResolvedProjectionColumn>, ReadOnlyScanError> {
    if select.kind != SelectStatementKind::Select
        || select.is_in_braces
        || !select.values.is_empty()
    {
        return unsupported(UnsupportedReadOnlyFeature::QueryForm);
    }
    if select.with.is_some() {
        return unsupported(UnsupportedReadOnlyFeature::CommonTableExpression);
    }
    if !select.hints.is_empty() || select.calc_found_rows || select.distinct || select.all {
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

    if select.where_clause.is_some() {
        return unsupported(UnsupportedReadOnlyFeature::Predicate);
    }
    if !select.group_by.is_empty() || select.rollup || select.having.is_some() {
        return unsupported(UnsupportedReadOnlyFeature::Grouping);
    }
    if !select.windows.is_empty() {
        return unsupported(UnsupportedReadOnlyFeature::Window);
    }
    if !select.order_by.is_empty() {
        return unsupported(UnsupportedReadOnlyFeature::Ordering);
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
        let column = resolve_column_path(path, table_ref, table)?;
        let output_name = alias
            .as_deref()
            .filter(|alias| !alias.is_empty())
            .unwrap_or(&column.name)
            .to_owned();
        columns.push(ResolvedProjectionColumn::new(output_name, column));
    }
    Ok(columns)
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
) -> Result<&'a ConfiguredColumn, ReadOnlyScanError> {
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
                .find(|column| identifier_eq(name, &column.name))
        })
        .ok_or_else(|| ReadOnlyScanError::UnknownColumn(path.join(".")))
}

fn identifier_eq(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn unsupported<T>(feature: UnsupportedReadOnlyFeature) -> Result<T, ReadOnlyScanError> {
    Err(ReadOnlyScanError::Unsupported(feature))
}
