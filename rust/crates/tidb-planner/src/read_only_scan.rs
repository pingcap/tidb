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
    BinaryOp, Expr, JoinNode, JoinType, QueryStmt, SelectField, SelectStatementKind, SelectStmt,
    Stmt, TableRef, UnaryOp,
};

use crate::{
    access_path::{
        DataSourceAccessPath, PointGetAdmission, ResolvedTableDescriptor, ResolvedTableScanKind,
        TableAccessPath, TableScanExplainIdSuffix,
    },
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UnboundComparisonOperand {
    Column(usize),
    Int(i64),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct UnboundComparison {
    op: ComparisonOp,
    lhs: UnboundComparisonOperand,
    rhs: UnboundComparisonOperand,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ValidatedReadOnlySelect {
    projected_columns: Vec<ResolvedProjectionColumn>,
    comparisons: Vec<UnboundComparison>,
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

        let validated = validate_select(&select, table)?;
        let projected_columns = validated.projected_columns;
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
            let column = &table.columns[column_index];
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
    }
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
        let (_, column) = resolve_column_path(path, table_ref, table)?;
        let output_name = alias
            .as_deref()
            .filter(|alias| !alias.is_empty())
            .unwrap_or(&column.name)
            .to_owned();
        columns.push(ResolvedProjectionColumn::new(output_name, column));
    }
    Ok(ValidatedReadOnlySelect {
        projected_columns: columns,
        comparisons,
    })
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
            if !matches!(
                (lhs, rhs),
                (
                    UnboundComparisonOperand::Column(_),
                    UnboundComparisonOperand::Int(_)
                ) | (
                    UnboundComparisonOperand::Int(_),
                    UnboundComparisonOperand::Column(_)
                )
            ) {
                return unsupported_predicate(UnsupportedReadOnlyPredicate::ColumnIntegerPair);
            }
            comparisons.push(UnboundComparison { op, lhs, rhs });
            Ok(())
        }
        _ => unsupported_predicate(UnsupportedReadOnlyPredicate::ComparisonOperator),
    }
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
