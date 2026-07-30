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

//! The fail-closed rejection vocabulary of the bounded read-only node: the
//! unsupported-feature and unsupported-predicate enumerations, the prepared
//! plan/bind errors, and `ReadOnlyScanError` with its `Display`/`Error`
//! impls.
//!
//! Split out of `read_only_scan.rs`. Go raises these as
//! `plannererrors`/`dbterror` values scattered across `pkg/planner/core`;
//! this node enumerates them so every refusal is one named shape.

use super::*;

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
