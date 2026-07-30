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

//! Prepared (parameterized) point reads for the bounded read-only node: the
//! resolved `ORDER BY` key and `SUM` aggregate descriptors, the reusable
//! `ConfiguredPreparedPointReadTemplate` and its `bind`, and the planner
//! resolvers that build those descriptors out of one parsed `SELECT`.
//!
//! Split out of `read_only_scan.rs`; mirrors the prepared-statement plan
//! cache path of Go `pkg/planner/core` (`plan_cache.go`'s
//! `PlanCacheStmt` / parameter rebinding) as it applies to a point read.

use super::*;

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
    nullable: bool,
}

impl PreparedOrderColumn {
    /// Creates a resolved order column from an output-row offset, its direction,
    /// the projected column's scalar type, and whether it admits `NULL`.
    #[must_use]
    pub const fn new(
        output_offset: usize,
        direction: ConfiguredOrderDirection,
        scalar_type: ConfiguredScalarType,
        nullable: bool,
    ) -> Self {
        Self {
            output_offset,
            direction,
            scalar_type,
            nullable,
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

    /// Returns whether this key column admits `NULL` values.
    #[must_use]
    pub const fn is_nullable(&self) -> bool {
        self.nullable
    }
}

/// A single-column aggregate the prepared read evaluates over its scan output.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreparedAggregateKind {
    /// `SUM(<signed integer or decimal column>)`, whose result is an exact
    /// `DECIMAL`.
    Sum,
}

/// One planner-resolved single-column aggregate for a prepared read.
///
/// The prepared read has no `GROUP BY`, so a `SUM` collapses the whole scan to
/// one output row. The scan still projects the summed column; the executor folds
/// that column's values into the single result row (Go `AggFuncSum`: an integer
/// argument promotes to an exact `DECIMAL`, a decimal argument sums exactly in
/// place, an empty set yields `NULL`). The result column metadata is the
/// aggregate's own type — `DECIMAL(flen, decimal)` per Go `typeInfer4Sum` (an
/// integer argument always yields `decimal = 0`; a decimal argument keeps its
/// own scale) — not the summed column's.
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

    /// Returns the result `DECIMAL` column's scale (`0` for an integer `SUM`,
    /// the argument's own scale for a decimal `SUM`).
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
    lock: Option<ReadLockRequest>,
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
                // The locking clause is parameter-independent but not
                // executor-side: it belongs to the bound plan, because the
                // handles it locks are the ones the bound parameters produced.
                lock: self.lock,
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
        lock: validated.lock,
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

/// Resolves the one supported aggregate shape: a single
/// `SUM(<integer or decimal column>)` field.
///
/// Returns `None` when no field is an aggregate, so the caller treats the query
/// as an ordinary column projection. Returns `Some((scan projections, aggregate))`
/// for the supported shape: the scan projects the summed column, and the
/// aggregate carries the `DECIMAL` result metadata (Go `typeInfer4Sum`). Any
/// other aggregate shape — more than one field, `DISTINCT`, a non-column or
/// non-integer/non-decimal argument, or a function other than `SUM` — fails
/// closed, since a wrong aggregate is a silent correctness bug rather than a
/// missing feature.
pub(super) fn resolve_prepared_aggregate(
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
    // Go `typeInfer4Sum` returns a DECIMAL only for an integer or DECIMAL
    // argument; a string argument would be a DOUBLE result, a type path this
    // narrow read does not own yet.
    let (result_column_length, result_decimals) = match column.scalar_type() {
        ConfiguredScalarType::Int
        | ConfiguredScalarType::BigInt
        | ConfiguredScalarType::UnsignedBigInt => {
            let arg_flen = column.scalar_type().result_column_length() as u32;
            // `SetFlenUnderLimit(arg.Flen + 21)`, `SetDecimal(0)`.
            let flen = (arg_flen + SUM_DECIMAL_FLEN_EXTENSION).min(MAX_DECIMAL_WIDTH);
            (flen, 0)
        }
        // A DECIMAL argument keeps `typeInfer4Sum`'s DECIMAL result, widened
        // per `UpdateFlenAndDecimalUnderLimit(arg, deltaDecimal=0,
        // deltaFlen=22)`: the scale (decimal) is unchanged from the argument's
        // own declared scale, and the flen grows by 22 digits, clamped to
        // `mysql.MaxDecimalWidth` (65). Captured against Go
        // (`pkg/executor/zz_dump_sumdec_test.go`): SUM(DECIMAL(10,2)) sums
        // exactly (10.10+20.20+12.34 = 42.64), an empty group is NULL, and
        // SUM(DECIMAL(65,2)) does not overflow (flen clamps to 65).
        ConfiguredScalarType::Decimal { precision, scale } => {
            let flen = (precision + SUM_DECIMAL_ARG_FLEN_EXTENSION).min(MAX_DECIMAL_WIDTH);
            (flen, scale as u8)
        }
        // Go `typeInfer4Sum` returns a DOUBLE (not DECIMAL) for a floating-point
        // argument, a result shape this DECIMAL-only path does not own yet.
        ConfiguredScalarType::Double
        | ConfiguredScalarType::Char { .. }
        | ConfiguredScalarType::Varchar { .. }
        | ConfiguredScalarType::Date
        | ConfiguredScalarType::Datetime { .. }
        | ConfiguredScalarType::Timestamp { .. }
        | ConfiguredScalarType::Duration { .. } => {
            return unsupported(UnsupportedReadOnlyFeature::Aggregate)
        }
    };
    let output_name = alias
        .as_deref()
        .filter(|alias| !alias.is_empty())
        .map_or_else(|| format!("{}({})", name, column.name), str::to_owned);
    let aggregate = PreparedAggregate::new(
        PreparedAggregateKind::Sum,
        0,
        output_name,
        result_column_length,
        result_decimals,
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
pub(super) fn resolve_prepared_order_by(
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
                column.is_nullable(),
            ))
        })
        .collect()
}
