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

//! Mechanical lowering for a `DataSource` access receipt selected by the
//! shared planner. This module does not enumerate or cost access paths: it
//! converts the chosen physical scan's ranges, order, direction, and reader
//! kind into executor objects.

use super::access::{index_key_part_name, source_schema_columns};
use super::*;

fn remap_filter(
    filter: &crate::driver::planner_bridge::AccessFilter,
    visible: &str,
    columns: &[(String, FieldType)],
) -> Result<Vec<Expression>, DriverError> {
    fn remap_expression(
        expression: &Expression,
        bindings: &[(i64, crate::driver::merge_decision::RelColumn)],
        visible: &str,
        columns: &[(String, FieldType)],
    ) -> Result<Expression, DriverError> {
        match expression {
            Expression::Column(column) => {
                let binding = bindings
                    .iter()
                    .find(|(unique_id, _)| *unique_id == column.unique_id)
                    .map(|(_, binding)| binding)
                    .ok_or_else(|| {
                        DriverError::unsupported(format!(
                            "shared planner filter column {} has no source binding",
                            column.unique_id
                        ))
                    })?;
                if !binding.relation.eq_ignore_ascii_case(visible) {
                    return Err(DriverError::unsupported(format!(
                        "shared planner filter for {} reached leaf {visible}",
                        binding.relation
                    )));
                }
                let index = columns
                    .iter()
                    .position(|(name, _)| name.eq_ignore_ascii_case(&binding.column))
                    .ok_or_else(|| {
                        DriverError::unsupported(format!(
                            "shared planner filter column {}.{} is absent from the runtime leaf",
                            binding.relation, binding.column
                        ))
                    })?;
                let mut remapped = column.clone();
                remapped.index = index as i64;
                remapped.unique_id = (index + 1) as i64;
                remapped.ret_type = Some(columns[index].1.clone());
                Ok(Expression::Column(remapped))
            }
            Expression::Constant(_) => Ok(expression.clone()),
            Expression::CorrelatedColumn(_) => Err(DriverError::unsupported(
                "a correlated condition cannot be lowered as a DataSource filter",
            )),
            Expression::ScalarFunction(function) => {
                let mut remapped = function.clone();
                remapped.args = function
                    .args
                    .iter()
                    .map(|argument| remap_expression(argument, bindings, visible, columns))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Expression::ScalarFunction(remapped))
            }
        }
    }

    filter
        .conditions
        .iter()
        .map(|condition| remap_expression(condition, &filter.columns, visible, columns))
        .collect()
}

/// Lowers one exact access receipt. Range and filter conversion here is
/// representation work only; the shared planner has already selected the
/// table/index path, reader family, scan direction, ordering property, and
/// the index-side versus table-side physical selections.
pub(crate) fn lower_planner_access(
    table: &KvTable,
    visible: &str,
    columns: &[(String, FieldType)],
    catalog: &Catalog,
    selected: &crate::driver::planner_bridge::AccessDecision,
) -> Result<LeafAccessPath, DriverError> {
    let stats = catalog.table_statistics(table.stats_physical_id());
    let estimate = crate::access_cost::ScanEstimate {
        rows: selected.estimated_rows.unwrap_or_else(|| {
            crate::access_cost::realtime_row_count(stats.as_deref().map(AsRef::as_ref))
        }),
        pseudo: stats.as_deref().is_none_or(|stats| stats.pseudo),
    };
    let convert_ranges = |ranges: &tidb_planner::ranger::types::Ranges| {
        ranges
            .iter()
            .map(|range| IndexRange {
                low: range.low_val.clone(),
                high: range.high_val.clone(),
                low_exclusive: range.low_exclude,
                high_exclusive: range.high_exclude,
            })
            .collect::<Vec<_>>()
    };
    match &selected.path {
        crate::driver::planner_bridge::AccessPath::Table {
            ranges,
            keep_order,
            desc,
        } => {
            if !matches!(
                selected.reader,
                crate::driver::planner_bridge::AccessReader::Table
                    | crate::driver::planner_bridge::AccessReader::Point
                    | crate::driver::planner_bridge::AccessReader::BatchPoint
            ) {
                return Err(DriverError::unsupported(format!(
                    "shared planner selected a non-table reader for table scan {visible}"
                )));
            }
            let unsigned_int_handle = table
                .pk_handle_offset()
                .and_then(|offset| table.columns.get(offset))
                .is_some_and(|column| column.field_type.is_unsigned());
            let ranges = (!ranges.is_empty()
                && !tidb_planner::ranger::types::has_full_range(ranges, unsigned_int_handle))
            .then(|| convert_ranges(ranges));
            Ok(LeafAccessPath::Table {
                ranges,
                estimate,
                filters: remap_filter(&selected.table_filter, visible, columns)?,
                keep_order: *keep_order,
                desc: *desc,
            })
        }
        crate::driver::planner_bridge::AccessPath::Index {
            index_id,
            ranges,
            keep_order,
            desc,
        } => {
            if !matches!(
                selected.reader,
                crate::driver::planner_bridge::AccessReader::Index
                    | crate::driver::planner_bridge::AccessReader::IndexLookup
                    | crate::driver::planner_bridge::AccessReader::Point
                    | crate::driver::planner_bridge::AccessReader::BatchPoint
            ) {
                return Err(DriverError::unsupported(format!(
                    "shared planner selected a table reader for index scan {visible}"
                )));
            }
            let index = table
                .indexes()
                .iter()
                .find(|index| index.id == *index_id)
                .ok_or_else(|| {
                    DriverError::unsupported(format!(
                        "shared planner selected missing index {index_id} for {visible}"
                    ))
                })?;
            let ranges = if ranges.is_empty() {
                vec![IndexRange::full()]
            } else {
                convert_ranges(ranges)
            };
            let order = if *keep_order {
                leaf_index_order(table, index, columns)
            } else {
                Vec::new()
            };
            Ok(LeafAccessPath::Index(LeafIndexPath {
                index_id: *index_id,
                ranges,
                estimate,
                order,
                keep_order: *keep_order,
                desc: *desc,
                covering: selected.reader == crate::driver::planner_bridge::AccessReader::Index,
                index_filters: remap_filter(&selected.index_filter, visible, columns)?,
                table_filters: remap_filter(&selected.table_filter, visible, columns)?,
            }))
        }
    }
}

/// The exact table or index path named by one shared-planner receipt. A table
/// range keeps the already-built `TableScanExec`; an index range replaces it
/// with the streaming index source.
pub(crate) enum LeafAccessPath {
    /// A clustered-handle range over the existing table scan.
    Table {
        /// The ranges to offer to the table source. `None` is the full range.
        ranges: Option<Vec<IndexRange>>,
        /// The estimate printed for the narrowed scan.
        estimate: crate::access_cost::ScanEstimate,
        /// The physical table-side Selection selected above the scan.
        filters: Vec<Expression>,
        /// Go `PhysicalTableScan.KeepOrder`.
        keep_order: bool,
        /// Go `PhysicalTableScan.Desc`.
        desc: bool,
    },
    /// A secondary-index range source.
    Index(LeafIndexPath),
}

/// The order an index walk of `index` delivers, as offsets into the LEAF's
/// row (the layout `columns` describes), or the empty order when a key part
/// names a column that row does not carry.
///
/// [`crate::kv_table::KvIndex::ordered_column_offsets`] is the cut at the
/// first PREFIX key part, which is Go's `idxColLens[colIdx] ==
/// types.UnspecifiedLength` test made unrepresentable. The name lookup is the
/// same one [`crate::driver::merge_decision`] does: an expression index's
/// hidden column is a column of the TABLE that no query row carries, so it
/// truncates the order rather than pointing at the wrong offset.
pub(crate) fn leaf_index_order(
    table: &KvTable,
    index: &crate::kv_table::KvIndex,
    columns: &[(String, FieldType)],
) -> Vec<usize> {
    let mut order = Vec::with_capacity(index.ordered_column_offsets().len());
    for offset in index.ordered_column_offsets() {
        let Some(column) = table.columns.get(*offset) else {
            break;
        };
        let Some(at) = columns
            .iter()
            .position(|(name, _)| name.eq_ignore_ascii_case(&column.name))
        else {
            break;
        };
        order.push(at);
    }
    order
}

/// The whole-index path a join leaf committed to: what
/// [`leaf_index_path`] decided and [`leaf_index_source`] then builds.
pub(crate) struct LeafIndexPath {
    index_id: i64,
    ranges: Vec<IndexRange>,
    estimate: crate::access_cost::ScanEstimate,
    /// The order this walk delivers, in the leaf's own row offsets. Read back
    /// by [`crate::driver::from::build_from`] as the leaf's DELIVERY report;
    /// see [`leaf_index_order`].
    order: Vec<usize>,
    /// Go's `PhysicalIndexScan.KeepOrder`: whether this path was chosen to
    /// SATISFY a property, which is what makes the source answer in index
    /// order rather than reordering its handle batches
    /// ([`IndexRangeSourceExec::answer_in_index_order`]).
    keep_order: bool,
    /// Go's selected scan direction.
    desc: bool,
    /// The shared planner selected a covering `PhysicalIndexReader` rather
    /// than an `IndexLookUpReader`.
    covering: bool,
    /// The physical Selection selected on the index side.
    index_filters: Vec<Expression>,
    /// The physical Selection selected on the table side of a double read.
    table_filters: Vec<Expression>,
}

impl LeafIndexPath {
    /// The order the walk this path describes delivers.
    pub(crate) fn order(&self) -> &[usize] {
        &self.order
    }

    /// Go's cop-side `Selection` above this index scan, when present.
    pub(crate) fn index_filters(&self) -> &[Expression] {
        &self.index_filters
    }

    /// Conditions selected on the table side of an index lookup.
    pub(crate) fn table_filters(&self) -> &[Expression] {
        &self.table_filters
    }

    /// Consume the representation-only fields shared by SELECT and DML
    /// lowering. Both callers must interpret one physical access receipt the
    /// same way; executor construction details stay in [`leaf_index_source`].
    pub(crate) fn into_scan_parts(
        self,
    ) -> (i64, Vec<IndexRange>, crate::access_cost::ScanEstimate) {
        (self.index_id, self.ranges, self.estimate)
    }
}

/// The streaming source and the `EXPLAIN` node for an index path a leaf
/// committed to, replacing the whole-table scan
/// [`crate::driver::from::build_from`] installed for it.
///
/// The shared physical scan already records `KeepOrder` and `Desc`; this
/// lowering does not run a second executor-local order proof.
pub(crate) fn leaf_index_source(
    table: &KvTable,
    visible: &str,
    columns: &[(String, FieldType)],
    path: LeafIndexPath,
    trace: Option<&mut PlanTrace>,
    ctx: &crate::StmtContext,
) -> Box<dyn Executor> {
    let LeafIndexPath {
        index_id,
        ranges,
        estimate,
        order: _,
        keep_order,
        desc,
        covering,
        index_filters: _,
        table_filters: _,
    } = path;
    let mut trace = trace;
    if let Some(trace) = trace.as_deref_mut() {
        let index = table
            .indexes()
            .iter()
            .find(|index| index.id == index_id)
            .expect("the chosen path names an index of this table");
        let index_columns: Vec<String> = index
            .column_offsets
            .iter()
            .map(|offset| index_key_part_name(table, *offset))
            .collect();
        let index_columns: Vec<&str> = index_columns.iter().map(String::as_str).collect();
        if ranges.is_empty() {
            trace.empty_range_table_dual();
        } else if ranges.len() == 1 && ranges[0].is_full() {
            trace.index_full_scan(visible, &index.name, &index_columns, estimate, keep_order);
        } else {
            trace.index_range_scan(visible, &index.name, &index_columns, &ranges, estimate);
            if keep_order {
                trace.keep_order(desc);
            }
        }
    }
    let mut exec = IndexRangeSourceExec::new_with_context(
        ExecutorMeta::new(
            Schema::new(source_schema_columns(columns)),
            0,
            INIT_CAP,
            MAX_CHUNK_SIZE,
        ),
        table.clone(),
        index_id,
        ranges,
        crate::kv_table::RowDecodeContext::for_query(ctx),
    );
    exec.set_lookup_concurrency(ctx.executor_concurrency());
    // The schema above may already be NARROWER than the table (the leaf
    // demand prunes before the access path replaces the source), so the
    // reader is told which stored column each slot is rather than assuming
    // the first n. `_tidb_rowid` has no stored column at all -- it is the
    // record HANDLE, which this reader already holds for every row it looks
    // up -- so it is named separately.
    let handle_slot = crate::access_path::extra_handle_slot(columns);
    if let Some(slot) = handle_slot {
        exec.read_extra_handle(slot);
    }
    let stored = handle_slot.map_or(columns, |slot| &columns[..slot]);
    if let Some(offsets) = crate::access_path::stored_column_offsets(table, stored) {
        exec.read_table_columns(offsets);
    }
    crate::table_access::TableAccess::accept_scan_estimate(&mut exec, estimate.rows);
    if covering {
        exec.mark_covering();
        exec.answer_in_index_order();
    }
    if keep_order {
        // Go's `keep order:true` index read: `canReorderHandles` is false, so
        // the lookup batches are sorted BACK into index order and the rows
        // leave in the order the walk produced them. Without this the source
        // answers in handle order, which is the exact promise-without-delivery
        // a parent merge join must never be given.
        //
        // A DIRTY table is NOT gated here, though Go wraps a join leaf's
        // reader in `UnionScanExec` exactly as it wraps a single-table read's
        // (`buildUnionScanFromReader` is reached per `DataSource`). MEASURED:
        // adding `|| table.has_dirty_content()` changes no row order this tier
        // can produce -- a leaf that reaches this builder is already answering
        // in index order for both the clean and the dirty read of the same
        // join, with either hash side forced -- and it would leave
        // `LeafIndexPath::order` reporting an order the source now delivers
        // but does not promise. The single-table gate in
        // `super::access::commit_index_range_source` is the one that moves
        // rows.
        if !crate::table_access::TableAccess::accept_keep_order(&mut exec, desc) {
            unreachable!("IndexRangeSourceExec accepts the selected index order");
        }
    }
    if let Some(trace) = trace {
        if !covering {
            let lowered = trace.index_lookup(visible, estimate);
            debug_assert!(lowered, "an index lookup receipt must follow an index scan");
        }
        trace.set_scan_act_rows(exec.produced_rows());
    }
    Box::new(exec)
}
