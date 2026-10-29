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

//! Go `PhysicalPlan.MemoryUsage` for every operator
//! (`pkg/planner/core/operator/physicalop/*.go`).
//!
//! The plan cache refuses a plan whose `MemoryUsage()` exceeds
//! `tidb_plan_cache_max_plan_size` (`plan_cacheable_checker.go:588`), so the
//! number has to be the one Go computes, not the weight of this port's own
//! layouts. Each constant is Go's `unsafe.Sizeof` on 64-bit and each term is
//! the one Go sums, including the places where Go counts the same data twice
//! (a reader's `PlanPartInfo` and its scan's, the scan's filter conditions and
//! the Selection's) and the one place where it counts nothing
//! (`PhysicalIndexReader` discards `IndexPlan.MemoryUsage()`).
//!
//! Go charges slice CAPACITIES; every list in this port is built exact-sized,
//! so lengths stand in for them.

use tidb_expression_aggregation::{AggFuncDesc, BaseFuncDesc, ByItems};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_size::{
    SIZE_OF_BOOL, SIZE_OF_FLOAT64, SIZE_OF_INT, SIZE_OF_INT32, SIZE_OF_INTERFACE, SIZE_OF_MAP,
    SIZE_OF_POINTER, SIZE_OF_SLICE, SIZE_OF_STRING, SIZE_OF_UINT64, SIZE_OF_UINT8,
};

use super::part_info::part_info_memory_usage;
use super::{BasePhysicalPlan, PhysicalPlan, PhysicalTableScan};
use crate::physical_property::{ColumnSortItem, MppPartitionColumn, PhysicalProperty};

/// Go `emptyPhysicalTableScanSize`.
pub const GO_EMPTY_PHYSICAL_TABLE_SCAN_SIZE: i64 = 672;
/// Go `emptyPhysicalIndexScanSize`.
pub const GO_EMPTY_PHYSICAL_INDEX_SCAN_SIZE: i64 = 568;
/// Go `emptyPointGetPlanSize`.
pub const GO_EMPTY_POINT_GET_PLAN_SIZE: i64 = 480;
/// Go `emptyBatchPointGetPlanSize`.
pub const GO_EMPTY_BATCH_POINT_GET_PLAN_SIZE: i64 = 544;
/// Go `model.EmptyColumnInfoSize`.
pub const GO_EMPTY_COLUMN_INFO_SIZE: i64 = 344;
/// Go `pushedDownLimitSize`.
const GO_PUSHED_DOWN_LIMIT_SIZE: i64 = 16;
/// Go `unsafe.Sizeof(ast.SelectLockInfo{})`.
const GO_SELECT_LOCK_INFO_SIZE: i64 = 40;

/// Go `CIStr.MemoryUsage` for one spelling.
fn ci_str_memory_usage(name: &str) -> i64 {
    let ci = tidb_ast::CiString::new(name);
    SIZE_OF_STRING * 2 + (ci.original().len() + ci.lowercase().len()) as i64
}

fn exprs_memory_usage(exprs: &[Expression]) -> i64 {
    exprs.iter().map(Expression::memory_usage).sum()
}

fn columns_memory_usage(columns: &[Column]) -> i64 {
    columns.iter().map(Column::memory_usage).sum()
}

/// Go `ByItems.MemoryUsage` (`planner/util/byitem.go:70`).
fn by_items_memory_usage(items: &[ByItems]) -> i64 {
    items
        .iter()
        .map(|item| SIZE_OF_BOOL + item.expr.memory_usage())
        .sum()
}

/// Go `property.SortItem.MemoryUsage`.
fn sort_items_memory_usage(items: &[ColumnSortItem]) -> i64 {
    items
        .iter()
        .map(|item| SIZE_OF_BOOL + item.col.memory_usage())
        .sum()
}

/// Go `baseFuncDesc.MemoryUsage` (`aggregation/base_func.go:541`).
fn base_func_desc_memory_usage(desc: &BaseFuncDesc) -> i64 {
    SIZE_OF_STRING
        + desc.name.len() as i64
        + desc.ret_type.memory_usage() as i64
        + exprs_memory_usage(&desc.args)
}

/// Go `AggFuncDesc.MemoryUsage` (`aggregation/descriptor.go:389`).
fn agg_func_desc_memory_usage(desc: &AggFuncDesc) -> i64 {
    base_func_desc_memory_usage(&desc.base)
        + SIZE_OF_INT
        + SIZE_OF_BOOL
        + by_items_memory_usage(&desc.order_by_items)
}

fn mpp_columns_memory_usage(columns: &[MppPartitionColumn]) -> i64 {
    columns
        .iter()
        .map(|column| SIZE_OF_INT32 + column.col.memory_usage())
        .sum()
}

impl BasePhysicalPlan {
    /// Go `BasePhysicalPlan.MemoryUsage` (`base_physical_plan.go:268`) WITHOUT
    /// the children, which [`PhysicalPlan::memory_usage`] walks: the embedded
    /// plan, the required-property list, the children slice (one slot more
    /// than it holds, as Go's `cap(children)+1`), three scalars, and every
    /// required property.
    #[must_use]
    pub fn go_memory_usage(&self) -> i64 {
        self.base.memory_usage()
            + SIZE_OF_SLICE
            + self.children_req_props.len() as i64 * SIZE_OF_POINTER
            + SIZE_OF_SLICE
            + (self.children.len() as i64 + 1) * SIZE_OF_INTERFACE
            + SIZE_OF_FLOAT64
            + SIZE_OF_UINT64
            + SIZE_OF_BOOL
            + self
                .children_req_props
                .iter()
                .flatten()
                .map(PhysicalProperty::go_memory_usage)
                .sum::<i64>()
    }

    /// Go `PhysicalSchemaProducer.MemoryUsage` (`physical_schema_producer.go`):
    /// the base plus one pointer (the schema itself is NOT charged).
    #[must_use]
    pub fn go_schema_producer_memory_usage(&self) -> i64 {
        self.go_memory_usage() + SIZE_OF_POINTER
    }

    /// Go `SimpleSchemaProducer.MemoryUsage`: the plan, the schema and its
    /// names (point gets and DML roots embed this one, which charges the
    /// schema).
    fn go_simple_schema_producer_memory_usage(&self) -> i64 {
        SIZE_OF_POINTER
            + SIZE_OF_SLICE
            + self.base.memory_usage()
            + self.base.schema().map_or(0, |schema| schema.memory_usage())
            + names_memory_usage(self)
    }
}

/// Go `Plan.MemoryUsage` of a subtree including everything a reader owns:
/// the whole of what `plan.MemoryUsage()` returns for Go's operator.
fn subtree_usage(node: &PhysicalPlan) -> i64 {
    node.memory_usage()
}

/// The sum of `subtree_usage` over every node of a pushed-down chain: Go's
/// flattened `IndexPlans` / `TablePlans` lists hold each node, and each
/// node's `MemoryUsage()` already includes its children.
fn flat_usage(node: &PhysicalPlan) -> i64 {
    subtree_usage(node) + node.children().iter().map(flat_usage).sum::<i64>()
}

fn optional_subtree_usage(node: Option<&PhysicalPlan>) -> i64 {
    node.map_or(0, subtree_usage)
}

/// Go `ranger.FullIntRange` weighs this when a scan reads everything: this
/// port leaves such a scan's range list empty ("reads as the full scan"), so
/// the one range Go holds is charged here.
fn scan_ranges_memory_usage(ranges: &tidb_ranger::types::Ranges, reads_by_handle: bool) -> i64 {
    // A lookup's row-id side reads by handle and holds no ranges in Go either.
    if ranges.is_empty() && !reads_by_handle {
        tidb_ranger::points::full_int_range(false)
            .iter()
            .map(|range| range.mem_usage())
            .sum()
    } else {
        ranges.iter().map(|range| range.mem_usage()).sum()
    }
}

/// Go's `TableAsName` is always a non-nil `*CIStr`: the alias when one was
/// written, an empty name otherwise.
fn table_as_name_memory_usage(name: Option<&str>) -> i64 {
    ci_str_memory_usage(name.unwrap_or(""))
}

fn prop_memory_usage(prop: Option<&Box<PhysicalProperty>>) -> i64 {
    prop.map_or(0, |prop| prop.go_memory_usage())
}

fn table_scan_usage(scan: &PhysicalTableScan) -> i64 {
    GO_EMPTY_PHYSICAL_TABLE_SCAN_SIZE
        + scan.base.go_schema_producer_memory_usage()
        + ci_str_memory_usage(&scan.db_name)
        + part_info_memory_usage(scan.plan_part_info.as_ref())
        + table_as_name_memory_usage(scan.table_as_name.as_deref())
        + prop_memory_usage(scan.prop.as_ref())
        + scan
            .range_rebuild
            .as_ref()
            .map_or(0, |rebuild| exprs_memory_usage(&rebuild.access_conditions))
        + exprs_memory_usage(&scan.filter_condition)
        + scan_ranges_memory_usage(
            &scan.ranges,
            scan.resolved_explain_id(true).as_deref() == Some("TableRowIDScan"),
        )
        + columns_memory_usage(&scan.cost_columns)
}

fn index_scan_usage(scan: &super::PhysicalIndexScan) -> i64 {
    let (access_conditions, index_columns) = scan.range_rebuild.as_ref().map_or_else(
        || (0, columns_memory_usage(&scan.cost_columns)),
        |rebuild| {
            (
                exprs_memory_usage(&rebuild.access_conditions),
                columns_memory_usage(&rebuild.index_columns),
            )
        },
    );
    GO_EMPTY_PHYSICAL_INDEX_SCAN_SIZE
        + scan.base.go_schema_producer_memory_usage()
        // `IdxColLens`.
        + scan
            .range_rebuild
            .as_ref()
            .map_or(0, |rebuild| rebuild.index_column_lengths.len() as i64 * SIZE_OF_INT)
        + ci_str_memory_usage(&scan.db_name)
        // `Columns`: one `ColumnInfo` per data-source column.
        + scan
            .data_source_schema
            .as_ref()
            .map_or(0, |schema| schema.columns.len() as i64 * GO_EMPTY_COLUMN_INFO_SIZE)
        + table_as_name_memory_usage(scan.table_as_name.as_deref())
        + scan.pk_is_handle_col.as_ref().map_or(0, Column::memory_usage)
        + prop_memory_usage(scan.prop.as_ref())
        + scan.data_source_schema.as_ref().map_or(0, |schema| schema.memory_usage())
        + access_conditions
        + index_columns
        + scan_ranges_memory_usage(&scan.ranges, false)
}

/// The node's own bytes under Go's `MemoryUsage`, children excluded (readers
/// include the push-down plans they own because those are not `children`).
pub(super) fn go_node_memory_usage(node: &PhysicalPlan) -> i64 {
    match node {
        PhysicalPlan::Selection(op) => {
            op.base.go_memory_usage() + SIZE_OF_BOOL + exprs_memory_usage(&op.conditions)
        }
        PhysicalPlan::Projection(op) => {
            op.base.go_memory_usage() + SIZE_OF_BOOL * 2 + exprs_memory_usage(&op.exprs)
        }
        PhysicalPlan::TableScan(op) => table_scan_usage(op),
        PhysicalPlan::TableReader(op) => {
            op.base.go_schema_producer_memory_usage()
                + SIZE_OF_UINT8 * 2
                + SIZE_OF_BOOL
                + part_info_memory_usage(op.plan_part_info.as_ref())
                + optional_subtree_usage(op.table_plan.as_deref())
                + mpp_scan_infos_usage(op.table_plan.as_deref())
        }
        PhysicalPlan::IndexScan(op) => index_scan_usage(op),
        PhysicalPlan::IndexReader(op) => {
            op.base.go_schema_producer_memory_usage()
                + part_info_memory_usage(op.plan_part_info.as_ref())
                // Go computes `IndexPlan.MemoryUsage()` and drops the result.
                + op.index_plan.as_deref().map_or(0, flat_usage)
                + columns_memory_usage(&op.output_columns)
        }
        PhysicalPlan::IndexLookUpReader(op) => {
            op.base.go_schema_producer_memory_usage()
                + SIZE_OF_BOOL * 3
                + part_info_memory_usage(op.plan_part_info.as_ref())
                + SIZE_OF_UINT64
                + SIZE_OF_INT * op.index_plans_un_natural_orders.len() as i64
                + optional_subtree_usage(op.index_plan.as_deref())
                + optional_subtree_usage(op.table_plan.as_deref())
                + op.pushed_limit.map_or(0, |_| GO_PUSHED_DOWN_LIMIT_SIZE)
                + columns_memory_usage(&op.common_handle_cols)
        }
        PhysicalPlan::IndexMergeReader(op) => {
            op.base.go_schema_producer_memory_usage()
                + part_info_memory_usage(op.plan_part_info.as_ref())
                + optional_subtree_usage(op.table_plan.as_deref())
                // `PartialPlans`, the flattened chains.
                + op.partial_plans_raw.iter().map(flat_usage).sum::<i64>()
                // `TablePlans`, the flattened table chain.
                + op.table_plan.as_deref().map_or(0, flat_usage)
                // `PartialPlansRaw`.
                + op.partial_plans_raw.iter().map(subtree_usage).sum::<i64>()
        }
        // Go `PhysicalUnionScan.MemoryUsage`: the base, one slice, the handle
        // columns and every condition.
        PhysicalPlan::UnionScan(op) => {
            op.base.go_memory_usage()
                + SIZE_OF_SLICE
                + columns_memory_usage(&op.handle_cols)
                + exprs_memory_usage(&op.conditions)
        }
        PhysicalPlan::Sort(op) => sort_memory_usage(op),
        PhysicalPlan::TopN(op) => {
            op.base.go_memory_usage()
                + SIZE_OF_SLICE
                + op.by_items.len() as i64 * SIZE_OF_POINTER
                + SIZE_OF_UINT64 * 2
                + SIZE_OF_POINTER
                + SIZE_OF_INT
                + by_items_memory_usage(&op.by_items)
                + op.partition_by
                    .iter()
                    .map(|item| SIZE_OF_BOOL + item.col.memory_usage())
                    .sum::<i64>()
        }
        PhysicalPlan::Limit(op) => {
            op.base.go_schema_producer_memory_usage()
                + SIZE_OF_UINT64 * 2
                + SIZE_OF_POINTER
                + SIZE_OF_INT
        }
        PhysicalPlan::HashAgg(op) => {
            op.base.go_schema_producer_memory_usage()
                + SIZE_OF_INT
                + op.agg_funcs
                    .iter()
                    .map(agg_func_desc_memory_usage)
                    .sum::<i64>()
                + exprs_memory_usage(&op.group_by_items)
                + mpp_columns_memory_usage(&op.mpp_partition_cols)
        }
        PhysicalPlan::StreamAgg(op) => {
            op.base.go_schema_producer_memory_usage()
                + SIZE_OF_INT
                + op.agg_funcs
                    .iter()
                    .map(agg_func_desc_memory_usage)
                    .sum::<i64>()
                + exprs_memory_usage(&op.group_by_items)
        }
        PhysicalPlan::UnionAll(op) => op.base.go_schema_producer_memory_usage() + SIZE_OF_BOOL,
        PhysicalPlan::MaxOneRow(op) => op.base.go_memory_usage(),
        PhysicalPlan::TableDual(op) => {
            op.base.go_schema_producer_memory_usage() + SIZE_OF_INT + SIZE_OF_SLICE
        }
        PhysicalPlan::Lock(op) => {
            op.base.go_memory_usage()
                + SIZE_OF_POINTER
                + SIZE_OF_MAP * 2
                + GO_SELECT_LOCK_INFO_SIZE
                + op.tbl_id_to_handle_cols
                    .values()
                    .map(|cols| {
                        tidb_size::SIZE_OF_INT64
                            + SIZE_OF_SLICE
                            + cols.len() as i64 * SIZE_OF_INTERFACE
                            + columns_memory_usage(cols)
                    })
                    .sum::<i64>()
                + op.tbl_id_to_phys_tbl_id_col
                    .values()
                    .map(|col| {
                        tidb_size::SIZE_OF_INT64 + SIZE_OF_POINTER + col.memory_usage()
                    })
                    .sum::<i64>()
        }
        PhysicalPlan::Window(op) => {
            op.base.go_schema_producer_memory_usage()
                + SIZE_OF_SLICE * 3
                + op.window_func_descs.len() as i64 * SIZE_OF_POINTER
                + SIZE_OF_UINT8
                + op.window_func_descs
                    .iter()
                    .map(|desc| base_func_desc_memory_usage(&desc.base))
                    .sum::<i64>()
                + sort_items_memory_usage(&op.partition_by)
                + sort_items_memory_usage(&op.order_by)
        }
        PhysicalPlan::PointGet(op) => {
            // `PointGetPlan` embeds the plan, not a schema producer: it charges
            // its schema and its output names itself.
            GO_EMPTY_POINT_GET_PLAN_SIZE
                + op.base.base.memory_usage()
                + op.base
                    .base
                    .schema()
                    .map_or(0, |schema| schema.memory_usage())
                + point_plan_db_name_len(&op.base)
                + (point_plan_name_count(&op.base) + 1) * SIZE_OF_POINTER
                + names_memory_usage_without_slots(&op.base)
                + point_plan_keys_usage(op.range_rebuild.as_ref(), op.index_id.is_none(), false)
        }
        PhysicalPlan::BatchPointGet(op) => {
            GO_EMPTY_BATCH_POINT_GET_PLAN_SIZE
                + op.base.go_simple_schema_producer_memory_usage()
                + point_plan_db_name_len(&op.base)
                + point_plan_keys_usage(op.range_rebuild.as_ref(), op.index_id.is_none(), true)
        }
        PhysicalPlan::CTE(op) => {
            // `PhysicalCTE` and its `CTEDefinition` each hold the name and
            // the same two plans.
            2 * (op.base.go_schema_producer_memory_usage() + ci_str_memory_usage(&op.cte_as_name))
                + 2 * (subtree_usage(&op.seed_plan)
                    + optional_subtree_usage(op.recursive_plan.as_deref()))
        }
        PhysicalPlan::CTETable(op) => op.base.go_schema_producer_memory_usage() + SIZE_OF_INT,
        PhysicalPlan::Dml(root) => {
            root.base.go_memory_usage()
                + (root.fk_triggers.capacity() * std::mem::size_of::<super::FkTriggerNode>()) as i64
                + root
                    .fk_triggers
                    .iter()
                    .map(|node| {
                        (node.access.capacity()
                            + node.info.capacity()
                            + node.child_database.capacity()
                            + node.child_table.capacity()
                            + node.parent_database.capacity()
                            + node.parent_table.capacity()
                            + node.fk_string.capacity()
                            + (node.child_offsets.capacity() + node.parent_offsets.capacity())
                                * std::mem::size_of::<usize>()) as i64
                    })
                    .sum::<i64>()
                + optional_subtree_usage(root.select_plan.as_deref())
        }
        PhysicalPlan::ShuffleReceiver(receiver) => {
            receiver.base.go_schema_producer_memory_usage()
                + std::mem::size_of::<Box<PhysicalPlan>>() as i64
                + receiver.data_source.memory_usage()
        }
        PhysicalPlan::ExchangeReceiver(receiver) => {
            receiver.base.go_memory_usage()
                + SIZE_OF_SLICE * 2
                + receiver.tasks.len() as i64 * SIZE_OF_POINTER
        }
        PhysicalPlan::Shuffle(shuffle) => shuffle_usage(shuffle),
        PhysicalPlan::TableSample(sample) => sample.memory_usage(),
        PhysicalPlan::HashJoin(join) => join.memory_usage(),
        PhysicalPlan::MergeJoin(join) => join.memory_usage(),
        PhysicalPlan::IndexJoin(join) => join.memory_usage(),
        PhysicalPlan::Apply(apply) => apply.memory_usage(),
        other => other.base().go_schema_producer_memory_usage(),
    }
}

/// The key material of a fast point plan: the handle's field type, or an
/// index key's datums (`IndexValues`) and column types (`ColsFieldType` /
/// `IndexColTypes`). A `Handles` slot costs an interface in a batch.
fn point_plan_keys_usage(
    rebuild: Option<&crate::physical_plan_cache::PointRangeRebuild>,
    is_handle: bool,
    is_batch: bool,
) -> i64 {
    use crate::physical_plan_cache::PointRangeRebuild;
    let Some(PointRangeRebuild::Constants(constants)) = rebuild else {
        return 0;
    };
    let types: i64 = constants
        .field_types
        .iter()
        .map(|field_type| field_type.memory_usage() as i64)
        .sum();
    if is_handle {
        let handles = if is_batch {
            constants.rows.len() as i64 * SIZE_OF_INTERFACE
        } else {
            0
        };
        return handles
            + constants
                .field_types
                .first()
                .map_or(0, |ft| ft.memory_usage() as i64);
    }
    let rows: Vec<_> = if is_batch {
        constants.rows.iter().collect()
    } else {
        constants.rows.first().into_iter().collect()
    };
    let values: i64 = rows
        .iter()
        .flat_map(|row| row.iter())
        .map(|key| key.value.estimated_mem_usage() as i64)
        .sum();
    values + types
}

/// A fast point plan is built without output names in this port; Go's
/// `PointGetPlan.outputNames` / `SimpleSchemaProducer.names` hold one
/// `FieldName` per schema column, so the same bytes are rebuilt from each
/// column's `db.table.column` spelling when no names were kept.
/// Returns `(name count, name bytes, database-name length)`.
fn point_plan_names(base: &BasePhysicalPlan) -> (i64, i64, i64) {
    let names = base.base.output_names();
    if !names.is_empty() {
        return (
            names.len() as i64,
            names
                .iter()
                .map(tidb_datatype::FieldName::memory_usage)
                .sum(),
            names[0].names.database.original.len() as i64,
        );
    }
    let Some(schema) = base.base.schema() else {
        return (0, 0, 0);
    };
    let ci = |text: &str| SIZE_OF_STRING * 2 + (text.len() * 2) as i64;
    let mut bytes = 0;
    let mut database_len = 0;
    for column in &schema.columns {
        let mut parts = column.orig_name.rsplitn(3, '.');
        let name = parts.next().unwrap_or("");
        let table = parts.next().unwrap_or("");
        let database = parts.next().unwrap_or("");
        database_len = database.len() as i64;
        bytes += ci(table) + ci(name) + ci(database) + ci(table) + ci(name) + SIZE_OF_BOOL * 3;
    }
    (schema.columns.len() as i64, bytes, database_len)
}

fn point_plan_db_name_len(base: &BasePhysicalPlan) -> i64 {
    point_plan_names(base).2
}

fn names_memory_usage_without_slots(base: &BasePhysicalPlan) -> i64 {
    point_plan_names(base).1
}

fn point_plan_name_count(base: &BasePhysicalPlan) -> i64 {
    point_plan_names(base).0
}

fn names_memory_usage(base: &BasePhysicalPlan) -> i64 {
    let (count, bytes, _) = point_plan_names(base);
    count * SIZE_OF_POINTER + bytes
}

/// Go `PhysicalShuffle.MemoryUsage` (`physical_shuffle.go:938`): the base, the
/// slices, every `Tails` and `DataSources` plan (each counted with its whole
/// subtree, on top of the child tree the base walk already covers) and the
/// `ByItemArrays`.
fn shuffle_usage(shuffle: &super::shuffle::PhysicalShuffle) -> i64 {
    shuffle.base.go_memory_usage()
        + SIZE_OF_INT * 2
        + SIZE_OF_SLICE * (3 + shuffle.by_item_arrays.len() as i64)
        + (shuffle.tails.len() + shuffle.data_sources.len()) as i64 * SIZE_OF_INTERFACE
        + shuffle
            .tails
            .iter()
            .chain(&shuffle.data_sources)
            .filter_map(|id| shuffle.referenced_plan(*id).ok())
            .map(subtree_usage)
            .sum::<i64>()
        + shuffle
            .by_item_arrays
            .iter()
            .map(|items| items.len() as i64 * SIZE_OF_INTERFACE + exprs_memory_usage(items))
            .sum::<i64>()
}

/// Go `PhysicalSort.MemoryUsage` (`physical_sort.go`): the physical base, the
/// order items, and the partial-sort flag.
pub(super) fn sort_memory_usage(op: &super::PhysicalSort) -> i64 {
    op.base.go_memory_usage()
        + SIZE_OF_SLICE
        + op.by_items.len() as i64 * SIZE_OF_POINTER
        + SIZE_OF_BOOL
        + by_items_memory_usage(&op.by_items)
}

/// Go `collectPartitionInfosFromMPPPlan`: an MPP reader keeps a
/// `TableScanAndPartitionInfo` per scan (its partition info and the scan
/// again), which a TiKV reader does not.
fn mpp_scan_infos_usage(table_plan: Option<&PhysicalPlan>) -> i64 {
    fn walk(node: &PhysicalPlan, total: &mut i64) {
        if let PhysicalPlan::TableScan(scan) = node {
            *total += part_info_memory_usage(scan.plan_part_info.as_ref()) + table_scan_usage(scan);
            return;
        }
        for child in node.children() {
            walk(child, total);
        }
    }
    let mut total = 0;
    if let Some(PhysicalPlan::ExchangeSender(_)) = table_plan {
        if let Some(plan) = table_plan {
            walk(plan, &mut total);
        }
    }
    total
}

impl PhysicalPlan {
    /// One row of [`Self::memory_breakdown`].
    ///
    /// Diagnostic: the same walk Go's probe prints (`MEMOP depth type usage
    /// own`), so each operator's bytes can be compared with Go's one by one.
    #[must_use]
    pub fn memory_breakdown(&self) -> Vec<(usize, String, i64, i64)> {
        fn walk(node: &PhysicalPlan, depth: usize, out: &mut Vec<(usize, String, i64, i64)>) {
            let usage = node.memory_usage();
            let own = usage
                - node
                    .children()
                    .iter()
                    .map(PhysicalPlan::memory_usage)
                    .sum::<i64>();
            let name = node.base().base.tp().to_owned();
            out.push((depth, name, usage, own));
            for child in node.children() {
                walk(child, depth + 1, out);
            }
            match node {
                PhysicalPlan::TableReader(reader) => {
                    if let Some(plan) = reader.table_plan.as_deref() {
                        walk(plan, depth + 1, out);
                    }
                }
                PhysicalPlan::IndexReader(reader) => {
                    if let Some(plan) = reader.index_plan.as_deref() {
                        walk(plan, depth + 1, out);
                    }
                }
                PhysicalPlan::IndexLookUpReader(reader) => {
                    if let Some(plan) = reader.index_plan.as_deref() {
                        walk(plan, depth + 1, out);
                    }
                    if let Some(plan) = reader.table_plan.as_deref() {
                        walk(plan, depth + 1, out);
                    }
                }
                PhysicalPlan::IndexMergeReader(reader) => {
                    if let Some(plan) = reader.table_plan.as_deref() {
                        walk(plan, depth + 1, out);
                    }
                    for plan in &reader.partial_plans_raw {
                        walk(plan, depth + 1, out);
                    }
                }
                _ => {}
            }
        }
        let mut out = Vec::new();
        walk(self, 0, &mut out);
        out
    }
}
