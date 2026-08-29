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

//! Go `executorBuilder.build`: instantiate a retained physical SELECT tree.
//!
//! Fresh plans and plans rebuilt by `RebuildPlan4CachedPlan` enter the same
//! constructor switch. This module must not inspect the SQL AST to choose
//! access, aggregation, join, sort, or reader policy; the AST is used only
//! for client-visible result-column labels.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use crate::access_path::{
    HandleOutputColumn, HandleSourceExec, IndexJoinLookupExec, IndexRangeSourceExec, LookupObject,
    LookupProbeBound, LookupProbeBoundOp, LookupProbePart, SharedIndexJoinProbes,
    UniqueIndexPointSourceExec,
};
use crate::apply::NestedLoopApplyExec;
use crate::executor::{Executor, ExecutorMeta};
use crate::hash_agg::{AggFunc, AggKind, GroupedStreamAggExec, HashAggExec, StreamAggExec};
use crate::index_merge_reader::{
    ExecutorPartialHandleSource, IndexMergeReaderExec, MergeByItem, PartialHandleColumns,
    PartialHandleSource, PushedDownLimit as ExecutorPushedDownLimit,
};
use crate::join::{IndexLookupPlan, IndexLookupSource, IndexProbeKeyDomain, JoinExec, JoinKind};
use crate::joiner::{new_joiner, JoinType as JoinerType, JoinerChunkSizes};
use crate::kv_table::{IndexRange, RowDecodeContext, TableHandle, TableScanExec};
use crate::limit::LimitExec;
use crate::mem_table::MemTableSourceExec;
use crate::physical_cte::{
    CteExec, CteProducer, CteTableReaderExec, SharedCteProducer, SharedCteStorage,
};
use crate::projection::ProjectionExec;
use crate::remote_scan::{
    PushdownAggregateFunction, PushdownAggregateKind, PushdownPartialAggregate,
    PushdownStatementContext,
};
use crate::selection::SelectionExec;
use crate::sort::{SortByItem, SortExec};
use crate::table_access::TableAccess;
use crate::table_dual::TableDualExec;
use crate::topn::TopNExec;
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_expr::aggregation::{AggFuncDesc, AggFunctionMode};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::scalar_function::ScalarFunction;
use tidb_expr::schema::Schema;
use tidb_planner::find_best_task::LogicalJoinType;
use tidb_planner::physical::{
    PhysicalBatchPointGet, PhysicalIndexScan, PhysicalMemTable, PhysicalPlan, PhysicalPointGet,
    PhysicalTableScan,
};

use super::{Catalog, DriverError, SelectMeta, INIT_CAP, MAX_CHUNK_SIZE};

struct CteBuildSlot {
    result: SharedCteStorage,
    iter_in: SharedCteStorage,
    producer: Option<SharedCteProducer>,
}

#[derive(Default)]
struct BuildState {
    cte_slots: HashMap<i32, CteBuildSlot>,
    runtime_counters: Option<PhysicalRuntimeStats>,
}

pub(crate) type PhysicalRuntimeStats = HashMap<usize, Rc<Cell<u64>>>;

pub(crate) fn runtime_plan_key(plan: &PhysicalPlan) -> usize {
    std::ptr::from_ref(plan).addr()
}

impl BuildState {
    fn meter(&mut self, plan: &PhysicalPlan, mut executor: Box<dyn Executor>) -> Box<dyn Executor> {
        let Some(counters) = self.runtime_counters.as_mut() else {
            return executor;
        };
        if matches!(
            plan,
            PhysicalPlan::TableScan(_) | PhysicalPlan::IndexScan(_)
        ) {
            if let Some(counter) = executor
                .table_access()
                .and_then(|access| access.scanned_rows_counter())
            {
                counters.insert(runtime_plan_key(plan), counter);
                return executor;
            }
        }
        let counter = Rc::new(Cell::new(0));
        counters.insert(runtime_plan_key(plan), Rc::clone(&counter));
        Box::new(PhysicalCountExec {
            child: executor,
            counter,
        })
    }
}

/// Go's per-physical-operator runtime-stat registration. The wrapper counts
/// rows produced by the executor built for exactly one retained physical
/// node; it never selects or rebuilds a plan.
struct PhysicalCountExec {
    child: Box<dyn Executor>,
    counter: Rc<Cell<u64>>,
}

impl Executor for PhysicalCountExec {
    fn open(&mut self) -> Result<(), crate::ExecError> {
        self.child.open()
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), crate::ExecError> {
        self.child.next(req)?;
        self.counter
            .set(self.counter.get().saturating_add(req.num_rows() as u64));
        Ok(())
    }

    fn close(&mut self) -> Result<(), crate::ExecError> {
        self.child.close()
    }

    fn schema(&self) -> &Schema {
        self.child.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.child.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.child.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.child.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.child.new_chunk()
    }

    fn table_access(&mut self) -> Option<&mut dyn crate::table_access::TableAccess> {
        self.child.table_access()
    }

    fn agg_tree_input_empty(&self) -> bool {
        self.child.agg_tree_input_empty()
    }
}

/// Go `BatchPointGetExec.initialize`'s `slices.SortFunc(e.handles, less)`.
/// Unsigned integer handles compare their stored bits, while signed and
/// common handles use their natural order.
fn sort_handles_for_keep_order(
    handles: &mut [TableHandle],
    desc: bool,
    unsigned_pk_is_handle: bool,
) {
    handles.sort_by(|left, right| {
        let ordering = if unsigned_pk_is_handle {
            match (left, right) {
                (TableHandle::Int(left), TableHandle::Int(right)) => {
                    (*left as u64).cmp(&(*right as u64))
                }
                _ => left.cmp(right),
            }
        } else {
            left.cmp(right)
        };
        if desc {
            ordering.reverse()
        } else {
            ordering
        }
    });
}

/// Go `MaxOneRowExec`: emit one row (NULL when the child is empty) and reject
/// a second row. The retained physical operator caps its child request at two;
/// this executor still performs the second pull because not every child fills
/// a requested chunk.
struct MaxOneRowExec {
    meta: ExecutorMeta,
    child: Box<dyn Executor>,
    evaluated: bool,
}

impl MaxOneRowExec {
    fn new(meta: ExecutorMeta, child: Box<dyn Executor>) -> Self {
        Self {
            meta,
            child,
            evaluated: false,
        }
    }
}

impl Executor for MaxOneRowExec {
    fn open(&mut self) -> Result<(), crate::ExecError> {
        self.evaluated = false;
        self.child.open()
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), crate::ExecError> {
        req.reset();
        if self.evaluated {
            return Ok(());
        }
        self.evaluated = true;
        self.child.next(req)?;
        match req.num_rows() {
            0 => {
                if self.meta.schema().is_empty() {
                    req.set_num_virtual_rows(1);
                } else {
                    for column in 0..self.meta.schema().len() {
                        req.append_null(column);
                    }
                }
                Ok(())
            }
            1 => {
                let mut extra = self.child.new_chunk();
                self.child.next(&mut extra)?;
                if extra.num_rows() == 0 {
                    Ok(())
                } else {
                    Err(crate::ExecError::SubqueryReturnsMoreThanOneRow)
                }
            }
            _ => Err(crate::ExecError::SubqueryReturnsMoreThanOneRow),
        }
    }

    fn close(&mut self) -> Result<(), crate::ExecError> {
        self.child.close()
    }

    fn schema(&self) -> &Schema {
        self.meta.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }
}

fn plan_schema(plan: &PhysicalPlan) -> Result<Schema, DriverError> {
    plan.schema()
        .cloned()
        .ok_or_else(|| DriverError::unsupported("a physical operator has no schema"))
}

fn unary_schema(plan: &PhysicalPlan, child: &dyn Executor) -> Schema {
    plan.base()
        .base
        .schema()
        .cloned()
        .unwrap_or_else(|| child.schema().clone())
}

fn meta(plan: &PhysicalPlan, schema: Schema) -> ExecutorMeta {
    ExecutorMeta::new(
        schema,
        i64::from(plan.base().base.id()),
        INIT_CAP,
        MAX_CHUNK_SIZE,
    )
}

fn only_child(plan: &PhysicalPlan) -> Result<&PhysicalPlan, DriverError> {
    let [child] = plan.children() else {
        return Err(DriverError::unsupported(
            "a unary physical operator has the wrong child count",
        ));
    };
    Ok(child)
}

fn resolve_expression(
    mut expression: Expression,
    schema: &Schema,
) -> Result<Expression, DriverError> {
    super::planner_bridge::materialize_physical_expression(&mut expression);
    tidb_expr::simple_expr::resolve_indices_in_place(&mut expression, schema).map_err(
        |_error| DriverError::unsupported("a physical expression does not resolve in its child"),
    )?;
    Ok(expression)
}

fn resolve_expressions(
    expressions: &[Expression],
    schema: &Schema,
) -> Result<Vec<Expression>, DriverError> {
    expressions
        .iter()
        .cloned()
        .map(|expression| resolve_expression(expression, schema))
        .collect()
}

fn executor_ranges(ranges: &tidb_planner::ranger::types::Ranges) -> Vec<IndexRange> {
    ranges
        .iter()
        .map(|range| IndexRange {
            low: range.low_val.clone(),
            high: range.high_val.clone(),
            low_exclusive: range.low_exclude,
            high_exclusive: range.high_exclude,
        })
        .collect()
}

fn table_scan_schema(
    scan: &PhysicalTableScan,
    table: &crate::KvTable,
) -> Result<(Schema, Vec<usize>, Option<usize>), DriverError> {
    let output = scan
        .base
        .base
        .schema()
        .ok_or_else(|| DriverError::unsupported("a physical table scan has no schema"))?;
    let mut full = Vec::with_capacity(table.logical_data_source_column_count());
    for (index, column) in table.logical_columns().iter().enumerate() {
        let mut planned = scan
            .cost_columns
            .iter()
            .find(|planned| planned.id == column.id)
            .cloned()
            .unwrap_or_else(|| {
                let mut planned = Column::new(column.id, column.field_type.clone());
                planned.id = column.id;
                planned
            });
        planned.index = index as i64;
        full.push(planned);
    }
    let mut keep = Vec::with_capacity(output.columns.len());
    let mut extra_handle_slot = None;
    for (output_offset, wanted) in output.columns.iter().enumerate() {
        if wanted.id == tidb_model::column::EXTRA_HANDLE_ID {
            if extra_handle_slot.is_some() || output_offset + 1 != output.columns.len() {
                return Err(DriverError::unsupported(
                    "a physical table-scan _tidb_rowid column is not its final output",
                ));
            }
            extra_handle_slot = Some(keep.len());
            continue;
        }
        keep.push(
            full.iter()
                .position(|column| {
                    column.unique_id == wanted.unique_id
                        || (wanted.id != 0 && column.id == wanted.id)
                })
                .ok_or_else(|| {
                    DriverError::unsupported(
                        "a physical table-scan output column is absent from its table",
                    )
                })?,
        );
    }
    Ok((Schema::new(full), keep, extra_handle_slot))
}

fn build_table_scan(
    plan: &PhysicalPlan,
    scan: &PhysicalTableScan,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    let table = catalog
        .kv_table_by_id(scan.table_id)
        .ok_or_else(|| DriverError::unsupported("physical table ID is absent from the catalog"))?;
    let (schema, keep, extra_handle_slot) = table_scan_schema(scan, table)?;
    let mut source = TableScanExec::new_with_context(
        meta(plan, schema.clone()),
        table.clone(),
        RowDecodeContext::for_query(ctx),
        PushdownStatementContext::from_stmt(ctx),
    );
    if keep.len() != table.logical_data_source_column_count()
        || keep
            .iter()
            .copied()
            .ne(0..table.logical_data_source_column_count())
    {
        if !source.accept_column_prune(&keep) {
            return Err(DriverError::unsupported(
                "the physical table scan cannot apply its projection",
            ));
        }
    }
    if let Some(slot) = extra_handle_slot {
        if !source.accept_extra_handle(slot) {
            return Err(DriverError::unsupported(
                "the physical table scan cannot emit its _tidb_rowid column",
            ));
        }
    }
    if scan.keep_order && !source.accept_keep_order(scan.desc) {
        return Err(DriverError::unsupported(
            "the physical table scan cannot preserve its order",
        ));
    }
    if !scan.ranges.is_empty() {
        let ranges = executor_ranges(&scan.ranges);
        if !source.accept_handle_ranges(&ranges) {
            return Err(DriverError::unsupported(
                "the physical table scan cannot apply its ranges",
            ));
        }
    }
    if let Some(rows) = scan.base.base.stats_info().map(|stats| stats.row_count()) {
        source.accept_scan_estimate(rows);
    }
    Ok(Box::new(source))
}

fn build_mem_table(
    plan: &PhysicalPlan,
    scan: &PhysicalMemTable,
    catalog: &Catalog,
) -> Result<Box<dyn Executor>, DriverError> {
    let super::TableEntry::Mem(table) = catalog
        .table_in(&scan.db_name, &scan.table_name)
        .ok_or_else(|| {
            DriverError::unsupported("physical memory table is absent from the catalog")
        })?
    else {
        return Err(DriverError::unsupported(
            "PhysicalMemTable resolved to a non-memory catalog table",
        ));
    };

    let offsets = scan
        .columns
        .iter()
        .map(|column| {
            table
                .columns
                .iter()
                .position(|(name, _)| name.eq_ignore_ascii_case(&column.name))
                .ok_or_else(|| {
                    DriverError::unsupported(
                        "a physical memory-table output column is absent from its table",
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let rows = table
        .rows
        .iter()
        .map(|row| {
            offsets
                .iter()
                .map(|offset| {
                    row.get(*offset).cloned().ok_or_else(|| {
                        DriverError::unsupported(
                            "a memory-table row is shorter than its declared schema",
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Box::new(MemTableSourceExec::new(
        meta(plan, plan_schema(plan)?),
        rows,
    )))
}

fn aggregate_function(
    descriptor: &AggFuncDesc,
    child_schema: &Schema,
    _ctx: &crate::StmtContext,
) -> Result<AggFunc, DriverError> {
    let mut args = resolve_expressions(&descriptor.base.args, child_schema)?;
    let upper = descriptor.base.name.to_ascii_uppercase();
    let separator = if upper == "GROUP_CONCAT" {
        Some(super::agg_build::separator_text(args.last().ok_or_else(
            || DriverError::unsupported("GROUP_CONCAT has no separator argument"),
        )?)?)
    } else {
        None
    };
    if separator.is_some() {
        args.pop();
    }
    let mut kind = super::agg_build::aggregate_kind(&upper, &args, separator)?;
    if descriptor.mode == AggFunctionMode::Final && matches!(kind, AggKind::Count) {
        kind = AggKind::FinalCount;
    }
    if upper == "APPROX_PERCENTILE" {
        args.truncate(1);
    }
    let arg = (!args.is_empty()).then(|| args.remove(0));
    Ok(AggFunc {
        kind,
        arg,
        extra_args: args,
        order_by: descriptor
            .order_by_items
            .iter()
            .map(|item| {
                resolve_expression(item.expr.clone(), child_schema)
                    .map(|expression| (expression, item.desc))
            })
            .collect::<Result<Vec<_>, _>>()?,
        distinct: descriptor.has_distinct,
        arg_orig_name: String::new(),
    })
}

fn build_aggregation(
    plan: &PhysicalPlan,
    descriptors: &[AggFuncDesc],
    group_by: &[Expression],
    stream: bool,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
    state: &mut BuildState,
) -> Result<Box<dyn Executor>, DriverError> {
    let child = build_with_state(only_child(plan)?, catalog, ctx, state)?;
    build_aggregation_over_child(plan, descriptors, group_by, stream, child, ctx)
}

fn build_aggregation_over_child(
    plan: &PhysicalPlan,
    descriptors: &[AggFuncDesc],
    group_by: &[Expression],
    stream: bool,
    mut child: Box<dyn Executor>,
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    let child_schema = child.schema().clone();
    let group_by = resolve_expressions(group_by, &child_schema)?;
    let functions = descriptors
        .iter()
        .map(|descriptor| aggregate_function(descriptor, &child_schema, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    let output_schema = plan_schema(plan)?;
    if descriptors
        .iter()
        .all(|descriptor| descriptor.mode == AggFunctionMode::Partial1)
    {
        if let Some(partial) = pushed_partial_aggregation(
            &functions,
            &group_by,
            stream,
            &child_schema,
            &output_schema,
        )? {
            if child
                .table_access()
                .is_some_and(|access| access.accept_partial_aggregate(&partial, ctx))
            {
                let expressions = child
                    .schema()
                    .columns
                    .iter()
                    .cloned()
                    .map(Expression::Column)
                    .collect();
                return Ok(Box::new(ProjectionExec::new(
                    meta(plan, output_schema),
                    expressions,
                    child,
                    ctx.clone(),
                )));
            }
        }
    }
    let executor_meta = meta(plan, output_schema);
    if stream {
        if group_by.is_empty() {
            Ok(Box::new(StreamAggExec::new(
                executor_meta,
                functions,
                child,
                ctx.clone(),
            )))
        } else {
            let output_positions = (0..functions.len()).collect();
            Ok(Box::new(GroupedStreamAggExec::new(
                executor_meta,
                group_by,
                functions,
                output_positions,
                child,
                ctx.clone(),
            )))
        }
    } else {
        Ok(Box::new(HashAggExec::new(
            executor_meta,
            group_by,
            functions,
            child,
            ctx.clone(),
            ctx.statement_memory(),
        )))
    }
}

fn pushed_partial_aggregation(
    functions: &[AggFunc],
    group_by: &[Expression],
    streamed: bool,
    child_schema: &Schema,
    output_schema: &Schema,
) -> Result<Option<PushdownPartialAggregate>, DriverError> {
    let mut pushed_functions = Vec::new();
    let mut output = 0;
    for function in functions {
        if function.distinct || !function.extra_args.is_empty() || !function.order_by.is_empty() {
            return Ok(None);
        }
        let mut input = function.arg.clone();
        if matches!(function.kind, AggKind::Count)
            && input.as_ref().is_none_or(|argument| {
                matches!(argument, Expression::Constant(constant)
                    if matches!(constant.value, tidb_datatype::Datum::Int(1)
                        | tidb_datatype::Datum::UInt(1)))
            })
        {
            input = None;
        }
        if input.as_ref().is_some_and(|argument| {
            tidb_expr::pushdown_catalog::from_expression(argument).is_none()
        }) {
            return Ok(None);
        }
        let kinds: &[PushdownAggregateKind] = match function.kind {
            AggKind::Count => &[PushdownAggregateKind::Count],
            AggKind::Sum => &[PushdownAggregateKind::Sum],
            AggKind::Min => &[PushdownAggregateKind::Min],
            AggKind::Max => &[PushdownAggregateKind::Max],
            AggKind::Avg => &[PushdownAggregateKind::Count, PushdownAggregateKind::Sum],
            _ => return Ok(None),
        };
        for kind in kinds {
            let Some(column) = output_schema.columns.get(output) else {
                return Err(DriverError::unsupported(
                    "a physical partial aggregate output schema is incomplete",
                ));
            };
            pushed_functions.push(PushdownAggregateFunction {
                kind: *kind,
                input: input.clone(),
                output_type: column
                    .ret_type
                    .clone()
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong)),
            });
            output += 1;
        }
    }
    let mut group_offsets = Vec::with_capacity(group_by.len());
    let mut group_types = Vec::with_capacity(group_by.len());
    for expression in group_by {
        let Expression::Column(column) = expression else {
            return Ok(None);
        };
        let offset = usize::try_from(column.index)
            .ok()
            .filter(|offset| *offset < child_schema.len());
        let Some(offset) = offset else {
            return Ok(None);
        };
        group_offsets.push(offset);
        group_types.push(
            child_schema.columns[offset]
                .ret_type
                .clone()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong)),
        );
    }
    if output + group_offsets.len() != output_schema.len() {
        return Ok(None);
    }
    let partial = if group_offsets.is_empty() {
        PushdownPartialAggregate::Global {
            functions: pushed_functions,
            streamed,
        }
    } else {
        PushdownPartialAggregate::Grouped {
            group_offsets,
            group_types,
            functions: pushed_functions,
            streamed,
        }
    };
    Ok(Some(partial))
}

fn build_reader(
    embedded: Option<&PhysicalPlan>,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
    state: &mut BuildState,
) -> Result<Box<dyn Executor>, DriverError> {
    build_with_state(
        embedded.ok_or_else(|| DriverError::unsupported("a physical reader has no pushed plan"))?,
        catalog,
        ctx,
        state,
    )
}

fn embedded_index_scan(plan: &PhysicalPlan) -> Option<&PhysicalIndexScan> {
    match plan {
        PhysicalPlan::IndexScan(scan) => Some(scan),
        _ => plan.children().iter().find_map(embedded_index_scan),
    }
}

fn reader_has_selection(plan: &PhysicalPlan) -> bool {
    matches!(plan, PhysicalPlan::Selection(_)) || plan.children().iter().any(reader_has_selection)
}

fn lower_index_lookup_selections(
    plan: &PhysicalPlan,
    source: &mut IndexRangeSourceExec,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    for child in plan.children() {
        lower_index_lookup_selections(child, source, ctx)?;
    }
    let PhysicalPlan::Selection(selection) = plan else {
        return Ok(());
    };
    let filters = resolve_expressions(&selection.conditions, source.schema())?;
    let pushed = crate::predicate_pushdown::PushedScanFilter::from_physical_conditions(filters);
    if !source.accept_scan_filter(&pushed, ctx) {
        return Err(DriverError::unsupported(
            "a physical index lookup cannot apply its retained Selection",
        ));
    }
    Ok(())
}

fn lower_index_merge_selections(
    plan: &PhysicalPlan,
    executor: &mut IndexMergeReaderExec,
    schema: &Schema,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    for child in plan.children() {
        lower_index_merge_selections(child, executor, schema, ctx)?;
    }
    let PhysicalPlan::Selection(selection) = plan else {
        return Ok(());
    };
    executor.push_table_filters(
        resolve_expressions(&selection.conditions, schema)?,
        ctx.clone(),
    );
    Ok(())
}

fn reader_output_offsets(
    schema: &Schema,
    scan: &PhysicalIndexScan,
    table: &crate::KvTable,
) -> Result<(Vec<usize>, Option<usize>), DriverError> {
    let extra_handle = schema.columns.iter().position(|column| {
        column.id == tidb_model::column::EXTRA_HANDLE_ID
            || column.orig_name.rsplit('.').next().is_some_and(|name| {
                name.eq_ignore_ascii_case(tidb_model::column::EXTRA_HANDLE_NAME)
            })
    });
    let offsets = schema
        .columns
        .iter()
        .enumerate()
        .filter(|(slot, _)| Some(*slot) != extra_handle)
        .map(|(_, output)| {
            let source = scan
                .cost_columns
                .iter()
                .find(|column| column.unique_id == output.unique_id)
                .unwrap_or(output);
            table
                .columns
                .iter()
                .position(|column| column.id == source.id)
                .or_else(|| {
                    source.orig_name.rsplit('.').next().and_then(|name| {
                        table
                            .columns
                            .iter()
                            .position(|column| column.name.eq_ignore_ascii_case(name))
                    })
                })
                .ok_or_else(|| {
                    DriverError::unsupported(
                        "a physical index reader output is absent from its table",
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((offsets, extra_handle))
}

fn build_index_reader(
    plan: &PhysicalPlan,
    index_plan: &PhysicalPlan,
    table_plan: Option<&PhysicalPlan>,
    covering: bool,
    keep_order: bool,
    pushed_limit: Option<tidb_planner::physical::PushedDownLimit>,
    expect_cnt: Option<u64>,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    let scan = embedded_index_scan(index_plan)
        .ok_or_else(|| DriverError::unsupported("a physical index reader has no index scan"))?;
    let table = catalog.kv_table_by_id(scan.table_id).ok_or_else(|| {
        DriverError::unsupported("physical index table ID is absent from the catalog")
    })?;
    if !table
        .indexes()
        .iter()
        .any(|index| index.id == scan.index_id)
    {
        return Err(DriverError::unsupported(
            "physical index ID is absent from its table",
        ));
    }
    let schema = plan_schema(plan)?;
    let (keep, extra_handle) = reader_output_offsets(&schema, scan, table)?;
    let ranges = if scan.ranges.is_empty() {
        vec![IndexRange::full()]
    } else {
        executor_ranges(&scan.ranges)
    };
    let mut source = IndexRangeSourceExec::new_with_statement(
        meta(plan, schema.clone()),
        table.clone(),
        scan.index_id,
        ranges,
        RowDecodeContext::for_query(ctx),
        PushdownStatementContext::from_stmt(ctx),
    );
    if let Some(slot) = extra_handle {
        source.read_extra_handle(slot);
    }
    source.read_table_columns(keep);
    source.set_lookup_concurrency(ctx.executor_concurrency());
    if covering {
        source.mark_covering();
        source.answer_in_index_order();
    }
    if (scan.keep_order || keep_order) && !source.accept_keep_order(scan.desc) {
        return Err(DriverError::unsupported(
            "the physical index reader cannot preserve its order",
        ));
    }
    if let Some(rows) = scan.base.base.stats_info().map(|stats| stats.row_count()) {
        source.accept_scan_estimate(rows);
    }
    lower_index_lookup_selections(index_plan, &mut source, ctx)?;
    let has_table_selection = table_plan.is_some_and(reader_has_selection);
    if !has_table_selection && reader_has_selection(index_plan) && !source.accept_index_filter() {
        return Err(DriverError::unsupported(
            "a physical index lookup cannot apply its index-side Selection",
        ));
    }
    if let Some(table_plan) = table_plan {
        lower_index_lookup_selections(table_plan, &mut source, ctx)?;
    }
    if let Some(limit) = pushed_limit {
        if !source.accept_embedded_lookup_limit(limit.offset, limit.count) {
            return Err(DriverError::unsupported(
                "the physical index lookup cannot apply its pushed limit",
            ));
        }
    }
    if let Some(expect_cnt) = expect_cnt.filter(|_| !covering) {
        if !source.accept_lookup_batch_size(expect_cnt) {
            return Err(DriverError::unsupported(
                "the physical index lookup cannot apply its paging size",
            ));
        }
    }
    Ok(Box::new(source))
}

fn join_kind(join_type: LogicalJoinType) -> Result<JoinKind, DriverError> {
    match join_type {
        LogicalJoinType::Inner => Ok(JoinKind::Inner),
        LogicalJoinType::LeftOuter => Ok(JoinKind::Left),
        LogicalJoinType::RightOuter => Ok(JoinKind::Right),
        LogicalJoinType::Semi => Ok(JoinKind::Semi),
        LogicalJoinType::AntiSemi => Ok(JoinKind::AntiSemi),
        LogicalJoinType::LeftOuterSemi => Ok(JoinKind::LeftOuterSemi),
        LogicalJoinType::AntiLeftOuterSemi => Err(DriverError::unsupported(
            "physical anti-left-outer-semi join execution is not implemented",
        )),
    }
}

fn joiner_type(join_type: LogicalJoinType) -> JoinerType {
    match join_type {
        LogicalJoinType::Inner => JoinerType::Inner,
        LogicalJoinType::LeftOuter => JoinerType::LeftOuter,
        LogicalJoinType::RightOuter => JoinerType::RightOuter,
        LogicalJoinType::Semi => JoinerType::SemiJoin,
        LogicalJoinType::AntiSemi => JoinerType::AntiSemiJoin,
        LogicalJoinType::LeftOuterSemi => JoinerType::LeftOuterSemiJoin,
        LogicalJoinType::AntiLeftOuterSemi => JoinerType::AntiLeftOuterSemiJoin,
    }
}

fn filtered_join_child(
    plan: &PhysicalPlan,
    child: Box<dyn Executor>,
    conditions: &[Expression],
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    if conditions.is_empty() {
        return Ok(child);
    }
    let schema = child.schema().clone();
    let filters = resolve_expressions(conditions, &schema)?;
    Ok(Box::new(SelectionExec::new(
        meta(plan, schema),
        filters,
        child,
        ctx.clone(),
        ctx.statement_memory(),
    )))
}

fn join_equality(
    left: &Column,
    right: &Column,
    schema: &Schema,
) -> Result<Expression, DriverError> {
    join_equality_with_null(left, right, schema, false)
}

fn join_equality_with_null(
    left: &Column,
    right: &Column,
    schema: &Schema,
    null_safe: bool,
) -> Result<Expression, DriverError> {
    let expression = Expression::ScalarFunction(ScalarFunction::new(
        tidb_ast::CiString::new(if null_safe { "nulleq" } else { "eq" }),
        FieldType::new(FieldTypeCode::Tiny),
        vec![
            Expression::Column(left.clone()),
            Expression::Column(right.clone()),
        ],
    ));
    resolve_expression(expression, schema)
}

fn join_output_offsets(
    output_schema: &Schema,
    left_schema: &Schema,
    right_schema: &Schema,
) -> Result<Vec<usize>, DriverError> {
    let merged_columns = left_schema
        .columns
        .iter()
        .chain(&right_schema.columns)
        .collect::<Vec<_>>();
    output_schema
        .columns
        .iter()
        .map(|output| {
            merged_columns
                .iter()
                .position(|column| column.unique_id == output.unique_id)
                .or_else(|| {
                    (output.id != 0).then(|| {
                        merged_columns
                            .iter()
                            .position(|column| column.id == output.id)
                    })?
                })
                .ok_or_else(|| {
                    DriverError::unsupported(
                        "a physical join output is absent from both child schemas",
                    )
                })
        })
        .collect()
}

fn physical_table_schema(plan: &PhysicalPlan, table: &crate::KvTable) -> Schema {
    fn scan_columns(plan: &PhysicalPlan) -> Option<&[Column]> {
        match plan {
            PhysicalPlan::TableScan(scan) => Some(scan.cost_columns.as_slice()),
            PhysicalPlan::IndexScan(scan) => Some(scan.cost_columns.as_slice()),
            _ => plan.children().iter().find_map(scan_columns),
        }
    }
    let planned_columns = scan_columns(plan);
    let columns = table
        .visible_columns()
        .iter()
        .enumerate()
        .map(|(index, stored)| {
            let mut column = planned_columns
                .and_then(|columns| columns.iter().find(|column| column.id == stored.id))
                .cloned()
                .unwrap_or_else(|| Column::new(stored.id, stored.field_type.clone()));
            column.id = stored.id;
            column.index = index as i64;
            column
        })
        .collect();
    Schema::new(columns)
}

fn collect_index_inner_filters(
    plan: &PhysicalPlan,
    schema: &Schema,
    filters: &mut Vec<Expression>,
) -> Result<(), DriverError> {
    for child in plan.children() {
        collect_index_inner_filters(child, schema, filters)?;
    }
    if let PhysicalPlan::Selection(selection) = plan {
        filters.extend(resolve_expressions(&selection.conditions, schema)?);
    }
    Ok(())
}

fn index_inner_output_offsets(
    schema: &Schema,
    table: &crate::KvTable,
) -> Result<Vec<usize>, DriverError> {
    schema
        .columns
        .iter()
        .map(|column| {
            table
                .visible_columns()
                .iter()
                .position(|stored| stored.id == column.id)
                .or_else(|| {
                    column.orig_name.rsplit('.').next().and_then(|name| {
                        table
                            .visible_columns()
                            .iter()
                            .position(|stored| stored.name.eq_ignore_ascii_case(name))
                    })
                })
                .ok_or_else(|| {
                    DriverError::unsupported(
                        "an index-join reader output is absent from its retained table",
                    )
                })
        })
        .collect()
}

fn index_join_probe_shape(
    join: &tidb_planner::physical::PhysicalIndexJoin,
) -> Result<(Vec<usize>, Vec<LookupProbePart>), DriverError> {
    let mut dynamic = join
        .key_off2_idx_off
        .iter()
        .copied()
        .enumerate()
        .filter_map(|(key, index)| usize::try_from(index).ok().map(|index| (index, key)))
        .collect::<Vec<_>>();
    dynamic.sort_unstable_by_key(|(index, _)| *index);
    if dynamic.is_empty() {
        return Err(DriverError::unsupported(
            "a physical index join retained no usable lookup key",
        ));
    }
    if dynamic.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(DriverError::unsupported(
            "a physical index join maps two join keys to one lookup column",
        ));
    }
    let prefix_len = join.compare_filters.as_ref().map_or_else(
        || dynamic.last().expect("non-empty above").0 + 1,
        |filters| filters.target_index_offset,
    );
    if dynamic
        .last()
        .is_some_and(|(index, _)| *index >= prefix_len)
    {
        return Err(DriverError::unsupported(
            "a physical index-join CompareFilters target overlaps a lookup key",
        ));
    }
    let probe_keys = dynamic.iter().map(|(_, key)| *key).collect::<Vec<_>>();
    let mut parts = Vec::with_capacity(prefix_len);
    for index in 0..prefix_len {
        if let Some((position, _)) = dynamic
            .iter()
            .enumerate()
            .find(|(_, (object_index, _))| *object_index == index)
        {
            parts.push(LookupProbePart::Dynamic(position));
            continue;
        }
        let mut values = join.ranges.iter().filter_map(|range| {
            let low = range.low_val.get(index)?;
            let high = range.high_val.get(index)?;
            (!range.low_exclude && !range.high_exclude && low == high).then_some(low)
        });
        let Some(first) = values.next().cloned() else {
            return Err(DriverError::unsupported(
                "an index-join lookup prefix has neither a join key nor a fixed range value",
            ));
        };
        if values.any(|value| value != &first) {
            return Err(DriverError::unsupported(
                "an index-join lookup prefix retained inconsistent fixed range values",
            ));
        }
        parts.push(LookupProbePart::Constant(first));
    }
    Ok((probe_keys, parts))
}

fn index_join_probe_key_domains(
    join: &tidb_planner::physical::PhysicalIndexJoin,
    probe_parts: &[LookupProbePart],
    catalog: &Catalog,
) -> Result<Vec<IndexProbeKeyDomain>, DriverError> {
    let table_id = join.inner_access_table_id.ok_or_else(|| {
        DriverError::unsupported("a physical index join has no retained inner table ID")
    })?;
    let table = catalog.kv_table_by_id(table_id).ok_or_else(|| {
        DriverError::unsupported("a physical index-join table ID is absent from the catalog")
    })?;
    let object_types = if let Some(index_id) = join.inner_access_index_id {
        let index = table
            .plan_indexes()
            .find(|index| index.id == index_id)
            .ok_or_else(|| {
                DriverError::unsupported("a physical index-join index ID is absent from its table")
            })?;
        index
            .column_offsets
            .iter()
            .map(|offset| {
                table
                    .logical_columns()
                    .get(*offset)
                    .map(|column| column.field_type.clone())
                    .ok_or_else(|| {
                        DriverError::unsupported(
                            "an index-join key column is absent from its retained table",
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?
    } else if !table.common_handle_offsets().is_empty() {
        table
            .common_handle_offsets()
            .iter()
            .map(|offset| {
                table
                    .logical_columns()
                    .get(*offset)
                    .map(|column| column.field_type.clone())
                    .ok_or_else(|| {
                        DriverError::unsupported(
                            "a common-handle join key is absent from its retained table",
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        vec![table
            .pk_handle_offset()
            .and_then(|offset| table.logical_columns().get(offset))
            .map_or_else(
                || FieldType::new(FieldTypeCode::LongLong),
                |column| column.field_type.clone(),
            )]
    };

    let dynamic_count = probe_parts
        .iter()
        .filter(|part| matches!(part, LookupProbePart::Dynamic(_)))
        .count();
    let mut domains = vec![None; dynamic_count];
    for (object_offset, part) in probe_parts.iter().enumerate() {
        let LookupProbePart::Dynamic(dynamic_offset) = part else {
            continue;
        };
        let field_type = object_types.get(object_offset).cloned().ok_or_else(|| {
            DriverError::unsupported("an index-join lookup key exceeds its retained object")
        })?;
        let prefix_length = join
            .idx_col_lens
            .get(object_offset)
            .copied()
            .unwrap_or(crate::ddl::index_prefix::UNSPECIFIED_LENGTH);
        domains[*dynamic_offset] = Some(IndexProbeKeyDomain {
            field_type,
            prefix_length,
        });
    }
    domains
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| DriverError::unsupported("an index-join lookup key domain is incomplete"))
}

const fn lookup_probe_bound_op(
    op: tidb_planner::physical::IndexJoinCompareOp,
) -> LookupProbeBoundOp {
    match op {
        tidb_planner::physical::IndexJoinCompareOp::Ge => LookupProbeBoundOp::Ge,
        tidb_planner::physical::IndexJoinCompareOp::Gt => LookupProbeBoundOp::Gt,
        tidb_planner::physical::IndexJoinCompareOp::Lt => LookupProbeBoundOp::Lt,
        tidb_planner::physical::IndexJoinCompareOp::Le => LookupProbeBoundOp::Le,
    }
}

fn index_inner_reader_payload<'a>(plan: &'a PhysicalPlan) -> Option<(&'a PhysicalPlan, bool)> {
    match plan {
        PhysicalPlan::TableReader(reader) => reader.table_plan.as_deref().map(|plan| (plan, false)),
        PhysicalPlan::IndexReader(reader) => reader.index_plan.as_deref().map(|plan| (plan, true)),
        PhysicalPlan::IndexLookUpReader(reader) => reader
            .table_plan
            .as_deref()
            .or(reader.index_plan.as_deref())
            .map(|plan| (plan, false)),
        PhysicalPlan::TableScan(_) => Some((plan, false)),
        PhysicalPlan::IndexScan(_) => Some((plan, true)),
        _ => None,
    }
}

fn contains_index_inner_reader(plan: &PhysicalPlan) -> bool {
    index_inner_reader_payload(plan).is_some()
        || plan.children().iter().any(contains_index_inner_reader)
}

fn build_index_inner_reader(
    plan: &PhysicalPlan,
    join: &tidb_planner::physical::PhysicalIndexJoin,
    probe_parts: &[LookupProbePart],
    shared: Option<&std::rc::Rc<std::cell::RefCell<SharedIndexJoinProbes>>>,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<IndexJoinLookupExec, DriverError> {
    let table_id = join.inner_access_table_id.ok_or_else(|| {
        DriverError::unsupported("a physical index join has no retained inner table ID")
    })?;
    let table = catalog.kv_table_by_id(table_id).ok_or_else(|| {
        DriverError::unsupported("a physical index-join table ID is absent from the catalog")
    })?;
    let (embedded, covering) = index_inner_reader_payload(plan).ok_or_else(|| {
        DriverError::unsupported("an index-join inner leaf is not a physical reader")
    })?;
    if retained_table_id(embedded)? != table_id {
        return Err(DriverError::unsupported(
            "an index-join inner reader does not match its retained table ID",
        ));
    }
    let object = if let Some(index_id) = join.inner_access_index_id {
        if !table.plan_indexes().any(|index| index.id == index_id) {
            return Err(DriverError::unsupported(
                "a physical index-join index ID is absent from its table",
            ));
        }
        LookupObject::Index(index_id)
    } else if table.common_handle_offsets().is_empty() {
        LookupObject::Handle
    } else {
        LookupObject::CommonHandle
    };
    let schema = plan_schema(plan)?;
    let output_offsets = index_inner_output_offsets(&schema, table)?;
    let filter_schema = physical_table_schema(embedded, table);
    let mut filters = Vec::new();
    collect_index_inner_filters(embedded, &filter_schema, &mut filters)?;
    let mut source = IndexJoinLookupExec::new_with_context(
        meta(plan, schema),
        table.clone(),
        object,
        RowDecodeContext::for_query(ctx),
    );
    source.set_probe_parts(probe_parts.to_vec());
    source.set_probe_key_prefix_lengths(join.idx_col_lens.clone());
    if let Some(compare_filters) = &join.compare_filters {
        source.set_probe_bound_ops(
            compare_filters
                .ops
                .iter()
                .copied()
                .map(lookup_probe_bound_op)
                .collect(),
        );
    }
    source.set_filters(filters, ctx.clone());
    source.set_column_projection(Some(output_offsets), []);
    if covering {
        source.mark_covering();
    }
    if let Some(shared) = shared {
        source.set_shared_probes(std::rc::Rc::clone(shared));
    }
    Ok(source)
}

fn build_index_inner_subtree(
    plan: &PhysicalPlan,
    join: &tidb_planner::physical::PhysicalIndexJoin,
    probe_parts: &[LookupProbePart],
    shared: &std::rc::Rc<std::cell::RefCell<SharedIndexJoinProbes>>,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
    state: &mut BuildState,
) -> Result<Box<dyn Executor>, DriverError> {
    if index_inner_reader_payload(plan).is_some() {
        return Ok(Box::new(build_index_inner_reader(
            plan,
            join,
            probe_parts,
            Some(shared),
            catalog,
            ctx,
        )?));
    }
    match plan {
        PhysicalPlan::Projection(projection) => {
            let child = build_index_inner_subtree(
                only_child(plan)?,
                join,
                probe_parts,
                shared,
                catalog,
                ctx,
                state,
            )?;
            let expressions = resolve_expressions(&projection.exprs, child.schema())?;
            Ok(Box::new(ProjectionExec::new(
                meta(plan, plan_schema(plan)?),
                expressions,
                child,
                ctx.clone(),
            )) as Box<dyn Executor>)
        }
        PhysicalPlan::Selection(selection) => {
            let child = build_index_inner_subtree(
                only_child(plan)?,
                join,
                probe_parts,
                shared,
                catalog,
                ctx,
                state,
            )?;
            let filters = resolve_expressions(&selection.conditions, child.schema())?;
            let schema = unary_schema(plan, child.as_ref());
            Ok(Box::new(SelectionExec::new(
                meta(plan, schema),
                filters,
                child,
                ctx.clone(),
                ctx.statement_memory(),
            )))
        }
        PhysicalPlan::HashAgg(aggregation) => {
            let child = build_index_inner_subtree(
                only_child(plan)?,
                join,
                probe_parts,
                shared,
                catalog,
                ctx,
                state,
            )?;
            build_aggregation_over_child(
                plan,
                &aggregation.agg_funcs,
                &aggregation.group_by_items,
                false,
                child,
                ctx,
            )
        }
        PhysicalPlan::StreamAgg(aggregation) => {
            let child = build_index_inner_subtree(
                only_child(plan)?,
                join,
                probe_parts,
                shared,
                catalog,
                ctx,
                state,
            )?;
            build_aggregation_over_child(
                plan,
                &aggregation.agg_funcs,
                &aggregation.group_by_items,
                true,
                child,
                ctx,
            )
        }
        PhysicalPlan::HashJoin(hash_join) => {
            let [left_plan, right_plan] = plan.children() else {
                return Err(DriverError::unsupported(
                    "an index-join inner HashJoin has the wrong child count",
                ));
            };
            let left_has_reader = contains_index_inner_reader(left_plan);
            let right_has_reader = contains_index_inner_reader(right_plan);
            if left_has_reader == right_has_reader {
                return Err(DriverError::unsupported(
                    "an index-join inner HashJoin must contain one retained lookup reader",
                ));
            }
            let left = if left_has_reader {
                build_index_inner_subtree(
                    left_plan,
                    join,
                    probe_parts,
                    shared,
                    catalog,
                    ctx,
                    state,
                )?
            } else {
                build_with_state(left_plan, catalog, ctx, state)?
            };
            let right = if right_has_reader {
                build_index_inner_subtree(
                    right_plan,
                    join,
                    probe_parts,
                    shared,
                    catalog,
                    ctx,
                    state,
                )?
            } else {
                build_with_state(right_plan, catalog, ctx, state)?
            };
            let mut executor = build_join_over_children(
                plan,
                hash_join.join_type,
                &hash_join.left_join_keys,
                &hash_join.right_join_keys,
                &hash_join.left_conditions,
                &hash_join.right_conditions,
                &hash_join.other_conditions,
                left,
                right,
                ctx,
            )?;
            let build_child_idx = if hash_join.use_outer_to_build {
                1usize
                    .checked_sub(hash_join.inner_child_idx)
                    .ok_or_else(|| {
                        DriverError::unsupported(
                            "an index-join inner HashJoin has an invalid inner child",
                        )
                    })?
            } else {
                hash_join.inner_child_idx
            };
            executor.set_hash_build_is_left(build_child_idx == 0);
            executor.set_parallelism(hash_join.concurrency);
            executor.set_default_values(hash_join.default_values.clone());
            Ok(Box::new(executor))
        }
        _ => Err(DriverError::unsupported(format!(
            "physical {} cannot be rebuilt as an index-join inner plan",
            plan.tp()
        ))),
    }
}

fn build_index_lookup_source(
    inner: &PhysicalPlan,
    join: &tidb_planner::physical::PhysicalIndexJoin,
    probe_parts: &[LookupProbePart],
    catalog: &Catalog,
    ctx: &crate::StmtContext,
    state: &mut BuildState,
) -> Result<IndexLookupSource, DriverError> {
    if index_inner_reader_payload(inner).is_some() {
        return build_index_inner_reader(inner, join, probe_parts, None, catalog, ctx)
            .map(IndexLookupSource::Leaf);
    }
    let probes = std::rc::Rc::new(std::cell::RefCell::new(SharedIndexJoinProbes::default()));
    let exec = build_index_inner_subtree(inner, join, probe_parts, &probes, catalog, ctx, state)?;
    Ok(IndexLookupSource::Composite { exec, probes })
}

fn build_index_join(
    plan: &PhysicalPlan,
    join: &tidb_planner::physical::PhysicalIndexJoin,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
    state: &mut BuildState,
) -> Result<Box<dyn Executor>, DriverError> {
    let [left_plan, right_plan] = plan.children() else {
        return Err(DriverError::unsupported(
            "a physical index join has the wrong child count",
        ));
    };
    if join.inner_child_idx > 1 {
        return Err(DriverError::unsupported(
            "a physical index join has an invalid inner child",
        ));
    }
    let (inner_plan, outer_plan, inner_conditions, outer_conditions) = if join.inner_child_idx == 0
    {
        (
            left_plan,
            right_plan,
            &join.left_conditions,
            &join.right_conditions,
        )
    } else {
        (
            right_plan,
            left_plan,
            &join.right_conditions,
            &join.left_conditions,
        )
    };
    if !inner_conditions.is_empty() {
        return Err(DriverError::unsupported(
            "a physical index join retained a condition on its inner side",
        ));
    }
    let outer = filtered_join_child(
        plan,
        build_with_state(outer_plan, catalog, ctx, state)?,
        outer_conditions,
        ctx,
    )?;
    let left_schema = plan_schema(left_plan)?;
    let right_schema = plan_schema(right_plan)?;
    let condition_schema = tidb_expr::schema::merge_schema(Some(&left_schema), Some(&right_schema))
        .ok_or_else(|| DriverError::unsupported("a physical index join has no condition schema"))?;
    let (left_keys, right_keys) = if join.outer_hash_keys.is_empty() {
        (&join.left_join_keys, &join.right_join_keys)
    } else if join.inner_child_idx == 0 {
        (&join.inner_hash_keys, &join.outer_hash_keys)
    } else {
        (&join.outer_hash_keys, &join.inner_hash_keys)
    };
    let mut conditions = left_keys
        .iter()
        .zip(right_keys)
        .enumerate()
        .map(|(index, (left, right))| {
            join_equality_with_null(
                left,
                right,
                &condition_schema,
                join.is_null_eq.get(index).copied().unwrap_or(false),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    conditions.extend(resolve_expressions(
        &join.other_conditions,
        &condition_schema,
    )?);
    let (probe_keys, probe_parts) = index_join_probe_shape(join)?;
    let probe_key_domains = index_join_probe_key_domains(join, &probe_parts, catalog)?;
    let probe_bounds = join
        .compare_filters
        .as_ref()
        .map(|filters| {
            filters
                .ops
                .iter()
                .copied()
                .zip(&filters.args)
                .map(|(_, argument)| {
                    resolve_expression(argument.clone(), outer.schema())
                        .map(|arg| LookupProbeBound { arg })
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();
    let source = build_index_lookup_source(inner_plan, join, &probe_parts, catalog, ctx, state)?;
    let lookup_is_left = join.inner_child_idx == 0;
    let lookup = IndexLookupPlan {
        lookup_is_left,
        probe_keys,
        probe_key_domains,
        source,
        outer_not_null: Vec::new(),
        inner_not_null: Vec::new(),
        probe_bounds,
    };
    let mut executor = JoinExec::new_index_lookup(
        meta(plan, plan_schema(plan)?),
        join_kind(join.join_type)?,
        conditions,
        outer,
        left_schema
            .columns
            .iter()
            .map(|column| {
                column
                    .ret_type
                    .clone()
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong))
            })
            .collect(),
        right_schema
            .columns
            .iter()
            .map(|column| {
                column
                    .ret_type
                    .clone()
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong))
            })
            .collect(),
        ctx.clone(),
        ctx.statement_memory(),
        lookup,
    );
    executor.set_output_offsets(join_output_offsets(
        &plan_schema(plan)?,
        &left_schema,
        &right_schema,
    )?);
    executor.set_default_values(join.default_values.clone());
    Ok(Box::new(executor))
}

#[allow(clippy::too_many_arguments)]
fn build_join(
    plan: &PhysicalPlan,
    join_type: LogicalJoinType,
    left_join_keys: &[Column],
    right_join_keys: &[Column],
    left_conditions: &[Expression],
    right_conditions: &[Expression],
    other_conditions: &[Expression],
    catalog: &Catalog,
    ctx: &crate::StmtContext,
    state: &mut BuildState,
) -> Result<JoinExec<crate::StmtContext>, DriverError> {
    let [left_plan, right_plan] = plan.children() else {
        return Err(DriverError::unsupported(
            "a physical join has the wrong child count",
        ));
    };
    build_join_over_children(
        plan,
        join_type,
        left_join_keys,
        right_join_keys,
        left_conditions,
        right_conditions,
        other_conditions,
        build_with_state(left_plan, catalog, ctx, state)?,
        build_with_state(right_plan, catalog, ctx, state)?,
        ctx,
    )
}

#[allow(clippy::too_many_arguments)]
fn build_join_over_children(
    plan: &PhysicalPlan,
    join_type: LogicalJoinType,
    left_join_keys: &[Column],
    right_join_keys: &[Column],
    left_conditions: &[Expression],
    right_conditions: &[Expression],
    other_conditions: &[Expression],
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    ctx: &crate::StmtContext,
) -> Result<JoinExec<crate::StmtContext>, DriverError> {
    let left = filtered_join_child(plan, left, left_conditions, ctx)?;
    let right = filtered_join_child(plan, right, right_conditions, ctx)?;
    let left_schema = left.schema().clone();
    let right_schema = right.schema().clone();
    let condition_schema =
        tidb_expr::schema::merge_schema(Some(left.schema()), Some(right.schema()))
            .ok_or_else(|| DriverError::unsupported("a physical join has no condition schema"))?;
    let mut conditions = left_join_keys
        .iter()
        .zip(right_join_keys)
        .map(|(left, right)| join_equality(left, right, &condition_schema))
        .collect::<Result<Vec<_>, _>>()?;
    conditions.extend(resolve_expressions(other_conditions, &condition_schema)?);
    let mut executor = JoinExec::new(
        meta(plan, plan_schema(plan)?),
        join_kind(join_type)?,
        conditions,
        left,
        right,
        ctx.clone(),
        ctx.statement_memory(),
    );
    let output_offsets = join_output_offsets(&plan_schema(plan)?, &left_schema, &right_schema)?;
    executor.set_output_offsets(output_offsets);
    Ok(executor)
}

fn build_apply(
    plan: &PhysicalPlan,
    apply: &tidb_planner::physical::PhysicalApply,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
    state: &mut BuildState,
) -> Result<Box<dyn Executor>, DriverError> {
    let [left_plan, right_plan] = plan.children() else {
        return Err(DriverError::unsupported(
            "a physical Apply has the wrong child count",
        ));
    };
    if apply.hash_join.inner_child_idx > 1 {
        return Err(DriverError::unsupported(
            "a physical Apply has an invalid inner child",
        ));
    }
    let left = build_with_state(left_plan, catalog, ctx, state)?;
    let right = build_with_state(right_plan, catalog, ctx, state)?;
    let left_types = left.ret_field_types().to_vec();
    let right_types = right.ret_field_types().to_vec();
    let condition_schema =
        tidb_expr::schema::merge_schema(Some(left.schema()), Some(right.schema()))
            .ok_or_else(|| DriverError::unsupported("a physical Apply has no condition schema"))?;
    let mut conditions = apply
        .hash_join
        .equal_conditions
        .iter()
        .chain(&apply.hash_join.na_equal_conditions)
        .cloned()
        .map(Expression::ScalarFunction)
        .map(|condition| resolve_expression(condition, &condition_schema))
        .collect::<Result<Vec<_>, _>>()?;
    conditions.extend(resolve_expressions(
        &apply.hash_join.other_conditions,
        &condition_schema,
    )?);
    let left_filter = resolve_expressions(&apply.hash_join.left_conditions, left.schema())?;
    let right_filter = resolve_expressions(&apply.hash_join.right_conditions, right.schema())?;
    let inner_width = if apply.hash_join.inner_child_idx == 0 {
        left_types.len()
    } else {
        right_types.len()
    };
    let default_values = if apply.hash_join.default_values.is_empty() {
        vec![tidb_datatype::Datum::Null; inner_width]
    } else {
        apply.hash_join.default_values.clone()
    };
    if default_values.len() < inner_width {
        return Err(DriverError::unsupported(
            "a physical Apply has too few default inner values",
        ));
    }
    let joiner = new_joiner(
        ctx.clone(),
        joiner_type(apply.hash_join.join_type),
        apply.hash_join.inner_child_idx == 0,
        &default_values,
        conditions,
        &left_types,
        &right_types,
        None,
        false,
        JoinerChunkSizes {
            init_chunk_size: INIT_CAP,
            max_chunk_size: MAX_CHUNK_SIZE,
        },
    );
    let (outer, inner, outer_filter, inner_filter) = if apply.hash_join.inner_child_idx == 0 {
        (right, left, right_filter, left_filter)
    } else {
        (left, right, left_filter, right_filter)
    };
    let cache_columns = apply
        .outer_schema
        .iter()
        .map(|column| {
            usize::try_from(column.column.index).map_err(|_| {
                DriverError::unsupported("an Apply correlated column has a negative index")
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let executor = NestedLoopApplyExec::new(
        meta(plan, plan_schema(plan)?),
        outer,
        inner,
        outer_filter,
        inner_filter,
        apply.outer_schema.clone(),
        apply.hash_join.join_type != LogicalJoinType::Inner,
        joiner,
        ctx.clone(),
        ctx.statement_memory(),
    );
    let executor = if apply.can_use_cache {
        executor.with_cache(
            ctx.apply_cache_capacity(),
            cache_columns,
            ctx.session_zone(),
        )
    } else {
        executor
    };
    Ok(Box::new(executor))
}

fn join_key_offsets(
    left_keys: &[Column],
    right_keys: &[Column],
    left_schema: &Schema,
    right_schema: &Schema,
) -> Result<Vec<crate::merge_join_plan::MergeJoinKey>, DriverError> {
    left_keys
        .iter()
        .zip(right_keys)
        .map(|(left, right)| {
            let left = left_schema
                .columns
                .iter()
                .position(|column| column.unique_id == left.unique_id)
                .ok_or_else(|| {
                    DriverError::unsupported("a physical merge key is absent on the left")
                })?;
            let right = right_schema
                .columns
                .iter()
                .position(|column| column.unique_id == right.unique_id)
                .ok_or_else(|| {
                    DriverError::unsupported("a physical merge key is absent on the right")
                })?;
            Ok(crate::merge_join_plan::MergeJoinKey { left, right })
        })
        .collect()
}

fn retained_table_id(plan: &PhysicalPlan) -> Result<i64, DriverError> {
    fn collect(plan: &PhysicalPlan, table_id: &mut Option<i64>) -> Result<(), DriverError> {
        let current = match plan {
            PhysicalPlan::TableScan(scan) => Some(scan.table_id),
            PhysicalPlan::IndexScan(scan) => Some(scan.table_id),
            _ => None,
        };
        if let Some(current) = current {
            match *table_id {
                Some(expected) if expected != current => {
                    return Err(DriverError::unsupported(
                        "an index-merge partial tree reads more than one table",
                    ));
                }
                None => *table_id = Some(current),
                _ => {}
            }
        }
        for child in plan.children() {
            collect(child, table_id)?;
        }
        Ok(())
    }

    let mut table_id = None;
    collect(plan, &mut table_id)?;
    table_id
        .ok_or_else(|| DriverError::unsupported("an index-merge physical tree has no table access"))
}

fn table_output_columns(
    schema: &Schema,
    table: &crate::KvTable,
) -> Result<Vec<HandleOutputColumn>, DriverError> {
    schema
        .columns
        .iter()
        .map(|column| {
            if column.id == tidb_model::column::EXTRA_HANDLE_ID {
                return Ok(HandleOutputColumn::ExtraHandle);
            }
            table
                .columns
                .iter()
                .position(|stored| stored.id == column.id)
                .map(HandleOutputColumn::Stored)
                .ok_or_else(|| {
                    DriverError::unsupported(format!(
                        "retained table output column {} is absent from physical table {}",
                        column.id, table.table_id
                    ))
                })
        })
        .collect()
}

fn point_range_values(
    range: &tidb_planner::ranger::types::Range,
) -> Result<&[tidb_datatype::Datum], DriverError> {
    if range.low_exclude || range.high_exclude || range.low_val != range.high_val {
        return Err(DriverError::unsupported(
            "a retained point-get range is not one closed key",
        ));
    }
    Ok(&range.low_val)
}

fn point_handles(
    table: &crate::KvTable,
    ranges: &tidb_planner::ranger::types::Ranges,
    ctx: &crate::StmtContext,
) -> Result<Vec<crate::kv_table::TableHandle>, DriverError> {
    ranges
        .iter()
        .map(|range| {
            let values = point_range_values(range)?;
            if !table.common_handle_offsets().is_empty() {
                if values.len() != table.common_handle_offsets().len()
                    || values.iter().any(tidb_datatype::Datum::is_null)
                {
                    return Err(DriverError::unsupported(
                        "a retained point-get range does not name one common handle",
                    ));
                }
                let encoded = tidb_codec::encode_key_in_timezone(&ctx.session_zone(), values)
                    .map_err(|_| {
                        DriverError::unsupported(
                            "a retained point-get common handle cannot be encoded",
                        )
                    })?;
                let handle = tidb_txnkv::CommonHandle::new(encoded).map_err(|_| {
                    DriverError::unsupported("a retained point-get common handle is invalid")
                })?;
                return Ok(crate::kv_table::TableHandle::Common(
                    handle.encoded().to_vec(),
                ));
            }
            match values {
                [tidb_datatype::Datum::Int(value)] => Ok(crate::kv_table::TableHandle::Int(*value)),
                [tidb_datatype::Datum::UInt(value)] => {
                    Ok(crate::kv_table::TableHandle::Int(*value as i64))
                }
                _ => Err(DriverError::unsupported(
                    "a retained point-get range does not name one integer handle",
                )),
            }
        })
        .collect()
}

fn unique_index_point_values(
    table: &crate::KvTable,
    index_id: i64,
    ranges: &tidb_planner::ranger::types::Ranges,
    ctx: &crate::StmtContext,
    skip_null: bool,
    keep_order: bool,
    desc: bool,
) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
    let index = table
        .plan_indexes()
        .find(|index| index.id == index_id)
        .ok_or_else(|| DriverError::unsupported("physical point-get index ID is absent"))?;
    if !index.unique || index.has_prefix() {
        return Err(DriverError::unsupported(
            "a retained index point-get requires a non-prefix unique index",
        ));
    }
    let mut encoded_values = Vec::with_capacity(ranges.len());
    let mut seen = std::collections::HashSet::with_capacity(ranges.len());
    let mut kept_null = false;
    for range in ranges {
        let values = point_range_values(range)?;
        if values.len() != index.column_offsets.len() {
            return Err(DriverError::unsupported(
                "a retained point-get key has the wrong unique-index width",
            ));
        }
        if values.iter().any(tidb_datatype::Datum::is_null) {
            if !skip_null && !kept_null {
                kept_null = true;
                encoded_values.push((None, values.to_vec()));
            }
            continue;
        }
        let encoded = tidb_codec::Encoder::new(table.use_new_collation())
            .encode_key_in_timezone(&ctx.session_zone(), values)
            .map_err(|_| {
                DriverError::unsupported("a retained unique point-get key cannot be encoded")
            })?;
        if seen.insert(encoded.clone()) {
            encoded_values.push((Some(encoded), values.to_vec()));
        }
    }
    if keep_order {
        encoded_values.sort_by(|left, right| {
            let order = left.0.cmp(&right.0);
            if desc {
                order.reverse()
            } else {
                order
            }
        });
    }
    Ok(encoded_values
        .into_iter()
        .map(|(_, values)| values)
        .collect())
}

fn build_point_get(
    plan: &PhysicalPlan,
    point: &PhysicalPointGet,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    let table = catalog
        .kv_table_by_id(point.table_id)
        .ok_or_else(|| DriverError::unsupported("physical point-get table ID is absent"))?;
    if table.partition().is_some() {
        return Err(DriverError::unsupported(
            "a partitioned PointGet lacks its retained physical table ID",
        ));
    }
    let schema = plan_schema(plan)?;
    let output_columns = table_output_columns(&schema, table)?;
    let executor_meta = ExecutorMeta::new(schema, i64::from(plan.base().base.id()), 1, 1);
    if let Some(index_id) = point.index_id {
        let index_values =
            unique_index_point_values(table, index_id, &point.ranges, ctx, false, false, false)?;
        if index_values.len() != 1 {
            return Err(DriverError::unsupported(
                "a physical unique-index PointGet does not retain exactly one key",
            ));
        }
        return Ok(Box::new(UniqueIndexPointSourceExec::new(
            executor_meta,
            table.clone(),
            index_id,
            index_values,
            output_columns,
            RowDecodeContext::for_query(ctx),
            true,
        )));
    }
    let handles = point_handles(table, &point.ranges, ctx)?;
    if handles.len() != 1 {
        return Err(DriverError::unsupported(
            "a physical PointGet does not retain exactly one key",
        ));
    }
    Ok(Box::new(HandleSourceExec::new_point_mapped_with_context(
        executor_meta,
        table.clone(),
        handles.into_iter().next().expect("checked one handle"),
        output_columns,
        RowDecodeContext::for_query(ctx),
    )))
}

fn build_batch_point_get(
    plan: &PhysicalPlan,
    batch: &PhysicalBatchPointGet,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    let table = catalog
        .kv_table_by_id(batch.table_id)
        .ok_or_else(|| DriverError::unsupported("physical batch-point table ID is absent"))?;
    if table.partition().is_some() {
        return Err(DriverError::unsupported(
            "a partitioned BatchPointGet lacks retained physical table IDs",
        ));
    }
    let schema = plan_schema(plan)?;
    let output_columns = table_output_columns(&schema, table)?;
    if let Some(index_id) = batch.index_id {
        let index_values = unique_index_point_values(
            table,
            index_id,
            &batch.ranges,
            ctx,
            true,
            batch.keep_order,
            batch.desc,
        )?;
        return Ok(Box::new(UniqueIndexPointSourceExec::new(
            meta(plan, schema),
            table.clone(),
            index_id,
            index_values,
            output_columns,
            RowDecodeContext::for_query(ctx),
            false,
        )));
    }
    let mut handles = point_handles(table, &batch.ranges, ctx)?;
    let mut seen = std::collections::HashSet::with_capacity(handles.len());
    handles.retain(|handle| seen.insert(handle.clone()));
    if batch.keep_order {
        let unsigned_pk_is_handle = table
            .pk_handle_offset()
            .and_then(|offset| table.columns.get(offset))
            .is_some_and(|column| column.field_type.is_unsigned());
        sort_handles_for_keep_order(&mut handles, batch.desc, unsigned_pk_is_handle);
    }
    Ok(Box::new(HandleSourceExec::new_mapped_with_context(
        meta(plan, schema),
        table.clone(),
        handles,
        output_columns,
        RowDecodeContext::for_query(ctx),
    )))
}

fn schema_column_slot(schema: &Schema, wanted: &Column) -> Option<usize> {
    schema
        .columns
        .iter()
        .position(|column| column.unique_id == wanted.unique_id)
        .or_else(|| {
            (wanted.id != 0).then(|| {
                schema
                    .columns
                    .iter()
                    .position(|column| column.id == wanted.id)
            })?
        })
}

fn partial_handle_columns(
    schema: &Schema,
    table: &crate::KvTable,
) -> Result<PartialHandleColumns, DriverError> {
    let table_column_slot = |offset: usize| {
        let id = table.columns.get(offset)?.id;
        schema.columns.iter().position(|column| column.id == id)
    };
    if let Some(offset) = table.pk_handle_offset() {
        return table_column_slot(offset)
            .map(PartialHandleColumns::Int)
            .ok_or_else(|| {
                DriverError::unsupported(
                    "an index-merge partial tree does not emit its integer handle",
                )
            });
    }
    if !table.common_handle_offsets().is_empty() {
        return table
            .common_handle_offsets()
            .iter()
            .map(|offset| {
                table_column_slot(*offset).ok_or_else(|| {
                    DriverError::unsupported(
                        "an index-merge partial tree does not emit every common-handle column",
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .map(PartialHandleColumns::Common);
    }
    schema
        .columns
        .iter()
        .position(|column| column.id == tidb_model::column::EXTRA_HANDLE_ID)
        .map(PartialHandleColumns::Int)
        .ok_or_else(|| {
            DriverError::unsupported(
                "an index-merge partial tree over a heap table does not emit _tidb_rowid",
            )
        })
}

fn partial_sort_key_columns(
    schema: &Schema,
    by_items: &[tidb_expr::aggregation::ByItems],
) -> Result<Vec<usize>, DriverError> {
    by_items
        .iter()
        .map(|item| {
            let Expression::Column(column) = &item.expr else {
                return Err(DriverError::unsupported(
                    "an index-merge by-item is not a retained physical column",
                ));
            };
            schema_column_slot(schema, column).ok_or_else(|| {
                DriverError::unsupported(
                    "an index-merge by-item is absent from a partial plan output",
                )
            })
        })
        .collect()
}

fn build_index_merge_reader(
    plan: &PhysicalPlan,
    reader: &tidb_planner::physical::PhysicalIndexMergeReader,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
    state: &mut BuildState,
) -> Result<Box<dyn Executor>, DriverError> {
    let first = reader.partial_plans_raw.first().ok_or_else(|| {
        DriverError::unsupported("a physical index-merge reader has no partial plan")
    })?;
    let table_id = retained_table_id(first)?;
    for partial in &reader.partial_plans_raw[1..] {
        if retained_table_id(partial)? != table_id {
            return Err(DriverError::unsupported(
                "physical index-merge partial plans read different tables",
            ));
        }
    }
    let table_plan = reader.table_plan.as_deref().ok_or_else(|| {
        DriverError::unsupported("a physical index-merge reader has no final table plan")
    })?;
    if retained_table_id(table_plan)? != table_id {
        return Err(DriverError::unsupported(
            "a physical index-merge final table plan reads a different table",
        ));
    }
    let table = catalog.kv_table_by_id(table_id).ok_or_else(|| {
        DriverError::unsupported("physical index-merge table ID is absent from the catalog")
    })?;
    if table.record_physical_ids().len() != 1 {
        return Err(DriverError::unsupported(
            "physical index merge needs retained partition identity",
        ));
    }

    let mut partials: Vec<Box<dyn PartialHandleSource>> = Vec::new();
    for partial in &reader.partial_plans_raw {
        let partial_schema = plan_schema(partial)?;
        let handle_columns = partial_handle_columns(&partial_schema, table)?;
        let sort_key_columns = partial_sort_key_columns(&partial_schema, &reader.by_items)?;
        let executor = build_with_state(partial, catalog, ctx, state)?;
        if executor.schema().len() != partial_schema.len() {
            return Err(DriverError::unsupported(
                "an index-merge partial executor changed its retained output width",
            ));
        }
        partials.push(Box::new(ExecutorPartialHandleSource::new(
            executor,
            table.clone(),
            RowDecodeContext::for_query(ctx),
            handle_columns,
            sort_key_columns,
        )));
    }

    let schema = plan_schema(plan)?;
    let output_columns = table_output_columns(&schema, table)?;
    let by_items = reader
        .by_items
        .iter()
        .map(|item| MergeByItem {
            collation: tidb_expr::collation_derive::collation_of_node(&item.expr),
            desc: item.desc,
        })
        .collect();
    let mut executor = IndexMergeReaderExec::new(
        meta(plan, schema.clone()),
        table.clone(),
        RowDecodeContext::for_query(ctx),
        partials,
        reader.is_intersection_type,
    )
    .with_output_columns(output_columns)
    .with_by_items(by_items);
    lower_index_merge_selections(table_plan, &mut executor, &schema, ctx)?;
    if let Some(limit) = reader.pushed_limit {
        executor = executor.with_pushed_limit(ExecutorPushedDownLimit {
            offset: limit.offset,
            count: limit.count,
        });
    }
    Ok(Box::new(executor))
}

fn prepare_apply_bindings(plan: &mut PhysicalPlan) -> Result<(), DriverError> {
    if let PhysicalPlan::Apply(apply) = plan {
        let inner_index = apply.hash_join.inner_child_idx;
        if inner_index > 1 {
            return Err(DriverError::unsupported(
                "a physical Apply has an invalid inner child",
            ));
        }
        let children = apply.hash_join.base.children_mut();
        if children.len() != 2 {
            return Err(DriverError::unsupported(
                "a physical Apply has the wrong child count",
            ));
        }
        let (left, right) = children.split_at_mut(1);
        let (inner, outer) = if inner_index == 0 {
            (&mut left[0], &right[0])
        } else {
            (&mut right[0], &left[0])
        };
        let outer_schema = plan_schema(outer)?;
        apply.outer_schema =
            tidb_planner::physical::rebind_correlated_columns_by_schema_4_physical_plan(
                inner,
                &outer_schema,
            );
    }

    match plan {
        PhysicalPlan::TableReader(reader) => {
            if let Some(plan) = reader.table_plan.as_deref_mut() {
                prepare_apply_bindings(plan)?;
            }
        }
        PhysicalPlan::IndexReader(reader) => {
            if let Some(plan) = reader.index_plan.as_deref_mut() {
                prepare_apply_bindings(plan)?;
            }
        }
        PhysicalPlan::IndexLookUpReader(reader) => {
            if let Some(plan) = reader.index_plan.as_deref_mut() {
                prepare_apply_bindings(plan)?;
            }
            if let Some(plan) = reader.table_plan.as_deref_mut() {
                prepare_apply_bindings(plan)?;
            }
        }
        PhysicalPlan::IndexMergeReader(reader) => {
            for partial in &mut reader.partial_plans_raw {
                prepare_apply_bindings(partial)?;
            }
            if let Some(plan) = reader.table_plan.as_deref_mut() {
                prepare_apply_bindings(plan)?;
            }
        }
        PhysicalPlan::Dml(root) => {
            if let Some(plan) = root.select_plan.as_deref_mut() {
                prepare_apply_bindings(plan)?;
            }
        }
        _ => {}
    }
    for child in plan.base_mut().children_mut() {
        prepare_apply_bindings(child)?;
    }
    Ok(())
}

fn cte_reader(
    plan: &PhysicalPlan,
    cte: &tidb_planner::physical::PhysicalCTE,
    producer: SharedCteProducer,
) -> Result<Box<dyn Executor>, DriverError> {
    Ok(Box::new(CteExec::new(
        meta(plan, plan_schema(plan)?),
        producer,
        cte.has_limit,
        cte.limit_beg,
        cte.limit_end,
    )))
}

fn build_cte(
    plan: &PhysicalPlan,
    cte: &tidb_planner::physical::PhysicalCTE,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
    state: &mut BuildState,
) -> Result<Box<dyn Executor>, DriverError> {
    if let Some(producer) = state
        .cte_slots
        .get(&cte.id_for_storage)
        .and_then(|slot| slot.producer.as_ref())
        .cloned()
    {
        return cte_reader(plan, cte, producer);
    }
    if state.cte_slots.contains_key(&cte.id_for_storage) {
        return Err(DriverError::unsupported(format!(
            "producer should already be set up by CTEExec(id: {})",
            cte.id_for_storage
        )));
    }

    let seed = build_with_state(&cte.seed_plan, catalog, ctx, state)?;
    let output_types = seed.ret_field_types().to_vec();
    let mut result_storage = crate::cte_storage::CteStorage::new(
        output_types.clone(),
        MAX_CHUNK_SIZE,
        ctx.statement_memory(),
    );
    result_storage.open_and_ref()?;
    let result = Rc::new(RefCell::new(result_storage));
    let mut iter_in_storage = crate::cte_storage::CteStorage::new(
        output_types.clone(),
        MAX_CHUNK_SIZE,
        ctx.statement_memory(),
    );
    if let Err(error) = iter_in_storage.open_and_ref() {
        result.borrow_mut().deref_and_close()?;
        return Err(error.into());
    }
    let iter_in = Rc::new(RefCell::new(iter_in_storage));
    state.cte_slots.insert(
        cte.id_for_storage,
        CteBuildSlot {
            result: Rc::clone(&result),
            iter_in: Rc::clone(&iter_in),
            producer: None,
        },
    );

    let recursive = match cte.recursive_plan.as_deref() {
        Some(recursive) => match build_with_state(recursive, catalog, ctx, state) {
            Ok(executor) => Some(executor),
            Err(error) => {
                state.cte_slots.remove(&cte.id_for_storage);
                return Err(error);
            }
        },
        None => None,
    };
    let slot = state
        .cte_slots
        .get(&cte.id_for_storage)
        .expect("CTE slot inserted before recursive executor build");
    let result = Rc::clone(&slot.result);
    let iter_in = Rc::clone(&slot.iter_in);
    let producer = Rc::new(RefCell::new(CteProducer::new(
        seed,
        recursive,
        result,
        iter_in,
        cte.is_distinct,
        cte.has_limit,
        cte.limit_end,
        ctx.clone(),
        output_types,
        MAX_CHUNK_SIZE,
    )));
    state
        .cte_slots
        .get_mut(&cte.id_for_storage)
        .expect("CTE slot inserted before recursive executor build")
        .producer = Some(Rc::clone(&producer));
    cte_reader(plan, cte, producer)
}

fn build_cte_table(
    plan: &PhysicalPlan,
    table: &tidb_planner::physical::PhysicalCTETable,
    state: &BuildState,
) -> Result<Box<dyn Executor>, DriverError> {
    let slot = state.cte_slots.get(&table.id_for_storage).ok_or_else(|| {
        DriverError::unsupported(format!(
            "iterInTbl should already be set up by CTEExec(id: {})",
            table.id_for_storage
        ))
    })?;
    Ok(Box::new(CteTableReaderExec::new(
        meta(plan, plan_schema(plan)?),
        Rc::clone(&slot.iter_in),
    )))
}

/// Recursively instantiates one retained physical operator tree.
fn build_with_state(
    plan: &PhysicalPlan,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
    state: &mut BuildState,
) -> Result<Box<dyn Executor>, DriverError> {
    let executor: Box<dyn Executor> = match plan {
        PhysicalPlan::MemTable(scan) => build_mem_table(plan, scan, catalog),
        PhysicalPlan::TableScan(scan) => build_table_scan(plan, scan, catalog, ctx),
        PhysicalPlan::IndexScan(scan) => build_index_reader(
            plan,
            plan,
            None,
            true,
            scan.keep_order,
            None,
            None,
            catalog,
            ctx,
        ),
        PhysicalPlan::TableReader(reader) => {
            let executor = build_reader(reader.table_plan.as_deref(), catalog, ctx, state)?;
            ctx.mark_physical_table_reader();
            Ok(executor)
        }
        PhysicalPlan::IndexReader(reader) => {
            build_reader(reader.index_plan.as_deref(), catalog, ctx, state)
        }
        PhysicalPlan::IndexLookUpReader(reader) => {
            let executor = build_index_reader(
                plan,
                reader.index_plan.as_deref().ok_or_else(|| {
                    DriverError::unsupported("a physical index lookup has no index plan")
                })?,
                reader.table_plan.as_deref(),
                false,
                reader.keep_order,
                reader.pushed_limit,
                reader.paging.then_some(reader.expect_cnt),
                catalog,
                ctx,
            )?;
            ctx.mark_physical_table_reader();
            Ok(executor)
        }
        PhysicalPlan::IndexMergeReader(reader) => {
            let executor = build_index_merge_reader(plan, reader, catalog, ctx, state)?;
            ctx.mark_physical_table_reader();
            Ok(executor)
        }
        PhysicalPlan::PointGet(point) => build_point_get(plan, point, catalog, ctx),
        PhysicalPlan::BatchPointGet(batch) => build_batch_point_get(plan, batch, catalog, ctx),
        PhysicalPlan::Projection(projection) => {
            let child = build_with_state(only_child(plan)?, catalog, ctx, state)?;
            let expressions = resolve_expressions(&projection.exprs, child.schema())?;
            let executor: Box<dyn Executor> = Box::new(ProjectionExec::new(
                meta(plan, plan_schema(plan)?),
                expressions,
                child,
                ctx.clone(),
            ));
            Ok(executor)
        }
        PhysicalPlan::Selection(selection) => {
            let mut child = build_with_state(only_child(plan)?, catalog, ctx, state)?;
            let filters = resolve_expressions(&selection.conditions, child.schema())?;
            let pushed = crate::predicate_pushdown::PushedScanFilter::from_physical_conditions(
                filters.clone(),
            );
            if child
                .table_access()
                .is_some_and(|access| access.accept_scan_filter(&pushed, ctx))
            {
                Ok(child)
            } else {
                let schema = unary_schema(plan, child.as_ref());
                Ok::<Box<dyn Executor>, DriverError>(Box::new(SelectionExec::new(
                    meta(plan, schema),
                    filters,
                    child,
                    ctx.clone(),
                    ctx.statement_memory(),
                )))
            }
        }
        PhysicalPlan::Limit(limit) => {
            let child = build_with_state(only_child(plan)?, catalog, ctx, state)?;
            let schema = unary_schema(plan, child.as_ref());
            Ok(Box::new(LimitExec::new(
                meta(plan, schema),
                limit.offset,
                limit.count,
                child,
            )) as Box<dyn Executor>)
        }
        PhysicalPlan::Sort(sort) => {
            let child = build_with_state(only_child(plan)?, catalog, ctx, state)?;
            let by_items = sort
                .by_items
                .iter()
                .map(|item| {
                    Ok::<SortByItem, DriverError>(SortByItem {
                        expr: resolve_expression(item.expr.clone(), child.schema())?,
                        desc: item.desc,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let schema = unary_schema(plan, child.as_ref());
            Ok(Box::new(
                SortExec::new(
                    meta(plan, schema),
                    by_items,
                    child,
                    ctx.clone(),
                    ctx.statement_memory(),
                )
                .with_parallelism(ctx.executor_concurrency()),
            ) as Box<dyn Executor>)
        }
        PhysicalPlan::TopN(topn) => {
            let child = build_with_state(only_child(plan)?, catalog, ctx, state)?;
            let by_items = topn
                .by_items
                .iter()
                .map(|item| {
                    resolve_expression(item.expr.clone(), child.schema()).map(|expr| SortByItem {
                        expr,
                        desc: item.desc,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let schema = unary_schema(plan, child.as_ref());
            Ok(Box::new(
                TopNExec::new(
                    meta(plan, schema),
                    by_items,
                    child,
                    ctx.clone(),
                    topn.offset,
                    topn.count,
                    ctx.statement_memory(),
                )
                .with_parallelism(ctx.executor_concurrency()),
            ) as Box<dyn Executor>)
        }
        PhysicalPlan::HashAgg(aggregation) => build_aggregation(
            plan,
            &aggregation.agg_funcs,
            &aggregation.group_by_items,
            false,
            catalog,
            ctx,
            state,
        ),
        PhysicalPlan::StreamAgg(aggregation) => build_aggregation(
            plan,
            &aggregation.agg_funcs,
            &aggregation.group_by_items,
            true,
            catalog,
            ctx,
            state,
        ),
        PhysicalPlan::HashJoin(join) => {
            let mut executor = build_join(
                plan,
                join.join_type,
                &join.left_join_keys,
                &join.right_join_keys,
                &join.left_conditions,
                &join.right_conditions,
                &join.other_conditions,
                catalog,
                ctx,
                state,
            )?;
            let build_child_idx = if join.use_outer_to_build {
                1usize.checked_sub(join.inner_child_idx).ok_or_else(|| {
                    DriverError::unsupported("a physical hash join has an invalid inner child")
                })?
            } else {
                join.inner_child_idx
            };
            executor.set_hash_build_is_left(build_child_idx == 0);
            executor.set_parallelism(join.concurrency);
            executor.set_default_values(join.default_values.clone());
            Ok(Box::new(executor) as Box<dyn Executor>)
        }
        PhysicalPlan::MergeJoin(join) => {
            let [left, right] = plan.children() else {
                return Err(DriverError::unsupported(
                    "a physical merge join has the wrong child count",
                ));
            };
            let keys = join_key_offsets(
                &join.left_join_keys,
                &join.right_join_keys,
                &plan_schema(left)?,
                &plan_schema(right)?,
            )?;
            let mut executor = build_join(
                plan,
                join.join_type,
                &join.left_join_keys,
                &join.right_join_keys,
                &join.left_conditions,
                &join.right_conditions,
                &join.other_conditions,
                catalog,
                ctx,
                state,
            )?;
            executor.set_merge_plan(crate::merge_join_plan::MergeJoinPlan {
                keys,
                desc: join.desc,
            });
            executor.set_default_values(join.default_values.clone());
            Ok(Box::new(executor) as Box<dyn Executor>)
        }
        PhysicalPlan::IndexJoin(join) => build_index_join(plan, join, catalog, ctx, state),
        PhysicalPlan::Apply(apply) => build_apply(plan, apply, catalog, ctx, state),
        PhysicalPlan::TableDual(dual) => Ok(Box::new(TableDualExec::new(
            meta(
                plan,
                plan.base()
                    .base
                    .schema()
                    .cloned()
                    .unwrap_or_else(|| Schema::new(Vec::new())),
            ),
            dual.row_count,
        )) as Box<dyn Executor>),
        PhysicalPlan::UnionAll(_) => {
            let children = plan
                .children()
                .iter()
                .map(|child| build_with_state(child, catalog, ctx, state))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Box::new(super::set_opr::UnionAllExec::new(
                meta(plan, plan_schema(plan)?),
                children,
            )) as Box<dyn Executor>)
        }
        PhysicalPlan::MaxOneRow(_) => {
            let child = build_with_state(only_child(plan)?, catalog, ctx, state)?;
            let executor_meta =
                ExecutorMeta::new(plan_schema(plan)?, i64::from(plan.base().base.id()), 2, 2);
            Ok(Box::new(MaxOneRowExec::new(executor_meta, child)) as Box<dyn Executor>)
        }
        PhysicalPlan::NominalSort(_) => build_with_state(only_child(plan)?, catalog, ctx, state),
        PhysicalPlan::CTE(cte) => build_cte(plan, cte, catalog, ctx, state),
        PhysicalPlan::CTETable(table) => build_cte_table(plan, table, state),
        _ => Err(DriverError::unsupported(format!(
            "physical executor construction for {} is not implemented",
            plan.base().base.tp()
        ))),
    }?;
    Ok(state.meter(plan, executor))
}

pub(super) fn build(
    plan: &PhysicalPlan,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    build_with_state(plan, catalog, ctx, &mut BuildState::default())
}

fn schema_column_name(column: &Column, ordinal: usize) -> String {
    column
        .orig_name
        .rsplit('.')
        .find(|part| !part.is_empty())
        .map_or_else(|| format!("Column#{}", ordinal + 1), str::to_owned)
}

fn result_columns(select: &tidb_ast::SelectStmt, schema: &Schema) -> Vec<(String, FieldType)> {
    let expression_names = select
        .fields
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(index, field)| match field {
            tidb_ast::SelectField::Expr { expr, alias } => {
                Some(alias.clone().unwrap_or_else(|| {
                    super::default_field_display_name(&select.fields, index, expr)
                }))
            }
            tidb_ast::SelectField::Wildcard(_) => None,
        })
        .collect::<Vec<_>>();
    schema
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let name = if expression_names.len() == schema.len() {
                expression_names[index].clone()
            } else {
                schema_column_name(column, index)
            };
            let field_type = column
                .ret_type
                .clone()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
            (name, field_type)
        })
        .collect()
}

fn physical_result_columns(plan: &PhysicalPlan, schema: &Schema) -> Vec<(String, FieldType)> {
    let names = plan.base().base.output_names();
    schema
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let name = names.get(index).map_or_else(
                || schema_column_name(column, index),
                |field| {
                    if field.names.column.original.is_empty() {
                        if field.names.column.lower.is_empty() {
                            schema_column_name(column, index)
                        } else {
                            field.names.column.lower.clone()
                        }
                    } else {
                        field.names.column.original.clone()
                    }
                },
            );
            let field_type = column
                .ret_type
                .clone()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
            (name, field_type)
        })
        .collect()
}

/// Derives the client result metadata from the selected physical root without
/// constructing or draining an executor. Go keeps this metadata on the same
/// `ExecStmt.Plan` used by `buildExecutor`; planning-only consumers must not
/// enter a second AST execution pipeline to rediscover it.
pub(super) fn planned_result_columns(
    select: &tidb_ast::SelectStmt,
    physical: &PhysicalPlan,
) -> Result<Vec<(String, FieldType)>, DriverError> {
    Ok(result_columns(select, &plan_schema(physical)?))
}

/// Go's common query execution seam: both SELECT and set-operation logical
/// roots are already physical plans before executor construction begins.
pub(super) fn execute_query(
    query: &tidb_ast::QueryStmt,
    physical: &mut PhysicalPlan,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    prepare_apply_bindings(physical)?;
    let root = build(physical, catalog, ctx)?;
    let columns = match query {
        tidb_ast::QueryStmt::Select(select) => result_columns(select, root.schema()),
        tidb_ast::QueryStmt::SetOpr(_) => physical_result_columns(physical, root.schema()),
    };
    super::drain_root_executor(root, columns, ctx)
}

/// Builds and drains the target of `EXPLAIN ANALYZE` through the same
/// physical executor switch used by ordinary and cached execution. The
/// returned counters are keyed by the retained physical-node addresses, so
/// rendering cannot accidentally attribute a duplicate plan id to a sibling.
pub(crate) fn execute_for_explain(
    physical: &mut PhysicalPlan,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<PhysicalRuntimeStats, DriverError> {
    prepare_apply_bindings(physical)?;
    let mut state = BuildState {
        runtime_counters: Some(HashMap::new()),
        ..BuildState::default()
    };
    let mut root = build_with_state(physical, catalog, ctx, &mut state)?;
    let result = (|| {
        root.open()?;
        let mut req = root.new_chunk();
        loop {
            super::next_executor(root.as_mut(), &mut req, &ctx.statement_memory())?;
            if req.num_rows() == 0 {
                break;
            }
        }
        Ok::<(), DriverError>(())
    })();
    let close_result = root.close().map_err(DriverError::from);
    result?;
    close_result?;
    Ok(state.runtime_counters.take().unwrap_or_default())
}

/// Builds and drains the retained child of an UPDATE or DELETE. Go's DML
/// executors consume this same physical child to obtain both the row values
/// and the handle columns used by the write. Runtime counters are collected
/// only for EXPLAIN ANALYZE; ordinary and cached execution use the identical
/// builder without the metering wrappers.
pub(crate) fn execute_dml_source(
    physical: &mut PhysicalPlan,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
    analyze: bool,
) -> Result<(Vec<Vec<tidb_datatype::Datum>>, PhysicalRuntimeStats), DriverError> {
    prepare_apply_bindings(physical)?;
    let mut state = BuildState {
        runtime_counters: analyze.then(HashMap::new),
        ..BuildState::default()
    };
    let root = build_with_state(physical, catalog, ctx, &mut state)?;
    let field_types = root.ret_field_types().to_vec();
    let rows = super::drain_executor_rows(root, &field_types, &ctx.statement_memory())?;
    Ok((rows, state.runtime_counters.take().unwrap_or_default()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::Datum;
    use tidb_planner::physical::{
        BasePhysicalPlan, PhysicalBatchPointGet, PhysicalCTE, PhysicalCTETable,
        PhysicalIndexMergeReader, PhysicalMaxOneRow, PhysicalPointGet, PhysicalSelection,
        PhysicalTableDual, PhysicalTableReader, PhysicalUnionAll,
    };

    fn int_handles(values: &[i64]) -> Vec<TableHandle> {
        values.iter().copied().map(TableHandle::Int).collect()
    }

    fn int_values(handles: &[TableHandle]) -> Vec<i64> {
        handles
            .iter()
            .map(|handle| handle.int_value().expect("int handle"))
            .collect()
    }

    #[test]
    fn retained_batch_point_handle_order_matches_go() {
        let mut signed = int_handles(&[5, -3, 1]);
        sort_handles_for_keep_order(&mut signed, false, false);
        assert_eq!(int_values(&signed), vec![-3, 1, 5]);
        sort_handles_for_keep_order(&mut signed, true, false);
        assert_eq!(int_values(&signed), vec![5, 1, -3]);

        let mut unsigned = int_handles(&[-1, 1, i64::MIN]);
        sort_handles_for_keep_order(&mut unsigned, false, true);
        assert_eq!(int_values(&unsigned), vec![1, i64::MIN, -1]);

        let mut common = vec![
            TableHandle::Common(vec![2, 0]),
            TableHandle::Common(vec![1, 9]),
        ];
        sort_handles_for_keep_order(&mut common, false, false);
        assert_eq!(common[0], TableHandle::Common(vec![1, 9]));
    }

    fn empty_schema() -> Schema {
        Schema::new(Vec::new())
    }

    fn dual(id: i32, rows: usize) -> PhysicalPlan {
        let mut base = BasePhysicalPlan::with_id(id, "TableDual", 0);
        base.base.set_schema(Some(empty_schema()));
        PhysicalPlan::TableDual(PhysicalTableDual {
            base,
            row_count: rows,
        })
    }

    fn union(id: i32, children: Vec<PhysicalPlan>) -> PhysicalPlan {
        let mut base = BasePhysicalPlan::with_id(id, "UnionAll", 0);
        base.base.set_schema(Some(empty_schema()));
        base.set_children(children);
        PhysicalPlan::UnionAll(PhysicalUnionAll { base, mpp: false })
    }

    fn cte_table(id: i32, storage_id: i32) -> PhysicalPlan {
        let mut base = BasePhysicalPlan::with_id(id, "CTETable", 0);
        base.base.set_schema(Some(empty_schema()));
        PhysicalPlan::CTETable(PhysicalCTETable {
            base,
            id_for_storage: storage_id,
        })
    }

    fn cte(
        id: i32,
        storage_id: i32,
        seed: PhysicalPlan,
        recursive: Option<PhysicalPlan>,
        distinct: bool,
        limit: Option<(u64, u64)>,
    ) -> PhysicalPlan {
        let mut base = BasePhysicalPlan::with_id(id, "CTE", 0);
        base.base.set_schema(Some(empty_schema()));
        PhysicalPlan::CTE(PhysicalCTE {
            base,
            seed_plan: Box::new(seed),
            recursive_plan: recursive.map(Box::new),
            id_for_storage: storage_id,
            is_distinct: distinct,
            has_limit: limit.is_some(),
            limit_beg: limit.map_or(0, |limit| limit.0),
            limit_end: limit.map_or(0, |limit| limit.1),
            cte_as_name: "r".to_owned(),
            cte_name: "r".to_owned(),
        })
    }

    fn drain_row_count(mut executor: Box<dyn Executor>) -> Result<usize, crate::ExecError> {
        executor.open()?;
        let mut rows = 0;
        loop {
            let mut output = executor.new_chunk();
            executor.next(&mut output)?;
            if output.num_rows() == 0 {
                break;
            }
            rows += output.num_rows();
        }
        executor.close()?;
        Ok(rows)
    }

    #[test]
    fn physical_cte_uses_the_recursive_delta_and_go_limit_window() {
        let plan = cte(
            3,
            41,
            dual(1, 1),
            Some(cte_table(2, 41)),
            false,
            Some((1, 4)),
        );
        let executor = build(&plan, &Catalog::default(), &crate::StmtContext::for_query())
            .expect("build recursive physical CTE");
        assert_eq!(drain_row_count(executor).expect("run recursive CTE"), 3);
    }

    #[test]
    fn physical_union_distinct_cte_stops_when_its_delta_is_empty() {
        let plan = cte(3, 42, dual(1, 1), Some(cte_table(2, 42)), true, None);
        let executor = build(&plan, &Catalog::default(), &crate::StmtContext::for_query())
            .expect("build distinct recursive physical CTE");
        assert_eq!(drain_row_count(executor).expect("run distinct CTE"), 1);
    }

    #[test]
    fn cached_physical_union_and_max_one_row_build_directly() {
        let catalog = Catalog::default();
        let ctx = crate::StmtContext::default();

        let mut union_executor = build(&union(3, vec![dual(1, 1), dual(2, 1)]), &catalog, &ctx)
            .expect("build cached union");
        union_executor.open().expect("open cached union");
        let mut output = union_executor.new_chunk();
        union_executor.next(&mut output).expect("first union row");
        assert_eq!(output.num_rows(), 1);
        union_executor.next(&mut output).expect("second union row");
        assert_eq!(output.num_rows(), 1);
        union_executor.next(&mut output).expect("union eof");
        assert_eq!(output.num_rows(), 0);

        let union = union(6, vec![dual(4, 1), dual(5, 1)]);
        let mut base = BasePhysicalPlan::with_id(7, "MaxOneRow", 0);
        base.base.set_schema(Some(empty_schema()));
        base.set_children(vec![union]);
        let max_one = PhysicalPlan::MaxOneRow(PhysicalMaxOneRow { base });
        let mut max_executor = build(&max_one, &catalog, &ctx).expect("build cached max-one-row");
        max_executor.open().expect("open cached max-one-row");
        let error = max_executor
            .next(&mut max_executor.new_chunk())
            .expect_err("two union rows violate MaxOneRow");
        assert!(matches!(
            error,
            crate::ExecError::SubqueryReturnsMoreThanOneRow
        ));
    }

    fn long_column(name: &str, id: i64) -> crate::kv_table::KvColumn {
        crate::kv_table::KvColumn {
            name: name.to_owned(),
            id,
            field_type: FieldType::new(FieldTypeCode::LongLong),
            column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
            default_value: None,
            origin_default: None,
            comment: String::new(),
            generated: None,
        }
    }

    fn two_long_schema() -> Schema {
        Schema::new(
            [1, 2]
                .into_iter()
                .enumerate()
                .map(|(index, id)| {
                    let mut column = Column::new(id, FieldType::new(FieldTypeCode::LongLong));
                    column.id = id;
                    column.index = index as i64;
                    column
                })
                .collect(),
        )
    }

    fn table_scan(id: i32, table_id: i64, low: i64, high: i64) -> PhysicalPlan {
        let schema = two_long_schema();
        let mut base = BasePhysicalPlan::with_id(id, "TableScan", 0);
        base.base.set_schema(Some(schema.clone()));
        PhysicalPlan::TableScan(PhysicalTableScan {
            base,
            table_id,
            cost_columns: schema.columns,
            ranges: vec![tidb_planner::ranger::types::Range {
                low_val: vec![tidb_datatype::Datum::Int(low)],
                high_val: vec![tidb_datatype::Datum::Int(high)],
                collators: vec![tidb_datatype::Collation::Binary],
                low_exclude: false,
                high_exclude: false,
            }],
            ..PhysicalTableScan::default()
        })
    }

    #[test]
    fn table_reader_build_records_go_stmtctx_table_access() {
        let table_id = 40;
        let mut catalog = Catalog::default();
        catalog.register_kv(
            "reader",
            crate::KvTable::new(
                table_id,
                vec![long_column("id", 1), long_column("value", 2)],
            ),
        );
        let scan = table_scan(1, table_id, 1, 1);
        let mut base = BasePhysicalPlan::with_id(2, "TableReader", 0);
        base.base.set_schema(scan.schema().cloned());
        let plan = PhysicalPlan::TableReader(PhysicalTableReader {
            base,
            table_plan: Some(Box::new(scan)),
            store_type: tidb_planner::physical_table_reader::StoreType::TiKv,
            is_common_handle: false,
            read_req_type: tidb_planner::physical_table_reader::ReadReqType::Cop,
        });

        let session = crate::SessionMemory::new(-1, crate::OomAction::Cancel, 7);
        let cancellation = session.begin_query_cancellation();
        cancellation.cancel();
        let ctx = crate::StmtContext::for_query().with_statement_memory(session.statement());
        let _executor = build(&plan, &catalog, &ctx).expect("build physical table reader");

        assert!(tidb_expr::Columns::sleep_for(
            &ctx,
            std::time::Duration::from_secs(1)
        ));
        assert!(tidb_expr::Columns::sleep_for(
            &ctx,
            std::time::Duration::from_secs(1)
        ));
    }

    #[test]
    fn cached_physical_table_scan_emits_only_extra_handle() {
        let mut table = crate::KvTable::new(41, vec![long_column("value", 1)]);
        for (row_id, value) in [(11, 100), (13, 200)] {
            table
                .insert_row_with_row_id(
                    &[Datum::Int(value)],
                    Some(row_id),
                    0,
                    &tidb_expr::NoColumns,
                )
                .expect("seed heap row");
        }
        let mut catalog = Catalog::default();
        catalog.register_kv("heap", table);

        let mut handle = Column::new(
            tidb_model::column::EXTRA_HANDLE_ID,
            FieldType::new(FieldTypeCode::LongLong),
        );
        handle.id = tidb_model::column::EXTRA_HANDLE_ID;
        handle.index = 0;
        let mut base = BasePhysicalPlan::with_id(9, "TableScan", 0);
        base.base
            .set_schema(Some(Schema::new(vec![handle.clone()])));
        let plan = PhysicalPlan::TableScan(PhysicalTableScan {
            base,
            table_id: 41,
            cost_columns: vec![handle],
            ..PhysicalTableScan::default()
        });

        let ctx = crate::StmtContext::for_query();
        let mut executor = build(&plan, &catalog, &ctx).expect("build cached table scan");
        executor.open().expect("open cached table scan");
        let mut output = executor.new_chunk();
        executor.next(&mut output).expect("read cached table scan");
        assert_eq!(output.num_cols(), 1);
        assert_eq!(output.num_rows(), 2);
        assert_eq!(output.get_row(0).get_int64(0), 11);
        assert_eq!(output.get_row(1).get_int64(0), 13);
    }

    #[test]
    fn cached_physical_point_get_builds_direct_handle_read() {
        let mut table =
            crate::KvTable::new(43, vec![long_column("id", 1), long_column("value", 2)]);
        table.set_pk_handle_offset(0);
        for value in 1..=3 {
            table
                .insert_row(
                    &[Datum::Int(value), Datum::Int(value * 10)],
                    &tidb_expr::NoColumns,
                )
                .expect("seed clustered row");
        }
        let mut catalog = Catalog::default();
        catalog.register_kv("point", table);

        let mut base = BasePhysicalPlan::with_id(10, "PointGet", 0);
        base.base.set_schema(Some(two_long_schema()));
        let plan = PhysicalPlan::PointGet(PhysicalPointGet {
            base,
            table_id: 43,
            index_id: None,
            ranges: vec![tidb_planner::ranger::types::Range {
                low_val: vec![Datum::Int(2)],
                high_val: vec![Datum::Int(2)],
                collators: vec![tidb_datatype::Collation::Binary],
                low_exclude: false,
                high_exclude: false,
            }],
            range_rebuild: None,
        });

        let ctx = crate::StmtContext::for_query();
        let mut executor = build(&plan, &catalog, &ctx).expect("build cached point get");
        assert_eq!(executor.init_cap(), 1);
        assert_eq!(executor.max_chunk_size(), 1);
        executor.open().expect("open cached point get");
        let mut output = executor.new_chunk();
        executor.next(&mut output).expect("read cached point get");
        assert_eq!(output.num_rows(), 1);
        assert_eq!(output.get_row(0).get_int64(0), 2);
        assert_eq!(output.get_row(0).get_int64(1), 20);
        executor.next(&mut output).expect("point get eof");
        assert_eq!(output.num_rows(), 0);
    }

    #[test]
    fn cached_physical_point_get_resolves_unique_index_before_common_handle() {
        let mut table =
            crate::KvTable::new(46, vec![long_column("id", 1), long_column("value", 2)]);
        table.set_common_handle_offsets(vec![0, 1]);
        table
            .create_index_with_context(
                crate::kv_table::KvIndex {
                    id: 8,
                    name: "value_uidx".to_owned(),
                    comment: String::new(),
                    unique: true,
                    column_offsets: vec![1],
                    prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH],
                    visible: true,
                    global: false,
                    clustered_primary: false,
                },
                &crate::StmtContext::for_query(),
            )
            .expect("create unique index");
        for value in 1..=3 {
            table
                .insert_row(
                    &[Datum::Int(value), Datum::Int(value * 10)],
                    &tidb_expr::NoColumns,
                )
                .expect("seed common-handle row");
        }
        let mut catalog = Catalog::default();
        catalog.register_kv("unique_point", table);

        let mut base = BasePhysicalPlan::with_id(13, "PointGet", 0);
        base.base.set_schema(Some(two_long_schema()));
        let plan = PhysicalPlan::PointGet(PhysicalPointGet {
            base,
            table_id: 46,
            index_id: Some(8),
            ranges: vec![tidb_planner::ranger::types::Range {
                low_val: vec![Datum::Int(20)],
                high_val: vec![Datum::Int(20)],
                collators: vec![tidb_datatype::Collation::Binary],
                low_exclude: false,
                high_exclude: false,
            }],
            range_rebuild: None,
        });

        let ctx = crate::StmtContext::for_query();
        let mut executor = build(&plan, &catalog, &ctx).expect("build unique point get");
        executor.open().expect("open unique point get");
        let mut output = executor.new_chunk();
        executor.next(&mut output).expect("read unique point get");
        assert_eq!(output.num_rows(), 1);
        assert_eq!(output.get_row(0).get_int64(0), 2);
        assert_eq!(output.get_row(0).get_int64(1), 20);
        executor.next(&mut output).expect("unique point get eof");
        assert_eq!(output.num_rows(), 0);
    }

    #[test]
    fn cached_physical_batch_point_get_deduplicates_and_orders_handles() {
        let mut table =
            crate::KvTable::new(44, vec![long_column("id", 1), long_column("value", 2)]);
        table.set_pk_handle_offset(0);
        for value in 1..=3 {
            table
                .insert_row(
                    &[Datum::Int(value), Datum::Int(value * 10)],
                    &tidb_expr::NoColumns,
                )
                .expect("seed clustered row");
        }
        let mut catalog = Catalog::default();
        catalog.register_kv("batch", table);

        let mut base = BasePhysicalPlan::with_id(11, "BatchPointGet", 0);
        base.base.set_schema(Some(two_long_schema()));
        let point_range = |value| tidb_planner::ranger::types::Range {
            low_val: vec![Datum::Int(value)],
            high_val: vec![Datum::Int(value)],
            collators: vec![tidb_datatype::Collation::Binary],
            low_exclude: false,
            high_exclude: false,
        };
        let plan = PhysicalPlan::BatchPointGet(PhysicalBatchPointGet {
            base,
            table_id: 44,
            index_id: None,
            ranges: [3, 1, 3, 9].into_iter().map(point_range).collect(),
            range_rebuild: None,
            keep_order: true,
            desc: false,
        });

        let ctx = crate::StmtContext::for_query();
        let mut executor = build(&plan, &catalog, &ctx).expect("build cached batch point get");
        executor.open().expect("open cached batch point get");
        let mut output = executor.new_chunk();
        executor
            .next(&mut output)
            .expect("read cached batch point get");
        assert_eq!(output.num_rows(), 2);
        assert_eq!(output.get_row(0).get_int64(0), 1);
        assert_eq!(output.get_row(1).get_int64(0), 3);
    }

    #[test]
    fn cached_physical_batch_point_get_uses_batched_unique_index_lookup() {
        let mut table =
            crate::KvTable::new(45, vec![long_column("id", 1), long_column("value", 2)]);
        table.set_pk_handle_offset(0);
        table
            .create_index_with_context(
                crate::kv_table::KvIndex {
                    id: 8,
                    name: "value_uidx".to_owned(),
                    comment: String::new(),
                    unique: true,
                    column_offsets: vec![1],
                    prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH],
                    visible: true,
                    global: false,
                    clustered_primary: false,
                },
                &crate::StmtContext::for_query(),
            )
            .expect("create unique index");
        for value in 1..=3 {
            table
                .insert_row(
                    &[Datum::Int(value), Datum::Int(value * 10)],
                    &tidb_expr::NoColumns,
                )
                .expect("seed unique-index row");
        }
        let mut catalog = Catalog::default();
        catalog.register_kv("unique_batch", table);

        let mut base = BasePhysicalPlan::with_id(12, "BatchPointGet", 0);
        base.base.set_schema(Some(two_long_schema()));
        let point_range = |value| tidb_planner::ranger::types::Range {
            low_val: vec![Datum::Int(value)],
            high_val: vec![Datum::Int(value)],
            collators: vec![tidb_datatype::Collation::Binary],
            low_exclude: false,
            high_exclude: false,
        };
        let plan = PhysicalPlan::BatchPointGet(PhysicalBatchPointGet {
            base,
            table_id: 45,
            index_id: Some(8),
            ranges: [30, 10, 30, 90].into_iter().map(point_range).collect(),
            range_rebuild: None,
            keep_order: true,
            desc: false,
        });

        let ctx = crate::StmtContext::for_query();
        let mut executor = build(&plan, &catalog, &ctx).expect("build unique batch point get");
        executor.open().expect("open unique batch point get");
        let mut output = executor.new_chunk();
        executor
            .next(&mut output)
            .expect("read unique batch point get");
        assert_eq!(output.num_rows(), 2);
        assert_eq!(output.get_row(0).get_int64(0), 1);
        assert_eq!(output.get_row(1).get_int64(0), 3);
    }

    fn index_scan(id: i32, table_id: i64, index_id: i64, low: i64, high: i64) -> PhysicalPlan {
        let schema = two_long_schema();
        let mut base = BasePhysicalPlan::with_id(id, "IndexScan", 0);
        base.base.set_schema(Some(schema.clone()));
        PhysicalPlan::IndexScan(PhysicalIndexScan {
            base,
            table_id,
            cost_columns: schema.columns,
            index_id,
            index_name: "value_idx".to_owned(),
            ranges: vec![tidb_planner::ranger::types::Range {
                low_val: vec![tidb_datatype::Datum::Int(low)],
                high_val: vec![tidb_datatype::Datum::Int(high)],
                collators: vec![tidb_datatype::Collation::Binary],
                low_exclude: false,
                high_exclude: false,
            }],
            ..PhysicalIndexScan::default()
        })
    }

    fn table_selection(id: i32, child: PhysicalPlan, minimum_value: i64) -> PhysicalPlan {
        let schema = two_long_schema();
        let mut value = schema.columns[1].clone();
        value.index = 1;
        let condition = Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("gt"),
            FieldType::new(FieldTypeCode::Tiny),
            vec![
                Expression::Column(value),
                Expression::Constant(tidb_expr::constant::Constant::new(
                    Datum::Int(minimum_value),
                    FieldType::new(FieldTypeCode::LongLong),
                )),
            ],
        ));
        let mut base = BasePhysicalPlan::with_id(id, "Selection", 0);
        base.base.set_schema(Some(schema));
        base.set_children(vec![child]);
        PhysicalPlan::Selection(PhysicalSelection {
            base,
            conditions: vec![condition],
            from_data_source: true,
        })
    }

    #[test]
    fn cached_physical_index_merge_builds_from_retained_partial_trees() {
        let mut table =
            crate::KvTable::new(42, vec![long_column("id", 1), long_column("value", 2)]);
        table.set_pk_handle_offset(0);
        table
            .create_index_with_context(
                crate::kv_table::KvIndex {
                    id: 7,
                    name: "value_idx".to_owned(),
                    comment: String::new(),
                    unique: false,
                    column_offsets: vec![1],
                    prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH],
                    visible: true,
                    global: false,
                    clustered_primary: false,
                },
                &crate::StmtContext::for_query(),
            )
            .expect("create secondary index");
        for value in 1..=4 {
            table
                .insert_row(
                    &[Datum::Int(value), Datum::Int(value * 10)],
                    &tidb_expr::NoColumns,
                )
                .expect("seed row");
        }
        let mut catalog = Catalog::default();
        catalog.register_kv("im", table);

        let mut base = BasePhysicalPlan::with_id(13, "IndexMerge", 0);
        base.base.set_schema(Some(two_long_schema()));
        let plan = PhysicalPlan::IndexMergeReader(PhysicalIndexMergeReader {
            base,
            partial_plans_raw: vec![index_scan(10, 42, 7, 10, 30), index_scan(11, 42, 7, 30, 40)],
            table_plan: Some(Box::new(table_selection(
                12,
                table_scan(14, 42, i64::MIN, i64::MAX),
                20,
            ))),
            ..PhysicalIndexMergeReader::default()
        });

        let ctx = crate::StmtContext::for_query();
        let mut executor = build(&plan, &catalog, &ctx).expect("build cached index merge");
        executor.open().expect("open cached index merge");
        let mut rows = Vec::new();
        loop {
            let mut output = executor.new_chunk();
            executor.next(&mut output).expect("read cached index merge");
            if output.num_rows() == 0 {
                break;
            }
            for row in 0..output.num_rows() {
                rows.push((
                    output.get_row(row).get_int64(0),
                    output.get_row(row).get_int64(1),
                ));
            }
        }
        assert_eq!(rows, vec![(3, 30), (4, 40)]);
    }
}
