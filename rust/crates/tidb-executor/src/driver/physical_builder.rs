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

use crate::access_path::{
    HandleOutputColumn, HandleSourceExec, IndexRangeSourceExec, UniqueIndexPointSourceExec,
};
use crate::executor::{Executor, ExecutorMeta};
use crate::hash_agg::{AggFunc, AggKind, GroupedStreamAggExec, HashAggExec, StreamAggExec};
use crate::index_merge_reader::{
    ExecutorPartialHandleSource, IndexMergeReaderExec, MergeByItem, PartialHandleColumns,
    PartialHandleSource, PushedDownLimit as ExecutorPushedDownLimit,
};
use crate::join::{JoinExec, JoinKind};
use crate::kv_table::{IndexRange, RowDecodeContext, TableHandle, TableScanExec};
use crate::limit::LimitExec;
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
    PhysicalBatchPointGet, PhysicalIndexScan, PhysicalPlan, PhysicalPointGet, PhysicalTableScan,
};

use super::{Catalog, DriverError, SelectMeta, INIT_CAP, MAX_CHUNK_SIZE};

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
    plan.base()
        .base
        .schema()
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
    tidb_expr::simple_expr::resolve_indices_in_place(&mut expression, schema).map_err(|_| {
        DriverError::unsupported("a physical expression does not resolve in its child")
    })?;
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
    let mut full = Vec::with_capacity(table.visible_column_count());
    for (index, column) in table.visible_columns().iter().enumerate() {
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
        meta(plan, schema),
        table.clone(),
        RowDecodeContext::for_query(ctx),
        PushdownStatementContext::from_stmt(ctx),
    );
    if keep.len() != table.visible_column_count()
        || keep.iter().copied().ne(0..table.visible_column_count())
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

fn aggregate_function(
    descriptor: &AggFuncDesc,
    child_schema: &Schema,
    ctx: &crate::StmtContext,
) -> Result<AggFunc, DriverError> {
    let mut args = resolve_expressions(&descriptor.base.args, child_schema)?;
    let upper = descriptor.base.name.to_ascii_uppercase();
    let mut kind = if matches!(upper.as_str(), "FIRST_ROW" | "FIRSTROW") {
        AggKind::FirstRow
    } else {
        super::agg_build::agg_kind_and_type_with_div_precision(
            &upper,
            &args,
            ctx.div_precision_increment(),
        )
        .map_err(|_| {
            DriverError::unsupported(format!("physical aggregate `{upper}` is not executable"))
        })?
        .0
    };
    if descriptor.mode == AggFunctionMode::Final && matches!(kind, AggKind::Count) {
        kind = AggKind::FinalCount;
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
) -> Result<Box<dyn Executor>, DriverError> {
    let mut child = build(only_child(plan)?, catalog, ctx)?;
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
) -> Result<Box<dyn Executor>, DriverError> {
    build(
        embedded.ok_or_else(|| DriverError::unsupported("a physical reader has no pushed plan"))?,
        catalog,
        ctx,
    )
}

fn embedded_index_scan(plan: &PhysicalPlan) -> Option<&PhysicalIndexScan> {
    match plan {
        PhysicalPlan::IndexScan(scan) => Some(scan),
        _ => plan.children().iter().find_map(embedded_index_scan),
    }
}

fn collect_reader_conditions(plan: &PhysicalPlan, conditions: &mut Vec<Expression>) {
    if let PhysicalPlan::Selection(selection) = plan {
        conditions.extend(selection.conditions.iter().cloned());
    }
    for child in plan.children() {
        collect_reader_conditions(child, conditions);
    }
}

fn reader_output_offsets(
    schema: &Schema,
    scan: &PhysicalIndexScan,
    table: &crate::KvTable,
) -> Result<(Vec<usize>, Option<usize>), DriverError> {
    let extra_handle = schema.columns.iter().position(|column| {
        column.id == tidb_model::column::EXTRA_HANDLE_ID
            || column.orig_name.rsplit('.').next().is_some_and(|name| {
                name.eq_ignore_ascii_case(super::leaf_demand::EXTRA_HANDLE_NAME)
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
    let mut conditions = Vec::new();
    collect_reader_conditions(index_plan, &mut conditions);
    if let Some(table_plan) = table_plan {
        collect_reader_conditions(table_plan, &mut conditions);
    }
    if conditions.is_empty() {
        return Ok(Box::new(source));
    }
    let filters = resolve_expressions(&conditions, &schema)?;
    Ok(Box::new(SelectionExec::new(
        meta(plan, schema),
        filters,
        Box::new(source),
        ctx.clone(),
        ctx.statement_memory(),
    )))
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
    let expression = Expression::ScalarFunction(ScalarFunction::new(
        tidb_ast::CiString::new("eq"),
        FieldType::new(FieldTypeCode::Tiny),
        vec![
            Expression::Column(left.clone()),
            Expression::Column(right.clone()),
        ],
    ));
    resolve_expression(expression, schema)
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
) -> Result<JoinExec<crate::StmtContext>, DriverError> {
    let [left_plan, right_plan] = plan.children() else {
        return Err(DriverError::unsupported(
            "a physical join has the wrong child count",
        ));
    };
    let left = filtered_join_child(plan, build(left_plan, catalog, ctx)?, left_conditions, ctx)?;
    let right = filtered_join_child(
        plan,
        build(right_plan, catalog, ctx)?,
        right_conditions,
        ctx,
    )?;
    let condition_schema =
        tidb_expr::schema::merge_schema(Some(left.schema()), Some(right.schema()))
            .ok_or_else(|| DriverError::unsupported("a physical join has no condition schema"))?;
    let mut conditions = left_join_keys
        .iter()
        .zip(right_join_keys)
        .map(|(left, right)| join_equality(left, right, &condition_schema))
        .collect::<Result<Vec<_>, _>>()?;
    conditions.extend(resolve_expressions(other_conditions, &condition_schema)?);
    Ok(JoinExec::new(
        meta(plan, plan_schema(plan)?),
        join_kind(join_type)?,
        conditions,
        left,
        right,
        ctx.clone(),
        ctx.statement_memory(),
    ))
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
                    DriverError::unsupported(
                        "a retained table output is absent from its physical table",
                    )
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
        let executor = build(partial, catalog, ctx)?;
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
    let mut table_conditions = Vec::new();
    collect_reader_conditions(table_plan, &mut table_conditions);
    let table_filters = resolve_expressions(&table_conditions, &schema)?;
    let by_items = reader
        .by_items
        .iter()
        .map(|item| MergeByItem {
            collation: tidb_expr::collation_derive::collation_of_node(&item.expr),
            desc: item.desc,
        })
        .collect();
    let mut executor = IndexMergeReaderExec::new(
        meta(plan, schema),
        table.clone(),
        RowDecodeContext::for_query(ctx),
        partials,
        reader.is_intersection_type,
    )
    .with_output_columns(output_columns)
    .with_table_filters(table_filters, ctx.clone())
    .with_by_items(by_items);
    if let Some(limit) = reader.pushed_limit {
        executor = executor.with_pushed_limit(ExecutorPushedDownLimit {
            offset: limit.offset,
            count: limit.count,
        });
    }
    Ok(Box::new(executor))
}

/// Recursively instantiates one retained physical operator tree.
pub(super) fn build(
    plan: &PhysicalPlan,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    match plan {
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
            build_reader(reader.table_plan.as_deref(), catalog, ctx)
        }
        PhysicalPlan::IndexReader(reader) => {
            build_reader(reader.index_plan.as_deref(), catalog, ctx)
        }
        PhysicalPlan::IndexLookUpReader(reader) => build_index_reader(
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
        ),
        PhysicalPlan::IndexMergeReader(reader) => {
            build_index_merge_reader(plan, reader, catalog, ctx)
        }
        PhysicalPlan::PointGet(point) => build_point_get(plan, point, catalog, ctx),
        PhysicalPlan::BatchPointGet(batch) => build_batch_point_get(plan, batch, catalog, ctx),
        PhysicalPlan::Projection(projection) => {
            let child = build(only_child(plan)?, catalog, ctx)?;
            let expressions = resolve_expressions(&projection.exprs, child.schema())?;
            Ok(Box::new(ProjectionExec::new(
                meta(plan, plan_schema(plan)?),
                expressions,
                child,
                ctx.clone(),
            )))
        }
        PhysicalPlan::Selection(selection) => {
            let mut child = build(only_child(plan)?, catalog, ctx)?;
            let filters = resolve_expressions(&selection.conditions, child.schema())?;
            let pushed = crate::predicate_pushdown::PushedScanFilter::from_physical_conditions(
                filters.clone(),
            );
            if child
                .table_access()
                .is_some_and(|access| access.accept_scan_filter(&pushed, ctx))
            {
                return Ok(child);
            }
            let schema = unary_schema(plan, child.as_ref());
            Ok(Box::new(SelectionExec::new(
                meta(plan, schema),
                filters,
                child,
                ctx.clone(),
                ctx.statement_memory(),
            )))
        }
        PhysicalPlan::Limit(limit) => {
            let child = build(only_child(plan)?, catalog, ctx)?;
            let schema = unary_schema(plan, child.as_ref());
            Ok(Box::new(LimitExec::new(
                meta(plan, schema),
                limit.offset,
                limit.count,
                child,
            )))
        }
        PhysicalPlan::Sort(sort) => {
            let child = build(only_child(plan)?, catalog, ctx)?;
            let by_items = sort
                .by_items
                .iter()
                .map(|item| {
                    child
                        .schema()
                        .columns
                        .iter()
                        .find(|column| column.unique_id == item.col)
                        .cloned()
                        .map(|column| SortByItem {
                            expr: Expression::Column(column),
                            desc: item.desc,
                        })
                        .ok_or_else(|| {
                            DriverError::unsupported(
                                "a physical sort column is absent from its child",
                            )
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
            ))
        }
        PhysicalPlan::TopN(topn) => {
            let child = build(only_child(plan)?, catalog, ctx)?;
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
            ))
        }
        PhysicalPlan::HashAgg(aggregation) => build_aggregation(
            plan,
            &aggregation.agg_funcs,
            &aggregation.group_by_items,
            false,
            catalog,
            ctx,
        ),
        PhysicalPlan::StreamAgg(aggregation) => build_aggregation(
            plan,
            &aggregation.agg_funcs,
            &aggregation.group_by_items,
            true,
            catalog,
            ctx,
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
            )?;
            let build_child_idx = if join.use_outer_to_build {
                1usize.checked_sub(join.inner_child_idx).ok_or_else(|| {
                    DriverError::unsupported("a physical hash join has an invalid inner child")
                })?
            } else {
                join.inner_child_idx
            };
            executor.set_hash_build_is_left(build_child_idx == 0);
            Ok(Box::new(executor))
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
            )?;
            executor.set_merge_plan(crate::merge_join_plan::MergeJoinPlan {
                keys,
                desc: join.desc,
            });
            Ok(Box::new(executor))
        }
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
        ))),
        PhysicalPlan::UnionAll(_) => {
            let children = plan
                .children()
                .iter()
                .map(|child| build(child, catalog, ctx))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Box::new(super::set_opr::UnionAllExec::new(
                meta(plan, plan_schema(plan)?),
                children,
            )))
        }
        PhysicalPlan::MaxOneRow(_) => {
            let child = build(only_child(plan)?, catalog, ctx)?;
            let executor_meta =
                ExecutorMeta::new(plan_schema(plan)?, i64::from(plan.base().base.id()), 2, 2);
            Ok(Box::new(MaxOneRowExec::new(executor_meta, child)))
        }
        PhysicalPlan::NominalSort(_) => build(only_child(plan)?, catalog, ctx),
        _ => Err(DriverError::unsupported(format!(
            "physical executor construction for {} is not implemented",
            plan.base().base.tp()
        ))),
    }
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

/// Builds and drains one fresh or cache-rebuilt physical tree through the
/// same executor path as Go's `ExecStmt.buildExecutor`.
pub(super) fn execute_select(
    select: &tidb_ast::SelectStmt,
    physical: &PhysicalPlan,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    let root = build(physical, catalog, ctx)?;
    let columns = result_columns(select, root.schema());
    super::drain_root_executor(root, columns, ctx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::Datum;
    use tidb_planner::physical::{
        BasePhysicalPlan, PhysicalBatchPointGet, PhysicalIndexMergeReader, PhysicalMaxOneRow,
        PhysicalPointGet, PhysicalSelection, PhysicalTableDual, PhysicalUnionAll,
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
