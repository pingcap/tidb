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

//! Go `pkg/executor/internal/builder/builder_utils.go`'s `ConstructDAGReq`
//! for a reader on the `kv.TiDB` store, with the list-form `ToPB` of each
//! plan the planner pushed into it (`PhysicalTableScan`, `PhysicalSelection`,
//! `PhysicalTopN`, `PhysicalLimit`, `PhysicalHashAgg`, `PhysicalStreamAgg`)
//! and `aggregation.AggFuncToPBExpr`; plus the reader's key ranges (Go
//! `RequestBuilder.SetHandleRanges`).

use tidb_datatype::{FieldType, SessionTimeZone};
use tidb_driver_error::DriverError;
use tidb_expr::expression::Expression;
use tidb_expr::pushdown_catalog::ColumnDescriptor;
use tidb_expr::schema::Schema;
use tidb_expression_aggregation::{AggFuncDesc, AggFunctionMode};
use tidb_planner_core_operator_physicalop::{PhysicalPlan, PhysicalTableScan};
use tidb_proto::{coprocessor, tipb};

/// What a reader on the `kv.TiDB` store sends every TiDB (Go's `kv.Request`
/// for `buildTiDBMemCopTasks`): the DAG and its key ranges, and the types
/// of the reader's output rows.
#[derive(Clone, Debug)]
pub struct TiDBCopRequest {
    /// Go `dagPB`.
    pub dag: tipb::DagRequest,
    /// Go `KeyRanges`.
    pub ranges: Vec<coprocessor::KeyRange>,
    /// The reader's output column types, which decode the responses.
    pub field_types: Vec<FieldType>,
    /// Go `getPhysicalPlanIDs(TablePlans)`: the pushed plans' IDs in DAG
    /// order, under which the select result records each executor.
    pub cop_plan_ids: Vec<isize>,
    /// Go `GetClusterTableCopDestination` of the scanned table: every TiDB,
    /// or the DDL owner alone (`SetTiDBServerID`).
    pub cop_destination: tidb_infoschema::infoschema_meta::ClusterTableCopDestination,
}

fn unsupported(message: impl Into<String>) -> DriverError {
    DriverError::unsupported(message.into())
}

/// Go `flattenPushDownPlan`'s list for a linear cop plan: the scan first,
/// then each executor above it.
fn flatten_push_down_plan(root: &PhysicalPlan) -> Vec<&PhysicalPlan> {
    let mut plans = Vec::new();
    let mut node = Some(root);
    while let Some(plan) = node {
        plans.push(plan);
        node = plan.children().first();
    }
    plans.reverse();
    plans
}

/// The descriptor of one column of a pushed executor's input row, from the
/// column's own type (Go `ExprToPB` writes `ToPBFieldType(col.RetType)`).
fn column_descriptor(schema: &Schema, offset: u32) -> Option<ColumnDescriptor> {
    let field_type = schema.columns.get(offset as usize)?.ret_type.as_ref()?;
    Some(ColumnDescriptor {
        tp: i32::from(field_type.code().mysql_type()),
        flag: field_type.flags(),
        // Go `ToPBFieldType`: `int32(ft.GetFlen())`, wrapping as Go does.
        flen: field_type.flen() as i32,
        decimal: field_type.decimal() as i32,
        charset: field_type.charset_name().to_owned(),
        collation: field_type.collation_name().to_owned(),
        elems: field_type
            .elems_snapshot()
            .into_iter()
            .map(|elem| elem.to_string())
            .collect(),
        array: field_type.is_array(),
    })
}

/// Go `expression.ExpressionsToPBList` over the input row `schema`.
fn expressions_to_pb_list(
    expressions: &[Expression],
    schema: &Schema,
    zone: &SessionTimeZone,
) -> Result<Vec<tipb::Expr>, DriverError> {
    let resolved = crate::driver::physical_builder::resolve_expressions(expressions, schema)?;
    resolved
        .iter()
        .map(|expression| {
            tidb_expr::pushdown_catalog::expression_to_pb_in(
                expression,
                &|offset| column_descriptor(schema, offset),
                zone,
            )
            .ok_or_else(|| unsupported("an expression pushed to TiDB cannot be converted to PB"))
        })
        .collect()
}

/// Go `PhysicalTableScan.ToPB` with `tables.BuildTableScanFromInfos`: the
/// scanned columns as the table declares them, the common handle's column
/// ids, and the scan's order.
fn physical_table_scan_to_pb(
    plan: &PhysicalPlan,
    scan: &PhysicalTableScan,
    is_common_handle: bool,
) -> Result<tipb::Executor, DriverError> {
    let table_columns = tidb_infoschema::infoschema_meta::table_schema(&scan.table_name)
        .ok_or_else(|| unsupported("a TiDB-store scan reads an unregistered table"))?;
    let schema = plan
        .schema()
        .ok_or_else(|| unsupported("a TiDB-store scan has no schema"))?;
    let columns = schema
        .columns
        .iter()
        .map(|column| {
            let offset = usize::try_from(column.id - 1)
                .ok()
                .filter(|offset| *offset < table_columns.len())
                .ok_or_else(|| unsupported("a TiDB-store scan column is absent from its table"))?;
            crate::cop_reader::column_to_proto(&tidb_distsql::remote_scan::PushdownScanColumn {
                id: column.id,
                field_type: table_columns[offset].1.clone(),
                is_handle: false,
                origin_default: None,
            })
            .map(|column| crate::dag_request::column_to_pb(&column))
            .ok_or_else(|| unsupported("unsupported cluster memory-table column"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    // Go `TryGetCommonPkColumnIds`: a common handle's primary-key columns.
    let primary_column_ids = table_columns
        .iter()
        .enumerate()
        .filter(|_| is_common_handle)
        .filter(|(_, (_, field_type))| {
            field_type.has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY)
        })
        .map(|(offset, _)| offset as i64 + 1)
        .collect();
    Ok(tipb::Executor {
        tp: Some(tipb::ExecType::TypeTableScan as i32),
        tbl_scan: Some(tipb::TableScan {
            table_id: Some(scan.table_id),
            columns,
            desc: Some(scan.desc),
            primary_column_ids,
            keep_order: Some(scan.keep_order),
            is_fast_scan: Some(false),
            max_wait_time_ms: Some(0),
            ..Default::default()
        }),
        executor_id: Some(String::new()),
        ..Default::default()
    })
}

/// Go `PhysicalSelection.ToPB`.
fn physical_selection_to_pb(
    conditions: &[Expression],
    input: &Schema,
    zone: &SessionTimeZone,
) -> Result<tipb::Executor, DriverError> {
    Ok(tipb::Executor {
        tp: Some(tipb::ExecType::TypeSelection as i32),
        selection: Some(Box::new(tipb::Selection {
            conditions: expressions_to_pb_list(conditions, input, zone)?,
            ..Default::default()
        })),
        executor_id: Some(String::new()),
        ..Default::default()
    })
}

/// Go `PhysicalLimit.ToPB`: the pushed `Count` (a pushed Limit has no
/// offset of its own).
fn physical_limit_to_pb(count: u64) -> tipb::Executor {
    crate::dag_request::limit_to_pb(count)
}

/// Go `PhysicalTopN.ToPB` with `SortByItemToPB` for each item.
fn physical_top_n_to_pb(
    by_items: &[tidb_expression_aggregation::ByItems],
    count: u64,
    input: &Schema,
    zone: &SessionTimeZone,
) -> Result<tipb::Executor, DriverError> {
    let order_by = by_items
        .iter()
        .map(|item| {
            Ok(tipb::ByItem {
                expr: Some(
                    expressions_to_pb_list(std::slice::from_ref(&item.expr), input, zone)?
                        .remove(0),
                ),
                desc: Some(item.desc),
                ..Default::default()
            })
        })
        .collect::<Result<Vec<_>, DriverError>>()?;
    Ok(tipb::Executor {
        tp: Some(tipb::ExecType::TypeTopN as i32),
        top_n: Some(Box::new(tipb::TopN {
            order_by,
            limit: Some(count),
            ..Default::default()
        })),
        executor_id: Some(String::new()),
        ..Default::default()
    })
}

/// Go `AggFunctionModeToPB`.
fn agg_function_mode_to_pb(mode: AggFunctionMode) -> tipb::AggFunctionMode {
    match mode {
        AggFunctionMode::Complete => tipb::AggFunctionMode::CompleteMode,
        AggFunctionMode::Final => tipb::AggFunctionMode::FinalMode,
        AggFunctionMode::Partial1 => tipb::AggFunctionMode::Partial1Mode,
        AggFunctionMode::Partial2 => tipb::AggFunctionMode::Partial2Mode,
        AggFunctionMode::Dedup => tipb::AggFunctionMode::DedupMode,
    }
}

/// Go `baseFuncDesc.GetTiPBExpr(false)`.
fn get_tipb_expr(name: &str) -> Option<tipb::ExprType> {
    use tidb_expression_aggregation::names;
    use tipb::ExprType;
    Some(match name {
        names::COUNT => ExprType::Count,
        "approx_count_distinct" => ExprType::ApproxCountDistinct,
        names::FIRST_ROW => ExprType::First,
        names::GROUP_CONCAT => ExprType::GroupConcat,
        names::MAX => ExprType::Max,
        names::MIN => ExprType::Min,
        names::SUM => ExprType::Sum,
        names::SUM_INT => ExprType::SumInt,
        names::AVG => ExprType::Avg,
        names::BIT_OR => ExprType::AggBitOr,
        names::BIT_XOR => ExprType::AggBitXor,
        names::BIT_AND => ExprType::AggBitAnd,
        names::VAR_POP => ExprType::VarPop,
        names::JSON_ARRAYAGG => ExprType::JsonArrayAgg,
        "json_objectagg" => ExprType::JsonObjectAgg,
        names::STDDEV_POP => ExprType::StddevPop,
        names::VAR_SAMP => ExprType::VarSamp,
        names::STDDEV_SAMP => ExprType::StddevSamp,
        _ => return None,
    })
}

/// Go `aggregation.AggFuncToPBExpr`.
fn agg_func_to_pb_expr(
    ctx: &crate::StmtContext,
    agg_func: &AggFuncDesc,
    input: &Schema,
    zone: &SessionTimeZone,
) -> Result<tipb::Expr, DriverError> {
    let tp = get_tipb_expr(&agg_func.base.name)
        .ok_or_else(|| unsupported("select request is not supported by client"))?;
    let children = expressions_to_pb_list(&agg_func.base.args, input, zone)?;
    let field_type = tidb_expr::pushdown_catalog::to_pb_field_type(&agg_func.base.ret_type)
        .ok_or_else(|| unsupported("an aggregate's return type cannot be converted to PB"))?;
    let mut expr = tipb::Expr {
        tp: Some(tp as i32),
        children,
        field_type: Some(field_type),
        has_distinct: Some(agg_func.has_distinct),
        agg_func_mode: Some(agg_function_mode_to_pb(agg_func.mode) as i32),
        ..Default::default()
    };
    if tp == tipb::ExprType::GroupConcat {
        expr.order_by = agg_func
            .order_by_items
            .iter()
            .map(|item| {
                Ok(tipb::ByItem {
                    expr: Some(
                        expressions_to_pb_list(std::slice::from_ref(&item.expr), input, zone)?
                            .remove(0),
                    ),
                    desc: Some(item.desc),
                    ..Default::default()
                })
            })
            .collect::<Result<Vec<_>, DriverError>>()?;
        // "encode GroupConcatMaxLen"
        let max_len = match tidb_exprctx::Columns::sysvar(ctx, None, "group_concat_max_len") {
            Some(tidb_datatype::Datum::UInt(max_len)) => max_len,
            _ => 1024,
        };
        let mut val = Vec::new();
        tidb_codec::encode_uint(&mut val, max_len);
        expr.val = Some(val.into());
    }
    Ok(expr)
}

/// Go `PhysicalHashAgg.ToPB` / `PhysicalStreamAgg.ToPB` in the list form.
fn physical_agg_to_pb(
    ctx: &crate::StmtContext,
    agg_funcs: &[AggFuncDesc],
    group_by_items: &[Expression],
    streamed: bool,
    input: &Schema,
    zone: &SessionTimeZone,
) -> Result<tipb::Executor, DriverError> {
    let aggregation = tipb::Aggregation {
        group_by: expressions_to_pb_list(group_by_items, input, zone)?,
        agg_func: agg_funcs
            .iter()
            .map(|agg_func| agg_func_to_pb_expr(ctx, agg_func, input, zone))
            .collect::<Result<Vec<_>, _>>()?,
        ..Default::default()
    };
    Ok(tipb::Executor {
        tp: Some(if streamed {
            tipb::ExecType::TypeStreamAgg
        } else {
            tipb::ExecType::TypeAggregation
        } as i32),
        aggregation: Some(Box::new(aggregation)),
        executor_id: Some(String::new()),
        ..Default::default()
    })
}

/// Go `PhysicalProjection.ToPB` (`ProjectionExpressionsToPBList`), whose
/// executor also carries its child's (`projExec.Child`) and its explain ID.
fn physical_projection_to_pb(
    plan: &PhysicalPlan,
    exprs: &[Expression],
    input: &Schema,
    child: Option<&tipb::Executor>,
    zone: &SessionTimeZone,
) -> Result<tipb::Executor, DriverError> {
    Ok(tipb::Executor {
        tp: Some(tipb::ExecType::TypeProjection as i32),
        projection: Some(Box::new(tipb::Projection {
            exprs: expressions_to_pb_list(exprs, input, zone)?,
            child: child.cloned().map(Box::new),
            ..Default::default()
        })),
        executor_id: Some(format!("{}_{}", plan.tp(), plan.id())),
        ..Default::default()
    })
}

/// One pushed plan's `ToPB`; `input` is its child's output schema and
/// `child` its child's executor.
fn to_pb(
    ctx: &crate::StmtContext,
    is_common_handle: bool,
    plan: &PhysicalPlan,
    input: Option<&Schema>,
    child: Option<&tipb::Executor>,
    zone: &SessionTimeZone,
) -> Result<tipb::Executor, DriverError> {
    let input = || input.ok_or_else(|| unsupported("a pushed executor has no input"));
    match plan {
        PhysicalPlan::Projection(projection) => {
            physical_projection_to_pb(plan, &projection.exprs, input()?, child, zone)
        }
        PhysicalPlan::TableScan(scan) => physical_table_scan_to_pb(plan, scan, is_common_handle),
        PhysicalPlan::Selection(selection) => {
            physical_selection_to_pb(&selection.conditions, input()?, zone)
        }
        PhysicalPlan::Limit(limit) => Ok(physical_limit_to_pb(limit.count)),
        PhysicalPlan::TopN(top_n) => {
            physical_top_n_to_pb(&top_n.by_items, top_n.count, input()?, zone)
        }
        PhysicalPlan::HashAgg(agg) => physical_agg_to_pb(
            ctx,
            &agg.agg_funcs,
            &agg.group_by_items,
            false,
            input()?,
            zone,
        ),
        PhysicalPlan::StreamAgg(agg) => physical_agg_to_pb(
            ctx,
            &agg.agg_funcs,
            &agg.group_by_items,
            true,
            input()?,
            zone,
        ),
        _ => Err(unsupported(format!(
            "a {} pushed to TiDB has no list-form ToPB",
            plan.tp()
        ))),
    }
}

/// Go `ConstructListBasedDistExec`.
fn construct_list_based_dist_exec(
    ctx: &crate::StmtContext,
    is_common_handle: bool,
    plans: &[&PhysicalPlan],
    zone: &SessionTimeZone,
) -> Result<Vec<tipb::Executor>, DriverError> {
    let mut executors = Vec::with_capacity(plans.len());
    let mut input: Option<&Schema> = None;
    for plan in plans {
        let executor = to_pb(ctx, is_common_handle, plan, input, executors.last(), zone)?;
        executors.push(executor);
        input = plan.schema();
    }
    Ok(executors)
}

/// Go `ConstructDAGReq(ctx, plans, kv.TiDB)` plus `buildTableReader`'s
/// `OutputOffsets` (every reader output column) for the plan `table_plan`
/// a TiDB-store reader pushes.
///
/// # Errors
/// A pushed plan or expression that has no protobuf form.
pub fn construct_dag_req(
    ctx: &crate::StmtContext,
    table_plan: &PhysicalPlan,
    is_common_handle: bool,
) -> Result<tipb::DagRequest, DriverError> {
    let zone = ctx.session_zone();
    let (time_zone_name, time_zone_offset) = zone.dag_zone();
    let plans = flatten_push_down_plan(table_plan);
    let executors = construct_list_based_dist_exec(ctx, is_common_handle, &plans, &zone)?;
    let div_precision_increment = ctx.div_precision_increment();
    // Go `distsql.SetEncodeType`: chunk RPC when the store supports it.
    let (encode_type, chunk_memory_layout) = if tidb_config::tikvcfg::get_global_config()
        .tikv_client
        .enable_chunk_rpc
    {
        let endian = match tidb_distsql::system_endian() {
            tidb_distsql::SystemEndian::Little => tipb::Endian::LittleEndian,
            tidb_distsql::SystemEndian::Big => tipb::Endian::BigEndian,
        };
        (
            tipb::EncodeType::TypeChunk,
            Some(tipb::ChunkMemoryLayout {
                endian: Some(endian as i32),
            }),
        )
    } else {
        (tipb::EncodeType::TypeDefault, None)
    };
    let width = table_plan.schema().map_or(0, Schema::len);
    Ok(tipb::DagRequest {
        executors,
        time_zone_name: Some(time_zone_name),
        time_zone_offset: Some(time_zone_offset),
        flags: Some(ctx.push_down_flags()),
        // Go `sc.RuntimeStatsColl != nil`.
        collect_execution_summaries: ctx.cop_response_sink().is_some().then_some(true),
        div_precision_increment: (div_precision_increment
            != crate::dag_request::DEFAULT_DIV_PRECISION_INCREMENT)
            .then_some(div_precision_increment),
        encode_type: Some(encode_type as i32),
        chunk_memory_layout,
        output_offsets: (0..width as u32).collect(),
        ..Default::default()
    })
}

/// Go `RequestBuilder.SetHandleRanges(dctx, tableID, isCommonHandle,
/// ranges)` for a TiDB-store scan: int-handle ranges through
/// `TableHandleRangesToKVRanges`, common-handle ones through
/// `CommonHandleRangesToKVRanges` in the statement's location.
///
/// # Errors
/// A range bound that does not encode.
pub fn set_handle_ranges(
    ctx: &crate::StmtContext,
    scan: &PhysicalTableScan,
    is_common_handle: bool,
) -> Result<Vec<coprocessor::KeyRange>, DriverError> {
    let ranges = if is_common_handle {
        tidb_distsql::common_handle_ranges_to_kv_ranges(
            Some(&ctx.session_zone()),
            scan.table_id,
            &scan.ranges,
        )
    } else {
        tidb_distsql::table_ranges_to_kv_ranges(scan.table_id, &scan.ranges)
    }
    .map_err(|error| unsupported(format!("{error:?}")))?;
    Ok(ranges
        .into_iter()
        .map(|range| coprocessor::KeyRange {
            start: range.start_key.into(),
            end: range.end_key.into(),
        })
        .collect())
}

/// Go `buildTableReader` for a reader on the `kv.TiDB` store: its DAG, key
/// ranges and output types.
///
/// # Errors
/// The pushed plan has no protobuf form or its ranges do not encode.
pub fn build_tidb_cop_request(
    ctx: &crate::StmtContext,
    reader: &PhysicalPlan,
    table_plan: &PhysicalPlan,
    scan: &PhysicalTableScan,
    is_common_handle: bool,
) -> Result<TiDBCopRequest, DriverError> {
    let dag = construct_dag_req(ctx, table_plan, is_common_handle)?;
    let ranges = set_handle_ranges(ctx, scan, is_common_handle)?;
    let field_types = reader
        .schema()
        .map(|schema| {
            schema
                .columns
                .iter()
                .map(|column| {
                    column
                        .ret_type
                        .clone()
                        .unwrap_or_else(|| FieldType::new(tidb_datatype::FieldTypeCode::Null))
                })
                .collect()
        })
        .unwrap_or_default();
    let cop_plan_ids = flatten_push_down_plan(table_plan)
        .iter()
        .map(|plan| isize::try_from(plan.id()).unwrap_or(0))
        .collect();
    Ok(TiDBCopRequest {
        dag,
        ranges,
        field_types,
        cop_plan_ids,
        cop_destination: tidb_infoschema::infoschema_meta::get_cluster_table_cop_destination(
            &scan.table_name,
        ),
    })
}
