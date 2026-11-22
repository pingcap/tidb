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

use tidb_datatype::FieldType;
use tidb_driver_error::DriverError;
use tidb_expr::schema::Schema;
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

/// Go `ConstructDAGReq(ctx, plans, kv.TiDB)` plus `buildTableReader`'s
/// `OutputOffsets` (every reader output column) for the plan `table_plan`
/// a TiDB-store reader pushes.
///
/// # Errors
/// A pushed plan or expression that has no protobuf form.
pub fn construct_tidb_dag_req(
    ctx: &crate::StmtContext,
    table_plan: &PhysicalPlan,
) -> Result<tipb::DagRequest, DriverError> {
    let statement = crate::remote_scan::pushdown_statement_context(ctx);
    let plans = tidb_planner_core_operator_physicalop::flatten_list_push_down_plan(table_plan);
    let mut dag = construct_dag_req(ctx, &statement, &plans, tidb_kv::store_type::StoreType::TiDb)
        .map_err(crate::driver::planner_error_to_driver)?;
    dag.output_offsets = (0..table_plan.schema().map_or(0, Schema::len) as u32).collect();
    Ok(dag)
}

/// Go `ctx.GetBuildPBCtx()`.
fn build_pb_ctx<'a>(
    ctx: &'a crate::StmtContext,
    statement: &tidb_distsql::remote_scan::PushdownStatementContext,
) -> tidb_planner_core_operator_physicalop::to_pb::BuildPbContext<'a> {
    tidb_planner_core_operator_physicalop::to_pb::BuildPbContext {
        expr_ctx: ctx,
        tiflash_fast_scan: ctx.tiflash_fast_scan(),
        tiflash_fine_grained_shuffle_batch_size: statement.tiflash_fine_grained_shuffle_batch_size,
        group_concat_max_len: match tidb_exprctx::Columns::sysvar(ctx, None, "group_concat_max_len") {
            Some(tidb_datatype::Datum::UInt(max_len)) => max_len,
            _ => tidb_vardef::defaults::DEF_GROUP_CONCAT_MAX_LEN,
        },
        in_explain_stmt: ctx.in_explain_stmt(),
        use_dynamic_partition_prune: !ctx.static_partition_prune(),
    }
}

/// Go `distsql.SetEncodeType`: chunk RPC when the store supports it, with
/// the system's chunk memory layout.
pub(crate) fn set_encode_type(dag_req: &mut tipb::DagRequest) {
    if tidb_config::tikvcfg::get_global_config().tikv_client.enable_chunk_rpc {
        dag_req.encode_type = Some(tipb::EncodeType::TypeChunk as i32);
        // Go `setChunkMemoryLayout`.
        let endian = match tidb_distsql::system_endian() {
            tidb_distsql::SystemEndian::Little => tipb::Endian::LittleEndian,
            tidb_distsql::SystemEndian::Big => tipb::Endian::BigEndian,
        };
        dag_req.chunk_memory_layout = Some(tipb::ChunkMemoryLayout {
            endian: Some(endian as i32),
        });
    } else {
        dag_req.encode_type = Some(tipb::EncodeType::TypeDefault as i32);
    }
}

/// Go `ConstructDAGReq`: each plan's `ToPB` -- one tree for `kv.TiFlash`
/// (`ConstructTreeBasedDistExec` over `plans[0]`), a list otherwise
/// (`ConstructListBasedDistExec`) -- under the statement's zone, flags and
/// div precision, encoded as `distsql.SetEncodeType` says.
///
/// # Errors
/// A plan or expression with no protobuf form.
pub(crate) fn construct_dag_req(
    ctx: &crate::StmtContext,
    statement: &tidb_distsql::remote_scan::PushdownStatementContext,
    plans: &[&PhysicalPlan],
    store_type: tidb_kv::store_type::StoreType,
) -> Result<tipb::DagRequest, tidb_planner_core_base::PlanError> {
    let (time_zone_name, time_zone_offset) = statement.time_zone.dag_zone();
    let mut dag_req = tipb::DagRequest {
        time_zone_name: Some(time_zone_name),
        time_zone_offset: Some(time_zone_offset),
        // Go `sc.RuntimeStatsColl != nil`.
        collect_execution_summaries: ctx.cop_response_sink().is_some().then_some(true),
        flags: Some(statement.push_down_flags),
        div_precision_increment: (statement.div_precision_increment
            != crate::dag_request::DEFAULT_DIV_PRECISION_INCREMENT)
            .then_some(statement.div_precision_increment),
        ..Default::default()
    };
    let pb_ctx = build_pb_ctx(ctx, statement);
    if store_type == tidb_kv::store_type::StoreType::TiFlash {
        // Go `ConstructTreeBasedDistExec`.
        dag_req.root_executor = Some(plans[0].to_pb(&pb_ctx, store_type)?);
    } else {
        // Go `ConstructListBasedDistExec`.
        dag_req.executors = plans
            .iter()
            .map(|plan| plan.to_pb(&pb_ctx, tidb_kv::store_type::StoreType::TiKv))
            .collect::<Result<_, _>>()?;
    }
    set_encode_type(&mut dag_req);
    Ok(dag_req)
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
            scan.physical_table_id,
            &scan.ranges,
        )
    } else {
        tidb_distsql::table_ranges_to_kv_ranges(scan.physical_table_id, &scan.ranges)
    }
    .map_err(|error| DriverError::unsupported(format!("{error:?}")))?;
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
    let dag = construct_tidb_dag_req(ctx, table_plan)?;
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
    let cop_plan_ids = tidb_planner_core_operator_physicalop::flatten_list_push_down_plan(table_plan)
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
