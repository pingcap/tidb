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
//! and `aggregation.AggFuncToPBExpr`.

use tidb_planner_core_operator_physicalop::PhysicalPlan;
use tidb_proto::tipb;

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
