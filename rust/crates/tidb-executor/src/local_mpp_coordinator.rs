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

//! Go `pkg/executor/internal/mpp/local_mpp_coordinator.go`: the coordinator
//! of one MPP gather. It cuts the reader's plan into fragments
//! (`GenerateRootMPPTasks`), dispatches every fragment's tasks, and receives
//! the root tasks' packets; and `executor/internal/util/partition_table.go`'s
//! `UpdateExecutorTableID`, which points each task's DAG at its table.
//!
//! # Narrowings, by name
//!
//! * `mppcoordmanager` (the coordinator registry TiFlash reports task status
//!   to): not ported, so the coordinator address is the one Go's manager has
//!   before the server registers its address, `""`, and no execution summary
//!   is reported.
//! * `taskZoneInfoHelper`: the stores carry no zone labels here, so every
//!   exchange's same-zone flags stay unset, as Go leaves them for stores
//!   without zones.
//! * `CheckVisibility` (the GC safe point check after each packet): the MPP
//!   client has no safe point to read.
//! * The dispatch meta's `SqlDigest` / `PlanDigest`: the statement context
//!   carries no digest here, so they are sent empty.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender};
use std::sync::Arc;
use std::time::Duration;

use prost::Message;
use tidb_distsql::remote_scan::PushdownStatementContext;
use tidb_kv::store_type::StoreType;
use tidb_planner_core_base::PlanError;
use tidb_planner_core_operator_physicalop::fragment::{
    generate_root_mpp_tasks, Fragment, MppTaskGeneratorContext,
};
use tidb_planner_core_operator_physicalop::to_pb::BuildPbContext;
use tidb_planner_core_operator_physicalop::{MppTask, PhysicalPlan, PhysicalTableScan};
use tidb_proto::mpp::TaskMeta;
use tidb_proto::tipb::{self, DagRequest, EncodeType, ExecType};
use tidb_txnkv::{
    KeyRange, MppBuildTasksRequest, MppClient, MppContext, MppError, MppQueryId, MppStoreTask,
    MppTaskLocation, MppVersion, QueryResponseError, QueryResultSubset,
};

use crate::dag_request::DEFAULT_DIV_PRECISION_INCREMENT;
use crate::mpp_coordinator::MppGatherResponse;
use crate::StatementMemory;

/// Go `UpdateExecutorTableID`: points the table (or partition) scan of
/// `exec`'s tree at `partition_ids`, following each executor's child when
/// `recursive`.
///
/// # Errors
/// An executor of a type Go does not know.
pub fn update_executor_table_id(
    exec: Option<&mut tipb::Executor>,
    recursive: bool,
    partition_ids: &[i64],
) -> Result<(), String> {
    let Some(exec) = exec else {
        return Ok(());
    };
    let tp = exec.tp.unwrap_or_default();
    let child: Option<&mut tipb::Executor> = match ExecType::try_from(tp) {
        Ok(ExecType::TypeTableScan) => {
            if let Some(scan) = exec.tbl_scan.as_mut() {
                scan.table_id = Some(partition_ids[0]);
            }
            None
        }
        Ok(ExecType::TypePartitionTableScan) => {
            if let Some(scan) = exec.partition_table_scan.as_mut() {
                scan.partition_ids = partition_ids.to_vec();
            }
            None
        }
        Ok(ExecType::TypeIndexScan) => {
            if let Some(scan) = exec.idx_scan.as_mut() {
                scan.table_id = Some(partition_ids[0]);
            }
            None
        }
        Ok(ExecType::TypeSelection) => exec.selection.as_mut().and_then(|e| e.child.as_deref_mut()),
        Ok(ExecType::TypeAggregation | ExecType::TypeStreamAgg) => {
            exec.aggregation.as_mut().and_then(|e| e.child.as_deref_mut())
        }
        Ok(ExecType::TypeTopN) => exec.top_n.as_mut().and_then(|e| e.child.as_deref_mut()),
        Ok(ExecType::TypeLimit) => exec.limit.as_mut().and_then(|e| e.child.as_deref_mut()),
        Ok(ExecType::TypeExchangeSender) => {
            exec.exchange_sender.as_mut().and_then(|e| e.child.as_deref_mut())
        }
        Ok(ExecType::TypeExchangeReceiver | ExecType::TypeCteSource) => None,
        Ok(ExecType::TypeCteSink) => exec.cte_sink.as_mut().and_then(|e| e.child.as_deref_mut()),
        Ok(ExecType::TypeJoin) => exec.join.as_mut().and_then(|join| {
            let probe = 1 - join.inner_idx.unwrap_or_default() as usize;
            join.children.get_mut(probe)
        }),
        Ok(ExecType::TypeProjection) => exec.projection.as_mut().and_then(|e| e.child.as_deref_mut()),
        Ok(ExecType::TypeWindow) => exec.window.as_mut().and_then(|e| e.child.as_deref_mut()),
        Ok(ExecType::TypeSort) => exec.sort.as_mut().and_then(|e| e.child.as_deref_mut()),
        Ok(ExecType::TypeExpand) => exec.expand.as_mut().and_then(|e| e.child.as_deref_mut()),
        Ok(ExecType::TypeExpand2) => exec.expand2.as_mut().and_then(|e| e.child.as_deref_mut()),
        _ => return Err(format!("unknown new tipb protocol {tp}")),
    };
    if child.is_some() && recursive {
        return update_executor_table_id(child, recursive, partition_ids);
    }
    Ok(())
}

/// What Go's `mppTaskGenerator` reads off the session: the statement's
/// context and the store's `kv.MPPClient`.
struct GeneratorContext<'a> {
    ctx: &'a crate::StmtContext,
    catalog: &'a tidb_infoschema::SessionExtendedInfoSchema,
    statement: &'a PushdownStatementContext,
    client: &'a Arc<dyn MppClient>,
    mpp_ctx: &'a Arc<dyn MppContext>,
    start_ts: u64,
}

impl MppTaskGeneratorContext for GeneratorContext<'_> {
    fn choose_mpp_version(&self) -> MppVersion {
        self.ctx.mpp_version()
    }

    fn connection_id(&self) -> u64 {
        tidb_exprctx::Columns::connection_id(self.ctx).unwrap_or_default()
    }

    fn session_alias(&self) -> String {
        // The statement's `tidb_session_alias`, as the DDL trace takes it.
        self.ctx.ddl_session_alias().to_owned()
    }

    fn alloc_mpp_task_id(&self) -> i64 {
        self.statement.mpp_query_info.alloc_task_id()
    }

    fn use_dynamic_partition_prune(&self) -> bool {
        !self.ctx.static_partition_prune()
    }

    fn table_handle_ranges_to_kv_ranges(
        &self,
        tid: i64,
        is_common_handle: bool,
        ranges: &[tidb_ranger::types::Range],
    ) -> Result<Vec<KeyRange>, PlanError> {
        let ranges = if is_common_handle {
            tidb_distsql::common_handle_ranges_to_kv_ranges(
                Some(&self.statement.time_zone),
                tid,
                ranges,
            )
        } else {
            tidb_distsql::table_ranges_to_kv_ranges(tid, ranges)
        };
        ranges.map_err(|err| PlanError::internal(format!("{err:?}")))
    }

    /// Go `partitionPruning` (`fragment.go`): the partitions
    /// `partitionpruning.PartitionPruning` keeps; when none survive, TiFlash
    /// still gets one to read (the first, or the first named).
    fn partition_pruning(&self, ts: &PhysicalTableScan) -> Result<Vec<i64>, PlanError> {
        let info = ts
            .plan_part_info
            .as_ref()
            .ok_or_else(|| PlanError::internal("a partitioned MPP scan has no partition info"))?;
        let table = ts
            .table
            .as_ref()
            .ok_or_else(|| PlanError::internal("an MPP scan has no table"))?;
        let table = table.read();
        let pi = table
            .get_partition_info()
            .ok_or_else(|| PlanError::internal("an MPP partition scan's table is unpartitioned"))?;
        let partition_names: Vec<String> =
            info.partition_names.iter().map(|name| name.original.clone()).collect();
        let idx_arr = crate::driver::planner_bridge::partition_pruning(
            self.catalog,
            table.id,
            &info.pruning_conds,
            &partition_names,
            &info.columns,
            self.ctx,
        )?;
        let definitions = pi.read().definitions.snapshot();
        let mut ret: Vec<i64> = idx_arr.iter().map(|&idx| definitions[idx].id).collect();
        if ret.is_empty() {
            // TiFlash cannot process an empty task correctly, so choose to
            // leave it with some data to read.
            if partition_names.is_empty() {
                ret = vec![definitions[0].id];
            } else if let Some(def) = definitions
                .iter()
                .find(|def| def.name.lowercase() == partition_names[0].to_lowercase())
            {
                ret = vec![def.id];
            }
        }
        Ok(ret)
    }

    fn resolve_correlated_columns(
        &self,
        ts: &PhysicalTableScan,
    ) -> Result<Vec<tidb_ranger::types::Range>, PlanError> {
        let rebuild = crate::driver::physical_builder::correlated_table_range_rebuilder(ts, self.ctx)
            .ok_or_else(|| PlanError::internal("an MPP scan's correlated access conditions have no range rebuild"))?;
        rebuild().map_err(|err| PlanError::internal(format!("{err:?}")))
    }

    fn construct_mpp_tasks(
        &self,
        req: &MppBuildTasksRequest,
    ) -> Result<Vec<Arc<dyn MppTaskLocation>>, PlanError> {
        let kv_context = self
            .statement
            .kv_context
            .as_ref()
            .map(tidb_txnkv::StatementKvContextCell::get)
            .unwrap_or_default()
            // Go `MPPClient.ConstructMPPTasks`: `ctx = context.WithValue(ctx,
            // tikv.TxnStartKey(), req.StartTS)`.
            .with_txn_start_ts(self.start_ts);
        let policy = tidb_tiflashcompute::DispatchPolicy::parse(
            &self.statement.tiflash_compute_dispatch_policy,
        )
        .map_err(PlanError::internal)?;
        let tasks = self
            .client
            .construct_mpp_tasks(
                self.mpp_ctx,
                req,
                &kv_context,
                policy,
                self.statement.tiflash_replica_read,
                &mut |warning| self.statement.warnings.append_warning(warning),
            )
            .map_err(|err| PlanError::internal(err.to_string()))?;
        Ok(tasks
            .into_iter()
            .map(|task| Arc::new(task) as Arc<dyn MppTaskLocation>)
            .collect())
    }
}

/// Go `kv.MPPDispatchRequest`.
struct MppDispatchRequest {
    data: Vec<u8>,
    task: MppTask,
    is_root: bool,
    meta: TaskMeta,
}

/// Go `mppResponse` as `respChan` carries it.
type MppResponse = Result<QueryResultSubset, QueryResponseError>;

/// Go `localMppCoordinator`.
pub struct LocalMppCoordinator {
    /// Go `respChan`; `None` once closed.
    resp_rx: Option<Receiver<MppResponse>>,
    /// Go `closed` and `finishCh`.
    closed: Arc<AtomicBool>,
    /// Go `wgDoneChan`: the dispatching thread.
    dispatch_all: Option<std::thread::JoinHandle<()>>,
    node_cnt: usize,
    memory: StatementMemory,
}

/// The coordinator's context: the statement's, cancelled once the
/// coordinator closes (Go `ctxChild, c.cancelFunc = context.WithCancel(ctx)`).
struct CoordinatorContext {
    memory: StatementMemory,
    closed: Arc<AtomicBool>,
}

impl MppContext for CoordinatorContext {
    fn check(&self) -> Result<(), tidb_terror::MysqlError> {
        tidb_txnkv::MppContext::check(&self.memory)?;
        if self.closed.load(Ordering::Acquire) {
            return Err(tidb_terror::MysqlError::from_terror(
                &tidb_exeerrors::ERR_QUERY_INTERRUPTED.gen_with_stack_by_args(&[]),
            ));
        }
        Ok(())
    }

    fn consume(&self, bytes: i64) {
        tidb_txnkv::MppContext::consume(&self.memory, bytes);
    }
}

impl LocalMppCoordinator {
    /// Go `NewLocalMPPCoordinator` and `Execute`: the fragments of `sender`
    /// (the reader's plan), every task dispatched on its own thread, and the
    /// key ranges the gather reads. The coordinator answers the root tasks'
    /// packets.
    ///
    /// # Errors
    /// The fragments, their tasks or their DAGs could not be built.
    pub fn execute(
        ctx: &crate::StmtContext,
        catalog: &tidb_infoschema::SessionExtendedInfoSchema,
        client: &Arc<dyn MppClient>,
        sender: &PhysicalPlan,
        start_ts: u64,
        mpp_query_id: MppQueryId,
        gather_id: u64,
    ) -> Result<(Self, Vec<KeyRange>), String> {
        let statement = crate::remote_scan::pushdown_statement_context(ctx);
        let closed = Arc::new(AtomicBool::new(false));
        let mpp_ctx: Arc<dyn MppContext> = Arc::new(CoordinatorContext {
            memory: statement.memory.clone(),
            closed: Arc::clone(&closed),
        });
        let gen_ctx = GeneratorContext {
            ctx,
            catalog,
            statement: &statement,
            client,
            mpp_ctx: &mpp_ctx,
            start_ts,
        };
        let (frags, kv_ranges, node_info) =
            generate_root_mpp_tasks(&gen_ctx, start_ts, gather_id, mpp_query_id, sender)
                .map_err(|err| err.to_string())?;
        let mut mpp_reqs = Vec::new();
        for frag in &frags {
            append_mpp_dispatch_req(ctx, &statement, frag, gather_id, &mut mpp_reqs)?;
        }

        let (resp_tx, resp_rx) = std::sync::mpsc::sync_channel(0);
        let dispatch_all = {
            let client = Arc::clone(client);
            let closed = Arc::clone(&closed);
            let kv_context = statement
                .kv_context
                .as_ref()
                .map(tidb_txnkv::StatementKvContextCell::get)
                .unwrap_or_default();
            let rpc_interceptor = statement.rpc_interceptor.clone();
            std::thread::spawn(move || {
                dispatch_all(&client, &mpp_ctx, &kv_context, &rpc_interceptor, mpp_reqs, &closed, &resp_tx);
            })
        };
        Ok((
            Self {
                resp_rx: Some(resp_rx),
                closed,
                dispatch_all: Some(dispatch_all),
                node_cnt: node_info.len(),
                memory: statement.memory,
            },
            kv_ranges,
        ))
    }

    /// Go `closeWithoutReport`: cancels the tasks' context and waits for
    /// every dispatching thread.
    fn close_without_report(&mut self) {
        self.closed.store(true, Ordering::Release);
        self.resp_rx = None;
        if let Some(dispatch_all) = self.dispatch_all.take() {
            let _ = dispatch_all.join();
        }
    }
}

/// Go `appendMPPDispatchReq`: the fragment's DAG (`ConstructDAGReq` over its
/// sink, every sink column as an output, TypeChunk for the root fragment and
/// TypeCHBlock for the others), pointed at each task's table.
fn append_mpp_dispatch_req(
    ctx: &crate::StmtContext,
    statement: &PushdownStatementContext,
    pf: &Fragment,
    gather_id: u64,
    mpp_reqs: &mut Vec<MppDispatchRequest>,
) -> Result<(), String> {
    let sink = pf
        .sink
        .as_ref()
        .ok_or_else(|| "the fragment has no sink".to_owned())?;
    let mut dag_req = construct_dag_req(ctx, statement, sink)?;
    for i in 0..sink.schema().map_or(0, |schema| schema.columns.len()) {
        dag_req.output_offsets.push(i as u32);
    }
    dag_req.encode_type = Some(if pf.is_root {
        EncodeType::TypeChunk
    } else {
        EncodeType::TypeChBlock
    } as i32);
    let PhysicalPlan::ExchangeSender(sender) = sink else {
        return Err("the fragment's sink is not an exchange sender".to_owned());
    };
    let rg_name = if tidb_vardef::ENABLE_RESOURCE_CONTROL.load(Ordering::SeqCst) {
        statement.resource_group_name.clone()
    } else {
        String::new()
    };
    for mpp_task in &sender.tasks {
        if !mpp_task.partition_table_ids.is_empty() {
            update_executor_table_id(dag_req.root_executor.as_mut(), true, &mpp_task.partition_table_ids)?;
        } else if !mpp_task.tiflash_static_prune {
            // If isDisaggregatedTiFlashStaticPrune is true, it means this
            // TableScan is under PartitionUnion, tableID in TableScan is
            // already the physical table id of this partition, no need to
            // update again.
            update_executor_table_id(dag_req.root_executor.as_mut(), true, &[mpp_task.table_id])?;
        }
        let pb_data = dag_req.encode_to_vec();
        // Go `MPPClient.DispatchMPPTask`'s task meta.
        let meta = TaskMeta {
            start_ts: mpp_task.start_ts,
            query_ts: mpp_task.query_id.query_ts,
            local_query_id: mpp_task.query_id.local_query_id,
            task_id: mpp_task.id,
            server_id: mpp_task.query_id.server_id,
            gather_id,
            address: mpp_task.location.address().to_owned(),
            coordinator_address: String::new(),
            report_execution_summary: false,
            mpp_version: mpp_task.version.as_i64(),
            resource_group_name: rg_name.clone(),
            connection_id: mpp_task.session_id,
            connection_alias: mpp_task.session_alias.clone(),
            sql_digest: String::new(),
            plan_digest: String::new(),
            ..TaskMeta::default()
        };
        mpp_reqs.push(MppDispatchRequest {
            data: pb_data,
            task: Arc::clone(mpp_task),
            is_root: pf.is_root,
            meta,
        });
    }
    Ok(())
}

/// Go `ConstructDAGReq(ctx, []PhysicalPlan{sink}, kv.TiFlash)`.
fn construct_dag_req(
    ctx: &crate::StmtContext,
    statement: &PushdownStatementContext,
    sink: &PhysicalPlan,
) -> Result<DagRequest, String> {
    let (time_zone_name, time_zone_offset) = statement.time_zone.dag_zone();
    let pb_ctx = BuildPbContext {
        expr_ctx: ctx,
        tiflash_fast_scan: ctx.tiflash_fast_scan(),
        tiflash_fine_grained_shuffle_batch_size: statement.tiflash_fine_grained_shuffle_batch_size,
        group_concat_max_len: match tidb_exprctx::Columns::sysvar(ctx, None, "group_concat_max_len") {
            Some(tidb_datatype::Datum::UInt(max_len)) => max_len,
            _ => tidb_vardef::defaults::DEF_GROUP_CONCAT_MAX_LEN,
        },
        in_explain_stmt: ctx.in_explain_stmt(),
        use_dynamic_partition_prune: !ctx.static_partition_prune(),
    };
    // Go `ConstructTreeBasedDistExec`.
    let root_executor = sink.to_pb(&pb_ctx, StoreType::TiFlash).map_err(|err| err.to_string())?;
    let endian = if tidb_distsql::system_endian() == tidb_distsql::SystemEndian::Little {
        tipb::Endian::LittleEndian
    } else {
        tipb::Endian::BigEndian
    };
    Ok(DagRequest {
        time_zone_name: Some(time_zone_name),
        time_zone_offset: Some(time_zone_offset),
        flags: Some(statement.push_down_flags),
        collect_execution_summaries: ctx.cop_response_sink().is_some().then_some(true),
        div_precision_increment: (statement.div_precision_increment != DEFAULT_DIV_PRECISION_INCREMENT)
            .then_some(statement.div_precision_increment),
        root_executor: Some(root_executor),
        chunk_memory_layout: Some(tipb::ChunkMemoryLayout {
            endian: Some(endian as i32),
        }),
        ..DagRequest::default()
    })
}

/// Go `dispatchAll`: every task's `handleDispatchReq` on its own thread;
/// `respChan` closes once they all end.
fn dispatch_all(
    client: &Arc<dyn MppClient>,
    mpp_ctx: &Arc<dyn MppContext>,
    kv_context: &tidb_txnkv::StatementKvContext,
    rpc_interceptor: &tidb_txnkv::RpcInterceptorOption,
    mpp_reqs: Vec<MppDispatchRequest>,
    closed: &AtomicBool,
    resp_tx: &SyncSender<MppResponse>,
) {
    std::thread::scope(|scope| {
        for task in mpp_reqs {
            if closed.load(Ordering::Acquire) {
                break;
            }
            scope.spawn(move || {
                handle_dispatch_req(client, mpp_ctx, kv_context, rpc_interceptor, task, resp_tx);
            });
        }
    });
}

/// Go `handleDispatchReq`: dispatches the task; a root task's packets are
/// then received (`receiveResults`).
fn handle_dispatch_req(
    client: &Arc<dyn MppClient>,
    mpp_ctx: &Arc<dyn MppContext>,
    kv_context: &tidb_txnkv::StatementKvContext,
    rpc_interceptor: &tidb_txnkv::RpcInterceptorOption,
    req: MppDispatchRequest,
    resp_tx: &SyncSender<MppResponse>,
) {
    // Go `DispatchMPPTask`: `originalTask, ok := req.Meta.(*batchCopTask)`;
    // a task placed by address alone carries no regions.
    let store_task = req
        .task
        .location
        .as_any()
        .downcast_ref::<MppStoreTask>()
        .cloned()
        .unwrap_or_else(|| MppStoreTask {
            address: req.meta.address.clone(),
            regions: Vec::new(),
            partition_table_regions: Vec::new(),
        });
    if let Err(err) = client.dispatch_mpp_task(
        mpp_ctx,
        store_task,
        req.meta.clone(),
        req.data,
        kv_context,
        rpc_interceptor,
    ) {
        send_error(resp_tx, &err);
        return;
    }
    if !req.is_root {
        return;
    }
    // Only root task should establish a stream conn with tiFlash to receive
    // result.
    let task_meta = TaskMeta {
        start_ts: req.meta.start_ts,
        gather_id: req.meta.gather_id,
        query_ts: req.meta.query_ts,
        local_query_id: req.meta.local_query_id,
        task_id: req.meta.task_id,
        server_id: req.meta.server_id,
        address: req.meta.address.clone(),
        mpp_version: req.meta.mpp_version,
        resource_group_name: req.meta.resource_group_name.clone(),
        sql_digest: req.meta.sql_digest.clone(),
        plan_digest: req.meta.plan_digest.clone(),
        ..TaskMeta::default()
    };
    receive_results(client, mpp_ctx, kv_context, rpc_interceptor, &req.meta, task_meta, resp_tx);
}

/// Go `receiveResults`.
fn receive_results(
    client: &Arc<dyn MppClient>,
    mpp_ctx: &Arc<dyn MppContext>,
    kv_context: &tidb_txnkv::StatementKvContext,
    rpc_interceptor: &tidb_txnkv::RpcInterceptorOption,
    req_meta: &TaskMeta,
    task_meta: TaskMeta,
    resp_tx: &SyncSender<MppResponse>,
) {
    // Go `EstablishMPPConns`' receiver: the TiDB side, task -1.
    let receiver = TaskMeta {
        start_ts: req_meta.start_ts,
        gather_id: req_meta.gather_id,
        query_ts: req_meta.query_ts,
        local_query_id: req_meta.local_query_id,
        server_id: req_meta.server_id,
        mpp_version: req_meta.mpp_version,
        task_id: -1,
        resource_group_name: req_meta.resource_group_name.clone(),
        ..TaskMeta::default()
    };
    let mut stream = match client.establish_mpp_conns(
        Arc::clone(mpp_ctx),
        &req_meta.address,
        task_meta,
        receiver,
        kv_context,
        rpc_interceptor,
    ) {
        Ok(stream) => stream,
        Err(err) => {
            send_error(resp_tx, &err);
            return;
        }
    };
    loop {
        match stream.next() {
            Ok(Some(resp)) => {
                // The gather charges what it holds.
                stream.release_packet_memory();
                if resp_tx.send(Ok(resp)).is_err() {
                    break;
                }
            }
            Ok(None) => break,
            Err(err) => {
                let _ = resp_tx.send(Err(err));
                break;
            }
        }
    }
    stream.close();
}

/// Go `sendError`.
fn send_error(resp_tx: &SyncSender<MppResponse>, err: &MppError) {
    let err = match err {
        MppError::Sql(err) => QueryResponseError::Sql {
            code: err.code,
            message: err.message.clone(),
        },
        MppError::Other(message) => QueryResponseError::Source(message.clone()),
    };
    let _ = resp_tx.send(Err(err));
}

impl tidb_txnkv::Response for LocalMppCoordinator {
    /// Go `Next` / `nextImpl`: the next root packet, checking the
    /// statement's kill every three seconds while waiting.
    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        let Some(resp_rx) = self.resp_rx.as_ref() else {
            return Ok(None);
        };
        loop {
            match resp_rx.recv_timeout(Duration::from_secs(3)) {
                Ok(resp) => return resp.map(Some),
                Err(RecvTimeoutError::Disconnected) => return Ok(None),
                Err(RecvTimeoutError::Timeout) => {
                    if let Err(err) = self.memory.check() {
                        let err = err.to_mysql_error();
                        return Err(QueryResponseError::Sql {
                            code: err.code,
                            message: err.message,
                        });
                    }
                }
            }
        }
    }

    /// Go `Close`.
    fn close(&mut self) {
        self.close_without_report();
    }
}

impl MppGatherResponse for LocalMppCoordinator {
    fn node_count(&self) -> usize {
        self.node_cnt
    }
}

impl Drop for LocalMppCoordinator {
    fn drop(&mut self) {
        self.close_without_report();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scan(table_id: i64) -> tipb::Executor {
        tipb::Executor {
            tp: Some(ExecType::TypeTableScan as i32),
            tbl_scan: Some(tipb::TableScan {
                table_id: Some(table_id),
                ..tipb::TableScan::default()
            }),
            ..tipb::Executor::default()
        }
    }

    fn receiver() -> tipb::Executor {
        tipb::Executor {
            tp: Some(ExecType::TypeExchangeReceiver as i32),
            exchange_receiver: Some(tipb::ExchangeReceiver::default()),
            ..tipb::Executor::default()
        }
    }

    /// Go `UpdateExecutorTableID`: the walk follows each executor's child,
    /// a join's probe side (`Children[1-InnerIdx]`), down to the scan.
    #[test]
    fn update_executor_table_id_follows_the_probe_side_to_the_scan() {
        let join = tipb::Executor {
            tp: Some(ExecType::TypeJoin as i32),
            join: Some(tipb::Join {
                inner_idx: Some(1),
                children: vec![
                    tipb::Executor {
                        tp: Some(ExecType::TypeSelection as i32),
                        selection: Some(Box::new(tipb::Selection {
                            child: Some(Box::new(scan(10))),
                            ..tipb::Selection::default()
                        })),
                        ..tipb::Executor::default()
                    },
                    receiver(),
                ],
                ..tipb::Join::default()
            }),
            ..tipb::Executor::default()
        };
        let mut sender = tipb::Executor {
            tp: Some(ExecType::TypeExchangeSender as i32),
            exchange_sender: Some(Box::new(tipb::ExchangeSender {
                child: Some(Box::new(join)),
                ..tipb::ExchangeSender::default()
            })),
            ..tipb::Executor::default()
        };
        update_executor_table_id(Some(&mut sender), true, &[42]).unwrap();
        let join = sender.exchange_sender.unwrap().child.unwrap().join.unwrap();
        let selection = join.children[0].selection.as_ref().unwrap();
        assert_eq!(selection.child.as_ref().unwrap().tbl_scan.as_ref().unwrap().table_id, Some(42));

        let mut partition_scan = tipb::Executor {
            tp: Some(ExecType::TypePartitionTableScan as i32),
            partition_table_scan: Some(tipb::PartitionTableScan::default()),
            ..tipb::Executor::default()
        };
        update_executor_table_id(Some(&mut partition_scan), true, &[7, 8]).unwrap();
        assert_eq!(partition_scan.partition_table_scan.unwrap().partition_ids, vec![7, 8]);

        let mut unknown = tipb::Executor {
            tp: Some(ExecType::TypeKill as i32),
            ..tipb::Executor::default()
        };
        assert_eq!(
            update_executor_table_id(Some(&mut unknown), true, &[1]).unwrap_err(),
            format!("unknown new tipb protocol {}", ExecType::TypeKill as i32)
        );
    }
}
