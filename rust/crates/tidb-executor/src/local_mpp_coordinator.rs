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
//! * `CheckVisibility` (the GC safe point check after each packet): the MPP
//!   client has no safe point to read.
//! * The dispatch meta's `SqlDigest` / `PlanDigest`: the statement context
//!   carries no digest here, so they are sent empty.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::{Duration, Instant};

use prost::Message;
use tidb_distsql::remote_scan::PushdownStatementContext;
use tidb_kv::store_type::StoreType;
use tidb_planner_core_base::PlanError;
use tidb_planner_core_operator_physicalop::fragment::{
    generate_root_mpp_tasks, Fragment, MppTaskGeneratorContext,
};
use tidb_planner_core_operator_physicalop::to_pb::BuildPbContext;
use tidb_planner_core_operator_physicalop::{MppTask, PhysicalPlan, PhysicalTableScan};
use tidb_proto::mpp::{ReportTaskStatusRequest, TaskMeta};
use tidb_proto::tipb::{self, DagRequest, EncodeType, ExecType};
use tidb_txnkv::{
    KeyRange, MppBuildTasksRequest, MppClient, MppContext, MppError, MppQueryId, MppStoreTask,
    MppTaskLocation, MppVersion, QueryResponseError, QueryResultSubset,
};

use crate::dag_request::DEFAULT_DIV_PRECISION_INCREMENT;
use crate::mpp_coordinator::MppGatherResponse;
use crate::mppcoordmanager::{CoordinatorUniqueId, MppCoordinator, INSTANCE_MPP_COORDINATOR_MANAGER};
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

/// Go `placement.DCLabelKey`.
const DC_LABEL_KEY: &str = "zone";

/// Go `tiFlashStoreInfo`.
#[derive(Clone, Debug, Default)]
struct TiFlashStoreInfo {
    zone: String,
    store_id: u64,
}

/// Go `addTiFlashStoreInfo`.
fn add_tiflash_store_info(
    all_tiflash_store_info: &mut HashMap<String, TiFlashStoreInfo>,
    tiflash_store: &tikv_client::proto::metapb::Store,
) {
    let zone = tiflash_store
        .labels
        .iter()
        .find(|label| label.key == DC_LABEL_KEY)
        .map(|label| label.value.clone())
        .unwrap_or_default();
    all_tiflash_store_info.insert(
        tiflash_store.address.clone(),
        TiFlashStoreInfo {
            zone,
            store_id: tiflash_store.id,
        },
    );
}

/// Go `taskZoneInfoHelper`: used to help reset exchange executor's same zone
/// flags.
struct TaskZoneInfoHelper<'a> {
    all_tiflash_store_info: &'a HashMap<String, TiFlashStoreInfo>,
    /// exchangeZoneInfo is used to cache one mpp task's zone info: key is
    /// executor id, value is zone info array. For ExchangeSender, it's
    /// target tiflash nodes' zone info; for ExchangeReceiver, it's source
    /// tiflash nodes' zone info.
    exchange_zone_info: HashMap<String, Vec<String>>,
    tidb_zone: String,
    current_task_zone: String,
    is_root: bool,
}

impl<'a> TaskZoneInfoHelper<'a> {
    /// Go `taskZoneInfoHelper.init`.
    fn init(all_tiflash_store_info: &'a HashMap<String, TiFlashStoreInfo>) -> Self {
        Self {
            all_tiflash_store_info,
            exchange_zone_info: HashMap::with_capacity(2),
            tidb_zone: tidb_config::config_tree::config::get_global_config()
                .labels
                .get(DC_LABEL_KEY)
                .cloned()
                .unwrap_or_default(),
            current_task_zone: String::new(),
            is_root: false,
        }
    }

    /// Go `allTiFlashStoreInfo[address].zone`: the store's zone, empty for
    /// an address that is no store's.
    fn zone_of(&self, address: &str) -> &str {
        self.all_tiflash_store_info
            .get(address)
            .map_or("", |store_info| store_info.zone.as_str())
    }

    /// Go `tryQuickFillWithUncertainZones`.
    fn try_quick_fill_with_uncertain_zones(
        &self,
        exec: &tipb::Executor,
        slots: usize,
        same_zone_flags: &mut Vec<bool>,
    ) -> bool {
        if exec.executor_id.is_none() || self.current_task_zone.is_empty() {
            same_zone_flags.extend(std::iter::repeat(true).take(slots));
            return true;
        }
        if self.is_root && exec.tp == Some(ExecType::TypeExchangeSender as i32) {
            same_zone_flags.push(self.tidb_zone.is_empty() || self.current_task_zone == self.tidb_zone);
            return true;
        }
        false
    }

    /// Go `collectExchangeZoneInfos`.
    fn collect_exchange_zone_infos(&self, encoded_task_meta: &[Vec<u8>]) -> Vec<String> {
        encoded_task_meta
            .iter()
            .map(|task_bytes| {
                TaskMeta::decode(task_bytes.as_slice())
                    .map(|task_meta| self.zone_of(&task_meta.address).to_owned())
                    .unwrap_or_default()
            })
            .collect()
    }

    /// Go `inferSameZoneFlag`.
    fn infer_same_zone_flag(&mut self, exec: &tipb::Executor, encoded_task_meta: &[Vec<u8>]) -> Vec<bool> {
        let slots = encoded_task_meta.len();
        let mut same_zone_flags = Vec::with_capacity(slots);
        if self.try_quick_fill_with_uncertain_zones(exec, slots, &mut same_zone_flags) {
            return same_zone_flags;
        }
        let executor_id = exec.executor_id.as_deref().unwrap_or_default();
        if !self.exchange_zone_info.contains_key(executor_id) {
            let zone_infos = self.collect_exchange_zone_infos(encoded_task_meta);
            self.exchange_zone_info.insert(executor_id.to_owned(), zone_infos);
        }
        let zone_infos = &self.exchange_zone_info[executor_id];

        if zone_infos.len() != slots {
            // This branch is for safety purpose, not expected
            same_zone_flags.extend(std::iter::repeat(true).take(slots));
            return same_zone_flags;
        }

        same_zone_flags.extend(
            zone_infos
                .iter()
                .map(|zone_info| zone_info.is_empty() || &self.current_task_zone == zone_info),
        );
        same_zone_flags
    }

    /// Go `fillSameZoneFlagForExchange`.
    fn fill_same_zone_flag_for_exchange(&mut self, exec: &mut tipb::Executor) {
        let tp = exec.tp.unwrap_or_default();
        let mut children: Vec<&mut tipb::Executor> = Vec::with_capacity(2);
        match ExecType::try_from(tp) {
            Ok(
                ExecType::TypeTableScan
                | ExecType::TypePartitionTableScan
                | ExecType::TypeIndexScan
                | ExecType::TypeCteSource,
            ) => {}
            Ok(ExecType::TypeSelection) => children.extend(exec.selection.as_mut().and_then(|e| e.child.as_deref_mut())),
            Ok(ExecType::TypeAggregation | ExecType::TypeStreamAgg) => {
                children.extend(exec.aggregation.as_mut().and_then(|e| e.child.as_deref_mut()));
            }
            Ok(ExecType::TypeTopN) => children.extend(exec.top_n.as_mut().and_then(|e| e.child.as_deref_mut())),
            Ok(ExecType::TypeLimit) => children.extend(exec.limit.as_mut().and_then(|e| e.child.as_deref_mut())),
            Ok(ExecType::TypeExchangeSender) => {
                let same_zone_flag = exec
                    .exchange_sender
                    .as_ref()
                    .map(|sender| self.infer_same_zone_flag(exec, &sender.encoded_task_meta));
                if let (Some(sender), Some(same_zone_flag)) = (exec.exchange_sender.as_mut(), same_zone_flag) {
                    sender.same_zone_flag = same_zone_flag;
                    children.extend(sender.child.as_deref_mut());
                }
            }
            Ok(ExecType::TypeExchangeReceiver) => {
                let same_zone_flag = exec
                    .exchange_receiver
                    .as_ref()
                    .map(|receiver| self.infer_same_zone_flag(exec, &receiver.encoded_task_meta));
                if let (Some(receiver), Some(same_zone_flag)) = (exec.exchange_receiver.as_mut(), same_zone_flag) {
                    receiver.same_zone_flag = same_zone_flag;
                }
            }
            Ok(ExecType::TypeCteSink) => children.extend(exec.cte_sink.as_mut().and_then(|e| e.child.as_deref_mut())),
            Ok(ExecType::TypeJoin) => children.extend(exec.join.as_mut().into_iter().flat_map(|e| e.children.iter_mut())),
            Ok(ExecType::TypeProjection) => {
                children.extend(exec.projection.as_mut().and_then(|e| e.child.as_deref_mut()));
            }
            Ok(ExecType::TypeWindow) => children.extend(exec.window.as_mut().and_then(|e| e.child.as_deref_mut())),
            Ok(ExecType::TypeSort) => children.extend(exec.sort.as_mut().and_then(|e| e.child.as_deref_mut())),
            Ok(ExecType::TypeExpand) => children.extend(exec.expand.as_mut().and_then(|e| e.child.as_deref_mut())),
            Ok(ExecType::TypeExpand2) => children.extend(exec.expand2.as_mut().and_then(|e| e.child.as_deref_mut())),
            _ => tidb_logutil::bg_logger().warn(&format!("unknown new tipb protocol {tp}"), &[]),
        }
        for child in children {
            self.fill_same_zone_flag_for_exchange(child);
        }
    }
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

/// Go `receiveReportTimeout`.
const RECEIVE_REPORT_TIMEOUT: Duration = Duration::from_millis(100);

/// Go `mppRequestReport`.
#[derive(Default)]
struct MppRequestReport {
    received_report: bool,
    err_msg: String,
    execution_summaries: Vec<tipb::ExecutorExecutionSummary>,
}

/// What Go `localMppCoordinator.mu` guards.
#[derive(Default)]
struct Reports {
    /// Go `reqMap`, by task id.
    req_map: HashMap<i64, MppRequestReport>,
    reported_req_count: usize,
    first_err_msg: String,
    /// Go `reportStatusCh`, closed once every task reported.
    report_status_closed: bool,
}

/// The state of Go `localMppCoordinator` that the coordinator manager
/// reaches while the gather runs: `ReportStatus` and `IsClosed`.
#[derive(Default)]
pub struct CoordinatorReports {
    /// Go `closed` and `finishCh`.
    closed: Arc<AtomicBool>,
    dispatch_failed: AtomicBool,
    all_reports_handled: AtomicBool,
    mu: Mutex<Reports>,
    /// Wakes the waiter on Go `reportStatusCh`.
    report_status_ch: Condvar,
}

impl CoordinatorReports {
    fn lock(&self) -> std::sync::MutexGuard<'_, Reports> {
        self.mu.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl MppCoordinator for CoordinatorReports {
    /// Go `localMppCoordinator.ReportStatus`.
    fn report_status(&self, request: &ReportTaskStatusRequest) -> Result<(), String> {
        let task_id = request.meta.as_ref().map_or(0, |meta| meta.task_id);
        let err_msg = request.error.as_ref().map_or_else(String::new, |error| error.msg.clone());
        let execution_info =
            tipb::TiFlashExecutionInfo::decode(request.data.as_slice()).map_err(|err| err.to_string())?;

        let mut reports = self.lock();
        let Some(req) = reports.req_map.get_mut(&task_id) else {
            return Err(format!("ReportMPPTaskStatus task not exists taskID: {task_id}"));
        };
        if req.received_report {
            return Err(format!("ReportMPPTaskStatus task already received taskID: {task_id}"));
        }
        req.received_report = true;
        req.execution_summaries = execution_info.execution_summaries;
        if !err_msg.is_empty() {
            req.err_msg.clone_from(&err_msg);
            if reports.first_err_msg.is_empty() {
                reports.first_err_msg = err_msg;
            }
        }
        reports.reported_req_count += 1;
        if reports.reported_req_count == reports.req_map.len() {
            reports.report_status_closed = true;
            self.report_status_ch.notify_all();
        }
        Ok(())
    }

    /// Go `localMppCoordinator.IsClosed`.
    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

/// Go `needReportExecutionSummary`: DFS to check if plan needs report
/// execution summary through ReportMPPTaskStatus mpp service. Currently,
/// return true if there is a limit operator in the path from current
/// TableReader to root.
fn need_report_execution_summary(plan: &PhysicalPlan, dest_table_plan_id: i64, found_limit: bool) -> bool {
    match plan {
        PhysicalPlan::Limit(_) => need_report_execution_summary(&plan.children()[0], dest_table_plan_id, true),
        PhysicalPlan::TableReader(reader) => {
            found_limit
                && reader
                    .table_plan
                    .as_ref()
                    .is_some_and(|table_plan| i64::from(table_plan.id()) == dest_table_plan_id)
        }
        PhysicalPlan::ShuffleReceiver(stub) => {
            need_report_execution_summary(&stub.data_source, dest_table_plan_id, found_limit)
        }
        PhysicalPlan::CTE(cte) => {
            need_report_execution_summary(&cte.seed_plan, dest_table_plan_id, found_limit)
                || cte
                    .recursive_plan
                    .as_ref()
                    .is_some_and(|recur| need_report_execution_summary(recur, dest_table_plan_id, found_limit))
        }
        _ => plan
            .children()
            .iter()
            .any(|child| need_report_execution_summary(child, dest_table_plan_id, found_limit)),
    }
}

/// Go `localMppCoordinator`.
pub struct LocalMppCoordinator {
    /// Go `respChan`; `None` once closed.
    resp_rx: Option<Receiver<MppResponse>>,
    reports: Arc<CoordinatorReports>,
    /// Go `wgDoneChan`: the dispatching thread.
    dispatch_all: Option<std::thread::JoinHandle<()>>,
    node_cnt: usize,
    memory: StatementMemory,
    start_ts: u64,
    mpp_query_id: MppQueryId,
    gather_id: u64,
    /// Go `planIDs`: the plans of the gather's table plan.
    plan_ids: Vec<isize>,
    coordinator_addr: String,
    report_execution_info: bool,
    /// Go `sessionCtx.GetSessionVars().StmtCtx.RuntimeStatsColl`.
    runtime_stats_coll: Option<Arc<dyn tidb_execdetails::runtime_stats::ExecutorRuntimeStatsSource>>,
    /// Go `ctx.Value(clientutil.RUDetailsCtxKey)`.
    ru_details: Option<Arc<tikv_client::RuDetails>>,
    /// Go `execdetails.RUV2MetricsFromContext(ctx)`.
    ruv2_metrics: Option<Arc<tidb_execdetails::ruv2_metrics::RUV2Metrics>>,
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
    /// Go `NewLocalMPPCoordinator`: the coordinator of `plan` (the gather's
    /// table plan), which TiFlash reports to at `coordinator_addr`.
    #[must_use]
    pub fn new(
        ctx: &crate::StmtContext,
        plan: &PhysicalPlan,
        plan_ids: Vec<isize>,
        start_ts: u64,
        mpp_query_id: MppQueryId,
        gather_id: u64,
        mut coordinator_addr: String,
    ) -> Self {
        if ctx.mpp_version() < MppVersion::V2 {
            coordinator_addr.clear();
        }
        let report_execution_info = !coordinator_addr.is_empty()
            && ctx
                .stmt_plan()
                .and_then(|stored| stored.get::<PhysicalPlan>())
                .is_some_and(|stmt_plan| need_report_execution_summary(&stmt_plan, i64::from(plan.id()), false));
        Self {
            resp_rx: None,
            reports: Arc::new(CoordinatorReports::default()),
            dispatch_all: None,
            node_cnt: 0,
            memory: crate::remote_scan::pushdown_statement_context(ctx).memory,
            start_ts,
            mpp_query_id,
            gather_id,
            plan_ids,
            coordinator_addr,
            report_execution_info,
            runtime_stats_coll: ctx.executor_runtime_stats_source().cloned(),
            ru_details: ctx.ru_details(),
            ruv2_metrics: ctx.ruv2_metrics().cloned(),
        }
    }

    /// Go `CoordinatorUniqueID` of this coordinator.
    #[must_use]
    pub fn unique_id(&self) -> CoordinatorUniqueId {
        CoordinatorUniqueId {
            mpp_query_id: self.mpp_query_id,
            gather_id: self.gather_id,
        }
    }

    /// What the coordinator manager holds of this coordinator.
    #[must_use]
    pub fn reports(&self) -> Arc<dyn MppCoordinator> {
        Arc::clone(&self.reports) as Arc<dyn MppCoordinator>
    }

    /// Go `Execute`: the fragments of `sender` (the reader's plan), every
    /// task dispatched on its own thread, and the key ranges the gather
    /// reads. The coordinator answers the root tasks' packets.
    ///
    /// # Errors
    /// The fragments, their tasks or their DAGs could not be built.
    pub fn execute(
        &mut self,
        ctx: &crate::StmtContext,
        catalog: &tidb_infoschema::SessionExtendedInfoSchema,
        client: &Arc<dyn MppClient>,
        sender: &PhysicalPlan,
    ) -> Result<Vec<KeyRange>, String> {
        let (start_ts, mpp_query_id, gather_id) = (self.start_ts, self.mpp_query_id, self.gather_id);
        let statement = crate::remote_scan::pushdown_statement_context(ctx);
        let closed = Arc::clone(&self.reports.closed);
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
        let mut all_tiflash_store_info = HashMap::new();
        for tiflash_store in client.tiflash_stores() {
            add_tiflash_store_info(&mut all_tiflash_store_info, &tiflash_store);
        }
        if tidb_config::config_tree::config::get_global_config().disaggregated_tiflash {
            for tiflash_store in client.tiflash_compute_stores().unwrap_or_default() {
                add_tiflash_store_info(&mut all_tiflash_store_info, &tiflash_store);
            }
        }
        let mut mpp_reqs = Vec::new();
        for frag in &frags {
            self.append_mpp_dispatch_req(ctx, &statement, frag, &all_tiflash_store_info, &mut mpp_reqs)?;
        }

        let (resp_tx, resp_rx) = std::sync::mpsc::sync_channel(0);
        let dispatch_all = {
            let client = Arc::clone(client);
            let reports = Arc::clone(&self.reports);
            let kv_context = statement
                .kv_context
                .as_ref()
                .map(tidb_txnkv::StatementKvContextCell::get)
                .unwrap_or_default();
            let rpc_interceptor = statement.rpc_interceptor.clone();
            std::thread::spawn(move || {
                dispatch_all(&client, &mpp_ctx, &kv_context, &rpc_interceptor, mpp_reqs, &reports, &resp_tx);
            })
        };
        self.resp_rx = Some(resp_rx);
        self.dispatch_all = Some(dispatch_all);
        self.node_cnt = node_info.len();
        Ok(kv_ranges)
    }

    /// Go `closeWithoutReport`: cancels the tasks' context and waits for
    /// every dispatching thread. Go's `ExecutorWithRetry` unregisters the
    /// coordinator right after either close; the coordinator holds its own
    /// id, so it does that here.
    fn close_without_report(&mut self) {
        self.reports.closed.store(true, Ordering::Release);
        self.resp_rx = None;
        if let Some(dispatch_all) = self.dispatch_all.take() {
            let _ = dispatch_all.join();
        }
        INSTANCE_MPP_COORDINATOR_MANAGER.unregister(self.unique_id());
    }

    /// Go `handleAllReports`: once every task reported, their execution
    /// summaries are the gather's runtime statistics; a report missing after
    /// `receiveReportTimeout` is given up on.
    ///
    /// # Errors
    /// A reported RU consumption does not decode.
    fn handle_all_reports(&self) -> Result<(), String> {
        if !self.report_execution_info
            || self.reports.dispatch_failed.load(Ordering::Acquire)
            || self
                .reports
                .all_reports_handled
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return Ok(());
        }
        let start_time = Instant::now();
        let (reports, _) = self
            .reports
            .report_status_ch
            .wait_timeout_while(self.reports.lock(), RECEIVE_REPORT_TIMEOUT, |reports| {
                !reports.report_status_closed
            })
            .unwrap_or_else(PoisonError::into_inner);
        if !reports.report_status_closed {
            tidb_metrics::executor::MPP_COORDINATOR_STATS
                .with_label_values(&["reportNotRcv"])
                .inc();
            tidb_logutil::bg_logger().info(
                &format!(
                    "Mpp coordinator not received all reports within {} ms",
                    RECEIVE_REPORT_TIMEOUT.as_millis()
                ),
                &[
                    tidb_log::Field::new("txnStartTS", tidb_log::Value::U64(self.start_ts)),
                    tidb_log::Field::new("gatherID", tidb_log::Value::U64(self.gather_id)),
                    tidb_log::Field::new("expectCount", tidb_log::Value::U64(reports.req_map.len() as u64)),
                    tidb_log::Field::new("actualCount", tidb_log::Value::U64(reports.reported_req_count as u64)),
                ],
            );
            return Ok(());
        }
        tidb_metrics::executor::MPP_COORDINATOR_LATENCY
            .with_label_values(&["rcvReports"])
            .observe(start_time.elapsed().as_millis() as f64);
        let Some(coll) = self.runtime_stats_coll.as_deref() else {
            return Ok(());
        };
        let mut recorded_plan_ids = HashSet::new();
        for report in reports.req_map.values() {
            for detail in &report.execution_summaries {
                if detail.time_processed_ns.is_some()
                    && detail.num_produced_rows.is_some()
                    && detail.num_iterations.is_some()
                {
                    recorded_plan_ids.insert(coll.record_one_cop_task(-1, StoreType::TiFlash, detail));
                }
            }
            if !self.ruv2_metrics.as_ref().is_some_and(|metrics| metrics.bypass()) {
                if let Some(ru_details) = &self.ru_details {
                    tidb_execdetails::tiflash_stats::merge_tiflash_ru_consumption(
                        &report.execution_summaries,
                        ru_details,
                    )
                    .map_err(|err| err.to_string())?;
                }
            }
        }
        tidb_distsql::select_result_stats::fill_dummy_summaries_for_tiflash_tasks(
            coll,
            StoreType::TiFlash,
            &self.plan_ids,
            &recorded_plan_ids,
        );
        Ok(())
    }

    /// Go `appendMPPDispatchReq`: the fragment's DAG (`ConstructDAGReq` over
    /// its sink, every sink column as an output, TypeChunk for the root
    /// fragment and TypeCHBlock for the others), pointed at each task's
    /// table.
    fn append_mpp_dispatch_req(
        &self,
        ctx: &crate::StmtContext,
        statement: &PushdownStatementContext,
        pf: &Fragment,
        all_tiflash_store_info: &HashMap<String, TiFlashStoreInfo>,
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
        let mut zone_helper = TaskZoneInfoHelper::init(all_tiflash_store_info);
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
            zone_helper.is_root = pf.is_root;
            zone_helper.current_task_zone = zone_helper.zone_of(mpp_task.location.address()).to_owned();
            if let Some(root_executor) = dag_req.root_executor.as_mut() {
                zone_helper.fill_same_zone_flag_for_exchange(root_executor);
            }
            let pb_data = dag_req.encode_to_vec();
            // Go `MPPClient.DispatchMPPTask`'s task meta.
            let meta = TaskMeta {
                start_ts: mpp_task.start_ts,
                query_ts: mpp_task.query_id.query_ts,
                local_query_id: mpp_task.query_id.local_query_id,
                task_id: mpp_task.id,
                server_id: mpp_task.query_id.server_id,
                gather_id: self.gather_id,
                address: mpp_task.location.address().to_owned(),
                coordinator_address: self.coordinator_addr.clone(),
                report_execution_summary: self.report_execution_info,
                mpp_version: mpp_task.version.as_i64(),
                resource_group_name: rg_name.clone(),
                connection_id: mpp_task.session_id,
                connection_alias: mpp_task.session_alias.clone(),
                sql_digest: String::new(),
                plan_digest: String::new(),
                ..TaskMeta::default()
            };
            self.reports.lock().req_map.insert(mpp_task.id, MppRequestReport::default());
            mpp_reqs.push(MppDispatchRequest {
                data: pb_data,
                task: Arc::clone(mpp_task),
                is_root: pf.is_root,
                meta,
            });
        }
        Ok(())
    }
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
    reports: &CoordinatorReports,
    resp_tx: &SyncSender<MppResponse>,
) {
    std::thread::scope(|scope| {
        for task in mpp_reqs {
            if reports.closed.load(Ordering::Acquire) {
                break;
            }
            scope.spawn(move || {
                handle_dispatch_req(client, mpp_ctx, kv_context, rpc_interceptor, task, reports, resp_tx);
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
    reports: &CoordinatorReports,
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
        reports.dispatch_failed.store(true, Ordering::Release);
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
    receive_results(
        client,
        mpp_ctx,
        kv_context,
        rpc_interceptor,
        &req.meta,
        task_meta,
        reports,
        resp_tx,
    );
}

/// Go `handleMPPStreamResponse`'s error for a packet that carries its task's
/// error, whose message is `msg`: the first error the tasks reported, when
/// there is one, avoids a confusing message.
fn mpp_stream_error(reports: &CoordinatorReports, msg: &str, req_meta: &TaskMeta) -> QueryResponseError {
    let first_err_msg = reports.lock().first_err_msg.clone();
    let msg = if first_err_msg.is_empty() { msg } else { &first_err_msg };
    let err = format!("other error for mpp stream: {msg}");
    tidb_logutil::bg_logger().warn(
        "other error",
        &[
            tidb_log::Field::new("txnStartTS", tidb_log::Value::U64(req_meta.start_ts)),
            tidb_log::Field::new("storeAddr", tidb_log::Value::Str(req_meta.address.clone())),
            tidb_log::Field::new("mpp-version", tidb_log::Value::I64(req_meta.mpp_version)),
            tidb_log::Field::new("task-id", tidb_log::Value::I64(req_meta.task_id)),
            tidb_log::Field::new("error", tidb_log::Value::Str(err.clone())),
        ],
    );
    QueryResponseError::Source(err)
}

/// Go `receiveResults`.
fn receive_results(
    client: &Arc<dyn MppClient>,
    mpp_ctx: &Arc<dyn MppContext>,
    kv_context: &tidb_txnkv::StatementKvContext,
    rpc_interceptor: &tidb_txnkv::RpcInterceptorOption,
    req_meta: &TaskMeta,
    task_meta: TaskMeta,
    reports: &CoordinatorReports,
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
                let err = match stream.packet_error() {
                    Some(msg) => mpp_stream_error(reports, msg, req_meta),
                    None => err,
                };
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

    /// Go `Close`. Its error is a report whose RU consumption does not
    /// decode, which a response's close has nowhere to return.
    fn close(&mut self) {
        self.close_without_report();
        if let Err(err) = self.handle_all_reports() {
            tidb_logutil::bg_logger().warn(
                "mpp coordinator handle reports meet error",
                &[tidb_log::Field::new("error", tidb_log::Value::Str(err))],
            );
        }
    }
}

impl MppGatherResponse for LocalMppCoordinator {
    fn node_count(&self) -> usize {
        self.node_cnt
    }

    fn close_without_report(&mut self) {
        LocalMppCoordinator::close_without_report(self);
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
    use tidb_planner_core_base::BasePlan;
    use tidb_planner_core_operator_physicalop::{
        BasePhysicalPlan, PhysicalLimit, PhysicalSelection, PhysicalTableReader,
    };

    fn base(id: i32, children: Vec<PhysicalPlan>) -> BasePhysicalPlan {
        let mut base = BasePhysicalPlan::default();
        base.base = BasePlan::with_id(id, "", 0);
        base.set_children(children);
        base
    }

    fn reader(table_plan_id: i32) -> PhysicalPlan {
        PhysicalPlan::TableReader(PhysicalTableReader {
            base: base(10, Vec::new()),
            table_plan: Some(Box::new(selection(table_plan_id, Vec::new()))),
            ..PhysicalTableReader::default()
        })
    }

    fn selection(id: i32, children: Vec<PhysicalPlan>) -> PhysicalPlan {
        PhysicalPlan::Selection(PhysicalSelection {
            base: base(id, children),
            ..PhysicalSelection::default()
        })
    }

    fn limit(child: PhysicalPlan) -> PhysicalPlan {
        PhysicalPlan::Limit(PhysicalLimit {
            base: base(20, vec![child]),
            ..PhysicalLimit::default()
        })
    }

    /// Go `needReportExecutionSummary`: true if there is a limit operator in
    /// the path from the gather's TableReader to the root.
    #[test]
    fn a_table_reader_under_a_limit_reports_its_execution_summary() {
        assert!(need_report_execution_summary(&limit(reader(5)), 5, false));
        assert!(!need_report_execution_summary(&limit(reader(5)), 6, false));
        assert!(!need_report_execution_summary(&reader(5), 5, false));
        assert!(need_report_execution_summary(&reader(5), 5, true));
        assert!(need_report_execution_summary(&limit(selection(30, vec![reader(5)])), 5, false));
        assert!(need_report_execution_summary(&selection(30, vec![limit(reader(5))]), 5, false));
        assert!(!need_report_execution_summary(&selection(30, vec![reader(5)]), 5, false));
        assert!(need_report_execution_summary(
            &selection(30, vec![reader(4), limit(reader(5))]),
            5,
            false
        ));
    }

    fn mock_task_zone_info_helper<'a>(
        is_root: bool,
        task_zone: &str,
        tidb_zone: &str,
        all_tiflash_store_info: &'a HashMap<String, TiFlashStoreInfo>,
    ) -> TaskZoneInfoHelper<'a> {
        TaskZoneInfoHelper {
            all_tiflash_store_info,
            exchange_zone_info: HashMap::new(),
            tidb_zone: tidb_zone.to_owned(),
            current_task_zone: task_zone.to_owned(),
            is_root,
        }
    }

    /// Go `TestZoneHelperTryQuickFill`.
    #[test]
    fn test_zone_helper_try_quick_fill() {
        let mut slots = 3;
        let all_tiflash_zone_info = HashMap::new();
        let mut helper = mock_task_zone_info_helper(false, "", "east", &all_tiflash_zone_info);
        let sender = tipb::Executor {
            executor_id: Some("ExchangeSender_1".to_owned()),
            tp: Some(ExecType::TypeExchangeSender as i32),
            exchange_sender: Some(Box::default()),
            ..tipb::Executor::default()
        };
        let mut same_zone_flags = Vec::with_capacity(slots);
        // When task zone is empty, then the function returns true, and all sameZoneFlags are true
        assert!(helper.try_quick_fill_with_uncertain_zones(&sender, slots, &mut same_zone_flags));
        assert_eq!(same_zone_flags, [true; 3]);

        // When task is root task, and executor is exchangeSender then the function compares tidbZone with currentTaskZone
        helper.is_root = true;
        helper.current_task_zone = "west".to_owned();
        slots = 1;
        same_zone_flags = Vec::with_capacity(slots);
        assert!(helper.try_quick_fill_with_uncertain_zones(&sender, slots, &mut same_zone_flags));
        assert_eq!(same_zone_flags, [false]);

        helper.current_task_zone = "east".to_owned();
        same_zone_flags = Vec::with_capacity(slots);
        assert!(helper.try_quick_fill_with_uncertain_zones(&sender, slots, &mut same_zone_flags));
        assert_eq!(same_zone_flags, [true]);

        // When task is neither root exchange sender nor current task zone is empty, return false, and empty sameZoneFlags
        helper.is_root = false;
        helper.current_task_zone = "west".to_owned();
        slots = 3;
        same_zone_flags = Vec::with_capacity(slots);
        assert!(!helper.try_quick_fill_with_uncertain_zones(&sender, slots, &mut same_zone_flags));
        assert!(same_zone_flags.is_empty());

        helper.is_root = true;
        helper.current_task_zone = "west".to_owned();
        same_zone_flags = Vec::with_capacity(slots);
        let receiver = tipb::Executor {
            executor_id: Some("ExchangeReceiver_2".to_owned()),
            tp: Some(ExecType::TypeExchangeReceiver as i32),
            exchange_receiver: Some(tipb::ExchangeReceiver::default()),
            ..tipb::Executor::default()
        };
        assert!(!helper.try_quick_fill_with_uncertain_zones(&receiver, slots, &mut same_zone_flags));
        assert!(same_zone_flags.is_empty());
    }

    /// Go's `fillSameZoneFlagForExchange` over a sender above a join of a
    /// scan and a receiver, by the bytes Go marshals each filled executor to
    /// (`testdata/task_zone_info_helper_go.txt`, from a test in Go's package):
    /// stores a, b and c in zones east, west and none, a sender to a, b, c, an
    /// unknown store and a task meta that does not decode, a receiver from b
    /// and a.
    #[test]
    fn fill_same_zone_flag_for_exchange_fills_gos_bytes() {
        let go: HashMap<&str, Vec<u8>> = include_str!("../testdata/task_zone_info_helper_go.txt")
            .lines()
            .map(|line| {
                let (name, hex) = line.split_once(' ').unwrap();
                let bytes = (0..hex.len())
                    .step_by(2)
                    .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
                    .collect();
                (name, bytes)
            })
            .collect();
        let input = || tipb::Executor::decode(go["input"].as_slice()).unwrap();
        assert_eq!(input().encode_to_vec(), go["input"]);
        let mut stores = HashMap::new();
        for (id, (address, zone)) in [("a:3930", "east"), ("b:3930", "west"), ("c:3930", "")].into_iter().enumerate() {
            add_tiflash_store_info(
                &mut stores,
                &tikv_client::proto::metapb::Store {
                    id: id as u64 + 1,
                    address: address.to_owned(),
                    labels: vec![
                        tikv_client::proto::metapb::StoreLabel {
                            key: "engine".to_owned(),
                            value: "tiflash".to_owned(),
                        },
                        tikv_client::proto::metapb::StoreLabel {
                            key: "zone".to_owned(),
                            value: zone.to_owned(),
                        },
                    ],
                    ..Default::default()
                },
            );
        }
        assert_eq!(stores["b:3930"].store_id, 2);
        for (name, is_root, task_zone, tidb_zone, has_id) in [
            ("nonroot_west", false, "west", "east", true),
            ("nonroot_east", false, "east", "east", true),
            ("root_west_tidb_east", true, "west", "east", true),
            ("root_west_tidb_west", true, "west", "west", true),
            ("root_west_tidb_empty", true, "west", "", true),
            ("task_zone_empty", false, "", "east", true),
            ("root_task_zone_empty", true, "", "east", true),
            ("no_executor_id", false, "west", "east", false),
        ] {
            let mut helper = mock_task_zone_info_helper(is_root, task_zone, tidb_zone, &stores);
            let mut exec = input();
            if !has_id {
                exec.executor_id = None;
            }
            helper.fill_same_zone_flag_for_exchange(&mut exec);
            assert_eq!(exec.encode_to_vec(), go[name], "{name}");
        }

        // One helper over a fragment's tasks: an executor's zones are read once.
        let mut helper = mock_task_zone_info_helper(false, "west", "east", &stores);
        helper.fill_same_zone_flag_for_exchange(&mut input());
        helper.current_task_zone = "east".to_owned();
        let meta = |address: &str| {
            TaskMeta {
                address: address.to_owned(),
                ..TaskMeta::default()
            }
            .encode_to_vec()
        };
        let mut second = input();
        second.exchange_sender.as_mut().unwrap().encoded_task_meta = vec![meta("b:3930"); 5];
        let tipb::Executor { join: Some(join), .. } = second.exchange_sender.as_mut().unwrap().child.as_deref_mut().unwrap()
        else {
            panic!("a join under the sender");
        };
        join.children[1].exchange_receiver.as_mut().unwrap().encoded_task_meta = vec![meta("a:3930")];
        helper.fill_same_zone_flag_for_exchange(&mut second);
        assert_eq!(second.encode_to_vec(), go["cached_second_task"]);
    }

    fn summary(executor_id: &str, rows: u64) -> tipb::ExecutorExecutionSummary {
        tipb::ExecutorExecutionSummary {
            time_processed_ns: Some(1),
            num_produced_rows: Some(rows),
            num_iterations: Some(1),
            executor_id: Some(executor_id.to_owned()),
            ..Default::default()
        }
    }

    fn report(task_id: i64, error: &str, summaries: Vec<tipb::ExecutorExecutionSummary>) -> ReportTaskStatusRequest {
        ReportTaskStatusRequest {
            meta: Some(TaskMeta {
                task_id,
                ..TaskMeta::default()
            }),
            data: tipb::TiFlashExecutionInfo {
                execution_summaries: summaries,
            }
            .encode_to_vec()
            .into(),
            error: (!error.is_empty()).then(|| tidb_proto::mpp::Error {
                msg: error.to_owned(),
                ..Default::default()
            }),
        }
    }

    fn reports_of(task_ids: &[i64]) -> Arc<CoordinatorReports> {
        let reports = Arc::new(CoordinatorReports::default());
        for task_id in task_ids {
            reports.lock().req_map.insert(*task_id, MppRequestReport::default());
        }
        reports
    }

    /// Go `localMppCoordinator.ReportStatus`.
    #[test]
    fn a_task_reports_its_status_once() {
        let reports = reports_of(&[1, 2]);
        assert_eq!(
            reports.report_status(&report(3, "", Vec::new())),
            Err("ReportMPPTaskStatus task not exists taskID: 3".to_owned())
        );
        assert_eq!(reports.report_status(&report(1, "", vec![summary("TableFullScan_5", 7)])), Ok(()));
        assert_eq!(
            reports.report_status(&report(1, "", Vec::new())),
            Err("ReportMPPTaskStatus task already received taskID: 1".to_owned())
        );
        let mut undecodable = report(2, "", Vec::new());
        undecodable.data = vec![0xff].into();
        assert!(reports.report_status(&undecodable).is_err());
        {
            let state = reports.lock();
            assert_eq!(state.reported_req_count, 1);
            assert!(!state.report_status_closed);
            assert!(!state.req_map[&2].received_report);
            assert_eq!(state.req_map[&1].execution_summaries, vec![summary("TableFullScan_5", 7)]);
        }
        assert_eq!(reports.report_status(&report(2, "first", Vec::new())), Ok(()));
        let state = reports.lock();
        assert_eq!(state.reported_req_count, 2);
        assert!(state.report_status_closed);
        assert_eq!(state.req_map[&2].err_msg, "first");
        assert_eq!(state.first_err_msg, "first");
        assert!(!reports.is_closed());
    }

    /// Go `handleMPPStreamResponse`: firstErrMsg is only used when already
    /// received error response from root tasks.
    #[test]
    fn a_stream_error_is_the_first_reported_error() {
        let reports = reports_of(&[1, 2]);
        assert_eq!(
            mpp_stream_error(&reports, "exchange closed", &TaskMeta::default()),
            QueryResponseError::Source("other error for mpp stream: exchange closed".to_owned())
        );
        reports.report_status(&report(1, "division by zero", Vec::new())).unwrap();
        reports.report_status(&report(2, "cancelled", Vec::new())).unwrap();
        assert_eq!(
            mpp_stream_error(&reports, "exchange closed", &TaskMeta::default()),
            QueryResponseError::Source("other error for mpp stream: division by zero".to_owned())
        );
    }

    fn coordinator(
        reports: Arc<CoordinatorReports>,
        report_execution_info: bool,
        coll: &Arc<tidb_execdetails::runtime_stats::RuntimeStatsColl>,
    ) -> LocalMppCoordinator {
        LocalMppCoordinator {
            resp_rx: None,
            reports,
            dispatch_all: None,
            node_cnt: 1,
            memory: StatementMemory::default(),
            start_ts: 1,
            mpp_query_id: MppQueryId::default(),
            gather_id: 1,
            plan_ids: vec![5, 6],
            coordinator_addr: "127.0.0.1:10080".to_owned(),
            report_execution_info,
            runtime_stats_coll: Some(Arc::clone(coll) as _),
            ru_details: None,
            ruv2_metrics: None,
        }
    }

    /// Go `handleAllReports`: the reported summaries are the runtime
    /// statistics of the plans their executor IDs name, and the gather's
    /// other plans get a dummy; nothing is recorded when a report is
    /// missing, a dispatch failed, or no summary is asked for, and the
    /// reports are handled once.
    #[test]
    fn the_reported_summaries_are_the_gathers_runtime_stats() {
        let new_coll = || Arc::new(tidb_execdetails::runtime_stats::RuntimeStatsColl::new(None));
        let reported = || {
            let reports = reports_of(&[1]);
            reports
                .report_status(&report(1, "", vec![summary("TableFullScan_5", 7)]))
                .unwrap();
            reports
        };

        let coll = new_coll();
        let coord = coordinator(reported(), true, &coll);
        coord.handle_all_reports().unwrap();
        assert_eq!(coll.get_cop_count_and_rows(5), (1, 7));
        assert_eq!(coll.get_cop_count_and_rows(6), (1, 0));
        coord.handle_all_reports().unwrap();
        assert_eq!(coll.get_cop_count_and_rows(5), (1, 7));

        let coll = new_coll();
        coordinator(reported(), false, &coll).handle_all_reports().unwrap();
        assert!(!coll.exists_cop_stats(5));

        let coll = new_coll();
        let reports = reported();
        reports.dispatch_failed.store(true, Ordering::Release);
        coordinator(reports, true, &coll).handle_all_reports().unwrap();
        assert!(!coll.exists_cop_stats(5));

        let coll = new_coll();
        let not_received = tidb_metrics::executor::MPP_COORDINATOR_STATS.with_label_values(&["reportNotRcv"]);
        let before = not_received.get();
        let start = Instant::now();
        coordinator(reports_of(&[1]), true, &coll).handle_all_reports().unwrap();
        assert!(start.elapsed() >= RECEIVE_REPORT_TIMEOUT);
        assert!(!coll.exists_cop_stats(5));
        assert!(!coll.exists_cop_stats(6));
        assert!(not_received.get() >= before + 1.0);
    }

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
