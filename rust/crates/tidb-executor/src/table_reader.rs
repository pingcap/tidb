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

//! Go `TableReaderExecutor` (`pkg/executor/table_reader.go`) for a reader on
//! the `kv.TiDB` store: a CLUSTER_* table's pushed plan goes, through
//! `distsql.SelectWithRuntimeStats`, to every TiDB the store's client lists,
//! and the reader's rows are the select result's.
//!
//! [`TableReaderExecutor`] is the same executor for a reader on the `kv.TiKV`
//! store (a list-based request) and on the `kv.TiFlash` store when it is not
//! an MPP gather (a tree-based request, sent as a batch coprocessor request,
//! `batchCop`, or region by region, `cop`).

use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use tidb_distsql::select_response::SelectResponseIter;
use tidb_distsql::{
    DistSqlContext, ExecutorKind, ExecutorShape, QueryResultContext, RequestBuilder,
    RequestEnvelope, SelectInput,
};
use tidb_driver_error::ExecError;
use tidb_executor_internal_exec::{Executor, ExecutorMeta};
use tidb_expr::schema::Schema;
use tidb_planner_core_operator_physicalop::physical_table_reader::ReadReqType;
use tidb_planner_core_operator_physicalop::{PhysicalPlan, PhysicalTableReader};
use tidb_proto::tipb;

use crate::builder_utils::TiDBCopRequest;

/// What a reader on the `kv.TiDB` store sends through: Go `dctx.Client`
/// (copr's `CopClient`, whose `Send` builds `buildTiDBMemCopTasks`' tasks)
/// and the login user `buildTableReader` puts on the DAG for the TiDBs'
/// privilege checks.
pub struct TiDbStoreReader {
    /// Go `dctx.Client`.
    pub client: Arc<dyn tidb_txnkv::Client>,
    /// Go `SessionVars.User`'s `Username` / `Hostname`.
    pub user: Option<tipb::UserIdentity>,
    /// Go `tableReaderExecutorContext.getDDLOwner`'s server ID
    /// (`OwnerManager.GetOwnerID`, then `infosync.GetServerInfoByID`), for a
    /// table read on the DDL owner; `None` in a context without DDL.
    pub ddl_owner_server_id: Option<Arc<dyn Fn() -> Result<u64, String> + Send + Sync>>,
}

/// Go `TableReaderExecutor` over the `kv.TiDB` store, its request built by
/// [`crate::builder_utils::build_tidb_cop_request`] until that store's scan
/// has its `ToPB` (`docs/tikv-reader-execplan.md`).
pub struct TiDbTableReaderExecutor {
    meta: ExecutorMeta,
    ctx: crate::StmtContext,
    request: Arc<TiDBCopRequest>,
    reader: TiDbStoreReader,
    /// Go `startTS`, read when the executor is built (`getSnapshotTS`).
    start_ts: u64,
    /// Go `resultHandler`'s select result.
    result: Option<SelectResponseIter>,
    exhausted: bool,
}

impl TiDbTableReaderExecutor {
    /// The reader of `request` through `reader`, at `start_ts`.
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        ctx: crate::StmtContext,
        request: Arc<TiDBCopRequest>,
        reader: TiDbStoreReader,
        start_ts: u64,
    ) -> Self {
        Self {
            meta,
            ctx,
            request,
            reader,
            start_ts,
            result: None,
            exhausted: false,
        }
    }

    /// Go `buildResp` over `buildKVReq` and `distsql.SelectWithRuntimeStats`.
    fn build_resp(&self) -> Result<SelectResponseIter, ExecError> {
        let ctx = &self.ctx;
        let mut dag = self.request.dag.clone();
        dag.user = self.reader.user.clone();
        let plan_id = isize::try_from(self.meta.id()).unwrap_or(0);
        // Go `TableReaderExecutor.memTracker`, under the statement's.
        let memory_tracker = Some(ctx.statement_memory().operator_tracker(self.meta.id()));
        let mut context = DistSqlContext::new();
        context.request.dist_sql_concurrency = ctx.dist_sql_scan_concurrency();
        context.request.store_batch_size = ctx.store_batch_size();
        context.request.resource_group_name = ctx.resource_group_name().to_owned();
        context.request.in_restricted_sql = ctx.request_source().internal;
        context.request.request_source_type = ctx.request_source().source_type.clone();
        context.request.explicit_request_source_type =
            ctx.request_source().explicit_source_type.clone();
        context.request.replica_read = ctx.replica_read();
        context.request.max_execution_time_ms = ctx.max_execution_time_ms();
        context.request.priority = crate::remote_scan::get_kv_priority(ctx.statement_priority());
        context.request.not_fill_cache = ctx.not_fill_cache();
        let kv_context = ctx
            .statement_kv_context()
            .as_ref()
            .map(tidb_txnkv::StatementKvContextCell::get)
            .unwrap_or_default();
        let mut builder = RequestBuilder::from_context(&context);
        if let Some(tagger) = &kv_context.resource_group_tagger {
            builder.set_resource_group_tagger(tagger());
        }
        builder.set_non_partitioned_key_ranges(
            self.request
                .ranges
                .iter()
                .map(|range| tidb_txnkv::KeyRange {
                    start_key: tidb_txnkv::Key::from_bytes(range.start.to_vec()),
                    end_key: tidb_txnkv::Key::from_bytes(range.end.to_vec()),
                })
                .collect(),
        );
        // Go `buildKVReq`: a table read on the DDL owner names its server.
        if self.request.cop_destination
            == tidb_infoschema::infoschema_meta::ClusterTableCopDestination::DdlOwner
        {
            let owner = self.reader.ddl_owner_server_id.as_ref().ok_or_else(|| {
                ExecError::Internal("GetDDLOwner in a context without DDL".into())
            })?;
            builder.set_tidb_server_id(
                owner().map_err(|error| ExecError::Internal(error.into()))?,
            );
        }
        builder
            .set_start_ts(self.start_ts)
            .set_mem_tracker(memory_tracker.clone())
            .set_store_type(tidb_txnkv::StoreType::TiDb)
            .set_dag_request(executor_shapes(&dag), prost::Message::encode_to_vec(&dag));
        let request = builder
            .build_transport_request(ctx.statement_memory().coprocessor_request_cancellation())
            .map_err(|error| ExecError::Internal(format!("{error:?}").into()))?
            .with_kv_context(kv_context);
        let result = tidb_distsql::select_with_runtime_stats_over_client(
            self.reader.client.as_ref(),
            &request,
            SelectInput::default(),
            QueryResultContext::new(self.request.field_types.clone(), ctx.cop_warning_sink())
                .with_time_zone(ctx.session_zone()),
            self.request.cop_plan_ids.clone(),
            plan_id,
            ctx.cop_response_sink().is_some(),
        )
        .map_err(|error| ExecError::Internal(error.to_string().into()))?;
        Ok(result
            .into_select_iter(Vec::new())
            .with_cop_response_sink(ctx.cop_response_sink())
            .with_memory_tracker(memory_tracker))
    }
}

/// The executor shapes `RequestBuilder.SetDAGRequest` reads off the DAG.
fn executor_shapes(dag: &tipb::DagRequest) -> RequestEnvelope {
    RequestEnvelope::new(
        dag.executors
            .iter()
            .map(|executor| {
                match executor.tp.and_then(|tp| tipb::ExecType::try_from(tp).ok()) {
                    Some(tipb::ExecType::TypeTableScan) => {
                        ExecutorShape::new(ExecutorKind::TableScan)
                    }
                    Some(tipb::ExecType::TypeLimit) => ExecutorShape::limit(
                        executor.limit.as_ref().and_then(|limit| limit.limit).unwrap_or(0),
                        None,
                    ),
                    _ => ExecutorShape::new(ExecutorKind::Other),
                }
            })
            .collect(),
    )
}

impl Executor for TiDbTableReaderExecutor {
    fn open(&mut self) -> Result<(), ExecError> {
        self.result = Some(self.build_resp()?);
        self.exhausted = false;
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if self.exhausted {
            return Ok(());
        }
        let Some(result) = self.result.as_mut() else {
            return Ok(());
        };
        let required = req.required_rows().max(1);
        loop {
            if !result
                .fill_chunk_with_required_rows(required, req)
                .map_err(|error| ExecError::from(crate::cop_reader::select_response_error(error)))?
            {
                self.exhausted = true;
                req.reset();
                return Ok(());
            }
            if req.num_rows() > 0 {
                return Ok(());
            }
        }
    }

    fn close(&mut self) -> Result<(), ExecError> {
        match self.result.take() {
            Some(mut result) => result
                .close()
                .map_err(|error| ExecError::from(crate::cop_reader::select_response_error(error))),
            None => Ok(()),
        }
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

/// Go `kvRangeBuilderFromRangeAndPartition`'s `TableHandleRangesToKVRanges`
/// for one physical table.
fn table_handle_ranges_to_kv_ranges(
    time_zone: &tidb_datatype::SessionTimeZone,
    tid: i64,
    is_common_handle: bool,
    ranges: &[tidb_ranger::types::Range],
) -> Result<Vec<tidb_txnkv::KeyRange>, ExecError> {
    let ranges = if is_common_handle {
        tidb_distsql::common_handle_ranges_to_kv_ranges(Some(time_zone), tid, ranges)
    } else {
        tidb_distsql::table_ranges_to_kv_ranges(tid, ranges)
    };
    ranges.map_err(|error| ExecError::Internal(format!("{error:?}").into()))
}

/// Go `TableReaderExecutor` over the `kv.TiKV` store, or over the
/// `kv.TiFlash` store for a reader that is not an MPP gather.
pub struct TableReaderExecutor {
    meta: ExecutorMeta,
    ctx: crate::StmtContext,
    /// Go `storeType`.
    store_type: tidb_kv::store_type::StoreType,
    /// Go `netDataSize` (`v.GetNetDataSize()`), the closest-replica
    /// adjuster's estimate.
    net_data_size: f64,
    /// Go `dctx.Client`.
    client: Arc<dyn tidb_txnkv::Client>,
    /// Go `tablePlan`.
    table_plan: PhysicalPlan,
    /// Go `getPhysicalPlanIDs(e.plans)`.
    plan_ids: Vec<isize>,
    /// Go `table.GetPhysicalID()`.
    physical_table_id: i64,
    is_common_handle: bool,
    /// Go `ranges`.
    ranges: Vec<tidb_ranger::types::Range>,
    /// Go `corColInAccess`: the ranges rebuilt over the outer row
    /// (`ts.ResolveCorrelatedColumns`).
    range_rebuild: Option<crate::access_path::RangeRebuilder>,
    /// Go `kvRangeBuilder` (`kvRangeBuilderFromRangeAndPartition`): the
    /// pruned partitions' physical IDs, ascending.
    partitions: Option<Vec<i64>>,
    keep_order: bool,
    desc: bool,
    /// Go `batchCop`.
    batch_cop: bool,
    /// Go `startTS`.
    start_ts: u64,
    /// Go `SetFromInfoSchema`'s schema version.
    schema_version: i64,
    /// Go `columns`.
    columns: Vec<tidb_model::GoShared<tidb_model::ColumnInfo>>,
    /// Go `virtualColumnIndex` / `virtualColumnRetFieldTypes`.
    virtual_column_index: Vec<usize>,
    virtual_column_ret_field_types: Vec<FieldType>,
    /// Go `resultHandler`: the select results, read one after another.
    results: std::collections::VecDeque<SelectResponseIter>,
    /// Go `serialSelectResults.cur`.
    cur: usize,
    /// Go `kvRanges`.
    kv_ranges: Vec<tidb_txnkv::KeyRange>,
    /// Go `paging` (the session's `EnablePaging`; never for a reader built
    /// from handles).
    paging: bool,
    /// Go `memTracker`, made on `Open`; a reader its caller built and never
    /// opens has none.
    mem_tracker: Option<Arc<tidb_memory::Tracker>>,
    /// Go `dagPB.Size()` of the reader's request.
    dag_size: usize,
    /// Go `dummy`: not a real reader, only the KV ranges a union scan reads
    /// the cached or temporary table's rows over (`setDummy`).
    dummy: bool,
}

/// What Go's `IndexLookUpExecutor.buildTableReader` puts in the
/// `TableReaderExecutor` a table task reads its handles through.
pub(crate) struct TableReaderForHandles<'a> {
    /// The reader's shape: the lookup's schema under its table plans' root.
    pub meta: ExecutorMeta,
    pub ctx: &'a crate::StmtContext,
    pub client: Arc<dyn tidb_txnkv::Client>,
    /// Go `tblPlans`' root.
    pub table_plan: &'a PhysicalPlan,
    /// Go `getPhysicalPlanIDs(e.tblPlans)`.
    pub plan_ids: Vec<isize>,
    /// Go `tableRequest`.
    pub dag: tipb::DagRequest,
    /// Go `getPhysicalTableID(table)`.
    pub physical_table_id: i64,
    pub is_common_handle: bool,
    pub columns: Vec<tidb_model::GoShared<tidb_model::ColumnInfo>>,
    pub start_ts: u64,
    /// Go `avgRowSize * len(task.handles)`.
    pub net_data_size: f64,
}

impl TableReaderExecutor {
    /// Go `dataReaderBuilder.buildTableReaderFromHandles(ctx, e, handles,
    /// true)`: the handles sorted, their ranges and row-count hints the
    /// request (`SetTableHandles`), which `buildTableReaderBase` sends at
    /// once -- the reader is never opened.
    ///
    /// # Errors
    /// The request could not be sent.
    pub(crate) fn build_table_reader_from_handles(
        e: TableReaderForHandles<'_>,
        handles: &mut [tidb_txnkv::Handle],
    ) -> Result<Self, ExecError> {
        handles.sort_by(|a, b| a.clone().compare(b).unwrap_or(std::cmp::Ordering::Equal));
        let (kv_ranges, hints) = tidb_distsql::table_handles_to_kv_ranges(e.physical_table_id, handles);
        let (virtual_column_index, virtual_column_ret_field_types) =
            crate::mpp_gather::build_virtual_column_info(e.meta.schema(), &e.columns);
        let mut reader = Self {
            ctx: e.ctx.clone(),
            store_type: tidb_kv::store_type::StoreType::TiKv,
            net_data_size: e.net_data_size,
            client: e.client,
            table_plan: e.table_plan.clone(),
            plan_ids: e.plan_ids,
            physical_table_id: e.physical_table_id,
            is_common_handle: e.is_common_handle,
            ranges: Vec::new(),
            range_rebuild: None,
            partitions: None,
            keep_order: false,
            desc: false,
            batch_cop: false,
            start_ts: e.start_ts,
            schema_version: 0,
            columns: e.columns,
            virtual_column_index,
            virtual_column_ret_field_types,
            results: std::collections::VecDeque::new(),
            cur: 0,
            kv_ranges: kv_ranges.clone(),
            paging: false,
            mem_tracker: None,
            dag_size: prost::Message::encoded_len(&e.dag),
            dummy: false,
            meta: e.meta,
        };
        let statement = crate::remote_scan::pushdown_statement_context(&reader.ctx)
            .with_estimated_net_bytes(reader.net_data_size);
        let result = reader.select_result(&statement, e.dag, kv_ranges, hints, Vec::new())?;
        reader.results.push_back(result);
        Ok(reader)
    }

    /// Go `TableReaderExecutor.memUsage()`: the executor, its `ranges`, its
    /// `kvRanges` (each key allocated exactly) and `dagPB.Size()`.
    pub(crate) fn mem_usage(&self) -> i64 {
        // Go `unsafe.Sizeof(TableReaderExecutor{})`, `size.SizeOfPointer`
        // and `unsafe.Sizeof(kv.KeyRange{})`.
        const SIZEOF_TABLE_READER_EXECUTOR: usize = 768;
        const SIZEOF_POINTER: usize = 8;
        const SIZEOF_KEY_RANGE: usize = 48;
        let ranges = self.ranges.len() * SIZEOF_POINTER
            + self.ranges.iter().map(|range| range.mem_usage() as usize).sum::<usize>();
        let key_bytes: usize = self.kv_ranges.iter().map(|range| range.start_key.len() + range.end_key.len()).sum();
        let kv_ranges_cap = if self.kv_ranges.is_empty() {
            0
        } else {
            tidb_goruntime::go_runtime::go_64_next_slice_capacity_for_element(
                self.kv_ranges.len(),
                0,
                SIZEOF_KEY_RANGE,
                tidb_goruntime::go_runtime::GoSliceElementLayout::PointerBearing,
            )
        };
        (SIZEOF_TABLE_READER_EXECUTOR + ranges + kv_ranges_cap * SIZEOF_KEY_RANGE + key_bytes + self.dag_size) as i64
    }

    /// Go `buildTableReader` for a `kv.TiKV` reader or a `kv.TiFlash` one
    /// `useMPPExecution` refuses: `buildNoRangeTableReader`, then the
    /// reader's ranges and, for
    /// a dynamically pruned partitioned table, its partitions -- a
    /// `TableDualExec` when none survive.
    ///
    /// # Errors
    /// A reader without exactly one table scan, a store without a client,
    /// or a failed partition pruning.
    pub fn build(
        meta: ExecutorMeta,
        ctx: &crate::StmtContext,
        is: &tidb_infoschema::SessionExtendedInfoSchema,
        v: &PhysicalTableReader,
        range_group: Option<&tidb_ranger::types::Ranges>,
    ) -> Result<Box<dyn Executor>, ExecError> {
        let dual = meta.clone();
        let Some(reader) = Self::build_no_range(meta, ctx, is, v, range_group)? else {
            return Ok(Box::new(crate::table_dual::TableDualExec::new(dual, 0)));
        };
        let by_items = v.table_plan.as_deref().and_then(|plan| {
            let mut scans = Vec::new();
            crate::mpp_gather::get_table_scans(plan, &mut scans);
            scans.first().map(|ts| ts.by_items.clone())
        });
        match (&reader.partitions, by_items) {
            // Go `buildKVReqSeparately` under `NewSortedSelectResults`: a
            // keep-order read of several TiKV partitions reads each partition
            // as its own request and merges them by the scan's `ByItems`.
            (Some(partitions), Some(by_items))
                if reader.store_type != tidb_kv::store_type::StoreType::TiFlash
                    && partitions.len() > 1
                    && !by_items.is_empty() =>
            {
                reader.sorted_partition_results(&by_items)
            }
            _ => Ok(Box::new(reader)),
        }
    }

    /// Go `buildNoRangeTableReader` and `buildTableReader`'s partition
    /// pruning: the reader, or `None` when no partition survives.
    ///
    /// # Errors
    /// As [`Self::build`].
    pub(crate) fn build_no_range(
        meta: ExecutorMeta,
        ctx: &crate::StmtContext,
        is: &tidb_infoschema::SessionExtendedInfoSchema,
        v: &PhysicalTableReader,
        range_group: Option<&tidb_ranger::types::Ranges>,
    ) -> Result<Option<Self>, ExecError> {
        let table_plan = v
            .table_plan
            .as_deref()
            .ok_or_else(|| ExecError::Internal("the table reader has no plan".into()))?;
        let mut table_scans = Vec::new();
        crate::mpp_gather::get_table_scans(table_plan, &mut table_scans);
        let [ts] = table_scans[..] else {
            return Err(ExecError::Internal("TableReader should only contain 1 table scan".into()));
        };
        let table = ts
            .table
            .as_ref()
            .ok_or_else(|| ExecError::Internal("the table reader's scan has no table".into()))?
            .read()
            .clone();
        let store = ctx
            .txn_store()
            .ok_or_else(|| ExecError::Internal("the table reader has no store".into()))?;
        let client = store
            .client()
            .ok_or_else(|| ExecError::Internal("the store has no coprocessor client".into()))?;
        // Go `getPhysicalPlanIDs(e.plans)`: a list request's summaries come
        // in `v.TablePlans` order, the scan first.
        let mut plan_ids = Vec::new();
        if v.store_type == tidb_kv::store_type::StoreType::TiFlash {
            crate::mpp_gather::collect_plan_ids(table_plan, &mut plan_ids);
        } else {
            plan_ids = crate::builder_utils::flatten_table_plan(table_plan)
                .into_iter()
                .map(|plan| plan.id() as isize)
                .collect();
        }
        let (virtual_column_index, virtual_column_ret_field_types) =
            crate::mpp_gather::build_virtual_column_info(meta.schema(), &ts.columns);
        // Go `ts.IsPartition()`: a statically pruned scan reads its partition.
        let is_partition = ts.table_id != table.id;
        let mut partitions = None;
        if !ctx.static_partition_prune() && !is_partition {
            if let Some(pi) = table.get_partition_info() {
                let info = v.plan_part_info.as_ref().ok_or_else(|| {
                    ExecError::Internal("a partitioned table reader has no partition info".into())
                })?;
                let partition_names: Vec<String> =
                    info.partition_names.iter().map(|name| name.original.clone()).collect();
                let idx_arr = crate::driver::planner_bridge::partition_pruning(
                    is,
                    table.id,
                    &info.pruning_conds,
                    &partition_names,
                    &info.columns,
                    ctx,
                )
                .map_err(|error| ExecError::Internal(error.to_string().into()))?;
                if idx_arr.is_empty() {
                    return Ok(None);
                }
                let definitions = pi.read().definitions.snapshot();
                // Sort the partition is necessary to make the final
                // multiple partition key ranges ordered.
                let mut pids: Vec<i64> = idx_arr.iter().map(|&idx| definitions[idx].id).collect();
                pids.sort_unstable();
                partitions = Some(pids);
            }
        }
        Ok(Some(Self {
            meta,
            ctx: ctx.clone(),
            store_type: v.store_type,
            net_data_size: table_plan.cop_net_data_size(
                false,
                tidb_config::tikvcfg::get_global_config().tikv_client.enable_chunk_rpc,
            ),
            client,
            table_plan: table_plan.clone(),
            plan_ids,
            physical_table_id: ts.table_id,
            is_common_handle: table.is_common_handle,
            // Go `buildRespForGroupedRanges`' one group, read as its own
            // reader under the grouped-range merge.
            ranges: range_group.unwrap_or(&ts.ranges).clone(),
            range_rebuild: crate::driver::physical_builder::correlated_table_range_rebuilder(ts, ctx),
            partitions,
            keep_order: ts.keep_order,
            desc: ts.desc,
            batch_cop: v.read_req_type == ReadReqType::BatchCop,
            start_ts: store.start_ts(),
            schema_version: is.metadata_version() as i64,
            columns: ts.columns.clone(),
            virtual_column_index,
            virtual_column_ret_field_types,
            results: std::collections::VecDeque::new(),
            cur: 0,
            kv_ranges: Vec::new(),
            paging: crate::remote_scan::pushdown_statement_context(ctx).paging,
            mem_tracker: None,
            dag_size: 0,
            dummy: false,
        }))
    }

    /// Go `buildTableReaderBase` over a reader `buildNoRangeTableReader`
    /// built: its request over `kv_ranges` (with `SetTableHandles`' hints
    /// when they come from handles) sent at once; the reader is never
    /// opened (`buildTableReaderFromHandles` / `buildTableReaderFromKvRanges`).
    ///
    /// # Errors
    /// The request could not be built or sent.
    pub(crate) fn build_table_reader_base(
        &mut self,
        kv_ranges: Vec<tidb_txnkv::KeyRange>,
        key_range_hints: Vec<usize>,
    ) -> Result<(), ExecError> {
        let statement = crate::remote_scan::pushdown_statement_context(&self.ctx)
            .with_estimated_net_bytes(self.net_data_size);
        let plans = crate::builder_utils::flatten_table_plan(&self.table_plan);
        let mut dag = crate::builder_utils::construct_dag_req(&self.ctx, &statement, &plans, self.store_type)
            .map_err(|error| ExecError::Internal(error.to_string().into()))?;
        dag.output_offsets = (0..self.meta.schema().columns.len() as u32).collect();
        self.dag_size = prost::Message::encoded_len(&dag);
        self.kv_ranges.clone_from(&kv_ranges);
        self.results.clear();
        self.cur = 0;
        let result = self.select_result(&statement, dag, kv_ranges, key_range_hints, Vec::new())?;
        self.results.push_back(result);
        Ok(())
    }

    /// Go `setDummy`.
    pub(crate) fn set_dummy(&mut self) {
        self.dummy = true;
    }

    /// Go `physicalTableID` and whether the table has a common handle.
    pub(crate) fn physical_table_id_and_common_handle(&self) -> (i64, bool) {
        (self.physical_table_id, self.is_common_handle)
    }

    /// Go `distsql.NewSortedSelectResults` over one reader a partition: the
    /// merge reads each key column of the reader's output.
    fn sorted_partition_results(
        self,
        by_items: &[tidb_expression_aggregation::ByItems],
    ) -> Result<Box<dyn Executor>, ExecError> {
        let schema = self.meta.schema().clone();
        let by_items = by_items
            .iter()
            .map(|item| {
                let tidb_expr::expression::Expression::Column(column) = &item.expr else {
                    return Err(ExecError::Internal("a sorted partition read orders by a non-column".into()));
                };
                // Go `buildKeyColumns`: `schema.ColumnIndex(col)`.
                let position = usize::try_from(schema.column_index(column))
                    .map_err(|_| ExecError::Internal("a sorted partition read's key is absent from its schema".into()))?;
                let mut column = schema.columns[position].clone();
                column.index = i64::try_from(position).unwrap_or(i64::MAX);
                Ok(tidb_executor_sortexec::SortByItem {
                    expr: tidb_expr::expression::Expression::Column(column),
                    desc: item.desc,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let partitions = self.partitions.clone().unwrap_or_default();
        let meta = self.meta.clone();
        let ctx = self.ctx.clone();
        let children = partitions
            .into_iter()
            .map(|pid| {
                Box::new(Self {
                    meta: self.meta.clone(),
                    ctx: self.ctx.clone(),
                    store_type: self.store_type,
                    net_data_size: self.net_data_size,
                    client: Arc::clone(&self.client),
                    table_plan: self.table_plan.clone(),
                    plan_ids: self.plan_ids.clone(),
                    physical_table_id: self.physical_table_id,
                    is_common_handle: self.is_common_handle,
                    ranges: self.ranges.clone(),
                    range_rebuild: self.range_rebuild.clone(),
                    partitions: Some(vec![pid]),
                    keep_order: self.keep_order,
                    desc: self.desc,
                    batch_cop: self.batch_cop,
                    start_ts: self.start_ts,
                    schema_version: self.schema_version,
                    columns: self.columns.clone(),
                    virtual_column_index: self.virtual_column_index.clone(),
                    virtual_column_ret_field_types: self.virtual_column_ret_field_types.clone(),
                    results: std::collections::VecDeque::new(),
                    cur: 0,
                    kv_ranges: Vec::new(),
                    paging: self.paging,
                    mem_tracker: None,
                    dag_size: 0,
                    dummy: false,
                }) as Box<dyn Executor>
            })
            .collect();
        Ok(Box::new(crate::range_group_merge::SortedSelectResults::new(meta, by_items, children, ctx)?))
    }

    /// Go `columns`.
    pub(crate) fn columns(&self) -> &[tidb_model::GoShared<tidb_model::ColumnInfo>] {
        &self.columns
    }

    /// Go `kvRanges`.
    #[must_use]
    pub fn kv_ranges(&self) -> &[tidb_txnkv::KeyRange] {
        &self.kv_ranges
    }

    /// Go `SelectResult` over one request of `dag`.
    fn select_result(
        &self,
        statement: &tidb_distsql::remote_scan::PushdownStatementContext,
        dag: tipb::DagRequest,
        key_ranges: Vec<tidb_txnkv::KeyRange>,
        key_range_hints: Vec<usize>,
        partition_id_and_ranges: Vec<tidb_txnkv::PartitionIdAndRanges>,
    ) -> Result<SelectResponseIter, ExecError> {
        crate::cop_reader::open_table_read(
            self.client.as_ref(),
            statement,
            crate::cop_reader::TableRead {
                store_type: self.store_type,
                dag,
                key_ranges,
                key_range_hints,
                partition_id_and_ranges,
                batch_cop: self.batch_cop,
                schema_version: self.schema_version,
                connection_id: tidb_exprctx::Columns::connection_id(&self.ctx).unwrap_or_default(),
                connection_alias: self.ctx.ddl_session_alias().to_owned(),
                start_ts: self.start_ts,
                keep_order: self.keep_order,
                desc: self.desc,
                // Go `paging`: the session's `EnablePaging`.
                paging: self.paging,
                field_types: self.meta.ret_field_types().to_vec(),
                cop_plan_ids: self.plan_ids.clone(),
                root_plan_id: isize::try_from(self.meta.id()).unwrap_or(0),
                // Go `memTracker`, `NewTracker(e.ID(), -1)` under the statement's.
                mem_tracker: self.mem_tracker.clone(),
            },
        )
        .map_err(|error| ExecError::Internal(error.into()))
        .inspect(|_| tidb_kv::storage::note_storage_op(|ops| ops.cop_scans += 1))
    }

    /// Go `buildRespForGroupedRanges` over the reader's one range group: a
    /// partitioned table's `buildKVReqSeparately` (one request a partition,
    /// read serially) or, for a batch request, its
    /// `buildKVReqForPartitionTableScan`; any other table's `buildKVReq`.
    fn build_resp(
        &mut self,
        statement: &tidb_distsql::remote_scan::PushdownStatementContext,
        dag: &tipb::DagRequest,
        ranges: &[tidb_ranger::types::Range],
    ) -> Result<(), ExecError> {
        let zone = &statement.time_zone;
        let Some(partitions) = self.partitions.clone() else {
            let key_ranges =
                table_handle_ranges_to_kv_ranges(zone, self.physical_table_id, self.is_common_handle, ranges)?;
            self.kv_ranges.extend(key_ranges.iter().cloned());
            let result = self.select_result(statement, dag.clone(), key_ranges, Vec::new(), Vec::new())?;
            self.results.push_back(result);
            return Ok(());
        };
        if self.store_type != tidb_kv::store_type::StoreType::TiFlash {
            // Go `buildKVReq` over `kvRangeBuilderFromRangeAndPartition`: one
            // request over every partition's ranges.
            let mut key_ranges = Vec::new();
            for pid in partitions {
                key_ranges.extend(table_handle_ranges_to_kv_ranges(zone, pid, self.is_common_handle, ranges)?);
            }
            self.kv_ranges.extend(key_ranges.iter().cloned());
            let result = self.select_result(statement, dag.clone(), key_ranges, Vec::new(), Vec::new())?;
            self.results.push_back(result);
            return Ok(());
        }
        let update = |dag: &mut tipb::DagRequest, pids: &[i64]| {
            crate::local_mpp_coordinator::update_executor_table_id(dag.root_executor.as_mut(), true, pids)
                .map_err(|error| ExecError::Internal(error.into()))
        };
        if !self.batch_cop {
            for pid in partitions {
                let key_ranges = table_handle_ranges_to_kv_ranges(zone, pid, self.is_common_handle, ranges)?;
                self.kv_ranges.extend(key_ranges.iter().cloned());
                let mut dag = dag.clone();
                update(&mut dag, &[pid])?;
                let result = self.select_result(statement, dag, key_ranges, Vec::new(), Vec::new())?;
                self.results.push_back(result);
            }
            return Ok(());
        }
        let mut partition_id_and_ranges = Vec::with_capacity(partitions.len());
        for &pid in &partitions {
            let key_ranges = table_handle_ranges_to_kv_ranges(zone, pid, self.is_common_handle, ranges)?;
            self.kv_ranges.extend(key_ranges.iter().cloned());
            partition_id_and_ranges.push(tidb_txnkv::PartitionIdAndRanges { id: pid, key_ranges });
        }
        let mut dag = dag.clone();
        update(&mut dag, &partitions)?;
        let result = self.select_result(statement, dag, Vec::new(), Vec::new(), partition_id_and_ranges)?;
        self.results.push_back(result);
        Ok(())
    }
}

/// Go `RuntimeStatsColl.GetCopCountAndRows(e.plans[0].ID())`, which the
/// reader's index usage report reads.
impl tidb_executor_internal_exec::table_access::TableAccess for TableReaderExecutor {
    fn cop_count_and_rows(&self) -> Option<(u64, u64)> {
        let scan_id = *self.plan_ids.first()?;
        Some(self.results.iter().fold((0, 0), |(requests, rows), result| {
            let (more_requests, more_rows) = result.runtime_stats().get_cop_count_and_rows(scan_id);
            (requests + more_requests, rows + more_rows)
        }))
    }
}

impl Executor for TableReaderExecutor {
    fn table_access(&mut self) -> Option<&mut dyn tidb_executor_internal_exec::table_access::TableAccess> {
        Some(self)
    }

    fn union_scan_reader(&mut self) -> Option<&mut dyn std::any::Any> {
        Some(self)
    }

    /// Go `TableReaderExecutor.Open`.
    fn open(&mut self) -> Result<(), ExecError> {
        let statement = crate::remote_scan::pushdown_statement_context(&self.ctx)
            .with_estimated_net_bytes(self.net_data_size);
        // Go `buildNoRangeTableReader`'s `ConstructDAGReq` over `v.TablePlans`
        // (the table plan itself for `kv.TiFlash`), rebuilt here, so a
        // correlated filter reads the outer row as `corColInFilter` does.
        let plans = if self.store_type == tidb_kv::store_type::StoreType::TiFlash {
            vec![&self.table_plan]
        } else {
            crate::builder_utils::flatten_table_plan(&self.table_plan)
        };
        let mut dag = crate::builder_utils::construct_dag_req(&self.ctx, &statement, &plans, self.store_type)
            .map_err(|error| ExecError::Internal(error.to_string().into()))?;
        dag.output_offsets = (0..self.meta.schema().columns.len() as u32).collect();
        self.dag_size = prost::Message::encoded_len(&dag);
        // Go `Open`: `memTracker` reset, or made, and attached to the
        // statement's.
        let tracker = match self.mem_tracker.take() {
            Some(tracker) => {
                tracker.reset();
                tracker.attach_to(statement.memory.stmt_tracker());
                tracker
            }
            None => statement.memory.operator_tracker(self.meta.id()),
        };
        self.mem_tracker = Some(tracker);
        let ranges = match &self.range_rebuild {
            Some(rebuild) => rebuild().map_err(|error| ExecError::Internal(format!("{error:?}").into()))?,
            None => self.ranges.clone(),
        };
        self.kv_ranges.clear();
        self.results.clear();
        self.cur = 0;
        // Treat temporary table as dummy table, avoid sending distsql request
        // to TiKV. In a test case IndexLookupJoin, there is a case that
        // TableReaderExecutor.Open is called with nil kvRanges.
        let (first_part_ranges, second_part_ranges) = tidb_distsql::split_ranges_across_int64_boundary(
            ranges,
            self.keep_order,
            self.desc,
            self.is_common_handle,
        );
        // Go: a dummy reader only calculates the KV ranges the union scan
        // relies on, the parts swapped for a descending read (the union scan
        // reads them forward and reverses its rows).
        if self.dummy {
            let (first, second) = if self.desc && !second_part_ranges.is_empty() {
                (second_part_ranges, first_part_ranges)
            } else {
                (first_part_ranges, second_part_ranges)
            };
            let zone = &statement.time_zone;
            for ranges in [first, second] {
                for pid in self.partitions.clone().unwrap_or_else(|| vec![self.physical_table_id]) {
                    self.kv_ranges
                        .extend(table_handle_ranges_to_kv_ranges(zone, pid, self.is_common_handle, &ranges)?);
                }
            }
            return Ok(());
        }
        self.build_resp(&statement, &dag, &first_part_ranges)?;
        if !second_part_ranges.is_empty() {
            self.build_resp(&statement, &dag, &second_part_ranges)?;
        }
        self.kv_ranges.sort_by(|a, b| a.start_key.cmp(&b.start_key));
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if self.dummy {
            return Ok(());
        }
        let required = req.required_rows().max(1);
        // Go `serialSelectResults.Next`: the results one after another; each
        // stays open until the reader's `Close`.
        while let Some(result) = self.results.get_mut(self.cur) {
            if !result
                .fill_chunk_with_required_rows(required, req)
                .map_err(|error| ExecError::from(crate::cop_reader::select_response_error(error)))?
            {
                req.reset();
                self.cur += 1;
                continue;
            }
            if req.num_rows() > 0 {
                let rows = req.num_rows() as u64;
                tidb_kv::storage::note_storage_op(|ops| ops.cop_rows += rows);
                return crate::mpp_gather::fill_virtual_column_value(
                    &self.ctx,
                    self.meta.schema(),
                    &self.columns,
                    &self.virtual_column_index,
                    &self.virtual_column_ret_field_types,
                    req,
                );
            }
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        let mut first_error = Ok(());
        for mut result in self.results.drain(..) {
            let closed = result
                .close()
                .map_err(|error| ExecError::from(crate::cop_reader::select_response_error(error)));
            if first_error.is_ok() {
                first_error = closed;
            }
        }
        first_error
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
