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
//! [`TiFlashTableReaderExecutor`] is the same executor for a reader on the
//! `kv.TiFlash` store that is not an MPP gather: a tree-based request, sent
//! as a batch coprocessor request (`batchCop`) or region by region (`cop`).

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

/// Go `TableReaderExecutor` over the `kv.TiDB` store.
pub struct TableReaderExecutor {
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

impl TableReaderExecutor {
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

impl Executor for TableReaderExecutor {
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

/// Go `TableReaderExecutor` over the `kv.TiFlash` store, for a reader that
/// is not an MPP gather.
pub struct TiFlashTableReaderExecutor {
    meta: ExecutorMeta,
    ctx: crate::StmtContext,
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
    /// Go `kvRanges`.
    kv_ranges: Vec<tidb_txnkv::KeyRange>,
}

impl TiFlashTableReaderExecutor {
    /// Go `buildTableReader` for a `kv.TiFlash` reader `useMPPExecution`
    /// refuses: `buildNoRangeTableReader`, then the reader's ranges and, for
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
    ) -> Result<Box<dyn Executor>, ExecError> {
        let table_plan = v
            .table_plan
            .as_deref()
            .ok_or_else(|| ExecError::Internal("the TiFlash reader has no plan".into()))?;
        let mut table_scans = Vec::new();
        crate::mpp_gather::get_table_scans(table_plan, &mut table_scans);
        let [ts] = table_scans[..] else {
            return Err(ExecError::Internal("TableReader should only contain 1 table scan".into()));
        };
        let table = ts
            .table
            .as_ref()
            .ok_or_else(|| ExecError::Internal("the TiFlash reader's scan has no table".into()))?
            .read()
            .clone();
        let store = ctx
            .txn_store()
            .ok_or_else(|| ExecError::Internal("the TiFlash reader has no store".into()))?;
        let client = store
            .client()
            .ok_or_else(|| ExecError::Internal("the store has no coprocessor client".into()))?;
        let mut plan_ids = Vec::new();
        crate::mpp_gather::collect_plan_ids(table_plan, &mut plan_ids);
        let (virtual_column_index, virtual_column_ret_field_types) =
            crate::mpp_gather::build_virtual_column_info(meta.schema(), &ts.columns);
        // Go `ts.IsPartition()`: a statically pruned scan reads its partition.
        let is_partition = ts.table_id != table.id;
        let mut partitions = None;
        if !ctx.static_partition_prune() && !is_partition {
            if let Some(pi) = table.get_partition_info() {
                let info = v.plan_part_info.as_ref().ok_or_else(|| {
                    ExecError::Internal("a partitioned TiFlash reader has no partition info".into())
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
                    return Ok(Box::new(crate::table_dual::TableDualExec::new(meta, 0)));
                }
                let definitions = pi.read().definitions.snapshot();
                // Sort the partition is necessary to make the final
                // multiple partition key ranges ordered.
                let mut pids: Vec<i64> = idx_arr.iter().map(|&idx| definitions[idx].id).collect();
                pids.sort_unstable();
                partitions = Some(pids);
            }
        }
        Ok(Box::new(Self {
            meta,
            ctx: ctx.clone(),
            client,
            table_plan: table_plan.clone(),
            plan_ids,
            physical_table_id: ts.table_id,
            is_common_handle: table.is_common_handle,
            ranges: ts.ranges.clone(),
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
            kv_ranges: Vec::new(),
        }))
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
        partition_id_and_ranges: Vec<tidb_txnkv::PartitionIdAndRanges>,
    ) -> Result<SelectResponseIter, ExecError> {
        crate::cop_reader::open_tiflash_read(
            self.client.as_ref(),
            statement,
            crate::cop_reader::TiFlashRead {
                dag,
                key_ranges,
                partition_id_and_ranges,
                batch_cop: self.batch_cop,
                schema_version: self.schema_version,
                connection_id: tidb_exprctx::Columns::connection_id(&self.ctx).unwrap_or_default(),
                connection_alias: self.ctx.ddl_session_alias().to_owned(),
                start_ts: self.start_ts,
                keep_order: self.keep_order,
                desc: self.desc,
                field_types: self.meta.ret_field_types().to_vec(),
                cop_plan_ids: self.plan_ids.clone(),
                root_plan_id: isize::try_from(self.meta.id()).unwrap_or(0),
            },
        )
        .map_err(|error| ExecError::Internal(error.into()))
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
            let result = self.select_result(statement, dag.clone(), key_ranges, Vec::new())?;
            self.results.push_back(result);
            return Ok(());
        };
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
                let result = self.select_result(statement, dag, key_ranges, Vec::new())?;
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
        let result = self.select_result(statement, dag, Vec::new(), partition_id_and_ranges)?;
        self.results.push_back(result);
        Ok(())
    }
}

impl Executor for TiFlashTableReaderExecutor {
    /// Go `TableReaderExecutor.Open`.
    fn open(&mut self) -> Result<(), ExecError> {
        let statement = crate::remote_scan::pushdown_statement_context(&self.ctx);
        // Go `buildNoRangeTableReader`'s `ConstructDAGReq` (rebuilt here, so
        // a correlated filter reads the outer row as `corColInFilter` does).
        let mut dag = crate::local_mpp_coordinator::construct_dag_req(&self.ctx, &statement, &self.table_plan)
            .map_err(|error| ExecError::Internal(error.into()))?;
        // Go `distsql.SetEncodeType`.
        if tidb_config::tikvcfg::get_global_config().tikv_client.enable_chunk_rpc {
            dag.encode_type = Some(tipb::EncodeType::TypeChunk as i32);
        } else {
            dag.encode_type = Some(tipb::EncodeType::TypeDefault as i32);
            dag.chunk_memory_layout = None;
        }
        dag.output_offsets = (0..self.meta.schema().columns.len() as u32).collect();
        let ranges = match &self.range_rebuild {
            Some(rebuild) => rebuild().map_err(|error| ExecError::Internal(format!("{error:?}").into()))?,
            None => self.ranges.clone(),
        };
        self.kv_ranges.clear();
        self.results.clear();
        // Treat temporary table as dummy table, avoid sending distsql request
        // to TiKV. In a test case IndexLookupJoin, there is a case that
        // TableReaderExecutor.Open is called with nil kvRanges.
        let (first_part_ranges, second_part_ranges) = tidb_distsql::split_ranges_across_int64_boundary(
            ranges,
            self.keep_order,
            self.desc,
            self.is_common_handle,
        );
        self.build_resp(&statement, &dag, &first_part_ranges)?;
        if !second_part_ranges.is_empty() {
            self.build_resp(&statement, &dag, &second_part_ranges)?;
        }
        self.kv_ranges.sort_by(|a, b| a.start_key.cmp(&b.start_key));
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        let required = req.required_rows().max(1);
        while let Some(result) = self.results.front_mut() {
            if !result
                .fill_chunk_with_required_rows(required, req)
                .map_err(|error| ExecError::from(crate::cop_reader::select_response_error(error)))?
            {
                req.reset();
                if let Some(mut finished) = self.results.pop_front() {
                    finished
                        .close()
                        .map_err(|error| ExecError::from(crate::cop_reader::select_response_error(error)))?;
                }
                continue;
            }
            if req.num_rows() > 0 {
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
