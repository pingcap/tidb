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

//! Go `IndexReaderExecutor` (`pkg/executor/distsql.go`) over the `kv.TiKV`
//! store: the reader's index plans lowered by `ConstructDAGReq`, its index
//! ranges' key ranges, one `distsql.Select` -- or, for a keep-order read of
//! several key ranges, one a range merged by the scan's `ByItems`.

use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use tidb_distsql::select_response::SelectResponseIter;
use tidb_driver_error::ExecError;
use tidb_executor_internal_exec::{Executor, ExecutorMeta};
use tidb_expr::schema::Schema;
use tidb_planner_core_operator_physicalop::{PhysicalIndexReader, PhysicalPlan};

use crate::range_group_merge::ReaderResult;

/// Go `getPartitionIDsAfterPruning` (with `partitionPruning`'s ordinals
/// mapped to the definitions' IDs): the partitions a dynamically pruned
/// reader of `table_info` reaches, in definition order.
///
/// # Errors
/// The reader has no partition info, or pruning fails.
pub(crate) fn get_partition_ids_after_pruning(
    is: &tidb_infoschema::SessionExtendedInfoSchema,
    table_info: &tidb_model::TableInfo,
    info: Option<&tidb_planner_core_operator_physicalop::PhysPlanPartInfo>,
    ctx: &crate::StmtContext,
) -> Result<Vec<i64>, ExecError> {
    let info = info.ok_or_else(|| {
        ExecError::Internal("physPlanPartInfo in getPartitionIDsAfterPruning must not be nil".into())
    })?;
    let partition_names: Vec<String> = info.partition_names.iter().map(|name| name.original.clone()).collect();
    let idx_arr = crate::driver::planner_bridge::partition_pruning(
        is,
        table_info.id,
        &info.pruning_conds,
        &partition_names,
        &info.columns,
        ctx,
    )
    .map_err(|error| ExecError::Internal(error.to_string().into()))?;
    let Some(pi) = table_info.get_partition_info() else {
        return Ok(Vec::new());
    };
    let definitions = pi.read().definitions.snapshot();
    Ok(idx_arr.iter().map(|&idx| definitions[idx].id).collect())
}

/// Go `IndexReaderExecutor`.
pub struct IndexReaderExecutor {
    meta: ExecutorMeta,
    ctx: crate::StmtContext,
    /// Go `dctx.Client`.
    client: Arc<dyn tidb_txnkv::Client>,
    /// Go `plans` (`v.IndexPlans`), the index scan first.
    index_plan: PhysicalPlan,
    /// Go `getPhysicalPlanIDs(e.plans)`.
    plan_ids: Vec<isize>,
    /// Go `physicalTableID`, or the pruned partitions' ids (Go
    /// `partitions`), ascending.
    table_ids: Vec<i64>,
    /// Go `index.ID`.
    index_id: i64,
    /// Go `ranges`.
    ranges: Vec<tidb_ranger::types::Range>,
    /// Go `is.GroupByColIdxs`: the columns whose values group `ranges` into
    /// `groupedRanges`.
    group_by_col_idxs: Vec<usize>,
    /// Go `corColInAccess`: the ranges rebuilt over the outer row
    /// (`rebuildIndexRanges`).
    range_rebuild: Option<crate::access_path::RangeRebuilder>,
    /// Go `open(ctx, kvRanges)`'s key ranges, which its caller built
    /// (`buildIndexReaderForIndexJoin`).
    fixed_kv_ranges: Option<Vec<tidb_txnkv::KeyRange>>,
    keep_order: bool,
    desc: bool,
    /// Go `byItems`.
    by_items: Vec<tidb_executor_sortexec::SortByItem>,
    /// Go `startTS`.
    start_ts: u64,
    /// Go `dagReq.OutputOffsets`: `v.OutputColumns`' offsets in the index
    /// plans' output.
    output_offsets: Vec<u32>,
    /// Go `netDataSize`.
    net_data_size: f64,
    /// Go `result`: one request's, or `NewSortedSelectResults` over one
    /// request a key range.
    result: Option<ReaderResult>,
    /// Go `memTracker`, made on `open`.
    mem_tracker: Option<Arc<tidb_memory::Tracker>>,
    /// Go `partitionIDMap`: a global index's pruned partitions.
    partition_id_map: std::collections::BTreeSet<i64>,
    /// Go `kvRanges`.
    kv_ranges: Vec<tidb_txnkv::KeyRange>,
    /// Go `dummy`: not a real reader, only the KV ranges a union scan reads
    /// the cached or temporary table's entries over (`setDummy`).
    dummy: bool,
}

impl IndexReaderExecutor {
    /// Go `buildIndexReader` for a `kv.TiKV` reader: `buildNoRangeIndexReader`,
    /// then, for a dynamically pruned partitioned table, its partitions -- a
    /// `TableDualExec` when none survive.
    ///
    /// # Errors
    /// A reader without an index scan, a store without a client, or a
    /// failed partition pruning.
    pub fn build(
        meta: ExecutorMeta,
        ctx: &crate::StmtContext,
        is: &tidb_infoschema::SessionExtendedInfoSchema,
        v: &PhysicalIndexReader,
    ) -> Result<Box<dyn Executor>, ExecError> {
        let dual = meta.clone();
        Ok(match Self::build_no_range(meta, ctx, is, v)? {
            Some(reader) => Box::new(reader),
            None => Box::new(crate::table_dual::TableDualExec::new(dual, 0)),
        })
    }

    /// Go `buildNoRangeIndexReader` and `buildIndexReader`'s partition
    /// pruning: the reader, or `None` when no partition survives.
    ///
    /// # Errors
    /// As [`Self::build`].
    pub(crate) fn build_no_range(
        meta: ExecutorMeta,
        ctx: &crate::StmtContext,
        is: &tidb_infoschema::SessionExtendedInfoSchema,
        v: &PhysicalIndexReader,
    ) -> Result<Option<Self>, ExecError> {
        let index_plan = v
            .index_plan
            .as_deref()
            .ok_or_else(|| ExecError::Internal("the index reader has no plan".into()))?;
        let plans = crate::builder_utils::flatten_table_plan(index_plan);
        let PhysicalPlan::IndexScan(scan) = plans[0] else {
            return Err(ExecError::Internal("the index reader's plans start with no index scan".into()));
        };
        let store = ctx
            .txn_store()
            .ok_or_else(|| ExecError::Internal("the index reader has no store".into()))?;
        let client = store
            .client()
            .ok_or_else(|| ExecError::Internal("the store has no coprocessor client".into()))?;
        let table = is
            .physical_kv_table_by_id(scan.table_id)
            .ok_or_else(|| ExecError::Internal("the index reader's table is absent from the catalog".into()))?;
        let table_info = table.meta().read().clone();
        let global = table_info
            .indices
            .iter_deref()
            .find(|index| index.read().id == scan.index_id)
            .is_some_and(|index| index.read().global);
        let mut table_ids = vec![scan.table_id];
        let mut partition_id_map = std::collections::BTreeSet::new();
        // Go `buildIndexReader`: a dynamically pruned partitioned table reads
        // its pruned partitions; a global index reads the table itself and
        // keeps its entries of the pruned partitions (`partitionIDMap`).
        if !ctx.static_partition_prune() && scan.table_id == table_info.id && table_info.get_partition_info().is_some() {
            let pids = get_partition_ids_after_pruning(is, &table_info, v.plan_part_info.as_deref(), ctx)?;
            if global {
                partition_id_map = pids.into_iter().collect();
            } else if pids.is_empty() {
                return Ok(None);
            } else {
                table_ids = pids;
            }
        }
        let schema = index_plan
            .schema()
            .ok_or_else(|| ExecError::Internal("the index plans have no schema".into()))?;
        let output_offsets = v
            .output_columns
            .iter()
            .map(|output| {
                // Go `ResolveIndices` -> `Schema.ColumnIndex`, which passes over
                // a prefix index column for the full one (a clustered table's
                // common handle carries it).
                u32::try_from(schema.column_index(output))
                    .map_err(|_| ExecError::Internal("an index reader output column is absent from its plans".into()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let by_items = scan
            .by_items
            .iter()
            .map(|item| {
                let tidb_expr::expression::Expression::Column(column) = &item.expr else {
                    return Err(ExecError::Internal("an index reader orders by a non-column".into()));
                };
                // Go `buildKeyColumns`: `schema.ColumnIndex(col)` of the
                // reader's output.
                let output = meta.schema();
                let position = usize::try_from(output.column_index(column))
                    .map_err(|_| ExecError::Internal("an index reader's order key is absent from its output".into()))?;
                let mut column = output.columns[position].clone();
                column.index = i64::try_from(position).unwrap_or(i64::MAX);
                Ok(tidb_executor_sortexec::SortByItem {
                    expr: tidb_expr::expression::Expression::Column(column),
                    desc: item.desc,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some(Self {
            meta,
            ctx: ctx.clone(),
            client,
            index_plan: index_plan.clone(),
            plan_ids: plans.iter().map(|plan| plan.id() as isize).collect(),
            table_ids,
            index_id: scan.index_id,
            ranges: scan.ranges.clone(),
            group_by_col_idxs: scan.group_by_col_idxs.clone(),
            range_rebuild: crate::driver::physical_builder::correlated_index_range_rebuilder(scan, ctx),
            fixed_kv_ranges: None,
            keep_order: scan.keep_order,
            desc: scan.desc,
            by_items,
            start_ts: store.start_ts(),
            output_offsets,
            net_data_size: index_plan.cop_net_data_size(
                true,
                tidb_config::tikvcfg::get_global_config().tikv_client.enable_chunk_rpc,
            ),
            result: None,
            mem_tracker: None,
            partition_id_map,
            kv_ranges: Vec::new(),
            // Go: a temporary table's reader is a dummy; its rows are the
            // union scan's.
            dummy: table_info.temp_table_type != tidb_model::TempTableType::NONE,
        }))
    }

    /// Go `IndexReaderExecutor.open(ctx, kvRanges)` over the key ranges its
    /// caller built (`buildIndexReaderForIndexJoin`).
    ///
    /// # Errors
    /// As [`Executor::open`].
    pub(crate) fn open_with_kv_ranges(&mut self, kv_ranges: Vec<tidb_txnkv::KeyRange>) -> Result<(), ExecError> {
        self.fixed_kv_ranges = Some(kv_ranges);
        self.open()
    }

    /// Go `setDummy`.
    pub(crate) fn set_dummy(&mut self) {
        self.dummy = true;
    }

    /// Go `kvRanges` and `outputColumns`' indices (`buildMemIndexReader`).
    pub(crate) fn kv_ranges_and_output_offsets(&self) -> (&[tidb_txnkv::KeyRange], &[u32]) {
        (&self.kv_ranges, &self.output_offsets)
    }

    /// Go `partitionIDMap`, which the union scan above reads.
    pub(crate) fn partition_id_map(&self) -> &std::collections::BTreeSet<i64> {
        &self.partition_id_map
    }

    /// Go `physicalTableID` and `index.ID`.
    pub(crate) fn physical_table_id_and_index_id(&self) -> (i64, i64) {
        (self.table_ids[0], self.index_id)
    }

    /// Go `buildKVRangesForIndexReader`.
    fn build_kv_ranges(&self) -> Result<Vec<tidb_txnkv::KeyRange>, ExecError> {
        if let Some(fixed) = &self.fixed_kv_ranges {
            return Ok(fixed.clone());
        }
        let ranges = match &self.range_rebuild {
            Some(rebuild) => rebuild().map_err(|error| ExecError::Internal(format!("{error:?}").into()))?,
            None => self.ranges.clone(),
        };
        // Go `groupedRanges`, rebuilt with the ranges (`GroupRangesByCols`),
        // or the ranges as one group.
        let grouped_ranges = if self.group_by_col_idxs.is_empty() {
            vec![ranges]
        } else {
            tidb_planner::find_best_task::dispatch::group_ranges_by_cols(&ranges, &self.group_by_col_idxs)
                .map_err(|error| ExecError::Internal(error.into()))?
        };
        let zone = tidb_exprctx::Columns::time_zone(&self.ctx);
        let mut results = Vec::new();
        for ranges in &grouped_ranges {
            let kv_ranges = tidb_distsql::index_ranges_to_kv_ranges(Some(&zone), &self.table_ids, self.index_id, ranges)
                .map_err(|error| ExecError::Internal(format!("{error:?}").into()))?;
            results.extend(kv_ranges.into_iter().flatten());
        }
        Ok(results)
    }

    /// Go `SelectResult` over the key ranges `kv_ranges`.
    fn select_result(
        &self,
        statement: &tidb_distsql::remote_scan::PushdownStatementContext,
        dag: tidb_proto::tipb::DagRequest,
        kv_ranges: Vec<tidb_txnkv::KeyRange>,
    ) -> Result<SelectResponseIter, ExecError> {
        crate::cop_reader::open_table_read(
            self.client.as_ref(),
            statement,
            crate::cop_reader::TableRead {
                store_type: tidb_kv::store_type::StoreType::TiKv,
                dag,
                key_ranges: kv_ranges,
                key_range_hints: Vec::new(),
                partition_id_and_ranges: Vec::new(),
                batch_cop: false,
                schema_version: 0,
                connection_id: tidb_exprctx::Columns::connection_id(&self.ctx).unwrap_or_default(),
                connection_alias: self.ctx.ddl_session_alias().to_owned(),
                start_ts: self.start_ts,
                keep_order: self.keep_order,
                desc: self.desc,
                // Go `paging`: the session's `EnablePaging`.
                paging: statement.paging,
                field_types: self.meta.ret_field_types().to_vec(),
                cop_plan_ids: self.plan_ids.clone(),
                root_plan_id: isize::try_from(self.meta.id()).unwrap_or(0),
                mem_tracker: self.mem_tracker.clone(),
            },
        )
        .map_err(|error| ExecError::Internal(error.into()))
        .inspect(|_| tidb_kv::storage::note_storage_op(|ops| ops.cop_scans += 1))
    }
}

/// Go `RuntimeStatsColl.GetCopCountAndRows(e.plans[0].ID())`, which the
/// reader's index usage report reads.
impl tidb_executor_internal_exec::table_access::TableAccess for IndexReaderExecutor {
    fn cop_count_and_rows(&self) -> Option<(u64, u64)> {
        let scan_id = *self.plan_ids.first()?;
        Some(self.result.iter().flat_map(ReaderResult::select_results).fold((0, 0), |(requests, rows), result| {
            let (more_requests, more_rows) = result.runtime_stats().get_cop_count_and_rows(scan_id);
            (requests + more_requests, rows + more_rows)
        }))
    }
}

impl Executor for IndexReaderExecutor {
    fn table_access(&mut self) -> Option<&mut dyn tidb_executor_internal_exec::table_access::TableAccess> {
        Some(self)
    }

    fn union_scan_reader(&mut self) -> Option<&mut dyn std::any::Any> {
        Some(self)
    }

    /// Go `IndexReaderExecutor.Open` / `open`.
    fn open(&mut self) -> Result<(), ExecError> {
        let statement = crate::remote_scan::pushdown_statement_context(&self.ctx)
            .with_estimated_net_bytes(self.net_data_size);
        let mut kv_ranges = self.build_kv_ranges()?;
        // Go `ConstructDAGReq(b.sctx, v.IndexPlans, kv.TiKV)`, rebuilt here,
        // so a correlated filter reads the outer row as `corColInFilter` does.
        let plans = crate::builder_utils::flatten_table_plan(&self.index_plan);
        let mut dag = crate::builder_utils::construct_dag_req(
            &self.ctx,
            &statement,
            &plans,
            tidb_kv::store_type::StoreType::TiKv,
        )
        .map_err(|error| ExecError::Internal(error.to_string().into()))?;
        dag.output_offsets.clone_from(&self.output_offsets);
        self.result = None;
        // Treat temporary table as dummy table, avoid sending distsql request
        // to TiKV (its KV ranges are left in build order).
        if self.dummy {
            self.kv_ranges = kv_ranges;
            return Ok(());
        }
        // Go `memTracker`, reset or made (`NewTracker(e.ID(), -1)`), under
        // the statement's.
        self.mem_tracker = match self.mem_tracker.take() {
            Some(tracker) => {
                tracker.reset();
                tracker.attach_to(statement.memory.stmt_tracker());
                Some(tracker)
            }
            None => (self.meta.id() > 0).then(|| statement.memory.operator_tracker(self.meta.id())),
        };
        kv_ranges.sort_by(|a, b| a.start_key.cmp(&b.start_key));
        self.kv_ranges.clone_from(&kv_ranges);
        // Go `needMergeSort(e.byItems, len(kvRanges))`: one request a key
        // range, merged by `byItems`.
        if !self.by_items.is_empty() && kv_ranges.len() > 1 {
            let results = kv_ranges
                .into_iter()
                .map(|kv_range| self.select_result(&statement, dag.clone(), vec![kv_range]))
                .collect::<Result<Vec<_>, _>>()?;
            let merged = crate::range_group_merge::SortedSelectResults::new(
                self.meta.clone(),
                self.by_items.clone(),
                results,
                self.ctx.clone(),
            )?
            .with_mem_tracker(self.mem_tracker.clone());
            self.result = Some(ReaderResult::Sorted(merged));
            return Ok(());
        }
        self.result = Some(ReaderResult::One(self.select_result(&statement, dag, kv_ranges)?));
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if self.dummy {
            return Ok(());
        }
        let Some(result) = self.result.as_mut() else {
            return Ok(());
        };
        result.next(req)?;
        let rows = req.num_rows() as u64;
        tidb_kv::storage::note_storage_op(|ops| ops.cop_rows += rows);
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.kv_ranges.clear();
        match self.result.take() {
            Some(mut result) => result.close(),
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
