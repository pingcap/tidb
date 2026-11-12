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
    /// Go `corColInAccess`: the ranges rebuilt over the outer row
    /// (`rebuildIndexRanges`).
    range_rebuild: Option<crate::access_path::RangeRebuilder>,
    /// Go `kvRanges` this reader reads instead of its ranges': one key range
    /// of a merged keep-order read.
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
    /// Go `result`.
    result: Option<SelectResponseIter>,
    /// Go `NewSortedSelectResults`: one reader a key range, merged.
    merged: Option<Box<dyn Executor>>,
    /// Go `kvRanges`.
    kv_ranges: Vec<tidb_txnkv::KeyRange>,
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
        range_group: Option<&tidb_ranger::types::Ranges>,
    ) -> Result<Box<dyn Executor>, ExecError> {
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
        // Go `buildIndexReader`: a dynamically pruned partitioned table reads
        // its pruned partitions (a global index reads the table itself).
        if !ctx.static_partition_prune() && !global && scan.table_id == table_info.id {
            if let Some(pi) = table_info.get_partition_info() {
                let info = v.plan_part_info.as_ref().ok_or_else(|| {
                    ExecError::Internal("a partitioned index reader has no partition info".into())
                })?;
                let partition_names: Vec<String> =
                    info.partition_names.iter().map(|name| name.original.clone()).collect();
                let idx_arr = crate::driver::planner_bridge::partition_pruning(
                    is,
                    table_info.id,
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
                table_ids = idx_arr.iter().map(|&idx| definitions[idx].id).collect();
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
        Ok(Box::new(Self {
            meta,
            ctx: ctx.clone(),
            client,
            index_plan: index_plan.clone(),
            plan_ids: plans.iter().map(|plan| plan.id() as isize).collect(),
            table_ids,
            index_id: scan.index_id,
            ranges: range_group.unwrap_or(&scan.ranges).clone(),
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
            merged: None,
            kv_ranges: Vec::new(),
        }))
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
        let zone = tidb_exprctx::Columns::time_zone(&self.ctx);
        let kv_ranges = tidb_distsql::index_ranges_to_kv_ranges(Some(&zone), &self.table_ids, self.index_id, &ranges)
            .map_err(|error| ExecError::Internal(format!("{error:?}").into()))?;
        Ok(kv_ranges.into_iter().flatten().collect())
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
                partition_id_and_ranges: Vec::new(),
                batch_cop: false,
                schema_version: 0,
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
        .inspect(|_| tidb_kv::storage::note_storage_op(|ops| ops.cop_scans += 1))
    }

    /// One reader of the merge, reading `kv_range` alone.
    fn reader_of(&self, kv_range: tidb_txnkv::KeyRange) -> Box<dyn Executor> {
        Box::new(Self {
            meta: self.meta.clone(),
            ctx: self.ctx.clone(),
            client: Arc::clone(&self.client),
            index_plan: self.index_plan.clone(),
            plan_ids: self.plan_ids.clone(),
            table_ids: self.table_ids.clone(),
            index_id: self.index_id,
            ranges: Vec::new(),
            range_rebuild: None,
            fixed_kv_ranges: Some(vec![kv_range]),
            keep_order: self.keep_order,
            desc: self.desc,
            by_items: Vec::new(),
            start_ts: self.start_ts,
            output_offsets: self.output_offsets.clone(),
            net_data_size: self.net_data_size,
            result: None,
            merged: None,
            kv_ranges: Vec::new(),
        })
    }
}

/// Go `RuntimeStatsColl.GetCopCountAndRows(e.plans[0].ID())`, which the
/// reader's index usage report reads.
impl tidb_executor_internal_exec::table_access::TableAccess for IndexReaderExecutor {
    fn cop_count_and_rows(&self) -> Option<(u64, u64)> {
        let scan_id = *self.plan_ids.first()?;
        Some(
            self.result
                .as_ref()
                .map_or((0, 0), |result| result.runtime_stats().get_cop_count_and_rows(scan_id)),
        )
    }
}

impl Executor for IndexReaderExecutor {
    fn table_access(&mut self) -> Option<&mut dyn tidb_executor_internal_exec::table_access::TableAccess> {
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
        kv_ranges.sort_by(|a, b| a.start_key.cmp(&b.start_key));
        self.kv_ranges.clone_from(&kv_ranges);
        self.result = None;
        self.merged = None;
        // Go `needMergeSort(e.byItems, len(kvRanges))`.
        if !self.by_items.is_empty() && kv_ranges.len() > 1 {
            let children = kv_ranges.into_iter().map(|kv_range| self.reader_of(kv_range)).collect();
            let mut merged = crate::range_group_merge::SortedSelectResults::new(
                self.meta.clone(),
                self.by_items.clone(),
                children,
                self.ctx.clone(),
            )?;
            merged.open()?;
            self.merged = Some(Box::new(merged));
            return Ok(());
        }
        self.result = Some(self.select_result(&statement, dag, kv_ranges)?);
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        if let Some(merged) = self.merged.as_mut() {
            return merged.next(req);
        }
        req.reset();
        let Some(result) = self.result.as_mut() else {
            return Ok(());
        };
        let required = req.required_rows().max(1);
        if !result
            .fill_chunk_with_required_rows(required, req)
            .map_err(|error| ExecError::from(crate::cop_reader::select_response_error(error)))?
        {
            req.reset();
            return Ok(());
        }
        let rows = req.num_rows() as u64;
        tidb_kv::storage::note_storage_op(|ops| ops.cop_rows += rows);
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        if let Some(mut merged) = self.merged.take() {
            merged.close()?;
        }
        self.kv_ranges.clear();
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
