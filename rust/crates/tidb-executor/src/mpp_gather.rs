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

//! Go `pkg/executor/mpp_gather.go`: `MPPGather` dispatches a TiFlash
//! reader's fragments through the local coordinator
//! ([`crate::local_mpp_coordinator`]) under Go's `ExecutorWithRetry`, and
//! reads the root tasks' rows.

use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use tidb_distsql::mpp_result_metadata;
use tidb_distsql::select_response::SelectResponseIter;
use tidb_driver_error::ExecError;
use tidb_executor_internal_exec::{Executor, ExecutorMeta};
use tidb_expr::schema::Schema;
use tidb_planner_core_operator_physicalop::{PhysicalPlan, PhysicalTableReader};
use tidb_txnkv::{KeyRange, MppClient, MppQueryId, QueryResponseError};

use crate::local_mpp_coordinator::LocalMppCoordinator;
use crate::mpp_coordinator::{ExecutorWithRetry, MppGatherResponse};
use crate::mppcoordmanager::INSTANCE_MPP_COORDINATOR_MANAGER;

/// Go `useMPPExecution`: the session allows MPP and the reader's plan is an
/// MPP fragment's sender.
#[must_use]
pub fn use_mpp_execution(ctx: &crate::StmtContext, tr: &PhysicalTableReader) -> bool {
    if !ctx.cost_session().mpp_allowed {
        return false;
    }
    matches!(tr.table_plan.as_deref(), Some(PhysicalPlan::ExchangeSender(_)))
}

/// Go `collectPlanIDs`.
pub(crate) fn collect_plan_ids(plan: &PhysicalPlan, ids: &mut Vec<isize>) {
    ids.push(plan.id() as isize);
    for child in plan.children() {
        collect_plan_ids(child, ids);
    }
}

/// Go `MPPGather`.
pub struct MppGather {
    meta: ExecutorMeta,
    ctx: crate::StmtContext,
    /// Go `is`.
    is: tidb_infoschema::SessionExtendedInfoSchema,
    client: Arc<dyn MppClient>,
    original_plan: PhysicalPlan,
    start_ts: u64,
    mpp_query_id: MppQueryId,
    resp_iter: Option<SelectResponseIter>,
    /// Go `columns`: the single table scan's columns.
    columns: Vec<tidb_model::GoShared<tidb_model::ColumnInfo>>,
    /// Go `virtualColumnIndex`.
    virtual_column_index: Vec<usize>,
    /// Go `virtualColumnRetFieldTypes`.
    virtual_column_ret_field_types: Vec<FieldType>,
    /// Go `kvRanges`: what a `UnionScan` above reads the mem buffer with.
    kv_ranges: Vec<KeyRange>,
    exhausted: bool,
}

/// Go `PhysicalTableReader.GetTableScans`: the table scans below the plan.
pub(crate) fn get_table_scans<'p>(plan: &'p PhysicalPlan, scans: &mut Vec<&'p tidb_planner_core_operator_physicalop::PhysicalTableScan>) {
    if let PhysicalPlan::TableScan(scan) = plan {
        scans.push(scan);
    }
    for child in plan.children() {
        get_table_scans(child, scans);
    }
}

/// Go `buildVirtualColumnInfo`: the schema's virtual columns ordered by their
/// column offsets, and their types.
pub(crate) fn build_virtual_column_info(
    schema: &Schema,
    cols: &[tidb_model::GoShared<tidb_model::ColumnInfo>],
) -> (Vec<usize>, Vec<FieldType>) {
    let mut virtual_column_index: Vec<usize> =
        schema.columns.iter().enumerate().filter(|(_, col)| col.virtual_expr.is_some()).map(|(i, _)| i).collect();
    // Go `FindColumnInfoByID(cols, schema.Columns[i].ID).Offset`.
    let offset = |i: usize| {
        cols.iter()
            .map(|col| col.read())
            .find(|col| col.id == schema.columns[i].id)
            .map_or(0, |col| col.offset)
    };
    virtual_column_index.sort_by_key(|i| offset(*i));
    let ret_types = virtual_column_index
        .iter()
        .map(|idx| {
            schema.columns[*idx]
                .ret_type
                .clone()
                .unwrap_or_else(|| FieldType::new(tidb_datatype::FieldTypeCode::Null))
        })
        .collect();
    (virtual_column_index, ret_types)
}

/// What Go's `checkUseMPP` and `mpp_coordinator_execute_err` failpoints
/// replace.
#[cfg(any(test, feature = "test-util"))]
pub mod mock {
    use std::sync::{Mutex, PoisonError};

    static CHECK_USE_MPP: Mutex<Option<bool>> = Mutex::new(None);
    static MPP_COORDINATOR_EXECUTE_ERR: Mutex<bool> = Mutex::new(false);

    /// Go `failpoint.Enable("checkUseMPP", "return(v)")`: whether every
    /// table reader a user statement builds must be an MPP gather.
    pub fn set_check_use_mpp(use_mpp: Option<bool>) {
        *CHECK_USE_MPP.lock().unwrap_or_else(PoisonError::into_inner) = use_mpp;
    }

    pub(crate) fn check_use_mpp() -> Option<bool> {
        *CHECK_USE_MPP.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Go `failpoint.Enable("mpp_coordinator_execute_err", "return()")`: the
    /// coordinator's `Execute` answers `mock mpp error`.
    pub fn set_mpp_coordinator_execute_err(enabled: bool) {
        *MPP_COORDINATOR_EXECUTE_ERR.lock().unwrap_or_else(PoisonError::into_inner) = enabled;
    }

    pub(super) fn mpp_coordinator_execute_err() -> bool {
        *MPP_COORDINATOR_EXECUTE_ERR.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl MppGather {
    /// Go `executorBuilder.buildMPPGather`.
    ///
    /// # Errors
    /// The store has no MPP client, or the gather's shape is refused.
    pub fn build(
        meta: ExecutorMeta,
        ctx: &crate::StmtContext,
        is: &tidb_infoschema::SessionExtendedInfoSchema,
        v: &PhysicalTableReader,
    ) -> Result<Self, ExecError> {
        let client = ctx
            .txn_store()
            .and_then(|store| store.mpp_client())
            .ok_or_else(|| {
                ExecError::Internal(
                    "tiflash mpp: this node has no TiFlash dispatch seam; \
                     the read cannot be served from the columnar replica"
                        .into(),
                )
            })?;
        let original_plan = v
            .table_plan
            .as_deref()
            .cloned()
            .ok_or_else(|| ExecError::Internal("the MPP reader has no plan".into()))?;
        let has_virtual_col = meta
            .schema()
            .columns
            .iter()
            .any(|col| col.virtual_expr.is_some());
        let mut table_scans = Vec::new();
        get_table_scans(&original_plan, &mut table_scans);
        // 1. hasVirtualCol: when got virtual column in TableScan, there will be
        //    no other operators in the MPP fragment.
        // 2. UnionScan: there won't be any operators like Join between UnionScan
        //    and TableScan.
        let (columns, virtual_column_index, virtual_column_ret_field_types) = if table_scans.len() == 1 {
            let columns = table_scans[0].columns.clone();
            let (index, ret_types) = if has_virtual_col {
                build_virtual_column_info(meta.schema(), &columns)
            } else {
                (Vec::new(), Vec::new())
            };
            (columns, index, ret_types)
        } else {
            if has_virtual_col {
                return Err(ExecError::internal(format!(
                    "should only have one TableScan in MPP fragment(hasVirtualCol: {has_virtual_col}, encounterUnionScan: false)"
                )));
            }
            (Vec::new(), Vec::new(), Vec::new())
        };
        // Go `b.getSnapshotTS()`.
        let start_ts = ctx.txn_store().map_or(0, |store| store.start_ts());
        let statement = crate::remote_scan::pushdown_statement_context(ctx);
        let mpp_query_id = statement.mpp_query_info.query_id(statement.mpp_server_id);
        Ok(Self {
            meta,
            ctx: ctx.clone(),
            is: is.clone(),
            client,
            original_plan,
            start_ts,
            mpp_query_id,
            resp_iter: None,
            columns,
            virtual_column_index,
            virtual_column_ret_field_types,
            kv_ranges: Vec::new(),
            exhausted: false,
        })
    }

    /// Go `kvRanges`.
    #[must_use]
    pub fn kv_ranges(&self) -> &[KeyRange] {
        &self.kv_ranges
    }
}

impl MppGather {
    /// Go `table.FillVirtualColumnValue` over the gather's virtual columns.
    fn fill_virtual_column_value(&self, req: &mut Chunk) -> Result<(), ExecError> {
        fill_virtual_column_value(
            &self.ctx,
            self.meta.schema(),
            &self.columns,
            &self.virtual_column_index,
            &self.virtual_column_ret_field_types,
            req,
        )
    }
}

/// Go `table.FillVirtualColumnValue`: each virtual column of `req` computed
/// from its generating expression and cast to the column's type.
pub(crate) fn fill_virtual_column_value(
    ctx: &crate::StmtContext,
    schema: &Schema,
    columns: &[tidb_model::GoShared<tidb_model::ColumnInfo>],
    virtual_column_index: &[usize],
    virtual_column_ret_field_types: &[FieldType],
    req: &mut Chunk,
) -> Result<(), ExecError> {
    if virtual_column_index.is_empty() {
        return Ok(());
    }
    let mut vir_cols = Chunk::new_chunk_with_capacity(virtual_column_ret_field_types, req.capacity());
    let flags = tidb_exprctx::Columns::type_flags(ctx);
    for (i, &idx) in virtual_column_index.iter().enumerate() {
        let exp_col = &schema.columns[idx];
        let virtual_expr = exp_col
            .virtual_expr
            .as_ref()
            .ok_or_else(|| ExecError::internal("mpp gather: virtual column has no generating expression"))?;
        let col_info = columns[idx].read();
        for row_idx in 0..req.num_rows() {
            let datum = virtual_expr.eval(ctx, req.get_row(row_idx))?;
            // Because the expression might return different type from
            // the generated column, we should wrap a CAST on the result.
            let cast_datum = tidb_table::generated_column::cast_virtual_column_value(
                datum,
                &col_info.field_type,
                col_info.name.original(),
                ctx,
                flags,
            )
            .map_err(|error| ExecError::Mysql(error.to_mysql_error()))?;
            vir_cols.append_datum(i, &cast_datum);
        }
        req.set_col(idx, vir_cols.column_handle(i));
    }
    Ok(())
}

/// Go `ExecutorWithRetry.setupMPPCoordinator`: a new gather id, the
/// coordinator (`buildCoordinator`) registered with the manager, its
/// `Execute`, and at least one TiFlash node.
fn setup_mpp_coordinator(
    ctx: &crate::StmtContext,
    is: &tidb_infoschema::SessionExtendedInfoSchema,
    client: &Arc<dyn MppClient>,
    plan: &PhysicalPlan,
    plan_ids: &[isize],
    start_ts: u64,
    mpp_query_id: MppQueryId,
) -> Result<(LocalMppCoordinator, Vec<KeyRange>), String> {
    // Make sure gatherID is updated before build coord.
    let gather_id = ctx.mpp_query_info().alloc_gather_id();
    let (_, server_addr) = INSTANCE_MPP_COORDINATOR_MANAGER.get_server_addr();
    let mut coord =
        LocalMppCoordinator::new(ctx, plan, plan_ids.to_vec(), start_ts, mpp_query_id, gather_id, server_addr);
    INSTANCE_MPP_COORDINATOR_MANAGER.register(coord.unique_id(), coord.reports())?;
    let executed = coord.execute(ctx, is, client, plan);
    #[cfg(any(test, feature = "test-util"))]
    let executed = if mock::mpp_coordinator_execute_err() {
        Err("mock mpp error".to_owned())
    } else {
        executed
    };
    let kv_ranges = executed?;
    let node_cnt = coord.node_count();
    if node_cnt == 0 {
        return Err(format!("tiflash node count should be greater than zero: {node_cnt}"));
    }
    Ok((coord, kv_ranges))
}

impl Executor for MppGather {
    /// Go `MPPGather.Open`: `NewExecutorWithRetry`, then
    /// `GenSelectResultFromMPPResponse` over the gather's field types.
    fn open(&mut self) -> Result<(), ExecError> {
        let mut plan_ids = Vec::new();
        collect_plan_ids(&self.original_plan, &mut plan_ids);
        let (coord, kv_ranges) = setup_mpp_coordinator(
            &self.ctx,
            &self.is,
            &self.client,
            &self.original_plan,
            &plan_ids,
            self.start_ts,
            self.mpp_query_id,
        )
        .map_err(|err| ExecError::Internal(err.into()))?;
        self.kv_ranges = kv_ranges;

        let statement = crate::remote_scan::pushdown_statement_context(&self.ctx);
        let config = tidb_config::config_tree::config::get_global_config();
        let disagg_tiflash_with_auto_scaler =
            config.disaggregated_tiflash && config.use_auto_scaler;
        let (ctx, is, client, plan, reopen_plan_ids, start_ts, mpp_query_id) = (
            self.ctx.clone(),
            self.is.clone(),
            Arc::clone(&self.client),
            self.original_plan.clone(),
            plan_ids.clone(),
            self.start_ts,
            self.mpp_query_id,
        );
        let response = ExecutorWithRetry::new(
            Box::new(coord),
            disagg_tiflash_with_auto_scaler,
            statement.allow_tiflash_fallback,
            statement.memory.clone(),
            Box::new(move || {
                setup_mpp_coordinator(&ctx, &is, &client, &plan, &reopen_plan_ids, start_ts, mpp_query_id)
                    .map(|(coord, _)| Box::new(coord) as Box<dyn MppGatherResponse>)
                    .map_err(QueryResponseError::Source)
            }),
            Box::new(|node_count| {
                tidb_tiflashcompute::global_topo_fetcher()
                    .ok_or_else(|| "TiFlash compute topology fetcher is not initialized".to_owned())?
                    .recovery_and_get_topo(
                        tidb_tiflashcompute::RecoveryType::MEM_LIMIT,
                        node_count as i64,
                    )
                    .map(|_| ())
                    .map_err(|error| error.to_string())
            }),
        );
        self.resp_iter = Some(SelectResponseIter::from_query_response(
            Box::new(response),
            self.meta.ret_field_types().to_vec(),
            Vec::new(),
            statement.time_zone.clone(),
            statement.warnings.clone(),
            mpp_result_metadata(
                self.meta.ret_field_types().len(),
                plan_ids,
                self.meta.id() as isize,
            ),
            None,
        ));
        self.exhausted = false;
        Ok(())
    }

    /// Go `MPPGather.Next`.
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if self.exhausted {
            return Ok(());
        }
        let Some(resp_iter) = self.resp_iter.as_mut() else {
            return Ok(());
        };
        let required = req.required_rows().max(1);
        loop {
            if !resp_iter
                .fill_chunk_with_required_rows(required, req)
                .map_err(|error| ExecError::from(crate::cop_reader::select_response_error(error)))?
            {
                self.exhausted = true;
                req.reset();
                return Ok(());
            }
            if req.num_rows() > 0 {
                return self.fill_virtual_column_value(req);
            }
        }
    }

    /// Go `MPPGather.Close`.
    fn close(&mut self) -> Result<(), ExecError> {
        match self.resp_iter.take() {
            Some(mut resp_iter) => resp_iter
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
