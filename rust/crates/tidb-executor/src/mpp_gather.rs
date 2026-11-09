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
//!
//! # Narrowings, by name
//!
//! * Virtual columns (`buildVirtualColumnInfo`, `FillVirtualColumnValue`):
//!   a gather whose schema has one is refused by name.

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
fn collect_plan_ids(plan: &PhysicalPlan, ids: &mut Vec<isize>) {
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
    /// Go `kvRanges`: what a `UnionScan` above reads the mem buffer with.
    kv_ranges: Vec<KeyRange>,
    exhausted: bool,
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
        if has_virtual_col {
            return Err(ExecError::Internal(
                "tiflash mpp: virtual columns over an MPP gather are not ported yet".into(),
            ));
        }
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

/// Go `ExecutorWithRetry.setupMPPCoordinator`: a new gather id, the
/// coordinator's `Execute`, and at least one TiFlash node.
fn setup_mpp_coordinator(
    ctx: &crate::StmtContext,
    is: &tidb_infoschema::SessionExtendedInfoSchema,
    client: &Arc<dyn MppClient>,
    plan: &PhysicalPlan,
    start_ts: u64,
    mpp_query_id: MppQueryId,
) -> Result<(LocalMppCoordinator, Vec<KeyRange>), String> {
    // Make sure gatherID is updated before build coord.
    let gather_id = ctx.mpp_query_info().alloc_gather_id();
    let (coord, kv_ranges) =
        LocalMppCoordinator::execute(ctx, is, client, plan, start_ts, mpp_query_id, gather_id)?;
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
            self.start_ts,
            self.mpp_query_id,
        )
        .map_err(|err| ExecError::Internal(err.into()))?;
        self.kv_ranges = kv_ranges;

        let statement = crate::remote_scan::pushdown_statement_context(&self.ctx);
        let config = tidb_config::config_tree::config::get_global_config();
        // Go `NewExecutorWithRetry`'s `enableMPPRecovery`.
        let enable_mpp_recovery = config.disaggregated_tiflash
            && config.use_auto_scaler
            && !statement.allow_tiflash_fallback;
        let (ctx, is, client, plan, start_ts, mpp_query_id) = (
            self.ctx.clone(),
            self.is.clone(),
            Arc::clone(&self.client),
            self.original_plan.clone(),
            self.start_ts,
            self.mpp_query_id,
        );
        let response = ExecutorWithRetry::new(
            Box::new(coord),
            enable_mpp_recovery,
            statement.memory.clone(),
            Box::new(move || {
                setup_mpp_coordinator(&ctx, &is, &client, &plan, start_ts, mpp_query_id)
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
                return Ok(());
            }
        }
    }

    /// Go `MPPGather.Close`.
    fn close(&mut self) -> Result<(), ExecError> {
        if let Some(mut resp_iter) = self.resp_iter.take() {
            resp_iter.close();
        }
        Ok(())
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
