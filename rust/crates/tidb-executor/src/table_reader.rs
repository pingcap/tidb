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
        if self.request.tidb_server_id > 0 {
            builder.set_tidb_server_id(self.request.tidb_server_id);
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
        if let Some(mut result) = self.result.take() {
            result.close();
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
