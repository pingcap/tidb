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

//! The executor's side of the coprocessor seam: the request types are
//! `tidb_distsql::remote_scan`'s; a request's statement context is built here from
//! the executor's statement context, and a pushed partial aggregation is bound
//! and inspected here.

#[allow(unused_imports)]
use crate::RowDecodeContextExt;
use std::sync::Arc;

use tidb_distsql::remote_scan::*;

/// The coprocessor seam of one statement, taken from its context.
#[must_use]
pub fn pushdown_statement_context(ctx: &crate::StmtContext) -> PushdownStatementContext {
    PushdownStatementContext {
        memory: ctx.statement_memory(),
        rpc_interceptor: ctx.rpc_interceptor(),
        kv_context: ctx.statement_kv_context(),
        mpp_query_info: ctx.mpp_query_info(),
        mpp_server_id: ctx.mpp_server_id(),
        tiflash_compute_dispatch_policy: ctx.tiflash_compute_dispatch_policy().into(),
        allow_tiflash_fallback: ctx.allow_tiflash_fallback(),
        plan_id: 0,
        cop_plan_chain: Vec::new(),
        cop_root_plan_id: 0,
        lookup_cop_plan_chain: None,
        push_down_flags: ctx.push_down_flags(),
        cop_lite_worker: ctx.cop_lite_worker(),
        warnings: ctx.cop_warning_sink(),
        time_zone: ctx.session_zone(),
        eval_context: Some(StatementEval(Arc::new(ctx.clone()))),
        resource_group_name: ctx.resource_group_name().to_owned(),
        request_source: ctx.request_source().clone(),
        replica_read: ctx.replica_read(),
        read_replica_scope: ctx.read_replica_scope(),
        is_staleness: ctx.is_staleness(),
        rc_check_ts: ctx.rc_check_ts(),
        weak_consistency: ctx.weak_consistency(),
        read_policy: ctx.coprocessor_read_policy().clone(),
        estimated_net_bytes: 0.0,
        lookup_avg_row_bytes: None,
        max_execution_time_ms: ctx.max_execution_time_ms(),
        priority: get_kv_priority(ctx.statement_priority()),
        not_fill_cache: ctx.not_fill_cache(),
        dist_sql_scan_concurrency: ctx.dist_sql_scan_concurrency(),
        enabled_rate_limit_action: ctx.enabled_rate_limit_action(),
        store_batch_size: ctx.store_batch_size(),
        tiflash_replica_read: ctx.tiflash_replica_read(),
        query_cop_store_limiter: ctx.query_cop_store_limiter(),
        runaway_checker: ctx.runaway_checker(),
        div_precision_increment: ctx.div_precision_increment(),
        staged_writes: ctx.staged_writes_handle(),
        keys_read: ctx.keys_read().clone(),
        cop_response_sink: ctx.cop_response_sink(),
        select_mem_tracker: None,
        select_mem_untracked: false,
        cop_projection: false,
        lookup_without_identity: false,
        init_chunk_size: ctx.executor_chunk_sizes().0,
        alloc_pool: ctx.chunk_allocator().map(tidb_distsql::remote_scan::AllocPool),
        lookup_table_limit: None,
        lookup_table_topn: None,
        tiflash_fine_grained_shuffle_batch_size: tiflash_fine_grained_shuffle_batch_size(ctx),
        deferred_open: false,
        paging: true,
        cop_projection_exprs: None,
        need_partition_handle: false,
    }
}

/// Go `SessionVars.TiFlashFineGrainedShuffleBatchSize`.
pub(crate) fn tiflash_fine_grained_shuffle_batch_size(ctx: &crate::StmtContext) -> u64 {
    match tidb_exprctx::Columns::sysvar(ctx, None, tidb_vardef::tidb_vars::TIFLASH_FINE_GRAINED_SHUFFLE_BATCH_SIZE) {
        Some(tidb_datatype::Datum::UInt(size)) => size,
        Some(tidb_datatype::Datum::Int(size)) => u64::try_from(size).unwrap_or_default(),
        _ => tidb_vardef::defaults::DEF_TIFLASH_FINE_GRAINED_SHUFFLE_BATCH_SIZE as u64,
    }
}

/// Go `RequestBuilder.getKVPriority`: the AST priority enum projected onto
/// the `mysql.PriorityEnum` the DistSQL context carries. `Delayed` maps to
/// `NoPriority` only later, at the KV boundary, so the DistSQL context keeps
/// the spelling the statement used.
pub const fn get_kv_priority(priority: tidb_ast::StatementPriority) -> tidb_mysql::Priority {
    match priority {
        tidb_ast::StatementPriority::None => tidb_mysql::Priority::None,
        tidb_ast::StatementPriority::Low => tidb_mysql::Priority::Low,
        tidb_ast::StatementPriority::High => tidb_mysql::Priority::High,
        tidb_ast::StatementPriority::Delayed => tidb_mysql::Priority::Delayed,
    }
}

/// Go `PbConverter.conOrCorColToPBExpr` over a partial aggregation's inputs:
/// resolve mutable leaves in the request-owned description, leaving the
/// retained aggregate definition untouched for the next Open. No session state
/// crosses the storage seam.
pub fn bind_partial_aggregate(
    aggregate: &mut PushdownPartialAggregate,
    context: &dyn tidb_exprctx::Columns,
) -> Result<(), tidb_exprctx::EvalError> {
    let (PushdownPartialAggregate::Grouped { functions, .. }
    | PushdownPartialAggregate::Global { functions, .. }) = aggregate;
    for input in functions
        .iter_mut()
        .filter_map(|function| function.input.as_mut())
    {
        tidb_expr::expr_to_pb::con_or_cor_col_to_pb_expr(input, context)?;
    }
    Ok(())
}

/// Every scan-column offset a partial aggregation reads, ascending and
/// distinct: its group keys and the columns under its function inputs.
#[must_use]
pub fn partial_aggregate_input_offsets(aggregate: &PushdownPartialAggregate) -> Vec<usize> {
    let (group_by, functions) = match aggregate {
        PushdownPartialAggregate::Grouped {
            group_by,
            functions,
            ..
        } => (group_by.as_slice(), functions),
        PushdownPartialAggregate::Global { functions, .. } => (&[][..], functions),
    };
    let mut offsets = crate::access_path::expression_column_offsets(group_by);
    for input in functions.iter().filter_map(|function| function.input.as_ref()) {
        offsets.extend(crate::access_path::expression_column_offsets(
            std::slice::from_ref(input),
        ));
    }
    offsets.sort_unstable();
    offsets.dedup();
    offsets
}

/// Go's reader over the store's coprocessor: opens `request` through
/// `store`'s `kv.Client` (or, for a TiFlash read, its `kv.MPPClient`) at the
/// statement's snapshot, beside the transaction's staged writes in the same
/// ranges (what Go's `UnionScanExec` above the reader adds).
///
/// `None` -- a store with no coprocessor, or a request the lowering refused
/// -- sends the caller to the byte-level cursor, which answers the same
/// question from the same snapshot.
/// [`open_remote_scan`] for a read of `table`. A temporary table's rows are
/// never in the store (Go `temporaryTableKVFilter` keeps them out of every
/// commit), so its reader never goes remote: Go builds a dummy
/// `TableReader` for it and serves it from the session.
pub(crate) fn open_table_remote_scan(
    table: &tidb_table::tables::TableCommon,
    store: &dyn tidb_kv::storage::TableStorage,
    request: &PushdownScanRequest,
) -> Option<Result<PushdownScan, tidb_kv::storage::StorageError>> {
    if table.is_temporary() {
        return None;
    }
    open_remote_scan(store, request)
}

pub fn open_remote_scan(
    store: &dyn tidb_kv::storage::TableStorage,
    request: &PushdownScanRequest,
) -> Option<Result<PushdownScan, tidb_kv::storage::StorageError>> {
    let client = store.client()?;
    // One staged slice per requested range, concatenated. The ranges are
    // ascending; equal point ranges are allowed by Go's table-handle lookup
    // and deliberately repeat the same staged row. Thus the concatenation
    // remains nondecreasing, which is the order the merge relies on. A staged
    // row outside every range is dropped for the same reason a snapshot row
    // there is: the ranges bound the `WHERE`'s access conditions, so no row
    // outside them can satisfy the statement. An empty buffered value is
    // Go's tombstone, a staged delete.
    let buffer = store.mem_buffer_snapshot(&request.ranges);
    let mut staged: Vec<_> = buffer
        .iter()
        .flat_map(|buffer| {
            request
                .ranges
                .iter()
                .flat_map(|(start, end)| buffer.entries(start, end, false))
        })
        .map(|(key, value)| (key, (!value.is_empty()).then_some(value)))
        .collect();
    // A `desc` request's remote stream arrives in DESCENDING key order; the
    // staged slice reverses to match, so the caller's one-pass merge walks
    // both sides the same way.
    if request.desc {
        staged.reverse();
    }
    let mut request = request.clone();
    request.snapshot_ts = store.start_ts();
    request.keep_order |= (!staged.is_empty() && !request.allow_unordered_response)
        || request.aggregate.as_ref().is_some_and(|aggregate| {
            matches!(
                aggregate,
                PushdownPartialAggregate::Grouped { streamed: true, .. }
            )
        });
    if !staged.is_empty() {
        request.limit = None;
    }
    // Go's `TableReaderExecutor` picks the MPP client when the plan reads the
    // columnar replica; a store without it refuses rather than silently
    // answering from row storage.
    let opened = match request.read_engine {
        PushdownReadEngine::TiKv if request.statement.deferred_open => {
            crate::cop_reader::build_resp_deferred(Arc::clone(&client), &request)
        }
        PushdownReadEngine::TiKv => crate::cop_reader::build_resp(client.as_ref(), &request),
        PushdownReadEngine::TiFlash => match store.mpp_client() {
            Some(mpp) => crate::mpp_coordinator::open_mpp_scan(&mpp, &request),
            None => Err(PushdownScannerError::Backend(tidb_kv::storage::StorageError::Backend(
                "tiflash mpp: this node has no TiFlash dispatch seam; \
                 the read cannot be served from the columnar replica"
                    .to_owned(),
            ))),
        },
    };
    match opened {
        Ok(stream) => Some(Ok(PushdownScan { stream, staged })),
        Err(PushdownScannerError::Unsupported(_reason)) => None,
        Err(PushdownScannerError::Backend(error)) => Some(Err(error)),
    }
}

