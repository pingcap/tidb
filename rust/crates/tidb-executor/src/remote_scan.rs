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
        kv_exec_counter: ctx.kv_exec_counter(),
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
        replica_read: ctx.replica_read(),
        read_replica_scope: ctx.read_replica_scope(),
        is_staleness: ctx.is_staleness(),
        read_policy: ctx.coprocessor_read_policy().clone(),
        estimated_net_bytes: 0.0,
        lookup_avg_row_bytes: None,
        max_execution_time_ms: ctx.max_execution_time_ms(),
        priority: kv_priority(ctx.statement_priority()),
        not_fill_cache: ctx.not_fill_cache(),
        dist_sql_scan_concurrency: ctx.dist_sql_scan_concurrency(),
        store_batch_size: ctx.store_batch_size(),
        tiflash_replica_read: ctx.tiflash_replica_read(),
        query_cop_store_limiter: ctx.query_cop_store_limiter(),
        div_precision_increment: ctx.div_precision_increment(),
        staged_writes: ctx.staged_writes_handle(),
        keys_read: ctx.keys_read().clone(),
        cop_response_sink: ctx.cop_response_sink(),
    }
}

/// Go `RequestBuilder.getKVPriority`: the AST priority enum projected onto
/// the `mysql.PriorityEnum` the DistSQL context carries. `Delayed` maps to
/// `NoPriority` only later, at the KV boundary, so the DistSQL context keeps
/// the spelling the statement used.
pub const fn kv_priority(priority: tidb_ast::StatementPriority) -> tidb_mysql::Priority {
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
    context: &dyn tidb_expr::Columns,
) -> Result<(), tidb_expr::EvalError> {
    let (PushdownPartialAggregate::Grouped { functions, .. }
    | PushdownPartialAggregate::Global { functions, .. }) = aggregate;
    for input in functions
        .iter_mut()
        .filter_map(|function| function.input.as_mut())
    {
        tidb_expr::pushdown_catalog::bind_deferred_leaves(input, context)?;
    }
    Ok(())
}

/// Every scan-column offset a partial aggregation reads, ascending and
/// distinct: its group keys and the columns under its function inputs.
#[must_use]
pub fn partial_aggregate_input_offsets(aggregate: &PushdownPartialAggregate) -> Vec<usize> {
    let (group_offsets, functions) = match aggregate {
        PushdownPartialAggregate::Grouped {
            group_offsets,
            functions,
            ..
        } => (group_offsets.as_slice(), functions),
        PushdownPartialAggregate::Global { functions, .. } => (&[][..], functions),
    };
    let mut offsets = group_offsets.to_vec();
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
        PushdownReadEngine::TiKv => crate::cop_reader::open_cop_scan(client.as_ref(), &request),
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

#[cfg(test)]
mod tests {
    use crate::KvTableReader;
    use tidb_datatype::{Datum, FieldType, SessionTimeZone};
    use tidb_expr::expression::Expression;
    use tidb_kv::storage::StorageError;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    use tidb_ast::CiString;
    use tidb_datatype::FieldTypeCode;
    use tidb_expr::column::Column;
    use tidb_expr::expression::ScalarFunction;
    use tidb_expr::schema::Schema;
    use tidb_txnkv::Key;

    use super::*;
    use crate::access_path::{IndexJoinLookupExec, LookupObject, LookupProbePart};
    use tidb_kv::cluster_storage::{
        ClusterSnapshot, ClusterTableStorage, MutationBuffer, SnapshotPairs,
    };
    use crate::driver::{
        build_prepared_select_plan, run_select_on, Catalog, PreparedPlanCacheEnvironment,
        DEFAULT_DATABASE,
    };
    use crate::executor::{Executor, ExecutorMeta};
    use crate::join::{IndexLookupPlan, IndexLookupSource, JoinExec, JoinKind};
    use tidb_table::kv_table::TableHandle;
    use tidb_table::kv_table::KvTable;
    use tidb_table::kv_table::KvIndex;
    use tidb_table::kv_table::KvColumn;
    use crate::mem_table::MemTableSourceExec;
    use crate::run_prepared_select_for_test;
    use tidb_kv::storage::capture_storage_ops;

    #[test]
    fn cop_read_lookup_batches_use_their_table_width_and_handle_count() {
        let statement = PushdownStatementContext {
            estimated_net_bytes: 1_000_000.0,
            lookup_avg_row_bytes: Some(32.125),
            ..Default::default()
        };
        assert_eq!(statement.for_lookup_batch(8).estimated_net_bytes, 257.0);
        assert_eq!(statement.for_lookup_batch(2).estimated_net_bytes, 64.25);
        assert_eq!(statement.estimated_net_bytes, 1_000_000.0);
    }

    /// The committed half of a cluster read, shared by the snapshot the
    /// session reads through and by the coprocessor below it.
    #[derive(Debug, Default)]
    struct MockSnapshot {
        data: BTreeMap<Vec<u8>, Vec<u8>>,
    }

    impl ClusterSnapshot for MockSnapshot {
        /// The statement's snapshot, above the one transaction the fake
        /// region commits the committed half in (at ts 2).
        fn start_ts(&self) -> u64 {
            100
        }

        fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
            Ok(self.data.get(key.as_bytes()).cloned())
        }

        fn scan(
            &mut self,
            start: &Key,
            end: &Key,
            limit: Option<usize>,
        ) -> Result<SnapshotPairs, StorageError> {
            Ok(self
                .data
                .range(start.as_bytes().to_vec()..end.as_bytes().to_vec())
                .take(limit.unwrap_or(usize::MAX))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect())
        }
    }

    /// The store behind the node's `kv.Client`, standing in for TiKV: the
    /// executor's encoded DAG runs on unistore's coprocessor over the
    /// committed half only, as Go's executor tests run on mockstore's
    /// unistore.
    ///
    /// It is deliberately blind to the session's staged writes -- that is
    /// precisely the property the merge has to repair, and a fake that quietly
    /// saw the buffer would prove nothing.
    #[derive(Debug)]
    struct FakeRegion {
        snapshot: Arc<Mutex<MockSnapshot>>,
        /// Rows the scan matched -- what its last executor below any
        /// aggregation produced -- across every request.
        returned: Arc<AtomicU64>,
        /// Rows the coprocessor read before its own filter.
        scanned: Arc<AtomicU64>,
        /// When set, the region answers every request with an other error,
        /// as unistore does for a DAG shape it cannot run.
        refuse: AtomicBool,
        /// Every request this region was sent, decoded.
        sent: Mutex<Vec<crate::test_region::SentRequest>>,
        /// Simulate region completion order only for explicitly unordered reads.
        reverse_unordered: AtomicBool,
        /// A warning the region reports on each request, standing in for
        /// TiKV's `SelectResponse.warnings`.
        region_warning: Mutex<Option<(i32, String)>>,
    }

    impl FakeRegion {
        fn sent(&self) -> Vec<crate::test_region::SentRequest> {
            self.sent.lock().unwrap().clone()
        }

        /// How many requests were sent.
        fn opened(&self) -> usize {
            self.sent.lock().unwrap().len()
        }

        /// Every `DAGRequest.flags` value this region was sent, in request
        /// order. A region acts on these bits; a fake that ignored them would
        /// let the literal `0` back in unnoticed.
        fn requested_flags(&self) -> Vec<u64> {
            self.sent().iter().map(|sent| sent.dag.flags()).collect()
        }

        /// Whether each request was required to preserve key order.
        fn requested_keep_orders(&self) -> Vec<bool> {
            self.sent().iter().map(|sent| sent.keep_order).collect()
        }

        /// Each request's reader-local projection TiKV applies after its
        /// predicates (`DAGRequest.output_offsets`).
        fn requested_output_offsets(&self) -> Vec<Vec<u32>> {
            self.sent()
                .iter()
                .map(|sent| sent.dag.output_offsets.clone())
                .collect()
        }

        /// Each request's `(reads an index, network bytes)` as the
        /// closest-replica adjuster carries it (Go `newClosestReadAdjuster`),
        /// for a statement read under [`adaptive_read_ctx`].
        fn read_estimates(&self) -> Vec<(bool, u64)> {
            self.sent()
                .iter()
                .filter_map(|sent| Some((sent.is_index(), sent.estimated_net_bytes?)))
                .collect()
        }
    }

    /// A query context reading `closest-adaptive` with a one-byte threshold,
    /// so every request carries the adjuster a reader's network estimate
    /// reaches the wire through.
    fn adaptive_read_ctx() -> crate::StmtContext {
        crate::StmtContext::for_query()
            .with_replica_read(crate::ReplicaReadType::ClosestAdaptive)
            .with_coprocessor_read_policy(CoprocessorReadPolicy {
                closest_read_threshold: 1,
                ..CoprocessorReadPolicy::default()
            })
    }

    impl tidb_txnkv::Client for FakeRegion {
        fn send(
            &self,
            request: &tidb_txnkv::TransportRequest,
        ) -> Result<Box<dyn tidb_txnkv::Response + Send>, String> {
            use prost::Message;
            let sent = crate::test_region::SentRequest::decode(request);
            self.sent.lock().unwrap().push(sent.clone());
            if self.refuse.load(Ordering::Relaxed) {
                return Ok(crate::test_region::Subsets::new(
                    vec![Err(tidb_txnkv::QueryResponseError::Source(
                        "coprocessor other error: this closure-executor shape is a later \
                         course of this port"
                            .to_owned(),
                    ))],
                    None,
                ));
            }
            let mut store = crate::test_region::committed_store(
                self.snapshot.lock().unwrap().data.clone(),
            );
            // One answer per range when the reader accepts any completion
            // order, so the order the regions finish in can be reversed;
            // otherwise one answer in key order, as an ordered read gets.
            let unordered = !sent.keep_order && self.reverse_unordered.load(Ordering::Relaxed);
            let tasks: Vec<&[(Vec<u8>, Vec<u8>)]> = if unordered {
                sent.ranges.iter().rev().map(std::slice::from_ref).collect()
            } else {
                vec![&sent.ranges[..]]
            };
            let subsets = tasks
                .into_iter()
                .map(|task| {
                    let answer = crate::test_region::run_dag(&mut store, &sent.dag, task, 100)
                        .map_err(|message| {
                            tidb_txnkv::QueryResponseError::Source(format!(
                                "coprocessor other error: {message}"
                            ))
                        })?;
                    self.scanned.fetch_add(answer.scanned, Ordering::Relaxed);
                    self.returned.fetch_add(answer.matched, Ordering::Relaxed);
                    let mut select = answer.select;
                    if let Some((code, message)) = self.region_warning.lock().unwrap().clone() {
                        select.warnings.push(tidb_proto::tipb::Error {
                            code: Some(code),
                            msg: Some(message),
                        });
                        select.warning_count = Some(select.warning_count.unwrap_or(0) + 1);
                    }
                    Ok(tidb_txnkv::QueryResultSubset {
                        data: select.encode_to_vec().into(),
                        runtime: None,
                    })
                })
                .collect();
            Ok(crate::test_region::Subsets::new(subsets, None))
        }

        fn is_request_type_supported(&self, _request_type: i64, _sub_type: i64) -> bool {
            true
        }
    }

    fn column(name: &str, id: i64) -> KvColumn {
        KvColumn {
            name: name.to_owned(),
            id,
            field_type: FieldType::new(FieldTypeCode::LongLong),
            column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
            default_value: None,
            origin_default: None,
            comment: String::new(),
            generated: None,
        }
    }

    fn commit(buffer: &MutationBuffer, snapshot: &Arc<Mutex<MockSnapshot>>) {
        let mut snapshot = snapshot.lock().unwrap();
        for (key, value) in buffer.snapshot() {
            match value {
                Some(value) => snapshot.data.insert(key.as_bytes().to_vec(), value),
                None => snapshot.data.remove(key.as_bytes()),
            };
        }
        buffer.reset();
    }

    struct Fixture {
        table: KvTable,
        buffer: MutationBuffer,
        snapshot: Arc<Mutex<MockSnapshot>>,
        returned: Arc<AtomicU64>,
        scanned: Arc<AtomicU64>,
        region: Arc<FakeRegion>,
    }

    /// A cluster-backed `t(a, b)` whose scans go through the coprocessor.
    fn fixture() -> Fixture {
        fixture_with(None)
    }

    /// [`fixture`] with `a` as the clustered integer handle, so a `WHERE` over
    /// it builds handle ranges.
    fn clustered_fixture() -> Fixture {
        fixture_with(Some(0))
    }

    /// A cluster fixture with (a, b) as the clustered common handle.
    fn common_handle_fixture() -> Fixture {
        let mut fixture = fixture_with(None);
        fixture.table.set_common_handle_offsets(vec![0, 1]);
        fixture.table.add_index(
            KvIndex {
                id: 1,
                name: "PRIMARY".to_owned(),
                comment: String::new(),
                unique: true,
                column_offsets: vec![0, 1],
                prefix_lengths: vec![tidb_datatype::UNSPECIFIED_LENGTH; 2],
                visible: true,
                global: false,
                global_index_version: 0,
                clustered_primary: false,
                primary: true,
            },
            false,
        );
        fixture
    }

    /// A clustered common-handle table whose catalog does not carry a
    /// physical PRIMARY `KvIndex`. This mirrors the metadata shape produced
    /// by the real clustered-table loader during a merge, where the handle
    /// offsets are present but the index entry is synthesized by the scan
    /// path.
    fn common_handle_without_catalog_index_fixture() -> Fixture {
        let mut fixture = fixture_with(None);
        fixture.table.set_common_handle_offsets(vec![0, 1]);
        fixture
    }

    fn fixture_with(pk_handle_offset: Option<usize>) -> Fixture {
        fixture_with_columns(pk_handle_offset, vec![column("a", 1), column("b", 2)])
    }

    fn fixture_with_columns(pk_handle_offset: Option<usize>, columns: Vec<KvColumn>) -> Fixture {
        let snapshot = Arc::new(Mutex::new(MockSnapshot::default()));
        let handle: Arc<Mutex<dyn ClusterSnapshot>> = Arc::clone(&snapshot) as _;
        let buffer = MutationBuffer::new();
        let returned = Arc::new(AtomicU64::new(0));
        let scanned = Arc::new(AtomicU64::new(0));
        let region = Arc::new(FakeRegion {
            snapshot: Arc::clone(&snapshot),
            returned: Arc::clone(&returned),
            scanned: Arc::clone(&scanned),
            refuse: AtomicBool::new(false),
            sent: Mutex::default(),
            reverse_unordered: AtomicBool::new(false),
            region_warning: Mutex::new(None),
        });
        let storage = ClusterTableStorage::new(buffer.clone(), handle)
            .with_client(Arc::clone(&region) as Arc<dyn tidb_txnkv::Client>);
        let mut table = KvTable::with_storage(91, columns, Box::new(storage));
        if let Some(offset) = pk_handle_offset {
            table.set_pk_handle_offset(offset);
        }
        Fixture {
            table,
            buffer,
            snapshot,
            returned,
            scanned,
            region,
        }
    }

    #[test]
    fn remote_extra_handle_survives_pruning_and_staged_merge() {
        let mut fixture = fixture();
        let mut handles = Vec::new();
        for a in [100, 200] {
            let handle = fixture
                .table
                .insert_row(&[Datum::Int(a), Datum::Int(a + 10)], &tidb_expr::NoColumns)
                .unwrap();
            let tidb_table::kv_table::TableHandle::Int(handle) = handle else {
                unreachable!()
            };
            handles.push(handle);
        }
        commit(&fixture.buffer, &fixture.snapshot);
        let mut catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query_in_dirty_transaction(&[91]);
        let (rows, ops) = capture_storage_ops(|| {
            run_select_on(
                "SELECT _tidb_rowid FROM t ORDER BY _tidb_rowid",
                &catalog,
                &ctx,
            )
            .unwrap()
        });
        assert_eq!(
            rows,
            handles
                .iter()
                .map(|id| vec![Datum::Int(*id)])
                .collect::<Vec<_>>()
        );
        assert_eq!(ops.cop_scans, 1);
        assert_eq!(
            crate::run_update_on("UPDATE t SET a=a+1 WHERE a=100", &mut catalog, &ctx).unwrap(),
            1
        );
        assert_eq!(
            crate::run_delete_on("DELETE FROM t WHERE a=200", &mut catalog, &ctx).unwrap(),
            1
        );
        let (rows, ops) = capture_storage_ops(|| {
            run_select_on(
                "SELECT _tidb_rowid,a FROM t ORDER BY _tidb_rowid",
                &catalog,
                &ctx,
            )
            .unwrap()
        });
        assert_eq!(rows, vec![vec![Datum::Int(handles[0]), Datum::Int(101)]]);
        assert_eq!(ops.cop_scans, 1);
    }

    #[test]
    fn write_range_reader_preserves_record_identity_and_staged_rows() {
        for mut fixture in [fixture(), clustered_fixture(), common_handle_fixture()] {
            fixture
                .region
                .reverse_unordered
                .store(true, Ordering::Relaxed);
            for row in [[1, 10], [2, 20], [3, 30], [4, 40]] {
                fixture
                    .table
                    .insert_row(
                        &[Datum::Int(row[0]), Datum::Int(row[1])],
                        &tidb_expr::NoColumns,
                    )
                    .unwrap();
            }
            commit(&fixture.buffer, &fixture.snapshot);
            let mut catalog = catalog_of(fixture.table);
            let ctx = crate::StmtContext::for_query_in_dirty_transaction(&[91]);
            let (changed, ops) = capture_storage_ops(|| {
                crate::run_update_on(
                    "UPDATE t SET b=b+100 WHERE a>=1 AND a<=3 AND a<>2",
                    &mut catalog,
                    &ctx,
                )
                .unwrap()
            });
            assert_eq!(changed, 2);
            assert_eq!(
                ops.cop_scans, 1,
                "a clean write range must use the shared remote reader"
            );
            assert_eq!(
                crate::run_delete_on(
                    "DELETE FROM t WHERE a>=2 AND a<=4 AND a<>3",
                    &mut catalog,
                    &ctx,
                )
                .unwrap(),
                2
            );
            assert_eq!(
                run_select_on("SELECT * FROM t ORDER BY a", &catalog, &ctx).unwrap(),
                vec![
                    vec![Datum::Int(1), Datum::Int(110)],
                    vec![Datum::Int(3), Datum::Int(130)]
                ]
            );
        }
    }

    #[test]
    fn composite_cnf_writes_fetch_only_the_matching_keys() {
        let mut fixture = fixture_with_columns(
            None,
            vec![
                column("a", 1),
                column("b", 2),
                column("c", 3),
                column("v", 4),
            ],
        );
        fixture.table.set_common_handle_offsets(vec![0, 1, 2]);
        for b in 1..=4 {
            for c in 1..=4 {
                fixture
                    .table
                    .insert_row(
                        &[Datum::Int(1), Datum::Int(b), Datum::Int(c), Datum::Int(10)],
                        &tidb_expr::NoColumns,
                    )
                    .unwrap();
            }
        }
        commit(&fixture.buffer, &fixture.snapshot);
        let mut catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();
        let (rows, reads) = capture_storage_ops(|| {
            run_select_on(
            "SELECT v FROM t WHERE (a,b) IN ((1,1),(1,3)) AND c=2 AND v>0 ORDER BY b DESC LIMIT 1",
            &catalog, &ctx,
        ).unwrap()
        });
        assert_eq!(rows, vec![vec![Datum::Int(10)]]);
        assert_eq!(
            reads,
            tidb_kv::storage::StorageOps {
                gets: 1,
                ..Default::default()
            },
            "complete composite ranges must execute one batched read, not just print a point plan"
        );
        assert_eq!(
            crate::run_update_on(
                "UPDATE t SET v=999 WHERE (a,b) IN ((1,1),(1,3)) AND c=2 AND v=999",
                &mut catalog,
                &ctx,
            )
            .unwrap(),
            0,
            "a batched handle read must retain the write's residual predicate"
        );
        assert_eq!(
            crate::run_delete_on(
                "DELETE FROM t WHERE (a,b) IN ((1,1),(1,3)) AND c=2 AND v=999",
                &mut catalog,
                &ctx,
            )
            .unwrap(),
            0,
            "a batched delete must retain its residual predicate"
        );
        let (updated, ops) = capture_storage_ops(|| {
            crate::run_update_on(
                "UPDATE t SET v=v+1 WHERE (a,b) IN ((1,1),(1,3)) AND c=2",
                &mut catalog,
                &ctx,
            )
            .unwrap()
        });
        assert_eq!(updated, 2);
        assert_eq!(
            (ops.scans, ops.cop_scans),
            (0, 0),
            "point writes must not open scans"
        );
        assert_eq!(
            run_select_on(
                "SELECT v FROM t WHERE (a,b) IN ((1,1),(1,3)) AND c=2 ORDER BY b",
                &catalog,
                &ctx,
            )
            .unwrap(),
            vec![vec![Datum::Int(11)], vec![Datum::Int(11)]]
        );
        let (deleted, ops) = capture_storage_ops(|| {
            crate::run_delete_on(
                "DELETE FROM t WHERE (a,b) IN ((1,1),(1,3)) AND c=2",
                &mut catalog,
                &ctx,
            )
            .unwrap()
        });
        assert_eq!(deleted, 2);
        assert_eq!(
            (ops.scans, ops.cop_scans),
            (0, 0),
            "dirty point deletes must not open scans"
        );
    }

    #[test]
    fn unsigned_staged_rows_merge_in_the_readers_value_order() {
        let mut columns = vec![column("a", 1), column("b", 2)];
        columns[0]
            .field_type
            .add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
        let mut fixture = fixture_with_columns(Some(0), columns);
        fixture
            .region
            .reverse_unordered
            .store(true, Ordering::Relaxed);
        for (a, b) in [(0, 10), (1, 20), (1_u64 << 63, 30), (u64::MAX, 40)] {
            fixture
                .table
                .insert_row(&[Datum::UInt(a), Datum::Int(b)], &tidb_expr::NoColumns)
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        let mut catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query_in_dirty_transaction(&[91]);
        assert_eq!(
            crate::run_update_on(
                "UPDATE t SET b=b+100 WHERE a>=1 AND a<=18446744073709551615 AND b<>20",
                &mut catalog,
                &ctx,
            )
            .unwrap(),
            2
        );
        assert_eq!(
            crate::run_delete_on(
                "DELETE FROM t WHERE a>=9223372036854775808 AND b<140",
                &mut catalog,
                &ctx,
            )
            .unwrap(),
            1
        );
        let (rows, ops) = capture_storage_ops(|| {
            run_select_on("SELECT * FROM t ORDER BY a", &catalog, &ctx).unwrap()
        });
        assert_eq!(
            rows,
            vec![
                vec![Datum::UInt(0), Datum::Int(10)],
                vec![Datum::UInt(1), Datum::Int(20)],
                vec![Datum::UInt(u64::MAX), Datum::Int(140)],
            ]
        );
        assert_eq!(
            ops.cop_scans, 2,
            "both unsigned value-order groups remain remote"
        );
        let mut descending = rows.clone();
        descending.reverse();
        assert_eq!(
            run_select_on("SELECT * FROM t ORDER BY a DESC", &catalog, &ctx).unwrap(),
            descending
        );
        assert_eq!(
            run_select_on("SELECT * FROM t ORDER BY a LIMIT 2", &catalog, &ctx).unwrap(),
            rows[..2]
        );
        assert_eq!(
            run_select_on("SELECT * FROM t ORDER BY a DESC LIMIT 2", &catalog, &ctx).unwrap(),
            descending[..2]
        );
        assert_eq!(
            run_select_on("SELECT b FROM t WHERE a>=1 ORDER BY a", &catalog, &ctx).unwrap(),
            vec![vec![Datum::Int(20)], vec![Datum::Int(140)]],
            "pruning the output handle must preserve unsigned range order"
        );
        let mut unordered = run_select_on("SELECT * FROM t", &catalog, &ctx).unwrap();
        unordered.sort_by_key(|row| match row[0] {
            Datum::UInt(value) => value,
            _ => unreachable!(),
        });
        assert_eq!(
            unordered, rows,
            "physical-order scans merge the same record set"
        );
    }

    #[test]
    fn virtual_dependency_expansion_preserves_reader_output() {
        for primary in ["PRIMARY KEY", ""] {
            let mut catalog = tidb_infoschema_perfschema::mock_catalog();
            let ctx = crate::StmtContext::for_query();
            crate::run_create_table_on(
                &format!(
                    "CREATE TABLE t (a BIGINT {primary}, b BIGINT AS (a+10), \
                    d BIGINT AS (b+1), c BIGINT, INDEX idx_c(c))"
                ),
                &mut catalog,
            )
            .unwrap();
            crate::run_insert_on(
                "INSERT INTO t(a,c) VALUES (1,21),(2,22),(3,23)",
                &mut catalog,
                &ctx,
            )
            .unwrap();
            for sql in [
                "SELECT b,d,b FROM t FORCE INDEX(idx_c) WHERE c>=21 ORDER BY c",
                "SELECT b,d,b FROM t IGNORE INDEX(idx_c) ORDER BY c",
                "SELECT b,d,b FROM t FORCE INDEX(idx_c) WHERE c>=21 AND b>10 ORDER BY c",
                "SELECT b,d,b FROM t IGNORE INDEX(idx_c) WHERE b>10 ORDER BY c",
            ] {
                assert_eq!(
                    run_select_on(sql, &catalog, &ctx).unwrap(),
                    vec![
                        vec![Datum::Int(11), Datum::Int(12), Datum::Int(11)],
                        vec![Datum::Int(12), Datum::Int(13), Datum::Int(12)],
                        vec![Datum::Int(13), Datum::Int(14), Datum::Int(13)],
                    ],
                    "{sql}; primary={primary}"
                );
            }
            assert_eq!(
                run_select_on("SELECT SUM(d) FROM t", &catalog, &ctx).unwrap(),
                vec![vec![Datum::Decimal(tidb_datatype::Decimal::from_int(39))]]
            );
        }
    }

    #[test]
    fn write_range_reader_reconstructs_virtual_columns() {
        for pk_handle_offset in [Some(0), None] {
            let mut columns = vec![column("a", 1), column("b", 2), column("c", 3)];
            let tidb_ast::Stmt::Ddl(ddl) =
                tidb_parser::parse("CREATE TABLE t (b BIGINT AS (a+10))").unwrap()
            else {
                unreachable!()
            };
            let tidb_ast::DdlStmt::CreateTable(create) = &*ddl else {
                unreachable!()
            };
            let expression = create.columns[0]
                .options
                .iter()
                .find_map(|option| {
                    if let tidb_ast::ColumnOption::Generated { expression, .. } = option {
                        Some(expression)
                    } else {
                        None
                    }
                })
                .unwrap();
            columns[1].generated = Some(
                tidb_ddl_build::generated_column::build_added_generated_column(
                    "b",
                    expression,
                    false,
                    &["a".to_owned(), "b".to_owned()],
                    &[
                        FieldType::new(FieldTypeCode::LongLong),
                        FieldType::new(FieldTypeCode::LongLong),
                    ],
                    &tidb_datatype::SessionTimeZone::utc(),
                )
                .unwrap(),
            );
            let mut fixture = fixture_with_columns(pk_handle_offset, columns);
            fixture.table.add_index(
                KvIndex {
                    id: 2,
                    name: "check_stored".to_owned(),
                    comment: String::new(),
                    unique: false,
                    column_offsets: vec![2],
                    prefix_lengths: vec![tidb_datatype::UNSPECIFIED_LENGTH],
                    visible: true,
                    global: false,
                    global_index_version: 0,
                    clustered_primary: false,
                    primary: false,
                },
                false,
            );
            for a in [1, 2, 3] {
                fixture
                    .table
                    .insert_row(
                        &[Datum::Int(a), Datum::Null, Datum::Int(a + 20)],
                        &tidb_expr::NoColumns,
                    )
                    .unwrap();
            }
            commit(&fixture.buffer, &fixture.snapshot);
            let ctx = crate::StmtContext::for_query();
            let handles = [TableHandle::Int(3), TableHandle::Int(1)];
            let staged = fixture
                .table
                .build_table_reader_from_routed_handles(
                    &handles,
                    None,
                    &[1],
                    &[],
                    &crate::RowDecodeContext::legacy_default(&ctx.session_zone()),
                    &crate::remote_scan::pushdown_statement_context(&ctx),
                    2,
                )
                .unwrap()
                .expect("the table probe remains remote");
            let lookup = KvTable::finish_routed_rows_by_handles(&handles, staged)
                .unwrap()
                .expect("remote table rows");
            assert_eq!(
                lookup.0,
                vec![
                    (tidb_table::kv_table::TableHandle::Int(3), vec![Datum::Int(13)]),
                    (tidb_table::kv_table::TableHandle::Int(1), vec![Datum::Int(11)]),
                ]
            );
            let mut catalog = catalog_of(fixture.table);
            for (suffix, expected) in [
                ("", vec![12, 13]),
                (" LIMIT 1", vec![12]),
                (" LIMIT 1 OFFSET 1", vec![13]),
                (" DESC LIMIT 1", vec![13]),
            ] {
                let sql = format!(
                    "SELECT b FROM t FORCE INDEX(check_stored) WHERE c>=21 AND b>11 ORDER BY c{suffix}"
                );
                assert_eq!(
                    run_select_on(&sql, &catalog, &ctx).unwrap(),
                    expected
                        .into_iter()
                        .map(|value| vec![Datum::Int(value)])
                        .collect::<Vec<_>>(),
                    "{sql}; handle={pk_handle_offset:?}"
                );
            }
            let (rows, ops) = capture_storage_ops(|| {
                run_select_on(
                    "SELECT b FROM t WHERE b>11 ORDER BY a LIMIT 1",
                    &catalog,
                    &ctx,
                )
                .unwrap()
            });
            assert_eq!(rows, vec![vec![Datum::Int(12)]]);
            assert_eq!(
                ops.cop_scans, 1,
                "virtual projection retains the remote physical scan"
            );
            let (changed, ops) = capture_storage_ops(|| {
                crate::run_update_on(
                    "UPDATE t SET a=a+10 WHERE a>=1 AND a<=3 AND b=12",
                    &mut catalog,
                    &ctx,
                )
                .unwrap()
            });
            assert_eq!(changed, 1);
            assert_eq!(ops.cop_scans, 1);
            // The UPDATE is staged, not committed: the reads that follow run
            // in its transaction, where Go reads the table through a union
            // scan.
            let dirty = crate::StmtContext::for_query_in_dirty_transaction(&[91]);
            assert_eq!(
                run_select_on("SELECT * FROM t ORDER BY a", &catalog, &dirty).unwrap(),
                vec![
                    vec![Datum::Int(1), Datum::Int(11), Datum::Int(21)],
                    vec![Datum::Int(3), Datum::Int(13), Datum::Int(23)],
                    vec![Datum::Int(12), Datum::Int(22), Datum::Int(22)]
                ]
            );
            for (sql, expected) in [
                ("SELECT b FROM t WHERE b>=12 ORDER BY a LIMIT 1", vec![13]),
                ("SELECT b FROM t ORDER BY b DESC LIMIT 2", vec![22, 13]),
                ("SELECT b FROM t WHERE a>=3 ORDER BY a", vec![13, 22]),
                ("SELECT COUNT(b) FROM t WHERE b>12", vec![2]),
                ("SELECT MAX(b) FROM t", vec![22]),
            ] {
                assert_eq!(
                    run_select_on(sql, &catalog, &dirty).unwrap(),
                    expected
                        .into_iter()
                        .map(|value| vec![Datum::Int(value)])
                        .collect::<Vec<_>>(),
                    "{sql}"
                );
            }
        }
    }

    #[test]
    fn dirty_common_handle_reads_share_the_remote_staged_merge() {
        let ctx = crate::StmtContext::for_query_in_dirty_transaction(&[91]);
        for (key_type, key, predicate) in [
            (
                FieldType::new(FieldTypeCode::LongLong),
                Datum::Int(1),
                "a=1",
            ),
            (
                FieldType::new(FieldTypeCode::LongLong)
                    .with_flags(tidb_datatype::FieldTypeFlags::UNSIGNED),
                Datum::UInt(u64::MAX),
                "a=18446744073709551615",
            ),
            (
                FieldType::new(FieldTypeCode::Varchar)
                    .with_collation(tidb_datatype::Collation::Utf8Mb4GeneralCi),
                Datum::new_collation_string(
                    b"AbC".to_vec(),
                    tidb_datatype::Collation::Utf8Mb4GeneralCi,
                ),
                "a='ABC'",
            ),
        ] {
            let mut columns = vec![column("a", 1), column("b", 2), column("c", 3)];
            columns[0].field_type = key_type;
            let mut fixture = fixture_with_columns(None, columns);
            fixture
                .region
                .reverse_unordered
                .store(true, Ordering::Relaxed);
            fixture.table.set_common_handle_offsets(vec![0, 1]);
            let mut handles = Vec::new();
            for b in 1..=4 {
                handles.push(
                    fixture
                        .table
                        .insert_row(
                            &[key.clone(), Datum::Int(b), Datum::Int(b * 10)],
                            &tidb_expr::NoColumns,
                        )
                        .unwrap(),
                );
            }
            commit(&fixture.buffer, &fixture.snapshot);
            fixture
                .table
                .update_row_with_context(
                    &handles[0],
                    &[key.clone(), Datum::Int(1), Datum::Int(110)],
                    &ctx,
                )
                .unwrap();
            fixture
                .table
                .delete_row_with_context(&handles[1], &ctx)
                .unwrap();
            fixture
                .table
                .update_row_with_context(
                    &handles[2],
                    &[key.clone(), Datum::Int(13), Datum::Int(130)],
                    &ctx,
                )
                .unwrap();
            fixture
                .table
                .insert_row(
                    &[key.clone(), Datum::Int(0), Datum::Int(0)],
                    &tidb_expr::NoColumns,
                )
                .unwrap();
            let catalog = catalog_of(fixture.table);
            for (condition, expected) in [("b=1", vec![110]), ("b IN (1,4,1,99)", vec![110, 40])] {
                let sql = format!("SELECT c FROM t WHERE {predicate} AND {condition}");
                let (rows, ops) =
                    capture_storage_ops(|| run_select_on(&sql, &catalog, &ctx).unwrap());
                assert_eq!(
                    rows,
                    expected
                        .into_iter()
                        .map(|v| vec![Datum::Int(v)])
                        .collect::<Vec<_>>(),
                    "{sql}"
                );
                assert_eq!((ops.scans, ops.cop_scans), (0, 0), "{sql}");
            }
            for (suffix, expected) in [
                ("ORDER BY b", vec![0, 110, 40, 130]),
                ("ORDER BY b DESC", vec![130, 40, 110, 0]),
                ("ORDER BY b LIMIT 2", vec![0, 110]),
                ("ORDER BY b DESC LIMIT 2", vec![130, 40]),
                ("ORDER BY c DESC LIMIT 2", vec![130, 110]),
                ("AND c>50 ORDER BY b", vec![110, 130]),
                ("AND c<50 ORDER BY b", vec![0, 40]),
            ] {
                let sql = format!("SELECT c FROM t WHERE {predicate} {suffix}");
                let (rows, ops) =
                    capture_storage_ops(|| run_select_on(&sql, &catalog, &ctx).unwrap());
                assert_eq!(
                    rows,
                    expected
                        .into_iter()
                        .map(|v| vec![Datum::Int(v)])
                        .collect::<Vec<_>>(),
                    "{sql}"
                );
                assert_eq!(
                    ops.cop_scans, 1,
                    "dirty common-handle scan remained local: {sql}"
                );
            }
            let sql = format!("SELECT SUM(c) FROM t WHERE {predicate}");
            fixture.region.sent.lock().unwrap().clear();
            let (rows, ops) = capture_storage_ops(|| run_select_on(&sql, &catalog, &ctx).unwrap());
            assert_eq!(
                rows,
                vec![vec![Datum::Decimal(tidb_datatype::Decimal::from_int(280))]]
            );
            assert_eq!(
                ops.cop_scans, 1,
                "the dirty aggregate must reuse the merged reader"
            );
            assert_eq!(
                fixture.region.requested_keep_orders(),
                vec![false],
                "an unordered aggregate must not acquire ordered-scan concurrency"
            );
            let (rows, ops) = capture_storage_ops(|| {
                run_select_on("SELECT c FROM t WHERE c>=0 ORDER BY c", &catalog, &ctx).unwrap()
            });
            assert_eq!(
                rows,
                vec![0, 40, 110, 130]
                    .into_iter()
                    .map(|v| vec![Datum::Int(v)])
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                ops.cop_scans, 1,
                "a projection omitting all handle columns remains remote"
            );
        }
    }

    #[test]
    fn a_clean_common_handle_range_uses_the_coprocessor() {
        let mut fixture = common_handle_fixture();
        for row in [[1, 10], [1, 20], [2, 30]] {
            fixture
                .table
                .insert_row(
                    &[Datum::Int(row[0]), Datum::Int(row[1])],
                    &tidb_expr::NoColumns,
                )
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);

        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();
        let (rows, ops) = capture_storage_ops(|| {
            run_select_on("SELECT a, b FROM t WHERE a = 1 ORDER BY b", &catalog, &ctx).unwrap()
        });
        assert_eq!(
            rows,
            vec![
                vec![Datum::Int(1), Datum::Int(10)],
                vec![Datum::Int(1), Datum::Int(20)]
            ]
        );
        assert_eq!(ops.cop_scans, 1);
        assert_eq!(ops.cop_rows, 2);
        assert_eq!(ops.gets, 0);
    }

    #[test]
    fn a_clean_common_handle_without_catalog_index_uses_the_coprocessor() {
        let mut fixture = common_handle_without_catalog_index_fixture();
        for row in [[1, 10], [1, 20], [2, 30]] {
            fixture
                .table
                .insert_row(
                    &[Datum::Int(row[0]), Datum::Int(row[1])],
                    &tidb_expr::NoColumns,
                )
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);

        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();
        let (rows, ops) = capture_storage_ops(|| {
            run_select_on("SELECT a, b FROM t WHERE a = 1 ORDER BY b", &catalog, &ctx).unwrap()
        });
        assert_eq!(
            rows,
            vec![
                vec![Datum::Int(1), Datum::Int(10)],
                vec![Datum::Int(1), Datum::Int(20)]
            ]
        );
        assert_eq!(ops.cop_scans, 1);
        assert_eq!(ops.cop_rows, 2);
        assert_eq!(ops.gets, 0);
    }

    #[test]
    fn an_index_join_sends_a_complete_common_handle_task_in_one_coprocessor_scan() {
        let mut fixture = common_handle_fixture();
        for row in [[1, 10], [1, 20], [2, 30], [3, 40]] {
            fixture
                .table
                .insert_row(
                    &[Datum::Int(row[0]), Datum::Int(row[1])],
                    &tidb_expr::NoColumns,
                )
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);

        let field_types = vec![
            FieldType::new(FieldTypeCode::LongLong),
            FieldType::new(FieldTypeCode::LongLong),
        ];
        let schema = Schema::new(
            field_types
                .iter()
                .enumerate()
                .map(|(offset, field_type)| {
                    let mut column = Column::new(offset as i64 + 1, field_type.clone());
                    column.index = offset as i64;
                    column
                })
                .collect(),
        );
        let ctx = crate::StmtContext::for_query();
        let mut source = IndexJoinLookupExec::new_with_context(
            ExecutorMeta::new(schema, 0, 32, 1024),
            fixture.table,
            LookupObject::CommonHandle,
            crate::RowDecodeContext::for_query(&ctx),
        );
        source.set_probe_parts(vec![LookupProbePart::Dynamic(0)]);
        // Go `buildKvRangesForIndexJoin` hands every lookup content in one
        // inner task to one table reader. This exceeds the former 4,096-range
        // Rust split, which opened multiple independent scan sessions and
        // made TPC-H q21 associate an incomplete inner set with the task.
        source
            .set_probes(crate::access_path::IndexJoinProbes {
                keys: (1..=4097).map(|probe| vec![Datum::Int(probe)]).collect(),
                bound_values: Vec::new(),
            })
            .unwrap();

        let (rows, ops) = capture_storage_ops(|| {
            source.open().unwrap();
            let mut rows = Vec::new();
            let mut chunk = source.new_chunk();
            loop {
                source.next(&mut chunk).unwrap();
                if chunk.num_rows() == 0 {
                    break;
                }
                for row in 0..chunk.num_rows() {
                    rows.push(
                        field_types
                            .iter()
                            .enumerate()
                            .map(|(column, field_type)| {
                                chunk.get_row(row).get_datum(column, field_type)
                            })
                            .collect::<Vec<_>>(),
                    );
                }
            }
            source.close().unwrap();
            rows
        });
        assert_eq!(
            rows,
            vec![
                vec![Datum::Int(1), Datum::Int(10)],
                vec![Datum::Int(1), Datum::Int(20)],
                vec![Datum::Int(2), Datum::Int(30)],
                vec![Datum::Int(3), Datum::Int(40)],
            ]
        );
        // One coprocessor request carries the whole task (Go
        // `buildKvRangesForIndexJoin`). The region answers it on the
        // client's worker thread, so its storage reads are not this thread's
        // to count; the rows above are the complete answer.
        assert_eq!(ops.cop_scans, 1);
    }

    /// Go's IndexHashJoin hands every outer task to an inner worker that
    /// builds and reads the task's own inner executor; the tasks overlap
    /// because the workers run concurrently, not because one is opened
    /// before another is read. Two outer batches therefore open two inner
    /// readers, and the join's answer is complete whichever finishes first.
    #[test]
    fn an_index_join_opens_one_inner_reader_per_outer_task() {
        let mut fixture = common_handle_fixture();
        for row in [[1, 10], [2, 20]] {
            fixture
                .table
                .insert_row(
                    &[Datum::Int(row[0]), Datum::Int(row[1])],
                    &tidb_expr::NoColumns,
                )
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        let field = FieldType::new(FieldTypeCode::LongLong);
        let schema = |width: usize| {
            Schema::new(
                (0..width)
                    .map(|offset| {
                        let mut column = Column::new(offset as i64 + 1, field.clone());
                        column.index = offset as i64;
                        column
                    })
                    .collect(),
            )
        };
        let outer_rows = (0..1025)
            .map(|row| vec![Datum::Int(row % 2 + 1)])
            .collect::<Vec<_>>();
        let outer: Box<dyn Executor> = Box::new(MemTableSourceExec::new(
            ExecutorMeta::new(schema(1), 0, 32, 1024),
            outer_rows,
        ));
        let mut left = Column::new(1, field.clone());
        left.index = 0;
        let mut right = Column::new(2, field.clone());
        right.index = 1;
        let equality = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("eq"),
            field.clone(),
            vec![Expression::Column(left), Expression::Column(right)],
        ));
        let ctx = crate::StmtContext::for_query();
        let mut lookup = IndexJoinLookupExec::new_with_context(
            ExecutorMeta::new(schema(2), 0, 32, 1024),
            fixture.table,
            LookupObject::CommonHandle,
            crate::RowDecodeContext::for_query(&ctx),
        );
        lookup.set_probe_parts(vec![LookupProbePart::Dynamic(0)]);
        let mut join = JoinExec::new_index_lookup(
            ExecutorMeta::new(schema(1), 0, 32, 1024),
            JoinKind::Semi,
            vec![equality],
            outer,
            vec![field.clone()],
            vec![field.clone(), field],
            ctx.clone(),
            ctx.statement_memory(),
            IndexLookupPlan {
                lookup_is_left: false,
                probe_keys: vec![0],
                probe_key_domains: Vec::new(),
                source: IndexLookupSource::Leaf(lookup),
                outer_not_null: Vec::new(),
                inner_not_null: Vec::new(),
                probe_bounds: Vec::new(),
            },
        );

        join.open().unwrap();
        let mut rows = 0;
        let mut chunk = join.new_chunk();
        loop {
            join.next(&mut chunk).unwrap();
            if chunk.num_rows() == 0 {
                break;
            }
            rows += chunk.num_rows();
        }
        join.close().unwrap();
        assert_eq!(rows, 1025);
        assert_eq!(fixture.region.opened(), 2);
    }

    fn catalog_of(table: KvTable) -> Catalog {
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        catalog.register_kv("t", table);
        catalog
    }

    /// Go carries `PhysicalTableScan.KeepOrder` through
    /// `TableReaderExecutor.keepOrder` into every DistSQL request. A merge
    /// join depends on both child streams having that exact property; merely
    /// printing `keep order:true` while issuing unordered requests can drop
    /// matches when region responses arrive out of key order.
    #[test]
    fn a_merge_join_sends_keep_order_to_both_remote_table_scans() {
        let mut fixture = clustered_fixture();
        for row in [[1, 10], [2, 20], [3, 30]] {
            fixture
                .table
                .insert_row(
                    &[Datum::Int(row[0]), Datum::Int(row[1])],
                    &tidb_expr::NoColumns,
                )
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        let region = Arc::clone(&fixture.region);
        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();

        let rows = run_select_on(
            "SELECT /*+ TIDB_SMJ(t1, t2) */ t1.a FROM t t1 JOIN t t2 ON t1.a=t2.a",
            &catalog,
            &ctx,
        )
        .unwrap();

        assert_eq!(rows.len(), 3);
        assert_eq!(
            region.requested_keep_orders(),
            vec![true, true],
            "both MergeJoin children must ask DistSQL to preserve record-key order",
        );
    }

    #[test]
    fn index_probe_readers_retain_network_estimates() {
        let mut secondary = fixture();
        secondary.table.add_index(
            KvIndex {
                id: 1,
                name: "ia".to_owned(),
                comment: String::new(),
                unique: false,
                column_offsets: vec![0],
                prefix_lengths: vec![-1],
                visible: true,
                global: false,
                global_index_version: 0,
                clustered_primary: false,
                primary: false,
            },
            false,
        );
        for mut fixture in [clustered_fixture(), common_handle_fixture(), secondary] {
            for row in [[1, 10], [2, 20]] {
                fixture
                    .table
                    .insert_row(
                        &[Datum::Int(row[0]), Datum::Int(row[1])],
                        &tidb_expr::NoColumns,
                    )
                    .unwrap();
            }
            commit(&fixture.buffer, &fixture.snapshot);
            let region = Arc::clone(&fixture.region);
            let catalog = catalog_of(fixture.table);
            let ctx = adaptive_read_ctx();
            for (hint, output) in [
                ("INL_JOIN", "i.b"),
                ("INL_HASH_JOIN", "i.b"),
                ("INL_JOIN", "i.a"),
                ("INL_HASH_JOIN", "i.a"),
            ] {
                region.sent.lock().unwrap().clear();
                let rows = run_select_on(
                    &format!("SELECT /*+ {hint}(i) */ o.a, {output} FROM t o JOIN t i ON o.a=i.a"),
                    &catalog,
                    &ctx,
                )
                .unwrap();
                assert_eq!(rows.len(), 2);
                let estimates = region.read_estimates();
                assert!(
                    estimates.len() >= 2,
                    "outer and inner requests: {estimates:?}"
                );
                // The handle-count arithmetic itself is
                // `cop_read_lookup_batches_use_their_table_width_and_handle_count`;
                // here every rebuilt reader must reach the wire with one.
                assert!(
                    estimates.iter().all(|(_, bytes)| *bytes >= 1),
                    "each rebuilt inner reader must carry its plan estimate: {estimates:?}"
                );
            }
        }
    }

    #[test]
    fn table_reader_lookup_retains_identity_outside_projection() {
        for mut fixture in [
            clustered_fixture(),
            common_handle_without_catalog_index_fixture(),
        ] {
            let mut handles = Vec::new();
            for row in [[1, 10], [2, 20], [3, 30]] {
                handles.push(
                    fixture
                        .table
                        .insert_row(&row.map(Datum::Int), &tidb_expr::NoColumns)
                        .unwrap(),
                );
            }
            commit(&fixture.buffer, &fixture.snapshot);
            fixture
                .region
                .reverse_unordered
                .store(true, Ordering::SeqCst);
            for keep in [vec![1], vec![1, 0], vec![]] {
                let staged = fixture
                    .table
                    .build_table_reader_from_routed_handles(
                        &handles,
                        None,
                        &keep,
                        &[],
                        &crate::RowDecodeContext::legacy_default(&SessionTimeZone::utc()),
                        &crate::remote_scan::pushdown_statement_context(&crate::StmtContext::for_query()),
                        2,
                    )
                    .unwrap()
                    .expect("integer/common handles use the shared table request");
                let (rows, _, _, _) = KvTable::finish_routed_rows_by_handles(&handles, staged)
                    .unwrap()
                    .expect("remote table rows");
                assert_eq!(
                    rows,
                    handles
                        .iter()
                        .enumerate()
                        .map(|(i, h)| {
                            let row = [Datum::Int(i as i64 + 1), Datum::Int((i as i64 + 1) * 10)];
                            (
                                h.clone(),
                                keep.iter().map(|offset| row[*offset].clone()).collect(),
                            )
                        })
                        .collect::<Vec<_>>()
                );
            }
        }
    }

    #[test]
    fn common_reader_lookup_and_probe_share_table_requests() {
        let mut fixture =
            fixture_with_columns(None, vec![column("a", 1), column("b", 2), column("c", 3)]);
        fixture.table.set_common_handle_offsets(vec![0, 1]);
        fixture.table.add_index(
            KvIndex {
                id: 1,
                name: "ib".into(),
                comment: String::new(),
                unique: false,
                column_offsets: vec![1],
                prefix_lengths: vec![-1],
                visible: true,
                global: false,
                global_index_version: 0,
                clustered_primary: false,
                primary: false,
            },
            false,
        );
        for row in [[1, 30, 100], [2, 20, 200], [3, 10, 300]] {
            fixture
                .table
                .insert_row(&row.map(Datum::Int), &tidb_expr::NoColumns)
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        let region = Arc::clone(&fixture.region);
        region.reverse_unordered.store(true, Ordering::SeqCst);
        let catalog = catalog_of(fixture.table);
        for (sql, expected) in [
            (
                "SELECT c FROM t FORCE INDEX(ib) WHERE b<40 ORDER BY b",
                vec![
                    vec![Datum::Int(300)],
                    vec![Datum::Int(200)],
                    vec![Datum::Int(100)],
                ],
            ),
            (
                "SELECT /*+ INL_JOIN(r) */ r.c FROM t l JOIN t r USE INDEX(ib) ON r.b=l.b WHERE l.a<3 ORDER BY l.a",
                vec![vec![Datum::Int(100)], vec![Datum::Int(200)]],
            ),
        ] {
            region.sent.lock().unwrap().clear();
            assert_eq!(
                run_select_on(sql, &catalog, &adaptive_read_ctx()).unwrap(),
                expected
            );
            let requests = region.read_estimates();
            assert!(
                requests.iter().any(|(index, bytes)| !*index && *bytes >= 1),
                "common table task must carry Go estimates: {sql}; {requests:?}"
            );
        }
    }

    #[test]
    fn common_reader_partition_order_retains_pruned_identity() {
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        crate::run_create_table_on("CREATE TABLE t (a BIGINT, b BIGINT, PRIMARY KEY(a,b) CLUSTERED) PARTITION BY HASH(a) PARTITIONS 2", &mut catalog).unwrap();
        let Some(crate::TableEntry::Kv(table)) = catalog.table_in("test", "t") else {
            panic!("KV table");
        };
        let partition = table.partition().unwrap().clone();
        let mut fixture = common_handle_without_catalog_index_fixture();
        fixture.table.set_partition(partition);
        for row in [[1, 30], [2, 20], [3, 10], [4, 40]] {
            fixture
                .table
                .insert_row(&row.map(Datum::Int), &tidb_expr::NoColumns)
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        for projected in [false, true] {
            for desc in [false, true] {
                let mut cursor = fixture
                    .table
                    .pushdown_row_cursor_with_context(
                        &[1],
                        &[],
                        projected.then_some([0].as_slice()),
                        None,
                        None,
                        None,
                        None,
                        desc,
                        true,
                        false,
                        &tidb_table::kv_table::RowDecodeContext::for_test_query_utc(),
                        &PushdownStatementContext::default(),
                        PushdownReadEngine::TiKv,
                        0,
                    )
                    .unwrap()
                    .expect("ordered common-handle partitions remain remote");
                let mut rows = Vec::new();
                while let Some(row) = cursor.next_row().unwrap() {
                    rows.push(row);
                }
                let mut expected = vec![
                    vec![Datum::Int(30)],
                    vec![Datum::Int(20)],
                    vec![Datum::Int(10)],
                    vec![Datum::Int(40)],
                ];
                if desc {
                    expected.reverse();
                }
                assert_eq!(rows, expected);
            }
        }
    }

    fn partition_reader_fixture() -> Fixture {
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        crate::run_create_table_on(
            "CREATE TABLE t(a BIGINT,b BIGINT,c BIGINT) PARTITION BY HASH(a) PARTITIONS 2",
            &mut catalog,
        )
        .unwrap();
        let Some(crate::TableEntry::Kv(table)) = catalog.table_in("test", "t") else {
            panic!("KV table")
        };
        let mut fixture =
            fixture_with_columns(None, vec![column("a", 1), column("b", 2), column("c", 3)]);
        fixture
            .table
            .set_partition(table.partition().unwrap().clone());
        for (id, name, offset) in [(1, "ia", 0), (2, "ib", 1)] {
            fixture.table.add_index(
                KvIndex {
                    id,
                    name: name.into(),
                    comment: String::new(),
                    unique: false,
                    column_offsets: vec![offset],
                    prefix_lengths: vec![-1],
                    visible: true,
                    global: false,
                    global_index_version: 0,
                    clustered_primary: false,
                    primary: false,
                },
                false,
            );
        }
        for row in [[1, 30, 100], [2, 20, 200], [3, 10, 300], [4, 40, 400]] {
            fixture
                .table
                .insert_row(&row.map(Datum::Int), &tidb_expr::NoColumns)
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        fixture
            .region
            .reverse_unordered
            .store(true, Ordering::SeqCst);
        fixture
    }

    #[test]
    fn partition_readers_retain_remote_policy() {
        let fixture = partition_reader_fixture();
        let region = Arc::clone(&fixture.region);
        let catalog = catalog_of(fixture.table);
        for (sql, values) in [
            ("SELECT c FROM t FORCE INDEX(ib) WHERE b<40 ORDER BY b", vec![300, 200, 100]),
            ("SELECT /*+ INL_JOIN(r) */ r.c FROM t l JOIN t r USE INDEX(ib) ON r.b=l.b WHERE l.a<3 ORDER BY l.a", vec![100, 200]),
            ("SELECT /*+ USE_INDEX_MERGE(t,ia,ib) */ c FROM t WHERE a<3 OR b<20 ORDER BY c", vec![100, 200, 300]),
        ] {
            region.sent.lock().unwrap().clear();
            let expected = values.into_iter().map(|value| vec![Datum::Int(value)]).collect::<Vec<_>>();
            assert_eq!(run_select_on(sql, &catalog, &adaptive_read_ctx()).unwrap(), expected);
            let requests = region.read_estimates();
            assert!(requests.iter().any(|(index, bytes)| !index && *bytes >= 1),
                "partition table tasks must use shared request policy: {sql}; {requests:?}");
        }
    }

    #[test]
    fn reader_task_ordinary_lookup_uses_table_width() {
        let mut fixture = fixture();
        fixture.table.add_index(
            KvIndex {
                id: 1,
                name: "ia".into(),
                comment: String::new(),
                unique: false,
                column_offsets: vec![0],
                prefix_lengths: vec![-1],
                visible: true,
                global: false,
                global_index_version: 0,
                clustered_primary: false,
                primary: false,
            },
            false,
        );
        for row in [[1, 10], [2, 20], [3, 30]] {
            fixture
                .table
                .insert_row(&row.map(Datum::Int), &tidb_expr::NoColumns)
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        let region = Arc::clone(&fixture.region);
        let catalog = catalog_of(fixture.table);
        let rows = run_select_on(
            "SELECT b FROM t USE INDEX(ia) WHERE a < 3 ORDER BY a",
            &catalog,
            &adaptive_read_ctx(),
        )
        .unwrap();
        assert_eq!(rows, vec![vec![Datum::Int(10)], vec![Datum::Int(20)]]);
        let estimates = region.read_estimates();
        // Go `buildNoRangeIndexLookUpReader` estimates the index side with
        // the TABLE's average row width, and each table task with that width
        // times its handle count (two here). The adjuster carries whole
        // bytes, so the table side is twice the index side up to rounding.
        let index = estimates
            .iter()
            .find(|(index, _)| *index)
            .expect("index request")
            .1;
        assert!(index >= 1, "the index side carries the table width");
        assert!(
            estimates
                .iter()
                .any(|(is_index, bytes)| !is_index && (*bytes == 2 * index || *bytes == 2 * index + 1)),
            "the table task is two handles of the table width: {estimates:?}"
        );
    }

    #[test]
    fn reader_task_index_merge_uses_shared_table_requests() {
        for mut fixture in [
            fixture(),
            clustered_fixture(),
            common_handle_without_catalog_index_fixture(),
        ] {
            for (id, name, offset) in [(1, "ia", 0), (2, "ib", 1)] {
                fixture.table.add_index(
                    KvIndex {
                        id,
                        name: name.into(),
                        comment: String::new(),
                        unique: false,
                        column_offsets: vec![offset],
                        prefix_lengths: vec![-1],
                        visible: true,
                        global: false,
                        global_index_version: 0,
                        clustered_primary: false,
                        primary: false,
                    },
                    false,
                );
            }
            for row in [[1, 30], [2, 20], [3, 10]] {
                fixture
                    .table
                    .insert_row(&row.map(Datum::Int), &tidb_expr::NoColumns)
                    .unwrap();
            }
            commit(&fixture.buffer, &fixture.snapshot);
            let region = Arc::clone(&fixture.region);
            region.reverse_unordered.store(true, Ordering::SeqCst);
            let catalog = catalog_of(fixture.table);
            let ctx = adaptive_read_ctx();
            let rows = run_select_on("SELECT /*+ USE_INDEX_MERGE(t, ia, ib) */ a,b FROM t WHERE a < 3 OR b < 20 ORDER BY a",
                &catalog, &ctx).unwrap();
            assert_eq!(
                rows,
                vec![
                    vec![Datum::Int(1), Datum::Int(30)],
                    vec![Datum::Int(2), Datum::Int(20)],
                    vec![Datum::Int(3), Datum::Int(10)]
                ]
            );
            let estimates = region.read_estimates();
            assert!(
                estimates.iter().filter(|(index, _)| *index).count() >= 2,
                "both partial paths: {estimates:?}"
            );
            let table = estimates
                .iter()
                .filter(|(index, _)| !index)
                .collect::<Vec<_>>();
            assert!(
                !table.is_empty(),
                "index merge must build table requests: {estimates:?}"
            );
            assert!(
                table.iter().all(|(_, bytes)| *bytes >= 1),
                "per-task handle estimates: {table:?}"
            );
            assert!(region
                .requested_flags()
                .iter()
                .all(|flags| *flags == ctx.push_down_flags()));
        }
    }

    /// `DAGRequest.flags` reaches the region from the STATEMENT, through the
    /// production request builder, and is not the literal `0` it used to be.
    ///
    /// `0` is TiKV's strictest branch: no truncation tolerated, no
    /// zero-in-date tolerated, division by zero an error. A `SELECT` that
    /// TiDB answers with a value plus a 1292 warning made the region fail the
    /// whole request instead.
    ///
    /// The expected value is COMPUTED from the same statement context the
    /// query ran under, not written down, so a change to the derivation moves
    /// both sides together; the second assertion is what keeps that from being
    /// vacuous if the derivation ever collapses to zero.
    #[test]
    fn the_statements_push_down_flags_reach_the_coprocessor_request() {
        let mut fixture = fixture();
        fixture
            .table
            .insert_row(&[Datum::Int(1), Datum::Int(10)], &tidb_expr::NoColumns)
            .unwrap();
        commit(&fixture.buffer, &fixture.snapshot);
        let region = Arc::clone(&fixture.region);

        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();
        run_select_on("SELECT a FROM t", &catalog, &ctx).unwrap();

        let flags = region.requested_flags();
        assert_eq!(flags, vec![ctx.push_down_flags()]);
        assert_ne!(
            flags[0], 0,
            "flags 0 is TiKV's strictest branch, which is the bug this pins"
        );
        assert_eq!(
            flags[0], 482,
            "a plain SELECT: TruncateAsWarning 2 | InSelectStmt 32 | \
             OverflowAsWarning 64 | IgnoreZeroInDate 128 | \
             DividedByZeroAsWarning 256"
        );
    }

    /// A different statement class sends different flags: the derivation's
    /// INPUTS are threaded, not one statement's output.
    ///
    /// Go's `*ast.InsertStmt` arm runs `GetTypeFlagsForInsert`, which under a
    /// strict `sql_mode` clears `TruncateAsWarning` and `IgnoreZeroInDate` and
    /// resolves `ErrGroupDividedByZero` to `LevelError` -- so the read half of
    /// a strict `INSERT ... SELECT` tells the region to FAIL on exactly the
    /// values a plain `SELECT` tells it to warn about.
    #[test]
    fn a_strict_insert_select_sends_different_flags_than_a_plain_select() {
        let mut fixture = fixture();
        fixture
            .table
            .insert_row(&[Datum::Int(1), Datum::Int(10)], &tidb_expr::NoColumns)
            .unwrap();
        commit(&fixture.buffer, &fixture.snapshot);
        let region = Arc::clone(&fixture.region);

        let catalog = catalog_of(fixture.table);
        // `for_dml(error_for_division_by_zero, strict, ignore_err)` with the INSERT arm's
        // statement class: the read half of `INSERT INTO u SELECT a FROM t`.
        let ctx = crate::StmtContext::for_dml(true, true, false)
            // TiDB's default `sql_mode` bits that `GetTypeFlagsForInsert`
            // reads; without them `IgnoreZeroInDate` is true and the flags
            // would be 136 rather than 8.
            .with_date_modes(tidb_datatype::DateModes {
                no_zero_date: true,
                no_zero_in_date: true,
                allow_invalid_dates: false,
            })
            .with_statement_class(crate::StatementClass::Insert);
        run_select_on("SELECT a FROM t", &catalog, &ctx).unwrap();

        let flags = region.requested_flags();
        assert_eq!(flags, vec![ctx.push_down_flags()]);
        assert_eq!(
            flags[0],
            tidb_sessionctx::statement_pushdown::FLAG_IN_INSERT_STMT,
            "a strict INSERT tolerates nothing, and says only which statement \
             it is"
        );
        assert_ne!(
            flags[0],
            crate::StmtContext::for_query().push_down_flags(),
            "hardcoding one statement's flags would make these equal"
        );
    }

    /// A warning TiKV reports lands in the STATEMENT'S buffer, which is what
    /// `SHOW WARNINGS` and the OK packet's count read.
    ///
    /// Every production site used to build a fresh `WarningCollector` here, so
    /// `response_channel` appended TiKV's warnings correctly, in Go's order,
    /// into a buffer that was then dropped.
    ///
    /// DEFERRED LIVE CHECK: only a real TiKV can produce the warning for the
    /// audit's named case, `SELECT ROUND(s) FROM t` with `s = '12abc'`, where
    /// TiDB reports the truncated value plus 1292. The fake below stands in
    /// for the region's `SelectResponse.warnings` field; that the region fills
    /// it under flags 482 is what a playground run must confirm.
    #[test]
    fn a_warning_the_region_reports_lands_in_the_statements_buffer() {
        let mut fixture = fixture();
        fixture
            .table
            .insert_row(&[Datum::Int(1), Datum::Int(10)], &tidb_expr::NoColumns)
            .unwrap();
        commit(&fixture.buffer, &fixture.snapshot);
        let region = Arc::clone(&fixture.region);
        *region.region_warning.lock().unwrap() =
            Some((1292, "Truncated incorrect DOUBLE value: '12abc'".to_owned()));

        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();
        run_select_on("SELECT a FROM t", &catalog, &ctx).unwrap();

        assert_eq!(
            ctx.take_warnings().iter().map(tidb_context::SqlWarn::parts).collect::<Vec<_>>(),
            vec![(
                tidb_context::WarningLevel::Warning,
                1292u16,
                "Truncated incorrect DOUBLE value: '12abc'".to_owned()
            )],
            "the sink the request carried is the statement's own, and keeps \
             the level TiKV's warning was collected under"
        );
    }

    /// The wire win: with a predicate lowered into the request, the rows that
    /// cross the network are the qualifying ones and not the relation.
    #[test]
    fn a_pushed_predicate_keeps_the_rejected_rows_off_the_wire() {
        let mut fixture = fixture();
        for a in 1..=100 {
            fixture
                .table
                .insert_row(&[Datum::Int(a), Datum::Int(a * 10)], &tidb_expr::NoColumns)
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        fixture.returned.store(0, Ordering::Relaxed);
        fixture.scanned.store(0, Ordering::Relaxed);

        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();
        let rows = run_select_on("SELECT a FROM t WHERE a > 97", &catalog, &ctx).unwrap();
        assert_eq!(
            rows,
            vec![
                vec![Datum::Int(98)],
                vec![Datum::Int(99)],
                vec![Datum::Int(100)]
            ]
        );
        assert_eq!(
            fixture.scanned.load(Ordering::Relaxed),
            100,
            "the coprocessor read the relation, as a full scan must"
        );
        assert_eq!(
            fixture.returned.load(Ordering::Relaxed),
            3,
            "but only the qualifying rows crossed the network"
        );
    }

    /// A clustered handle whose `WHERE` admits NO handle at all reads nothing
    /// -- it does not send a coprocessor request with no ranges.
    ///
    /// `id > 97 AND id < 97` builds an EMPTY range list, which the local cursor
    /// states exactly by opening no iterator; the coprocessor's `Ranges` list
    /// cannot state it, and the transport rejects the request instead
    /// (`missing_ranges`). Go master also reduces BETWEEN NULL AND NULL to
    /// TableDual, without reading storage. The control keeps the narrowed
    /// range on the coprocessor, so this is not "stop pushing ranges down".
    #[test]
    fn an_empty_handle_range_reads_nothing_instead_of_a_rangeless_request() {
        let mut fixture = clustered_fixture();
        for a in 1..=100 {
            fixture
                .table
                .insert_row(&[Datum::Int(a), Datum::Int(a * 10)], &tidb_expr::NoColumns)
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        fixture.returned.store(0, Ordering::Relaxed);

        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();
        assert_eq!(
            run_select_on("SELECT a FROM t WHERE a > 97 AND a < 97", &catalog, &ctx).unwrap(),
            Vec::<Vec<Datum>>::new()
        );
        assert_eq!(
            fixture.returned.load(Ordering::Relaxed),
            0,
            "no row crossed the network for an empty handle range"
        );
        // Go master fdfadb96b2: EXPLAIN returns TableDual, rows:0. Check
        // requests too: an empty result alone could hide a pointless scan.
        let (rows, ops) = capture_storage_ops(|| {
            run_select_on(
                "SELECT a FROM t WHERE a BETWEEN NULL AND NULL",
                &catalog,
                &ctx,
            )
            .unwrap()
        });
        assert_eq!(rows, Vec::<Vec<Datum>>::new());
        assert_eq!(ops, tidb_kv::storage::StorageOps::default());
        assert_eq!(
            fixture.returned.load(Ordering::Relaxed),
            0,
            "TableDual must not read the relation for NULL bounds"
        );

        // Control: a range that DOES admit rows still reaches the coprocessor.
        fixture.returned.store(0, Ordering::Relaxed);
        assert_eq!(
            run_select_on("SELECT a FROM t WHERE a BETWEEN 98 AND 100", &catalog, &ctx).unwrap(),
            vec![
                vec![Datum::Int(98)],
                vec![Datum::Int(99)],
                vec![Datum::Int(100)]
            ]
        );
        assert_eq!(
            fixture.returned.load(Ordering::Relaxed),
            3,
            "the narrowed range is still served remotely"
        );
    }

    /// A cap travels with the request when nothing is staged, so the
    /// coprocessor stops reading instead of returning the relation.
    #[test]
    fn a_pushed_limit_stops_the_remote_scan() {
        let mut fixture = fixture();
        for a in 1..=100 {
            fixture
                .table
                .insert_row(&[Datum::Int(a), Datum::Int(a * 10)], &tidb_expr::NoColumns)
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        fixture.scanned.store(0, Ordering::Relaxed);

        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();
        let rows = run_select_on("SELECT a FROM t LIMIT 4", &catalog, &ctx).unwrap();
        assert_eq!(rows.len(), 4);
        assert_eq!(
            fixture.scanned.load(Ordering::Relaxed),
            4,
            "the cap reached the coprocessor, which stopped there"
        );
    }

    /// The correctness core. A coprocessor answers from the snapshot, so the
    /// transaction's own staged writes must be merged back in and filtered by
    /// the same predicate: this is the remote twin of the byte-level test in
    /// `crate::predicate_pushdown`, and it must produce the identical answer.
    #[test]
    fn staged_rows_survive_the_remote_scan_and_are_filtered_by_the_same_predicate() {
        let mut fixture = fixture();
        fixture
            .region
            .reverse_unordered
            .store(true, Ordering::Relaxed);
        let committed_low = fixture
            .table
            .insert_row(&[Datum::Int(1), Datum::Int(10)], &tidb_expr::NoColumns)
            .unwrap();
        fixture
            .table
            .insert_row(&[Datum::Int(9), Datum::Int(90)], &tidb_expr::NoColumns)
            .unwrap();
        let committed_moved = fixture
            .table
            .insert_row(&[Datum::Int(2), Datum::Int(20)], &tidb_expr::NoColumns)
            .unwrap();
        commit(&fixture.buffer, &fixture.snapshot);

        // One open transaction stages all four shapes.
        fixture
            .table
            .insert_row(&[Datum::Int(7), Datum::Int(70)], &tidb_expr::NoColumns)
            .unwrap();
        fixture
            .table
            .insert_row(&[Datum::Int(3), Datum::Int(30)], &tidb_expr::NoColumns)
            .unwrap();
        fixture
            .table
            .update_row_with_context(
                &committed_moved,
                &[Datum::Int(8), Datum::Int(80)],
                &crate::StmtContext::for_dml(false, false, false),
            )
            .unwrap();
        fixture
            .table
            .delete_row_with_context(
                &committed_low,
                &crate::StmtContext::for_dml(false, false, false),
            )
            .unwrap();
        assert!(!fixture.buffer.is_empty(), "the writes are staged");

        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query_in_dirty_transaction(&[91]);
        assert_eq!(
            run_select_on("SELECT a, b FROM t WHERE a > 5 ORDER BY a", &catalog, &ctx).unwrap(),
            vec![
                vec![Datum::Int(7), Datum::Int(70)],
                vec![Datum::Int(8), Datum::Int(80)],
                vec![Datum::Int(9), Datum::Int(90)],
            ],
            "a staged INSERT and a staged UPDATE that satisfy the predicate are \
             kept, and the staged row that does not is dropped"
        );
        assert_eq!(
            run_select_on("SELECT a FROM t WHERE a < 5 ORDER BY a", &catalog, &ctx).unwrap(),
            vec![vec![Datum::Int(3)]],
            "the staged DELETE hid the committed row the coprocessor still \
             returns, and the updated row's old value went with it"
        );
        assert_eq!(
            run_select_on("SELECT a FROM t ORDER BY a", &catalog, &ctx).unwrap(),
            vec![
                vec![Datum::Int(3)],
                vec![Datum::Int(7)],
                vec![Datum::Int(8)],
                vec![Datum::Int(9)],
            ],
            "and the merged relation itself is the union-scan answer"
        );
    }

    /// A cap must not travel while writes are staged: the coprocessor's first
    /// `n` snapshot rows are the wrong prefix once a staged delete uncovers a
    /// row past them.
    #[test]
    fn a_cap_does_not_travel_while_writes_are_staged() {
        let mut fixture = fixture();
        for a in 1..=6 {
            fixture
                .table
                .insert_row(&[Datum::Int(a), Datum::Int(a * 10)], &tidb_expr::NoColumns)
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        // A cap of three applied at the coprocessor would have returned rows
        // 1..3, of which only one survives the overlay.
        fixture
            .table
            .delete_row_with_context(
                &TableHandle::Int(1),
                &crate::StmtContext::for_dml(false, false, false),
            )
            .unwrap();
        fixture
            .table
            .delete_row_with_context(
                &TableHandle::Int(2),
                &crate::StmtContext::for_dml(false, false, false),
            )
            .unwrap();

        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query_in_dirty_transaction(&[91]);
        assert_eq!(
            run_select_on("SELECT a FROM t LIMIT 3", &catalog, &ctx).unwrap(),
            vec![
                vec![Datum::Int(3)],
                vec![Datum::Int(4)],
                vec![Datum::Int(5)]
            ],
            "the cap is enforced on the merged stream, not on the snapshot's prefix"
        );
    }

    /// A DECIMAL-column counterpart of [`aggregate_fixture`]: task 71 traced a
    /// panic (`index out of bounds` in `Column::is_null`, reached from
    /// `hash_agg::input::prepare_decimal_cache`) to a NULL DECIMAL cell in a
    /// coprocessor partial-aggregate SUM/COUNT, at the default
    /// pipeline-eligible concurrency (`DEF_EXECUTOR_CONCURRENCY`, which is
    /// `> 1`, so `HashAggContext::pipeline_eligibility` selects the parallel
    /// worker path these tests exercise -- not the serial one). This fixture
    /// carries a real `NewDecimal` column, unlike [`aggregate_fixture`]'s
    /// `LongLong` one, so it actually reaches `prepare_decimal_cache`.
    fn decimal_aggregate_fixture() -> Fixture {
        let mut decimal_col = column("b", 2);
        decimal_col.field_type = FieldType::new(tidb_datatype::FieldTypeCode::NewDecimal);
        let mut fixture = fixture_with_columns(None, vec![column("a", 1), decimal_col]);
        for a in 1..=100 {
            let b = if a > 96 {
                Datum::Null
            } else {
                Datum::Decimal(tidb_datatype::Decimal::from_int(a * 10))
            };
            fixture
                .table
                .insert_row(&[Datum::Int(a), b], &tidb_expr::NoColumns)
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        fixture.returned.store(0, Ordering::Relaxed);
        fixture
    }

    /// Task 71's exact traced shape: every row a predicate admits has NULL in
    /// the summed DECIMAL column.
    #[test]
    fn a_decimal_sum_over_null_rows_the_predicate_admits_does_not_crash() {
        let fixture = decimal_aggregate_fixture();
        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();
        let (rows, wire) = run_counting(
            "SELECT COUNT(*), COUNT(b), SUM(b) FROM t WHERE a > 97",
            &catalog,
            &ctx,
            &fixture.returned,
        );
        assert_eq!(rows, vec![vec![Datum::Int(3), Datum::Int(0), Datum::Null]]);
        assert_eq!(wire, 3);

        let (rows, wire) = run_counting(
            "SELECT a, COUNT(*), SUM(b) FROM t WHERE a > 97 GROUP BY a ORDER BY a",
            &catalog,
            &ctx,
            &fixture.returned,
        );
        assert_eq!(
            rows,
            vec![
                vec![Datum::Int(98), Datum::Int(1), Datum::Null],
                vec![Datum::Int(99), Datum::Int(1), Datum::Null],
                vec![Datum::Int(100), Datum::Int(1), Datum::Null],
            ]
        );
        assert_eq!(wire, 3);
    }

    /// Every row NULL, not merely the ones a predicate admits.
    #[test]
    fn a_decimal_sum_over_an_entirely_null_column_does_not_crash() {
        let mut decimal_col = column("b", 2);
        decimal_col.field_type = FieldType::new(tidb_datatype::FieldTypeCode::NewDecimal);
        let mut fixture = fixture_with_columns(None, vec![column("a", 1), decimal_col]);
        for a in 1..=100 {
            fixture
                .table
                .insert_row(&[Datum::Int(a), Datum::Null], &tidb_expr::NoColumns)
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        fixture.returned.store(0, Ordering::Relaxed);
        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();

        let (rows, wire) = run_counting(
            "SELECT COUNT(*), COUNT(b), SUM(b) FROM t",
            &catalog,
            &ctx,
            &fixture.returned,
        );
        assert_eq!(
            rows,
            vec![vec![Datum::Int(100), Datum::Int(0), Datum::Null]]
        );
        assert_eq!(wire, 100);

        let (rows, _) = run_counting(
            "SELECT a, COUNT(*), SUM(b) FROM t GROUP BY a ORDER BY a",
            &catalog,
            &ctx,
            &fixture.returned,
        );
        assert_eq!(rows.len(), 100);
        assert!(rows
            .iter()
            .all(|row| row[2] == Datum::Null && row[1] == Datum::Int(1)));
    }

    /// A zero-row DECIMAL column: what task 71 flagged as a distinct
    /// possibility from an all-NULL one ("or the chunk/row indexing feeding
    /// it row 0 of what should be a zero-row column").
    #[test]
    fn a_decimal_sum_over_zero_matched_rows_does_not_crash() {
        let fixture = decimal_aggregate_fixture();
        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();
        let (rows, wire) = run_counting(
            "SELECT COUNT(*), COUNT(b), SUM(b) FROM t WHERE a > 10000",
            &catalog,
            &ctx,
            &fixture.returned,
        );
        assert_eq!(rows, vec![vec![Datum::Int(0), Datum::Int(0), Datum::Null]]);
        assert_eq!(wire, 0);
    }

    fn aggregate_fixture() -> Fixture {
        let mut fixture = fixture();
        for a in 1..=100 {
            // `b` is NULL for the three rows a `WHERE a > 97` keeps, plus one
            // it rejects, so COUNT(*), COUNT(b) and SUM(b) all differ.
            let b = if a > 96 {
                Datum::Null
            } else {
                Datum::Int(a * 10)
            };
            fixture
                .table
                .insert_row(&[Datum::Int(a), b], &tidb_expr::NoColumns)
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        fixture.returned.store(0, Ordering::Relaxed);
        fixture
    }

    /// One statement's answer and the rows its coprocessor scans matched at
    /// the region (`matched`, the fixture's counter): what the scan's
    /// Selection let through, which a partial aggregate above it then
    /// reduces before anything crosses the wire.
    fn run_counting(
        sql: &str,
        catalog: &Catalog,
        ctx: &crate::StmtContext,
        matched: &AtomicU64,
    ) -> (Vec<Vec<Datum>>, u64) {
        matched.store(0, Ordering::Relaxed);
        let rows = run_select_on(sql, catalog, ctx).unwrap();
        (rows, matched.load(Ordering::Relaxed))
    }

    /// An aggregate over a filtered table must not forfeit the pushed
    /// predicate.
    ///
    /// The aggregate pipeline returned from the driver BEFORE the predicate
    /// was ever offered to the scan, so `SELECT COUNT(*) ... WHERE a > 97`
    /// dragged all hundred rows across the network to return one -- while the
    /// identical `SELECT a ... WHERE a > 97` sent three. Nothing about the
    /// ANSWER differed, which is why only the receipt could see it. Go pushes
    /// the `Selection` into the `DataSource` in `rule_predicate_push_down`
    /// whether or not an `Aggregation` sits above it.
    #[test]
    fn an_aggregate_does_not_forfeit_the_pushed_predicate() {
        let fixture = aggregate_fixture();
        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();

        // The control: the same predicate WITHOUT an aggregate has always
        // narrowed the wire, so a difference below is the aggregate's doing
        // and not the predicate's pushability.
        let (rows, wire) = run_counting(
            "SELECT a FROM t WHERE a > 97",
            &catalog,
            &ctx,
            &fixture.returned,
        );
        assert_eq!(rows.len(), 3);
        assert_eq!(wire, 3);

        let (rows, wire) = run_counting(
            "SELECT COUNT(*), COUNT(b), SUM(b) FROM t WHERE a > 97",
            &catalog,
            &ctx,
            &fixture.returned,
        );
        assert_eq!(
            rows,
            vec![vec![Datum::Int(3), Datum::Int(0), Datum::Null]],
            "COUNT(*) counts the NULL rows COUNT(b) skips, and a SUM over no \
             non-NULL value is NULL rather than zero"
        );
        assert_eq!(wire, 3, "and only those three rows crossed the network");

        let (rows, wire) = run_counting(
            "SELECT a, COUNT(*) FROM t WHERE a > 97 GROUP BY a ORDER BY a",
            &catalog,
            &ctx,
            &fixture.returned,
        );
        assert_eq!(
            rows,
            vec![
                vec![Datum::Int(98), Datum::Int(1)],
                vec![Datum::Int(99), Datum::Int(1)],
                vec![Datum::Int(100), Datum::Int(1)],
            ]
        );
        assert_eq!(wire, 3, "GROUP BY does not forfeit it either");
    }

    /// The values a filtered aggregate must still produce, over the group the
    /// predicate leaves EMPTY and over the NULLs it keeps.
    #[test]
    fn a_pushed_predicate_does_not_move_an_aggregate_value() {
        let fixture = aggregate_fixture();
        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();

        // An empty input: SUM is NULL, COUNT is 0 -- and AVG is NULL, which is
        // what makes it something other than SUM/COUNT.
        let (rows, wire) = run_counting(
            "SELECT SUM(b), COUNT(*), COUNT(b), AVG(b) FROM t WHERE a > 1000",
            &catalog,
            &ctx,
            &fixture.returned,
        );
        assert_eq!(
            rows,
            vec![vec![Datum::Null, Datum::Int(0), Datum::Int(0), Datum::Null]]
        );
        assert_eq!(wire, 0, "a predicate that admits no row sends no row back");

        // The NULLs the predicate KEEPS, counted the two different ways.
        let (rows, wire) = run_counting(
            "SELECT COUNT(*), COUNT(b), SUM(b) FROM t WHERE b IS NULL",
            &catalog,
            &ctx,
            &fixture.returned,
        );
        assert_eq!(rows, vec![vec![Datum::Int(4), Datum::Int(0), Datum::Null]]);
        assert_eq!(wire, 4, "IS NULL is lowered, so only the NULL rows crossed");

        // And the whole relation, as the reference the narrowed answers must
        // agree with.
        let (rows, wire) = run_counting(
            "SELECT COUNT(*), COUNT(b), SUM(b) FROM t",
            &catalog,
            &ctx,
            &fixture.returned,
        );
        assert_eq!(
            rows,
            vec![vec![
                Datum::Int(100),
                Datum::Int(96),
                // MySQL's `SUM` over an integer column is DECIMAL, not
                // integer, and it keeps the argument's scale -- so the type is
                // pinned here alongside the value.
                Datum::Decimal(tidb_datatype::Decimal::from_literal("46560")),
            ]],
            "the sum of 10..960 by tens, over the 96 non-NULL rows"
        );
        assert_eq!(
            wire, 100,
            "an unfiltered aggregate still drags the relation: the AGGREGATE \
             is not pushed down, only the predicate below it"
        );
    }

    /// The receipt must not be BLIND on the cluster backend: a point get
    /// through it is one key lookup, no scan and no coprocessor request.
    ///
    /// Only the in-process backend used to be counted, so every cluster read
    /// reported zero of everything -- a probe that answers "no reads" for a
    /// statement that read a row is worse than none, because a test that
    /// pins an access path against it passes for the wrong reason.
    #[test]
    fn a_cluster_point_get_is_one_key_lookup_and_no_coprocessor_request() {
        let mut fixture = clustered_fixture();
        for a in 1..=100 {
            fixture
                .table
                .insert_row(&[Datum::Int(a), Datum::Int(a * 10)], &tidb_expr::NoColumns)
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();

        let (rows, ops) =
            capture_storage_ops(|| run_select_on("SELECT b FROM t WHERE a = 5", &catalog, &ctx));
        assert_eq!(rows.unwrap(), vec![vec![Datum::Int(50)]]);
        assert_eq!(
            ops,
            tidb_kv::storage::StorageOps {
                gets: 1,
                scans: 0,
                cop_scans: 0,
                cop_rows: 0,
            },
            "the handle is known, so the plan reads the one key and opens \
             neither an iterator nor a coprocessor scan"
        );

        // The ordinary ranger, not the AST fast path, derives these keys.
        // Residuals, absent rows and ORDER/LIMIT still execute above the get.
        for (sql, expected) in [
            (
                "SELECT b FROM t WHERE a>=5 AND a<=5 AND b>1",
                vec![vec![Datum::Int(50)]],
            ),
            ("SELECT b FROM t WHERE a>=5 AND a<=5 AND b>50", vec![]),
            (
                "SELECT b FROM t WHERE a IN (5,7,999) AND b>50 ORDER BY b DESC LIMIT 1",
                vec![vec![Datum::Int(70)]],
            ),
        ] {
            let (rows, ops) = capture_storage_ops(|| run_select_on(sql, &catalog, &ctx).unwrap());
            assert_eq!(rows, expected, "{sql}");
            assert_eq!(
                ops,
                tidb_kv::storage::StorageOps {
                    gets: 1,
                    ..Default::default()
                },
                "{sql}"
            );
            let (fix, _) =
                tidb_fixcontrol::OptimizerFixControl::parse("52592:ON").unwrap();
            assert_eq!(
                fix.get_bool(tidb_fixcontrol::FIX_52592),
                Some(true)
            );
            let disabled = ctx.clone().with_optimizer_fix_control(fix);
            let (rows, ops) =
                capture_storage_ops(|| run_select_on(sql, &catalog, &disabled).unwrap());
            assert_eq!(rows, expected, "disabled: {sql}");
            assert!(
                ops.scans > 0 || ops.cop_scans > 0,
                "fix 52592 must retain scan execution: {sql}: {ops:?}"
            );
        }

        // Control: the same table read as a RANGE does send a coprocessor
        // request, so the zeros above are the point plan's and not a probe
        // that cannot see this backend at all.
        let (_, ops) = capture_storage_ops(|| {
            run_select_on("SELECT b FROM t WHERE a BETWEEN 5 AND 7", &catalog, &ctx)
        });
        assert_eq!(ops.gets, 0);
        assert_eq!((ops.cop_scans, ops.cop_rows), (1, 3));
    }

    /// Go keeps the selected PhysicalProjection inside the TableReader cop
    /// task. The physical-plan receipt must therefore narrow the remote row,
    /// not merely print that projection in EXPLAIN while a root ProjectionExec
    /// still receives every scan column.
    #[test]
    fn a_clean_clustered_range_sends_the_cop_projection() {
        let mut fixture = clustered_fixture();
        for a in 1..=10 {
            fixture
                .table
                .insert_row(&[Datum::Int(a), Datum::Int(a * 10)], &tidb_expr::NoColumns)
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        let region = Arc::clone(&fixture.region);
        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();

        let rows =
            run_select_on("SELECT b FROM t WHERE a BETWEEN 5 AND 7", &catalog, &ctx).unwrap();

        assert_eq!(
            rows,
            vec![
                vec![Datum::Int(50)],
                vec![Datum::Int(60)],
                vec![Datum::Int(70)]
            ]
        );
        assert_eq!(
            region.requested_output_offsets(),
            vec![vec![1]],
            "the selected cop projection must reach DAGRequest.output_offsets",
        );
    }

    /// A prepared cache hit recursively rebuilds the retained range tree and
    /// must preserve the same reader-local projection receipt as ordinary
    /// planning.
    #[test]
    fn a_cached_clustered_range_sends_the_cop_projection_after_rebuild() {
        let cache = crate::SessionPlanCache::default();
        let mut fixture = clustered_fixture();
        for a in 1..=10 {
            fixture
                .table
                .insert_row(&[Datum::Int(a), Datum::Int(a * 10)], &tidb_expr::NoColumns)
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        let region = Arc::clone(&fixture.region);
        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();
        let statement = tidb_parser::parse("SELECT b FROM t WHERE a BETWEEN ? AND ?").unwrap();
        let plan = Arc::new(
            build_prepared_select_plan(&statement, 2, &catalog, DEFAULT_DATABASE, &ctx)
                .expect("the range statement is cacheable"),
        );
        let environment = PreparedPlanCacheEnvironment::default();

        for (bounds, expected, cache_hit) in [
            (
                [Datum::Int(5), Datum::Int(7)],
                vec![Datum::Int(50), Datum::Int(60), Datum::Int(70)],
                false,
            ),
            (
                [Datum::Int(8), Datum::Int(9)],
                vec![Datum::Int(80), Datum::Int(90)],
                true,
            ),
        ] {
            let execution = plan
                .bind(
                    &cache,
                    &bounds,
                    &catalog,
                    DEFAULT_DATABASE,
                    &ctx,
                    &environment,
                )
                .expect("the integer bounds bind");
            assert_eq!(execution.cache_hit(), cache_hit);
            let (_, rows) =
                run_prepared_select_for_test(&execution, &catalog, DEFAULT_DATABASE, &ctx).unwrap();
            let expected = expected
                .into_iter()
                .map(|value| vec![value])
                .collect::<Vec<_>>();
            assert_eq!(rows, expected);
        }
        assert_eq!(
            region.requested_output_offsets(),
            vec![vec![1], vec![1]],
            "both the first build and recursive cache rebuild keep the cop projection",
        );
    }

    /// Go `copr`'s other error (`coprocessor.go`'s `handleCopResponse`) is
    /// the read's error: a region that cannot run the DAG it was sent --
    /// unistore refusing an executor shape by name -- fails the statement
    /// with that message rather than being answered from somewhere else.
    #[test]
    fn a_region_other_error_is_the_statements_error() {
        let fixture = aggregate_fixture();
        let region = Arc::clone(&fixture.region);
        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();
        region.refuse.store(true, Ordering::Relaxed);
        let error = run_select_on("SELECT a FROM t WHERE a > 97", &catalog, &ctx).unwrap_err();
        assert!(
            format!("{error:?}").contains("this closure-executor shape is a later course"),
            "{error:?}"
        );
    }

    /// The remote path may only ever narrow: with the coprocessor lowering
    /// nothing of the predicate, the local test still answers exactly.
    #[test]
    fn an_unlowered_predicate_is_still_answered_exactly() {
        let mut fixture = fixture();
        for a in 1..=20 {
            fixture
                .table
                .insert_row(&[Datum::Int(a), Datum::Int(a * 10)], &tidb_expr::NoColumns)
                .unwrap();
        }
        commit(&fixture.buffer, &fixture.snapshot);
        let catalog = catalog_of(fixture.table);
        let ctx = crate::StmtContext::for_query();
        assert_eq!(
            run_select_on("SELECT a FROM t WHERE b + 1 > 190", &catalog, &ctx).unwrap(),
            vec![vec![Datum::Int(19)], vec![Datum::Int(20)]],
            "the arithmetic conjunct is residual, so it runs above the scan"
        );
    }
}
