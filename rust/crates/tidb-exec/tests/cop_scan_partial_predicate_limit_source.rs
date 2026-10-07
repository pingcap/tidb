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

//! The row cap and a partly-lowered predicate, at the layer that answers rows.
//!
//! `cop_scan` lowers the conjuncts it can express and leaves the rest to the
//! scan source above it. A row cap sent alongside a predicate that only partly
//! travelled makes TiKV count its `limit` rows against a *weaker* filter, and
//! the local pass then removes some of them -- a silently short answer. This
//! drives a real [`CopScanSource`] against a coprocessor fake and asserts the
//! rows the `SELECT` returns, because a DAG-shape assertion would pass for a
//! request that still answers wrongly.


#![allow(missing_docs)]

use std::sync::{Arc, Mutex};

use prost::Message;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_distsql::query_runtime::{QueryResponse, QueryResponseError, QueryResultSubset};
use tidb_distsql::{QueryDispatch, QueryTransport, TransportRequest};
use tidb_exec::cop_scan::CopScanSource;
use tidb_exec::real_tikv_read::RealTiKvSessionTransportFactory;
use tidb_executor::cluster_storage::{
    ClusterSnapshot, ClusterTableStorage, MutationBuffer, SnapshotPairs,
};
use tidb_executor::driver::{run_select_on, Catalog};
use tidb_executor::kv_table::{KvColumn, KvTable};
use tidb_executor::remote_scan::{
    PushdownAggregateFunction, PushdownAggregateKind, PushdownPartialAggregate, PushdownScanColumn,
    PushdownScanRequest, PushdownScanner, PushdownStatementContext,
};
use tidb_executor::storage::StorageError;
use tidb_executor::StmtContext;
use tidb_proto::tipb::{Chunk, DagRequest, ExecType, Expr, ExprType, SelectResponse};
use tidb_txnkv::Key;

fn region_rows() -> Vec<(i64, i64)> {
    (1..=20)
        .map(|id| (id, if id % 4 == 0 { 7 } else { 0 }))
        .collect()
}

#[derive(Debug)]
struct EmptySnapshot;

impl ClusterSnapshot for EmptySnapshot {
    fn get(&mut self, _key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(None)
    }

    fn scan(
        &mut self,
        _start: &Key,
        _end: &Key,
        _limit: Option<usize>,
    ) -> Result<SnapshotPairs, StorageError> {
        Ok(Vec::new())
    }

    fn start_ts(&self) -> u64 {
        4_242
    }
}

fn encode_signed_varint(output: &mut Vec<u8>, value: i64) {
    let mut unsigned = (value as u64) << 1;
    if value < 0 {
        unsigned = !unsigned;
    }
    while unsigned >= 0x80 {
        output.push((unsigned as u8) | 0x80);
        unsigned >>= 7;
    }
    output.push(unsigned as u8);
}

/// What the fake coprocessor did with one request, so the test can say which
/// half of the seam produced the answer.
#[derive(Clone, Debug, Default)]
struct Observation {
    read_replica_scope: String,
    is_staleness: bool,
    request_concurrency: isize,
    request_limit_size: u64,
    max_execution_time_ms: u64,
    busy_threshold_ns: i64,
    read_timeout_ms: u64,
    adaptive_labels: Vec<tidb_txnkv::StoreLabel>,
    adaptive_mode_after_adjustment: tidb_txnkv::ReplicaReadType,
    /// Go `SessionVars.GetReplicaRead()` carried by RequestBuilder.
    replica_read: tidb_txnkv::ReplicaReadType,
    /// Direction carried by the DistSQL request, which orders region tasks.
    request_desc: bool,
    /// Direction carried by the TableScan executor, which orders rows inside
    /// each region.
    scan_desc: bool,
    /// Common-handle column ids carried by the table scan.
    primary_column_ids: Vec<i64>,
    /// Common-handle prefix column ids carried by the table scan.
    primary_prefix_column_ids: Vec<i64>,
    /// The cap the DAG carried, if any.
    remote_limit: Option<u64>,
    /// Conditions in the DAG's Selection, if any.
    conditions: usize,
    /// Rows the fake sent back.
    rows_sent: usize,
    /// The children of the COUNT function sent to TiKV, if this is an
    /// aggregate request.
    count_children: Vec<Expr>,
    /// Physical hash/stream operator selected for the aggregation DAG node.
    aggregate_type: Option<i32>,
}

#[derive(Debug, Default)]
struct FakeRegion {
    observations: Mutex<Vec<Observation>>,
    response_threads: Mutex<Vec<std::thread::ThreadId>>,
}

/// A coprocessor that executes the DAG it is given, the way TiKV does: the
/// table scan reads the region's rows in key order, the Selection admits them
/// (`id > 0` holds for every fixture row, which is asserted below rather than
/// assumed), and the Limit -- if the request carries one -- stops the scan.
struct FakeTransport {
    region: Arc<FakeRegion>,
}

struct FakeResponse {
    subsets: Vec<QueryResultSubset>,
    region: Arc<FakeRegion>,
}

impl QueryResponse for FakeResponse {
    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        self.region
            .response_threads
            .lock()
            .unwrap()
            .push(std::thread::current().id());
        Ok(if self.subsets.is_empty() {
            None
        } else {
            Some(self.subsets.remove(0))
        })
    }

    fn close(&mut self) {
        self.subsets.clear();
    }
}

impl QueryTransport for FakeTransport {
    type Response = FakeResponse;

    fn send(
        &mut self,
        request: &TransportRequest,
        _dispatch: &QueryDispatch,
    ) -> Result<Option<Self::Response>, String> {
        let metadata = request.metadata();
        let bytes = metadata.data.clone().expect("a DAG request");
        let dag = DagRequest::decode(bytes.as_slice()).expect("the request is a TiDB DAG");
        let scan = dag.executors[0]
            .tbl_scan
            .as_ref()
            .expect("the first executor is the table scan");
        let column_ids: Vec<i64> = scan
            .columns
            .iter()
            .map(|column| column.column_id.unwrap_or(-1))
            .collect();
        let mut observation = Observation::default();
        observation.request_concurrency = metadata.concurrency;
        observation.request_limit_size = metadata.limit_size;
        observation.replica_read = metadata.replica_read;
        observation.read_replica_scope = metadata.read_replica_scope.clone();
        observation.is_staleness = metadata.is_staleness;
        observation.max_execution_time_ms = metadata.max_execution_time_ms;
        observation.busy_threshold_ns = metadata.store_busy_threshold_ns;
        observation.read_timeout_ms = metadata.tikv_client_read_timeout_ms;
        let mut adjusted = metadata.clone();
        if let Some(adjuster) = metadata.closest_replica_read_adjuster.as_ref() {
            adjuster.adjust(&mut adjusted, 1);
        }
        observation.adaptive_mode_after_adjustment = adjusted.replica_read;
        observation.adaptive_labels = adjusted.match_store_labels.clone();
        observation.request_desc = metadata.desc;
        observation.scan_desc = scan.desc.unwrap_or(false);
        observation.primary_column_ids = scan.primary_column_ids.clone();
        observation.primary_prefix_column_ids = scan.primary_prefix_column_ids.clone();
        for executor in &dag.executors {
            if executor.tp == Some(ExecType::TypeLimit as i32) {
                observation.remote_limit = executor.limit.as_ref().and_then(|limit| limit.limit);
            }
            if executor.tp == Some(ExecType::TypeSelection as i32) {
                observation.conditions = executor
                    .selection
                    .as_ref()
                    .map_or(0, |selection| selection.conditions.len());
            }
            if executor.tp == Some(ExecType::TypeAggregation as i32)
                || executor.tp == Some(ExecType::TypeStreamAgg as i32)
            {
                observation.aggregate_type = executor.tp;
                let aggregation = executor
                    .aggregation
                    .as_ref()
                    .expect("an aggregation executor carries its descriptor");
                assert_eq!(
                    aggregation.streamed, None,
                    "Go's list DAG carries the mode only in Executor.tp"
                );
                let count = aggregation
                    .agg_func
                    .iter()
                    .find(|function| function.tp == Some(ExprType::Count as i32))
                    .expect("the aggregate request carries COUNT");
                assert_eq!(
                    count.agg_func_mode,
                    Some(tidb_proto::tipb::AggFunctionMode::Partial1Mode as i32)
                );
                observation.count_children = count.children.clone();
            }
        }

        let mut rows_data = Vec::new();
        let mut sent = 0usize;
        if observation.count_children.is_empty() && observation.aggregate_type.is_some() {
            // Still return a valid partial count when the malformed request
            // has no child, so the regression fails on the encoded DAG rather
            // than on response decoding.
            rows_data.push(8);
            encode_signed_varint(&mut rows_data, region_rows().len() as i64);
            sent = 1;
        } else if !observation.count_children.is_empty() {
            rows_data.push(8);
            encode_signed_varint(&mut rows_data, region_rows().len() as i64);
            sent = 1;
        } else {
            for (id, tag) in region_rows() {
                assert!(id > 0, "the fixture's lowered conjunct admits every row");
                if observation
                    .remote_limit
                    .is_some_and(|limit| sent as u64 >= limit)
                {
                    break;
                }
                for column_id in &column_ids {
                    // The handle column (`_tidb_rowid`, id -1) carries the row's
                    // handle, which is the row's `id` here.
                    let value = match column_id {
                        2 => tag,
                        _ => id,
                    };
                    rows_data.push(8);
                    encode_signed_varint(&mut rows_data, value);
                }
                sent += 1;
            }
        }
        observation.rows_sent = sent;
        self.region
            .observations
            .lock()
            .unwrap()
            .push(observation.clone());

        let response = SelectResponse {
            chunks: vec![Chunk {
                rows_data: Some((rows_data).into()),
                rows_meta: Vec::new(),
            }],
            ..SelectResponse::default()
        };
        Ok(Some(FakeResponse {
            subsets: vec![QueryResultSubset {
                data: response.encode_to_vec().into(),
                runtime: None,
            }],
            region: Arc::clone(&self.region),
        }))
    }
}

struct FakeFactory {
    region: Arc<FakeRegion>,
}

impl RealTiKvSessionTransportFactory for FakeFactory {
    type Transport = FakeTransport;

    fn open_session_transport(&self) -> Result<Self::Transport, String> {
        Ok(FakeTransport {
            region: Arc::clone(&self.region),
        })
    }
}

fn column(name: &str, id: i64, unsigned: bool) -> KvColumn {
    let mut field_type = FieldType::new(FieldTypeCode::LongLong);
    if unsigned {
        field_type.add_flags(32);
    }
    KvColumn {
        name: name.to_owned(),
        id,
        field_type,
        column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
        default_value: None,
        origin_default: None,
        comment: String::new(),
        generated: None,
    }
}

/// `t(id BIGINT, tag BIGINT UNSIGNED)` read through a real [`CopScanSource`].
///
/// The query below compares `tag` with the *string* `'7'`. That is the point:
/// the coprocessor can describe the column, so the scan is served remotely,
/// and the driver pushes the conjunct -- but the Selection lowering refuses a
/// non-integer constant, because Go rewrites one through
/// `RefineComparedConstant` rather than sending it as written. So `tag = '7'`
/// stays behind while `id > 0` travels. That is the partial lowering the cap
/// must not accompany.
fn fixture() -> (Catalog, Arc<FakeRegion>) {
    let region = Arc::new(FakeRegion::default());
    let factory = Arc::new(FakeFactory {
        region: Arc::clone(&region),
    });
    let scanner = Arc::new(CopScanSource::new(factory));
    let snapshot: Arc<Mutex<dyn ClusterSnapshot>> = Arc::new(Mutex::new(EmptySnapshot));
    let columns = vec![column("id", 1, false), column("tag", 2, true)];
    let storage = ClusterTableStorage::new(MutationBuffer::new(), snapshot)
        .with_remote_scanner(scanner as Arc<dyn PushdownScanner>);
    let mut catalog = Catalog::default();
    catalog.register_kv("t", KvTable::with_storage(91, columns, Box::new(storage)));
    (catalog, region)
}

#[test]
fn simple_scan_limit_preserves_request_builder_concurrency_and_limit_size() {
    for (limit, concurrency) in [(1, 1), (100_000, 15)] {
        let (catalog, region) = fixture();
        let rows = run_select_on(
            &format!("SELECT id FROM t LIMIT {limit}"),
            &catalog,
            &StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(rows.len(), (limit as usize).min(region_rows().len()));
        let observations = region.observations.lock().unwrap();
        let [observation] = observations.as_slice() else {
            panic!("one cop request: {observations:?}");
        };
        assert_eq!(observation.remote_limit, Some(limit));
        assert_eq!(
            (
                observation.request_concurrency,
                observation.request_limit_size
            ),
            (concurrency, limit)
        );
    }
}

/// A `LIMIT` over a predicate only half of which reached TiKV must still
/// return the rows the query asked for. With the cap travelling regardless,
/// TiKV counted five rows against `id > 0` alone, the local `tag = 7` pass
/// removed four of them, and the statement answered one row.
#[test]
fn a_limit_over_a_partly_lowered_predicate_returns_every_qualifying_row() {
    let (catalog, region) = fixture();
    let rows = run_select_on(
        "SELECT id FROM t WHERE id > 0 AND tag = '7' LIMIT 5",
        &catalog,
        &StmtContext::for_query(),
    )
    .expect("the scan is served by the coprocessor");
    assert_eq!(
        rows,
        vec![
            vec![Datum::Int(4)],
            vec![Datum::Int(8)],
            vec![Datum::Int(12)],
            vec![Datum::Int(16)],
            vec![Datum::Int(20)],
        ],
        "MySQL returns five rows here; a cap counted against the weaker \
         remote filter returns one"
    );

    let observations = region.observations.lock().unwrap();
    let [observation] = observations.as_slice() else {
        panic!("exactly one coprocessor request: {observations:?}");
    };
    assert_eq!(
        observation.conditions, 1,
        "only `id > 0` lowered; `tag = '7'` stayed behind"
    );
    assert_eq!(
        observation.remote_limit, None,
        "so the cap must not have travelled with it"
    );
    assert_eq!(observation.rows_sent, region_rows().len());
}

/// A synchronous SQL worker must pull its own coprocessor iterator. Spawning
/// an OS-thread producer and rendezvousing through a bounded channel turns
/// every small OLTP range read into two scheduler crossings; Go's equivalent
/// iterator is pulled by the statement goroutine itself.
#[test]
fn coprocessor_response_is_pulled_by_query_worker() {
    let query_thread = std::thread::current().id();
    let (catalog, region) = fixture();

    let rows = run_select_on(
        "SELECT id FROM t WHERE id > 0",
        &catalog,
        &StmtContext::for_query(),
    )
    .expect("the scan is served by the coprocessor");
    assert_eq!(rows.len(), region_rows().len());

    let response_threads = region.response_threads.lock().unwrap();
    assert!(
        !response_threads.is_empty(),
        "the response iterator was pulled"
    );
    assert!(
        response_threads
            .iter()
            .all(|thread| *thread == query_thread),
        "the query ran on {query_thread:?}, but the response was pulled on \
         {response_threads:?}"
    );
}

/// The same invariant with a **pushed builtin call** as the conjunct that did
/// travel: `ROUND(id)` lowers through the push-down catalog, `tag = '7'` still
/// does not, so the predicate is again only half lowered and the cap must again
/// stay behind.
///
/// Widening what can be pushed widens what can be *partly* pushed, so this is
/// the invariant re-proved for the newly pushable family rather than assumed to
/// carry over.
#[test]
fn a_limit_over_a_partly_lowered_builtin_predicate_returns_every_qualifying_row() {
    let (catalog, region) = fixture();
    let rows = run_select_on(
        "SELECT id FROM t WHERE round(id) AND tag = '7' LIMIT 5",
        &catalog,
        &StmtContext::for_query(),
    )
    .expect("the scan is served by the coprocessor");
    assert_eq!(
        rows,
        vec![
            vec![Datum::Int(4)],
            vec![Datum::Int(8)],
            vec![Datum::Int(12)],
            vec![Datum::Int(16)],
            vec![Datum::Int(20)],
        ],
        "every row `ROUND(id) AND tag = 7` selects, up to the cap"
    );

    let observations = region.observations.lock().unwrap();
    let [observation] = observations.as_slice() else {
        panic!("exactly one coprocessor request: {observations:?}");
    };
    assert_eq!(
        observation.conditions, 1,
        "only `ROUND(id)` lowered; `tag = '7'` stayed behind"
    );
    assert_eq!(
        observation.remote_limit, None,
        "so the cap must not have travelled with it"
    );
    assert_eq!(observation.rows_sent, region_rows().len());
}

/// And the other side of the same invariant: when the *whole* predicate is a
/// pushed builtin, the cap does travel, so the widening did not cost the
/// saving the cap exists for.
///
/// Every fixture row has `id >= 1`, so `ROUND(id)` is truthy for all of them
/// and the rows the cap admits are the rows the query wants.
#[test]
fn a_limit_over_a_fully_lowered_builtin_predicate_travels_with_it() {
    let (catalog, region) = fixture();
    let rows = run_select_on(
        "SELECT id FROM t WHERE round(id) LIMIT 5",
        &catalog,
        &StmtContext::for_query(),
    )
    .expect("the scan is served by the coprocessor");
    assert_eq!(
        rows,
        (1..=5).map(|id| vec![Datum::Int(id)]).collect::<Vec<_>>()
    );

    let observations = region.observations.lock().unwrap();
    let [observation] = observations.as_slice() else {
        panic!("exactly one coprocessor request: {observations:?}");
    };
    assert_eq!(observation.conditions, 1);
    assert_eq!(observation.request_concurrency, 15);
    assert_eq!(observation.request_limit_size, 5);
    assert_eq!(
        observation.remote_limit,
        Some(5),
        "nothing stayed behind, so the cap travels"
    );
    assert_eq!(
        observation.rows_sent, 5,
        "and only the capped rows crossed the wire"
    );
}

/// FAIL-BEFORE/PASS-AFTER: a physical access receipt does not imply that it
/// consumed a root-only Selection. Go leaves `TAN` above the reader because
/// TiKV does not support it; the local Selection must therefore still run,
/// and its Limit must remain at root as well.
#[test]
fn a_root_only_predicate_is_not_swallowed_by_the_access_receipt() {
    // Go permits arithmetic and NOT in TiKV, but TAN is unsupported even
    // beneath NOT. An empty receipt must not bypass these root predicates,
    // project away their inputs, or count raw rows against a pushed TopN/Limit.
    let ints = |values: &[i64]| values.iter().copied().map(Datum::Int).collect::<Vec<_>>();
    for (sql, expected) in [
        (
            "SELECT id FROM t WHERE tan(id) > 0 LIMIT 5",
            ints(&[1, 4, 7, 10, 13]),
        ),
        (
            "SELECT id FROM t WHERE NOT (tan(id) > 0) LIMIT 5",
            ints(&[2, 3, 5, 6, 8]),
        ),
        (
            "SELECT tag FROM t WHERE tan(id) > 0 LIMIT 5",
            vec![
                Datum::UInt(0),
                Datum::UInt(7),
                Datum::UInt(0),
                Datum::UInt(0),
                Datum::UInt(0),
            ],
        ),
        (
            "SELECT id FROM t WHERE tan(id) > 0 ORDER BY id DESC LIMIT 3",
            ints(&[20, 19, 17]),
        ),
    ] {
        let (catalog, region) = fixture();
        let rows = run_select_on(sql, &catalog, &StmtContext::for_query())
            .expect("the scan is served by the coprocessor");
        assert_eq!(
            rows,
            expected
                .into_iter()
                .map(|value| vec![value])
                .collect::<Vec<_>>(),
            "{sql}"
        );

        let observations = region.observations.lock().unwrap();
        let [observation] = observations.as_slice() else {
            panic!("exactly one coprocessor request: {observations:?}");
        };
        assert_eq!(observation.conditions, 0, "{sql}");
        assert_eq!(observation.remote_limit, None, "{sql}");
        assert_eq!(observation.rows_sent, region_rows().len(), "{sql}");
    }
}

/// Go rewrites `COUNT(*)` to `COUNT(1)` before `AggFuncToPBExpr` serializes
/// every aggregate argument. TiKV's COUNT parser therefore always reads one
/// child; a zero-child COUNT is malformed and panics the region worker. The
/// surrounding `PhysicalTableScan.ToPB` also carries common-handle metadata;
/// omitting it makes TiKV look for primary-key columns in the row value.
#[test]
fn count_star_lowers_to_count_with_one_constant_child() {
    for streamed in [false, true] {
        let region = Arc::new(FakeRegion::default());
        let scanner = CopScanSource::new(Arc::new(FakeFactory {
            region: Arc::clone(&region),
        }));
        let mut count_type = FieldType::new(FieldTypeCode::LongLong);
        count_type.set_flen(21);
        count_type.set_decimal(0);
        let request = PushdownScanRequest {
            table_id: 91,
            index: None,
            read_engine: tidb_executor::remote_scan::PushdownReadEngine::TiKv,
            schema_version: 0,
            columns: vec![PushdownScanColumn {
                id: 1,
                field_type: FieldType::new(FieldTypeCode::LongLong),
                is_handle: false,
                // No stored `DEFAULT` for this synthetic column.
                origin_default: None,
            }],
            handle_index: None,
            primary_column_ids: vec![1],
            primary_prefix_column_ids: vec![1],
            predicates: Vec::new(),
            output_offsets: None,
            topn: None,
            limit: None,
            prefix_limit: None,
            paging_min_size: None,
            aggregate: Some(PushdownPartialAggregate::Global {
                streamed,
                functions: vec![PushdownAggregateFunction {
                    kind: PushdownAggregateKind::Count,
                    input: None,
                    output_type: count_type,
                }],
            }),
            keep_order: false,
            allow_unordered_response: false,
            // Go's `desc` on the TableScan executor: this request walks its one
            // range forwards.
            desc: false,
            snapshot_ts: 4_242,
            ranges: vec![(Key::from_bytes(b"a"), Key::from_bytes(b"z"))],
            range_hints: Vec::new(),
            statement: PushdownStatementContext {
                replica_read: tidb_txnkv::ReplicaReadType::Follower,
                ..PushdownStatementContext::default()
            },
        };
        let mut stream = scanner
            .open(&request)
            .expect("the partial count is served by the coprocessor");
        assert_eq!(
            stream.next_row().expect("the partial count row"),
            Some(vec![Datum::Int(region_rows().len() as i64)])
        );
        stream.close();

        let observations = region.observations.lock().unwrap();
        let [observation] = observations.as_slice() else {
            panic!("exactly one coprocessor request: {observations:?}");
        };
        assert_eq!(
            observation.aggregate_type,
            Some(if streamed {
                ExecType::TypeStreamAgg as i32
            } else {
                ExecType::TypeAggregation as i32
            })
        );
        assert_eq!(
            observation.replica_read,
            tidb_txnkv::ReplicaReadType::Follower
        );
        assert_eq!(observation.primary_column_ids, [1]);
        assert_eq!(observation.primary_prefix_column_ids, [1]);
        let [argument] = observation.count_children.as_slice() else {
            panic!(
                "Go sends COUNT(1) with one child, got {:?}",
                observation.count_children
            );
        };
        assert_eq!(argument.tp, Some(ExprType::Int64 as i32));
        assert_eq!(
            tidb_codec::decode_int(argument.val.as_deref().expect("the literal value"))
                .expect("the signed literal encoding"),
            (&[][..], 1)
        );
    }
}

/// A globally ordered descending scan has two direction owners: TableScan
/// walks each region backwards, while the DistSQL request visits the region
/// tasks backwards. Setting only the former returns descending islands in
/// ascending region order once a table spans more than one region.
#[test]
fn a_descending_scan_marks_both_the_dag_and_dist_sql_request() {
    let region = Arc::new(FakeRegion::default());
    let scanner = CopScanSource::new(Arc::new(FakeFactory {
        region: Arc::clone(&region),
    }));
    let request = PushdownScanRequest {
        table_id: 91,
        index: None,
        read_engine: tidb_executor::remote_scan::PushdownReadEngine::TiKv,
        schema_version: 0,
        columns: vec![PushdownScanColumn {
            id: 1,
            field_type: FieldType::new(FieldTypeCode::LongLong),
            is_handle: true,
            origin_default: None,
        }],
        handle_index: Some(0),
        primary_column_ids: Vec::new(),
        primary_prefix_column_ids: Vec::new(),
        predicates: Vec::new(),
        output_offsets: None,
        topn: None,
        limit: Some(1),
        prefix_limit: None,
        paging_min_size: None,
        aggregate: None,
        keep_order: true,
        desc: true,
        // A globally ordered scan cannot accept unordered region responses.
        allow_unordered_response: false,
        snapshot_ts: 4_242,
        ranges: vec![(Key::from_bytes(b"a"), Key::from_bytes(b"z"))],
        range_hints: Vec::new(),
        statement: PushdownStatementContext::default(),
    };
    let mut stream = scanner
        .open(&request)
        .expect("the descending scan is served by the coprocessor");
    assert!(stream.next_row().expect("the first row").is_some());
    stream.close();

    let observations = region.observations.lock().unwrap();
    let [observation] = observations.as_slice() else {
        panic!("exactly one coprocessor request: {observations:?}");
    };
    assert!(
        observation.scan_desc,
        "TableScan must walk rows backwards inside each region"
    );
    assert!(
        observation.request_desc,
        "DistSQL must visit region tasks backwards for global order"
    );
}

#[test]
fn adaptive_small_scan_and_execution_deadline_reach_coprocessor() {
    let (catalog, region) = fixture();
    let ctx = StmtContext::for_query()
        .with_replica_read(tidb_txnkv::ReplicaReadType::ClosestAdaptive)
        .with_stats_load_policy(0, true, 731);
    let rows = run_select_on("SELECT id FROM t LIMIT 1", &catalog, &ctx).unwrap();
    assert_eq!(rows.len(), 1);
    let observations = region.observations.lock().unwrap();
    let observation = &observations[0];
    assert_eq!(observation.max_execution_time_ms, 731);
    assert_eq!(
        observation.adaptive_mode_after_adjustment,
        tidb_txnkv::ReplicaReadType::Leader
    );
}

#[test]
fn adaptive_small_scan_falls_back_after_region_tasks_exist() {
    let (catalog, region) = fixture();
    let ctx =
        StmtContext::for_query().with_replica_read(tidb_txnkv::ReplicaReadType::ClosestAdaptive);
    run_select_on("SELECT id FROM t LIMIT 1", &catalog, &ctx).unwrap();
    assert_eq!(
        region.observations.lock().unwrap()[0].adaptive_mode_after_adjustment,
        tidb_txnkv::ReplicaReadType::Leader
    );
}

#[test]
fn coprocessor_read_policy_preserves_large_reads_and_explicit_modes() {
    use tidb_executor::remote_scan::CoprocessorReadPolicy;
    for (mode, threshold, expected, labels, sql, row_count) in [
        (
            tidb_txnkv::ReplicaReadType::ClosestAdaptive,
            4096,
            tidb_txnkv::ReplicaReadType::ClosestAdaptive,
            1,
            "SELECT id FROM t",
            20,
        ),
        (
            tidb_txnkv::ReplicaReadType::ClosestAdaptive,
            4096,
            tidb_txnkv::ReplicaReadType::Leader,
            0,
            "SELECT id FROM t LIMIT 1",
            1,
        ),
        (
            tidb_txnkv::ReplicaReadType::ClosestAdaptive,
            0,
            tidb_txnkv::ReplicaReadType::ClosestAdaptive,
            1,
            "SELECT id FROM t LIMIT 1",
            1,
        ),
        (
            tidb_txnkv::ReplicaReadType::ClosestAdaptive,
            i64::MAX,
            tidb_txnkv::ReplicaReadType::Leader,
            0,
            "SELECT id FROM t LIMIT 1",
            1,
        ),
        (
            tidb_txnkv::ReplicaReadType::Follower,
            i64::MAX,
            tidb_txnkv::ReplicaReadType::Follower,
            0,
            "SELECT id FROM t LIMIT 1",
            1,
        ),
    ] {
        let (catalog, region) = fixture();
        let context = StmtContext::for_query()
            .with_replica_read(mode)
            .with_coprocessor_read_policy(CoprocessorReadPolicy {
                closest_read_threshold: threshold,
                busy_threshold_ns: 1234,
                read_timeout_ms: 731,
            });
        let rows = run_select_on(sql, &catalog, &context).unwrap();
        assert_eq!(rows.len(), row_count);
        let observations = region.observations.lock().unwrap();
        let observation = &observations[0];
        assert_eq!(
            (observation.busy_threshold_ns, observation.read_timeout_ms),
            (1234, 731)
        );
        assert_eq!(observation.adaptive_mode_after_adjustment, expected);
        assert_eq!(observation.adaptive_labels.len(), labels);
    }
}

#[test]
fn read_consistency_scan_preserves_transaction_scope_and_staleness() {
    let (catalog, region) = fixture();
    for (scope, stale) in [("zone-a", true), ("zone-b", false), ("global", false)] {
        let context = StmtContext::for_query()
            .with_replica_read(tidb_txnkv::ReplicaReadType::Follower)
            .with_read_consistency(scope, stale);
        assert_eq!(
            run_select_on("SELECT id FROM t", &catalog, &context)
                .unwrap()
                .len(),
            20
        );
        let observations = region.observations.lock().unwrap();
        let actual = observations.last().unwrap();
        assert_eq!(actual.read_replica_scope, scope);
        assert_eq!(actual.is_staleness, stale);
    }
}
