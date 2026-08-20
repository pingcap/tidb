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
    PushdownAggregateKind, PushdownGlobalAggregateFunction, PushdownPartialAggregate,
    PushdownScanColumn, PushdownScanRequest, PushdownScanner, PushdownStatementContext,
};
use tidb_executor::storage::StorageError;
use tidb_executor::StmtContext;
use tidb_proto::tipb::{Chunk, DagRequest, ExecType, Expr, ExprType, SelectResponse};
use tidb_txnkv::Key;

/// Rows the region holds, as `(id, tag)`. Every id is positive, so the one
/// conjunct that lowers (`id > 0`) admits all of them; only every fourth row
/// carries the tag the query asks for.
fn region_rows() -> Vec<(i64, i64)> {
    (1..=20)
        .map(|id| (id, if id % 4 == 0 { 7 } else { 0 }))
        .collect()
}

/// The snapshot half of the session: empty, so every row the query sees came
/// from the coprocessor and the staged merge adds nothing.
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
}

#[derive(Debug, Default)]
struct FakeRegion {
    observations: Mutex<Vec<Observation>>,
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
}

impl QueryResponse for FakeResponse {
    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
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
            if executor.tp == Some(ExecType::TypeAggregation as i32) {
                let aggregation = executor
                    .aggregation
                    .as_ref()
                    .expect("an aggregation executor carries its descriptor");
                let count = aggregation
                    .agg_func
                    .iter()
                    .find(|function| function.tp == Some(ExprType::Count as i32))
                    .expect("the aggregate request carries COUNT");
                observation.count_children = count.children.clone();
            }
        }

        let mut rows_data = Vec::new();
        let mut sent = 0usize;
        if observation.count_children.is_empty() && dag.executors.iter().any(|executor| {
            executor.tp == Some(ExecType::TypeAggregation as i32)
        }) {
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
                rows_data: Some(rows_data),
                rows_meta: Vec::new(),
            }],
            ..SelectResponse::default()
        };
        Ok(Some(FakeResponse {
            subsets: vec![QueryResultSubset {
                data: response.encode_to_vec(),
                runtime: None,
            }],
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
    catalog.register_kv(
        "t",
        KvTable::with_storage(91, columns, Box::new(storage)),
    );
    (catalog, region)
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

/// Go rewrites `COUNT(*)` to `COUNT(1)` before `AggFuncToPBExpr` serializes
/// every aggregate argument. TiKV's COUNT parser therefore always reads one
/// child; a zero-child COUNT is malformed and panics the region worker. The
/// surrounding `PhysicalTableScan.ToPB` also carries common-handle metadata;
/// omitting it makes TiKV look for primary-key columns in the row value.
#[test]
fn count_star_lowers_to_count_with_one_constant_child() {
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
        columns: vec![PushdownScanColumn {
            id: 1,
            field_type: FieldType::new(FieldTypeCode::LongLong),
            is_handle: false,
        }],
        handle_index: None,
        primary_column_ids: vec![1],
        primary_prefix_column_ids: vec![1],
        predicates: Vec::new(),
        output_offsets: None,
        topn: None,
        limit: None,
        aggregate: Some(PushdownPartialAggregate::Global {
            functions: vec![PushdownGlobalAggregateFunction {
                kind: PushdownAggregateKind::Count,
                input: None,
                output_type: count_type,
            }],
        }),
        keep_order: false,
        read_ahead_batches: tidb_executor::remote_scan::DEFAULT_SCAN_READ_AHEAD_BATCHES,
        snapshot_ts: 4_242,
        ranges: vec![(Key::from_bytes(b"a"), Key::from_bytes(b"z"))],
        statement: PushdownStatementContext::default(),
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
