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

//! A narrowed scan output travels when every conjunct lowers, and refuses
//! when one stays behind.
//!
//! `PushdownScanRequest::output_offsets` promises the caller narrower rows.
//! The contract says a backend must refuse unless it can lower EVERY
//! predicate -- once the narrower row crosses the wire, no residual conjunct
//! can be repeated locally over the dropped columns. Both halves are pinned
//! here against a coprocessor fake that projects rows exactly the way TiKV
//! reads `DAGRequest.output_offsets`: it encodes only the offsets the DAG
//! names, so a wrong lowering is visible as wrong data, not as nothing.

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
use tidb_executor::predicate_pushdown::{ScanComparison, ScanComparisonOp, ScanPredicate};
use tidb_executor::remote_scan::{
    PushdownScanColumn, PushdownScanRequest, PushdownScanner, PushdownScannerError,
    PushdownStatementContext,
};
use tidb_executor::storage::StorageError;
use tidb_proto::tipb::{Chunk, DagRequest, ExecType, Expr, SelectResponse};
use tidb_txnkv::Key;

/// Go `mysql.UnsignedFlag`.
const UNSIGNED_FLAG: u32 = 32;

/// Rows the region holds, as `(id, tag)`. Every id is positive, so the one
/// conjunct that lowers (`id > 0`) admits all of them; the tag differs from
/// the id so narrowing to the tag cannot be confused with returning both.
fn region_rows() -> Vec<(i64, i64)> {
    (1..=6).map(|id| (id, id * 10)).collect()
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

/// What the fake coprocessor did with one request.
#[derive(Clone, Debug, Default)]
struct Observation {
    /// The DAG's `output_offsets`: what TiKV would encode per row.
    dag_output_offsets: Vec<u32>,
    /// Conditions in the DAG's Selection.
    conditions: usize,
    /// Values the fake encoded, flattened in wire order.
    sent_values: Vec<i64>,
}

#[derive(Debug, Default)]
struct FakeRegion {
    observations: Mutex<Vec<Observation>>,
}

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

/// Evaluates one lowered `id > 0` condition over a row, the way TiKV does it.
fn admits(condition: &Expr, id: i64) -> bool {
    let sig = condition.sig.expect("the condition carries its signature");
    assert_eq!(sig, tidb_proto::tipb::ScalarFuncSig::GtInt as i32);
    let children = &condition.children;
    let constant = children
        .iter()
        .find(|child| child.tp != Some(tidb_proto::tipb::ExprType::ColumnRef as i32) && child.val.is_some())
        .expect("the constant operand");
    let (_, literal) =
        tidb_codec::decode_int(constant.val.as_deref().expect("the literal carries bytes"))
            .expect("a comparable-int literal");
    id > literal
}

impl QueryTransport for FakeTransport {
    type Response = FakeResponse;

    fn send(
        &mut self,
        request: &TransportRequest,
        _dispatch: &QueryDispatch,
    ) -> Result<Option<Self::Response>, String> {
        let bytes = request.metadata().data.clone().expect("a DAG request");
        let dag = DagRequest::decode(bytes.as_slice()).expect("the request is a TiDB DAG");
        let conditions: Vec<Expr> = dag
            .executors
            .iter()
            .filter(|executor| executor.tp == Some(ExecType::TypeSelection as i32))
            .flat_map(|executor| {
                executor
                    .selection
                    .as_ref()
                    .map(|selection| selection.conditions.clone())
                    .unwrap_or_default()
            })
            .collect();

        // TiKV encodes exactly `DAGRequest.output_offsets`, in that order --
        // the projection this test pins.
        let dag_offsets: Vec<u32> = dag.output_offsets.clone();
        let mut observation = Observation {
            dag_output_offsets: dag_offsets.clone(),
            conditions: conditions.len(),
            sent_values: Vec::new(),
        };
        let mut rows_data = Vec::new();
        for (id, tag) in region_rows() {
            if !conditions.iter().all(|condition| admits(condition, id)) {
                continue;
            }
            for offset in &dag_offsets {
                let value = match *offset {
                    1 => tag,
                    _ => id,
                };
                rows_data.push(8);
                encode_signed_varint(&mut rows_data, value);
                observation.sent_values.push(value);
            }
        }
        self.region.observations.lock().unwrap().push(observation);

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

fn bigint() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
}

fn column(id: i64) -> PushdownScanColumn {
    PushdownScanColumn {
        id,
        field_type: bigint(),
        is_handle: false,
        origin_default: None,
    }
}

/// Column 1 as UNSIGNED, so a negative constant against it is the shape this
/// lowering refuses.
fn unsigned_column(id: i64) -> PushdownScanColumn {
    let mut field_type = bigint();
    field_type.add_flags(UNSIGNED_FLAG);
    PushdownScanColumn {
        id,
        field_type,
        is_handle: false,
        origin_default: None,
    }
}

/// The lowered conjunct: `id > 0` over offset 0.
fn lowered_conjunct() -> ScanPredicate {
    ScanPredicate::Compare(ScanComparison {
        collation: tidb_datatype::Collation::Utf8Mb4Bin,
        column_offset: 0,
        column_type: bigint(),
        literal_type: bigint(),
        op: ScanComparisonOp::Gt,
        literal: Datum::Int(0),
        column_on_left: true,
    })
}

/// A conjunct this lowering REFUSES: an unsigned column against a negative
/// constant, the exact shape Go's `refineArgsByUnsignedFlag` leaves alone.
fn residual_conjunct() -> ScanPredicate {
    let mut unsigned = bigint();
    unsigned.add_flags(UNSIGNED_FLAG);
    ScanPredicate::Compare(ScanComparison {
        collation: tidb_datatype::Collation::Utf8Mb4Bin,
        column_offset: 0,
        column_type: unsigned.clone(),
        literal_type: unsigned,
        op: ScanComparisonOp::Gt,
        literal: Datum::Int(-1),
        column_on_left: true,
    })
}

fn scanner(region: &Arc<FakeRegion>) -> CopScanSource<FakeFactory> {
    CopScanSource::new(Arc::new(FakeFactory {
        region: Arc::clone(region),
    }))
}

fn request(
    predicates: Vec<ScanPredicate>,
    output_offsets: Option<Vec<usize>>,
) -> PushdownScanRequest {
    request_over(vec![column(1), column(2)], predicates, output_offsets)
}

fn request_over(
    columns: Vec<PushdownScanColumn>,
    predicates: Vec<ScanPredicate>,
    output_offsets: Option<Vec<usize>>,
) -> PushdownScanRequest {
    PushdownScanRequest {
        table_id: 91,
        index: None,
        columns,
        handle_index: None,
        primary_column_ids: vec![1],
        primary_prefix_column_ids: vec![1],
        predicates,
        output_offsets,
        topn: None,
        limit: None,
        aggregate: None,
        keep_order: false,
        desc: false,
        allow_unordered_response: false,
        read_ahead_batches: tidb_executor::remote_scan::DEFAULT_SCAN_READ_AHEAD_BATCHES,
        snapshot_ts: 4_242,
        ranges: vec![(Key::from_bytes(b"a"), Key::from_bytes(b"z"))],
        statement: PushdownStatementContext::default(),
    }
}

/// A projected scan whose every conjunct lowers sends the narrowed offsets in
/// the DAG and consumes NARROWED rows back: only the tag column crosses the
/// wire, once per surviving row.
#[test]
fn a_fully_lowered_projected_scan_sends_its_output_offsets_and_reads_narrow_rows() {
    let region = Arc::new(FakeRegion::default());
    let mut stream = scanner(&region)
        .open(&request(vec![lowered_conjunct()], Some(vec![1])))
        .expect("the narrowing travels when every predicate lowers");
    assert!(
        stream.predicates_applied(),
        "every conjunct was reported lowered"
    );
    let mut rows = Vec::new();
    while let Some(row) = stream.next_row().expect("the next narrowed row") {
        assert_eq!(row.len(), 1, "only the projected column crosses the wire");
        rows.push(row[0].clone());
    }
    stream.close();
    assert_eq!(
        rows,
        region_rows()
            .into_iter()
            .map(|(_, tag)| Datum::Int(tag))
            .collect::<Vec<_>>()
    );

    let observations = region.observations.lock().unwrap();
    let [observation] = observations.as_slice() else {
        panic!("exactly one coprocessor request: {observations:?}");
    };
    assert_eq!(
        observation.dag_output_offsets,
        [1],
        "the DAG carries the caller's projection, not identity"
    );
    assert_eq!(
        observation.conditions, 1,
        "the conjunct travelled beside it"
    );
    assert_eq!(observation.sent_values.len(), region_rows().len());
}

/// The other half of the contract: a residual conjunct means the backend must
/// refuse the narrowing BY NAME -- silently serving full-width rows for a
/// narrowed request would mis-shape the caller's row, and serving narrowed
/// rows would make the answer depend on a filter nobody applied. Nothing may
/// reach the region either way.
#[test]
fn a_projected_scan_with_a_residual_conjunct_is_refused_before_the_wire() {
    let region = Arc::new(FakeRegion::default());
    let stream = scanner(&region).open(&request_over(
        vec![unsigned_column(1), column(2)],
        vec![residual_conjunct()],
        Some(vec![1]),
    ));
    let reason = match stream {
        Err(PushdownScannerError::Unsupported(reason)) => reason,
        Err(other) => panic!("an unsupported refusal, not {other:?}"),
        Ok(_) => panic!("narrowing over a residual conjunct must be refused"),
    };
    assert!(
        reason.contains("does not narrow output columns"),
        "the refusal names the shape: {reason}"
    );

    let observations = region.observations.lock().unwrap();
    assert!(
        observations.is_empty(),
        "no request may travel behind a refusal: {observations:?}"
    );
}
