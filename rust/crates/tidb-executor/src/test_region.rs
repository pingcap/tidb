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

//! The store behind a test table's `kv.Client`. Go's executor tests run on
//! mockstore's unistore, which answers the encoded DAG; [`TestRegion`] does
//! the same over the committed bytes a test hands it, records every request
//! as [`SentRequest`] (the DAG and the `kv.Request` fields a test asserts
//! on), and can answer canned rows or fail a read instead.

use std::sync::{Arc, Mutex};

use prost::Message;
use tidb_datatype::Datum;
use tidb_proto::tipb::{self, DagRequest, ExecType, Executor, Expr, ExprType};
use tidb_txnkv::{QueryResponseError, QueryResultSubset, TransportRequest};

/// One request a [`TestRegion`] was sent, decoded.
#[derive(Clone, Debug)]
pub(crate) struct SentRequest {
    /// The encoded DAG.
    pub(crate) dag: DagRequest,
    /// `kv.Request.KeepOrder`.
    pub(crate) keep_order: bool,
    /// `kv.Request.Desc`: the region tasks' order.
    pub(crate) desc: bool,
    /// `kv.Request.KeyRanges`, flattened.
    pub(crate) ranges: Vec<(Vec<u8>, Vec<u8>)>,
    /// The ranges' row-count hints (Go `SetTableHandles`), flattened.
    pub(crate) range_hints: Vec<usize>,
    /// The per-task network estimate the closest-replica adjuster carries,
    /// as whole bytes (Go `newClosestReadAdjuster`'s `netDataSize`), read
    /// back by probing the adjuster under a threshold of one byte. `None`
    /// unless the statement reads with `closest-adaptive`.
    pub(crate) estimated_net_bytes: Option<u64>,
}

impl SentRequest {
    /// Decodes `request` as a test region receives it.
    pub(crate) fn decode(request: &TransportRequest) -> Self {
        let metadata = request.metadata();
        let dag = DagRequest::decode(metadata.data.as_deref().expect("a DAG request"))
            .expect("the request is a TiDB DAG");
        let ranges = metadata
            .key_ranges
            .as_ref()
            .map(|ranges| {
                ranges
                    .partitions()
                    .iter()
                    .flatten()
                    .map(|range| {
                        (
                            range.start_key.as_bytes().to_vec(),
                            range.end_key.as_bytes().to_vec(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        let range_hints = metadata
            .key_ranges
            .as_ref()
            .map(|ranges| ranges.row_count_hints().iter().flatten().copied().collect())
            .unwrap_or_default();
        let estimated_net_bytes = metadata.closest_replica_read_adjuster.as_ref().map(|adjuster| {
            // Under a one-byte threshold a task of `count` passes exactly when
            // `bytes / count >= 1`; the largest such count is `floor(bytes)`.
            let (mut low, mut high) = (0_u64, 1_u64 << 48);
            while low < high {
                let middle = low + (high - low).div_ceil(2);
                let mut probe = tidb_txnkv::Request::clone(metadata);
                if adjuster.adjust(&mut probe, middle as usize) {
                    low = middle;
                } else {
                    high = middle - 1;
                }
            }
            low
        });
        Self {
            dag,
            keep_order: metadata.keep_order,
            desc: metadata.desc,
            ranges,
            range_hints,
            estimated_net_bytes,
        }
    }

    fn executor(&self, tp: ExecType) -> Option<&Executor> {
        self.dag.executors.iter().find(|executor| executor.tp() == tp)
    }

    /// Whether the DAG reads an index.
    pub(crate) fn is_index(&self) -> bool {
        self.dag.executors.first().map(Executor::tp) == Some(ExecType::TypeIndexScan)
    }

    /// The physical table the scan reads.
    pub(crate) fn table_id(&self) -> i64 {
        let scan = &self.dag.executors[0];
        scan.tbl_scan
            .as_ref()
            .and_then(|scan| scan.table_id)
            .or_else(|| scan.idx_scan.as_ref().and_then(|scan| scan.table_id))
            .expect("a scan names its table")
    }

    /// The index an index scan reads.
    pub(crate) fn index_id(&self) -> Option<i64> {
        self.dag.executors[0]
            .idx_scan
            .as_ref()
            .and_then(|scan| scan.index_id)
    }

    /// The position of the handle column in the scan schema (an integer
    /// primary key or `_tidb_rowid`, which the descriptor marks `pk_handle`).
    pub(crate) fn handle_index(&self) -> Option<usize> {
        let scan = &self.dag.executors[0];
        scan.tbl_scan
            .as_ref()
            .map(|scan| &scan.columns)
            .or_else(|| scan.idx_scan.as_ref().map(|scan| &scan.columns))
            .expect("a scan carries its columns")
            .iter()
            .position(|column| column.pk_handle == Some(true))
    }

    /// The TopN's sort keys, as scan-schema offsets.
    pub(crate) fn topn_offsets(&self) -> Vec<usize> {
        self.executor(ExecType::TypeTopN)
            .and_then(|executor| executor.top_n.as_ref())
            .map(|topn| {
                topn.order_by
                    .iter()
                    .flat_map(|item| item.expr.as_ref().map(column_ref_offsets).unwrap_or_default())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// A prefix Limit (Go `Limit.TruncateKeyExpr`): its count and the
    /// offset of the column whose prefix it counts distinct values of.
    pub(crate) fn prefix_limit(&self) -> Option<(u64, usize)> {
        let limit = self.executor(ExecType::TypeLimit)?.limit.as_ref()?;
        let [key] = limit.truncate_key_expr.as_slice() else {
            return None;
        };
        Some((limit.limit?, *column_ref_offsets(key).first()?))
    }

    /// The scanned columns' ids.
    pub(crate) fn column_ids(&self) -> Vec<i64> {
        let scan = &self.dag.executors[0];
        scan.tbl_scan
            .as_ref()
            .map(|scan| &scan.columns)
            .or_else(|| scan.idx_scan.as_ref().map(|scan| &scan.columns))
            .expect("a scan carries its columns")
            .iter()
            .map(|column| column.column_id())
            .collect()
    }

    /// The scan's `primary_column_ids`.
    pub(crate) fn primary_column_ids(&self) -> Vec<i64> {
        let scan = &self.dag.executors[0];
        scan.tbl_scan
            .as_ref()
            .map(|scan| scan.primary_column_ids.clone())
            .or_else(|| scan.idx_scan.as_ref().map(|scan| scan.primary_column_ids.clone()))
            .unwrap_or_default()
    }

    /// The scan's `primary_prefix_column_ids`.
    pub(crate) fn primary_prefix_column_ids(&self) -> Vec<i64> {
        self.dag.executors[0]
            .tbl_scan
            .as_ref()
            .map(|scan| scan.primary_prefix_column_ids.clone())
            .unwrap_or_default()
    }

    /// The scan executor's own direction.
    pub(crate) fn scan_desc(&self) -> bool {
        let scan = &self.dag.executors[0];
        scan.tbl_scan
            .as_ref()
            .and_then(|scan| scan.desc)
            .or_else(|| scan.idx_scan.as_ref().and_then(|scan| scan.desc))
            .unwrap_or(false)
    }

    /// The Selection's conditions; empty when none travelled.
    pub(crate) fn conditions(&self) -> &[Expr] {
        self.executor(ExecType::TypeSelection)
            .and_then(|executor| executor.selection.as_ref())
            .map_or(&[], |selection| selection.conditions.as_slice())
    }

    /// The Limit executor's count.
    pub(crate) fn limit(&self) -> Option<u64> {
        self.executor(ExecType::TypeLimit)
            .and_then(|executor| executor.limit.as_ref())
            .and_then(|limit| limit.limit)
    }

    /// Whether the DAG carries a partial aggregation.
    pub(crate) fn has_aggregation(&self) -> bool {
        self.dag.executors.iter().any(|executor| {
            matches!(
                executor.tp(),
                ExecType::TypeAggregation | ExecType::TypeStreamAgg
            )
        })
    }

    /// `DAGRequest.output_offsets` when they narrow a row read: `None` is
    /// every scanned column, in order.
    pub(crate) fn narrowed_output(&self) -> Option<Vec<u32>> {
        let width = self.column_ids().len() as u32;
        (self.has_aggregation() || self.dag.output_offsets != (0..width).collect::<Vec<_>>())
            .then(|| self.dag.output_offsets.clone())
    }
}

/// The scan-schema offsets every column reference in `expr` reads.
pub(crate) fn column_ref_offsets(expr: &Expr) -> Vec<usize> {
    let mut offsets = Vec::new();
    let mut stack = vec![expr];
    while let Some(expr) = stack.pop() {
        if expr.tp() == ExprType::ColumnRef {
            let (_, offset) = tidb_codec::decode_int(expr.val()).expect("a column offset");
            offsets.push(offset as usize);
        }
        stack.extend(expr.children.iter().rev());
    }
    offsets
}

/// The committed bytes as unistore's MVCC store, one transaction committed
/// at ts 2, below every test statement's snapshot.
pub(crate) fn committed_store(
    data: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
) -> tidb_unistore::mvcc_store::MvccStore {
    let mut store = tidb_unistore::mvcc_store::MvccStore::new();
    let mutations: Vec<tidb_proto::KvrpcMutation> = data
        .into_iter()
        .map(|(key, value)| tidb_proto::KvrpcMutation {
            op: tidb_proto::KvrpcOp::Put as i32,
            key,
            value,
            ..tidb_proto::KvrpcMutation::default()
        })
        .collect();
    let Some(primary) = mutations.first().map(|mutation| mutation.key.clone()) else {
        return store;
    };
    let keys: Vec<Vec<u8>> = mutations.iter().map(|mutation| mutation.key.clone()).collect();
    store
        .prewrite(&tidb_unistore::mvcc_store::PrewriteReq {
            mutations,
            primary_lock: primary,
            start_version: 1,
            ..tidb_unistore::mvcc_store::PrewriteReq::default()
        })
        .unwrap();
    store.commit(&keys, 1, 2).unwrap();
    store
}

/// What one coprocessor request over `ranges` produced, with each
/// executor's produced-row count from the execution summaries.
pub(crate) struct RegionAnswer {
    /// The response, summaries kept only when the DAG asked for them.
    pub(crate) select: tipb::SelectResponse,
    /// Rows the scan read.
    pub(crate) scanned: u64,
    /// Rows the last executor below any aggregation produced: what the
    /// region matched.
    pub(crate) matched: u64,
}

/// Runs `dag` over `ranges` on unistore's coprocessor.
pub(crate) fn run_dag(
    store: &mut tidb_unistore::mvcc_store::MvccStore,
    dag: &DagRequest,
    ranges: &[(Vec<u8>, Vec<u8>)],
    start_ts: u64,
) -> Result<RegionAnswer, String> {
    let mut summarized = dag.clone();
    summarized.collect_execution_summaries = Some(true);
    let response = tidb_unistore::cophandler::handle_cop_request(
        store,
        &tidb_proto::coprocessor::Request {
            tp: tidb_unistore::cophandler::REQ_TYPE_DAG,
            data: summarized.encode_to_vec(),
            ranges: ranges
                .iter()
                .map(|(start, end)| tidb_proto::coprocessor::KeyRange {
                    start: start.clone(),
                    end: end.clone(),
                })
                .collect(),
            start_ts,
            ..tidb_proto::coprocessor::Request::default()
        },
    );
    if !response.other_error.is_empty() {
        return Err(response.other_error);
    }
    let mut select =
        tipb::SelectResponse::decode(response.data.as_ref()).map_err(|error| error.to_string())?;
    let produced = |index: usize| {
        select
            .execution_summaries
            .get(index)
            .and_then(|summary| summary.num_produced_rows)
            .unwrap_or(0)
    };
    let scanned = produced(0);
    let matched = produced(
        dag.executors
            .iter()
            .rposition(|executor| {
                !matches!(
                    executor.tp(),
                    ExecType::TypeAggregation | ExecType::TypeStreamAgg
                )
            })
            .unwrap_or(0),
    );
    if !dag.collect_execution_summaries() {
        select.execution_summaries.clear();
    }
    Ok(RegionAnswer {
        select,
        scanned,
        matched,
    })
}

/// One subset carrying `rows` in the default (datum) encoding, which the
/// select result decodes against the request's own output types.
pub(crate) fn canned_rows(rows: &[Vec<Datum>]) -> QueryResultSubset {
    let mut rows_data = Vec::new();
    for row in rows {
        rows_data.extend(tidb_codec::encode_value(row).expect("a canned row encodes"));
    }
    QueryResultSubset {
        data: tipb::SelectResponse {
            chunks: vec![tipb::Chunk {
                rows_data: Some(rows_data.into()),
                rows_meta: Vec::new(),
            }],
            ..tipb::SelectResponse::default()
        }
        .encode_to_vec()
        .into(),
        runtime: None,
    }
}

/// A region's answers to one request, in completion order. Closing it
/// counts in `closed`.
pub(crate) struct Subsets {
    subsets: std::vec::IntoIter<Result<QueryResultSubset, QueryResponseError>>,
    closed: Option<Arc<std::sync::atomic::AtomicBool>>,
}

impl Subsets {
    /// The response yielding `subsets` in order.
    pub(crate) fn new(
        subsets: Vec<Result<QueryResultSubset, QueryResponseError>>,
        closed: Option<Arc<std::sync::atomic::AtomicBool>>,
    ) -> Box<Self> {
        Box::new(Self {
            subsets: subsets.into_iter(),
            closed,
        })
    }
}

impl tidb_txnkv::Response for Subsets {
    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        self.subsets.next().transpose()
    }

    fn close(&mut self) {
        self.subsets = Vec::new().into_iter();
        if let Some(closed) = &self.closed {
            closed.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

/// What a [`TestRegion`] answers one request with.
pub(crate) type Answer = Box<
    dyn Fn(&SentRequest) -> Vec<Result<QueryResultSubset, QueryResponseError>> + Send + Sync,
>;

/// A region that records every request and answers through `answer`.
pub(crate) struct TestRegion {
    /// Every request, in send order.
    pub(crate) sent: Mutex<Vec<SentRequest>>,
    answer: Answer,
    closed: Option<Arc<std::sync::atomic::AtomicBool>>,
}

impl std::fmt::Debug for TestRegion {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("TestRegion")
    }
}

impl TestRegion {
    /// A region answering through `answer`; closing a response sets
    /// `closed` when one is given.
    pub(crate) fn new(answer: Answer, closed: Option<Arc<std::sync::atomic::AtomicBool>>) -> Self {
        Self {
            sent: Mutex::new(Vec::new()),
            answer,
            closed,
        }
    }

    /// A region that answers every request with no rows.
    pub(crate) fn empty() -> Self {
        Self::new(Box::new(|_| Vec::new()), None)
    }

    /// A region that answers from `data`'s committed bytes.
    pub(crate) fn over(data: impl Fn() -> Vec<(Vec<u8>, Vec<u8>)> + Send + Sync + 'static) -> Self {
        Self::new(
            Box::new(move |sent| {
                let mut store = committed_store(data());
                vec![run_dag(&mut store, &sent.dag, &sent.ranges, 100)
                    .map(|answer| QueryResultSubset {
                        data: answer.select.encode_to_vec().into(),
                        runtime: None,
                    })
                    .map_err(|message| {
                        QueryResponseError::Source(format!("coprocessor other error: {message}"))
                    })]
            }),
            None,
        )
    }

    /// The last request sent.
    pub(crate) fn last(&self) -> Option<SentRequest> {
        self.sent.lock().unwrap().last().cloned()
    }
}

impl tidb_txnkv::Client for TestRegion {
    fn send(
        &self,
        request: &TransportRequest,
    ) -> Result<Box<dyn tidb_txnkv::Response + Send>, String> {
        let sent = SentRequest::decode(request);
        let subsets = (self.answer)(&sent);
        self.sent.lock().unwrap().push(sent);
        Ok(Subsets::new(subsets, self.closed.clone()))
    }

    fn is_request_type_supported(&self, _request_type: i64, _sub_type: i64) -> bool {
        true
    }
}

/// The committed bytes of an in-process table store.
pub(crate) fn storage_pairs(store: &mut dyn tidb_kv::storage::TableStorage) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut iter = store.iter(None, None).expect("an in-process store iterates");
    let mut pairs = Vec::new();
    while iter.valid() {
        pairs.push((iter.key().as_bytes().to_vec(), iter.value().to_vec()));
        iter.next().expect("an in-process iterator advances");
    }
    pairs
}
