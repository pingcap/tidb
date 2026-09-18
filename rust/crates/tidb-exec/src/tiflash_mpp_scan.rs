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

//! The TiFlash MPP pushdown lowering: Go `pkg/store/copr/mpp.go` +
//! `pkg/executor/internal/mpp/local_mpp_coordinator.go`, narrowed to the
//! single-fragment table scan the planner builds for a forced TiFlash read
//! (TableReader -> ExchangeSender(PassThrough) -> TableScan, mpp version 3).
//!
//! The flow per open scan is Go's dispatch choreography: pick a live TiFlash
//! store (`GetAllStores` + the `engine=tiflash` label, `addTiFlashStoreInfo`),
//! collect the table's record-range regions from PD
//! (`constructMPPTasksImpl`'s region split), marshal the tree-form DAG with
//! the scan under a PassThrough exchange sender (`appendMPPDispatchReq` ->
//! `ConstructDAGReq`, `dagReq.EncodeType = TypeChunk` for the root fragment),
//! `DispatchMPPTask` it to the store, then `EstablishMPPConnection` and wrap
//! the packet stream (`MPPDataPacket.data` = tipb `SelectResponse`) in the
//! same `SelectResponseIter` the TiKV cop path consumes.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use prost::Message as _;
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_distsql::query_runtime::query_response::QueryResultSubset;
use tidb_distsql::{
    QueryResponseError, ResponseChannelError, SelectResponseIter, WarningCollector,
    mpp_result_metadata,
};
use tidb_executor::remote_scan::{
    PushdownReadEngine, PushdownRowStream, PushdownScanRequest, PushdownScannerError,
};
use tidb_executor::storage::StorageError;
use tidb_pd_client::{PdClient, PdKeyRange, PdStoreState};
use tidb_proto::mpp::{
    DispatchTaskRequest, EstablishMppConnectionRequest, MppDataPacket, TaskMeta,
};
use tidb_proto::tikvpb::tikv_client::TikvClient;
use tidb_proto::tipb::{
    DagRequest, EncodeType, Endian, ExchangeSender as PbExchangeSender, ExchangeType, ExecType,
    Executor, TableScan,
};

use crate::cop_scan::scan_column;
use crate::dag_request::{DEFAULT_DIV_PRECISION_INCREMENT, DagRequestContext, column_to_pb};

/// The process-owned MPP dispatch capability.
///
/// The PD client is shared with the node's other workers through `Clone`; the
/// tokio runtime is this module's own because MPP dispatch and the result
/// stream are direct store RPCs outside the BatchCommands transport the
/// session transport factory owns (Go opens the MPPConn stream on the TiKV
/// client directly, `mpp.go:235` EstablishMPPConns).
pub struct TiFlashMppScanSource {
    pd: Mutex<PdClient>,
    runtime: tokio::runtime::Runtime,
    /// Go `is.SchemaMetaVersion()`, read at dispatch time from the node's
    /// catalog watch: TiFlash resolves the request's table in the schema
    /// generation the coordinator names, so a stale or zero version makes
    /// even a synced table "not exist".
    schema_version: Box<dyn Fn() -> i64 + Send + Sync>,
}

impl std::fmt::Debug for TiFlashMppScanSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("TiFlashMppScanSource")
    }
}

impl TiFlashMppScanSource {
    /// Builds the source over the node's PD membership and catalog watch.
    pub fn new(pd: PdClient, schema_version: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        Self {
            pd: Mutex::new(pd),
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("the TiFlash MPP runtime starts"),
            schema_version: Box::new(schema_version),
        }
    }

    /// The address of one live TiFlash store, or why none is usable.
    fn tiflash_store_address(&self) -> Result<String, String> {
        let client = self
            .pd
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let stores = client.all_stores().map_err(|error| error.to_string())?;
        stores
            .iter()
            .find(|store| {
                store.state == PdStoreState::Up
                    && store.labels.iter().any(|(key, value)| {
                        key.eq_ignore_ascii_case("engine") && value.eq_ignore_ascii_case("tiflash")
                    })
            })
            .map(|store| store.address.clone())
            .ok_or_else(|| {
                "no live TiFlash store answers the dispatch: check the replica placement".to_owned()
            })
    }

    /// The regions covering the scan's record ranges, in key order. The
    /// ranges are the executor's WIRE record keys (flagged-int encodings),
    /// the same bytes a coprocessor request would carry.
    fn table_regions(
        &self,
        ranges: &[(tidb_txnkv::Key, tidb_txnkv::Key)],
    ) -> Result<(Vec<u8>, Vec<u8>, Vec<tidb_pd_client::PdRegion>), String> {
        let start = ranges
            .iter()
            .map(|(low, _)| low.as_slice().to_vec())
            .min()
            .ok_or_else(|| "the scan carries no key ranges".to_owned())?;
        let end = ranges
            .iter()
            .map(|(_, high)| high.as_slice().to_vec())
            .max()
            .ok_or_else(|| "the scan carries no key ranges".to_owned())?;
        let client = self
            .pd
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let regions = client
            .batch_scan_regions(
                &[PdKeyRange {
                    start_key: start.clone(),
                    end_key: end.clone(),
                }],
                10_000,
                false,
                false,
            )
            .map_err(|error| error.to_string())?;
        Ok((start, end, regions))
    }

    /// Opens one MPP-served scan.
    pub(crate) fn open_mpp(
        &self,
        request: &PushdownScanRequest,
    ) -> Result<Box<dyn PushdownRowStream>, PushdownScannerError> {
        let refuse = |reason: String| {
            PushdownScannerError::Backend(StorageError::Backend(format!("tiflash mpp: {reason}")))
        };
        if request.read_engine != PushdownReadEngine::TiFlash {
            return Err(refuse(
                "the request did not name the columnar engine".to_owned(),
            ));
        }
        // This lowering is the single-fragment full scan. Every carried
        // operator is refused BY NAME rather than served from row storage:
        // the caller asked for the columnar engine, and a silent TiKV
        // fallback would be the wrong answer, not a slower one.
        if !request.predicates.is_empty() {
            return Err(refuse("pushed Selection is not lowered yet".to_owned()));
        }
        if request.topn.is_some() {
            return Err(refuse("pushed TopN is not lowered yet".to_owned()));
        }
        if request.aggregate.is_some() {
            return Err(refuse("partial aggregation is not lowered yet".to_owned()));
        }
        if request.desc || request.keep_order {
            return Err(refuse("ordered scans are not lowered yet".to_owned()));
        }
        if request.index.is_some() {
            return Err(refuse("index scans have no TiFlash path".to_owned()));
        }
        let address = self.tiflash_store_address().map_err(refuse)?;
        let (start_key, end_key, regions) = self.table_regions(&request.ranges).map_err(refuse)?;
        if regions.is_empty() {
            return Err(refuse(
                "no region covers the table's record range".to_owned(),
            ));
        }

        // Go `appendMPPDispatchReq`: the root fragment's DAG carries the
        // exchange sender over the scan, every column as the output offset,
        // and TypeChunk encoding for the coordinator's own fragment.
        let (time_zone_name, time_zone_offset) = request.statement.time_zone.dag_zone();
        let mut context = DagRequestContext::new(
            time_zone_name,
            time_zone_offset,
            request.statement.push_down_flags,
            tidb_distsql::EncodeType::Chunk,
        );
        context.div_precision_increment = request.statement.div_precision_increment;
        let mut columns = request
            .columns
            .iter()
            .map(scan_column)
            .collect::<Option<Vec<tidb_planner::tikv_scan_spec::ScanColumnInfo>>>()
            .ok_or_else(|| refuse("a column has no bounded coprocessor descriptor".to_owned()))?;
        // Go `util.ColumnToProto` rewrites EVERY column's collation id for
        // the new-collation framework (binary included, so an INT reads
        // -63); the shared TiKV lowering keeps the un-rewritten binary
        // constant, which TiKV accepts. Match Go's wire bytes here.
        for column in &mut columns {
            column.collation = tidb_datatype::rewrite_new_collation_id_if_needed(column.collation);
        }
        let query_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs())
            .unwrap_or_default();

        // Go `EstablishMPPConns`' receiver meta: the TiDB coordinator-side
        // pseudo task, task_id -1. The SAME encoding rides the sender's
        // encoded_task_meta in the dispatch (Go `buildDAGRec` /
        // `appendMPPDispatchReq`), which is how TiFlash marks the task root.
        let receiver_meta = TaskMeta {
            start_ts: request.snapshot_ts,
            task_id: -1,
            gather_id: 1,
            query_ts,
            local_query_id: 1,
            server_id: u64::from(std::process::id()),
            mpp_version: 3,
            resource_group_name: request.statement.resource_group_name.clone(),
            ..TaskMeta::default()
        };
        let meta = TaskMeta {
            start_ts: request.snapshot_ts,
            task_id: 1,
            partition_id: -1,
            address: address.clone(),
            gather_id: 1,
            query_ts,
            local_query_id: 1,
            server_id: u64::from(std::process::id()),
            mpp_version: 3,
            // Go `MPPDispatchRequest.KeySpaceID`: for a classic API-V1
            // cluster the keyspace is the NULL keyspace, whose id is
            // 4294967295 (`keyspace.NullKeyspaceID`), NOT zero. TiFlash
            // registers every storage under this keyspace and resolves the
            // task's table through the SAME id; a zero here makes even a
            // synced table read as missing.
            keyspace_id: 4294967295,
            coordinator_address: "tidb-mpp-coordinator".to_owned(),
            report_execution_summary: false,
            api_version: 1,
            resource_group_name: request.statement.resource_group_name.clone(),
            connection_id: 0,
            connection_alias: String::new(),
            sql_digest: String::new(),
            plan_digest: String::new(),
        };

        let scan_executor = Executor {
            tp: Some(ExecType::TypeTableScan as i32),
            tbl_scan: Some(TableScan {
                table_id: Some(request.table_id),
                columns: columns.iter().map(column_to_pb).collect(),
                desc: Some(false),
                primary_column_ids: request.primary_column_ids.clone(),
                next_read_engine: Some(tidb_proto::tipb::EngineType::Local as i32),
                primary_prefix_column_ids: request.primary_prefix_column_ids.clone(),
                keep_order: Some(false),
                is_fast_scan: Some(false),
                max_wait_time_ms: Some(0),
            }),
            idx_scan: None,
            selection: None,
            aggregation: None,
            top_n: None,
            limit: None,
            // Go carries each executor's ExplainID into the tree form; TiFlash
            // rejects duplicate empty ids ("executor id `` duplicate").
            executor_id: Some("TableFullScan_2".to_owned()),
            parent_idx: None,
            exchange_sender: None,
        };
        let sender_executor = Executor {
            tp: Some(ExecType::TypeExchangeSender as i32),
            tbl_scan: None,
            idx_scan: None,
            selection: None,
            aggregation: None,
            top_n: None,
            limit: None,
            executor_id: Some("ExchangeSender_1".to_owned()),
            parent_idx: None,
            exchange_sender: Some(Box::new(PbExchangeSender {
                tp: Some(ExchangeType::PassThrough as i32),
                // Go fills the sender's target-task meta so TiFlash
                // registers the coordinator tunnel and marks the task root:
                // for a root fragment the target is the TiDB-side receiver
                // (task_id -1), exactly Go's EstablishMPPConns receiver meta.
                encoded_task_meta: vec![receiver_meta.encode_to_vec()],
                child: Some(Box::new(scan_executor)),
                all_field_types: columns
                    .iter()
                    .map(column_to_pb)
                    .map(|info| tidb_proto::tipb::FieldType {
                        tp: info.tp,
                        flag: info.flag.map(|value| value as u32),
                        flen: info.column_len,
                        decimal: info.decimal,
                        collate: info.collation,
                        charset: Some(String::new()),
                        elems: Vec::new(),
                        array: Some(false),
                    })
                    .collect(),
            })),
        };
        let endian = if tidb_distsql::system_endian() == tidb_distsql::SystemEndian::Little {
            Endian::LittleEndian
        } else {
            Endian::BigEndian
        };
        let dag = DagRequest {
            executors: Vec::new(),
            time_zone_offset: Some(context.time_zone_offset),
            flags: Some(context.push_down_flags),
            output_offsets: (0..columns.len() as u32).collect(),
            encode_type: Some(EncodeType::TypeChunk as i32),
            time_zone_name: Some(context.time_zone_name.clone()),
            collect_execution_summaries: Some(false),
            chunk_memory_layout: Some(tidb_proto::tipb::ChunkMemoryLayout {
                endian: Some(endian as i32),
            }),
            div_precision_increment: (context.div_precision_increment
                != DEFAULT_DIV_PRECISION_INCREMENT)
                .then_some(context.div_precision_increment),
            root_executor: Some(sender_executor),
        };
        eprintln!(
            "{{\"event\":\"tiflash_mpp_dispatch\",\"address\":\"{address}\",\"schema_ver\":{},\"start_ts\":{},\"regions\":{},\"table_id\":{},\"ranges\":{:?},\"region_ids\":{:?},\"region_spans\":{:?}}}",
            (self.schema_version)(),
            request.snapshot_ts,
            regions.len(),
            request.table_id,
            request
                .ranges
                .iter()
                .map(|(low, high)| (
                    low.as_slice()
                        .iter()
                        .map(|b| format!("{b:02X}"))
                        .collect::<String>(),
                    high.as_slice()
                        .iter()
                        .map(|b| format!("{b:02X}"))
                        .collect::<String>()
                ))
                .collect::<Vec<_>>(),
            regions.iter().map(|region| region.id).collect::<Vec<_>>(),
            regions
                .iter()
                .map(|region| (
                    region
                        .start_key
                        .iter()
                        .map(|b| format!("{b:02X}"))
                        .collect::<String>(),
                    region
                        .end_key
                        .iter()
                        .map(|b| format!("{b:02X}"))
                        .collect::<String>()
                ))
                .collect::<Vec<_>>()
        );
        let encoded_plan = dag.encode_to_vec();

        // Go `mppTaskGenerator.AllocMPPQueryID`: one query timestamp in
        // seconds plus a session-local query id; the server id names the
        // coordinator. Task ids count from one per query.
        // Region infos are clipped to the table's record range: PD answers
        // with whole regions whose bounds may straddle the prefix.
        let region_infos = regions
            .iter()
            .map(|region| {
                let range_start = if region.start_key.as_slice() > start_key.as_slice() {
                    region.start_key.clone()
                } else {
                    start_key.clone()
                };
                let range_end = if region.end_key.is_empty()
                    || region.end_key.as_slice() > end_key.as_slice()
                {
                    end_key.clone()
                } else {
                    region.end_key.clone()
                };
                tidb_proto::coprocessor::RegionInfo {
                    region_id: region.id,
                    region_epoch: Some(tidb_proto::coprocessor::RegionEpoch {
                        conf_ver: region.epoch.conf_ver,
                        version: region.epoch.version,
                    }),
                    ranges: vec![tidb_proto::coprocessor::KeyRange {
                        start: range_start,
                        end: range_end,
                    }],
                }
            })
            .collect();
        let dispatch_request = DispatchTaskRequest {
            meta: Some(meta.clone()),
            encoded_plan,
            timeout: 10,
            regions: region_infos,
            schema_ver: (self.schema_version)(),
        };

        // The dispatch and the result stream run on this module's runtime.
        // The stream drains fully before rows are served: the root task's
        // whole answer for the minimal read fits one exchange, and the
        // consumer-facing iteration stays incremental through the decoded
        // chunk queue.
        let packets = self
            .runtime
            .block_on(async {
                let endpoint = format!("http://{address}");
                let mut client = TikvClient::connect(endpoint)
                    .await
                    .map_err(|error| format!("tiflash mpp: dial {address}: {error}"))?;
                let response = client
                    .dispatch_mpp_task(dispatch_request)
                    .await
                    .map_err(|error| format!("tiflash mpp: dispatch: {error}"))?;
                let dispatch_response = response.into_inner();
                if let Some(error) = &dispatch_response.error {
                    return Err(format!(
                        "tiflash mpp: dispatch refused: {} ({})",
                        error.msg, error.code
                    ));
                }
                // Go `MPPClient.DispatchMPPTask` (mpp.go:174-189): retry regions
                // only invalidate the coordinator's region cache; they are NOT a
                // dispatch failure. The task itself has already registered and
                // will serve its regions through the learner read.
                if !dispatch_response.retry_regions.is_empty() {
                    eprintln!(
                        "{{\"event\":\"tiflash_mpp_stale_regions\",\"count\":{}}}",
                        dispatch_response.retry_regions.len()
                    );
                }
                let connection = client
                    .establish_mpp_connection(EstablishMppConnectionRequest {
                        sender_meta: Some(meta.clone()),
                        receiver_meta: Some(receiver_meta.clone()),
                    })
                    .await
                    .map_err(|error| format!("tiflash mpp: connect stream: {error}"))?;
                let mut stream = connection.into_inner();
                let mut packets: VecDeque<prost::bytes::Bytes> = VecDeque::new();
                while let Some(packet) = stream
                    .message()
                    .await
                    .map_err(|error| format!("tiflash mpp: stream: {error}"))?
                {
                    let MppDataPacket { data, error, .. } = packet;
                    if let Some(error) = error {
                        return Err(format!(
                            "tiflash mpp: task error: {} ({})",
                            error.msg, error.code
                        ));
                    }
                    packets.push_back(data.into());
                }
                Ok(packets)
            })
            .map_err(refuse)?;

        let field_types: Vec<FieldType> = request
            .columns
            .iter()
            .map(|column| column.field_type.clone())
            .collect();
        let time_zone = request.statement.time_zone.clone();
        let warnings = request.statement.warnings.clone();
        let response = MppQueryResponse {
            packets,
            closed: false,
        };
        let iter = SelectResponseIter::from_query_response(
            Box::new(response),
            field_types,
            Vec::new(),
            time_zone,
            warnings,
            mpp_result_metadata(request.columns.len(), Vec::new(), request.statement.plan_id),
            None,
        );
        Ok(Box::new(MppRowStream {
            iter: Some(iter),
            pending: None,
            pending_row: 0,
            field_types: request
                .columns
                .iter()
                .map(|column| column.field_type.clone())
                .collect(),
            returned: 0,
            exhausted: false,
        }))
    }
}

/// One packet's result subset, feeding the shared response decoder.
struct MppQueryResponse {
    packets: VecDeque<prost::bytes::Bytes>,
    closed: bool,
}

impl tidb_distsql::QueryResponse for MppQueryResponse {
    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        if self.closed {
            return Ok(None);
        }
        match self.packets.pop_front() {
            Some(data) => Ok(Some(QueryResultSubset {
                data,
                runtime: None,
            })),
            None => {
                self.closed = true;
                Ok(None)
            }
        }
    }

    fn close(&mut self) {
        self.closed = true;
        self.packets.clear();
    }
}

/// The caller's end of one MPP-served scan, mirroring `CopRowStream`'s
/// decoded-chunk queue.
struct MppRowStream {
    iter: Option<SelectResponseIter>,
    pending: Option<Chunk>,
    pending_row: usize,
    field_types: Vec<FieldType>,
    returned: u64,
    exhausted: bool,
}

impl MppRowStream {
    fn pull_chunk(&mut self, required_rows: usize) -> Result<Option<Chunk>, StorageError> {
        if self.exhausted {
            return Ok(None);
        }
        let Some(iter) = self.iter.as_mut() else {
            return Ok(None);
        };
        loop {
            let batch = iter
                .next_chunk_with_required_rows(required_rows.max(1))
                .map_err(|error| match error {
                    ResponseChannelError::SelectResponse { code, message } => {
                        StorageError::Sql(tidb_executor::MysqlError::new(code as u16, message))
                    }
                    other => StorageError::Backend(format!("tiflash mpp stream: {other}")),
                })?;
            let Some(batch) = batch else {
                self.exhausted = true;
                return Ok(None);
            };
            if batch.row.num_rows() == 0 {
                continue;
            }
            return Ok(Some(batch.row));
        }
    }
}

impl PushdownRowStream for MppRowStream {
    fn next_row(&mut self) -> Result<Option<Vec<Datum>>, StorageError> {
        loop {
            if let Some(batch) = &self.pending {
                if self.pending_row < batch.num_rows() {
                    let row = batch
                        .get_row(self.pending_row)
                        .try_get_datum_row(&self.field_types)
                        .map_err(|error| StorageError::Backend(error.to_string()))?;
                    self.pending_row += 1;
                    if self.pending_row == batch.num_rows() {
                        self.pending = None;
                        self.pending_row = 0;
                    }
                    self.returned += 1;
                    return Ok(Some(row));
                }
                self.pending = None;
                self.pending_row = 0;
            }
            match self.pull_chunk(1)? {
                Some(batch) => self.pending = Some(batch),
                None => return Ok(None),
            }
        }
    }

    fn close(&mut self) {
        if let Some(iter) = self.iter.as_mut() {
            iter.close();
        }
        self.iter = None;
        self.pending = None;
        self.exhausted = true;
    }

    fn rows_returned(&self) -> u64 {
        self.returned
    }
}
