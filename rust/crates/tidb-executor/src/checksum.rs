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

//! Go `pkg/executor/checksum.go`: `ADMIN CHECKSUM TABLE`.
//!
//! Each table is one [`ChecksumContext`]. Its tasks are one coprocessor
//! `ReqTypeChecksum` request for every physical table -- the table's own ID
//! first, then every partition -- and one for every public index on each,
//! all read at one snapshot. `tidb_checksum_table_concurrency` workers send
//! them; every region's `tipb.ChecksumResponse` folds into its table's
//! answer by `updateChecksumResponse`.
//!
//! The requests go through the node's coprocessor client
//! ([`PushdownScanner::checksum`]). The in-process engine has none: it
//! stands in for the oracle's embedded unistore, whose
//! `handleCopChecksumRequest` answers every region request with the fixed
//! `ChecksumResponse{Checksum: 1, TotalKvs: 1, TotalBytes: 1}`, and an
//! in-process table is a single region.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use prost::Message;
use tidb_datatype::Datum;
use tidb_distsql::{DatumRange, RequestBuilder, TransportRequest};
use tidb_proto::tipb;

use crate::remote_scan::{PushdownScanner, PushdownScannerError};
use crate::{DriverError, ExecError, StmtContext};

/// Go `checksumContext`: one table of the statement, with the metadata its
/// requests are built from (Go reads them off `model.TableInfo`).
#[derive(Clone, Debug)]
pub struct ChecksumContext {
    /// Go `dbInfo.Name.O`.
    pub db_name: String,
    /// Go `tableInfo.Name.O`.
    pub table_name: String,
    /// Go `tableInfo.ID`.
    pub table_id: i64,
    /// Go `tableInfo.Partition.Definitions[*].ID`, in definition order.
    pub partition_ids: Vec<i64>,
    /// Go `tableInfo.Indices[*].ID` of the `StatePublic` indexes, in order.
    pub public_index_ids: Vec<i64>,
    /// Go `tableInfo.IsCommonHandle`.
    pub is_common_handle: bool,
    /// The folded answer, Go `checksumContext.response`.
    pub response: tipb::ChecksumResponse,
}

/// Go `checksumTask`.
#[derive(Debug)]
struct ChecksumTask {
    /// Position of the owning table in [`ChecksumTableExec::tables`], the
    /// stand-in for Go's `tableID` map key.
    table: usize,
    request: TransportRequest,
}

/// The session state Go's request builders read for every request.
#[derive(Clone, Debug)]
struct RequestSettings {
    start_ts: u64,
    dist_sql_scan_concurrency: u64,
    resource_group_name: String,
    explicit_request_source_type: String,
    statement: crate::StatementMemory,
    kv_exec_counter: tidb_util::topsql_stmtstats::KvExecCounterHandle,
    /// The statement ctx's values the requests carry: its `RUDetails`, which
    /// the sends charge, and its `StmtCtx.GetResourceGroupTagger`.
    kv_context: tidb_txnkv::StatementKvContext,
}

/// Go `ChecksumTableExec`.
pub struct ChecksumTableExec {
    tables: Vec<ChecksumContext>,
    client: Option<Arc<dyn PushdownScanner>>,
    settings: RequestSettings,
}

impl ChecksumContext {
    /// Go `newChecksumContext`, from the table's metadata.
    #[must_use]
    pub fn new(db_name: &str, table: &crate::kv_table::KvTable) -> Self {
        Self {
            db_name: db_name.to_owned(),
            table_name: table.name().to_owned(),
            table_id: table.table_id,
            partition_ids: table.partition().map_or_else(Vec::new, |partition| {
                partition
                    .definitions
                    .iter()
                    .map(|definition| definition.id)
                    .collect()
            }),
            // `KvTable::indexes` holds exactly the public ones; indexes a DDL
            // is still adding or dropping live apart.
            public_index_ids: table.indexes().iter().map(|index| index.id).collect(),
            is_common_handle: !table.common_handle_offsets().is_empty(),
            response: tipb::ChecksumResponse::default(),
        }
    }

    /// Go `checksumContext.buildTasks`.
    fn build_tasks(
        &self,
        table: usize,
        settings: &RequestSettings,
    ) -> Result<Vec<ChecksumTask>, DriverError> {
        let mut tasks =
            Vec::with_capacity((self.public_index_ids.len() + 1) * (self.partition_ids.len() + 1));
        self.append_request_4_physical_table(table, self.table_id, settings, &mut tasks)?;
        for partition_id in &self.partition_ids {
            self.append_request_4_physical_table(table, *partition_id, settings, &mut tasks)?;
        }
        Ok(tasks)
    }

    /// Go `checksumContext.appendRequest4PhysicalTable`.
    fn append_request_4_physical_table(
        &self,
        table: usize,
        physical_table_id: i64,
        settings: &RequestSettings,
        tasks: &mut Vec<ChecksumTask>,
    ) -> Result<(), DriverError> {
        tasks.push(ChecksumTask {
            table,
            request: self.build_table_request(physical_table_id, settings)?,
        });
        for index_id in &self.public_index_ids {
            tasks.push(ChecksumTask {
                table,
                request: build_index_request(physical_table_id, *index_id, settings)?,
            });
        }
        Ok(())
    }

    /// Go `checksumContext.buildTableRequest`: the whole handle range,
    /// `ranger.FullNotNullRange()` for a common handle and
    /// `ranger.FullIntRange(false)` otherwise.
    fn build_table_request(
        &self,
        physical_table_id: i64,
        settings: &RequestSettings,
    ) -> Result<TransportRequest, DriverError> {
        let checksum = tipb::ChecksumRequest {
            scan_on: Some(tipb::ChecksumScanOn::Table as i32),
            algorithm: Some(tipb::ChecksumAlgorithm::Crc64Xor as i32),
            ..tipb::ChecksumRequest::default()
        };
        let ranges = if self.is_common_handle {
            DatumRange::inclusive(vec![Datum::MinNotNull], vec![Datum::MaxValue])
        } else {
            DatumRange::inclusive(vec![Datum::Int(i64::MIN)], vec![Datum::Int(i64::MAX)])
        };
        let mut builder = RequestBuilder::new();
        builder.set_handle_ranges(
            physical_table_id,
            self.is_common_handle,
            std::slice::from_ref(&ranges),
        );
        finish_request(builder, &checksum, settings)
    }

    /// Go `checksumContext.handleResponse`.
    fn handle_response(&mut self, update: &tipb::ChecksumResponse) {
        update_checksum_response(&mut self.response, update);
    }
}

/// Go `checksumContext.buildIndexRequest`: the whole index, `ranger.FullRange()`
/// (NULL entries included).
fn build_index_request(
    physical_table_id: i64,
    index_id: i64,
    settings: &RequestSettings,
) -> Result<TransportRequest, DriverError> {
    let checksum = tipb::ChecksumRequest {
        scan_on: Some(tipb::ChecksumScanOn::Index as i32),
        algorithm: Some(tipb::ChecksumAlgorithm::Crc64Xor as i32),
        ..tipb::ChecksumRequest::default()
    };
    let range = DatumRange::inclusive(vec![Datum::Null], vec![Datum::MaxValue]);
    let mut builder = RequestBuilder::new();
    builder.set_index_ranges(physical_table_id, index_id, std::slice::from_ref(&range));
    finish_request(builder, &checksum, settings)
}

/// The builder chain both of Go's request builders end with:
/// `SetChecksumRequest`, `SetStartTS`, `SetConcurrency`,
/// `SetResourceGroupName`, `SetResourceGroupTagger`,
/// `SetExplicitRequestSourceType`, `Build`, sent under the statement ctx.
///
/// Go's builder starts from a zero `kv.Request`, whose paging is off; Go
/// `BuildCopIterator` turns it off for every non-DAG request anyway.
fn finish_request(
    mut builder: RequestBuilder,
    checksum: &tipb::ChecksumRequest,
    settings: &RequestSettings,
) -> Result<TransportRequest, DriverError> {
    builder
        .set_checksum_request(checksum.encode_to_vec())
        .set_paging(false)
        .set_start_ts(settings.start_ts)
        .set_concurrency(settings.dist_sql_scan_concurrency)
        .set_resource_group_name(settings.resource_group_name.clone())
        .set_explicit_request_source_type(settings.explicit_request_source_type.clone());
    if let Some(tagger) = &settings.kv_context.resource_group_tagger {
        builder.set_resource_group_tagger(tagger());
    }
    let request = builder
        .build_transport_request(settings.statement.coprocessor_request_cancellation())
        .map_err(|error| {
            DriverError::Exec(ExecError::Internal(
                format!("checksum request: {error:?}").into(),
            ))
        })?;
    Ok(request
        .with_kv_exec_counter(settings.kv_exec_counter.clone())
        .with_ru_details(settings.kv_context.ru_details.clone()))
}

/// Go `updateChecksumResponse`.
pub fn update_checksum_response(
    response: &mut tipb::ChecksumResponse,
    update: &tipb::ChecksumResponse,
) {
    response.checksum = Some(response.checksum() ^ update.checksum());
    response.total_kvs = Some(response.total_kvs().wrapping_add(update.total_kvs()));
    response.total_bytes = Some(response.total_bytes().wrapping_add(update.total_bytes()));
}

impl ChecksumTableExec {
    /// Go `executorBuilder.buildChecksumTable`: one context per distinct
    /// table ID (a table named twice is checksummed once), every request
    /// reading at `start_ts`, the statement's snapshot (`getSnapshotTS`).
    ///
    /// `client` is the node's coprocessor client; `None` is the in-process
    /// engine (module doc).
    #[must_use]
    pub fn new(
        tables: Vec<ChecksumContext>,
        client: Option<Arc<dyn PushdownScanner>>,
        start_ts: u64,
        explicit_request_source_type: String,
        ctx: &StmtContext,
    ) -> Self {
        let mut distinct: Vec<ChecksumContext> = Vec::with_capacity(tables.len());
        for table in tables {
            match distinct
                .iter_mut()
                .find(|seen| seen.table_id == table.table_id)
            {
                Some(seen) => *seen = table,
                None => distinct.push(table),
            }
        }
        Self {
            tables: distinct,
            client,
            settings: RequestSettings {
                start_ts,
                dist_sql_scan_concurrency: ctx.dist_sql_scan_concurrency(),
                resource_group_name: ctx.resource_group_name().to_owned(),
                explicit_request_source_type,
                statement: ctx.statement_memory(),
                kv_exec_counter: ctx.kv_exec_counter(),
                kv_context: ctx
                    .statement_kv_context()
                    .map(|context| context.get())
                    .unwrap_or_default(),
            },
        }
    }

    /// Go `ChecksumTableExec.Open` then `Next`: runs every task on
    /// `concurrency` workers (`tidb_checksum_table_concurrency`) and returns
    /// the folded tables. A failed task does not stop the others; as in Go,
    /// the last failure collected is the statement's error.
    pub fn run(mut self, concurrency: usize) -> Result<Vec<ChecksumContext>, DriverError> {
        let tasks = self.build_tasks()?;
        let task_count = tasks.len();
        let queue = Mutex::new(VecDeque::from(tasks));
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let exec = &self;
        std::thread::scope(|scope| {
            // The sysvar's floor is 1, so `max(1)` never changes a reachable
            // value; it only keeps a zero from leaving the tasks unserved.
            for _ in 0..concurrency.max(1) {
                let result_tx = result_tx.clone();
                let queue = &queue;
                scope.spawn(move || exec.checksum_worker(queue, &result_tx));
            }
        });
        drop(result_tx);
        let mut error = None;
        for (table, result) in result_rx.iter().take(task_count) {
            match result {
                Ok(response) => self.tables[table].handle_response(&response),
                Err(err) => error = Some(err),
            }
        }
        match error {
            Some(error) => Err(error),
            None => Ok(self.tables),
        }
    }

    /// Go `ChecksumTableExec.buildTasks`.
    fn build_tasks(&self) -> Result<Vec<ChecksumTask>, DriverError> {
        let mut tasks = Vec::new();
        for (position, table) in self.tables.iter().enumerate() {
            tasks.extend(table.build_tasks(position, &self.settings)?);
        }
        Ok(tasks)
    }

    /// Go `ChecksumTableExec.checksumWorker`.
    fn checksum_worker(
        &self,
        queue: &Mutex<VecDeque<ChecksumTask>>,
        results: &std::sync::mpsc::Sender<(usize, Result<tipb::ChecksumResponse, DriverError>)>,
    ) {
        loop {
            let task = queue
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop_front();
            let Some(task) = task else {
                return;
            };
            let result = self.handle_checksum_request(&task.request);
            if results.send((task.table, result)).is_err() {
                return;
            }
        }
    }

    /// Go `ChecksumTableExec.handleChecksumRequest`.
    fn handle_checksum_request(
        &self,
        request: &TransportRequest,
    ) -> Result<tipb::ChecksumResponse, DriverError> {
        self.handle_signal()?;
        let mut response = tipb::ChecksumResponse::default();
        let Some(client) = &self.client else {
            // The in-process engine: unistore's `handleCopChecksumRequest`
            // answer for the one region (module doc).
            update_checksum_response(
                &mut response,
                &tipb::ChecksumResponse {
                    checksum: Some(1),
                    total_kvs: Some(1),
                    total_bytes: Some(1),
                },
            );
            return Ok(response);
        };
        let mut result = client.checksum(request).map_err(|error| match error {
            PushdownScannerError::Backend(error) => DriverError::from(error),
            PushdownScannerError::Unsupported(reason) => DriverError::unsupported(reason),
        })?;
        while let Some(data) = result.next_raw().map_err(DriverError::from)? {
            let checksum = tipb::ChecksumResponse::decode(data.as_slice()).map_err(|error| {
                DriverError::Exec(ExecError::Internal(error.to_string().into()))
            })?;
            update_checksum_response(&mut response, &checksum);
            self.handle_signal()?;
        }
        Ok(response)
    }

    /// Go `SQLKiller.HandleSignal`.
    fn handle_signal(&self) -> Result<(), DriverError> {
        match self.settings.statement.sql_killer().handle_signal() {
            Some(error) => Err(DriverError::from(ExecError::Killed(error.to_sql_error()))),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::remote_scan::{PushdownRowStream, PushdownScanRequest, RawResponseStream};
    use crate::storage::StorageError;

    fn settings() -> RequestSettings {
        RequestSettings {
            start_ts: 42,
            dist_sql_scan_concurrency: 15,
            resource_group_name: "default".to_owned(),
            explicit_request_source_type: "lightning".to_owned(),
            statement: crate::StatementMemory::default(),
            kv_exec_counter: Default::default(),
            kv_context: Default::default(),
        }
    }

    fn context(partition_ids: Vec<i64>, public_index_ids: Vec<i64>) -> ChecksumContext {
        ChecksumContext {
            db_name: "acc".to_owned(),
            table_name: "t".to_owned(),
            table_id: 100,
            partition_ids,
            public_index_ids,
            is_common_handle: false,
            response: tipb::ChecksumResponse::default(),
        }
    }

    fn key_ranges(request: &TransportRequest) -> Vec<(Vec<u8>, Vec<u8>)> {
        request
            .metadata()
            .key_ranges
            .as_ref()
            .unwrap()
            .partitions()
            .iter()
            .flatten()
            .map(|range| (range.start_key.to_vec(), range.end_key.to_vec()))
            .collect()
    }

    /// Go `checksumContext.buildTasks`: the logical table ID first, then
    /// every partition, each followed by one request per public index.
    #[test]
    fn tasks_cover_the_table_id_every_partition_and_every_public_index() {
        let table = context(vec![101, 102], vec![1, 2]);
        let tasks = table.build_tasks(0, &settings()).unwrap();
        let shapes: Vec<_> = tasks
            .iter()
            .map(|task| {
                let request =
                    tipb::ChecksumRequest::decode(task.request.metadata().data.as_deref().unwrap())
                        .unwrap();
                let (start, _) = key_ranges(&task.request).remove(0);
                (tidb_codec::decode_table_id(&start), request.scan_on())
            })
            .collect();
        use tipb::ChecksumScanOn::{Index, Table};
        assert_eq!(
            shapes,
            vec![
                (100, Table),
                (100, Index),
                (100, Index),
                (101, Table),
                (101, Index),
                (101, Index),
                (102, Table),
                (102, Index),
                (102, Index),
            ]
        );
    }

    /// Go `buildTableRequest`/`buildIndexRequest`: `SetChecksumRequest`
    /// (`ReqTypeChecksum`, `NotFillCache`, Crc64_Xor), the snapshot, the
    /// scan concurrency, the resource group and the request source, over
    /// the full handle and index ranges.
    #[test]
    fn requests_carry_gos_checksum_shape_and_full_ranges() {
        let table = context(Vec::new(), vec![3]);
        let tasks = table.build_tasks(0, &settings()).unwrap();
        let [record, index] = tasks.as_slice() else {
            panic!("{tasks:?}");
        };
        for task in [record, index] {
            let metadata = task.request.metadata();
            assert_eq!(metadata.request_type, tidb_distsql::RequestType::Checksum);
            assert!(metadata.not_fill_cache);
            assert!(!metadata.paging.enabled);
            assert!(!metadata.keep_order);
            assert_eq!(metadata.start_ts, 42);
            assert_eq!(metadata.concurrency, 15);
            assert_eq!(metadata.resource_group_name, "default");
            assert_eq!(metadata.request_source.explicit_source_type, "lightning");
        }
        let decode = |task: &ChecksumTask| {
            tipb::ChecksumRequest::decode(task.request.metadata().data.as_deref().unwrap()).unwrap()
        };
        assert_eq!(decode(record).scan_on(), tipb::ChecksumScanOn::Table);
        assert_eq!(
            decode(record).algorithm(),
            tipb::ChecksumAlgorithm::Crc64Xor
        );
        assert_eq!(decode(index).scan_on(), tipb::ChecksumScanOn::Index);

        // `FullIntRange(false)`: [MinInt64, MaxInt64] through `encodeHandleKey`.
        assert_eq!(
            key_ranges(&record.request),
            tidb_distsql::table_ranges_to_kv_ranges(
                100,
                &[DatumRange::inclusive(
                    vec![Datum::Int(i64::MIN)],
                    vec![Datum::Int(i64::MAX)]
                )],
            )
            .unwrap()
            .into_iter()
            .map(|range| (range.start_key.to_vec(), range.end_key.to_vec()))
            .collect::<Vec<_>>()
        );
        // `FullRange()`: from the NULL datum, so NULL index entries count.
        let mut null_start = tidb_codec::table_key::encode_index_seek_key(100, 3, &[]);
        null_start.push(0);
        assert_eq!(key_ranges(&index.request)[0].0, null_start);
    }

    /// Go `buildTableRequest` on a clustered table: `FullNotNullRange()`
    /// through `CommonHandleRangesToKVRanges`.
    #[test]
    fn a_common_handle_table_reads_its_full_not_null_range() {
        let mut table = context(Vec::new(), Vec::new());
        table.is_common_handle = true;
        let tasks = table.build_tasks(0, &settings()).unwrap();
        let low = tidb_codec::encode_key(&[Datum::MinNotNull]).unwrap();
        let high = tidb_txnkv::Key::from_bytes(tidb_codec::encode_key(&[Datum::MaxValue]).unwrap())
            .prefix_next()
            .into_bytes();
        assert_eq!(
            key_ranges(&tasks[0].request),
            vec![(
                tidb_codec::encode_row_key(100, &low),
                tidb_codec::encode_row_key(100, &high)
            )]
        );
    }

    /// Go `updateChecksumResponse`: XOR the checksums, add the counts.
    #[test]
    fn responses_fold_by_xor_and_sum() {
        let mut response = tipb::ChecksumResponse::default();
        for (checksum, kvs, bytes) in [(0b1100, 2, 30), (0b1010, 3, 40), (0b0001, 1, 5)] {
            update_checksum_response(
                &mut response,
                &tipb::ChecksumResponse {
                    checksum: Some(checksum),
                    total_kvs: Some(kvs),
                    total_bytes: Some(bytes),
                },
            );
        }
        assert_eq!(
            (
                response.checksum(),
                response.total_kvs(),
                response.total_bytes()
            ),
            (0b0111, 6, 75)
        );
    }

    #[derive(Debug)]
    struct Regions(Vec<tipb::ChecksumResponse>);

    struct Responses(VecDeque<Vec<u8>>);

    impl RawResponseStream for Responses {
        fn next_raw(&mut self) -> Result<Option<Vec<u8>>, StorageError> {
            Ok(self.0.pop_front())
        }
    }

    impl PushdownScanner for Regions {
        fn open(
            &self,
            _request: &PushdownScanRequest,
        ) -> Result<Box<dyn PushdownRowStream>, PushdownScannerError> {
            Err(PushdownScannerError::Unsupported("rows".to_owned()))
        }

        fn checksum(
            &self,
            request: &TransportRequest,
        ) -> Result<Box<dyn RawResponseStream>, PushdownScannerError> {
            assert_eq!(
                request.metadata().request_type,
                tidb_distsql::RequestType::Checksum
            );
            Ok(Box::new(Responses(
                self.0.iter().map(Message::encode_to_vec).collect(),
            )))
        }
    }

    fn exec(
        client: Option<Arc<dyn PushdownScanner>>,
        tables: Vec<ChecksumContext>,
    ) -> ChecksumTableExec {
        ChecksumTableExec {
            tables,
            client,
            settings: settings(),
        }
    }

    /// Every region response of every task folds into its own table.
    #[test]
    fn every_region_of_every_task_folds_into_its_table() {
        let regions = Regions(vec![
            tipb::ChecksumResponse {
                checksum: Some(0xF0),
                total_kvs: Some(10),
                total_bytes: Some(100),
            },
            tipb::ChecksumResponse {
                checksum: Some(0x0F),
                total_kvs: Some(20),
                total_bytes: Some(200),
            },
        ]);
        let mut other = context(Vec::new(), Vec::new());
        other.table_id = 200;
        let tables = exec(
            Some(Arc::new(regions)),
            vec![context(Vec::new(), vec![1]), other],
        )
        .run(4)
        .unwrap();
        // Two tasks of two regions each XOR back to zero for the first table.
        assert_eq!(
            tables
                .iter()
                .map(|table| (
                    table.table_id,
                    table.response.checksum(),
                    table.response.total_kvs(),
                    table.response.total_bytes()
                ))
                .collect::<Vec<_>>(),
            vec![(100, 0, 60, 600), (200, 0xFF, 30, 300)]
        );
    }

    /// The in-process engine answers each request as Go unistore does for
    /// one region: `{1, 1, 1}`, so a table's answer is its task count.
    #[test]
    fn the_in_process_engine_answers_unistores_stub() {
        let tables = exec(None, vec![context(vec![101, 102, 103, 104], vec![1])])
            .run(1)
            .unwrap();
        let response = &tables[0].response;
        assert_eq!(
            (
                response.checksum(),
                response.total_kvs(),
                response.total_bytes()
            ),
            (0, 10, 10)
        );
    }

    /// Go keys its contexts by table ID: a table named twice answers once.
    #[test]
    fn a_table_named_twice_is_checksummed_once() {
        let exec = ChecksumTableExec::new(
            vec![
                context(Vec::new(), Vec::new()),
                context(Vec::new(), Vec::new()),
            ],
            None,
            1,
            String::new(),
            &StmtContext::default(),
        );
        assert_eq!(exec.run(2).unwrap().len(), 1);
    }
}
