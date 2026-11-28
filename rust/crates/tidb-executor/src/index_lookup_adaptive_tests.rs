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

//! Go master `pkg/executor/pkg_test.go`'s index-worker halves of
//! `TestAdaptiveLimitEligibility` and `TestAdaptiveLimitReservationExitPaths`
//! (0b505ecc58b): pending handles beyond a reservation, and every exit path
//! of an adaptive extraction releasing or handing over its reservation.

use std::sync::atomic::AtomicBool;
use std::sync::mpsc::sync_channel;
use std::sync::{Arc, Mutex};

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_driver_error::ExecError;
use tidb_executor_internal_exec::adaptive_limit_controller::{AdaptiveLimitConfig, AdaptiveLimitController};
use tidb_executor_internal_exec::{Executor, ExecutorMeta};
use tidb_expr::column::Column;
use tidb_expr::schema::Schema;
use tidb_planner_core_operator_physicalop::PushedDownLimit;
use tidb_txnkv::{Handle, IntHandle};

use super::{IndexWorker, LookupTask, Shared, TaskDone};

/// A client no extraction reaches.
#[derive(Debug)]
struct NoClient;

impl tidb_txnkv::Client for NoClient {
    fn send(&self, _request: &tidb_txnkv::TransportRequest) -> Box<dyn tidb_txnkv::Response + Send> {
        unreachable!("an extraction test sends no request")
    }

    fn is_request_type_supported(&self, _request_type: i64, _sub_type: i64) -> bool {
        false
    }
}

/// Go `oversizedChunkSelectResult` (and `partialThenErrorSelectResult`
/// when `error` is set): one chunk of every row regardless of the rows
/// required, then the end, or the error.
struct OversizedChunkSelectResult {
    meta: ExecutorMeta,
    rows: Vec<i64>,
    returned: bool,
    error: Option<&'static str>,
}

impl OversizedChunkSelectResult {
    fn new(rows: Vec<i64>) -> Self {
        Self { meta: int_meta(), rows, returned: false, error: None }
    }
}

impl Executor for OversizedChunkSelectResult {
    fn open(&mut self) -> Result<(), ExecError> {
        Ok(())
    }
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if self.returned {
            return match self.error {
                Some(error) => Err(ExecError::internal(error)),
                None => Ok(()),
            };
        }
        for value in &self.rows {
            req.append_datum(0, &Datum::Int(*value));
        }
        self.returned = true;
        Ok(())
    }
    fn close(&mut self) -> Result<(), ExecError> {
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

fn handle_column() -> Column {
    let mut column = Column::new(0, FieldType::new(FieldTypeCode::LongLong));
    column.id = tidb_model::column::EXTRA_HANDLE_ID;
    column
}

fn int_meta() -> ExecutorMeta {
    ExecutorMeta::new(Schema::new(vec![handle_column()]), 1, 32, 32)
}

fn new_controller(window: u64) -> Arc<AdaptiveLimitController> {
    Arc::new(AdaptiveLimitController::new(AdaptiveLimitConfig {
        demand_rows: 100,
        initial_outer_window: window,
        max_outer_window: 128,
        initial_lookup_window: window,
        max_lookup_window: if window == 2 { 2 } else { 128 },
        initial_lookup_batch_size: window,
        max_lookup_batch_size: if window == 2 { 2 } else { 128 },
        ..AdaptiveLimitConfig::default()
    }))
}

/// Go `newExtractionWorker`: an index worker over one int handle column.
fn new_extraction_worker(controller: Option<Arc<AdaptiveLimitController>>, batch_size: usize) -> IndexWorker {
    let ctx = crate::StmtContext::default();
    let statement = crate::remote_scan::pushdown_statement_context(&ctx);
    let shared = Arc::new(Shared {
        ctx,
        client: Arc::new(NoClient),
        table_plans: Vec::new(),
        table_plan_ids: Vec::new(),
        table_info: tidb_model::TableInfo::default(),
        primary_key_index: None,
        codec_table_info: tidb_tablecodec::TableInfo::default(),
        handle_cols: vec![handle_column()],
        handle_idx: Vec::new(),
        partition_handle_from_index: false,
        partition_handle_from_table: false,
        keep_order: true,
        start_ts: 0,
        columns: Vec::new(),
        physical_table_id: 0,
        avg_row_size: 0.0,
        finished: AtomicBool::new(false),
        index_scan_plan_id: 0,
        index_cop_count_and_rows: Mutex::new((0, 0)),
        mem_tracker: tidb_memory::Tracker::new(-1, -1),
        table_reader_meta: int_meta(),
        table_reader_ruv2: None,
        stats: None,
        index: tidb_model::IndexInfo::default(),
        check_index_value: None,
    });
    IndexWorker {
        shared,
        meta: int_meta(),
        statement,
        dag: tidb_proto::tipb::DagRequest::default(),
        kv_ranges: Vec::new(),
        table_ids: Vec::new(),
        index_types: vec![FieldType::new(FieldTypeCode::LongLong)],
        by_items: Vec::new(),
        need_merge: false,
        desc: false,
        index_plan_ids: Vec::new(),
        index_root_plan_id: 0,
        idx_net_data_size: 0.0,
        pushed_limit: None,
        batch_size,
        max_batch_size: batch_size,
        scanned_keys: 0,
        mem_tracker: tidb_memory::Tracker::new(-1, -1),
        next_task_id: 0,
        partition_table_mode: false,
        index_paging: false,
        index_look_up_push_down: false,
        tbl_scan_idx_for_rewrite_partition_id: None,
        ctx: tidb_gocontext::Context::background(),
        adaptive_limit_controller: controller,
        pending_handles: Vec::new(),
        pending_handles_mem_usage: 0,
    }
}

fn int_values(handles: &[Handle]) -> Vec<i64> {
    handles.iter().map(|handle| handle.int_value().expect("an int handle")).collect()
}

/// Go `TestAdaptiveLimitEligibility`'s pending-handle half: handles beyond
/// the batch stay pending and charged, later extractions take them first,
/// and a pushed limit is not reached while any are pending.
#[test]
fn test_adaptive_limit_pending_handles() {
    let controller = new_controller(32);
    let mut worker = new_extraction_worker(Some(Arc::clone(&controller)), 2);
    let mut handles = Vec::new();
    let mut pending_handles_mem_usage = 0;
    for i in 0..5 {
        pending_handles_mem_usage += worker.append_extracted_handle(&mut handles, Handle::Int(IntHandle::new(i)));
    }
    worker.consume_pending_handles_memory(pending_handles_mem_usage);
    assert_eq!(handles.len(), 2);
    assert_eq!(worker.pending_handles.len(), 3);
    assert!(worker.mem_tracker.bytes_consumed() > 0);
    let mut handles = Vec::new();
    worker.take_pending_handles(&mut handles);
    assert_eq!(handles.len(), 2);
    assert_eq!(worker.pending_handles.len(), 1);
    assert!(worker.mem_tracker.bytes_consumed() > 0);
    let mut handles = Vec::new();
    worker.take_pending_handles(&mut handles);
    assert_eq!(handles.len(), 1);
    assert!(worker.pending_handles.is_empty());
    assert_eq!(worker.mem_tracker.bytes_consumed(), 0);
    let mut handles = Vec::new();
    let mut pending_handles_mem_usage = 0;
    for i in 0..4 {
        pending_handles_mem_usage += worker.append_extracted_handle(&mut handles, Handle::Int(IntHandle::new(i)));
    }
    worker.consume_pending_handles_memory(pending_handles_mem_usage);
    assert_eq!(worker.pending_handles.len(), 2);
    assert!(worker.mem_tracker.bytes_consumed() > 0);
    worker.release_pending_handles();
    assert!(worker.pending_handles.is_empty());
    assert_eq!(worker.pending_handles_mem_usage, 0);
    assert_eq!(worker.mem_tracker.bytes_consumed(), 0);
    worker.pushed_limit = Some(PushedDownLimit { offset: 0, count: 5 });
    worker.scanned_keys = 5;
    worker.pending_handles = vec![Handle::Int(IntHandle::new(5))];
    assert!(!worker.reached_pushed_limit());
    worker.pending_handles.clear();
    assert!(worker.reached_pushed_limit());

    // An oversized DistSQL chunk is admitted two handles at a time.
    let mut worker = new_extraction_worker(Some(new_controller(2)), 2);
    worker.pushed_limit = Some(PushedDownLimit { offset: 0, count: 5 });
    let mut result = OversizedChunkSelectResult::new(vec![1, 2, 3, 4, 5]);
    let mut chk = Chunk::new(&[FieldType::new(FieldTypeCode::LongLong)], 32, 32);
    for (round, expected) in [vec![1, 2], vec![3, 4], vec![5]].into_iter().enumerate() {
        worker.batch_size = 2;
        let (handles, _, _) = worker.extract_task_handles(&mut result, &mut chk, &[0]).unwrap();
        assert_eq!(int_values(&handles), expected, "round {round}");
        assert_eq!(worker.pending_handles.len(), [3, 1, 0][round], "round {round}");
        if round < 2 {
            assert!(worker.mem_tracker.bytes_consumed() > 0, "round {round}");
        } else {
            assert_eq!(worker.mem_tracker.bytes_consumed(), 0, "round {round}");
        }
    }
    assert!(worker.reached_pushed_limit());
    assert_eq!(worker.scanned_keys, 5);

    // A canceled worker context releases nothing it never reserved.
    let canceled_controller = new_controller(2);
    let mut canceled_worker = new_extraction_worker(Some(Arc::clone(&canceled_controller)), 2);
    let (canceled_ctx, cancel) = tidb_gocontext::Context::with_cancel(&tidb_gocontext::Context::background());
    cancel.cancel();
    canceled_worker.ctx = canceled_ctx;
    let (result_tx, _result_rx) = sync_channel::<TaskDone>(4);
    let (work_tx, _work_rx) = sync_channel::<LookupTask>(4);
    let mut source = OversizedChunkSelectResult::new(vec![1]);
    let error = canceled_worker
        .fetch_handles(&mut source, None, &mut chk, &[0], &result_tx, &work_tx)
        .expect_err("a canceled reservation fails the extraction");
    assert!(format!("{error:?}").contains("context canceled"), "{error:?}");
    assert_eq!(canceled_controller.snapshot().lookup_reserved, 0);
}

/// Go `TestAdaptiveLimitReservationExitPaths`' index-worker cases.
#[test]
fn test_adaptive_limit_reservation_exit_paths() {
    let mut chk = Chunk::new(&[FieldType::new(FieldTypeCode::LongLong)], 32, 32);

    // partial extraction error
    let controller = new_controller(2);
    let mut worker = new_extraction_worker(Some(Arc::clone(&controller)), 2);
    let (result_tx, _result_rx) = sync_channel::<TaskDone>(4);
    let (work_tx, _work_rx) = sync_channel::<LookupTask>(4);
    let mut source = OversizedChunkSelectResult::new(vec![1]);
    source.error = Some("adaptive limit extraction failed");
    let error = worker
        .fetch_handles(&mut source, None, &mut chk, &[0], &result_tx, &work_tx)
        .expect_err("the extraction error surfaces");
    assert!(format!("{error:?}").contains("adaptive limit extraction failed"), "{error:?}");
    let snapshot = controller.snapshot();
    assert_eq!(snapshot.lookup_reserved, 0);
    assert_eq!(snapshot.lookup_handles, 0);
    assert_eq!(snapshot.lookup_rows, 0);

    // empty extraction EOF
    let controller = new_controller(2);
    let mut worker = new_extraction_worker(Some(Arc::clone(&controller)), 2);
    let mut source = OversizedChunkSelectResult::new(Vec::new());
    assert!(worker.fetch_handles(&mut source, None, &mut chk, &[0], &result_tx, &work_tx).unwrap());
    assert_eq!(controller.snapshot().lookup_reserved, 0);

    // partial extraction dispatch: the task owns the trimmed reservation
    let controller = new_controller(2);
    let mut worker = new_extraction_worker(Some(Arc::clone(&controller)), 2);
    let (result_tx, result_rx) = sync_channel::<TaskDone>(4);
    let (work_tx, work_rx) = sync_channel::<LookupTask>(4);
    let mut source = OversizedChunkSelectResult::new(vec![1]);
    assert!(worker.fetch_handles(&mut source, None, &mut chk, &[0], &result_tx, &work_tx).unwrap());
    let done = result_rx.try_recv().expect("one dispatched task");
    assert_eq!(done.adaptive_limit_reservation, 1);
    assert_eq!(done.handles, 1);
    assert_eq!(work_rx.try_recv().expect("one table task").handles.len(), 1);
    assert_eq!(controller.snapshot().lookup_reserved, 1);
    // Go `completeAdaptiveLookupTask` of the fully consumed task.
    controller.complete_lookup(done.adaptive_limit_reservation, done.handles, 1);
    let snapshot = controller.snapshot();
    assert_eq!(snapshot.lookup_reserved, 0);
    assert_eq!(snapshot.lookup_handles, 1);
    assert_eq!(snapshot.lookup_rows, 1);

    // finished before dispatch
    let controller = new_controller(2);
    let mut worker = new_extraction_worker(Some(Arc::clone(&controller)), 2);
    worker.shared.finished.store(true, std::sync::atomic::Ordering::Release);
    let mut source = OversizedChunkSelectResult::new(vec![1, 2]);
    assert!(!worker.fetch_handles(&mut source, None, &mut chk, &[0], &result_tx, &work_tx).unwrap());
    assert_eq!(controller.snapshot().lookup_reserved, 0);

    // a closed result channel aborts the dispatched reservation
    let controller = new_controller(2);
    let mut worker = new_extraction_worker(Some(Arc::clone(&controller)), 2);
    let (result_tx, result_rx) = sync_channel::<TaskDone>(4);
    drop(result_rx);
    let (work_tx, _work_rx) = sync_channel::<LookupTask>(4);
    let mut source = OversizedChunkSelectResult::new(vec![1, 2]);
    assert!(!worker.fetch_handles(&mut source, None, &mut chk, &[0], &result_tx, &work_tx).unwrap());
    assert_eq!(controller.snapshot().lookup_reserved, 0);
}
