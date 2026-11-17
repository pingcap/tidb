// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ddl/backfilling_operators.go`: the ingest pipeline's operators
//! over Go's `pkg/dxf/operator`. A table scan task source splits the range
//! into region tasks, the table scan operator reads each task's rows, the
//! index ingest operator encodes them (`writeChunk`) into the engines'
//! writers, and the result sink imports the engines as their quota fills.
use super::*;
use super::ingest::{expected_ingest_worker_cnt, PipelineError};
use operator::{
    compose, AsyncOperator, AsyncPipeline, Context, Operator, OperatorError, SimpleDataChannel,
    TunableOperator, WithSink, WithSource,
};
use std::sync::atomic::{AtomicI64, AtomicUsize};
use std::thread::JoinHandle;
use tidb_ddl_build::table_backfill::IngestRows;
use tidb_ddl_ingest::backend::BackendCtx;
use tidb_dxf_execute::Collector;
use tidb_resourcemanager_pool_workerpool::{TaskMayPanic, Worker, WorkerPool};

/// Go `metrics.LblAddIndex`.
const LBL_ADD_INDEX: &str = "add_index";

/// Go `dbterror.ErrReorgPanic`, which a panicking task reports.
fn reorg_panic() -> OperatorError {
    OperatorError::from_error(PipelineError::Ingest(tidb_ddl_ingest::Error::Database(
        tidb_dbterror::ERR_REORG_PANIC.gen_with_stack_by_args(&[]),
    )))
}

fn operator_error(error: PipelineError) -> OperatorError {
    OperatorError::from_error(error)
}

/// What Go's operators reach through `store`, `sessPool`, `tbl` and
/// `copCtx`: the transaction opener the scan reads through, the backfiller
/// that decodes a table's rows and encodes its index entries, the
/// reorganization (the table and the indexes being added) and the physical
/// table scanned. `warnings` collects the scan's decode warnings for the
/// job.
pub struct ScanContext<S: StoreClient> {
    /// Go `store`.
    pub opener: Arc<RealOptimisticTransactionOpener<S>>,
    /// The scan transactions' RPC deadline.
    pub timeout: Duration,
    /// Go `sessPool` and the table codecs.
    pub backfiller: Arc<dyn IndexBackfiller + Send>,
    /// Go `tbl.Meta()` and the indexes being added.
    pub reorg: Arc<AddIndexReorg>,
    /// Go `tbl.GetPhysicalID()`.
    pub physical_id: i64,
    /// The scan's decode warnings, each code's first one and its count.
    pub warnings: Arc<Mutex<Vec<(u16, String, i64)>>>,
}

impl<S: StoreClient> ScanContext<S> {
    /// Go `NewReorgCopContext(reorgMeta, tblInfo, idxInfos, ...)`: the scan
    /// of `job`'s table for the indexes `index_ids`, through `backfiller`.
    #[must_use]
    pub fn new(
        opener: Arc<RealOptimisticTransactionOpener<S>>,
        timeout: Duration,
        backfiller: Arc<dyn IndexBackfiller + Send>,
        job: &tidb_model::Job,
        table: tidb_model::TableInfo,
        index_ids: Vec<i64>,
        physical_id: i64,
    ) -> Arc<Self> {
        let meta = crate::backfilling_dist_executor::BackfillTaskMeta {
            job: job.clone(),
            ele_ids: Some(index_ids),
            ..Default::default()
        };
        Arc::new(Self {
            opener,
            timeout,
            backfiller,
            reorg: Arc::new(super::dist::reorg_for_task(&meta, table)),
            physical_id,
            warnings: Arc::default(),
        })
    }
}

/// Go `TableScanTask`: the start key and the end key of a region.
#[derive(Clone, Debug)]
pub struct TableScanTask {
    /// Go `ID`.
    pub id: usize,
    /// Go `Start`.
    pub start: Vec<u8>,
    /// Go `End`.
    pub end: Vec<u8>,
}

impl TaskMayPanic<OperatorError> for TableScanTask {
    /// Go `TableScanTask.RecoverArgs`.
    fn recover_args(&self) -> (String, String, Option<OperatorError>) {
        (LBL_ADD_INDEX.to_owned(), "TableScanTask".to_owned(), Some(reorg_panic()))
    }
}

/// Go `IndexRecordChunk`: one chunk read from its `TableScanTask`.
pub struct IndexRecordChunk {
    /// Go `ID`.
    pub id: usize,
    /// Go `Chunk`.
    pub chunk: Option<IngestRows>,
    /// Go `Done`.
    pub done: bool,
    /// Go `tableScanRowCount`: the rows the task scanned for this chunk.
    pub table_scan_row_count: i64,
}

impl TaskMayPanic<OperatorError> for IndexRecordChunk {
    /// Go `IndexRecordChunk.RecoverArgs`.
    fn recover_args(&self) -> (String, String, Option<OperatorError>) {
        (LBL_ADD_INDEX.to_owned(), "IndexRecordChunk".to_owned(), Some(reorg_panic()))
    }
}

/// Go `IndexWriteResult`: the result of writing a chunk to the engines.
#[derive(Clone, Debug)]
pub struct IndexWriteResult {
    /// Go `ID`.
    pub id: usize,
    /// Go `RowCnt`.
    pub row_cnt: usize,
}

impl TaskMayPanic<OperatorError> for IndexWriteResult {
    fn recover_args(&self) -> (String, String, Option<OperatorError>) {
        (LBL_ADD_INDEX.to_owned(), "IndexWriteResult".to_owned(), Some(reorg_panic()))
    }
}

/// Go `newTaskIDAllocator`.
#[derive(Default)]
struct TaskIdAllocator {
    id: usize,
}

impl TaskIdAllocator {
    fn alloc(&mut self) -> usize {
        let id = self.id;
        self.id += 1;
        id
    }
}

/// Go `TableScanTaskSource`: produces `TableScanTask`s by splitting the
/// table's records into region ranges.
pub struct TableScanTaskSource {
    ctx: Arc<Context>,
    region_store: Option<Arc<dyn tidb_kv::region_map::RegionStore>>,
    physical_id: i64,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    cp_op: Option<Arc<dyn BackendCtx>>,
    sink: Mutex<Option<SimpleDataChannel<TableScanTask>>>,
    err_group: Mutex<Option<JoinHandle<Result<(), OperatorError>>>>,
}

impl TableScanTaskSource {
    /// Go `NewTableScanTaskSource`; `region_store` is Go `store`'s region
    /// cache.
    #[must_use]
    pub fn new(
        ctx: Arc<Context>,
        region_store: Option<Arc<dyn tidb_kv::region_map::RegionStore>>,
        physical_id: i64,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        cp_op: Option<Arc<dyn BackendCtx>>,
    ) -> Self {
        Self {
            ctx,
            region_store,
            physical_id,
            start_key,
            end_key,
            cp_op,
            sink: Mutex::new(None),
            err_group: Mutex::new(None),
        }
    }
}

/// Go `TableScanTaskSource.adjustStartKey`: skip the ranges the checkpoint
/// manager says are processed.
fn adjust_start_key(cp_op: Option<&Arc<dyn BackendCtx>>, start: &[u8], end: &[u8]) -> (Vec<u8>, bool) {
    let Some(cp_op) = cp_op else {
        return (start.to_vec(), false);
    };
    let cp_key = cp_op.next_start_key();
    if cp_key.is_empty() {
        return (start.to_vec(), false);
    }
    if cp_key.as_slice() < start || cp_key.as_slice() > end {
        tidb_logutil::bg_logger().error(
            "invalid checkpoint key",
            &[
                tidb_log::Field::new("last_process_key", tidb_log::Value::Str(hex_string(&cp_key))),
                tidb_log::Field::new("start", tidb_log::Value::Str(hex_string(start))),
                tidb_log::Field::new("end", tidb_log::Value::Str(hex_string(end))),
            ],
        );
        return (start.to_vec(), false);
    }
    let done = cp_key.as_slice() == end;
    (cp_key, done)
}

fn hex_string(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Go `TableScanTaskSource.generateTasks`.
fn generate_tasks(
    ctx: &Context,
    sink: &SimpleDataChannel<TableScanTask>,
    region_store: Option<&dyn tidb_kv::region_map::RegionStore>,
    physical_id: i64,
    start_key: &[u8],
    end_key: &[u8],
    cp_op: Option<&Arc<dyn BackendCtx>>,
) -> Result<(), OperatorError> {
    let mut task_id_alloc = TaskIdAllocator::default();
    let (mut start_key, done) = adjust_start_key(cp_op, start_key, end_key);
    if done {
        // All table data are done.
        return Ok(());
    }
    loop {
        let kv_ranges = super::super::backfilling::load_table_ranges(
            region_store,
            physical_id,
            &start_key,
            end_key,
            super::super::backfilling::BACKFILL_TASK_CHAN_SIZE,
        )
        .map_err(|error| operator_error(PipelineError::Storage(error)))?;
        let Some(last) = kv_ranges.last() else {
            break;
        };
        let next_start = last.1.clone();
        for task in get_batch_table_scan_task(physical_id, kv_ranges, &mut task_id_alloc) {
            if !sink.send_or_done(task, ctx) {
                return Err(ctx.err().unwrap_or_else(OperatorError::canceled));
            }
        }
        start_key = next_start;
        if start_key.as_slice() >= end_key {
            break;
        }
    }
    Ok(())
}

/// Go `TableScanTaskSource.getBatchTableScanTask`.
fn get_batch_table_scan_task(
    physical_id: i64,
    kv_ranges: Vec<(Vec<u8>, Vec<u8>)>,
    task_id_alloc: &mut TaskIdAllocator,
) -> Vec<TableScanTask> {
    let prefix = tidb_tablecodec::table_key::gen_table_record_prefix(physical_id);
    kv_ranges
        .into_iter()
        .map(|(start, end)| TableScanTask {
            id: task_id_alloc.alloc(),
            start: if start.is_empty() { prefix.clone() } else { start },
            end: if end.is_empty() {
                tidb_txnkv::Key::from_bytes(prefix.clone()).prefix_next().as_bytes().to_vec()
            } else {
                end
            },
        })
        .collect()
}

impl WithSink<TableScanTask> for TableScanTaskSource {
    /// Go `TableScanTaskSource.SetSink`.
    fn set_sink(&self, sink: SimpleDataChannel<TableScanTask>) {
        *self.sink.lock().unwrap() = Some(sink);
    }
}

impl Operator for TableScanTaskSource {
    /// Go `TableScanTaskSource.Open`.
    fn open(&self) -> Result<(), OperatorError> {
        let ctx = Arc::clone(&self.ctx);
        let sink = self.sink.lock().unwrap().clone().expect("TableScanTaskSource sink is required");
        let region_store = self.region_store.clone();
        let physical_id = self.physical_id;
        let (start_key, end_key) = (self.start_key.clone(), self.end_key.clone());
        let cp_op = self.cp_op.clone();
        let handle = std::thread::Builder::new()
            .name("table-scan-task-source".to_owned())
            .spawn(tidb_cpuprofile::labels::inherit(move || {
                // Go `defer src.sink.Finish()`.
                let result = generate_tasks(
                    &ctx,
                    &sink,
                    region_store.as_deref(),
                    physical_id,
                    &start_key,
                    &end_key,
                    cp_op.as_ref(),
                );
                sink.finish();
                result
            }))
            .map_err(|error| OperatorError::new(error.to_string()))?;
        *self.err_group.lock().unwrap() = Some(handle);
        Ok(())
    }

    /// Go `TableScanTaskSource.Close`: the task generation's error.
    fn close(&self) -> Result<(), OperatorError> {
        match self.err_group.lock().unwrap().take() {
            Some(handle) => handle.join().unwrap_or_else(|_| Err(reorg_panic())),
            None => Ok(()),
        }
    }

    fn operator_string(&self) -> String {
        "TableScanTaskSource".to_owned()
    }
}
/// Go `TableScanOperator`: scans the table records of each task's range.
pub struct TableScanOperator {
    inner: AsyncOperator<TableScanTask, IndexRecordChunk>,
    total_count: Arc<AtomicI64>,
}

impl TableScanOperator {
    /// Go `NewTableScanOperator`.
    #[must_use]
    pub fn new<S: StoreClient + 'static>(
        ctx: Arc<Context>,
        scan: Arc<ScanContext<S>>,
        concurrency: usize,
        hint_batch_size: usize,
        reorg_meta: Option<tidb_model::GoShared<tidb_model::DDLReorgMeta>>,
        cp_op: Option<Arc<dyn BackendCtx>>,
        collector: Arc<dyn Collector>,
    ) -> Self {
        let total_count = Arc::new(AtomicI64::new(0));
        let pool = {
            let (ctx, total_count) = (Arc::clone(&ctx), Arc::clone(&total_count));
            WorkerPool::new(
                "TableScanOperator".to_owned(),
                tidb_resourcemanager_util::Component::Ddl,
                concurrency as isize,
                move || {
                    Some(Box::new(TableScanWorker {
                        ctx: Arc::clone(&ctx),
                        scan: Arc::clone(&scan),
                        se: None,
                        cp_op: cp_op.clone(),
                        reorg_meta: reorg_meta.clone(),
                        hint_batch_size,
                        total_count: Arc::clone(&total_count),
                        collector: Arc::clone(&collector),
                    }) as Box<dyn Worker<TableScanTask, IndexRecordChunk, OperatorError>>)
                },
                Vec::new(),
            )
        };
        Self { inner: AsyncOperator::new(ctx, pool), total_count }
    }

    /// Go `TunableOperator.GetWorkerPoolSize`.
    #[must_use]
    pub fn get_worker_pool_size(&self) -> i32 {
        self.inner.worker_pool_size()
    }

    /// Go `TunableOperator.TuneWorkerPoolSize`.
    pub fn tune_worker_pool_size(&self, worker_num: i32, wait: bool) {
        self.inner.tune_worker_pool_size(worker_num, wait);
    }
}

impl WithSource<TableScanTask> for TableScanOperator {
    fn set_source(&self, channel: SimpleDataChannel<TableScanTask>) {
        self.inner.set_source(channel);
    }
}

impl WithSink<IndexRecordChunk> for TableScanOperator {
    fn set_sink(&self, channel: SimpleDataChannel<IndexRecordChunk>) {
        self.inner.set_sink(channel);
    }
}

impl Operator for TableScanOperator {
    fn open(&self) -> Result<(), OperatorError> {
        self.inner.open()
    }

    /// Go `TableScanOperator.Close`.
    fn close(&self) -> Result<(), OperatorError> {
        let result = self.inner.close();
        tidb_logutil::bg_logger().info(
            "table scan operator total count",
            &[tidb_log::Field::new("count", tidb_log::Value::I64(self.total_count.load(Ordering::SeqCst)))],
        );
        result
    }

    fn operator_string(&self) -> String {
        self.inner.operator_string()
    }

    fn tunable(&self) -> Option<&dyn TunableOperator> {
        Some(&self.inner)
    }
}

/// Go `tableScanWorker`.
struct TableScanWorker<S: StoreClient> {
    ctx: Arc<Context>,
    scan: Arc<ScanContext<S>>,
    /// Go `se`: the pooled session whose transaction the scan reads at.
    se: Option<Box<dyn tidb_ddl_build::session_pool::StepSqlSession>>,
    cp_op: Option<Arc<dyn BackendCtx>>,
    reorg_meta: Option<tidb_model::GoShared<tidb_model::DDLReorgMeta>>,
    hint_batch_size: usize,
    total_count: Arc<AtomicI64>,
    collector: Arc<dyn Collector>,
}

impl<S: StoreClient + 'static> Worker<TableScanTask, IndexRecordChunk, OperatorError> for TableScanWorker<S> {
    /// Go `tableScanWorker.HandleTask`.
    fn handle_task(
        &mut self,
        task: TableScanTask,
        sender: &mut dyn FnMut(IndexRecordChunk),
    ) -> Result<(), OperatorError> {
        tidb_failpoint::inject("github.com/pingcap/tidb/pkg/ddl/injectPanicForTableScan", |_| {
            panic!("mock panic");
        });
        crate::job_ctx::attach_top_profiling_info(
            self.scan.reorg.job_id,
            &self.scan.reorg.query.to_utf8_lossy_go(),
        );
        if self.se.is_none() {
            // Go `sessPool.Get()`: each worker scans on a pooled internal
            // session, so the session manager lists its snapshot among the
            // internal sessions' start timestamps.
            self.se = self.scan.backfiller.session_pool().and_then(|pool| pool.open_sql_session().ok());
        }
        self.scan_records(task, sender)
    }

    /// Go `tableScanWorker.Close`: the session goes back to the pool.
    fn close(&mut self) -> Result<(), OperatorError> {
        self.se = None;
        Ok(())
    }
}

impl<S: StoreClient + 'static> TableScanWorker<S> {
    /// Go `tableScanWorker.getChunk`'s capacity: the job's current batch
    /// size, which `TaskMetaModified` updates in place.
    fn batch_size(&self) -> usize {
        self.reorg_meta.as_ref().map_or(self.hint_batch_size, |meta| {
            meta.read().get_batch_size(crate::index::reorg_defaults()).max(1) as usize
        })
        .max(1)
    }

    /// Go `tableScanWorker.scanRecords` in `wrapInBeginRollback`: the task's
    /// rows at the start timestamp of a transaction the pooled session opens
    /// and rolls back. Local ingest sends the chunks only after the scan's
    /// transaction ends, since it may import while it receives them.
    fn scan_records(
        &mut self,
        task: TableScanTask,
        sender: &mut dyn FnMut(IndexRecordChunk),
    ) -> Result<(), OperatorError> {
        tidb_logutil::bg_logger().info(
            "start a table scan task",
            &[tidb_log::Field::new("id", tidb_log::Value::I64(task.id as i64))],
        );
        let mut enable_streaming = self.reorg_meta.as_ref().is_some_and(|meta| meta.read().use_cloud_storage);
        tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/checkEnableStreaming", &mut enable_streaming);
        let cp_op = self.cp_op.clone();
        let total_count = Arc::clone(&self.total_count);
        let mut send_result = |chunk: IndexRecordChunk| {
            let (id, rows, done) = (chunk.id, chunk.table_scan_row_count, chunk.done);
            sender(chunk);
            if let Some(cp_op) = &cp_op {
                cp_op.update_chunk(id, rows, done);
            }
            total_count.fetch_add(rows, Ordering::SeqCst);
        };
        let mut start_ts = match self.se.as_deref_mut() {
            Some(se) => {
                se.begin().map_err(|error| operator_error(PipelineError::Sql(LockSqlError::from(error))))?;
                match se.txn_start_ts() {
                    Ok(start_ts) => Some(start_ts),
                    Err(error) => {
                        se.rollback();
                        return Err(operator_error(PipelineError::Sql(LockSqlError::from(error))));
                    }
                }
            }
            None => None,
        };
        if let Some(start_ts) = start_ts.as_mut() {
            tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/wrapInBeginRollbackStartTS", start_ts);
        }
        let batch_size = self.batch_size();
        let mut results = Vec::new();
        let scan = Arc::clone(&self.scan);
        let ctx = Arc::clone(&self.ctx);
        let collector = Arc::clone(&self.collector);
        let reorg_meta = self.reorg_meta.clone();
        let result = reorg_transaction_at(
            &scan.opener,
            scan.timeout,
            &crate::job_ctx::backfill_request_source(scan.reorg.job_id),
            start_ts,
            |_, snapshot, buffer, _| {
                let result = (|| {
                    if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockScanRecordError").is_some() {
                        return Err(operator_error(PipelineError::Ingest(tidb_ddl_ingest::Error::Other(
                            "mock scan record error".into(),
                        ))));
                    }
                    if let Some(meta) = &reorg_meta {
                        // Go passes the worker's `*model.DDLReorgMeta`.
                        tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/scanRecordExec", &mut meta.clone());
                    }
                    if let Some(cp_op) = &cp_op {
                        cp_op.add_chunk(task.id, task.end.clone());
                    }
                    let mut start = task.start.clone();
                    loop {
                        if let Some(error) = ctx.err() {
                            return Err(error);
                        }
                        tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/beforeGetChunk", &mut ());
                        let work = AddIndexReorgWork::Ingest {
                            physical_id: scan.physical_id,
                            start: &start,
                            end: &task.end,
                            batch_size,
                        };
                        let fetched = scan.backfiller.reorg_add_index(&scan.reorg, work, snapshot.clone(), buffer);
                        if let Some(value) = tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockScanRecordPartialError") {
                            if value.bool() {
                                return Err(operator_error(PipelineError::Ingest(tidb_ddl_ingest::Error::Other(
                                    "mock partial scan error".into(),
                                ))));
                            }
                        }
                        let rows = match fetched {
                            Ok(AddIndexReorgAnswer::Ingest(rows)) => rows,
                            Err(error) => return Err(operator_error(PipelineError::Sql(error))),
                            Ok(_) => {
                                return Err(operator_error(PipelineError::Storage(
                                    ClusterDdlError::backfill_message("ingest scan answered something else"),
                                )));
                            }
                        };
                        collector.accepted(rows.batch.read_bytes as i64);
                        let done = rows.batch.done;
                        start = rows.batch.next_key.clone();
                        super::super::backfilling::merge_warnings(
                            &mut scan.warnings.lock().unwrap(),
                            rows.batch.warnings.clone(),
                        );
                        let chunk = IndexRecordChunk {
                            id: task.id,
                            done,
                            table_scan_row_count: rows.num_rows() as i64,
                            chunk: Some(rows),
                        };
                        if enable_streaming {
                            send_result(chunk);
                        } else {
                            results.push(chunk);
                        }
                        if done {
                            break;
                        }
                    }
                    Ok(())
                })();
                buffer.reset();
                Ok((result, Vec::new()))
            },
        );
        tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/wrapInBeginRollbackAfterFn", &mut ());
        if let Some(se) = self.se.as_deref_mut() {
            se.rollback();
        }
        if !enable_streaming {
            for chunk in results {
                send_result(chunk);
            }
        }
        result.map_err(|error| operator_error(PipelineError::Storage(error)))?
    }
}
/// Go `IndexIngestOperator`: writes each chunk's index records to the
/// ingest engines.
pub struct IndexIngestOperator {
    inner: AsyncOperator<IndexRecordChunk, IndexWriteResult>,
}

impl IndexIngestOperator {
    /// Go `NewIndexIngestOperator`. `engines` are in the order of the
    /// reorganization's indexes; `mem_cache_size` is the writers' memory
    /// cache (Go `getLocalWriterConfig`).
    #[must_use]
    pub fn new<S: StoreClient + 'static>(
        ctx: Arc<Context>,
        scan: Arc<ScanContext<S>>,
        engines: Vec<Arc<dyn tidb_ddl_ingest::engine::IngestEngine>>,
        concurrency: usize,
        mem_cache_size: usize,
        collector: Arc<dyn Collector>,
    ) -> Self {
        let writer_id_alloc = Arc::new(AtomicUsize::new(0));
        let pool = {
            let ctx = Arc::clone(&ctx);
            WorkerPool::new(
                "indexIngestOperator".to_owned(),
                tidb_resourcemanager_util::Component::Ddl,
                concurrency as isize,
                move || {
                    let mut writers = Vec::with_capacity(engines.len());
                    for engine in &engines {
                        let writer_id = writer_id_alloc.fetch_add(1, Ordering::SeqCst) + 1;
                        match Arc::clone(engine).create_writer(writer_id, mem_cache_size) {
                            Ok(writer) => writers.push(writer),
                            Err(error) => {
                                tidb_logutil::bg_logger().error(
                                    "create index ingest worker failed",
                                    &[tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string()))],
                                );
                                ctx.on_error(operator_error(PipelineError::Ingest(error)));
                                return None;
                            }
                        }
                    }
                    // Go `initIndexConditionCheckers` and `initSessCtx`.
                    let encoder = match scan.backfiller.ingest_chunk_encoder(&scan.reorg) {
                        Ok(encoder) => encoder,
                        Err(error) => {
                            ctx.on_error(operator_error(PipelineError::Sql(error)));
                            return None;
                        }
                    };
                    Some(Box::new(IndexIngestWorker {
                        scan: Arc::clone(&scan),
                        writers,
                        encoder,
                        collector: Arc::clone(&collector),
                    }) as Box<dyn Worker<IndexRecordChunk, IndexWriteResult, OperatorError>>)
                },
                Vec::new(),
            )
        };
        Self { inner: AsyncOperator::new(ctx, pool) }
    }

    /// Go `TunableOperator.GetWorkerPoolSize`.
    #[must_use]
    pub fn get_worker_pool_size(&self) -> i32 {
        self.inner.worker_pool_size()
    }

    /// Go `TunableOperator.TuneWorkerPoolSize`.
    pub fn tune_worker_pool_size(&self, worker_num: i32, wait: bool) {
        self.inner.tune_worker_pool_size(worker_num, wait);
    }
}

impl WithSource<IndexRecordChunk> for IndexIngestOperator {
    fn set_source(&self, channel: SimpleDataChannel<IndexRecordChunk>) {
        self.inner.set_source(channel);
    }
}

impl WithSink<IndexWriteResult> for IndexIngestOperator {
    fn set_sink(&self, channel: SimpleDataChannel<IndexWriteResult>) {
        self.inner.set_sink(channel);
    }
}

impl Operator for IndexIngestOperator {
    fn open(&self) -> Result<(), OperatorError> {
        self.inner.open()
    }

    fn close(&self) -> Result<(), OperatorError> {
        self.inner.close()
    }

    fn operator_string(&self) -> String {
        self.inner.operator_string()
    }

    fn tunable(&self) -> Option<&dyn TunableOperator> {
        Some(&self.inner)
    }
}

/// Go `indexIngestWorker`.
struct IndexIngestWorker<S: StoreClient> {
    scan: Arc<ScanContext<S>>,
    writers: Vec<Box<dyn tidb_ddl_ingest::engine::IngestWriter>>,
    encoder: Box<dyn IngestChunkEncoder>,
    collector: Arc<dyn Collector>,
}

impl<S: StoreClient + 'static> Worker<IndexRecordChunk, IndexWriteResult, OperatorError> for IndexIngestWorker<S> {
    /// Go `indexIngestWorker.HandleTask`.
    fn handle_task(
        &mut self,
        chunk: IndexRecordChunk,
        send: &mut dyn FnMut(IndexWriteResult),
    ) -> Result<(), OperatorError> {
        tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/mockIndexIngestWorkerFault", &mut ());
        crate::job_ctx::attach_top_profiling_info(
            self.scan.reorg.job_id,
            &self.scan.reorg.query.to_utf8_lossy_go(),
        );
        let bytes = self.write_chunk(&chunk)?;
        self.collector.processed(bytes, chunk.table_scan_row_count);
        send(IndexWriteResult { id: chunk.id, row_cnt: chunk.table_scan_row_count as usize });
        Ok(())
    }

    fn close(&mut self) -> Result<(), OperatorError> {
        Ok(())
    }
}

impl<S: StoreClient + 'static> IndexIngestWorker<S> {
    /// Go `indexIngestWorker.WriteChunk` and `writeChunk`: the chunk's index
    /// KVs, each to its index's writer. Answers the bytes written.
    fn write_chunk(&mut self, chunk: &IndexRecordChunk) -> Result<i64, OperatorError> {
        if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockWriteLocalError").is_some() {
            return Err(operator_error(PipelineError::Ingest(tidb_ddl_ingest::Error::Other(
                "mock write local error".into(),
            ))));
        }
        tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/writeLocalExec", &mut chunk.done.clone());
        let Some(rows) = &chunk.chunk else {
            return Ok(0);
        };
        if rows.rows.is_empty() {
            return Ok(0);
        }
        let entries = self
            .encoder
            .encode(self.scan.physical_id, rows)
            .map_err(|error| operator_error(PipelineError::Sql(error)))?;
        let mut kv_bytes = 0i64;
        for entry in entries {
            let position = self
                .scan
                .reorg
                .index_ids
                .iter()
                .position(|id| *id == entry.index_id)
                .expect("an entry of a registered index");
            if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockLocalWriterPanic").is_some() {
                panic!("mock panic");
            }
            if let Err(error) = self.writers[position].write_row(&entry.key, &entry.value, &entry.row_id) {
                let index = self
                    .scan
                    .reorg
                    .table
                    .indices
                    .iter_deref()
                    .find(|index| index.read().id == entry.index_id)
                    .expect("an entry of a registered index");
                let error =
                    tidb_ddl_ingest::util::try_convert_to_key_exists_err(error, &index.read(), &self.scan.reorg.table);
                return Err(operator_error(PipelineError::Ingest(error)));
            }
            kv_bytes += (entry.key.len() + entry.value.len()) as i64;
            if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockLocalWriterError").is_some() {
                return Err(operator_error(PipelineError::Ingest(tidb_ddl_ingest::Error::Other(
                    "mock engine error".into(),
                ))));
            }
        }
        Ok(kv_bytes)
    }
}

/// Go `indexWriteResultSink`: imports the engines once their memory quota
/// is exceeded, and flushes them all once every result arrived.
pub struct IndexWriteResultSink {
    ctx: Arc<Context>,
    backend_ctx: Option<Arc<dyn BackendCtx>>,
    source: Mutex<Option<SimpleDataChannel<IndexWriteResult>>>,
    err_group: Mutex<Option<JoinHandle<Result<(), OperatorError>>>>,
}

impl IndexWriteResultSink {
    /// Go `newIndexWriteResultSink`.
    #[must_use]
    pub fn new(ctx: Arc<Context>, backend_ctx: Option<Arc<dyn BackendCtx>>) -> Self {
        Self { ctx, backend_ctx, source: Mutex::new(None), err_group: Mutex::new(None) }
    }
}

/// Go `indexWriteResultSink.collectResult`.
fn collect_result(
    ctx: &Context,
    backend_ctx: Option<&Arc<dyn BackendCtx>>,
    source: &SimpleDataChannel<IndexWriteResult>,
) -> Result<(), OperatorError> {
    let receiver = source.receiver();
    let [own, parent] = ctx.done();
    let background = tidb_gocontext::Context::background();
    loop {
        crossbeam_channel::select! {
            recv(own) -> _ => return Err(OperatorError::canceled()),
            recv(parent) -> _ => return Err(OperatorError::canceled()),
            recv(receiver) -> result => {
                let Ok(result) = result else {
                    // The worker pool also closes the channel after the
                    // context is canceled; do not start the final flush and
                    // import then.
                    if let Some(error) = ctx.err() {
                        return Err(error);
                    }
                    let flushed = flush(&background, backend_ctx);
                    if let Err(error) = &flushed {
                        ctx.on_error(error.clone());
                    }
                    return flushed;
                };
                if let Some(backend_ctx) = backend_ctx {
                    if let Err(error) = backend_ctx.ingest_if_quota_exceeded(&background, result.id, result.row_cnt as i64) {
                        let error = operator_error(PipelineError::Ingest(error));
                        ctx.on_error(error.clone());
                        return Err(error);
                    }
                }
            }
        }
    }
}

/// Go `indexWriteResultSink.flush`.
fn flush(ctx: &tidb_gocontext::Context, backend_ctx: Option<&Arc<dyn BackendCtx>>) -> Result<(), OperatorError> {
    let Some(backend_ctx) = backend_ctx else {
        return Ok(());
    };
    if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockFlushError").is_some() {
        return Err(operator_error(PipelineError::Ingest(tidb_ddl_ingest::Error::Other("mock flush error".into()))));
    }
    backend_ctx.ingest(ctx).map_err(|error| operator_error(PipelineError::Ingest(error)))
}

impl WithSource<IndexWriteResult> for IndexWriteResultSink {
    fn set_source(&self, source: SimpleDataChannel<IndexWriteResult>) {
        *self.source.lock().unwrap() = Some(source);
    }
}

impl Operator for IndexWriteResultSink {
    /// Go `indexWriteResultSink.Open`.
    fn open(&self) -> Result<(), OperatorError> {
        let ctx = Arc::clone(&self.ctx);
        let backend_ctx = self.backend_ctx.clone();
        let source = self.source.lock().unwrap().clone().expect("indexWriteResultSink source is required");
        let handle = std::thread::Builder::new()
            .name("index-write-result-sink".to_owned())
            .spawn(tidb_cpuprofile::labels::inherit(move || collect_result(&ctx, backend_ctx.as_ref(), &source)))
            .map_err(|error| OperatorError::new(error.to_string()))?;
        *self.err_group.lock().unwrap() = Some(handle);
        Ok(())
    }

    /// Go `indexWriteResultSink.Close`.
    fn close(&self) -> Result<(), OperatorError> {
        match self.err_group.lock().unwrap().take() {
            Some(handle) => handle.join().unwrap_or_else(|_| Err(reorg_panic())),
            None => Ok(()),
        }
    }

    fn operator_string(&self) -> String {
        "indexWriteResultSink".to_owned()
    }
}

/// Go `NewAddIndexIngestPipeline`: the source, scan, ingest and sink
/// operators of a local-ingest backfill over `[start_key, end_key)`.
#[allow(clippy::too_many_arguments)]
pub fn new_add_index_ingest_pipeline<S: StoreClient + 'static>(
    ctx: &Arc<Context>,
    scan: Arc<ScanContext<S>>,
    backend_ctx: Arc<dyn BackendCtx>,
    engines: Vec<Arc<dyn tidb_ddl_ingest::engine::IngestEngine>>,
    job_id: i64,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    reorg_meta: Option<tidb_model::GoShared<tidb_model::DDLReorgMeta>>,
    avg_row_size: usize,
    concurrency: usize,
    collector: Arc<dyn Collector>,
) -> Arc<AsyncPipeline> {
    let use_cloud_storage = reorg_meta.as_ref().is_some_and(|meta| meta.read().use_cloud_storage);
    let (reader_cnt, writer_cnt) = expected_ingest_worker_cnt(concurrency, avg_row_size, use_cloud_storage);
    tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/beforeAddIndexScan", &mut ());
    let hint_batch_size = scan.reorg.batch_size;
    let mem_cache_size = backend_ctx.get_local_backend().map_or(0, |backend| backend.cfg.local_writer_mem_cache_size);
    let src_op = Arc::new(TableScanTaskSource::new(
        Arc::clone(ctx),
        scan.backfiller.region_store(),
        scan.physical_id,
        start_key,
        end_key,
        Some(Arc::clone(&backend_ctx)),
    ));
    let scan_op = Arc::new(TableScanOperator::new(
        Arc::clone(ctx),
        Arc::clone(&scan),
        reader_cnt,
        hint_batch_size,
        reorg_meta,
        Some(Arc::clone(&backend_ctx)),
        Arc::clone(&collector),
    ));
    let ingest_op = Arc::new(IndexIngestOperator::new(
        Arc::clone(ctx),
        scan,
        engines,
        writer_cnt,
        mem_cache_size,
        collector,
    ));
    let sink_op = Arc::new(IndexWriteResultSink::new(Arc::clone(ctx), Some(backend_ctx)));
    compose(src_op.as_ref(), scan_op.as_ref());
    compose(scan_op.as_ref(), ingest_op.as_ref());
    compose(ingest_op.as_ref(), sink_op.as_ref());
    tidb_logutil::bg_logger().info(
        "build add index local storage operators",
        &[
            tidb_log::Field::new("jobID", tidb_log::Value::I64(job_id)),
            tidb_log::Field::new("avgRowSize", tidb_log::Value::I64(avg_row_size as i64)),
            tidb_log::Field::new("reader", tidb_log::Value::I64(reader_cnt as i64)),
            tidb_log::Field::new("writer", tidb_log::Value::I64(writer_cnt as i64)),
        ],
    );
    Arc::new(AsyncPipeline::new(vec![src_op, scan_op, ingest_op, sink_op]))
}
