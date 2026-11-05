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

//! Go `pkg/util/topsql/reporter/reporter.go`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crossbeam_channel::{bounded, select, Receiver, Sender, TrySendError};
use tidb_pd_client::rm_client::RuVersion;
use tidb_proto::tipb::TopRuRecord;
use tidb_topsql_collector::{ProcessCpuTimeUpdater, SqlCpuCollector, SqlCpuTimeRecord};
use tidb_topsql_reporter_metrics::{
    IGNORE_COLLECT_CHANNEL_FULL_COUNTER, IGNORE_COLLECT_RU_CHANNEL_FULL_COUNTER,
    IGNORE_COLLECT_STMT_CHANNEL_FULL_COUNTER, IGNORE_REPORT_CHANNEL_FULL_COUNTER,
};
use tidb_topsql_state::GLOBAL_STATE;
use tidb_topsql_stmtstats::{RuIncrementMap, StatementStatsMap};

use crate::datamodel::{
    cpu_records_top_n, records_to_proto, Collecting, CpuRecords, NormalizedPlanMap, NormalizedSqlMap,
};
use crate::datasink::{DataSink, DataSinkRegisterer, DefaultDataSinkRegisterer, ReportData};
use crate::report_ticker::new_report_ticker;
use crate::ru_window_aggregator::{RuBatch, RuWindowAggregator};

/// Go `reportTimeout`.
const REPORT_TIMEOUT: Duration = Duration::from_secs(40);
/// Go `collectChanBufferSize`.
const COLLECT_CHAN_BUFFER_SIZE: usize = 2;

/// Go `nowFunc`.
fn now_unix() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

/// Go `planBinaryDecodeFunc`, owned for the reporter's lifetime.
pub type PlanBinaryDecoder = Box<dyn Fn(&str) -> Result<String, String> + Send + Sync>;
/// Go `planBinaryCompressFunc`, owned for the reporter's lifetime.
pub type PlanBinaryCompressor = Box<dyn Fn(&[u8]) -> String + Send + Sync>;

/// Go's `context.WithCancel(context.Background())` for the reporter and its
/// sinks: threads select on [`Context::done`], tasks await
/// [`Context::cancelled`].
pub struct Context {
    done_tx: Mutex<Option<Sender<()>>>,
    done_rx: Receiver<()>,
    cancelled: tokio::sync::watch::Sender<bool>,
}

impl Context {
    /// Go `context.WithCancel`.
    #[must_use]
    pub fn new() -> Arc<Self> {
        let (done_tx, done_rx) = bounded(0);
        Arc::new(Self {
            done_tx: Mutex::new(Some(done_tx)),
            done_rx,
            cancelled: tokio::sync::watch::Sender::new(false),
        })
    }

    /// Go `cancel()`.
    pub fn cancel(&self) {
        self.done_tx.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take();
        self.cancelled.send_replace(true);
    }

    /// Go `ctx.Done()` for a thread's `select!`: disconnected once cancelled.
    #[must_use]
    pub fn done(&self) -> &Receiver<()> {
        &self.done_rx
    }

    /// Go's `select { case <-ctx.Done(): ... default: }`.
    #[must_use]
    pub fn is_done(&self) -> bool {
        *self.cancelled.borrow()
    }

    /// Go `<-ctx.Done()` for a task.
    pub async fn cancelled(&self) {
        let mut receiver = self.cancelled.subscribe();
        let _ = receiver.wait_for(|cancelled| *cancelled).await;
    }

    /// Go `ctx.Err()` once cancelled.
    #[must_use]
    pub fn err() -> String {
        "context canceled".to_owned()
    }
}

/// Go `TopSQLReporter` collects Top SQL metrics.
pub trait TopSqlReporter:
    tidb_topsql_collector::Collector + tidb_topsql_stmtstats::Collector + Send + Sync
{
    /// Go `Start` uses to start the reporter.
    fn start(self: Arc<Self>);

    /// Go `RegisterSQL` registers a normalizedSQL with SQLDigest.
    ///
    /// Note that the normalized SQL string can be of >1M long. This function
    /// should be thread-safe, which means concurrently calling it in several
    /// goroutines should be fine. It should also return immediately, and do
    /// any CPU-intensive job asynchronously.
    fn register_sql(&self, sql_digest: &[u8], normalized_sql: &str, is_internal: bool);

    /// Go `RegisterPlan` like RegisterSQL, but for normalized plan strings.
    /// `is_large` indicates the size of normalizedPlan is big.
    fn register_plan(&self, plan_digest: &[u8], normalized_plan: &str, is_large: bool);

    /// Go `BindProcessCPUTimeUpdater` is used to pass ProcessCPUTimeUpdater.
    fn bind_process_cpu_time_updater(&self, updater: Arc<dyn ProcessCpuTimeUpdater>);

    /// Go `BindKeyspaceName` binds the keyspace name to the reporter.
    fn bind_keyspace_name(&self, keyspace_name: Vec<u8>);

    /// Go `Close` uses to close and release the reporter resource.
    fn close(&self);

    /// Go's `globalTopProfilingReport.(reporter.DataSinkRegisterer)`.
    fn data_sink_registerer(self: Arc<Self>) -> Option<Arc<dyn DataSinkRegisterer>> {
        None
    }

    /// Go's `globalTopProfilingReport.(stmtstats.RUCollector)`.
    fn ru_collector(self: Arc<Self>) -> Option<Arc<dyn tidb_topsql_stmtstats::RuCollector>> {
        None
    }
}

/// The channel senders the reporter's producers push into, shared with the
/// SQL CPU collector it owns (Go `NewSQLCPUCollector(tsr)`).
struct CollectChans {
    collect_cpu_time_chan: Sender<CpuRecords>,
}

impl tidb_topsql_collector::Collector for CollectChans {
    /// Go `RemoteTopSQLReporter.Collect`.
    fn collect(&self, data: Vec<SqlCpuTimeRecord>) {
        if data.is_empty() {
            return;
        }
        if let Err(TrySendError::Full(_)) = self.collect_cpu_time_chan.try_send(data) {
            // ignore if chan blocked
            IGNORE_COLLECT_CHANNEL_FULL_COUNTER.inc();
        }
    }
}

/// Go `collectedData` is used for transmission in the channel.
struct CollectedData {
    collected: Collecting,
    normalized_sql_map: NormalizedSqlMap,
    normalized_plan_map: NormalizedPlanMap,
    ru_records: Option<Vec<TopRuRecord>>,
}

/// Go `RemoteTopSQLReporter` implements TopSQLReporter that sends data to a
/// remote agent. This should be called periodically to collect TopSQL
/// resource usage metrics.
pub struct RemoteTopSqlReporter {
    ctx: Arc<Context>,
    report_collected_data_chan: (Sender<CollectedData>, Receiver<CollectedData>),
    sql_cpu_collector: Mutex<SqlCpuCollector>,
    chans: Arc<CollectChans>,
    collect_cpu_time_chan: Receiver<CpuRecords>,
    collect_stmt_stats_chan: (Sender<StatementStatsMap>, Receiver<StatementStatsMap>),
    collect_ru_increments_chan: (Sender<RuBatch>, Receiver<RuBatch>),
    collecting: Mutex<Collecting>,
    /// Go `ruAggregator`: online 15s RU aggregation (400->200->100 pipeline).
    ru_aggregator: RuWindowAggregator,
    normalized_sql_map: NormalizedSqlMap,
    normalized_plan_map: NormalizedPlanMap,
    /// Go `stmtStatsBuffer`: timestamp => stmtstats.StatementStatsMap.
    stmt_stats_buffer: Mutex<HashMap<u64, StatementStatsMap>>,
    /// Go `decodePlan`: calling decodePlan this can take a while, so should
    /// not block critical paths.
    decode_plan: PlanBinaryDecoder,
    /// Go `compressPlan`: instead of dropping large plans, we compress it
    /// into encoded format and report.
    compress_plan: PlanBinaryCompressor,
    /// Go's embedded `DefaultDataSinkRegisterer`.
    pub(crate) registerer: DefaultDataSinkRegisterer,
    keyspace_name: RwLock<Vec<u8>>,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

impl RemoteTopSqlReporter {
    /// Go `NewRemoteTopSQLReporter` creates a new RemoteTopSQLReporter.
    ///
    /// `decode_plan` is a decoding function which will be called
    /// asynchronously to decode the plan binary to string.
    #[must_use]
    pub fn new(decode_plan: PlanBinaryDecoder, compress_plan: PlanBinaryCompressor) -> Arc<Self> {
        let ctx = Context::new();
        let (cpu_tx, cpu_rx) = bounded(COLLECT_CHAN_BUFFER_SIZE);
        let chans = Arc::new(CollectChans { collect_cpu_time_chan: cpu_tx });
        Arc::new(Self {
            registerer: DefaultDataSinkRegisterer::new(Arc::clone(&ctx)),
            ctx,
            sql_cpu_collector: Mutex::new(SqlCpuCollector::new(Arc::clone(&chans) as _)),
            chans,
            collect_cpu_time_chan: cpu_rx,
            collect_stmt_stats_chan: bounded(COLLECT_CHAN_BUFFER_SIZE),
            collect_ru_increments_chan: bounded(COLLECT_CHAN_BUFFER_SIZE),
            report_collected_data_chan: bounded(1),
            collecting: Mutex::new(Collecting::new()),
            ru_aggregator: RuWindowAggregator::new(),
            normalized_sql_map: NormalizedSqlMap::new(),
            normalized_plan_map: NormalizedPlanMap::new(),
            stmt_stats_buffer: Mutex::default(),
            decode_plan,
            compress_plan,
            keyspace_name: RwLock::default(),
            workers: Mutex::default(),
        })
    }

    fn keyspace_name(&self) -> Vec<u8> {
        self.keyspace_name.read().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }

    /// Go `collectWorker` consumes and collects data from
    /// tracecpu.Collector/stmtstats.Collector.
    fn collect_worker(&self) {
        let report_ticker = new_report_ticker();
        loop {
            select! {
                recv(self.ctx.done()) -> _ => return,
                recv(self.collect_cpu_time_chan) -> data => {
                    let Ok(data) = data else { return };
                    self.process_cpu_time_data(now_unix(), data);
                }
                recv(self.collect_stmt_stats_chan.1) -> data => {
                    let Ok(data) = data else { return };
                    self.lock_stmt_stats_buffer().insert(now_unix(), data);
                }
                recv(self.collect_ru_increments_chan.1) -> batch => {
                    let Ok(batch) = batch else { return };
                    self.ru_aggregator.add_batch(&batch);
                }
                recv(report_ticker) -> _ => {
                    let timestamp = now_unix();
                    self.process_stmt_stats_data();
                    self.take_data_and_send_to_report_chan(timestamp);
                }
            }
        }
    }

    fn lock_collecting(&self) -> std::sync::MutexGuard<'_, Collecting> {
        self.collecting.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lock_stmt_stats_buffer(&self) -> std::sync::MutexGuard<'_, HashMap<u64, StatementStatsMap>> {
        self.stmt_stats_buffer.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Go `processCPUTimeData` collects top N cpuRecords of each round into
    /// tsr.collecting, and evict the data that is not in top N. All the
    /// evicted cpuRecords will be summary into the others.
    pub fn process_cpu_time_data(&self, timestamp: u64, data: CpuRecords) {
        // Get top N cpuRecords of each round cpuRecords. Collect the top N to
        // tsr.collecting for each round. SQL meta will not be evicted, since
        // the evicted SQL can be appeared on other components (TiKV) TopN
        // DataRecords.
        let n = GLOBAL_STATE.max_statement_count.load(std::sync::atomic::Ordering::SeqCst);
        let (top, evicted) = cpu_records_top_n(data, n.max(0) as usize);
        let mut collecting = self.lock_collecting();
        for r in &top {
            collecting
                .get_or_create_record(&r.sql_digest, &r.plan_digest)
                .append_cpu_time(timestamp, r.cpu_time_ms);
        }
        if evicted.is_empty() {
            return;
        }
        let mut total_evicted_cpu_time = 0u32;
        for e in &evicted {
            total_evicted_cpu_time = total_evicted_cpu_time.wrapping_add(e.cpu_time_ms);
            // Mark which digests are evicted under each timestamp. We will
            // determine whether the corresponding CPUTime has been evicted
            // when collecting stmtstats. If so, then we can ignore it directly.
            collecting.mark_as_evicted(timestamp, &e.sql_digest, &e.plan_digest);
        }
        collecting.append_others_cpu_time(timestamp, total_evicted_cpu_time);
    }

    /// Go `processStmtStatsData` collects tsr.stmtStatsBuffer into
    /// tsr.collecting. All the evicted items will be summary into the others.
    pub fn process_stmt_stats_data(&self) {
        let buffer = std::mem::take(&mut *self.lock_stmt_stats_buffer());
        let k = GLOBAL_STATE.max_statement_count.load(std::sync::atomic::Ordering::SeqCst).max(0) as usize;
        let mut collecting = self.lock_collecting();
        for (timestamp, data) in buffer {
            let kth_network_bytes = find_kth_network_bytes(&data, k);
            for (digest, item) in data.0 {
                let (sql_digest, plan_digest) = (&digest.sql_digest.0, &digest.plan_digest.0);
                // Note, by filtering with the kthNetworkBytes, we get fewer
                // than N records(N - 1 records, at most time). The actual
                // picked records count is decided by the count of duplicated
                // kthNetworkBytes, if kthNetworkBytes is unique, For
                // performance reason, do not convert the whole map into a
                // slice and pick exactly topN records.
                if item.network_in_bytes + item.network_out_bytes > kth_network_bytes
                    || !collecting.has_evicted(timestamp, sql_digest, plan_digest)
                {
                    collecting
                        .get_or_create_record(sql_digest, plan_digest)
                        .append_stmt_stats_item(timestamp, item);
                } else {
                    collecting.append_others_stmt_stats_item(timestamp, item);
                }
            }
        }
    }

    /// Go `takeDataAndSendToReportChan` takes records data and then send to
    /// the report channel for reporting. TopRU extraction runs on the same
    /// report tick path. Each call emits at most one aligned closed 60s RU
    /// window.
    fn take_data_and_send_to_report_chan(&self, timestamp: u64) {
        let ru_records = self.ru_aggregator.take_report_records(
            timestamp,
            tidb_topsql_state::get_top_ru_item_interval() as u64,
            &self.keyspace_name(),
        );
        let data = CollectedData {
            collected: self.lock_collecting().take(),
            ru_records,
            normalized_sql_map: self.normalized_sql_map.take(),
            normalized_plan_map: self.normalized_plan_map.take(),
        };
        // Send to report channel. When channel is full, data will be dropped.
        if let Err(TrySendError::Full(_)) = self.report_collected_data_chan.0.try_send(data) {
            // ignore if chan blocked
            IGNORE_REPORT_CHANNEL_FULL_COUNTER.inc();
        }
    }

    /// Go `reportWorker` sends data to the gRPC endpoint from the
    /// `reportCollectedDataChan` one by one.
    fn report_worker(&self) {
        loop {
            select! {
                recv(self.report_collected_data_chan.1) -> data => {
                    let Ok(mut data) = data else { return };
                    // When `reportCollectedDataChan` receives something, there
                    // could be ongoing `RegisterSQL` and `RegisterPlan`
                    // running, who writes to the data structure that `data`
                    // contains. So we wait for a little while to ensure that
                    // writes are finished.
                    std::thread::sleep(Duration::from_millis(100));
                    let rs = data.collected.get_report_records();
                    let keyspace_name = self.keyspace_name();
                    // Convert to protobuf data and do report.
                    self.do_report(ReportData {
                        data_records: records_to_proto(&rs, &keyspace_name),
                        ru_records: data.ru_records.unwrap_or_default(),
                        sql_metas: data.normalized_sql_map.to_proto(&keyspace_name),
                        plan_metas: data.normalized_plan_map.to_proto(
                            &keyspace_name,
                            &*self.decode_plan,
                            &*self.compress_plan,
                        ),
                    });
                }
                recv(self.ctx.done()) -> _ => return,
            }
        }
    }

    /// Go `doReport` sends ReportData to DataSinks.
    fn do_report(&self, data: ReportData) {
        if !data.has_data() {
            return;
        }
        let _ = self.try_send(Arc::new(data), Instant::now() + REPORT_TIMEOUT);
    }

    /// Go `trySend` sends ReportData to all internal registered DataSinks.
    fn try_send(&self, data: Arc<ReportData>, deadline: Instant) -> Result<(), String> {
        let data_sinks = self.registerer.lock().data_sinks.clone();
        for ds in data_sinks {
            if let Err(error) = ds.try_send(Arc::clone(&data), deadline) {
                tidb_logutil::bg_logger().warn(
                    "failed to send data to datasink",
                    &[category(), tidb_logutil::zap_error(error)],
                );
            }
        }
        Ok(())
    }

    /// Go `onReporterClosing` calls the OnReporterClosing method of all
    /// internally registered DataSinks.
    fn on_reporter_closing(&self) {
        let m = std::mem::take(&mut self.registerer.lock().data_sinks);
        for d in m {
            d.on_reporter_closing();
        }
    }
}

/// Go `zap.String("category", "top-sql")`.
pub(crate) fn category() -> tidb_log::Field {
    tidb_log::Field::new("category", tidb_log::Value::Str("top-sql".to_owned()))
}

/// Go `findKthNetworkBytes` finds the k-th largest network bytes in data
/// using quickselect algorithm.
#[must_use]
pub fn find_kth_network_bytes(data: &StatementStatsMap, k: usize) -> u64 {
    if data.0.len() <= k {
        return 0;
    }
    let mut u64_slice: Vec<u64> =
        data.0.values().map(|item| item.network_in_bytes + item.network_out_bytes).collect();
    // Go's QuickSelect(k) with a greater-than Less leaves the k largest first.
    u64_slice.select_nth_unstable_by(k, |a, b| b.cmp(a));
    u64_slice[..k].iter().copied().min().unwrap_or(u64_slice[0])
}

impl tidb_topsql_collector::Collector for RemoteTopSqlReporter {
    /// Go `Collect` implements tracecpu.Collector.
    ///
    /// WARN: It will drop the DataRecords if the processing is not in time.
    /// This function is thread-safe and efficient.
    fn collect(&self, data: Vec<SqlCpuTimeRecord>) {
        self.chans.collect(data);
    }
}

impl tidb_topsql_stmtstats::Collector for RemoteTopSqlReporter {
    /// Go `CollectStmtStatsMap` implements stmtstats.Collector.
    ///
    /// WARN: It will drop the DataRecords if the processing is not in time.
    /// This function is thread-safe and efficient.
    fn collect_stmt_stats_map(&self, data: &StatementStatsMap) {
        if data.0.is_empty() {
            return;
        }
        if let Err(TrySendError::Full(_)) = self.collect_stmt_stats_chan.0.try_send(data.clone()) {
            // ignore if chan blocked
            IGNORE_COLLECT_STMT_CHANNEL_FULL_COUNTER.inc();
        }
    }
}

impl tidb_topsql_stmtstats::RuCollector for RemoteTopSqlReporter {
    /// Go `CollectRUIncrements` implements stmtstats.RUCollector. It
    /// receives merged RU increments from aggregator every second.
    /// Best-effort window contract: RU is timestamped when collected here.
    /// For boundary cases, RU can be attributed to the next report window
    /// instead of backfilling an already closed window.
    ///
    /// WARN: It will drop the data if the processing is not in time.
    /// This function is thread-safe and efficient.
    fn collect_ru_increments(&self, data: &RuIncrementMap, version: RuVersion) {
        if data.0.is_empty() {
            return;
        }
        let batch = RuBatch { timestamp: now_unix(), data: data.clone(), version };
        if let Err(TrySendError::Full(_)) = self.collect_ru_increments_chan.0.try_send(batch) {
            IGNORE_COLLECT_RU_CHANNEL_FULL_COUNTER.inc();
        }
    }

    /// Go `OnRUVersionChange` clears reporter-side RU state for an
    /// aggregator-detected handover.
    fn on_ru_version_change(&self, version: RuVersion) {
        self.ru_aggregator.reset_for_handover(version, now_unix());
    }
}

impl DataSinkRegisterer for RemoteTopSqlReporter {
    fn register(&self, data_sink: Arc<dyn DataSink>) -> Result<(), String> {
        self.registerer.register(data_sink)
    }

    fn deregister(&self, data_sink: &Arc<dyn DataSink>) {
        self.registerer.deregister(data_sink);
    }
}

impl TopSqlReporter for RemoteTopSqlReporter {
    fn start(self: Arc<Self>) {
        self.sql_cpu_collector.lock().unwrap_or_else(std::sync::PoisonError::into_inner).start();
        let mut workers = self.workers.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let tsr = Arc::clone(&self);
        workers.extend(
            std::thread::Builder::new()
                .name("topsql-collect-worker".to_owned())
                .spawn(move || tsr.collect_worker())
                .ok(),
        );
        let tsr = Arc::clone(&self);
        workers.extend(
            std::thread::Builder::new()
                .name("topsql-report-worker".to_owned())
                .spawn(move || tsr.report_worker())
                .ok(),
        );
    }

    /// This function is thread-safe and efficient.
    fn register_sql(&self, sql_digest: &[u8], normalized_sql: &str, is_internal: bool) {
        self.normalized_sql_map.register(sql_digest, normalized_sql, is_internal);
    }

    /// This function is thread-safe and efficient.
    fn register_plan(&self, plan_digest: &[u8], normalized_plan: &str, is_large: bool) {
        self.normalized_plan_map.register(plan_digest, normalized_plan, is_large);
    }

    fn bind_process_cpu_time_updater(&self, updater: Arc<dyn ProcessCpuTimeUpdater>) {
        self.sql_cpu_collector
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .set_process_cpu_updater(updater);
    }

    fn bind_keyspace_name(&self, keyspace_name: Vec<u8>) {
        *self.keyspace_name.write().unwrap_or_else(std::sync::PoisonError::into_inner) = keyspace_name;
    }

    fn close(&self) {
        self.ctx.cancel();
        self.sql_cpu_collector.lock().unwrap_or_else(std::sync::PoisonError::into_inner).stop();
        self.on_reporter_closing();
        let workers = std::mem::take(&mut *self.workers.lock().unwrap_or_else(std::sync::PoisonError::into_inner));
        for worker in workers {
            let _ = worker.join();
        }
    }

    fn data_sink_registerer(self: Arc<Self>) -> Option<Arc<dyn DataSinkRegisterer>> {
        Some(self)
    }

    fn ru_collector(self: Arc<Self>) -> Option<Arc<dyn tidb_topsql_stmtstats::RuCollector>> {
        Some(self)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use tidb_topsql_stmtstats::{SqlPlanDigest, StatementStatsItem};

    const MAX_SQL_NUM: usize = 5000;
    const KEYSPACE_NAME: &[u8] = b"123";

    /// Go `mockDataSink`: hands every report to a channel.
    pub(crate) struct MockDataSink(pub(crate) Sender<Arc<ReportData>>);

    impl DataSink for MockDataSink {
        fn try_send(&self, data: Arc<ReportData>, _deadline: Instant) -> Result<(), String> {
            self.0.send(data).map_err(|error| error.to_string())
        }

        fn on_reporter_closing(&self) {}
    }

    pub(crate) fn new_reporter() -> Arc<RemoteTopSqlReporter> {
        RemoteTopSqlReporter::new(Box::new(|plan| Ok(plan.to_owned())), Box::new(|plan| {
            String::from_utf8_lossy(plan).into_owned()
        }))
    }

    fn record(sql: &str, plan: &str, cpu_time_ms: u32) -> SqlCpuTimeRecord {
        SqlCpuTimeRecord { sql_digest: sql.into(), plan_digest: plan.into(), cpu_time_ms }
    }

    /// Go `setupRemoteTopSQLReporter` + `populateCache`.
    fn populate_cache(begin: usize, end: usize, timestamp: u64) -> Arc<ReportData> {
        GLOBAL_STATE.max_statement_count.store(MAX_SQL_NUM as i64, std::sync::atomic::Ordering::SeqCst);
        GLOBAL_STATE.max_collect.store(10000, std::sync::atomic::Ordering::SeqCst);
        let tsr = new_reporter();
        let (tx, rx) = bounded(1);
        tsr.register(Arc::new(MockDataSink(tx))).unwrap();
        for i in begin..end {
            tsr.register_sql(format!("sqlDigest{}", i + 1).as_bytes(), &format!("sqlNormalized{}", i + 1), false);
            tsr.register_plan(format!("planDigest{}", i + 1).as_bytes(), &format!("planNormalized{}", i + 1), false);
        }
        let records = (begin..end)
            .map(|i| record(&format!("sqlDigest{}", i + 1), &format!("planDigest{}", i + 1), (i + 1) as u32))
            .collect();
        tsr.process_cpu_time_data(timestamp, records);
        // Go `reportCache`.
        tsr.do_report(ReportData {
            data_records: records_to_proto(&tsr.lock_collecting().take().get_report_records(), KEYSPACE_NAME),
            sql_metas: tsr.normalized_sql_map.take().to_proto(KEYSPACE_NAME),
            plan_metas: tsr.normalized_plan_map.take().to_proto(KEYSPACE_NAME, &*tsr.decode_plan, &*tsr.compress_plan),
            ..ReportData::default()
        });
        rx.try_recv().unwrap()
    }

    fn id_of(sql_digest: &[u8]) -> usize {
        std::str::from_utf8(sql_digest).unwrap().strip_prefix("sqlDigest").map_or(0, |n| n.parse().unwrap())
    }

    fn check_metas(data: &ReportData, sql_digest: &[u8], plan_digest: &[u8], id: usize) {
        let sql_meta = data.sql_metas.iter().find(|m| m.sql_digest == sql_digest).unwrap();
        assert_eq!(sql_meta.keyspace_name, KEYSPACE_NAME);
        assert_eq!(sql_meta.normalized_sql, format!("sqlNormalized{id}"));
        let plan_meta = data.plan_metas.iter().find(|m| m.plan_digest == plan_digest).unwrap();
        assert_eq!(plan_meta.normalized_plan, format!("planNormalized{id}"));
    }

    #[test]
    fn collect_and_send_batch() {
        let data = populate_cache(0, MAX_SQL_NUM, 1);
        assert_eq!(data.data_records.len(), MAX_SQL_NUM);
        for req in &data.data_records {
            let id = id_of(&req.sql_digest);
            assert_eq!(req.items.len(), 1);
            assert_eq!(req.items[0].timestamp_sec, 1);
            assert_eq!(req.items[0].cpu_time_ms, id as u32);
            check_metas(&data, &req.sql_digest, &req.plan_digest, id);
        }
    }

    #[test]
    fn collect_and_evicted() {
        let data = populate_cache(0, MAX_SQL_NUM * 2, 2);
        assert_eq!(data.data_records.len(), MAX_SQL_NUM + 1);
        for req in &data.data_records {
            let id = id_of(&req.sql_digest);
            assert_eq!(req.items.len(), 1);
            assert_eq!(req.items[0].timestamp_sec, 2);
            if id == 0 {
                // test for others
                assert!(req.sql_digest.is_empty() && req.plan_digest.is_empty());
                // 1 + 2 + 3 + ... + 5000 = (1 + 5000) * 2500 = 12502500
                assert_eq!(req.items[0].cpu_time_ms, 12_502_500);
                continue;
            }
            assert!(id > MAX_SQL_NUM);
            assert_eq!(req.items[0].cpu_time_ms, id as u32);
            check_metas(&data, &req.sql_digest, &req.plan_digest, id);
        }
    }

    #[test]
    fn do_report_sends_meta_when_ru_records_empty() {
        let tsr = new_reporter();
        let (tx, rx) = bounded(1);
        tsr.register(Arc::new(MockDataSink(tx))).unwrap();
        tsr.do_report(ReportData {
            sql_metas: vec![tidb_proto::tipb::SqlMeta {
                sql_digest: b"S_meta_only".to_vec(),
                normalized_sql: "select /* meta only */ 1".to_owned(),
                ..Default::default()
            }],
            ..ReportData::default()
        });
        let payload = rx.recv_timeout(Duration::from_secs(1)).expect("meta-only payload should still be reported");
        assert!(payload.ru_records.is_empty() && payload.data_records.is_empty());
        assert_eq!(payload.sql_metas[0].sql_digest, b"S_meta_only");
        // Nothing to report sends nothing.
        tsr.do_report(ReportData::default());
        assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());
        tsr.close();
    }

    fn stmt_item(exec_count: u64, network_in_bytes: u64, network_out_bytes: u64) -> StatementStatsItem {
        StatementStatsItem { exec_count, network_in_bytes, network_out_bytes, ..StatementStatsItem::default() }
    }

    fn digest(sql: &str, plan: &str) -> SqlPlanDigest {
        tidb_topsql_stmtstats::new_sql_plan_digest(sql.as_bytes(), plan.as_bytes())
    }

    /// Go `TestReporterWorker` and `TestProcessStmtStatsData`: the started
    /// workers collect CPU time and statement stats and report on the tick;
    /// a stats item beyond top N whose CPU time was evicted goes to others.
    #[test]
    fn reporter_worker_reports_cpu_time_and_stmt_stats() {
        let restore = crate::report_ticker::set_report_ticker_interval_seconds_for_test(1);
        GLOBAL_STATE.max_statement_count.store(3, std::sync::atomic::Ordering::SeqCst);
        let r = new_reporter();
        Arc::clone(&r).start();
        let (tx, rx) = bounded(1);
        r.register(Arc::new(MockDataSink(tx))).unwrap();

        tidb_topsql_collector::Collector::collect(&*r, Vec::new());
        tidb_topsql_collector::Collector::collect(&*r, vec![record("S1", "P1", 1), record("S2", "P2", 2)]);
        let mut stats = StatementStatsMap::default();
        stats.0.insert(digest("S1", "P1"), stmt_item(1, 1, 0));
        stats.0.insert(digest("S2", "P2"), stmt_item(2, 0, 2));
        stats.0.insert(digest("S3", "P3"), stmt_item(3, 1, 2));
        tidb_topsql_stmtstats::Collector::collect_stmt_stats_map(&*r, &StatementStatsMap::default());
        tidb_topsql_stmtstats::Collector::collect_stmt_stats_map(&*r, &stats);

        let data = rx.recv_timeout(Duration::from_secs(5)).expect("no data in ch");
        r.close();
        restore();
        let mut records = data.data_records.clone();
        assert_eq!(records.len(), 3);
        records.sort_by_key(|r| r.items[0].stmt_network_in_bytes + r.items[0].stmt_network_out_bytes);
        for (i, record) in records.iter().enumerate() {
            let j = i + 1;
            assert_eq!(record.sql_digest, format!("S{j}").as_bytes());
            assert_eq!(record.plan_digest, format!("P{j}").as_bytes());
            assert_eq!(record.items[0].stmt_exec_count, j as u64);
            assert_eq!(record.items[0].stmt_network_in_bytes + record.items[0].stmt_network_out_bytes, j as u64);
        }
        assert_eq!(records[0].items[0].cpu_time_ms, 1);
        assert_eq!(records[1].items[0].cpu_time_ms, 2);
    }

    /// Go `findKthNetworkBytes`: the k-th largest when there are more than k
    /// items, else 0.
    #[test]
    fn find_kth_network_bytes_is_the_kth_largest() {
        let mut data = StatementStatsMap::default();
        for (i, bytes) in [5u64, 1, 9, 7, 3].into_iter().enumerate() {
            data.0.insert(digest(&format!("S{i}"), ""), stmt_item(1, bytes, 0));
        }
        assert_eq!(find_kth_network_bytes(&data, 2), 7);
        assert_eq!(find_kth_network_bytes(&data, 1), 9);
        assert_eq!(find_kth_network_bytes(&data, 4), 3);
        assert_eq!(find_kth_network_bytes(&data, 5), 0);
    }

    /// Go `Close` cancels the registerer: later registrations fail and the
    /// sinks hear the reporter closing.
    #[test]
    fn close_notifies_sinks_and_refuses_registration() {
        struct ClosingSink(Sender<()>);
        impl DataSink for ClosingSink {
            fn try_send(&self, _data: Arc<ReportData>, _deadline: Instant) -> Result<(), String> {
                Ok(())
            }
            fn on_reporter_closing(&self) {
                let _ = self.0.send(());
            }
        }
        let r = new_reporter();
        Arc::clone(&r).start();
        let (tx, rx) = bounded(1);
        r.register(Arc::new(ClosingSink(tx.clone()))).unwrap();
        r.close();
        assert!(rx.try_recv().is_ok());
        assert_eq!(r.register(Arc::new(ClosingSink(tx))).unwrap_err(), "DefaultDataSinkRegisterer closed");
    }
}
