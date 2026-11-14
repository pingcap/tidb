// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/dxf/framework/metering/metering.go`.
use crate::{CATEGORY, Data, MeterItem, Recorder, TaskBase};
use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tidb_gocontext::Context;

/// Go `writeTimeout`.
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
/// Go `maxRetryCount`.
const MAX_RETRY_COUNT: usize = 10;
/// Go `retryInterval`.
const RETRY_INTERVAL: Duration = Duration::from_secs(5);
/// Go `FlushInterval`, stored as nanoseconds (Go `time.Duration`).
pub static FLUSH_INTERVAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(60_000_000_000);

/// Go SDK `common.MeteringData`, passed to `MeteringWriter.Write`.
#[derive(Clone, Debug)]
pub struct MeteringData {
    /// Go `SelfID`.
    pub self_id: String,
    /// Go `Timestamp`.
    pub timestamp: i64,
    /// Go `Category`.
    pub category: String,
    /// Go `Data`.
    pub data: Vec<MeterItem>,
}
/// Go SDK `writer.MeteringWriter` operations used by this package.
/// The storage provider/SDK adapter is a separate dependency of `NewMeter`.
pub trait MeteringWriter: Send + Sync {
    /// Go `MeteringWriter.Write`.
    fn write(&self, ctx: &Context, data: &MeteringData) -> Result<(), String>;
    /// Go `MeteringWriter.Close`.
    fn close(&self) -> Result<(), String>;
}

/// Go `meteringInstance`.
static METERING_INSTANCE: LazyLock<Mutex<Option<Arc<Meter>>>> = LazyLock::new(|| Mutex::new(None));
/// Go `SetMetering`.
pub fn set_metering(meter: Option<Arc<Meter>>) {
    *METERING_INSTANCE.lock().unwrap() = meter;
}
/// Go `RegisterRecorder`.
pub fn register_recorder(task: &TaskBase) -> Arc<Recorder> {
    if tidb_config_kerneltype::is_classic() {
        return Arc::new(Recorder::default());
    }
    let meter = METERING_INSTANCE.lock().unwrap().clone();
    match meter {
        None => Arc::new(Recorder::default()),
        Some(meter) => meter.get_or_register_recorder(Arc::new(Recorder {
            task_id: task.id,
            task_type: task.tp.to_string(),
            keyspace: task.keyspace.clone(),
            ..Recorder::default()
        })),
    }
}
/// Go `UnregisterRecorder`.
pub fn unregister_recorder(task_id: i64) {
    if tidb_config_kerneltype::is_classic() {
        return;
    }
    let meter = METERING_INSTANCE.lock().unwrap().clone();
    if let Some(meter) = meter {
        meter.unregister_recorder(task_id);
    }
}
/// Go `WriteMeterData`.
pub fn write_meter_data(
    ctx: &Context,
    ts: i64,
    uuid: &str,
    items: &[MeterItem],
) -> Result<(), String> {
    if tidb_config_kerneltype::is_classic() {
        return Ok(());
    }
    let meter = METERING_INSTANCE.lock().unwrap().clone();
    match meter {
        None => Ok(()),
        Some(meter) => meter.write_meter_data(ctx, ts, uuid, items),
    }
}
struct WrappedRecorder {
    recorder: Arc<Recorder>,
    unregistered: bool,
}
struct WriteFailData {
    retry_cnt: std::sync::atomic::AtomicUsize,
    items: Vec<MeterItem>,
}
#[derive(Default)]
struct MeterState {
    recorders: BTreeMap<i64, WrappedRecorder>,
    last_flushed_data: BTreeMap<i64, Data>,
}
/// Go `Meter`: delta snapshots and immutable retry batches, keyed by timestamp.
pub struct Meter {
    state: Mutex<MeterState>,
    pending_retry_data: Mutex<BTreeMap<i64, Arc<WriteFailData>>>,
    uuid: String,
    writer: Option<Arc<dyn MeteringWriter>>,
}
impl Meter {
    /// Go `newMeterWithWriter`.
    pub fn new_meter_with_writer(writer: Option<Arc<dyn MeteringWriter>>) -> Self {
        let mut bytes = [0u8; 16];
        getrandom::fill(&mut bytes).expect("uuid.New random source");
        bytes[6] = (bytes[6] & 15) | 64;
        bytes[8] = (bytes[8] & 63) | 128;
        let uuid = bytes
            .iter()
            .enumerate()
            .map(|(i, byte)| {
                format!(
                    "{}{:02x}",
                    if [4, 6, 8, 10].contains(&i) { "_" } else { "" },
                    byte
                )
            })
            .collect();
        Self {
            state: Mutex::default(),
            pending_retry_data: Mutex::default(),
            uuid,
            writer,
        }
    }
    /// Go `Meter.getOrRegisterRecorder`.
    fn get_or_register_recorder(&self, recorder: Arc<Recorder>) -> Arc<Recorder> {
        let mut state = self.state.lock().unwrap();
        let entry = state
            .recorders
            .entry(recorder.task_id)
            .or_insert(WrappedRecorder {
                recorder,
                unregistered: false,
            });
        entry.unregistered = false;
        Arc::clone(&entry.recorder)
    }
    /// Go `Meter.unregisterRecorder`: cleanup waits for the final scrape.
    fn unregister_recorder(&self, task_id: i64) {
        if let Some(r) = self.state.lock().unwrap().recorders.get_mut(&task_id) {
            r.unregistered = true;
        }
    }
    /// Go `Meter.scrapeCurrData`.
    fn scrape_curr_data(&self) -> BTreeMap<i64, Data> {
        self.state
            .lock()
            .unwrap()
            .recorders
            .iter()
            .map(|(id, r)| (*id, r.recorder.curr_data()))
            .collect()
    }
    /// Go `Meter.calculateDataItems`.
    fn calculate_data_items(&self, current: &BTreeMap<i64, Data>) -> Vec<MeterItem> {
        let state = self.state.lock().unwrap();
        current
            .iter()
            .filter_map(|(id, data)| {
                data.cal_meter_data_item(
                    state.last_flushed_data.get(id).unwrap_or(&Data::default()),
                )
            })
            .collect()
    }
    /// Go `Meter.afterFlush` and `cleanupUnregisteredRecorders`.
    fn after_flush(&self, flushed: BTreeMap<i64, Data>) {
        let removed = {
            let mut state = self.state.lock().unwrap();
            state.last_flushed_data = flushed;
            let ids: Vec<_> = state
                .recorders
                .iter()
                .filter(|(id, r)| {
                    r.unregistered
                        && state
                            .last_flushed_data
                            .get(id)
                            .is_some_and(|last| last.equals(&r.recorder.curr_data()))
                })
                .map(|(id, _)| *id)
                .collect();
            let mut removed = Vec::new();
            for id in ids {
                state.last_flushed_data.remove(&id);
                removed.push(state.recorders.remove(&id).unwrap().recorder);
            }
            removed
        };
        for recorder in removed {
            tidb_failpoint::inject_call(
                "github.com/pingcap/tidb/pkg/dxf/framework/metering/meteringFinalFlush",
                &mut recorder.curr_data(),
            );
        }
    }
    /// Go `Meter.flush`: snapshot advances even when the write must be retried.
    fn flush(&self, ctx: &Context, ts: i64) {
        let current = self.scrape_curr_data();
        let items = self.calculate_data_items(&current);
        if !items.is_empty() && self.write_meter_data(ctx, ts, &self.uuid, &items).is_err() {
            self.pending_retry_data.lock().unwrap().insert(
                ts,
                Arc::new(WriteFailData {
                    retry_cnt: std::sync::atomic::AtomicUsize::new(0),
                    items,
                }),
            );
        }
        self.after_flush(current);
    }
    /// Go `Meter.retryWrite`: preserve the original timestamp and batch.
    fn retry_write(&self, ctx: &Context) {
        let data = self.pending_retry_data.lock().unwrap().clone();
        let mut removed = Vec::new();
        for (ts, batch) in data {
            let result = self.write_meter_data(ctx, ts, &self.uuid, &batch.items);
            if result.is_ok() {
                removed.push(ts);
            } else {
                if ctx.err().is_some() {
                    break;
                }
                if batch
                    .retry_cnt
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    + 1
                    >= MAX_RETRY_COUNT
                {
                    removed.push(ts);
                }
            }
        }
        let mut pending = self.pending_retry_data.lock().unwrap();
        for ts in removed {
            pending.remove(&ts);
        }
    }

    /// Go `Meter.WriteMeterData`: a ten-second child context and failure metric.
    pub fn write_meter_data(
        &self,
        ctx: &Context,
        mut ts: i64,
        uuid: &str,
        items: &[MeterItem],
    ) -> Result<(), String> {
        tidb_failpoint::inject_call(
            "github.com/pingcap/tidb/pkg/dxf/framework/metering/forceTSAtMinuteBoundary",
            &mut ts,
        );
        let data = MeteringData {
            self_id: uuid.into(),
            timestamp: ts,
            category: CATEGORY.into(),
            data: items.to_vec(),
        };
        let (flush_ctx, cancel) = Context::with_timeout(ctx, WRITE_TIMEOUT);
        let result = self
            .writer
            .as_ref()
            .expect("metering writer")
            .write(&flush_ctx, &data);
        cancel.cancel();
        if result.is_err() {
            tidb_dxf_dxfmetric::EXECUTE_EVENT_COUNTER
                .with_label_values(&["-", tidb_dxf_dxfmetric::EVENT_METER_WRITE_FAILED])
                .inc();
        }
        result
    }
    /// Go `Meter.StartFlushLoop`: joins both loops before returning.
    pub fn start_flush_loop(&self, ctx: &Context) {
        std::thread::scope(|scope| {
            scope.spawn(|| self.flush_loop(ctx));
            scope.spawn(|| self.retry_loop(ctx));
        });
    }
    /// Go `Meter.retryLoop`.
    fn retry_loop(&self, ctx: &Context) {
        loop {
            if ctx.done().recv_timeout(RETRY_INTERVAL)
                != Err(crossbeam_channel::RecvTimeoutError::Timeout)
            {
                return;
            }
            self.retry_write(ctx);
        }
    }
    /// Go `Meter.flushLoop`: align timestamps and perform a final flush on exit.
    fn flush_loop(&self, ctx: &Context) {
        let interval = FLUSH_INTERVAL.load(std::sync::atomic::Ordering::SeqCst) as u128;
        assert!(interval > 0, "FlushInterval must be positive");
        let now = || {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        };
        let mut current = now();
        let mut next = current / interval * interval + interval;
        while ctx.err().is_none() {
            if ctx.done().recv_timeout(Duration::from_nanos(
                next.saturating_sub(current).min(u64::MAX as u128) as u64,
            )) == Err(crossbeam_channel::RecvTimeoutError::Timeout)
            {
                self.flush(ctx, (next / 1_000_000_000) as i64);
                next += interval;
                current = now();
            }
        }
        self.flush(ctx, (next / 1_000_000_000) as i64);
        let _ = self.close();
    }
    /// Go `Meter.Close`.
    pub fn close(&self) -> Result<(), String> {
        self.writer.as_ref().map_or(Ok(()), |w| w.close())
    }
}

#[cfg(test)]
#[path = "metering_test.rs"]
mod tests;
