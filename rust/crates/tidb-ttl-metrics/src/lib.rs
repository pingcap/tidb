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

//! Go `pkg/ttl/metrics` (`metrics.go`): the TTL worker's phase names, its
//! labeled metric handles over `pkg/metrics`' TTL families, the per-worker
//! phase tracer and the watermark-delay gauges.
//!
//! Go keeps the tracer in a `context.Context` (`CtxWithPhaseTracer` /
//! `PhaseTracerFromCtx`) and lets every method run on a nil `*PhaseTracer`.
//! Rust callers pass the tracer explicitly as `Option<&PhaseTracer>`, and
//! [`PhaseTracerExt`] gives that `Option` Go's nil-receiver methods.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime};

use prometheus::core::{AtomicF64, GenericCounter, GenericGauge};
use prometheus::Histogram;
use tidb_metrics::server::{
    TIDB_SERVER_TTL_JOB_STATUS, TIDB_SERVER_TTL_PHASE_TIME, TIDB_SERVER_TTL_PROCESSED_EXPIRED_ROWS,
    TIDB_SERVER_TTL_TASK_STATUS, TIDB_SERVER_TTL_WATERMARK_DELAY, TTL_QUERY_DURATION,
};

/// Go `PhaseIdle`.
pub const PHASE_IDLE: &str = "idle";
/// Go `PhaseBeginTxn`.
pub const PHASE_BEGIN_TXN: &str = "begin_txn";
/// Go `PhaseCommitTxn`.
pub const PHASE_COMMIT_TXN: &str = "commit_txn";
/// Go `PhaseQuery`.
pub const PHASE_QUERY: &str = "query";
/// Go `PhaseCheckTTL`.
pub const PHASE_CHECK_TTL: &str = "check_ttl";
/// Go `PhaseWaitRetry`.
pub const PHASE_WAIT_RETRY: &str = "wait_retry";
/// Go `PhaseDispatch`.
pub const PHASE_DISPATCH: &str = "dispatch";
/// Go `PhaseWaitToken`.
pub const PHASE_WAIT_TOKEN: &str = "wait_token";
/// Go `PhaseOther`.
pub const PHASE_OTHER: &str = "other";

/// Go `metrics.LblOK`.
const LBL_OK: &str = "ok";
/// Go `metrics.LblError`.
const LBL_ERROR: &str = "error";

fn labels<'a>(pairs: &[(&'a str, &'a str)]) -> HashMap<&'a str, &'a str> {
    pairs.iter().copied().collect()
}

fn query_duration(sql_type: &str, result: &str) -> Histogram {
    TTL_QUERY_DURATION.with(&labels(&[("sql_type", sql_type), ("result", result)]))
}

fn processed_rows(sql_type: &str, result: &str) -> GenericCounter<AtomicF64> {
    TIDB_SERVER_TTL_PROCESSED_EXPIRED_ROWS.with(&labels(&[("sql_type", sql_type), ("result", result)]))
}

/// Go `SelectSuccessDuration`.
pub static SELECT_SUCCESS_DURATION: LazyLock<Histogram> =
    LazyLock::new(|| query_duration("select", LBL_OK));
/// Go `SelectErrorDuration`.
pub static SELECT_ERROR_DURATION: LazyLock<Histogram> =
    LazyLock::new(|| query_duration("select", LBL_ERROR));
/// Go `DeleteSuccessDuration`.
pub static DELETE_SUCCESS_DURATION: LazyLock<Histogram> =
    LazyLock::new(|| query_duration("delete", LBL_OK));
/// Go `DeleteErrorDuration`.
pub static DELETE_ERROR_DURATION: LazyLock<Histogram> =
    LazyLock::new(|| query_duration("delete", LBL_ERROR));

/// Go `ScannedExpiredRows`.
pub static SCANNED_EXPIRED_ROWS: LazyLock<GenericCounter<AtomicF64>> =
    LazyLock::new(|| processed_rows("select", LBL_OK));
/// Go `DeleteSuccessExpiredRows`.
pub static DELETE_SUCCESS_EXPIRED_ROWS: LazyLock<GenericCounter<AtomicF64>> =
    LazyLock::new(|| processed_rows("delete", LBL_OK));
/// Go `DeleteErrorExpiredRows`.
pub static DELETE_ERROR_EXPIRED_ROWS: LazyLock<GenericCounter<AtomicF64>> =
    LazyLock::new(|| processed_rows("delete", LBL_ERROR));

/// Go `RunningJobsCnt`.
pub static RUNNING_JOBS_CNT: LazyLock<GenericGauge<AtomicF64>> =
    LazyLock::new(|| TIDB_SERVER_TTL_JOB_STATUS.with(&labels(&[("type", "running")])));
/// Go `CancellingJobsCnt`.
pub static CANCELLING_JOBS_CNT: LazyLock<GenericGauge<AtomicF64>> =
    LazyLock::new(|| TIDB_SERVER_TTL_JOB_STATUS.with(&labels(&[("type", "cancelling")])));
/// Go `ScanningTaskCnt`.
pub static SCANNING_TASK_CNT: LazyLock<GenericGauge<AtomicF64>> =
    LazyLock::new(|| TIDB_SERVER_TTL_TASK_STATUS.with(&labels(&[("type", "scanning")])));
/// Go `DeletingTaskCnt`.
pub static DELETING_TASK_CNT: LazyLock<GenericGauge<AtomicF64>> =
    LazyLock::new(|| TIDB_SERVER_TTL_TASK_STATUS.with(&labels(&[("type", "deleting")])));

/// One entry of Go `WaterMarkScheduleDelayNames`.
pub struct WaterMarkScheduleDelayName {
    /// Go `Name`.
    pub name: &'static str,
    /// Go `Delay`.
    pub delay: Duration,
}

const HOUR: Duration = Duration::from_secs(3600);

/// Go `WaterMarkScheduleDelayNames` (its "one week" entry carries Go's
/// 72-hour delay).
pub const WATER_MARK_SCHEDULE_DELAY_NAMES: [WaterMarkScheduleDelayName; 8] = [
    WaterMarkScheduleDelayName { name: "01 hour", delay: HOUR },
    WaterMarkScheduleDelayName { name: "02 hour", delay: HOUR },
    WaterMarkScheduleDelayName { name: "06 hour", delay: Duration::from_secs(6 * 3600) },
    WaterMarkScheduleDelayName { name: "12 hour", delay: Duration::from_secs(12 * 3600) },
    WaterMarkScheduleDelayName { name: "24 hour", delay: Duration::from_secs(24 * 3600) },
    WaterMarkScheduleDelayName { name: "72 hour", delay: Duration::from_secs(72 * 3600) },
    WaterMarkScheduleDelayName { name: "one week", delay: Duration::from_secs(72 * 3600) },
    // Go `math.MaxInt64` nanoseconds.
    WaterMarkScheduleDelayName { name: "others", delay: Duration::from_nanos(i64::MAX as u64) },
];

const WORKER_PHASES: [&str; 9] = [
    PHASE_IDLE,
    PHASE_BEGIN_TXN,
    PHASE_COMMIT_TXN,
    PHASE_QUERY,
    PHASE_WAIT_RETRY,
    PHASE_DISPATCH,
    PHASE_CHECK_TTL,
    PHASE_WAIT_TOKEN,
    PHASE_OTHER,
];

/// Go `initWorkerPhases`.
fn init_worker_phases(worker_type: &str) -> HashMap<&'static str, GenericCounter<AtomicF64>> {
    WORKER_PHASES
        .iter()
        .map(|phase| {
            (
                *phase,
                TIDB_SERVER_TTL_PHASE_TIME.with(&labels(&[("type", worker_type), ("phase", phase)])),
            )
        })
        .collect()
}

static SCAN_WORKER_PHASES: LazyLock<HashMap<&'static str, GenericCounter<AtomicF64>>> =
    LazyLock::new(|| init_worker_phases("scan_worker"));
static DELETE_WORKER_PHASES: LazyLock<HashMap<&'static str, GenericCounter<AtomicF64>>> =
    LazyLock::new(|| init_worker_phases("delete_worker"));

type GetTime = Box<dyn Fn() -> Instant + Send + Sync>;
type RecordDuration = Box<dyn Fn(&str, Duration) + Send + Sync>;

/// Go `PhaseTracer`: accumulates the time spent in each phase.
pub struct PhaseTracer {
    get_time: GetTime,
    record_duration: RecordDuration,
    /// Go `phase` and `phaseTime`.
    state: Mutex<(String, Instant)>,
}

/// Go `NewScanWorkerPhaseTracer`.
pub fn new_scan_worker_phase_tracer() -> PhaseTracer {
    new_phase_tracer(Box::new(Instant::now), Box::new(|status, duration| {
        if let Some(counter) = SCAN_WORKER_PHASES.get(status) {
            counter.inc_by(duration.as_secs_f64());
        }
    }))
}

/// Go `NewDeleteWorkerPhaseTracer`.
pub fn new_delete_worker_phase_tracer() -> PhaseTracer {
    new_phase_tracer(Box::new(Instant::now), Box::new(|status, duration| {
        if let Some(counter) = DELETE_WORKER_PHASES.get(status) {
            counter.inc_by(duration.as_secs_f64());
        }
    }))
}

/// Go `newPhaseTracer`.
pub fn new_phase_tracer(get_time: GetTime, record_duration: RecordDuration) -> PhaseTracer {
    let phase_time = get_time();
    PhaseTracer {
        get_time,
        record_duration,
        state: Mutex::new((String::new(), phase_time)),
    }
}

/// Go's `*PhaseTracer` methods, which all accept a nil receiver.
pub trait PhaseTracerExt {
    /// Go `Phase`: the current phase, `""` for a nil tracer.
    fn phase(&self) -> String;
    /// Go `EnterPhase`.
    fn enter_phase(&self, phase: &str);
    /// Go `EndPhase`.
    fn end_phase(&self);
}

impl PhaseTracerExt for Option<&PhaseTracer> {
    fn phase(&self) -> String {
        match self {
            None => String::new(),
            Some(tracer) => tracer.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner).0.clone(),
        }
    }

    fn enter_phase(&self, phase: &str) {
        let Some(tracer) = self else {
            return;
        };
        let now = (tracer.get_time)();
        let mut state = tracer.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.0.is_empty() {
            (tracer.record_duration)(&state.0, now.saturating_duration_since(state.1));
        }
        *state = (phase.to_owned(), now);
    }

    fn end_phase(&self) {
        self.enter_phase("");
    }
}

/// Go `DelayMetricsRecord`: the delay metric record for a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelayMetricsRecord {
    /// Go `TableID`.
    pub table_id: i64,
    /// Go `LastJobTime`.
    pub last_job_time: SystemTime,
    /// Go `AbsoluteDelay`.
    pub absolute_delay: Duration,
    /// Go `ScheduleRelativeDelay`.
    pub schedule_relative_delay: Duration,
}

/// Go `getWaterMarkScheduleDelayName`.
pub fn get_water_mark_schedule_delay_name(delay: Duration) -> &'static str {
    for entry in &WATER_MARK_SCHEDULE_DELAY_NAMES {
        if delay <= entry.delay {
            return entry.name;
        }
    }
    WATER_MARK_SCHEDULE_DELAY_NAMES[WATER_MARK_SCHEDULE_DELAY_NAMES.len() - 1].name
}

/// Go `UpdateDelayMetrics`.
pub fn update_delay_metrics(records: &HashMap<i64, DelayMetricsRecord>) {
    let mut schedule_metrics: HashMap<&str, f64> = WATER_MARK_SCHEDULE_DELAY_NAMES
        .iter()
        .map(|entry| (entry.name, 0.0))
        .collect();
    for record in records.values() {
        let name = get_water_mark_schedule_delay_name(record.schedule_relative_delay);
        *schedule_metrics.entry(name).or_insert(0.0) += 1.0;
    }
    for (delay, value) in schedule_metrics {
        TIDB_SERVER_TTL_WATERMARK_DELAY
            .with(&labels(&[("type", "schedule"), ("name", delay)]))
            .set(value);
    }
}

/// Go `ClearDelayMetrics`.
pub fn clear_delay_metrics() {
    TIDB_SERVER_TTL_WATERMARK_DELAY.reset();
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    /// Go `TestPhaseTracer`.
    #[test]
    fn phase_tracer_records_the_left_phase_once_per_transition() {
        let now = Arc::new(Mutex::new(Instant::now()));
        let records = Arc::new(Mutex::new(Vec::new()));
        let tracer = new_phase_tracer(
            {
                let now = Arc::clone(&now);
                Box::new(move || *now.lock().unwrap())
            },
            {
                let records = Arc::clone(&records);
                Box::new(move |phase, duration| records.lock().unwrap().push((phase.to_owned(), duration)))
            },
        );
        let tracer = Some(&tracer);
        let advance = |seconds| *now.lock().unwrap() += Duration::from_secs(seconds);

        assert_eq!(tracer.phase(), "");
        advance(1);
        tracer.enter_phase("p1");
        assert_eq!(tracer.phase(), "p1");
        assert!(records.lock().unwrap().is_empty());

        advance(5);
        tracer.enter_phase("p2");
        assert_eq!(*records.lock().unwrap(), vec![("p1".to_owned(), Duration::from_secs(5))]);

        advance(10);
        tracer.enter_phase("p2");
        assert_eq!(records.lock().unwrap().last(), Some(&("p2".to_owned(), Duration::from_secs(10))));

        advance(3);
        tracer.end_phase();
        assert_eq!(tracer.phase(), "");
        assert_eq!(records.lock().unwrap().last(), Some(&("p2".to_owned(), Duration::from_secs(3))));

        advance(9);
        tracer.enter_phase("p3");
        assert_eq!(records.lock().unwrap().len(), 3);

        let nil: Option<&PhaseTracer> = None;
        nil.enter_phase("p1");
        assert_eq!(nil.phase(), "");
    }

    /// Go `getWaterMarkScheduleDelayName`'s first-fit bucket order.
    #[test]
    fn delay_names_take_the_first_bucket_that_fits() {
        assert_eq!(get_water_mark_schedule_delay_name(Duration::ZERO), "01 hour");
        assert_eq!(get_water_mark_schedule_delay_name(HOUR), "01 hour");
        assert_eq!(get_water_mark_schedule_delay_name(HOUR + Duration::from_secs(1)), "06 hour");
        assert_eq!(get_water_mark_schedule_delay_name(Duration::from_secs(73 * 3600)), "others");
    }
}
