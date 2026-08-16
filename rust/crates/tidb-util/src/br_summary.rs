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

//! Go `br/pkg/summary` lands as a complete package: the field-aggregating
//! collector behind BR's end-of-run summary line (`collector.go`) and the
//! package-level façade over the process-wide collector (`summary.go`), with
//! the package's one test function plus tests for the behavior Go leaves
//! unpinned.
//!
//! A BR backup or restore reports itself by accumulating named counters as it
//! runs — per-range durations and byte counts, integer and unsigned tallies,
//! and one failure reason per failed unit — and then emitting all of them as a
//! single structured log line at the end. [`LogCollectorImpl::summary`] is that
//! emit step: it decides between the "success summary" and "failed summary"
//! shapes, renders the byte-valued fields as human sizes, derives an average
//! speed from the elapsed wall time, and clears the per-run maps so a second
//! phase can reuse the same collector.
//!
//! # Narrowings and boundaries
//!
//! - **`go.uber.org/zap.Field`** → [`tidb_log::Field`] / [`tidb_log::Value`],
//!   the same mapping `crate::lightning_verification` uses. `zap.Duration`
//!   becomes `Value::Duration` (nanoseconds), `zap.Int` `Value::I64`,
//!   `zap.Uint64` `Value::U64`, `zap.String` `Value::Str`, and `zap.Error`
//!   `Value::Error`.
//! - **`github.com/docker/go-units.HumanSize`** is reimplemented as
//!   [`human_size`]: divide by 1000 while the value is at least 1000, stopping
//!   at `YB`, then render with Go's `%.4g` — which [`format_g4`] provides,
//!   since Rust has no `%g` verb. That is the whole of what BR uses from
//!   `go-units`.
//! - **`error`** — Go stores `map[string]error` and asks
//!   `berror.Cause(reason) != context.Canceled` to decide whether a failure is
//!   worth naming in the log. The two outcomes are the two variants of
//!   [`FailureReason`]; the rest of Go's error-wrapping chain is not needed to
//!   make that decision.
//! - **`CollectSuccessUnit(name, unitCount, arg any)`** — Go type-switches
//!   `arg` over `time.Duration` and `uint64` and silently drops every other
//!   dynamic type. [`SuccessArg`] is the closed set of the two cases Go acts
//!   on; the dropped-type branch becomes unrepresentable rather than silent.
//! - **`InitCollector(hasLogFile)`** — Go, when BR writes to a log file, builds
//!   a second logger over an empty `log.Config` so the summary also reaches
//!   stdout. [`init_collector`] keeps that intent by installing a collector
//!   that logs through `tidb_log`'s global logger, and, when `has_log_file` is
//!   set, additionally through a freshly initialized default-config logger.
//! - **Map iteration order.** Go ranges over `map[string]...`, whose order is
//!   randomized; the aggregation itself is order-independent, but the emitted
//!   field order is not. `BTreeMap` here makes the summary line deterministic
//!   in key order.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};

use tidb_log::{Field, Value};

/// Go `BackupUnit`: tells summary it is in backup.
pub const BACKUP_UNIT: &str = "backup";
/// Go `RestoreUnit`: tells summary it is in restore.
pub const RESTORE_UNIT: &str = "restore";

/// Go `TotalKV`: a field collected during backup/restore.
pub const TOTAL_KV: &str = "total kv";
/// Go `TotalBytes`: a field collected during backup/restore.
pub const TOTAL_BYTES: &str = "total bytes";
/// Go `BackupDataSize`: a field collected after backup finishes.
pub const BACKUP_DATA_SIZE: &str = "backup data size(after compressed)";
/// Go `RestoreDataSize`: a field collected after restore finishes.
pub const RESTORE_DATA_SIZE: &str = "restore data size(after compressed)";
/// Go `SkippedKVCountByCheckpoint`: a field skipped during backup/restore.
pub const SKIPPED_KV_COUNT_BY_CHECKPOINT: &str = "skipped kv count by checkpoint";
/// Go `SkippedBytesByCheckpoint`: a field skipped during backup/restore.
pub const SKIPPED_BYTES_BY_CHECKPOINT: &str = "skipped bytes by checkpoint";

/// Go `logFunc`: the sink a collector writes its summary line to.
pub type LogFunc = Arc<dyn Fn(&str, &[Field]) + Send + Sync>;

/// The dynamic types Go's `CollectSuccessUnit` acts on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SuccessArg {
    /// Go's `case time.Duration`: bumps the success-unit count and the named
    /// cost.
    Cost(Duration),
    /// Go's `case uint64`: accumulates into the named data counter.
    Data(u64),
}

/// Why a unit failed, narrowed to the distinction Go's `Summary` draws with
/// `berror.Cause(reason) != context.Canceled`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FailureReason {
    /// The unit was canceled; Go counts it but does not name it in the log.
    Canceled,
    /// Anything else; Go logs the unit name and the error.
    Error(String),
}

impl FailureReason {
    /// The message this reason contributes to the summary log.
    pub fn message(&self) -> String {
        match self {
            Self::Canceled => "context canceled".to_owned(),
            Self::Error(message) => message.clone(),
        }
    }
}

/// Go `LogCollector`: collects info into the summary log.
pub trait LogCollector: Send + Sync {
    /// Go `SetUnit`.
    fn set_unit(&self, unit: &str);
    /// Go `CollectSuccessUnit`.
    fn collect_success_unit(&self, name: &str, unit_count: i64, arg: SuccessArg);
    /// Go `CollectFailureUnit`; only the first reason per name is kept.
    fn collect_failure_unit(&self, name: &str, reason: FailureReason);
    /// Go `CollectDuration`.
    fn collect_duration(&self, name: &str, t: Duration);
    /// Go `CollectInt`.
    fn collect_int(&self, name: &str, t: i64);
    /// Go `CollectUInt`.
    fn collect_uint(&self, name: &str, t: u64);
    /// Go `SetSuccessStatus`.
    fn set_success_status(&self, success: bool);
    /// Go `NowDureTime`: time elapsed since the collector's start time.
    fn now_dure_time(&self) -> Duration;
    /// Go `AdjustStartTimeToEarlierTime`: moves the start time back by `t`.
    fn adjust_start_time_to_earlier_time(&self, t: Duration);
    /// Go `Summary`: emits the summary line and clears the per-run maps.
    fn summary(&self, name: &str);
    /// Go `Log`.
    fn log(&self, msg: &str, fields: &[Field]);
}

#[derive(Default)]
struct CollectorState {
    unit: String,
    success_unit_count: i64,
    failure_unit_count: i64,
    success_costs: BTreeMap<String, Duration>,
    success_data: BTreeMap<String, u64>,
    failure_reasons: BTreeMap<String, FailureReason>,
    durations: BTreeMap<String, Duration>,
    ints: BTreeMap<String, i64>,
    uints: BTreeMap<String, u64>,
    success_status: bool,
    start_time: Option<Instant>,
}

/// Go `logCollector`.
pub struct LogCollectorImpl {
    state: Mutex<CollectorState>,
    log: LogFunc,
}

impl LogCollectorImpl {
    /// Go `NewLogCollector`.
    pub fn new(logf: LogFunc) -> LogCollectorImpl {
        LogCollectorImpl {
            state: Mutex::new(CollectorState {
                start_time: Some(Instant::now()),
                ..CollectorState::default()
            }),
            log: logf,
        }
    }

    /// The unit set by [`LogCollector::set_unit`]; Go keeps the field private
    /// and never reads it back, so this exists for tests.
    pub fn unit(&self) -> String {
        self.state.lock().expect("summary collector").unit.clone()
    }
}

/// Go `logKeyFor`: summary field keys are hyphenated.
fn log_key_for(key: &str) -> String {
    key.replace(' ', "-")
}

fn duration_field(key: impl Into<String>, value: Duration) -> Field {
    Field::new(key, Value::Duration(value.as_nanos() as i64))
}

impl LogCollector for LogCollectorImpl {
    fn set_unit(&self, unit: &str) {
        let mut state = self.state.lock().expect("summary collector");
        state.unit = unit.to_owned();
    }

    fn collect_success_unit(&self, name: &str, unit_count: i64, arg: SuccessArg) {
        let mut state = self.state.lock().expect("summary collector");
        match arg {
            SuccessArg::Cost(value) => {
                state.success_unit_count += unit_count;
                *state.success_costs.entry(name.to_owned()).or_default() += value;
            }
            SuccessArg::Data(value) => {
                *state.success_data.entry(name.to_owned()).or_default() += value;
            }
        }
    }

    fn collect_failure_unit(&self, name: &str, reason: FailureReason) {
        let mut state = self.state.lock().expect("summary collector");
        if !state.failure_reasons.contains_key(name) {
            state.failure_reasons.insert(name.to_owned(), reason);
            state.failure_unit_count += 1;
        }
    }

    fn collect_duration(&self, name: &str, t: Duration) {
        let mut state = self.state.lock().expect("summary collector");
        *state.durations.entry(name.to_owned()).or_default() += t;
    }

    fn collect_int(&self, name: &str, t: i64) {
        let mut state = self.state.lock().expect("summary collector");
        *state.ints.entry(name.to_owned()).or_default() += t;
    }

    fn collect_uint(&self, name: &str, t: u64) {
        let mut state = self.state.lock().expect("summary collector");
        *state.uints.entry(name.to_owned()).or_default() += t;
    }

    fn set_success_status(&self, success: bool) {
        let mut state = self.state.lock().expect("summary collector");
        state.success_status = success;
    }

    fn now_dure_time(&self) -> Duration {
        let state = self.state.lock().expect("summary collector");
        state
            .start_time
            .map_or(Duration::ZERO, |start| start.elapsed())
    }

    fn adjust_start_time_to_earlier_time(&self, t: Duration) {
        let mut state = self.state.lock().expect("summary collector");
        // Go `tc.startTime = tc.startTime.Add(-t)`. `Instant` refuses to go
        // before the process start, in which case the elapsed time saturates.
        state.start_time = state
            .start_time
            .map(|start| start.checked_sub(t).unwrap_or(start));
    }

    fn summary(&self, name: &str) {
        let mut state = self.state.lock().expect("summary collector");

        let mut log_fields = Vec::with_capacity(state.durations.len() + state.ints.len() + 3);
        log_fields.push(Field::new(
            "total-ranges",
            Value::I64(state.failure_unit_count + state.success_unit_count),
        ));
        log_fields.push(Field::new(
            "ranges-succeed",
            Value::I64(state.success_unit_count),
        ));
        log_fields.push(Field::new(
            "ranges-failed",
            Value::I64(state.failure_unit_count),
        ));

        for (key, value) in &state.durations {
            log_fields.push(duration_field(log_key_for(key), *value));
        }
        for (key, value) in &state.ints {
            log_fields.push(Field::new(log_key_for(key), Value::I64(*value)));
        }
        for (key, value) in &state.uints {
            log_fields.push(Field::new(log_key_for(key), Value::U64(*value)));
        }

        let failed = !state.failure_reasons.is_empty() || !state.success_status;
        if failed {
            let mut canceled_units = 0i64;
            for (unit_name, reason) in &state.failure_reasons {
                match reason {
                    FailureReason::Canceled => canceled_units += 1,
                    FailureReason::Error(message) => {
                        log_fields.push(Field::new("unit-name", Value::Str(unit_name.clone())));
                        log_fields.push(Field::new(
                            "error",
                            Value::Error {
                                basic: message.clone(),
                                verbose: None,
                            },
                        ));
                    }
                }
            }
            // Only the total number of canceled units is printed.
            tidb_log::info(
                "units canceled",
                &[Field::new("cancel-unit", Value::I64(canceled_units))],
            );
            (self.log)(&format!("{name} failed summary"), &log_fields);
        } else {
            let total_dure_time = state
                .start_time
                .map_or(Duration::ZERO, |start| start.elapsed());
            log_fields.push(duration_field("total-take", total_dure_time));

            let total_seconds = total_dure_time.as_secs_f64();
            let unit_total = state.failure_unit_count + state.success_unit_count;
            let success_status = state.success_status;
            for (data_name, data) in &state.success_data {
                let data_float = *data as f64;
                if data_name == TOTAL_BYTES {
                    log_fields.push(Field::new(
                        "total-kv-size",
                        Value::Str(human_size(data_float)),
                    ));
                    log_fields.push(Field::new(
                        "average-speed",
                        Value::Str(format!("{}/s", human_size(data_float / total_seconds))),
                    ));
                    continue;
                }
                if data_name == SKIPPED_BYTES_BY_CHECKPOINT {
                    log_fields.push(Field::new(
                        "skipped-kv-size-by-checkpoint",
                        Value::Str(human_size(data_float)),
                    ));
                    continue;
                }
                if data_name == BACKUP_DATA_SIZE || data_name == RESTORE_DATA_SIZE {
                    let verb = if data_name == BACKUP_DATA_SIZE {
                        // Go's literal typo, preserved: "Nothing to bakcup".
                        "Nothing to bakcup"
                    } else {
                        "Nothing to restore"
                    };
                    if unit_total == 0 && !success_status {
                        log_fields.push(Field::new("Result", Value::Str(verb.to_owned())));
                    } else {
                        log_fields.push(Field::new(
                            log_key_for(data_name),
                            Value::Str(human_size(data_float)),
                        ));
                    }
                    continue;
                }
                log_fields.push(Field::new(log_key_for(data_name), Value::U64(*data)));
            }

            (self.log)(&format!("{name} success summary"), &log_fields);
        }

        // Go clears these in a deferred block; note that `uints` and
        // `successData` deliberately survive across summaries.
        state.durations.clear();
        state.ints.clear();
        state.success_costs.clear();
        state.failure_reasons.clear();
    }

    fn log(&self, msg: &str, fields: &[Field]) {
        (self.log)(msg, fields);
    }
}

/// Go `units.HumanSize`: decimal (base-1000) byte sizes rendered with `%.4g`.
pub fn human_size(size: f64) -> String {
    const DECIMAL_ABBRS: [&str; 9] = ["B", "kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"];
    const BASE: f64 = 1000.0;

    let mut size = size;
    let mut index = 0usize;
    while size >= BASE && index < DECIMAL_ABBRS.len() - 1 {
        size /= BASE;
        index += 1;
    }
    format!("{}{}", format_g4(size), DECIMAL_ABBRS[index])
}

/// Go's `fmt` verb `%.4g` (i.e. `strconv.FormatFloat(v, 'g', 4, 64)`): four
/// significant digits, scientific notation outside `[1e-4, 1e4)`, trailing
/// zeros trimmed. Rust has no `%g`, so this derives the choice from the
/// rounded exponent the same way C and Go do.
fn format_g4(value: f64) -> String {
    const PRECISION: i32 = 4;

    if value.is_nan() {
        return "NaN".to_owned();
    }
    if value.is_infinite() {
        return if value.is_sign_negative() {
            "-Inf".to_owned()
        } else {
            "+Inf".to_owned()
        };
    }
    if value == 0.0 {
        return if value.is_sign_negative() { "-0" } else { "0" }.to_owned();
    }

    // Round to `PRECISION` significant digits first; the exponent of the
    // *rounded* value is what selects the notation (9999.5 rounds up to
    // 1.000e4, so it prints as `1e+04`, not as `9999`).
    let scientific = format!("{:.*e}", (PRECISION - 1) as usize, value);
    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("Rust `{:e}` always emits an exponent");
    let exponent: i32 = exponent
        .parse()
        .expect("Rust `{:e}` exponent is an integer");

    if !(-4..PRECISION).contains(&exponent) {
        let mantissa = trim_trailing_zeros(mantissa);
        let sign = if exponent < 0 { '-' } else { '+' };
        return format!("{mantissa}e{sign}{:02}", exponent.abs());
    }

    let decimals = (PRECISION - 1 - exponent).max(0) as usize;
    trim_trailing_zeros(&format!("{value:.decimals$}"))
}

fn trim_trailing_zeros(text: &str) -> String {
    if !text.contains('.') {
        return text.to_owned();
    }
    text.trim_end_matches('0').trim_end_matches('.').to_owned()
}

/// The process-wide collector, Go's package-level `collector` var.
fn global() -> &'static RwLock<Arc<dyn LogCollector>> {
    static COLLECTOR: OnceLock<RwLock<Arc<dyn LogCollector>>> = OnceLock::new();
    COLLECTOR.get_or_init(|| {
        RwLock::new(Arc::new(LogCollectorImpl::new(Arc::new(
            |msg: &str, fields: &[Field]| tidb_log::info(msg, fields),
        ))))
    })
}

fn last_status() -> &'static std::sync::atomic::AtomicBool {
    static LAST_STATUS: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    &LAST_STATUS
}

fn collector() -> Arc<dyn LogCollector> {
    Arc::clone(&global().read().expect("summary collector"))
}

/// Go `InitCollector`: installs the process-wide collector.
///
/// When BR writes to a log file, Go additionally initializes a default-config
/// logger so the summary is always duplicated to stdout; that duplication is
/// reproduced here.
pub fn init_collector(has_log_file: bool) {
    let logf: LogFunc = if has_log_file {
        match tidb_log::init_logger(&tidb_log::Config::default()) {
            Ok((logger, _)) => {
                let logger = Arc::new(logger);
                Arc::new(move |msg: &str, fields: &[Field]| {
                    logger.info(msg, fields);
                    tidb_log::info(msg, fields);
                })
            }
            Err(_) => Arc::new(|msg: &str, fields: &[Field]| tidb_log::info(msg, fields)),
        }
    } else {
        Arc::new(|msg: &str, fields: &[Field]| tidb_log::info(msg, fields))
    };
    set_log_collector(Arc::new(LogCollectorImpl::new(logf)));
}

/// Go `SetLogCollector`: allows passing a `LogCollector` in from outside.
pub fn set_log_collector(collector: Arc<dyn LogCollector>) {
    *global().write().expect("summary collector") = collector;
}

/// Go `SetUnit`: sets unit "backup"/"restore" for the summary log.
pub fn set_unit(unit: &str) {
    collector().set_unit(unit);
}

/// Go `CollectSuccessUnit`: collects success time costs.
pub fn collect_success_unit(name: &str, unit_count: i64, arg: SuccessArg) {
    collector().collect_success_unit(name, unit_count, arg);
}

/// Go `CollectFailureUnit`: collects a failure reason.
pub fn collect_failure_unit(name: &str, reason: FailureReason) {
    collector().collect_failure_unit(name, reason);
}

/// Go `CollectDuration`: collects a log time field.
pub fn collect_duration(name: &str, t: Duration) {
    collector().collect_duration(name, t);
}

/// Go `CollectInt`: collects a log int field.
pub fn collect_int(name: &str, t: i64) {
    collector().collect_int(name, t);
}

/// Go `CollectUint`: collects a log uint field.
pub fn collect_uint(name: &str, t: u64) {
    collector().collect_uint(name, t);
}

/// Go `SetSuccessStatus`: sets the final success status.
pub fn set_success_status(success: bool) {
    last_status().store(success, std::sync::atomic::Ordering::SeqCst);
    collector().set_success_status(success);
}

/// Go `Succeed`: whether the last [`set_success_status`] call passed `true`.
pub fn succeed() -> bool {
    last_status().load(std::sync::atomic::Ordering::SeqCst)
}

/// Go `NowDureTime`: the duration between the start time and now.
pub fn now_dure_time() -> Duration {
    collector().now_dure_time()
}

/// Go `AdjustStartTimeToEarlierTime`.
pub fn adjust_start_time_to_earlier_time(t: Duration) {
    collector().adjust_start_time_to_earlier_time(t);
}

/// Go `Summary`: outputs the summary log.
pub fn summary(name: &str) {
    collector().summary(name);
}

/// Go `Log`: outputs a log line through the collector's sink.
pub fn log(msg: &str, fields: &[Field]) {
    collector().log(msg, fields);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `logFunc` that records what it was handed, standing in for Go's
    /// closure-over-a-slice test sink.
    #[derive(Default)]
    struct Recorder {
        messages: Mutex<Vec<String>>,
        fields: Mutex<Vec<Field>>,
    }

    impl Recorder {
        fn sink(self: &Arc<Self>) -> LogFunc {
            let recorder = Arc::clone(self);
            Arc::new(move |msg: &str, fields: &[Field]| {
                recorder
                    .messages
                    .lock()
                    .expect("recorder")
                    .push(msg.to_owned());
                recorder
                    .fields
                    .lock()
                    .expect("recorder")
                    .extend_from_slice(fields);
            })
        }

        fn fields(&self) -> Vec<Field> {
            self.fields.lock().expect("recorder").clone()
        }

        fn messages(&self) -> Vec<String> {
            self.messages.lock().expect("recorder").clone()
        }

        fn value_of(&self, key: &str) -> Option<Value> {
            self.fields()
                .into_iter()
                .find(|field| field.key == key)
                .map(|field| field.value)
        }
    }

    fn assert_contains(recorder: &Recorder, key: &str, expected: &Value) {
        let actual = recorder
            .value_of(key)
            .unwrap_or_else(|| panic!("{key} is not in {:?}", recorder.fields()));
        assert_eq!(format!("{actual:?}"), format!("{expected:?}"));
    }

    /// Go `TestSumDurationInt`.
    #[test]
    fn test_sum_duration_int() {
        let recorder = Arc::new(Recorder::default());
        let col = LogCollectorImpl::new(recorder.sink());
        col.collect_duration("a", Duration::from_secs(1));
        col.collect_duration("b", Duration::from_secs(1));
        col.collect_duration("b", Duration::from_secs(1));
        col.collect_int("c", 2);
        col.collect_int("c", 2);
        col.set_success_status(true);
        col.summary("foo");

        assert_eq!(recorder.fields().len(), 7);
        assert_contains(
            &recorder,
            "a",
            &Value::Duration(Duration::from_secs(1).as_nanos() as i64),
        );
        assert_contains(
            &recorder,
            "b",
            &Value::Duration(Duration::from_secs(2).as_nanos() as i64),
        );
        assert_contains(&recorder, "c", &Value::I64(4));
    }

    /// Not in the Go package's test set: `units.HumanSize` is reimplemented
    /// here, so its output is pinned against `docker/go-units`' documented
    /// behavior (base 1000, `%.4g`, `B..YB`).
    #[test]
    fn test_human_size() {
        assert_eq!(human_size(0.0), "0B");
        assert_eq!(human_size(1.0), "1B");
        assert_eq!(human_size(999.0), "999B");
        assert_eq!(human_size(1000.0), "1kB");
        assert_eq!(human_size(1024.0), "1.024kB");
        assert_eq!(human_size(1_000_000.0), "1MB");
        assert_eq!(human_size(1_048_576.0), "1.049MB");
        assert_eq!(human_size(1_000_000_000.0), "1GB");
        assert_eq!(human_size(1e12), "1TB");
        assert_eq!(human_size(1e15), "1PB");
        assert_eq!(human_size(1e18), "1EB");
        assert_eq!(human_size(1e21), "1ZB");
        assert_eq!(human_size(1e24), "1YB");
        // Past YB the unit saturates and the number keeps growing.
        assert_eq!(human_size(1e27), "1000YB");
        assert_eq!(human_size(1e30), "1e+06YB");
        // Four significant digits, trailing zeros trimmed.
        assert_eq!(human_size(12_345.0), "12.35kB");
        assert_eq!(human_size(123_456.0), "123.5kB");
        assert_eq!(human_size(1_234_567.0), "1.235MB");
    }

    /// Not in the Go package's test set: `%.4g` is the piece of `HumanSize`
    /// most likely to drift, including its non-finite spellings.
    #[test]
    fn test_format_g4() {
        assert_eq!(format_g4(0.0001), "0.0001");
        assert_eq!(format_g4(0.00001), "1e-05");
        assert_eq!(format_g4(0.5), "0.5");
        assert_eq!(format_g4(9999.0), "9999");
        assert_eq!(format_g4(10000.0), "1e+04");
        // Rounding to four significant digits can push the value into the
        // exponent branch.
        assert_eq!(format_g4(9999.5), "1e+04");
        assert_eq!(format_g4(1.0006), "1.001");
        // `1.0005` is stored as 1.00049999...; both Go and Rust round it down.
        assert_eq!(format_g4(1.0005), "1");
        assert_eq!(format_g4(f64::NAN), "NaN");
        assert_eq!(format_g4(f64::INFINITY), "+Inf");
        assert_eq!(format_g4(f64::NEG_INFINITY), "-Inf");
    }

    /// Not in the Go package's test set: the success path's byte-valued fields
    /// are the only place `HumanSize` is reached from, and the special-cased
    /// names are easy to get wrong.
    #[test]
    fn test_summary_success_data_fields() {
        let recorder = Arc::new(Recorder::default());
        let col = LogCollectorImpl::new(recorder.sink());
        col.collect_success_unit(TOTAL_BYTES, 0, SuccessArg::Data(2_000_000));
        col.collect_success_unit(TOTAL_KV, 0, SuccessArg::Data(17));
        col.collect_success_unit(BACKUP_DATA_SIZE, 0, SuccessArg::Data(1_500_000));
        col.collect_success_unit(SKIPPED_BYTES_BY_CHECKPOINT, 0, SuccessArg::Data(3_000));
        col.collect_success_unit("range", 4, SuccessArg::Cost(Duration::from_secs(2)));
        col.set_success_status(true);
        col.summary("backup");

        assert_eq!(
            recorder.messages(),
            vec!["backup success summary".to_owned()]
        );
        assert_contains(&recorder, "total-ranges", &Value::I64(4));
        assert_contains(&recorder, "ranges-succeed", &Value::I64(4));
        assert_contains(&recorder, "ranges-failed", &Value::I64(0));
        assert_contains(&recorder, "total-kv-size", &Value::Str("2MB".to_owned()));
        assert_contains(&recorder, "total-kv", &Value::U64(17));
        assert_contains(
            &recorder,
            "backup-data-size(after-compressed)",
            &Value::Str("1.5MB".to_owned()),
        );
        assert_contains(
            &recorder,
            "skipped-kv-size-by-checkpoint",
            &Value::Str("3kB".to_owned()),
        );
        // The average speed is wall-clock dependent; only its shape is pinned.
        let Some(Value::Str(speed)) = recorder.value_of("average-speed") else {
            panic!("average-speed missing or not a string");
        };
        assert!(speed.ends_with("/s"), "{speed}");
    }

    /// Not in the Go package's test set: with no units at all and a false
    /// success status, `BackupDataSize` renders Go's `Result` placeholder
    /// instead of a size — and the failure path is taken.
    #[test]
    fn test_summary_nothing_to_backup() {
        // The failed-summary branch logs through the global logger.
        let _guard = crate::global_logger_test_guard();
        let recorder = Arc::new(Recorder::default());
        let col = LogCollectorImpl::new(recorder.sink());
        col.collect_success_unit(BACKUP_DATA_SIZE, 0, SuccessArg::Data(0));
        col.summary("backup");
        // `successStatus` is false, so Go takes the failed-summary branch and
        // never reaches the `Result` placeholder.
        assert_eq!(
            recorder.messages(),
            vec!["backup failed summary".to_owned()]
        );
        assert!(recorder.value_of("Result").is_none());

        // Flipping the status to true is what exposes the placeholder branch's
        // sibling: the size is rendered because the branch is not taken.
        let recorder = Arc::new(Recorder::default());
        let col = LogCollectorImpl::new(recorder.sink());
        col.collect_success_unit(RESTORE_DATA_SIZE, 0, SuccessArg::Data(4_096));
        col.set_success_status(true);
        col.summary("restore");
        assert_contains(
            &recorder,
            "restore-data-size(after-compressed)",
            &Value::Str("4.096kB".to_owned()),
        );
    }

    /// Not in the Go package's test set: failure aggregation keeps only the
    /// first reason per unit, counts canceled units without naming them, and
    /// switches the summary line to the failed shape.
    #[test]
    fn test_summary_failure_fields() {
        // The failed-summary branch logs through the global logger.
        let _guard = crate::global_logger_test_guard();
        let recorder = Arc::new(Recorder::default());
        let col = LogCollectorImpl::new(recorder.sink());
        col.collect_failure_unit("r1", FailureReason::Error("boom".to_owned()));
        // The second reason for the same name is dropped, and does not bump
        // the failure count.
        col.collect_failure_unit("r1", FailureReason::Error("ignored".to_owned()));
        col.collect_failure_unit("r2", FailureReason::Canceled);
        col.set_success_status(true);
        col.summary("restore");

        assert_eq!(
            recorder.messages(),
            vec!["restore failed summary".to_owned()]
        );
        assert_contains(&recorder, "total-ranges", &Value::I64(2));
        assert_contains(&recorder, "ranges-failed", &Value::I64(2));
        assert_contains(&recorder, "unit-name", &Value::Str("r1".to_owned()));
        assert_contains(
            &recorder,
            "error",
            &Value::Error {
                basic: "boom".to_owned(),
                verbose: None,
            },
        );
        // The canceled unit is counted, never named.
        assert!(!recorder
            .fields()
            .iter()
            .any(|field| matches!(&field.value, Value::Str(text) if text == "r2")));
        assert_eq!(FailureReason::Canceled.message(), "context canceled");
    }

    /// Not in the Go package's test set: a summary clears the per-run maps but
    /// deliberately leaves `uints` and `successData` in place.
    #[test]
    fn test_summary_resets_only_per_run_maps() {
        let recorder = Arc::new(Recorder::default());
        let col = LogCollectorImpl::new(recorder.sink());
        col.collect_duration("d", Duration::from_secs(1));
        col.collect_int("i", 3);
        col.collect_uint("u", 5);
        col.collect_success_unit(TOTAL_KV, 0, SuccessArg::Data(9));
        col.set_success_status(true);
        col.summary("first");
        col.summary("second");

        let keys: Vec<String> = recorder
            .fields()
            .into_iter()
            .map(|field| field.key)
            .collect();
        // `d` and `i` appear once (first summary only); `u` and `total-kv`
        // appear in both.
        assert_eq!(keys.iter().filter(|key| *key == "d").count(), 1);
        assert_eq!(keys.iter().filter(|key| *key == "i").count(), 1);
        assert_eq!(keys.iter().filter(|key| *key == "u").count(), 2);
        assert_eq!(keys.iter().filter(|key| *key == "total-kv").count(), 2);
    }

    /// Not in the Go package's test set: the clock helpers and `SetUnit`.
    #[test]
    fn test_clock_and_unit() {
        let recorder = Arc::new(Recorder::default());
        let col = LogCollectorImpl::new(recorder.sink());
        col.set_unit(BACKUP_UNIT);
        assert_eq!(col.unit(), "backup");

        let before = col.now_dure_time();
        col.adjust_start_time_to_earlier_time(Duration::from_secs(30));
        let after = col.now_dure_time();
        assert!(
            after >= before + Duration::from_secs(29),
            "{after:?} should be ~30s past {before:?}"
        );

        col.log("hello", &[Field::new("k", Value::I64(1))]);
        assert_eq!(recorder.messages(), vec!["hello".to_owned()]);
    }

    /// Not in the Go package's test set: `log_key_for` is the only shared
    /// key-rewriting rule between all field families.
    #[test]
    fn test_log_key_for() {
        assert_eq!(log_key_for(TOTAL_KV), "total-kv");
        assert_eq!(
            log_key_for(BACKUP_DATA_SIZE),
            "backup-data-size(after-compressed)"
        );
        assert_eq!(log_key_for("nospaces"), "nospaces");
    }

    /// Not in the Go package's test set: the package-level façade routes to
    /// whatever collector was installed, and `Succeed` tracks the last status
    /// independently of the collector.
    #[test]
    fn test_package_level_facade() {
        // The failed-summary branch logs through the global logger.
        let _guard = crate::global_logger_test_guard();
        let recorder = Arc::new(Recorder::default());
        set_log_collector(Arc::new(LogCollectorImpl::new(recorder.sink())));

        set_unit(RESTORE_UNIT);
        collect_duration("d", Duration::from_millis(1500));
        collect_int("i", 7);
        collect_uint("u", 8);
        collect_success_unit("range", 1, SuccessArg::Cost(Duration::from_secs(1)));
        collect_failure_unit("bad", FailureReason::Canceled);
        set_success_status(true);
        assert!(succeed());
        assert!(now_dure_time() < Duration::from_secs(60));
        adjust_start_time_to_earlier_time(Duration::from_secs(1));
        log("direct", &[]);
        summary("restore");

        assert_eq!(
            recorder.messages(),
            vec!["direct".to_owned(), "restore failed summary".to_owned()]
        );
        assert_contains(&recorder, "d", &Value::Duration(1_500_000_000));
        assert_contains(&recorder, "i", &Value::I64(7));
        assert_contains(&recorder, "u", &Value::U64(8));

        set_success_status(false);
        assert!(!succeed());
    }
}
