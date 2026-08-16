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

//! Go `pkg/util/traceevent` lands as a complete package: the trace-event entry
//! point and its sinks (`traceevent.go`), the configurable flight recorder with
//! its AND/OR dump-trigger compiler (`flightrecorder.go`), and the client-go
//! trace bridge (`adapter.go`), with all eleven of the package's test
//! functions.
//!
//! A trace event is emitted through [`trace_event`], gated by the categories
//! the active [`HttpFlightRecorder`] enables. Recording fans out to the
//! always-on [`flight_recorder`] ring buffer, the per-statement [`Trace`]
//! carried by the context, and the global [`Sink`] (by default [`LogSink`],
//! which emits only in `full` mode). A statement's [`Trace`] is kept or
//! dropped at [`Trace::discard_or_flush`] time by evaluating the compiled
//! dump-trigger truth table against the trigger bits the statement accumulated.
//!
//! # Narrowings and boundaries
//!
//! - **`context.Context`** → [`tracing::TraceContext`], as in `crate::tracing`.
//!   Go's `Sink.Record(ctx, event)` carries a context that only [`LogSink`]
//!   ever reads (to pick up the context logger); `crate::tracing::Sink::record`
//!   takes just the event, so [`log_event`] takes the context explicitly and
//!   [`LogSink`] logs through the background logger.
//! - **`sink.(*Trace)` type assertions.** Go recovers the statement's `*Trace`
//!   from the context by asserting on the `Sink` interface value, in
//!   `GenerateTraceID`, `CheckFlightRecorderDumpTrigger`, and
//!   `handleTraceControlExtractor`. `crate::tracing::Sink` is not downcastable
//!   (making it so would change a trait every crate above this one
//!   implements), so those three functions take the `&Trace` explicitly. The
//!   assertion-failure branches become the `Option::None` arms.
//! - **client-go `github.com/tikv/client-go/v2/trace`.** Not part of this
//!   workspace; `adapter.go` ports against the [`adapter`] boundary types
//!   [`ClientGoCategory`], [`TraceControlFlags`], and
//!   [`ClientGoTraceRegistry`], which reproduce exactly the surface TiDB uses.
//!   The concrete numeric flag values live in client-go and are not observable
//!   here, so [`TraceControlFlags`] assigns its own bits; only the named flags
//!   are meaningful.
//! - **Package `init()`.** Go's init installs the default sink, puts the
//!   process in `base` mode, and calls `RegisterWithClientGo`. Rust has no
//!   package initializer: the mode defaults are the initial values of the
//!   statics below, and [`register_with_client_go`] is called explicitly.
//! - **`copyFields` / `copyFieldsWithCapacity`** guard Go against a caller
//!   reusing a `[]zap.Field` buffer. Rust's ownership gives that invariant for
//!   free, so the events own their `Vec<Field>` directly.
//! - **`getCategoryName`** is dead code in Go (shadowed by
//!   `tracing.TraceCategory.String`, which its own test exercises) and is not
//!   duplicated here; `crate::tracing::TraceCategory::name` is the live
//!   spelling and covers strictly more categories.
//! - **`zapcore.NewJSONEncoder`** in `ConvertEventsForRendering` becomes
//!   [`fields_to_json`], which renders the same field set through `serde_json`.
//!   Go's production encoder config is used only for this Perfetto rendering
//!   path and has no test.
//! - The two Go benchmarks (`BenchmarkTraceEventDisabled`,
//!   `BenchmarkTraceEventEnabled`) measure Go allocation behavior on the
//!   disabled/enabled fast paths and are not translated.

mod adapter;
mod flightrecorder;

pub use adapter::{
    handle_client_go_is_category_enabled, handle_client_go_trace_event,
    handle_trace_control_extractor, map_category, register_with_client_go, ClientGoCategory,
    ClientGoTraceRegistry, IsCategoryEnabledFn, TraceControlExtractorFn, TraceControlFlags,
    TraceEventFn,
};
pub use flightrecorder::{
    check_flight_recorder_dump_trigger, check_truth_table, get_flight_recorder, parse_categories,
    start_http_flight_recorder, start_log_flight_recorder, truth_table_for_and, truth_table_for_or,
    CompiledDumpTriggerConfig, DevDebugConfig, DumpTriggerConfig, FlightRecorderConfig,
    HttpFlightRecorder, SuspiciousEventConfig, Trace, UserCommandConfig,
    DEV_DEBUG_TYPE_EXECUTE_INTERNAL_TRACE_MISSING, DEV_DEBUG_TYPE_SEND_REQUEST_TRACE_ID_MISSING,
};

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tidb_log::{Field, Value};

use crate::logutil;
use crate::tracing::{self, TraceContext};

pub use crate::tracing::{Event, Sink, TraceCategory};

/// Go `ModeOff`: disables all trace event recording.
pub const MODE_OFF: &str = "off";
/// Go `ModeBase`: flight recorder only (the default mode).
pub const MODE_BASE: &str = "base";
/// Go `ModeFull`: flight recorder plus log emission.
pub const MODE_FULL: &str = "full";

/// Go `TxnLifecycle`.
pub const TXN_LIFECYCLE: TraceCategory = TraceCategory::TXN_LIFECYCLE;
/// Go `Txn2PC`.
pub const TXN_2PC: TraceCategory = TraceCategory::TXN_2PC;
/// Go `TxnLockResolve`.
pub const TXN_LOCK_RESOLVE: TraceCategory = TraceCategory::TXN_LOCK_RESOLVE;
/// Go `StmtLifecycle`.
pub const STMT_LIFECYCLE: TraceCategory = TraceCategory::STMT_LIFECYCLE;
/// Go `StmtPlan`.
pub const STMT_PLAN: TraceCategory = TraceCategory::STMT_PLAN;
/// Go `KvRequest`.
pub const KV_REQUEST: TraceCategory = TraceCategory::KV_REQUEST;
/// Go `General`.
pub const GENERAL: TraceCategory = TraceCategory::GENERAL;
/// Go `UnknownClient`.
pub const UNKNOWN_CLIENT: TraceCategory = TraceCategory::UNKNOWN_CLIENT;
/// Go `AllCategories`.
pub const ALL_CATEGORIES: TraceCategory = TraceCategory::ALL;

/// Go `DefaultFlightRecorderCapacity`.
pub const DEFAULT_FLIGHT_RECORDER_CAPACITY: usize = 1024;

/// Go `FlightRecorderCoolingOffPeriod`: the minimum time between full dumps.
pub const FLIGHT_RECORDER_COOLING_OFF_PERIOD: Duration = Duration::from_secs(10);

/// Go `recorderEnabled`, initialized by `init()` to base mode.
static RECORDER_ENABLED: AtomicBool = AtomicBool::new(true);
/// Go `loggingEnabled`, initialized by `init()` to base mode.
static LOGGING_ENABLED: AtomicBool = AtomicBool::new(false);
/// Go `lastDumpTime`.
static LAST_DUMP_TIME: AtomicI64 = AtomicI64::new(0);

/// Go `eventSink`, holding the `sinkHolder`.
static EVENT_SINK: LazyLock<RwLock<Arc<dyn Sink>>> =
    LazyLock::new(|| RwLock::new(Arc::new(LogSink)));

/// Go `flightRecorder`, the always-on rolling buffer.
static FLIGHT_RECORDER: LazyLock<RingBufferSink> =
    LazyLock::new(|| RingBufferSink::new(DEFAULT_FLIGHT_RECORDER_CAPACITY));

/// Go `Enable`: turns on every category in `categories`.
pub fn enable(categories: TraceCategory) {
    tracing::enable(categories);
}

/// Go `IsEnabled`: whether the category is enabled on the active recorder.
#[must_use]
pub fn is_enabled(category: TraceCategory) -> bool {
    if tidb_config::kerneltype::is_classic() && !crate::intest::IN_TEST {
        return false;
    }
    match get_flight_recorder() {
        None => false,
        Some(recorder) => recorder.enabled_categories().0 & category.0 != 0,
    }
}

/// Go `GetEnabledCategories`: the categories the active recorder enables.
#[must_use]
pub fn get_enabled_categories() -> TraceCategory {
    get_flight_recorder().map_or(TraceCategory(0), |fr| fr.enabled_categories())
}

/// Go `NormalizeMode`: canonicalizes a user-supplied tracing mode string.
///
/// # Errors
///
/// Returns Go's `unsupported trace event mode` message for anything else.
pub fn normalize_mode(mode: &str) -> Result<&'static str, String> {
    match mode.trim().to_lowercase().as_str() {
        MODE_OFF | "0" | "false" => Ok(MODE_OFF),
        MODE_BASE => Ok(MODE_BASE),
        MODE_FULL => Ok(MODE_FULL),
        _ => Err(format!(
            "unsupported trace event mode {mode:?}, valid modes: off, base, full"
        )),
    }
}

/// Go `SetMode`: applies the requested mode and returns its canonical value.
///
/// # Errors
///
/// Propagates [`normalize_mode`]'s error.
pub fn set_mode(mode: &str) -> Result<&'static str, String> {
    let normalized = normalize_mode(mode)?;
    match normalized {
        MODE_OFF => {
            RECORDER_ENABLED.store(false, Ordering::SeqCst);
            LOGGING_ENABLED.store(false, Ordering::SeqCst);
        }
        MODE_BASE => {
            RECORDER_ENABLED.store(true, Ordering::SeqCst);
            LOGGING_ENABLED.store(false, Ordering::SeqCst);
        }
        _ => {
            RECORDER_ENABLED.store(true, Ordering::SeqCst);
            LOGGING_ENABLED.store(true, Ordering::SeqCst);
        }
    }
    Ok(normalized)
}

/// Go `CurrentMode`: the canonical tracing mode string.
#[must_use]
pub fn current_mode() -> &'static str {
    let recorder = RECORDER_ENABLED.load(Ordering::SeqCst);
    let logging = LOGGING_ENABLED.load(Ordering::SeqCst);
    if !recorder && !logging {
        return MODE_OFF;
    }
    if recorder && logging {
        return MODE_FULL;
    }
    if recorder && !logging {
        return MODE_BASE;
    }
    // Shouldn't happen (logging without recorder), but return full for
    // consistency.
    MODE_FULL
}

/// Go `SetSink`: replaces the global sink. `None` restores the default sink.
pub fn set_sink(sink: Option<Arc<dyn Sink>>) {
    let sink = sink.unwrap_or_else(|| Arc::new(LogSink));
    *EVENT_SINK
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = sink;
}

/// Go `CurrentSink`: the sink currently used for trace events.
#[must_use]
pub fn current_sink() -> Arc<dyn Sink> {
    Arc::clone(
        &EVENT_SINK
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    )
}

/// Go `FlightRecorder()`: the always-on in-memory ring buffer.
#[must_use]
pub fn flight_recorder() -> &'static RingBufferSink {
    &FLIGHT_RECORDER
}

/// Go `TraceEvent`: records an event when its category is enabled.
///
/// The caller is responsible for applying any necessary redaction to `fields`.
pub fn trace_event(ctx: &TraceContext, category: TraceCategory, name: &str, fields: Vec<Field>) {
    if !is_enabled(category) {
        return;
    }

    let event = Event {
        category,
        name: name.to_owned(),
        phase: tracing::Phase::Instant,
        timestamp: SystemTime::now(),
        trace_id: trace_id_from_context(ctx).to_vec(),
        fields,
    };

    // Record to flight recorder if enabled (base or full mode).
    if RECORDER_ENABLED.load(Ordering::SeqCst) {
        flight_recorder().record(&event);
        if let Some(sink) = ctx.sink() {
            sink.record(&event);
        }
    }

    // Record to log sink if logging is enabled (full mode).
    current_sink().record(&event);
}

/// Go `TraceIDFromContext`: the trace identifier carried by the context.
#[must_use]
pub fn trace_id_from_context(ctx: &TraceContext) -> &[u8] {
    ctx.extract_trace_id()
}

/// Go `ContextWithTraceID`: a context carrying the given trace identifier.
#[must_use]
pub fn context_with_trace_id(ctx: &TraceContext, trace_id: &[u8]) -> TraceContext {
    let mut next = ctx.clone();
    next.trace_id = trace_id.to_vec();
    next
}

/// Go `GenerateTraceID`: a 20-byte identifier
/// `[start_ts (8)][stmt_count (8)][random (4)]` in big-endian order.
///
/// The random suffix distinguishes statement executions, and is taken from the
/// statement's [`Trace`] when it has one (Go asserts the context sink to
/// `*Trace`; see the module boundaries). Call once per statement execution,
/// not per retry.
#[must_use]
pub fn generate_trace_id(trace: Option<&Trace>, start_ts: u64, stmt_count: u64) -> Vec<u8> {
    let mut trace_id = vec![0_u8; 20];
    trace_id[0..8].copy_from_slice(&start_ts.to_be_bytes());
    trace_id[8..16].copy_from_slice(&stmt_count.to_be_bytes());
    let mut rand32 = trace.map_or(0, Trace::rand32);
    if rand32 == 0 {
        rand32 = crate::fastrand::uint32();
    }
    trace_id[16..20].copy_from_slice(&rand32.to_be_bytes());
    trace_id
}

/// Go `LogSink`: serializes trace events to the global logger.
#[derive(Clone, Copy, Debug, Default)]
pub struct LogSink;

impl Sink for LogSink {
    fn record(&self, event: &Event) {
        if !LOGGING_ENABLED.load(Ordering::SeqCst) {
            return;
        }
        log_event(None, event);
    }
}

/// Go `logEvent`: emits one event with `[category] [timestamp] [trace_id?]`
/// appended after the event's own fields.
pub fn log_event(ctx: Option<&TraceContext>, event: &Event) {
    let mut fields = event.fields.clone();
    fields.push(Field::new("category", Value::Str(event.category.name())));
    fields.push(Field::new(
        "event_ts",
        Value::I64(unix_micro(event.timestamp)),
    ));
    if !event.trace_id.is_empty() {
        fields.push(Field::new(
            "trace_id",
            Value::Str(hex_encode(&event.trace_id)),
        ));
    }

    let logger = logutil::logger_with_trace_info(
        &logutil::bg_logger(),
        ctx.and_then(TraceContext::trace_info),
    );
    logger.info(&format!("[trace-event] {}", event.name), &fields);
}

/// Go `MultiSink`: distributes events to multiple sinks.
pub struct MultiSink {
    sinks: Vec<Arc<dyn Sink>>,
}

impl MultiSink {
    /// Go `NewMultiSink`.
    #[must_use]
    pub fn new(sinks: Vec<Arc<dyn Sink>>) -> Self {
        Self { sinks }
    }
}

impl Sink for MultiSink {
    fn record(&self, event: &Event) {
        for sink in &self.sinks {
            sink.record(event);
        }
    }
}

/// Go `RingBufferSink`: buffers the most recent events in a ring.
#[derive(Debug)]
pub struct RingBufferSink {
    state: Mutex<RingBufferState>,
    capacity: usize,
}

#[derive(Debug)]
struct RingBufferState {
    buf: Vec<Event>,
    next: usize,
}

impl RingBufferSink {
    /// Go `NewRingBufferSink`. A non-positive capacity becomes 1.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            state: Mutex::new(RingBufferState {
                buf: Vec::with_capacity(capacity),
                next: 0,
            }),
            capacity,
        }
    }

    /// Go `RingBufferSink.DiscardOrFlush`: clears all buffered events.
    pub fn discard_or_flush(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.buf.clear();
        state.next = 0;
    }

    /// Go `RingBufferSink.Snapshot`: buffered events, oldest to newest.
    #[must_use]
    pub fn snapshot(&self) -> Vec<Event> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.buf.is_empty() {
            return Vec::new();
        }
        if state.buf.len() < self.capacity {
            return state.buf.clone();
        }
        let mut result = Vec::with_capacity(state.buf.len());
        result.extend_from_slice(&state.buf[state.next..]);
        result.extend_from_slice(&state.buf[..state.next]);
        result
    }
}

impl Sink for RingBufferSink {
    fn record(&self, event: &Event) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.buf.len() < self.capacity {
            state.buf.push(event.clone());
            if state.buf.len() == self.capacity {
                state.next = 0;
            }
            return;
        }
        let next = state.next;
        state.buf[next] = event.clone();
        state.next = (next + 1) % self.capacity;
    }
}

/// Go `DumpFlightRecorderToLogger`: emits the buffered events to the background
/// logger for crash diagnostics. Inside the cooling-off period only a summary
/// line is emitted.
pub fn dump_flight_recorder_to_logger(reason: &str) {
    let events = flight_recorder().snapshot();
    if events.is_empty() {
        return;
    }

    let logger = logutil::bg_logger();
    let now = unix_seconds(SystemTime::now());
    let last = LAST_DUMP_TIME.load(Ordering::SeqCst);
    let elapsed = now.saturating_sub(last);

    let cooling_off_secs = i64::try_from(FLIGHT_RECORDER_COOLING_OFF_PERIOD.as_secs()).unwrap_or(0);
    if last > 0 && elapsed < cooling_off_secs {
        logger.info(
            "flight recorder dump suppressed (cooling off)",
            &[
                Field::new("reason", Value::Str(reason.to_owned())),
                Field::new("event_count", Value::I64(event_count(&events))),
                Field::new(
                    "elapsed_since_last_dump",
                    Value::Duration(elapsed.saturating_mul(1_000_000_000)),
                ),
            ],
        );
        return;
    }

    LAST_DUMP_TIME.store(now, Ordering::SeqCst);

    logger.info(
        "dump flight recorder",
        &[
            Field::new("reason", Value::Str(reason.to_owned())),
            Field::new("event_count", Value::I64(event_count(&events))),
        ],
    );
    for event in &events {
        let mut fields = Vec::with_capacity(event.fields.len() + 5);
        fields.push(Field::new("event_name", Value::Str(event.name.clone())));
        fields.push(Field::new("category", Value::Str(event.category.name())));
        fields.push(Field::new(
            "event_ts",
            Value::I64(unix_micro(event.timestamp)),
        ));
        if !event.trace_id.is_empty() {
            fields.push(Field::new(
                "trace_id",
                Value::Str(hex_encode(&event.trace_id)),
            ));
        }
        fields.extend(event.fields.iter().cloned());
        logger.info("[trace-event-flight]", &fields);
    }
}

/// Go `RenderEvent`: the event shape consumed by <https://ui.perfetto.dev>.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RenderEvent {
    /// Go `name`.
    pub name: String,
    /// Go `ph`.
    #[serde(rename = "ph", serialize_with = "serialize_phase")]
    pub phase: tracing::Phase,
    /// Go `ts`, in microseconds.
    pub ts: i64,
    /// Go `pid`.
    pub pid: u32,
    /// Go `tid`.
    pub tid: u32,
    /// Go `id`, used by async/flow events.
    #[serde(skip_serializing_if = "is_zero_u64")]
    pub id: u64,
    /// Go `cat`.
    #[serde(rename = "cat", skip_serializing_if = "String::is_empty")]
    pub category: String,
    /// Go `args`, the event's fields as a JSON object.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<serde_json::Value>,
}

#[expect(clippy::trivially_copy_pass_by_ref, reason = "serde serializer shape")]
fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

fn serialize_phase<S: serde::Serializer>(
    phase: &tracing::Phase,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(phase.as_str())
}

/// Go `extractRandFromTraceID`.
///
/// Go reads the four bytes at offset 16 through a `*uint32`, i.e. in the host's
/// native byte order, even though [`generate_trace_id`] writes them big-endian.
/// This mirrors that native-endian read exactly rather than "fixing" it.
#[must_use]
fn extract_rand_from_trace_id(trace_id: &[u8]) -> u32 {
    if trace_id.len() != 20 {
        return 0;
    }
    u32::from_ne_bytes([trace_id[16], trace_id[17], trace_id[18], trace_id[19]])
}

/// Go `ConvertEventsForRendering`.
#[must_use]
pub fn convert_events_for_rendering(events: &[Event]) -> Vec<RenderEvent> {
    let mut tid = 0_u32;
    let mut res = Vec::with_capacity(events.len());
    for event in events {
        let mut rendered = RenderEvent {
            name: event.name.clone(),
            phase: event.phase,
            ts: unix_micro(event.timestamp),
            pid: 0,
            tid: 0,
            id: 0,
            category: event.category.name(),
            args: None,
        };
        if tid == 0 {
            tid = extract_rand_from_trace_id(&event.trace_id);
        } else {
            let value = extract_rand_from_trace_id(&event.trace_id);
            if tid != value {
                logutil::bg_logger().info(
                    "wrong traceid",
                    &[
                        Field::new("expect", Value::U64(u64::from(tid))),
                        Field::new("get", Value::U64(u64::from(value))),
                    ],
                );
            }
        }
        if !event.trace_id.is_empty() && event.trace_id.len() != 20 {
            logutil::bg_logger().info(
                "wrong traceid format",
                &[Field::new(
                    "trace_id",
                    Value::Str(String::from_utf8_lossy(&event.trace_id).into_owned()),
                )],
            );
        }

        if !event.fields.is_empty() {
            let mut fields = event.fields.clone();
            if !event.trace_id.is_empty() {
                fields.push(Field::new(
                    "trace_id",
                    Value::Str(hex_encode(&event.trace_id)),
                ));
            }
            rendered.args = Some(fields_to_json(&fields));
        }
        res.push(rendered);
    }
    if tid == 0 {
        logutil::bg_logger().info("wrong traceid", &[]);
    }
    for rendered in &mut res {
        rendered.tid = tid;
    }
    res
}

/// Renders log fields as the JSON object Go's zap JSON encoder produces for
/// `RenderEvent.Args`.
#[must_use]
pub fn fields_to_json(fields: &[Field]) -> serde_json::Value {
    let mut map = serde_json::Map::with_capacity(fields.len());
    for field in fields {
        map.insert(field.key.clone(), field_value_to_json(&field.value));
    }
    serde_json::Value::Object(map)
}

fn field_value_to_json(value: &Value) -> serde_json::Value {
    use serde_json::Value as Json;
    match value {
        Value::Str(text) => Json::String(text.clone()),
        Value::I64(number) => Json::from(*number),
        Value::U64(number) => Json::from(*number),
        Value::F64(number) => {
            serde_json::Number::from_f64(*number).map_or(Json::Null, Json::Number)
        }
        Value::Bool(flag) => Json::Bool(*flag),
        Value::Complex { real, imag } => Json::String(format!("{real}+{imag}i")),
        // zap's production config renders durations as fractional seconds.
        Value::Duration(nanos) => {
            #[expect(clippy::cast_precision_loss, reason = "zap encodes seconds as f64")]
            let seconds = *nanos as f64 / 1e9;
            serde_json::Number::from_f64(seconds).map_or(Json::Null, Json::Number)
        }
        Value::Binary(bytes) => {
            use base64::Engine as _;
            Json::String(base64::engine::general_purpose::STANDARD.encode(bytes))
        }
        Value::ByteString(bytes) => Json::String(String::from_utf8_lossy(bytes).into_owned()),
        Value::Reflect(json) => {
            serde_json::from_str(json).unwrap_or_else(|_| Json::String(json.clone()))
        }
        Value::Array(values) => Json::Array(values.iter().map(field_value_to_json).collect()),
        Value::Object(fields) => fields_to_json(fields),
        Value::Error { basic, .. } => Json::String(basic.clone()),
    }
}

/// Go `hex.EncodeToString`.
fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// Go `time.Time.UnixMicro`.
fn unix_micro(time: SystemTime) -> i64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(delta) => i64::try_from(delta.as_micros()).unwrap_or(i64::MAX),
        Err(error) => -i64::try_from(error.duration().as_micros()).unwrap_or(i64::MAX),
    }
}

/// Go `time.Time.Unix`.
fn unix_seconds(time: SystemTime) -> i64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(delta) => i64::try_from(delta.as_secs()).unwrap_or(i64::MAX),
        Err(error) => -i64::try_from(error.duration().as_secs()).unwrap_or(i64::MAX),
    }
}

/// Go `zap.Int("event_count", len(events))`.
fn event_count(events: &[Event]) -> i64 {
    i64::try_from(events.len()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests;
