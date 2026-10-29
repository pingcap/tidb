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

//! TiDB trace categories, events, context propagation, and span regions.

use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use tidb_log::Field;

use tidb_intest::IN_TEST;

/// Go `TiDBTrace`, the baggage key marking a TiDB trace.
pub const TIDB_TRACE: &str = "tr";

/// Go `TraceCategory`: a bitmask selecting which trace events are emitted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TraceCategory(pub u64);

impl TraceCategory {
    /// Transaction begin/commit/rollback events.
    pub const TXN_LIFECYCLE: Self = Self(1 << 0);
    /// Two-phase commit prewrite and commit phases.
    pub const TXN_2PC: Self = Self(1 << 1);
    /// Lock resolution and conflict handling.
    pub const TXN_LOCK_RESOLVE: Self = Self(1 << 2);
    /// Statement start/finish events.
    pub const STMT_LIFECYCLE: Self = Self(1 << 3);
    /// Statement plan digest and optimization.
    pub const STMT_PLAN: Self = Self(1 << 4);
    /// client-go KV requests and responses.
    pub const KV_REQUEST: Self = Self(1 << 5);
    /// Fallback for client-go events in categories this build does not map.
    pub const UNKNOWN_CLIENT: Self = Self(1 << 6);
    /// The category used by the tracing API itself.
    pub const GENERAL: Self = Self(1 << 7);
    /// DDL job events.
    pub const DDL_JOB: Self = Self(1 << 8);
    /// Development and debugging events.
    pub const DEV_DEBUG: Self = Self(1 << 9);
    /// client-go `FlagTiKVCategoryRequest`.
    pub const TIKV_REQUEST: Self = Self(1 << 10);
    /// client-go `FlagTiKVCategoryWriteDetails`.
    pub const TIKV_WRITE_DETAILS: Self = Self(1 << 11);
    /// client-go `FlagTiKVCategoryReadDetails`.
    pub const TIKV_READ_DETAILS: Self = Self(1 << 12);
    /// Region cache events.
    pub const REGION_CACHE: Self = Self(1 << 13);

    /// Go's private `traceCategorySentinel`, one past the last category.
    const SENTINEL: u64 = 1 << 14;

    /// Go `AllCategories`: every known category.
    pub const ALL: Self = Self(Self::SENTINEL - 1);

    /// Go `getCategoryName`. A mask that is not exactly one known category
    /// renders as Go's `unknown(N)` fallback.
    pub fn name(self) -> String {
        let known = match self {
            Self::TXN_LIFECYCLE => "txn_lifecycle",
            Self::TXN_2PC => "txn_2pc",
            Self::TXN_LOCK_RESOLVE => "txn_lock_resolve",
            Self::STMT_LIFECYCLE => "stmt_lifecycle",
            Self::STMT_PLAN => "stmt_plan",
            Self::KV_REQUEST => "kv_request",
            Self::UNKNOWN_CLIENT => "unknown_client",
            Self::GENERAL => "general",
            Self::DDL_JOB => "ddl_job",
            Self::DEV_DEBUG => "dev_debug",
            Self::TIKV_REQUEST => "tikv_request",
            Self::TIKV_WRITE_DETAILS => "tikv_write_details",
            Self::TIKV_READ_DETAILS => "tikv_read_details",
            Self::REGION_CACHE => "region_cache",
            other => return format!("unknown({})", other.0),
        };
        known.to_owned()
    }

    /// Go `ParseTraceCategory`: the category with this name, or the invalid
    /// zero category.
    pub fn parse(name: &str) -> Self {
        let mut bit = 1_u64;
        while bit < Self::SENTINEL {
            let category = Self(bit);
            if category.name() == name {
                return category;
            }
            bit <<= 1;
        }
        Self(0)
    }
}

impl std::fmt::Display for TraceCategory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.name())
    }
}

impl std::ops::BitOr for TraceCategory {
    type Output = Self;
    fn bitor(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

impl std::ops::BitOrAssign for TraceCategory {
    fn bitor_assign(&mut self, other: Self) {
        self.0 |= other.0;
    }
}

impl std::ops::BitAnd for TraceCategory {
    type Output = Self;
    fn bitand(self, other: Self) -> Self {
        Self(self.0 & other.0)
    }
}

/// Go's package-level `enabledCategories`, defaulting to none enabled.
static ENABLED_CATEGORIES: AtomicU64 = AtomicU64::new(0);

/// Go `Enable`: turns on every category in `categories`.
pub fn enable(categories: TraceCategory) {
    ENABLED_CATEGORIES.fetch_or(categories.0, Ordering::SeqCst);
}

/// Go `Disable`: turns off every category in `categories`.
pub fn disable(categories: TraceCategory) {
    ENABLED_CATEGORIES.fetch_and(!categories.0, Ordering::SeqCst);
}

/// Go `SetCategories`: replaces the enabled set outright.
pub fn set_categories(categories: TraceCategory) {
    ENABLED_CATEGORIES.store(categories.0, Ordering::SeqCst);
}

/// Go `GetEnabledCategories`.
pub fn enabled_categories() -> TraceCategory {
    TraceCategory(ENABLED_CATEGORIES.load(Ordering::SeqCst))
}

/// Go `IsEnabled`. Trace events only work for the next-generation kernel, so
/// a classic build outside tests is always disabled.
pub fn is_enabled(category: TraceCategory) -> bool {
    if tidb_config_kerneltype::is_classic() && !IN_TEST {
        return false;
    }
    enabled_categories().0 & category.0 != 0
}

/// Go `Phase`: an event's position in its interval.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Phase {
    /// Go `PhaseBegin`.
    Begin,
    /// Go `PhaseEnd`.
    End,
    /// Go `PhaseAsyncBegin`.
    AsyncBegin,
    /// Go `PhaseAsyncEnd`.
    AsyncEnd,
    /// Go `PhaseFlowBegin`.
    FlowBegin,
    /// Go `PhaseFlowEnd`.
    FlowEnd,
    /// Go `PhaseInstant`.
    Instant,
    /// Any phase string accepted by Go's open string type.
    Other(String),
}

impl Phase {
    /// The single-letter wire spelling Go stores in the `Phase` string.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Begin => "B",
            Self::End => "E",
            Self::AsyncBegin => "b",
            Self::AsyncEnd => "e",
            Self::FlowBegin => "s",
            Self::FlowEnd => "f",
            Self::Instant => "i",
            Self::Other(value) => value,
        }
    }
}

impl std::fmt::Display for Phase {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Go `Event`: one traced event.
///
/// Go documents `Fields` as immutable once created because the slice may be
/// shared across goroutines; Rust's ownership gives that invariant for free —
/// a sink receives a shared reference and cannot mutate it.
#[derive(Clone, Debug)]
pub struct Event {
    /// When the event occurred.
    pub timestamp: SystemTime,
    /// Event name; for a region, the region type.
    pub name: String,
    /// Interval position.
    pub phase: Phase,
    /// Trace identifier of the owning statement, empty when untraced.
    pub trace_id: Vec<u8>,
    /// Structured fields.
    pub fields: Vec<Field>,
    /// The category gating this event.
    pub category: TraceCategory,
}

/// Go `Sink`: the destination trace events are recorded to.
pub trait Sink: std::any::Any + Send + Sync {
    /// Go `Sink.Record`.
    fn record(&self, context: &TraceContext, event: &Event);

    /// Supports Go's concrete sink type assertions.
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Go `FlightRecorder`, which is exactly a [`Sink`].
pub trait FlightRecorder: Sink {}

impl<T: Sink> FlightRecorder for T {}

/// Go `tracing.TraceInfo`: the SQL tracing identity of a statement.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TraceInfo {
    /// Alias of the session.
    pub session_alias: String,
    /// Trace ID of the SQL statement.
    pub trace_id: Vec<u8>,
    /// ID of the connection.
    pub connection_id: u64,
}

/// Go `CETraceRecord`: one expression and its cardinality-estimation result.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct CeTraceRecord {
    /// Table the expression was estimated against.
    pub table_name: String,
    /// Estimation type.
    #[serde(rename = "type")]
    pub kind: String,
    /// Restored expression text.
    pub expr: String,
    /// Table ID; excluded from the JSON form by Go's `json:"-"` tag.
    #[serde(skip)]
    pub table_id: i64,
    /// Estimated row count.
    pub row_count: u64,
}

/// Go's intentionally empty optimizer trace marker.
pub struct OptimizeTracer;

/// Go `DedupCETrace`: keeps the first occurrence of each distinct record,
/// preserving input order.
pub fn dedup_ce_trace(records: &[Arc<CeTraceRecord>]) -> Vec<Arc<CeTraceRecord>> {
    let mut seen = std::collections::HashSet::with_capacity(records.len());
    let mut deduped = Vec::with_capacity(records.len());
    for record in records {
        if seen.insert(record.as_ref().clone()) {
            deduped.push(Arc::clone(record));
        }
    }
    deduped
}

/// Go `basictracer.RawSpan`: what a finished span hands its recorder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawSpan {
    /// The span's operation name.
    pub operation: String,
    /// Identity of this span.
    pub context: SpanContext,
    /// Identity of the parent span; zero for a root span.
    pub parent_span_id: u64,
    /// Baggage propagated with the span.
    pub baggage: std::collections::BTreeMap<String, String>,
    /// When the span started.
    pub start: SystemTime,
    /// How long the span lasted.
    pub duration: Duration,
    /// Go `RawSpan.Tags`, rendered as strings.
    pub tags: std::collections::BTreeMap<String, String>,
    /// Go `RawSpan.Logs`, in logging order.
    pub logs: Vec<LogRecord>,
}

/// Go `opentracing.LogRecord`: one timestamped set of string fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogRecord {
    /// When the record was logged.
    pub timestamp: SystemTime,
    /// The `(key, value)` fields, in logging order.
    pub fields: Vec<(String, String)>,
}

/// Go `logutil.TraceEventKey`: the log field `logutil.Eventf` writes.
pub const TRACE_EVENT_KEY: &str = "event";

/// Go `basictracer.SpanContext`: the propagated identity of a span.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SpanContext {
    /// Trace this span belongs to.
    pub trace_id: u64,
    /// This span's own identifier.
    pub span_id: u64,
}

/// Go `CallbackRecorder`: a recorder that invokes a callback per finished
/// span.
pub type CallbackRecorder = Arc<dyn Fn(RawSpan) + Send + Sync>;

/// The tracer that owns span identity and recording.
///
/// A tracer with no recorder is Go's `opentracing.NoopTracer`: it still hands
/// out spans, but finishing them records nothing.
#[derive(Clone)]
pub struct Tracer {
    recorder: Option<CallbackRecorder>,
    next_id: Arc<AtomicU64>,
    /// The wall-clock moment and monotonic instant the tracer was made at:
    /// span times are this moment plus monotonic time since, so they keep
    /// the order and resolution of the monotonic clock (Go's `time.Now()`
    /// carries a monotonic reading the same way).
    epoch: (SystemTime, Instant),
}

impl std::fmt::Debug for Tracer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Tracer")
            .field("recording", &self.recorder.is_some())
            .finish()
    }
}

impl Tracer {
    /// Go `basictracer.New(recorder)`.
    pub fn new(recorder: CallbackRecorder) -> Self {
        Self {
            recorder: Some(recorder),
            next_id: Arc::new(AtomicU64::new(1)),
            epoch: (SystemTime::now(), Instant::now()),
        }
    }

    /// Go `opentracing.NoopTracer{}`.
    pub fn noop() -> Self {
        Self {
            recorder: None,
            next_id: Arc::new(AtomicU64::new(1)),
            epoch: (SystemTime::now(), Instant::now()),
        }
    }

    /// Whether this tracer discards every span (Go's no-op tracer check).
    pub fn is_noop(&self) -> bool {
        self.recorder.is_none()
    }

    /// Now, on the tracer's clock.
    fn now(&self) -> (SystemTime, Instant) {
        let now = Instant::now();
        (self.system_time_of(now), now)
    }

    /// The moment a monotonic instant was, on the tracer's clock: what a
    /// span measured elsewhere (a client span) starts at.
    pub fn system_time_of(&self, instant: Instant) -> SystemTime {
        match instant.checked_duration_since(self.epoch.1) {
            Some(after) => self.epoch.0 + after,
            None => self.epoch.0 - self.epoch.1.duration_since(instant),
        }
    }

    /// The monotonic instant a moment on the tracer's clock was.
    fn instant_of(&self, moment: SystemTime) -> Instant {
        match moment.duration_since(self.epoch.0) {
            Ok(after) => self.epoch.1 + after,
            Err(before) => self
                .epoch
                .1
                .checked_sub(before.duration())
                .unwrap_or(self.epoch.1),
        }
    }

    fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Go `Tracer.StartSpan(operation)`: a root span.
    pub fn start_span(self: &Arc<Self>, operation: &str) -> Span {
        self.start_child(operation, None)
    }

    /// Go `Tracer.StartSpan(operation, opentracing.ChildOf(parent))`.
    pub fn start_span_child_of(self: &Arc<Self>, operation: &str, parent: SpanContext) -> Span {
        self.start_child(operation, Some(parent))
    }

    /// Go `Tracer.StartSpan(operation, opentracing.FollowsFrom(parent))`.
    ///
    /// TiDB never inspects the reference kind, only the resulting parentage,
    /// so this shares `ChildOf`'s model.
    pub fn start_span_following(self: &Arc<Self>, operation: &str, parent: SpanContext) -> Span {
        self.start_child(operation, Some(parent))
    }

    /// Go `Tracer.StartSpan(operation, opentracing.ChildOf(parent),
    /// opentracing.StartTime(start))`: a span whose start was measured
    /// elsewhere, as the client spans client-rust reports carry their start.
    pub fn start_span_child_of_at(
        self: &Arc<Self>,
        operation: &str,
        parent: SpanContext,
        start: SystemTime,
    ) -> Span {
        let span = self.start_child(operation, Some(parent));
        {
            let mut state = span
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.start_instant = self.instant_of(start);
            state.start = start;
        }
        span
    }

    fn start_child(self: &Arc<Self>, operation: &str, parent: Option<SpanContext>) -> Span {
        let span_id = self.next_id();
        let (start, start_instant) = self.now();
        Span {
            tracer: Arc::clone(self),
            context: SpanContext {
                trace_id: parent.map_or(span_id, |parent| parent.trace_id),
                span_id,
            },
            parent_span_id: parent.map_or(0, |parent| parent.span_id),
            operation: operation.to_owned(),
            state: Arc::new(Mutex::new(SpanState {
                baggage: std::collections::BTreeMap::new(),
                finished: false,
                start,
                start_instant,
                tags: std::collections::BTreeMap::new(),
                logs: Vec::new(),
            })),
        }
    }
}

/// Go's `opentracing` global tracer.
static GLOBAL_TRACER: OnceLock<Mutex<Arc<Tracer>>> = OnceLock::new();

fn global_tracer_slot() -> &'static Mutex<Arc<Tracer>> {
    GLOBAL_TRACER.get_or_init(|| Mutex::new(Arc::new(Tracer::noop())))
}

/// Go `opentracing.SetGlobalTracer`.
pub fn set_global_tracer(tracer: Arc<Tracer>) {
    *global_tracer_slot()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = tracer;
}

/// Go `opentracing.GlobalTracer`.
pub fn global_tracer() -> Arc<Tracer> {
    Arc::clone(
        &global_tracer_slot()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()),
    )
}

/// Go `opentracing.Span`, as produced by [`Tracer`].
#[derive(Clone, Debug)]
pub struct Span {
    tracer: Arc<Tracer>,
    context: SpanContext,
    parent_span_id: u64,
    operation: String,
    state: Arc<Mutex<SpanState>>,
}

#[derive(Debug)]
struct SpanState {
    baggage: std::collections::BTreeMap<String, String>,
    finished: bool,
    start: SystemTime,
    start_instant: Instant,
    tags: std::collections::BTreeMap<String, String>,
    logs: Vec<LogRecord>,
}

impl Span {
    /// Go `Span.Context`.
    pub const fn context(&self) -> SpanContext {
        self.context
    }

    /// Go `Span.Tracer`.
    pub fn tracer(&self) -> &Arc<Tracer> {
        &self.tracer
    }

    /// Whether this span discards its recording (Go's no-op tracer check).
    pub fn is_noop(&self) -> bool {
        self.tracer.is_noop()
    }

    /// Go `Span.SetBaggageItem`.
    pub fn set_baggage_item(&self, key: &str, value: &str) {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .baggage
            .insert(key.to_owned(), value.to_owned());
    }

    /// Go `Span.BaggageItem`.
    pub fn baggage_item(&self, key: &str) -> Option<String> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .baggage
            .get(key)
            .cloned()
    }

    /// Go `Span.SetTag(key, value)`, the value in its `%+v` form.
    pub fn set_tag(&self, key: &str, value: &str) {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .tags
            .insert(key.to_owned(), value.to_owned());
    }

    /// Go `Span.LogKV(key, value)`.
    pub fn log_kv(&self, key: &str, value: &str) {
        self.log_fields(vec![(key.to_owned(), value.to_owned())]);
    }

    /// Go `Span.LogFields(fields...)` at the current time.
    pub fn log_fields(&self, fields: Vec<(String, String)>) {
        self.log_fields_at(fields, self.tracer.now().0);
    }

    /// Go `Span.LogFields(fields...)` for a record whose time was measured
    /// elsewhere (`opentracing.LogRecord.Timestamp`), as the client's span
    /// events arrive with theirs.
    pub fn log_fields_at(&self, fields: Vec<(String, String)>, timestamp: SystemTime) {
        if self.is_noop() {
            return;
        }
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .logs
            .push(LogRecord { timestamp, fields });
    }

    /// Go `logutil.Eventf(ctx, ...)` on this span: a log record whose only
    /// field is [`TRACE_EVENT_KEY`].
    pub fn log_event(&self, event: &str) {
        self.log_fields(vec![(TRACE_EVENT_KEY.to_owned(), event.to_owned())]);
    }

    /// Go `Span.Finish`: hands the raw span to the tracer's recorder. Like
    /// Go's, a second finish records nothing.
    pub fn finish(&self) {
        self.finish_with(None);
    }

    /// Go `Span.FinishWithOptions(opentracing.FinishOptions{FinishTime:
    /// end})`.
    pub fn finish_at(&self, end: SystemTime) {
        self.finish_with(Some(end));
    }

    fn finish_with(&self, end: Option<SystemTime>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.finished {
            return;
        }
        state.finished = true;
        let duration = match end {
            Some(end) => end.duration_since(state.start).unwrap_or_default(),
            None => state.start_instant.elapsed(),
        };
        let raw = self.tracer.recorder.as_ref().map(|_| RawSpan {
            operation: self.operation.clone(),
            context: self.context,
            parent_span_id: self.parent_span_id,
            baggage: state.baggage.clone(),
            start: state.start,
            duration,
            tags: std::mem::take(&mut state.tags),
            logs: std::mem::take(&mut state.logs),
        });
        drop(state);
        if let (Some(recorder), Some(raw)) = (&self.tracer.recorder, raw) {
            recorder(raw);
        }
    }
}

/// Go's private `noopSpan`.
fn noop_span() -> Span {
    Arc::new(Tracer::noop()).start_span("DefaultSpan")
}

/// Go `NewRecordedTrace`: installs a recording tracer globally and returns its
/// root span, tagged with the TiDB trace baggage.
pub fn new_recorded_trace(
    operation: &str,
    callback: impl Fn(RawSpan) + Send + Sync + 'static,
) -> Span {
    let tracer = Arc::new(Tracer::new(Arc::new(callback)));
    set_global_tracer(Arc::clone(&tracer));
    let span = tracer.start_span(operation);
    span.set_baggage_item(TIDB_TRACE, "1");
    span
}

/// The values Go propagates through `context.Context` for tracing.
///
/// Cloning shares the sink; the span and trace info are per-context, so a
/// child context can carry a child span without disturbing its parent — the
/// same shape as Go's `context.WithValue` chain.
#[derive(Clone, Default)]
pub struct TraceContext {
    span: Option<Span>,
    sink: Option<Arc<dyn Sink>>,
    trace_info: Option<Arc<TraceInfo>>,
    trace_id: Vec<u8>,
}

impl std::fmt::Debug for TraceContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TraceContext")
            .field("span", &self.span)
            .field("has_sink", &self.sink.is_some())
            .field("trace_info", &self.trace_info)
            .field("trace_id", &self.trace_id)
            .finish()
    }
}

impl TraceContext {
    /// An empty context, like Go's `context.Background()`.
    pub fn background() -> Self {
        Self::default()
    }

    /// Go `opentracing.ContextWithSpan`.
    pub fn with_span(&self, span: Span) -> Self {
        let mut next = self.clone();
        next.span = Some(span);
        next
    }

    /// Go `opentracing.SpanFromContext`: the stored span, if any.
    pub fn span(&self) -> Option<&Span> {
        self.span.as_ref()
    }

    /// Go `WithFlightRecorder`.
    pub fn with_flight_recorder(&self, sink: Arc<dyn Sink>) -> Self {
        let mut next = self.clone();
        next.sink = Some(sink);
        next
    }

    /// Go `GetSink`.
    pub fn sink(&self) -> Option<&Arc<dyn Sink>> {
        self.sink.as_ref()
    }

    /// Go `ContextWithTraceInfo`. A `None` info returns the context unchanged,
    /// exactly as Go returns the original context for a nil info.
    pub fn with_trace_info(&self, info: Option<Arc<TraceInfo>>) -> Self {
        match info {
            None => self.clone(),
            Some(info) => {
                let mut next = self.clone();
                next.trace_info = Some(info);
                next
            }
        }
    }

    /// Go `TraceInfoFromContext`.
    pub fn trace_info(&self) -> Option<&TraceInfo> {
        self.trace_info.as_deref()
    }

    /// Go `ExtractTraceID`.
    pub fn extract_trace_id(&self) -> &[u8] {
        &self.trace_id
    }

    /// Returns a copy of this context carrying `trace_id`.
    pub fn with_trace_id(&self, trace_id: &[u8]) -> Self {
        let mut next = self.clone();
        next.trace_id = trace_id.to_vec();
        next
    }
}

/// Go `ExtractTraceID`.
pub fn extract_trace_id(context: &TraceContext) -> &[u8] {
    context.extract_trace_id()
}

/// Go `SpanFromContext`: the context's span, or a no-op span.
pub fn span_from_context(context: &TraceContext) -> Span {
    context.span().cloned().unwrap_or_else(noop_span)
}

/// Go `ChildSpanFromContxt` (Go's spelling): a child of the context's span
/// plus the context carrying it. A missing or no-op parent yields a no-op
/// span and the original context.
pub fn child_span_from_context(context: &TraceContext, operation: &str) -> (Span, TraceContext) {
    if let Some(parent) = context.span() {
        if !parent.is_noop() {
            let child = global_tracer().start_span_child_of(operation, parent.context());
            let child_context = context.with_span(child.clone());
            return (child, child_context);
        }
    }
    (noop_span(), context.clone())
}

/// Go `StartRegionWithNewRootSpan`: starts a root span on the global tracer
/// and stores it in the returned context.
pub fn start_region_with_new_root_span(
    context: &TraceContext,
    region_type: &str,
) -> (Region, TraceContext) {
    let span = global_tracer().start_span(region_type);
    let region = Region {
        span: Some(span.clone()),
        recorded: None,
    };
    (region, context.with_span(span))
}

/// Go `StartRegion`: opens a traced region, emitting a `General` begin event
/// to the context's sink when that category is enabled.
pub fn start_region(context: &TraceContext, region_type: &str) -> Region {
    let span = context.span().map(|parent| {
        parent
            .tracer()
            .start_span_child_of(region_type, parent.context())
    });

    let mut region = Region {
        span,
        recorded: None,
    };
    if is_enabled(TraceCategory::GENERAL) {
        if let Some(sink) = context.sink() {
            let event = Event {
                timestamp: SystemTime::now(),
                name: region_type.to_owned(),
                phase: Phase::Begin,
                trace_id: extract_trace_id(context).to_vec(),
                fields: Vec::new(),
                category: TraceCategory::GENERAL,
            };
            sink.record(context, &event);
            region.recorded = Some((context.clone(), event, Arc::clone(sink)));
        }
    }
    region
}

/// Go `StartRegionEx`: [`start_region`] plus the context carrying its span.
pub fn start_region_ex(context: &TraceContext, region_type: &str) -> (Region, TraceContext) {
    let region = start_region(context, region_type);
    let context = match region.span.clone() {
        Some(span) => context.with_span(span),
        None => context.clone(),
    };
    (region, context)
}

/// Go `Region`: a code region whose execution interval is traced.
///
/// Go relies on `defer r.End()`; Rust ends the region explicitly through
/// [`Region::end`], and dropping one without ending it records nothing —
/// matching Go's behavior when `End` is never reached.
#[derive(Debug)]
pub struct Region {
    span: Option<Span>,
    recorded: Option<(TraceContext, Event, Arc<dyn Sink>)>,
}

impl std::fmt::Debug for dyn Sink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Sink")
    }
}

impl Region {
    /// Go `Region.End`: finishes the span and records the matching end event.
    pub fn end(mut self) {
        if let Some(span) = &mut self.span {
            span.finish();
        }
        if let Some((context, event, sink)) = &mut self.recorded {
            event.phase = Phase::End;
            event.timestamp = SystemTime::now();
            sink.record(context, event);
        }
    }
}

// Go threads the span through every call as `ctx`; this tier keeps the span
// of the code a thread is running instead. A thread with no span (every
// statement but a traced one) pays one thread-local read per region.
thread_local! {
    static THREAD_SPAN: RefCell<Option<Span>> = const { RefCell::new(None) };
}

/// Go `opentracing.SpanFromContext(ctx)` for the code this thread runs:
/// the span a region opened here becomes a child of, `None` when untraced.
pub fn current_span() -> Option<Span> {
    THREAD_SPAN.with(|current| current.borrow().clone())
}

/// Go `opentracing.ContextWithSpan(ctx, span)` for the code this thread
/// runs until the guard drops; work handed to another thread carries the
/// span there with its own guard, as Go's goroutines inherit `ctx`.
pub fn with_thread_span(span: Option<Span>) -> ThreadSpanGuard {
    let previous = THREAD_SPAN.with(|current| current.replace(span));
    ThreadSpanGuard {
        previous,
        open_regions: None,
    }
}

/// [`with_thread_span`] for a whole traced run (Go `TraceExec`'s
/// `ContextWithSpan`): when the guard drops, every region
/// [`begin_thread_region`] opened under it that is still open ends too, as
/// Go's deferred `End`s have all run once the traced call returns.
pub fn enter_thread_trace(span: Span) -> ThreadSpanGuard {
    let open_regions = OPEN_THREAD_REGIONS.with(|open| open.borrow().len());
    let mut guard = with_thread_span(Some(span));
    guard.open_regions = Some(open_regions);
    guard
}

/// Restores the thread's previous span.
#[must_use = "the span is removed when the guard drops"]
pub struct ThreadSpanGuard {
    previous: Option<Span>,
    /// For [`enter_thread_trace`]: how many regions were open when it began.
    open_regions: Option<usize>,
}

impl Drop for ThreadSpanGuard {
    fn drop(&mut self) {
        if let Some(open_regions) = self.open_regions {
            let left_open = OPEN_THREAD_REGIONS.with(|open| {
                let mut open = open.borrow_mut();
                let keep = open_regions.min(open.len());
                open.split_off(keep)
            });
            for region in left_open.into_iter().rev() {
                region.span.finish();
            }
        }
        let previous = self.previous.take();
        THREAD_SPAN.with(|current| *current.borrow_mut() = previous);
    }
}

/// A region opened on the thread's span: Go's
/// `defer tracing.StartRegion(ctx, name).End()`, whose code keeps the
/// caller's ctx, or `tracing.StartRegionEx`, whose code runs under the
/// region's span. The region ends when the value drops; an ex-region gives
/// the thread its caller's span back then.
#[must_use = "the region ends when it drops"]
pub struct ThreadRegion {
    span: Option<Span>,
    entered: Option<ThreadSpanGuard>,
}

impl ThreadRegion {
    /// The region's span, `None` when the thread is untraced.
    pub fn span(&self) -> Option<&Span> {
        self.span.as_ref()
    }

    /// Go `Region.End`.
    pub fn end(self) {}
}

impl Drop for ThreadRegion {
    fn drop(&mut self) {
        drop(self.entered.take());
        if let Some(span) = self.span.take() {
            span.finish();
        }
    }
}

fn start_thread_region_with(region_type: &str, enter: bool) -> ThreadRegion {
    let span = THREAD_SPAN.with(|current| {
        current.borrow().as_ref().map(|parent| {
            parent
                .tracer()
                .start_span_child_of(region_type, parent.context())
        })
    });
    let entered = match (&span, enter) {
        (Some(span), true) => Some(with_thread_span(Some(span.clone()))),
        _ => None,
    };
    ThreadRegion { span, entered }
}

/// Go `tracing.StartRegion(ctx, regionType)` on the thread's span: the
/// region's own code keeps the caller's span.
pub fn start_thread_region(region_type: &str) -> ThreadRegion {
    start_thread_region_with(region_type, false)
}

/// Go `tracing.StartRegionEx(ctx, regionType)` on the thread's span: the
/// code that runs while the region is open runs under its span.
pub fn start_thread_region_ex(region_type: &str) -> ThreadRegion {
    start_thread_region_with(region_type, true)
}

// Regions this tier opens in one frame and ends in another (Go opens them
// with `tracing.StartRegionEx` and ends them with a deferred `End` in the
// same function, but here the function's work is split across layers):
// the open ones, innermost last, each with the span the thread had before.
thread_local! {
    static OPEN_THREAD_REGIONS: RefCell<Vec<OpenThreadRegion>> = const { RefCell::new(Vec::new()) };
}

struct OpenThreadRegion {
    name: &'static str,
    span: Span,
    previous: Option<Span>,
    note: Option<String>,
}

/// Go `r, ctx := tracing.StartRegionEx(ctx, name)` whose `defer r.End()`
/// runs in another frame: the thread runs under the region's span until
/// [`end_thread_region`] ends it. `note` is handed back by that call (the
/// statement text Go's `runStmt` logs, for instance). Returns the region's
/// span, `None` when the thread is untraced.
pub fn begin_thread_region(name: &'static str, note: Option<String>) -> Option<Span> {
    let parent = current_span()?;
    let span = parent.tracer().start_span_child_of(name, parent.context());
    let previous = THREAD_SPAN.with(|current| current.replace(Some(span.clone())));
    OPEN_THREAD_REGIONS.with(|open| {
        open.borrow_mut().push(OpenThreadRegion {
            name,
            span: span.clone(),
            previous,
            note,
        });
    });
    Some(span)
}

/// Go `Region.End` for the innermost region [`begin_thread_region`] opened
/// as `name`, and, as Go's deferred `End`s run, for every region opened
/// after it that is still open; the thread gets back the span it had before
/// the region began. Returns the region's note, `None` when no region of
/// that name is open.
pub fn end_thread_region(name: &'static str) -> Option<Option<String>> {
    let ended = OPEN_THREAD_REGIONS.with(|open| {
        let mut open = open.borrow_mut();
        let position = open.iter().rposition(|region| region.name == name)?;
        Some(open.split_off(position))
    })?;
    let mut ended = ended.into_iter().rev();
    let mut outermost = None;
    for region in ended.by_ref() {
        region.span.finish();
        outermost = Some(region);
    }
    let outermost = outermost?;
    THREAD_SPAN.with(|current| *current.borrow_mut() = outermost.previous);
    Some(outermost.note)
}

/// [`begin_thread_region`] whose [`end_thread_region`] runs when the value
/// drops, on every return path of the frame that opened it.
pub fn begin_thread_region_scope(name: &'static str) -> ThreadRegionScope {
    let opened = begin_thread_region(name, None).is_some();
    ThreadRegionScope { name, opened }
}

/// Ends its region when it drops; see [`begin_thread_region_scope`].
#[must_use = "the region ends when it drops"]
pub struct ThreadRegionScope {
    name: &'static str,
    opened: bool,
}

impl Drop for ThreadRegionScope {
    fn drop(&mut self) {
        if self.opened {
            end_thread_region(self.name);
        }
    }
}

/// Whether a region [`begin_thread_region`] opened as `name` is still open
/// on this thread.
pub fn thread_region_open(name: &'static str) -> bool {
    OPEN_THREAD_REGIONS.with(|open| open.borrow().iter().any(|region| region.name == name))
}

/// Go `logutil.Eventf(ctx, ...)` on the thread's span; `event` is formatted
/// only when the thread is traced.
pub fn thread_event(event: impl FnOnce() -> String) {
    THREAD_SPAN.with(|current| {
        if let Some(span) = current.borrow().as_ref() {
            span.log_event(&event());
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    // The global tracer and the enabled-category mask are process-wide. Go
    // runs a package's tests sequentially; Rust runs them in parallel, so the
    // tests that install a tracer or edit the mask take this lock.
    static GLOBAL_STATE: Mutex<()> = Mutex::new(());

    fn lock_global_state() -> std::sync::MutexGuard<'static, ()> {
        GLOBAL_STATE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn recorder() -> (Arc<Mutex<Vec<RawSpan>>>, impl Fn(RawSpan) + Send + Sync) {
        let collected = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&collected);
        (collected, move |span| sink.lock().unwrap().push(span))
    }

    // Go `TestSpanFromContext`.
    #[test]
    fn span_from_context_is_noop_without_a_span() {
        let _guard = lock_global_state();
        let context = TraceContext::background();
        assert!(span_from_context(&context).is_noop());

        let (collected, callback) = recorder();
        let span = new_recorded_trace("test", callback);
        span.finish();
        let context = context.with_span(span);
        span_from_context(&context).finish();

        assert_eq!(collected.lock().unwrap()[0].operation, "test");
    }

    // Go `TestChildSpanFromContext`.
    #[test]
    fn child_span_from_context_records_the_child() {
        let _guard = lock_global_state();
        let context = TraceContext::background();
        let (noop, _) = child_span_from_context(&context, "");
        assert!(noop.is_noop());

        let (collected, callback) = recorder();
        let span = new_recorded_trace("test", callback);
        span.finish();
        let context = context.with_span(span);
        let (child, _) = child_span_from_context(&context, "test_child");
        child.finish();

        let spans = collected.lock().unwrap();
        assert_eq!(spans[1].operation, "test_child");
    }

    // Go `TestFollowFrom`: only a root span has a zero parent.
    #[test]
    fn follows_from_span_keeps_its_parent() {
        let _guard = lock_global_state();
        let (collected, callback) = recorder();
        let root = new_recorded_trace("test", callback);
        let follower = root
            .tracer()
            .start_span_following("follow_from", root.context());
        root.finish();
        follower.finish();

        let spans = collected.lock().unwrap();
        assert_eq!(spans[1].operation, "follow_from");
        assert_ne!(spans[1].parent_span_id, 0);
        assert_eq!(spans[0].parent_span_id, 0);
    }

    // Go `TestCreateSapnBeforeSetupGlobalTracer`: a span started before the
    // recording tracer is installed is dropped.
    #[test]
    fn spans_started_before_the_global_tracer_are_dropped() {
        let _guard = lock_global_state();
        global_tracer().start_span("before").finish();

        let (collected, callback) = recorder();
        new_recorded_trace("test", callback).finish();

        assert_eq!(collected.lock().unwrap().len(), 1);
    }

    // Go `TestTreeRelationship`.
    #[test]
    fn nested_contexts_build_a_span_tree() {
        let _guard = lock_global_state();
        let (collected, callback) = recorder();
        let root = new_recorded_trace("test", callback);
        let context = TraceContext::background().with_span(root.clone());

        let (parent, parent_context) = child_span_from_context(&context, "parent");
        let (child, _) = child_span_from_context(&parent_context, "child");

        root.finish();
        parent.finish();
        child.finish();

        let spans = collected.lock().unwrap();
        assert_eq!(spans[0].operation, "test");
        assert_eq!(spans[1].operation, "parent");
        assert_eq!(spans[2].operation, "child");
        assert_eq!(spans[0].context.span_id, spans[1].parent_span_id);
        assert_eq!(spans[1].context.span_id, spans[2].parent_span_id);
    }

    // An opentracing span is a shared handle: finishing any clone finishes
    // the underlying span once.
    #[test]
    fn cloned_span_records_only_once() {
        let _guard = lock_global_state();
        let (collected, callback) = recorder();
        let span = new_recorded_trace("test", callback);
        let clone = span.clone();

        span.finish();
        clone.finish();

        assert_eq!(collected.lock().unwrap().len(), 1);
    }

    // An untraced thread opens nothing and pays no recording.
    #[test]
    fn thread_regions_are_inert_without_a_span() {
        assert!(current_span().is_none());
        assert!(start_thread_region_ex("a").span().is_none());
        assert!(begin_thread_region("b", None).is_none());
        assert!(end_thread_region("b").is_none());
        assert!(current_span().is_none());
    }

    // Go's ctx nesting on one thread: an ex-region's code runs under its
    // span; a region begun in one frame and ended in another ends, as Go's
    // deferred `End`s run, together with every region begun after it, and
    // the thread gets its caller's span back.
    #[test]
    fn thread_regions_nest_as_gos_ctx_does() {
        let spans = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&spans);
        let tracer = Arc::new(Tracer::new(Arc::new(move |raw| {
            sink.lock().unwrap().push(raw);
        })));
        let root = tracer.start_span("trace");
        let entered = with_thread_span(Some(root.clone()));
        let outer = begin_thread_region("outer", Some("note".to_owned())).unwrap();
        start_thread_region("sibling").end();
        let inner = begin_thread_region("inner", None).unwrap();
        {
            let next = start_thread_region_ex("next");
            thread_event(|| "inside".to_owned());
            assert_eq!(
                current_span().unwrap().context(),
                next.span().unwrap().context()
            );
        }
        assert_eq!(current_span().unwrap().context(), inner.context());
        assert_eq!(end_thread_region("outer"), Some(Some("note".to_owned())));
        assert!(!thread_region_open("inner"));
        assert_eq!(current_span().unwrap().context(), root.context());
        drop(entered);
        assert!(current_span().is_none());
        root.finish();

        let spans = spans.lock().unwrap();
        let by_name = |name: &str| spans.iter().find(|span| span.operation == name).unwrap();
        assert_eq!(
            spans
                .iter()
                .map(|span| span.operation.as_str())
                .collect::<Vec<_>>(),
            ["sibling", "next", "inner", "outer", "trace"]
        );
        assert_eq!(by_name("outer").parent_span_id, root.context().span_id);
        assert_eq!(by_name("sibling").parent_span_id, outer.context().span_id);
        assert_eq!(by_name("inner").parent_span_id, outer.context().span_id);
        assert_eq!(by_name("next").parent_span_id, inner.context().span_id);
        assert_eq!(
            by_name("next").logs[0].fields,
            [(TRACE_EVENT_KEY.to_owned(), "inside".to_owned())]
        );
    }

    // A traced run's guard ends the regions its run left open, so none
    // outlives the trace on the thread.
    #[test]
    fn a_trace_ends_the_regions_its_run_left_open() {
        let spans = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&spans);
        let tracer = Arc::new(Tracer::new(Arc::new(move |raw| {
            sink.lock().unwrap().push(raw);
        })));
        let root = tracer.start_span("trace");
        let entered = enter_thread_trace(root);
        begin_thread_region("session.runStmt", None).unwrap();
        drop(entered);
        assert!(!thread_region_open("session.runStmt"));
        assert!(current_span().is_none());
        assert_eq!(spans.lock().unwrap()[0].operation, "session.runStmt");
    }

    // Go `opentracing.StartTime` / `FinishOptions.FinishTime`: a span whose
    // bounds were measured elsewhere keeps them.
    #[test]
    fn explicit_start_and_finish_times_are_kept() {
        let spans = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&spans);
        let tracer = Arc::new(Tracer::new(Arc::new(move |raw| {
            sink.lock().unwrap().push(raw);
        })));
        let root = tracer.start_span("trace");
        let start = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let child = tracer.start_span_child_of_at("tikv.RPC", root.context(), start);
        child.finish_at(start + Duration::from_micros(7));
        let spans = spans.lock().unwrap();
        assert_eq!(spans[0].start, start);
        assert_eq!(spans[0].duration, Duration::from_micros(7));
    }

    // Go `TestTraceInfoFromContext`.
    #[test]
    fn trace_info_round_trips_through_the_context() {
        let context = TraceContext::background();
        assert!(context.trace_info().is_none());
        // A nil info leaves the context untouched.
        assert!(context.with_trace_info(None).trace_info().is_none());

        let context = context.with_trace_info(Some(Arc::new(TraceInfo {
            connection_id: 12345,
            session_alias: "alias1".to_owned(),
            trace_id: Vec::new(),
        })));
        let info = context.trace_info().unwrap();
        assert_eq!(info.connection_id, 12345);
        assert_eq!(info.session_alias, "alias1");
    }
}
