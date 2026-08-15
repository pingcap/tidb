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

//! Complete transcreation of Go `pkg/util/tracing` (`util.go`,
//! `opt_trace.go`): TiDB's trace-category mask, trace events and sinks, the
//! traced-region API, and the span tree TiDB builds over its tracer.
//!
//! Two Go-side mechanisms have no Rust counterpart and map onto explicit
//! values instead of ambient state:
//!
//! * `context.Context` propagation. Go stashes the active span, the flight
//!   recorder, and the SQL `TraceInfo` in the context; this workspace threads
//!   values explicitly, so all three live in [`TraceContext`], which callers
//!   pass down the same call chains Go passes a context down.
//! * `opentracing`/`basictracer`. Go delegates span identity and recording to
//!   those libraries. What TiDB actually observes is the span *tree* — a span
//!   taken from a context is a child of the context's span, a context without
//!   one yields a no-op span, and finished spans reach the recorder callback
//!   with their operation name and parent — so [`Tracer`] owns that model
//!   directly rather than depending on a Rust tracing framework whose span
//!   identity TiDB would not control.
//!
//! Go's `runtime/trace` regions inside [`Region`] are a Go runtime profiling
//! detail with no observable SQL behavior and are deliberately absent. So is
//! `ExtractTraceID`, which forwards to client-go's context key: the trace ID
//! is carried by [`TraceContext::trace_id`] here.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::SystemTime;

use tidb_log::Field;

use crate::intest::IN_TEST;

/// Go `TiDBTrace`, the baggage key marking a TiDB trace.
pub const TIDB_TRACE: &str = "tr";

/// Go `TraceCategory`: a bitmask selecting which trace events are emitted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
    #[must_use]
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
    #[must_use]
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

    /// Whether every bit of `other` is set here.
    #[must_use]
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 != 0
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
#[must_use]
pub fn enabled_categories() -> TraceCategory {
    TraceCategory(ENABLED_CATEGORIES.load(Ordering::SeqCst))
}

/// Go `IsEnabled`. Trace events only work for the next-generation kernel, so
/// a classic build outside tests is always disabled.
#[must_use]
pub fn is_enabled(category: TraceCategory) -> bool {
    if tidb_config::kerneltype::is_classic() && !IN_TEST {
        return false;
    }
    enabled_categories().contains(category)
}

/// Go `Phase`: an event's position in its interval.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
}

impl Phase {
    /// The single-letter wire spelling Go stores in the `Phase` string.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Begin => "B",
            Self::End => "E",
            Self::AsyncBegin => "b",
            Self::AsyncEnd => "e",
            Self::FlowBegin => "s",
            Self::FlowEnd => "f",
            Self::Instant => "i",
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

impl Event {
    /// A field-less event at the current time.
    #[must_use]
    pub fn new(category: TraceCategory, name: &str, phase: Phase, trace_id: &[u8]) -> Self {
        Self {
            timestamp: SystemTime::now(),
            name: name.to_owned(),
            phase,
            trace_id: trace_id.to_vec(),
            fields: Vec::new(),
            category,
        }
    }
}

/// Go `Sink`: the destination trace events are recorded to.
pub trait Sink: Send + Sync {
    /// Go `Sink.Record`.
    fn record(&self, event: &Event);
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
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct CeTraceRecord {
    /// Table the expression was estimated against.
    pub table_name: String,
    /// Estimation type.
    pub kind: String,
    /// Restored expression text.
    pub expr: String,
    /// Table ID; excluded from the JSON form by Go's `json:"-"` tag.
    pub table_id: i64,
    /// Estimated row count.
    pub row_count: u64,
}

/// Go `DedupCETrace`: keeps the first occurrence of each distinct record,
/// preserving input order.
#[must_use]
pub fn dedup_ce_trace(records: &[CeTraceRecord]) -> Vec<CeTraceRecord> {
    let mut seen = std::collections::HashSet::with_capacity(records.len());
    let mut deduped = Vec::with_capacity(records.len());
    for record in records {
        if seen.insert(record.clone()) {
            deduped.push(record.clone());
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
}

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
    #[must_use]
    pub fn new(recorder: CallbackRecorder) -> Self {
        Self {
            recorder: Some(recorder),
            next_id: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Go `opentracing.NoopTracer{}`.
    #[must_use]
    pub fn noop() -> Self {
        Self {
            recorder: None,
            next_id: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Whether this tracer discards every span (Go's no-op tracer check).
    #[must_use]
    pub fn is_noop(&self) -> bool {
        self.recorder.is_none()
    }

    fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Go `Tracer.StartSpan(operation)`: a root span.
    #[must_use]
    pub fn start_span(self: &Arc<Self>, operation: &str) -> Span {
        self.start_child(operation, None)
    }

    /// Go `Tracer.StartSpan(operation, opentracing.ChildOf(parent))`.
    #[must_use]
    pub fn start_span_child_of(self: &Arc<Self>, operation: &str, parent: SpanContext) -> Span {
        self.start_child(operation, Some(parent))
    }

    /// Go `Tracer.StartSpan(operation, opentracing.FollowsFrom(parent))`.
    ///
    /// TiDB never inspects the reference kind, only the resulting parentage,
    /// so this shares `ChildOf`'s model.
    #[must_use]
    pub fn start_span_following(self: &Arc<Self>, operation: &str, parent: SpanContext) -> Span {
        self.start_child(operation, Some(parent))
    }

    fn start_child(self: &Arc<Self>, operation: &str, parent: Option<SpanContext>) -> Span {
        let span_id = self.next_id();
        Span {
            tracer: Arc::clone(self),
            context: SpanContext {
                trace_id: parent.map_or(span_id, |parent| parent.trace_id),
                span_id,
            },
            parent_span_id: parent.map_or(0, |parent| parent.span_id),
            operation: operation.to_owned(),
            baggage: std::collections::BTreeMap::new(),
            finished: false,
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
#[must_use]
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
    baggage: std::collections::BTreeMap<String, String>,
    finished: bool,
}

impl Span {
    /// Go `Span.Context`.
    #[must_use]
    pub const fn context(&self) -> SpanContext {
        self.context
    }

    /// Go `Span.Tracer`.
    #[must_use]
    pub fn tracer(&self) -> &Arc<Tracer> {
        &self.tracer
    }

    /// Whether this span discards its recording (Go's no-op tracer check).
    #[must_use]
    pub fn is_noop(&self) -> bool {
        self.tracer.is_noop()
    }

    /// Go `Span.SetBaggageItem`.
    pub fn set_baggage_item(&mut self, key: &str, value: &str) {
        self.baggage.insert(key.to_owned(), value.to_owned());
    }

    /// Go `Span.BaggageItem`.
    #[must_use]
    pub fn baggage_item(&self, key: &str) -> Option<&str> {
        self.baggage.get(key).map(String::as_str)
    }

    /// Go `Span.Finish`: hands the raw span to the tracer's recorder. Like
    /// Go's, a second finish records nothing.
    pub fn finish(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        if let Some(recorder) = &self.tracer.recorder {
            recorder(RawSpan {
                operation: self.operation.clone(),
                context: self.context,
                parent_span_id: self.parent_span_id,
                baggage: self.baggage.clone(),
            });
        }
    }
}

/// Go's private `noopSpan`.
#[must_use]
pub fn noop_span() -> Span {
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
    let mut span = tracer.start_span(operation);
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
    /// The statement's trace identifier, Go's `ExtractTraceID(ctx)`.
    pub trace_id: Vec<u8>,
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
    #[must_use]
    pub fn background() -> Self {
        Self::default()
    }

    /// Go `opentracing.ContextWithSpan`.
    #[must_use]
    pub fn with_span(&self, span: Span) -> Self {
        let mut next = self.clone();
        next.span = Some(span);
        next
    }

    /// Go `opentracing.SpanFromContext`: the stored span, if any.
    #[must_use]
    pub fn span(&self) -> Option<&Span> {
        self.span.as_ref()
    }

    /// Go `WithFlightRecorder`.
    #[must_use]
    pub fn with_flight_recorder(&self, sink: Arc<dyn Sink>) -> Self {
        let mut next = self.clone();
        next.sink = Some(sink);
        next
    }

    /// Go `GetSink`.
    #[must_use]
    pub fn sink(&self) -> Option<&Arc<dyn Sink>> {
        self.sink.as_ref()
    }

    /// Go `ContextWithTraceInfo`. A `None` info returns the context unchanged,
    /// exactly as Go returns the original context for a nil info.
    #[must_use]
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
    #[must_use]
    pub fn trace_info(&self) -> Option<&TraceInfo> {
        self.trace_info.as_deref()
    }

    /// Go `ExtractTraceID`.
    #[must_use]
    pub fn extract_trace_id(&self) -> &[u8] {
        &self.trace_id
    }
}

/// Go `SpanFromContext`: the context's span, or a no-op span.
#[must_use]
pub fn span_from_context(context: &TraceContext) -> Span {
    context.span().cloned().unwrap_or_else(noop_span)
}

/// Go `ChildSpanFromContxt` (Go's spelling): a child of the context's span
/// plus the context carrying it. A missing or no-op parent yields a no-op
/// span and the original context.
#[must_use]
pub fn child_span_from_context(context: &TraceContext, operation: &str) -> (Span, TraceContext) {
    if let Some(parent) = context.span() {
        if !parent.is_noop() {
            let child = parent
                .tracer()
                .start_span_child_of(operation, parent.context());
            let child_context = context.with_span(child.clone());
            return (child, child_context);
        }
    }
    (noop_span(), context.clone())
}

/// Go `StartRegionWithNewRootSpan`: starts a root span on the global tracer
/// and stores it in the returned context.
#[must_use]
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
#[must_use]
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
            let event = Event::new(
                TraceCategory::GENERAL,
                region_type,
                Phase::Begin,
                context.extract_trace_id(),
            );
            sink.record(&event);
            region.recorded = Some((event, Arc::clone(sink)));
        }
    }
    region
}

/// Go `StartRegionEx`: [`start_region`] plus the context carrying its span.
#[must_use]
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
    recorded: Option<(Event, Arc<dyn Sink>)>,
}

impl std::fmt::Debug for dyn Sink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Sink")
    }
}

impl Region {
    /// The region's span, when one was started.
    #[must_use]
    pub fn span(&self) -> Option<&Span> {
        self.span.as_ref()
    }

    /// Go `Region.End`: finishes the span and records the matching end event.
    pub fn end(mut self) {
        if let Some(span) = &mut self.span {
            span.finish();
        }
        if let Some((event, sink)) = &mut self.recorded {
            event.phase = Phase::End;
            event.timestamp = SystemTime::now();
            sink.record(event);
        }
    }
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
        let mut span = new_recorded_trace("test", callback);
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
        let mut span = new_recorded_trace("test", callback);
        span.finish();
        let context = context.with_span(span);
        let (mut child, _) = child_span_from_context(&context, "test_child");
        child.finish();

        let spans = collected.lock().unwrap();
        assert_eq!(spans[1].operation, "test_child");
    }

    // Go `TestFollowFrom`: only a root span has a zero parent.
    #[test]
    fn follows_from_span_keeps_its_parent() {
        let _guard = lock_global_state();
        let (collected, callback) = recorder();
        let mut root = new_recorded_trace("test", callback);
        let mut follower = root
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
        let mut root = new_recorded_trace("test", callback);
        let context = TraceContext::background().with_span(root.clone());

        let (mut parent, parent_context) = child_span_from_context(&context, "parent");
        let (mut child, _) = child_span_from_context(&parent_context, "child");

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

    // `TraceCategory`'s naming and parsing round trip over every category.
    #[test]
    fn categories_round_trip_through_their_names() {
        let categories = [
            (TraceCategory::TXN_LIFECYCLE, "txn_lifecycle"),
            (TraceCategory::TXN_2PC, "txn_2pc"),
            (TraceCategory::TXN_LOCK_RESOLVE, "txn_lock_resolve"),
            (TraceCategory::STMT_LIFECYCLE, "stmt_lifecycle"),
            (TraceCategory::STMT_PLAN, "stmt_plan"),
            (TraceCategory::KV_REQUEST, "kv_request"),
            (TraceCategory::UNKNOWN_CLIENT, "unknown_client"),
            (TraceCategory::GENERAL, "general"),
            (TraceCategory::DDL_JOB, "ddl_job"),
            (TraceCategory::DEV_DEBUG, "dev_debug"),
            (TraceCategory::TIKV_REQUEST, "tikv_request"),
            (TraceCategory::TIKV_WRITE_DETAILS, "tikv_write_details"),
            (TraceCategory::TIKV_READ_DETAILS, "tikv_read_details"),
            (TraceCategory::REGION_CACHE, "region_cache"),
        ];
        for (category, name) in categories {
            assert_eq!(category.name(), name);
            assert_eq!(TraceCategory::parse(name), category);
            assert!(TraceCategory::ALL.contains(category));
        }
        // Go's fallback spelling, and an unparsable name is the zero category.
        assert_eq!(TraceCategory(1 << 20).name(), "unknown(1048576)");
        assert_eq!(TraceCategory::parse("nope"), TraceCategory(0));
        assert_eq!(TraceCategory::ALL.0, (1 << 14) - 1);
    }

    // Phase spellings are the wire values Go stores.
    #[test]
    fn phases_use_the_source_letters() {
        for (phase, letter) in [
            (Phase::Begin, "B"),
            (Phase::End, "E"),
            (Phase::AsyncBegin, "b"),
            (Phase::AsyncEnd, "e"),
            (Phase::FlowBegin, "s"),
            (Phase::FlowEnd, "f"),
            (Phase::Instant, "i"),
        ] {
            assert_eq!(phase.as_str(), letter);
        }
    }

    // Go `DedupCETrace` keeps the first of each distinct record, in order.
    #[test]
    fn ce_trace_records_dedup_in_order() {
        let record = |table: &str, count: u64| CeTraceRecord {
            table_name: table.to_owned(),
            kind: "range".to_owned(),
            expr: "a > 1".to_owned(),
            table_id: 1,
            row_count: count,
        };
        let records = vec![record("t1", 10), record("t2", 20), record("t1", 10)];
        let deduped = dedup_ce_trace(&records);
        assert_eq!(deduped, vec![record("t1", 10), record("t2", 20)]);
        assert!(dedup_ce_trace(&[]).is_empty());
    }

    struct CollectingSink(Mutex<Vec<(String, Phase)>>);

    impl Sink for CollectingSink {
        fn record(&self, event: &Event) {
            self.0
                .lock()
                .unwrap()
                .push((event.name.clone(), event.phase));
        }
    }

    // A region emits the Begin/End pair only while `General` is enabled.
    #[test]
    fn regions_record_begin_and_end_events() {
        let _guard = lock_global_state();
        let sink = Arc::new(CollectingSink(Mutex::new(Vec::new())));
        let context = TraceContext::background().with_flight_recorder(sink.clone());

        let previous = enabled_categories();
        set_categories(TraceCategory::GENERAL);
        start_region(&context, "traced").end();
        assert_eq!(
            *sink.0.lock().unwrap(),
            vec![
                ("traced".to_owned(), Phase::Begin),
                ("traced".to_owned(), Phase::End)
            ]
        );

        // Disabled categories record nothing.
        sink.0.lock().unwrap().clear();
        set_categories(TraceCategory(0));
        start_region(&context, "untraced").end();
        assert!(sink.0.lock().unwrap().is_empty());
        set_categories(previous);
    }

    // Enable/Disable are additive and subtractive over the shared mask.
    #[test]
    fn enable_and_disable_edit_the_shared_mask() {
        let _guard = lock_global_state();
        let previous = enabled_categories();
        set_categories(TraceCategory(0));

        enable(TraceCategory::DDL_JOB | TraceCategory::STMT_PLAN);
        assert!(is_enabled(TraceCategory::DDL_JOB));
        assert!(is_enabled(TraceCategory::STMT_PLAN));
        assert!(!is_enabled(TraceCategory::KV_REQUEST));

        disable(TraceCategory::DDL_JOB);
        assert!(!is_enabled(TraceCategory::DDL_JOB));
        assert!(is_enabled(TraceCategory::STMT_PLAN));

        set_categories(TraceCategory::ALL);
        assert!(is_enabled(TraceCategory::KV_REQUEST));
        set_categories(previous);
    }
}
