// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Client-side tracing hooks and TiKV trace-control flags.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::ops::{BitOr, BitOrAssign};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use lazy_static::lazy_static;

use crate::proto::kvrpcpb;

/// One source `spanInfo` node reconstructed from TiKV `ExecDetailsV2`.
///
/// `duration` is the duration explicitly reported for this node. A zero value
/// is intentionally retained for formatting; timeline construction derives it
/// from synchronous children exactly as client-go's `calcDur` does.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionDetailSpan {
    pub name: &'static str,
    pub duration: Duration,
    pub asynchronous: bool,
    pub children: Vec<ExecutionDetailSpan>,
}

impl ExecutionDetailSpan {
    fn leaf(name: &'static str, nanos: u64) -> Self {
        Self {
            name,
            duration: Duration::from_nanos(nanos),
            asynchronous: false,
            children: Vec::new(),
        }
    }

    fn calculate_duration(&mut self) -> Duration {
        if self.duration.is_zero() {
            self.duration = self
                .children
                .iter_mut()
                .filter_map(|child| {
                    let duration = child.calculate_duration();
                    (!child.asynchronous).then_some(duration)
                })
                .sum();
        }
        self.duration
    }

    fn calculate_timeline_durations(&mut self) {
        self.calculate_duration();
        for child in &mut self.children {
            child.calculate_timeline_durations();
        }
    }

    fn append_timeline(
        &self,
        start_offset: Duration,
        output: &mut Vec<ExecutionDetailTiming>,
    ) -> Duration {
        if self.duration.is_zero() {
            return start_offset;
        }
        let mut child_offset = start_offset;
        for child in &self.children {
            child_offset = child.append_timeline(child_offset, output);
        }
        output.push(ExecutionDetailTiming {
            name: self.name,
            start_offset,
            duration: self.duration,
            asynchronous: self.asynchronous,
        });
        if self.asynchronous {
            start_offset
        } else {
            start_offset + self.duration
        }
    }

    /// Return child-before-parent span timings, matching the finish order of
    /// client-go's recursive `spanInfo.addTo` implementation.
    pub fn timeline(&self) -> Vec<ExecutionDetailTiming> {
        let mut normalized = self.clone();
        normalized.calculate_timeline_durations();
        let mut output = Vec::new();
        normalized.append_timeline(Duration::ZERO, &mut output);
        output
    }
}

impl fmt::Display for ExecutionDetailSpan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name)?;
        if self.asynchronous {
            formatter.write_str("'")?;
        }
        if !self.duration.is_zero() {
            write!(formatter, "[{}]", format_go_duration(self.duration))?;
        }
        if !self.children.is_empty() {
            formatter.write_str("{")?;
            for child in &self.children {
                write!(formatter, " {child}")?;
            }
            formatter.write_str(" }")?;
        }
        Ok(())
    }
}

/// Historical timing for one emitted execution-detail span.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionDetailTiming {
    pub name: &'static str,
    pub start_offset: Duration,
    pub duration: Duration,
    pub asynchronous: bool,
}

/// Per-task sink used by [`with_trace_exec_details`]. The first argument is
/// the physical RPC start instant; the tree supplies source-exact offsets.
pub type ExecutionDetailsTraceHandler = Arc<dyn Fn(Instant, &ExecutionDetailSpan) + Send + Sync>;

tokio::task_local! {
    static EXECUTION_DETAILS_TRACE_HANDLER: ExecutionDetailsTraceHandler;
}

/// Enable TiKV execution-detail tracing for one asynchronous operation.
///
/// This is the native async counterpart of client-go's
/// `ContextWithTraceExecDetails`: every physical RPC awaited by `future`
/// reports its reconstructed historical span tree to `handler`.
pub async fn with_trace_exec_details<F>(
    handler: ExecutionDetailsTraceHandler,
    future: F,
) -> F::Output
where
    F: Future,
{
    EXECUTION_DETAILS_TRACE_HANDLER.scope(handler, future).await
}

pub(crate) fn current_execution_details_trace_handler() -> Option<ExecutionDetailsTraceHandler> {
    EXECUTION_DETAILS_TRACE_HANDLER
        .try_with(ExecutionDetailsTraceHandler::clone)
        .ok()
}

/// Return whether the current asynchronous scope requests TiKV execution details.
pub fn trace_exec_details_enabled() -> bool {
    EXECUTION_DETAILS_TRACE_HANDLER.try_with(|_| ()).is_ok()
}

/// Build client-go's exact execution-detail span tree.
pub fn build_execution_detail_span(
    details: &kvrpcpb::ExecDetailsV2,
) -> Option<ExecutionDetailSpan> {
    let (rpc_duration, wait_duration, process_duration, suspend_duration, has_v2) =
        if let Some(time) = details.time_detail_v2.as_ref() {
            (
                time.total_rpc_wall_time_ns,
                time.wait_wall_time_ns,
                time.process_wall_time_ns,
                time.process_suspend_wall_time_ns,
                true,
            )
        } else {
            let time = details.time_detail.as_ref()?;
            (
                time.total_rpc_wall_time_ns,
                time.wait_wall_time_ms.saturating_mul(1_000_000),
                time.process_wall_time_ms.saturating_mul(1_000_000),
                0,
                false,
            )
        };

    let mut wait = ExecutionDetailSpan::leaf("tikv.Wait", wait_duration);
    let mut process = ExecutionDetailSpan::leaf("tikv.Process", process_duration);
    if let Some(scan) = details.scan_detail_v2.as_ref() {
        wait.children.push(ExecutionDetailSpan::leaf(
            "tikv.GetSnapshot",
            scan.get_snapshot_nanos,
        ));
        if details.write_detail.is_none() {
            process.children.push(ExecutionDetailSpan::leaf(
                "tikv.RocksDBBlockRead",
                scan.rocksdb_block_read_nanos,
            ));
        }
    }

    let mut root = ExecutionDetailSpan::leaf("tikv.RPC", rpc_duration);
    root.children.push(wait);
    root.children.push(process);
    if has_v2 {
        root.children
            .push(ExecutionDetailSpan::leaf("tikv.Suspend", suspend_duration));
    }

    if let Some(write) = details.write_detail.as_ref() {
        let mut persist_log = ExecutionDetailSpan::leaf("tikv.PersistLog", write.persist_log_nanos);
        persist_log.asynchronous = true;
        persist_log.children = vec![
            ExecutionDetailSpan::leaf(
                "tikv.RaftDBWriteWait",
                write.raft_db_write_leader_wait_nanos,
            ),
            ExecutionDetailSpan::leaf("tikv.RaftDBWriteWAL", write.raft_db_sync_log_nanos),
            ExecutionDetailSpan::leaf(
                "tikv.RaftDBWriteMemtable",
                write.raft_db_write_memtable_nanos,
            ),
        ];
        let mut apply_log = ExecutionDetailSpan::leaf("tikv.ApplyLog", write.apply_log_nanos);
        apply_log.children = vec![
            ExecutionDetailSpan::leaf("tikv.ApplyMutexLock", write.apply_mutex_lock_nanos),
            ExecutionDetailSpan::leaf(
                "tikv.ApplyWriteLeaderWait",
                write.apply_write_leader_wait_nanos,
            ),
            ExecutionDetailSpan::leaf("tikv.ApplyWriteWAL", write.apply_write_wal_nanos),
            ExecutionDetailSpan::leaf("tikv.ApplyWriteMemtable", write.apply_write_memtable_nanos),
        ];
        root.children.push(ExecutionDetailSpan {
            name: "tikv.AsyncWrite",
            duration: Duration::ZERO,
            asynchronous: false,
            children: vec![
                ExecutionDetailSpan::leaf("tikv.StoreBatchWait", write.store_batch_wait_nanos),
                ExecutionDetailSpan::leaf("tikv.ProposeSendWait", write.propose_send_wait_nanos),
                persist_log,
                ExecutionDetailSpan::leaf("tikv.CommitLog", write.commit_log_nanos),
                ExecutionDetailSpan::leaf("tikv.ApplyBatchWait", write.apply_batch_wait_nanos),
                apply_log,
            ],
        });
    }
    Some(root)
}

pub(crate) fn trace_exec_details_response(started_at: Instant, response: &dyn Any) {
    let Ok(handler) = EXECUTION_DETAILS_TRACE_HANDLER.try_with(ExecutionDetailsTraceHandler::clone)
    else {
        return;
    };
    let Some(details) = crate::store::exec_details_v2(response) else {
        return;
    };
    let Some(span) = build_execution_detail_span(details) else {
        return;
    };
    handler(started_at, &span);
}

fn format_go_duration(duration: Duration) -> String {
    let nanos = duration.as_nanos();
    if nanos == 0 {
        return "0s".to_owned();
    }
    fn decimal(whole: u128, remainder: u128, width: usize, suffix: &str) -> String {
        if remainder == 0 {
            return format!("{whole}{suffix}");
        }
        let fraction = format!("{remainder:0width$}")
            .trim_end_matches('0')
            .to_owned();
        format!("{whole}.{fraction}{suffix}")
    }
    if nanos < 1_000 {
        return format!("{nanos}ns");
    }
    if nanos < 1_000_000 {
        return decimal(nanos / 1_000, nanos % 1_000, 3, "µs");
    }
    if nanos < 1_000_000_000 {
        return decimal(nanos / 1_000_000, nanos % 1_000_000, 6, "ms");
    }

    let seconds = nanos / 1_000_000_000;
    let fraction = nanos % 1_000_000_000;
    let hours = seconds / 3_600;
    let minutes = (seconds % 3_600) / 60;
    let seconds = seconds % 60;
    let seconds = decimal(seconds, fraction, 9, "s");
    if hours > 0 {
        format!("{hours}h{minutes}m{seconds}")
    } else if minutes > 0 {
        format!("{minutes}m{seconds}")
    } else {
        seconds
    }
}

/// Trace logging control bits sent to TiKV.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(transparent)]
pub struct TraceControlFlags(pub u64);

impl TraceControlFlags {
    pub const IMMEDIATE_LOG: Self = Self(1 << 0);
    pub const TIKV_CATEGORY_REQUEST: Self = Self(1 << 1);
    pub const TIKV_CATEGORY_WRITE_DETAILS: Self = Self(1 << 2);
    pub const TIKV_CATEGORY_READ_DETAILS: Self = Self(1 << 3);

    pub const fn has(self, flag: Self) -> bool {
        self.0 & flag.0 != 0
    }

    pub const fn with(self, flag: Self) -> Self {
        Self(self.0 | flag.0)
    }
}

impl BitOr for TraceControlFlags {
    type Output = Self;

    fn bitor(self, right: Self) -> Self::Output {
        Self(self.0 | right.0)
    }
}

impl BitOrAssign for TraceControlFlags {
    fn bitor_assign(&mut self, right: Self) {
        self.0 |= right.0;
    }
}

/// Client trace-event family. Discriminants match client-go.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum Category {
    TransactionTwoPhaseCommit = 0,
    TransactionLockResolve = 1,
    KvRequest = 2,
    RegionCache = 3,
}

/// Native structured field passed to a trace event handler.
#[derive(Clone)]
pub struct TraceField {
    pub name: String,
    value: Arc<dyn Any + Send + Sync>,
}

impl TraceField {
    pub fn new<V>(name: impl Into<String>, value: V) -> Self
    where
        V: Any + Send + Sync,
    {
        Self {
            name: name.into(),
            value: Arc::new(value),
        }
    }

    pub fn value<V: Any + Send + Sync>(&self) -> Option<&V> {
        self.value.downcast_ref()
    }
}

impl std::fmt::Debug for TraceField {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TraceField")
            .field("name", &self.name)
            .field("value_type", &self.value.type_id())
            .finish()
    }
}

/// Immutable context supporting type-keyed values and trace-ID propagation.
#[derive(Clone, Default)]
pub struct TraceContext {
    values: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
    trace_id: Option<Arc<[u8]>>,
}

impl TraceContext {
    pub fn new() -> Self {
        Self::default()
    }

    /// Return a derived context with a value keyed by the marker type `K`.
    pub fn with_value<K, V>(&self, value: V) -> Self
    where
        K: 'static,
        V: Any + Send + Sync,
    {
        let mut derived = self.clone();
        derived.values.insert(TypeId::of::<K>(), Arc::new(value));
        derived
    }

    pub fn value<K, V>(&self) -> Option<&V>
    where
        K: 'static,
        V: Any + Send + Sync,
    {
        self.values
            .get(&TypeId::of::<K>())
            .and_then(|value| value.downcast_ref())
    }

    pub fn with_trace_id(&self, trace_id: impl Into<Vec<u8>>) -> Self {
        let mut derived = self.clone();
        derived.trace_id = Some(Arc::from(trace_id.into()));
        derived
    }

    pub fn trace_id(&self) -> Option<&[u8]> {
        self.trace_id.as_deref()
    }
}

pub type TraceEventHandler =
    Arc<dyn Fn(&TraceContext, Category, &str, &[TraceField]) + Send + Sync>;
pub type CategoryEnabledHandler = Arc<dyn Fn(Category) -> bool + Send + Sync>;
pub type TraceControlExtractor = Arc<dyn Fn(&TraceContext) -> TraceControlFlags + Send + Sync>;
/// Native counterpart of grpc-opentracing's global text-map injector.
/// Implementations add the active trace carrier to outgoing gRPC metadata.
pub type GrpcTraceMetadataInjector = Arc<dyn Fn(&mut tonic::metadata::MetadataMap) + Send + Sync>;

fn no_op_event(_: &TraceContext, _: Category, _: &str, _: &[TraceField]) {}
fn no_categories(_: Category) -> bool {
    false
}
fn default_trace_control(_: &TraceContext) -> TraceControlFlags {
    TraceControlFlags::TIKV_CATEGORY_REQUEST
}
fn no_op_grpc_trace_injector(_: &mut tonic::metadata::MetadataMap) {}

lazy_static! {
    static ref TRACE_EVENT_HANDLER: RwLock<TraceEventHandler> = RwLock::new(Arc::new(no_op_event));
    static ref CATEGORY_ENABLED_HANDLER: RwLock<CategoryEnabledHandler> =
        RwLock::new(Arc::new(no_categories));
    static ref TRACE_CONTROL_EXTRACTOR: RwLock<TraceControlExtractor> =
        RwLock::new(Arc::new(default_trace_control));
    static ref GRPC_TRACE_METADATA_INJECTOR: RwLock<GrpcTraceMetadataInjector> =
        RwLock::new(Arc::new(no_op_grpc_trace_injector));
}

tokio::task_local! {
    static GRPC_OPEN_TRACING_ENABLED: bool;
}

/// Replace the event handler; `None` restores the no-op implementation.
pub fn set_trace_event_handler(handler: Option<TraceEventHandler>) {
    *TRACE_EVENT_HANDLER.write().unwrap() = handler.unwrap_or_else(|| Arc::new(no_op_event));
}

/// Replace the category predicate; `None` restores the always-disabled implementation.
pub fn set_category_enabled_handler(handler: Option<CategoryEnabledHandler>) {
    *CATEGORY_ENABLED_HANDLER.write().unwrap() = handler.unwrap_or_else(|| Arc::new(no_categories));
}

/// Emit an event through the currently registered handler.
pub fn trace_event(context: &TraceContext, category: Category, name: &str, fields: &[TraceField]) {
    let handler = TRACE_EVENT_HANDLER.read().unwrap().clone();
    handler(context, category, name, fields);
}

pub fn is_category_enabled(category: Category) -> bool {
    let handler = CATEGORY_ENABLED_HANDLER.read().unwrap().clone();
    handler(category)
}

/// Replace the control extractor; `None` restores request-category tracing.
pub fn set_trace_control_extractor(extractor: Option<TraceControlExtractor>) {
    *TRACE_CONTROL_EXTRACTOR.write().unwrap() =
        extractor.unwrap_or_else(|| Arc::new(default_trace_control));
}

/// Installs the process-wide carrier injector used when
/// [`crate::Config::open_tracing_enable`] is true. `None` restores the no-op
/// global tracer behavior.
pub fn set_grpc_trace_metadata_injector(injector: Option<GrpcTraceMetadataInjector>) {
    *GRPC_TRACE_METADATA_INJECTOR.write().unwrap() =
        injector.unwrap_or_else(|| Arc::new(no_op_grpc_trace_injector));
}

pub(crate) async fn with_grpc_open_tracing<F>(enabled: bool, future: F) -> F::Output
where
    F: Future,
{
    GRPC_OPEN_TRACING_ENABLED.scope(enabled, future).await
}

pub(crate) fn inject_current_grpc_trace_metadata(metadata: &mut tonic::metadata::MetadataMap) {
    if GRPC_OPEN_TRACING_ENABLED.try_with(|enabled| *enabled) != Ok(true) {
        return;
    }
    let injector = GRPC_TRACE_METADATA_INJECTOR.read().unwrap().clone();
    injector(metadata);
}

pub(crate) fn inject_grpc_trace_metadata(
    metadata: &mut tonic::metadata::MetadataMap,
    enabled: bool,
) {
    if !enabled {
        return;
    }
    let injector = GRPC_TRACE_METADATA_INJECTOR.read().unwrap().clone();
    injector(metadata);
}

pub fn trace_control_flags(context: &TraceContext) -> TraceControlFlags {
    let extractor = TRACE_CONTROL_EXTRACTOR.read().unwrap().clone();
    extractor(context)
}

pub fn immediate_logging_enabled(context: &TraceContext) -> bool {
    trace_control_flags(context).has(TraceControlFlags::IMMEDIATE_LOG)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn reset_handlers() {
        set_trace_event_handler(None);
        set_category_enabled_handler(None);
        set_trace_control_extractor(None);
        set_grpc_trace_metadata_injector(None);
    }

    #[test]
    fn trace_control_flag_values_and_operations_match_client_go() {
        assert_eq!(TraceControlFlags::IMMEDIATE_LOG.0, 1 << 0);
        assert_eq!(TraceControlFlags::TIKV_CATEGORY_REQUEST.0, 1 << 1);
        assert_eq!(TraceControlFlags::TIKV_CATEGORY_WRITE_DETAILS.0, 1 << 2);
        assert_eq!(TraceControlFlags::TIKV_CATEGORY_READ_DETAILS.0, 1 << 3);

        let empty = TraceControlFlags::default();
        assert!(!empty.has(TraceControlFlags::IMMEDIATE_LOG));
        let flags = empty
            .with(TraceControlFlags::IMMEDIATE_LOG)
            .with(TraceControlFlags::TIKV_CATEGORY_REQUEST)
            .with(TraceControlFlags::TIKV_CATEGORY_WRITE_DETAILS)
            .with(TraceControlFlags::TIKV_CATEGORY_READ_DETAILS);
        assert_eq!(flags.0, 0b1111);
        assert_eq!(flags.with(TraceControlFlags::IMMEDIATE_LOG), flags);
    }

    #[test]
    #[serial]
    fn extractor_defaults_custom_context_values_and_reset_match_source() {
        reset_handlers();
        let context = TraceContext::new();
        assert_eq!(
            trace_control_flags(&context),
            TraceControlFlags::TIKV_CATEGORY_REQUEST
        );
        assert!(!immediate_logging_enabled(&context));

        set_trace_control_extractor(Some(Arc::new(|_| {
            TraceControlFlags::IMMEDIATE_LOG | TraceControlFlags::TIKV_CATEGORY_REQUEST
        })));
        assert!(immediate_logging_enabled(&context));

        struct FlagsKey;
        set_trace_control_extractor(Some(Arc::new(|context| {
            context
                .value::<FlagsKey, TraceControlFlags>()
                .copied()
                .unwrap_or_default()
        })));
        assert_eq!(trace_control_flags(&context), TraceControlFlags::default());
        let detailed = context.with_value::<FlagsKey, _>(
            TraceControlFlags::TIKV_CATEGORY_WRITE_DETAILS
                | TraceControlFlags::TIKV_CATEGORY_READ_DETAILS,
        );
        assert!(trace_control_flags(&detailed).has(TraceControlFlags::TIKV_CATEGORY_WRITE_DETAILS));
        assert!(!immediate_logging_enabled(&detailed));

        set_trace_control_extractor(None);
        assert_eq!(
            trace_control_flags(&detailed),
            TraceControlFlags::TIKV_CATEGORY_REQUEST
        );
        reset_handlers();
    }

    #[test]
    #[serial]
    fn event_and_category_handlers_are_independent_and_resettable() {
        reset_handlers();
        let called = Arc::new(AtomicBool::new(false));
        let observed = called.clone();
        set_trace_event_handler(Some(Arc::new(move |_, category, name, fields| {
            assert_eq!(category, Category::TransactionTwoPhaseCommit);
            assert_eq!(name, "test");
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].name, "key");
            assert_eq!(fields[0].value::<&str>(), Some(&"value"));
            observed.store(true, Ordering::SeqCst);
        })));
        trace_event(
            &TraceContext::new(),
            Category::TransactionTwoPhaseCommit,
            "test",
            &[TraceField::new("key", "value")],
        );
        assert!(called.load(Ordering::SeqCst));

        assert!(!is_category_enabled(Category::TransactionTwoPhaseCommit));
        set_category_enabled_handler(Some(Arc::new(|category| {
            category == Category::TransactionTwoPhaseCommit
        })));
        assert!(is_category_enabled(Category::TransactionTwoPhaseCommit));
        assert!(!is_category_enabled(Category::TransactionLockResolve));

        called.store(false, Ordering::SeqCst);
        set_trace_event_handler(None);
        trace_event(
            &TraceContext::new(),
            Category::TransactionTwoPhaseCommit,
            "test",
            &[],
        );
        assert!(!called.load(Ordering::SeqCst));
        reset_handlers();
    }

    #[test]
    fn trace_ids_are_absent_in_root_contexts_and_override_in_derived_contexts() {
        let context = TraceContext::new();
        assert_eq!(context.trace_id(), None);
        let first = context.with_trace_id(vec![1, 2, 3, 4, 5]);
        assert_eq!(first.trace_id(), Some(&[1, 2, 3, 4, 5][..]));
        let second = first.with_trace_id(vec![6, 7, 8, 9, 10]);
        assert_eq!(second.trace_id(), Some(&[6, 7, 8, 9, 10][..]));
        assert_eq!(first.trace_id(), Some(&[1, 2, 3, 4, 5][..]));
    }

    fn timing_millis(span: &ExecutionDetailSpan) -> Vec<(&'static str, u128, u128, bool)> {
        span.timeline()
            .into_iter()
            .map(|timing| {
                (
                    timing.name,
                    timing.start_offset.as_millis(),
                    timing.duration.as_millis(),
                    timing.asynchronous,
                )
            })
            .collect()
    }

    #[test]
    fn execution_detail_tree_and_historical_timeline_match_client_go() {
        assert_eq!(
            build_execution_detail_span(&kvrpcpb::ExecDetailsV2::default()),
            None
        );

        let rpc_only = build_execution_detail_span(&kvrpcpb::ExecDetailsV2 {
            time_detail_v2: Some(kvrpcpb::TimeDetailV2 {
                total_rpc_wall_time_ns: 1_000_000_000,
                ..Default::default()
            }),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            rpc_only.to_string(),
            "tikv.RPC[1s]{ tikv.Wait tikv.Process tikv.Suspend }"
        );
        assert_eq!(
            timing_millis(&rpc_only),
            vec![("tikv.RPC", 0, 1_000, false)]
        );

        let time = kvrpcpb::TimeDetailV2 {
            total_rpc_wall_time_ns: 1_000_000_000,
            wait_wall_time_ns: 100_000_000,
            process_wall_time_ns: 500_000_000,
            process_suspend_wall_time_ns: 50_000_000,
            ..Default::default()
        };
        let timed = build_execution_detail_span(&kvrpcpb::ExecDetailsV2 {
            time_detail_v2: Some(time.clone()),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            timed.to_string(),
            "tikv.RPC[1s]{ tikv.Wait[100ms] tikv.Process[500ms] tikv.Suspend[50ms] }"
        );
        assert_eq!(
            timing_millis(&timed),
            vec![
                ("tikv.Wait", 0, 100, false),
                ("tikv.Process", 100, 500, false),
                ("tikv.Suspend", 600, 50, false),
                ("tikv.RPC", 0, 1_000, false),
            ]
        );

        let scan = kvrpcpb::ScanDetailV2 {
            get_snapshot_nanos: 80_000_000,
            rocksdb_block_read_nanos: 200_000_000,
            ..Default::default()
        };
        let read = build_execution_detail_span(&kvrpcpb::ExecDetailsV2 {
            time_detail_v2: Some(time.clone()),
            scan_detail_v2: Some(scan.clone()),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            read.to_string(),
            "tikv.RPC[1s]{ tikv.Wait[100ms]{ tikv.GetSnapshot[80ms] } tikv.Process[500ms]{ tikv.RocksDBBlockRead[200ms] } tikv.Suspend[50ms] }"
        );
        assert_eq!(
            timing_millis(&read),
            vec![
                ("tikv.GetSnapshot", 0, 80, false),
                ("tikv.Wait", 0, 100, false),
                ("tikv.RocksDBBlockRead", 100, 200, false),
                ("tikv.Process", 100, 500, false),
                ("tikv.Suspend", 600, 50, false),
                ("tikv.RPC", 0, 1_000, false),
            ]
        );

        let empty_write = build_execution_detail_span(&kvrpcpb::ExecDetailsV2 {
            time_detail_v2: Some(time),
            scan_detail_v2: Some(scan),
            write_detail: Some(kvrpcpb::WriteDetail::default()),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            empty_write.to_string(),
            "tikv.RPC[1s]{ tikv.Wait[100ms]{ tikv.GetSnapshot[80ms] } tikv.Process[500ms] tikv.Suspend[50ms] tikv.AsyncWrite{ tikv.StoreBatchWait tikv.ProposeSendWait tikv.PersistLog'{ tikv.RaftDBWriteWait tikv.RaftDBWriteWAL tikv.RaftDBWriteMemtable } tikv.CommitLog tikv.ApplyBatchWait tikv.ApplyLog{ tikv.ApplyMutexLock tikv.ApplyWriteLeaderWait tikv.ApplyWriteWAL tikv.ApplyWriteMemtable } } }"
        );
        assert_eq!(
            timing_millis(&empty_write),
            vec![
                ("tikv.GetSnapshot", 0, 80, false),
                ("tikv.Wait", 0, 100, false),
                ("tikv.Process", 100, 500, false),
                ("tikv.Suspend", 600, 50, false),
                ("tikv.RPC", 0, 1_000, false),
            ]
        );

        let write = build_execution_detail_span(&kvrpcpb::ExecDetailsV2 {
            time_detail_v2: Some(kvrpcpb::TimeDetailV2 {
                total_rpc_wall_time_ns: 1_000_000_000,
                ..Default::default()
            }),
            scan_detail_v2: Some(kvrpcpb::ScanDetailV2 {
                get_snapshot_nanos: 80_000_000,
                ..Default::default()
            }),
            write_detail: Some(kvrpcpb::WriteDetail {
                store_batch_wait_nanos: 10_000_000,
                propose_send_wait_nanos: 10_000_000,
                persist_log_nanos: 100_000_000,
                raft_db_write_leader_wait_nanos: 20_000_000,
                raft_db_sync_log_nanos: 30_000_000,
                raft_db_write_memtable_nanos: 30_000_000,
                commit_log_nanos: 200_000_000,
                apply_batch_wait_nanos: 20_000_000,
                apply_log_nanos: 300_000_000,
                apply_mutex_lock_nanos: 10_000_000,
                apply_write_leader_wait_nanos: 10_000_000,
                apply_write_wal_nanos: 80_000_000,
                apply_write_memtable_nanos: 50_000_000,
                ..Default::default()
            }),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            write.to_string(),
            "tikv.RPC[1s]{ tikv.Wait{ tikv.GetSnapshot[80ms] } tikv.Process tikv.Suspend tikv.AsyncWrite{ tikv.StoreBatchWait[10ms] tikv.ProposeSendWait[10ms] tikv.PersistLog'[100ms]{ tikv.RaftDBWriteWait[20ms] tikv.RaftDBWriteWAL[30ms] tikv.RaftDBWriteMemtable[30ms] } tikv.CommitLog[200ms] tikv.ApplyBatchWait[20ms] tikv.ApplyLog[300ms]{ tikv.ApplyMutexLock[10ms] tikv.ApplyWriteLeaderWait[10ms] tikv.ApplyWriteWAL[80ms] tikv.ApplyWriteMemtable[50ms] } } }"
        );
        assert_eq!(
            timing_millis(&write),
            vec![
                ("tikv.GetSnapshot", 0, 80, false),
                ("tikv.Wait", 0, 80, false),
                ("tikv.StoreBatchWait", 80, 10, false),
                ("tikv.ProposeSendWait", 90, 10, false),
                ("tikv.RaftDBWriteWait", 100, 20, false),
                ("tikv.RaftDBWriteWAL", 120, 30, false),
                ("tikv.RaftDBWriteMemtable", 150, 30, false),
                ("tikv.PersistLog", 100, 100, true),
                ("tikv.CommitLog", 100, 200, false),
                ("tikv.ApplyBatchWait", 300, 20, false),
                ("tikv.ApplyMutexLock", 320, 10, false),
                ("tikv.ApplyWriteLeaderWait", 330, 10, false),
                ("tikv.ApplyWriteWAL", 340, 80, false),
                ("tikv.ApplyWriteMemtable", 420, 50, false),
                ("tikv.ApplyLog", 320, 300, false),
                ("tikv.AsyncWrite", 80, 540, false),
                ("tikv.RPC", 0, 1_000, false),
            ]
        );

        let legacy = build_execution_detail_span(&kvrpcpb::ExecDetailsV2 {
            time_detail: Some(kvrpcpb::TimeDetail {
                wait_wall_time_ms: 2,
                process_wall_time_ms: 3,
                total_rpc_wall_time_ns: 6_000_000,
                ..Default::default()
            }),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            legacy.to_string(),
            "tikv.RPC[6ms]{ tikv.Wait[2ms] tikv.Process[3ms] }"
        );
    }

    #[tokio::test]
    async fn execution_detail_scope_is_task_local_and_opt_in() {
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = observed.clone();
        let response = kvrpcpb::GetResponse {
            exec_details_v2: Some(kvrpcpb::ExecDetailsV2 {
                time_detail_v2: Some(kvrpcpb::TimeDetailV2 {
                    total_rpc_wall_time_ns: 1_000_000,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let start = Instant::now();
        trace_exec_details_response(start, &response);
        assert!(observed.lock().unwrap().is_empty());

        with_trace_exec_details(
            Arc::new(move |started_at, span| {
                sink.lock().unwrap().push((started_at, span.to_string()));
            }),
            async {
                trace_exec_details_response(start, &response);
            },
        )
        .await;
        assert_eq!(
            *observed.lock().unwrap(),
            vec![(
                start,
                "tikv.RPC[1ms]{ tikv.Wait tikv.Process tikv.Suspend }".to_owned()
            )]
        );
    }
}
