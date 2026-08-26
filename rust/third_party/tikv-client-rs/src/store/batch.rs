//! BatchCommands request selection and grouping.
//!
//! This module is the ownership-based mapping of client-go's
//! `internal/client/client_batch.go` and `conn_batch.go`: collection,
//! publication, response retirement, stream supervision, forwarding-host
//! isolation, recovery epochs, and connection-local admission live together.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

use crate::proto::tikvpb;
use futures::FutureExt;
use rand::Rng;
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex};
use tokio::task::JoinHandle;

use super::client::{ClientEventListener, KvClient, KvRpcClient, TIKV_DIAL_TIMEOUT};
use super::command::BatchRequestIdAllocator;
use super::command::{
    BatchPendingResponses, BatchResponseDisposition, BatchStreamMetricLabels, BatchStreamProgress,
};
use super::priority_queue::{PriorityItem, PriorityQueue};
use super::BatchCommandRequest;
use super::BatchCommandResponse;
use crate::async_util::Cancellation;
use crate::retry::{RetryBackoffer, BO_TIKV_RPC};
use crate::stats::{
    increment_batch_stream_request_counter, observe_batch_best_size, observe_batch_client_recycle,
    observe_batch_client_unavailable, observe_batch_client_wait_establish,
    observe_batch_head_arrival_interval, observe_batch_more_requests,
    observe_batch_pending_requests, observe_batch_requests, observe_batch_send_loop,
    observe_batch_send_tail, observe_batch_stream_recv_loop, observe_batch_stream_tail,
    BatchStreamRequestCounter, BatchStreamTailKind,
};
use crate::{Error, Result};

/// Client-go's threshold for requests which may exceed the normal batch
/// admission limit.
pub(crate) const HIGH_TASK_PRIORITY: u64 = 10;

const TURBO_BATCH_ALWAYS: i64 = 0;
const TURBO_BATCH_TIME_BASED: i64 = 1;
const TURBO_BATCH_PROB_BASED: i64 = 2;
const BATCH_RECV_TAIL_LATENCY_THRESHOLD: Duration = Duration::from_millis(20);
const BATCH_SEND_TAIL_LATENCY_THRESHOLD: Duration = Duration::from_millis(20);
const BATCH_REQUEST_INSPECT_INTERVAL: Duration = Duration::from_secs(60);

/// Source JSON shape for a client-go custom batch policy. Missing fields
/// retain Go's zero values, matching tags are case-insensitive, duplicate
/// keys use the last non-null value, and `null` leaves the current value
/// unchanged.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct TurboBatchOptions {
    strategy: i64,
    max_intervals: i64,
    wait_seconds: f64,
    smoothing_weight: f64,
    fetch_threshold: f64,
    wait_size_fraction: f64,
}

impl<'de> serde::Deserialize<'de> for TurboBatchOptions {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct OptionsVisitor;

        impl<'de> serde::de::Visitor<'de> for OptionsVisitor {
            type Value = TurboBatchOptions;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a client-go custom batch policy object")
            }

            fn visit_map<A>(self, mut map: A) -> std::result::Result<TurboBatchOptions, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut options = TurboBatchOptions::default();
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "v" | "V" => {
                            if let Some(value) = map.next_value::<Option<i64>>()? {
                                options.strategy = value;
                            }
                        }
                        "n" | "N" => {
                            if let Some(value) = map.next_value::<Option<i64>>()? {
                                options.max_intervals = value;
                            }
                        }
                        "t" | "T" => {
                            if let Some(value) = map.next_value::<Option<f64>>()? {
                                options.wait_seconds = value;
                            }
                        }
                        "w" | "W" => {
                            if let Some(value) = map.next_value::<Option<f64>>()? {
                                options.smoothing_weight = value;
                            }
                        }
                        "p" | "P" => {
                            if let Some(value) = map.next_value::<Option<f64>>()? {
                                options.fetch_threshold = value;
                            }
                        }
                        "q" | "Q" => {
                            if let Some(value) = map.next_value::<Option<f64>>()? {
                                options.wait_size_fraction = value;
                            }
                        }
                        _ => {
                            map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(options)
            }
        }

        deserializer.deserialize_map(OptionsVisitor)
    }
}

/// The dynamic batch trigger from client-go `turboBatchTrigger`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct TurboBatchTrigger {
    options: TurboBatchOptions,
    estimated_fetch_more_probability: f64,
    estimated_arrival_interval: f64,
    max_arrival_interval: f64,
}

impl TurboBatchTrigger {
    fn from_policy(policy: &str) -> (Self, bool) {
        let options = match policy {
            crate::config::BATCH_POLICY_BASIC => TurboBatchOptions::default(),
            crate::config::BATCH_POLICY_STANDARD => TurboBatchOptions {
                strategy: TURBO_BATCH_TIME_BASED,
                wait_seconds: 0.0001,
                max_intervals: 5,
                smoothing_weight: 0.2,
                fetch_threshold: 0.8,
                wait_size_fraction: 0.8,
            },
            crate::config::BATCH_POLICY_POSITIVE => TurboBatchOptions {
                wait_seconds: 0.0001,
                ..TurboBatchOptions::default()
            },
            _ => {
                // Go uses strings.CutPrefix, so a bare JSON policy is also
                // accepted in addition to `custom <json>`.
                let raw = policy
                    .strip_prefix(crate::config::BATCH_POLICY_CUSTOM)
                    .unwrap_or(policy)
                    .trim();
                if raw == "null" {
                    return (Self::default(), true);
                }
                return match serde_json::from_str(raw) {
                    Ok(options) => (
                        Self {
                            options,
                            ..Self::default()
                        },
                        true,
                    ),
                    Err(_) => (
                        Self {
                            options: Self::standard_options(),
                            ..Self::default()
                        },
                        false,
                    ),
                };
            }
        };
        (
            Self {
                options,
                ..Self::default()
            },
            true,
        )
    }

    const fn standard_options() -> TurboBatchOptions {
        TurboBatchOptions {
            strategy: TURBO_BATCH_TIME_BASED,
            max_intervals: 5,
            wait_seconds: 0.0001,
            smoothing_weight: 0.2,
            fetch_threshold: 0.8,
            wait_size_fraction: 0.8,
        }
    }

    fn turbo_wait_time(self) -> Option<Duration> {
        (self.options.wait_seconds > 0.0)
            .then(|| Duration::from_secs_f64(self.options.wait_seconds))
    }

    fn need_fetch_more(&mut self, request_arrival_interval: Duration) -> bool {
        match self.options.strategy {
            TURBO_BATCH_TIME_BASED => {
                let mut interval = request_arrival_interval.as_secs_f64();
                if self.max_arrival_interval == 0.0 {
                    self.max_arrival_interval =
                        self.options.wait_seconds * self.options.max_intervals as f64;
                }
                if interval > self.max_arrival_interval {
                    interval = self.max_arrival_interval;
                }
                if self.estimated_arrival_interval == 0.0 {
                    self.estimated_arrival_interval = interval;
                } else {
                    self.estimated_arrival_interval = self.options.smoothing_weight * interval
                        + (1.0 - self.options.smoothing_weight) * self.estimated_arrival_interval;
                }
                self.estimated_arrival_interval
                    < self.options.wait_seconds * self.options.fetch_threshold
            }
            TURBO_BATCH_PROB_BASED => {
                let probability =
                    if request_arrival_interval.as_secs_f64() < self.options.wait_seconds {
                        1.0
                    } else {
                        0.0
                    };
                self.estimated_fetch_more_probability = self.options.smoothing_weight * probability
                    + (1.0 - self.options.smoothing_weight) * self.estimated_fetch_more_probability;
                self.estimated_fetch_more_probability > self.options.fetch_threshold
            }
            _ => true,
        }
    }

    fn preferred_batch_wait_size(
        &self,
        average_batch_wait_size: f64,
        default_size: usize,
    ) -> usize {
        if self.options.strategy == TURBO_BATCH_ALWAYS {
            return default_size;
        }
        let integer = average_batch_wait_size.trunc() as usize;
        integer + usize::from(average_batch_wait_size.fract() >= self.options.wait_size_fraction)
    }
}

/// The source `batchSendLoop` wait decision, separated from queue ownership so
/// the eventual worker can apply it after its initial greedy head drain.
struct BatchCollectionPolicy {
    max_batch_size: usize,
    batch_wait_size: usize,
    max_batch_wait_time: Duration,
    overload_threshold: u64,
    trigger: TurboBatchTrigger,
    turbo_wait_time: Option<Duration>,
    average_batch_wait_size: f64,
}

impl BatchCollectionPolicy {
    fn from_config(config: &crate::config::TiKvClient) -> Self {
        let (trigger, _) = TurboBatchTrigger::from_policy(&config.batch_policy);
        Self {
            max_batch_size: usize::try_from(config.max_batch_size).unwrap_or(usize::MAX),
            batch_wait_size: usize::try_from(config.batch_wait_size).unwrap_or(usize::MAX),
            max_batch_wait_time: config.max_batch_wait_time,
            overload_threshold: config.overload_threshold,
            turbo_wait_time: trigger.turbo_wait_time(),
            trigger,
            average_batch_wait_size: config.batch_wait_size as f64,
        }
    }

    /// Returns the exact `(wait-size, wait-duration)` choice for the source's
    /// `fetchMorePendingRequests`, with overload taking precedence.
    fn wait_for_more(
        &mut self,
        current_size: usize,
        head_arrival_interval: Option<Duration>,
        transport_layer_load: u64,
    ) -> Option<(usize, Duration)> {
        if current_size >= self.max_batch_size {
            return None;
        }
        if !self.max_batch_wait_time.is_zero() && transport_layer_load > self.overload_threshold {
            return Some((self.batch_wait_size, self.max_batch_wait_time));
        }
        let interval = head_arrival_interval?;
        let wait = self.turbo_wait_time?;
        self.trigger.need_fetch_more(interval).then(|| {
            (
                self.trigger
                    .preferred_batch_wait_size(self.average_batch_wait_size, self.batch_wait_size),
                wait,
            )
        })
    }

    fn observe_batch_size(&mut self, size: usize) {
        self.average_batch_wait_size = 0.2 * size as f64 + 0.8 * self.average_batch_wait_size;
    }
}

/// Cancellation token for a command which has not yet been selected into a
/// concrete `BatchCommandsRequest`.
#[derive(Clone, Debug)]
pub(crate) struct BatchCommandCancellation(Arc<AtomicBool>);

impl BatchCommandCancellation {
    pub(crate) fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }
}

/// Caller-owned completion for one enqueued batch command. Dropping
/// `response` after publication represents caller cancellation; the receive
/// path still retires the source request ID when its response arrives.
pub(crate) struct BatchCommandSubmission {
    pub(crate) cancellation: BatchCommandCancellation,
    response: oneshot::Receiver<Result<BatchCommandResponse>>,
    telemetry: Arc<BatchRequestTelemetry>,
}

impl BatchCommandSubmission {
    /// Wait for the one terminal result while retaining cancellation ownership
    /// until the caller has finished observing it.
    pub(crate) async fn recv(
        &mut self,
    ) -> std::result::Result<Result<BatchCommandResponse>, oneshot::error::RecvError> {
        (&mut self.response).await
    }

    #[cfg(test)]
    fn try_recv(
        &mut self,
    ) -> std::result::Result<Result<BatchCommandResponse>, oneshot::error::TryRecvError> {
        self.response.try_recv()
    }

    pub(crate) fn timeout_reason(&self, timeout: Duration, now: Instant) -> String {
        self.telemetry.timeout_reason(timeout, now)
    }

    pub(crate) fn complete_with_error(&self, error: &Error) {
        self.telemetry.complete(Some(error));
    }
}

/// Source `batchRequestStage` labels, preserved in the request-stage metric.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BatchRequestStage {
    BatchWait,
    SendWait,
    RecvWait,
    Done,
}

impl BatchRequestStage {
    const fn as_str(self) -> &'static str {
        match self {
            Self::BatchWait => "batch_wait",
            Self::SendWait => "send_wait",
            Self::RecvWait => "recv_wait",
            Self::Done => "done",
        }
    }
}

/// Source `batchRequestOutcome` labels, derived from the Rust terminal error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BatchRequestOutcome {
    Ok,
    Timeout,
    Cancelled,
    Failed,
    Closed,
}

impl BatchRequestOutcome {
    fn from_error(error: Option<&Error>) -> Self {
        let Some(error) = error else {
            return Self::Ok;
        };
        match error {
            Error::GrpcAPI(status) if status.code() == tonic::Code::DeadlineExceeded => {
                Self::Timeout
            }
            Error::GrpcAPI(status) if status.code() == tonic::Code::Cancelled => Self::Cancelled,
            Error::Connection { source, .. } => Self::from_error(Some(source)),
            Error::StringError(message)
                if message == "batch client closed" || message == "batchConn closed" =>
            {
                Self::Closed
            }
            Error::StringError(message) if message == "batch request cancelled" => Self::Cancelled,
            _ => Self::Failed,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Timeout => "timeout",
            Self::Cancelled => "canceled",
            Self::Failed => "failed",
            Self::Closed => "closed",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BatchRequestObservation {
    stage: BatchRequestStage,
    outcome: BatchRequestOutcome,
    duration: Duration,
}

struct BatchRequestSendState {
    batch_size: usize,
    send_started_at: Instant,
    sent_after_start_ns: AtomicU64,
    first_response_after_start_ns: AtomicU64,
}

struct BatchRequestPublishedState {
    request_id: u64,
    forwarded_host: String,
    progress: Arc<BatchStreamProgress>,
}

/// Shared timing state follows a command from collector arrival through its
/// response/failure retirement. It is the owned Rust form of client-go's
/// `batchCommandsEntry` plus `batchCommandsRequestState` atomics.
pub(crate) struct BatchRequestTelemetry {
    store_id: u64,
    arrived_at: Instant,
    selected_after_arrival_ns: AtomicU64,
    send_state: Mutex<Option<Arc<BatchRequestSendState>>>,
    published_state: Mutex<Option<BatchRequestPublishedState>>,
    received_after_arrival_ns: AtomicU64,
    terminal: AtomicBool,
}

impl BatchRequestTelemetry {
    pub(crate) fn new(store_id: u64, arrived_at: Instant) -> Self {
        Self {
            store_id,
            arrived_at,
            selected_after_arrival_ns: AtomicU64::new(0),
            send_state: Mutex::new(None),
            published_state: Mutex::new(None),
            received_after_arrival_ns: AtomicU64::new(0),
            terminal: AtomicBool::new(false),
        }
    }

    fn elapsed_ns(&self, now: Instant) -> u64 {
        u64::try_from(now.duration_since(self.arrived_at).as_nanos())
            .unwrap_or(u64::MAX)
            .max(1)
    }

    pub(crate) fn wait_duration(&self, now: Instant) -> Duration {
        now.duration_since(self.arrived_at)
    }

    fn mark_selected(&self, now: Instant) {
        self.selected_after_arrival_ns
            .compare_exchange(0, self.elapsed_ns(now), Ordering::AcqRel, Ordering::Acquire)
            .ok();
    }

    fn mark_send_started(
        &self,
        state: Arc<BatchRequestSendState>,
        request_id: u64,
        forwarded_host: String,
        progress: Arc<BatchStreamProgress>,
    ) {
        *self.send_state.lock().unwrap() = Some(state);
        *self.published_state.lock().unwrap() = Some(BatchRequestPublishedState {
            request_id,
            forwarded_host,
            progress,
        });
    }

    #[cfg(test)]
    fn mark_send_started_for_test(&self, batch_size: usize, now: Instant) {
        self.mark_send_started(
            Arc::new(BatchRequestSendState {
                batch_size,
                send_started_at: now,
                sent_after_start_ns: AtomicU64::new(0),
                first_response_after_start_ns: AtomicU64::new(0),
            }),
            1,
            String::new(),
            Arc::new(BatchStreamProgress::default()),
        );
    }

    pub(crate) fn mark_sent(&self, now: Instant) {
        if let Some(state) = self.send_state.lock().unwrap().as_ref() {
            let elapsed = u64::try_from(now.duration_since(state.send_started_at).as_nanos())
                .unwrap_or(u64::MAX)
                .max(1);
            let _ = state.sent_after_start_ns.compare_exchange(
                0,
                elapsed,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }

    pub(crate) fn mark_received(&self, now: Instant) {
        self.received_after_arrival_ns
            .compare_exchange(0, self.elapsed_ns(now), Ordering::AcqRel, Ordering::Acquire)
            .ok();
        if let Some(state) = self.send_state.lock().unwrap().as_ref() {
            let elapsed = u64::try_from(now.duration_since(state.send_started_at).as_nanos())
                .unwrap_or(u64::MAX)
                .max(1);
            let _ = state.first_response_after_start_ns.compare_exchange(
                0,
                elapsed,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }

    pub(crate) fn cancelled_response_tail(&self) -> Option<Duration> {
        let selected = self.selected_after_arrival_ns.load(Ordering::Acquire);
        let received = self.received_after_arrival_ns.load(Ordering::Acquire);
        (selected != 0 && received != 0)
            .then(|| Duration::from_nanos(received.saturating_sub(selected).max(1)))
    }

    fn observations(
        &self,
        terminal: BatchRequestOutcome,
        now: Instant,
    ) -> Vec<BatchRequestObservation> {
        let now_ns = self.elapsed_ns(now);
        let batched_ns = self.selected_after_arrival_ns.load(Ordering::Acquire);
        if batched_ns == 0 {
            return vec![BatchRequestObservation {
                stage: BatchRequestStage::BatchWait,
                outcome: terminal,
                duration: Duration::from_nanos(now_ns),
            }];
        }

        let received_ns = self.received_after_arrival_ns.load(Ordering::Acquire);
        let (batch_size, mut sent_ns, first_response_ns) = self
            .send_state
            .lock()
            .unwrap()
            .as_ref()
            .map(|state| {
                let start_ns = u64::try_from(
                    state
                        .send_started_at
                        .duration_since(self.arrived_at)
                        .as_nanos(),
                )
                .unwrap_or(u64::MAX)
                .max(1);
                (
                    state.batch_size,
                    (state.sent_after_start_ns.load(Ordering::Acquire) != 0)
                        .then(|| {
                            state
                                .sent_after_start_ns
                                .load(Ordering::Acquire)
                                .saturating_add(start_ns)
                        })
                        .unwrap_or(0),
                    (state.first_response_after_start_ns.load(Ordering::Acquire) != 0)
                        .then(|| {
                            state
                                .first_response_after_start_ns
                                .load(Ordering::Acquire)
                                .saturating_add(start_ns)
                        })
                        .unwrap_or(0),
                )
            })
            .unwrap_or((0, 0, 0));
        let _ = batch_size; // retained in state for source progress diagnostics.
        let boundary_ns = if received_ns != 0 {
            received_ns
        } else {
            first_response_ns
        };
        if boundary_ns != 0 {
            if sent_ns == 0 {
                sent_ns = batched_ns.saturating_add(1);
            } else if sent_ns > boundary_ns {
                sent_ns = boundary_ns.saturating_sub(1).max(1);
            }
        }

        let mut observations = vec![BatchRequestObservation {
            stage: BatchRequestStage::BatchWait,
            outcome: BatchRequestOutcome::Ok,
            duration: Duration::from_nanos(batched_ns),
        }];
        if sent_ns == 0 && received_ns == 0 {
            if first_response_ns != 0 {
                observations.push(BatchRequestObservation {
                    stage: BatchRequestStage::SendWait,
                    outcome: BatchRequestOutcome::Ok,
                    duration: Duration::from_nanos(sent_ns.saturating_sub(batched_ns).max(1)),
                });
                observations.push(BatchRequestObservation {
                    stage: BatchRequestStage::RecvWait,
                    outcome: terminal,
                    duration: Duration::from_nanos(now_ns.saturating_sub(sent_ns).max(1)),
                });
            } else {
                observations.push(BatchRequestObservation {
                    stage: BatchRequestStage::SendWait,
                    outcome: terminal,
                    duration: Duration::from_nanos(now_ns.saturating_sub(batched_ns).max(1)),
                });
            }
            return observations;
        }
        observations.push(BatchRequestObservation {
            stage: BatchRequestStage::SendWait,
            outcome: BatchRequestOutcome::Ok,
            duration: Duration::from_nanos(sent_ns.saturating_sub(batched_ns).max(1)),
        });
        if received_ns == 0 {
            observations.push(BatchRequestObservation {
                stage: BatchRequestStage::RecvWait,
                outcome: terminal,
                duration: Duration::from_nanos(now_ns.saturating_sub(sent_ns).max(1)),
            });
            return observations;
        }
        observations.push(BatchRequestObservation {
            stage: BatchRequestStage::RecvWait,
            outcome: BatchRequestOutcome::Ok,
            duration: Duration::from_nanos(received_ns.saturating_sub(sent_ns).max(1)),
        });
        if terminal == BatchRequestOutcome::Ok {
            observations.push(BatchRequestObservation {
                stage: BatchRequestStage::Done,
                outcome: BatchRequestOutcome::Ok,
                duration: Duration::from_nanos(now_ns),
            });
        }
        observations
    }

    fn timeout_reason(&self, timeout: Duration, now: Instant) -> String {
        use std::fmt::Write;

        let mut out = format!(
            "wait recvLoop timeout, timeout:{}, EntryProgress{{",
            crate::util::format_duration(timeout)
        );
        let now_ns = self.elapsed_ns(now);
        let batched_ns = self.selected_after_arrival_ns.load(Ordering::Acquire);
        if batched_ns == 0 {
            out.push('}');
            return out;
        }
        let received_ns = self.received_after_arrival_ns.load(Ordering::Acquire);
        let (batch_size, mut sent_ns, first_response_ns) = self
            .send_state
            .lock()
            .unwrap()
            .as_ref()
            .map(|state| {
                let start_ns = u64::try_from(
                    state
                        .send_started_at
                        .duration_since(self.arrived_at)
                        .as_nanos(),
                )
                .unwrap_or(u64::MAX)
                .max(1);
                let sent = state.sent_after_start_ns.load(Ordering::Acquire);
                let first = state.first_response_after_start_ns.load(Ordering::Acquire);
                (
                    state.batch_size,
                    (sent != 0)
                        .then(|| start_ns.saturating_add(sent))
                        .unwrap_or(0),
                    (first != 0)
                        .then(|| start_ns.saturating_add(first))
                        .unwrap_or(0),
                )
            })
            .unwrap_or((0, 0, 0));
        let boundary_ns = if received_ns != 0 {
            received_ns
        } else {
            first_response_ns
        };
        if boundary_ns != 0 {
            if sent_ns == 0 {
                sent_ns = batched_ns.saturating_add(1);
            } else if sent_ns > boundary_ns {
                sent_ns = boundary_ns.saturating_sub(1).max(1);
            }
        }
        let published = self.published_state.lock().unwrap();
        let received_by_tikv = published.as_ref().is_some_and(|published| {
            published.request_id != 0
                && published.request_id <= published.progress.max_response_id()
        });

        let _ = write!(
            out,
            "batch:{}",
            crate::util::format_duration(Duration::from_nanos(batched_ns))
        );
        if batch_size != 0 {
            let _ = write!(out, ", size:{batch_size}");
        }
        if sent_ns == 0 && received_ns == 0 {
            let _ = write!(
                out,
                ", send:{}",
                crate::util::format_duration(Duration::from_nanos(
                    now_ns.saturating_sub(batched_ns).max(1)
                ))
            );
            if received_by_tikv {
                out.push_str(", ack:yes");
            }
            out.push('}');
            return out;
        }
        let _ = write!(
            out,
            ", send:{}",
            crate::util::format_duration(Duration::from_nanos(
                sent_ns.saturating_sub(batched_ns).max(1)
            ))
        );
        if first_response_ns != 0 {
            let _ = write!(
                out,
                ", ack:{}",
                crate::util::format_duration(Duration::from_nanos(
                    first_response_ns.saturating_sub(sent_ns).max(1)
                ))
            );
        } else if received_by_tikv {
            out.push_str(", ack:yes");
        }
        if received_ns != 0 {
            let _ = write!(
                out,
                ", recv:{}",
                crate::util::format_duration(Duration::from_nanos(
                    received_ns.saturating_sub(sent_ns).max(1)
                ))
            );
        }
        if let Some(published) = published
            .as_ref()
            .filter(|published| !published.forwarded_host.is_empty())
        {
            let _ = write!(out, ", fwd:{}", published.forwarded_host);
        }
        out.push('}');
        out
    }

    pub(crate) fn complete(&self, error: Option<&Error>) {
        if self
            .terminal
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let outcome = BatchRequestOutcome::from_error(error);
        for observation in self.observations(outcome, Instant::now()) {
            crate::stats::observe_batch_request_stage(
                self.store_id,
                observation.stage.as_str(),
                observation.outcome.as_str(),
                observation.duration,
            );
        }
    }
}

impl Drop for BatchCommandSubmission {
    fn drop(&mut self) {
        // A Rust caller cancels an in-flight `dispatch` future by dropping
        // it. Make that cancellation visible before the builder assigns an
        // ID, just as client-go's context AfterFunc marks its async entry
        // cancelled before `buildWithLimit` selects it. Once published, the
        // receive path still owns retirement of the request ID.
        self.cancellation.cancel();
    }
}

/// An owned batch candidate. A non-empty forwarding host must use a distinct
/// gRPC stream, matching client-go's metadata-per-stream constraint.
pub(crate) struct BatchCommandEntry {
    // The command is moved into the protobuf batch at selection time. The
    // selected entry remains in its group to carry cancellation and routing
    // metadata until the response loop retires its request ID.
    request: Option<BatchCommandRequest>,
    priority: u64,
    forwarded_host: String,
    cancellation: Arc<AtomicBool>,
    response_sender: oneshot::Sender<Result<BatchCommandResponse>>,
    arrived_at: Instant,
    telemetry: Arc<BatchRequestTelemetry>,
}

/// Queue-driven counterpart to client-go's `fetchAllPendingRequests` plus
/// `fetchMorePendingRequests`. It owns no streams: callers decide whether a
/// turbo/overload wait is appropriate, then pass that wait into `collect`.
struct BatchRequestCollector {
    latest_arrival: Option<Instant>,
    last_head_received_at: Option<Instant>,
    last_head_arrival_interval: Option<Duration>,
    last_extra_fetched: Option<usize>,
    last_waited_for_overload: bool,
}

impl BatchRequestCollector {
    fn new() -> Self {
        Self {
            latest_arrival: None,
            last_head_received_at: None,
            last_head_arrival_interval: None,
            last_extra_fetched: None,
            last_waited_for_overload: false,
        }
    }

    /// Blocks for one head entry, drains immediately available work, then—if
    /// requested—waits only until the source wait-size/deadline boundary.
    /// Returns the head-arrival interval used by `turboBatchTrigger`, or
    /// `None` when the sender is closed before a head arrives.
    async fn collect(
        &mut self,
        receiver: &mut mpsc::Receiver<BatchCommandEntry>,
        builder: &mut BatchCommandsBuilder,
        max_batch_size: usize,
        batch_wait_size: usize,
        wait: Option<Duration>,
    ) -> Option<Option<Duration>> {
        let head = receiver.recv().await?;
        let interval = self.accept_head(head, builder);
        Self::drain_ready(receiver, builder, max_batch_size);
        if let Some(wait) = wait {
            Self::collect_more(receiver, builder, max_batch_size, batch_wait_size, wait).await;
        }
        Some(interval)
    }

    fn accept_head(
        &mut self,
        head: BatchCommandEntry,
        builder: &mut BatchCommandsBuilder,
    ) -> Option<Duration> {
        self.last_head_received_at = Some(Instant::now());
        let interval = self
            .latest_arrival
            .filter(|previous| head.arrived_at > *previous)
            .map(|previous| head.arrived_at.duration_since(previous));
        self.latest_arrival = Some(head.arrived_at);
        builder.entries.push(head);
        interval
    }

    async fn collect_more(
        receiver: &mut mpsc::Receiver<BatchCommandEntry>,
        builder: &mut BatchCommandsBuilder,
        max_batch_size: usize,
        batch_wait_size: usize,
        wait: Duration,
    ) {
        if builder.len() >= max_batch_size {
            return;
        }
        let deadline = tokio::time::Instant::now() + wait;
        while builder.len() < batch_wait_size.min(max_batch_size) {
            match tokio::time::timeout_at(deadline, receiver.recv()).await {
                Ok(Some(entry)) => builder.entries.push(entry),
                Ok(None) | Err(_) => return,
            }
        }

        // client-go yields once before its final non-blocking drain.
        tokio::task::yield_now().await;
        Self::drain_ready(receiver, builder, max_batch_size);
    }

    async fn collect_with_policy(
        &mut self,
        receiver: &mut mpsc::Receiver<BatchCommandEntry>,
        builder: &mut BatchCommandsBuilder,
        policy: &mut BatchCollectionPolicy,
        transport_layer_load: u64,
    ) -> bool {
        let Some(head) = receiver.recv().await else {
            return false;
        };
        self.collect_head_with_policy(receiver, builder, policy, transport_layer_load, head)
            .await;
        true
    }

    async fn collect_head_with_policy(
        &mut self,
        receiver: &mut mpsc::Receiver<BatchCommandEntry>,
        builder: &mut BatchCommandsBuilder,
        policy: &mut BatchCollectionPolicy,
        transport_layer_load: u64,
        head: BatchCommandEntry,
    ) {
        self.last_head_received_at = None;
        self.last_head_arrival_interval = None;
        self.last_extra_fetched = None;
        self.last_waited_for_overload = false;
        let interval = self.accept_head(head, builder);
        Self::drain_ready(receiver, builder, policy.max_batch_size);
        self.last_head_arrival_interval = interval;
        let initial_size = builder.len();
        let overload_wait = initial_size < policy.max_batch_size
            && !policy.max_batch_wait_time.is_zero()
            && transport_layer_load > policy.overload_threshold;
        if let Some((wait_size, wait)) =
            policy.wait_for_more(builder.len(), interval, transport_layer_load)
        {
            Self::collect_more(receiver, builder, policy.max_batch_size, wait_size, wait).await;
            if overload_wait {
                self.last_waited_for_overload = true;
            } else {
                self.last_extra_fetched = Some(builder.len().saturating_sub(initial_size));
            }
        }
        policy.observe_batch_size(builder.len());
    }

    fn drain_ready(
        receiver: &mut mpsc::Receiver<BatchCommandEntry>,
        builder: &mut BatchCommandsBuilder,
        max_batch_size: usize,
    ) {
        while builder.len() < max_batch_size {
            match receiver.try_recv() {
                Ok(entry) => builder.entries.push(entry),
                Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                    return;
                }
            }
        }
    }
}

impl PriorityItem for BatchCommandEntry {
    fn priority(&self) -> u64 {
        self.priority
    }

    fn is_cancelled(&self) -> bool {
        self.cancellation.load(Ordering::Acquire)
    }
}

impl BatchCommandEntry {
    fn fail<F>(self, error: F)
    where
        F: FnOnce() -> Error,
    {
        let error = error();
        self.telemetry.complete(Some(&error));
        let _ = self.response_sender.send(Err(error));
    }
}

/// One sendable group for either the direct stream or one forwarding host.
pub(crate) struct BatchCommandGroup {
    pub(crate) forwarded_host: String,
    pub(crate) request: tikvpb::BatchCommandsRequest,
    pub(crate) entries: Vec<BatchCommandEntry>,
}

impl BatchCommandGroup {
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Publishes every entry before attempting the stream send. If the stream
    /// channel is already closed, only this group's IDs are retired, leaving a
    /// potential server response from an ambiguous earlier send classified as
    /// outdated rather than routed to another caller.
    pub(crate) async fn publish(
        self,
        pending: &BatchPendingResponses,
        outbound: &mpsc::Sender<tikvpb::BatchCommandsRequest>,
        target: &str,
        connection_index: usize,
    ) -> Result<()> {
        self.publish_with_progress(
            pending,
            outbound,
            target,
            connection_index,
            Arc::new(BatchStreamProgress::default()),
        )
        .await
    }

    async fn publish_with_progress(
        mut self,
        pending: &BatchPendingResponses,
        outbound: &mpsc::Sender<tikvpb::BatchCommandsRequest>,
        target: &str,
        connection_index: usize,
        progress: Arc<BatchStreamProgress>,
    ) -> Result<()> {
        assert_eq!(
            self.request.request_ids.len(),
            self.entries.len(),
            "BatchCommands group IDs and entries must stay aligned"
        );
        let ids = self.request.request_ids.clone();
        let send_started_at = Instant::now();
        let batch_size = self.entries.len();
        let send_state = Arc::new(BatchRequestSendState {
            batch_size,
            send_started_at,
            sent_after_start_ns: AtomicU64::new(0),
            first_response_after_start_ns: AtomicU64::new(0),
        });
        for (id, entry) in ids.iter().zip(self.entries) {
            entry.telemetry.mark_send_started(
                send_state.clone(),
                *id,
                self.forwarded_host.clone(),
                progress.clone(),
            );
            pending.register_sender_with_telemetry(
                *id,
                self.forwarded_host.clone(),
                entry.response_sender,
                entry.telemetry,
                Some(BatchStreamMetricLabels {
                    progress: progress.clone(),
                    ..BatchStreamMetricLabels::new(
                        target.to_owned(),
                        connection_index,
                        !self.forwarded_host.is_empty(),
                    )
                }),
            );
        }
        increment_batch_stream_request_counter(
            target,
            connection_index,
            !self.forwarded_host.is_empty(),
            BatchStreamRequestCounter::Tracked,
            ids.len(),
        );
        self.request.client_send_time_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |duration| {
                u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
            });
        outbound.send(self.request).await.map_err(|_| {
            pending.fail_ids(&ids, || {
                Error::StringError("BatchCommands stream request channel closed".to_owned())
            });
            Error::StringError("BatchCommands stream request channel closed".to_owned())
        })?;
        pending.mark_sent(&ids, Instant::now());
        Ok(())
    }

    /// Delivers a stream-creation failure to entries which have not yet been
    /// published. Unlike a send failure, no request ID is in flight yet.
    fn fail<F>(self, mut error: F)
    where
        F: FnMut() -> Error,
    {
        for entry in self.entries {
            entry.fail(&mut error);
        }
    }
}

/// An owned queue and task for the source batch-send loop. The existing
/// `enqueue`/`flush` pair remains available for deterministic tests; normal
/// scheduling should use this worker so collection owns timing.
pub(crate) struct BatchCommandsWorker {
    sender: mpsc::Sender<BatchCommandEntry>,
    task: JoinHandle<()>,
    dispatcher: Arc<BatchCommandsDispatcher>,
}

const BATCH_IDLE_TIMEOUT: Duration = Duration::from_secs(3 * 60);

impl BatchCommandsWorker {
    pub(crate) async fn submit_until(
        &self,
        request: BatchCommandRequest,
        priority: u64,
        forwarded_host: impl Into<String>,
        deadline: tokio::time::Instant,
    ) -> Result<BatchCommandSubmission> {
        let (entry, submission) = BatchCommandsBuilder::entry(request, priority, forwarded_host);
        if self.dispatcher.cancellation.is_cancelled() {
            entry.fail(|| Error::StringError("batch client closed".to_owned()));
            return Ok(submission);
        }
        let reservation = tokio::select! {
            _ = self.dispatcher.cancellation.cancelled() => {
                entry.fail(|| Error::StringError("batch client closed".to_owned()));
                return Ok(submission);
            }
            reservation = tokio::time::timeout_at(deadline, self.sender.reserve()) => reservation,
        };
        match reservation {
            Ok(Ok(permit)) => permit.send(entry),
            Ok(Err(_)) => {
                entry.fail(|| Error::StringError("batch client closed".to_owned()));
            }
            Err(_) => {
                entry.fail(|| Error::GrpcAPI(tonic::Status::deadline_exceeded("wait sendLoop")));
                return Err(Error::GrpcAPI(tonic::Status::deadline_exceeded(
                    "wait sendLoop",
                )));
            }
        }
        Ok(submission)
    }

    pub(crate) async fn submit(
        &self,
        request: BatchCommandRequest,
        priority: u64,
        forwarded_host: impl Into<String>,
    ) -> BatchCommandSubmission {
        let (entry, submission) = BatchCommandsBuilder::entry(request, priority, forwarded_host);
        if self.dispatcher.cancellation.is_cancelled() {
            entry.fail(|| Error::StringError("batch client closed".to_owned()));
            return submission;
        }
        if let Err(error) = self.sender.send(entry).await {
            error
                .0
                .fail(|| Error::StringError("batch client closed".to_owned()));
        }
        submission
    }

    /// Mirrors `batchConn.Close`: subsequent submissions fail, the collector
    /// drains queued entries, and published entries are retired by the shared
    /// dispatcher shutdown path.
    pub(crate) fn close(&self) {
        self.dispatcher.close_now();
    }
}

impl Drop for BatchCommandsWorker {
    fn drop(&mut self) {
        self.dispatcher.close_now();
        self.task.abort();
    }
}

/// Source receive-loop accounting for one protobuf batch response. It is kept
/// separate from transport timing/metrics until the stream lifecycle owns
/// those values as well.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct BatchReceiveSummary {
    pub(crate) delivered: usize,
    pub(crate) cancelled: usize,
    pub(crate) outdated: usize,
    pub(crate) highest_request_id: Option<u64>,
    pub(crate) transport_layer_load: u64,
}

#[derive(Debug)]
struct BatchReceiveError {
    error: Error,
    summary: BatchReceiveSummary,
}

impl std::fmt::Display for BatchReceiveError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.error, formatter)
    }
}

impl std::error::Error for BatchReceiveError {}

enum BatchReceiveLoopExit {
    Stopped,
    Failed(Error),
}

fn duplicate_batch_failure(error: &Error) -> Error {
    match error {
        Error::GrpcAPI(status) => Error::GrpcAPI(status.clone()),
        Error::Connection {
            source,
            address,
            version,
        } => Error::Connection {
            source: Box::new(duplicate_batch_failure(source)),
            address: address.clone(),
            version: *version,
        },
        Error::StringError(message) => Error::StringError(message.clone()),
        error => Error::StringError(error.to_string()),
    }
}

fn claim_batch_recovery_epoch(epoch: &AtomicU64, observed_epoch: &mut u64) -> bool {
    match epoch.compare_exchange(
        *observed_epoch,
        observed_epoch.wrapping_add(1),
        Ordering::AcqRel,
        Ordering::Acquire,
    ) {
        Ok(_) => {
            *observed_epoch = observed_epoch.wrapping_add(1);
            true
        }
        Err(current) => {
            *observed_epoch = current;
            false
        }
    }
}

/// Correlates all IDs in one received protobuf batch. Extra responses without
/// IDs are ignored. For a malformed short response, the valid prefix is
/// retired before the missing index is reported, preserving client-go's
/// observable loop order before its receive supervisor recovers the panic.
fn receive_batch_response(
    pending: &BatchPendingResponses,
    response: tikvpb::BatchCommandsResponse,
) -> std::result::Result<BatchReceiveSummary, BatchReceiveError> {
    let request_id_count = response.request_ids.len();
    let response_count = response.responses.len();
    let mut responses = response.responses.into_iter();
    let mut summary = BatchReceiveSummary {
        transport_layer_load: response.transport_layer_load,
        ..Default::default()
    };
    for id in response.request_ids {
        let Some(response) = responses.next() else {
            return Err(BatchReceiveError {
                error: Error::InternalError {
                    message: format!(
                        "BatchCommands response has {request_id_count} request IDs but only {response_count} responses"
                    ),
                },
                summary,
            });
        };
        summary.highest_request_id = Some(summary.highest_request_id.map_or(id, |max| max.max(id)));
        match pending.complete_result(id, BatchCommandResponse::from_proto(response)) {
            BatchResponseDisposition::Delivered => summary.delivered += 1,
            BatchResponseDisposition::Cancelled => summary.cancelled += 1,
            BatchResponseDisposition::Outdated => summary.outdated += 1,
        }
    }
    Ok(summary)
}

/// Runs the response half of one direct or forwarded batch stream. A stream
/// failure is deliberately isolated to its forwarding host: client-go keeps
/// sibling forwarding streams alive on the same pooled connection.
async fn run_batch_receive_loop<S>(
    mut responses: S,
    pending: Arc<BatchPendingResponses>,
    forwarded_host: String,
    target: &str,
    connection_index: usize,
    progress: Arc<BatchStreamProgress>,
    cancellation: Cancellation,
    transport_layer_load: Arc<AtomicU64>,
    event_listener: Arc<std::sync::RwLock<Option<Arc<dyn ClientEventListener>>>>,
) -> BatchReceiveLoopExit
where
    S: futures::Stream<Item = std::result::Result<tikvpb::BatchCommandsResponse, tonic::Status>>
        + Unpin,
{
    run_batch_receive_loop_generation(
        &mut responses,
        &pending,
        &forwarded_host,
        target,
        connection_index,
        &progress,
        &cancellation,
        &transport_layer_load,
        &event_listener,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_batch_receive_loop_generation<S>(
    responses: &mut S,
    pending: &BatchPendingResponses,
    forwarded_host: &str,
    target: &str,
    connection_index: usize,
    progress: &BatchStreamProgress,
    cancellation: &Cancellation,
    transport_layer_load: &AtomicU64,
    event_listener: &std::sync::RwLock<Option<Arc<dyn ClientEventListener>>>,
) -> BatchReceiveLoopExit
where
    S: futures::Stream<Item = std::result::Result<tikvpb::BatchCommandsResponse, tonic::Status>>
        + Unpin,
{
    use futures::StreamExt;

    let forwarded = !forwarded_host.is_empty();
    loop {
        let receive_started = Instant::now();
        let response = tokio::select! {
            _ = cancellation.cancelled() => return BatchReceiveLoopExit::Stopped,
            response = responses.next() => response,
        };
        let receive_duration = receive_started.elapsed();
        observe_batch_stream_recv_loop(
            target,
            connection_index,
            forwarded,
            "recv",
            receive_duration,
        );
        if receive_duration > BATCH_RECV_TAIL_LATENCY_THRESHOLD {
            observe_batch_stream_tail(
                target,
                connection_index,
                forwarded,
                BatchStreamTailKind::Receive,
                receive_duration,
            );
        }
        let Some(response) = response else {
            return BatchReceiveLoopExit::Failed(Error::StringError(
                "BatchCommands response stream closed".to_owned(),
            ));
        };
        match response {
            Ok(response) => {
                if response.tikv_send_time_ns != 0 {
                    let tikv_send_time = Duration::from_nanos(response.tikv_send_time_ns);
                    if let Ok(elapsed_since_epoch) =
                        SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)
                    {
                        let estimated_one_way_delay =
                            elapsed_since_epoch.saturating_sub(tikv_send_time);
                        if estimated_one_way_delay > BATCH_RECV_TAIL_LATENCY_THRESHOLD / 2 {
                            observe_batch_stream_tail(
                                target,
                                connection_index,
                                forwarded,
                                BatchStreamTailKind::TikvSend,
                                estimated_one_way_delay,
                            );
                        }
                    }
                }
                let process_started = Instant::now();
                if let Some(feedback) = response.health_feedback.as_ref() {
                    if let Some(listener) = event_listener.read().unwrap().clone() {
                        listener.on_health_feedback(feedback);
                    }
                }
                let result = receive_batch_response(pending, response);
                if let Ok(summary) = &result {
                    if let Some(highest_request_id) = summary.highest_request_id {
                        progress.observe_response_ids(&[highest_request_id]);
                    }
                    // The collection policy ignores this value unless
                    // overload batching is enabled, matching client-go's
                    // conditional use of transport-layer feedback.
                    if summary.transport_layer_load > 0 {
                        transport_layer_load.store(summary.transport_layer_load, Ordering::Release);
                    }
                    increment_batch_stream_request_counter(
                        target,
                        connection_index,
                        forwarded,
                        BatchStreamRequestCounter::Outdated,
                        summary.outdated,
                    );
                }
                if let Err(error) = result {
                    increment_batch_stream_request_counter(
                        target,
                        connection_index,
                        forwarded,
                        BatchStreamRequestCounter::Outdated,
                        error.summary.outdated,
                    );
                    // client-go indexes the response slice while iterating
                    // request IDs. A short response therefore panics only
                    // after delivering its valid prefix; the outer receive
                    // supervisor catches that panic and resumes `Recv` on the
                    // same stream without retiring the remaining entries.
                    panic!("{error}");
                }
                observe_batch_stream_recv_loop(
                    target,
                    connection_index,
                    forwarded,
                    "process",
                    process_started.elapsed(),
                );
            }
            Err(status) => {
                return BatchReceiveLoopExit::Failed(Error::GrpcAPI(status));
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_batch_receive_loop_recovering_panics<S>(
    mut responses: S,
    pending: Arc<BatchPendingResponses>,
    forwarded_host: String,
    target: &str,
    connection_index: usize,
    progress: Arc<BatchStreamProgress>,
    cancellation: Cancellation,
    transport_layer_load: Arc<AtomicU64>,
    event_listener: Arc<std::sync::RwLock<Option<Arc<dyn ClientEventListener>>>>,
) -> BatchReceiveLoopExit
where
    S: futures::Stream<Item = std::result::Result<tikvpb::BatchCommandsResponse, tonic::Status>>
        + Unpin,
{
    loop {
        let result = AssertUnwindSafe(run_batch_receive_loop_generation(
            &mut responses,
            &pending,
            &forwarded_host,
            target,
            connection_index,
            &progress,
            &cancellation,
            &transport_layer_load,
            &event_listener,
        ))
        .catch_unwind()
        .await;
        match result {
            Ok(stop) => return stop,
            Err(_) => {
                // client-go's deferred recovery starts another recv loop over
                // the existing stream. Pending requests remain owned by that
                // stream and are retired only by a later response, stream
                // error, explicit close, or caller timeout.
                crate::stats::increment_batch_receive_loop_panic();
            }
        }
    }
}

/// Owned bidirectional stream for exactly one forwarding destination on one
/// selected pool channel. Its supervisor resumes receive-loop panics on the
/// same stream and participates in the connection-wide recovery epoch after
/// an actual stream failure.
pub(crate) struct BatchCommandsStream {
    forwarded_host: String,
    target: String,
    connection_index: usize,
    progress: Arc<BatchStreamProgress>,
    pending: Arc<BatchPendingResponses>,
    state: Arc<AsyncMutex<BatchStreamState>>,
    ready: Arc<tokio::sync::Notify>,
    cancellation: Cancellation,
    supervisor: JoinHandle<()>,
}

struct BatchStreamState {
    outbound: Option<mpsc::Sender<tikvpb::BatchCommandsRequest>>,
}

async fn begin_batch_recovery(
    reconnect_gate: Arc<AsyncMutex<()>>,
    state: &AsyncMutex<BatchStreamState>,
    ready: &tokio::sync::Notify,
) -> tokio::sync::OwnedMutexGuard<()> {
    // client-go's lockForRecreate first excludes batchSendLoop and only then
    // makes the stream unavailable. Clearing `outbound` before this gate can
    // make a finite-limit sender wait for readiness while it holds the same
    // gate, preventing recovery forever.
    let reconnect_guard = reconnect_gate.lock_owned().await;
    state.lock().await.outbound = None;
    ready.notify_waiters();
    reconnect_guard
}

type BatchOpenFuture = futures::future::BoxFuture<
    'static,
    Result<tonic::codec::Streaming<tikvpb::BatchCommandsResponse>>,
>;

impl BatchCommandsStream {
    pub(crate) fn open_on(
        client: &KvRpcClient,
        connection_index: usize,
        forwarded_host: impl Into<String>,
        pending: Arc<BatchPendingResponses>,
        queue_capacity: usize,
        cancellation: Cancellation,
        reconnect_gate: Arc<AsyncMutex<()>>,
        connection_epoch: Arc<AtomicU64>,
        transport_layer_load: Arc<AtomicU64>,
        event_listener: Arc<std::sync::RwLock<Option<Arc<dyn ClientEventListener>>>>,
    ) -> Result<Self> {
        assert_ne!(
            queue_capacity, 0,
            "batch stream queue capacity must be nonzero"
        );
        let forwarded_host = forwarded_host.into();
        let target = client.batch_metric_target();
        let progress = Arc::new(BatchStreamProgress::default());
        let (outbound, responses) =
            Self::start_generation(client, connection_index, &forwarded_host, queue_capacity);
        let state = Arc::new(AsyncMutex::new(BatchStreamState {
            outbound: Some(outbound),
        }));
        let ready = Arc::new(tokio::sync::Notify::new());
        let supervisor = tokio::spawn(Self::supervise(
            client.clone(),
            connection_index,
            progress.clone(),
            pending.clone(),
            forwarded_host.clone(),
            queue_capacity,
            state.clone(),
            ready.clone(),
            reconnect_gate.clone(),
            cancellation.clone(),
            connection_epoch,
            transport_layer_load,
            event_listener,
            responses,
        ));
        Ok(Self {
            forwarded_host,
            target,
            connection_index,
            progress,
            pending,
            state,
            ready,
            cancellation,
            supervisor,
        })
    }

    fn start_generation(
        client: &KvRpcClient,
        connection_index: usize,
        forwarded_host: &str,
        queue_capacity: usize,
    ) -> (mpsc::Sender<tikvpb::BatchCommandsRequest>, BatchOpenFuture) {
        let (outbound, inbound) = mpsc::channel(queue_capacity);
        let requests = futures::stream::unfold(inbound, |mut inbound| async move {
            inbound.recv().await.map(|request| (request, inbound))
        });
        let client = client.clone();
        let forwarded_host = forwarded_host.to_owned();
        let responses = async move {
            client
                .open_batch_commands_on(connection_index, &forwarded_host, requests)
                .await
        }
        .boxed();
        (outbound, responses)
    }

    #[allow(clippy::too_many_arguments)]
    async fn supervise(
        client: KvRpcClient,
        connection_index: usize,
        progress: Arc<BatchStreamProgress>,
        pending: Arc<BatchPendingResponses>,
        forwarded_host: String,
        queue_capacity: usize,
        state: Arc<AsyncMutex<BatchStreamState>>,
        ready: Arc<tokio::sync::Notify>,
        reconnect_gate: Arc<AsyncMutex<()>>,
        cancellation: Cancellation,
        connection_epoch: Arc<AtomicU64>,
        transport_layer_load: Arc<AtomicU64>,
        event_listener: Arc<std::sync::RwLock<Option<Arc<dyn ClientEventListener>>>>,
        mut opening: BatchOpenFuture,
    ) {
        let target = client.batch_metric_target();
        let mut observed_epoch = connection_epoch.load(Ordering::Acquire);
        loop {
            let failure = match opening.await {
                Ok(responses) => {
                    match run_batch_receive_loop_recovering_panics(
                        responses,
                        pending.clone(),
                        forwarded_host.clone(),
                        &target,
                        connection_index,
                        progress.clone(),
                        cancellation.clone(),
                        transport_layer_load.clone(),
                        event_listener.clone(),
                    )
                    .await
                    {
                        BatchReceiveLoopExit::Stopped => return,
                        BatchReceiveLoopExit::Failed(error) => error,
                    }
                }
                Err(error) => error,
            };
            client.mark_connection_transient_failure(connection_index);

            // client-go advances one epoch per pooled connection, so sibling
            // direct/forwarded streams cannot simultaneously run independent
            // transport retry loops. They still each reopen their own stream
            // once the shared channel is available.
            let _reconnect_guard =
                begin_batch_recovery(reconnect_gate.clone(), &state, &ready).await;
            let unavailable_started = Instant::now();

            let recovery_leader =
                claim_batch_recovery_epoch(&connection_epoch, &mut observed_epoch);
            if !recovery_leader {
                // Another direct/forwarded stream already advanced this
                // pooled connection's epoch. client-go recreates this sibling
                // stream once without retiring its pending requests or
                // running a second transport backoff loop.
                if cancellation.is_cancelled() {
                    return;
                }
                let establish_started = Instant::now();
                let (outbound, reopen) = Self::start_generation(
                    &client,
                    connection_index,
                    &forwarded_host,
                    queue_capacity,
                );
                state.lock().await.outbound = Some(outbound);
                ready.notify_waiters();
                let reopened = tokio::time::timeout(TIKV_DIAL_TIMEOUT, reopen).await;
                observe_batch_client_wait_establish(establish_started.elapsed());
                observe_batch_client_unavailable(unavailable_started.elapsed());
                match reopened {
                    Ok(Ok(reopened)) => {
                        progress.reset();
                        opening = futures::future::ready(Ok(reopened)).boxed();
                    }
                    Ok(Err(error)) => {
                        state.lock().await.outbound = None;
                        ready.notify_waiters();
                        opening = futures::future::ready(Err(error)).boxed();
                    }
                    Err(_) => {
                        state.lock().await.outbound = None;
                        ready.notify_waiters();
                        opening = futures::future::ready(Err(Error::GrpcAPI(
                            tonic::Status::deadline_exceeded(format!(
                                "BatchCommands reconnect timed out after {:?}",
                                TIKV_DIAL_TIMEOUT
                            )),
                        )))
                        .boxed();
                    }
                }
                continue;
            }

            pending.fail_for_host(&forwarded_host, || duplicate_batch_failure(&failure));
            let mut backoffer = RetryBackoffer::new(cancellation.child(), i32::MAX as u64);
            loop {
                if cancellation.is_cancelled() {
                    return;
                }
                let establish_started = Instant::now();
                let (outbound, reopen) = Self::start_generation(
                    &client,
                    connection_index,
                    &forwarded_host,
                    queue_capacity,
                );
                state.lock().await.outbound = Some(outbound);
                ready.notify_waiters();
                let reopen = tokio::time::timeout(TIKV_DIAL_TIMEOUT, reopen).await;
                observe_batch_client_wait_establish(establish_started.elapsed());
                match reopen {
                    Ok(Ok(reopened)) => {
                        progress.reset();
                        opening = futures::future::ready(Ok(reopened)).boxed();
                        observe_batch_client_unavailable(unavailable_started.elapsed());
                        break;
                    }
                    Ok(Err(error)) => {
                        state.lock().await.outbound = None;
                        ready.notify_waiters();
                        if backoffer
                            .backoff(BO_TIKV_RPC, error.to_string())
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    Err(_) => {
                        state.lock().await.outbound = None;
                        ready.notify_waiters();
                        if backoffer
                            .backoff(
                                BO_TIKV_RPC,
                                format!(
                                    "BatchCommands reconnect timed out after {:?}",
                                    TIKV_DIAL_TIMEOUT
                                ),
                            )
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                }
            }
        }
    }

    pub(crate) async fn publish(
        &self,
        group: BatchCommandGroup,
        fast_fail_when_unavailable: bool,
    ) -> Result<()> {
        assert_eq!(
            group.forwarded_host, self.forwarded_host,
            "BatchCommands group must use its matching forwarding stream"
        );
        loop {
            let state = self.state.lock().await;
            if let Some(outbound) = state.outbound.as_ref() {
                // Holding the state lock across `send` prevents a receive
                // failure from retiring the old generation between request-ID
                // publication and the outbound handoff.
                return group
                    .publish_with_progress(
                        &self.pending,
                        outbound,
                        &self.target,
                        self.connection_index,
                        self.progress.clone(),
                    )
                    .await;
            }
            drop(state);
            if fast_fail_when_unavailable {
                crate::stats::increment_no_available_batch_connection();
                group.fail(|| Error::StringError("no available connections".to_owned()));
                return Err(Error::StringError("no available connections".to_owned()));
            }
            tokio::select! {
                _ = self.ready.notified() => {}
                _ = self.cancellation.cancelled() => {
                    return Err(Error::StringError("batch client closed".to_owned()));
                }
            }
        }
    }
}

impl Drop for BatchCommandsStream {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.supervisor.abort();
    }
}

/// Source batch-connection coordinator. A flush scans pool slots in source
/// round-robin order, then all direct and forwarded groups selected for one
/// slot reuse distinct streams on that slot. It owns adaptive wait,
/// connection-local concurrency admission, and stream recreation.
#[allow(dead_code)]
pub(crate) struct BatchCommandsDispatcher {
    client: KvRpcClient,
    pending: Arc<BatchPendingResponses>,
    builder: Mutex<BatchCommandsBuilder>,
    streams: AsyncMutex<HashMap<usize, HashMap<String, BatchCommandsStream>>>,
    reconnect_gates: Mutex<HashMap<usize, Arc<AsyncMutex<()>>>>,
    connection_epochs: Mutex<HashMap<usize, Arc<AtomicU64>>>,
    queue_capacity: usize,
    max_concurrency_request_limit: usize,
    cancellation: Cancellation,
    transport_layer_load: Arc<AtomicU64>,
    #[cfg(test)]
    panic_next_send_loop: AtomicBool,
    #[cfg(test)]
    panic_after_collect: AtomicBool,
}

#[allow(dead_code)]
impl BatchCommandsDispatcher {
    /// Creates the per-store batch dispatcher only when client-go would
    /// enable batching for the configured `max-batch-size`.
    pub(crate) fn from_config(
        client: KvRpcClient,
        config: &crate::config::TiKvClient,
    ) -> Option<Self> {
        let queue_capacity = usize::try_from(config.max_batch_size).ok()?;
        (queue_capacity != 0).then(|| {
            Self::new_with_concurrency(
                client,
                queue_capacity,
                usize::try_from(config.max_concurrency_request_limit).unwrap_or(usize::MAX),
            )
        })
    }

    /// Starts the source-style queue worker. The worker is intentionally
    /// constructed from an `Arc` so its task and callers share exactly one
    /// dispatcher/stream registry.
    pub(crate) fn spawn_worker(
        self: Arc<Self>,
        config: &crate::config::TiKvClient,
    ) -> BatchCommandsWorker {
        self.spawn_worker_with_idle_timeout(config, BATCH_IDLE_TIMEOUT)
    }

    fn spawn_worker_with_idle_timeout(
        self: Arc<Self>,
        config: &crate::config::TiKvClient,
        idle_timeout: Duration,
    ) -> BatchCommandsWorker {
        let capacity =
            usize::try_from(config.max_batch_size).expect("enabled batch size fits usize");
        let (sender, mut receiver) = mpsc::channel::<BatchCommandEntry>(capacity);
        let config = config.clone();
        let transport_layer_load = self.transport_layer_load.clone();
        let dispatcher = self.clone();
        let task = tokio::spawn(async move {
            // These are connection-owned in client-go. Keeping them outside
            // the caught generation preserves already-collected entries and
            // the latest-arrival boundary if the send loop panics.
            let mut collector = BatchRequestCollector::new();
            let mut collected = BatchCommandsBuilder::new();
            // client-go owns this timer at the batch-connection level. It is
            // reset as soon as a head request is accepted and is not reset by
            // either a completed send or send-loop panic recovery.
            let mut idle_deadline = tokio::time::Instant::now() + idle_timeout;
            loop {
                let generation = AssertUnwindSafe(async {
                    #[cfg(test)]
                    if dispatcher
                        .panic_next_send_loop
                        .swap(false, Ordering::AcqRel)
                    {
                        panic!("source batch-send-loop panic injection");
                    }
                    let mut policy = BatchCollectionPolicy::from_config(&config);
                    let initial_inspect_age = Duration::from_nanos(
                        rand::thread_rng().gen_range(
                            0..u64::try_from(BATCH_REQUEST_INSPECT_INTERVAL.as_nanos())
                                .expect("batch inspection interval fits u64 nanoseconds"),
                        ),
                    );
                    let mut last_pending_inspect_at = Instant::now() - initial_inspect_age;
                    loop {
                        collected.reset();
                        let send_loop_started = Instant::now();
                        let mut idle = false;
                        let head = tokio::select! {
                        _ = dispatcher.cancellation.cancelled() => None,
                        _ = tokio::time::sleep_until(idle_deadline) => {
                            idle = true;
                            None
                        }
                        head = receiver.recv() => head,
                            };
                        let Some(head) = head else {
                            // client-go inspects pending published entries before a
                            // closed or idle batch-send loop exits. Those entries may
                            // still be waiting in a live stream even though no new
                            // candidate was collected.
                            dispatcher.inspect_pending_requests(Instant::now());
                            if idle {
                                // client-go's batch loop marks its pool idle, then
                                // the next request recycles it. Closing this shared
                                // pool state makes the next Rust dispatch take the
                                // existing connection-error retry/replacement path.
                                let recycle_started = Instant::now();
                                KvClient::close(&dispatcher.client);
                                dispatcher.close_now();
                                observe_batch_client_recycle(recycle_started.elapsed());
                            }
                            collected
                                .cancel(|| Error::StringError("batch client closed".to_owned()));
                            while let Ok(entry) = receiver.try_recv() {
                                entry.fail(|| Error::StringError("batch client closed".to_owned()));
                            }
                            return;
                        };
                        idle_deadline = tokio::time::Instant::now() + idle_timeout;
                        collector
                            .collect_head_with_policy(
                                &mut receiver,
                                &mut collected,
                                &mut policy,
                                transport_layer_load.load(Ordering::Acquire),
                                head,
                            )
                            .await;
                        #[cfg(test)]
                        if dispatcher.panic_after_collect.swap(false, Ordering::AcqRel) {
                            panic!("source batch-send-loop post-collect panic injection");
                        }
                        let target = dispatcher.client.batch_metric_target();
                        let head_received_at =
                            collector.last_head_received_at.unwrap_or(send_loop_started);
                        observe_batch_pending_requests(&target, receiver.len() + collected.len());
                        observe_batch_best_size(&target, policy.average_batch_wait_size);
                        observe_batch_head_arrival_interval(
                            &target,
                            collector.last_head_arrival_interval.unwrap_or_default(),
                        );
                        observe_batch_send_loop(
                            &target,
                            "wait-head",
                            head_received_at.saturating_duration_since(send_loop_started),
                        );
                        observe_batch_send_loop(&target, "wait-more", send_loop_started.elapsed());
                        if collector.last_waited_for_overload {
                            crate::stats::increment_batch_wait_overload();
                        }
                        if let Some(extra) = collector.last_extra_fetched {
                            observe_batch_more_requests(&target, extra);
                        }
                        let entries = collected.entries.take(collected.len());
                        {
                            let mut builder = dispatcher.builder.lock().unwrap();
                            for entry in entries {
                                builder.entries.push(entry);
                            }
                        }
                        let _ = dispatcher.flush().await;
                        observe_batch_send_loop(&target, "send", send_loop_started.elapsed());
                        let send_tail = head_received_at.elapsed();
                        if send_tail > BATCH_SEND_TAIL_LATENCY_THRESHOLD {
                            observe_batch_send_tail(&target, send_tail);
                        }
                        let now = Instant::now();
                        if now.duration_since(last_pending_inspect_at)
                            >= BATCH_REQUEST_INSPECT_INTERVAL
                        {
                            dispatcher.inspect_pending_requests(now);
                            last_pending_inspect_at = now;
                        }
                    }
                })
                .catch_unwind()
                .await;
                if generation.is_ok() {
                    return;
                }
                crate::stats::increment_batch_send_loop_panic();
            }
        });
        BatchCommandsWorker {
            sender,
            task,
            dispatcher: self,
        }
    }

    pub(crate) fn new(client: KvRpcClient, queue_capacity: usize) -> Self {
        Self::new_with_concurrency(client, queue_capacity, usize::MAX)
    }

    pub(crate) fn new_with_concurrency(
        client: KvRpcClient,
        queue_capacity: usize,
        max_concurrency_request_limit: usize,
    ) -> Self {
        assert_ne!(
            queue_capacity, 0,
            "batch stream queue capacity must be nonzero"
        );
        Self {
            client,
            pending: Arc::new(BatchPendingResponses::new()),
            builder: Mutex::new(BatchCommandsBuilder::new()),
            streams: AsyncMutex::new(HashMap::new()),
            reconnect_gates: Mutex::new(HashMap::new()),
            connection_epochs: Mutex::new(HashMap::new()),
            queue_capacity,
            max_concurrency_request_limit,
            cancellation: Cancellation::default(),
            transport_layer_load: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            panic_next_send_loop: AtomicBool::new(false),
            #[cfg(test)]
            panic_after_collect: AtomicBool::new(false),
        }
    }

    pub(crate) fn enqueue(
        &self,
        request: BatchCommandRequest,
        priority: u64,
        forwarded_host: impl Into<String>,
    ) -> BatchCommandSubmission {
        self.builder
            .lock()
            .unwrap()
            .push(request, priority, forwarded_host)
    }

    /// Builds and publishes at most the source normal-priority limit, while
    /// still admitting every queued high-priority entry. Returns the number
    /// of individual RPCs selected into this flush.
    pub(crate) async fn flush(&self) -> Result<usize> {
        if self.cancellation.is_cancelled() {
            return Err(Error::StringError("batch client closed".to_owned()));
        }

        let has_high_priority_task = self.builder.lock().unwrap().has_high_priority_task();
        let Some((connection_index, normal_limit, _send_guard)) = self
            .select_connection_for_send(has_high_priority_task)
            .await
        else {
            crate::stats::increment_no_available_batch_connection();
            if self.max_concurrency_request_limit == i64::MAX as usize {
                self.builder
                    .lock()
                    .unwrap()
                    .cancel(|| Error::StringError("no available connections".to_owned()));
            }
            // With an explicit finite limit client-go leaves the candidates
            // queued until another head wakes the send loop (or callers time
            // out). The legacy/default limit fails them immediately above.
            return Ok(0);
        };
        let (direct, forwarded) = self.builder.lock().unwrap().build_with_limit(normal_limit);
        let groups = direct
            .into_iter()
            .chain(forwarded.into_values())
            .collect::<Vec<_>>();
        if groups.is_empty() {
            return Ok(0);
        }
        let reconnect_gate = self.reconnect_gate(connection_index);
        let mut published = 0;
        let mut first_error = None;
        let fast_fail_when_unavailable = self.max_concurrency_request_limit == i64::MAX as usize;

        for group in groups {
            let group_len = group.len();
            let forwarded_host = group.forwarded_host.clone();
            let result = {
                let mut streams = self.streams.lock().await;
                match streams
                    .entry(connection_index)
                    .or_default()
                    .entry(forwarded_host.clone())
                {
                    std::collections::hash_map::Entry::Occupied(entry) => {
                        entry
                            .into_mut()
                            .publish(group, fast_fail_when_unavailable)
                            .await
                    }
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        match BatchCommandsStream::open_on(
                            &self.client,
                            connection_index,
                            forwarded_host,
                            self.pending.clone(),
                            self.queue_capacity,
                            self.cancellation.child(),
                            reconnect_gate.clone(),
                            self.connection_epoch(connection_index),
                            self.transport_layer_load.clone(),
                            self.client.event_listener(),
                        ) {
                            Ok(stream) => {
                                entry
                                    .insert(stream)
                                    .publish(group, fast_fail_when_unavailable)
                                    .await
                            }
                            Err(error) => {
                                let message = error.to_string();
                                group.fail(|| Error::StringError(message.clone()));
                                Err(error)
                            }
                        }
                    }
                }
            };
            match result {
                Ok(()) => published += group_len,
                Err(error) => {
                    // `getClientAndSend` attempts the direct group and every
                    // forwarding group independently. Retire this group's
                    // entries but continue so a sibling never hangs merely
                    // because another forwarding destination is invalid.
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        if published != 0 {
            observe_batch_requests(&self.client.batch_metric_target(), published);
        }
        first_error.map_or(Ok(published), Err)
    }

    fn reconnect_gate(&self, connection_index: usize) -> Arc<AsyncMutex<()>> {
        self.reconnect_gates
            .lock()
            .unwrap()
            .entry(connection_index)
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    }

    fn connection_epoch(&self, connection_index: usize) -> Arc<AtomicU64> {
        self.connection_epochs
            .lock()
            .unwrap()
            .entry(connection_index)
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone()
    }

    async fn connection_streams_ready(&self, connection_index: usize) -> bool {
        let streams = self.streams.lock().await;
        let Some(streams) = streams.get(&connection_index) else {
            return true;
        };
        for stream in streams.values() {
            if stream.state.lock().await.outbound.is_none() {
                return false;
            }
        }
        true
    }

    async fn select_connection_for_send(
        &self,
        has_high_priority_task: bool,
    ) -> Option<(usize, usize, tokio::sync::OwnedMutexGuard<()>)> {
        for _ in 0..self.client.batch_connection_count() {
            let connection_index = self.client.next_batch_connection_index();
            let available = self
                .max_concurrency_request_limit
                .saturating_sub(self.pending.len_for_connection(connection_index));
            if !has_high_priority_task && available == 0 {
                continue;
            }
            let Ok(send_guard) = self.reconnect_gate(connection_index).try_lock_owned() else {
                continue;
            };
            if self.connection_streams_ready(connection_index).await {
                return Some((connection_index, available, send_guard));
            }
        }
        None
    }

    /// Stops stream tasks and rejects future flushes. Unlike a recoverable
    /// per-stream failure, explicit source-pool close retires both queued and
    /// published work because all channels have been shut down.
    pub(crate) async fn close(&self) {
        self.close_now();
        self.streams.lock().await.clear();
    }

    fn close_now(&self) {
        self.cancellation.cancel();
        self.builder
            .lock()
            .unwrap()
            .cancel(|| Error::StringError("batch client closed".to_owned()));
        self.pending
            .fail_all(|| Error::StringError("batch client closed".to_owned()));
    }

    fn inspect_pending_requests(&self, now: Instant) {
        let stats = self.pending.inspect(now);
        if let (Some(oldest_id), Some(oldest_wait)) = (stats.oldest_id, stats.oldest_wait) {
            log::warn!(
                "BatchCommands detects slow pending request: target={}, request_id={}, wait={:?}, slow_count={}, slow_unconfirmed_count={}, hanging_count={}, hanging_unconfirmed_count={}",
                self.client.batch_metric_target(),
                oldest_id,
                oldest_wait,
                stats.slow_count,
                stats.slow_unconfirmed_count,
                stats.hanging_count,
                stats.hanging_unconfirmed_count,
            );
        }
    }
}

/// Source-compatible batch selector. IDs remain monotonic over `reset`, just
/// as a Go `batchCommandsBuilder` is retained by its batch connection.
pub(crate) struct BatchCommandsBuilder {
    id_allocator: BatchRequestIdAllocator,
    entries: PriorityQueue<BatchCommandEntry>,
}

impl BatchCommandsBuilder {
    pub(crate) fn new() -> Self {
        Self {
            id_allocator: BatchRequestIdAllocator::default(),
            entries: PriorityQueue::new(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Queues one candidate and returns the token that can cancel it before
    /// selection. Once selected, cancellation is owned by the response path.
    pub(crate) fn push(
        &mut self,
        request: BatchCommandRequest,
        priority: u64,
        forwarded_host: impl Into<String>,
    ) -> BatchCommandSubmission {
        let (entry, submission) = Self::entry(request, priority, forwarded_host);
        self.entries.push(entry);
        submission
    }

    fn entry(
        request: BatchCommandRequest,
        priority: u64,
        forwarded_host: impl Into<String>,
    ) -> (BatchCommandEntry, BatchCommandSubmission) {
        let cancellation = Arc::new(AtomicBool::new(false));
        let (response_sender, response) = oneshot::channel();
        let arrived_at = Instant::now();
        let telemetry = Arc::new(BatchRequestTelemetry::new(request.store_id(), arrived_at));
        let entry = BatchCommandEntry {
            request: Some(request),
            priority,
            forwarded_host: forwarded_host.into(),
            cancellation: cancellation.clone(),
            response_sender,
            arrived_at,
            telemetry: telemetry.clone(),
        };
        let submission = BatchCommandSubmission {
            cancellation: BatchCommandCancellation(cancellation),
            response,
            telemetry: telemetry.clone(),
        };
        (entry, submission)
    }

    fn has_high_priority_task(&self) -> bool {
        self.entries.highest_priority() >= HIGH_TASK_PRIORITY
    }

    /// Builds direct and forwarding request groups with client-go's limit
    /// accounting. Normal requests consume `limit`; high-priority requests do
    /// not. A zero limit therefore admits only high-priority requests.
    pub(crate) fn build_with_limit(
        &mut self,
        limit: usize,
    ) -> (
        Option<BatchCommandGroup>,
        HashMap<String, BatchCommandGroup>,
    ) {
        let mut normal_count = 0;
        let mut direct = BatchCommandGroup {
            forwarded_host: String::new(),
            request: tikvpb::BatchCommandsRequest::default(),
            entries: Vec::new(),
        };
        let mut forwarded = HashMap::new();

        while (normal_count < limit && !self.entries.is_empty()) || self.has_high_priority_task() {
            // `batchCommandsBuilder` asks its priority queue to take one item
            // when the configured limit is zero. That path is reachable only
            // for high-priority work because of the loop condition above.
            let take = limit.max(1);
            for mut entry in self.entries.take(take) {
                if entry.is_cancelled() {
                    entry.fail(|| {
                        Error::StringError(
                            "BatchCommands request cancelled before selection".to_owned(),
                        )
                    });
                    continue;
                }
                if entry.priority < HIGH_TASK_PRIORITY {
                    normal_count += 1;
                }

                let id = self.id_allocator.next();
                entry.telemetry.mark_selected(Instant::now());
                let host = entry.forwarded_host.clone();
                let group = if host.is_empty() {
                    &mut direct
                } else {
                    forwarded
                        .entry(host.clone())
                        .or_insert_with(|| BatchCommandGroup {
                            forwarded_host: host,
                            request: tikvpb::BatchCommandsRequest::default(),
                            entries: Vec::new(),
                        })
                };
                group.request.request_ids.push(id);
                group.request.requests.push(
                    entry
                        .request
                        .take()
                        .expect("a queued batch command has a request")
                        .into_proto(),
                );
                group.entries.push(entry);
            }
        }

        let direct = (!direct.entries.is_empty()).then_some(direct);
        (direct, forwarded)
    }

    /// Retires canceled candidates before the next batch. Selected requests
    /// are owned by returned groups and are intentionally unaffected.
    pub(crate) fn reset(&mut self) {
        // Go's context cancellation settles its callback before `reset`
        // removes the entry. Rust owns that completion sender here, so keep
        // the same terminal outcome instead of silently dropping it.
        for entry in self.entries.take(self.entries.len()) {
            if entry.is_cancelled() {
                entry.fail(|| {
                    Error::StringError(
                        "BatchCommands request cancelled before selection".to_owned(),
                    )
                });
            } else {
                self.entries.push(entry);
            }
        }
    }

    fn cancel<F>(&mut self, mut error: F)
    where
        F: FnMut() -> Error,
    {
        for entry in self.entries.take(self.entries.len()) {
            entry.fail(&mut error);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::kvrpcpb;
    use crate::proto::tikvpb::tikv_client::TikvClient;
    use crate::store::KvClient;
    use std::convert::Infallible;
    use std::pin::Pin;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex;
    use std::task::{Context, Poll};
    use tonic::codegen::{http, Body, BoxFuture, Service, StdError};
    use tonic::transport::{Channel, Endpoint, Server};

    type BatchResponseStream = Pin<
        Box<
            dyn futures::Stream<
                    Item = std::result::Result<tikvpb::BatchCommandsResponse, tonic::Status>,
                > + Send
                + 'static,
        >,
    >;

    /// A deliberately narrow in-process TiKV service. Implementing the full
    /// generated `Tikv` trait would require unrelated RPC stubs; this router
    /// exercises the exact generated BatchCommands gRPC route instead.
    #[derive(Clone)]
    struct BatchOnlyService {
        metadata: Arc<Mutex<Vec<(Option<String>, Option<String>)>>>,
        client_send_times: Arc<Mutex<Vec<u64>>>,
        core: Arc<super::super::mockserver::MockServerCore>,
        close_after_first_stream: bool,
        served_streams: Arc<AtomicUsize>,
    }

    impl tonic::server::StreamingService<tikvpb::BatchCommandsRequest> for BatchOnlyService {
        type Response = tikvpb::BatchCommandsResponse;
        type ResponseStream = BatchResponseStream;
        type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;

        fn call(
            &mut self,
            request: tonic::Request<tonic::Streaming<tikvpb::BatchCommandsRequest>>,
        ) -> Self::Future {
            let forwarded_host = request.metadata().get("tikv-forwarded-host").map(|value| {
                value
                    .to_str()
                    .expect("valid forwarding metadata")
                    .to_owned()
            });
            let connection_index = request
                .metadata()
                .get("tikv-batch-conn-index")
                .map(|value| {
                    value
                        .to_str()
                        .expect("valid connection metadata")
                        .to_owned()
                });
            self.metadata
                .lock()
                .unwrap()
                .push((forwarded_host, connection_index));
            let close_after_response = self.close_after_first_stream
                && self.served_streams.fetch_add(1, Ordering::Relaxed) == 0;
            let core = self.core.clone();
            let client_send_times = self.client_send_times.clone();
            let mut requests = request.into_inner();

            Box::pin(async move {
                // TiKV's grpcio handler does not complete the stream-open
                // future until it has received the first envelope. Keeping
                // this test server equally strict catches clients which wait
                // for response headers before publishing that envelope.
                let first_request = requests.message().await?;
                let responses: BatchResponseStream = Box::pin(futures::stream::unfold(
                    (Some(requests), false, first_request),
                    move |(requests, complete, buffered_request)| {
                        let core = core.clone();
                        let client_send_times = client_send_times.clone();
                        async move {
                            if complete {
                                return None;
                            }
                            let mut requests = requests?;
                            let request = match buffered_request {
                                Some(request) => Some(request),
                                None => match requests.message().await {
                                    Ok(request) => request,
                                    Err(status) => {
                                        return Some((Err(status), (None, true, None)));
                                    }
                                },
                            };
                            match request {
                                Some(request) => {
                                    client_send_times
                                        .lock()
                                        .unwrap()
                                        .push(request.client_send_time_ns);
                                    let responses: Vec<tikvpb::batch_commands_response::Response> =
                                        request
                                            .requests
                                            .into_iter()
                                            .map(|request| {
                                                match request.cmd {
                                    Some(tikvpb::batch_commands_request::request::Cmd::Get(
                                        request,
                                    )) => tikvpb::batch_commands_response::Response {
                                        cmd: Some(
                                            tikvpb::batch_commands_response::response::Cmd::Get(
                                                crate::proto::kvrpcpb::GetResponse {
                                                    value: request.key,
                                                    ..Default::default()
                                                },
                                            ),
                                        ),
                                    },
                                    Some(tikvpb::batch_commands_request::request::Cmd::Empty(
                                        request,
                                    )) => tikvpb::batch_commands_response::Response {
                                        cmd: Some(
                                            tikvpb::batch_commands_response::response::Cmd::Empty(
                                                tikvpb::BatchCommandsEmptyResponse {
                                                    test_id: request.test_id,
                                                },
                                            ),
                                        ),
                                    },
                                    _ => tikvpb::batch_commands_response::Response::default(),
                                }
                                            })
                                            .collect();
                                    let response = if responses.iter().all(|response| {
                                        matches!(
                                    response.cmd,
                                    Some(tikvpb::batch_commands_response::response::Cmd::Empty(_))
                                )
                                    }) {
                                        core.batch_commands(tikvpb::BatchCommandsRequest {
                                            request_ids: request.request_ids,
                                            ..Default::default()
                                        })
                                    } else {
                                        Ok(tikvpb::BatchCommandsResponse {
                                            request_ids: request.request_ids,
                                            responses,
                                            ..Default::default()
                                        })
                                    };
                                    Some((response, (Some(requests), close_after_response, None)))
                                }
                                None => None,
                            }
                        }
                    },
                ));
                Ok(tonic::Response::new(responses))
            })
        }
    }

    #[derive(Clone)]
    struct BatchOnlyServer {
        service: BatchOnlyService,
    }

    impl tonic::server::NamedService for BatchOnlyServer {
        const NAME: &'static str = "tikvpb.Tikv";
    }

    impl<B> Service<http::Request<B>> for BatchOnlyServer
    where
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;

        fn poll_ready(
            &mut self,
            _: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: http::Request<B>) -> Self::Future {
            match request.uri().path() {
                "/tikvpb.Tikv/BatchCommands" => {
                    let service = self.service.clone();
                    Box::pin(async move {
                        let mut grpc =
                            tonic::server::Grpc::new(tonic::codec::ProstCodec::default());
                        Ok(grpc.streaming(service, request).await)
                    })
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(tonic::body::empty_body())
                        .unwrap())
                }),
            }
        }
    }

    fn empty(id: u64) -> BatchCommandRequest {
        BatchCommandRequest::Empty(tikvpb::BatchCommandsEmptyRequest {
            test_id: id,
            delay_time: 0,
        })
    }

    fn queued_entry(builder: &mut BatchCommandsBuilder, id: u64) -> BatchCommandEntry {
        let _submission = builder.push(empty(id), 0, "");
        builder
            .entries
            .take(1)
            .into_iter()
            .next()
            .expect("one queued entry")
    }

    #[tokio::test]
    async fn source_collector_blocks_for_head_then_greedily_drains_to_max_size() {
        let (sender, mut receiver) = mpsc::channel(4);
        let mut source = BatchCommandsBuilder::new();
        sender.send(queued_entry(&mut source, 1)).await.unwrap();
        sender.send(queued_entry(&mut source, 2)).await.unwrap();
        sender.send(queued_entry(&mut source, 3)).await.unwrap();

        let mut collector = BatchRequestCollector::new();
        let mut collected = BatchCommandsBuilder::new();
        assert_eq!(
            collector
                .collect(&mut receiver, &mut collected, 2, 2, None)
                .await,
            Some(None)
        );
        assert_eq!(collected.len(), 2);
        assert_eq!(receiver.len(), 1);
    }

    #[tokio::test]
    async fn source_collector_applies_overload_wait_even_for_first_head() {
        let (sender, mut receiver) = mpsc::channel(4);
        let mut source = BatchCommandsBuilder::new();
        sender.send(queued_entry(&mut source, 1)).await.unwrap();
        let delayed = queued_entry(&mut source, 2);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            sender.send(delayed).await.unwrap();
        });

        let mut config = crate::config::TiKvClient::default();
        config.max_batch_size = 4;
        config.batch_wait_size = 2;
        config.max_batch_wait_time = Duration::from_millis(20);
        config.overload_threshold = 0;
        let mut policy = BatchCollectionPolicy::from_config(&config);
        let mut collector = BatchRequestCollector::new();
        let mut collected = BatchCommandsBuilder::new();
        assert!(
            collector
                .collect_with_policy(&mut receiver, &mut collected, &mut policy, 1)
                .await
        );
        assert_eq!(collected.len(), 2);
    }

    #[test]
    fn source_turbo_batch_policy_presets_and_custom_values_are_preserved() {
        let (basic, valid) = TurboBatchTrigger::from_policy(crate::config::BATCH_POLICY_BASIC);
        assert!(valid);
        assert_eq!(basic.turbo_wait_time(), None);

        let (mut positive, valid) =
            TurboBatchTrigger::from_policy(crate::config::BATCH_POLICY_POSITIVE);
        assert!(valid);
        assert_eq!(positive.turbo_wait_time(), Some(Duration::from_micros(100)));
        assert!(positive.need_fetch_more(Duration::from_secs(1)));
        assert_eq!(positive.preferred_batch_wait_size(1.8, 8), 8);

        let (mut standard, valid) =
            TurboBatchTrigger::from_policy(crate::config::BATCH_POLICY_STANDARD);
        assert!(valid);
        assert_eq!(standard.preferred_batch_wait_size(1.0, 8), 1);
        assert_eq!(standard.preferred_batch_wait_size(1.8, 8), 2);
        assert!(!standard.need_fetch_more(Duration::from_micros(100)));
        assert!(!standard.need_fetch_more(Duration::from_micros(80)));
        assert!(standard.need_fetch_more(Duration::from_micros(10)));
        assert!(standard.need_fetch_more(Duration::from_micros(80)));
        assert!(!standard.need_fetch_more(Duration::from_micros(90)));

        let (custom, valid) =
            TurboBatchTrigger::from_policy(r#"{"v":2,"t":0.001,"w":0.2,"p":0.5}"#);
        assert!(valid);
        assert_eq!(custom.turbo_wait_time(), Some(Duration::from_millis(1)));
        assert_eq!(custom.preferred_batch_wait_size(1.2, 8), 2);

        let (mut go_int_domain, valid) =
            TurboBatchTrigger::from_policy(r#"{"v":256,"n":-5,"t":0.001}"#);
        assert!(valid, "valid Go int values must not trigger fallback");
        assert!(go_int_domain.need_fetch_more(Duration::from_secs(1)));
        assert_eq!(go_int_domain.options.strategy, 256);
        assert_eq!(go_int_domain.options.max_intervals, -5);

        let (go_json_case_and_null, valid) =
            TurboBatchTrigger::from_policy(r#"{"V":2,"N":null,"T":0.001}"#);
        assert!(valid);
        assert_eq!(go_json_case_and_null.options.strategy, 2);
        assert_eq!(go_json_case_and_null.options.max_intervals, 0);
        assert_eq!(go_json_case_and_null.options.wait_seconds, 0.001);
        let (duplicate_keys, valid) =
            TurboBatchTrigger::from_policy(r#"{"v":1,"V":2,"v":null,"N":3,"n":4}"#);
        assert!(valid, "Go accepts duplicate JSON object keys");
        assert_eq!(duplicate_keys.options.strategy, 2);
        assert_eq!(duplicate_keys.options.max_intervals, 4);
        let (null_policy, valid) = TurboBatchTrigger::from_policy("custom null");
        assert!(valid);
        assert_eq!(null_policy.options, TurboBatchOptions::default());

        let (fallback, valid) = TurboBatchTrigger::from_policy("custom {x:1}");
        assert!(!valid);
        assert_eq!(fallback.options, TurboBatchTrigger::standard_options());
    }

    #[test]
    fn source_collection_policy_prioritizes_overload_then_turbo_wait() {
        let mut config = crate::config::TiKvClient::default();
        config.max_batch_size = 8;
        config.batch_wait_size = 3;
        config.max_batch_wait_time = Duration::from_millis(5);
        config.overload_threshold = 200;
        let mut policy = BatchCollectionPolicy::from_config(&config);
        assert_eq!(
            policy.wait_for_more(1, None, 201),
            Some((3, Duration::from_millis(5)))
        );
        assert_eq!(
            policy.wait_for_more(8, Some(Duration::from_nanos(1)), 201),
            None
        );

        config.max_batch_wait_time = Duration::ZERO;
        let mut policy = BatchCollectionPolicy::from_config(&config);
        // The first short positive interval establishes the source moving
        // average and activates the standard turbo policy.
        assert_eq!(
            policy.wait_for_more(1, Some(Duration::from_micros(10)), 0),
            Some((3, Duration::from_micros(100)))
        );
        policy.observe_batch_size(5);
        assert!((policy.average_batch_wait_size - 3.4).abs() < 1e-12);
    }

    #[tokio::test]
    async fn source_concurrency_limit_is_per_connection_and_scans_round_robin() {
        let clients = vec![
            TikvClient::new(Channel::from_static("http://127.0.0.1:1").connect_lazy()),
            TikvClient::new(Channel::from_static("http://127.0.0.1:2").connect_lazy()),
        ];
        let dispatcher = BatchCommandsDispatcher::new_with_concurrency(
            KvRpcClient::new(clients, Duration::from_secs(1)),
            8,
            1,
        );
        let (sender, _receiver) = oneshot::channel();
        dispatcher.pending.register_sender_with_telemetry(
            1,
            "",
            sender,
            Arc::new(BatchRequestTelemetry::new(0, Instant::now())),
            Some(BatchStreamMetricLabels::new(String::new(), 1, false)),
        );

        // Round robin checks slot 1 first, sees its independent limit is
        // exhausted, and selects slot 0 instead of applying a pool-wide cap.
        let (connection_index, available, send_guard) = dispatcher
            .select_connection_for_send(false)
            .await
            .expect("the second pooled connection remains available");
        assert_eq!(connection_index, 0);
        assert_eq!(available, 1);
        drop(send_guard);

        let (sender, _receiver) = oneshot::channel();
        dispatcher.pending.register_sender_with_telemetry(
            2,
            "",
            sender,
            Arc::new(BatchRequestTelemetry::new(0, Instant::now())),
            Some(BatchStreamMetricLabels::new(String::new(), 0, false)),
        );
        let (connection_index, available, send_guard) = dispatcher
            .select_connection_for_send(true)
            .await
            .expect("high-priority work bypasses the exhausted normal limit");
        assert_eq!(connection_index, 1);
        assert_eq!(available, 0);
        drop(send_guard);

        let mut builder = BatchCommandsBuilder::new();
        let _first = builder.push(empty(1), 0, "");
        let _second = builder.push(empty(2), HIGH_TASK_PRIORITY, "");
        let (group, _) = builder.build_with_limit(0);
        assert_eq!(group.expect("high-priority group").request.request_ids, [1]);
        assert_eq!(builder.len(), 1);
    }

    #[tokio::test]
    async fn source_connection_selection_skips_a_recreating_pool_slot() {
        let clients = vec![
            TikvClient::new(Channel::from_static("http://127.0.0.1:1").connect_lazy()),
            TikvClient::new(Channel::from_static("http://127.0.0.1:2").connect_lazy()),
        ];
        let dispatcher = BatchCommandsDispatcher::new_with_concurrency(
            KvRpcClient::new(clients, Duration::from_secs(1)),
            8,
            usize::MAX,
        );
        let recreating = dispatcher.reconnect_gate(1).lock_owned().await;

        let (connection_index, _, send_guard) = dispatcher
            .select_connection_for_send(false)
            .await
            .expect("the next ready pool slot is selected");
        assert_eq!(connection_index, 0);
        drop(send_guard);
        drop(recreating);
    }

    #[tokio::test]
    async fn source_recovery_waits_for_active_sender_before_retiring_stream() {
        let reconnect_gate = Arc::new(AsyncMutex::new(()));
        let active_sender = reconnect_gate.clone().lock_owned().await;
        let (outbound, _inbound) = mpsc::channel(1);
        let state = Arc::new(AsyncMutex::new(BatchStreamState {
            outbound: Some(outbound),
        }));
        let ready = Arc::new(tokio::sync::Notify::new());
        let recovery = tokio::spawn({
            let reconnect_gate = reconnect_gate.clone();
            let state = state.clone();
            let ready = ready.clone();
            async move {
                begin_batch_recovery(reconnect_gate, &state, &ready).await;
            }
        });

        tokio::task::yield_now().await;
        assert!(
            state.lock().await.outbound.is_some(),
            "lockForRecreate cannot invalidate the stream while send owns the gate"
        );
        drop(active_sender);
        recovery.await.unwrap();
        assert!(state.lock().await.outbound.is_none());
    }

    #[tokio::test]
    async fn source_connection_epoch_retires_only_the_recovery_leaders_host() {
        let epoch = AtomicU64::new(0);
        let mut direct_epoch = 0;
        let mut forwarded_epoch = 0;
        let pending = BatchPendingResponses::new();
        let direct = pending.register(1, "");
        let mut forwarded = pending.register(2, "store-2");

        assert!(claim_batch_recovery_epoch(&epoch, &mut direct_epoch));
        pending.fail_for_host("", || {
            Error::GrpcAPI(tonic::Status::unavailable("shared channel failed"))
        });
        assert!(!claim_batch_recovery_epoch(&epoch, &mut forwarded_epoch));

        assert!(matches!(
            direct.await,
            Ok(Err(Error::GrpcAPI(status))) if status.code() == tonic::Code::Unavailable
        ));
        assert!(matches!(
            forwarded.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(direct_epoch, 1);
        assert_eq!(forwarded_epoch, 1);
        assert_eq!(pending.len(), 1);
    }

    #[tokio::test]
    async fn source_request_deadline_includes_waiting_for_queue_capacity() {
        let client = KvRpcClient::new(
            vec![TikvClient::new(
                Channel::from_static("http://127.0.0.1:1").connect_lazy(),
            )],
            Duration::from_secs(1),
        );
        let dispatcher = Arc::new(BatchCommandsDispatcher::new(client, 1));
        let (sender, receiver) = mpsc::channel(1);
        let (occupied, _submission) = BatchCommandsBuilder::entry(empty(1), 0, "");
        sender.send(occupied).await.unwrap();
        let worker = BatchCommandsWorker {
            sender,
            task: tokio::spawn(futures::future::pending()),
            dispatcher,
        };

        let error = match worker
            .submit_until(
                empty(2),
                0,
                "",
                tokio::time::Instant::now() + Duration::from_millis(5),
            )
            .await
        {
            Ok(_) => panic!("a full queue must honor the enqueue deadline"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            Error::GrpcAPI(status)
                if status.code() == tonic::Code::DeadlineExceeded
                    && status.message() == "wait sendLoop"
        ));
        assert_eq!(receiver.len(), 1, "the timed-out entry was never enqueued");
        worker.close();
        drop(receiver);
    }

    #[tokio::test]
    async fn source_close_interrupts_waiting_for_queue_capacity() {
        let client = KvRpcClient::new(
            vec![TikvClient::new(
                Channel::from_static("http://127.0.0.1:1").connect_lazy(),
            )],
            Duration::from_secs(1),
        );
        let dispatcher = Arc::new(BatchCommandsDispatcher::new(client, 1));
        let (sender, receiver) = mpsc::channel(1);
        let (occupied, _submission) = BatchCommandsBuilder::entry(empty(1), 0, "");
        sender.send(occupied).await.unwrap();
        let worker = BatchCommandsWorker {
            sender,
            task: tokio::spawn(futures::future::pending()),
            dispatcher,
        };

        let (submission, ()) = tokio::join!(
            worker.submit_until(
                empty(2),
                0,
                "",
                tokio::time::Instant::now() + Duration::from_secs(1),
            ),
            async {
                tokio::task::yield_now().await;
                worker.close();
            }
        );
        let mut submission = submission.expect("source close is a response, not enqueue failure");
        assert!(matches!(
            submission.recv().await,
            Ok(Err(Error::StringError(message))) if message == "batch client closed"
        ));
        drop(receiver);
    }

    #[test]
    fn source_timeout_diagnostics_share_the_batch_first_response_boundary() {
        let start = Instant::now();
        let first = Arc::new(BatchRequestTelemetry::new(0, start));
        let second = Arc::new(BatchRequestTelemetry::new(0, start));
        first.mark_selected(start + Duration::from_millis(4));
        second.mark_selected(start + Duration::from_millis(4));
        let progress = Arc::new(BatchStreamProgress::default());
        let send_state = Arc::new(BatchRequestSendState {
            batch_size: 2,
            send_started_at: start + Duration::from_millis(4),
            sent_after_start_ns: AtomicU64::new(0),
            first_response_after_start_ns: AtomicU64::new(0),
        });
        first.mark_send_started(
            send_state.clone(),
            1,
            "store-2".to_owned(),
            progress.clone(),
        );
        second.mark_send_started(send_state, 2, "store-2".to_owned(), progress);
        first.mark_sent(start + Duration::from_millis(5));
        first.mark_received(start + Duration::from_millis(8));

        assert_eq!(
            second.timeout_reason(Duration::from_millis(10), start + Duration::from_millis(14)),
            "wait recvLoop timeout, timeout:10ms, EntryProgress{batch:4ms, size:2, send:1ms, ack:3ms, fwd:store-2}"
        );
    }

    #[test]
    fn source_batch_request_stage_observations_preserve_terminal_boundaries() {
        assert_eq!(
            BatchRequestOutcome::from_error(Some(&Error::StringError(
                "BatchCommands stream request channel closed".to_owned()
            ))),
            BatchRequestOutcome::Failed
        );
        assert_eq!(
            BatchRequestOutcome::from_error(Some(&Error::StringError(
                "batch client closed".to_owned()
            ))),
            BatchRequestOutcome::Closed
        );
        let start = Instant::now();
        let observation = |stage, outcome, duration| BatchRequestObservation {
            stage,
            outcome,
            duration,
        };
        let telemetry = BatchRequestTelemetry::new(7, start);
        assert_eq!(
            telemetry.observations(
                BatchRequestOutcome::Timeout,
                start + Duration::from_millis(25)
            ),
            vec![observation(
                BatchRequestStage::BatchWait,
                BatchRequestOutcome::Timeout,
                Duration::from_millis(25)
            )]
        );

        let telemetry = BatchRequestTelemetry::new(7, start);
        telemetry.mark_selected(start + Duration::from_millis(4));
        assert_eq!(
            telemetry.observations(
                BatchRequestOutcome::Cancelled,
                start + Duration::from_millis(11)
            ),
            vec![
                observation(
                    BatchRequestStage::BatchWait,
                    BatchRequestOutcome::Ok,
                    Duration::from_millis(4)
                ),
                observation(
                    BatchRequestStage::SendWait,
                    BatchRequestOutcome::Cancelled,
                    Duration::from_millis(7)
                ),
            ]
        );

        let telemetry = BatchRequestTelemetry::new(7, start);
        telemetry.mark_selected(start + Duration::from_millis(4));
        telemetry.mark_send_started_for_test(1, start + Duration::from_millis(4));
        telemetry.mark_sent(start + Duration::from_millis(5));
        assert_eq!(
            telemetry.observations(
                BatchRequestOutcome::Timeout,
                start + Duration::from_millis(10)
            ),
            vec![
                observation(
                    BatchRequestStage::BatchWait,
                    BatchRequestOutcome::Ok,
                    Duration::from_millis(4)
                ),
                observation(
                    BatchRequestStage::SendWait,
                    BatchRequestOutcome::Ok,
                    Duration::from_millis(1)
                ),
                observation(
                    BatchRequestStage::RecvWait,
                    BatchRequestOutcome::Timeout,
                    Duration::from_millis(5)
                ),
            ]
        );

        let telemetry = BatchRequestTelemetry::new(7, start);
        telemetry.mark_selected(start + Duration::from_millis(4));
        telemetry.mark_send_started_for_test(1, start + Duration::from_millis(4));
        telemetry.mark_sent(start + Duration::from_millis(5));
        telemetry.mark_received(start + Duration::from_millis(10));
        assert_eq!(
            telemetry.observations(BatchRequestOutcome::Ok, start + Duration::from_millis(12)),
            vec![
                observation(
                    BatchRequestStage::BatchWait,
                    BatchRequestOutcome::Ok,
                    Duration::from_millis(4)
                ),
                observation(
                    BatchRequestStage::SendWait,
                    BatchRequestOutcome::Ok,
                    Duration::from_millis(1)
                ),
                observation(
                    BatchRequestStage::RecvWait,
                    BatchRequestOutcome::Ok,
                    Duration::from_millis(5)
                ),
                observation(
                    BatchRequestStage::Done,
                    BatchRequestOutcome::Ok,
                    Duration::from_millis(12)
                ),
            ]
        );

        let telemetry = BatchRequestTelemetry::new(7, start);
        telemetry.mark_selected(start + Duration::from_millis(4));
        telemetry.mark_send_started_for_test(1, start + Duration::from_millis(4));
        telemetry.mark_received(start + Duration::from_millis(8));
        assert_eq!(
            telemetry.observations(BatchRequestOutcome::Ok, start + Duration::from_millis(10)),
            vec![
                observation(
                    BatchRequestStage::BatchWait,
                    BatchRequestOutcome::Ok,
                    Duration::from_millis(4)
                ),
                observation(
                    BatchRequestStage::SendWait,
                    BatchRequestOutcome::Ok,
                    Duration::from_nanos(1)
                ),
                observation(
                    BatchRequestStage::RecvWait,
                    BatchRequestOutcome::Ok,
                    Duration::from_millis(4) - Duration::from_nanos(1)
                ),
                observation(
                    BatchRequestStage::Done,
                    BatchRequestOutcome::Ok,
                    Duration::from_millis(10)
                ),
            ]
        );
    }

    #[tokio::test]
    async fn source_batch_dispatcher_construction_uses_client_configuration() {
        let clients = vec![TikvClient::new(
            Channel::from_static("http://127.0.0.1:1").connect_lazy(),
        )];
        let client = KvRpcClient::new(clients, Duration::from_secs(1));
        let mut config = crate::config::TiKvClient::default();
        config.max_batch_size = 0;
        assert!(BatchCommandsDispatcher::from_config(client.clone(), &config).is_none());

        config.max_batch_size = 17;
        config.max_concurrency_request_limit = 9;
        let dispatcher = BatchCommandsDispatcher::from_config(client, &config)
            .expect("batching enabled by nonzero max batch size");
        assert_eq!(dispatcher.queue_capacity, 17);
        assert_eq!(dispatcher.max_concurrency_request_limit, 9);
    }

    #[tokio::test]
    async fn source_epoch_retires_only_the_recovery_leader_when_reopen_cannot_finish() {
        let client = KvRpcClient::new(
            vec![TikvClient::new(
                Channel::from_static("http://127.0.0.1:1").connect_lazy(),
            )],
            Duration::from_secs(1),
        );
        let dispatcher = Arc::new(BatchCommandsDispatcher::new(client, 4));
        let mut config = crate::config::TiKvClient::default();
        config.max_batch_size = 4;
        let worker = dispatcher.clone().spawn_worker(&config);
        let mut first = worker.submit(empty(1), 0, "invalid\nforward-1").await;
        let mut second = worker.submit(empty(2), 0, "invalid\nforward-2").await;

        let (first_failed, second_failed) = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let first_failed = matches!(first.try_recv(), Ok(Err(_)));
                let second_failed = matches!(second.try_recv(), Ok(Err(_)));
                if first_failed || second_failed {
                    break (first_failed, second_failed);
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("one stream promptly wins recovery");
        assert_ne!(
            first_failed, second_failed,
            "only the stream that won the shared epoch retires its host while it owns recovery"
        );
        dispatcher.close().await;
        let remaining = if first_failed {
            second.recv().await
        } else {
            first.recv().await
        };
        assert!(matches!(
            remaining,
            Ok(Err(Error::StringError(message))) if message == "batch client closed"
        ));
        drop(worker);
    }

    #[test]
    fn source_builder_groups_direct_and_forwarded_requests_with_monotonic_ids() {
        let mut builder = BatchCommandsBuilder::new();
        let _first = builder.push(empty(1), 0, "");
        let _second = builder.push(empty(2), 0, "store-2");
        let _third = builder.push(
            BatchCommandRequest::Get(kvrpcpb::GetRequest {
                key: b"key".to_vec(),
                ..Default::default()
            }),
            0,
            "",
        );

        let (direct, forwarded) = builder.build_with_limit(8);
        let direct = direct.expect("direct group");
        assert_eq!(direct.forwarded_host, "");
        assert_eq!(direct.request.request_ids, [1, 3]);
        assert_eq!(direct.len(), 2);
        assert_eq!(forwarded.len(), 1);
        let forwarded = &forwarded["store-2"];
        assert_eq!(forwarded.forwarded_host, "store-2");
        assert_eq!(forwarded.request.request_ids, [2]);
        assert_eq!(forwarded.len(), 1);

        let _fourth = builder.push(empty(4), 0, "");
        let (next, forwarded) = builder.build_with_limit(1);
        assert!(forwarded.is_empty());
        assert_eq!(next.expect("next direct group").request.request_ids, [4]);
    }

    #[test]
    fn source_builder_prioritizes_and_allows_high_priority_to_exceed_limit() {
        let mut builder = BatchCommandsBuilder::new();
        let _first = builder.push(empty(1), 1, "");
        let _second = builder.push(empty(2), HIGH_TASK_PRIORITY, "");
        let _third = builder.push(empty(3), HIGH_TASK_PRIORITY + 1, "");
        let _fourth = builder.push(empty(4), 2, "");

        let (group, forwarded) = builder.build_with_limit(1);
        assert!(forwarded.is_empty());
        let group = group.expect("direct group");
        // 11 and 10 bypass the normal limit; the next lower-priority entry
        // consumes its single normal slot.
        assert_eq!(group.request.request_ids, [1, 2, 3]);
        assert_eq!(group.len(), 3);
        assert_eq!(builder.len(), 1);
        let (remaining, _) = builder.build_with_limit(1);
        assert_eq!(remaining.expect("remaining group").request.request_ids, [4]);
    }

    #[test]
    fn source_builder_skips_cancelled_entries_without_consuming_ids() {
        let mut builder = BatchCommandsBuilder::new();
        let mut cancellation = builder.push(empty(1), 0, "");
        let _live = builder.push(empty(2), 0, "");
        cancellation.cancellation.cancel();

        let (group, forwarded) = builder.build_with_limit(8);
        assert!(forwarded.is_empty());
        assert_eq!(group.expect("non-cancelled group").request.request_ids, [1]);
        builder.reset();
        assert_eq!(builder.len(), 0);
        assert!(matches!(
            cancellation.try_recv(),
            Ok(Err(Error::StringError(message))) if message == "BatchCommands request cancelled before selection"
        ));
    }

    #[test]
    fn source_dropped_submission_cancels_before_batch_selection() {
        let mut builder = BatchCommandsBuilder::new();
        let dropped = builder.push(empty(1), 0, "");
        let live = builder.push(empty(2), 0, "");
        drop(dropped);

        let (group, forwarded) = builder.build_with_limit(8);
        assert!(forwarded.is_empty());
        assert_eq!(group.expect("live group").request.request_ids, [1]);
        drop(live);
    }

    #[test]
    fn source_reset_settles_cancelled_entries_left_unselected_by_the_limit() {
        let mut builder = BatchCommandsBuilder::new();
        let mut cancelled = builder.push(empty(1), 0, "");
        let _selected = builder.push(empty(2), HIGH_TASK_PRIORITY, "");
        cancelled.cancellation.cancel();

        let (selected, _) = builder.build_with_limit(0);
        assert_eq!(selected.unwrap().request.request_ids, [1]);
        assert_eq!(builder.len(), 1);
        builder.reset();
        assert!(builder.entries.is_empty());
        assert!(matches!(
            cancelled.try_recv(),
            Ok(Err(Error::StringError(message))) if message == "BatchCommands request cancelled before selection"
        ));
    }

    #[tokio::test]
    async fn source_receive_loop_routes_each_id_and_retains_terminal_outcomes() {
        let pending = BatchPendingResponses::new();
        let received = pending.register(1, "");
        let cancelled = pending.register(2, "");
        drop(cancelled);

        let summary = receive_batch_response(
            &pending,
            tikvpb::BatchCommandsResponse {
                request_ids: vec![2, 8, 1],
                responses: vec![
                    tikvpb::batch_commands_response::Response {
                        cmd: Some(tikvpb::batch_commands_response::response::Cmd::Empty(
                            tikvpb::BatchCommandsEmptyResponse { test_id: 2 },
                        )),
                    },
                    tikvpb::batch_commands_response::Response {
                        cmd: Some(tikvpb::batch_commands_response::response::Cmd::Empty(
                            tikvpb::BatchCommandsEmptyResponse { test_id: 8 },
                        )),
                    },
                    tikvpb::batch_commands_response::Response {
                        cmd: Some(tikvpb::batch_commands_response::response::Cmd::Empty(
                            tikvpb::BatchCommandsEmptyResponse { test_id: 1 },
                        )),
                    },
                ],
                ..Default::default()
            },
        )
        .expect("well-formed batch response");
        assert_eq!(
            summary,
            BatchReceiveSummary {
                delivered: 1,
                cancelled: 1,
                outdated: 1,
                highest_request_id: Some(8),
                transport_layer_load: 0,
            }
        );
        assert!(matches!(
            received.await,
            Ok(Ok(BatchCommandResponse::Empty(response))) if response.test_id == 1
        ));
    }

    #[tokio::test]
    async fn source_receive_loop_delivers_one_response_conversion_error() {
        let pending = BatchPendingResponses::new();
        let received = pending.register(1, "");
        let summary = receive_batch_response(
            &pending,
            tikvpb::BatchCommandsResponse {
                request_ids: vec![1],
                responses: vec![tikvpb::batch_commands_response::Response { cmd: None }],
                ..Default::default()
            },
        )
        .expect("the response itself was well formed");
        assert_eq!(summary.delivered, 1);
        assert!(matches!(
            received.await,
            Ok(Err(Error::StringError(message))) if message == "Unknown command response"
        ));
    }

    #[tokio::test]
    async fn source_malformed_batch_response_retires_valid_prefix_before_failure() {
        let pending = BatchPendingResponses::new();
        let mut first = pending.register(1, "");
        let mut missing = pending.register(2, "");

        let error = receive_batch_response(
            &pending,
            tikvpb::BatchCommandsResponse {
                request_ids: vec![1, 2],
                responses: vec![tikvpb::batch_commands_response::Response {
                    cmd: Some(tikvpb::batch_commands_response::response::Cmd::Empty(
                        tikvpb::BatchCommandsEmptyResponse { test_id: 1 },
                    )),
                }],
                ..Default::default()
            },
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "BatchCommands response has 2 request IDs but only 1 responses"
        );
        assert_eq!(error.summary.delivered, 1);
        assert_eq!(error.summary.outdated, 0);
        assert!(matches!(
            first.try_recv(),
            Ok(Ok(BatchCommandResponse::Empty(response))) if response.test_id == 1
        ));
        assert!(matches!(
            missing.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(pending.len(), 1);
    }

    #[tokio::test]
    async fn source_publish_registers_before_send_and_retires_only_failed_group_ids() {
        let target = "source-publish-accounting";
        let connection_index = 29;
        let pending = BatchPendingResponses::new();
        let mut builder = BatchCommandsBuilder::new();
        let mut submitted = builder.push(empty(1), 0, "");
        let (outbound, mut receiver) = mpsc::channel(1);
        let (group, forwarded) = builder.build_with_limit(1);
        assert!(forwarded.is_empty());
        group
            .expect("direct group")
            .publish(&pending, &outbound, target, connection_index)
            .await
            .expect("open request stream channel");
        let request = receiver.recv().await.expect("published request");
        assert_eq!(request.request_ids, [1]);
        assert_eq!(
            pending.complete(
                1,
                BatchCommandResponse::Empty(tikvpb::BatchCommandsEmptyResponse { test_id: 1 })
            ),
            BatchResponseDisposition::Delivered
        );
        assert!(matches!(
            submitted.recv().await,
            Ok(Ok(BatchCommandResponse::Empty(response))) if response.test_id == 1
        ));

        let mut submitted = builder.push(empty(2), 0, "");
        let (outbound, receiver) = mpsc::channel(1);
        drop(receiver);
        let (group, _) = builder.build_with_limit(1);
        assert!(matches!(
            group
                .expect("second direct group")
                .publish(&pending, &outbound, target, connection_index)
                .await,
            Err(Error::StringError(message)) if message == "BatchCommands stream request channel closed"
        ));
        assert!(matches!(
            submitted.recv().await,
            Ok(Err(Error::StringError(message))) if message == "BatchCommands stream request channel closed"
        ));
        assert_eq!(
            pending.complete(
                2,
                BatchCommandResponse::Empty(tikvpb::BatchCommandsEmptyResponse { test_id: 2 })
            ),
            BatchResponseDisposition::Outdated
        );
        assert_eq!(
            crate::stats::batch_stream_request_counter_values(target, connection_index, false),
            (2, 2, 1, 0)
        );
    }

    #[tokio::test]
    async fn source_close_fails_only_entries_not_yet_published() {
        let mut builder = BatchCommandsBuilder::new();
        let mut published = builder.push(empty(1), 0, "");
        let pending = BatchPendingResponses::new();
        let mut queued = builder.push(empty(2), 0, "");
        let (group, _) = builder.build_with_limit(1);
        let (outbound, _receiver) = mpsc::channel(1);
        group
            .expect("first group")
            .publish(&pending, &outbound, "test", 0)
            .await
            .expect("publish first request");

        builder.cancel(|| Error::StringError("batch client closed".to_owned()));
        assert!(matches!(
            queued.recv().await,
            Ok(Err(Error::StringError(message))) if message == "batch client closed"
        ));
        assert!(matches!(
            published.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn source_explicit_close_retires_published_and_future_worker_entries() {
        let client = KvRpcClient::new(
            vec![TikvClient::new(
                Channel::from_static("http://127.0.0.1:1").connect_lazy(),
            )],
            Duration::from_secs(1),
        );
        let dispatcher = Arc::new(BatchCommandsDispatcher::new(client, 2));
        let published = dispatcher.pending.register(1, "");
        let mut config = crate::config::TiKvClient::default();
        config.max_batch_size = 2;
        let worker = dispatcher.clone().spawn_worker(&config);

        worker.close();
        assert!(matches!(
            published.await,
            Ok(Err(Error::StringError(message))) if message == "batch client closed"
        ));

        let mut after_close = worker.submit(empty(2), 0, "").await;
        assert!(matches!(
            after_close.recv().await,
            Ok(Err(Error::StringError(message))) if message == "batch client closed"
        ));
    }

    #[tokio::test]
    async fn source_idle_batch_worker_retires_its_pool_before_the_next_submission() {
        let client = KvRpcClient::new(
            vec![TikvClient::new(
                Channel::from_static("http://127.0.0.1:1").connect_lazy(),
            )],
            Duration::from_secs(1),
        );
        let dispatcher = Arc::new(BatchCommandsDispatcher::new(client, 2));
        let mut config = crate::config::TiKvClient::default();
        config.max_batch_size = 2;
        let worker = dispatcher
            .clone()
            .spawn_worker_with_idle_timeout(&config, Duration::from_millis(5));

        tokio::time::sleep(Duration::from_millis(20)).await;
        let mut after_idle = worker.submit(empty(1), 0, "").await;
        assert!(matches!(
            after_idle.recv().await,
            Ok(Err(Error::StringError(message))) if message == "batch client closed"
        ));
    }

    #[tokio::test]
    async fn source_idle_deadline_does_not_interrupt_collection_after_a_head_arrives() {
        let dispatcher = Arc::new(BatchCommandsDispatcher::new(
            KvRpcClient::new(
                vec![TikvClient::new(
                    Channel::from_static("http://127.0.0.1:1").connect_lazy(),
                )],
                Duration::from_secs(1),
            ),
            2,
        ));
        dispatcher.transport_layer_load.store(1, Ordering::Release);
        let mut config = crate::config::TiKvClient::default();
        config.max_batch_size = 2;
        config.batch_wait_size = 2;
        config.max_batch_wait_time = Duration::from_millis(30);
        config.overload_threshold = 0;
        let worker = dispatcher
            .clone()
            .spawn_worker_with_idle_timeout(&config, Duration::from_millis(5));

        let _submitted = worker.submit(empty(1), 0, "").await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            !worker.task.is_finished(),
            "idle expiry must not interrupt source fetchMorePendingRequests"
        );
        drop(worker);
    }

    #[tokio::test]
    async fn source_receive_stream_failure_only_retires_matching_forwarding_host() {
        let target = "source-receive-accounting";
        let connection_index = 31;
        let pending = Arc::new(BatchPendingResponses::new());
        let (direct_completed_sender, direct_completed) = oneshot::channel();
        pending.register_sender_with_telemetry(
            1,
            "",
            direct_completed_sender,
            Arc::new(BatchRequestTelemetry::new(0, Instant::now())),
            Some(BatchStreamMetricLabels::new(
                target.to_owned(),
                connection_index,
                false,
            )),
        );
        let (direct_failed_sender, direct_failed) = oneshot::channel();
        pending.register_sender_with_telemetry(
            2,
            "",
            direct_failed_sender,
            Arc::new(BatchRequestTelemetry::new(0, Instant::now())),
            Some(BatchStreamMetricLabels::new(
                target.to_owned(),
                connection_index,
                false,
            )),
        );
        let mut forwarded_live = pending.register(3, "store-2");
        let transport_layer_load = Arc::new(AtomicU64::new(0));
        let responses = futures::stream::iter(vec![
            Ok(tikvpb::BatchCommandsResponse {
                request_ids: vec![1],
                transport_layer_load: 42,
                responses: vec![tikvpb::batch_commands_response::Response {
                    cmd: Some(tikvpb::batch_commands_response::response::Cmd::Empty(
                        tikvpb::BatchCommandsEmptyResponse { test_id: 1 },
                    )),
                }],
                ..Default::default()
            }),
            Err(tonic::Status::unavailable("direct stream failed")),
        ]);

        let failure = match run_batch_receive_loop(
            responses,
            pending.clone(),
            String::new(),
            target,
            connection_index,
            Arc::new(BatchStreamProgress::default()),
            Cancellation::default(),
            transport_layer_load.clone(),
            Arc::new(std::sync::RwLock::new(None)),
        )
        .await
        {
            BatchReceiveLoopExit::Failed(error) => error,
            BatchReceiveLoopExit::Stopped => panic!("the test stream was not cancelled"),
        };
        pending.fail_for_host("", || duplicate_batch_failure(&failure));
        assert_eq!(transport_layer_load.load(Ordering::Acquire), 42);
        assert!(matches!(
            direct_completed.await,
            Ok(Ok(BatchCommandResponse::Empty(response))) if response.test_id == 1
        ));
        assert!(matches!(
            direct_failed.await,
            Ok(Err(Error::GrpcAPI(status))) if status.code() == tonic::Code::Unavailable
        ));
        assert!(matches!(
            forwarded_live.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(
            crate::stats::batch_stream_request_counter_values(target, connection_index, false),
            (0, 2, 1, 0)
        );
    }

    #[test]
    fn source_inspect_pending_batch_requests_separates_confirmed_entries() {
        let pending = BatchPendingResponses::new();
        let now = Instant::now();
        let confirmed = Arc::new(BatchStreamProgress::default());
        confirmed.observe_response_ids(&[5]);
        let unconfirmed = Arc::new(BatchStreamProgress::default());
        let mut receivers = Vec::new();

        let mut register = |id: u64, wait: Duration, progress: Arc<BatchStreamProgress>| {
            let arrived_at = now - wait;
            let telemetry = Arc::new(BatchRequestTelemetry::new(0, arrived_at));
            telemetry.mark_selected(arrived_at);
            telemetry.mark_send_started_for_test(1, arrived_at);
            let (sender, receiver) = oneshot::channel();
            let mut metrics = BatchStreamMetricLabels::new("diagnostic".to_owned(), 0, false);
            metrics.progress = progress;
            pending.register_sender_with_telemetry(id, "", sender, telemetry, Some(metrics));
            receivers.push(receiver);
        };

        register(1, Duration::from_secs(7 * 60 + 30), unconfirmed.clone());
        register(2, Duration::from_secs(5 * 60), confirmed.clone());
        register(3, Duration::from_secs(30), unconfirmed.clone());
        register(4, Duration::from_secs(4 * 60), confirmed);
        register(5, Duration::from_secs(6 * 60), unconfirmed);

        assert_eq!(
            pending.inspect(now),
            super::super::command::PendingBatchRequestStats {
                oldest_id: Some(1),
                oldest_wait: Some(Duration::from_secs(7 * 60 + 30)),
                slow_count: 5,
                slow_unconfirmed_count: 3,
                hanging_count: 4,
                hanging_unconfirmed_count: 2,
            }
        );
        assert_eq!(receivers.len(), 5);
    }

    #[tokio::test]
    async fn source_cancelled_response_records_its_stream_tail() {
        let target = "source-cancelled-entry-tail";
        let connection_index = 37;
        let pending = BatchPendingResponses::new();
        let telemetry = Arc::new(BatchRequestTelemetry::new(0, Instant::now()));
        telemetry.mark_selected(Instant::now());
        let (sender, receiver) = oneshot::channel();
        drop(receiver);
        pending.register_sender_with_telemetry(
            1,
            "",
            sender,
            telemetry,
            Some(BatchStreamMetricLabels::new(
                target.to_owned(),
                connection_index,
                false,
            )),
        );

        let summary = receive_batch_response(
            &pending,
            tikvpb::BatchCommandsResponse {
                request_ids: vec![1],
                responses: vec![tikvpb::batch_commands_response::Response {
                    cmd: Some(tikvpb::batch_commands_response::response::Cmd::Empty(
                        tikvpb::BatchCommandsEmptyResponse { test_id: 1 },
                    )),
                }],
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(summary.cancelled, 1);
        assert_eq!(
            crate::stats::batch_stream_cancelled_entry_tail_samples(
                target,
                connection_index,
                false,
            ),
            1
        );
    }

    #[derive(Default)]
    struct RecordedHealthFeedback(Mutex<Vec<u64>>);

    impl ClientEventListener for RecordedHealthFeedback {
        fn on_health_feedback(&self, feedback: &kvrpcpb::HealthFeedback) {
            self.0.lock().unwrap().push(feedback.feedback_seq_no);
        }
    }

    struct PanickingHealthFeedback;

    impl ClientEventListener for PanickingHealthFeedback {
        fn on_health_feedback(&self, _feedback: &kvrpcpb::HealthFeedback) {
            panic!("source receive-loop panic injection");
        }
    }

    #[tokio::test]
    async fn source_receive_loop_recovers_panics_on_the_same_stream() {
        let pending = Arc::new(BatchPendingResponses::new());
        let (sender, receiver) = oneshot::channel();
        pending.register_sender_with_telemetry(
            9,
            "",
            sender,
            Arc::new(BatchRequestTelemetry::new(0, Instant::now())),
            None,
        );
        let listener: Arc<dyn ClientEventListener> = Arc::new(PanickingHealthFeedback);
        let exit = run_batch_receive_loop_recovering_panics(
            futures::stream::iter([
                Ok(tikvpb::BatchCommandsResponse {
                    request_ids: vec![9],
                    responses: vec![tikvpb::batch_commands_response::Response {
                        cmd: Some(tikvpb::batch_commands_response::response::Cmd::Empty(
                            tikvpb::BatchCommandsEmptyResponse { test_id: 1 },
                        )),
                    }],
                    health_feedback: Some(kvrpcpb::HealthFeedback::default()),
                    ..Default::default()
                }),
                Ok(tikvpb::BatchCommandsResponse {
                    request_ids: vec![9],
                    responses: vec![tikvpb::batch_commands_response::Response {
                        cmd: Some(tikvpb::batch_commands_response::response::Cmd::Empty(
                            tikvpb::BatchCommandsEmptyResponse { test_id: 2 },
                        )),
                    }],
                    ..Default::default()
                }),
            ]),
            pending.clone(),
            String::new(),
            "panic-recovery",
            0,
            Arc::new(BatchStreamProgress::default()),
            Cancellation::default(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(std::sync::RwLock::new(Some(listener))),
        )
        .await;

        assert!(matches!(exit, BatchReceiveLoopExit::Failed(_)));
        assert!(matches!(
            receiver.await,
            Ok(Ok(BatchCommandResponse::Empty(response))) if response.test_id == 2
        ));
        assert_eq!(pending.len(), 0);
    }

    #[tokio::test]
    async fn source_short_response_recovers_without_retiring_the_missing_suffix() {
        let pending = Arc::new(BatchPendingResponses::new());
        let first = pending.register(1, "");
        let second = pending.register(2, "");

        let exit = run_batch_receive_loop_recovering_panics(
            futures::stream::iter([
                Ok(tikvpb::BatchCommandsResponse {
                    request_ids: vec![1, 2],
                    responses: vec![tikvpb::batch_commands_response::Response {
                        cmd: Some(tikvpb::batch_commands_response::response::Cmd::Empty(
                            tikvpb::BatchCommandsEmptyResponse { test_id: 1 },
                        )),
                    }],
                    ..Default::default()
                }),
                Ok(tikvpb::BatchCommandsResponse {
                    request_ids: vec![2],
                    responses: vec![tikvpb::batch_commands_response::Response {
                        cmd: Some(tikvpb::batch_commands_response::response::Cmd::Empty(
                            tikvpb::BatchCommandsEmptyResponse { test_id: 2 },
                        )),
                    }],
                    ..Default::default()
                }),
            ]),
            pending.clone(),
            String::new(),
            "malformed-recovery",
            0,
            Arc::new(BatchStreamProgress::default()),
            Cancellation::default(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(std::sync::RwLock::new(None)),
        )
        .await;

        assert!(matches!(exit, BatchReceiveLoopExit::Failed(_)));
        assert!(matches!(
            first.await,
            Ok(Ok(BatchCommandResponse::Empty(response))) if response.test_id == 1
        ));
        assert!(matches!(
            second.await,
            Ok(Ok(BatchCommandResponse::Empty(response))) if response.test_id == 2
        ));
        assert_eq!(pending.len(), 0);
    }

    #[tokio::test]
    async fn source_send_loop_recovers_panics_and_keeps_collecting() {
        let client = KvRpcClient::new(
            vec![TikvClient::new(
                Channel::from_static("http://127.0.0.1:1").connect_lazy(),
            )],
            Duration::from_secs(1),
        );
        let dispatcher = Arc::new(BatchCommandsDispatcher::new(client, 2));
        dispatcher
            .panic_next_send_loop
            .store(true, Ordering::Release);
        let mut config = crate::config::TiKvClient::default();
        config.max_batch_size = 2;
        let worker = dispatcher.spawn_worker(&config);

        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert!(
            !worker.task.is_finished(),
            "the recovered send loop must restart and wait for work"
        );
        worker.close();
    }

    #[tokio::test]
    async fn source_send_loop_panic_preserves_entries_collected_before_recovery() {
        let (mut server, _) = crate::store::mockserver::start_mock_tikv_service()
            .await
            .unwrap();
        let address = server.addr().unwrap();
        let channel = Channel::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let dispatcher = Arc::new(BatchCommandsDispatcher::new(
            KvRpcClient::new(vec![TikvClient::new(channel)], Duration::from_secs(1)),
            4,
        ));
        dispatcher
            .panic_after_collect
            .store(true, Ordering::Release);
        let mut config = crate::config::TiKvClient::default();
        config.max_batch_size = 4;
        let worker = dispatcher.clone().spawn_worker(&config);

        let mut collected_before_panic = worker.submit(empty(1), 0, "").await;
        tokio::task::yield_now().await;
        let mut wake_recovered_loop = worker.submit(empty(2), 0, "").await;
        for submission in [&mut collected_before_panic, &mut wake_recovered_loop] {
            assert!(matches!(
                tokio::time::timeout(Duration::from_secs(1), submission.recv()).await,
                Ok(Ok(Ok(BatchCommandResponse::Empty(_))))
            ));
        }

        dispatcher.close().await;
        drop(worker);
        server.stop().await.unwrap();
    }

    #[tokio::test]
    async fn source_default_concurrency_fast_fails_an_unavailable_batch_stream() {
        let pending = Arc::new(BatchPendingResponses::new());
        let cancellation = Cancellation::default();
        let stream = BatchCommandsStream {
            forwarded_host: String::new(),
            target: "fast-fail".to_owned(),
            connection_index: 0,
            progress: Arc::new(BatchStreamProgress::default()),
            pending,
            state: Arc::new(AsyncMutex::new(BatchStreamState { outbound: None })),
            ready: Arc::new(tokio::sync::Notify::new()),
            cancellation,
            supervisor: tokio::spawn(futures::future::pending()),
        };
        let mut builder = BatchCommandsBuilder::new();
        let mut submission = builder.push(empty(1), 0, "");
        let (group, forwarded) = builder.build_with_limit(1);
        assert!(forwarded.is_empty());

        let error = stream
            .publish(group.expect("one direct group"), true)
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "no available connections");
        assert!(matches!(
            submission.recv().await,
            Ok(Err(Error::StringError(message))) if message == "no available connections"
        ));
    }

    #[tokio::test]
    async fn source_health_feedback_listener_is_replaced_and_runs_before_demux() {
        let client = KvRpcClient::new(
            vec![TikvClient::new(
                Channel::from_static("http://127.0.0.1:1").connect_lazy(),
            )],
            Duration::from_secs(1),
        );
        let replaced = Arc::new(RecordedHealthFeedback::default());
        let active = Arc::new(RecordedHealthFeedback::default());
        client.set_event_listener(replaced.clone());
        client.set_event_listener(active.clone());

        let _ = run_batch_receive_loop(
            futures::stream::iter(vec![Ok(tikvpb::BatchCommandsResponse {
                health_feedback: Some(kvrpcpb::HealthFeedback {
                    feedback_seq_no: 7,
                    ..Default::default()
                }),
                ..Default::default()
            })]),
            Arc::new(BatchPendingResponses::new()),
            String::new(),
            "test",
            0,
            Arc::new(BatchStreamProgress::default()),
            Cancellation::default(),
            Arc::new(AtomicU64::new(0)),
            client.event_listener(),
        )
        .await;

        assert!(replaced.0.lock().unwrap().is_empty());
        assert_eq!(*active.0.lock().unwrap(), [7]);
    }

    #[tokio::test]
    async fn source_receive_loop_records_slow_and_tikv_send_tail_metrics() {
        let target = "source-tail-metrics";
        let connection_index = 17;
        let tikv_send_time_ns = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .saturating_sub(Duration::from_millis(30))
            .as_nanos()
            .try_into()
            .unwrap();
        let responses = Box::pin(futures::stream::once(async move {
            tokio::time::sleep(Duration::from_millis(25)).await;
            Ok(tikvpb::BatchCommandsResponse {
                tikv_send_time_ns,
                ..Default::default()
            })
        }));

        let _ = run_batch_receive_loop(
            responses,
            Arc::new(BatchPendingResponses::new()),
            String::new(),
            target,
            connection_index,
            Arc::new(BatchStreamProgress::default()),
            Cancellation::default(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(std::sync::RwLock::new(None)),
        )
        .await;

        let (receive, receive_tail, tikv_send_tail) =
            crate::stats::batch_stream_metric_sample_counts(target, connection_index, false);
        assert!(receive >= 1);
        assert!(receive_tail >= 1);
        assert!(tikv_send_tail >= 1);
    }

    #[tokio::test]
    async fn source_dispatcher_recreates_failed_streams_and_preserves_metadata_per_host() {
        let metadata = Arc::new(Mutex::new(Vec::new()));
        let client_send_times = Arc::new(Mutex::new(Vec::new()));
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind test server port");
        let address = listener.local_addr().expect("test server address");
        drop(listener);

        let (shutdown, shutdown_requested) = oneshot::channel();
        let server_metadata = metadata.clone();
        let server_client_send_times = client_send_times.clone();
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(BatchOnlyServer {
                    service: BatchOnlyService {
                        metadata: server_metadata,
                        client_send_times: server_client_send_times,
                        core: Arc::new(super::super::mockserver::MockServerCore::default()),
                        close_after_first_stream: true,
                        served_streams: Arc::new(AtomicUsize::new(0)),
                    },
                })
                .serve_with_shutdown(address, async move {
                    let _ = shutdown_requested.await;
                })
                .await
        });

        let endpoint = format!("http://{address}");
        let channel = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let endpoint = Endpoint::from_shared(endpoint.clone())
                    .expect("valid test endpoint")
                    .connect_timeout(Duration::from_millis(20));
                match endpoint.connect().await {
                    Ok(channel) => break channel,
                    Err(_) => tokio::time::sleep(Duration::from_millis(5)).await,
                }
            }
        })
        .await
        .expect("test server starts promptly");

        let client = KvRpcClient::new(
            vec![
                TikvClient::new(channel.clone()),
                TikvClient::new(channel.clone()),
            ],
            Duration::from_secs(1),
        );
        let dispatcher = Arc::new(BatchCommandsDispatcher::new(client, 8));
        let mut config = crate::config::TiKvClient::default();
        config.max_batch_size = 8;
        let worker = dispatcher.clone().spawn_worker(&config);
        let mut direct = worker.submit(empty(11), 0, "").await;
        let mut forwarded = worker.submit(empty(22), 0, "store-2").await;
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), direct.recv()).await,
            Ok(Ok(Ok(BatchCommandResponse::Empty(response)))) if response.test_id == 0
        ));
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), forwarded.recv()).await,
            Ok(Ok(Ok(BatchCommandResponse::Empty(response)))) if response.test_id == 0
        ));

        // The first server-side stream closes after responding. A later
        // direct request must use the supervisor's recreated stream.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let mut reopened = worker.submit(empty(33), 0, "").await;
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), reopened.recv()).await,
            Ok(Ok(Ok(BatchCommandResponse::Empty(response)))) if response.test_id == 0
        ));

        let direct_client = KvRpcClient::new(
            vec![TikvClient::new(channel.clone())],
            Duration::from_secs(1),
        )
        .with_batch_worker(&config);
        let response = KvClient::dispatch(
            &direct_client,
            &crate::proto::kvrpcpb::GetRequest {
                key: b"normal-dispatch".to_vec(),
                ..Default::default()
            },
        )
        .await
        .expect("normal batchable dispatch");
        assert_eq!(
            response
                .downcast::<crate::proto::kvrpcpb::GetResponse>()
                .expect("typed normal dispatch response")
                .value,
            b"normal-dispatch"
        );

        dispatcher.close().await;
        drop(worker);
        drop(direct_client);
        let metadata = metadata.lock().unwrap().clone();
        assert!(metadata.len() >= 3);
        assert!(metadata.contains(&(None, Some("1".to_owned()))));
        assert!(metadata.contains(&(Some("store-2".to_owned()), Some("1".to_owned()))));
        assert!(
            metadata
                .iter()
                .filter(|(forwarded_host, connection_index)| {
                    forwarded_host.is_none() && connection_index.as_deref() == Some("1")
                })
                .count()
                >= 2,
            "the direct stream must have been recreated"
        );
        assert!(
            client_send_times
                .lock()
                .unwrap()
                .iter()
                .all(|timestamp| *timestamp > 0),
            "every published BatchCommands envelope carries client_send_time_ns"
        );

        shutdown.send(()).expect("test server is still running");
        server
            .await
            .expect("test server task completes")
            .expect("test server stops cleanly");
    }
}
