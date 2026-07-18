// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Source-shaped BatchCommands request observations.

use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// One observable stage in a BatchCommands request lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BatchRequestStage {
    /// Time spent waiting for batch selection.
    BatchWait,
    /// Time spent waiting for the selected batch to be sent.
    SendWait,
    /// Time spent waiting for the request response.
    ReceiveWait,
    /// Total duration of a successfully completed request.
    Done,
}

/// Terminal outcome attached to one request-stage observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BatchRequestOutcome {
    /// The observed stage completed successfully.
    Ok,
    /// The request deadline elapsed.
    Timeout,
    /// The caller canceled the request.
    Canceled,
    /// The request failed for another reason.
    Failed,
    /// The batch connection or client closed.
    Closed,
}

/// Typed terminal failure used to derive a request outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BatchTerminalError {
    /// The request deadline elapsed.
    DeadlineExceeded,
    /// The caller canceled the request.
    Canceled,
    /// The active batch connection closed.
    BatchConnectionClosed,
    /// The owning batch client closed.
    BatchClientClosed,
    /// The request failed for another reason.
    Failed,
}

/// Maps a typed terminal failure to the source observation outcome.
pub const fn terminal_outcome(error: Option<BatchTerminalError>) -> BatchRequestOutcome {
    match error {
        None => BatchRequestOutcome::Ok,
        Some(BatchTerminalError::DeadlineExceeded) => BatchRequestOutcome::Timeout,
        Some(BatchTerminalError::Canceled) => BatchRequestOutcome::Canceled,
        Some(BatchTerminalError::BatchConnectionClosed | BatchTerminalError::BatchClientClosed) => {
            BatchRequestOutcome::Closed
        }
        Some(BatchTerminalError::Failed) => BatchRequestOutcome::Failed,
    }
}

/// One stage, outcome, and duration emitted for a batch request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BatchRequestObservation {
    /// Lifecycle stage being observed.
    pub stage: BatchRequestStage,
    /// Outcome assigned to this stage.
    pub outcome: BatchRequestOutcome,
    /// Time spent in this stage or request lifecycle.
    pub duration: Duration,
}

#[derive(Debug, Default)]
struct BatchRequestStateInner {
    batch_size: AtomicUsize,
    send_started_at: Mutex<Option<Instant>>,
    sent_after_send_start_ns: AtomicU64,
    first_response_after_send_start_ns: AtomicU64,
    stream_state: Mutex<Option<BatchStreamState>>,
}

#[derive(Debug, Default)]
struct BatchStreamStateInner {
    max_response_request_id: AtomicU64,
}

/// Response progress shared by every batch request on one concrete stream.
#[derive(Clone, Debug, Default)]
pub struct BatchStreamState {
    inner: Arc<BatchStreamStateInner>,
}

impl BatchStreamState {
    /// Whether two handles reference the same concrete stream state.
    pub fn shares_state_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    /// Advances the greatest response request ID observed on the stream.
    pub fn record_max_response_request_id(&self, request_id: u64) {
        self.inner
            .max_response_request_id
            .fetch_max(request_id, Ordering::AcqRel);
    }

    /// Returns the greatest response request ID observed on the stream.
    pub fn max_response_request_id(&self) -> u64 {
        self.inner.max_response_request_id.load(Ordering::Acquire)
    }
}

/// State shared by every entry in one concrete BatchCommands request.
#[derive(Clone, Debug, Default)]
pub struct BatchRequestState {
    inner: Arc<BatchRequestStateInner>,
}

impl BatchRequestState {
    /// Whether two handles reference the same concrete batch state.
    pub fn shares_state_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    /// Returns the number of entries assigned to this batch.
    pub fn batch_size(&self) -> usize {
        self.inner.batch_size.load(Ordering::Acquire)
    }

    /// Records the final number of entries assigned to this batch.
    pub fn set_batch_size(&self, batch_size: usize) {
        self.inner.batch_size.store(batch_size, Ordering::Release);
    }

    /// Records the absolute instant at which sending began.
    pub fn record_send_started_at(&self, send_started_at: Instant) {
        *self
            .inner
            .send_started_at
            .lock()
            .expect("batch send start lock") = Some(send_started_at);
    }

    /// Records the delay from send start until packet writing completed.
    pub fn record_sent_after_send_start(&self, duration: Duration) {
        self.inner
            .sent_after_send_start_ns
            .store(nonzero_duration_ns(duration), Ordering::Release);
    }

    /// Records the delay from send start until the first response arrived.
    pub fn record_first_response_after_send_start(&self, duration: Duration) {
        self.inner
            .first_response_after_send_start_ns
            .store(nonzero_duration_ns(duration), Ordering::Release);
    }

    /// Attaches the concrete stream-wide response progress authority.
    pub fn attach_stream_state(&self, stream_state: BatchStreamState) {
        *self
            .inner
            .stream_state
            .lock()
            .expect("batch stream state lock") = Some(stream_state);
    }

    /// Returns the attached stream-wide response progress, if any.
    pub fn stream_state(&self) -> Option<BatchStreamState> {
        self.inner
            .stream_state
            .lock()
            .expect("batch stream state lock")
            .clone()
    }
}

/// Concurrent progress state owned by one scheduled entry.
#[derive(Debug)]
pub struct BatchRequestProgress {
    arrived_at: Instant,
    request_id: AtomicU64,
    batch_selected_after_arrival_ns: AtomicU64,
    batch_state: Mutex<Option<BatchRequestState>>,
    received_after_arrival_ns: AtomicU64,
    forwarded_host: Mutex<Option<String>>,
}

impl Default for BatchRequestProgress {
    fn default() -> Self {
        Self::new(None)
    }
}

impl BatchRequestProgress {
    /// Creates progress stamped with the current arrival instant.
    pub fn new(forwarded_host: Option<String>) -> Self {
        Self::with_arrival(Instant::now(), forwarded_host)
    }

    /// Creates progress with a caller-supplied arrival instant.
    pub fn with_arrival(arrived_at: Instant, forwarded_host: Option<String>) -> Self {
        Self {
            arrived_at,
            request_id: AtomicU64::new(0),
            batch_selected_after_arrival_ns: AtomicU64::new(0),
            batch_state: Mutex::new(None),
            received_after_arrival_ns: AtomicU64::new(0),
            forwarded_host: Mutex::new(forwarded_host),
        }
    }

    /// Returns the request ID assigned during batch selection.
    pub fn request_id(&self) -> u64 {
        self.request_id.load(Ordering::Acquire)
    }

    /// Returns the delay from arrival until batch selection.
    pub fn batch_selected_after_arrival(&self) -> Option<Duration> {
        load_optional_duration(&self.batch_selected_after_arrival_ns)
    }

    /// Returns the shared state of the selected batch, if assigned.
    pub fn batch_state(&self) -> Option<BatchRequestState> {
        self.batch_state
            .lock()
            .expect("batch progress state lock")
            .clone()
    }

    /// Records batch selection using a caller-supplied relative delay.
    pub fn record_batch_selected(
        &self,
        request_id: u64,
        selected_after_arrival: Duration,
        batch_state: BatchRequestState,
    ) {
        self.request_id.store(request_id, Ordering::Release);
        self.batch_selected_after_arrival_ns.store(
            nonzero_duration_ns(selected_after_arrival),
            Ordering::Release,
        );
        *self.batch_state.lock().expect("batch progress state lock") = Some(batch_state);
    }

    /// Records the delay from arrival until this entry's response arrived.
    pub fn record_received_after_arrival(&self, duration: Duration) {
        self.received_after_arrival_ns
            .store(nonzero_duration_ns(duration), Ordering::Release);
    }

    pub(crate) fn record_batch_selected_at(
        &self,
        request_id: u64,
        selected_at: Instant,
        batch_state: BatchRequestState,
    ) {
        self.record_batch_selected(
            request_id,
            selected_at.saturating_duration_since(self.arrived_at),
            batch_state,
        );
    }

    pub(crate) fn set_forwarded_host(&self, forwarded_host: String) {
        *self
            .forwarded_host
            .lock()
            .expect("batch forwarded host lock") = Some(forwarded_host);
    }

    /// Formats source-compatible request progress at the supplied elapsed time.
    pub fn format(&self, now_after_arrival: Duration) -> String {
        let mut output = String::from("EntryProgress{");
        let (batched_ns, sent_ns, first_response_ns, received_ns) = self.load();
        if batched_ns == 0 {
            output.push('}');
            return output;
        }

        output.push_str("batch:");
        output.push_str(&format_metric_duration(batched_ns));
        if let Some(state) = self.batch_state() {
            if state.batch_size() > 0 {
                output.push_str(", size:");
                output.push_str(&state.batch_size().to_string());
            }
        }

        if sent_ns == 0 && received_ns == 0 {
            output.push_str(", send:");
            output.push_str(&format_metric_duration(
                duration_ns(now_after_arrival)
                    .saturating_sub(batched_ns)
                    .max(1),
            ));
            if self.received_by_tikv() {
                output.push_str(", ack:yes");
            }
            output.push('}');
            return output;
        }

        output.push_str(", send:");
        output.push_str(&format_metric_duration(
            sent_ns.saturating_sub(batched_ns).max(1),
        ));
        if first_response_ns > 0 {
            output.push_str(", ack:");
            output.push_str(&format_metric_duration(
                first_response_ns.saturating_sub(sent_ns).max(1),
            ));
        } else if self.received_by_tikv() {
            output.push_str(", ack:yes");
        }
        if received_ns > 0 {
            output.push_str(", recv:");
            output.push_str(&format_metric_duration(
                received_ns.saturating_sub(sent_ns).max(1),
            ));
        }
        let forwarded_host = self
            .forwarded_host
            .lock()
            .expect("batch forwarded host lock");
        if let Some(forwarded_host) = forwarded_host.as_deref() {
            if !forwarded_host.is_empty() {
                output.push_str(", fwd:");
                output.push_str(forwarded_host);
            }
        }
        output.push('}');
        output
    }

    /// Formats the source timeout reason and current request progress.
    pub fn format_timeout(&self, timeout: Duration, now_after_arrival: Duration) -> String {
        format!(
            "wait recvLoop timeout, timeout:{}, {}",
            GoDuration(timeout),
            self.format(now_after_arrival)
        )
    }

    /// Builds source-compatible stage observations for one terminal outcome.
    pub fn observations(
        &self,
        terminal: BatchRequestOutcome,
        now_after_arrival: Duration,
    ) -> Vec<BatchRequestObservation> {
        let now_ns = duration_ns(now_after_arrival).max(1);
        let (batched_ns, sent_ns, _first_response_ns, received_ns) = self.load();
        let mut observations = Vec::with_capacity(4);
        if batched_ns == 0 {
            observations.push(observation(BatchRequestStage::BatchWait, terminal, now_ns));
            return observations;
        }
        observations.push(observation(
            BatchRequestStage::BatchWait,
            BatchRequestOutcome::Ok,
            batched_ns,
        ));

        if sent_ns == 0 && received_ns == 0 {
            observations.push(observation(
                BatchRequestStage::SendWait,
                terminal,
                now_ns.saturating_sub(batched_ns).max(1),
            ));
            return observations;
        }
        observations.push(observation(
            BatchRequestStage::SendWait,
            BatchRequestOutcome::Ok,
            sent_ns.saturating_sub(batched_ns).max(1),
        ));

        if received_ns == 0 {
            observations.push(observation(
                BatchRequestStage::ReceiveWait,
                terminal,
                now_ns.saturating_sub(sent_ns).max(1),
            ));
            return observations;
        }
        observations.push(observation(
            BatchRequestStage::ReceiveWait,
            BatchRequestOutcome::Ok,
            received_ns.saturating_sub(sent_ns).max(1),
        ));
        if terminal == BatchRequestOutcome::Ok {
            observations.push(observation(
                BatchRequestStage::Done,
                BatchRequestOutcome::Ok,
                now_ns,
            ));
        }
        observations
    }

    fn received_by_tikv(&self) -> bool {
        let request_id = self.request_id();
        request_id > 0
            && self
                .batch_state()
                .and_then(|state| state.stream_state())
                .is_some_and(|state| state.max_response_request_id() >= request_id)
    }

    fn load(&self) -> (u64, u64, u64, u64) {
        let batched_ns = self.batch_selected_after_arrival_ns.load(Ordering::Acquire);
        let mut sent_ns = 0;
        let mut first_response_ns = 0;
        if let Some(state) = self.batch_state() {
            let send_started_at = *state
                .inner
                .send_started_at
                .lock()
                .expect("batch send start lock");
            if let Some(send_started_at) = send_started_at {
                let send_start_ns =
                    nonzero_duration_ns(send_started_at.saturating_duration_since(self.arrived_at));
                let sent_after_start = state.inner.sent_after_send_start_ns.load(Ordering::Acquire);
                if sent_after_start > 0 {
                    sent_ns = send_start_ns.saturating_add(sent_after_start);
                }
                let first_response_after_start = state
                    .inner
                    .first_response_after_send_start_ns
                    .load(Ordering::Acquire);
                if first_response_after_start > 0 {
                    first_response_ns = send_start_ns.saturating_add(first_response_after_start);
                }
            }
        }
        let received_ns = self.received_after_arrival_ns.load(Ordering::Acquire);
        sent_ns = normalize_observed_sent_ns(batched_ns, sent_ns, first_response_ns, received_ns);
        (batched_ns, sent_ns, first_response_ns, received_ns)
    }
}

fn load_optional_duration(value: &AtomicU64) -> Option<Duration> {
    let value = value.load(Ordering::Acquire);
    (value > 0).then_some(Duration::from_nanos(value))
}

fn nonzero_duration_ns(duration: Duration) -> u64 {
    duration_ns(duration).max(1)
}

/// Normalizes a missing or out-of-order send timestamp against observed replies.
pub const fn normalize_observed_sent_ns(
    batched_ns: u64,
    mut sent_ns: u64,
    first_response_ns: u64,
    received_ns: u64,
) -> u64 {
    let boundary_ns = if received_ns == 0 {
        first_response_ns
    } else {
        received_ns
    };
    if boundary_ns > 0 {
        if sent_ns == 0 {
            sent_ns = batched_ns.saturating_add(1);
        } else if sent_ns > boundary_ns {
            sent_ns = boundary_ns.saturating_sub(1);
        }
    }
    sent_ns
}

fn observation(
    stage: BatchRequestStage,
    outcome: BatchRequestOutcome,
    duration_ns: u64,
) -> BatchRequestObservation {
    BatchRequestObservation {
        stage,
        outcome,
        duration: Duration::from_nanos(duration_ns),
    }
}

fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

fn format_metric_duration(nanoseconds: u64) -> String {
    const MICROSECOND: u64 = 1_000;
    const MILLISECOND: u64 = 1_000_000;
    const SECOND: u64 = 1_000_000_000;

    if nanoseconds <= MICROSECOND {
        return GoDuration(Duration::from_nanos(nanoseconds)).to_string();
    }
    let unit = if nanoseconds >= SECOND {
        SECOND
    } else if nanoseconds >= MILLISECOND {
        MILLISECOND
    } else {
        MICROSECOND
    };
    let precision = if nanoseconds < 10 * unit { 100 } else { 10 };
    let quantum = unit / precision;
    let rounded = nanoseconds
        .saturating_add(quantum / 2)
        .checked_div(quantum)
        .unwrap_or_default()
        .saturating_mul(quantum);
    GoDuration(Duration::from_nanos(rounded)).to_string()
}

struct GoDuration(Duration);

impl fmt::Display for GoDuration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let nanoseconds = duration_ns(self.0);
        if nanoseconds == 0 {
            return formatter.write_str("0s");
        }
        if nanoseconds >= 1_000_000_000 {
            return write_decimal_duration(formatter, nanoseconds, 1_000_000_000, "s");
        }
        if nanoseconds >= 1_000_000 {
            return write_decimal_duration(formatter, nanoseconds, 1_000_000, "ms");
        }
        if nanoseconds >= 1_000 {
            return write_decimal_duration(formatter, nanoseconds, 1_000, "µs");
        }
        write!(formatter, "{nanoseconds}ns")
    }
}

fn write_decimal_duration(
    formatter: &mut fmt::Formatter<'_>,
    nanoseconds: u64,
    unit: u64,
    suffix: &str,
) -> fmt::Result {
    let whole = nanoseconds / unit;
    let remainder = nanoseconds % unit;
    if remainder == 0 {
        return write!(formatter, "{whole}{suffix}");
    }
    let width = unit.ilog10() as usize;
    let fractional = format!("{remainder:0width$}");
    write!(
        formatter,
        "{whole}.{}{suffix}",
        fractional.trim_end_matches('0')
    )
}
