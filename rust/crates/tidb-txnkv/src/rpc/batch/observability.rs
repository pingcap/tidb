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
use std::time::Duration;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BatchRequestStage {
    BatchWait,
    SendWait,
    ReceiveWait,
    Done,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BatchRequestOutcome {
    Ok,
    Timeout,
    Canceled,
    Failed,
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BatchTerminalError {
    DeadlineExceeded,
    Canceled,
    BatchConnectionClosed,
    BatchClientClosed,
    Failed,
}

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BatchRequestObservation {
    pub stage: BatchRequestStage,
    pub outcome: BatchRequestOutcome,
    pub duration: Duration,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BatchRequestState {
    pub batch_size: usize,
    pub send_start_after_arrival: Option<Duration>,
    pub sent_after_send_start: Option<Duration>,
    pub first_response_after_send_start: Option<Duration>,
    pub max_response_request_id: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BatchRequestProgress {
    pub request_id: u64,
    pub batch_selected_after_arrival: Option<Duration>,
    pub batch_state: Option<BatchRequestState>,
    pub received_after_arrival: Option<Duration>,
    pub forwarded_host: Option<String>,
}

impl BatchRequestProgress {
    pub fn format(&self, now_after_arrival: Duration) -> String {
        let mut output = String::from("EntryProgress{");
        let (batched_ns, sent_ns, first_response_ns, received_ns) = self.load();
        if batched_ns == 0 {
            output.push('}');
            return output;
        }

        output.push_str("batch:");
        output.push_str(&format_metric_duration(batched_ns));
        if let Some(state) = self.batch_state {
            if state.batch_size > 0 {
                output.push_str(", size:");
                output.push_str(&state.batch_size.to_string());
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
        if let Some(forwarded_host) = self.forwarded_host.as_deref() {
            if !forwarded_host.is_empty() {
                output.push_str(", fwd:");
                output.push_str(forwarded_host);
            }
        }
        output.push('}');
        output
    }

    pub fn format_timeout(&self, timeout: Duration, now_after_arrival: Duration) -> String {
        format!(
            "wait recvLoop timeout, timeout:{}, {}",
            GoDuration(timeout),
            self.format(now_after_arrival)
        )
    }

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
        self.request_id > 0
            && self
                .batch_state
                .is_some_and(|state| state.max_response_request_id >= self.request_id)
    }

    fn load(&self) -> (u64, u64, u64, u64) {
        let batched_ns = self.batch_selected_after_arrival.map_or(0, duration_ns);
        let mut sent_ns = 0;
        let mut first_response_ns = 0;
        if let Some(state) = self.batch_state {
            if let Some(send_start) = state.send_start_after_arrival {
                let send_start_ns = duration_ns(send_start).max(1);
                if let Some(sent_after_start) = state.sent_after_send_start {
                    sent_ns = send_start_ns.saturating_add(duration_ns(sent_after_start));
                }
                if let Some(first_response_after_start) = state.first_response_after_send_start {
                    first_response_ns =
                        send_start_ns.saturating_add(duration_ns(first_response_after_start));
                }
            }
        }
        let received_ns = self.received_after_arrival.map_or(0, duration_ns);
        sent_ns = normalize_observed_sent_ns(batched_ns, sent_ns, first_response_ns, received_ns);
        (batched_ns, sent_ns, first_response_ns, received_ns)
    }
}

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
