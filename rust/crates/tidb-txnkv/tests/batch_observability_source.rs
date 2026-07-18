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

//! Direct transit of client-go's pure BatchCommands request-progress tests.

#[path = "../src/rpc/batch/mod.rs"]
mod batch;

use std::time::Duration;

use batch::{
    terminal_outcome, BatchRequestObservation, BatchRequestOutcome, BatchRequestProgress,
    BatchRequestStage, BatchRequestState, BatchTerminalError,
};

#[test]
fn test_batch_request_terminal_outcome() {
    let cases = [
        ("ok", None, BatchRequestOutcome::Ok),
        (
            "timeout",
            Some(BatchTerminalError::DeadlineExceeded),
            BatchRequestOutcome::Timeout,
        ),
        (
            "canceled",
            Some(BatchTerminalError::Canceled),
            BatchRequestOutcome::Canceled,
        ),
        (
            "batch conn closed",
            Some(BatchTerminalError::BatchConnectionClosed),
            BatchRequestOutcome::Closed,
        ),
        (
            "batch client closed",
            Some(BatchTerminalError::BatchClientClosed),
            BatchRequestOutcome::Closed,
        ),
        (
            "failed",
            Some(BatchTerminalError::Failed),
            BatchRequestOutcome::Failed,
        ),
    ];
    for (name, error, expected) in cases {
        assert_eq!(terminal_outcome(error), expected, "{name}");
    }
}

#[test]
fn test_visit_batch_request_observations() {
    use BatchRequestOutcome::{Canceled, Ok, Timeout};
    use BatchRequestStage::{BatchWait, Done, ReceiveWait, SendWait};

    let cases = [
        (
            progress(0, 0, 0),
            Timeout,
            25,
            vec![observation(BatchWait, Timeout, 25_000_000)],
        ),
        (
            progress(4, 0, 0),
            Canceled,
            11,
            vec![
                observation(BatchWait, Ok, 4_000_000),
                observation(SendWait, Canceled, 7_000_000),
            ],
        ),
        (
            progress(4, 5, 0),
            Timeout,
            10,
            vec![
                observation(BatchWait, Ok, 4_000_000),
                observation(SendWait, Ok, 1_000_000),
                observation(ReceiveWait, Timeout, 5_000_000),
            ],
        ),
        (
            progress(4, 5, 10),
            Ok,
            12,
            vec![
                observation(BatchWait, Ok, 4_000_000),
                observation(SendWait, Ok, 1_000_000),
                observation(ReceiveWait, Ok, 5_000_000),
                observation(Done, Ok, 12_000_000),
            ],
        ),
        (
            progress(4, 5, 10),
            Canceled,
            12,
            vec![
                observation(BatchWait, Ok, 4_000_000),
                observation(SendWait, Ok, 1_000_000),
                observation(ReceiveWait, Ok, 5_000_000),
            ],
        ),
        (
            progress(4, 0, 8),
            Ok,
            10,
            vec![
                observation(BatchWait, Ok, 4_000_000),
                observation(SendWait, Ok, 1),
                observation(ReceiveWait, Ok, 4_000_000 - 1),
                observation(Done, Ok, 10_000_000),
            ],
        ),
        (
            progress(4, 7, 5),
            Ok,
            10,
            vec![
                observation(BatchWait, Ok, 4_000_000),
                observation(SendWait, Ok, 1_000_000 - 1),
                observation(ReceiveWait, Ok, 1),
                observation(Done, Ok, 10_000_000),
            ],
        ),
    ];

    for (progress, terminal, now_ms, expected) in cases {
        assert_eq!(
            progress.observations(terminal, Duration::from_millis(now_ms)),
            expected
        );
    }
}

#[test]
fn test_format_batch_request_timeout_reason_normalizes_observed_sent_ns() {
    let progress = BatchRequestProgress {
        batch_selected_after_arrival: Some(Duration::from_millis(4)),
        batch_state: Some(BatchRequestState {
            batch_size: 1,
            send_start_after_arrival: Some(Duration::from_millis(4)),
            first_response_after_send_start: Some(Duration::from_millis(4)),
            ..BatchRequestState::default()
        }),
        received_after_arrival: Some(Duration::from_millis(8)),
        ..BatchRequestProgress::default()
    };
    assert_eq!(
        progress.format_timeout(Duration::from_millis(10), Duration::from_millis(10)),
        "wait recvLoop timeout, timeout:10ms, EntryProgress{batch:4ms, size:1, send:1ns, ack:4ms, recv:4ms}"
    );

    let progress = BatchRequestProgress {
        batch_selected_after_arrival: Some(Duration::from_millis(4)),
        batch_state: Some(BatchRequestState {
            batch_size: 1,
            send_start_after_arrival: Some(Duration::from_millis(4)),
            sent_after_send_start: Some(Duration::from_millis(3)),
            first_response_after_send_start: Some(Duration::from_millis(1)),
            ..BatchRequestState::default()
        }),
        received_after_arrival: Some(Duration::from_millis(5)),
        ..BatchRequestProgress::default()
    };
    assert_eq!(
        progress.format_timeout(Duration::from_millis(10), Duration::from_millis(10)),
        "wait recvLoop timeout, timeout:10ms, EntryProgress{batch:4ms, size:1, send:1ms, ack:1ns, recv:1ns}"
    );
}

#[test]
fn test_write_batch_commands_entry_progress() {
    assert_eq!(
        BatchRequestProgress::default().format(Duration::from_millis(10)),
        "EntryProgress{}"
    );

    let progress = BatchRequestProgress {
        batch_selected_after_arrival: Some(Duration::from_millis(4)),
        batch_state: Some(BatchRequestState {
            batch_size: 1,
            send_start_after_arrival: Some(Duration::from_millis(4)),
            first_response_after_send_start: Some(Duration::from_millis(4)),
            ..BatchRequestState::default()
        }),
        received_after_arrival: Some(Duration::from_millis(8)),
        ..BatchRequestProgress::default()
    };
    assert_eq!(
        progress.format(Duration::from_millis(10)),
        "EntryProgress{batch:4ms, size:1, send:1ns, ack:4ms, recv:4ms}"
    );
    assert_eq!(
        format!("prefix={}", progress.format(Duration::from_millis(10))),
        "prefix=EntryProgress{batch:4ms, size:1, send:1ns, ack:4ms, recv:4ms}"
    );

    let acknowledged = BatchRequestProgress {
        request_id: 4,
        batch_selected_after_arrival: Some(Duration::from_millis(4)),
        batch_state: Some(BatchRequestState {
            batch_size: 1,
            send_start_after_arrival: Some(Duration::from_millis(4)),
            max_response_request_id: 5,
            ..BatchRequestState::default()
        }),
        ..BatchRequestProgress::default()
    };
    assert_eq!(
        acknowledged.format(Duration::from_millis(10)),
        "EntryProgress{batch:4ms, size:1, send:6ms, ack:yes}"
    );

    let forwarded = BatchRequestProgress {
        forwarded_host: Some("store-2".to_owned()),
        ..progress
    };
    assert_eq!(
        forwarded.format(Duration::from_millis(10)),
        "EntryProgress{batch:4ms, size:1, send:1ns, ack:4ms, recv:4ms, fwd:store-2}"
    );
}

fn progress(batched_ms: u64, sent_ms: u64, received_ms: u64) -> BatchRequestProgress {
    let batch_selected_after_arrival = (batched_ms > 0).then(|| Duration::from_millis(batched_ms));
    let mut batch_state = None;
    if batched_ms > 0 && (sent_ms > 0 || received_ms > 0) {
        batch_state = Some(BatchRequestState {
            batch_size: 1,
            send_start_after_arrival: Some(Duration::from_millis(batched_ms)),
            sent_after_send_start: (sent_ms > 0)
                .then(|| Duration::from_millis(sent_ms - batched_ms).max(Duration::from_nanos(1))),
            first_response_after_send_start: (received_ms > 0).then(|| {
                Duration::from_millis(received_ms - batched_ms).max(Duration::from_nanos(1))
            }),
            ..BatchRequestState::default()
        });
    }
    BatchRequestProgress {
        batch_selected_after_arrival,
        batch_state,
        received_after_arrival: (received_ms > 0).then(|| Duration::from_millis(received_ms)),
        ..BatchRequestProgress::default()
    }
}

const fn observation(
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
