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

use std::time::{Duration, Instant};

use tidb_txnkv::rpc::batch::{
    terminal_outcome, BatchRequestObservation, BatchRequestOutcome, BatchRequestProgress,
    BatchRequestStage, BatchRequestState, BatchStreamState, BatchTerminalError,
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
    let missing_sent = progress(4, 0, 8);
    assert_eq!(
        missing_sent.format_timeout(Duration::from_millis(10), Duration::from_millis(10)),
        "wait recvLoop timeout, timeout:10ms, EntryProgress{batch:4ms, size:1, send:1ns, ack:4ms, recv:4ms}"
    );

    let out_of_order = progress(4, 7, 5);
    assert_eq!(
        out_of_order.format_timeout(Duration::from_millis(10), Duration::from_millis(10)),
        "wait recvLoop timeout, timeout:10ms, EntryProgress{batch:4ms, size:1, send:1ms, ack:1ns, recv:1ns}"
    );
}

#[test]
fn test_write_batch_commands_entry_progress() {
    assert_eq!(
        BatchRequestProgress::default().format(Duration::from_millis(10)),
        "EntryProgress{}"
    );

    let progress = progress(4, 0, 8);
    assert_eq!(
        progress.format(Duration::from_millis(10)),
        "EntryProgress{batch:4ms, size:1, send:1ns, ack:4ms, recv:4ms}"
    );
    assert_eq!(
        format!("prefix={}", progress.format(Duration::from_millis(10))),
        "prefix=EntryProgress{batch:4ms, size:1, send:1ns, ack:4ms, recv:4ms}"
    );

    let arrived_at = Instant::now();
    let acknowledged = BatchRequestProgress::with_arrival(arrived_at, None);
    let acknowledged_stream = BatchStreamState::default();
    acknowledged_stream.record_max_response_request_id(5);
    let acknowledged_state = BatchRequestState::new(
        1,
        arrived_at + Duration::from_millis(4),
        acknowledged_stream,
    );
    acknowledged.record_batch_selected(Duration::from_millis(4));
    acknowledged.publish_batch(4, acknowledged_state);
    assert_eq!(
        acknowledged.format(Duration::from_millis(10)),
        "EntryProgress{batch:4ms, size:1, send:6ms, ack:yes}"
    );

    let forwarded = progress_with_host(4, 0, 8, Some("store-2".to_owned()));
    assert_eq!(
        forwarded.format(Duration::from_millis(10)),
        "EntryProgress{batch:4ms, size:1, send:1ns, ack:4ms, recv:4ms, fwd:store-2}"
    );
}

#[test]
fn shared_send_start_is_derived_from_each_entry_arrival() {
    let first_arrival = Instant::now();
    let second_arrival = first_arrival + Duration::from_millis(4);
    let send_started_at = first_arrival + Duration::from_millis(10);
    let state = BatchRequestState::new(2, send_started_at, BatchStreamState::default());
    state.record_sent_after_send_start(Duration::from_millis(2));
    state.record_sent_after_send_start(Duration::from_millis(20));

    let first = BatchRequestProgress::with_arrival(first_arrival, None);
    first.record_batch_selected(Duration::from_millis(3));
    first.publish_batch(1, state.clone());
    let second = BatchRequestProgress::with_arrival(second_arrival, None);
    second.record_batch_selected(Duration::from_millis(1));
    second.publish_batch(2, state);

    assert_eq!(
        first.format(Duration::from_millis(15)),
        "EntryProgress{batch:3ms, size:2, send:9ms}"
    );
    assert_eq!(
        second.format(Duration::from_millis(11)),
        "EntryProgress{batch:1ms, size:2, send:7ms}"
    );
}

#[test]
fn successive_groups_share_stream_ack_progress() {
    let stream = BatchStreamState::default();
    let first_group = BatchRequestState::new(1, Instant::now(), stream.clone());
    let second_group = BatchRequestState::new(1, Instant::now(), stream.clone());

    let first = BatchRequestProgress::default();
    first.record_batch_selected(Duration::from_millis(1));
    first.publish_batch(4, first_group.clone());
    let second = BatchRequestProgress::default();
    second.record_batch_selected(Duration::from_millis(1));
    second.publish_batch(7, second_group.clone());

    assert!(!first_group.shares_state_with(&second_group));
    assert!(first_group
        .stream_state()
        .shares_state_with(second_group.stream_state()));
    stream.record_max_response_request_id(5);
    assert_eq!(
        first.format(Duration::from_millis(10)),
        "EntryProgress{batch:1ms, size:1, send:9ms, ack:yes}"
    );
    assert_eq!(
        second.format(Duration::from_millis(10)),
        "EntryProgress{batch:1ms, size:1, send:9ms}"
    );

    stream.record_max_response_request_id(7);
    assert_eq!(
        second.format(Duration::from_millis(10)),
        "EntryProgress{batch:1ms, size:1, send:9ms, ack:yes}"
    );
}

fn progress(batched_ms: u64, sent_ms: u64, received_ms: u64) -> BatchRequestProgress {
    progress_with_host(batched_ms, sent_ms, received_ms, None)
}

fn progress_with_host(
    batched_ms: u64,
    sent_ms: u64,
    received_ms: u64,
    forwarded_host: Option<String>,
) -> BatchRequestProgress {
    let arrived_at = Instant::now();
    let progress = BatchRequestProgress::with_arrival(arrived_at, forwarded_host);
    if batched_ms > 0 {
        progress.record_batch_selected(Duration::from_millis(batched_ms));
        if sent_ms > 0 || received_ms > 0 {
            let batch_state = BatchRequestState::new(
                1,
                arrived_at + Duration::from_millis(batched_ms),
                BatchStreamState::default(),
            );
            if sent_ms > 0 {
                batch_state.record_sent_after_send_start(
                    Duration::from_millis(sent_ms - batched_ms).max(Duration::from_nanos(1)),
                );
            }
            if received_ms > 0 {
                batch_state.record_first_response_after_send_start(
                    Duration::from_millis(received_ms - batched_ms).max(Duration::from_nanos(1)),
                );
                batch_state.record_first_response_after_send_start(Duration::from_secs(1));
            }
            progress.publish_batch(1, batch_state);
        }
    }
    if received_ms > 0 {
        progress.record_received_after_arrival(Duration::from_millis(received_ms));
    }
    progress
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
