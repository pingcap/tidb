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

//! Source-shaped BatchCommands pending, failure, cancellation, and demux tests.

use std::sync::Arc;
use std::time::Duration;

use tidb_txnkv::rpc::batch::{
    BatchCommandTag, BatchEnvelopeKind, BatchInflightError, BatchInflightTable, BatchPublishError,
    BatchRequestProgress, BatchRequestState, BatchRetirementReport, BatchRoute, BatchWireError,
    BatchWireResponse, OpaqueBatchCommand, PendingBatchCommand,
};
use tidb_txnkv::rpc::{completion_pair, CompletionPull, CompletionRunLoop};

type Pull = CompletionPull<OpaqueBatchCommand, BatchInflightError>;

fn pending(request_id: u64) -> (PendingBatchCommand, Pull, Arc<BatchRequestProgress>) {
    let (completion, pull) = completion_pair(CompletionRunLoop::new(), || {});
    let progress = Arc::new(BatchRequestProgress::new(None));
    let batch_state = BatchRequestState::default();
    batch_state.set_batch_size(1);
    progress.record_batch_selected(request_id, Duration::from_millis(1), batch_state);
    (
        PendingBatchCommand::new(request_id, completion, Arc::clone(&progress)),
        pull,
        progress,
    )
}

fn response(ids: Vec<u64>) -> BatchWireResponse {
    let commands = ids
        .iter()
        .map(|id| OpaqueBatchCommand::new(BatchCommandTag::Empty, id.to_le_bytes().to_vec()))
        .collect();
    BatchWireResponse::new(commands, ids, 0, None, 0).unwrap()
}

// client-go/internal/client/client_batch.go:1013-1038 publish-before-send and
// exact-ID fast failure.
#[test]
fn publication_is_atomic_and_precedes_send_result() {
    let route = BatchRoute::direct("store-1:20160");
    let (first, mut first_pull, _) = pending(1);
    let (duplicate, mut duplicate_pull, _) = pending(1);
    let mut inflight = BatchInflightTable::new();

    inflight.publish(route.clone(), vec![first]).unwrap();
    assert_eq!(inflight.route_len(&route), 1);
    assert_eq!(
        inflight.publish(route.clone(), vec![duplicate]),
        Err(BatchPublishError::DuplicateRequestId(1))
    );
    assert_eq!(inflight.route_len(&route), 1);
    assert_eq!(duplicate_pull.try_complete(), Ok(None));

    assert_eq!(
        inflight.fail_ids(
            &route,
            &[1],
            BatchInflightError::Transport("send failed".to_owned()),
        ),
        1
    );
    assert_eq!(
        first_pull.try_complete(),
        Ok(Some(Err(BatchInflightError::Transport(
            "send failed".to_owned()
        ))))
    );
    assert!(inflight.is_empty());
}

// client-go/internal/client/client_batch.go:1270-1321 response-ID demux and
// client-go/util/async/core_test.go:51 once-only fulfillment.
#[test]
fn response_retires_exact_ids_and_fulfills_once() {
    let route = BatchRoute::direct("store-1:20160");
    let (request, mut pull, _) = pending(7);
    let mut inflight = BatchInflightTable::new();
    inflight.publish(route.clone(), vec![request]).unwrap();

    let packet = response(vec![7]);
    assert_eq!(
        inflight.receive(&route, packet.clone()),
        BatchRetirementReport {
            completed: 1,
            max_response_request_id: 7,
            ..BatchRetirementReport::default()
        }
    );
    assert_eq!(
        pull.try_complete().unwrap().unwrap().unwrap().body(),
        7_u64.to_le_bytes()
    );
    assert_eq!(pull.try_complete(), Ok(None));

    assert_eq!(
        inflight.receive(&route, packet),
        BatchRetirementReport {
            outdated: 1,
            max_response_request_id: 7,
            ..BatchRetirementReport::default()
        }
    );
    assert_eq!(pull.try_complete(), Ok(None));
}

// client-go/internal/client/client_async_test.go:239 TestSendRequestAsyncTimeout.
#[test]
fn canceled_response_is_retired_without_fabricating_a_terminal_result() {
    let route = BatchRoute::direct("store-1:20160");
    let (request, mut pull, _) = pending(9);
    let mut inflight = BatchInflightTable::new();
    inflight.publish(route.clone(), vec![request]).unwrap();
    pull.cancel();

    assert_eq!(
        inflight.receive(&route, response(vec![9])),
        BatchRetirementReport {
            canceled: 1,
            max_response_request_id: 9,
            ..BatchRetirementReport::default()
        }
    );
    assert_eq!(pull.try_complete(), Ok(None));
    assert!(inflight.is_empty());
}

// client-go/internal/client/client_test.go:742 TestInspectPendingBatchRequests.
#[test]
fn outdated_ids_still_advance_the_monotonic_stream_acknowledgement() {
    let route = BatchRoute::direct("store-1:20160");
    let (request, mut pull, progress) = pending(10);
    let mut inflight = BatchInflightTable::new();
    inflight.publish(route.clone(), vec![request]).unwrap();

    assert_eq!(
        inflight.receive(&route, response(vec![11, 10])),
        BatchRetirementReport {
            completed: 1,
            outdated: 1,
            max_response_request_id: 11,
            ..BatchRetirementReport::default()
        }
    );
    assert!(pull.try_complete().unwrap().unwrap().is_ok());
    let stream_state = progress
        .batch_state()
        .unwrap()
        .stream_state()
        .expect("publish attaches the route stream state");
    assert_eq!(stream_state.max_response_request_id(), 11);

    let _ = inflight.receive(&route, response(vec![8]));
    assert_eq!(
        inflight
            .stream_state(&route)
            .unwrap()
            .max_response_request_id(),
        11
    );
}

// client-go's response loop relies on equal response/ID cardinality. Rust
// makes that stream failure explicit instead of recovering from an index panic.
#[test]
fn malformed_cardinality_fails_the_exact_route_as_a_protocol_error() {
    let direct = BatchRoute::direct("store-1:20160");
    let forwarded = BatchRoute::forwarded("store-1:20160", "store-2:20160");
    let (direct_request, mut direct_pull, _) = pending(1);
    let (forwarded_request, mut forwarded_pull, _) = pending(2);
    let mut inflight = BatchInflightTable::new();
    inflight
        .publish(direct.clone(), vec![direct_request])
        .unwrap();
    inflight
        .publish(forwarded.clone(), vec![forwarded_request])
        .unwrap();

    // One Empty response (field 255) but no request_ids field.
    let malformed = [0x0a, 0x03, 0xfa, 0x0f, 0x00];
    let expected = BatchInflightError::Protocol(BatchWireError::Cardinality {
        kind: BatchEnvelopeKind::Response,
        commands: 1,
        request_ids: 0,
    });
    assert_eq!(
        inflight.receive_encoded(&forwarded, &malformed),
        Err(expected.clone())
    );
    assert_eq!(forwarded_pull.try_complete(), Ok(Some(Err(expected))));
    assert_eq!(inflight.route_len(&forwarded), 0);
    assert_eq!(inflight.route_len(&direct), 1);
    assert_eq!(direct_pull.try_complete(), Ok(None));
}

// client-go/internal/client/client_batch.go:1043-1077 forwarded failure and
// async close fanout; client_async_test.go:354,406 close timing.
#[test]
fn forwarded_stream_failure_does_not_retire_direct_or_sibling_routes() {
    let direct = BatchRoute::direct("proxy:20160");
    let first_forward = BatchRoute::forwarded("proxy:20160", "store-1:20160");
    let second_forward = BatchRoute::forwarded("proxy:20160", "store-2:20160");
    let (direct_request, mut direct_pull, _) = pending(1);
    let (first_request, mut first_pull, _) = pending(2);
    let (second_request, mut second_pull, _) = pending(3);
    let mut inflight = BatchInflightTable::new();
    inflight
        .publish(direct.clone(), vec![direct_request])
        .unwrap();
    inflight
        .publish(first_forward.clone(), vec![first_request])
        .unwrap();
    inflight
        .publish(second_forward.clone(), vec![second_request])
        .unwrap();

    let failure = BatchInflightError::Transport("first forwarded stream failed".to_owned());
    assert_eq!(inflight.fail_route(&first_forward, failure.clone()), 1);
    assert_eq!(first_pull.try_complete(), Ok(Some(Err(failure))));
    assert_eq!(inflight.route_len(&direct), 1);
    assert_eq!(inflight.route_len(&second_forward), 1);
    assert_eq!(direct_pull.try_complete(), Ok(None));
    assert_eq!(second_pull.try_complete(), Ok(None));

    assert_eq!(inflight.close(), 2);
    assert_eq!(
        direct_pull.try_complete(),
        Ok(Some(Err(BatchInflightError::Closed)))
    );
    assert_eq!(
        second_pull.try_complete(),
        Ok(Some(Err(BatchInflightError::Closed)))
    );
    assert_eq!(inflight.close(), 0);
}
