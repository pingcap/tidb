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

//! Continuation and shutdown: only a successful page creates the next
//! attempt, closing before the first pull stops every unsent attempt, and a
//! remotely cancelled task closes its exact channel generation before the same
//! task is resent.

#![allow(missing_docs)]

use crate::direct_unary_client_fixture::*;

#[test]
fn only_successful_paging_creates_a_continuation_attempt() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let first = CoprocessorResponse {
        data: b"page-one".to_vec(),
        range: Some(CoprocessorKeyRange {
            start: b"a".to_vec(),
            end: b"m".to_vec(),
        }),
        ..CoprocessorResponse::default()
    }
    .encode_to_vec();
    let mut metadata = metadata("a", "z");
    metadata.paging.enabled = true;
    metadata.paging.min_size = 2;
    metadata.paging.max_size = 8;
    let mut runtime = InjectedQueryRuntime::new(transport(
        Rc::clone(&calls),
        [Ok(first), Ok(response(b"page-two"))],
        [location(1, "a", "z", "tikv-1:20160")],
    ));
    let mut result = select_result(&mut runtime, &transport_request(metadata));

    assert_eq!(result.next_raw().unwrap(), Some(b"page-one".to_vec()));
    assert_eq!(calls.borrow().len(), 1);
    assert_eq!(result.next_raw().unwrap(), Some(b"page-two".to_vec()));
    assert_eq!(calls.borrow().len(), 2);
    assert_eq!(result.next_raw().unwrap(), None);
}

#[test]
fn unordered_paging_delivers_each_page_once() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let first = CoprocessorResponse {
        data: b"page-one".to_vec(),
        range: Some(CoprocessorKeyRange {
            start: b"a".to_vec(),
            end: b"m".to_vec(),
        }),
        ..CoprocessorResponse::default()
    }
    .encode_to_vec();
    let mut metadata = metadata("a", "z");
    metadata.keep_order = false;
    metadata.concurrency = 1;
    metadata.paging.enabled = true;
    metadata.paging.min_size = 2;
    metadata.paging.max_size = 8;
    let mut runtime = InjectedQueryRuntime::new(batch_first_transport(
        Rc::clone(&calls),
        [Ok(first), Ok(response(b"page-two"))],
        [location(1, "a", "z", "tikv-1:20160")],
        [true, true],
    ));
    let mut result = select_result(&mut runtime, &transport_request(metadata));

    assert_eq!(result.next_raw().unwrap(), Some(b"page-one".to_vec()));
    assert_eq!(result.next_raw().unwrap(), Some(b"page-two".to_vec()));
    assert_eq!(result.next_raw().unwrap(), None);
    assert_eq!(calls.borrow().len(), 2);
}

#[test]
fn close_before_pull_stops_every_unsent_attempt() {
    let cancel = std::sync::Arc::new(tidb_distsql::CancelHandle::default());
    let calls = Rc::new(RefCell::new(Vec::new()));
    let mut runtime = InjectedQueryRuntime::new(transport(
        Rc::clone(&calls),
        [Ok(response(b"never"))],
        [location(1, "a", "z", "tikv-1:20160")],
    ));
    let request = TransportRequest::new(metadata("a", "z"), std::sync::Arc::clone(&cancel));
    let mut result = select_result(&mut runtime, &request);
    result.close();
    result.close();
    assert!(
        !cancel.is_cancelled(),
        "closing one response must not cancel the outer execution"
    );
    assert_eq!(result.next_raw().unwrap(), None);
    assert!(calls.borrow().is_empty());
}

#[test]
fn remote_canceled_closes_exact_generation_before_resending_the_same_task() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let events = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Rc::new(RecordingRetryControl::default());
    let mut runtime = InjectedQueryRuntime::new(transport_with_transport_failures(
        Rc::clone(&calls),
        [
            Err(connection_failure(
                "tikv-1:20160",
                41,
                DirectUnaryTransportClass::RemoteGrpc,
                Some(DirectUnaryGrpcCode::Canceled),
            )),
            Ok(response(b"retried")),
        ],
        [Ok(StoreLiveness::Unreachable)],
        Rc::clone(&events),
        [location_with_second_peer(
            1,
            "a",
            "z",
            "tikv-1:20160",
            "tikv-2:20160",
        )],
        DirectUnaryRuntimeConfig {
            region_retry_waiter: retry_control.clone(),
            ..DirectUnaryRuntimeConfig::default()
        },
    ));
    let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));

    assert_eq!(result.next_raw().unwrap(), Some(b"retried".to_vec()));
    assert_eq!(result.next_raw().unwrap(), None);
    assert_eq!(calls.borrow().len(), 2);
    assert_eq!(calls.borrow()[0].region_id, calls.borrow()[1].region_id);
    assert_eq!(calls.borrow()[0].data, calls.borrow()[1].data);
    assert_eq!(
        events.borrow().as_slice(),
        [
            ClientEvent::Send("tikv-1:20160".to_owned()),
            ClientEvent::CloseGeneration {
                address: "tikv-1:20160".to_owned(),
                version: 41,
            },
            ClientEvent::Liveness {
                address: "tikv-1:20160".to_owned(),
                timeout: Duration::from_secs(1),
            },
            ClientEvent::Send("tikv-2:20160".to_owned()),
        ]
    );
    assert_eq!(retry_control.sleeps.borrow().len(), 1);
}
