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

//! Transport failures and who consumes them: attempt exhaustion rebuilding
//! through a region miss instead of returning the client error, caller
//! cancellation being terminal before any failure is consumed or retry state
//! mutated, non-connection failures closing without a future dispatch, and a
//! batch retry keeping its logical request selector.

#![allow(missing_docs)]

use crate::direct_unary_client_fixture::*;

#[test]
fn transport_attempt_exhaustion_rebuilds_through_region_miss_instead_of_returning_client() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let events = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Rc::new(RecordingRetryControl::default());
    let mut responses = Vec::new();
    for version in 1..=10 {
        responses.push(Err(connection_failure(
            "tikv-stuck:20160",
            version,
            DirectUnaryTransportClass::Connection,
            None,
        )));
    }
    responses.push(Ok(response(b"reloaded-after-exhaustion")));
    let mut runtime = InjectedQueryRuntime::new(transport_with_transport_failures(
        Rc::clone(&calls),
        responses,
        std::iter::repeat_n(Ok(StoreLiveness::Reachable), 10),
        Rc::clone(&events),
        [
            location(1, "a", "z", "tikv-stuck:20160"),
            location(1, "a", "z", "tikv-reloaded:20160"),
        ],
        DirectUnaryRuntimeConfig {
            region_retry_waiter: retry_control.clone(),
            region_retry_max_sleep: Duration::from_secs(60),
            ..DirectUnaryRuntimeConfig::default()
        },
    ));
    let mut request_metadata = metadata("a", "z");
    request_metadata.tikv_client_read_timeout_ms = 60_000;
    let mut result = select_result(&mut runtime, &transport_request(request_metadata));

    assert_eq!(
        result.next_raw().unwrap(),
        Some(b"reloaded-after-exhaustion".to_vec())
    );
    assert_eq!(result.next_raw().unwrap(), None);
    let calls = calls.borrow();
    assert_eq!(calls.len(), 11);
    assert!(calls[..10]
        .iter()
        .all(|call| call.address == "tikv-stuck:20160"));
    assert_eq!(calls[10].address, "tikv-reloaded:20160");
    assert_eq!(
        events
            .borrow()
            .iter()
            .filter(|event| matches!(event, ClientEvent::Liveness { .. }))
            .count(),
        10
    );
    assert_eq!(retry_control.sleeps.borrow().len(), 11);
}

#[test]
fn caller_cancellation_is_terminal_before_failure_consumption_or_retry_mutation() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let events = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Rc::new(RecordingRetryControl::default());
    let mut runtime = InjectedQueryRuntime::new(transport_with_transport_failures(
        Rc::clone(&calls),
        [
            Err(DirectUnaryClientError::CallerCancelled),
            Ok(response(b"same-cached-route-next-query")),
        ],
        [],
        Rc::clone(&events),
        [location(1, "a", "z", "tikv-1:20160")],
        DirectUnaryRuntimeConfig {
            region_retry_waiter: retry_control.clone(),
            ..DirectUnaryRuntimeConfig::default()
        },
    ));
    let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));

    let error = result.next_raw().unwrap_err().to_string();
    assert!(error.contains("cancelled by caller"), "{error}");
    assert_eq!(result.next_raw().unwrap(), None);
    drop(result);
    let mut next = select_result(&mut runtime, &transport_request(metadata("a", "z")));
    assert_eq!(
        next.next_raw().unwrap(),
        Some(b"same-cached-route-next-query".to_vec())
    );
    assert_eq!(next.next_raw().unwrap(), None);
    assert_eq!(calls.borrow().len(), 2);
    assert!(calls
        .borrow()
        .iter()
        .all(|call| call.address == "tikv-1:20160"));
    assert_eq!(
        events.borrow().as_slice(),
        [
            ClientEvent::Send("tikv-1:20160".to_owned()),
            ClientEvent::Send("tikv-1:20160".to_owned()),
        ]
    );
    assert!(retry_control.sleeps.borrow().is_empty());
}

#[test]
fn return_region_error_and_non_connection_failures_close_without_future_dispatch() {
    let cases = [
        (
            Ok(undetermined_region_error("ambiguous write")),
            "region_error",
        ),
        (Ok(raft_entry_too_large(1)), "terminal region error"),
        (Err("connection reset".to_owned()), "connection reset"),
        (Ok(vec![0x0a, 0x02, 0x01]), "invalid unary response"),
    ];
    for (scripted, expected) in cases {
        let calls = Rc::new(RefCell::new(Vec::new()));
        let mut runtime = InjectedQueryRuntime::new(transport(
            Rc::clone(&calls),
            [scripted, Ok(response(b"must remain unsent"))],
            [
                location(1, "a", "m", "tikv-1:20160"),
                location(2, "m", "z", "tikv-2:20160"),
            ],
        ));
        let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));
        let error = result.next_raw().unwrap_err().to_string();
        assert!(error.contains(expected), "{error}");
        assert_eq!(result.next_raw().unwrap(), None);
        // A terminal error on the first logical task must not probe the next
        // address or consume its scripted response.
        assert_eq!(calls.borrow().len(), 1);
    }
}

#[test]
fn batch_transport_failure_retains_the_logical_request_selector_for_batch_retry() {
    let source = include_str!("../src/cop_paging/direct_unary_query_transport.rs");
    let settle = source
        .find("fn settle_dispatch(")
        .expect("shared async and sync settlement owner");
    let record = source[settle..]
        .find("self.record_attempt_result(logical_task_id")
        .map(|offset| settle + offset)
        .expect("one selector records the failed batch attempt");
    let recover = source[record..]
        .find("self.recover_transport_failure(")
        .map(|offset| record + offset)
        .expect("same synchronous recovery loop");
    let recovery_owner = source[recover..]
        .find("fn recover_transport_failure(")
        .map(|offset| recover + offset)
        .expect("response-owned synchronous recovery method");
    let retry = source[recovery_owner..]
        .find("self.install_same_task_retry(replacement)")
        .map(|offset| recovery_owner + offset)
        .expect("same logical task is reinstalled");
    let next_dispatch = &source[retry..];
    assert!(record < recover);
    assert!(recover < retry);
    assert!(next_dispatch.contains("debug_assert!(self.request_selectors.contains_key"));
    assert!(!source[recovery_owner..retry].contains("request_selectors.remove(&logical_task_id)"));
}
