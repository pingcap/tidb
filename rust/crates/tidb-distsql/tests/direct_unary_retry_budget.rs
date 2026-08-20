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

//! What a retry is allowed to spend: the nil-leader sleep and the PD reload
//! after it, the bind-anchored deadline no wait may cross, cancellation
//! winning over both, and the per-region budget an in-place rebuild and its
//! split children each get.

#![allow(missing_docs)]

use crate::direct_unary_client_fixture::*;

#[test]
fn nil_leader_sleeps_then_invalidates_reloads_and_resends() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let loader_calls = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Rc::new(RecordingRetryControl::default());
    let transport = transport_with_loader_calls_and_config(
        Rc::clone(&calls),
        [Ok(not_leader(1, None)), Ok(response(b"reloaded"))],
        [
            location(1, "a", "z", "tikv-old:20160"),
            location(1, "a", "z", "tikv-new:20160"),
        ],
        9001,
        Rc::clone(&loader_calls),
        DirectUnaryRuntimeConfig {
            seed_read_bytes: 4096,
            observation_time,
            region_retry_waiter: retry_control.clone(),
            ..DirectUnaryRuntimeConfig::default()
        },
    );
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));

    assert_eq!(result.next_raw().unwrap(), Some(b"reloaded".to_vec()));
    assert_eq!(calls.borrow()[0].address, "tikv-old:20160");
    assert_eq!(calls.borrow()[1].address, "tikv-new:20160");
    assert_eq!(
        loader_calls.borrow().as_slice(),
        [b"a".to_vec(), b"a".to_vec()]
    );
    assert_eq!(
        retry_control.sleeps.borrow().as_slice(),
        [Duration::from_millis(2), Duration::from_millis(2)]
    );
}

#[test]
fn cancellation_during_nil_leader_sleep_keeps_cached_route_and_skips_pd_and_redispatch() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let loader_calls = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Rc::new(RecordingRetryControl::default());
    retry_control.fail_next_sleep.set(true);
    let transport = transport_with_loader_calls_and_config(
        Rc::clone(&calls),
        [Ok(not_leader(1, None)), Ok(response(b"same-cached-route"))],
        [
            location(1, "a", "z", "tikv-old:20160"),
            location(1, "a", "z", "must-remain-unloaded:20160"),
        ],
        9001,
        Rc::clone(&loader_calls),
        DirectUnaryRuntimeConfig {
            seed_read_bytes: 4096,
            observation_time,
            region_retry_waiter: retry_control.clone(),
            ..DirectUnaryRuntimeConfig::default()
        },
    );
    let mut runtime = InjectedQueryRuntime::new(transport);
    let request = transport_request(metadata("a", "z"));

    let mut cancelled = select_result(&mut runtime, &request);
    let error = cancelled.next_raw().unwrap_err().to_string();
    assert!(error.contains("query cancelled by caller"), "{error}");
    assert_eq!(calls.borrow().len(), 1);
    assert_eq!(loader_calls.borrow().as_slice(), [b"a".to_vec()]);

    let mut next_query = select_result(&mut runtime, &transport_request(metadata("a", "z")));
    assert_eq!(
        next_query.next_raw().unwrap(),
        Some(b"same-cached-route".to_vec())
    );
    assert_eq!(calls.borrow()[1].address, "tikv-old:20160");
    assert_eq!(loader_calls.borrow().as_slice(), [b"a".to_vec()]);
}

#[test]
fn retry_wait_never_crosses_the_bind_anchored_deadline() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let events = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Rc::new(RecordingRetryControl::default());
    let transport = transport_with_transport_failures(
        Rc::clone(&calls),
        [Err(connection_failure(
            "tikv-old:20160",
            1,
            DirectUnaryTransportClass::Connection,
            None,
        ))],
        [Ok(StoreLiveness::Reachable)],
        Rc::clone(&events),
        [location(1, "a", "z", "tikv-old:20160")],
        DirectUnaryRuntimeConfig {
            region_retry_waiter: retry_control.clone(),
            ..DirectUnaryRuntimeConfig::default()
        },
    );
    let mut request_metadata = metadata("a", "z");
    request_metadata.tikv_client_read_timeout_ms = 50;
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut result = select_result(&mut runtime, &transport_request(request_metadata));

    let error = result.next_raw().unwrap_err().to_string();
    assert!(error.contains("query deadline exceeded"), "{error}");
    assert!(!error.contains("cancelled by caller"), "{error}");
    assert!(retry_control.sleeps.borrow().is_empty());
    assert_eq!(calls.borrow().len(), 1);
    assert_eq!(
        events
            .borrow()
            .iter()
            .filter(|event| matches!(event, ClientEvent::Liveness { .. }))
            .count(),
        1
    );
}

#[test]
fn elapsed_deadline_blocks_zero_wait_dispatch_and_cancellation_wins() {
    for cancel_execution in [false, true] {
        let calls = Rc::new(RefCell::new(Vec::new()));
        let retry_control = Rc::new(RecordingRetryControl::default());
        let execution = std::sync::Arc::new(tidb_distsql::CancelHandle::default());
        let mut request_metadata = metadata("a", "z");
        request_metadata.tikv_client_read_timeout_ms = 1;
        let request = TransportRequest::new(request_metadata, std::sync::Arc::clone(&execution));
        let transport = transport_with_loader_calls_and_config(
            Rc::clone(&calls),
            [Ok(response(b"must-not-dispatch"))],
            [location(1, "a", "z", "tikv-1:20160")],
            9001,
            Rc::new(RefCell::new(Vec::new())),
            DirectUnaryRuntimeConfig {
                region_retry_waiter: retry_control.clone(),
                ..DirectUnaryRuntimeConfig::default()
            },
        );
        let mut runtime = InjectedQueryRuntime::new(transport);
        let mut result = select_result(&mut runtime, &request);
        std::thread::sleep(Duration::from_millis(3));
        if cancel_execution {
            execution.cancel();
        }

        let error = result.next_raw().unwrap_err().to_string();
        if cancel_execution {
            assert!(error.contains("query cancelled by caller"), "{error}");
        } else {
            assert!(error.contains("query deadline exceeded"), "{error}");
        }
        assert!(calls.borrow().is_empty());
        assert!(retry_control.sleeps.borrow().is_empty());
    }
}

#[test]
fn rebuild_splits_failed_task_in_place_and_keeps_future_task_order_and_attempt() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let loader_calls = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Rc::new(RecordingRetryControl::default());
    let transport = transport_with_loader_calls_and_config(
        Rc::clone(&calls),
        [
            Ok(region_not_found(1)),
            Ok(response(b"split-left")),
            Ok(response(b"split-right")),
            Ok(response(b"future-original")),
        ],
        [
            location(1, "a", "m", "tikv-old-1:20160"),
            location(2, "m", "z", "tikv-old-2:20160"),
            location(10, "a", "g", "tikv-new-10:20160"),
            location(11, "g", "m", "tikv-new-11:20160"),
        ],
        9001,
        Rc::clone(&loader_calls),
        DirectUnaryRuntimeConfig {
            seed_read_bytes: 4096,
            observation_time,
            region_retry_waiter: retry_control,
            ..DirectUnaryRuntimeConfig::default()
        },
    );
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));

    assert_eq!(result.next_raw().unwrap(), Some(b"split-left".to_vec()));
    assert_eq!(result.next_raw().unwrap(), Some(b"split-right".to_vec()));
    assert_eq!(
        result.next_raw().unwrap(),
        Some(b"future-original".to_vec())
    );
    assert_eq!(result.next_raw().unwrap(), None);
    let calls = calls.borrow();
    assert_eq!(
        calls
            .iter()
            .map(|call| call.address.as_str())
            .collect::<Vec<_>>(),
        [
            "tikv-old-1:20160",
            "tikv-new-10:20160",
            "tikv-new-11:20160",
            "tikv-old-2:20160",
        ]
    );
    assert_eq!(calls[3].region_id, 2);
    assert_eq!(calls[3].task_id, 29);
}

#[test]
fn unordered_rebuild_replaces_the_completed_region_instead_of_the_first_region() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let loader_calls = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Rc::new(RecordingRetryControl::default());
    let transport = batch_first_transport_with_config(
        Rc::clone(&calls),
        [
            Ok(response(b"left-pending")),
            Ok(region_not_found(2)),
            Ok(response(b"split-middle")),
            Ok(response(b"split-right")),
        ],
        [
            location(1, "a", "m", "tikv-old-1:20160"),
            location(2, "m", "z", "tikv-old-2:20160"),
            location(20, "m", "t", "tikv-new-20:20160"),
            location(21, "t", "z", "tikv-new-21:20160"),
        ],
        [false, true, true, true],
        Rc::clone(&loader_calls),
        DirectUnaryRuntimeConfig {
            region_retry_waiter: retry_control,
            ..DirectUnaryRuntimeConfig::default()
        },
    );
    let mut request_metadata = metadata("a", "z");
    request_metadata.keep_order = false;
    request_metadata.concurrency = 2;
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut result = select_result(&mut runtime, &transport_request(request_metadata));

    assert_eq!(
        result.next_raw().unwrap(),
        Some(b"split-middle".to_vec())
    );
    assert_eq!(result.next_raw().unwrap(), Some(b"split-right".to_vec()));
    assert_eq!(
        calls
            .borrow()
            .iter()
            .map(|call| call.address.as_str())
            .collect::<Vec<_>>(),
        [
            "tikv-old-1:20160",
            "tikv-old-2:20160",
            "tikv-new-20:20160",
            "tikv-new-21:20160",
        ]
    );
}

#[test]
fn one_region_budget_is_shared_by_sender_and_outer_rebuild_backoff() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Rc::new(RecordingRetryControl::default());
    let transport = transport_with_loader_calls_and_config(
        Rc::clone(&calls),
        [Ok(not_leader(1, None))],
        [location(1, "a", "z", "tikv-old:20160")],
        9001,
        Rc::new(RefCell::new(Vec::new())),
        DirectUnaryRuntimeConfig {
            region_retry_waiter: retry_control.clone(),
            region_retry_max_sleep: Duration::from_millis(1),
            ..DirectUnaryRuntimeConfig::default()
        },
    );
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));

    let error = result.next_raw().unwrap_err().to_string();
    assert!(error.contains("terminal region error"), "{error}");
    assert_eq!(calls.borrow().len(), 1);
    assert_eq!(
        retry_control.sleeps.borrow().as_slice(),
        [Duration::from_millis(2)]
    );
}

#[test]
fn split_child_region_gets_an_independent_budget() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Rc::new(RecordingRetryControl::default());
    let transport = transport_with_loader_calls_and_config(
        Rc::clone(&calls),
        [Ok(region_not_found(1)), Ok(not_leader(10, None))],
        [
            location(1, "a", "z", "tikv-old:20160"),
            location(10, "a", "m", "tikv-new-10:20160"),
            location(11, "m", "z", "tikv-new-11:20160"),
        ],
        9001,
        Rc::new(RefCell::new(Vec::new())),
        DirectUnaryRuntimeConfig {
            region_retry_waiter: retry_control.clone(),
            region_retry_max_sleep: Duration::from_millis(1),
            ..DirectUnaryRuntimeConfig::default()
        },
    );
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));

    let error = result.next_raw().unwrap_err().to_string();
    assert!(error.contains("terminal region error"), "{error}");
    assert_eq!(calls.borrow().len(), 2);
    assert_eq!(calls.borrow()[1].region_id, 10);
    assert_eq!(
        retry_control.sleeps.borrow().as_slice(),
        [Duration::from_millis(2), Duration::from_millis(2)]
    );
}

#[test]
fn unknown_region_error_invalidates_and_rebuilds_under_outer_region_miss() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let loader_calls = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Rc::new(RecordingRetryControl::default());
    let transport = transport_with_loader_calls_and_config(
        Rc::clone(&calls),
        [
            Ok(unknown_region_error("future kvproto field")),
            Ok(response(b"rebuilt")),
        ],
        [
            location(1, "a", "z", "tikv-old:20160"),
            location(1, "a", "z", "tikv-reloaded:20160"),
        ],
        9001,
        Rc::clone(&loader_calls),
        DirectUnaryRuntimeConfig {
            region_retry_waiter: retry_control.clone(),
            ..DirectUnaryRuntimeConfig::default()
        },
    );
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));

    assert_eq!(result.next_raw().unwrap(), Some(b"rebuilt".to_vec()));
    assert_eq!(calls.borrow()[1].address, "tikv-reloaded:20160");
    assert_eq!(
        loader_calls.borrow().as_slice(),
        [b"a".to_vec(), b"a".to_vec()]
    );
    assert_eq!(
        retry_control.sleeps.borrow().as_slice(),
        [Duration::from_millis(2)]
    );
}
