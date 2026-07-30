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

//! `StoreNotMatch` and the channel it is allowed to close.
//!
//! The load-bearing distinction is *which* channel: a current mismatch closes
//! the exact channel it observed, while a delayed or stale-forwarded one must
//! not close the replacement that took its place, and a proxy channel shared
//! by several logical targets must survive a mismatch on one of them. The two
//! scripted clients here exist to make "the channel was replaced in between"
//! reproducible.

#![allow(missing_docs)]

use crate::direct_unary_client_fixture::*;

#[derive(Debug, Default)]
struct DelayedStoreMismatchState {
    active_generation: Option<u64>,
    next_generation: u64,
    close_requests: Vec<u64>,
    force_closed_generations: Vec<u64>,
    sent_generations: Vec<u64>,
}

struct DelayedStoreMismatchClient {
    state: Rc<RefCell<DelayedStoreMismatchState>>,
    replace_before_first_response: bool,
    cancel_after_first_response: bool,
}

impl DirectUnaryClient for DelayedStoreMismatchClient {
    fn send_request(
        &mut self,
        address: &str,
        _request: &DirectUnaryRequest,
        _timeout: Duration,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        assert_eq!(address, "shared-tikv:20160");
        let mut state = self.state.borrow_mut();
        let generation = match state.active_generation {
            Some(generation) => generation,
            None => {
                state.next_generation += 1;
                let generation = state.next_generation;
                state.active_generation = Some(generation);
                generation
            }
        };
        state.sent_generations.push(generation);
        if state.sent_generations.len() == 1 {
            assert_eq!(generation, 1);
            if self.replace_before_first_response {
                // Session B replaces the channel while session A's request is
                // in flight. The delayed StoreNotMatch below still belongs to
                // generation 1 and must not close this replacement.
                state.active_generation = Some(2);
                state.next_generation = 2;
            }
            return Ok(DirectUnaryResponse::new(
                store_not_match(1),
                address,
                generation,
            ));
        }
        assert_eq!(generation, 2);
        Ok(DirectUnaryResponse::new(
            response(b"replacement-survived"),
            address,
            generation,
        ))
    }

    fn send_request_with_context(
        &mut self,
        address: &str,
        request: &DirectUnaryRequest,
        call: &UnaryCallContext,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        let result = self.send_request(address, request, call.timeout());
        if self.cancel_after_first_response && self.state.borrow().sent_generations.len() == 1 {
            call.cancellation().cancel();
        }
        result
    }

    fn close_address(&mut self, address: &str) -> Result<(), DirectUnaryClientError> {
        assert_eq!(address, "shared-tikv:20160");
        let mut state = self.state.borrow_mut();
        if let Some(generation) = state.active_generation.take() {
            state.force_closed_generations.push(generation);
        }
        Ok(())
    }

    fn close_address_version(
        &mut self,
        address: &str,
        version: u64,
    ) -> Result<(), DirectUnaryClientError> {
        assert_eq!(address, "shared-tikv:20160");
        let mut state = self.state.borrow_mut();
        state.close_requests.push(version);
        if state
            .active_generation
            .is_some_and(|generation| generation == version)
        {
            let generation = state.active_generation.take().unwrap();
            state.force_closed_generations.push(generation);
        }
        Ok(())
    }

    fn liveness(
        &self,
        _address: &str,
        _timeout: Duration,
    ) -> Result<StoreLiveness, DirectUnaryClientError> {
        Ok(StoreLiveness::Reachable)
    }

    fn close(&mut self) -> Result<(), DirectUnaryClientError> {
        Ok(())
    }
}

impl tidb_txnkv::lock::LockRecoveryClient for DelayedStoreMismatchClient {

    fn check_secondary_locks_for_lock(
        &mut self,
        _address: &str,
        _request: &tidb_proto::KvrpcCheckSecondaryLocksRequest,
        _context: &tidb_proto::KvrpcContext,
        _call: &tidb_txnkv::UnaryCallContext,
    ) -> Result<tidb_proto::KvrpcCheckSecondaryLocksResponse, DirectUnaryClientError> {
        panic!("this test does not resolve async-commit locks")
    }
    fn check_txn_status_for_lock(
        &mut self,
        _address: &str,
        _request: &tidb_proto::KvrpcCheckTxnStatusRequest,
        _context: &tidb_proto::KvrpcContext,
        _call: &UnaryCallContext,
    ) -> Result<tidb_proto::KvrpcCheckTxnStatusResponse, DirectUnaryClientError> {
        Err(DirectUnaryClientError::InvalidRequest(
            "unexpected lock in delayed StoreNotMatch read".to_owned(),
        ))
    }


    fn pessimistic_rollback_for_lock(
        &mut self,
        _address: &str,
        _request: &tidb_proto::KvrpcPessimisticRollbackRequest,
        _context: &tidb_proto::KvrpcContext,
        _call: &UnaryCallContext,
    ) -> Result<tidb_proto::KvrpcPessimisticRollbackResponse, DirectUnaryClientError> {
        panic!("runtime test does not clean pessimistic locks")
    }
    fn resolve_lock_for_read(
        &mut self,
        _address: &str,
        _request: &tidb_proto::KvrpcResolveLockRequest,
        _context: &tidb_proto::KvrpcContext,
        _call: &UnaryCallContext,
    ) -> Result<tidb_proto::KvrpcResolveLockResponse, DirectUnaryClientError> {
        Err(DirectUnaryClientError::InvalidRequest(
            "unexpected lock in delayed StoreNotMatch read".to_owned(),
        ))
    }
}

#[derive(Debug, Default)]
struct ForwardedStaleMismatchState {
    calls: Vec<(String, Option<String>)>,
    close_requests: Vec<(String, u64)>,
    active_proxy_version: u64,
}

struct ForwardedStaleMismatchClient {
    state: Rc<RefCell<ForwardedStaleMismatchState>>,
}

impl DirectUnaryClient for ForwardedStaleMismatchClient {
    fn send_request(
        &mut self,
        address: &str,
        request: &DirectUnaryRequest,
        timeout: Duration,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        self.send_request_with_route(
            address,
            None,
            request,
            &UnaryCallContext::with_timeout(timeout),
        )
    }

    fn send_request_with_context(
        &mut self,
        address: &str,
        request: &DirectUnaryRequest,
        call: &UnaryCallContext,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        self.send_request_with_route(address, None, request, call)
    }

    fn send_request_with_route(
        &mut self,
        address: &str,
        forwarded_host: Option<&str>,
        _request: &DirectUnaryRequest,
        _call: &UnaryCallContext,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        let mut state = self.state.borrow_mut();
        state
            .calls
            .push((address.to_owned(), forwarded_host.map(str::to_owned)));
        match state.calls.len() {
            1 => Err(DirectUnaryClientError::Connection(
                DirectUnaryConnectionError::connection(
                    "logical-target:20160",
                    1,
                    "force forwarding".to_owned(),
                ),
            )),
            2 => {
                assert_eq!(address, "shared-proxy:20160");
                assert_eq!(forwarded_host, Some("logical-target:20160"));
                state.active_proxy_version = 2;
                Ok(DirectUnaryResponse::new(store_not_match(1), address, 1))
            }
            3 => {
                assert_eq!(address, "shared-proxy:20160");
                assert_eq!(forwarded_host, Some("logical-target:20160"));
                Ok(DirectUnaryResponse::new(
                    response(b"stale-forwarded-route-reloaded"),
                    address,
                    state.active_proxy_version,
                ))
            }
            calls => panic!("unexpected forwarded stale call {calls}"),
        }
    }

    fn close_address(&mut self, address: &str) -> Result<(), DirectUnaryClientError> {
        self.state
            .borrow_mut()
            .close_requests
            .push((address.to_owned(), 0));
        Ok(())
    }

    fn close_address_version(
        &mut self,
        address: &str,
        version: u64,
    ) -> Result<(), DirectUnaryClientError> {
        self.state
            .borrow_mut()
            .close_requests
            .push((address.to_owned(), version));
        Ok(())
    }

    fn liveness(
        &self,
        address: &str,
        _timeout: Duration,
    ) -> Result<StoreLiveness, DirectUnaryClientError> {
        assert_eq!(address, "logical-target:20160");
        Ok(StoreLiveness::Unreachable)
    }

    fn close(&mut self) -> Result<(), DirectUnaryClientError> {
        Ok(())
    }
}

impl tidb_txnkv::lock::LockRecoveryClient for ForwardedStaleMismatchClient {

    fn check_secondary_locks_for_lock(
        &mut self,
        _address: &str,
        _request: &tidb_proto::KvrpcCheckSecondaryLocksRequest,
        _context: &tidb_proto::KvrpcContext,
        _call: &tidb_txnkv::UnaryCallContext,
    ) -> Result<tidb_proto::KvrpcCheckSecondaryLocksResponse, DirectUnaryClientError> {
        panic!("this test does not resolve async-commit locks")
    }
    fn check_txn_status_for_lock(
        &mut self,
        _address: &str,
        _request: &tidb_proto::KvrpcCheckTxnStatusRequest,
        _context: &tidb_proto::KvrpcContext,
        _call: &UnaryCallContext,
    ) -> Result<tidb_proto::KvrpcCheckTxnStatusResponse, DirectUnaryClientError> {
        Err(DirectUnaryClientError::InvalidRequest(
            "unexpected lock in forwarded stale read".to_owned(),
        ))
    }


    fn pessimistic_rollback_for_lock(
        &mut self,
        _address: &str,
        _request: &tidb_proto::KvrpcPessimisticRollbackRequest,
        _context: &tidb_proto::KvrpcContext,
        _call: &UnaryCallContext,
    ) -> Result<tidb_proto::KvrpcPessimisticRollbackResponse, DirectUnaryClientError> {
        panic!("runtime test does not clean pessimistic locks")
    }
    fn resolve_lock_for_read(
        &mut self,
        _address: &str,
        _request: &tidb_proto::KvrpcResolveLockRequest,
        _context: &tidb_proto::KvrpcContext,
        _call: &UnaryCallContext,
    ) -> Result<tidb_proto::KvrpcResolveLockResponse, DirectUnaryClientError> {
        Err(DirectUnaryClientError::InvalidRequest(
            "unexpected lock in forwarded stale read".to_owned(),
        ))
    }
}

fn run_store_not_match_with_channel_replacement(
    replace_before_first_response: bool,
) -> Rc<RefCell<DelayedStoreMismatchState>> {
    let state = Rc::new(RefCell::new(DelayedStoreMismatchState {
        active_generation: Some(1),
        next_generation: 1,
        ..DelayedStoreMismatchState::default()
    }));
    let transport = DirectUnaryQueryTransport::new_injected(
        DelayedStoreMismatchClient {
            state: Rc::clone(&state),
            replace_before_first_response,
            cancel_after_first_response: false,
        },
        RegionCache::new(ScriptedLoader {
            cluster_id: 9001,
            calls: Rc::new(RefCell::new(Vec::new())),
            regions: [
                location(1, "a", "z", "shared-tikv:20160"),
                location(1, "a", "z", "shared-tikv:20160"),
            ]
            .into_iter()
            .collect(),
        }),
        DirectUnaryRuntimeConfig::default(),
        tidb_txnkv::lock::FixedTimestampSource::new(1 << 18),
    )
    .unwrap();
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut result = runtime
        .select_with_runtime_stats(
            &transport_request(metadata("a", "z")),
            SelectInput::default(),
            QueryResultContext::new(Vec::<FieldType>::new(), WarningCollector::new()),
            vec![1],
            2,
            true,
        )
        .unwrap();

    assert_eq!(
        result.next_raw().unwrap(),
        Some(b"replacement-survived".to_vec())
    );
    assert_eq!(result.next_raw().unwrap(), None);
    drop(result);
    state
}

#[test]
fn current_store_not_match_closes_exact_observed_channel() {
    let state = run_store_not_match_with_channel_replacement(false);
    let state = state.borrow();
    assert_eq!(state.sent_generations, [1, 2]);
    assert_eq!(state.close_requests, [1]);
    assert_eq!(state.force_closed_generations, [1]);
    assert_eq!(state.active_generation, Some(2));
}

#[test]
fn delayed_store_not_match_cannot_close_a_replacement_channel() {
    let state = run_store_not_match_with_channel_replacement(true);
    let state = state.borrow();
    assert_eq!(state.sent_generations, [1, 2]);
    assert_eq!(state.close_requests, [1]);
    assert!(state.force_closed_generations.is_empty());
    assert_eq!(state.active_generation, Some(2));
}

#[test]
fn caller_cancellation_wins_before_store_not_match_mutates_channel_or_route() {
    let state = Rc::new(RefCell::new(DelayedStoreMismatchState {
        active_generation: Some(1),
        next_generation: 1,
        ..DelayedStoreMismatchState::default()
    }));
    let loader_calls = Rc::new(RefCell::new(Vec::new()));
    let transport = DirectUnaryQueryTransport::new_injected(
        DelayedStoreMismatchClient {
            state: Rc::clone(&state),
            replace_before_first_response: false,
            cancel_after_first_response: true,
        },
        RegionCache::new(ScriptedLoader {
            cluster_id: 9001,
            calls: Rc::clone(&loader_calls),
            regions: VecDeque::from([location(1, "a", "z", "shared-tikv:20160")]),
        }),
        DirectUnaryRuntimeConfig::default(),
        tidb_txnkv::lock::FixedTimestampSource::new(1 << 18),
    )
    .unwrap();
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut result = runtime
        .select_with_runtime_stats(
            &transport_request(metadata("a", "z")),
            SelectInput::default(),
            QueryResultContext::new(Vec::<FieldType>::new(), WarningCollector::new()),
            vec![1],
            2,
            true,
        )
        .unwrap();

    let error = result.next_raw().unwrap_err().to_string();
    assert!(error.contains("cancelled by caller"), "{error}");
    assert_eq!(result.next_raw().unwrap(), None);
    let state = state.borrow();
    assert_eq!(state.sent_generations, [1]);
    assert!(state.close_requests.is_empty());
    assert!(state.force_closed_generations.is_empty());
    assert_eq!(state.active_generation, Some(1));
    assert_eq!(loader_calls.borrow().as_slice(), &[b"a".to_vec()]);
}

#[test]
fn stale_forwarded_store_not_match_preserves_replacement_proxy_channel() {
    let state = Rc::new(RefCell::new(ForwardedStaleMismatchState {
        active_proxy_version: 1,
        ..ForwardedStaleMismatchState::default()
    }));
    let loader_calls = Rc::new(RefCell::new(Vec::new()));
    let location =
        location_with_second_peer(1, "a", "z", "logical-target:20160", "shared-proxy:20160");
    let transport = DirectUnaryQueryTransport::new_injected(
        ForwardedStaleMismatchClient {
            state: Rc::clone(&state),
        },
        RegionCache::new(ScriptedLoader {
            cluster_id: 9001,
            calls: Rc::clone(&loader_calls),
            regions: VecDeque::from([location.clone(), location]),
        }),
        DirectUnaryRuntimeConfig {
            enable_forwarding: true,
            region_retry_waiter: Rc::new(RecordingRetryControl::default()),
            ..DirectUnaryRuntimeConfig::default()
        },
        tidb_txnkv::lock::FixedTimestampSource::new(1 << 18),
    )
    .unwrap();
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut result = runtime
        .select_with_runtime_stats(
            &transport_request(metadata("a", "z")),
            SelectInput::default(),
            QueryResultContext::new(Vec::<FieldType>::new(), WarningCollector::new()),
            vec![1],
            2,
            true,
        )
        .unwrap();

    assert_eq!(
        result.next_raw().unwrap(),
        Some(b"stale-forwarded-route-reloaded".to_vec())
    );
    assert_eq!(result.next_raw().unwrap(), None);
    let state = state.borrow();
    assert_eq!(
        state.calls,
        [
            ("logical-target:20160".to_owned(), None),
            (
                "shared-proxy:20160".to_owned(),
                Some("logical-target:20160".to_owned()),
            ),
            (
                "shared-proxy:20160".to_owned(),
                Some("logical-target:20160".to_owned()),
            ),
        ]
    );
    assert!(
        state.close_requests.is_empty(),
        "StoreNotMatch must not close the shared proxy's replacement channel"
    );
    assert_eq!(state.active_proxy_version, 2);
    assert_eq!(
        loader_calls.borrow().as_slice(),
        &[b"a".to_vec(), b"a".to_vec()]
    );
}

#[test]
fn shared_proxy_store_not_match_refreshes_only_affected_logical_target() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let events = Rc::new(RefCell::new(Vec::new()));
    let loader_calls = Rc::new(RefCell::new(Vec::new()));
    let mut left = location_with_second_peer(1, "a", "m", "target-a:20160", "shared-proxy:20160");
    let mut right = location_with_second_peer(2, "m", "z", "target-b:20160", "shared-proxy:20160");
    for location in [&mut left, &mut right] {
        location.peers[1].store_id = 999;
        location.stores[1].id = 999;
    }
    let transport = DirectUnaryQueryTransport::new_injected(
        ScriptedClient {
            calls: Rc::clone(&calls),
            responses: VecDeque::from([
                Err(connection_failure(
                    "target-a:20160",
                    1,
                    DirectUnaryTransportClass::Connection,
                    None,
                )),
                Ok(store_not_match(1)),
                Ok(response(b"left-after-refresh")),
                Err(connection_failure(
                    "target-b:20160",
                    1,
                    DirectUnaryTransportClass::Connection,
                    None,
                )),
                Ok(response(b"right-without-refresh")),
            ]),
            events: Rc::clone(&events),
            liveness: RefCell::new(VecDeque::from([
                Ok(StoreLiveness::Unreachable),
                Ok(StoreLiveness::Unreachable),
            ])),
            batch_errors: RefCell::new(VecDeque::new()),
            batch_completion_gate: None,
        },
        RegionCache::new(ScriptedLoader {
            cluster_id: 9001,
            calls: Rc::clone(&loader_calls),
            regions: VecDeque::from([left.clone(), right, left]),
        }),
        DirectUnaryRuntimeConfig {
            enable_forwarding: true,
            region_retry_waiter: Rc::new(RecordingRetryControl::default()),
            ..DirectUnaryRuntimeConfig::default()
        },
        tidb_txnkv::lock::FixedTimestampSource::new(1 << 18),
    )
    .unwrap();
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));

    assert_eq!(
        result.next_raw().unwrap(),
        Some(b"left-after-refresh".to_vec())
    );
    assert_eq!(
        result.next_raw().unwrap(),
        Some(b"right-without-refresh".to_vec())
    );
    assert_eq!(result.next_raw().unwrap(), None);
    assert_eq!(
        calls
            .borrow()
            .iter()
            .map(|call| (call.address.as_str(), call.forwarded_host.as_deref()))
            .collect::<Vec<_>>(),
        [
            ("target-a:20160", None),
            ("shared-proxy:20160", Some("target-a:20160")),
            ("shared-proxy:20160", Some("target-a:20160")),
            ("target-b:20160", None),
            ("shared-proxy:20160", Some("target-b:20160")),
        ]
    );
    assert_eq!(
        loader_calls.borrow().as_slice(),
        &[b"a".to_vec(), b"m".to_vec(), b"a".to_vec()]
    );
    assert!(events.borrow().iter().all(|event| {
        !matches!(
            event,
            ClientEvent::ForceClose(address)
                | ClientEvent::CloseGeneration { address, .. }
                if address == "shared-proxy:20160"
        )
    }));
}

#[test]
fn forwarded_store_not_match_invalidates_target_without_closing_proxy_channel() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let events = Rc::new(RefCell::new(Vec::new()));
    let location =
        location_with_second_peer(1, "a", "z", "logical-target:20160", "healthy-proxy:20160");
    let mut runtime = InjectedQueryRuntime::new(transport_with_transport_failures(
        Rc::clone(&calls),
        [
            Err(connection_failure(
                "logical-target:20160",
                1,
                DirectUnaryTransportClass::Connection,
                None,
            )),
            Ok(store_not_match(1)),
            Ok(response(b"logical-route-reloaded")),
        ],
        [Ok(StoreLiveness::Unreachable)],
        Rc::clone(&events),
        [location.clone(), location],
        DirectUnaryRuntimeConfig {
            enable_forwarding: true,
            region_retry_waiter: Rc::new(RecordingRetryControl::default()),
            ..DirectUnaryRuntimeConfig::default()
        },
    ));
    let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));

    assert_eq!(
        result.next_raw().unwrap(),
        Some(b"logical-route-reloaded".to_vec())
    );
    assert_eq!(result.next_raw().unwrap(), None);
    assert_eq!(
        calls
            .borrow()
            .iter()
            .map(|call| call.address.as_str())
            .collect::<Vec<_>>(),
        [
            "logical-target:20160",
            "healthy-proxy:20160",
            "healthy-proxy:20160",
        ]
    );
    assert!(events.borrow().iter().all(|event| {
        !matches!(
            event,
            ClientEvent::ForceClose(address)
                | ClientEvent::CloseGeneration { address, .. }
                if address == "healthy-proxy:20160"
        )
    }));
}
