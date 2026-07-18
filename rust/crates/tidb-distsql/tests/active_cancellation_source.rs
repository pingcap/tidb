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

#![allow(missing_docs)]

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::{mpsc, Arc};
use std::time::Duration;

use tidb_datatype::FieldType;
use tidb_distsql::cop_paging::RegionRetryWaiter;
use tidb_distsql::{
    CancelHandle, DirectUnaryClient, DirectUnaryClientError, DirectUnaryQueryTransport,
    DirectUnaryRequest, DirectUnaryResponse, DirectUnaryRuntimeConfig, InjectedQueryRuntime,
    KvRequestMetadata, LockedResponseAction, LockedResponseDelegate, LockedResponseObservation,
    QueryResultContext, RequestKeyRange, RequestKeyRanges, RequestType, SelectInput, StoreType,
    TransportRequest, WarningCollector,
};
use tidb_txnkv::region::{
    Peer, PeerRole, RegionCache, RegionLoadError, RegionLoader, RegionLocation, RegionMetadata,
    RegionRecoveryLoader, RegionVerId, Store, StoreLiveness, StoreResolveState,
};
use tidb_txnkv::{DirectUnaryConnectionError, SharedReadRuntime, UnaryCallContext};

#[derive(Debug, Default)]
struct NoRetryMutation {
    sleeps: RefCell<Vec<Duration>>,
}

impl RegionRetryWaiter for NoRetryMutation {
    fn wait(&self, cancellation: &tidb_txnkv::UnaryCancellation, delay: Duration) -> bool {
        self.sleeps.borrow_mut().push(delay);
        cancellation.is_cancelled()
    }
}

#[derive(Debug)]
struct RejectUnexpectedLock;

impl<C, L> LockedResponseDelegate<C, L> for RejectUnexpectedLock {
    fn handle_locked_response(
        &self,
        _runtime: &SharedReadRuntime<C, L>,
        _observation: LockedResponseObservation,
    ) -> Result<LockedResponseAction, String> {
        Err("unexpected lock in cancellation test".to_owned())
    }
}

struct RecordingLoader {
    calls: Rc<RefCell<Vec<Vec<u8>>>>,
    regions: VecDeque<RegionLocation>,
}

impl RegionLoader for RecordingLoader {
    fn cluster_id(&self) -> u64 {
        9001
    }

    fn load_region(&mut self, key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        self.calls.borrow_mut().push(key.to_vec());
        self.regions
            .pop_front()
            .ok_or_else(|| RegionLoadError::new("cancel-test-pd-empty", "no region"))
    }
}

impl RegionRecoveryLoader for RecordingLoader {
    fn hydrate_region(
        &mut self,
        metadata: &RegionMetadata,
        _leader_store_id: u64,
    ) -> Result<RegionLocation, RegionLoadError> {
        self.load_region(&metadata.encoded_start_key)
    }
}

#[derive(Debug, Default)]
struct ClientObservations {
    sends: Cell<usize>,
    closes: Cell<usize>,
    liveness: Cell<usize>,
    predicted_read_bytes: RefCell<Vec<u64>>,
    addresses: RefCell<Vec<String>>,
}

struct CancellationBlockingClient {
    observations: Rc<ClientObservations>,
    dispatch_started: Option<mpsc::Sender<()>>,
}

impl DirectUnaryClient for CancellationBlockingClient {
    fn send_request(
        &mut self,
        _address: &str,
        _request: &DirectUnaryRequest,
        _timeout: Duration,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        panic!("direct runtime must preserve the explicit cancellation context")
    }

    fn send_request_with_context(
        &mut self,
        address: &str,
        request: &DirectUnaryRequest,
        call: &UnaryCallContext,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        self.observations
            .sends
            .set(self.observations.sends.get() + 1);
        self.observations
            .predicted_read_bytes
            .borrow_mut()
            .push(request.predicted_read_bytes);
        self.observations
            .addresses
            .borrow_mut()
            .push(address.to_owned());
        self.dispatch_started
            .take()
            .expect("only the first logical task may dispatch")
            .send(())
            .unwrap();
        while !call.cancellation().is_cancelled() {
            std::thread::yield_now();
        }
        Err(DirectUnaryClientError::CallerCancelled)
    }

    fn close_address(&mut self, _address: &str) -> Result<(), DirectUnaryClientError> {
        self.observations
            .closes
            .set(self.observations.closes.get() + 1);
        Ok(())
    }

    fn close_address_version(
        &mut self,
        _address: &str,
        _version: u64,
    ) -> Result<(), DirectUnaryClientError> {
        self.observations
            .closes
            .set(self.observations.closes.get() + 1);
        Ok(())
    }

    fn liveness(
        &self,
        _address: &str,
        _timeout: Duration,
    ) -> Result<StoreLiveness, DirectUnaryClientError> {
        self.observations
            .liveness
            .set(self.observations.liveness.get() + 1);
        Ok(StoreLiveness::Reachable)
    }

    fn close(&mut self) -> Result<(), DirectUnaryClientError> {
        self.observations
            .closes
            .set(self.observations.closes.get() + 1);
        Ok(())
    }
}

struct CancellationThenTransportErrorClient {
    observations: Rc<ClientObservations>,
}

impl DirectUnaryClient for CancellationThenTransportErrorClient {
    fn send_request(
        &mut self,
        _address: &str,
        _request: &DirectUnaryRequest,
        _timeout: Duration,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        panic!("test requires explicit call context")
    }

    fn send_request_with_context(
        &mut self,
        address: &str,
        _request: &DirectUnaryRequest,
        call: &UnaryCallContext,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        self.observations
            .sends
            .set(self.observations.sends.get() + 1);
        call.cancellation().cancel();
        Err(DirectUnaryClientError::Connection(
            DirectUnaryConnectionError::connection(address, 9, "raced transport error".to_owned()),
        ))
    }

    fn close_address(&mut self, _address: &str) -> Result<(), DirectUnaryClientError> {
        self.observations
            .closes
            .set(self.observations.closes.get() + 1);
        Ok(())
    }

    fn close_address_version(
        &mut self,
        _address: &str,
        _version: u64,
    ) -> Result<(), DirectUnaryClientError> {
        self.observations
            .closes
            .set(self.observations.closes.get() + 1);
        Ok(())
    }

    fn liveness(
        &self,
        _address: &str,
        _timeout: Duration,
    ) -> Result<StoreLiveness, DirectUnaryClientError> {
        self.observations
            .liveness
            .set(self.observations.liveness.get() + 1);
        Ok(StoreLiveness::Unreachable)
    }

    fn close(&mut self) -> Result<(), DirectUnaryClientError> {
        Ok(())
    }
}

fn range(start: &str, end: &str) -> RequestKeyRange {
    RequestKeyRange {
        start_key: start.as_bytes().to_vec(),
        end_key: end.as_bytes().to_vec(),
    }
}

fn metadata() -> KvRequestMetadata {
    KvRequestMetadata {
        request_type: RequestType::Dag,
        data: Some(b"cancelled-dag".to_vec()),
        key_ranges: Some(RequestKeyRanges::new_non_partitioned(vec![range("a", "z")])),
        keep_order: true,
        store_type: StoreType::TiKv,
        start_ts: 42,
        read_replica_scope: "global".to_owned(),
        txn_scope: "global".to_owned(),
        ..KvRequestMetadata::default()
    }
}

fn location(region_id: u64, start: &str, end: &str) -> RegionLocation {
    RegionLocation {
        region: RegionVerId::new(region_id, 1, 2),
        start_key: start.as_bytes().to_vec(),
        end_key: end.as_bytes().to_vec(),
        peers: vec![Peer {
            id: region_id + 100,
            store_id: region_id + 200,
            role: PeerRole::Voter,
            is_witness: false,
            store_epoch: 7,
        }],
        leader_peer_id: Some(region_id + 100),
        stores: vec![Store {
            id: region_id + 200,
            address: format!("tikv-{region_id}:20160"),
            epoch: 7,
        }],
    }
}

#[test]
fn pre_cancelled_query_never_reaches_pd_selector_cache_or_client() {
    let cancel = Arc::new(CancelHandle::default());
    cancel.cancel();
    let observations = Rc::new(ClientObservations::default());
    let loader_calls = Rc::new(RefCell::new(Vec::new()));
    let shared = SharedReadRuntime::new(
        CancellationBlockingClient {
            observations: Rc::clone(&observations),
            dispatch_started: None,
        },
        RegionCache::new(RecordingLoader {
            calls: Rc::clone(&loader_calls),
            regions: [location(1, "a", "z")].into_iter().collect(),
        }),
    );
    let transport = DirectUnaryQueryTransport::with_locked_response_delegate(
        shared,
        DirectUnaryRuntimeConfig::default(),
        Rc::new(RejectUnexpectedLock),
    )
    .unwrap();
    let mut runtime = InjectedQueryRuntime::new(transport);
    let error = runtime
        .select_with_runtime_stats(
            &TransportRequest::new(metadata(), Arc::clone(&cancel)),
            SelectInput::default(),
            QueryResultContext::new(Vec::<FieldType>::new(), WarningCollector::new()),
            vec![1],
            2,
            true,
        )
        .err()
        .expect("pre-cancel must be typed before a response exists");

    assert_eq!(error, tidb_distsql::QueryRuntimeError::Cancelled);
    assert!(loader_calls.borrow().is_empty());
    assert_eq!(observations.sends.get(), 0);
    assert_eq!(observations.closes.get(), 0);
    assert_eq!(observations.liveness.get(), 0);
}

#[test]
fn cancellation_after_rpc_wins_over_transport_error_before_recovery_mutation() {
    let cancel = Arc::new(CancelHandle::default());
    let observations = Rc::new(ClientObservations::default());
    let loader_calls = Rc::new(RefCell::new(Vec::new()));
    let shared = SharedReadRuntime::new(
        CancellationThenTransportErrorClient {
            observations: Rc::clone(&observations),
        },
        RegionCache::new(RecordingLoader {
            calls: Rc::clone(&loader_calls),
            regions: [location(1, "a", "z")].into_iter().collect(),
        }),
    );
    let transport = DirectUnaryQueryTransport::with_locked_response_delegate(
        shared,
        DirectUnaryRuntimeConfig::default(),
        Rc::new(RejectUnexpectedLock),
    )
    .unwrap();
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut result = runtime
        .select_with_runtime_stats(
            &TransportRequest::new(metadata(), Arc::clone(&cancel)),
            SelectInput::default(),
            QueryResultContext::new(Vec::<FieldType>::new(), WarningCollector::new()),
            vec![1],
            2,
            true,
        )
        .unwrap();

    assert_eq!(
        result.next_raw(),
        Err(tidb_distsql::QueryResponseError::Cancelled)
    );
    assert!(
        !cancel.is_cancelled(),
        "request-local cancellation must not poison the outer execution"
    );
    assert_eq!(observations.sends.get(), 1);
    assert_eq!(observations.closes.get(), 0);
    assert_eq!(observations.liveness.get(), 0);
    assert_eq!(loader_calls.borrow().as_slice(), [b"a".to_vec()]);
}

#[test]
fn execution_cancellation_interrupts_dispatch_before_all_recovery_and_success_mutation() {
    let cancel = Arc::new(CancelHandle::default());
    let acquired_before_cancel = cancel.unary_cancellation();
    assert!(acquired_before_cancel.shares_state_with(&cancel.unary_cancellation()));

    let observations = Rc::new(ClientObservations::default());
    let loader_calls = Rc::new(RefCell::new(Vec::new()));
    let retry = Rc::new(NoRetryMutation::default());
    let (dispatch_started, dispatch_started_rx) = mpsc::channel();
    let shared = SharedReadRuntime::new(
        CancellationBlockingClient {
            observations: Rc::clone(&observations),
            dispatch_started: Some(dispatch_started),
        },
        RegionCache::new(RecordingLoader {
            calls: Rc::clone(&loader_calls),
            regions: [location(1, "a", "m"), location(2, "m", "z")]
                .into_iter()
                .collect(),
        }),
    );
    let transport = DirectUnaryQueryTransport::with_locked_response_delegate(
        shared.clone(),
        DirectUnaryRuntimeConfig {
            seed_read_bytes: 4096,
            region_retry_waiter: retry.clone(),
            ..DirectUnaryRuntimeConfig::default()
        },
        Rc::new(RejectUnexpectedLock),
    )
    .unwrap();
    let cancel_after_dispatch = Arc::clone(&cancel);
    let canceller = std::thread::spawn(move || {
        dispatch_started_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("direct unary dispatch reached the injected client");
        cancel_after_dispatch.cancel();
    });
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut result = runtime
        .select_with_runtime_stats(
            &TransportRequest::new(metadata(), Arc::clone(&cancel)),
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
    canceller.join().unwrap();
    assert!(acquired_before_cancel.is_cancelled());
    assert_eq!(observations.sends.get(), 1, "future task must not dispatch");
    assert_eq!(observations.closes.get(), 0);
    assert_eq!(observations.liveness.get(), 0);
    assert_eq!(
        observations.predicted_read_bytes.borrow().as_slice(),
        [4096]
    );
    assert_eq!(
        observations.addresses.borrow().as_slice(),
        ["tikv-1:20160".to_owned()]
    );
    assert!(retry.sleeps.borrow().is_empty());
    assert_eq!(
        loader_calls.borrow().as_slice(),
        [b"a".to_vec(), b"m".to_vec()]
    );

    drop(result);
    let cache = shared.region_cache();
    let mut cache = cache.borrow_mut();
    for (key, store_id) in [(b"a".as_slice(), 201), (b"m".as_slice(), 202)] {
        cache.locate_key(key).unwrap();
        let store = cache.store_state(store_id).unwrap();
        assert_eq!(store.epoch(), 7);
        assert_eq!(store.resolve_state(), StoreResolveState::Resolved);
        assert_eq!(store.liveness(), StoreLiveness::Reachable);
    }
    assert_eq!(
        loader_calls.borrow().as_slice(),
        [b"a".to_vec(), b"m".to_vec()]
    );
}

#[test]
fn caller_cancellation_branch_is_terminal_before_response_and_retry_mutation() {
    let source = include_str!("../src/cop_paging/direct_unary_query_transport.rs");
    let branch = source
        .find("if self.cancellation.is_cancelled()")
        .expect("caller cancellation precedence branch");
    let raw_response = source[branch..]
        .find("let raw_response = match send_result")
        .map(|offset| branch + offset)
        .expect("response classification follows cancellation");
    let terminal = &source[branch..raw_response];
    assert!(terminal.contains("return Err(DirectUnaryTransportError::CallerCancelled)"));
    for forbidden in [
        "record_attempt_result",
        "recover_transport_failure",
        "close_address",
        ".liveness(",
        "region_cache",
        "request_selectors",
        "region_backoffs",
        "rebuild_",
        "locate_",
        "consume_failed_attempt",
        "retry_transport_attempt",
        "promote_successful_request",
        "handle_locked_response",
        "accept_response",
        "install_same_task_retry",
        "ResponseChannel",
    ] {
        assert!(
            !terminal.contains(forbidden),
            "unexpected mutation: {forbidden}"
        );
    }
}
