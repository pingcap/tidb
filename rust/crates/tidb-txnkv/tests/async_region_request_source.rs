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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tidb_proto::KvrpcContext;
use tidb_txnkv::region::{
    AsyncRegionAttemptDecision, AsyncRegionAttemptPolicy, AsyncRegionAttemptPoll,
    AsyncRegionAttemptState, AsyncRegionRequestAttempt, AsyncRegionRetryCause, LeaderRequest,
    PeerRole, RegionAttempt, RegionVerId, ReplicaReadMode, RoutedRegionResponse,
};
use tidb_txnkv::rpc::{
    completion_pair, AsyncRequestDispatcher, CompletionError, CompletionRunLoop, PendingRequest,
    UnaryCallContext, UnaryCancellation,
};
use tidb_txnkv::{
    ClientReplicaReadType, DirectUnaryClientError, DirectUnaryRequest, DirectUnaryResponse,
    EndpointType,
};

const SOURCE: &str = include_str!("../src/region/async_request.rs");

fn position(needle: &str) -> usize {
    SOURCE
        .find(needle)
        .unwrap_or_else(|| panic!("missing async RegionRequest contract: {needle}"))
}

// client-go/internal/locate/region_request_test.go:400 TestSendReqAsync.
#[test]
fn selected_route_is_dispatched_exactly_once_and_retry_stays_caller_owned() {
    assert_eq!(SOURCE.matches("dispatcher.begin(").count(), 1);
    assert!(!SOURCE.contains("send_retry_sync("));
    assert!(!SOURCE.contains("request_selector("));
    assert!(!SOURCE.contains("RegionCache<"));
    assert!(SOURCE.contains("AttemptPhase::AwaitingSynchronousRetry"));
    assert!(SOURCE.contains("AsyncRegionAttemptPoll::Retry(cause)"));
}

// client-go/internal/locate/region_request_test.go:78
// TestRegionRequestToSingleStore and region_request3_test.go:71
// TestRegionRequestToThreeStores.
#[test]
fn immutable_selected_attempt_owns_physical_and_forwarded_addresses() {
    assert!(SOURCE.contains("route.dispatch_address()"));
    assert!(SOURCE.contains("route.forwarded_host()"));
    assert!(SOURCE.contains("route.attempt.region.id"));
    assert!(SOURCE.contains("route.attempt.peer_id"));
    assert!(SOURCE.contains("route.attempt.store_id"));
    assert!(SOURCE.contains("prepared.context.region_epoch"));
    assert!(SOURCE.contains("prepared.context.peer"));
    assert!(SOURCE.contains("prepared.context.replica_read = route.replica_read"));
    assert!(SOURCE.contains("prepared.context.stale_read = route.stale_read"));
}

// client-go/internal/locate/region_request_test.go:314
// TestOnSendFailedWithCancelledUsingAsyncAPI and :354
// TestNoReloadRegionWhenCtxCanceledUsingAsyncAPI.
#[test]
fn cancellation_precedes_selector_cache_feedback() {
    let ready_callback = position("let outcome = match pending.try_complete()");
    let raced_cancellation = position("// Cancellation wins even when it races");
    let feedback = position("let decision = match outcome");
    assert!(ready_callback < raced_cancellation);
    assert!(raced_cancellation < feedback);
    assert!(SOURCE.contains("pending.cancel();"));
    assert!(SOURCE.contains("DirectUnaryClientError::CallerCancelled"));
}

// client-go/internal/locate/region_request_state_test.go:267
// TestRegionCacheStaleReadUsingAsyncAPI. The async layer copies the already
// selected route flags; it does not reinterpret stale/replica policy.
#[test]
fn stale_read_and_replica_selection_remain_selector_owned() {
    assert!(SOURCE.contains("prepared.replica_read = route.replica_read"));
    assert!(SOURCE.contains("prepared.stale_read = route.stale_read"));
    assert!(!SOURCE.contains("ReplicaReadMode"));
    assert!(!SOURCE.contains("ReadPolicy"));
}

// client-go's transport callback and synchronous continuation both schedule
// onto the caller-owned run loop. Invoke is reserved for local validation.
#[test]
fn direct_callback_and_sync_retry_share_one_completion_request() {
    assert!(!SOURCE.contains("self.completion.invoke(result)"));
    assert!(SOURCE.contains("self.completion.schedule(result)"));
    assert!(SOURCE.contains("completion.invoke(Err(error))"));
    assert_eq!(SOURCE.matches("completion: CompletionRequest<").count(), 2);
}

#[derive(Debug)]
struct FakePending {
    outcome: Option<Result<DirectUnaryResponse, DirectUnaryClientError>>,
    canceled: Arc<AtomicBool>,
}

impl PendingRequest for FakePending {
    fn try_complete(
        &mut self,
    ) -> Result<Option<Result<DirectUnaryResponse, DirectUnaryClientError>>, CompletionError> {
        Ok(self.outcome.take())
    }

    fn cancel(&mut self) {
        self.canceled.store(true, Ordering::Release);
    }
}

enum FakeBegin {
    Error(DirectUnaryClientError),
    Ready(Result<DirectUnaryResponse, DirectUnaryClientError>),
}

struct FakeDispatcher {
    begin: Option<FakeBegin>,
    dispatched: Vec<(String, Option<String>, DirectUnaryRequest)>,
}

impl FakeDispatcher {
    fn new(begin: FakeBegin) -> Self {
        Self {
            begin: Some(begin),
            dispatched: Vec::new(),
        }
    }
}

impl AsyncRequestDispatcher for FakeDispatcher {
    type Pending = FakePending;

    fn begin(
        &mut self,
        physical_address: &str,
        forwarded_host: Option<&str>,
        request: &DirectUnaryRequest,
        _call: &UnaryCallContext,
    ) -> Result<Self::Pending, DirectUnaryClientError> {
        self.dispatched.push((
            physical_address.to_owned(),
            forwarded_host.map(str::to_owned),
            request.clone(),
        ));
        match self.begin.take().unwrap() {
            FakeBegin::Error(error) => Err(error),
            FakeBegin::Ready(outcome) => Ok(FakePending {
                outcome: Some(outcome),
                canceled: Arc::new(AtomicBool::new(false)),
            }),
        }
    }

    fn send_retry_sync(
        &mut self,
        _physical_address: &str,
        _forwarded_host: Option<&str>,
        _request: &DirectUnaryRequest,
        _call: &UnaryCallContext,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        panic!("the async attempt must not own the synchronous retry loop")
    }
}

struct FakePolicy {
    decision: Option<AsyncRegionAttemptDecision>,
    send_failures: Vec<DirectUnaryClientError>,
    responses: usize,
    fallback_calls: usize,
}

impl FakePolicy {
    fn new(decision: AsyncRegionAttemptDecision) -> Self {
        Self {
            decision: Some(decision),
            send_failures: Vec::new(),
            responses: 0,
            fallback_calls: 0,
        }
    }
}

impl AsyncRegionAttemptPolicy for FakePolicy {
    fn on_response(
        &mut self,
        _route: &LeaderRequest,
        _elapsed: Duration,
        _response: DirectUnaryResponse,
    ) -> AsyncRegionAttemptDecision {
        self.responses += 1;
        self.decision.take().unwrap()
    }

    fn on_send_failure(
        &mut self,
        _route: &LeaderRequest,
        _elapsed: Duration,
        error: DirectUnaryClientError,
    ) -> AsyncRegionAttemptDecision {
        self.send_failures.push(error);
        self.decision.take().unwrap()
    }
}

fn route() -> LeaderRequest {
    LeaderRequest {
        attempt: RegionAttempt {
            region: RegionVerId::new(9, 2, 3),
            peer_id: 11,
            store_id: 101,
            address: "logical-store-1".to_owned(),
            store_epoch: 7,
        },
        proxy: Some(RegionAttempt {
            region: RegionVerId::new(9, 2, 3),
            peer_id: 12,
            store_id: 102,
            address: "physical-proxy".to_owned(),
            store_epoch: 7,
        }),
        role: PeerRole::Voter,
        is_witness: false,
        replica_read: false,
        stale_read: false,
        cached_leader: true,
        forwarding: true,
        read_mode: ReplicaReadMode::Leader,
    }
}

fn request() -> DirectUnaryRequest {
    DirectUnaryRequest {
        endpoint: EndpointType::TiKv,
        replica_read_type: ClientReplicaReadType::Leader,
        replica_read: false,
        stale_read: false,
        input_request_source: String::new(),
        predicted_read_bytes: 0,
        read_replica_scope: String::new(),
        txn_scope: String::new(),
        context: KvrpcContext::default(),
        encoded_request: vec![1],
    }
}

fn response(value: u8) -> DirectUnaryResponse {
    DirectUnaryResponse {
        encoded_response: vec![value],
    }
}

// client-go/internal/locate/region_request_test.go:149 TestOnRegionError.
// A response carrying a canonical region-error decision must transfer control
// back to the synchronous loop without publishing the raw response.
#[test]
fn region_error_feedback_precedes_retry_and_suppresses_publication() {
    let run_loop = CompletionRunLoop::new();
    let (completion, mut pull) = completion_pair(run_loop.clone(), || {});
    let raw = response(3);
    let cause = AsyncRegionRetryCause::RegionError(raw.clone());
    let mut dispatcher = FakeDispatcher::new(FakeBegin::Ready(Ok(raw)));
    let policy = FakePolicy::new(AsyncRegionAttemptDecision::Retry(cause.clone()));
    let mut attempt = AsyncRegionRequestAttempt::begin(
        &mut dispatcher,
        route(),
        &request(),
        UnaryCallContext::with_timeout(Duration::from_secs(1)),
        completion,
        policy,
    );

    assert_eq!(attempt.poll(), AsyncRegionAttemptPoll::Retry(cause.clone()));
    assert_eq!(attempt.poll(), AsyncRegionAttemptPoll::Retry(cause));
    assert_eq!(
        attempt.state(),
        AsyncRegionAttemptState::AwaitingSynchronousRetry
    );
    assert_eq!(run_loop.num_runnable(), 0);
    assert_eq!(pull.try_complete(), Ok(None));
    assert_eq!(attempt.into_policy().responses, 1);
}

// client-go/internal/locate/region_request_test.go:226 and :270. An
// immediate begin failure is still an exact attempted route and must feed the
// shared policy before the same policy drives synchronous fallback.
#[test]
fn immediate_failure_feedback_runs_same_policy_retry_and_keeps_final_address() {
    let run_loop = CompletionRunLoop::new();
    let (completion, mut pull) = completion_pair(run_loop.clone(), || {});
    let failure = DirectUnaryClientError::Closed;
    let mut dispatcher = FakeDispatcher::new(FakeBegin::Error(failure.clone()));
    let policy = FakePolicy::new(AsyncRegionAttemptDecision::Retry(
        AsyncRegionRetryCause::SendFailure(failure.clone()),
    ));
    let mut attempt = AsyncRegionRequestAttempt::begin(
        &mut dispatcher,
        route(),
        &request(),
        UnaryCallContext::with_timeout(Duration::from_secs(1)),
        completion,
        policy,
    );

    assert_eq!(
        attempt.state(),
        AsyncRegionAttemptState::AwaitingSynchronousRetry
    );
    assert_eq!(
        attempt.poll(),
        AsyncRegionAttemptPoll::Retry(AsyncRegionRetryCause::SendFailure(failure.clone()))
    );
    attempt.run_synchronous_retry(|policy, _call| {
        assert_eq!(policy.send_failures, [failure]);
        policy.fallback_calls += 1;
        Ok(RoutedRegionResponse::new(response(7), "logical-store-2"))
    });
    assert_eq!(run_loop.num_runnable(), 1);
    let routed = pull.try_complete().unwrap().unwrap().unwrap();
    assert_eq!(routed.response, response(7));
    assert_eq!(routed.logical_address, "logical-store-2");

    attempt.complete_retry(Ok(RoutedRegionResponse::new(response(8), "duplicate")));
    assert_eq!(pull.try_complete(), Ok(None));
    assert_eq!(attempt.into_policy().fallback_calls, 1);
}

// client-go/internal/locate/region_request_test.go:314 and :354. A caller
// cancellation which happens inside synchronous fallback overrides its result.
#[test]
fn cancellation_during_synchronous_fallback_wins_before_publication() {
    let run_loop = CompletionRunLoop::new();
    let (completion, mut pull) = completion_pair(run_loop, || {});
    let cancellation = UnaryCancellation::new();
    let cancel_during_retry = cancellation.clone();
    let failure = DirectUnaryClientError::Closed;
    let mut dispatcher = FakeDispatcher::new(FakeBegin::Error(failure.clone()));
    let policy = FakePolicy::new(AsyncRegionAttemptDecision::Retry(
        AsyncRegionRetryCause::SendFailure(failure),
    ));
    let mut attempt = AsyncRegionRequestAttempt::begin(
        &mut dispatcher,
        route(),
        &request(),
        UnaryCallContext::new(Duration::from_secs(1), cancellation),
        completion,
        policy,
    );

    attempt.run_synchronous_retry(|policy, _call| {
        policy.fallback_calls += 1;
        cancel_during_retry.cancel();
        Ok(RoutedRegionResponse::new(response(9), "must-not-win"))
    });
    assert_eq!(
        pull.try_complete(),
        Ok(Some(Err(DirectUnaryClientError::CallerCancelled)))
    );
    assert_eq!(attempt.into_policy().fallback_calls, 1);
}

#[test]
fn pull_cancellation_during_synchronous_fallback_suppresses_publication() {
    let run_loop = CompletionRunLoop::new();
    let (completion, mut pull) = completion_pair(run_loop.clone(), || {});
    let failure = DirectUnaryClientError::Closed;
    let mut dispatcher = FakeDispatcher::new(FakeBegin::Error(failure.clone()));
    let policy = FakePolicy::new(AsyncRegionAttemptDecision::Retry(
        AsyncRegionRetryCause::SendFailure(failure),
    ));
    let mut attempt = AsyncRegionRequestAttempt::begin(
        &mut dispatcher,
        route(),
        &request(),
        UnaryCallContext::with_timeout(Duration::from_secs(1)),
        completion,
        policy,
    );

    attempt.run_synchronous_retry(|policy, _call| {
        policy.fallback_calls += 1;
        pull.cancel();
        Ok(RoutedRegionResponse::new(response(9), "must-not-publish"))
    });
    assert_eq!(run_loop.num_runnable(), 0);
    assert_eq!(pull.try_complete(), Ok(None));
    assert_eq!(attempt.into_policy().fallback_calls, 1);
}

// client-go/internal/locate/region_request_test.go:400. Callback-originated
// completion is queued, and the routed response names the logical target rather
// than the physical forwarding proxy.
#[test]
fn ready_async_success_is_scheduled_once_with_logical_address() {
    let run_loop = CompletionRunLoop::new();
    let (completion, mut pull) = completion_pair(run_loop.clone(), || {});
    let mut dispatcher = FakeDispatcher::new(FakeBegin::Ready(Ok(response(3))));
    let policy = FakePolicy::new(AsyncRegionAttemptDecision::Complete(Ok(response(3))));
    let mut attempt = AsyncRegionRequestAttempt::begin(
        &mut dispatcher,
        route(),
        &request(),
        UnaryCallContext::with_timeout(Duration::from_secs(1)),
        completion,
        policy,
    );

    assert_eq!(attempt.poll(), AsyncRegionAttemptPoll::Complete);
    assert_eq!(run_loop.num_runnable(), 1);
    let routed = pull.try_complete().unwrap().unwrap().unwrap();
    assert_eq!(routed.response, response(3));
    assert_eq!(routed.logical_address, "logical-store-1");
    assert_eq!(pull.try_complete(), Ok(None));
    assert_eq!(dispatcher.dispatched[0].0, "physical-proxy");
    assert_eq!(
        dispatcher.dispatched[0].1.as_deref(),
        Some("logical-store-1")
    );
}
