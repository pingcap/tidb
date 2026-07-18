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

// client-go/internal/locate/region_request_test.go:149 TestOnRegionError,
// :226 TestOnSendFailedWithStoreRestartUsingAsyncAPI, and :270
// TestOnSendFailedWithCloseKnownStoreThenUseNewOneUsingAsyncAPI.
#[test]
fn callback_policy_distinguishes_region_and_send_feedback_before_retry() {
    assert!(SOURCE.contains("AsyncRegionRetryCause::RegionError"));
    assert!(SOURCE.contains("AsyncRegionRetryCause::SendFailure"));
    assert!(SOURCE.contains("self.policy.on_response"));
    assert!(SOURCE.contains(".on_send_failure"));
    assert!(SOURCE.contains("record `elapsed` against `route.target()` exactly"));
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

// client-go's async callback invokes a terminal result directly, while the
// synchronous continuation schedules its result on the same run loop.
#[test]
fn direct_callback_and_sync_retry_share_one_completion_request() {
    assert!(SOURCE.contains("self.completion.invoke(result)"));
    assert!(SOURCE.contains("self.completion.schedule(result)"));
    assert!(SOURCE.contains("Duplicate calls are suppressed"));
    assert_eq!(SOURCE.matches("completion: CompletionRequest<").count(), 2);
}
