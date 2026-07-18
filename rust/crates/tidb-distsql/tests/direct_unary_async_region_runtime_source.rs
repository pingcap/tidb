// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(missing_docs)]

const SOURCE: &str = include_str!("../src/cop_paging/direct_unary_query_transport.rs");

fn owner(name: &str, next: &str) -> &'static str {
    let start = SOURCE
        .find(name)
        .unwrap_or_else(|| panic!("missing {name}"));
    let tail = &SOURCE[start..];
    let end = tail.find(next).unwrap_or(tail.len());
    &tail[..end]
}

#[test]
fn batch_success_settles_through_the_existing_response_owner() {
    let dispatch = owner("fn dispatch_attempt(", "fn complete_batch_attempt(");
    assert_eq!(dispatch.matches("begin(").count(), 1);
    assert!(dispatch.contains("self.pending_batch = Some(PendingBatchAttempt"));
    assert!(!dispatch.contains("send_retry_sync"));

    let poll = owner("fn complete_batch_attempt(", "fn settle_dispatch(");
    assert!(poll.contains("attempt.pending.complete(&self.call)"));
    assert!(poll.contains("self.settle_dispatch("));

    let settle = owner("fn settle_dispatch(", "fn record_attempt_result(");
    assert!(settle.contains("recover_transport_failure("));
    assert!(settle.contains("recover_region_error("));
    assert!(settle.contains("accept_response("));
}

#[test]
fn first_batch_failure_reenters_the_same_sync_selector_loop() {
    let dispatch = owner("fn dispatch_attempt(", "fn complete_batch_attempt(");
    assert!(dispatch.contains("!self.sync_only_chains.contains"));
    assert!(dispatch.contains("send_request_with_route("));
    assert!(dispatch.contains("self.sync_only_chains.insert"));
    let settle = owner("fn settle_dispatch(", "fn record_attempt_result(");
    assert!(settle.contains("if batch_attempt"));
    assert!(settle.contains("self.sync_only_chains.insert(logical_task_id)"));
    assert!(SOURCE.contains("let sync_only_chain = self.sync_only_chains.contains"));
    assert!(SOURCE.contains("if sync_only_chain {"));
}

#[test]
fn successful_page_reenables_batch_for_the_same_logical_task() {
    let settle = owner("fn settle_dispatch(", "fn record_attempt_result(");
    let accepted = settle.find("self.runtime.accept_response(").unwrap();
    let clear = settle
        .find("self.sync_only_chains.remove(&logical_task_id)")
        .unwrap();
    let next_page = settle
        .find("Some(next_attempt_id)")
        .expect("paging continuation");
    assert!(accepted < clear);
    assert!(clear < next_page);
}

#[test]
fn cancellation_and_deadline_precede_async_feedback_and_late_publication() {
    let poll = owner("fn complete_batch_attempt(", "fn settle_dispatch(");
    let active = poll.find("self.check_retry_active()").unwrap();
    let complete = poll.find("attempt.pending.complete(&self.call)").unwrap();
    assert!(active < complete);
    assert!(poll.contains("attempt.pending.cancel()"));
    assert!(SOURCE.contains("impl<C, L> Drop for DirectUnaryQueryResponse<C, L>"));

    let settle = owner("fn settle_dispatch(", "fn record_attempt_result(");
    let cancellation = settle.find("self.cancellation.is_cancelled()").unwrap();
    let selector_feedback = settle.find("record_attempt_result").unwrap();
    let cache_feedback = settle.find("validate_route_observation").unwrap();
    assert!(cancellation < selector_feedback);
    assert!(cancellation < cache_feedback);
}

#[test]
fn forwarding_and_async_unavailable_fakes_keep_their_existing_contracts() {
    let begin = owner("fn begin_async_request", "/// One transport-owned rotating");
    assert!(begin.contains("selected.dispatch_address()"));
    assert!(begin.contains("selected.forwarded_host()"));

    let injected = owner(
        "pub fn new_injected<S>",
        "pub fn new_injected_batch_first<S>",
    );
    assert!(injected.contains("Self::with_shared_runtime("));
    assert!(!injected.contains("batch_first"));
    let base = owner("pub fn with_locked_response_delegate", "impl<C, L>");
    assert!(base.contains("async_begin: None"));
}
