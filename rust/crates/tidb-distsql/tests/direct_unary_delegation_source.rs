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

#[test]
fn direct_unary_call_uses_active_cancellation_context() {
    let source = include_str!("../src/cop_paging/direct_unary_query_transport.rs");
    let dispatch = source
        .find("fn dispatch_attempt(")
        .expect("direct unary dispatch owner");
    let dispatch = &source[dispatch..];
    assert!(dispatch.contains("active_unary_cancellation"));
    assert!(dispatch.contains("UnaryCallContext::new"));
    assert!(dispatch.contains("send_request_with_context"));
    let caller_cancelled = dispatch
        .find("DirectUnaryClientError::CallerCancelled")
        .expect("caller cancellation precedence branch");
    let before_response_classification = &dispatch[caller_cancelled..];
    assert!(before_response_classification.starts_with("DirectUnaryClientError::CallerCancelled"));
    assert!(before_response_classification.contains("DirectUnaryTransportError::Client(error)"));
}

#[test]
fn locked_response_delegates_before_success_mutation() {
    let source = include_str!("../src/cop_paging/direct_unary_query_transport.rs");
    let locked = source
        .find("if let Some(lock) = locked")
        .expect("locked-response branch");
    let tail = &source[locked..];
    let delegated = tail
        .find("handle_locked_response")
        .expect("lock delegate call");
    let recorded = tail
        .find("record_attempt_result")
        .expect("route observation after lock handling");
    let promoted = tail
        .find("promote_successful_request")
        .expect("success promotion after lock handling");
    let accepted = tail
        .find("accept_response")
        .expect("paging acceptance after lock handling");
    assert!(delegated < recorded);
    assert!(delegated < promoted);
    assert!(delegated < accepted);
    assert!(tail[delegated..recorded].contains("map_err(DirectUnaryTransportError::LockRecovery)"));
}

#[test]
fn one_shared_runtime_carries_client_and_region_cache_handles() {
    let runtime = include_str!("../../tidb-txnkv/src/read_runtime.rs");
    assert_eq!(runtime.matches("    client: Rc<RefCell<C>>").count(), 1);
    assert_eq!(
        runtime
            .matches("    region_cache: Rc<RefCell<RegionCache<L>>>")
            .count(),
        1
    );

    let direct = include_str!("../src/cop_paging/direct_unary_query_transport.rs");
    assert!(direct.contains("shared_runtime: SharedReadRuntime<C, L>"));
    assert!(!direct.contains("client: Rc<RefCell<C>>"));
    assert!(!direct.contains("region_cache: Rc<RefCell<RegionCache<L>>>"));
}
