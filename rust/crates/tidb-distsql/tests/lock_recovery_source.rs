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

use std::fs;
use std::path::PathBuf;

fn source(path: &str) -> String {
    fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path)).unwrap()
}

#[test]
fn delegate_has_one_shared_runtime_and_only_same_task_continuation() {
    let recovery = source("src/cop_paging/lock_recovery.rs");
    assert!(recovery.contains("runtime: &SharedReadRuntime<C, L>"));
    assert!(!recovery.contains("SharedReadRuntime::new"));
    assert!(!recovery.contains("RegionCache::new"));
    assert!(!recovery.contains("TonicCoprocessorClient::new"));
    assert!(recovery.contains("decode_lock_observation(&observation.lock)"));
    assert!(recovery.contains("observation.caller_start_ts"));
    assert!(recovery.contains("&observation.request_context"));
    assert!(recovery.contains("&observation.call"));
    assert!(recovery.contains("LockedResponseAction::RetrySameTask"));
}

#[test]
fn alive_ttl_uses_exact_cancellation_wait_without_polling() {
    let recovery = source("src/cop_paging/lock_recovery.rs");
    let alive = recovery
        .split("LockRecoveryResult::Alive(ttl)")
        .nth(1)
        .expect("alive branch");
    assert!(alive.contains("observation.call.cancellation().wait_timeout(ttl)"));
    assert!(alive.contains("cancelled by caller"));
    assert!(!alive.contains("thread::sleep"));
    assert!(!alive.contains("is_cancelled"));
}

#[test]
fn direct_transport_keeps_locked_attempt_unconsumed_until_delegate_success() {
    let direct = source("src/cop_paging/direct_unary_query_transport.rs");
    let locked = direct
        .split("if let Some(lock) = locked")
        .nth(1)
        .expect("locked response branch")
        .split("let accepted = self.runtime.accept_response")
        .next()
        .expect("locked branch before publication");
    let delegated = locked.find("handle_locked_response").unwrap();
    let consumed = locked.find("consume_failed_attempt").unwrap();
    let retried = locked.find("retry_transport_attempt").unwrap();
    assert!(delegated < consumed && consumed < retried);
    assert!(!locked.contains("accept_response"));
    assert!(!locked.contains("cache.put"));
}
