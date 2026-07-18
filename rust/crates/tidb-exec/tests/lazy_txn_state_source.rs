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

//! Source-backed tests for lazy transaction state predicates.

use tidb_exec::lazy_txn_state::LazyTxnState;

#[test]
fn lazy_txn_state_matches_source_valid_and_pending_predicates() {
    // Source: pkg/session/txn.go:225-236.
    // Direct Go coverage: pkg/session/test/txn/txn_test.go:131
    // (TestTxnLazyInitialize) and :378 (TestInTrans).
    let empty = LazyTxnState::default();
    assert!(!empty.valid());
    assert!(!empty.pending());
    assert!(!empty.valid_or_pending());

    assert!(LazyTxnState::new(true, true, false).valid());
    assert!(!LazyTxnState::new(true, false, false).valid());
    assert!(!LazyTxnState::new(false, true, false).valid());

    let pending = LazyTxnState::new(false, false, true);
    assert!(!pending.valid());
    assert!(pending.pending());
    assert!(pending.valid_or_pending());

    // The source validOrPending checks future existence directly, so a
    // transition state with both a future and an allocated transaction remains
    // true while pending() itself requires the transaction to be absent.
    let transitioning = LazyTxnState::new(true, false, true);
    assert!(!transitioning.pending());
    assert!(transitioning.valid_or_pending());
}
