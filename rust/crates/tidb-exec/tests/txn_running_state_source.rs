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

//! Source-backed tests for TIDB_TRX transaction-running states.

use tidb_exec::txn_running_state::{TxnRunningState, TXN_RUNNING_STATE_LABELS, TXN_STATE_COUNTER};

#[test]
fn txn_running_states_preserve_source_discriminants_and_labels() {
    // Source: pkg/session/txninfo/txn_info.go:32-48,145-148 and
    // tests/realtikvtest/txntest/txn_state_test.go:32-118
    // (TestBasicTxnState's LockAcquiring/Idle/Committing observations).
    let states = [
        TxnRunningState::Idle,
        TxnRunningState::Running,
        TxnRunningState::LockAcquiring,
        TxnRunningState::Committing,
        TxnRunningState::RollingBack,
    ];
    assert_eq!(TXN_STATE_COUNTER, states.len());
    assert_eq!(
        TXN_RUNNING_STATE_LABELS,
        [
            "Idle",
            "Running",
            "LockWaiting",
            "Committing",
            "RollingBack",
        ]
    );

    for (index, state) in states.into_iter().enumerate() {
        assert_eq!(state.as_i32(), index as i32);
        assert_eq!(state.label(), TXN_RUNNING_STATE_LABELS[index]);
    }
}

#[test]
fn txn_running_state_labels_keep_lock_acquiring_wire_name() {
    // Source: pkg/session/txninfo/txn_info.go:40-41,146-148.
    assert_eq!(TxnRunningState::LockAcquiring.as_i32(), 2);
    assert_eq!(TxnRunningState::LockAcquiring.label(), "LockWaiting");
}
