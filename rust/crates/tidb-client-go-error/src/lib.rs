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

//! client-go's `error` package (`tikverr`), the part TiDB's SQL layer
//! inspects: `ErrDeadlock`, the deadlock TiKV's detector proved. Go's
//! `util/deadlockhistory` reads it straight from client-go, below the store
//! driver that surfaces it.

use tidb_proto::KvrpcDeadlock;

/// One lock-wait edge in a deadlock cycle proved by TiKV's detector.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeadlockWaitChainItem {
    /// Transaction trying to acquire the lock.
    pub txn: u64,
    /// Transaction currently holding the lock.
    pub wait_for_txn: u64,
    /// Encoded key on which `txn` is waiting.
    pub key: Vec<u8>,
    /// Encoded resource-group tag carrying the blocked SQL digest.
    pub resource_group_tag: Vec<u8>,
}

/// A deadlock TiKV's detector proved, reported verbatim to the SQL layer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeadlockDetail {
    /// Start timestamp of the transaction holding the awaited lock.
    pub lock_ts: u64,
    /// Key this statement was blocked on.
    pub lock_key: Vec<u8>,
    /// Hash of the key that closes the cycle.
    pub deadlock_key_hash: u64,
    /// Key this transaction already holds that closes the cycle.
    pub deadlock_key: Vec<u8>,
    /// Whether the cycle contains another key from this same lock request.
    pub is_retryable: bool,
    /// Every lock wait on the detected cycle.
    pub wait_chain: Vec<DeadlockWaitChainItem>,
}

impl From<&KvrpcDeadlock> for DeadlockDetail {
    fn from(deadlock: &KvrpcDeadlock) -> Self {
        Self {
            lock_ts: deadlock.lock_ts,
            lock_key: deadlock.lock_key.clone(),
            deadlock_key_hash: deadlock.deadlock_key_hash,
            deadlock_key: deadlock.deadlock_key.clone(),
            is_retryable: false,
            wait_chain: deadlock
                .wait_chain
                .iter()
                .map(|entry| DeadlockWaitChainItem {
                    txn: entry.txn,
                    wait_for_txn: entry.wait_for_txn,
                    key: entry.key.clone(),
                    resource_group_tag: entry.resource_group_tag.clone(),
                })
                .collect(),
        }
    }
}
