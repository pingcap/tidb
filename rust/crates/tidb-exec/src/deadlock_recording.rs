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

//! Executor-side admission of TiKV deadlocks into `pkg/util/deadlockhistory`.

use std::sync::atomic::{AtomicBool, Ordering};

use tidb_executor::deadlock_history::{err_deadlock_to_deadlock_record, GLOBAL_DEADLOCK_HISTORY};
use tidb_txnkv::transaction::DeadlockDetail;

static COLLECT_RETRYABLE: AtomicBool = AtomicBool::new(false);

/// Applies the server's deadlock-history capacity and retryable policy.
pub fn configure_deadlock_history(capacity: usize, collect_retryable: bool) {
    COLLECT_RETRYABLE.store(collect_retryable, Ordering::Release);
    GLOBAL_DEADLOCK_HISTORY.resize(capacity);
}

pub(crate) fn record_deadlock(detail: &DeadlockDetail) {
    if detail.is_retryable && !COLLECT_RETRYABLE.load(Ordering::Acquire) {
        return;
    }
    GLOBAL_DEADLOCK_HISTORY.push(err_deadlock_to_deadlock_record(detail));
}
