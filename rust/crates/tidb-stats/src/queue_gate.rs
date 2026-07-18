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

//! Shared initialization gate from
//! `pkg/statistics/handle/autoanalyze/priorityqueue/queue.go`.
//!
//! Every public queue operation checks the same initialized bit and returns
//! the same error before touching heap or worker state. This leaf preserves
//! that contract without pulling session/domain lifecycle into the statistics
//! crate; future queue owners can compose it with their own state machine.

/// Exact source error text for operations before queue initialization.
pub const NOT_INITIALIZED_ERROR_MSG: &str = "priority queue not initialized";

/// Error returned when a queue operation is attempted before initialization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueueNotInitialized;

impl std::fmt::Display for QueueNotInitialized {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(NOT_INITIALIZED_ERROR_MSG)
    }
}

impl std::error::Error for QueueNotInitialized {}

/// Checks the source initialized gate shared by queue APIs.
pub const fn require_initialized(initialized: bool) -> Result<(), QueueNotInitialized> {
    if initialized {
        Ok(())
    } else {
        Err(QueueNotInitialized)
    }
}

/// Returns the source default for an uninitialized `IsEmptyForTest` call.
pub fn is_empty_for_test(initialized: bool) -> Result<bool, QueueNotInitialized> {
    require_initialized(initialized).map(|()| true)
}

/// Returns the source default for an uninitialized `Len` call.
pub fn queue_len(initialized: bool, len: usize) -> Result<usize, QueueNotInitialized> {
    require_initialized(initialized).map(|()| len)
}

/// Returns running IDs, preserving the source empty result before init.
#[must_use]
pub fn running_jobs(initialized: bool, ids: &[i64]) -> Vec<i64> {
    if initialized {
        ids.to_vec()
    } else {
        Vec::new()
    }
}
