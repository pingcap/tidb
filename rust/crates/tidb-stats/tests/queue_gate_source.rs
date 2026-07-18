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

//! Source-backed tests for the priority queue initialization gate.

use tidb_stats::{
    is_empty_for_test, queue_len, require_initialized, running_jobs, QueueNotInitialized,
    NOT_INITIALIZED_ERROR_MSG,
};

#[test]
fn source_uninitialized_queue_uses_one_error_contract() {
    assert_eq!(require_initialized(false), Err(QueueNotInitialized),);
    assert_eq!(
        require_initialized(false).unwrap_err().to_string(),
        NOT_INITIALIZED_ERROR_MSG
    );
    assert_eq!(is_empty_for_test(false), Err(QueueNotInitialized));
    assert_eq!(queue_len(false, 3), Err(QueueNotInitialized));
    // GetRunningJobs intentionally returns an empty snapshot before init.
    assert!(running_jobs(false, &[1, 2]).is_empty());
}

#[test]
fn source_initialized_gate_passes_caller_owned_state() {
    assert_eq!(require_initialized(true), Ok(()));
    assert_eq!(is_empty_for_test(true), Ok(true));
    assert_eq!(queue_len(true, 3), Ok(3));
    assert_eq!(running_jobs(true, &[1, 2]), vec![1, 2]);
}
