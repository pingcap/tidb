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

//! Dependency-closed vectors for logical SELECT-lock classification.
//!
//! The Go anchor is `TestPointGetWithSelectLock` at
//! `pkg/planner/core/integration_test.go:1466`.

use tidb_planner::logical_lock::{
    is_for_share_lock_type, is_for_update_lock_type, is_supported_select_lock_type, SelectLockType,
};

#[test]
fn point_get_lock_families_are_supported() {
    assert!(is_for_update_lock_type(SelectLockType::FOR_UPDATE));
    assert!(is_for_update_lock_type(SelectLockType::FOR_UPDATE_NO_WAIT));
    assert!(is_for_update_lock_type(SelectLockType::FOR_UPDATE_WAIT_N));
    assert!(is_for_share_lock_type(SelectLockType::FOR_SHARE));
    assert!(is_for_share_lock_type(SelectLockType::FOR_SHARE_NO_WAIT));
    for lock_type in [
        SelectLockType::FOR_UPDATE,
        SelectLockType::FOR_UPDATE_NO_WAIT,
        SelectLockType::FOR_UPDATE_WAIT_N,
        SelectLockType::FOR_SHARE,
        SelectLockType::FOR_SHARE_NO_WAIT,
    ] {
        assert!(is_supported_select_lock_type(lock_type));
    }
}

#[test]
fn skip_locked_and_unknown_locks_are_not_supported() {
    for raw in [6, 7, 42] {
        assert!(!is_supported_select_lock_type(SelectLockType::from_raw(
            raw
        )));
    }
}
