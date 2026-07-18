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

//! Dependency-closed vectors for PhysicalLock planning.
//!
//! The Go anchor is `TestIssue52592ForNextGen` at
//! `pkg/planner/core/tests/pointget/point_get_plan_test.go:407`.

use tidb_planner::physical_lock::{exhaust_physical_lock, LockExhaustion, PhysicalLockPlan};

#[test]
fn mpp_lock_is_rejected_before_plan_creation() {
    assert_eq!(
        exhaust_physical_lock(true, "for update", 0),
        LockExhaustion::UnsupportedFlash
    );
}

#[test]
fn point_get_lock_explain_info_preserves_source_text() {
    let LockExhaustion::Planned(plan) = exhaust_physical_lock(false, "for update", 0) else {
        unreachable!();
    };
    assert_eq!(plan.plan_type(), "Lock");
    assert_eq!(plan.query_block_offset(), 0);
    assert_eq!(plan.explain_info(), "for update 0");
}

#[test]
fn nonzero_wait_seconds_and_opaque_lock_text_are_lossless() {
    let plan = PhysicalLockPlan::init("lock in share mode", 42);
    assert_eq!(plan.lock_type(), "lock in share mode");
    assert_eq!(plan.wait_sec(), 42);
    assert_eq!(plan.explain_info(), "lock in share mode 42");
}
