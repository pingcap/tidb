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

//! Dependency-closed vectors for
//! `pkg/planner/util/utilfuncp/func_pointer_misc.go`.
//!
//! The direct Go anchor is `TestCloneConstantsForPlanCacheWithNilEntry` at
//! `pkg/planner/util/utilfuncp/func_pointer_misc_test.go:26`.

use std::sync::Arc;

use tidb_planner::plan_cache_constants::{
    clone_constants_for_plan_cache, ConstantRef, PlanCacheConstant,
};

fn constant(value: &[u8], safe: bool) -> ConstantRef {
    Arc::new(PlanCacheConstant::new(value, safe))
}

#[test]
fn source_unsafe_clone_preserves_nil_entries() {
    let unsafe_constant = constant(b"unsafe", false);
    let constants = [
        Some(Arc::clone(&unsafe_constant)),
        None,
        Some(Arc::clone(&unsafe_constant)),
    ];
    let cloned = clone_constants_for_plan_cache(Some(&constants), None).expect("cloned constants");

    assert_eq!(cloned.len(), 3);
    assert!(cloned[0].is_some());
    assert!(cloned[1].is_none());
    assert!(cloned[2].is_some());
    assert!(!Arc::ptr_eq(
        cloned[0].as_ref().expect("first"),
        &unsafe_constant
    ));
    assert!(!Arc::ptr_eq(
        cloned[2].as_ref().expect("third"),
        &unsafe_constant
    ));
    assert_eq!(
        cloned[0].as_ref().expect("first").value(),
        b"unsafe".as_slice()
    );
}

#[test]
fn source_safe_constants_share_and_destination_reuses() {
    let safe = constant(b"safe", true);
    let constants = [Some(Arc::clone(&safe)), None];
    let destination = vec![Some(constant(b"stale", false))];
    let cloned = clone_constants_for_plan_cache(Some(&constants), Some(destination))
        .expect("safe constants");
    assert!(Arc::ptr_eq(cloned[0].as_ref().expect("safe"), &safe));
    assert!(cloned[1].is_none());
    assert!(clone_constants_for_plan_cache(None, None).is_none());
}
