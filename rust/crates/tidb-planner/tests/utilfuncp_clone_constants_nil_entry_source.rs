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

//! Port of `pkg/planner/util/utilfuncp/func_pointer_misc_test.go::
//! TestCloneConstantsForPlanCacheWithNilEntry` (`pkg/planner.part22` item
//! 1278 on `origin/master`).
//!
//! Go's fixture (:34-46) builds one session-UNSAFE constant — a Column whose
//! `VirtualExpr` set makes `SafeToShareAcrossSession()` false
//! (`pkg/expression/column.go` SafeToShareAcrossSession), used as the
//! constant's DeferredExpr — then calls
//! `CloneConstantsForPlanCache([]{*unsafeConst, nil, *unsafeConst}, nil)`
//! (production at pkg/planner/util/utilfuncp/func_pointer_misc.go:488-521).
//! Re-derived contract:
//! 1. no panic on embedded nil entries (issue #66265 regression);
//! 2. result length equals input length;
//! 3. nil stays nil at its slot (:513-515);
//! 4. unsafe slots get CLONES — new objects carrying equal values
//!    (:516-520) — while the all-safe shortcut returns the ORIGINAL slice
//!    (:498-505).

use std::sync::Arc;

use tidb_planner::plan_cache_constants::{
    clone_constants_for_plan_cache, ConstantRef, PlanCacheConstant,
};

/// The Go test's stand-in for an unsafe constant: on this carrier "unsafe"
/// is exactly what `safe_to_share == false` encodes; Go reaches that state by
/// wiring a VirtualExpr-bearing column as the deferred expression (see module
/// docs).
fn unsafe_constant(value: i64) -> ConstantRef {
    Arc::new(PlanCacheConstant::new(value.to_be_bytes(), false))
}

/// GO PORT of
/// `pkg/planner/util/utilfuncp/func_pointer_misc_test.go:26
/// TestCloneConstantsForPlanCacheWithNilEntry`.
#[test]
fn clone_constants_for_plan_cache_preserves_nil_entries_while_cloning_unsafe_ones() {
    let first = unsafe_constant(1);
    let second = unsafe_constant(2);
    // constants slice contains a nil entry — the #66265 panic scenario.
    let constants = [Some(Arc::clone(&first)), None, Some(Arc::clone(&second))];

    let cloned = clone_constants_for_plan_cache(Some(&constants), None)
        .expect("non-nil input must stay non-nil");

    assert_eq!(cloned.len(), 3);
    let cloned_first = cloned[0]
        .as_ref()
        .expect("slot 0 must survive as a cloned constant");
    let cloned_second = cloned[2]
        .as_ref()
        .expect("slot 2 must survive as a cloned constant");

    // The nil entry must be preserved as nil in the cloned slice.
    assert!(cloned[1].is_none());

    // Unsafe slots are DEEP CLONES: new heap objects with equal payloads.
    assert!(!Arc::ptr_eq(cloned_first, &first));
    assert!(!Arc::ptr_eq(cloned_second, &second));
    assert_eq!(cloned_first.value(), first.value());
    assert_eq!(cloned_second.value(), second.value());
    assert!(!cloned_first.safe_to_share());

    // Same call with a reused destination buffer (Go's non-nil `cloned`
    // parameter is truncated and refilled, func_pointer_misc.go:507-511).
    let recycled = Vec::with_capacity(8);
    let recloned = clone_constants_for_plan_cache(Some(&constants), Some(recycled))
        .expect("non-nil input must stay non-nil");
    assert_eq!(recloned.len(), 3);
    assert!(recloned[1].is_none());

    // All-safe inputs return the original handles untouched
    // (func_pointer_misc.go:498-505 returns the source slice itself).
    let safe = Arc::new(PlanCacheConstant::new(7i64.to_be_bytes(), true));
    let safe_constants = [Some(Arc::clone(&safe)), None];
    let shared = clone_constants_for_plan_cache(Some(&safe_constants), None)
        .expect("non-nil input must stay non-nil");
    assert!(shared[0].as_ref().is_some_and(|constant| Arc::ptr_eq(constant, &safe)));

    // A nil input returns nil (func_pointer_misc.go:489-491).
    assert!(clone_constants_for_plan_cache(None, None).is_none());
}
