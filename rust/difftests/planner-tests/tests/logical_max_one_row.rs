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

//! Dependency-closed vectors for LogicalMaxOneRow identity/hash semantics.
//!
//! The Go anchor is `TestLogicalMaxOneRowHash64Equals` at
//! `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:510`.
//! These vectors isolate the generated MaxOneRow tag/plan-ID field order and
//! equality; context, child schema, statistics, and runtime behavior remain
//! external.

use tidb_planner::logical_max_one_row::LogicalMaxOneRowIdentity;

#[test]
fn matching_plan_ids_have_equal_hash_and_identity() {
    let first = LogicalMaxOneRowIdentity::new(1);
    let second = LogicalMaxOneRowIdentity::new(1);

    assert_eq!(first.hash64(), second.hash64());
    assert!(first.equals(second));
}

#[test]
fn different_plan_ids_change_hash_and_equality() {
    let first = LogicalMaxOneRowIdentity::new(1);
    let second = LogicalMaxOneRowIdentity::new(2);

    assert_ne!(first.hash64(), second.hash64());
    assert!(!first.equals(second));
}

#[test]
fn zero_and_negative_plan_ids_remain_source_integer_values() {
    let zero = LogicalMaxOneRowIdentity::new(0);
    let negative = LogicalMaxOneRowIdentity::new(-1);

    assert_eq!(zero.plan_id(), 0);
    assert_eq!(negative.plan_id(), -1);
    assert_ne!(zero.hash64(), negative.hash64());
    assert!(!zero.equals(negative));
}
