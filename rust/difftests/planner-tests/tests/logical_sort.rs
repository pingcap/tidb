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

//! Dependency-closed vectors for LogicalSort identity/hash semantics.
//!
//! The Go anchor is `TestLogicalSortHash64Equals` at
//! `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:158`.
//! These vectors isolate the generated Sort tag, nil/present ByItems framing,
//! normalized column expression identity, and direction; arbitrary expression
//! metadata, ExplainByItems formatting, and runtime ordering remain external.

use tidb_planner::logical_sort::{LogicalSortIdentity, SortByItem, SortColumnIdentity};

fn sort_with(expr: SortColumnIdentity, desc: bool) -> LogicalSortIdentity {
    LogicalSortIdentity::new(Some(vec![SortByItem::new(expr, desc)]))
}

#[test]
fn matching_by_items_have_equal_hash_and_identity() {
    let first = sort_with(SortColumnIdentity::new(1, 0, 0), true);
    let second = sort_with(SortColumnIdentity::new(1, 0, 0), true);

    assert_eq!(first.hash64(), second.hash64());
    assert!(first.equals(&second));
}

#[test]
fn nil_and_present_empty_by_items_are_distinct() {
    let nil_items = LogicalSortIdentity::new(None);
    let empty_items = LogicalSortIdentity::new(Some(Vec::new()));

    assert_ne!(nil_items.hash64(), empty_items.hash64());
    assert!(!nil_items.equals(&empty_items));
}

#[test]
fn by_item_column_identity_changes_hash_and_equality() {
    let first = sort_with(SortColumnIdentity::new(1, 0, 0), true);
    let second = sort_with(SortColumnIdentity::new(2, 0, 0), true);

    assert_ne!(first.hash64(), second.hash64());
    assert!(!first.equals(&second));
}

#[test]
fn by_item_direction_changes_hash_and_equality() {
    let first = sort_with(SortColumnIdentity::new(1, 0, 0), false);
    let second = sort_with(SortColumnIdentity::new(1, 0, 0), true);

    assert_ne!(first.hash64(), second.hash64());
    assert!(!first.equals(&second));
}

#[test]
fn by_item_type_fingerprint_is_part_of_normalized_expression_identity() {
    let first = sort_with(SortColumnIdentity::with_type_fingerprint(1, 0, 0, 10), true);
    let second = sort_with(SortColumnIdentity::with_type_fingerprint(1, 0, 0, 11), true);

    assert_ne!(first.hash64(), second.hash64());
    assert!(!first.equals(&second));
}
