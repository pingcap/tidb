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

//! Dependency-closed vectors for LogicalTopN identity/hash semantics.
//!
//! The Go anchor is `TestLogicalTopNHash64Equals` at
//! `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:36`.
//! These vectors isolate generated TopN field order, normalized schema and
//! ordering metadata, Offset, Count, and PreferLimitToCop; full expression,
//! property, plan-context, and runtime behavior remain external.

use tidb_planner::logical_top_n::{
    LogicalTopNIdentity, TopNByItem, TopNColumnIdentity, TopNSortItem,
};

fn top_n() -> LogicalTopNIdentity {
    let column = TopNColumnIdentity::new(1, 0, 0);
    LogicalTopNIdentity::new(
        None,
        Some(vec![TopNByItem::new(column.clone(), true)]),
        Some(vec![TopNSortItem::new(Some(column), true)]),
        0,
        0,
        false,
    )
}

#[test]
fn matching_top_n_metadata_have_equal_hash_and_identity() {
    let first = top_n();
    let second = top_n();

    assert_eq!(first.hash64(), second.hash64());
    assert!(first.equals(&second));
}

#[test]
fn by_item_column_identity_changes_hash_and_equality() {
    let first = top_n();
    let column = TopNColumnIdentity::new(2, 0, 0);
    let second = LogicalTopNIdentity::new(
        None,
        Some(vec![TopNByItem::new(column.clone(), true)]),
        Some(vec![TopNSortItem::new(
            Some(TopNColumnIdentity::new(1, 0, 0)),
            true,
        )]),
        0,
        0,
        false,
    );

    assert_ne!(first.hash64(), second.hash64());
    assert!(!first.equals(&second));
}

#[test]
fn by_item_direction_changes_hash_and_equality() {
    let first = top_n();
    let second = LogicalTopNIdentity::new(
        None,
        Some(vec![TopNByItem::new(
            TopNColumnIdentity::new(1, 0, 0),
            false,
        )]),
        Some(vec![TopNSortItem::new(
            Some(TopNColumnIdentity::new(1, 0, 0)),
            true,
        )]),
        0,
        0,
        false,
    );

    assert_ne!(first.hash64(), second.hash64());
    assert!(!first.equals(&second));
}

#[test]
fn partition_column_direction_offset_and_count_are_identity_fields() {
    let first = top_n();

    let changed_partition_direction = LogicalTopNIdentity::new(
        None,
        Some(vec![TopNByItem::new(
            TopNColumnIdentity::new(1, 0, 0),
            true,
        )]),
        Some(vec![TopNSortItem::new(
            Some(TopNColumnIdentity::new(1, 0, 0)),
            false,
        )]),
        0,
        0,
        false,
    );
    assert_ne!(first.hash64(), changed_partition_direction.hash64());
    assert!(!first.equals(&changed_partition_direction));

    let changed_partition_column = LogicalTopNIdentity::new(
        None,
        Some(vec![TopNByItem::new(
            TopNColumnIdentity::new(1, 0, 0),
            true,
        )]),
        Some(vec![TopNSortItem::new(
            Some(TopNColumnIdentity::new(2, 0, 0)),
            true,
        )]),
        0,
        0,
        false,
    );
    assert_ne!(first.hash64(), changed_partition_column.hash64());
    assert!(!first.equals(&changed_partition_column));

    let changed_offset = LogicalTopNIdentity::new(
        None,
        Some(vec![TopNByItem::new(
            TopNColumnIdentity::new(1, 0, 0),
            true,
        )]),
        Some(vec![TopNSortItem::new(
            Some(TopNColumnIdentity::new(1, 0, 0)),
            true,
        )]),
        2,
        0,
        false,
    );
    assert_ne!(first.hash64(), changed_offset.hash64());
    assert!(!first.equals(&changed_offset));

    let changed_count = LogicalTopNIdentity::new(
        None,
        Some(vec![TopNByItem::new(
            TopNColumnIdentity::new(1, 0, 0),
            true,
        )]),
        Some(vec![TopNSortItem::new(
            Some(TopNColumnIdentity::new(1, 0, 0)),
            true,
        )]),
        0,
        1,
        false,
    );
    assert_ne!(first.hash64(), changed_count.hash64());
    assert!(!first.equals(&changed_count));
}

#[test]
fn prefer_limit_to_cop_changes_hash_and_equality() {
    let first = top_n();
    let second = LogicalTopNIdentity::new(
        None,
        Some(vec![TopNByItem::new(
            TopNColumnIdentity::new(1, 0, 0),
            true,
        )]),
        Some(vec![TopNSortItem::new(
            Some(TopNColumnIdentity::new(1, 0, 0)),
            true,
        )]),
        0,
        0,
        true,
    );

    assert_ne!(first.hash64(), second.hash64());
    assert!(!first.equals(&second));
}

#[test]
fn nil_and_present_empty_optional_fields_are_distinct() {
    let nil_fields = LogicalTopNIdentity::new(None, None, None, 0, 0, false);
    let empty_fields = LogicalTopNIdentity::new(
        Some(Vec::new()),
        Some(Vec::new()),
        Some(Vec::new()),
        0,
        0,
        false,
    );

    assert_ne!(nil_fields.hash64(), empty_fields.hash64());
    assert!(!nil_fields.equals(&empty_fields));
}
