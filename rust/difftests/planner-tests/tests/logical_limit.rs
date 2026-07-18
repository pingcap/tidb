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

//! Dependency-closed vectors for LogicalLimit identity/hash semantics.
//!
//! The Go anchor is `TestLogicalLimitHash64Equals` at
//! `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:530`.
//! These vectors isolate source field order, schema/PartitionBy framing,
//! column and direction identity, Offset, Count, and basic ExplainInfo;
//! complete Go expression metadata, property formatting, and runtime behavior
//! remain external.

use tidb_planner::logical_limit::{LimitColumnIdentity, LimitSortItem, LogicalLimitIdentity};

fn limit(offset: u64, count: u64) -> LogicalLimitIdentity {
    LogicalLimitIdentity::new(
        None,
        Some(vec![LimitSortItem::new(
            Some(LimitColumnIdentity::new(1, 0, 0)),
            true,
        )]),
        offset,
        count,
    )
}

#[test]
fn matching_partition_and_limits_have_equal_hash_and_identity() {
    let first = limit(1, 1);
    let second = limit(1, 1);

    assert_eq!(first.hash64(), second.hash64());
    assert!(first.equals(&second));
}

#[test]
fn partition_column_identity_changes_hash_and_equality() {
    let first = limit(1, 1);
    let second = LogicalLimitIdentity::new(
        None,
        Some(vec![LimitSortItem::new(
            Some(LimitColumnIdentity::new(2, 0, 0)),
            true,
        )]),
        1,
        1,
    );

    assert_ne!(first.hash64(), second.hash64());
    assert!(!first.equals(&second));
}

#[test]
fn partition_direction_changes_hash_and_equality() {
    let first = limit(1, 1);
    let second = LogicalLimitIdentity::new(
        None,
        Some(vec![LimitSortItem::new(
            Some(LimitColumnIdentity::new(1, 0, 0)),
            false,
        )]),
        1,
        1,
    );

    assert_ne!(first.hash64(), second.hash64());
    assert!(!first.equals(&second));
}

#[test]
fn offset_and_count_changes_hash_and_equality() {
    let baseline = limit(1, 1);

    let changed_offset = limit(2, 1);
    assert_ne!(baseline.hash64(), changed_offset.hash64());
    assert!(!baseline.equals(&changed_offset));

    let changed_count = limit(1, 2);
    assert_ne!(baseline.hash64(), changed_count.hash64());
    assert!(!baseline.equals(&changed_count));
}

#[test]
fn nil_and_present_empty_partition_by_are_distinct() {
    let nil_partition = LogicalLimitIdentity::new(None, None, 0, 1);
    let empty_partition = LogicalLimitIdentity::new(None, Some(Vec::new()), 0, 1);

    assert_ne!(nil_partition.hash64(), empty_partition.hash64());
    assert!(!nil_partition.equals(&empty_partition));
}

#[test]
fn explain_info_preserves_source_offset_count_text() {
    assert_eq!(
        limit(1, 1).explain_info(),
        "partition by 1 items, offset:1, count:1"
    );
    assert_eq!(
        LogicalLimitIdentity::new(None, None, 0, 10).explain_info(),
        "offset:0, count:10"
    );
}
