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

//! Dependency-closed vectors for LogicalTableDual identity/hash semantics.
//!
//! The Go anchor is `TestLogicalTableDualHash64Equals` at
//! `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:114`.
//! These vectors isolate source field order, schema presence and column
//! identity, RowCount, equality, and ExplainInfo; complete FieldType/collation
//! and logical-plan runtime behavior remain external.

use tidb_planner::logical_table_dual::{ColumnIdentity, LogicalTableDualIdentity};

fn dual(row_count: i64) -> LogicalTableDualIdentity {
    LogicalTableDualIdentity::new(Some(vec![ColumnIdentity::new(1, 0, 0)]), row_count)
}

#[test]
fn matching_schema_and_row_count_have_equal_hash_and_identity() {
    let first = dual(1);
    let second = dual(1);

    assert_eq!(first.hash64(), second.hash64());
    assert!(first.equals(&second));
}

#[test]
fn schema_column_identity_changes_hash_and_equality() {
    let first = dual(1);
    let second = LogicalTableDualIdentity::new(Some(vec![ColumnIdentity::new(2, 0, 0)]), 1);

    assert_ne!(first.hash64(), second.hash64());
    assert!(!first.equals(&second));
}

#[test]
fn row_count_changes_hash_and_equality() {
    let first = dual(1);
    let second = dual(2);

    assert_ne!(first.hash64(), second.hash64());
    assert!(!first.equals(&second));
}

#[test]
fn nil_and_present_empty_schema_are_distinct() {
    let nil_schema = LogicalTableDualIdentity::new(None, 0);
    let empty_schema = LogicalTableDualIdentity::new(Some(Vec::new()), 0);

    assert_ne!(nil_schema.hash64(), empty_schema.hash64());
    assert!(!nil_schema.equals(&empty_schema));
}

#[test]
fn explain_info_preserves_source_rowcount_text() {
    assert_eq!(dual(0).explain_info(), "rowcount:0");
    assert_eq!(dual(1).explain_info(), "rowcount:1");
}
