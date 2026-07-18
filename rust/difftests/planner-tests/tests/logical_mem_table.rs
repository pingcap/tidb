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

//! Dependency-closed vectors for LogicalMemTable identity/hash semantics.
//!
//! The Go anchor is `TestLogicalMemTableHash64Equals` at
//! `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:416`.
//! These vectors isolate MemTableScan tag, schema, lower-case DBName, and
//! optional TableInfo ID; Extractor/Columns/QueryTimeRange, infoschema,
//! context, and runtime execution remain external.

use tidb_planner::logical_mem_table::{LogicalMemTableIdentity, MemTableColumnIdentity};

fn base() -> LogicalMemTableIdentity {
    LogicalMemTableIdentity::new(Some(vec![MemTableColumnIdentity::new(1, 0, 0)]), "", None)
}

#[test]
fn matching_mem_table_metadata_have_equal_hash_and_identity() {
    let first = base();
    let second = base();

    assert_eq!(first.hash64(), second.hash64());
    assert!(first.equals(&second));
}

#[test]
fn schema_column_identity_changes_hash_and_equality() {
    let first = base();
    let second =
        LogicalMemTableIdentity::new(Some(vec![MemTableColumnIdentity::new(2, 0, 0)]), "", None);

    assert_ne!(first.hash64(), second.hash64());
    assert!(!first.equals(&second));
}

#[test]
fn db_name_is_case_folded_and_part_of_identity() {
    let upper = LogicalMemTableIdentity::new(None, "D1", None);
    let lower = LogicalMemTableIdentity::new(None, "d1", None);
    let other = LogicalMemTableIdentity::new(None, "d2", None);

    assert_eq!(upper.db_name(), "d1");
    assert_eq!(upper.hash64(), lower.hash64());
    assert!(upper.equals(&lower));
    assert_ne!(upper.hash64(), other.hash64());
    assert!(!upper.equals(&other));
}

#[test]
fn table_info_nil_and_ids_are_distinct_identity_fields() {
    let nil_info = base();
    let zero_info = LogicalMemTableIdentity::new(
        Some(vec![MemTableColumnIdentity::new(1, 0, 0)]),
        "",
        Some(0),
    );
    let one_info = LogicalMemTableIdentity::new(
        Some(vec![MemTableColumnIdentity::new(1, 0, 0)]),
        "",
        Some(1),
    );

    assert_ne!(nil_info.hash64(), zero_info.hash64());
    assert!(!nil_info.equals(&zero_info));
    assert_ne!(zero_info.hash64(), one_info.hash64());
    assert!(!zero_info.equals(&one_info));
}

#[test]
fn normalized_type_fingerprint_is_part_of_schema_identity() {
    let first = LogicalMemTableIdentity::new(
        Some(vec![MemTableColumnIdentity::with_type_fingerprint(
            1, 0, 0, 10,
        )]),
        "",
        None,
    );
    let second = LogicalMemTableIdentity::new(
        Some(vec![MemTableColumnIdentity::with_type_fingerprint(
            1, 0, 0, 11,
        )]),
        "",
        None,
    );

    assert_ne!(first.hash64(), second.hash64());
    assert!(!first.equals(&second));
}
