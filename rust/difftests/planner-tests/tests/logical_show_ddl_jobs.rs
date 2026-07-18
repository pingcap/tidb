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

//! Dependency-closed vectors for LogicalShowDDLJobs identity/hash semantics.
//!
//! The Go anchor is `TestLogicalShowDDLJobs` at
//! `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:199`.
//! These vectors isolate the generated ShowDDLJobs tag and
//! LogicalSchemaProducer schema framing; JobNumber, DDL stats/context, and
//! runtime SHOW behavior remain external.

use tidb_planner::logical_show_ddl_jobs::{LogicalShowDDLJobsIdentity, ShowDDLJobsColumnIdentity};

#[test]
fn matching_schema_have_equal_hash_and_identity() {
    let first =
        LogicalShowDDLJobsIdentity::new(Some(vec![ShowDDLJobsColumnIdentity::new(1, 0, 0)]));
    let second =
        LogicalShowDDLJobsIdentity::new(Some(vec![ShowDDLJobsColumnIdentity::new(1, 0, 0)]));

    assert_eq!(first.hash64(), second.hash64());
    assert!(first.equals(&second));
}

#[test]
fn schema_column_identity_changes_hash_and_equality() {
    let first =
        LogicalShowDDLJobsIdentity::new(Some(vec![ShowDDLJobsColumnIdentity::new(1, 0, 0)]));
    let second =
        LogicalShowDDLJobsIdentity::new(Some(vec![ShowDDLJobsColumnIdentity::new(2, 0, 0)]));

    assert_ne!(first.hash64(), second.hash64());
    assert!(!first.equals(&second));
}

#[test]
fn nil_and_present_empty_schema_are_distinct() {
    let nil_schema = LogicalShowDDLJobsIdentity::new(None);
    let empty_schema = LogicalShowDDLJobsIdentity::new(Some(Vec::new()));

    assert_ne!(nil_schema.hash64(), empty_schema.hash64());
    assert!(!nil_schema.equals(&empty_schema));
}

#[test]
fn normalized_type_fingerprint_is_part_of_schema_identity() {
    let first = LogicalShowDDLJobsIdentity::new(Some(vec![
        ShowDDLJobsColumnIdentity::with_type_fingerprint(1, 0, 0, 10),
    ]));
    let second = LogicalShowDDLJobsIdentity::new(Some(vec![
        ShowDDLJobsColumnIdentity::with_type_fingerprint(1, 0, 0, 11),
    ]));

    assert_ne!(first.hash64(), second.hash64());
    assert!(!first.equals(&second));
}
