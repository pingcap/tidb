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

//! Dependency-closed tests for `pkg/planner/core/schema_table_key.go:21`.
//!
//! The Go integration anchor is `TestPreprocessCTE` at
//! `pkg/planner/core/preprocess_test.go:422`, whose nested CTE/view cases
//! exercise case-insensitive schema, table, and alias identity.

use tidb_planner::schema_table_key::{SchemaTableKey, TableAliasKey};

#[test]
fn schema_table_keys_normalize_case_insensitively() {
    let lower = SchemaTableKey::new("Test", "V1");
    let upper = SchemaTableKey::new("test", "v1");
    assert_eq!(lower, upper);
    assert_eq!(lower.schema(), "test");
    assert_eq!(lower.table(), "v1");
}

#[test]
fn aliases_preserve_qualification_in_identity() {
    let bare = TableAliasKey::new("T1");
    let qualified = TableAliasKey::qualified("TEST", "T1");
    assert_eq!(bare.name(), "t1");
    assert_eq!(bare.schema(), "");
    assert!(!bare.is_qualified());
    assert_eq!(qualified.name(), "t1");
    assert_eq!(qualified.schema(), "test");
    assert!(qualified.is_qualified());
    assert_ne!(bare, qualified);
}
