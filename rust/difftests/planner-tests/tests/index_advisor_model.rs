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

//! Dependency-closed vectors for `pkg/planner/indexadvisor/model.go`.
//!
//! The direct Go anchor is `TestOptimizerPrefixContainIndex` at
//! `pkg/planner/indexadvisor/optimizer_test.go:68`.

use tidb_planner::index_advisor_model::{Column, Index};

#[test]
fn source_index_prefix_relation_matches_optimizer_vectors() {
    let cases = [
        ("t1", &[] as &[&str], true),
        ("t1", &["a"][..], true),
        ("t1", &["b"][..], true),
        ("t1", &["b", "c"][..], true),
        ("t1", &["c"][..], false),
        ("t1", &["a", "b"][..], false),
        ("t1", &["b", "c", "a"][..], false),
        ("t2", &["a"][..], true),
        ("t2", &["a", "b"][..], true),
        ("t2", &["a", "b", "c"][..], true),
        ("t2", &["a", "b", "c", "d"][..], true),
        ("t2", &["d"][..], true),
        ("t2", &["d", "c"][..], true),
        ("t2", &["b"][..], false),
        ("t2", &["b", "a"][..], false),
        ("t2", &["b", "a", "c"][..], false),
    ];
    let existing = [
        Index::new("test", "t1", "a", &["a"]),
        Index::new("test", "t1", "bc", &["b", "c"]),
        Index::new("test", "t2", "abcd", &["a", "b", "c", "d"]),
        Index::new("test", "t2", "dcba", &["d", "c", "b", "a"]),
    ];
    for (table, columns, expected) in cases {
        let candidate = Index::new("TEST", table, "candidate", columns);
        let found = existing
            .iter()
            .any(|index| index.prefix_contains(&candidate));
        assert_eq!(found, expected, "table={table}, columns={columns:?}");
    }
}

#[test]
fn source_columns_and_indexes_normalize_identity_keys() {
    let column = Column::new("Test", "T", "A");
    assert_eq!(column.key(), "test.t.a");
    let index = Index::new("Test", "T", "IDX", &["A", "B"]);
    assert_eq!(index.key(), "test.t(a,b)");
}
