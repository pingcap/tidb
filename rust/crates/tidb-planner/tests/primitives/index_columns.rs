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

//! Dependency-closed tests for `pkg/planner/util/column.go:46`.
//!
//! The Go anchor is `TestIndexInfo2Cols` at
//! `pkg/planner/util/column_test.go:29`; these vectors isolate prefix stopping
//! and full-column missing-slot semantics from expression/catalog ownership.

use tidb_planner::index_columns::{
    project_index_columns, ColumnRef, IndexColumnProjection, IndexColumnRef, ResolvedColumn,
    UNSPECIFIED_LENGTH,
};

#[test]
fn projection_stops_prefix_at_missing_column_and_keeps_full_slots() {
    let infos = [ColumnRef::new("0", 100), ColumnRef::new("2", 100)];
    let columns = infos.clone();
    let index = [
        IndexColumnRef::new("0", UNSPECIFIED_LENGTH),
        IndexColumnRef::new("1", UNSPECIFIED_LENGTH),
        IndexColumnRef::new("2", UNSPECIFIED_LENGTH),
    ];
    let projection = project_index_columns(&infos, &columns, &index);
    assert_eq!(
        projection,
        IndexColumnProjection {
            prefix: vec![ResolvedColumn {
                source_index: 0,
                is_prefix: false,
            }],
            prefix_lengths: vec![UNSPECIFIED_LENGTH],
            full: vec![
                Some(ResolvedColumn {
                    source_index: 0,
                    is_prefix: false,
                }),
                None,
                Some(ResolvedColumn {
                    source_index: 1,
                    is_prefix: false,
                }),
            ],
            full_lengths: vec![UNSPECIFIED_LENGTH; 3],
        }
    );
}

#[test]
fn projection_marks_strict_prefix_and_normalizes_full_length() {
    let infos = [ColumnRef::new("a", 20)];
    let columns = infos.clone();
    let index = [IndexColumnRef::new("a", 10)];
    let projection = project_index_columns(&infos, &columns, &index);
    assert!(projection.prefix[0].is_prefix);
    assert_eq!(projection.prefix_lengths, vec![10]);

    let full = project_index_columns(&infos, &columns, &[IndexColumnRef::new("a", 20)]);
    assert_eq!(full.prefix_lengths, vec![UNSPECIFIED_LENGTH]);
    assert_eq!(full.full_lengths, vec![UNSPECIFIED_LENGTH]);
}
