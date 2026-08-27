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

//! Port of `pkg/planner/util/column_test.go::TestIndexInfo2Cols`
//! (`pkg/planner.part22` item 1268 on `origin/master`).
//!
//! Go's helper family (`indexInfo2ColsImpl`, pkg/planner/util/column.go:46-103)
//! resolves each `model.IndexColumn` BY NAME among `colInfos`
//! (`indexCol2Col`, column.go:25-41), projects the same-position entry from
//! `cols`, and stops prefix accumulation at the first miss while full columns
//! keep a nil slot per miss (:66-74). Prefix lengths switch to the
//! `UnspecifiedLength` sentinel when an index column's declared length equals
//! the field length (:77-82).
//!
//! The crate carrier [`tidb_planner::index_columns::project_index_columns`]
//! normalizes both metadata inputs to `(name, length)` pairs; Go's returned
//! column POINTERS become caller-slice POSITIONS ([`ResolvedColumn::
//! source_index`]), which pins the same identity: Go asserts `EqualColumn` on
//! those pointers and `EqualColumn` is UniqueID equality
//! (pkg/expression/column.go:327) while here position IS the identity handed
//! back. Go's fixture leaves every `IndexColumn.Length` at its zero value, so
//! neither the prefix-mark branch (column.go:31-33 needs Length > 0) nor the
//! sentinel normalization (:77-82) fires — every reported length stays 0,
//! which this test pins.
//!
//! One honest deviation: Go indexes `cols[i]` directly on a name match without
//! bounds-checking it (column.go:25-41); the crate treats an out-of-range
//! position as a miss instead of panicking. All four Go rows keep
//! `colInfos`/`cols` aligned, so the deviation is not observable here.

use tidb_planner::index_columns::{
    project_index_columns, ColumnRef, IndexColumnProjection, IndexColumnRef, ResolvedColumn,
    UNSPECIFIED_LENGTH,
};

/// GO PORT of `pkg/planner/util/column_test.go:29 TestIndexInfo2Cols`.
///
/// Fixture mirror (column_test.go:31-40): table columns named "0"/"1"/"2",
/// one three-column index over those names, and pruned-column inputs of one
/// or two entries; assertions follow the four Go rows in order.
#[test]
fn index_info_2_cols_projects_prefix_stop_and_full_nil_slots() {
    let index_columns = [
        IndexColumnRef::new("0", 0),
        IndexColumnRef::new("1", 0),
        IndexColumnRef::new("2", 0),
    ];

    let expect =
        |projection: &IndexColumnProjection, prefix_positions: &[usize], full: &[Option<usize>]| {
            let expected_prefix: Vec<ResolvedColumn> = prefix_positions
                .iter()
                .map(|&position| ResolvedColumn {
                    source_index: position,
                    is_prefix: false,
                })
                .collect();
            assert_eq!(projection.prefix, expected_prefix);
            // Fixture has zero Length everywhere, so no sentinel swap happens.
            assert!(projection.prefix_lengths.iter().all(|&length| length == 0));

            let expected_full: Vec<Option<ResolvedColumn>> = full
                .iter()
                .map(|slot| {
                    slot.map(|position| ResolvedColumn {
                        source_index: position,
                        is_prefix: false,
                    })
                })
                .collect();
            assert_eq!(projection.full, expected_full);
            // Counts pinned like Go pins len(resCols)/len(lengths).
            assert_eq!(projection.full_lengths.len(), projection.full.len());
        };

    // Row 1 (:42-47): cols=[col0], colInfos=[colInfo0]. Name "0" resolves;
    // "1"/"2" find no colInfo entry, so the prefix stops after one column.
    let infos_one = [ColumnRef::new("0", UNSPECIFIED_LENGTH)];
    let cols_one = [ColumnRef::new("0", UNSPECIFIED_LENGTH)];
    expect(
        &project_index_columns(&infos_one, &cols_one, &index_columns),
        &[0],
        &[Some(0), None, None],
    );

    // Row 2 (:49-54): cols=[col1], colInfos=[colInfo1]. The FIRST index
    // column misses (no info named "0"), so both prefix outputs are empty.
    // The full output still resolves "1" BY NAME among colInfos into the
    // caller's slot 0 — Go's IndexInfo2FullCols would answer [nil, col1, nil]
    // here (column.go:66-74 keep per-miss nil slots regardless of order).
    let infos_b = [ColumnRef::new("1", UNSPECIFIED_LENGTH)];
    let cols_b = [ColumnRef::new("1", UNSPECIFIED_LENGTH)];
    expect(
        &project_index_columns(&infos_b, &cols_b, &index_columns),
        &[],
        &[None, Some(0), None],
    );

    // Row 3 (:56-63): cols=[col0,col1], colInfos=[colInfo0,colInfo1]; both
    // leading index columns project in order.
    let infos_ab = [
        ColumnRef::new("0", UNSPECIFIED_LENGTH),
        ColumnRef::new("1", UNSPECIFIED_LENGTH),
    ];
    let cols_ab = [
        ColumnRef::new("0", UNSPECIFIED_LENGTH),
        ColumnRef::new("1", UNSPECIFIED_LENGTH),
    ];
    expect(
        &project_index_columns(&infos_ab, &cols_ab, &index_columns),
        &[0, 1],
        &[Some(0), Some(1), None],
    );

    // Rows 4a-4c (:65-87): col1 was PRUNED, cols=[col0,col2]. Prefix output
    // is exactly IndexInfo2PrefixCols' answer ([col0]); the full output is
    // IndexInfo2FullCols' [col0, NIL, col2].
    let infos_ac = [
        ColumnRef::new("0", UNSPECIFIED_LENGTH),
        ColumnRef::new("2", UNSPECIFIED_LENGTH),
    ];
    let cols_ac = [
        ColumnRef::new("0", UNSPECIFIED_LENGTH),
        ColumnRef::new("2", UNSPECIFIED_LENGTH),
    ];
    expect(
        &project_index_columns(&infos_ac, &cols_ac, &index_columns),
        &[0],
        &[Some(0), None, Some(1)],
    );
}
