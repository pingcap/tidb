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

//! Go `pkg/util/ranger/row_count_bound.go` (e48ac5fef19).

use std::collections::HashSet;

use tidb_datatype::{Datum, UNSPECIFIED_LENGTH};
use tidb_expr::column::Column;

use crate::types::Range;

/// Go `RowCountUpperBoundByUniqueKey`: a proven upper bound of the number of
/// rows an index scan over `ranges` can return, `None` when there is none.
///
/// `idx_cols` and `idx_col_lens` are the columns the ranges are built on
/// (`PhysicalIndexScan.IdxCols` / `IdxColLens`), which may include the int
/// handle appended for a non-unique index. The bound is `ranges.len()` when
/// every range fixes all columns of some unique key of the table to non-NULL
/// values, since such a range can match at most one row.
///
/// For a table with a unique key `uk(a)` scanned through a non-unique index
/// `idx(a, b)`:
///
/// - `a IN (1, 2, 3)` gives `[1, 1]`, `[2, 2]` and `[3, 3]`; each fixes `a`,
///   so the bound is 3.
/// - `a = 1 AND b > 5` gives `(1 5, 1 +inf]`; it fixes `a`, so the bound is 1.
/// - `a IN (1, 2) OR a > 10` gives `[1, 1]`, `[2, 2]` and `(10, +inf]`; the
///   last range does not fix `a`, so there is no bound.
#[must_use]
pub fn row_count_upper_bound_by_unique_key(
    tbl_info: Option<&tidb_model::TableInfo>,
    idx_cols: &[Column],
    idx_col_lens: &[i64],
    ranges: &[Range],
) -> Option<u64> {
    let tbl_info = tbl_info?;
    if ranges.is_empty() || idx_cols.is_empty() || idx_cols.len() != idx_col_lens.len() {
        return None;
    }
    let unique_keys = unique_key_column_ids(tbl_info);
    if unique_keys.is_empty() {
        return None;
    }
    let mut fixed = HashSet::with_capacity(idx_cols.len());
    for ran in ranges {
        fixed.clear();
        if !collect_fixed_not_null_columns(ran, idx_cols, idx_col_lens, &mut fixed) || !covers_any_key(&fixed, &unique_keys)
        {
            return None;
        }
    }
    Some(ranges.len() as u64)
}

/// Go `uniqueKeyColumnIDs`: the column IDs of each unique key enforced on
/// all rows of the table.
fn unique_key_column_ids(tbl_info: &tidb_model::TableInfo) -> Vec<Vec<i64>> {
    let mut keys = Vec::new();
    if tbl_info.pk_is_handle {
        if let Some(pk_col) = tbl_info.get_pk_col_info() {
            keys.push(vec![pk_col.read().id]);
        }
    }
    let columns: Vec<_> = tbl_info.columns.iter_deref().collect();
    for idx in tbl_info.indices.iter_deref() {
        let idx = idx.read();
        // A non-public index may not be unique on existing rows yet, a
        // partial index is only unique on rows matching its condition, and a
        // prefix or multi-valued index is not unique on the full column
        // values.
        if !(idx.unique || idx.primary)
            || idx.state != tidb_model::SchemaState::PUBLIC
            || idx.mv_index
            || idx.has_condition()
            || idx.has_prefix_index()
        {
            continue;
        }
        keys.push(
            idx.columns
                .iter_deref()
                .map(|idx_col| columns[idx_col.read().offset as usize].read().id)
                .collect(),
        );
    }
    keys
}

/// Go `Range.collectFixedNotNullColumns`: adds to `fixed` the IDs of the
/// leading index columns the range fixes to a single non-NULL value of the
/// full column; `false` when the range is malformed for this check.
///
/// The columns are walked left to right while the low and high values are
/// equal; the walk stops at the first column where they differ. A fixed
/// column is recorded only if its value is not NULL and the index stores the
/// full value. A NULL point still fixes the columns after it. A range equal
/// on every column but excluding an end is not a point.
fn collect_fixed_not_null_columns(
    ran: &Range,
    idx_cols: &[Column],
    idx_col_lens: &[i64],
    fixed: &mut HashSet<i64>,
) -> bool {
    let n = ran.low_val.len().min(ran.high_val.len()).min(idx_cols.len()).min(ran.collators.len());
    let mut all_fixed = true;
    for i in 0..n {
        let (low, high) = (&ran.low_val[i], &ran.high_val[i]);
        if matches!(low, Datum::MinNotNull | Datum::MaxValue) || matches!(high, Datum::MinNotNull | Datum::MaxValue) {
            all_fixed = false;
            break;
        }
        match low.compare(high, ran.collators[i]) {
            Ok(std::cmp::Ordering::Equal) => {}
            _ => {
                all_fixed = false;
                break;
            }
        }
        // The column is fixed, but only a full-length non-NULL value can be
        // matched against a unique key: a unique index allows duplicated
        // NULLs, and a prefix only fixes part of the value.
        if !matches!(low, Datum::Null) && idx_col_lens[i] == UNSPECIFIED_LENGTH {
            fixed.insert(idx_cols[i].id);
        }
    }
    // If every column of the range is fixed, an exclusive end means the range
    // is not a point. When the walk stopped early, an exclusive end only
    // applies to the column where it stopped.
    !(all_fixed
        && ran.low_val.len() == ran.high_val.len()
        && n == ran.low_val.len()
        && (ran.low_exclude || ran.high_exclude))
}

/// Go `coversAnyKey`: whether `fixed` contains every column of at least one
/// key.
fn covers_any_key(fixed: &HashSet<i64>, keys: &[Vec<i64>]) -> bool {
    keys.iter().any(|key| key.iter().all(|id| fixed.contains(id)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_charset::Collation;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_model::{ColumnInfo, IndexColumn, IndexInfo, SchemaState, TableInfo};

    const COL_A: i64 = 1; // a int
    const COL_HANDLE: i64 = 2; // id int primary key, the int handle
    const COL_NAME: i64 = 3; // name varchar(32)
    const FULL: i64 = UNSPECIFIED_LENGTH;
    /// MySQL `PRI_KEY_FLAG | NOT_NULL_FLAG`.
    const PRI_KEY_NOT_NULL: u64 = 2 | 1;

    fn new_bound_test_table(indices: Vec<IndexInfo>) -> TableInfo {
        let new_col = |id: i64, offset: i64, name: &str, tp: FieldTypeCode| {
            let mut col = ColumnInfo::new(id, name, FieldType::new(tp));
            col.offset = offset;
            col
        };
        let mut pk = new_col(COL_HANDLE, 1, "id", FieldTypeCode::LongLong);
        pk.add_flag(PRI_KEY_NOT_NULL);
        TableInfo {
            columns: vec![
                new_col(COL_A, 0, "a", FieldTypeCode::LongLong),
                pk,
                new_col(COL_NAME, 2, "name", FieldTypeCode::Varchar),
            ]
            .into(),
            pk_is_handle: true,
            indices: indices.into(),
            ..TableInfo::default()
        }
    }

    fn new_bound_test_index(unique: bool, cols: &[(i64, i64)]) -> IndexInfo {
        IndexInfo {
            unique,
            state: SchemaState::PUBLIC,
            columns: cols
                .iter()
                .map(|&(offset, length)| IndexColumn { offset, length, ..IndexColumn::default() })
                .collect::<Vec<_>>()
                .into(),
            ..IndexInfo::default()
        }
    }

    fn point_range(vals: Vec<Datum>) -> Range {
        Range {
            low_val: vals.clone().into(),
            high_val: vals.clone().into(),
            collators: vec![Collation::Binary; vals.len()],
            low_exclude: false,
            high_exclude: false,
        }
    }

    fn int_datums(vals: &[i64]) -> Vec<Datum> {
        vals.iter().map(|&v| Datum::Int(v)).collect()
    }

    fn cols_of(ids: &[i64]) -> Vec<Column> {
        ids.iter()
            .map(|&id| {
                let mut column = Column::new(0, FieldType::new(FieldTypeCode::LongLong));
                column.id = id;
                column
            })
            .collect()
    }

    fn string(value: &str) -> Datum {
        Datum::new_string_datum(value.as_bytes())
    }

    /// Go `TestRowCountUpperBoundByUniqueKey`.
    #[test]
    fn test_row_count_upper_bound_by_unique_key() {
        let uniq_a = || new_bound_test_index(true, &[(0, FULL)]);
        // The scanned index is a non-unique index on (a, id); its ranges
        // cover a and the handle.
        let (idx_a_cols, idx_a_lens) = (cols_of(&[COL_A, COL_HANDLE]), vec![FULL, FULL]);
        let mut non_public = new_bound_test_index(true, &[(0, FULL)]);
        non_public.state = SchemaState::WRITE_REORGANIZATION;
        let mut partial = new_bound_test_index(true, &[(2, FULL)]);
        partial.condition_expr_string = "a > 0".to_owned();
        let mut mv = new_bound_test_index(true, &[(2, FULL)]);
        mv.mv_index = true;
        let mut no_handle_tbl = new_bound_test_table(vec![new_bound_test_index(false, &[(0, FULL)])]);
        no_handle_tbl.pk_is_handle = false;
        let mut exclusive = point_range(int_datums(&[1]));
        exclusive.low_exclude = true;

        let cases: Vec<(&str, TableInfo, Vec<Column>, Vec<i64>, Vec<Range>, Option<u64>)> = vec![
            (
                "points on a unique prefix",
                new_bound_test_table(vec![uniq_a()]),
                idx_a_cols.clone(),
                idx_a_lens.clone(),
                vec![point_range(int_datums(&[1])), point_range(int_datums(&[2])), point_range(int_datums(&[3]))],
                Some(3),
            ),
            (
                "unique prefix fixed, later column is a range",
                new_bound_test_table(vec![uniq_a()]),
                idx_a_cols.clone(),
                idx_a_lens.clone(),
                vec![Range {
                    low_val: int_datums(&[1, 5]).into(),
                    high_val: vec![Datum::Int(1), Datum::MaxValue].into(),
                    low_exclude: true,
                    high_exclude: false,
                    collators: vec![Collation::Binary; 2],
                }],
                Some(1),
            ),
            (
                "points on all columns use the int handle",
                new_bound_test_table(Vec::new()),
                idx_a_cols.clone(),
                idx_a_lens.clone(),
                vec![point_range(int_datums(&[1, 10])), point_range(int_datums(&[1, 11]))],
                Some(2),
            ),
            (
                "NULL point",
                new_bound_test_table(vec![uniq_a()]),
                idx_a_cols.clone(),
                idx_a_lens.clone(),
                vec![point_range(int_datums(&[1])), point_range(vec![Datum::Null])],
                None,
            ),
            (
                "range on the unique column",
                new_bound_test_table(vec![uniq_a()]),
                idx_a_cols.clone(),
                idx_a_lens.clone(),
                vec![Range {
                    low_val: int_datums(&[5]).into(),
                    high_val: vec![Datum::MaxValue].into(),
                    low_exclude: true,
                    high_exclude: false,
                    collators: vec![Collation::Binary],
                }],
                None,
            ),
            ("exclusive point", new_bound_test_table(vec![uniq_a()]), idx_a_cols.clone(), idx_a_lens.clone(), vec![exclusive], None),
            (
                "only the handle is fixed but a is not",
                new_bound_test_table(vec![uniq_a()]),
                idx_a_cols.clone(),
                idx_a_lens.clone(),
                vec![Range {
                    low_val: vec![Datum::MinNotNull, Datum::Int(10)].into(),
                    high_val: vec![Datum::MaxValue, Datum::Int(10)].into(),
                    low_exclude: false,
                    high_exclude: false,
                    collators: vec![Collation::Binary; 2],
                }],
                None,
            ),
            (
                "non-unique index without any unique key",
                no_handle_tbl,
                cols_of(&[COL_A]),
                vec![FULL],
                vec![point_range(int_datums(&[1]))],
                None,
            ),
            (
                "unique index is not public",
                new_bound_test_table(vec![non_public]),
                cols_of(&[COL_A]),
                vec![FULL],
                vec![point_range(int_datums(&[1]))],
                None,
            ),
            (
                "point on a prefix column",
                new_bound_test_table(vec![new_bound_test_index(true, &[(2, FULL)])]),
                cols_of(&[COL_NAME]),
                vec![4],
                vec![point_range(vec![string("abcd")])],
                None,
            ),
            (
                "prefix unique index",
                new_bound_test_table(vec![new_bound_test_index(true, &[(2, 4)])]),
                cols_of(&[COL_NAME]),
                vec![FULL],
                vec![point_range(vec![string("abcd")])],
                None,
            ),
            (
                "partial unique index",
                new_bound_test_table(vec![partial]),
                cols_of(&[COL_NAME]),
                vec![FULL],
                vec![point_range(vec![string("abcd")])],
                None,
            ),
            (
                "multi-valued unique index",
                new_bound_test_table(vec![mv]),
                cols_of(&[COL_NAME]),
                vec![FULL],
                vec![point_range(vec![string("abcd")])],
                None,
            ),
            (
                "composite unique key partially fixed",
                new_bound_test_table(vec![new_bound_test_index(true, &[(0, FULL), (2, FULL)])]),
                cols_of(&[COL_A, COL_NAME]),
                vec![FULL, FULL],
                vec![point_range(int_datums(&[1]))],
                None,
            ),
            (
                "composite unique key fully fixed",
                new_bound_test_table(vec![new_bound_test_index(true, &[(0, FULL), (2, FULL)])]),
                cols_of(&[COL_A, COL_NAME]),
                vec![FULL, FULL],
                vec![point_range(vec![Datum::Int(1), string("x")])],
                Some(1),
            ),
            (
                "fixed prefix column before a fixed unique column",
                new_bound_test_table(Vec::new()),
                cols_of(&[COL_NAME, COL_HANDLE]),
                vec![4, FULL],
                vec![point_range(vec![string("abcd"), Datum::Int(7)])],
                Some(1),
            ),
        ];
        for (name, tbl, cols, lens, ranges, expected) in cases {
            assert_eq!(row_count_upper_bound_by_unique_key(Some(&tbl), &cols, &lens, &ranges), expected, "{name}");
        }
    }
}
