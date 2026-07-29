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

//! `pkg/util/chunk/chunk_util.go`: the bulk row copies the join executors use
//! to move a selected subset of a probe-result chunk into the output chunk
//! without going cell by cell.
//!
//! SEED SCOPE: the in-memory copies (`CopySelectedJoinRowsDirect`,
//! `CopySelectedJoinRowsWithSameOuterRows`, `CopySelectedRows`, `CopyRows`).
//! DEFERRED: the spill-to-disk reader/writer half of the Go file
//! (`diskFileReaderWriter` and friends), which needs the checksum/encrypt
//! packages.

use crate::chunk::Chunk;
use crate::column::Column;

/// Go `msgErrSelNotNil`: a bulk copy refuses chunks carrying a selection
/// vector, because the copy walks physical rows.
pub const MSG_ERR_SEL_NOT_NIL: &str =
    "The chunk with sel isn't supported, please use the chunk without sel";

/// Go `CopySelectedRows`: append every `selected` row of `src` to `dst`.
pub fn copy_selected_rows(dst: &mut Column, src: &Column, selected: &[bool]) {
    dst.copy_expected_rows_with_row_id_func(src, selected, true, 0, selected.len(), |i| i);
}

/// Go `CopySelectedRowsWithRowIDFunc`.
pub fn copy_selected_rows_with_row_id_func(
    dst: &mut Column,
    src: &Column,
    selected: &[bool],
    start: usize,
    end: usize,
    row_id_fn: impl Fn(usize) -> usize,
) {
    dst.copy_expected_rows_with_row_id_func(src, selected, true, start, end, row_id_fn);
}

/// Go `CopyExpectedRowsWithRowIDFunc`.
pub fn copy_expected_rows_with_row_id_func(
    dst: &mut Column,
    src: &Column,
    selected: &[bool],
    expected_result: bool,
    start: usize,
    end: usize,
    row_id_fn: impl Fn(usize) -> usize,
) {
    dst.copy_expected_rows_with_row_id_func(src, selected, expected_result, start, end, row_id_fn);
}

/// Go `CopyRows`: append the `src` rows named by `selected` (row ids) to `dst`.
pub fn copy_rows(dst: &mut Column, src: &Column, selected: &[usize]) {
    dst.copy_rows_from(src, selected);
}

/// Go `CopySelectedJoinRowsDirect`: copy the selected joined rows of `src`
/// straight into `dst`. Returns whether at least one row was selected.
///
/// # Errors
/// Returns [`MSG_ERR_SEL_NOT_NIL`] when either chunk carries a selection vector.
pub fn copy_selected_join_rows_direct(
    src: &Chunk,
    selected: &[bool],
    dst: &mut Chunk,
) -> Result<bool, &'static str> {
    if src.num_rows() == 0 {
        return Ok(false);
    }
    if src.has_sel() || dst.has_sel() {
        return Err(MSG_ERR_SEL_NOT_NIL);
    }
    if src.num_cols() == 0 {
        let num_selected = selected.iter().filter(|s| **s).count();
        dst.add_virtual_rows(num_selected);
        return Ok(num_selected > 0);
    }

    let old_len = dst.column(0).length();
    for j in 0..src.num_cols() {
        copy_selected_rows(dst.column_mut(j), src.column(j), selected);
    }
    let num_selected = dst.column(0).length() - old_len;
    dst.add_virtual_rows(num_selected);
    Ok(num_selected > 0)
}

/// Go `CopySelectedJoinRowsWithSameOuterRows`: copy the selected joined rows of
/// `src` into `dst`, where every outer row in `src` is known to be identical so
/// the outer columns can be blitted as one repeated block.
///
/// # Errors
/// Returns [`MSG_ERR_SEL_NOT_NIL`] when either chunk carries a selection vector.
pub fn copy_selected_join_rows_with_same_outer_rows(
    src: &Chunk,
    inner_col_offset: usize,
    inner_col_len: usize,
    outer_col_offset: usize,
    outer_col_len: usize,
    selected: &[bool],
    dst: &mut Chunk,
) -> Result<bool, &'static str> {
    if src.num_rows() == 0 {
        return Ok(false);
    }
    if src.has_sel() || dst.has_sel() {
        return Err(MSG_ERR_SEL_NOT_NIL);
    }

    let num_selected =
        copy_selected_inner_rows(inner_col_offset, inner_col_len, src, selected, dst);
    copy_same_outer_rows(outer_col_offset, outer_col_len, src, num_selected, dst);
    dst.add_virtual_rows(num_selected);
    Ok(num_selected > 0)
}

/// Go `copySelectedInnerRows`.
fn copy_selected_inner_rows(
    inner_col_offset: usize,
    inner_col_len: usize,
    src: &Chunk,
    selected: &[bool],
    dst: &mut Chunk,
) -> usize {
    if inner_col_len == 0 {
        return selected.iter().filter(|s| **s).count();
    }
    let old_len = dst.column(inner_col_offset).length();
    for j in 0..inner_col_len {
        copy_selected_rows(
            dst.column_mut(inner_col_offset + j),
            src.column(inner_col_offset + j),
            selected,
        );
    }
    dst.column(inner_col_offset).length() - old_len
}

/// Go `copySameOuterRows`.
fn copy_same_outer_rows(
    outer_col_offset: usize,
    outer_col_len: usize,
    src: &Chunk,
    num_rows: usize,
    dst: &mut Chunk,
) {
    if num_rows == 0 || outer_col_len == 0 {
        return;
    }
    // Go reads `src.GetRow(0)`; `src` is known to carry no selection here, so
    // the logical and physical index of row 0 coincide.
    let row_idx = 0;
    for i in 0..outer_col_len {
        dst.column_mut(outer_col_offset + i).copy_same_rows_from(
            src.column(outer_col_offset + i),
            row_idx,
            num_rows,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{CoreTime, FieldType, FieldTypeCode, Time, TimeType};

    /// Go `getChk`'s chunk shape: `newChunkWithInitCap(numRows, 0, 0, 8, 8,
    /// sizeTime, 0)` -- two var-length columns, two 8-byte columns, one Time
    /// column, one var-length column.
    fn chk_fields() -> Vec<FieldType> {
        vec![
            FieldType::new(FieldTypeCode::VarString),
            FieldType::new(FieldTypeCode::VarString),
            FieldType::new(FieldTypeCode::LongLong),
            FieldType::new(FieldTypeCode::LongLong),
            FieldType::new(FieldTypeCode::Datetime),
            FieldType::new(FieldTypeCode::VarString),
        ]
    }

    /// Go `types.ZeroDatetime`.
    fn zero_datetime() -> Time {
        Time::new(
            CoreTime::from_date(0, 0, 0, 0, 0, 0, 0),
            TimeType::DateTime,
            0,
        )
        .expect("the zero datetime is representable")
    }

    /// Stands in for Go's `rand.Int()` in `getChk(false)`, whose only job is to
    /// make the outer rows differ from each other. A fixed sequence keeps the
    /// test deterministic.
    fn pseudo_random(mut state: u64) -> i64 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        (state >> 1) as i64
    }

    /// Go `getChk` (`pkg/util/chunk/chunk_util_test.go:28`): 1024 rows where
    /// every 7th row is deselected and carries a NULL in column 2.
    /// `is_last3_col_the_same` reproduces Go's flag: when set, the trailing
    /// outer columns hold identical values in every row, which is the
    /// precondition `CopySelectedJoinRowsWithSameOuterRows` relies on.
    fn get_chk(is_last3_col_the_same: bool) -> (Chunk, Chunk, Vec<bool>) {
        let num_rows = 1024;
        let fields = chk_fields();
        let mut src_chk = Chunk::new_with_capacity(&fields, num_rows);
        // Go sets `selected[j] = true` for every row it does not make the
        // "7th row" special case.
        let selected: Vec<bool> = (0..num_rows).map(|j| j % 7 != 0).collect();
        for j in 0..num_rows {
            if is_last3_col_the_same {
                src_chk.append_string(0, "abc");
                src_chk.append_string(1, "abcdefg");
                if j % 7 == 0 {
                    src_chk.append_null(2);
                } else {
                    src_chk.append_int64(2, j as i64);
                }
                src_chk.append_int64(3, 123);
            } else if j % 7 == 0 {
                src_chk.append_string(0, "abc");
                src_chk.append_string(1, "abcdefg");
                src_chk.append_null(2);
                src_chk.append_int64(3, pseudo_random(j as u64 + 1));
            } else {
                src_chk.append_string(0, "aabc");
                src_chk.append_string(1, "ab234fg");
                src_chk.append_int64(2, j as i64);
                src_chk.append_int64(3, 123);
            }
            src_chk.append_time(4, zero_datetime());
            src_chk.append_string(5, "abcdefg");
        }
        // Go builds `srcChk` with `AppendPartialRow(0, row)`, which does not
        // bump `numVirtualRows`; the cell-by-cell appends above match that.
        let dst_chk = Chunk::new_with_capacity(&fields, num_rows);
        (src_chk, dst_chk, selected)
    }

    /// Builds the reference chunk the Go tests compare against: the same rows
    /// appended one at a time through `AppendRow`.
    fn append_row_by_row(src_chk: &Chunk, selected: &[bool], dst_chk: &mut Chunk) {
        for (i, sel) in selected.iter().enumerate().take(src_chk.num_rows()) {
            if *sel {
                dst_chk.append_row(src_chk.get_row(i));
            }
        }
    }

    /// Go `TestCopySelectedJoinRows` (`chunk_util_test.go:57`).
    #[test]
    fn copy_selected_join_rows_matches_row_by_row_append() {
        let (src_chk, mut dst_chk, selected) = get_chk(true);
        let num_rows = src_chk.num_rows();
        append_row_by_row(&src_chk, &selected, &mut dst_chk);

        // batch copy
        let mut dst_chk2 = Chunk::new_with_capacity(&chk_fields(), num_rows);
        copy_selected_join_rows_with_same_outer_rows(
            &src_chk,
            0,
            3,
            3,
            3,
            &selected,
            &mut dst_chk2,
        )
        .expect("neither chunk carries a selection");

        assert_eq!(dst_chk, dst_chk2);
        let num_selected = selected.iter().filter(|s| **s).count();
        assert_eq!(num_selected, dst_chk2.num_virtual_rows());
        assert_eq!(num_selected, dst_chk2.num_rows());
    }

    /// Go `TestCopySelectedJoinRowsWithoutSameOuters` (`chunk_util_test.go:82`):
    /// the whole row range is treated as inner, so nothing is blitted.
    #[test]
    fn copy_selected_join_rows_without_same_outers() {
        let (src_chk, mut dst_chk, selected) = get_chk(false);
        let num_rows = src_chk.num_rows();
        append_row_by_row(&src_chk, &selected, &mut dst_chk);

        let mut dst_chk2 = Chunk::new_with_capacity(&chk_fields(), num_rows);
        copy_selected_join_rows_with_same_outer_rows(
            &src_chk,
            0,
            6,
            0,
            0,
            &selected,
            &mut dst_chk2,
        )
        .expect("neither chunk carries a selection");

        assert_eq!(dst_chk, dst_chk2);
        let num_selected = selected.iter().filter(|s| **s).count();
        assert_eq!(num_selected, dst_chk2.num_virtual_rows());
        assert_eq!(num_selected, dst_chk2.num_rows());
    }

    /// Go `TestCopySelectedJoinRowsDirect` (`chunk_util_test.go:107`).
    #[test]
    fn copy_selected_join_rows_direct_matches_row_by_row_append() {
        let (src_chk, mut dst_chk, selected) = get_chk(false);
        let num_rows = src_chk.num_rows();
        append_row_by_row(&src_chk, &selected, &mut dst_chk);

        let mut dst_chk2 = Chunk::new_with_capacity(&chk_fields(), num_rows);
        copy_selected_join_rows_direct(&src_chk, &selected, &mut dst_chk2)
            .expect("neither chunk carries a selection");

        assert_eq!(dst_chk, dst_chk2);
        let num_selected = selected.iter().filter(|s| **s).count();
        assert_eq!(num_selected, dst_chk2.num_virtual_rows());
        assert_eq!(num_selected, dst_chk2.num_rows());
    }

    /// Go `TestCopySelectedVirtualNum` (`chunk_util_test.go:136`), the
    /// column-less branch of both entry points.
    #[test]
    fn copy_selected_virtual_num() {
        // srcChk does not contain columns
        let mut src_chk = Chunk::new_with_capacity(&[], 0);
        src_chk.set_num_virtual_rows(3);
        let mut dst_chk = Chunk::new_with_capacity(&[], 0);
        let selected = vec![true, false, true];

        let ok = copy_selected_join_rows_direct(&src_chk, &selected, &mut dst_chk)
            .expect("no selection vector");
        assert!(ok);
        assert_eq!(dst_chk.num_virtual_rows(), 2);

        let mut dst_chk2 = Chunk::new_with_capacity(&[], 0);
        let ok = copy_selected_join_rows_with_same_outer_rows(
            &src_chk,
            0,
            0,
            0,
            0,
            &selected,
            &mut dst_chk2,
        )
        .expect("no selection vector");
        assert!(ok);
        assert_eq!(dst_chk2.num_virtual_rows(), 2);
    }
}
