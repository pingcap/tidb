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

//! Ports of `pkg/util/chunk/chunk_util_test.go`.

use std::collections::HashMap;

use tidb_datatype::{CoreTime, Datum, FieldType, FieldTypeCode, Time, TimeType};

use crate::chunk::Chunk;
use crate::chunk_util::{
    copy_selected_join_rows_direct, copy_selected_join_rows_with_same_outer_rows,
    ColumnSwapHelper,
};

const NUM_ROWS: usize = 1024;

fn util_fields() -> Vec<FieldType> {
    vec![
        FieldType::new(FieldTypeCode::VarString),
        FieldType::new(FieldTypeCode::VarString),
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::Datetime),
        FieldType::new(FieldTypeCode::VarString),
    ]
}

fn zero_datetime() -> Time {
    Time::new(CoreTime::default(), TimeType::DateTime, 0).unwrap()
}

/// Go `getChk` (chunk_util_test.go): builds a 6-column source chunk where the
/// third column is NULL on every 7th row and `is_last_3_col_the_same` makes
/// the outer columns identical across all rows. Returns the source chunk and
/// Go's `selected` mask (`j % 7 != 0`).
fn get_chk(is_last_3_col_the_same: bool) -> (Chunk, Vec<bool>) {
    let fields = util_fields();
    let mut src_chk = Chunk::new_with_capacity(&fields, NUM_ROWS);
    let mut selected = vec![false; NUM_ROWS];
    for j in 0..NUM_ROWS {
        let third = if j % 7 == 0 {
            Datum::Null
        } else if is_last_3_col_the_same {
            // Same value as the fixed fourth cell below.
            Datum::Int(123)
        } else {
            Datum::Int(j as i64)
        };
        src_chk.append_string(0, "abc");
        src_chk.append_string(1, "abcdefg");
        src_chk.append_datum(2, &third);
        src_chk.append_int64(3, 123);
        src_chk.append_time(4, zero_datetime());
        src_chk.append_string(5, "abcdefg");
        if j % 7 != 0 {
            selected[j] = true;
        }
    }
    (src_chk, selected)
}

fn append_all_selected(src: &Chunk, selected: &[bool], dst: &mut Chunk) {
    for i in 0..src.num_rows() {
        if !selected[i] {
            continue;
        }
        dst.append_row(src.get_row(i));
    }
}

fn assert_selected_counts(selected: &[bool], dst: &Chunk) {
    let num_selected = selected.iter().filter(|s| **s).count();
    assert_eq!(num_selected, dst.num_virtual_rows());
    assert_eq!(num_selected, dst.num_rows());
}

/// Go `TestCopySelectedJoinRows` (chunk_util_test.go).
#[test]
fn copy_selected_join_rows() {
    let (src_chk, selected) = get_chk(true);
    let fields = util_fields();

    // Row-by-row reference result.
    let mut dst_chk = Chunk::new_with_capacity(&fields, NUM_ROWS);
    append_all_selected(&src_chk, &selected, &mut dst_chk);

    // Batch copy.
    let mut dst_chk2 = Chunk::new_with_capacity(&fields, NUM_ROWS);
    copy_selected_join_rows_with_same_outer_rows(
        &src_chk, 0, 3, 3, 3, &selected, &mut dst_chk2,
    )
    .expect("no selection vectors");

    assert_eq!(dst_chk, dst_chk2);
    assert_selected_counts(&selected, &dst_chk2);
}

/// Go `TestCopySelectedJoinRowsWithoutSameOuters` (chunk_util_test.go): with
/// varying inner values every column is an inner column (offset 0, len 6).
#[test]
fn copy_selected_join_rows_without_same_outers() {
    let (src_chk, selected) = get_chk(false);
    let fields = util_fields();

    let mut dst_chk = Chunk::new_with_capacity(&fields, NUM_ROWS);
    append_all_selected(&src_chk, &selected, &mut dst_chk);

    let mut dst_chk2 = Chunk::new_with_capacity(&fields, NUM_ROWS);
    copy_selected_join_rows_with_same_outer_rows(
        &src_chk, 0, 6, 0, 0, &selected, &mut dst_chk2,
    )
    .expect("no selection vectors");

    assert_eq!(dst_chk, dst_chk2);
    assert_selected_counts(&selected, &dst_chk2);
}

/// Go `TestCopySelectedJoinRowsDirect` (chunk_util_test.go).
#[test]
fn copy_selected_join_rows_direct_matches_row_by_row_copy() {
    let (src_chk, selected) = get_chk(false);
    let fields = util_fields();

    let mut dst_chk = Chunk::new_with_capacity(&fields, NUM_ROWS);
    append_all_selected(&src_chk, &selected, &mut dst_chk);

    let mut dst_chk2 = Chunk::new_with_capacity(&fields, NUM_ROWS);
    copy_selected_join_rows_direct(&src_chk, &selected, &mut dst_chk2)
        .expect("no selection vectors");

    assert_eq!(dst_chk, dst_chk2);
    assert_selected_counts(&selected, &dst_chk2);
}

/// Go `TestCopySelectedVirtualNum` (chunk_util_test.go): zero-column chunks
/// still propagate virtual row counts, and partial copies keep order.
#[test]
fn copy_selected_virtual_num() {
    // srcChk does not contain columns.
    let mut src_chk = Chunk::default();
    src_chk.truncate_to(3);
    let mut dst_chk = Chunk::default();
    let selected = [true, false, true];
    let ok = copy_selected_join_rows_direct(&src_chk, &selected, &mut dst_chk)
        .expect("empty chunks carry no selections");
    assert!(ok);
    assert_eq!(dst_chk.num_virtual_rows(), 2);

    let mut dst_chk = Chunk::default();
    let ok = copy_selected_join_rows_with_same_outer_rows(
        &src_chk, 0, 0, 0, 0, &selected, &mut dst_chk,
    )
    .expect("empty chunks carry no selections");
    assert!(ok);
    assert_eq!(dst_chk.num_virtual_rows(), 2);

    let fields = [FieldType::new(FieldTypeCode::LongLong)];
    let mut src_chk = Chunk::new(&fields, 32, 1024);
    src_chk.truncate_to(0);
    for i in 0..3i64 {
        src_chk.append_int64(0, i);
    }
    let mut dst_chk = Chunk::new_with_capacity(&fields, 0);
    let ok = copy_selected_join_rows_with_same_outer_rows(
        &src_chk, 0, 1, 1, 0, &selected, &mut dst_chk,
    )
    .expect("no selections");
    assert!(ok);
    assert_eq!(dst_chk.num_virtual_rows(), 2);
    assert_eq!(dst_chk.num_rows(), 2);
    assert_eq!(dst_chk.get_row(0).get_int64(0), 0);
    assert_eq!(dst_chk.get_row(1).get_int64(0), 2);

    let mut src_chk = Chunk::new(&fields, 32, 1024);
    src_chk.truncate_to(0);
    for _ in 0..3 {
        src_chk.append_int64(0, 3);
    }
    let mut dst_chk = Chunk::new_with_capacity(&fields, 0);
    let ok = copy_selected_join_rows_with_same_outer_rows(
        &src_chk, 1, 0, 0, 1, &selected, &mut dst_chk,
    )
    .expect("no selections");
    assert!(ok);
    assert_eq!(dst_chk.num_virtual_rows(), 2);
    assert_eq!(dst_chk.num_rows(), 2);
    assert_eq!(dst_chk.get_row(0).get_int64(0), 3);
    assert_eq!(dst_chk.get_row(1).get_int64(0), 3);
}

/// Go `TestMergeInputIdxToOutputIdxes` (chunk_util_test.go): swapping through
/// a projection whose input column fans out to several output slots leaves
/// every output slot sharing one identity, carrying the input value. The
/// merged-mapping cache itself is internal to this port and is pinned by the
/// identity contract tests.
#[test]
fn merge_input_idx_to_output_idxes() {
    let mut input_idx_to_output_idxes = HashMap::new();
    input_idx_to_output_idxes.insert(0usize, vec![0usize, 1]);
    input_idx_to_output_idxes.insert(1usize, vec![2, 3]);
    let column_eval = ColumnSwapHelper::from_mapping(input_idx_to_output_idxes);

    let longlong = FieldType::new(FieldTypeCode::LongLong);
    let input_fields = vec![longlong.clone(), longlong.clone()];
    let mut input = Chunk::new_empty(&input_fields);
    input.append_int64(0, 99);
    // Input chunk's 0th and 1st columns refer to the same owner.
    input.make_ref(0, 1);

    let output_fields = vec![
        longlong.clone(),
        longlong.clone(),
        longlong.clone(),
        longlong,
    ];
    let mut output = Chunk::new_empty(&output_fields);

    column_eval
        .swap_columns(&mut input, &mut output)
        .expect("no selections anywhere");

    // All four output columns are column-referred, pointing at the first one.
    assert_eq!(output.column(0), output.column(1));
    assert_eq!(output.column(1), output.column(2));
    assert_eq!(output.column(2), output.column(3));
    assert_eq!(output.get_row(0).get_int64(0), 99);
}
