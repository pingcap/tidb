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

use super::*;
use crate::chunk::Chunk;
use crate::column::Column;
use crate::iterator::{ChunkIterator, Iterator4Chunk};
use tidb_datatype::Decimal;

/// Stands in for Go's `rand` in the reconstruct tests: those tests draw a
/// fresh random selection and null pattern on every run, so a fixed
/// generator run over several seeds keeps the same coverage while staying
/// reproducible when it fails.
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    /// Go `rand.Intn(10)`.
    fn intn10(&mut self) -> u64 {
        self.next_u64() % 10
    }

    /// Go `rand.Int63()`.
    fn int63(&mut self) -> i64 {
        (self.next_u64() >> 1) as i64
    }
}

/// Go `TestReconstructFixedLen` (`pkg/util/chunk/column_test.go:432`).
#[test]
fn reconstruct_fixed_len() {
    for seed in 1..=8u64 {
        let mut rng = Rng(seed);
        let mut col = Column::new_column(
            &FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            1024,
        );
        let mut results: Vec<i64> = Vec::with_capacity(1024);
        let mut nulls: Vec<bool> = Vec::with_capacity(1024);
        let mut sel: Vec<usize> = Vec::with_capacity(1024);
        for i in 0..1024 {
            if rng.intn10() < 6 {
                sel.push(i);
            }
            if rng.intn10() < 2 {
                col.append_null();
                nulls.push(true);
                results.push(0);
                continue;
            }
            let v = rng.int63();
            col.append_int64(v);
            results.push(v);
            nulls.push(false);
        }

        col.reconstruct(&sel);
        let mut null_cnt = 0;
        for (n, &i) in sel.iter().enumerate() {
            if nulls[i] {
                null_cnt += 1;
                assert!(col.is_null(n), "seed {seed}: row {n} should be null");
            } else {
                assert_eq!(results[i], col.get_int64(n), "seed {seed}: row {n}");
            }
        }
        assert_eq!(col.null_count(), null_cnt);
        assert_eq!(sel.len(), col.length);

        for i in 0..128i64 {
            if i % 2 == 0 {
                col.append_null();
            } else {
                col.append_int64(i * i * i);
            }
        }

        assert_eq!(sel.len(), col.length - 128);
        assert_eq!(null_cnt + 128 / 2, col.null_count());
        for i in 0..128usize {
            if i % 2 == 0 {
                assert!(col.is_null(sel.len() + i));
            } else {
                let v = i as i64;
                assert_eq!(v * v * v, col.get_int64(sel.len() + i));
                assert!(!col.is_null(sel.len() + i));
            }
        }
    }
}

/// Go `TestReconstructVarLen` (`pkg/util/chunk/column_test.go:488`).
#[test]
fn reconstruct_var_len() {
    for seed in 1..=8u64 {
        let mut rng = Rng(seed);
        let mut col = Column::new_column(
            &FieldType::new(tidb_datatype::FieldTypeCode::VarString),
            1024,
        );
        let mut results: Vec<String> = Vec::with_capacity(1024);
        let mut nulls: Vec<bool> = Vec::with_capacity(1024);
        let mut sel: Vec<usize> = Vec::with_capacity(1024);
        for i in 0..1024 {
            if rng.intn10() < 6 {
                sel.push(i);
            }
            if rng.intn10() < 2 {
                col.append_null();
                nulls.push(true);
                results.push(String::new());
                continue;
            }
            let v = rng.int63().to_string();
            col.append_string(&v);
            results.push(v);
            nulls.push(false);
        }

        col.reconstruct(&sel);
        let mut null_cnt = 0;
        for (n, &i) in sel.iter().enumerate() {
            if nulls[i] {
                null_cnt += 1;
                assert!(col.is_null(n), "seed {seed}: row {n} should be null");
            } else {
                assert_eq!(
                    col.get_bytes(n).as_ref(),
                    results[i].as_bytes(),
                    "seed {seed}: row {n}"
                );
            }
        }
        assert_eq!(col.null_count(), null_cnt);
        assert_eq!(sel.len(), col.length);

        for i in 0..128usize {
            if i % 2 == 0 {
                col.append_null();
            } else {
                col.append_string((i * i * i).to_string());
            }
        }

        assert_eq!(sel.len(), col.length - 128);
        assert_eq!(null_cnt + 128 / 2, col.null_count());
        for i in 0..128usize {
            if i % 2 == 0 {
                assert!(col.is_null(sel.len() + i));
            } else {
                assert_eq!(
                    col.get_bytes(sel.len() + i).as_ref(),
                    (i * i * i).to_string().as_bytes()
                );
                assert!(!col.is_null(sel.len() + i));
            }
        }
    }
}

#[test]
fn fixed_int64_append_get_null() {
    let mut c = Column::new_fixed_len(8, 4);
    assert!(c.is_fixed());
    assert_eq!(c.type_size(), 8);
    c.append_int64(10);
    c.append_null();
    c.append_int64(-3);
    assert_eq!(c.rows(), 3);
    assert_eq!(c.get_int64(0), 10);
    assert!(!c.is_null(0));
    assert!(c.is_null(1));
    assert!(!c.is_null(2));
    assert_eq!(c.get_int64(2), -3);
}

#[test]
fn null_bitmap_spans_multiple_bytes() {
    let mut c = Column::new_fixed_len(8, 16);
    for i in 0..10 {
        if i % 2 == 0 {
            c.append_int64(i);
        } else {
            c.append_null();
        }
    }
    assert_eq!(c.rows(), 10);
    for i in 0..10 {
        assert_eq!(c.is_null(i as usize), i % 2 != 0, "row {i}");
    }
}

#[test]
fn float_and_uint_roundtrip() {
    let mut f = Column::new_fixed_len(8, 2);
    f.append_float64(3.5);
    f.append_float64(-1.25);
    assert_eq!(f.get_float64(0), 3.5);
    assert_eq!(f.get_float64(1), -1.25);

    let mut f32c = Column::new_fixed_len(4, 1);
    f32c.append_float32(2.5);
    assert_eq!(f32c.get_float32(0), 2.5);

    let mut u = Column::new_fixed_len(8, 1);
    u.append_uint64(u64::MAX);
    assert_eq!(u.get_uint64(0), u64::MAX);
}

#[test]
fn reset_clears_rows_keeps_kind() {
    let mut c = Column::new_fixed_len(8, 2);
    c.append_int64(7);
    c.reset();
    assert_eq!(c.rows(), 0);
    assert!(c.is_fixed());
    c.append_int64(9);
    assert_eq!(c.get_int64(0), 9);
}

#[test]
fn var_len_column_shape() {
    let c = Column::new_var_len(4);
    assert!(!c.is_fixed());
    assert_eq!(c.type_size(), VAR_ELEM_LEN);
}

#[test]
fn var_len_append_get_string_bytes_null() {
    let mut c = Column::new_var_len(4);
    c.append_string("hello");
    c.append_null();
    c.append_bytes(&[0x00, 0xff, 0x10]); // non-UTF8 binary
    c.append_string("");
    assert_eq!(c.rows(), 4);
    assert_eq!(c.get_bytes(0), b"hello");
    assert!(!c.is_null(0));
    // Null row has zero width and is flagged null.
    assert!(c.is_null(1));
    assert_eq!(c.get_bytes(1), b"");
    assert_eq!(c.get_bytes(2), &[0x00, 0xff, 0x10]);
    assert_eq!(c.get_raw(2), &[0x00, 0xff, 0x10]);
    assert_eq!(c.get_bytes(3), b"");
    assert!(!c.is_null(3)); // empty string is NOT null
}

/// The append side must produce the exact cell bytes Go's
/// `Column.AppendEnum`/`AppendSet` produce, because the chunk-codec
/// decoder (`tidb-codec`'s `decode_column_datums`) reads that layout: an
/// 8-byte native-endian value followed by the element name.
///
/// Captured from a real TiDB via a throwaway
/// `TestZZDumpTablesPriv` (`go test -tags=intest ./pkg/executor/`), which
/// printed `chunk.NewColumn(...).GetRaw(0)` for both types.
#[test]
fn enum_and_set_cells_are_the_bytes_go_writes() {
    use tidb_datatype::FieldTypeCode;
    let mut enums = Column::new_column(&FieldType::new(FieldTypeCode::Enum), 4);
    enums.append_enum(&MysqlEnum::new("bb", 2));
    assert_eq!(
        enums.get_raw(0),
        &[0x02, 0, 0, 0, 0, 0, 0, 0, b'b', b'b'],
        "Go printed: 02 00 00 00 00 00 00 00 62 62"
    );

    // `mysql.tables_priv`.`Table_priv` spells GRANT OPTION `Grant`, and its
    // element list puts it at bit 6, so `Select,Grant` is 1|64 = 0x41.
    let mut sets = Column::new_column(&FieldType::new(FieldTypeCode::Set), 4);
    sets.append_set(&MysqlSet::new("Select,Grant", 1 | 64));
    let mut expected = vec![0x41, 0, 0, 0, 0, 0, 0, 0];
    expected.extend_from_slice(b"Select,Grant");
    assert_eq!(sets.get_raw(0), expected.as_slice());
}

#[test]
fn enum_and_set_cells_round_trip_including_the_empty_and_null_ones() {
    use tidb_datatype::FieldTypeCode;
    let mut c = Column::new_column(&FieldType::new(FieldTypeCode::Set), 4);
    // Go's `getNameValue` answers the zero pair for a zero-width cell, and
    // an empty SET (`Value == 0`) is written with no name -- but Go still
    // writes the 8-byte prefix, so the cell is 8 bytes, not empty.
    c.append_set(&MysqlSet::new("", 0));
    c.append_null();
    c.append_set(&MysqlSet::new("Select,Update", 1 | 4));
    assert_eq!(c.get_set(0), MysqlSet::new("", 0));
    assert_eq!(c.get_raw(0).len(), 8);
    assert!(c.is_null(1));
    // A null cell is zero-width, which is exactly the case Go's
    // `getNameValue` short-circuits.
    assert_eq!(c.get_name_value(1), (GoString::default(), 0));
    assert_eq!(c.get_set(2), MysqlSet::new("Select,Update", 5));

    let mut e = Column::new_column(&FieldType::new(FieldTypeCode::Enum), 2);
    e.append_enum(&MysqlEnum::new("N", 1));
    e.append_enum(&MysqlEnum::new("Y", 2));
    assert_eq!(e.get_enum(0), MysqlEnum::new("N", 1));
    assert_eq!(e.get_enum(1), MysqlEnum::new("Y", 2));

    let mut raw = Column::new_column(&FieldType::new(FieldTypeCode::Enum), 2);
    raw.append_enum(&MysqlEnum::new(vec![0xff], 1));
    raw.append_set(&MysqlSet::new(vec![0xfe], 2));
    assert_eq!(raw.get_enum(0).name_bytes(), &[0xff]);
    assert_eq!(raw.get_set(1).name_bytes(), &[0xfe]);
}

#[test]
fn get_raw_fixed_and_var() {
    let mut f = Column::new_fixed_len(8, 1);
    f.append_int64(0x0102_0304_0506_0708);
    assert_eq!(f.get_raw(0), &0x0102_0304_0506_0708i64.to_ne_bytes());

    let mut v = Column::new_var_len(1);
    v.append_bytes(b"abc");
    assert_eq!(v.get_raw(0), b"abc");
}

#[test]
fn copy_construct_is_deep() {
    let mut c = Column::new_fixed_len(8, 2);
    c.append_int64(42);
    let d = c.copy_construct();
    assert_eq!(d.rows(), 1);
    assert_eq!(d.get_int64(0), 42);
}

#[test]
fn time_append_get_null_roundtrip() {
    use tidb_datatype::{CoreTime, TimeType};
    let mut c = Column::new_column(&FieldType::new(FieldTypeCode::Datetime), 4);
    assert_eq!(c.type_size(), SIZE_TIME);
    let dt = Time::new(
        CoreTime::from_date(2026, 7, 25, 12, 34, 56, 654_321),
        TimeType::DateTime,
        6,
    )
    .unwrap();
    let ts = Time::new(
        CoreTime::from_date(1999, 12, 31, 23, 59, 59, 0),
        TimeType::Timestamp,
        0,
    )
    .unwrap();
    let date = Time::new(
        CoreTime::from_date(2000, 2, 29, 0, 0, 0, 0),
        TimeType::Date,
        0,
    )
    .unwrap();
    c.append_time(dt);
    c.append_null();
    c.append_time(ts);
    c.append_time(date);
    assert_eq!(c.rows(), 4);
    assert_eq!(c.get_time(0), dt);
    assert!(c.is_null(1));
    assert!(!c.is_null(2));
    assert_eq!(c.get_time(2), ts);
    assert_eq!(c.get_time(3), date);
    // The stored bytes are exactly Go's packed uint64 (native-endian).
    assert_eq!(c.get_raw(0), &dt.go_raw().to_ne_bytes());
}

#[test]
fn duration_append_get_null_roundtrip() {
    let mut c = Column::new_column(&FieldType::new(FieldTypeCode::Duration), 4);
    assert_eq!(c.type_size(), 8);
    let d = MySqlDuration::new(11, 22, 33, 456_789, 6).unwrap();
    let neg = d.negated();
    c.append_duration(d);
    c.append_null();
    c.append_duration(neg);
    assert_eq!(c.rows(), 3);
    // Append ignores fsp; the reader supplies it (Go GetDuration fillFsp).
    assert_eq!(c.get_duration(0, 6), d);
    assert_eq!(c.get_duration(0, 3).nanoseconds(), d.nanoseconds());
    assert_eq!(c.get_duration(0, 3).fsp(), 3);
    assert!(c.is_null(1));
    assert_eq!(c.get_duration(2, 6), neg);
    // Stored as Go's int64 nanoseconds.
    assert_eq!(c.get_int64(0), d.nanoseconds());
}

#[test]
fn fixed_len_type_dispatch() {
    use tidb_datatype::FieldTypeCode;
    let ft = |c| FieldType::new(c);
    assert_eq!(get_fixed_len(&ft(FieldTypeCode::Float)), 4);
    assert_eq!(get_fixed_len(&ft(FieldTypeCode::Long)), 8);
    assert_eq!(get_fixed_len(&ft(FieldTypeCode::LongLong)), 8);
    assert_eq!(get_fixed_len(&ft(FieldTypeCode::Double)), 8);
    assert_eq!(get_fixed_len(&ft(FieldTypeCode::Duration)), 8);
    assert_eq!(get_fixed_len(&ft(FieldTypeCode::Datetime)), SIZE_TIME);
    assert_eq!(
        get_fixed_len(&ft(FieldTypeCode::NewDecimal)),
        MY_DECIMAL_STRUCT_SIZE
    );
    assert_eq!(get_fixed_len(&ft(FieldTypeCode::VarString)), VAR_ELEM_LEN);
    assert_eq!(get_fixed_len(&ft(FieldTypeCode::Blob)), VAR_ELEM_LEN);
}

#[test]
fn new_column_from_field_type() {
    use tidb_datatype::FieldTypeCode;
    let mut int_col = Column::new_column(&FieldType::new(FieldTypeCode::Long), 4);
    assert!(int_col.is_fixed());
    assert_eq!(int_col.type_size(), 8);
    int_col.append_int64(5);
    assert_eq!(int_col.get_int64(0), 5);

    let mut str_col = Column::new_column(&FieldType::new(FieldTypeCode::VarString), 4);
    assert!(!str_col.is_fixed());
    str_col.append_string("x");
    assert_eq!(str_col.get_bytes(0), b"x");

    let empty_fixed = Column::new_empty_column(&FieldType::new(FieldTypeCode::Float));
    assert!(empty_fixed.is_fixed());
    assert_eq!(empty_fixed.type_size(), 4);
    let empty_var = Column::new_empty_column(&FieldType::new(FieldTypeCode::Blob));
    assert!(!empty_var.is_fixed());
}

#[test]
fn copy_construct_owns_buffers_and_clears_zero_copy_reuse_guard() {
    let mut source = Column::new_var_len(2);
    source.append_string("owned value");
    source.avoid_reusing = true;

    let copied = source.copy_construct();
    assert_eq!(copied.get_bytes(0), b"owned value");
    assert!(!copied.avoid_reusing);
    assert!(source.avoid_reusing);
}

#[test]
fn resize_reserve_and_eval_type_reset_match_go_shapes() {
    let mut column = Column::new_column(&FieldType::new(FieldTypeCode::LongLong), 2);
    column.resize_int64(4, false);
    assert_eq!(column.rows(), 4);
    assert_eq!(column.null_bitmap, vec![0x0f]);
    assert_eq!(column.data.len(), 32);
    assert!(column.data.read().iter().all(|byte| *byte == 0));

    column.resize_uint64(11, false);
    assert_eq!(column.null_bitmap, vec![0xff, 0x07]);
    column.resize_uint64(7, true);
    assert_eq!(column.null_bitmap, vec![0]);

    column.reset_for_eval_type(EvalType::Duration);
    assert!(column.is_fixed());
    assert_eq!(column.type_size(), 8);
    assert_eq!(column.rows(), 0);
    column.append_duration(MySqlDuration::from_nanoseconds(7, 0).unwrap());
    assert_eq!(column.data.len(), 8);

    column.reset_for_eval_type(EvalType::String);
    assert!(!column.is_fixed());
    assert_eq!(column.offsets, vec![0]);
    column.append_string("x");
    assert_eq!(column.get_bytes(0), b"x");
}

/// Go `Column.resize` re-slices rather than recreating the append scratch
/// and leaves the unrelated offsets slice alone.  Both details are
/// observable after an evaluation-type transition.
#[test]
fn resize_preserves_scratch_and_offset_headers() {
    let mut fixed = Column::new_fixed_len(8, 2);
    let scratch = 0x0102_0304_0506_0708_i64;
    fixed.append_int64(scratch);
    fixed.resize_int64(0, false);
    fixed.append_null();
    assert!(fixed.is_null(0));
    assert_eq!(fixed.get_raw(0), &scratch.to_ne_bytes());

    let mut changing = Column::new_var_len(2);
    changing.offsets = vec![7, 9];
    changing.resize_int64(0, false);
    assert_eq!(changing.offsets, vec![7, 9]);
    changing.reserve_string(1);
    assert_eq!(changing.offsets, vec![7]);
}

#[test]
fn reserve_preserves_content_and_typed_reserve_clears_rows() {
    let mut column = Column::new_var_len(0);
    column.append_string("alpha");
    let bitmap = column.null_bitmap.clone();
    let offsets = column.offsets.clone();
    let data = column.data.clone();
    column.reserve(10, 10, 10);
    assert_eq!(column.null_bitmap, bitmap);
    assert_eq!(column.offsets, offsets);
    assert_eq!(column.data, data);
    assert!(column.null_bitmap.capacity() >= bitmap.len() + 10);
    assert!(column.offsets.capacity() >= offsets.len() + 10);
    assert!(column.data.capacity() >= data.len() + 10);

    column.reserve_string_with_size_hint(9, 36);
    assert_eq!(column.rows(), 0);
    assert_eq!(column.offsets, vec![0]);
    assert!(column.data_capacity() >= 9 * 36);
    assert!(column.offset_capacity() >= 10);
    assert!(column.null_bitmap_capacity() >= 2);
}

#[test]
fn append_cell_n_times_and_copy_reconstruct_cover_fixed_var_and_null() {
    let mut fixed = Column::new_fixed_len(8, 4);
    fixed.append_int64(11);
    fixed.append_null();
    fixed.append_int64(33);

    let mut fixed_copy = Column::new_fixed_len(8, 0);
    fixed_copy.append_cell_n_times(&fixed, 0, 3);
    fixed_copy.append_cell_n_times(&fixed, 1, 2);
    assert_eq!(fixed_copy.rows(), 5);
    assert_eq!(fixed_copy.get_int64(0), 11);
    assert_eq!(fixed_copy.get_int64(2), 11);
    assert!(fixed_copy.is_null(3));
    assert!(fixed_copy.is_null(4));

    let mut variable = Column::new_var_len(4);
    variable.append_string("a");
    variable.append_null();
    variable.append_string("ccc");
    let selected = variable.copy_reconstruct(Some(&[2, 0, 1]), None);
    assert_eq!(selected.get_bytes(0), b"ccc");
    assert_eq!(selected.get_bytes(1), b"a");
    assert!(selected.is_null(2));
    assert_eq!(selected.offsets, vec![0, 3, 4, 4]);
}

#[test]
fn copy_reconstruct_reuses_destination_and_preserves_avoid_reusing() {
    let mut fixed = Column::new_fixed_len(8, 3);
    fixed.append_int64(10);
    fixed.append_int64(20);
    fixed.append_int64(30);

    let mut borrowed = fixed.clone();
    borrowed.avoid_reusing = true;
    let owned = borrowed.copy_construct();
    assert!(borrowed.avoid_reusing);
    assert!(!owned.avoid_reusing);
    assert_eq!(owned.data, borrowed.data);

    let mut destination = Column::new_var_len(16);
    destination.append_string("old");
    destination.avoid_reusing = true;
    let original_data_capacity = destination.data_capacity();
    let full = fixed.copy_reconstruct(None, Some(destination));
    assert!(full.avoid_reusing);
    assert_eq!(full.rows(), 3);
    assert_eq!(full.get_int64(1), 20);
    assert!(full.data_capacity() >= original_data_capacity);
    assert!(full.offsets.is_empty());

    let mut destination = Column::new_var_len(8);
    destination.avoid_reusing = true;
    let selected = fixed.copy_reconstruct(Some(&[2, 0]), Some(destination));
    assert!(selected.avoid_reusing);
    assert_eq!(selected.rows(), 2);
    assert_eq!(selected.get_int64(0), 30);
    assert_eq!(selected.get_int64(1), 10);
    // Go's selected fixed reconstruction leaves the former var-len
    // destination's leading offset header in place.
    assert_eq!(selected.offsets, vec![0]);
}

#[test]
fn direct_string_json_and_raw_join_cell_surfaces_match_go() {
    let mut strings = Column::new_var_len(2);
    strings.append_string("hello");
    strings.append_bytes(&[0, 255]);
    assert_eq!(strings.get_string(0).as_bytes(), b"hello");
    assert_eq!(strings.get_string(1).as_bytes(), &[0, 255]);

    let json = tidb_datatype::BinaryJSON::parse(r#"{"a": 1}"#).expect("JSON");
    let mut json_column = Column::new_var_len(1);
    json_column.append_json(&json);
    assert_eq!(json_column.get_json(0), json);

    let mut fixed = Column::new_fixed_len(8, 1);
    fixed.append_null_bitmap(true);
    let mut fixed_stream = vec![9, 8, 7];
    fixed_stream.extend_from_slice(&123_i64.to_ne_bytes());
    assert_eq!(append_cell_from_raw_data(&mut fixed, &fixed_stream, 3), 11);
    assert_eq!(fixed.get_int64(0), 123);

    let mut variable = Column::new_var_len(1);
    variable.append_null_bitmap(true);
    let mut variable_stream = vec![9, 8, 7];
    variable_stream.extend_from_slice(&5_u32.to_ne_bytes());
    variable_stream.extend_from_slice(b"hello");
    assert_eq!(
        append_cell_from_raw_data(&mut variable, &variable_stream, 3),
        12
    );
    assert_eq!(variable.get_bytes(0), b"hello");
}

#[test]
fn set_null_ranges_and_merge_nulls_match_rowwise_and() {
    let mut left = Column::new_fixed_len(8, 16);
    let mut right = Column::new_fixed_len(8, 16);
    let mut result = Column::new_fixed_len(8, 16);
    left.resize_int64(11, false);
    right.resize_int64(11, false);
    result.resize_int64(11, false);
    left.set_nulls(1, 4, true);
    right.set_nulls(3, 8, true);
    result.merge_nulls(&[&left, &right]);
    for row in 0..11 {
        assert_eq!(
            result.is_null(row),
            left.is_null(row) || right.is_null(row),
            "row {row}"
        );
    }
    assert_eq!(result.null_count(), 7);
}

/// Go `TestLargeStringColumnOffset` (`pkg/util/chunk/column_test.go`): a
/// var-length column's offsets are 64-BIT. A 6M string field at a batch
/// size of 1024 puts the offset past 6GB, which an `int32` offset would
/// silently wrap.
#[test]
fn go_test_large_string_column_offset() {
    let mut col = Column::new_var_len(1);
    col.offsets[0] = 6 << 30;
    assert_eq!(col.offsets[0], 6_i64 << 30);
}

/// Go `TestJSONColumn` (`pkg/util/chunk/column_test.go`): 1024 distinct
/// JSON objects round-trip through the column, and reading them back
/// through the COLUMN and through a `Row` agrees, printed form included.
#[test]
fn go_test_json_column() {
    let field = FieldType::new(FieldTypeCode::Json);
    let mut chk = crate::chunk::Chunk::new_with_capacity(&[field], 1024);
    for i in 0..1024 {
        let json = tidb_datatype::BinaryJSON::parse(&format!("{{\"{i}\":{i}}}"))
            .expect("valid JSON object");
        chk.append_json(0, &json);
    }

    let mut it = crate::iterator::Iterator4Chunk::new(&chk);
    let mut i = 0;
    let mut row = crate::iterator::ChunkIterator::begin(&mut it);
    while row.is_some() {
        let j1 = chk.column(0).get_json(i);
        let j2 = row.expect("not end").get_json(0);
        assert_eq!(j2.to_string(), j1.to_string());
        assert_eq!(j1.to_string(), format!("{{\"{i}\": {i}}}"));
        i += 1;
        row = crate::iterator::ChunkIterator::next_row(&mut it);
    }
    assert_eq!(i, 1024);
}

#[test]
fn memory_usage_uses_the_public_go_payload_constant() {
    let column = Column::new_fixed_len(8, 17);
    let expected = 112
        + column.null_bitmap.capacity() as i64
        + (column.offsets.capacity() * 8) as i64
        + column.data.capacity() as i64
        + column.elem_buffer_capacity() as i64;
    assert_eq!(column.memory_usage(), expected);
}

#[test]
fn append_string_accepts_arbitrary_bytes_and_set_raw_copies_to_cell_width() {
    let mut column = Column::new_var_len(2);
    column.append_string(&[0xff, 0x00][..]);
    column.append_bytes(b"abcde");
    assert_eq!(column.get_bytes(0), &[0xff, 0x00]);

    column.set_raw(1, b"xy");
    assert_eq!(column.get_bytes(1), b"xycde");
    column.set_raw(1, b"123456789");
    assert_eq!(column.get_bytes(1), b"12345");
    column.set_raw(1, b"");
    assert_eq!(column.get_bytes(1), b"12345");
}

#[test]
fn guarded_cell_mutation_and_clone_isolation_are_immediate() {
    let mut source = Column::new_var_len(1);
    source.append_bytes(b"abc");
    let mut copy = source.clone();

    source.with_cell_bytes_mut(0, |cell| cell[1] = b'X');
    assert_eq!(source.get_bytes(0), b"aXc");
    assert_eq!(copy.get_bytes(0), b"abc");

    copy.with_cell_bytes_mut(0, |cell| cell[0] = b'Y');
    assert_eq!(copy.get_bytes(0), b"Ybc");
    assert_eq!(source.get_bytes(0), b"aXc");
}

#[test]
fn typed_mutation_callbacks_write_through_without_unsafe_casts() {
    let mut ints = Column::new_fixed_len(8, 2);
    ints.append_int64(1);
    ints.append_int64(2);
    ints.with_int64s_mut(|values| values.copy_from_slice(&[-3, 4]));
    assert_eq!((ints.get_int64(0), ints.get_int64(1)), (-3, 4));

    ints.with_uint64s_mut(|values| values[0] = 9);
    assert_eq!(ints.get_uint64(0), 9);
    ints.with_go_durations_mut(|values| values[1] = 77);
    assert_eq!(ints.get_int64(1), 77);

    let mut float32s = Column::new_fixed_len(4, 1);
    float32s.append_float32(1.0);
    float32s.with_float32s_mut(|values| values[0] = -2.5);
    assert_eq!(float32s.get_float32(0), -2.5);

    let mut float64s = Column::new_fixed_len(8, 1);
    float64s.append_float64(1.0);
    float64s.with_float64s_mut(|values| values[0] = 3.25);
    assert_eq!(float64s.get_float64(0), 3.25);

    let mut decimals = Column::new_fixed_len(MYDECIMAL_STRUCT_SIZE, 1);
    decimals.append_my_decimal(&MyDecimal::from_int(1));
    decimals.with_decimals_mut(|values| values[0] = MyDecimal::from_int(8));
    assert_eq!(decimals.get_my_decimal(0), MyDecimal::from_int(8));

    use tidb_datatype::{CoreTime, TimeType};
    let first = Time::new(
        CoreTime::from_date(2020, 1, 2, 3, 4, 5, 0),
        TimeType::DateTime,
        0,
    )
    .unwrap();
    let second = Time::new(
        CoreTime::from_date(2021, 6, 7, 8, 9, 10, 0),
        TimeType::DateTime,
        0,
    )
    .unwrap();
    let mut times = Column::new_fixed_len(SIZE_TIME as usize, 1);
    times.append_time(first);
    times.with_times_mut(|values| values[0] = second);
    assert_eq!(times.get_time(0), second);
}

#[test]
fn vector_decoder_accepts_a_valid_image_with_a_suffix() {
    let expected = VectorFloat32::must_create(vec![1.0, -2.5]);
    let mut encoded = expected.serialize();
    encoded.extend_from_slice(b"ignored suffix");
    let mut column = Column::new_var_len(1);
    column.append_bytes(&encoded);
    assert_eq!(column.get_vector_float32(0), expected);
}


/// Go `TestColumnCopy` (`pkg/util/chunk/column_test.go`): `CopyConstruct(nil)`
/// is an equal deep copy, and `CopyConstruct(dst)` reuses the destination.
#[test]
fn go_test_column_copy() {
    let mut col = Column::new_fixed_len(8, 10);
    for i in 0..10i64 {
        col.append_int64(i);
    }

    let c1 = col.copy_construct();
    assert_eq!(col, c1);

    let c2 = col.copy_reconstruct(None, Some(Column::new_fixed_len(8, 10)));
    assert_eq!(col, c2);
}

/// Go `TestColumnCopyReconstructFixedLen`: a random selection and null
/// pattern survive `CopyReconstruct`, and 128 appended rows land AFTER the
/// selected prefix untouched.
#[test]
fn go_test_column_copy_reconstruct_fixed_len() {
    for seed in 1..=4u64 {
        let mut rng = Rng(seed);
        let mut col = Column::new_column(
            &FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            1024,
        );
        let mut results: Vec<i64> = Vec::with_capacity(1024);
        let mut nulls: Vec<bool> = Vec::with_capacity(1024);
        let mut sel: Vec<usize> = Vec::with_capacity(1024);
        for i in 0..1024 {
            if rng.intn10() < 6 {
                sel.push(i);
            }
            if rng.intn10() < 2 {
                col.append_null();
                nulls.push(true);
                results.push(0);
                continue;
            }
            let v = rng.int63();
            col.append_int64(v);
            results.push(v);
            nulls.push(false);
        }

        let col = col.copy_reconstruct(Some(&sel), None);
        let mut null_cnt = 0usize;
        for (n, &i) in sel.iter().enumerate() {
            if nulls[i] {
                null_cnt += 1;
                assert!(col.is_null(n));
            } else {
                assert_eq!(results[i], col.get_int64(n));
            }
        }
        assert_eq!(col.null_count(), null_cnt);
        assert_eq!(col.length(), sel.len());

        let mut col = col;
        for i in 0..128usize {
            if i % 2 == 0 {
                col.append_null();
            } else {
                col.append_int64((i * i * i) as i64);
            }
        }

        assert_eq!(col.length() - 128, sel.len());
        assert_eq!(col.null_count(), null_cnt + 128 / 2);
        for i in 0..128usize {
            if i % 2 == 0 {
                assert!(col.is_null(sel.len() + i));
            } else {
                assert_eq!(col.get_int64(sel.len() + i), (i * i * i) as i64);
                assert!(!col.is_null(sel.len() + i));
            }
        }
    }
}

/// Go `TestColumnCopyReconstructVarLen`: the same contract over strings,
/// where reconstruct must also rewrite every offset.
#[test]
fn go_test_column_copy_reconstruct_var_len() {
    for seed in 1..=4u64 {
        let mut rng = Rng(seed);
        let mut col = Column::new_column(
            &FieldType::new(tidb_datatype::FieldTypeCode::VarString),
            1024,
        );
        let mut results: Vec<String> = Vec::with_capacity(1024);
        let mut nulls: Vec<bool> = Vec::with_capacity(1024);
        let mut sel: Vec<usize> = Vec::with_capacity(1024);
        for i in 0..1024 {
            if rng.intn10() < 6 {
                sel.push(i);
            }
            if rng.intn10() < 2 {
                col.append_null();
                nulls.push(true);
                results.push(String::new());
                continue;
            }
            let v = rng.int63().to_string();
            col.append_string(v.as_str());
            results.push(v);
            nulls.push(false);
        }

        let col = col.copy_reconstruct(Some(&sel), None);
        let mut null_cnt = 0usize;
        for (n, &i) in sel.iter().enumerate() {
            if nulls[i] {
                null_cnt += 1;
                assert!(col.is_null(n));
            } else {
                assert_eq!(col.get_string(n).as_bytes(), results[i].as_bytes());
            }
        }
        assert_eq!(col.null_count(), null_cnt);
        assert_eq!(col.length(), sel.len());

        let mut col = col;
        for i in 0..128usize {
            if i % 2 == 0 {
                col.append_null();
            } else {
                col.append_string((i * i * i).to_string().as_str());
            }
        }

        assert_eq!(col.length() - 128, sel.len());
        assert_eq!(col.null_count(), null_cnt + 128 / 2);
        for i in 0..128usize {
            if i % 2 == 0 {
                assert!(col.is_null(sel.len() + i));
            } else {
                assert_eq!(
                    col.get_string(sel.len() + i).as_bytes(),
                    (i * i * i).to_string().as_bytes()
                );
                assert!(!col.is_null(sel.len() + i));
            }
        }
    }
}

/// Go `TestI64Column`: mutating the `Int64s()` slice writes through to the
/// chunk, and the iterator observes it.
#[test]
fn go_test_i64_column() {
    let fields = vec![FieldType::new(FieldTypeCode::LongLong)];
    let mut chk = Chunk::new_with_capacity(&fields, 1024);
    for i in 0..1024i64 {
        chk.column_mut(0).append_int64(i);
    }

    chk.column_mut(0).with_int64s_mut(|values| {
        for value in values.iter_mut() {
            *value += 1;
        }
    });

    let mut it = crate::iterator::Iterator4Chunk::new(&chk);
    let mut i = 0i64;
    let mut row = it.begin();
    while row != it.end() {
        assert_eq!(row.expect("not end").get_int64(0), i + 1);
        assert_eq!(chk.column(0).get_int64(i as usize), i + 1);
        i += 1;
        row = it.next_row();
    }
    assert_eq!(i, 1024);
}

/// Go `TestF64Column`: mutating the `Float64s()` slice writes through to the
/// chunk, and the iterator observes it.
#[test]
fn go_test_f64_column() {
    let fields = vec![FieldType::new(FieldTypeCode::Double)];
    let mut chk = Chunk::new_with_capacity(&fields, 1024);
    for i in 0..1024i64 {
        chk.column_mut(0).append_float64(i as f64);
    }

    chk.column_mut(0).with_float64s_mut(|values| {
        for value in values.iter_mut() {
            *value /= 2.0;
        }
    });

    let mut it = crate::iterator::Iterator4Chunk::new(&chk);
    let mut i = 0i64;
    let mut row = it.begin();
    while row != it.end() {
        assert_eq!(row.expect("not end").get_float64(0), i as f64 / 2.0);
        assert_eq!(chk.column(0).get_float64(i as usize), i as f64 / 2.0);
        i += 1;
        row = it.next_row();
    }
}

/// Go `TestF32Column`: the same write-through contract over `Float32s()`.
#[test]
fn go_test_f32_column() {
    let fields = vec![FieldType::new(FieldTypeCode::Float)];
    let mut chk = Chunk::new_with_capacity(&fields, 1024);
    for i in 0..1024i64 {
        chk.column_mut(0).append_float32(i as f32);
    }

    chk.column_mut(0).with_float32s_mut(|values| {
        for value in values.iter_mut() {
            *value /= 2.0;
        }
    });

    let mut it = crate::iterator::Iterator4Chunk::new(&chk);
    let mut i = 0i64;
    let mut row = it.begin();
    while row != it.end() {
        assert_eq!(row.expect("not end").get_float32(0), i as f32 / 2.0);
        assert_eq!(chk.column(0).get_float32(i as usize), i as f32 / 2.0);
        i += 1;
        row = it.next_row();
    }
}

/// Go `TestDurationSliceColumn`: mutating `GoDurations()` doubles every cell
/// the iterator reads back.
#[test]
fn go_test_duration_slice_column() {
    let fields = vec![FieldType::new(FieldTypeCode::Duration)];
    let mut chk = Chunk::new_with_capacity(&fields, 1024);
    for i in 0..1024i64 {
        let d = MySqlDuration::from_nanoseconds(i, 0).expect("valid duration");
        chk.column_mut(0).append_duration(d);
    }

    chk.column_mut(0).with_go_durations_mut(|values| {
        for value in values.iter_mut() {
            *value *= 2;
        }
    });

    let mut it = crate::iterator::Iterator4Chunk::new(&chk);
    let mut i = 0i64;
    let mut row = it.begin();
    while row != it.end() {
        assert_eq!(row.expect("not end").get_duration(0, 0).nanoseconds(), i * 2);
        assert_eq!(
            chk.column(0).get_duration(i as usize, 0).nanoseconds(),
            i * 2
        );
        i += 1;
        row = it.next_row();
    }
}

/// Go `TestMyDecimal`: decimals round-trip through the raw 40-byte struct,
/// doubling is observable through the column, and the iterator agrees with
/// Go's InDelta tolerance.
#[test]
fn go_test_my_decimal() {
    let fields = vec![FieldType::new(FieldTypeCode::NewDecimal)];
    let mut chk = Chunk::new_with_capacity(&fields, 1024);
    for i in 0..1024i64 {
        let (d, error) = MyDecimal::from_float64(i as f64 * 1.1);
        assert!(error.is_none());
        chk.column_mut(0).append_my_decimal(&d);
    }

    for i in 0..1024usize {
        let (d, error) = MyDecimal::from_float64(i as f64 * 1.1);
        assert!(error.is_none());
        assert_eq!(d.compare(&chk.column(0).get_my_decimal(i)), std::cmp::Ordering::Equal);
    }

    // Go mutates the stored cells through the `Decimals()` slice view:
    // types.DecimalAdd(&ds[i], d, &ds[i]) doubles every stored decimal.
    chk.column_mut(0).with_decimals_mut(|ds| {
        for (i, cell) in ds.iter_mut().enumerate() {
            let (d, error) = MyDecimal::from_float64(i as f64 * 1.1);
            assert!(error.is_none());
            let (sum, warning) =
                Decimal::from_my_decimal(cell).add_mysql(&Decimal::from_my_decimal(&d));
            assert!(warning.is_none());
            *cell = sum.to_chunk_my_decimal().expect("doubled decimal fits");
        }
    });

    let mut it = crate::iterator::Iterator4Chunk::new(&chk);
    let mut i = 0i64;
    let mut row = it.begin();
    while row != it.end() {
        let (d, error) = MyDecimal::from_float64(i as f64 * 1.1 * 2.0);
        assert!(error.is_none());
        // Go: delta := DecimalSub(d, row.GetMyDecimal(0), fDelta); InDelta(0, fDelta, 0.0001).
        let got = row.expect("not end").get_my_decimal(0);
        let (got_f64, error) = got.to_float64();
        assert!(error.is_none());
        assert!((got_f64 - d.to_float64().0).abs() < 0.0001, "row {i}");
        i += 1;
        row = it.next_row();
    }
}

/// Go `TestStringColumn`: strings written into the chunk read back identically
/// through both the column and the iterator.
#[test]
fn go_test_string_column() {
    let fields = vec![FieldType::new(FieldTypeCode::VarString)];
    let mut chk = Chunk::new_with_capacity(&fields, 1024);
    for i in 0..1024usize {
        chk.column_mut(0).append_string((i * i).to_string().as_str());
    }

    let mut it = crate::iterator::Iterator4Chunk::new(&chk);
    let mut i = 0usize;
    let mut row = it.begin();
    while row != it.end() {
        let expect = (i * i).to_string();
        assert_eq!(row.expect("not end").get_string(0).as_bytes(), expect.as_bytes());
        assert_eq!(chk.column(0).get_string(i).as_bytes(), expect.as_bytes());
        i += 1;
        row = it.next_row();
    }
}

/// Go `TestSetColumn`: Set name/value cells agree across column and iterator.
#[test]
fn go_test_set_column() {
    let fields = vec![FieldType::new(FieldTypeCode::Set)];
    let mut chk = Chunk::new_with_capacity(&fields, 1024);
    for i in 0..1024u64 {
        chk.column_mut(0)
            .append_set(&MysqlSet::new(i.to_string(), i));
    }

    let mut it = crate::iterator::Iterator4Chunk::new(&chk);
    let mut i = 0u64;
    let mut row = it.begin();
    while row != it.end() {
        let s1 = chk.column(0).get_set(i as usize);
        let s2 = row.expect("not end").get_set(0);
        assert_eq!(s2.name(), s1.name());
        assert_eq!(s2.value(), s1.value());
        assert_eq!(s1.name_bytes(), i.to_string().as_bytes());
        assert_eq!(s1.value(), i);
        i += 1;
        row = it.next_row();
    }
}

/// Go `TestTimeColumn`: times written into the chunk agree across the column,
/// the packed `Times()` slice, and the iterator.
#[test]
fn go_test_time_column() {
    use tidb_datatype::CoreTime;
    let fields = vec![FieldType::new(FieldTypeCode::Datetime)];
    let mut chk = Chunk::new_with_capacity(&fields, 1024);
    let mut times = Vec::with_capacity(1024);
    for i in 0..1024i64 {
        let t = Time::new(
            CoreTime::from_date(2020, 1, (i % 28 + 1) as u8, 12, 30, 45, i as u32),
            tidb_datatype::TimeType::DateTime,
            0,
        )
        .expect("valid time");
        times.push(t);
        chk.column_mut(0).append_time(t);
    }

    let mut it = crate::iterator::Iterator4Chunk::new(&chk);
    let mut i = 0usize;
    let mut row = it.begin();
    while row != it.end() {
        let j1 = chk.column(0).get_time(i);
        let j2 = row.expect("not end").get_time(0);
        assert_eq!(j1.compare(j2), std::cmp::Ordering::Equal);
        assert_eq!(j1.compare(times[i]), std::cmp::Ordering::Equal);
        i += 1;
        row = it.next_row();
    }
}

/// Go `TestDurationColumn`: durations agree across the column and iterator.
#[test]
fn go_test_duration_column() {
    let fields = vec![FieldType::new(FieldTypeCode::Duration)];
    let mut chk = Chunk::new_with_capacity(&fields, 1024);
    for i in 0..1024i64 {
        chk.column_mut(0)
            .append_duration(MySqlDuration::from_raw_parts(i * 1_000_000_000, 0));
    }

    let mut it = crate::iterator::Iterator4Chunk::new(&chk);
    let mut i = 0usize;
    let mut row = it.begin();
    while row != it.end() {
        let j1 = chk.column(0).get_duration(i, 0);
        let j2 = row.expect("not end").get_duration(0, 0);
        assert_eq!(
            j1.nanoseconds().cmp(&j2.nanoseconds()),
            std::cmp::Ordering::Equal
        );
        i += 1;
        row = it.next_row();
    }
}

/// Go `TestEnumColumn`: Enum name/value cells agree across column and iterator.
#[test]
fn go_test_enum_column() {
    let fields = vec![FieldType::new(FieldTypeCode::Enum)];
    let mut chk = Chunk::new_with_capacity(&fields, 1024);
    for i in 0..1024u64 {
        chk.column_mut(0)
            .append_enum(&MysqlEnum::new(i.to_string(), i));
    }

    let mut it = crate::iterator::Iterator4Chunk::new(&chk);
    let mut i = 0u64;
    let mut row = it.begin();
    while row != it.end() {
        let s1 = chk.column(0).get_enum(i as usize);
        let s2 = row.expect("not end").get_enum(0);
        assert_eq!(s2.name(), s1.name());
        assert_eq!(s2.value(), s1.value());
        assert_eq!(s1.name_bytes(), i.to_string().as_bytes());
        assert_eq!(s1.value(), i);
        i += 1;
        row = it.next_row();
    }
}

/// Go `TestNullsColumn`: alternating nulls are visible to both the column and
/// the iterator.
#[test]
fn go_test_nulls_column() {
    let fields = vec![FieldType::new(FieldTypeCode::LongLong)];
    let mut chk = Chunk::new_with_capacity(&fields, 1024);
    for i in 0..1024usize {
        if i % 2 == 0 {
            chk.column_mut(0).append_null();
            continue;
        }
        chk.column_mut(0).append_int64(i as i64);
    }

    let mut it = crate::iterator::Iterator4Chunk::new(&chk);
    let mut i = 0usize;
    let mut row = it.begin();
    while row != it.end() {
        let cur = row.expect("not end");
        if i % 2 == 0 {
            assert!(cur.is_null(0));
            assert!(chk.column(0).is_null(i));
        } else {
            assert_eq!(cur.get_int64(0), i as i64);
        }
        i += 1;
        row = it.next_row();
    }
}
