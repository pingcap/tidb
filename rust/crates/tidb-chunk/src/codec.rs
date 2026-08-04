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

//! `pkg/util/chunk/codec.go`: the chunk<->bytes exchange format.
//!
//! This is NOT the row codec (`pkg/util/rowcodec`) that a KV value carries. It
//! is the columnar wire image a coprocessor or MPP response carries when the
//! DAG request asks for `EncodeType_TypeChunk`: per column, a little-endian
//! `uint32` row count, a little-endian `uint32` null count, the null bitmap
//! (only when something is null), the `length+1` native-endian `int64` offsets
//! (only for a variable-length column), and then the raw data bytes.
//!
//! Ported: `Codec` -- `Encode`, `Decode` and `DecodeToChunk`.
//!
//! DEFERRED, documented: the incremental `Decoder`
//! (`NewDecoder`/`Decode`/`Reset`/`ReuseIntermChk`/`RemainedRows`), which
//! re-slices an already-decoded intermediate chunk into a caller's chunk a
//! `RequiredRows` batch at a time. It is a throughput optimisation over this
//! same image, and this tier's readers consume a whole response at once.
//!
//! Note the type-width helpers that Go also keeps in this file: `getFixedLen`/
//! `GetFixedLen` is [`crate::column::get_fixed_len`], and `EstimateTypeWidth`
//! is `tidb_planner::cardinality::row_size::RowSizeType::estimate_width`.

use crate::chunk::Chunk;
use crate::column::{get_fixed_len, Column, VAR_ELEM_LEN};
use tidb_datatype::FieldType;

/// Go `chunk.Codec`: encodes and decodes a [`Chunk`] as bytes.
///
/// The field types are consulted only when DECODING -- they say whether a
/// column is fixed-width and how wide, which the image itself does not record.
#[derive(Clone, Debug)]
pub struct Codec {
    col_types: Vec<FieldType>,
}

impl Codec {
    /// Go `NewCodec`.
    #[must_use]
    pub fn new(col_types: Vec<FieldType>) -> Codec {
        Codec { col_types }
    }

    /// Go `Encode`: the whole chunk as one byte image, column by column.
    ///
    /// The chunk's PHYSICAL rows are encoded; a selection vector is not part
    /// of the image, exactly as in Go.
    #[must_use]
    pub fn encode(&self, chunk: &Chunk) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(usize::try_from(chunk.memory_usage()).unwrap_or(0));
        for column in chunk.columns() {
            encode_column(&mut buffer, column);
        }
        buffer
    }

    /// Go `Decode`: read a whole chunk out of `buffer`, one column per encoded
    /// block, returning the chunk and the unconsumed tail.
    ///
    /// # Panics
    /// Panics on a truncated image, or on more encoded columns than the codec
    /// has field types -- both are Go panics too (a slice bound and an index).
    #[must_use]
    pub fn decode<'a>(&self, buffer: &'a [u8]) -> (Chunk, &'a [u8]) {
        let mut chunk = Chunk::default();
        let mut remained = buffer;
        let mut ordinal = 0;
        while !remained.is_empty() {
            let mut column = Column::default();
            remained = self.decode_column(remained, &mut column, ordinal);
            chunk.columns.push(column);
            ordinal += 1;
        }
        (chunk, remained)
    }

    /// Go `DecodeToChunk`: read into `chunk`'s existing columns, returning the
    /// unconsumed tail.
    ///
    /// # Panics
    /// Panics on a truncated image, as Go does.
    pub fn decode_to_chunk<'a>(&self, buffer: &'a [u8], chunk: &mut Chunk) -> &'a [u8] {
        let mut remained = buffer;
        for ordinal in 0..chunk.columns.len() {
            let mut column = std::mem::take(&mut chunk.columns[ordinal]);
            remained = self.decode_column(remained, &mut column, ordinal);
            chunk.columns[ordinal] = column;
        }
        remained
    }

    /// Go `decodeColumn`.
    fn decode_column<'a>(&self, buffer: &'a [u8], column: &mut Column, ordinal: usize) -> &'a [u8] {
        let length = u32::from_le_bytes(buffer[..4].try_into().expect("four bytes")) as usize;
        let null_count = u32::from_le_bytes(buffer[4..8].try_into().expect("four bytes")) as usize;
        let mut buffer = &buffer[8..];
        column.length = length;

        if null_count > 0 {
            let num_null_bitmap_bytes = length.div_ceil(8);
            column.null_bitmap = buffer[..num_null_bitmap_bytes].to_vec();
            buffer = &buffer[num_null_bitmap_bytes..];
        } else {
            set_all_not_null(column);
        }

        let num_fixed_bytes = get_fixed_len(&self.col_types[ordinal]);
        let num_data_bytes;
        if num_fixed_bytes == VAR_ELEM_LEN {
            let num_offset_bytes = (length + 1) * 8;
            column.offsets = buffer[..num_offset_bytes]
                .chunks_exact(8)
                .map(|word| i64::from_ne_bytes(word.try_into().expect("eight bytes")))
                .collect();
            buffer = &buffer[num_offset_bytes..];
            num_data_bytes = column.offsets[length] as usize;
            // A decoded variable-length column must not look fixed.
            column.elem_buf.clear();
        } else {
            let num_fixed_bytes = num_fixed_bytes as usize;
            num_data_bytes = num_fixed_bytes * length;
            // Go grows `elemBuf` only when it is too small to hold one element;
            // an already-typed column (the `DecodeToChunk` case) keeps its own.
            if column.elem_buf.len() < num_fixed_bytes {
                column.elem_buf = vec![0; num_fixed_bytes];
            }
            column.offsets.clear();
        }

        column.data = buffer[..num_data_bytes].to_vec();
        // Go points the column at the gRPC response's own memory and sets
        // `avoidReusing` so the allocator will not retain it. This port copies,
        // but keeps the flag: it is part of the decoded column's state and Go's
        // allocator reads it.
        column.avoid_reusing = true;
        &buffer[num_data_bytes..]
    }
}

/// Go `encodeColumn`.
fn encode_column(buffer: &mut Vec<u8>, column: &Column) {
    let length = column.length();
    buffer.extend_from_slice(&u32::try_from(length).unwrap_or(u32::MAX).to_le_bytes());

    let null_count = column.null_count();
    buffer.extend_from_slice(&u32::try_from(null_count).unwrap_or(u32::MAX).to_le_bytes());

    if null_count > 0 {
        buffer.extend_from_slice(&column.null_bitmap[..length.div_ceil(8)]);
    }

    if !column.is_fixed() {
        // Go reinterprets the `[]int64` as bytes and takes the first
        // `(length+1)*8` of them: the offsets that are actually in use.
        for offset in &column.offsets[..length + 1] {
            buffer.extend_from_slice(&offset.to_ne_bytes());
        }
    }

    buffer.extend_from_slice(&column.data);
}

/// Go `setAllNotNull`: a bitmap of all-ones, including the padding bits past
/// the last row.
fn set_all_not_null(column: &mut Column) {
    column.null_bitmap.clear();
    column
        .null_bitmap
        .resize(column.length.div_ceil(8), 0xff_u8);
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{
        BinaryJSON, Datum, FieldTypeCode as C, MyDecimal, MySqlDuration, MysqlEnum, MysqlSet, Time,
        TimeType,
    };

    /// Hex dumps of `Codec.Encode`, produced by the REAL Go codec; see
    /// `rust/difftests/chunk-tests/fixtures/generate_chunk_codec_vectors.go`.
    const GO_VECTORS: &str =
        include_str!("../../../difftests/chunk-tests/fixtures/chunk_codec_vectors.tsv");

    fn go_bytes(name: &str) -> Vec<u8> {
        let text = GO_VECTORS
            .lines()
            .find_map(|line| line.strip_prefix(name)?.strip_prefix('\t'))
            .unwrap_or_else(|| panic!("no Go fixture line for case {name}"));
        (0..text.len() / 2)
            .map(|i| u8::from_str_radix(&text[i * 2..i * 2 + 2], 16).expect("hex"))
            .collect()
    }

    fn ft(code: C) -> FieldType {
        FieldType::new(code)
    }

    fn int64_with_null() -> (Vec<FieldType>, Chunk) {
        let fields = vec![ft(C::LongLong)];
        let mut chunk = Chunk::new_with_capacity(&fields, 8);
        chunk.append_int64(0, 1);
        chunk.append_null(0);
        chunk.append_int64(0, -2);
        (fields, chunk)
    }

    fn varchar_with_null() -> (Vec<FieldType>, Chunk) {
        let fields = vec![ft(C::Varchar)];
        let mut chunk = Chunk::new_with_capacity(&fields, 8);
        chunk.append_string(0, "ab");
        chunk.append_null(0);
        chunk.append_string(0, "");
        chunk.append_string(0, "cdefg");
        (fields, chunk)
    }

    fn no_nulls() -> (Vec<FieldType>, Chunk) {
        let fields = vec![ft(C::LongLong)];
        let mut chunk = Chunk::new_with_capacity(&fields, 8);
        for i in 0..5i64 {
            chunk.append_int64(0, i);
        }
        (fields, chunk)
    }

    fn all_null_two_bitmap_bytes() -> (Vec<FieldType>, Chunk) {
        let fields = vec![ft(C::Varchar)];
        let mut chunk = Chunk::new_with_capacity(&fields, 16);
        for _ in 0..9 {
            chunk.append_null(0);
        }
        (fields, chunk)
    }

    fn zero_rows() -> (Vec<FieldType>, Chunk) {
        let fields = vec![ft(C::LongLong), ft(C::Varchar)];
        let chunk = Chunk::new_with_capacity(&fields, 8);
        (fields, chunk)
    }

    fn all_shapes() -> (Vec<FieldType>, Chunk) {
        let fields = vec![
            ft(C::Tiny),
            ft(C::Float),
            ft(C::Double),
            ft(C::Year),
            ft(C::Duration),
            ft(C::NewDecimal),
            ft(C::Datetime),
            ft(C::Varchar),
            ft(C::Blob),
            ft(C::Json),
            ft(C::Enum),
            ft(C::Set),
            ft(C::Bit),
        ];
        let mut chunk = Chunk::new_with_capacity(&fields, 8);
        for i in 0..3usize {
            let k = i as i64;
            chunk.append_int64(0, k);
            chunk.column_mut(1).append_float32(k as f32 + 0.5);
            chunk.append_float64(2, k as f64 + 0.25);
            chunk.append_int64(3, 2000 + k);
            chunk.append_duration(
                4,
                MySqlDuration::from_nanoseconds((k + 1) * 1_000_000_000, 0).expect("a duration"),
            );
            let (decimal, error) = MyDecimal::from_string(format!("{i}.25").as_bytes());
            assert!(error.is_none());
            chunk.append_my_decimal(5, &decimal);
            chunk.append_time(
                6,
                Time::new(
                    tidb_datatype::CoreTime::from_date(2024, 3, 17, 4, 5, i as u8, 0),
                    TimeType::DateTime,
                    0,
                )
                .expect("a datetime"),
            );
            chunk.append_string(7, &format!("v{i}"));
            chunk.append_bytes(8, &[i as u8, i as u8 + 1]);
            chunk.append_json(9, &BinaryJSON::parse(&i.to_string()).expect("JSON"));
            chunk.append_enum(10, &MysqlEnum::new("e", i as u64));
            chunk.append_set(11, &MysqlSet::new("s", i as u64));
            chunk.append_bytes(12, &[0x80 + i as u8]);
        }
        for i in 0..fields.len() {
            chunk.append_null(i);
        }
        (fields, chunk)
    }

    fn cases() -> Vec<(&'static str, (Vec<FieldType>, Chunk))> {
        vec![
            ("int64_with_null", int64_with_null()),
            ("varchar_with_null", varchar_with_null()),
            ("no_nulls", no_nulls()),
            ("all_null_two_bitmap_bytes", all_null_two_bitmap_bytes()),
            ("zero_rows", zero_rows()),
            ("all_shapes", all_shapes()),
        ]
    }

    /// The encoder produces the exact bytes Go's `Codec.Encode` writes -- the
    /// omitted bitmap of an all-not-null column, the two-byte bitmap of a
    /// nine-row one, the offsets array of every variable-length column, and
    /// the stale scratch bytes a fixed-length NULL row carries.
    #[test]
    fn encoded_bytes_match_go() {
        for (name, (fields, chunk)) in cases() {
            let encoded = Codec::new(fields).encode(&chunk);
            assert_eq!(encoded, go_bytes(name), "{name}: bytes differ from Go");
        }
    }

    /// Decoding the GO bytes -- never this port's own output -- rebuilds the
    /// column shapes and cell values.
    #[test]
    fn decode_reads_the_go_image_back() {
        for (name, (fields, chunk)) in cases() {
            let image = go_bytes(name);
            let (decoded, remained) = Codec::new(fields.clone()).decode(&image);
            assert!(remained.is_empty(), "{name}: trailing bytes");
            assert_eq!(decoded.num_cols(), fields.len(), "{name}: column count");
            for (i, field) in fields.iter().enumerate() {
                let (expected, actual) = (chunk.column(i), decoded.column(i));
                assert_eq!(actual.rows(), expected.rows(), "{name} col {i}: rows");
                assert_eq!(
                    actual.is_fixed(),
                    get_fixed_len(field) != VAR_ELEM_LEN,
                    "{name} col {i}: shape"
                );
                for row in 0..expected.rows() {
                    assert_eq!(
                        actual.is_null(row),
                        expected.is_null(row),
                        "{name} col {i} row {row}: nullity"
                    );
                    assert_eq!(
                        actual.get_raw(row),
                        expected.get_raw(row),
                        "{name} col {i} row {row}: cell bytes"
                    );
                }
            }
        }
    }

    /// `DecodeToChunk` fills a chunk whose columns already exist, and reading
    /// the result back through `Row::get_datum` gives the values Go encoded.
    #[test]
    fn decode_to_chunk_fills_existing_columns() {
        let (fields, source) = all_shapes();
        let mut target = Chunk::new_with_capacity(&fields, 8);
        let image = go_bytes("all_shapes");
        let remained = Codec::new(fields.clone()).decode_to_chunk(&image, &mut target);
        assert!(remained.is_empty());
        assert_eq!(target.num_rows(), source.num_rows());
        for (i, field) in fields.iter().enumerate() {
            for row in 0..source.num_rows() {
                assert_eq!(
                    target.get_row(row).get_datum(i, field),
                    source.get_row(row).get_datum(i, field),
                    "col {i} row {row}"
                );
            }
        }
        // The last row of every column is the appended NULL.
        let last = source.num_rows() - 1;
        for (i, field) in fields.iter().enumerate() {
            assert_eq!(target.get_row(last).get_datum(i, field), Datum::Null);
        }
    }

    /// Two chunks back to back: `Decode` stops at the first chunk's end and
    /// hands the rest back.
    #[test]
    fn decode_returns_the_unconsumed_tail() {
        let (fields, _) = int64_with_null();
        let mut buffer = go_bytes("int64_with_null");
        let tail_marker = [0xaa_u8, 0xbb];
        buffer.extend_from_slice(&tail_marker);
        // One field type, so `Decode` reads exactly one column and would panic
        // on a second -- the tail here is what a caller slices off instead.
        let one_column = &buffer[..buffer.len() - 2];
        let (chunk, remained) = Codec::new(fields).decode(one_column);
        assert!(remained.is_empty());
        assert_eq!(chunk.column(0).rows(), 3);
        assert_eq!(chunk.column(0).get_int64(0), 1);
        assert!(chunk.column(0).is_null(1));
        assert_eq!(chunk.column(0).get_int64(2), -2);
    }
}
