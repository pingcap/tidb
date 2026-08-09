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
//! Ported: `Codec` -- `Encode`, `Decode` and `DecodeToChunk` -- and the
//! incremental `Decoder` that drains an intermediate decoded chunk in
//! `RequiredRows` batches rounded to bitmap-byte boundaries.
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
    /// block. The source loops until the buffer is exhausted, so the returned
    /// tail is empty for every valid image.
    ///
    /// # Panics
    /// Panics on a truncated image, or on more encoded columns than the codec
    /// has field types -- both are Go panics too (a slice bound and an index).
    #[must_use]
    pub fn decode<'a>(&self, buffer: &'a [u8]) -> (Chunk, &'a [u8]) {
        let mut columns = Vec::new();
        let mut remained = buffer;
        let mut ordinal = 0;
        while !remained.is_empty() {
            let mut column = Column::default();
            remained = self.decode_column(remained, &mut column, ordinal);
            columns.push(column);
            ordinal += 1;
        }
        let chunk = if columns.is_empty() {
            // Go starts with `&Chunk{}` and therefore preserves nil-column
            // zero-value semantics when the input itself is empty.
            Chunk::default()
        } else {
            Chunk::from_reusable_columns(columns, 0, 0)
        };
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
        } else {
            let num_fixed_bytes = num_fixed_bytes as usize;
            num_data_bytes = num_fixed_bytes * length;
            // Go grows `elemBuf` only when it is too small to hold one element;
            // an already-typed column (the `DecodeToChunk` case) keeps its own.
            if column.elem_buffer_capacity() < num_fixed_bytes {
                column.elem_buf = vec![0; num_fixed_bytes];
            }
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

/// Go `Decoder`: incrementally drains one decoded coprocessor chunk into
/// caller chunks.
#[derive(Debug)]
pub struct Decoder {
    intermediate: Chunk,
    codec: Codec,
    remained_rows: usize,
}

impl Decoder {
    /// Go `NewDecoder`.
    #[must_use]
    pub fn new(intermediate: Chunk, col_types: Vec<FieldType>) -> Self {
        Self {
            intermediate,
            codec: Codec::new(col_types),
            remained_rows: 0,
        }
    }

    /// Go `Decoder.Reset`: decode a complete TypeChunk image into the
    /// intermediate chunk and restart consumption at its first row.
    pub fn reset(&mut self, data: &[u8]) {
        let _remained = self.codec.decode_to_chunk(data, &mut self.intermediate);
        self.remained_rows = self.intermediate.num_rows();
    }

    /// Go `Decoder.IsFinished`.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.remained_rows == 0
    }

    /// Go `Decoder.RemainedRows`.
    #[must_use]
    pub fn remained_rows(&self) -> usize {
        self.remained_rows
    }

    /// Go `Decoder.Decode`.
    ///
    /// The requested deficit is rounded UP to a multiple of eight so each
    /// non-final batch consumes complete source bitmap bytes. Consequently a
    /// target can grow by up to seven rows beyond `required_rows`, exactly as
    /// in Go; the next caller observes `is_full` and returns it immediately.
    ///
    /// # Panics
    /// Panics for an overfull target. Go's signed deficit becomes a negative
    /// slice length in that invalid state; this checked boundary prevents an
    /// unsigned wrap. An exactly-full non-empty target is valid and performs
    /// the source's zero-row bitmap-padding normalization without consuming
    /// the intermediate chunk.
    pub fn decode(&mut self, target: &mut Chunk) {
        assert!(
            target.num_rows() <= target.required_rows(),
            "Decoder.Decode target exceeds RequiredRows"
        );
        let deficit = target.required_rows() - target.num_rows();
        let rows = deficit
            .div_ceil(8)
            .saturating_mul(8)
            .min(self.remained_rows);
        for ordinal in 0..target.num_cols() {
            self.decode_column(target, ordinal, rows);
        }
        self.remained_rows -= rows;
    }

    fn decode_column(&mut self, target: &mut Chunk, ordinal: usize, rows: usize) {
        let type_size = get_fixed_len(&self.codec.col_types[ordinal]);
        let source = self.intermediate.column_mut(ordinal);
        let destination = target.column_mut(ordinal);
        let data_bytes = if type_size == VAR_ELEM_LEN {
            let start = source.offsets[0];
            let end = source.offsets[rows];
            let delta = destination.offsets[destination.length] - start;
            if rows > 0 {
                let first_new_offset = destination.length + 1;
                destination
                    .offsets
                    .extend_from_slice(&source.offsets[1..rows + 1]);
                for offset in &mut destination.offsets[first_new_offset..=destination.length + rows]
                {
                    *offset += delta;
                }
            }
            source.offsets.drain(..rows);
            usize::try_from(end - start).expect("non-negative decoder data length")
        } else {
            usize::try_from(type_size)
                .expect("fixed width is positive")
                .checked_mul(rows)
                .expect("decoder data size overflow")
        };

        let bitmap_bytes = rows.div_ceil(8);
        if destination.length % 8 == 0 {
            destination
                .null_bitmap
                .extend_from_slice(&source.null_bitmap[..bitmap_bytes]);
        } else {
            destination.append_multi_same_null_bitmap(false, rows);
            let bitmap_len = destination.null_bitmap.len();
            let bit_offset = destination.length % 8;
            let start_index = (destination.length - 1) >> 3;
            for (index, source_byte) in source.null_bitmap[..bitmap_bytes].iter().enumerate() {
                destination.null_bitmap[start_index + index] |= *source_byte << bit_offset;
                if start_index + index + 1 < bitmap_len {
                    destination.null_bitmap[start_index + index + 1] |=
                        *source_byte >> (8 - bit_offset);
                }
            }
        }
        let redundant_bits = destination.null_bitmap.len() * 8 - destination.length - rows;
        let bit_mask = ((1u16 << (8 - redundant_bits)) as u8).wrapping_sub(1);
        let last = destination.null_bitmap.len() - 1;
        destination.null_bitmap[last] &= bit_mask;

        source.null_bitmap.drain(..bitmap_bytes);
        destination.length += rows;
        destination
            .data
            .extend_from_slice(&source.data[..data_bytes]);
        source.data.drain(..data_bytes);
    }

    /// Go `Decoder.ReuseIntermChk`: normalize the remaining variable offsets,
    /// then swap the intermediate columns directly into an empty target.
    pub fn reuse_intermediate_chunk(&mut self, target: &mut Chunk) {
        for (ordinal, column) in self.intermediate.columns.iter_mut().enumerate() {
            column.length = self.remained_rows;
            if get_fixed_len(&self.codec.col_types[ordinal]) == VAR_ELEM_LEN {
                let delta = column.offsets[0];
                if delta != 0 {
                    for offset in &mut column.offsets {
                        *offset -= delta;
                    }
                }
            }
        }
        target.swap_columns(&mut self.intermediate);
        self.remained_rows = 0;
    }

    /// The decoded intermediate chunk, exposed for source-boundary tests and
    /// allocator state inspection.
    #[must_use]
    pub fn intermediate_chunk(&self) -> &Chunk {
        &self.intermediate
    }

    /// Consume the decoder and return its intermediate chunk. This is the
    /// ownership-safe adapter for Go's one-shot caller that retains the
    /// `*Chunk` passed to `NewDecoder` after `Reset`.
    #[must_use]
    pub fn into_intermediate_chunk(self) -> Chunk {
        self.intermediate
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

    #[test]
    fn decode_reads_an_exact_single_column_image() {
        let (fields, _) = int64_with_null();
        let buffer = go_bytes("int64_with_null");
        let (chunk, remained) = Codec::new(fields).decode(&buffer);
        assert!(remained.is_empty());
        assert_eq!(chunk.column(0).rows(), 3);
        assert_eq!(chunk.column(0).get_int64(0), 1);
        assert!(chunk.column(0).is_null(1));
        assert_eq!(chunk.column(0).get_int64(2), -2);
    }

    #[test]
    fn decoding_empty_input_preserves_nil_column_state() {
        let (decoded, remained) = Codec::new(Vec::new()).decode(&[]);
        assert!(remained.is_empty());
        assert_eq!(decoded.num_cols(), 0);
        // Renewal is the public observer of Go nil versus non-nil empty
        // columns: only the literal nil state collapses to a zero-value chunk.
        let renewed = decoded.renew_with_capacity(7, 9);
        assert_eq!(renewed.capacity(), 0);
        assert_eq!(renewed.required_rows(), 0);
    }

    fn decoder_source(rows: usize) -> (Vec<FieldType>, Chunk) {
        let fields = vec![ft(C::LongLong), ft(C::Varchar)];
        let mut chunk = Chunk::new_with_capacity(&fields, rows);
        for row in 0..rows {
            if row % 5 == 1 {
                chunk.append_null(0);
            } else {
                chunk.append_int64(0, row as i64 * 10);
            }
            if row % 4 == 2 {
                chunk.append_null(1);
            } else {
                chunk.append_string(1, &format!("value-{row}"));
            }
        }
        (fields, chunk)
    }

    /// Go `Decoder.Decode`: a non-byte-aligned destination still receives a
    /// whole eight-row source bitmap batch, and both fixed data and
    /// variable offsets stay aligned with the resulting null bits.
    #[test]
    fn incremental_decoder_rounds_batches_and_merges_unaligned_bitmaps() {
        let (fields, source) = decoder_source(19);
        let image = Codec::new(fields.clone()).encode(&source);
        let intermediate = Chunk::new_with_capacity(&fields, 0);
        let mut decoder = Decoder::new(intermediate, fields.clone());
        decoder.reset(&image);
        assert_eq!(decoder.remained_rows(), 19);

        let mut target = Chunk::new(&fields, 3, 5);
        for value in [-3_i64, -2, -1] {
            target.append_int64(0, value);
            target.append_string(1, "prefix");
        }
        // Deficit is two, but Go rounds it to one complete bitmap byte.
        decoder.decode(&mut target);
        assert_eq!(target.num_rows(), 11);
        assert_eq!(decoder.remained_rows(), 11);
        for row in 0..8 {
            let got = target.get_row(row + 3);
            let want = source.get_row(row);
            assert_eq!(got.is_null(0), want.is_null(0));
            assert_eq!(got.is_null(1), want.is_null(1));
            if !want.is_null(0) {
                assert_eq!(got.get_int64(0), want.get_int64(0));
            }
            if !want.is_null(1) {
                assert_eq!(got.get_bytes(1), want.get_bytes(1));
            }
        }

        let mut second = Chunk::new(&fields, 8, 8);
        decoder.decode(&mut second);
        assert_eq!(second.num_rows(), 8);
        assert_eq!(decoder.remained_rows(), 3);
        for row in 0..8 {
            assert_eq!(
                second.get_row(row).get_datum(0, &fields[0]),
                source.get_row(row + 8).get_datum(0, &fields[0])
            );
            assert_eq!(
                second.get_row(row).get_datum(1, &fields[1]),
                source.get_row(row + 8).get_datum(1, &fields[1])
            );
        }
    }

    /// Go `Decoder.ReuseIntermChk`: after two batches have advanced a
    /// variable column's source slice, reuse rebases its first offset to zero
    /// and transfers only the three remaining rows.
    #[test]
    fn decoder_reuse_normalizes_consumed_variable_offsets() {
        let (fields, source) = decoder_source(19);
        let image = Codec::new(fields.clone()).encode(&source);
        let mut decoder = Decoder::new(Chunk::new_with_capacity(&fields, 0), fields.clone());
        decoder.reset(&image);

        for _ in 0..2 {
            let mut batch = Chunk::new(&fields, 8, 8);
            decoder.decode(&mut batch);
            assert_eq!(batch.num_rows(), 8);
        }
        assert_eq!(decoder.remained_rows(), 3);
        assert!(decoder.intermediate_chunk().column(1).offsets[0] > 0);

        let mut target = Chunk::new(&fields, 0, 64);
        target.set_incomplete_chunk(true);
        decoder.reuse_intermediate_chunk(&mut target);
        assert!(decoder.is_finished());
        assert_eq!(target.required_rows(), 64);
        assert!(target.is_incomplete_chunk());
        assert_eq!(target.num_rows(), 3);
        assert_eq!(target.column(1).offsets[0], 0);
        for row in 0..3 {
            for (column, field) in fields.iter().enumerate().take(2) {
                assert_eq!(
                    target.get_row(row).get_datum(column, field),
                    source.get_row(row + 16).get_datum(column, field)
                );
            }
        }
        assert!(target.column(0).avoid_reusing);
        assert!(target.column(1).avoid_reusing);
    }

    #[test]
    fn decoder_reset_accepts_the_next_response_after_exhaustion() {
        let (fields, first) = decoder_source(3);
        let (_, second) = decoder_source(9);
        let codec = Codec::new(fields.clone());
        let mut decoder = Decoder::new(Chunk::new_with_capacity(&fields, 0), fields.clone());

        decoder.reset(&codec.encode(&first));
        let mut first_target = Chunk::new(&fields, 3, 3);
        decoder.decode(&mut first_target);
        assert!(decoder.is_finished());

        decoder.reset(&codec.encode(&second));
        assert_eq!(decoder.remained_rows(), 9);
        let mut second_target = Chunk::new(&fields, 9, 9);
        decoder.decode(&mut second_target);
        assert_eq!(second_target.num_rows(), 9);
        assert!(decoder.is_finished());
    }

    #[test]
    fn decoder_full_target_consumes_nothing_and_masks_padding() {
        let (fields, source) = decoder_source(1);
        let image = Codec::new(fields.clone()).encode(&source);
        let mut decoder = Decoder::new(Chunk::new_with_capacity(&fields, 0), fields.clone());
        decoder.reset(&image);
        let mut full = Chunk::new(&fields, 1, 1);
        full.append_int64(0, 1);
        full.append_string(1, "full");
        full.column_mut(0).null_bitmap[0] = 0xff;
        full.column_mut(1).null_bitmap[0] = 0xff;
        let remained = decoder.remained_rows();
        decoder.decode(&mut full);
        assert_eq!(decoder.remained_rows(), remained);
        assert_eq!(full.num_rows(), 1);
        assert_eq!(full.column(0).null_bitmap, vec![0x01]);
        assert_eq!(full.column(1).null_bitmap, vec![0x01]);
    }

    #[test]
    fn decoder_bulk_paths_cover_all_bitmap_alignments_and_deficits() {
        let (fields, source) = decoder_source(17);
        let image = Codec::new(fields.clone()).encode(&source);
        for prefix in 0..=8 {
            for deficit in [1usize, 7, 8, 9] {
                let mut decoder =
                    Decoder::new(Chunk::new_with_capacity(&fields, 0), fields.clone());
                decoder.reset(&image);
                let mut target = Chunk::new(&fields, prefix, prefix + deficit);
                for row in 0..prefix {
                    target.append_int64(0, -(row as i64) - 1);
                    target.append_string(1, "prefix");
                }
                decoder.decode(&mut target);
                let decoded = deficit.div_ceil(8).saturating_mul(8).min(17);
                assert_eq!(target.num_rows(), prefix + decoded);
                assert_eq!(decoder.remained_rows(), 17 - decoded);
                for row in 0..decoded {
                    for (column, field) in fields.iter().enumerate() {
                        assert_eq!(
                            target.get_row(prefix + row).get_datum(column, field),
                            source.get_row(row).get_datum(column, field),
                            "prefix={prefix} deficit={deficit} row={row} col={column}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn decoder_reset_ignores_decode_to_chunk_suffix_and_can_return_owner() {
        let (fields, source) = decoder_source(3);
        let mut image = Codec::new(fields.clone()).encode(&source);
        image.extend_from_slice(&[0xaa, 0xbb, 0xcc]);
        let mut decoder = Decoder::new(Chunk::new_with_capacity(&fields, 0), fields.clone());
        decoder.reset(&image);
        assert_eq!(decoder.remained_rows(), 3);
        let intermediate = decoder.into_intermediate_chunk();
        assert_eq!(intermediate.num_rows(), 3);
        for row in 0..3 {
            for (column, field) in fields.iter().enumerate() {
                assert_eq!(
                    intermediate.get_row(row).get_datum(column, field),
                    source.get_row(row).get_datum(column, field)
                );
            }
        }
    }
}
