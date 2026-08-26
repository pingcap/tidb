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

//! Transcreation of Go `pkg/util/chunk/chunk_in_disk.go`: [`DataInDiskByChunks`],
//! the spill file a memory-limited operator writes whole chunks into and reads
//! whole chunks back out of.
//!
//! # The on-disk format is a raw column dump, not the wire codec
//!
//! This is NOT `pkg/util/chunk/codec.go` and NOT the distsql column codec
//! (`tidb-codec`'s `column`): those are cross-process wire formats that
//! re-encode cells. A spill file is read back only by the process that wrote
//! it, so Go dumps the columnar buffers verbatim -- null bitmap, packed data,
//! offsets -- with native-endian fixed-width headers:
//!
//! ```text
//! chunk   header: | numVirtualRows | capacity | requiredRows | selSize | sel... |
//! column1 data:   | length | nullMapSize | dataSize | offsetSize | nullBitmap... | data... | offsets... |
//! ...
//! columnN data:   | length | nullMapSize | dataSize | offsetSize | nullBitmap... | data... | offsets... |
//! ```
//!
//! Every header field is 8 bytes: Go's `int` and `int64` are both 8 bytes on
//! the 64-bit targets TiDB ships, and Go writes them through an
//! `unsafe.Pointer` cast, i.e. native-endian. `chkFixedSize` is `intLen*4`
//! and `colMetaSize` is `int64Len*4`, both 32.
//!
//! `selSize` and `offsetSize` are BYTE counts, not element counts -- Go
//! multiplies the slice length by the element width before storing them, and
//! divides on the way back.
//!
//! The byte layout here is verified against the real Go serializer, not
//! against this port's own reader: the fixtures in this module's tests are
//! hex dumps produced by calling `serializeDataToBuf` in
//! `pkg/util/chunk` and printing the buffer.

use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use tidb_datatype::FieldType;
use tidb_util::disk::{self, SpillStorage, LOCAL_TEMPORARY_SPACE_QUOTA_ERROR};
use tidb_util::layered_io::ReadAt;
use tidb_util::memory::LABEL_FOR_CHUNK_DATA_IN_DISK_BY_CHUNKS;

use crate::chunk::Chunk;
use crate::chunk_util::DiskFileReaderWriter;

/// Go `intLen` = `unsafe.Sizeof(int(0))` on a 64-bit target.
const INT_LEN: usize = 8;
/// Go `int64Len`.
const INT64_LEN: usize = 8;
/// Go `chkFixedSize = intLen * 4`.
pub const CHK_FIXED_SIZE: usize = INT_LEN * 4;
/// Go `colMetaSize = int64Len * 4`.
pub const COL_META_SIZE: usize = INT64_LEN * 4;

/// Go `DefaultChunkDataInDiskByChunksPath`.
pub const DEFAULT_CHUNK_DATA_IN_DISK_BY_CHUNKS_PATH: &str = "defaultChunkDataInDiskByChunksPath";

/// A failure on the spill path.
#[derive(Debug)]
pub enum DiskError {
    /// An underlying filesystem failure.
    Io(io::Error),
    /// One of Go's explicit `errors.New` guards on this path.
    Message(&'static str),
    /// An error recorded earlier and replayed, such as the error a failed
    /// spill leaves in `RowContainer`'s records.
    Owned(String),
    /// Process-wide local temporary-storage quota was reached.
    QuotaExceeded,
    /// Go `ErrCannotAddBecauseSorted`.
    CannotAddBecauseSorted,
}

impl From<io::Error> for DiskError {
    fn from(error: io::Error) -> Self {
        DiskError::Io(error)
    }
}

impl std::fmt::Display for DiskError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DiskError::Io(error) => error.fmt(formatter),
            DiskError::Message(message) => formatter.write_str(message),
            DiskError::Owned(message) => formatter.write_str(message),
            DiskError::QuotaExceeded => formatter.write_str(LOCAL_TEMPORARY_SPACE_QUOTA_ERROR),
            DiskError::CannotAddBecauseSorted => formatter.write_str("can not add because sorted"),
        }
    }
}

impl std::error::Error for DiskError {}

fn put_i64(buf: &mut Vec<u8>, value: i64) {
    buf.extend_from_slice(&value.to_ne_bytes());
}

fn get_i64(buf: &[u8], pos: &mut usize) -> i64 {
    let value = i64::from_ne_bytes(buf[*pos..*pos + INT64_LEN].try_into().expect("8 bytes"));
    *pos += INT64_LEN;
    value
}

/// Go `serializeDataToBuf` (plus `serializeChunkData`/`serializeColumns`/
/// `serializeColMeta`/`serializeOffset`, which only exist to split the write):
/// fills `buf` with `chk`'s on-disk image and returns its length.
///
/// Go recomputes `totalBytes` up front purely to size the buffer once; the
/// return value is the byte count `Add` then checks the write against.
pub fn serialize_data_to_buf(chk: &Chunk, buf: &mut Vec<u8>) -> i64 {
    let sel_size = chk.sel.as_ref().map_or(0, Vec::len) * INT_LEN;
    let mut total_bytes = CHK_FIXED_SIZE + sel_size;
    for col in &chk.columns {
        let col = col.read();
        total_bytes +=
            COL_META_SIZE + col.null_bitmap.len() + col.data.len() + col.offsets.len() * INT64_LEN;
    }

    buf.clear();
    buf.reserve(total_bytes);

    // Go stores these three as `int`; they are non-negative row counts, so the
    // i64 image is the same bit pattern Go writes.
    put_i64(buf, chk.num_virtual_rows as i64);
    put_i64(buf, chk.capacity as i64);
    put_i64(buf, chk.required_rows as i64);
    put_i64(buf, sel_size as i64);
    if let Some(sel) = &chk.sel {
        for &row in sel {
            put_i64(buf, row as i64);
        }
    }

    for col in &chk.columns {
        let col = col.read();
        put_i64(buf, col.length as i64);
        put_i64(buf, col.null_bitmap.len() as i64);
        put_i64(buf, col.data.len() as i64);
        put_i64(buf, (col.offsets.len() * INT64_LEN) as i64);
        buf.extend_from_slice(&col.null_bitmap);
        let data = col.data.read();
        buf.extend_from_slice(data.as_ref());
        for &offset in &col.offsets {
            put_i64(buf, offset);
        }
    }

    debug_assert_eq!(buf.len(), total_bytes);
    total_bytes as i64
}

/// Go `deserializeDataToChunk`: refills `chk`'s columns from `buf`.
///
/// `chk` supplies the column TYPES; the image supplies everything else. Go
/// calls this on a `NewEmptyChunk(fieldTypes)` (`GetChunk`) or on a caller's
/// chunk (`FillChunk`), and does not check that the column count matches --
/// a mismatch is a caller bug, and here it panics on the slice bound rather
/// than reading a neighbouring column's bytes.
pub fn deserialize_data_to_chunk(chk: &mut Chunk, buf: &[u8]) {
    let mut pos = 0usize;
    chk.num_virtual_rows = get_i64(buf, &mut pos) as usize;
    chk.capacity = get_i64(buf, &mut pos) as usize;
    chk.required_rows = get_i64(buf, &mut pos) as usize;
    let sel_size = get_i64(buf, &mut pos) as usize;
    // FAITHFUL: Go only touches `chk.sel` when `selSize != 0`, so refilling a
    // chunk that carried a selection with an image that has none LEAVES the
    // old selection in place. Reproduced rather than "fixed" because Go's
    // `FillChunk` callers rely on the destination they pass in.
    if sel_size != 0 {
        let sel_len = sel_size / INT_LEN;
        let mut sel = Vec::with_capacity(sel_len);
        for _ in 0..sel_len {
            sel.push(get_i64(buf, &mut pos) as usize);
        }
        chk.sel = Some(sel);
    }

    for col in &mut chk.columns {
        let mut col = col.write();
        let length = get_i64(buf, &mut pos);
        let null_map_size = get_i64(buf, &mut pos) as usize;
        let data_size = get_i64(buf, &mut pos) as usize;
        let offset_size = get_i64(buf, &mut pos) as usize;

        col.length = length as usize;
        col.null_bitmap.clear();
        col.null_bitmap
            .extend_from_slice(&buf[pos..pos + null_map_size]);
        pos += null_map_size;
        col.data.reset();
        col.data.extend_from_slice(&buf[pos..pos + data_size]);
        pos += data_size;
        col.offsets.clear();
        for _ in 0..offset_size / INT64_LEN {
            col.offsets.push(get_i64(buf, &mut pos));
        }
    }
}

/// Go `DataInDiskByChunks`: whole chunks spilled to one temporary file,
/// addressed by chunk index.
pub struct DataInDiskByChunks {
    field_types: Vec<FieldType>,
    offset_of_each_chunk: Vec<i64>,
    total_data_size: i64,
    total_row_num: i64,
    disk_tracker: Arc<disk::Tracker>,
    storage: Arc<SpillStorage>,
    data_file: DiskFileReaderWriter,
    /// Go `buf`: one scratch buffer reused by every write and read.
    buf: Vec<u8>,
    file_name_prefix_for_test: String,
}

impl DataInDiskByChunks {
    /// Go `NewDataInDiskByChunks`.
    #[must_use]
    pub fn new(
        field_types: Vec<FieldType>,
        file_name_prefix_for_test: &str,
        storage: Arc<SpillStorage>,
    ) -> Self {
        DataInDiskByChunks {
            field_types,
            offset_of_each_chunk: Vec::new(),
            total_data_size: 0,
            total_row_num: 0,
            // Go: "TODO: set the quota of disk usage."
            disk_tracker: disk::new_tracker(LABEL_FOR_CHUNK_DATA_IN_DISK_BY_CHUNKS, -1),
            storage,
            data_file: DiskFileReaderWriter::default(),
            buf: Vec::with_capacity(4096),
            file_name_prefix_for_test: file_name_prefix_for_test.to_owned(),
        }
    }

    /// Go `initDiskFile`.
    fn init_disk_file(&mut self) -> Result<(), DiskError> {
        let name = format!(
            "{}{}{}",
            self.file_name_prefix_for_test,
            DEFAULT_CHUNK_DATA_IN_DISK_BY_CHUNKS_PATH,
            self.disk_tracker.label()
        );
        self.data_file.init_with_file_name(&self.storage, &name)?;
        Ok(())
    }

    /// Go `GetDiskTracker`.
    #[must_use]
    pub fn disk_tracker(&self) -> &Arc<disk::Tracker> {
        &self.disk_tracker
    }

    /// Go `Add`: serializes `chk` and appends it to the spill file.
    ///
    /// Not safe to call concurrently -- Go says so too, and for the same
    /// reason: one shared `buf` and one append cursor.
    pub fn add(&mut self, chk: &Chunk) -> Result<(), DiskError> {
        if chk.num_rows() == 0 {
            return Err(DiskError::Message(
                "Chunk spilled to disk should have at least 1 row",
            ));
        }
        if !self.data_file.is_open() {
            self.init_disk_file()?;
        }

        let serialized_bytes_num = serialize_data_to_buf(chk, &mut self.buf);
        let write_num = self.data_file.write(&self.buf)?;
        if write_num as i64 != serialized_bytes_num {
            return Err(DiskError::Message("Some data fail to be spilled to disk"));
        }
        self.offset_of_each_chunk.push(self.total_data_size);
        self.total_data_size += serialized_bytes_num;
        self.total_row_num += chk.num_rows() as i64;

        if self
            .disk_tracker
            .consume_and_check_exceed(serialized_bytes_num)
        {
            return Err(DiskError::QuotaExceeded);
        }
        Ok(())
    }

    /// Go `GetTotalBytesInDisk`.
    #[must_use]
    pub fn total_bytes_in_disk(&self) -> i64 {
        self.total_data_size
    }

    /// Go `getChunkSize`.
    fn chunk_size(&self, chk_idx: usize) -> i64 {
        if chk_idx == self.offset_of_each_chunk.len() - 1 {
            self.total_data_size - self.offset_of_each_chunk[chk_idx]
        } else {
            self.offset_of_each_chunk[chk_idx + 1] - self.offset_of_each_chunk[chk_idx]
        }
    }

    /// Go `readFromFisk` (Go's spelling): refills `buf` with chunk `chk_idx`.
    fn read_from_disk(&mut self, chk_idx: usize) -> Result<(), DiskError> {
        let size = self.chunk_size(chk_idx) as usize;
        self.buf.clear();
        self.buf.resize(size, 0);
        // Go wraps the reader in an `io.SectionReader` at the chunk's offset
        // and `io.ReadFull`s it; the reader stack is positional either way.
        let read = self
            .data_file
            .read_full_at(&mut self.buf, self.offset_of_each_chunk[chk_idx])?;
        if read != size {
            return Err(DiskError::Message("Fail to restore the spilled chunk"));
        }
        Ok(())
    }

    /// Go `GetChunk`.
    pub fn get_chunk(&mut self, chk_idx: usize) -> Result<Chunk, DiskError> {
        self.read_from_disk(chk_idx)?;
        let mut chk = Chunk::new_empty(&self.field_types);
        deserialize_data_to_chunk(&mut chk, &self.buf);
        Ok(chk)
    }

    /// Go `FillChunk`.
    pub fn fill_chunk(&mut self, src_chk_idx: usize, dest: &mut Chunk) -> Result<(), DiskError> {
        self.read_from_disk(src_chk_idx)?;
        deserialize_data_to_chunk(dest, &self.buf);
        Ok(())
    }

    /// Go `NumRows`.
    #[must_use]
    pub fn num_rows(&self) -> i64 {
        self.total_row_num
    }

    /// Go `NumChunks`.
    #[must_use]
    pub fn num_chunks(&self) -> usize {
        self.offset_of_each_chunk.len()
    }

    /// Go `Close`: releases the tracked disk bytes, closes the file and
    /// REMOVES it. Also runs from `Drop`, so a spill file cannot outlive its
    /// container even on an early return.
    pub fn close(&mut self) {
        if self.data_file.is_open() {
            self.disk_tracker
                .consume(-self.disk_tracker.bytes_consumed());
            self.data_file.close();
        }
    }

    /// The spill file's path, for tests that must prove disk was used.
    #[must_use]
    pub fn file_path(&self) -> Option<&PathBuf> {
        self.data_file.path()
    }
}

impl Drop for DataInDiskByChunks {
    fn drop(&mut self) {
        self.close();
    }
}

/// Reads exactly `destination.len()` bytes at `offset` through `reader`,
/// Go's `io.ReadFull` over an `io.SectionReader`.
pub(crate) fn read_full_at(
    reader: &dyn ReadAt,
    destination: &mut [u8],
    offset: i64,
) -> io::Result<usize> {
    let mut total = 0;
    while total < destination.len() {
        let result = reader.read_at(&mut destination[total..], offset + total as i64);
        total += result.n;
        if total == destination.len() {
            return Ok(total);
        }
        if let Some(error) = result.error {
            return match error {
                tidb_util::layered_io::ReadAtError::Eof => {
                    Err(io::Error::new(io::ErrorKind::UnexpectedEof, error))
                }
                tidb_util::layered_io::ReadAtError::Io(error) => Err(error),
            };
        }
        if result.n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "positional reader made no progress before filling the destination",
            ));
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::Chunk;
    use tidb_datatype::FieldTypeCode;
    use tidb_util::layered_io::{ReadAtError, ReadAtResult};

    struct BytesReader(&'static [u8]);

    impl ReadAt for BytesReader {
        fn read_at(&self, destination: &mut [u8], offset: i64) -> ReadAtResult {
            let Ok(offset) = usize::try_from(offset) else {
                return ReadAtResult::io(
                    0,
                    io::Error::new(io::ErrorKind::InvalidInput, "negative offset"),
                );
            };
            if offset >= self.0.len() {
                return ReadAtResult::eof(0);
            }
            let count = destination.len().min(self.0.len() - offset);
            destination[..count].copy_from_slice(&self.0[offset..offset + count]);
            if count == destination.len() {
                ReadAtResult::ok(count)
            } else {
                ReadAtResult {
                    n: count,
                    error: Some(ReadAtError::Eof),
                }
            }
        }
    }

    #[test]
    fn read_full_at_rejects_a_short_spill_read() {
        let mut destination = [0x7f; 4];
        let error = read_full_at(&BytesReader(&[1, 2]), &mut destination, 0)
            .expect_err("a truncated spill image must not decode as zero-filled data");
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(destination, [1, 2, 0x7f, 0x7f]);

        let mut exact = [0; 2];
        assert_eq!(
            read_full_at(&BytesReader(&[3, 4]), &mut exact, 0).expect("exact read"),
            2
        );
        assert_eq!(exact, [3, 4]);
    }

    const ENCRYPTED_GO_VECTORS: &str =
        include_str!("../../../difftests/chunk-tests/fixtures/encrypted_spill_vectors.tsv");

    fn encrypted_go_vector(key: &str) -> &'static str {
        ENCRYPTED_GO_VECTORS
            .lines()
            .find_map(|line| line.strip_prefix(key)?.strip_prefix('\t'))
            .unwrap_or_else(|| panic!("encrypted fixture line {key} missing"))
    }

    fn bytes_of(hex: &str) -> Vec<u8> {
        hex.as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                u8::from_str_radix(std::str::from_utf8(pair).expect("hex pair"), 16)
                    .expect("hex byte")
            })
            .collect()
    }

    fn fixture_cipher(key: &str) -> tidb_util::encrypt::CtrCipher {
        let specification = encrypted_go_vector(key);
        let (key_hex, nonce_hex) = specification
            .strip_prefix("key=")
            .and_then(|value| value.split_once(",nonce="))
            .expect("key and nonce fixture");
        let key: [u8; 16] = bytes_of(key_hex).try_into().expect("AES-128 key");
        let nonce: [u8; 8] = bytes_of(nonce_hex).try_into().expect("CTR nonce");
        tidb_util::encrypt::CtrCipher::new_for_test(key, u64::from_be_bytes(nonce))
            .expect("fixture cipher")
    }

    /// Hex dumps of `d.buf` after `serializeDataToBuf`, produced by the REAL
    /// Go serializer in `pkg/util/chunk/chunk_in_disk.go` (a throwaway test in
    /// that package, run through `go test -overlay`), not by this port. Each
    /// case is reproduced below by building the same chunk here.
    ///
    /// Note what the `int64` fixture proves about `AppendNull` on a
    /// fixed-length column: the null row's 8 data bytes are the PREVIOUS
    /// value, because Go appends the scratch `elemBuf` without clearing it.
    /// A port that zeroed them would produce a different file.
    const GO_INT64: &str = "000000000000000008000000000000000800000000000000000000000000000003000000000000000100000000000000180000000000000000000000000000000501000000000000000100000000000000feffffffffffffff";
    const GO_VARCHAR: &str = "0000000000000000080000000000000008000000000000000000000000000000030000000000000001000000000000000700000000000000200000000000000005616263646566670000000000000000020000000000000002000000000000000700000000000000";
    const GO_MIXED_SEL: &str = "1d0000000000000011000000000000001700000000000000180000000000000000000000000000000200000000000000040000000000000005000000000000000100000000000000280000000000000000000000000000001f0000000000000000e803000000000000d007000000000000b80b000000000000a00f000000000000050000000000000001000000000000000a0000000000000030000000000000001f73307331733273337334000000000000000002000000000000000400000000000000060000000000000008000000000000000a0000000000000005000000000000000100000000000000280000000000000000000000000000001f000000000000e03f000000000000f83f00000000000004400000000000000c400000000000001240";
    const GO_ZERO_ROWS: &str = "00000000000000000800000000000000080000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000";

    fn hex(bytes: impl AsRef<[u8]>) -> String {
        bytes.as_ref().iter().map(|b| format!("{b:02x}")).collect()
    }

    fn int64_fields() -> Vec<FieldType> {
        vec![FieldType::new(FieldTypeCode::LongLong)]
    }

    fn mixed_fields() -> Vec<FieldType> {
        vec![
            FieldType::new(FieldTypeCode::LongLong),
            FieldType::new(FieldTypeCode::Varchar),
            FieldType::new(FieldTypeCode::Double),
        ]
    }

    fn int64_chunk() -> Chunk {
        let mut chk = Chunk::new_with_capacity(&int64_fields(), 8);
        chk.append_int64(0, 1);
        chk.append_null(0);
        chk.append_int64(0, -2);
        chk
    }

    fn varchar_chunk() -> Chunk {
        let mut chk = Chunk::new_with_capacity(&[FieldType::new(FieldTypeCode::Varchar)], 8);
        chk.append_string(0, "ab");
        chk.append_null(0);
        chk.append_string(0, "cdefg");
        chk
    }

    fn mixed_sel_chunk() -> Chunk {
        let mut chk = Chunk::new_with_capacity(&mixed_fields(), 8);
        for i in 0..5i64 {
            chk.append_int64(0, i * 1000);
            chk.append_string(1, format!("s{i}"));
            chk.append_float64(2, i as f64 + 0.5);
        }
        chk.capacity = 17;
        chk.required_rows = 23;
        chk.num_virtual_rows = 29;
        chk.sel = Some(vec![0, 2, 4]);
        chk
    }

    #[test]
    fn serialized_bytes_match_go() {
        for (name, chk, expected) in [
            ("int64", int64_chunk(), GO_INT64),
            ("varchar", varchar_chunk(), GO_VARCHAR),
            ("mixed_sel", mixed_sel_chunk(), GO_MIXED_SEL),
            (
                "zero_rows",
                Chunk::new_with_capacity(&int64_fields(), 8),
                GO_ZERO_ROWS,
            ),
        ] {
            let mut buf = Vec::new();
            let n = serialize_data_to_buf(&chk, &mut buf);
            assert_eq!(n as usize, buf.len(), "{name}: returned length");
            assert_eq!(hex(&buf), expected, "{name}: bytes differ from Go");
        }
    }

    #[test]
    fn deserialize_reads_the_go_image_back() {
        // Decode the GO fixture bytes -- not bytes this port produced -- so a
        // reader bug cannot be cancelled out by a matching writer bug.
        let go_bytes: Vec<u8> = (0..GO_MIXED_SEL.len() / 2)
            .map(|i| u8::from_str_radix(&GO_MIXED_SEL[i * 2..i * 2 + 2], 16).expect("hex"))
            .collect();
        let mut chk = Chunk::new_empty(&mixed_fields());
        deserialize_data_to_chunk(&mut chk, &go_bytes);

        assert_eq!(chk.capacity, 17);
        assert_eq!(chk.required_rows, 23);
        assert_eq!(chk.num_virtual_rows, 29);
        assert_eq!(chk.sel, Some(vec![0, 2, 4]));
        // `num_rows` is selection-aware, so it reports the 3 selected rows.
        assert_eq!(chk.num_rows(), 3);
        assert_eq!(chk.column(0).rows(), 5);
        for i in 0..5usize {
            let row = chk.column(0);
            assert_eq!(row.get_int64(i), i as i64 * 1000);
            assert_eq!(chk.column(1).get_bytes(i), format!("s{i}").as_bytes());
            assert!((chk.column(2).get_float64(i) - (i as f64 + 0.5)).abs() < f64::EPSILON);
        }
    }

    use crate::test_temp_storage::isolated_storage;
    use tidb_util::disk::SpillEncryptionMethod;

    /// Builds chunk `c` of `chunks` chunks x `rows` rows, deterministically.
    fn payload_chunk(fields: &[FieldType], c: usize, rows: usize) -> Chunk {
        let mut chk = Chunk::new_with_capacity(fields, rows);
        for r in 0..rows {
            let n = (c * 1000 + r) as i64;
            chk.append_int64(0, n);
            if r % 7 == 3 {
                chk.append_null(1);
            } else {
                chk.append_string(1, format!("row-{c}-{r}-{}", "x".repeat(r % 11)));
            }
            chk.append_float64(2, n as f64 / 3.0);
        }
        chk
    }

    fn encrypted_chunks() -> (Vec<FieldType>, Vec<Chunk>) {
        let fields = vec![
            FieldType::new(FieldTypeCode::Varchar),
            FieldType::new(FieldTypeCode::LongLong),
        ];
        let mut chunks = Vec::new();
        for chunk_idx in 0..9usize {
            let mut chk = Chunk::new_with_capacity(&fields, 32);
            for row_idx in 0..32usize {
                let ordinal = chunk_idx * 32 + row_idx;
                chk.append_string(0, "x".repeat(ordinal % 31 + 1));
                if ordinal % 11 == 5 {
                    chk.append_null(1);
                } else {
                    chk.append_int64(1, (ordinal * 17) as i64 - 9);
                }
            }
            chunks.push(chk);
        }
        (fields, chunks)
    }

    fn render_encrypted_chunks(container: &mut DataInDiskByChunks) -> String {
        let mut parts = Vec::new();
        for chunk_idx in 0..container.num_chunks() {
            let chk = container.get_chunk(chunk_idx).expect("get_chunk");
            for row_idx in 0..chk.num_rows() {
                let row = chk.get_row(row_idx);
                let second = if row.is_null(1) {
                    "NULL".to_owned()
                } else {
                    hex(row.get_raw(1))
                };
                parts.push(format!("{},{}", hex(row.get_raw(0)), second));
            }
        }
        parts.join("|")
    }

    /// A real spill: writes chunks to a real file in a real directory, proves
    /// the file exists and is non-empty, reads every row back, and proves the
    /// file is gone after `close`.
    #[test]
    fn data_in_disk_by_chunks_round_trips_through_a_real_file() {
        let storage = isolated_storage("bychunks", SpillEncryptionMethod::Plaintext);
        let dir = storage.path().to_path_buf();

        let fields = mixed_fields();
        // 40 chunks x 64 rows is far past the 1KiB checksum block, so the read
        // path exercises both flushed blocks and the writer's live cache.
        let (num_chunks, num_rows) = (40usize, 64usize);
        let mut container =
            DataInDiskByChunks::new(fields.clone(), "roundtrip", Arc::clone(&storage));
        for c in 0..num_chunks {
            container
                .add(&payload_chunk(&fields, c, num_rows))
                .expect("add");
        }

        // DISK WAS ACTUALLY USED: the file exists and holds bytes.
        let path = container.file_path().cloned().expect("spill file created");
        assert!(path.exists(), "spill file {path:?} must exist");
        let on_disk = std::fs::metadata(&path).expect("stat spill file").len();
        assert!(on_disk > 0, "spill file must not be empty");
        assert_eq!(container.num_chunks(), num_chunks);
        assert_eq!(container.num_rows() as usize, num_chunks * num_rows);
        assert!(container.total_bytes_in_disk() > 0);
        assert_eq!(
            container.disk_tracker().bytes_consumed(),
            container.total_bytes_in_disk()
        );

        // Every row comes back, in order, with the right values -- including
        // the tail chunk, which is still sitting in the checksum writer's
        // unflushed cache.
        for c in 0..num_chunks {
            let want = payload_chunk(&fields, c, num_rows);
            let got = container.get_chunk(c).expect("get_chunk");
            assert_eq!(got.num_rows(), num_rows, "chunk {c} row count");
            for r in 0..num_rows {
                assert_eq!(got.column(0).get_int64(r), want.column(0).get_int64(r));
                assert_eq!(got.column(1).is_null(r), want.column(1).is_null(r));
                if !want.column(1).is_null(r) {
                    assert_eq!(got.column(1).get_bytes(r), want.column(1).get_bytes(r));
                }
                assert!(
                    (got.column(2).get_float64(r) - want.column(2).get_float64(r)).abs()
                        < f64::EPSILON
                );
            }
        }

        // `fill_chunk` refills a caller's chunk with the same content.
        let mut dest = Chunk::new_empty(&fields);
        container.fill_chunk(7, &mut dest).expect("fill_chunk");
        assert_eq!(dest.column(0).get_int64(0), 7000);

        container.close();
        assert!(!path.exists(), "close must remove the spill file");
        drop(container);
        drop(storage);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn encrypted_chunk_file_matches_go_and_reads_through_both_live_caches() {
        let storage = isolated_storage("encrypted-chunks", SpillEncryptionMethod::Plaintext);
        let dir = storage.path().to_path_buf();

        let (fields, chunks) = encrypted_chunks();
        let mut container = DataInDiskByChunks::new(fields, "oracle", Arc::clone(&storage));
        container
            .data_file
            .init_with_file_name_and_cipher(
                &storage,
                "oracle-encrypted-chunks",
                fixture_cipher("chunks.data.cipher"),
            )
            .expect("data file");
        for chk in &chunks {
            container.add(chk).expect("add");
        }

        let data = std::fs::read(container.data_file.path().expect("data path")).expect("read");
        assert_eq!(hex(&data), encrypted_go_vector("chunks.data"));
        assert_eq!(
            render_encrypted_chunks(&mut container),
            encrypted_go_vector("chunks.readback")
        );

        drop(container);
        drop(storage);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn authority_aes_mode_selects_the_encrypted_stack() {
        let storage = isolated_storage("encrypted-mode", SpillEncryptionMethod::Aes128Ctr);
        let dir = storage.path().to_path_buf();

        let fields = int64_fields();
        let mut container = DataInDiskByChunks::new(fields, "mode", Arc::clone(&storage));
        container.add(&int64_chunk()).expect("add");
        assert!(container.data_file.is_encrypted());
        assert_eq!(container.get_chunk(0).expect("get_chunk").num_rows(), 3);

        drop(container);
        drop(storage);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Go returns an explicit error rather than writing a zero-row chunk.
    #[test]
    fn adding_an_empty_chunk_is_refused() {
        let storage = isolated_storage("emptychunk", SpillEncryptionMethod::Plaintext);
        let dir = storage.path().to_path_buf();
        let fields = int64_fields();
        let mut container = DataInDiskByChunks::new(fields.clone(), "empty", Arc::clone(&storage));
        let error = container
            .add(&Chunk::new_with_capacity(&fields, 4))
            .expect_err("a zero-row chunk must be refused");
        assert_eq!(
            error.to_string(),
            "Chunk spilled to disk should have at least 1 row"
        );
        assert!(container.file_path().is_none(), "no file for a refused add");
        drop(container);
        drop(storage);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Dropping without an explicit `close` still removes the file.
    #[test]
    fn drop_removes_the_spill_file() {
        let storage = isolated_storage("dropfile", SpillEncryptionMethod::Plaintext);
        let dir = storage.path().to_path_buf();
        let fields = int64_fields();
        let path = {
            let mut container =
                DataInDiskByChunks::new(fields.clone(), "dropped", Arc::clone(&storage));
            container.add(&int64_chunk()).expect("add");
            container.file_path().cloned().expect("spill file")
        };
        assert!(!path.exists(), "drop must remove the spill file");
        drop(storage);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn process_quota_error_is_fallible_and_cleanup_releases_usage() {
        let storage = crate::test_temp_storage::isolated_storage_with_quota(
            "quota",
            SpillEncryptionMethod::Plaintext,
            1,
        );
        let fields = int64_fields();
        let mut container = DataInDiskByChunks::new(fields, "quota", Arc::clone(&storage));
        container
            .disk_tracker()
            .attach_to_global_tracker(storage.global_tracker());

        let error = container
            .add(&int64_chunk())
            .expect_err("serialized chunk reaches the one-byte process quota");
        assert!(matches!(error, DiskError::QuotaExceeded));
        assert_eq!(
            error.to_string(),
            tidb_util::disk::LOCAL_TEMPORARY_SPACE_QUOTA_ERROR
        );
        assert!(storage.global_tracker().bytes_consumed() > 0);

        container.close();
        assert_eq!(storage.global_tracker().bytes_consumed(), 0);
    }

    /// Go `genString` (`row_in_disk_test.go:25`): "西xi瓜gua" doubled a random
    /// number of times. A fixed factor keeps the port deterministic while
    /// still exercising multi-byte string payloads.
    fn gen_string(factor: usize) -> String {
        "西xi瓜gua".repeat(1 << factor)
    }

    /// Go `initChunks` (`row_in_disk_test.go:46`), with `addAuxDataForChunks`
    /// (`chunk_in_disk_test.go:28`) folded in: every chunk carries string,
    /// two NULLs, an int64, and (on even chunks) a JSON cell, plus random
    /// aux data -- capacity/requiredRows/numVirtualRows in [0,100) and a
    /// selection of 1..=50 pseudo-random entries.
    fn init_chunks_with_aux(num_chk: usize, num_row: usize) -> (Vec<Chunk>, Vec<FieldType>) {
        let fields = vec![
            FieldType::new(FieldTypeCode::Varchar),
            FieldType::new(FieldTypeCode::LongLong),
            FieldType::new(FieldTypeCode::Varchar),
            FieldType::new(FieldTypeCode::LongLong),
            FieldType::new(FieldTypeCode::Json),
        ];
        // Go `math/rand` is replaced by a xorshift sequence; only variety,
        // not reproducibility with Go, matters here.
        let mut state: u64 = 0x9E3779B97F4A7C15;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };

        let mut chunks = Vec::with_capacity(num_chk);
        for chk_idx in 0..num_chk {
            let mut chk = Chunk::new_with_capacity(&fields, num_row);
            for row_idx in 0..num_row {
                let data = (chk_idx * num_row + row_idx) as i64;
                chk.append_string(0, gen_string((next() % 5) as usize));
                chk.append_null(1);
                chk.append_null(2);
                chk.append_int64(3, data);
                if chk_idx % 2 == 0 {
                    let s = gen_string((next() % 5) as usize);
                    chk.append_json(4, &tidb_datatype::BinaryJSON::parse(&format!("\"{s}\"")).unwrap());
                } else {
                    chk.append_null(4);
                }
            }
            chk.capacity = (next() % 100) as usize;
            chk.required_rows = (next() % 100) as usize;
            chk.num_virtual_rows = (next() % 100) as usize;
            let sel_len = (next() % 50 + 1) as usize;
            chk.sel = Some((0..sel_len).map(|_| (next() % 1_000_000) as usize).collect());
            chunks.push(chk);
        }
        (chunks, fields)
    }

    /// Go `checkAuxDataForChunk`: the on-disk image carries the chunk's aux
    /// metadata verbatim.
    fn check_aux_data(chk1: &Chunk, chk2: &Chunk) {
        assert_eq!(chk1.capacity, chk2.capacity);
        assert_eq!(chk1.required_rows, chk2.required_rows);
        assert_eq!(chk1.num_virtual_rows, chk2.num_virtual_rows);
        assert_eq!(chk1.sel, chk2.sel);
    }

    /// Go `checkRow`, applied column-wise over raw storage indices so the
    /// random selection vector cannot reorder what we compare. (Go's own
    /// `checkChunk` strips the aux data before comparing rows, which makes its
    /// row loop vacuous because `NumRows` reads `sel`; comparing columns over
    /// physical indices subsumes that check and actually pins the payload.)
    fn check_chunk_payload(chk1: &Chunk, chk2: &Chunk) {
        assert_eq!(chk1.column(0).rows(), chk2.column(0).rows());
        let num_rows = chk1.column(0).rows();
        for i in 0..num_rows {
            assert_eq!(chk1.column(0).get_bytes(i), chk2.column(0).get_bytes(i));
            assert!(chk1.column(1).is_null(i));
            assert!(chk2.column(1).is_null(i));
            assert!(chk1.column(2).is_null(i));
            assert!(chk2.column(2).is_null(i));
            assert_eq!(chk1.column(3).get_int64(i), chk2.column(3).get_int64(i));
            if !chk1.column(4).is_null(i) {
                assert_eq!(
                    chk1.column(4).get_json(i).to_string(),
                    chk2.column(4).get_json(i).to_string()
                );
            } else {
                assert!(chk2.column(4).is_null(i));
            }
        }
    }

    /// Go `TestDataInDiskByChunks` (`pkg/util/chunk/chunk_in_disk_test.go:103`):
    /// both read paths -- `GetChunk` into a fresh chunk and `FillChunk` into a
    /// reset, reused destination -- restore 100 spilled chunks including their
    /// aux metadata (capacity/requiredRows/numVirtualRows/sel).
    #[test]
    fn go_test_data_in_disk_by_chunks() {
        let (num_chk, num_row) = (100usize, 1000usize);
        let (chunks, fields) = init_chunks_with_aux(num_chk, num_row);
        let storage = isolated_storage("go_test_data_in_disk_by_chunks", SpillEncryptionMethod::Plaintext);
        let mut data_in_disk_by_chunks =
            DataInDiskByChunks::new(fields.clone(), "", Arc::clone(&storage));

        for chk in &chunks {
            data_in_disk_by_chunks.add(chk).expect("add");
        }

        // Go testImpl(isNewChunk = true): GetChunk into a fresh chunk.
        for i in 0..num_chk {
            let got = data_in_disk_by_chunks.get_chunk(i).expect("get_chunk");
            check_aux_data(&got, &chunks[i]);
            check_chunk_payload(&got, &chunks[i]);
        }

        // Go testImpl(isNewChunk = false): FillChunk into a reset chunk.
        let mut chk = Chunk::new_empty(&fields);
        for i in 0..num_chk {
            chk.reset();
            data_in_disk_by_chunks.fill_chunk(i, &mut chk).expect("fill_chunk");
            check_aux_data(&chk, &chunks[i]);
            check_chunk_payload(&chk, &chunks[i]);
        }

        data_in_disk_by_chunks.close();
    }
}
