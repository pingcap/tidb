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

//! `pkg/util/chunk/row_in_disk.go`: the ROW-addressed spill container.
//!
//! [`DataInDiskByRows`] writes each row of each added chunk as
//!
//! ```text
//! [8-byte size of col0] .. [8-byte size of colN]   (-1 == null)
//! [raw bytes of col0]   .. [raw bytes of colN]     (nulls contribute nothing)
//! ```
//!
//! into one temporary file, and the byte offset of every row into a SECOND
//! file, eight bytes per row. A [`RowPtr`] is then answered by reading the
//! row's offset out of the offset file and the row itself out of the data
//! file -- random access, which is what the chunk-addressed
//! [`crate::chunk_in_disk::DataInDiskByChunks`] cannot do.
//!
//! Both files go through [`DiskFileReaderWriter`], so every write passes the
//! checksum layer: what lands on disk is 1024-byte payload blocks each
//! prefixed by its four-byte CRC32-Castagnoli. [`ReaderWithCache`] exists
//! because the tail of the stream sits in the writer's block buffer and has
//! NOT reached the file, yet must still be readable.
//!
//! The `aes128-ctr` writer/reader stack is ported in
//! [`crate::chunk_util::DiskFileReaderWriter`]. It preserves both live caches:
//! checksum payload not yet framed and framed plaintext not yet encrypted to
//! disk. Go-written deterministic file images below pin both layers.

use std::io;

use tidb_util::disk;
use tidb_util::layered_io::{ReadAt, ReadAtResult};
use tidb_util::memory::LABEL_FOR_CHUNK_DATA_IN_DISK_BY_ROWS;

use crate::chunk::Chunk;
use crate::chunk_in_disk::DiskError;
use crate::chunk_util::DiskFileReaderWriter;
use crate::list::RowPtr;
use crate::row::Row;
use tidb_datatype::FieldType;

/// Go `defaultChunkDataInDiskByRowsPath`.
const DEFAULT_DATA_IN_DISK_BY_ROWS_PATH: &str = "chunk.DataInDiskByRows";
/// Go `defaultChunkDataInDiskByRowsOffsetPath`.
const DEFAULT_DATA_IN_DISK_BY_ROWS_OFFSET_PATH: &str = "chunk.DataInDiskByRowsOffset";

/// Go `i64SliceToBytes`: Go reinterprets the slice's memory, which is
/// native-endian. Every platform this runs on is little-endian, and the bytes
/// are compared against Go's own spill files, so the encoding is spelled out
/// rather than reinterpreted.
fn i64_slice_to_bytes(values: &[i64], out: &mut Vec<u8>) {
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
}

/// Go `bytesToI64Slice`, one element.
fn bytes_to_i64(bytes: &[u8]) -> i64 {
    i64::from_le_bytes(bytes.try_into().expect("8 bytes"))
}

/// Go `diskFormatRow`: one row's column sizes (`-1` meaning null) and the raw
/// bytes of its not-null columns.
#[derive(Default)]
struct DiskFormatRow {
    sizes_of_columns: Vec<i64>,
    cells: Vec<Vec<u8>>,
}

impl DiskFormatRow {
    /// Go `convertFromRow`: the shallow copy Go makes is a real copy here, so
    /// the serialized row does not borrow the source chunk.
    fn convert_from_row(row: Row<'_>, reuse: &mut DiskFormatRow) {
        reuse.sizes_of_columns.clear();
        reuse.cells.clear();
        for col_idx in 0..row.chunk().num_cols() {
            if row.is_null(col_idx) {
                reuse.sizes_of_columns.push(-1);
            } else {
                let cell = row.get_raw(col_idx);
                reuse.sizes_of_columns.push(cell.len() as i64);
                reuse.cells.push(cell.to_vec());
            }
        }
    }

    /// Go `rowInDisk.WriteTo`: sizes first, then the cells back to back.
    fn write_to(&self, out: &mut Vec<u8>) {
        i64_slice_to_bytes(&self.sizes_of_columns, out);
        for cell in &self.cells {
            out.extend_from_slice(cell);
        }
    }

    /// Go `diskFormatRow.toRow`'s append half. Destination selection and the
    /// nil/full replacement policy live at the public container boundary.
    fn to_row(&self, chk: &mut Chunk) -> usize {
        let mut cell_off = 0;
        for (col_idx, size) in self.sizes_of_columns.iter().enumerate() {
            let col = chk.column_mut(col_idx);
            if *size == -1 {
                col.append_null();
            } else {
                col.append_raw_cell(&self.cells[cell_off]);
                cell_off += 1;
            }
        }
        chk.num_rows() - 1
    }
}

/// The chunk carrying a row returned by
/// [`DataInDiskByRows::get_row_and_append_to_chunk`].
pub enum AppendedRowChunk<'a> {
    /// The caller's non-full chunk was reused in place.
    Existing(&'a mut Chunk),
    /// The caller supplied no chunk, or supplied a full one, so Go's fresh
    /// 1024-row replacement is owned by the result.
    Replacement(Chunk),
}

/// Rust ownership form of Go's `(Row, *Chunk)` return from
/// `GetRowAndAppendToChunk`.
pub struct AppendedDiskRow<'a> {
    chunk: AppendedRowChunk<'a>,
    row_index: usize,
}

impl AppendedDiskRow<'_> {
    /// The chunk that owns the appended row.
    #[must_use]
    pub fn chunk(&self) -> &Chunk {
        match &self.chunk {
            AppendedRowChunk::Existing(chunk) => &**chunk,
            AppendedRowChunk::Replacement(chunk) => chunk,
        }
    }

    /// The appended row's index in [`Self::chunk`].
    #[must_use]
    pub fn row_index(&self) -> usize {
        self.row_index
    }

    /// Whether the nil/full source path allocated a fresh chunk.
    #[must_use]
    pub fn is_replacement(&self) -> bool {
        matches!(&self.chunk, AppendedRowChunk::Replacement(_))
    }

    /// Take the fresh replacement and row index, or `None` when the caller's
    /// existing chunk was used.
    #[must_use]
    pub fn into_replacement(self) -> Option<(Chunk, usize)> {
        match self.chunk {
            AppendedRowChunk::Replacement(chunk) => Some((chunk, self.row_index)),
            AppendedRowChunk::Existing(_) => None,
        }
    }
}

/// Go `rowInDisk.ReadFrom`, reading positionally out of `file` at `offset`.
///
/// Go reads through an `io.SectionReader`, one `io.ReadFull` for the sizes and
/// one per not-null cell. The same reads happen here, at explicit offsets.
/// Returns the number of bytes the row occupies.
fn read_row_from(
    file: &DiskFileReaderWriter,
    offset: i64,
    num_col: usize,
    into: &mut DiskFormatRow,
) -> Result<i64, DiskError> {
    let mut sizes = vec![0u8; 8 * num_col];
    file.read_full_at(&mut sizes, offset)?;
    into.sizes_of_columns.clear();
    into.cells.clear();
    for i in 0..num_col {
        into.sizes_of_columns
            .push(bytes_to_i64(&sizes[i * 8..i * 8 + 8]));
    }
    let mut consumed = 8 * num_col as i64;
    for i in 0..num_col {
        let size = into.sizes_of_columns[i];
        if size == -1 {
            continue;
        }
        let mut cell = vec![0u8; size as usize];
        file.read_full_at(&mut cell, offset + consumed)?;
        consumed += size;
        into.cells.push(cell);
    }
    Ok(consumed)
}

/// Go `DataInDiskByRows`: rows spilled to disk, addressed by [`RowPtr`].
pub struct DataInDiskByRows {
    field_types: Vec<FieldType>,
    num_rows_of_each_chunk: Vec<usize>,
    row_num_of_each_chunk_first_row: Vec<usize>,
    total_num_rows: usize,
    disk_tracker: std::sync::Arc<disk::Tracker>,
    data_file: DiskFileReaderWriter,
    offset_file: DiskFileReaderWriter,
}

impl DataInDiskByRows {
    /// Go `NewDataInDiskByRows`.
    #[must_use]
    pub fn new(field_types: Vec<FieldType>) -> Self {
        DataInDiskByRows {
            field_types,
            num_rows_of_each_chunk: Vec::new(),
            row_num_of_each_chunk_first_row: Vec::new(),
            total_num_rows: 0,
            // Go: "TODO(fengliyuan): set the quota of disk usage."
            disk_tracker: disk::new_tracker(LABEL_FOR_CHUNK_DATA_IN_DISK_BY_ROWS, -1),
            data_file: DiskFileReaderWriter::default(),
            offset_file: DiskFileReaderWriter::default(),
        }
    }

    /// Go `initDiskFile`.
    fn init_disk_file(&mut self) -> io::Result<()> {
        disk::check_and_init_temp_dir()?;
        let label = self.disk_tracker.label();
        self.data_file
            .init_with_file_name(&format!("{DEFAULT_DATA_IN_DISK_BY_ROWS_PATH}{label}"))?;
        self.offset_file.init_with_file_name(&format!(
            "{DEFAULT_DATA_IN_DISK_BY_ROWS_OFFSET_PATH}{label}"
        ))
    }

    /// Go `Len`.
    #[must_use]
    pub fn len(&self) -> usize {
        self.total_num_rows
    }

    /// Whether no row has been spilled.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.total_num_rows == 0
    }

    /// Go `GetDiskTracker`.
    #[must_use]
    pub fn disk_tracker(&self) -> &std::sync::Arc<disk::Tracker> {
        &self.disk_tracker
    }

    /// Go `NumRowsOfChunk`.
    #[must_use]
    pub fn num_rows_of_chunk(&self, chk_id: usize) -> usize {
        self.num_rows_of_each_chunk[chk_id]
    }

    /// Go `NumChunks`.
    #[must_use]
    pub fn num_chunks(&self) -> usize {
        self.num_rows_of_each_chunk.len()
    }

    /// The field types the container was built with.
    #[must_use]
    pub fn field_types(&self) -> &[FieldType] {
        &self.field_types
    }

    /// The data spill file's path, once created. For tests.
    #[must_use]
    pub fn data_file_path(&self) -> Option<&std::path::PathBuf> {
        self.data_file.path()
    }

    /// Go `Add`: appends one chunk, plus one offset per row.
    ///
    /// Not safe to call concurrently, as Go's comment says.
    pub fn add(&mut self, chk: &Chunk) -> Result<(), DiskError> {
        if chk.num_rows() == 0 {
            return Err(DiskError::Message(
                "chunk appended to List should have at least 1 row",
            ));
        }
        if !self.data_file.is_open() {
            self.init_disk_file()?;
        }

        // Go `chunkInDisk.WriteTo`: serialize every row, remembering where
        // each one starts in the logical (pre-checksum) stream.
        let off_write = self.data_file.off_write();
        let mut buf = Vec::new();
        let mut offsets_of_rows = Vec::with_capacity(chk.num_rows());
        let mut format = DiskFormatRow::default();
        for row_idx in 0..chk.num_rows() {
            DiskFormatRow::convert_from_row(chk.get_row(row_idx), &mut format);
            offsets_of_rows.push(off_write + buf.len() as i64);
            format.write_to(&mut buf);
        }
        let n = self.data_file.write(&buf)? as i64;

        self.num_rows_of_each_chunk.push(offsets_of_rows.len());
        self.row_num_of_each_chunk_first_row
            .push(self.total_num_rows);
        let mut offset_buf = Vec::with_capacity(offsets_of_rows.len() * 8);
        i64_slice_to_bytes(&offsets_of_rows, &mut offset_buf);
        let n2 = self.offset_file.write(&offset_buf)? as i64;

        self.disk_tracker.consume(n + n2);
        self.total_num_rows += chk.num_rows();
        Ok(())
    }

    /// Go `getOffset`.
    fn get_offset(&self, chk_idx: u32, row_idx: u32) -> Result<i64, DiskError> {
        let offset_in_offset_file =
            self.row_num_of_each_chunk_first_row[chk_idx as usize] + row_idx as usize;
        let mut b = [0u8; 8];
        let n = self
            .offset_file
            .read_full_at(&mut b, offset_in_offset_file as i64 * 8)?;
        if n != 8 {
            return Err(DiskError::Message(
                "The file spilled is broken, can not get data offset from the disk",
            ));
        }
        Ok(bytes_to_i64(&b))
    }

    /// Go `GetChunk`: reads a whole spilled chunk back.
    ///
    /// Go hands the row decoding to a goroutine feeding a channel, purely for
    /// throughput; the rows are produced in the same order either way, so the
    /// loop is serial here.
    pub fn get_chunk(&self, chk_idx: usize) -> Result<Chunk, DiskError> {
        let chk_size = self.num_rows_of_each_chunk[chk_idx];
        let mut chk = Chunk::new_with_capacity(&self.field_types, chk_size);
        let mut offset = self.get_offset(chk_idx as u32, 0)?;
        let mut format = DiskFormatRow::default();
        for _ in 0..chk_size {
            let consumed =
                read_row_from(&self.data_file, offset, self.field_types.len(), &mut format)?;
            format.to_row(&mut chk);
            offset += consumed;
        }
        Ok(chk)
    }

    /// Go `GetRowAndAppendToChunk`: append into a supplied non-full chunk, or
    /// allocate and return a fresh capacity-1024 chunk when it is `None` or
    /// already full.
    pub fn get_row_and_append_to_chunk<'a>(
        &self,
        ptr: RowPtr,
        chunk: Option<&'a mut Chunk>,
    ) -> Result<AppendedDiskRow<'a>, DiskError> {
        let off = self.get_offset(ptr.chk_idx, ptr.row_idx)?;
        let mut format = DiskFormatRow::default();
        read_row_from(&self.data_file, off, self.field_types.len(), &mut format)?;
        match chunk {
            Some(chunk) if !chunk.is_full() => {
                let row_index = format.to_row(chunk);
                Ok(AppendedDiskRow {
                    chunk: AppendedRowChunk::Existing(chunk),
                    row_index,
                })
            }
            _ => {
                let mut replacement = Chunk::new_with_capacity(&self.field_types, 1024);
                let row_index = format.to_row(&mut replacement);
                Ok(AppendedDiskRow {
                    chunk: AppendedRowChunk::Replacement(replacement),
                    row_index,
                })
            }
        }
    }

    /// Checked thin helper for callers whose contract guarantees an existing
    /// non-full destination and needs only the appended row index.
    pub fn get_row_and_append_to_existing_chunk(
        &self,
        ptr: RowPtr,
        chunk: &mut Chunk,
    ) -> Result<usize, DiskError> {
        assert!(
            !chunk.is_full(),
            "existing row-in-disk destination must not be full"
        );
        Ok(self
            .get_row_and_append_to_chunk(ptr, Some(chunk))?
            .row_index())
    }

    /// Go `GetRow`: the row on its own, in a chunk allocated for it.
    ///
    /// Go's `GetRow` passes a nil chunk, so `toRow` allocates one of capacity
    /// 1024 and the returned `Row` keeps it alive. Here the chunk is returned
    /// with the row's index in it.
    pub fn get_row(&self, ptr: RowPtr) -> Result<(Chunk, usize), DiskError> {
        self.get_row_and_append_to_chunk(ptr, None)?
            .into_replacement()
            .ok_or(DiskError::Message(
                "nil row-in-disk destination did not allocate a replacement",
            ))
    }

    /// Go `Close`: releases the tracked disk usage and removes both files.
    pub fn close(&mut self) {
        if self.data_file.is_open() {
            self.disk_tracker
                .consume(-self.disk_tracker.bytes_consumed());
        }
        self.data_file.close();
        self.offset_file.close();
    }
}

impl Drop for DataInDiskByRows {
    fn drop(&mut self) {
        self.close();
    }
}

/// Go `ReaderWithCache`: reads through to `reader`, then tops the result up
/// from the writer's not-yet-flushed cache.
pub struct ReaderWithCache<R> {
    reader: R,
    cache_off: i64,
    cache: Vec<u8>,
}

impl<R: ReadAt> ReaderWithCache<R> {
    /// Go `NewReaderWithCache`.
    ///
    /// Go shares the writer's cache slice by reference and sees later writes
    /// through it; a snapshot is taken here instead, because the Rust writer
    /// owns its buffer mutably. Each read builds a fresh reader from the
    /// writer's current cache, so a reader never serves a stale tail.
    #[must_use]
    pub fn new(reader: R, cache: &[u8], cache_off: i64) -> Self {
        ReaderWithCache {
            reader,
            cache_off,
            cache: cache.to_vec(),
        }
    }
}

impl<R: ReadAt> ReadAt for ReaderWithCache<R> {
    fn read_at(&self, destination: &mut [u8], offset: i64) -> ReadAtResult {
        let result = self.reader.read_at(destination, offset);
        let Some(error) = &result.error else {
            return result;
        };
        if !error.is_eof() {
            return result;
        }
        let read_cnt = result.n;
        if read_cnt >= destination.len() {
            return result;
        }

        // The caller's buffer is not full, so the rest must come from cache.
        let remaining = &mut destination[read_cnt..];
        let begin = (offset - self.cache_off).max(0) as usize;
        let begin = begin.min(self.cache.len());
        let mut end = begin + remaining.len();
        let mut hit_eof = false;
        if end > self.cache.len() {
            hit_eof = true;
            end = self.cache.len();
        }
        let copied = end - begin;
        remaining[..copied].copy_from_slice(&self.cache[begin..end]);
        let total = read_cnt + copied;
        if hit_eof {
            ReadAtResult::eof(total)
        } else {
            ReadAtResult::ok(total)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{BinaryJSON, FieldTypeCode as C};

    /// The bytes GO WROTE: produced by
    /// `rust/difftests/chunk-tests/fixtures/generate_row_in_disk_vectors.go`,
    /// which drives the real `pkg/util/chunk.DataInDiskByRows` and dumps its
    /// spill files verbatim. Nothing here is compared against this port's own
    /// output.
    const GO_VECTORS: &str =
        include_str!("../../../difftests/chunk-tests/fixtures/row_in_disk_vectors.tsv");
    const ENCRYPTED_GO_VECTORS: &str =
        include_str!("../../../difftests/chunk-tests/fixtures/encrypted_spill_vectors.tsv");

    fn go_vector(key: &str) -> &'static str {
        GO_VECTORS
            .lines()
            .find_map(|line| line.strip_prefix(key)?.strip_prefix('\t'))
            .unwrap_or_else(|| panic!("fixture line {key} missing"))
    }

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

    fn hex_of(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    use crate::test_temp_storage::guard as temp_dir_guard;

    use crate::test_temp_storage::scratch_dir as scratch_temp_dir;

    /// Go generator case `mixed`.
    fn mixed_case() -> (Vec<FieldType>, Vec<Chunk>) {
        let fields = vec![
            FieldType::new(C::VarString),
            FieldType::new(C::LongLong),
            FieldType::new(C::VarString),
            FieldType::new(C::LongLong),
            FieldType::new(C::Json),
        ];
        let json = BinaryJSON::parse(r#"{"a": [1, 2, "b"]}"#).expect("JSON");
        let (num_chk, num_row) = (3usize, 8usize);
        let mut chunks = Vec::new();
        for chk_idx in 0..num_chk {
            let mut chk = Chunk::new_with_capacity(&fields, num_row);
            for row_idx in 0..num_row {
                chk.append_string(0, &"西xi瓜gua".repeat(row_idx + 1));
                chk.append_null(1);
                chk.append_null(2);
                chk.append_int64(3, (chk_idx * num_row + row_idx) as i64);
                if chk_idx % 2 == 0 {
                    chk.append_json(4, &json);
                } else {
                    chk.append_null(4);
                }
            }
            chunks.push(chk);
        }
        (fields, chunks)
    }

    /// Go generator case `int64_with_null`.
    fn int64_case() -> (Vec<FieldType>, Vec<Chunk>) {
        let fields = vec![FieldType::new(C::LongLong)];
        let mut chk = Chunk::new_with_capacity(&fields, 4);
        chk.append_int64(0, 1);
        chk.append_null(0);
        chk.append_int64(0, -2);
        (fields, vec![chk])
    }

    /// Go generator case `many_blocks`.
    fn many_blocks_case() -> (Vec<FieldType>, Vec<Chunk>) {
        let fields = vec![FieldType::new(C::Varchar), FieldType::new(C::LongLong)];
        let mut chunks = Vec::new();
        for chk_idx in 0..4usize {
            let mut chk = Chunk::new_with_capacity(&fields, 50);
            for row_idx in 0..50usize {
                let n = chk_idx * 50 + row_idx;
                chk.append_string(0, &"z".repeat(n % 17));
                chk.append_int64(1, n as i64);
            }
            chunks.push(chk);
        }
        (fields, chunks)
    }

    fn encrypted_case() -> (Vec<FieldType>, Vec<Chunk>) {
        let fields = vec![FieldType::new(C::Varchar), FieldType::new(C::LongLong)];
        let mut chunks = Vec::new();
        for chunk_idx in 0..9usize {
            let mut chk = Chunk::new_with_capacity(&fields, 32);
            for row_idx in 0..32usize {
                let ordinal = chunk_idx * 32 + row_idx;
                chk.append_string(0, &"x".repeat(ordinal % 31 + 1));
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

    fn spill(fields: &[FieldType], chunks: &[Chunk]) -> DataInDiskByRows {
        let mut container = DataInDiskByRows::new(fields.to_vec());
        for chk in chunks {
            container.add(chk).expect("add");
        }
        container
    }

    /// Every row read back, rendered exactly as the Go generator renders it.
    fn render_rows(container: &DataInDiskByRows) -> String {
        let mut parts = Vec::new();
        for chk_idx in 0..container.num_chunks() {
            for row_idx in 0..container.num_rows_of_chunk(chk_idx) {
                let ptr = RowPtr::new(chk_idx as u32, row_idx as u32);
                let (chk, idx) = container.get_row(ptr).expect("get_row");
                let row = chk.get_row(idx);
                let cells: Vec<String> = (0..container.field_types().len())
                    .map(|col_idx| {
                        if row.is_null(col_idx) {
                            "NULL".to_owned()
                        } else {
                            hex_of(row.get_raw(col_idx))
                        }
                    })
                    .collect();
                parts.push(format!("{chk_idx}:{row_idx}={}", cells.join(",")));
            }
        }
        parts.join("|")
    }

    fn render_encrypted_rows(container: &DataInDiskByRows) -> String {
        let mut parts = Vec::new();
        for chk_idx in 0..container.num_chunks() {
            for row_idx in 0..container.num_rows_of_chunk(chk_idx) {
                let ptr = RowPtr::new(chk_idx as u32, row_idx as u32);
                let (chk, idx) = container.get_row(ptr).expect("get_row");
                let row = chk.get_row(idx);
                let cells: Vec<String> = (0..container.field_types().len())
                    .map(|col_idx| {
                        if row.is_null(col_idx) {
                            "NULL".to_owned()
                        } else {
                            hex_of(row.get_raw(col_idx))
                        }
                    })
                    .collect();
                parts.push(cells.join(","));
            }
        }
        parts.join("|")
    }

    fn run_case(name: &str, fields: Vec<FieldType>, chunks: Vec<Chunk>) {
        let _guard = temp_dir_guard();
        let dir = scratch_temp_dir(name);
        disk::set_temp_storage_path(&dir);

        let container = spill(&fields, &chunks);

        // 1. THE FILE BYTES ARE GO'S. Both spill files, checksum framing
        //    included, before `close` unlinks them.
        let data = std::fs::read(container.data_file.path().expect("data file")).expect("read");
        assert_eq!(
            hex_of(&data),
            go_vector(&format!("{name}.data")),
            "{name} data file"
        );
        let offsets =
            std::fs::read(container.offset_file.path().expect("offset file")).expect("read");
        assert_eq!(
            hex_of(&offsets),
            go_vector(&format!("{name}.offsets")),
            "{name} offset file"
        );

        // 2. Every row reads back to the same bytes Go read back, including
        //    the tail still sitting in the writer's cache.
        assert_eq!(
            render_rows(&container),
            go_vector(&format!("{name}.rows")),
            "{name} rows"
        );

        // 3. The chunk/row bookkeeping.
        assert_eq!(
            format!(
                "numChunks={},len={}",
                container.num_chunks(),
                container.len()
            ),
            go_vector(&format!("{name}.meta")),
            "{name} meta"
        );
        drop(container);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Go `TestDataInDiskByRows` / `TestDataInDiskByRowsWithChecksum*`: the
    /// spilled bytes and the rows read back out of them.
    ///
    /// Go's plain and `WithChecksum` variants differ only by the config knob
    /// `SpilledFileEncryptionMethod`; the checksum layer is unconditional in
    /// `initWithFileName`, so both Go variants produce the byte image asserted
    /// here. The `AndEncrypt` variants have their own Go file-image test below.
    #[test]
    fn mixed_columns_spill_to_gos_bytes() {
        let (fields, chunks) = mixed_case();
        run_case("mixed", fields, chunks);
    }

    #[test]
    fn a_null_in_a_fixed_column_spills_to_gos_bytes() {
        let (fields, chunks) = int64_case();
        run_case("int64_with_null", fields, chunks);
    }

    /// The case that crosses the checksum layer's 1024-byte block boundary in
    /// BOTH files, so the block headers are part of what is compared.
    #[test]
    fn many_checksum_blocks_spill_to_gos_bytes() {
        let (fields, chunks) = many_blocks_case();
        run_case("many_blocks", fields, chunks);
    }

    #[test]
    fn encrypted_row_files_match_go_and_read_through_both_live_caches() {
        let _guard = temp_dir_guard();
        let dir = scratch_temp_dir("encrypted-rows");
        disk::set_temp_storage_path(&dir);
        disk::check_and_init_temp_dir().expect("temp dir");

        let (fields, chunks) = encrypted_case();
        let mut container = DataInDiskByRows::new(fields);
        container
            .data_file
            .init_with_file_name_and_cipher(
                "encrypted-rows-data",
                fixture_cipher("rows.data.cipher"),
            )
            .expect("data file");
        container
            .offset_file
            .init_with_file_name_and_cipher(
                "encrypted-rows-offsets",
                fixture_cipher("rows.offsets.cipher"),
            )
            .expect("offset file");
        for chk in &chunks {
            container.add(chk).expect("add");
        }

        let data = std::fs::read(container.data_file.path().expect("data path")).expect("read");
        let offsets =
            std::fs::read(container.offset_file.path().expect("offset path")).expect("read");
        assert_eq!(hex_of(&data), encrypted_go_vector("rows.data"));
        assert_eq!(hex_of(&offsets), encrypted_go_vector("rows.offsets"));
        assert_eq!(
            render_encrypted_rows(&container),
            encrypted_go_vector("rows.readback")
        );

        drop(container);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Go `GetChunk`: a whole spilled chunk read back at once must equal the
    /// rows read one at a time.
    #[test]
    fn get_chunk_reads_a_whole_chunk_back() {
        let _guard = temp_dir_guard();
        let dir = scratch_temp_dir("getchunk");
        disk::set_temp_storage_path(&dir);
        let (fields, chunks) = mixed_case();
        let container = spill(&fields, &chunks);

        for (chk_idx, original) in chunks.iter().enumerate() {
            let read = container.get_chunk(chk_idx).expect("get_chunk");
            assert_eq!(read.num_rows(), original.num_rows());
            for row_idx in 0..read.num_rows() {
                let (got, want) = (read.get_row(row_idx), original.get_row(row_idx));
                for col_idx in 0..fields.len() {
                    assert_eq!(got.is_null(col_idx), want.is_null(col_idx));
                    if !got.is_null(col_idx) {
                        assert_eq!(got.get_raw(col_idx), want.get_raw(col_idx));
                    }
                }
            }
        }
        drop(container);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Go `diskFormatRow.toRow` replaces a nil OR full destination with a
    /// fresh capacity-1024 chunk. A full caller-owned chunk is not mutated.
    #[test]
    fn row_read_replaces_a_full_destination() {
        let _guard = temp_dir_guard();
        let dir = scratch_temp_dir("full-row-destination");
        disk::set_temp_storage_path(&dir);
        let (fields, chunks) = int64_case();
        let container = spill(&fields, &chunks);

        let mut full = Chunk::new(&fields, 1, 1);
        full.append_int64(0, 99);
        assert!(full.is_full());
        let appended = container
            .get_row_and_append_to_chunk(RowPtr::new(0, 0), Some(&mut full))
            .expect("read row");
        assert!(appended.is_replacement());
        assert_eq!(appended.chunk().capacity(), 1024);
        assert_eq!(
            appended.chunk().get_row(appended.row_index()).get_int64(0),
            1
        );
        let (replacement, row_index) = appended.into_replacement().expect("replacement chunk");
        assert_eq!(replacement.get_row(row_index).get_int64(0), 1);

        // The source pointer passed to Go remains the same full chunk. The
        // borrowed Rust input proves the same identity/value contract.
        assert_eq!(full.num_rows(), 1);
        assert_eq!(full.get_row(0).get_int64(0), 99);
        drop(container);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn row_read_appends_to_a_non_full_destination() {
        let _guard = temp_dir_guard();
        let dir = scratch_temp_dir("non-full-row-destination");
        disk::set_temp_storage_path(&dir);
        let (fields, chunks) = int64_case();
        let container = spill(&fields, &chunks);

        let mut destination = Chunk::new(&fields, 1, 2);
        destination.append_int64(0, 99);
        let appended = container
            .get_row_and_append_to_chunk(RowPtr::new(0, 2), Some(&mut destination))
            .expect("read row");
        assert!(!appended.is_replacement());
        assert_eq!(appended.row_index(), 1);
        assert_eq!(appended.chunk().get_row(1).get_int64(0), -2);
        drop(appended);
        assert_eq!(destination.num_rows(), 2);

        drop(container);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Go `Add`'s first line: an empty chunk is refused.
    #[test]
    fn an_empty_chunk_is_refused() {
        let _guard = temp_dir_guard();
        let dir = scratch_temp_dir("emptyrow");
        disk::set_temp_storage_path(&dir);
        let fields = vec![FieldType::new(C::LongLong)];
        let mut container = DataInDiskByRows::new(fields.clone());
        let error = container
            .add(&Chunk::new_with_capacity(&fields, 4))
            .expect_err("a zero-row chunk must be refused");
        assert_eq!(
            error.to_string(),
            "chunk appended to List should have at least 1 row"
        );
        assert!(container.data_file_path().is_none());
        drop(container);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Go `TestDataInDiskByRows`'s deferred block: after `Close` the spill
    /// files are gone and the disk tracker is back to zero.
    #[test]
    fn close_removes_the_files_and_releases_the_tracker() {
        let _guard = temp_dir_guard();
        let dir = scratch_temp_dir("closerow");
        disk::set_temp_storage_path(&dir);
        let (fields, chunks) = many_blocks_case();
        let mut container = spill(&fields, &chunks);
        assert!(container.disk_tracker().bytes_consumed() > 0);
        let data_path = container.data_file.path().cloned().expect("data file");
        let offset_path = container.offset_file.path().cloned().expect("offset file");
        container.close();
        assert!(!data_path.exists(), "data file must be removed");
        assert!(!offset_path.exists(), "offset file must be removed");
        assert_eq!(container.disk_tracker().bytes_consumed(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
