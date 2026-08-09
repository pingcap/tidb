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
//! Ported: the in-memory copies (`CopySelectedJoinRowsDirect`,
//! `CopySelectedJoinRowsWithSameOuterRows`, `CopySelectedRows`, `CopyRows`),
//! and the spill-to-disk [`DiskFileReaderWriter`] half of the Go file, which
//! layers `tidb_util::checksum` over the temporary file.
//!
//! The spill stack has both Go variants: checksum -> file for `plaintext`, and
//! checksum -> AES-CTR -> file for `aes128-ctr`. [`set_spilled_file_encryption_method`]
//! is the config seam; the bounded server does not yet load the top-level Go
//! config tree, so its startup path must call that seam before it can claim to
//! honour `security.spilled-file-encryption-method`.

use crate::chunk::Chunk;
use crate::column::Column;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::OnceLock;
use tidb_util::layered_io::ReadAt;
use tidb_util::{checksum, encrypt};

/// Go `msgErrSelNotNil`: a bulk copy refuses chunks carrying a selection
/// vector, because the copy walks physical rows.
pub const MSG_ERR_SEL_NOT_NIL: &str =
    "The selection vector of Chunk is not nil. Please file a bug to the TiDB Team";

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

fn copy_selected_chunk_column(
    dst: &mut Chunk,
    dst_index: usize,
    src: &Chunk,
    src_index: usize,
    selected: &[bool],
) {
    if dst.column_is_shared(dst_index) || src.column_is_shared(src_index) {
        let source = src.column(src_index).copy_construct();
        let mut destination = dst.column_mut(dst_index);
        copy_selected_rows(&mut destination, &source, selected);
    } else {
        let source = src.column(src_index);
        let mut destination = dst.column_mut(dst_index);
        copy_selected_rows(&mut destination, &source, selected);
    }
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
        copy_selected_chunk_column(dst, j, src, j, selected);
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
        copy_selected_chunk_column(
            dst,
            inner_col_offset + j,
            src,
            inner_col_offset + j,
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
        let index = outer_col_offset + i;
        if dst.column_is_shared(index) || src.column_is_shared(index) {
            let source = src.column(index).copy_construct();
            dst.column_mut(index)
                .copy_same_rows_from(&source, row_idx, num_rows);
        } else {
            let source = src.column(index);
            dst.column_mut(index)
                .copy_same_rows_from(&source, row_idx, num_rows);
        }
    }
}

/// Go `ColumnSwapHelper`: maps input-column owners to output slots and caches
/// the runtime merge of input indexes that designate one owner.
#[derive(Debug)]
pub struct ColumnSwapHelper {
    /// Go `InputIdxToOutputIdxes`.
    pub input_idx_to_output_idxes: HashMap<usize, Vec<usize>>,
    merged_input_idx_to_output_idxes: OnceLock<HashMap<usize, Vec<usize>>>,
}

impl ColumnSwapHelper {
    /// Go `NewColumnSwapHelper`.
    #[must_use]
    pub fn new(used_column_indexes: &[usize]) -> Self {
        let mut input_idx_to_output_idxes = HashMap::new();
        for (output_index, &input_index) in used_column_indexes.iter().enumerate() {
            input_idx_to_output_idxes
                .entry(input_index)
                .or_insert_with(Vec::new)
                .push(output_index);
        }
        Self {
            input_idx_to_output_idxes,
            merged_input_idx_to_output_idxes: OnceLock::new(),
        }
    }

    /// Construct from the source-shaped public mapping.
    #[must_use]
    pub fn from_mapping(input_idx_to_output_idxes: HashMap<usize, Vec<usize>>) -> Self {
        Self {
            input_idx_to_output_idxes,
            merged_input_idx_to_output_idxes: OnceLock::new(),
        }
    }

    fn merge_input_indexes(&self, input: &Chunk) -> HashMap<usize, Vec<usize>> {
        let mut merged = HashMap::<usize, Vec<usize>>::new();
        for (&input_index, output_indexes) in &self.input_idx_to_output_idxes {
            let owner_index = (0..input.num_cols())
                .find(|&candidate| input.columns_share_identity(candidate, input, input_index))
                .unwrap_or(input_index);
            merged
                .entry(owner_index)
                .or_default()
                .extend_from_slice(output_indexes);
        }
        merged
    }

    /// Go `SwapColumns`. The empty mapping is a true no-op, including when a
    /// chunk carries a selection; a non-empty mapping validates both chunks
    /// before the first owner move.
    pub fn swap_columns(&self, input: &mut Chunk, output: &mut Chunk) -> Result<(), &'static str> {
        if self.input_idx_to_output_idxes.is_empty() {
            return Ok(());
        }
        if input.has_sel() || output.has_sel() {
            return Err(MSG_ERR_SEL_NOT_NIL);
        }
        let merged = self
            .merged_input_idx_to_output_idxes
            .get_or_init(|| self.merge_input_indexes(input));
        for (&input_index, output_indexes) in merged {
            let Some((&first, rest)) = output_indexes.split_first() else {
                continue;
            };
            output.swap_column_with(first, input, input_index)?;
            for &output_index in rest {
                output.make_ref(first, output_index);
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn merged_mapping(&self) -> Option<&HashMap<usize, Vec<usize>>> {
        self.merged_input_idx_to_output_idxes.get()
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

/// A random decimal run for a temporary file name, standing in for the one
/// Go's `os.CreateTemp` appends. Uniqueness is enforced by the `create_new`
/// retry loop in `crate::chunk_in_disk::create_temp_file`, exactly as Go's is.
pub(crate) fn next_random_suffix() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);
    nanos.wrapping_mul(6_364_136_223_846_793_005)
        ^ COUNTER
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_mul(1_442_695_040_888_963_407)
}

/// Process-wide choice corresponding to Go's
/// `config.Security.SpilledFileEncryptionMethod`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SpilledFileEncryptionMethod {
    /// Go `SpilledFileEncryptionMethodPlaintext`.
    #[default]
    Plaintext,
    /// Go `SpilledFileEncryptionMethodAES128CTR`.
    Aes128Ctr,
}

static SPILLED_FILE_ENCRYPTION_METHOD: AtomicU8 = AtomicU8::new(0);

/// Sets the process-wide spill-file encryption method.
///
/// Go reads this choice from its global config whenever it creates a spill
/// file. A Rust config loader must call this equivalent seam once at startup;
/// tests may switch it while holding their process-global spill guard.
pub fn set_spilled_file_encryption_method(method: SpilledFileEncryptionMethod) {
    let value = match method {
        SpilledFileEncryptionMethod::Plaintext => 0,
        SpilledFileEncryptionMethod::Aes128Ctr => 1,
    };
    SPILLED_FILE_ENCRYPTION_METHOD.store(value, Ordering::SeqCst);
}

/// Returns the method new spill files will use.
#[must_use]
pub fn spilled_file_encryption_method() -> SpilledFileEncryptionMethod {
    match SPILLED_FILE_ENCRYPTION_METHOD.load(Ordering::SeqCst) {
        0 => SpilledFileEncryptionMethod::Plaintext,
        _ => SpilledFileEncryptionMethod::Aes128Ctr,
    }
}

enum DiskWriter {
    Plaintext(checksum::Writer<File>),
    Aes128Ctr {
        writer: checksum::Writer<encrypt::Writer<File>>,
        cipher: encrypt::CtrCipher,
    },
}

impl DiskWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        match self {
            Self::Plaintext(writer) => writer.write(data),
            Self::Aes128Ctr { writer, .. } => writer.write(data),
        }
    }

    fn checksum_cache(&self) -> (&[u8], i64) {
        match self {
            Self::Plaintext(writer) => (writer.get_cache(), writer.get_cache_data_offset()),
            Self::Aes128Ctr { writer, .. } => (writer.get_cache(), writer.get_cache_data_offset()),
        }
    }

    fn close(self) -> io::Result<()> {
        match self {
            Self::Plaintext(writer) => writer.close(),
            Self::Aes128Ctr { writer, .. } => writer.close(),
        }
    }
}

#[derive(Default)]
/// Go `diskFileReaderWriter`: a temporary spill file plus checksum framing and
/// optional AES-CTR encryption.
///
/// Reads go through a SECOND handle on the same file so a chunk can be read
/// back while later chunks are still being appended. Each live writer cache
/// is overlaid in source order before the logical checksum payload is read.
pub struct DiskFileReaderWriter {
    /// The read handle. Go reuses one `*os.File` for both directions.
    file: Option<File>,
    path: Option<PathBuf>,
    writer: Option<DiskWriter>,
    /// Go `offWrite`: the current logical write offset.
    off_write: i64,
}

impl DiskFileReaderWriter {
    /// Go `initWithFileName`.
    pub fn init_with_file_name(&mut self, file_name: &str) -> io::Result<()> {
        let cipher = match spilled_file_encryption_method() {
            SpilledFileEncryptionMethod::Plaintext => None,
            SpilledFileEncryptionMethod::Aes128Ctr => Some(encrypt::CtrCipher::new()?),
        };
        self.init_with_file_name_and_optional_cipher(file_name, cipher)
    }

    fn init_with_file_name_and_optional_cipher(
        &mut self,
        file_name: &str,
        cipher: Option<encrypt::CtrCipher>,
    ) -> io::Result<()> {
        let (file, path) = crate::chunk_in_disk::create_temp_file(file_name)?;
        let write_handle = file.try_clone()?;
        self.file = Some(file);
        self.path = Some(path);
        self.writer = Some(match cipher {
            None => DiskWriter::Plaintext(checksum::Writer::new(write_handle)),
            Some(cipher) => {
                let encrypting = encrypt::Writer::new(write_handle, &cipher);
                DiskWriter::Aes128Ctr {
                    writer: checksum::Writer::new(encrypting),
                    cipher,
                }
            }
        });
        self.off_write = 0;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn init_with_file_name_and_cipher(
        &mut self,
        file_name: &str,
        cipher: encrypt::CtrCipher,
    ) -> io::Result<()> {
        self.init_with_file_name_and_optional_cipher(file_name, Some(cipher))
    }

    /// Whether the file has been created (Go's `l.file != nil`).
    #[must_use]
    pub fn is_open(&self) -> bool {
        self.file.is_some()
    }

    /// The spill file's path, once created.
    #[must_use]
    pub fn path(&self) -> Option<&PathBuf> {
        self.path.as_ref()
    }

    /// Go `offWrite`: the logical offset the next write lands at.
    #[must_use]
    pub fn off_write(&self) -> i64 {
        self.off_write
    }

    #[cfg(test)]
    pub(crate) fn is_encrypted(&self) -> bool {
        matches!(self.writer, Some(DiskWriter::Aes128Ctr { .. }))
    }

    /// Go `write`.
    pub fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| io::Error::other("spill file is not open"))?;
        let written = writer.write(data)?;
        self.off_write += written as i64;
        Ok(written)
    }

    /// Go `getReader` + `getSectionReader` + `io.ReadFull`, collapsed into the
    /// one operation every caller performs: fill `destination` from logical
    /// offset `offset`.
    pub fn read_full_at(&self, destination: &mut [u8], offset: i64) -> io::Result<usize> {
        let (Some(file), Some(writer)) = (self.file.as_ref(), self.writer.as_ref()) else {
            return Err(io::Error::other("spill file is not open"));
        };
        let (checksum_cache, checksum_cache_offset) = writer.checksum_cache();
        match writer {
            DiskWriter::Plaintext(_) => {
                let reader = crate::row_in_disk::ReaderWithCache::new(
                    checksum::Reader::new(file),
                    checksum_cache,
                    checksum_cache_offset,
                );
                crate::chunk_in_disk::read_full_at(&reader as &dyn ReadAt, destination, offset)
            }
            DiskWriter::Aes128Ctr { writer, cipher } => {
                let encrypting = writer.underlying();
                let decrypted = encrypt::Reader::new(file, cipher);
                let decrypted_with_cache = crate::row_in_disk::ReaderWithCache::new(
                    decrypted,
                    encrypting.get_cache(),
                    encrypting.get_cache_data_offset(),
                );
                let checksummed = checksum::Reader::new(decrypted_with_cache);
                let reader = crate::row_in_disk::ReaderWithCache::new(
                    checksummed,
                    checksum_cache,
                    checksum_cache_offset,
                );
                crate::chunk_in_disk::read_full_at(&reader as &dyn ReadAt, destination, offset)
            }
        }
    }

    /// Closes the file and REMOVES it, as Go's `DataInDiskByChunks.Close`
    /// does. Any flush error is dropped for the same reason Go's
    /// `terror.Call` drops it: the data is being discarded anyway.
    pub fn close(&mut self) {
        if let Some(writer) = self.writer.take() {
            let _ = writer.close();
        }
        self.file = None;
        if let Some(path) = self.path.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

impl Drop for DiskFileReaderWriter {
    fn drop(&mut self) {
        self.close();
    }
}
