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

//! Complete transcreation of `pkg/util/checksum`.
//!
//! The CRC block layer, pooled positional-read buffer, original corruption
//! tests, encrypt/checksum composition tests, build metadata, and test-process
//! support move together. Rust's test harness creates no package-owned
//! background workers, so Go's `goleak.VerifyTestMain` has no runtime analogue.

use crate::layered_io::{CloseWrite, ReadAt, ReadAtResult};
use crate::zeropool::Pool;
use std::io::{self, Write};
use std::sync::LazyLock;

/// Size of one physical checksum block.
pub const CHECKSUM_BLOCK_SIZE: usize = 1024;
/// Size of the little-endian CRC-32 field.
pub const CHECKSUM_SIZE: usize = 4;
/// Logical payload capacity in one checksum block.
pub const CHECKSUM_PAYLOAD_SIZE: usize = CHECKSUM_BLOCK_SIZE - CHECKSUM_SIZE;

static CHECKSUM_READER_BUFFER_POOL: LazyLock<Pool<Vec<u8>>> =
    LazyLock::new(|| Pool::new(|| vec![0; CHECKSUM_BLOCK_SIZE]));

struct PooledReadBuffer(Option<Vec<u8>>);

impl PooledReadBuffer {
    fn get() -> Self {
        Self(Some(CHECKSUM_READER_BUFFER_POOL.get()))
    }

    fn bytes_mut(&mut self) -> &mut [u8] {
        self.0.as_mut().expect("pooled buffer is present")
    }
}

impl Drop for PooledReadBuffer {
    fn drop(&mut self) {
        if let Some(buffer) = self.0.take() {
            CHECKSUM_READER_BUFFER_POOL.put(buffer);
        }
    }
}

#[derive(Clone)]
struct StickyError {
    kind: io::ErrorKind,
    message: String,
}

impl StickyError {
    fn from_error(error: &io::Error) -> Self {
        Self {
            kind: error.kind(),
            message: error.to_string(),
        }
    }

    fn to_error(&self) -> io::Error {
        io::Error::new(self.kind, self.message.clone())
    }
}

/// CRC-32 framing writer.
pub struct Writer<W>
where
    W: CloseWrite,
{
    error: Option<StickyError>,
    underlying: W,
    buffer: Vec<u8>,
    payload_used: usize,
    flushed_user_data_count: i64,
}

impl<W> Writer<W>
where
    W: CloseWrite,
{
    /// Creates a checksum writer over `underlying`.
    #[must_use]
    pub fn new(underlying: W) -> Self {
        Self {
            error: None,
            underlying,
            buffer: vec![0; CHECKSUM_BLOCK_SIZE],
            payload_used: 0,
            flushed_user_data_count: 0,
        }
    }

    /// Returns unused logical payload bytes in the current block.
    #[must_use]
    pub const fn available_size(&self) -> usize {
        CHECKSUM_PAYLOAD_SIZE - self.payload_used
    }

    /// Returns buffered logical payload bytes.
    #[must_use]
    pub const fn buffered(&self) -> usize {
        self.payload_used
    }

    /// Flushes the current CRC-framed block.
    pub fn flush_buffer(&mut self) -> io::Result<()> {
        if let Some(error) = &self.error {
            return Err(error.to_error());
        }
        if self.payload_used == 0 {
            return Ok(());
        }
        let payload_end = CHECKSUM_SIZE + self.payload_used;
        let checksum = crc32fast::hash(&self.buffer[CHECKSUM_SIZE..payload_end]);
        self.buffer[..CHECKSUM_SIZE].copy_from_slice(&checksum.to_le_bytes());
        let result = match self.underlying.write(&self.buffer[..payload_end]) {
            // Preserve the source check, which compares the physical byte
            // count against payload bytes rather than payload plus checksum.
            Ok(n) if n < self.payload_used => {
                Err(io::Error::new(io::ErrorKind::WriteZero, "short write"))
            }
            Ok(_) => Ok(()),
            Err(error) => Err(error),
        };
        if let Err(error) = &result {
            self.error = Some(StickyError::from_error(error));
            return result;
        }
        self.flushed_user_data_count += self.payload_used as i64;
        self.payload_used = 0;
        Ok(())
    }

    /// Returns logical payload not yet flushed.
    #[must_use]
    pub fn get_cache(&self) -> &[u8] {
        &self.buffer[CHECKSUM_SIZE..CHECKSUM_SIZE + self.payload_used]
    }

    /// Returns the logical offset of the cached payload.
    #[must_use]
    pub const fn get_cache_data_offset(&self) -> i64 {
        self.flushed_user_data_count
    }

    /// Flushes and closes every owned writer layer.
    pub fn close(mut self) -> io::Result<()> {
        self.flush_buffer()?;
        self.underlying.close()
    }
}

impl<W> Write for Writer<W>
where
    W: CloseWrite,
{
    fn write(&mut self, mut source: &[u8]) -> io::Result<usize> {
        let mut written = 0;
        while source.len() > self.available_size() && self.error.is_none() {
            let copied = self.available_size();
            let start = CHECKSUM_SIZE + self.payload_used;
            self.buffer[start..start + copied].copy_from_slice(&source[..copied]);
            self.payload_used += copied;
            self.flush_buffer()?;
            written += copied;
            source = &source[copied..];
        }
        if let Some(error) = &self.error {
            return if written == 0 {
                Err(error.to_error())
            } else {
                Ok(written)
            };
        }
        let copied = source.len().min(self.available_size());
        let start = CHECKSUM_SIZE + self.payload_used;
        self.buffer[start..start + copied].copy_from_slice(&source[..copied]);
        self.payload_used += copied;
        written += copied;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_buffer()
    }
}

impl<W> CloseWrite for Writer<W>
where
    W: CloseWrite,
{
    fn close(self) -> io::Result<()> {
        Self::close(self)
    }
}

/// Positional checksum-verifying reader.
pub struct Reader<R>
where
    R: ReadAt,
{
    underlying: R,
}

impl<R> Reader<R>
where
    R: ReadAt,
{
    /// Creates a checksum reader over `underlying`.
    #[must_use]
    pub const fn new(underlying: R) -> Self {
        Self { underlying }
    }
}

fn checksum_failure(n: usize) -> ReadAtResult {
    ReadAtResult::io(n, io::Error::other("error checksum"))
}

impl<R> ReadAt for Reader<R>
where
    R: ReadAt,
{
    fn read_at(&self, destination: &mut [u8], offset: i64) -> ReadAtResult {
        if destination.is_empty() {
            return ReadAtResult::ok(0);
        }
        if offset < 0 {
            return ReadAtResult::io(
                0,
                io::Error::new(io::ErrorKind::InvalidInput, "negative read offset"),
            );
        }
        let mut offset_in_payload = offset % CHECKSUM_PAYLOAD_SIZE as i64;
        let mut cursor = offset / CHECKSUM_PAYLOAD_SIZE as i64 * CHECKSUM_BLOCK_SIZE as i64;
        let mut pooled = PooledReadBuffer::get();
        let buffer = pooled.bytes_mut();
        let mut total = 0;

        while total < destination.len() {
            let result = self.underlying.read_at(buffer, cursor);
            if let Some(error) = result.error {
                if result.n == 0 || !error.is_eof() {
                    return ReadAtResult {
                        n: total,
                        error: Some(error),
                    };
                }
            }
            if result.n < CHECKSUM_SIZE {
                return checksum_failure(total);
            }
            cursor += result.n as i64;
            let original_checksum =
                u32::from_le_bytes(buffer[..CHECKSUM_SIZE].try_into().expect("CRC field"));
            let checksum = crc32fast::hash(&buffer[CHECKSUM_SIZE..result.n]);
            if original_checksum != checksum {
                return checksum_failure(total);
            }
            let start = CHECKSUM_SIZE + offset_in_payload as usize;
            let available = &buffer[start..result.n];
            let copied = available.len().min(destination.len() - total);
            destination[total..total + copied].copy_from_slice(&available[..copied]);
            total += copied;
            offset_in_payload = 0;
        }
        ReadAtResult::ok(total)
    }
}

#[cfg(test)]
mod tests {
    #![allow(non_snake_case)]

    use super::*;
    use crate::encrypt;
    use crate::layered_io::ReadAtError;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct MemoryFile(Arc<Mutex<Vec<u8>>>);

    impl MemoryFile {
        fn bytes(&self) -> Vec<u8> {
            self.0.lock().expect("memory file").clone()
        }
    }

    impl Write for MemoryFile {
        fn write(&mut self, source: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .expect("memory file")
                .extend_from_slice(source);
            Ok(source.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl CloseWrite for MemoryFile {
        fn close(self) -> io::Result<()> {
            Ok(())
        }
    }

    impl ReadAt for MemoryFile {
        fn read_at(&self, destination: &mut [u8], offset: i64) -> ReadAtResult {
            let bytes = self.0.lock().expect("memory file");
            if offset < 0 || offset as usize > bytes.len() {
                return ReadAtResult::eof(0);
            }
            let copied = destination
                .len()
                .min(bytes.len().saturating_sub(offset as usize));
            destination[..copied]
                .copy_from_slice(&bytes[offset as usize..offset as usize + copied]);
            if copied < destination.len() {
                ReadAtResult::eof(copied)
            } else {
                ReadAtResult::ok(copied)
            }
        }
    }

    struct MutatingWriter<W, F>
    where
        W: CloseWrite,
        F: FnMut(&mut Vec<u8>, usize),
    {
        underlying: W,
        mutate: F,
        offset: usize,
    }

    impl<W, F> Write for MutatingWriter<W, F>
    where
        W: CloseWrite,
        F: FnMut(&mut Vec<u8>, usize),
    {
        fn write(&mut self, source: &[u8]) -> io::Result<usize> {
            let reported = source.len();
            let mut bytes = source.to_vec();
            (self.mutate)(&mut bytes, self.offset);
            let written = self.underlying.write(&bytes)?;
            self.offset += written;
            Ok(reported)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.underlying.flush()
        }
    }

    impl<W, F> CloseWrite for MutatingWriter<W, F>
    where
        W: CloseWrite,
        F: FnMut(&mut Vec<u8>, usize),
    {
        fn close(self) -> io::Result<()> {
            self.underlying.close()
        }
    }

    fn repeated_data(repetitions: usize) -> Vec<u8> {
        b"0123456789".repeat(repetitions)
    }

    fn is_eof(result: &ReadAtResult) -> bool {
        result.error.as_ref().is_some_and(ReadAtError::is_eof)
    }

    fn is_checksum_failure(result: &ReadAtResult) -> bool {
        result
            .error
            .as_ref()
            .is_some_and(|error| error.to_string() == "error checksum")
    }

    fn read_layered(
        file: MemoryFile,
        cipher: Option<&encrypt::CtrCipher>,
        destination: &mut [u8],
        offset: i64,
    ) -> ReadAtResult {
        if let Some(cipher) = cipher {
            Reader::new(encrypt::Reader::new(file, cipher)).read_at(destination, offset)
        } else {
            Reader::new(file).read_at(destination, offset)
        }
    }

    fn write_with_mutation<F>(
        encrypted: bool,
        mutate: F,
    ) -> (MemoryFile, Option<encrypt::CtrCipher>)
    where
        F: FnMut(&mut Vec<u8>, usize),
    {
        let file = MemoryFile::default();
        let mutating = MutatingWriter {
            underlying: file.clone(),
            mutate,
            offset: 0,
        };
        let data = repeated_data(510);
        if encrypted {
            let cipher = encrypt::CtrCipher::new().expect("cipher");
            let encrypting = encrypt::Writer::new(mutating, &cipher);
            let mut writer = Writer::new(encrypting);
            writer.write_all(&data).unwrap();
            writer.write_all(&data).unwrap();
            writer.close().unwrap();
            (file, Some(cipher))
        } else {
            let mut writer = Writer::new(mutating);
            writer.write_all(&data).unwrap();
            writer.write_all(&data).unwrap();
            writer.close().unwrap();
            (file, None)
        }
    }

    fn check_corruption<F, A>(mutator: F, assertion: A)
    where
        F: FnMut(&mut Vec<u8>, usize) + Clone,
        A: Fn(usize, &ReadAtResult),
    {
        for encrypted in [false, true] {
            let (file, cipher) = write_with_mutation(encrypted, mutator.clone());
            for index in 0..32 {
                let mut destination = [0_u8; 10];
                let result = read_layered(
                    file.clone(),
                    cipher.as_ref(),
                    &mut destination,
                    (index * 1000) as i64,
                );
                if is_eof(&result) {
                    break;
                }
                assertion(index, &result);
            }
        }
    }

    #[test]
    fn TestChecksumReadAt() {
        let file = MemoryFile::default();
        let data = repeated_data(510);
        let first = Writer::new(file.clone());
        let second = Writer::new(first);
        let third = Writer::new(second);
        let mut fourth = Writer::new(third);
        fourth.write_all(&data).unwrap();
        fourth.write_all(&data).unwrap();
        fourth.close().unwrap();

        let reader = Reader::new(Reader::new(Reader::new(Reader::new(file))));
        for (offset, expected_n, expected, eof) in [
            (0_i64, 10, b"0123456789".as_slice(), false),
            (5, 10, b"5678901234".as_slice(), false),
            (10_195, 5, b"56789\0\0\0\0\0".as_slice(), true),
        ] {
            let mut destination = [0_u8; 10];
            let result = reader.read_at(&mut destination, offset);
            assert_eq!(result.n, expected_n);
            assert_eq!(destination, expected);
            assert_eq!(is_eof(&result), eof);
        }
    }

    #[test]
    fn TestAddOneByte() {
        check_corruption(
            |bytes, offset| {
                const INSERT_POSITION: usize = 5000;
                if offset < INSERT_POSITION && offset + bytes.len() >= INSERT_POSITION {
                    bytes.insert(INSERT_POSITION - offset, 0);
                }
            },
            |index, result| {
                if index < 5 {
                    assert!(result.error.is_none());
                } else {
                    assert!(is_checksum_failure(result));
                }
            },
        );
    }

    #[test]
    fn TestDeleteOneByte() {
        check_corruption(
            |bytes, offset| {
                const DELETE_POSITION: usize = 5000;
                if offset < DELETE_POSITION && offset + bytes.len() >= DELETE_POSITION {
                    bytes.remove(DELETE_POSITION - offset - 1);
                }
            },
            |index, result| {
                if index < 5 {
                    assert!(result.error.is_none());
                } else {
                    assert!(is_checksum_failure(result));
                }
            },
        );
    }

    #[test]
    fn TestModifyOneByte() {
        check_corruption(
            |bytes, offset| {
                const MODIFY_POSITION: usize = 5000;
                if offset < MODIFY_POSITION && offset + bytes.len() >= MODIFY_POSITION {
                    let index = MODIFY_POSITION - offset - 1;
                    bytes[index] = bytes[index].wrapping_sub(1);
                }
            },
            |index, result| {
                if index == 5 {
                    assert!(is_checksum_failure(result));
                } else {
                    assert!(result.error.is_none());
                }
            },
        );
    }

    #[test]
    fn TestReadEmptyFile() {
        for encrypted in [false, true] {
            let file = MemoryFile::default();
            let cipher = encrypted.then(|| encrypt::CtrCipher::new().expect("empty cipher"));
            for index in 0..11 {
                let mut destination = [0_u8; 10];
                let result = read_layered(
                    file.clone(),
                    cipher.as_ref(),
                    &mut destination,
                    (index * CHECKSUM_PAYLOAD_SIZE) as i64,
                );
                assert!(is_eof(&result));
            }
        }
    }

    #[test]
    fn TestModifyThreeBytes() {
        check_corruption(
            |bytes, offset| {
                const MODIFY_POSITION: usize = 5000;
                if offset < MODIFY_POSITION
                    && offset + bytes.len() >= MODIFY_POSITION
                    && bytes.len() == CHECKSUM_BLOCK_SIZE
                {
                    for index in [200, 300, 400] {
                        bytes[index] = bytes[index].wrapping_sub(1);
                    }
                }
            },
            |index, result| {
                if index == 5 {
                    assert!(is_checksum_failure(result));
                } else {
                    assert!(result.error.is_none());
                }
            },
        );
    }

    fn write_standard_file(encrypted: bool) -> (MemoryFile, Option<encrypt::CtrCipher>) {
        let file = MemoryFile::default();
        let data = repeated_data(510);
        if encrypted {
            let cipher = encrypt::CtrCipher::new().expect("cipher");
            let encrypting = encrypt::Writer::new(file.clone(), &cipher);
            let mut writer = Writer::new(encrypting);
            writer.write_all(&data).unwrap();
            writer.write_all(&data).unwrap();
            writer.close().unwrap();
            (file, Some(cipher))
        } else {
            let mut writer = Writer::new(file.clone());
            writer.write_all(&data).unwrap();
            writer.write_all(&data).unwrap();
            writer.close().unwrap();
            (file, None)
        }
    }

    fn assert_read(
        file: MemoryFile,
        cipher: Option<&encrypt::CtrCipher>,
        offset: i64,
        length: usize,
        expected_n: usize,
        expected: &[u8],
        eof: bool,
    ) {
        let mut destination = vec![0; length];
        let result = read_layered(file, cipher, &mut destination, offset);
        assert_eq!(result.n, expected_n);
        assert_eq!(destination, expected);
        assert_eq!(is_eof(&result), eof);
    }

    #[test]
    fn TestReadDifferentBlockSize() {
        for encrypted in [false, true] {
            let (file, cipher) = write_standard_file(encrypted);
            let cases = [
                (2000, 1000, 1000, b"0123456789".repeat(100), false),
                (3005, 3000, 3000, b"5678901234".repeat(300), false),
                (10000, 200, 200, b"0123456789".repeat(20), false),
                (
                    10000,
                    201,
                    200,
                    [b"0123456789".repeat(20), vec![0]].concat(),
                    true,
                ),
                (5000, 5200, 5200, b"0123456789".repeat(520), false),
                (
                    5000,
                    6000,
                    5200,
                    [b"0123456789".repeat(520), vec![0; 800]].concat(),
                    true,
                ),
                (0, 10200, 10200, b"0123456789".repeat(1020), false),
                (
                    0,
                    11000,
                    10200,
                    [b"0123456789".repeat(1020), vec![0; 800]].concat(),
                    true,
                ),
            ];
            for (offset, length, expected_n, expected, eof) in cases {
                assert_read(
                    file.clone(),
                    cipher.as_ref(),
                    offset,
                    length,
                    expected_n,
                    &expected,
                    eof,
                );
            }
        }
    }

    fn write_in_batches(
        file: MemoryFile,
        cipher: Option<&encrypt::CtrCipher>,
        data: &[u8],
        batch_size: Option<usize>,
    ) {
        if let Some(cipher) = cipher {
            let encrypting = encrypt::Writer::new(file, cipher);
            let mut writer = Writer::new(encrypting);
            if let Some(batch_size) = batch_size {
                for chunk in data.chunks(batch_size) {
                    writer.write_all(chunk).unwrap();
                }
            } else {
                writer.write_all(data).unwrap();
            }
            writer.close().unwrap();
        } else {
            let mut writer = Writer::new(file);
            if let Some(batch_size) = batch_size {
                for chunk in data.chunks(batch_size) {
                    writer.write_all(chunk).unwrap();
                }
            } else {
                writer.write_all(data).unwrap();
            }
            writer.close().unwrap();
        }
    }

    #[test]
    fn TestWriteDifferentBlockSize() {
        let data = repeated_data(1020);
        for encrypted in [false, true] {
            let first = MemoryFile::default();
            let second = MemoryFile::default();
            let cipher = encrypted.then(|| encrypt::CtrCipher::new().expect("cipher"));
            write_in_batches(first.clone(), cipher.as_ref(), &data, None);
            write_in_batches(second.clone(), cipher.as_ref(), &data, Some(100));
            assert_eq!(first.bytes(), second.bytes());
            let expected = repeated_data(1020);
            assert_read(first, cipher.as_ref(), 0, 10200, 10200, &expected, false);
            assert_read(second, cipher.as_ref(), 0, 10200, 10200, &expected, false);
        }
    }

    #[test]
    fn TestChecksumWriter() {
        let file = MemoryFile::default();
        let data = repeated_data(100);
        let mut writer = Writer::new(file.clone());
        assert_eq!(writer.write(&data).unwrap(), 1000);
        writer.flush_buffer().unwrap();
        assert_eq!(writer.get_cache_data_offset(), 1000);
        assert!(writer.get_cache().is_empty());

        let mut read = vec![0; 1000];
        let result = Reader::new(file).read_at(&mut read, 0);
        assert_eq!(result.n, 1000);
        assert!(result.error.is_none());
        assert_eq!(read, data);
    }

    #[test]
    fn TestChecksumWriterAutoFlush() {
        let file = MemoryFile::default();
        let data = repeated_data(102);
        let mut writer = Writer::new(file.clone());
        assert_eq!(writer.write(&data).unwrap(), data.len());
        assert_eq!(writer.write(b"0").unwrap(), 1);
        assert_eq!(writer.get_cache_data_offset(), data.len() as i64);
        assert_eq!(writer.get_cache(), b"0");

        let mut read = vec![0; 1020];
        let result = Reader::new(file).read_at(&mut read, 0);
        assert_eq!(result.n, 1020);
        assert!(result.error.is_none());
        assert_eq!(read, data);
    }
}
