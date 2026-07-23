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

//! Random-access AES-CTR layer from `pkg/util/encrypt/aes_layer.go`.

use super::aes::{AesCipher, AES_BLOCK_SIZE};
use crate::layered_io::{CloseWrite, ReadAt, ReadAtResult};
use std::io::{self, Write};
use std::sync::Arc;

/// Source default encrypt block size in bytes.
pub const DEFAULT_ENCRYPT_BLOCK_SIZE: i64 = 1024;

/// AES-CTR key, nonce, and random-access block geometry.
#[derive(Clone)]
pub struct CtrCipher {
    block: Arc<AesCipher>,
    nonce: u64,
    encrypt_block_size: i64,
    aes_block_count: i64,
}

impl CtrCipher {
    /// Creates a cipher with TiDB's default 1024-byte encryption block.
    pub fn new() -> io::Result<Self> {
        Self::new_with_block_size(DEFAULT_ENCRYPT_BLOCK_SIZE)
    }

    /// Creates a cipher with the requested encryption block size.
    pub fn new_with_block_size(encrypt_block_size: i64) -> io::Result<Self> {
        let mut key = [0_u8; AES_BLOCK_SIZE];
        getrandom::fill(&mut key)
            .map_err(|error| io::Error::other(format!("random AES key: {error}")))?;
        let block = AesCipher::new(&key).map_err(|error| io::Error::other(error.to_string()))?;
        if encrypt_block_size % AES_BLOCK_SIZE as i64 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid encrypt block size",
            ));
        }
        let nonce = loop {
            let mut bytes = [0_u8; 8];
            getrandom::fill(&mut bytes)
                .map_err(|error| io::Error::other(format!("random AES nonce: {error}")))?;
            let candidate = u64::from_be_bytes(bytes) & i64::MAX as u64;
            if candidate != i64::MAX as u64 {
                break candidate;
            }
        };
        Ok(Self {
            block: Arc::new(block),
            nonce,
            encrypt_block_size,
            aes_block_count: encrypt_block_size / AES_BLOCK_SIZE as i64,
        })
    }

    fn stream(&self, counter: u64) -> CtrStream {
        let mut counter_block = [0_u8; AES_BLOCK_SIZE];
        counter_block[..8].copy_from_slice(&self.nonce.to_be_bytes());
        counter_block[8..].copy_from_slice(&counter.to_be_bytes());
        CtrStream {
            block: Arc::clone(&self.block),
            counter: counter_block,
            mask: [0; AES_BLOCK_SIZE],
            used: AES_BLOCK_SIZE,
        }
    }
}

struct CtrStream {
    block: Arc<AesCipher>,
    counter: [u8; AES_BLOCK_SIZE],
    mask: [u8; AES_BLOCK_SIZE],
    used: usize,
}

impl CtrStream {
    fn xor_key_stream(&mut self, data: &mut [u8]) {
        for value in data {
            if self.used == AES_BLOCK_SIZE {
                self.mask = self.counter;
                self.block.encrypt_block(&mut self.mask);
                for counter_byte in self.counter.iter_mut().rev() {
                    *counter_byte = counter_byte.wrapping_add(1);
                    if *counter_byte != 0 {
                        break;
                    }
                }
                self.used = 0;
            }
            *value ^= self.mask[self.used];
            self.used += 1;
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

/// Encrypting writer layer.
pub struct Writer<W>
where
    W: CloseWrite,
{
    error: Option<StickyError>,
    underlying: W,
    cipher_stream: CtrStream,
    buffer: Vec<u8>,
    flushed_user_data_count: i64,
    used: usize,
}

impl<W> Writer<W>
where
    W: CloseWrite,
{
    /// Creates an encrypting writer over `underlying`.
    #[must_use]
    pub fn new(underlying: W, cipher: &CtrCipher) -> Self {
        let buffer_size =
            usize::try_from(cipher.encrypt_block_size).expect("negative encrypt block size");
        Self {
            error: None,
            underlying,
            cipher_stream: cipher.stream(0),
            buffer: vec![0; buffer_size],
            flushed_user_data_count: 0,
            used: 0,
        }
    }

    /// Returns unused bytes in the current buffer.
    #[must_use]
    pub fn available_size(&self) -> usize {
        self.buffer.len() - self.used
    }

    /// Returns buffered plaintext bytes.
    #[must_use]
    pub fn buffered(&self) -> usize {
        self.used
    }

    /// Flushes the current encrypted block to the underlying writer.
    pub fn flush_buffer(&mut self) -> io::Result<()> {
        if let Some(error) = &self.error {
            return Err(error.to_error());
        }
        if self.used == 0 {
            return Ok(());
        }
        self.cipher_stream
            .xor_key_stream(&mut self.buffer[..self.used]);
        let result = match self.underlying.write(&self.buffer[..self.used]) {
            Ok(n) => {
                self.flushed_user_data_count += n as i64;
                if n < self.used {
                    Err(io::Error::new(io::ErrorKind::WriteZero, "short write"))
                } else {
                    self.used = 0;
                    Ok(())
                }
            }
            Err(error) => Err(error),
        };
        if let Err(error) = &result {
            self.error = Some(StickyError::from_error(error));
        }
        result
    }

    /// Returns plaintext not yet flushed to the underlying object.
    #[must_use]
    pub fn get_cache(&self) -> &[u8] {
        &self.buffer[..self.used]
    }

    /// Returns the logical offset of the cached plaintext.
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
        if let Some(error) = &self.error {
            return Err(error.to_error());
        }
        let mut written = 0;
        while source.len() > self.available_size() && self.error.is_none() {
            let copied = self.available_size();
            self.buffer[self.used..].copy_from_slice(&source[..copied]);
            self.used += copied;
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
        self.buffer[self.used..self.used + copied].copy_from_slice(&source[..copied]);
        self.used += copied;
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

/// Positional decrypting reader layer.
pub struct Reader<R>
where
    R: ReadAt,
{
    underlying: R,
    cipher: CtrCipher,
}

impl<R> Reader<R>
where
    R: ReadAt,
{
    /// Creates a decrypting positional reader.
    #[must_use]
    pub fn new(underlying: R, cipher: &CtrCipher) -> Self {
        Self {
            underlying,
            cipher: cipher.clone(),
        }
    }
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
        let block_size = self.cipher.encrypt_block_size;
        let offset_in_block = offset % block_size;
        let counter = (offset / block_size) * self.cipher.aes_block_count;
        let mut cursor = offset - offset_in_block;
        let mut block_offset = offset_in_block as usize;
        let mut buffer = vec![0; block_size as usize];
        let mut stream = self.cipher.stream(counter as u64);
        let mut total = 0;

        while total < destination.len() {
            let result = self.underlying.read_at(&mut buffer, cursor);
            if let Some(error) = result.error {
                if result.n == 0 || !error.is_eof() {
                    return ReadAtResult {
                        n: total,
                        error: Some(error),
                    };
                }
            }
            cursor += result.n as i64;
            stream.xor_key_stream(&mut buffer[..result.n]);
            let available = &buffer[block_offset..result.n];
            let copied = available.len().min(destination.len() - total);
            destination[total..total + copied].copy_from_slice(&available[..copied]);
            total += copied;
            block_offset = 0;
        }
        ReadAtResult::ok(total)
    }
}

#[cfg(test)]
mod tests {
    #![allow(non_snake_case)]

    use super::*;
    use crate::checksum;
    use crate::layered_io::ReadAtError;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct MemoryFile(Arc<Mutex<Vec<u8>>>);

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

    fn assert_reads(reader: impl ReadAt, logical_length: usize) {
        for (offset, expected_n, expected, eof) in [
            (0_i64, 10, b"0123456789".as_slice(), false),
            (5, 10, b"5678901234".as_slice(), false),
            (
                logical_length as i64 - 5,
                5,
                b"56789\0\0\0\0\0".as_slice(),
                true,
            ),
        ] {
            let mut destination = [0_u8; 10];
            let result = reader.read_at(&mut destination, offset);
            assert_eq!(result.n, expected_n);
            assert_eq!(destination, expected);
            assert_eq!(result.error.as_ref().is_some_and(ReadAtError::is_eof), eof);
        }
    }

    #[test]
    fn TestReadAt() {
        let cipher1 = CtrCipher::new().expect("first cipher");
        let cipher2 = CtrCipher::new().expect("second cipher");
        let data = b"0123456789".repeat(510);
        let logical_length = data.len() * 2;

        let file = MemoryFile::default();
        let mut writer = Writer::new(file.clone(), &cipher1);
        writer.write_all(&data).unwrap();
        writer.write_all(&data).unwrap();
        writer.close().unwrap();
        assert_reads(Reader::new(file, &cipher1), logical_length);

        let file = MemoryFile::default();
        let encrypted = Writer::new(file.clone(), &cipher1);
        let mut writer = checksum::Writer::new(encrypted);
        writer.write_all(&data).unwrap();
        writer.write_all(&data).unwrap();
        writer.close().unwrap();
        assert_reads(
            checksum::Reader::new(Reader::new(file, &cipher1)),
            logical_length,
        );

        let file = MemoryFile::default();
        let checksummed = checksum::Writer::new(file.clone());
        let mut writer = Writer::new(checksummed, &cipher1);
        writer.write_all(&data).unwrap();
        writer.write_all(&data).unwrap();
        writer.close().unwrap();
        assert_reads(
            Reader::new(checksum::Reader::new(file), &cipher1),
            logical_length,
        );

        let file = MemoryFile::default();
        let first = Writer::new(file.clone(), &cipher1);
        let mut writer = Writer::new(first, &cipher2);
        writer.write_all(&data).unwrap();
        writer.write_all(&data).unwrap();
        writer.close().unwrap();
        assert_reads(
            Reader::new(Reader::new(file, &cipher1), &cipher2),
            logical_length,
        );
    }
}
