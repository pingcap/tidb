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

//! MySQL compressed-protocol envelopes from `pkg/server/internal/packetio.go`.

use std::io::{Cursor, Read, Write};

use flate2::{read::ZlibDecoder, write::ZlibEncoder, Compression};

use crate::PacketError;

/// Go's maximum `net_buffer_length`, used as one compressed-envelope batch.
pub const MAX_COMPRESSED_BATCH_SIZE: usize = 1 << 20;

/// MySQL's `MIN_COMPRESS_LENGTH` boundary.
///
/// Go compresses only when the buffered length is strictly greater than 50.
pub const MIN_COMPRESS_LENGTH: usize = 50;

/// Compression codec negotiated for the MySQL compressed protocol.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompressionAlgorithm {
    /// Ordinary four-byte MySQL packets without a compressed envelope.
    None,
    /// RFC 1950 zlib stream, matching `mysql.CompressionZlib`.
    Zlib,
    /// Zstandard frame, matching `mysql.CompressionZstd`.
    Zstd,
}

/// The seven-byte compressed-protocol header.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompressedHeader {
    /// Bytes carried after this header.
    pub compressed_len: usize,
    /// Independent compressed-envelope sequence number.
    pub sequence: u8,
    /// Decoded byte length, or zero when the payload was sent verbatim.
    pub uncompressed_len: usize,
}

impl CompressedHeader {
    /// Encodes both three-byte lengths and the outer sequence.
    pub fn encode(self) -> Result<[u8; 7], PacketError> {
        if self.compressed_len > crate::MAX_PAYLOAD_LEN {
            return Err(PacketError::PayloadLengthOverflow {
                length: self.compressed_len,
            });
        }
        if self.uncompressed_len > crate::MAX_PAYLOAD_LEN {
            return Err(PacketError::PayloadLengthOverflow {
                length: self.uncompressed_len,
            });
        }
        Ok([
            self.compressed_len as u8,
            (self.compressed_len >> 8) as u8,
            (self.compressed_len >> 16) as u8,
            self.sequence,
            self.uncompressed_len as u8,
            (self.uncompressed_len >> 8) as u8,
            (self.uncompressed_len >> 16) as u8,
        ])
    }

    /// Decodes a compressed-protocol header.
    #[must_use]
    pub fn decode(bytes: [u8; 7]) -> Self {
        Self {
            compressed_len: decode_u24(&bytes[..3]),
            sequence: bytes[3],
            uncompressed_len: decode_u24(&bytes[4..]),
        }
    }
}

/// Buffers inner MySQL packet bytes into 1 MiB compressed envelopes.
pub struct CompressedWriter<W> {
    inner: W,
    algorithm: CompressionAlgorithm,
    compressed_sequence: u8,
    zstd_level: i32,
    buffered: Vec<u8>,
}

impl<W: Write> CompressedWriter<W> {
    /// Creates an envelope writer starting with compressed sequence zero.
    pub fn new(inner: W, algorithm: CompressionAlgorithm) -> Result<Self, PacketError> {
        if algorithm == CompressionAlgorithm::None {
            return Err(PacketError::CompressionAlgorithmRequired);
        }
        Ok(Self {
            inner,
            algorithm,
            compressed_sequence: 0,
            zstd_level: 3,
            buffered: Vec::with_capacity(MAX_COMPRESSED_BATCH_SIZE),
        })
    }

    /// Returns the sequence assigned to the next compressed envelope.
    #[must_use]
    pub const fn compressed_sequence(&self) -> u8 {
        self.compressed_sequence
    }

    /// Sets the sequence assigned to the next compressed envelope.
    pub fn set_compressed_sequence(&mut self, sequence: u8) {
        self.compressed_sequence = sequence;
    }

    /// Sets the zstd encoder level used by subsequent envelopes.
    pub fn set_zstd_level(&mut self, level: i32) {
        self.zstd_level = level;
    }

    /// Buffers bytes and flushes each complete 1 MiB batch immediately.
    pub fn write_bytes(&mut self, mut data: &[u8]) -> Result<usize, PacketError> {
        let original_len = data.len();
        loop {
            let remaining = MAX_COMPRESSED_BATCH_SIZE - self.buffered.len();
            if data.len() <= remaining {
                self.buffered.extend_from_slice(data);
                return Ok(original_len);
            }
            self.buffered.extend_from_slice(&data[..remaining]);
            data = &data[remaining..];
            self.flush_envelope()?;
        }
    }

    /// Emits the current buffer as exactly one compressed envelope.
    pub fn flush_envelope(&mut self) -> Result<(), PacketError> {
        let data = std::mem::take(&mut self.buffered);
        let (payload, uncompressed_len) = if data.len() > MIN_COMPRESS_LENGTH {
            (
                compress(self.algorithm, self.zstd_level, &data)?,
                data.len(),
            )
        } else {
            (data, 0)
        };
        let header = CompressedHeader {
            compressed_len: payload.len(),
            sequence: self.compressed_sequence,
            uncompressed_len,
        }
        .encode()?;
        self.inner.write_all(&header)?;
        self.inner.write_all(&payload)?;
        self.compressed_sequence = self.compressed_sequence.wrapping_add(1);
        self.buffered = Vec::with_capacity(MAX_COMPRESSED_BATCH_SIZE);
        Ok(())
    }

    /// Flushes the underlying stream without creating another envelope.
    pub fn flush_inner(&mut self) -> Result<(), PacketError> {
        self.inner.flush().map_err(PacketError::from)
    }

    /// Borrows the underlying stream.
    #[must_use]
    pub const fn get_ref(&self) -> &W {
        &self.inner
    }

    /// Consumes the writer and returns the underlying stream.
    pub fn into_inner(self) -> W {
        self.inner
    }
}

/// Presents decoded compressed envelopes as one continuous inner-packet byte stream.
pub struct CompressedReader<R> {
    inner: R,
    algorithm: CompressionAlgorithm,
    compressed_sequence: u8,
    decoded: Vec<u8>,
    position: usize,
}

impl<R: Read> CompressedReader<R> {
    /// Creates an envelope reader expecting compressed sequence zero.
    pub fn new(inner: R, algorithm: CompressionAlgorithm) -> Result<Self, PacketError> {
        if algorithm == CompressionAlgorithm::None {
            return Err(PacketError::CompressionAlgorithmRequired);
        }
        Ok(Self {
            inner,
            algorithm,
            compressed_sequence: 0,
            decoded: Vec::new(),
            position: 0,
        })
    }

    /// Returns the compressed sequence expected for the next envelope.
    #[must_use]
    pub const fn compressed_sequence(&self) -> u8 {
        self.compressed_sequence
    }

    /// Sets the compressed sequence expected for the next envelope.
    pub fn set_compressed_sequence(&mut self, sequence: u8) {
        self.compressed_sequence = sequence;
    }

    /// Reads decoded inner-packet bytes, spanning envelopes when necessary.
    pub fn read_bytes(&mut self, output: &mut [u8]) -> Result<usize, PacketError> {
        if output.is_empty() {
            return Ok(0);
        }
        if self.position == self.decoded.len() {
            self.load_envelope()?;
        }
        let available = &self.decoded[self.position..];
        let count = output.len().min(available.len());
        output[..count].copy_from_slice(&available[..count]);
        self.position += count;
        if self.position == self.decoded.len() {
            self.decoded.clear();
            self.position = 0;
        }
        Ok(count)
    }

    /// Borrows the underlying stream.
    #[must_use]
    pub const fn get_ref(&self) -> &R {
        &self.inner
    }

    /// Consumes the reader and returns the underlying stream.
    pub fn into_inner(self) -> R {
        self.inner
    }

    fn load_envelope(&mut self) -> Result<(), PacketError> {
        let mut bytes = [0u8; 7];
        read_header_or_eof(&mut self.inner, &mut bytes)?;
        let header = CompressedHeader::decode(bytes);
        if header.sequence != self.compressed_sequence {
            return Err(PacketError::InvalidCompressedSequence {
                expected: self.compressed_sequence,
                received: header.sequence,
            });
        }
        self.compressed_sequence = self.compressed_sequence.wrapping_add(1);

        let mut payload = vec![0; header.compressed_len];
        self.inner.read_exact(&mut payload)?;
        self.decoded = if header.uncompressed_len == 0 {
            payload
        } else {
            let decoded = decompress(self.algorithm, &payload)?;
            if decoded.len() != header.uncompressed_len {
                return Err(PacketError::DecompressedLengthMismatch {
                    expected: header.uncompressed_len,
                    actual: decoded.len(),
                });
            }
            decoded
        };
        self.position = 0;
        Ok(())
    }
}

fn compress(
    algorithm: CompressionAlgorithm,
    zstd_level: i32,
    data: &[u8],
) -> Result<Vec<u8>, PacketError> {
    match algorithm {
        CompressionAlgorithm::Zlib => {
            let mut encoder = ZlibEncoder::new(Vec::new(), Compression::new(6));
            encoder.write_all(data)?;
            encoder.finish().map_err(PacketError::from)
        }
        CompressionAlgorithm::Zstd => {
            zstd::stream::encode_all(Cursor::new(data), zstd_level).map_err(PacketError::from)
        }
        CompressionAlgorithm::None => Err(PacketError::CompressionAlgorithmRequired),
    }
}

fn decompress(algorithm: CompressionAlgorithm, payload: &[u8]) -> Result<Vec<u8>, PacketError> {
    match algorithm {
        CompressionAlgorithm::Zlib => {
            let mut decoder = ZlibDecoder::new(Cursor::new(payload));
            let mut decoded = Vec::new();
            decoder.read_to_end(&mut decoded)?;
            Ok(decoded)
        }
        CompressionAlgorithm::Zstd => {
            zstd::stream::decode_all(Cursor::new(payload)).map_err(PacketError::from)
        }
        CompressionAlgorithm::None => Err(PacketError::CompressionAlgorithmRequired),
    }
}

fn decode_u24(bytes: &[u8]) -> usize {
    usize::from(bytes[0]) | (usize::from(bytes[1]) << 8) | (usize::from(bytes[2]) << 16)
}

fn read_header_or_eof(reader: &mut impl Read, header: &mut [u8]) -> Result<(), PacketError> {
    loop {
        match reader.read(&mut header[..1]) {
            Ok(0) => return Err(PacketError::EndOfStream),
            Ok(_) => break,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(PacketError::from(error)),
        }
    }
    reader
        .read_exact(&mut header[1..])
        .map_err(PacketError::from)
}
