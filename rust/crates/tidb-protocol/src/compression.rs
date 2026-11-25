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

use flate2::read::ZlibDecoder;

use crate::PacketError;

/// Go's maximum `net_buffer_length`, used as one compressed-envelope batch.
pub const MAX_COMPRESSED_BATCH_SIZE: usize = 1 << 20;

/// MySQL's `MIN_COMPRESS_LENGTH` boundary.
///
/// Go compresses only when the buffered length is strictly greater than 50.
pub const MIN_COMPRESS_LENGTH: usize = 50;

const ZLIB_DEFAULT_LEVEL: i32 = 6;
const ZLIB_SHORT_ENVELOPE_LIMIT: usize = 512;
const ZLIB_PERIODIC_ENVELOPE_LIMIT: usize = 640;
const ZLIB_SHORT_STORED_LIMIT: usize = 256;
const ZLIB_PROBE_MIN_LENGTH: usize = 16 * 1024;

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
    compressor: PacketCompressor,
    compressed_sequence: u8,
    buffered: Vec<u8>,
    compressed: Vec<u8>,
}

enum PacketCompressor {
    Zlib {
        encoder: Option<zlib_rs::Deflate>,
        short_encoder: Option<zlib_rs::Deflate>,
        probe: Option<zstd::bulk::Compressor<'static>>,
    },
    Zstd {
        level: i32,
        encoder: Option<zstd::bulk::Compressor<'static>>,
    },
}

impl PacketCompressor {
    fn compress(&mut self, data: &[u8], output: &mut Vec<u8>) -> Result<usize, PacketError> {
        match self {
            Self::Zlib {
                encoder,
                short_encoder,
                probe,
            } => {
                if data.len() <= ZLIB_SHORT_STORED_LIMIT
                    && data.get(8..24).is_some_and(|sample| !sample.is_ascii())
                    && short_block_prefers_stored(data)
                {
                    return Ok(write_stored_zlib(data, output));
                }
                let encoder = if data.len() <= ZLIB_SHORT_ENVELOPE_LIMIT
                    || (data.len() <= ZLIB_PERIODIC_ENVELOPE_LIMIT && has_short_period(data))
                {
                    // Use the fast encoder and a smaller match table. The
                    // window still covers the entire short envelope.
                    let encoder = short_encoder.get_or_insert_with(|| {
                        zlib_rs::Deflate::new_with_config(zlib_rs::DeflateConfig {
                            level: 1,
                            window_bits: 10,
                            mem_level: 4,
                            ..Default::default()
                        })
                    });
                    encoder.reset();
                    encoder
                } else {
                    // Go short-circuits match searching when a large block offers no
                    // savings. Use a fast match-finding pass before the level-6
                    // encoder so repeated data with uniform byte counts still compresses.
                    let level = if data.len() >= ZLIB_PROBE_MIN_LENGTH
                        && compress_zstd(probe, 1, data, output)? >= data.len()
                    {
                        0
                    } else {
                        ZLIB_DEFAULT_LEVEL
                    };
                    let encoder =
                        encoder.get_or_insert_with(|| zlib_rs::Deflate::new(level, true, 15));
                    encoder.reset();
                    encoder.set_level(level).map_err(zlib_error)?;
                    encoder
                };
                output.resize(output.len().max(zlib_rs::compress_bound(data.len())), 0);
                let status = encoder
                    .compress(data, output, zlib_rs::DeflateFlush::Finish)
                    .map_err(zlib_error)?;
                if status != zlib_rs::Status::StreamEnd {
                    return Err(std::io::Error::other("incomplete zlib envelope compression").into());
                }
                Ok(encoder.total_out() as usize)
            }
            Self::Zstd { level, encoder } => compress_zstd(encoder, *level, data, output),
        }
    }
}

fn short_block_prefers_stored(data: &[u8]) -> bool {
    debug_assert!(data.len() <= ZLIB_SHORT_STORED_LIMIT);
    // Keep the normal encoder for a small alphabet, where a dynamic table
    // can save bytes even without LZ matches. A fixed table also cannot make
    // up the stored block's extra 30 header bits when enough literals take
    // nine bits each.
    let mut seen = [0u64; 4];
    let mut long_codes = 0;
    for &byte in data {
        seen[usize::from(byte / 64)] |= 1 << (byte % 64);
        long_codes += usize::from(byte >= 144);
    }
    let distinct: u32 = seen.iter().map(|word| word.count_ones()).sum();
    if distinct as usize * 3 < data.len() * 2 || long_codes < 30 {
        return false;
    }

    // No match of three bytes can exist if every triple is different. The
    // table has twice as many slots as the maximum input, so probing ends.
    let mut triples = [0u32; ZLIB_SHORT_STORED_LIMIT * 2];
    for window in data.windows(3) {
        let key = 1 + u32::from_le_bytes([window[0], window[1], window[2], 0]);
        let mut slot = (key.wrapping_mul(0x9e37_79b1) >> 23) as usize;
        while triples[slot] != 0 {
            if triples[slot] == key {
                return false;
            }
            slot = (slot + 1) & (triples.len() - 1);
        }
        triples[slot] = key;
    }
    true
}

// Four or more identical periods let the short encoder reuse the same
// nearby match throughout the envelope. Scan after the initial four
// bytes so a packet header does not hide repetition in its payload.
fn has_short_period(data: &[u8]) -> bool {
    let payload = &data[4..];
    let prefix = &payload[..4];
    payload[1..payload.len() / 4 + 4]
        .windows(4)
        .position(|candidate| candidate == prefix)
        .is_some_and(|index| {
            let period = index + 1;
            payload[period..] == payload[..payload.len() - period]
        })
}

fn write_stored_zlib(data: &[u8], output: &mut Vec<u8>) -> usize {
    let len = data.len() as u16;
    let data_end = 7 + data.len();
    let total = data_end + 4;
    output.resize(output.len().max(total), 0);
    // RFC 1950 zlib header, then a final RFC 1951 stored block.
    output[..3].copy_from_slice(&[0x78, 0x01, 0x01]);
    output[3..5].copy_from_slice(&len.to_le_bytes());
    output[5..7].copy_from_slice(&(!len).to_le_bytes());
    output[7..data_end].copy_from_slice(data);
    let checksum = zlib_rs::adler32::adler32(1, data);
    output[data_end..total].copy_from_slice(&checksum.to_be_bytes());
    total
}

fn zlib_error(error: zlib_rs::DeflateError) -> std::io::Error {
    std::io::Error::other(error.as_str())
}

fn compress_zstd(
    encoder: &mut Option<zstd::bulk::Compressor<'static>>,
    level: i32,
    data: &[u8],
    output: &mut Vec<u8>,
) -> Result<usize, PacketError> {
    let encoder = match encoder {
        Some(encoder) => encoder,
        slot => slot.insert(zstd::bulk::Compressor::new(level)?),
    };
    output.resize(output.len().max(zstd::zstd_safe::compress_bound(data.len())), 0);
    Ok(encoder.compress_to_buffer(data, output.as_mut_slice())?)
}

impl<W: Write> CompressedWriter<W> {
    /// Creates an envelope writer starting with compressed sequence zero.
    pub fn new(inner: W, algorithm: CompressionAlgorithm) -> Result<Self, PacketError> {
        let compressor = match algorithm {
            CompressionAlgorithm::Zlib => PacketCompressor::Zlib {
                encoder: None,
                short_encoder: None,
                probe: None,
            },
            CompressionAlgorithm::Zstd => PacketCompressor::Zstd { level: 3, encoder: None },
            CompressionAlgorithm::None => return Err(PacketError::CompressionAlgorithmRequired),
        };
        Ok(Self {
            inner,
            compressor,
            compressed_sequence: 0,
            buffered: Vec::new(),
            compressed: Vec::new(),
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
        if let PacketCompressor::Zstd { level: current, encoder } = &mut self.compressor {
            if *current != level {
                *current = level;
                *encoder = None;
            }
        }
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
        let result = self.write_envelope();
        self.buffered.clear();
        result
    }

    fn write_envelope(&mut self) -> Result<(), PacketError> {
        let data = &self.buffered;
        let (payload, uncompressed_len) = if data.len() > MIN_COMPRESS_LENGTH {
            let written = self.compressor.compress(data, &mut self.compressed)?;
            (&self.compressed[..written], data.len())
        } else {
            (data.as_slice(), 0)
        };
        let header = CompressedHeader {
            compressed_len: payload.len(),
            sequence: self.compressed_sequence,
            uncompressed_len,
        }
        .encode()?;
        self.inner.write_all(&header)?;
        self.inner.write_all(payload)?;
        self.compressed_sequence = self.compressed_sequence.wrapping_add(1);
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
