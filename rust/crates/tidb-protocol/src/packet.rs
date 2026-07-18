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

use std::io::{Read, Write};

use crate::compression::{CompressedReader, CompressedWriter, CompressionAlgorithm};
use crate::error::PacketError;

/// Maximum payload represented by one MySQL packet frame.
pub const MAX_PAYLOAD_LEN: usize = (1 << 24) - 1;

/// TiDB's production default for `max_allowed_packet`.
pub const DEFAULT_MAX_ALLOWED_PACKET: usize = 64 << 20;

/// The four-byte uncompressed MySQL packet header.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PacketHeader {
    /// Number of payload bytes following this header.
    pub payload_len: usize,
    /// Packet sequence number, wrapping at 255.
    pub sequence: u8,
}

impl PacketHeader {
    /// Constructs a header after checking the three-byte length limit.
    pub fn new(payload_len: usize, sequence: u8) -> Result<Self, PacketError> {
        if payload_len > MAX_PAYLOAD_LEN {
            return Err(PacketError::PayloadLengthOverflow {
                length: payload_len,
            });
        }
        Ok(Self {
            payload_len,
            sequence,
        })
    }

    /// Encodes the header in MySQL's little-endian three-byte length format.
    pub fn encode(self) -> [u8; 4] {
        debug_assert!(self.payload_len <= MAX_PAYLOAD_LEN);
        [
            self.payload_len as u8,
            (self.payload_len >> 8) as u8,
            (self.payload_len >> 16) as u8,
            self.sequence,
        ]
    }

    /// Decodes a four-byte uncompressed MySQL packet header.
    pub fn decode(bytes: [u8; 4]) -> Self {
        Self {
            payload_len: usize::from(bytes[0])
                | (usize::from(bytes[1]) << 8)
                | (usize::from(bytes[2]) << 16),
            sequence: bytes[3],
        }
    }
}

/// Writes uncompressed MySQL packet frames to an I/O stream.
pub struct PacketWriter<W> {
    inner: W,
    sequence: u8,
}

impl<W: Write> PacketWriter<W> {
    /// Creates a writer whose first packet has sequence number zero.
    pub fn new(inner: W) -> Self {
        Self { inner, sequence: 0 }
    }

    /// Creates a writer starting at an explicit sequence number.
    pub fn with_sequence(inner: W, sequence: u8) -> Self {
        Self { inner, sequence }
    }

    /// Returns the sequence number that will be used by the next frame.
    pub fn sequence(&self) -> u8 {
        self.sequence
    }

    /// Sets the sequence number for the next frame.
    pub fn set_sequence(&mut self, sequence: u8) {
        self.sequence = sequence;
    }

    /// Writes one logical payload and its terminating frame.
    ///
    /// Payloads at least `MAX_PAYLOAD_LEN` bytes long are split into full
    /// frames.  As required by the MySQL protocol (and TiDB's Go
    /// `PacketIO.WritePacket`), an exact multiple receives a final zero-length
    /// frame so the reader can distinguish it from a continuation.
    pub fn write_packet(&mut self, payload: &[u8]) -> Result<(), PacketError> {
        let mut remaining = payload;
        loop {
            let frame_len = remaining.len().min(MAX_PAYLOAD_LEN);
            let header = PacketHeader::new(frame_len, self.sequence)?.encode();
            self.inner.write_all(&header)?;
            self.inner.write_all(&remaining[..frame_len])?;
            self.sequence = self.sequence.wrapping_add(1);
            remaining = &remaining[frame_len..];

            // A full frame always needs one more read to determine whether the
            // logical packet continues, including when no bytes remain.
            if frame_len < MAX_PAYLOAD_LEN {
                return Ok(());
            }
        }
    }

    /// Flushes all encoded bytes to the underlying stream.
    pub fn flush(&mut self) -> Result<(), PacketError> {
        self.inner.flush().map_err(PacketError::from)
    }

    /// Borrows the underlying stream.
    pub fn get_ref(&self) -> &W {
        &self.inner
    }

    /// Consumes the writer and returns the underlying stream.
    pub fn into_inner(self) -> W {
        self.inner
    }
}

/// Reads uncompressed MySQL packet frames from an I/O stream.
pub struct PacketReader<R> {
    inner: R,
    sequence: u8,
    max_allowed_packet: usize,
}

impl<R: Read> PacketReader<R> {
    /// Creates a reader with TiDB's 64 MiB default packet limit.
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            sequence: 0,
            max_allowed_packet: DEFAULT_MAX_ALLOWED_PACKET,
        }
    }

    /// Creates a reader with an explicit incoming packet limit.
    pub fn with_max_allowed_packet(inner: R, max_allowed_packet: usize) -> Self {
        Self {
            inner,
            sequence: 0,
            max_allowed_packet,
        }
    }

    /// Returns the sequence number expected for the next frame.
    pub fn sequence(&self) -> u8 {
        self.sequence
    }

    /// Sets the sequence number expected for the next frame.
    pub fn set_sequence(&mut self, sequence: u8) {
        self.sequence = sequence;
    }

    /// Returns the configured incoming logical-packet limit.
    pub fn max_allowed_packet(&self) -> usize {
        self.max_allowed_packet
    }

    /// Updates the incoming logical-packet limit.
    pub fn set_max_allowed_packet(&mut self, max_allowed_packet: usize) {
        self.max_allowed_packet = max_allowed_packet;
    }

    /// Reads one logical payload, joining continuation frames.
    pub fn read_packet(&mut self) -> Result<Vec<u8>, PacketError> {
        let mut payload = Vec::new();
        let mut accumulated_len = 0usize;

        loop {
            let header = self.read_header()?;
            let next_len = accumulated_len
                .checked_add(header.payload_len)
                .expect("usize overflow is impossible for a bounded packet header");
            if next_len > self.max_allowed_packet {
                return Err(PacketError::PacketTooLarge {
                    accumulated: next_len,
                    max_allowed: self.max_allowed_packet,
                });
            }

            let old_len = payload.len();
            payload.resize(old_len + header.payload_len, 0);
            if let Err(error) = self.inner.read_exact(&mut payload[old_len..]) {
                payload.truncate(old_len);
                return Err(PacketError::from(error));
            }
            accumulated_len = next_len;

            if header.payload_len < MAX_PAYLOAD_LEN {
                return Ok(payload);
            }
        }
    }

    /// Borrows the underlying stream.
    pub fn get_ref(&self) -> &R {
        &self.inner
    }

    /// Consumes the reader and returns the underlying stream.
    pub fn into_inner(self) -> R {
        self.inner
    }

    fn read_header(&mut self) -> Result<PacketHeader, PacketError> {
        let mut bytes = [0u8; 4];
        read_header_or_eof(&mut self.inner, &mut bytes)?;
        let header = PacketHeader::decode(bytes);
        if header.sequence != self.sequence {
            return Err(PacketError::InvalidSequence {
                expected: self.sequence,
                received: header.sequence,
            });
        }
        self.sequence = self.sequence.wrapping_add(1);
        Ok(header)
    }
}

enum PacketInput<R> {
    Uncompressed(R),
    Compressed(CompressedReader<R>),
}

/// Reads logical MySQL packets through the negotiated compression mode.
///
/// Inner packet sequence and outer compressed-envelope sequence are tracked
/// independently. Matching TiDB's MariaDB Connector/J compatibility rule,
/// only an inner sequence mismatch inside compressed mode is ignored; an
/// outer compressed-sequence mismatch is always an error.
pub struct PacketIoReader<R> {
    input: PacketInput<R>,
    sequence: u8,
    max_allowed_packet: usize,
}

impl<R: Read> PacketIoReader<R> {
    /// Creates a mode-aware packet reader with TiDB's default packet limit.
    pub fn new(inner: R, algorithm: CompressionAlgorithm) -> Result<Self, PacketError> {
        let input = match algorithm {
            CompressionAlgorithm::None => PacketInput::Uncompressed(inner),
            algorithm => PacketInput::Compressed(CompressedReader::new(inner, algorithm)?),
        };
        Ok(Self {
            input,
            sequence: 0,
            max_allowed_packet: DEFAULT_MAX_ALLOWED_PACKET,
        })
    }

    /// Returns the next expected inner packet sequence.
    #[must_use]
    pub const fn sequence(&self) -> u8 {
        self.sequence
    }

    /// Sets the next expected inner packet sequence.
    pub fn set_sequence(&mut self, sequence: u8) {
        self.sequence = sequence;
    }

    /// Returns the next expected compressed sequence when compression is active.
    #[must_use]
    pub fn compressed_sequence(&self) -> Option<u8> {
        match &self.input {
            PacketInput::Uncompressed(_) => None,
            PacketInput::Compressed(reader) => Some(reader.compressed_sequence()),
        }
    }

    /// Sets the next expected compressed sequence when compression is active.
    pub fn set_compressed_sequence(&mut self, sequence: u8) {
        if let PacketInput::Compressed(reader) = &mut self.input {
            reader.set_compressed_sequence(sequence);
        }
    }

    /// Updates the incoming logical-packet limit.
    pub fn set_max_allowed_packet(&mut self, max_allowed_packet: usize) {
        self.max_allowed_packet = max_allowed_packet;
    }

    /// Reads one logical payload, joining full-size continuation frames.
    pub fn read_packet(&mut self) -> Result<Vec<u8>, PacketError> {
        let mut payload = Vec::new();
        let mut accumulated_len = 0usize;
        loop {
            let mut header_bytes = [0; 4];
            self.read_header_exact(&mut header_bytes)?;
            let header = PacketHeader::decode(header_bytes);
            if header.sequence != self.sequence && !self.is_compressed() {
                return Err(PacketError::InvalidSequence {
                    expected: self.sequence,
                    received: header.sequence,
                });
            }
            // Compressed mode deliberately ignores a mismatched inner sequence
            // for MariaDB Connector/J 2.x, but advances TiDB's own expectation.
            self.sequence = self.sequence.wrapping_add(1);

            accumulated_len = accumulated_len
                .checked_add(header.payload_len)
                .expect("usize overflow is impossible for bounded packet headers");
            if accumulated_len > self.max_allowed_packet {
                return Err(PacketError::PacketTooLarge {
                    accumulated: accumulated_len,
                    max_allowed: self.max_allowed_packet,
                });
            }
            let old_len = payload.len();
            payload.resize(accumulated_len, 0);
            if let Err(error) = self.read_exact(&mut payload[old_len..]) {
                payload.truncate(old_len);
                return Err(error);
            }
            if header.payload_len < MAX_PAYLOAD_LEN {
                return Ok(payload);
            }
        }
    }

    /// Borrows the underlying stream.
    #[must_use]
    pub fn get_ref(&self) -> &R {
        match &self.input {
            PacketInput::Uncompressed(inner) => inner,
            PacketInput::Compressed(reader) => reader.get_ref(),
        }
    }

    /// Consumes the reader and returns the underlying stream.
    pub fn into_inner(self) -> R {
        match self.input {
            PacketInput::Uncompressed(inner) => inner,
            PacketInput::Compressed(reader) => reader.into_inner(),
        }
    }

    fn is_compressed(&self) -> bool {
        matches!(&self.input, PacketInput::Compressed(_))
    }

    fn read_exact(&mut self, output: &mut [u8]) -> Result<(), PacketError> {
        match &mut self.input {
            PacketInput::Uncompressed(inner) => inner.read_exact(output).map_err(PacketError::from),
            PacketInput::Compressed(reader) => {
                let mut position = 0;
                while position < output.len() {
                    let count = reader.read_bytes(&mut output[position..])?;
                    if count == 0 {
                        // Go's io.ReadFull retries a Reader that returns
                        // (0, nil). An empty compressed envelope has exactly
                        // that shape and must not terminate the inner stream.
                        continue;
                    }
                    position += count;
                }
                Ok(())
            }
        }
    }

    fn read_header_exact(&mut self, output: &mut [u8; 4]) -> Result<(), PacketError> {
        if let PacketInput::Uncompressed(inner) = &mut self.input {
            return read_header_or_eof(inner, output);
        }
        self.read_exact(output)
    }
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

enum PacketOutput<W> {
    Uncompressed(W),
    Compressed(CompressedWriter<W>),
}

/// Writes logical MySQL packets through the negotiated compression mode.
pub struct PacketIoWriter<W> {
    output: PacketOutput<W>,
    sequence: u8,
}

impl<W: Write> PacketIoWriter<W> {
    /// Creates a mode-aware writer whose inner and outer sequences start at zero.
    pub fn new(inner: W, algorithm: CompressionAlgorithm) -> Result<Self, PacketError> {
        let output = match algorithm {
            CompressionAlgorithm::None => PacketOutput::Uncompressed(inner),
            algorithm => PacketOutput::Compressed(CompressedWriter::new(inner, algorithm)?),
        };
        Ok(Self {
            output,
            sequence: 0,
        })
    }

    /// Returns the sequence assigned to the next inner packet frame.
    #[must_use]
    pub const fn sequence(&self) -> u8 {
        self.sequence
    }

    /// Sets the sequence assigned to the next inner packet frame.
    pub fn set_sequence(&mut self, sequence: u8) {
        self.sequence = sequence;
    }

    /// Returns the next compressed sequence when compression is active.
    #[must_use]
    pub fn compressed_sequence(&self) -> Option<u8> {
        match &self.output {
            PacketOutput::Uncompressed(_) => None,
            PacketOutput::Compressed(writer) => Some(writer.compressed_sequence()),
        }
    }

    /// Sets the next compressed sequence when compression is active.
    pub fn set_compressed_sequence(&mut self, sequence: u8) {
        if let PacketOutput::Compressed(writer) = &mut self.output {
            writer.set_compressed_sequence(sequence);
        }
    }

    /// Sets the zstd level used for subsequent compressed envelopes.
    pub fn set_zstd_level(&mut self, level: i32) {
        if let PacketOutput::Compressed(writer) = &mut self.output {
            writer.set_zstd_level(level);
        }
    }

    /// Writes one logical payload and its terminating frame.
    pub fn write_packet(&mut self, payload: &[u8]) -> Result<(), PacketError> {
        self.write_logical_payload(payload)
    }

    /// Writes Go `PacketIO.WritePacket`'s source-shaped buffer.
    ///
    /// The first four bytes are reserved for the packet header and are not
    /// part of the logical payload. This is the boundary used by TiDB's
    /// server encoder and by the original packetio tests.
    pub fn write_packet_buffer(&mut self, packet: &[u8]) -> Result<(), PacketError> {
        let payload = packet.get(4..).ok_or(PacketError::PacketBufferTooShort {
            length: packet.len(),
        })?;
        self.write_logical_payload(payload)
    }

    fn write_logical_payload(&mut self, payload: &[u8]) -> Result<(), PacketError> {
        let mut remaining = payload;
        loop {
            let frame_len = remaining.len().min(MAX_PAYLOAD_LEN);
            let header = PacketHeader::new(frame_len, self.sequence)?.encode();
            self.write_bytes(&header)?;
            self.write_bytes(&remaining[..frame_len])?;
            self.sequence = self.sequence.wrapping_add(1);
            remaining = &remaining[frame_len..];
            if frame_len < MAX_PAYLOAD_LEN {
                return Ok(());
            }
        }
    }

    /// Flushes buffered bytes to the underlying stream.
    ///
    /// Go resets the inner sequence to the next compressed sequence after a
    /// compressed flush; preserve that observable state transition exactly.
    pub fn flush(&mut self) -> Result<(), PacketError> {
        match &mut self.output {
            PacketOutput::Uncompressed(inner) => inner.flush().map_err(PacketError::from),
            PacketOutput::Compressed(writer) => {
                writer.flush_envelope()?;
                writer.flush_inner()?;
                self.sequence = writer.compressed_sequence();
                Ok(())
            }
        }
    }

    /// Borrows the underlying stream.
    #[must_use]
    pub fn get_ref(&self) -> &W {
        match &self.output {
            PacketOutput::Uncompressed(inner) => inner,
            PacketOutput::Compressed(writer) => writer.get_ref(),
        }
    }

    /// Consumes the writer and returns the underlying stream.
    pub fn into_inner(self) -> W {
        match self.output {
            PacketOutput::Uncompressed(inner) => inner,
            PacketOutput::Compressed(writer) => writer.into_inner(),
        }
    }

    fn write_bytes(&mut self, data: &[u8]) -> Result<(), PacketError> {
        match &mut self.output {
            PacketOutput::Uncompressed(inner) => inner.write_all(data).map_err(PacketError::from),
            PacketOutput::Compressed(writer) => {
                let written = writer.write_bytes(data)?;
                if written == data.len() {
                    Ok(())
                } else {
                    Err(PacketError::from(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "compressed packet writer accepted a short write",
                    )))
                }
            }
        }
    }
}
