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

use std::{fmt, io};

/// Errors returned while encoding or decoding a MySQL packet stream.
#[derive(Debug)]
pub enum PacketError {
    /// The stream ended cleanly before the first byte of a new packet header.
    EndOfStream,
    /// The underlying stream failed to read or write.
    Io(io::Error),
    /// A packet sequence byte did not match the next expected value.
    InvalidSequence {
        /// Sequence number expected by this stream.
        expected: u8,
        /// Sequence number received from the peer.
        received: u8,
    },
    /// A compressed-envelope sequence byte did not match the next expected value.
    InvalidCompressedSequence {
        /// Sequence number expected by the compressed stream.
        expected: u8,
        /// Sequence number received from the peer.
        received: u8,
    },
    /// The accumulated payload exceeds the configured incoming packet limit.
    PacketTooLarge {
        /// Payload length accumulated through the offending frame.
        accumulated: usize,
        /// Configured maximum payload accepted by the reader.
        max_allowed: usize,
    },
    /// A caller attempted to construct a header that cannot fit MySQL's
    /// three-byte payload-length field.
    PayloadLengthOverflow {
        /// Length that was rejected.
        length: usize,
    },
    /// A compressed reader or writer was constructed without a compression codec.
    CompressionAlgorithmRequired,
    /// A codec produced a different decoded length than the envelope declared.
    DecompressedLengthMismatch {
        /// Length declared in the compressed header.
        expected: usize,
        /// Number of bytes produced by the codec.
        actual: usize,
    },
    /// Go's `WritePacket` contract requires four reserved header bytes.
    PacketBufferTooShort {
        /// Buffer length supplied by the caller.
        length: usize,
    },
}

impl fmt::Display for PacketError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EndOfStream => formatter.write_str("packet stream reached EOF"),
            Self::Io(error) => write!(formatter, "packet I/O failed: {error}"),
            Self::InvalidSequence { expected, received } => write!(
                formatter,
                "invalid packet sequence: received {received}, expected {expected}"
            ),
            Self::InvalidCompressedSequence { expected, received } => write!(
                formatter,
                "invalid compressed sequence: received {received}, expected {expected}"
            ),
            Self::PacketTooLarge {
                accumulated,
                max_allowed,
            } => write!(
                formatter,
                "packet payload of {accumulated} bytes exceeds max_allowed_packet {max_allowed}"
            ),
            Self::PayloadLengthOverflow { length } => write!(
                formatter,
                "payload length {length} does not fit MySQL's three-byte packet header"
            ),
            Self::CompressionAlgorithmRequired => {
                formatter.write_str("compressed packet I/O requires zlib or zstd")
            }
            Self::DecompressedLengthMismatch { expected, actual } => write!(
                formatter,
                "compressed packet declared {expected} decoded bytes but produced {actual}"
            ),
            Self::PacketBufferTooShort { length } => write!(
                formatter,
                "packet buffer has {length} bytes; four reserved header bytes are required"
            ),
        }
    }
}

impl std::error::Error for PacketError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::EndOfStream
            | Self::InvalidSequence { .. }
            | Self::InvalidCompressedSequence { .. }
            | Self::PacketTooLarge { .. }
            | Self::PayloadLengthOverflow { .. }
            | Self::CompressionAlgorithmRequired
            | Self::DecompressedLengthMismatch { .. }
            | Self::PacketBufferTooShort { .. } => None,
        }
    }
}

impl From<io::Error> for PacketError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
