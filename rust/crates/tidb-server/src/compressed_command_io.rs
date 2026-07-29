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

//! Negotiated command packet I/O from `pkg/server/{conn.go,internal/packetio.go}`.
//!
//! The handshake parser remains the authority for capability intersection and
//! the requested zstd level. This module owns the immediate post-handshake
//! transition: choose zlib before zstd, read one sequence-zero command through
//! the mode-aware packet reader, dispatch it through the connection,
//! then write the sequence-one response through the matching packet writer.
//! Socket ownership, authentication, deadlines, metrics, and the server run
//! loop remain outside this boundary.

use std::io::{Read, Write};

use tidb_protocol::{CompressionAlgorithm, PacketError, PacketIoReader, PacketIoWriter};

use crate::handshake::CLIENT_ZSTD_COMPRESSION_ALGORITHM;
use crate::AuthHandshakeRequest;

/// MySQL `CLIENT_COMPRESS`, selected ahead of zstd when both bits are set.
pub const CLIENT_COMPRESS: u32 = 1 << 5;

/// Compression state copied from the completed handshake into command I/O.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NegotiatedCompression {
    algorithm: CompressionAlgorithm,
    zstd_level: i32,
}

impl NegotiatedCompression {
    /// Selects the source algorithm from an authentication-pending handshake.
    ///
    /// Go gives `CLIENT_COMPRESS` precedence when a client advertises both
    /// zlib and zstd. The zstd level is preserved even when zlib wins so the
    /// connection state remains an exact snapshot of the client response.
    #[must_use]
    pub fn from_handshake(request: &AuthHandshakeRequest) -> Self {
        let capability = request.negotiated_capability;
        let algorithm = if capability & CLIENT_COMPRESS != 0 {
            CompressionAlgorithm::Zlib
        } else if capability & CLIENT_ZSTD_COMPRESSION_ALGORITHM != 0 {
            CompressionAlgorithm::Zstd
        } else {
            CompressionAlgorithm::None
        };
        Self {
            algorithm,
            zstd_level: request.response.zstd_level,
        }
    }

    /// Returns the negotiated packet compression algorithm.
    #[must_use]
    pub const fn algorithm(self) -> CompressionAlgorithm {
        self.algorithm
    }

    /// Returns the zstd level preserved from the handshake response.
    #[must_use]
    pub const fn zstd_level(self) -> i32 {
        self.zstd_level
    }
}

/// Result of dispatching one command packet.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandIoOutcome {
    /// A response was written and flushed; the value is its logical packet count.
    ResponseWritten(usize),
    /// `COM_QUIT` closed the connection without writing a response.
    Quit,
}

/// Failure at one explicit command-I/O stage.
#[derive(Debug)]
pub enum CommandIoError {
    /// The negotiated packet reader rejected the request stream.
    Read(PacketError),
    /// The dispatcher's uncompressed response could not be decoded.
    Response(PacketError),
    /// The negotiated packet writer could not emit or flush the response.
    Write(PacketError),
}

/// Stateful command reader/writer using one negotiated compression mode.
pub struct CompressedCommandIo<R, W> {
    reader: PacketIoReader<R>,
    writer: PacketIoWriter<W>,
    compression: NegotiatedCompression,
}

impl<R: Read, W: Write> CompressedCommandIo<R, W> {
    /// Builds command I/O directly from the completed handshake request.
    pub fn from_handshake(
        reader: R,
        writer: W,
        request: &AuthHandshakeRequest,
    ) -> Result<Self, PacketError> {
        Self::new(
            reader,
            writer,
            NegotiatedCompression::from_handshake(request),
        )
    }

    /// Builds command I/O from an already selected compression snapshot.
    pub fn new(
        reader: R,
        writer: W,
        compression: NegotiatedCompression,
    ) -> Result<Self, PacketError> {
        let reader = PacketIoReader::new(reader, compression.algorithm())?;
        let mut writer = PacketIoWriter::new(writer, compression.algorithm())?;
        writer.set_zstd_level(compression.zstd_level());
        Ok(Self {
            reader,
            writer,
            compression,
        })
    }

    /// Returns the immutable negotiated compression snapshot.
    #[must_use]
    pub const fn compression(&self) -> NegotiatedCompression {
        self.compression
    }

    /// Borrows the caller-owned response stream.
    #[must_use]
    pub fn writer_ref(&self) -> &W {
        self.writer.get_ref()
    }

    /// Consumes the adapter and returns both caller-owned streams.
    pub fn into_inner(self) -> (R, W) {
        (self.reader.into_inner(), self.writer.into_inner())
    }
}
