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

//! Source-backed MySQL packet framing, including zlib/zstd compression.
//!
//! This leaf owns the four-byte inner packet header, continuation rules, and
//! seven-byte compressed envelope from `pkg/server/internal/packetio.go`.
//! TLS, authentication, socket deadlines, metrics, and server dispatch remain
//! explicit server obligations.

mod compression;
mod error;
mod packet;

pub use compression::{
    CompressedHeader, CompressedReader, CompressedWriter, CompressionAlgorithm,
    MAX_COMPRESSED_BATCH_SIZE, MIN_COMPRESS_LENGTH,
};
pub use error::PacketError;
pub use packet::{
    PacketHeader, PacketIoReader, PacketIoWriter, PacketReader, PacketWriter,
    DEFAULT_MAX_ALLOWED_PACKET, MAX_PAYLOAD_LEN,
};
