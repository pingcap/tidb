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

//! The eight-byte temporal payload used by Go `codec.EncodeMySQLTime`.
//!
//! Go puts a `uintFlag` in front of this payload and then writes
//! `Time.ToPackedUint()` with `EncodeUint` (big-endian, no sign transform).
//! Field type, fractional precision, and timestamp timezone conversion are
//! deliberately not encoded here; those belong to the caller's schema and
//! session context. In particular, this module does not claim a complete
//! temporal `Datum` or SQL formatting implementation.

use crate::number::{decode_uint, encode_uint};
use crate::CodecError;
use tidb_datatype::PackedTime;

/// Appends one source-equivalent packed temporal payload.
pub fn encode_packed_time(buffer: &mut Vec<u8>, value: PackedTime) {
    encode_uint(buffer, value.raw());
}

/// Decodes one packed temporal payload and returns the unconsumed suffix.
pub fn decode_packed_time(input: &[u8]) -> Result<(&[u8], PackedTime), CodecError> {
    decode_uint(input).map(|(remain, raw)| (remain, PackedTime::from_raw(raw)))
}
