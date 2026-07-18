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

use crate::number::{decode_varint, encode_varint};
use crate::CodecError;

const GROUP_SIZE: usize = 8;
const MARKER: u8 = 0xff;

/// Returns the exact byte length produced by [`encode_bytes`].
pub const fn encoded_bytes_len(input_len: usize) -> usize {
    input_len + (GROUP_SIZE - input_len % GROUP_SIZE) + 1 + input_len / GROUP_SIZE
}

/// Appends TiDB's ascending mem-comparable bytes representation.
pub fn encode_bytes(buffer: &mut Vec<u8>, input: &[u8]) {
    buffer.reserve(encoded_bytes_len(input.len()));
    for offset in (0..=input.len()).step_by(GROUP_SIZE) {
        let remain = input.len() - offset;
        let count = remain.min(GROUP_SIZE);
        buffer.extend_from_slice(&input[offset..offset + count]);
        let padding = GROUP_SIZE - count;
        buffer.resize(buffer.len() + padding, 0);
        buffer.push(MARKER - padding as u8);
        if padding != 0 {
            break;
        }
    }
}

/// Appends raw bytes for RawKV, or TiDB's ascending mem-comparable bytes
/// representation for transactional keys.
pub fn encode_bytes_ext(buffer: &mut Vec<u8>, input: &[u8], is_raw_kv: bool) {
    if is_raw_kv {
        buffer.extend_from_slice(input);
    } else {
        encode_bytes(buffer, input);
    }
}

/// Decodes TiDB's ascending mem-comparable bytes representation.
pub fn decode_bytes(input: &[u8]) -> Result<(&[u8], Vec<u8>), CodecError> {
    decode_bytes_inner(input, false)
}

/// Appends TiDB's descending mem-comparable bytes representation.
pub fn encode_bytes_desc(buffer: &mut Vec<u8>, input: &[u8]) {
    let start = buffer.len();
    encode_bytes(buffer, input);
    invert_bytes(&mut buffer[start..]);
}

/// Decodes TiDB's descending mem-comparable bytes representation.
pub fn decode_bytes_desc(input: &[u8]) -> Result<(&[u8], Vec<u8>), CodecError> {
    decode_bytes_inner(input, true)
}

/// Appends TiDB's length-prefixed byte representation.
///
/// Unlike [`encode_bytes`], the result is compact but not mem-comparable.
pub fn encode_compact_bytes(buffer: &mut Vec<u8>, input: &[u8]) {
    buffer.reserve(10 + input.len());
    encode_varint(buffer, input.len() as i64);
    buffer.extend_from_slice(input);
}

/// Decodes one length-prefixed byte representation and returns its remainder.
pub fn decode_compact_bytes(input: &[u8]) -> Result<(&[u8], &[u8]), CodecError> {
    let (payload, declared_len) = decode_varint(input)?;
    let length = usize::try_from(declared_len).map_err(|_| CodecError::InsufficientBytes)?;
    let value = payload.get(..length).ok_or(CodecError::InsufficientBytes)?;
    Ok((&payload[length..], value))
}

/// Returns the encoded byte-value length without allocating its decoded payload.
pub(crate) fn peek_bytes_len(input: &[u8], reverse: bool) -> Result<usize, CodecError> {
    let mut offset = 0;
    loop {
        let group = input
            .get(offset..offset + GROUP_SIZE + 1)
            .ok_or(CodecError::InsufficientBytes)?;
        let marker = group[GROUP_SIZE];
        let padding = if reverse { marker } else { MARKER - marker };
        if padding > GROUP_SIZE as u8 {
            return Err(CodecError::InvalidEncoding("invalid bytes marker"));
        }
        offset += GROUP_SIZE + 1;
        if padding != 0 {
            let expected = if reverse { MARKER } else { 0 };
            if group[GROUP_SIZE - usize::from(padding)..GROUP_SIZE]
                .iter()
                .any(|byte| *byte != expected)
            {
                return Err(CodecError::InvalidEncoding("invalid bytes padding"));
            }
            return Ok(offset);
        }
    }
}

fn decode_bytes_inner(input: &[u8], reverse: bool) -> Result<(&[u8], Vec<u8>), CodecError> {
    let encoded_len = peek_bytes_len(input, reverse)?;
    let mut output = Vec::with_capacity(encoded_len);
    for group in input[..encoded_len].chunks_exact(GROUP_SIZE + 1) {
        let padding = if reverse {
            group[GROUP_SIZE]
        } else {
            MARKER - group[GROUP_SIZE]
        };
        let real = GROUP_SIZE - usize::from(padding);
        if reverse {
            output.extend(group[..real].iter().map(|byte| !byte));
        } else {
            output.extend_from_slice(&group[..real]);
        }
    }
    Ok((&input[encoded_len..], output))
}

fn invert_bytes(bytes: &mut [u8]) {
    for byte in bytes {
        *byte = !*byte;
    }
}

#[cfg(test)]
mod tests {
    use super::invert_bytes;

    #[test]
    fn safe_inversion_replaces_go_runtime_specific_fast_path() {
        let original = [1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247];
        let mut bytes = original;
        invert_bytes(&mut bytes);
        assert_eq!(
            bytes,
            [
                254, 253, 252, 251, 250, 249, 248, 247, 0, 255, 255, 255, 255, 255, 255, 255, 255,
                8,
            ]
        );
        invert_bytes(&mut bytes);
        assert_eq!(bytes, original);
    }
}
