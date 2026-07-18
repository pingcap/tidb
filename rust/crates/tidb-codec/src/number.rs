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

use crate::CodecError;

const SIGN_MASK: u64 = 1 << 63;
const NEGATIVE_TAG_END: u8 = 8;
const POSITIVE_TAG_START: u8 = u8::MAX - 8;

/// Converts a signed integer to the unsigned value used by TiDB's comparable codec.
pub const fn encode_int_to_cmp_uint(value: i64) -> u64 {
    (value as u64) ^ SIGN_MASK
}

/// Reverses [`encode_int_to_cmp_uint`].
pub const fn decode_cmp_uint_to_int(value: u64) -> i64 {
    (value ^ SIGN_MASK) as i64
}

/// Appends an ascending mem-comparable signed integer.
pub fn encode_int(buffer: &mut Vec<u8>, value: i64) {
    buffer.extend_from_slice(&encode_int_to_cmp_uint(value).to_be_bytes());
}

/// Appends a descending mem-comparable signed integer.
pub fn encode_int_desc(buffer: &mut Vec<u8>, value: i64) {
    buffer.extend_from_slice(&(!encode_int_to_cmp_uint(value)).to_be_bytes());
}

/// Decodes one ascending mem-comparable signed integer.
pub fn decode_int(input: &[u8]) -> Result<(&[u8], i64), CodecError> {
    let (word, remain) = take_u64(input)?;
    Ok((remain, decode_cmp_uint_to_int(word)))
}

/// Decodes one descending mem-comparable signed integer.
pub fn decode_int_desc(input: &[u8]) -> Result<(&[u8], i64), CodecError> {
    let (word, remain) = take_u64(input)?;
    Ok((remain, decode_cmp_uint_to_int(!word)))
}

/// Appends an ascending mem-comparable unsigned integer.
pub fn encode_uint(buffer: &mut Vec<u8>, value: u64) {
    buffer.extend_from_slice(&value.to_be_bytes());
}

/// Appends a descending mem-comparable unsigned integer.
pub fn encode_uint_desc(buffer: &mut Vec<u8>, value: u64) {
    buffer.extend_from_slice(&(!value).to_be_bytes());
}

/// Decodes one ascending mem-comparable unsigned integer.
pub fn decode_uint(input: &[u8]) -> Result<(&[u8], u64), CodecError> {
    let (word, remain) = take_u64(input)?;
    Ok((remain, word))
}

/// Decodes one descending mem-comparable unsigned integer.
pub fn decode_uint_desc(input: &[u8]) -> Result<(&[u8], u64), CodecError> {
    let (word, remain) = take_u64(input)?;
    Ok((remain, !word))
}

/// Appends Go `encoding/binary.PutVarint`'s zig-zag LEB128 representation.
pub fn encode_varint(buffer: &mut Vec<u8>, value: i64) {
    let mut unsigned = (value as u64) << 1;
    if value < 0 {
        unsigned = !unsigned;
    }
    encode_uvarint(buffer, unsigned);
}

/// Decodes Go `encoding/binary.Varint`'s representation.
pub fn decode_varint(input: &[u8]) -> Result<(&[u8], i64), CodecError> {
    let (remain, unsigned) = decode_uvarint(input)?;
    let mut value = (unsigned >> 1) as i64;
    if unsigned & 1 != 0 {
        value = !value;
    }
    Ok((remain, value))
}

/// Appends Go `encoding/binary.PutUvarint`'s LEB128 representation.
pub fn encode_uvarint(buffer: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        buffer.push((value as u8) | 0x80);
        value >>= 7;
    }
    buffer.push(value as u8);
}

/// Decodes Go `encoding/binary.Uvarint`'s representation.
pub fn decode_uvarint(input: &[u8]) -> Result<(&[u8], u64), CodecError> {
    let mut value = 0_u64;
    for (index, byte) in input.iter().copied().enumerate() {
        if index == 9 && byte > 1 {
            return Err(CodecError::InvalidEncoding("varint larger than 64 bits"));
        }
        if byte < 0x80 {
            value |= u64::from(byte) << (7 * index);
            return Ok((&input[index + 1..], value));
        }
        if index >= 9 {
            return Err(CodecError::InvalidEncoding("varint larger than 64 bits"));
        }
        value |= u64::from(byte & 0x7f) << (7 * index);
    }
    Err(CodecError::InsufficientBytes)
}

/// Appends TiDB's variable-length mem-comparable signed integer.
pub fn encode_comparable_varint(buffer: &mut Vec<u8>, value: i64) {
    if value >= 0 {
        encode_comparable_uvarint(buffer, value as u64);
        return;
    }
    let bytes = value.to_be_bytes();
    let length = (1..8)
        .find(|length| {
            let threshold = -((1_i128 << (length * 8)) - 1);
            i128::from(value) >= threshold
        })
        .unwrap_or(8);
    buffer.push(NEGATIVE_TAG_END - length as u8);
    buffer.extend_from_slice(&bytes[8 - length..]);
}

/// Appends TiDB's variable-length mem-comparable unsigned integer.
pub fn encode_comparable_uvarint(buffer: &mut Vec<u8>, value: u64) {
    if value <= u64::from(POSITIVE_TAG_START - NEGATIVE_TAG_END) {
        buffer.push(value as u8 + NEGATIVE_TAG_END);
        return;
    }
    let bytes = value.to_be_bytes();
    let first = bytes.iter().position(|byte| *byte != 0).unwrap_or(7);
    let encoded = &bytes[first..];
    buffer.push(POSITIVE_TAG_START + encoded.len() as u8);
    buffer.extend_from_slice(encoded);
}

/// Decodes TiDB's variable-length mem-comparable unsigned integer.
pub fn decode_comparable_uvarint(input: &[u8]) -> Result<(&[u8], u64), CodecError> {
    let (&tag, remain) = input.split_first().ok_or(CodecError::InsufficientBytes)?;
    if tag < NEGATIVE_TAG_END {
        return Err(CodecError::InvalidEncoding(
            "negative tag for unsigned integer",
        ));
    }
    if tag <= POSITIVE_TAG_START {
        return Ok((remain, u64::from(tag - NEGATIVE_TAG_END)));
    }
    let length = usize::from(tag - POSITIVE_TAG_START);
    let bytes = remain.get(..length).ok_or(CodecError::InsufficientBytes)?;
    let value = bytes
        .iter()
        .fold(0_u64, |value, byte| (value << 8) | u64::from(*byte));
    Ok((&remain[length..], value))
}

/// Decodes TiDB's variable-length mem-comparable signed integer.
pub fn decode_comparable_varint(input: &[u8]) -> Result<(&[u8], i64), CodecError> {
    let (&tag, mut remain) = input.split_first().ok_or(CodecError::InsufficientBytes)?;
    if (NEGATIVE_TAG_END..=POSITIVE_TAG_START).contains(&tag) {
        return Ok((remain, i64::from(tag) - i64::from(NEGATIVE_TAG_END)));
    }
    let (length, mut value) = if tag < NEGATIVE_TAG_END {
        (usize::from(NEGATIVE_TAG_END - tag), u64::MAX)
    } else {
        (usize::from(tag - POSITIVE_TAG_START), 0)
    };
    let bytes = remain.get(..length).ok_or(CodecError::InsufficientBytes)?;
    for byte in bytes {
        value = (value << 8) | u64::from(*byte);
    }
    remain = &remain[length..];
    if tag > POSITIVE_TAG_START && value > i64::MAX as u64 {
        return Err(CodecError::InvalidEncoding(
            "positive comparable varint overflow",
        ));
    }
    if tag < NEGATIVE_TAG_END && value <= i64::MAX as u64 {
        return Err(CodecError::InvalidEncoding(
            "invalid negative comparable varint",
        ));
    }
    Ok((remain, value as i64))
}

fn take_u64(input: &[u8]) -> Result<(u64, &[u8]), CodecError> {
    let bytes: [u8; 8] = input
        .get(..8)
        .ok_or(CodecError::InsufficientBytes)?
        .try_into()
        .expect("slice length was checked");
    Ok((u64::from_be_bytes(bytes), &input[8..]))
}
