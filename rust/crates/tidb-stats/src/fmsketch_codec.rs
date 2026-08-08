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

//! Encoded-datum and protobuf boundaries from `pkg/statistics/fmsketch.go`.

use crate::{hash_bytes, FmSketch, MAX_SKETCH_SIZE};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FmSketchProto {
    pub mask: u64,
    pub hashset: Vec<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FmSketchCodecError {
    Truncated,
    VarintOverflow,
    InvalidWireType,
}

fn encode_varint(mut value: u64, output: &mut Vec<u8>) {
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn read_varint(input: &[u8], cursor: &mut usize) -> Result<u64, FmSketchCodecError> {
    let mut value = 0_u64;
    for shift in (0..70).step_by(7) {
        let byte = *input.get(*cursor).ok_or(FmSketchCodecError::Truncated)?;
        *cursor += 1;
        if shift == 63 && byte > 1 {
            return Err(FmSketchCodecError::VarintOverflow);
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(FmSketchCodecError::VarintOverflow)
}

fn skip_field(wire: u64, input: &[u8], cursor: &mut usize) -> Result<(), FmSketchCodecError> {
    let length = match wire {
        0 => return read_varint(input, cursor).map(|_| ()),
        1 => 8,
        2 => usize::try_from(read_varint(input, cursor)?)
            .map_err(|_| FmSketchCodecError::VarintOverflow)?,
        5 => 4,
        _ => return Err(FmSketchCodecError::InvalidWireType),
    };
    *cursor = cursor
        .checked_add(length)
        .filter(|end| *end <= input.len())
        .ok_or(FmSketchCodecError::Truncated)?;
    Ok(())
}

#[must_use]
pub fn fm_sketch_to_proto(sketch: Option<&FmSketch>) -> FmSketchProto {
    sketch.map_or_else(FmSketchProto::default, |sketch| FmSketchProto {
        mask: sketch.mask(),
        hashset: sketch.sorted_hashes(),
    })
}

#[must_use]
pub fn fm_sketch_from_proto(proto: Option<&FmSketchProto>) -> Option<FmSketch> {
    proto.map(|proto| FmSketch::from_raw_parts(proto.mask, 0, proto.hashset.iter().copied()))
}

/// Inserts one caller-owned `codec.EncodeValue` result.
pub fn insert_encoded_value(sketch: &mut FmSketch, encoded: &[u8]) {
    sketch.insert_hash(hash_bytes(encoded).h1);
}

/// Hashes encoded row values in order, equivalent to Go's streaming writes.
pub fn insert_encoded_row<'a>(
    sketch: &mut FmSketch,
    encoded_values: impl IntoIterator<Item = &'a [u8]>,
) {
    let mut row = Vec::new();
    for encoded in encoded_values {
        row.extend_from_slice(encoded);
    }
    insert_encoded_value(sketch, &row);
}

#[must_use]
pub fn encode_fm_sketch(sketch: Option<&FmSketch>) -> Option<Vec<u8>> {
    let proto = fm_sketch_to_proto(Some(sketch?));
    let mut output = Vec::new();
    if proto.mask != 0 {
        output.push(0x08);
        encode_varint(proto.mask, &mut output);
    }
    for hash in proto.hashset {
        output.push(0x10);
        encode_varint(hash, &mut output);
    }
    Some(output)
}

pub fn decode_fm_sketch(data: Option<&[u8]>) -> Result<Option<FmSketch>, FmSketchCodecError> {
    let Some(data) = data else {
        return Ok(None);
    };
    let mut cursor = 0;
    let mut proto = FmSketchProto::default();
    while cursor < data.len() {
        let tag = read_varint(data, &mut cursor)?;
        match (tag >> 3, tag & 7) {
            (1, 0) => proto.mask = read_varint(data, &mut cursor)?,
            (2, 0) => proto.hashset.push(read_varint(data, &mut cursor)?),
            (2, 2) => {
                let length = usize::try_from(read_varint(data, &mut cursor)?)
                    .map_err(|_| FmSketchCodecError::VarintOverflow)?;
                let end = cursor
                    .checked_add(length)
                    .filter(|end| *end <= data.len())
                    .ok_or(FmSketchCodecError::Truncated)?;
                while cursor < end {
                    proto.hashset.push(read_varint(&data[..end], &mut cursor)?);
                }
            }
            (0, _) => return Err(FmSketchCodecError::InvalidWireType),
            (_, wire) => skip_field(wire, data, &mut cursor)?,
        }
    }
    Ok(Some(FmSketch::from_raw_parts(
        proto.mask,
        MAX_SKETCH_SIZE,
        proto.hashset,
    )))
}
