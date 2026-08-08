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

use chrono::TimeZone;
use tidb_datatype::Datum;

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

fn skip_field(
    field_number: u64,
    wire: u64,
    input: &[u8],
    cursor: &mut usize,
) -> Result<(), FmSketchCodecError> {
    let length = match wire {
        0 => return read_varint(input, cursor).map(|_| ()),
        1 => 8,
        2 => usize::try_from(read_varint(input, cursor)?)
            .map_err(|_| FmSketchCodecError::VarintOverflow)?,
        3 => loop {
            let tag = read_varint(input, cursor)?;
            let nested_field = tag >> 3;
            let nested_wire = tag & 7;
            if nested_field == 0 || i32::try_from(nested_field).is_err() {
                return Err(FmSketchCodecError::InvalidWireType);
            }
            if nested_wire == 4 {
                return if nested_field == field_number {
                    Ok(())
                } else {
                    Err(FmSketchCodecError::InvalidWireType)
                };
            }
            skip_field(nested_field, nested_wire, input, cursor)?;
        },
        4 => return Err(FmSketchCodecError::InvalidWireType),
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

/// Go `hashDatum`: encode one typed datum with the statement time zone, then
/// hash the exact `codec.EncodeValue` bytes.
pub fn hash_datum<TZ: TimeZone>(
    timezone: &TZ,
    value: &Datum,
) -> Result<u64, tidb_codec::CodecError> {
    let encoded = tidb_codec::encode_value_in_timezone(timezone, std::slice::from_ref(value))?;
    Ok(hash_bytes(&encoded).h1)
}

/// Go `hashRow`: stream each typed datum's value encoding into one hash.
pub fn hash_row<TZ: TimeZone>(
    timezone: &TZ,
    values: &[Datum],
) -> Result<u64, tidb_codec::CodecError> {
    let encoded = tidb_codec::encode_value_in_timezone(timezone, values)?;
    Ok(hash_bytes(&encoded).h1)
}

/// Go `FMSketch.InsertValue` over a typed datum.
pub fn insert_value<TZ: TimeZone>(
    sketch: &mut FmSketch,
    timezone: &TZ,
    value: &Datum,
) -> Result<(), tidb_codec::CodecError> {
    sketch.insert_hash(hash_datum(timezone, value)?);
    Ok(())
}

/// Go `FMSketch.InsertRowValue` over typed row datums.
pub fn insert_row_value<TZ: TimeZone>(
    sketch: &mut FmSketch,
    timezone: &TZ,
    values: &[Datum],
) -> Result<(), tidb_codec::CodecError> {
    sketch.insert_hash(hash_row(timezone, values)?);
    Ok(())
}

#[must_use]
pub fn encode_fm_sketch(sketch: Option<&FmSketch>) -> Option<Vec<u8>> {
    let proto = fm_sketch_to_proto(Some(sketch?));
    let mut output = Vec::new();
    // Current generated tipb marks Mask non-nullable and always writes it.
    output.push(0x08);
    encode_varint(proto.mask, &mut output);
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
        let field_number = tag >> 3;
        let wire = tag & 7;
        if field_number == 0 || i32::try_from(field_number).is_err() {
            return Err(FmSketchCodecError::InvalidWireType);
        }
        match field_number {
            1 if wire == 0 => proto.mask = read_varint(data, &mut cursor)?,
            1 => return Err(FmSketchCodecError::InvalidWireType),
            2 if wire == 0 => proto.hashset.push(read_varint(data, &mut cursor)?),
            2 if wire == 2 => {
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
            2 => return Err(FmSketchCodecError::InvalidWireType),
            _ => skip_field(field_number, wire, data, &mut cursor)?,
        }
    }
    Ok(Some(FmSketch::from_raw_parts(
        proto.mask,
        MAX_SKETCH_SIZE,
        proto.hashset,
    )))
}
