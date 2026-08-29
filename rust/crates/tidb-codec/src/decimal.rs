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
use tidb_datatype::{decimal_bin_size, Decimal, DecimalCodecWarning};

const MAX_DECIMAL_SCALE: i64 = 30;

/// Appends TiDB/MySQL's mem-comparable decimal representation at the shape the
/// value carries.
///
/// The two leading bytes contain precision and scale, followed by MySQL's
/// packed decimal binary format. The lossless coefficient/storage scale from
/// `tidb-datatype` is used; SQL display rounding is never consulted.
///
/// The shape is [`Decimal::storage_shape`] because Go's generic
/// `codec.encode` (`pkg/util/codec/codec.go`, the encoder behind `EncodeKey`
/// and `EncodeValue`) reads `vals[i].Length(), vals[i].Frac()` exactly as
/// `rowcodec` does. A `DECIMAL(10, 4)` index key must therefore be written at
/// `(10, 4)`, which also keeps every key in that index the same payload width
/// — a natural shape per value would make two rows of one column mutually
/// incomparable. An unstamped value passes `(0, 0)` and gets its natural
/// shape, which is Go's unset `Datum.length`.
pub fn encode_decimal(buffer: &mut Vec<u8>, decimal: &Decimal) -> Result<(), CodecError> {
    let (precision, scale) = decimal.storage_shape();
    encode_decimal_fixed(buffer, decimal, precision, scale)
}

/// Appends TiDB/MySQL's fixed-schema decimal representation.
///
/// `precision == 0` selects the decimal's natural precision and storage scale,
/// matching Go `EncodeDecimal`. Otherwise `(precision, scale)` is the SQL
/// schema carried by Go's `Datum.Length`/`Datum.Frac`. The codec reports
/// truncation and integer overflow as distinct typed errors; deciding whether
/// either error is strict, ignored, or a warning belongs to the caller's
/// statement error context, just as it does after Go `EncodeDecimal` returns.
pub fn encode_decimal_fixed(
    buffer: &mut Vec<u8>,
    decimal: &Decimal,
    precision: i64,
    scale: i64,
) -> Result<(), CodecError> {
    let (precision, scale) = if precision == 0 {
        let (precision, scale) = derived_decimal_shape(decimal)?;
        (precision as i64, (scale as i64).min(MAX_DECIMAL_SCALE))
    } else {
        (precision, scale.min(MAX_DECIMAL_SCALE))
    };
    // Go appends these two bytes before `WriteBin`, including when `WriteBin`
    // rejects the signed shape. Preserve that partial-buffer contract.
    buffer.push(precision as u8);
    buffer.push(scale as u8);
    // The mem-comparable payload is exactly Go `MyDecimal.WriteBin`, ported and
    // byte-verified once in `tidb-datatype`; this codec only frames it with the
    // precision/scale header and maps the soft truncation/overflow signal to a
    // typed error for the caller's statement context.
    let (payload, warning) = decimal
        .to_bin(
            i32::try_from(precision).map_err(|_| CodecError::DecimalOutOfRange)?,
            i32::try_from(scale).map_err(|_| CodecError::DecimalOutOfRange)?,
        )
        .map_err(|_| CodecError::DecimalOutOfRange)?;
    buffer.extend_from_slice(&payload);
    match warning {
        None => Ok(()),
        Some(DecimalCodecWarning::Truncated) => Err(CodecError::DecimalTruncated),
        Some(DecimalCodecWarning::Overflow) => Err(CodecError::DecimalOverflow),
    }
}

/// Returns the fixed decimal encoding length, including precision/scale bytes.
///
/// This is the source-equivalent of Go `valueSizeOfDecimal`: it validates the
/// requested shape but does not encode or apply the caller's error policy.
pub(crate) fn decimal_encoded_len(
    decimal: &Decimal,
    precision: i64,
    scale: i64,
) -> Result<usize, CodecError> {
    let (precision, scale) = if precision == 0 {
        let (precision, scale) = derived_decimal_shape(decimal)?;
        (precision as i64, scale as i64)
    } else {
        (precision, scale)
    };
    if precision < 0 || scale < 0 {
        return Err(CodecError::DecimalOutOfRange);
    }
    Ok(2 + decimal_binary_len(precision as usize, scale as usize)?)
}

/// Decodes one TiDB/MySQL decimal, returning its precision and scale metadata.
pub fn decode_decimal(input: &[u8]) -> Result<(&[u8], Decimal, u8, u8), CodecError> {
    if input.len() < 3 {
        return Err(CodecError::InsufficientBytes);
    }
    let precision = input[0];
    let scale = input[1];
    let binary_len = decimal_binary_len(usize::from(precision), usize::from(scale))?;
    if binary_len == 0 {
        panic!("Go MyDecimal.FromBin indexes an empty decimal payload");
    }
    // The payload is Go `MyDecimal.FromBin`, ported and round-trip-verified in
    // `tidb-datatype`. A soft truncation warning is not an error here, matching
    // Go `FromBin` returning the value alongside it.
    let (value, _consumed, _warning) =
        Decimal::from_bin(&input[2..], i32::from(precision), i32::from(scale))
            .map_err(|_| CodecError::InvalidEncoding("invalid decimal payload"))?;
    Ok((&input[2 + binary_len..], value, precision, scale))
}

/// Returns the payload length including precision/scale bytes.
pub(crate) fn peek_decimal_len(input: &[u8]) -> Result<usize, CodecError> {
    let precision = usize::from(*input.first().ok_or(CodecError::InsufficientBytes)?);
    let scale = usize::from(*input.get(1).ok_or(CodecError::InsufficientBytes)?);
    Ok(2 + decimal_binary_len(precision, scale)?)
}

fn decimal_binary_len(precision: usize, scale: usize) -> Result<usize, CodecError> {
    decimal_bin_size(precision as i32, scale as i32).map_err(|_| CodecError::DecimalOutOfRange)
}

fn derived_decimal_shape(decimal: &Decimal) -> Result<(usize, usize), CodecError> {
    let source_scale = decimal.storage_scale() as usize;
    let coefficient = decimal.coefficient_digits();
    let integer_end = coefficient.len() - source_scale;
    let integer_digits = coefficient[..integer_end].trim_start_matches('0').len();
    let precision = (integer_digits + source_scale).max(1);
    Ok((precision, source_scale))
}

// The mem-comparable decimal PAYLOAD codec (Go `MyDecimal.WriteBin`/`FromBin`)
// lives once, faithfully ported and byte/round-trip verified, in
// `tidb-datatype` (`Decimal::to_bin`/`from_bin`); `encode_decimal_fixed` /
// `decode_decimal` above delegate to it rather than re-deriving the packing.
