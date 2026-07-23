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
use tidb_datatype::{Decimal, DecimalCodecWarning};

const DIGITS_PER_WORD: usize = 9;
const DIGITS_TO_BYTES: [usize; 10] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];
// `MyDecimal.WriteBin` accepts all nine base-1e9 words (81 digits), even
// though SQL's declared DECIMAL width is capped at 65.
const MAX_DECIMAL_PRECISION: usize = 81;
const MAX_DECIMAL_SCALE: usize = 30;

/// Physical metadata carried by one Go `MyDecimal.WriteBin` payload.
///
/// This is intentionally separate from [`Decimal`]: callers that only need
/// to frame a value can inspect the schema precision/scale and exact byte
/// length without materializing a numeric value or applying SQL rounding,
/// overflow, or warning policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecimalWireMetadata {
    precision: u8,
    scale: u8,
    payload_len: usize,
}

impl DecimalWireMetadata {
    /// Returns the source precision byte.
    pub const fn precision(self) -> u8 {
        self.precision
    }

    /// Returns the source scale byte.
    pub const fn scale(self) -> u8 {
        self.scale
    }

    /// Returns the complete payload length, including precision and scale.
    pub const fn payload_len(self) -> usize {
        self.payload_len
    }
}

/// Inspects one decimal payload without deserializing its coefficient.
///
/// The returned remainder starts after the exact `DecimalPeak`-equivalent
/// payload.  This boundary owns only physical shape and framing; converting
/// to a SQL decimal and deciding truncation/overflow behavior remain the
/// caller's responsibility.
pub fn inspect_decimal(input: &[u8]) -> Result<(&[u8], DecimalWireMetadata), CodecError> {
    let payload_len = peek_decimal_len(input)?;
    let precision = *input.first().ok_or(CodecError::InsufficientBytes)?;
    let scale = *input.get(1).ok_or(CodecError::InsufficientBytes)?;
    if input.len() < payload_len {
        return Err(CodecError::InsufficientBytes);
    }
    Ok((
        &input[payload_len..],
        DecimalWireMetadata {
            precision,
            scale,
            payload_len,
        },
    ))
}

/// Appends TiDB/MySQL's fixed-precision mem-comparable decimal representation.
///
/// The two leading bytes contain precision and scale, followed by MySQL's
/// packed decimal binary format. The lossless coefficient/storage scale from
/// `tidb-datatype` is used; SQL display rounding is never consulted.
pub fn encode_decimal(buffer: &mut Vec<u8>, decimal: &Decimal) -> Result<(), CodecError> {
    encode_decimal_fixed(buffer, decimal, 0, 0)
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
    precision: usize,
    scale: usize,
) -> Result<(), CodecError> {
    let (precision, scale) = decimal_shape(decimal, precision, scale)?;
    // The mem-comparable payload is exactly Go `MyDecimal.WriteBin`, ported and
    // byte-verified once in `tidb-datatype`; this codec only frames it with the
    // precision/scale header and maps the soft truncation/overflow signal to a
    // typed error for the caller's statement context.
    let (payload, warning) = decimal
        .to_bin(precision as i32, scale as i32)
        .map_err(|_| CodecError::DecimalOutOfRange)?;
    buffer.push(precision as u8);
    buffer.push(scale as u8);
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
pub fn decimal_encoded_len(
    decimal: &Decimal,
    precision: usize,
    scale: usize,
) -> Result<usize, CodecError> {
    let (precision, scale) = decimal_shape(decimal, precision, scale)?;
    Ok(2 + decimal_binary_len(precision, scale)?)
}

/// Decodes one TiDB/MySQL decimal, returning its precision and scale metadata.
pub fn decode_decimal(input: &[u8]) -> Result<(&[u8], Decimal, u8, u8), CodecError> {
    decode_decimal_with_fault(input, false)
}

/// Source `errorInDecodeDecimal` failpoint seam.
///
/// Rust tests pass `true` explicitly rather than mutating process-global
/// failpoint state. Production callers use [`decode_decimal`].
pub fn decode_decimal_with_fault(
    input: &[u8],
    inject_error: bool,
) -> Result<(&[u8], Decimal, u8, u8), CodecError> {
    if inject_error {
        return Err(CodecError::InjectedFailure("errorInDecodeDecimal"));
    }
    let precision = *input.first().ok_or(CodecError::InsufficientBytes)?;
    let scale = *input.get(1).ok_or(CodecError::InsufficientBytes)?;
    validate_decimal_shape(usize::from(precision), usize::from(scale))?;
    let binary_len = decimal_binary_len(usize::from(precision), usize::from(scale))?;
    let binary = input
        .get(2..2 + binary_len)
        .ok_or(CodecError::InsufficientBytes)?;
    // The payload is Go `MyDecimal.FromBin`, ported and round-trip-verified in
    // `tidb-datatype`. A soft truncation warning is not an error here, matching
    // Go `FromBin` returning the value alongside it.
    let (value, _consumed, _warning) =
        Decimal::from_bin(binary, i32::from(precision), i32::from(scale))
            .map_err(|_| CodecError::InvalidEncoding("invalid decimal payload"))?;
    Ok((&input[2 + binary_len..], value, precision, scale))
}

/// Returns the payload length including precision/scale bytes.
pub(crate) fn peek_decimal_len(input: &[u8]) -> Result<usize, CodecError> {
    let precision = usize::from(*input.first().ok_or(CodecError::InsufficientBytes)?);
    let scale = usize::from(*input.get(1).ok_or(CodecError::InsufficientBytes)?);
    validate_decimal_shape(precision, scale)?;
    Ok(2 + decimal_binary_len(precision, scale)?)
}

fn decimal_binary_len(precision: usize, scale: usize) -> Result<usize, CodecError> {
    validate_decimal_shape(precision, scale)?;
    let integer = precision - scale;
    Ok(integer / DIGITS_PER_WORD * 4
        + DIGITS_TO_BYTES[integer % DIGITS_PER_WORD]
        + scale / DIGITS_PER_WORD * 4
        + DIGITS_TO_BYTES[scale % DIGITS_PER_WORD])
}

fn decimal_shape(
    decimal: &Decimal,
    precision: usize,
    scale: usize,
) -> Result<(usize, usize), CodecError> {
    if precision != 0 {
        validate_decimal_shape(precision, scale)?;
        return Ok((precision, scale));
    }

    let source_scale = decimal.storage_scale() as usize;
    let coefficient = decimal.coefficient_digits();
    let integer_end = coefficient.len() - source_scale;
    let integer_digits = coefficient[..integer_end].trim_start_matches('0').len();
    let precision = (integer_digits + source_scale).max(1);
    let scale = source_scale.min(MAX_DECIMAL_SCALE);
    validate_decimal_shape(precision, scale)?;
    Ok((precision, scale))
}

fn validate_decimal_shape(precision: usize, scale: usize) -> Result<(), CodecError> {
    if precision == 0
        || precision > MAX_DECIMAL_PRECISION
        || scale > MAX_DECIMAL_SCALE
        || scale > precision
    {
        return Err(CodecError::DecimalOutOfRange);
    }
    Ok(())
}

// The mem-comparable decimal PAYLOAD codec (Go `MyDecimal.WriteBin`/`FromBin`)
// lives once, faithfully ported and byte/round-trip verified, in
// `tidb-datatype` (`Decimal::to_bin`/`from_bin`); `encode_decimal_fixed` /
// `decode_decimal` above delegate to it rather than re-deriving the packing.
