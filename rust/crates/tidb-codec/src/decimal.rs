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
use tidb_datatype::Decimal;

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
    let coefficient = decimal.coefficient_digits();
    let source_scale = decimal.storage_scale() as usize;
    let integer_end = coefficient.len() - source_scale;
    let source_integer = coefficient[..integer_end].trim_start_matches('0');
    let source_fraction = &coefficient[integer_end..];
    let target_integer_digits = precision - scale;

    let overflow = source_integer.len() > target_integer_digits;
    let integer = if overflow {
        source_integer[source_integer.len() - target_integer_digits..].to_string()
    } else {
        format!(
            "{}{}",
            "0".repeat(target_integer_digits - source_integer.len()),
            source_integer
        )
    };
    let retained_fraction = source_fraction.len().min(scale);
    let fraction = format!(
        "{}{}",
        &source_fraction[..retained_fraction],
        "0".repeat(scale - retained_fraction)
    );

    buffer.push(precision as u8);
    buffer.push(scale as u8);
    let start = buffer.len();
    encode_digit_groups(buffer, &integer, target_integer_digits, true)?;
    encode_digit_groups(buffer, &fraction, scale, false)?;

    if decimal.is_negative() {
        for byte in &mut buffer[start..] {
            *byte = !*byte;
        }
    }
    buffer[start] ^= 0x80;

    // `MyDecimal.WriteBin` assigns the fractional error after the integer
    // branch, so truncation wins when both conditions are present.
    if fraction_was_truncated(source_scale, scale) {
        Err(CodecError::DecimalTruncated)
    } else if overflow {
        Err(CodecError::DecimalOverflow)
    } else {
        Ok(())
    }
}

fn fraction_was_truncated(source_scale: usize, target_scale: usize) -> bool {
    let source_words = source_scale / DIGITS_PER_WORD;
    let source_trailing = source_scale % DIGITS_PER_WORD;
    let source_size = source_words * 4 + DIGITS_TO_BYTES[source_trailing];
    let target_words = target_scale / DIGITS_PER_WORD;
    let target_trailing = target_scale % DIGITS_PER_WORD;
    let target_size = target_words * 4 + DIGITS_TO_BYTES[target_trailing];

    target_size < source_size
        || (target_size == source_size
            && (target_trailing < source_trailing || target_words < source_words))
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
    let precision = *input.first().ok_or(CodecError::InsufficientBytes)?;
    let scale = *input.get(1).ok_or(CodecError::InsufficientBytes)?;
    validate_decimal_shape(usize::from(precision), usize::from(scale))?;
    let binary_len = decimal_binary_len(usize::from(precision), usize::from(scale))?;
    let binary = input
        .get(2..2 + binary_len)
        .ok_or(CodecError::InsufficientBytes)?;
    let negative = binary[0] & 0x80 == 0;
    let mut normalized = binary.to_vec();
    normalized[0] ^= 0x80;
    if negative {
        for byte in &mut normalized {
            *byte = !*byte;
        }
    }

    let integer_digits = usize::from(precision - scale);
    let mut offset = 0;
    let integer = decode_digit_groups(&normalized, &mut offset, integer_digits, true)?;
    let fraction = decode_digit_groups(&normalized, &mut offset, usize::from(scale), false)?;
    if offset != normalized.len() {
        return Err(CodecError::InvalidEncoding("decimal length mismatch"));
    }
    let literal = if scale == 0 {
        integer
    } else {
        format!("{integer}.{fraction}")
    };
    let value = Decimal::from_literal(&literal);
    let value = if negative && !value.is_zero() {
        value.negate()
    } else {
        value
    };
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

fn encode_digit_groups(
    output: &mut Vec<u8>,
    digits: &str,
    declared_digits: usize,
    leading_partial: bool,
) -> Result<(), CodecError> {
    if declared_digits == 0 {
        return Ok(());
    }
    let partial_digits = declared_digits % DIGITS_PER_WORD;
    let mut offset = 0;
    if leading_partial && partial_digits != 0 {
        let group = if digits.len() >= partial_digits {
            &digits[..partial_digits]
        } else {
            digits
        };
        write_group(output, parse_group(group)?, DIGITS_TO_BYTES[partial_digits]);
        offset = partial_digits.min(digits.len());
    }
    let full_groups = declared_digits / DIGITS_PER_WORD;
    for _ in 0..full_groups {
        let end = offset + DIGITS_PER_WORD;
        let group = digits.get(offset..end).ok_or(CodecError::InvalidEncoding(
            "decimal coefficient does not match scale",
        ))?;
        write_group(output, parse_group(group)?, 4);
        offset = end;
    }
    if !leading_partial && partial_digits != 0 {
        let end = offset + partial_digits;
        let group = digits.get(offset..end).ok_or(CodecError::InvalidEncoding(
            "decimal coefficient does not match scale",
        ))?;
        write_group(output, parse_group(group)?, DIGITS_TO_BYTES[partial_digits]);
    }
    Ok(())
}

fn decode_digit_groups(
    input: &[u8],
    offset: &mut usize,
    declared_digits: usize,
    leading_partial: bool,
) -> Result<String, CodecError> {
    if declared_digits == 0 {
        return Ok(if leading_partial {
            "0".to_string()
        } else {
            String::new()
        });
    }
    let partial_digits = declared_digits % DIGITS_PER_WORD;
    let mut digits = String::with_capacity(declared_digits.max(1));
    if leading_partial && partial_digits != 0 {
        let value = read_group(input, offset, DIGITS_TO_BYTES[partial_digits])?;
        if value >= 10_u32.pow(partial_digits as u32) {
            return Err(CodecError::InvalidEncoding(
                "decimal leading group overflow",
            ));
        }
        digits.push_str(&format!("{value:0partial_digits$}"));
    }
    for _ in 0..declared_digits / DIGITS_PER_WORD {
        let value = read_group(input, offset, 4)?;
        if value >= 1_000_000_000 {
            return Err(CodecError::InvalidEncoding("decimal word overflow"));
        }
        digits.push_str(&format!("{value:09}"));
    }
    if !leading_partial && partial_digits != 0 {
        let value = read_group(input, offset, DIGITS_TO_BYTES[partial_digits])?;
        if value >= 10_u32.pow(partial_digits as u32) {
            return Err(CodecError::InvalidEncoding(
                "decimal trailing group overflow",
            ));
        }
        digits.push_str(&format!("{value:0partial_digits$}"));
    }
    if leading_partial {
        let trimmed = digits.trim_start_matches('0');
        Ok(if trimmed.is_empty() { "0" } else { trimmed }.to_string())
    } else {
        Ok(digits)
    }
}

fn parse_group(group: &str) -> Result<u32, CodecError> {
    group
        .parse()
        .map_err(|_| CodecError::InvalidEncoding("non-decimal coefficient digit"))
}

fn write_group(output: &mut Vec<u8>, value: u32, bytes: usize) {
    let encoded = value.to_be_bytes();
    output.extend_from_slice(&encoded[4 - bytes..]);
}

fn read_group(input: &[u8], offset: &mut usize, bytes: usize) -> Result<u32, CodecError> {
    let group = input
        .get(*offset..*offset + bytes)
        .ok_or(CodecError::InsufficientBytes)?;
    *offset += bytes;
    Ok(group
        .iter()
        .fold(0_u32, |value, byte| (value << 8) | u32::from(*byte)))
}
