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

use std::error::Error;
use std::fmt;
use std::num::IntErrorKind;

/// The unspecified fractional-seconds precision accepted by TiDB.
pub const UNSPECIFIED_FSP: i64 = -1;
/// The maximum fractional-seconds precision accepted by MySQL and TiDB.
pub const MAX_FSP: i64 = 6;
/// The minimum fractional-seconds precision accepted by MySQL and TiDB.
pub const MIN_FSP: i64 = 0;
/// MySQL's default fractional-seconds precision.
pub const DEFAULT_FSP: i64 = 0;

/// An invalid fractional-seconds precision or decimal fraction.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum FspError {
    /// A precision below [`MIN_FSP`] other than [`UNSPECIFIED_FSP`].
    InvalidFsp(i64),
    /// A byte string that Go's `strconv.ParseInt` cannot parse as base 10.
    ParseInt {
        /// The complete byte slice passed to the integer parser.
        input: Vec<u8>,
        /// Whether the parsed integer exceeded the signed 64-bit range.
        out_of_range: bool,
    },
}

impl fmt::Display for FspError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFsp(fsp) => write!(formatter, "Invalid fsp {fsp}"),
            Self::ParseInt {
                input,
                out_of_range,
            } => {
                let input = String::from_utf8_lossy(input);
                let reason = if *out_of_range {
                    "value out of range"
                } else {
                    "invalid syntax"
                };
                write!(formatter, "strconv.ParseInt: parsing {input:?}: {reason}")
            }
        }
    }
}

impl Error for FspError {}

/// Applies TiDB's `CheckFsp` normalization.
///
/// An unspecified precision becomes the MySQL default, values above six are
/// clamped, and any other negative value is rejected.
pub const fn check_fsp(fsp: i64) -> Result<i64, FspError> {
    if fsp == UNSPECIFIED_FSP {
        Ok(DEFAULT_FSP)
    } else if fsp < MIN_FSP {
        Err(FspError::InvalidFsp(fsp))
    } else if fsp > MAX_FSP {
        Ok(MAX_FSP)
    } else {
        Ok(fsp)
    }
}

/// Parses and rounds a fractional-second byte string to microseconds.
///
/// The byte slice is deliberate: Go strings can contain arbitrary bytes and
/// `ParseFrac` performs byte-indexed slicing. Using `&str` here would narrow
/// that source contract and could introduce UTF-8 boundary panics.
pub fn parse_frac(input: &[u8], fsp: i64) -> Result<(i64, bool), FspError> {
    if input.is_empty() {
        return Ok((0, false));
    }

    let fsp = check_fsp(fsp)?;
    let fsp = usize::try_from(fsp).expect("checked FSP is non-negative");
    if fsp >= input.len() {
        let value = parse_i64(input)?;
        return Ok((value * pow10(MAX_FSP as usize - input.len()), false));
    }

    // Match Go's byte prefix and integer division, which truncates toward
    // zero for negative inputs.
    let value = (parse_i64(&input[..=fsp])? + 5) / 10;
    if value >= pow10(fsp) {
        return Ok((0, true));
    }

    Ok((value * pow10(MAX_FSP as usize - fsp), false))
}

/// Pads a fractional-second byte string to the requested digit width.
///
/// A leading minus sign does not count toward the width, matching TiDB's
/// internal `alignFrac` helper.
pub fn align_frac(input: &[u8], fsp: usize) -> Vec<u8> {
    let digits = input
        .len()
        .saturating_sub(usize::from(input.first() == Some(&b'-')));
    if digits >= fsp {
        return input.to_vec();
    }

    let aligned_len = input.len() + fsp - digits;
    let mut aligned = Vec::with_capacity(aligned_len);
    aligned.extend_from_slice(input);
    aligned.resize(aligned_len, b'0');
    aligned
}

fn parse_i64(input: &[u8]) -> Result<i64, FspError> {
    let out_of_range = match std::str::from_utf8(input) {
        Ok(text) => match text.parse::<i64>() {
            Ok(value) => return Ok(value),
            Err(error) => matches!(
                error.kind(),
                IntErrorKind::PosOverflow | IntErrorKind::NegOverflow
            ),
        },
        Err(_) => false,
    };
    Err(FspError::ParseInt {
        input: input.to_vec(),
        out_of_range,
    })
}

const fn pow10(exponent: usize) -> i64 {
    let mut value = 1_i64;
    let mut remaining = exponent;
    while remaining > 0 {
        value *= 10;
        remaining -= 1;
    }
    value
}
