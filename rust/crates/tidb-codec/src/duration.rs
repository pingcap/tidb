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

//! The fixed duration payload used by `pkg/util/codec`.
//!
//! Go's `EncodeValue` stores a MySQL duration as the duration's signed
//! nanoseconds passed through `EncodeInt`; the value has no FSP or SQL range
//! metadata on the wire. `DecodeOne` reconstructs a `types.Duration` with
//! `types.MaxFsp`, so this leaf preserves exactly that physical result while
//! leaving parsing, range checks, rounding, and session warning policy to the
//! typed time layer.

use crate::number::{decode_int, encode_int};
use crate::CodecError;

/// Go `types.MaxFsp`, assigned by `codec.DecodeOne` to decoded durations.
pub const MAX_DURATION_FSP: u8 = 6;

/// A source-shaped MySQL duration decoded from a value-codec payload.
///
/// The nanosecond count is the exact signed `time.Duration` integer from Go.
/// FSP is always [`MAX_DURATION_FSP`] for values reconstructed by
/// `pkg/util/codec.DecodeOne`; schema-specific FSP belongs to the caller that
/// decodes into a chunk column.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawDuration {
    nanoseconds: i64,
    fsp: u8,
}

/// Clock components extracted by Go `types.splitDuration`.
///
/// The decomposition is byte/value semantics only. MySQL's `TIME` range,
/// rounding policy, and warning state are intentionally not attached here.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawDurationParts {
    sign: i8,
    hours: u64,
    minutes: u8,
    seconds: u8,
    microseconds: u32,
}

impl RawDurationParts {
    /// Returns `-1` for negative values and `1` for zero/positive values.
    pub const fn sign(self) -> i8 {
        self.sign
    }

    /// Returns the complete hours component (not modulo 24).
    pub const fn hours(self) -> u64 {
        self.hours
    }

    /// Returns the minutes component.
    pub const fn minutes(self) -> u8 {
        self.minutes
    }

    /// Returns the seconds component.
    pub const fn seconds(self) -> u8 {
        self.seconds
    }

    /// Returns the truncated microseconds component.
    pub const fn microseconds(self) -> u32 {
        self.microseconds
    }
}

impl RawDuration {
    /// Creates the value produced by Go's `DecodeOne` duration branch.
    pub const fn from_nanoseconds(nanoseconds: i64) -> Self {
        Self {
            nanoseconds,
            fsp: MAX_DURATION_FSP,
        }
    }

    /// Returns the exact signed nanosecond count carried on the wire.
    pub const fn nanoseconds(self) -> i64 {
        self.nanoseconds
    }

    /// Returns the FSP assigned by the source decoder.
    pub const fn fsp(self) -> u8 {
        self.fsp
    }

    /// Splits nanoseconds using Go `types.splitDuration`'s clock semantics.
    ///
    /// Sub-microsecond nanoseconds are discarded, just as the source divides
    /// the final remainder by `time.Microsecond`. The returned hour count is
    /// unbounded by MySQL's SQL `TIME` maximum; range validation remains a
    /// separate typed temporal concern.
    pub const fn parts(self) -> RawDurationParts {
        let negative = self.nanoseconds < 0;
        let mut remaining = self.nanoseconds.unsigned_abs();
        let hour_nanos = 60_u64 * 60 * 1_000_000_000;
        let minute_nanos = 60_u64 * 1_000_000_000;
        let second_nanos = 1_000_000_000_u64;
        let hours = remaining / hour_nanos;
        remaining %= hour_nanos;
        let minutes = (remaining / minute_nanos) as u8;
        remaining %= minute_nanos;
        let seconds = (remaining / second_nanos) as u8;
        let microseconds = (remaining % second_nanos / 1_000) as u32;
        RawDurationParts {
            sign: if negative { -1 } else { 1 },
            hours,
            minutes,
            seconds,
            microseconds,
        }
    }
}

/// Appends Go `codec.EncodeInt(duration.Duration)` to `buffer`.
///
/// The FSP and MySQL `TIME` range are intentionally absent: the source wire
/// encoder writes only the signed nanosecond integer and does not validate the
/// schema metadata here.
pub fn encode_duration(buffer: &mut Vec<u8>, nanoseconds: i64) {
    encode_int(buffer, nanoseconds);
}

/// Decodes the eight-byte payload consumed by Go `codec.DecodeInt`.
///
/// The remainder is returned unchanged, matching Go's decoder contract. A
/// short payload is a physical framing error; no SQL duration interpretation
/// is attempted at this boundary.
pub fn decode_duration(input: &[u8]) -> Result<(&[u8], RawDuration), CodecError> {
    let (remain, nanoseconds) = decode_int(input)?;
    Ok((remain, RawDuration::from_nanoseconds(nanoseconds)))
}
