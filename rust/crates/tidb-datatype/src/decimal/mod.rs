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

use std::cmp::Ordering;
use std::ops::Deref;

use crate::MyDecimal;
use smallvec::SmallVec;

// TPC-H's DECIMAL(15,2) values need up to 17 coefficient bytes. Keeping the
// common fixed-point widths inline avoids a heap allocation while decoding
// every row and while folding SUM/AVG states. Wider DECIMAL values still use
// SmallVec's spill path, so this does not change the supported precision.
const INLINE_DECIMAL_DIGITS: usize = 24;

/// The unsigned coefficient behind [`Decimal`]. Go's `MyDecimal` keeps its
/// base-1e9 words in the value itself, so ordinary chunk reads and datum
/// copies do not allocate. Keeping the common coefficient size inline gives
/// this value layer the same property while the `SmallVec` spill path retains
/// the complete DECIMAL precision range.
#[derive(Clone, Debug)]
struct DecimalDigits(SmallVec<[u8; INLINE_DECIMAL_DIGITS]>);

impl DecimalDigits {
    fn from_ascii(bytes: SmallVec<[u8; INLINE_DECIMAL_DIGITS]>) -> Self {
        debug_assert!(bytes.iter().all(u8::is_ascii_digit));
        Self(bytes)
    }

    fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0).expect("decimal coefficients are ASCII digits")
    }

    fn insert(&mut self, index: usize, digit: char) {
        debug_assert!(digit.is_ascii_digit());
        self.0.insert(index, digit as u8);
    }

    fn remove(&mut self, index: usize) -> char {
        char::from(self.0.remove(index))
    }

    fn push_str(&mut self, digits: &str) {
        debug_assert!(digits.bytes().all(|digit| digit.is_ascii_digit()));
        self.0.extend_from_slice(digits.as_bytes());
    }

    fn pop(&mut self) -> Option<char> {
        self.0.pop().map(char::from)
    }

    fn from_unsigned(mut value: u128) -> Self {
        let mut digits = SmallVec::<[u8; INLINE_DECIMAL_DIGITS]>::new();
        if value == 0 {
            digits.push(b'0');
        } else {
            while value != 0 {
                digits.push(b'0' + (value % 10) as u8);
                value /= 10;
            }
            digits.reverse();
        }
        Self::from_ascii(digits)
    }
}

impl From<String> for DecimalDigits {
    fn from(digits: String) -> Self {
        debug_assert!(digits.bytes().all(|digit| digit.is_ascii_digit()));
        Self(SmallVec::from_vec(digits.into_bytes()))
    }
}

impl Deref for DecimalDigits {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl std::fmt::Display for DecimalDigits {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A fixed-point decimal value: `(-1)^negative * digits * 10^-scale`, where
/// `digits` is an unsigned decimal digit string (no separators) at least
/// `scale` characters long (left-padded with `0` so slicing off the
/// fractional part is always valid). Mirrors MySQL `DECIMAL`: the literal's
/// own scale is preserved as written (`3.14` and `3.140` compare equal but
/// render differently — `3.14`/`3.140`), and a numerically zero value always
/// renders and compares without a sign (`-0.00` normalizes to `0.00`).
///
/// Arithmetic is done digit-by-digit on these strings (schoolbook add/
/// subtract/multiply/long-division) rather than via a numeric type, so it is
/// exact for any precision `DECIMAL` supports — no float-style rounding
/// error, and no dependency on how Rust or Go format a binary float. `/`
/// (MySQL's `DECIMAL` division: dividend scale plus a fixed precision
/// increment, then MyDecimal's rounding) is [`Decimal::true_div`] — a
/// different, harder problem than the truncating division `DIV`/`MOD` need
/// (see [`Decimal::div_rem`]).
#[derive(Debug, Clone)]
pub struct Decimal {
    negative: bool,
    digits: DecimalDigits,
    /// Fractional digits visible through `Display`/SQL result formatting.
    /// This is MyDecimal's `resultFrac` equivalent.
    scale: u32,
    /// Fractional digits retained for later decimal arithmetic. Division in
    /// TiDB stores whole base-1e9 words here even when `scale` exposes fewer
    /// digits, so an aggregate can consume precision a scalar result does
    /// not print. Normal literals and exact arithmetic keep this equal to
    /// `scale`.
    storage_scale: u32,
    /// The DECLARED `DECIMAL(M, D)` shape this value was converted into for a
    /// column, or `None` when no column is involved (a literal, an expression
    /// result, an intermediate).
    ///
    /// This is Go's `Datum.length`/`Datum.decimal` pair, which lives beside
    /// the value on the datum rather than inside `MyDecimal`: Go's
    /// `convertToMysqlDecimal` stamps it from the target `FieldType`
    /// (`pkg/types/datum.go`), the row-v2 encoder passes it straight into
    /// `codec.EncodeDecimal` (`pkg/util/rowcodec/encoder.go`), and
    /// `MyDecimal.PrecisionAndFrac` keeps reporting the value's own natural
    /// shape regardless. Our `Datum::Decimal` is a newtype over this value, so
    /// the pair rides here; every value-producing operation (parse, arithmetic,
    /// rounding) goes through [`Decimal::new_with_storage`] and therefore
    /// resets it to `None`, exactly as a fresh Go `Datum` starts at length 0.
    ///
    /// Storage bytes must use this shape, not the natural one: `11.99` written
    /// to a `DECIMAL(10, 4)` column is 7 payload bytes under `(10, 4)` and 3
    /// under its natural `(4, 2)`, and TiDB/TiCDC row checksums are computed
    /// over those bytes.
    declared_shape: Option<(i64, i64)>,
}

/// Source `MyDecimal.ToInt`/`ToUint` non-fatal disposition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecimalIntegerWarning {
    /// A non-zero fractional part was discarded.
    Truncated,
    /// The integer magnitude was outside the destination range.
    Overflow,
}

/// Source `MyDecimal.FromString`'s single non-fatal/fatal disposition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecimalParseError {
    /// A valid numeric prefix was accepted and trailing or excess digits lost.
    Truncated,
    /// The fixed MyDecimal integer buffer could not hold the result.
    Overflow,
    /// Integer exponent parsing exceeded its representable range.
    BadNumber,
    /// No decimal digits were present.
    TruncatedWrongValue,
}

impl Decimal {
    /// The single normalization point for ordinary values, whose stored and
    /// displayed scale are identical.
    fn new(negative: bool, digits: impl Into<DecimalDigits>, scale: u32) -> Self {
        Self::new_with_storage(negative, digits, scale, scale)
    }

    /// Normalizes a value whose internal decimal payload may retain more
    /// fraction digits than its SQL-visible result scale. `storage_scale`
    /// must never be smaller than `scale`.
    fn new_with_storage(
        negative: bool,
        digits: impl Into<DecimalDigits>,
        scale: u32,
        storage_scale: u32,
    ) -> Self {
        let mut digits = digits.into();
        debug_assert!(storage_scale >= scale);
        // Left-pad `digits` to at least the storage scale, then strip any
        // excess leading zeros back down to that same floor.
        while (digits.len() as u32) < storage_scale {
            digits.insert(0, '0');
        }
        let min_len = storage_scale.max(1) as usize;
        while digits.len() > min_len && digits.as_bytes()[0] == b'0' {
            digits.remove(0);
        }
        let is_zero = digits.bytes().all(|b| b == b'0');
        Decimal {
            negative: negative && !is_zero,
            digits,
            scale,
            storage_scale,
            declared_shape: None,
        }
    }

    /// Parses a decimal literal's canonical text — an optional `-`/`+` sign
    /// then digits with at most one `.`, which is exactly what this type's own
    /// [`Display`](std::fmt::Display) produces.
    ///
    /// The sign is part of the accepted syntax rather than the caller's job:
    /// this is the parser every canonical round trip goes through (a chunk
    /// cell read back as `MyDecimal` text, a spilled aggregate state, a
    /// binary-protocol cell), and a leading `-` left in the caller's hands
    /// silently produced a value whose unsigned digit string still carried
    /// the `-`. Such a value PRINTED correctly while comparing as positive
    /// and panicking in digit arithmetic. Accepting the sign here is what
    /// removes that possibility from every caller at once.
    pub fn from_literal(text: &str) -> Self {
        let (negative, magnitude) = match text.strip_prefix('-') {
            Some(magnitude) => (true, magnitude),
            None => (false, text.strip_prefix('+').unwrap_or(text)),
        };
        let (int_part, frac_part) = magnitude.split_once('.').unwrap_or((magnitude, ""));
        let int_stripped = int_part.trim_start_matches('0');
        let int_norm = if int_stripped.is_empty() {
            "0"
        } else {
            int_stripped
        };
        let scale = frac_part.len() as u32;
        // `Decimal::new` normalizes a numerically zero magnitude back to
        // non-negative, so `-0.00` needs no branch of its own here.
        Decimal::new(negative, format!("{int_norm}{frac_part}"), scale)
    }

    /// Builds an exact decimal from a signed base-10 coefficient and scale.
    /// This is used when a fixed-scale aggregate is finalized.
    pub fn from_scaled_i128(value: i128, scale: u32) -> Self {
        Decimal::new(
            value < 0,
            DecimalDigits::from_unsigned(value.unsigned_abs()),
            scale,
        )
    }

    /// Converts the exact chunk-layout [`MyDecimal`] into the value-layer
    /// decimal without losing either its visible `resultFrac` or the hidden
    /// base-1e9 fraction digits retained for later arithmetic.
    #[must_use]
    pub fn from_my_decimal(value: &MyDecimal) -> Self {
        let (negative, digits, storage_scale, result_scale) = value.to_decimal_parts();
        let storage_scale = storage_scale.max(result_scale);
        let digits: DecimalDigits = if storage_scale == value.digits_frac().max(0) as u32 {
            DecimalDigits::from_ascii(digits)
        } else {
            pad_scale(
                std::str::from_utf8(&digits).expect("MyDecimal coefficients are ASCII digits"),
                value.digits_frac().max(0) as u32,
                storage_scale,
            )
            .into()
        };
        Self::new_with_storage(negative, digits, result_scale, storage_scale)
    }

    /// Converts this value to Go's exact `MyDecimal` storage shape without
    /// discarding fraction digits retained beyond the displayed scale.
    pub fn to_my_decimal(&self) -> Result<MyDecimal, crate::mydecimal::DecimalError> {
        MyDecimal::from_decimal_parts(
            self.negative,
            &self.digits,
            self.storage_scale,
            self.scale,
            false,
        )
    }

    /// Converts this value to the `MyDecimal` shape used by Go chunk datums.
    /// Go's datum-to-chunk path always has at least one integer digit, even
    /// for values below one, while hidden fraction words remain intact.
    pub fn to_chunk_my_decimal(&self) -> Result<MyDecimal, crate::mydecimal::DecimalError> {
        MyDecimal::from_decimal_parts(
            self.negative,
            &self.digits,
            self.storage_scale,
            self.scale,
            true,
        )
    }

    /// Parses the signed decimal strings accepted by datatype conversion.
    pub fn from_signed_literal(text: &str) -> Self {
        Self::parse_mysql(text).0
    }

    /// Source `MyDecimal.FromString`, including the fixed word buffer,
    /// exponent parsing, prefix acceptance, and exact error disposition.
    pub fn parse_mysql(text: &str) -> (Self, Option<DecimalParseError>) {
        Self::parse_mysql_with_word_limit(text, CODEC_WORD_BUF_LEN)
    }

    pub(crate) fn parse_mysql_with_word_limit(
        text: &str,
        word_limit: usize,
    ) -> (Self, Option<DecimalParseError>) {
        let input = text.trim_start_matches([' ', '\t']);
        if input.is_empty() {
            return (
                Self::from_int(0),
                Some(DecimalParseError::TruncatedWrongValue),
            );
        }
        let bytes = input.as_bytes();
        let (negative, start) = match bytes[0] {
            b'-' => (true, 1),
            b'+' => (false, 1),
            _ => (false, 0),
        };
        let mut cursor = start;
        while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
            cursor += 1;
        }
        let integer_end = cursor;
        let mut end = cursor;
        if cursor < bytes.len() && bytes[cursor] == b'.' {
            end += 1;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
        }
        let integer_digits = integer_end - start;
        let fraction_start = if integer_end < end {
            integer_end + 1
        } else {
            end
        };
        let fraction_digits = end - fraction_start;
        if integer_digits + fraction_digits == 0 {
            return (
                Self::from_int(0),
                Some(DecimalParseError::TruncatedWrongValue),
            );
        }

        let words_int = digits_to_words(integer_digits);
        let words_frac = digits_to_words(fraction_digits);
        let mut disposition = None;
        let (kept_integer_digits, kept_fraction_digits) = if words_int + words_frac <= word_limit {
            (integer_digits, fraction_digits)
        } else if words_int > word_limit {
            disposition = Some(DecimalParseError::Overflow);
            (word_limit * DIGITS_PER_WORD, 0)
        } else {
            disposition = Some(DecimalParseError::Truncated);
            (integer_digits, (word_limit - words_int) * DIGITS_PER_WORD)
        };

        let int_begin = integer_end.saturating_sub(kept_integer_digits);
        let integer = &input[int_begin..integer_end];
        let fraction_end = (fraction_start + kept_fraction_digits).min(end);
        let fraction = &input[fraction_start..fraction_end];
        let magnitude = if fraction.is_empty() {
            if integer.is_empty() {
                "0".to_owned()
            } else {
                integer.to_owned()
            }
        } else {
            format!(
                "{}.{fraction}",
                if integer.is_empty() { "0" } else { integer }
            )
        };
        let signed_magnitude = if negative {
            format!("-{magnitude}")
        } else {
            magnitude
        };
        let mut value = Self::from_literal(&signed_magnitude);

        if end < input.len() && matches!(bytes[end], b'e' | b'E') {
            let (exponent, exponent_error) = parse_mysql_exponent(&input[end + 1..]);
            match exponent_error {
                Some(DecimalParseError::BadNumber) => {
                    return (Self::from_int(0), Some(DecimalParseError::BadNumber));
                }
                Some(DecimalParseError::Truncated) => {
                    disposition = Some(DecimalParseError::Truncated);
                }
                _ => {}
            }
            if exponent > i64::from(i32::MAX) / 2 {
                let max = Self::max_or_min(negative, (word_limit * DIGITS_PER_WORD) as u32, 0);
                return (max, Some(DecimalParseError::Overflow));
            }
            if exponent < i64::from(i32::MIN) / 2 {
                return (Self::from_int(0), Some(DecimalParseError::Truncated));
            }
            let (shifted, shift_warning) =
                value.shift_mysql_with_word_limit(exponent as i32, word_limit);
            value = shifted;
            if let Some(warning) = shift_warning {
                disposition = Some(match warning {
                    DecimalCodecWarning::Truncated => DecimalParseError::Truncated,
                    DecimalCodecWarning::Overflow => DecimalParseError::Overflow,
                });
                if warning == DecimalCodecWarning::Overflow {
                    value = Self::max_or_min(negative, (word_limit * DIGITS_PER_WORD) as u32, 0);
                }
            }
        } else if !input[end..].trim().is_empty() {
            disposition = Some(DecimalParseError::Truncated);
        }
        (value, disposition)
    }

    /// Promotes an integer to a decimal of scale 0, for mixed `int op decimal`
    /// arithmetic/comparison (MySQL's implicit promotion rule).
    pub fn from_int(i: i64) -> Self {
        Decimal::new(i < 0, i.unsigned_abs().to_string(), 0)
    }

    /// Promotes an unsigned integer to a decimal of scale 0. This must not
    /// pass through `i64`: values above `i64::MAX` are ordinary MySQL
    /// `UNSIGNED` values and retain their full magnitude in decimal
    /// arithmetic and comparison.
    pub fn from_uint(i: u64) -> Self {
        Decimal::new(false, i.to_string(), 0)
    }

    /// Source `MyDecimal.FromFloat64`.
    pub fn from_f64(value: f64) -> Option<Self> {
        if !value.is_finite() {
            return None;
        }
        let rendered = value.to_string();
        let expanded = crate::convert_scientific_notation(&rendered).ok()?;
        Some(Self::from_signed_literal(&expanded))
    }

    /// Source `MyDecimal.FromParquetArray`: decode a signed big-endian
    /// two's-complement Parquet DECIMAL payload and apply its logical scale.
    /// As in Go, negative input is converted to magnitude in place.
    pub fn from_parquet_array(bytes: &mut [u8], scale: i32) -> (Self, Option<DecimalCodecWarning>) {
        if bytes.is_empty() {
            return (Self::from_int(0), None);
        }
        let negative = bytes[0] & 0x80 != 0;
        if negative {
            for byte in bytes.iter_mut() {
                *byte = !*byte;
            }
            for byte in bytes.iter_mut().rev() {
                *byte = byte.wrapping_add(1);
                if *byte != 0 {
                    break;
                }
            }
        }

        let mut magnitude = "0".to_owned();
        for byte in bytes.iter().copied() {
            magnitude = digit_add(&digit_mul(&magnitude, "256"), &u32::from(byte).to_string());
        }
        if magnitude.trim_start_matches('0').len() > CODEC_WORD_BUF_LEN * DIGITS_PER_WORD {
            return (Self::from_int(0), Some(DecimalCodecWarning::Overflow));
        }
        let integer = Self::new(negative, magnitude, 0);
        let (shifted, warning) = integer.shift_mysql(-scale);
        if warning.is_some() {
            return (shifted, warning);
        }
        (shifted.truncate_to_scale(scale), None)
    }

    /// Source `NewMaxOrMinDec`/`maxDecimal`.
    pub fn max_or_min(negative: bool, precision: u32, frac: u32) -> Self {
        if precision == 0 {
            return Self::from_int(0);
        }
        Self::new(negative, "9".repeat(precision as usize), frac)
    }

    /// Returns the number of fractional decimal digits preserved by this
    /// value's representation.
    pub fn scale(&self) -> u32 {
        self.scale
    }

    /// Returns whether the stored numeric value is negative.
    ///
    /// This is a semantic storage/protocol accessor, not a leak of Go's
    /// base-1e9 `MyDecimal` word layout. Zero is normalized to non-negative.
    pub const fn is_negative(&self) -> bool {
        self.negative
    }

    /// Returns the lossless unsigned coefficient digits retained for exact
    /// arithmetic and storage codecs.
    ///
    /// The decimal value is `coefficient * 10^-storage_scale`; callers must
    /// use [`Decimal::storage_scale`] rather than the SQL-visible [`Self::scale`]
    /// because division can retain hidden precision for a later aggregate.
    pub fn coefficient_digits(&self) -> &str {
        &self.digits
    }

    /// Returns the signed coefficient and retained fractional scale when the
    /// coefficient fits in an i128.
    pub fn coefficient_i128(&self) -> Option<(i128, u32)> {
        let magnitude = self.digits.as_str().parse::<i128>().ok()?;
        let value = if self.negative {
            magnitude.checked_neg()?
        } else {
            magnitude
        };
        Some((value, self.storage_scale))
    }

    #[cfg(test)]
    pub(crate) fn coefficient_is_inline(&self) -> bool {
        !self.digits.0.spilled()
    }

    /// Builds a value straight from coefficient parts for differential tests
    /// of [`Ord::cmp`]: the tests need shapes the parser cannot produce
    /// directly (hidden division precision, excess leading zeros).
    #[cfg(test)]
    pub(crate) fn from_test_parts(
        negative: bool,
        digits: &str,
        scale: u32,
        storage_scale: u32,
    ) -> Self {
        Self::new_with_storage(negative, digits.to_string(), scale, storage_scale)
    }

    /// Returns the scale of the lossless stored coefficient.
    ///
    /// This can exceed [`Self::scale`], which is the rounded SQL presentation
    /// scale. Storage and protocol codecs need this value to avoid discarding
    /// arithmetic precision.
    pub const fn storage_scale(&self) -> u32 {
        self.storage_scale
    }

    /// Stamps the declared `DECIMAL(M, D)` column shape onto this value.
    ///
    /// Source: Go `Datum.convertToMysqlDecimal`'s
    /// `ret.SetLength(target.GetFlen()); ret.SetFrac(target.GetDecimal())`.
    #[must_use]
    pub fn with_declared_shape(mut self, flen: i64, decimal: i64) -> Self {
        self.declared_shape = Some((flen, decimal));
        self
    }

    /// The declared column shape, or `None` for a value no column produced.
    pub const fn declared_shape(&self) -> Option<(i64, i64)> {
        self.declared_shape
    }

    /// The `(precision, frac)` pair storage codecs must encode under.
    ///
    /// `(0, 0)` means "no declared shape", which is precisely what Go's
    /// `EncodeDecimal` reads as `precision == 0` and answers with
    /// `PrecisionAndFrac`; an unstamped Go `Datum` reports `Length() == 0` the
    /// same way. Callers pass this pair through unchanged so the fallback stays
    /// in the one place Go put it.
    pub const fn storage_shape(&self) -> (i64, i64) {
        match self.declared_shape {
            Some(shape) => shape,
            None => (0, 0),
        }
    }

    /// Source `MyDecimal.PrecisionAndFrac`.
    ///
    /// This is the value's OWN shape and never the column's; a payload written
    /// at `DECIMAL(10, 4)` still reports `(4, 2)` for `11.99`, matching Go.
    /// Storage codecs want [`Decimal::storage_shape`] instead.
    pub fn precision_and_frac(&self) -> (i32, i32) {
        let split = self.digits.len() - self.storage_scale as usize;
        let integer_digits = self.digits[..split].trim_start_matches('0').len() as i32;
        let fraction = self.storage_scale as i32;
        ((integer_digits + fraction).max(1), fraction)
    }

    /// Source `MyDecimal.ToHashKey`: numerically equal decimals with different
    /// written scales produce the same key.
    pub fn to_hash_key(&self) -> Result<(Vec<u8>, Option<DecimalCodecWarning>), DecimalCodecError> {
        let split = self.digits.len() - self.storage_scale as usize;
        let integer_digits = self.digits[..split].trim_start_matches('0').len() as i32;
        let significant_fraction = self.digits[split..].trim_end_matches('0').len() as i32;
        let precision = (integer_digits + significant_fraction).max(1);
        let (mut key, warning) = self.to_bin(precision, significant_fraction)?;
        key.push(significant_fraction as u8);
        Ok((
            key,
            if warning == Some(DecimalCodecWarning::Truncated) {
                None
            } else {
                warning
            },
        ))
    }

    /// Source `MyDecimal.HashKeySize`.
    pub fn hash_key_size(&self) -> Result<usize, DecimalCodecError> {
        let split = self.digits.len() - self.storage_scale as usize;
        let integer_digits = self.digits[..split].trim_start_matches('0').len() as i32;
        let significant_fraction = self.digits[split..].trim_end_matches('0').len() as i32;
        let precision = (integer_digits + significant_fraction).max(1);
        decimal_bin_size(precision, significant_fraction).map(|size| size + 1)
    }

    /// Returns whether this value is numerically zero.
    pub fn is_zero(&self) -> bool {
        self.digits.bytes().all(|b| b == b'0')
    }

    /// Returns this value with its sign reversed, canonicalizing zero.
    pub fn negate(&self) -> Self {
        Decimal::new_with_storage(
            !self.negative,
            self.digits.clone(),
            self.scale,
            self.storage_scale,
        )
    }

    /// Returns the non-negative magnitude of this value.
    pub fn abs(&self) -> Self {
        Decimal::new_with_storage(false, self.digits.clone(), self.scale, self.storage_scale)
    }

    /// `-1` / `0` / `1`, for `SIGN`.
    pub fn signum(&self) -> i64 {
        if self.is_zero() {
            0
        } else if self.negative {
            -1
        } else {
            1
        }
    }

    /// Exact decimal addition: aligns both operands to `max(scale1, scale2)`
    /// (an exact, no-rounding rescale — padding the shorter fractional part
    /// with zero digits), then adds or subtracts magnitudes depending on sign.
    pub fn add(&self, other: &Decimal) -> Decimal {
        if let Some(result) = self.try_add_fast(other) {
            return result;
        }
        let storage_scale = self.storage_scale.max(other.storage_scale);
        let scale = self.scale.max(other.scale);
        let a = pad_scale(&self.digits, self.storage_scale, storage_scale);
        let b = pad_scale(&other.digits, other.storage_scale, storage_scale);
        if self.negative == other.negative {
            return Decimal::new_with_storage(
                self.negative,
                digit_add(&a, &b),
                scale,
                storage_scale,
            );
        }
        match digit_cmp(&a, &b) {
            Ordering::Equal => {
                Decimal::new_with_storage(false, "0".to_string(), scale, storage_scale)
            }
            Ordering::Greater => {
                Decimal::new_with_storage(self.negative, digit_sub(&a, &b), scale, storage_scale)
            }
            Ordering::Less => {
                Decimal::new_with_storage(other.negative, digit_sub(&b, &a), scale, storage_scale)
            }
        }
    }

    /// Fast path for the fixed-scale DECIMAL folds used by TPC-H aggregates.
    /// Go's `MyDecimal.Add` operates directly on its nine base-1e9 words. For
    /// the common case where both values have the same storage scale and their
    /// coefficients fit in `i128`, doing the signed coefficient operation
    /// directly avoids allocating two padded strings and a digit-wise result.
    /// Any wider value or scale mismatch keeps the complete arbitrary-precision
    /// implementation above.
    fn try_add_fast(&self, other: &Decimal) -> Option<Decimal> {
        if self.storage_scale != other.storage_scale {
            return None;
        }
        let left = self.digits.parse::<i128>().ok()?;
        let right = other.digits.parse::<i128>().ok()?;
        let left = if self.negative {
            left.checked_neg()?
        } else {
            left
        };
        let right = if other.negative {
            right.checked_neg()?
        } else {
            right
        };
        let sum = left.checked_add(right)?;
        let negative = sum < 0;
        let digits = DecimalDigits::from_unsigned(sum.unsigned_abs());
        Some(Decimal::new_with_storage(
            negative,
            digits,
            self.scale.max(other.scale),
            self.storage_scale,
        ))
    }

    /// Source `DecimalAdd`, including MyDecimal's nine-word result bound.
    pub fn add_mysql(&self, other: &Decimal) -> (Decimal, Option<DecimalCodecWarning>) {
        self.bound_add_sub_result(self.add(other))
    }

    /// Source `DecimalSub`, including MyDecimal's nine-word result bound.
    pub fn sub_mysql(&self, other: &Decimal) -> (Decimal, Option<DecimalCodecWarning>) {
        self.bound_add_sub_result(self.add(&other.negate()))
    }

    fn bound_add_sub_result(&self, result: Decimal) -> (Decimal, Option<DecimalCodecWarning>) {
        let split = result.digits.len() - result.storage_scale as usize;
        let integer_digits = result.digits[..split].trim_start_matches('0').len();
        let words_int = digits_to_words(integer_digits);
        if words_int > CODEC_WORD_BUF_LEN {
            // `doAdd` calls `maxDecimal` before assigning the result sign.
            return (
                Decimal::max_or_min(false, (CODEC_WORD_BUF_LEN * DIGITS_PER_WORD) as u32, 0),
                Some(DecimalCodecWarning::Overflow),
            );
        }
        let words_frac = digits_to_words(result.storage_scale as usize);
        if words_int + words_frac <= CODEC_WORD_BUF_LEN {
            return (result, None);
        }
        let kept_scale = ((CODEC_WORD_BUF_LEN - words_int) * DIGITS_PER_WORD) as i32;
        (
            result.truncate_to_scale(kept_scale),
            Some(DecimalCodecWarning::Truncated),
        )
    }

    /// Exact decimal multiplication: result scale is `scale1 + scale2`
    /// (multiplying two exact fixed-point values never loses precision, so
    /// this needs no rounding — unlike division).
    pub fn mul(&self, other: &Decimal) -> Decimal {
        let scale = self.scale + other.scale;
        Decimal::new_with_storage(
            self.negative != other.negative,
            digit_mul(&self.digits, &other.digits),
            scale,
            self.storage_scale + other.storage_scale,
        )
    }

    /// Ports `DecimalMul`'s bounded nine-word arithmetic and disposition.
    ///
    /// The returned warning is the source `ErrTruncated`/`ErrOverflow`
    /// outcome. [`Self::mul`] remains the exact digit-string primitive used
    /// below the MySQL storage boundary; SQL behavior must use this method.
    pub fn mul_mysql(&self, other: &Decimal) -> (Decimal, Option<DecimalCodecWarning>) {
        // TPC-H DECIMAL(15,2) arithmetic is far below MySQL's nine-word
        // boundary. Avoid converting both operands through decimal strings
        // and base-1e9 words in this common exact case; wider values retain
        // the complete source implementation below.
        if let Some(product) = self.try_mul_mysql_fast(other) {
            return (product, None);
        }
        let left = MyDecimalWords::from_decimal(self);
        let right = MyDecimalWords::from_decimal(other);
        let words_int_left = digits_to_words(left.digits_int.max(0) as usize) as i32;
        let mut words_frac_left = digits_to_words(left.digits_frac.max(0) as usize) as i32;
        let mut words_int_right = digits_to_words(right.digits_int.max(0) as usize) as i32;
        let mut words_frac_right = digits_to_words(right.digits_frac.max(0) as usize) as i32;
        let requested_words_int =
            digits_to_words((left.digits_int + right.digits_int).max(0) as usize) as i32;
        let requested_words_frac = words_frac_left + words_frac_right;
        let (words_int, words_frac, warning) =
            fix_word_cnt_error(requested_words_int as usize, requested_words_frac as usize);
        let words_int = words_int as i32;
        let words_frac = words_frac as i32;
        let result_scale = (self.scale + other.scale).min(CODEC_MAX_DECIMAL_SCALE as u32);

        if warning == Some(DecimalCodecWarning::Overflow) {
            return (Decimal::new(false, "0".to_owned(), 0), warning);
        }

        let mut tmp_int = requested_words_int;
        let mut tmp_frac = requested_words_frac;
        if warning.is_some() {
            if tmp_int > words_int {
                tmp_int -= words_int;
                tmp_frac = tmp_int >> 1;
                words_int_right -= tmp_int - tmp_frac;
                words_frac_left = 0;
                words_frac_right = 0;
            } else {
                tmp_frac -= words_frac;
                tmp_int = tmp_frac >> 1;
                if words_frac_left <= words_frac_right {
                    words_frac_left -= tmp_int;
                    words_frac_right -= tmp_frac - tmp_int;
                } else {
                    words_frac_right -= tmp_int;
                    words_frac_left -= tmp_frac - tmp_int;
                }
            }
        }

        let mut product = MyDecimalWords {
            negative: left.negative != right.negative,
            digits_int: words_int * DIGITS_PER_WORD as i32,
            digits_frac: (left.digits_frac + right.digits_frac)
                .min(words_frac * DIGITS_PER_WORD as i32),
            word_buf: [0; CODEC_WORD_BUF_LEN],
        };

        let mut start_to = words_int + words_frac - 1;
        let start_right = words_int_right + words_frac_right - 1;
        let mut index_left = words_int_left + words_frac_left - 1;
        while index_left >= 0 {
            let mut carry = 0;
            let mut index_to = start_to;
            let mut index_right = start_right;
            while index_right >= 0 {
                let value = i64::from(left.word_buf[index_left as usize])
                    * i64::from(right.word_buf[index_right as usize]);
                let base = i64::from(CODEC_POWERS10[DIGITS_PER_WORD]);
                let high = (value / base) as i32;
                let low = (value - i64::from(high) * base) as i32;
                (product.word_buf[index_to as usize], carry) =
                    add_two_decimal_words(product.word_buf[index_to as usize], low, carry);
                carry += high;
                index_right -= 1;
                index_to -= 1;
            }
            if carry > 0 {
                if index_to < 0 {
                    return (
                        Decimal::new(false, "0".to_owned(), 0),
                        Some(DecimalCodecWarning::Overflow),
                    );
                }
                (product.word_buf[index_to as usize], carry) =
                    add_decimal_words(product.word_buf[index_to as usize], 0, carry);
            }
            index_to -= 1;
            while carry > 0 {
                if index_to < 0 {
                    return (
                        Decimal::new(false, "0".to_owned(), 0),
                        Some(DecimalCodecWarning::Overflow),
                    );
                }
                (product.word_buf[index_to as usize], carry) =
                    add_decimal_words(product.word_buf[index_to as usize], 0, carry);
                index_to -= 1;
            }
            start_to -= 1;
            index_left -= 1;
        }

        if product.word_buf[..(words_int + words_frac) as usize]
            .iter()
            .all(|word| *word == 0)
        {
            return (Decimal::new(false, "0".to_owned(), result_scale), warning);
        }

        let value = product.to_decimal();
        let storage_scale = value.storage_scale.max(result_scale);
        (
            value.round_or_truncate_to_scale_with_storage(result_scale as i32, true, storage_scale),
            warning,
        )
    }

    /// Exact bounded multiply for values whose unscaled digits fit in `i128`.
    /// Returning `None` deliberately keeps all overflow, scale, and nine-word
    /// warning behavior in [`Self::mul_mysql`]'s complete implementation.
    fn try_mul_mysql_fast(&self, other: &Decimal) -> Option<Decimal> {
        let result_scale = self.scale.checked_add(other.scale)?;
        if result_scale > CODEC_MAX_DECIMAL_SCALE as u32 {
            return None;
        }
        let left = self.digits.parse::<i128>().ok()?;
        let right = other.digits.parse::<i128>().ok()?;
        let product = left.checked_mul(right)?;
        let negative = (product < 0) || (self.negative != other.negative && product != 0);
        // Keep the fixed-scale fast path allocation-free for the coefficient
        // itself. `to_string` rebuilt a heap `String` for every DECIMAL
        // multiply even though the value already fits in `i128`; the add
        // path uses the same inline digit representation.
        let digits = DecimalDigits::from_unsigned(product.unsigned_abs());
        let storage_scale = self.storage_scale.checked_add(other.storage_scale)?;
        Some(Decimal::new_with_storage(
            negative,
            digits,
            result_scale,
            storage_scale,
        ))
    }

    /// Source `MyDecimal.Shift`: multiply by `10^shift` inside MyDecimal's
    /// fixed nine-word buffer. Integer overflow leaves the value untouched;
    /// excess fractional words are rounded half-up and reported as truncated.
    pub fn shift_mysql(&self, shift: i32) -> (Decimal, Option<DecimalCodecWarning>) {
        self.shift_mysql_with_word_limit(shift, CODEC_WORD_BUF_LEN)
    }

    /// The source tests temporarily reduce Go's package-global `wordBufLen`.
    /// An explicit limit gives the same coverage without mutable global state.
    pub(crate) fn shift_mysql_with_word_limit(
        &self,
        shift: i32,
        word_limit: usize,
    ) -> (Decimal, Option<DecimalCodecWarning>) {
        if shift == 0 {
            return (self.clone(), None);
        }
        if self.is_zero() {
            return (Decimal::from_int(0), None);
        }

        let mut digits = self.digits.clone();
        let mut scale = i64::from(self.storage_scale) - i64::from(shift);
        if scale < 0 {
            digits.push_str(&"0".repeat((-scale) as usize));
            scale = 0;
        }

        // Shift computes new bounds from the first and last non-zero digit.
        while scale > 0 && digits.ends_with('0') {
            digits.pop();
            scale -= 1;
        }
        while digits.len() < scale as usize {
            digits.insert(0, '0');
        }
        let exact = Decimal::new(self.negative, digits, scale as u32);
        let split = exact.digits.len() - exact.storage_scale as usize;
        let integer_digits = exact.digits[..split].trim_start_matches('0').len();
        let words_int = digits_to_words(integer_digits);
        if words_int > word_limit {
            return (self.clone(), Some(DecimalCodecWarning::Overflow));
        }

        let words_frac = digits_to_words(exact.storage_scale as usize);
        if words_int + words_frac <= word_limit {
            return (exact, None);
        }

        let kept_scale = ((word_limit - words_int) * DIGITS_PER_WORD) as i32;
        let rounded = exact.round_to_scale(kept_scale);
        if rounded.is_zero() {
            return (Decimal::from_int(0), Some(DecimalCodecWarning::Truncated));
        }
        let rounded_split = rounded.digits.len() - rounded.storage_scale as usize;
        let rounded_integer_digits = rounded.digits[..rounded_split]
            .trim_start_matches('0')
            .len();
        if digits_to_words(rounded_integer_digits) > word_limit {
            return (self.clone(), Some(DecimalCodecWarning::Overflow));
        }
        (rounded, Some(DecimalCodecWarning::Truncated))
    }

    /// Truncating division (`DIV`) and its remainder (`MOD`): pads both
    /// operands to a common scale first — their digit strings, read as plain
    /// integers, then have the exact same ratio as the decimal values (the
    /// scaling cancels) — then does unsigned long division on those
    /// integers. The quotient is `trunc(a/b)`; the remainder, reinterpreted
    /// at that same common scale, is exactly `a - trunc(a/b)*b` — precisely
    /// the scale MySQL's decimal `MOD` uses (`max(scale_a, scale_b)`, sign of
    /// the dividend). `None` for division by zero (MySQL: `NULL`) or a
    /// quotient too large for `i64`.
    pub fn div_rem(&self, other: &Decimal) -> Option<(i64, Decimal)> {
        if other.is_zero() {
            return None;
        }
        let storage_scale = self.storage_scale.max(other.storage_scale);
        let scale = self.scale.max(other.scale);
        let a = pad_scale(&self.digits, self.storage_scale, storage_scale);
        let b = pad_scale(&other.digits, other.storage_scale, storage_scale);
        let (q_digits, r_digits) = digit_divmod(&a, &b);
        let q_mag: i64 = q_digits.parse().ok()?;
        let quotient = if self.negative != other.negative {
            -q_mag
        } else {
            q_mag
        };
        let remainder = Decimal::new_with_storage(self.negative, r_digits, scale, storage_scale);
        Some((quotient, remainder))
    }

    /// Source `DecimalMod`, without routing the discarded quotient through
    /// `i64` (the source accepts quotients wider than BIGINT).
    pub fn rem_mysql(&self, other: &Decimal) -> Option<Decimal> {
        if other.is_zero() {
            return None;
        }
        let storage_scale = self.storage_scale.max(other.storage_scale);
        let scale = self.scale.max(other.scale);
        let a = pad_scale(&self.digits, self.storage_scale, storage_scale);
        let b = pad_scale(&other.digits, other.storage_scale, storage_scale);
        let (_, remainder) = digit_divmod(&a, &b);
        Some(Decimal::new_with_storage(
            self.negative,
            remainder,
            scale,
            storage_scale,
        ))
    }

    /// Source `DecimalDiv`: retain the whole base-1e9 fraction words produced
    /// by the division while exposing `div_precision_increment` through
    /// `resultFrac`.
    pub fn div_mysql(&self, other: &Decimal, frac_increment: u32) -> Option<Decimal> {
        if other.is_zero() {
            return None;
        }
        let result_scale = (self.scale + frac_increment).min(CODEC_MAX_DECIMAL_SCALE as u32);
        if self.is_zero() {
            return Some(Decimal::new(false, "0".to_owned(), result_scale));
        }
        let frac1 = word_scale(self.storage_scale);
        let frac2 = word_scale(other.storage_scale);
        let padding = (frac1 - self.storage_scale) + (frac2 - other.storage_scale);
        let adjusted_increment = frac_increment.saturating_sub(padding);
        let storage_scale = word_scale(frac1 + frac2 + adjusted_increment);

        // AVG over integer/decimal columns is the hottest DECIMAL division
        // path in TPC-H q17. Go's MyDecimal implementation works on the
        // already-packed integer words; the general Rust compatibility path
        // below first pads and divides decimal strings. For the common
        // scale-zero case, perform the same truncating long division with
        // u128 and materialize only the result coefficient. The bounds checks
        // keep arbitrary-precision and overflow behavior on the complete path.
        let common_scale = self.storage_scale.max(other.storage_scale);
        if other.storage_scale == 0
            && self.scale <= self.storage_scale
            && other.scale <= other.storage_scale
        {
            if let (Ok(dividend), Ok(divisor)) =
                (self.digits.parse::<u128>(), other.digits.parse::<u128>())
            {
                let numerator_exponent = common_scale
                    .checked_add(storage_scale)
                    .and_then(|scale| scale.checked_sub(self.storage_scale));
                let divisor_exponent = common_scale.checked_sub(other.storage_scale);
                if let (Some(numerator_exponent), Some(divisor_exponent)) =
                    (numerator_exponent, divisor_exponent)
                {
                    let numerator_factor = 10u128.checked_pow(numerator_exponent);
                    let divisor_factor = 10u128.checked_pow(divisor_exponent);
                    if let (Some(numerator_factor), Some(divisor_factor)) =
                        (numerator_factor, divisor_factor)
                    {
                        if let (Some(numerator), Some(divisor)) = (
                            dividend.checked_mul(numerator_factor),
                            divisor.checked_mul(divisor_factor),
                        ) {
                            let quotient = numerator / divisor;
                            return Some(Decimal::new_with_storage(
                                self.negative != other.negative,
                                quotient.to_string(),
                                result_scale,
                                storage_scale,
                            ));
                        }
                    }
                }
            }
        }

        let numerator = pad_scale(
            &pad_scale(&self.digits, self.storage_scale, common_scale),
            common_scale,
            common_scale + storage_scale,
        );
        let divisor = pad_scale(&other.digits, other.storage_scale, common_scale);
        let (quotient, _) = digit_divmod(&numerator, &divisor);
        Some(Decimal::new_with_storage(
            self.negative != other.negative,
            quotient,
            result_scale,
            storage_scale,
        ))
    }

    /// MyDecimal `ToString`, which exposes stored fraction words without the
    /// `resultFrac` presentation rounding used by `String`.
    pub fn storage_string(&self) -> String {
        Decimal::new_with_storage(
            self.negative,
            self.digits.clone(),
            self.storage_scale,
            self.storage_scale,
        )
        .to_string()
    }

    /// True (rounding) division by a positive integer divisor, to
    /// `target_scale` fractional digits — `AVG`'s `SUM / COUNT`, where MySQL
    /// grows the result scale by the caller's `div_precision_increment`
    /// rather than dividing
    /// exactly. Computes one extra digit of precision via the same unsigned
    /// long division `div_rem` uses, then rounds that spare digit away
    /// (ties away from zero, matching every other decimal-to-integer rounding
    /// rule this crate implements) — unlike `div_rem`, which truncates.
    /// `target_scale` must be `>= self.scale` (always true for `AVG`, which
    /// only grows scale).
    pub fn div_round(&self, divisor: i64, target_scale: u32) -> Decimal {
        let increment = target_scale - self.scale;
        let storage_scale = word_scale(self.storage_scale + increment);
        let numerator = pad_scale(&self.digits, self.storage_scale, storage_scale);
        let (quotient, _) = digit_divmod(&numerator, &divisor.to_string());
        Decimal::new_with_storage(self.negative, quotient, target_scale, storage_scale)
    }

    /// True (rounding) division by an arbitrary `Decimal` divisor — MySQL's
    /// `/` operator, which (confirmed via `goeval`, not assumed) grows the
    /// result scale by the SAME fixed increment `AVG`'s `div_round` uses
    /// (4), applied past the DIVIDEND's own scale only — the divisor's own
    /// scale never affects the result scale (`5 / 2.5` and `5 / 2` both
    /// land at the dividend's `0 + 4 = 4` fractional digits). `None` for
    /// division by zero (MySQL: `NULL`). Aligns both operands to a common
    /// scale first (the same trick [`Decimal::div_rem`] uses — their digit
    /// strings, read as plain integers, then have the exact same ratio as
    /// the decimal values, since the shared scale cancels), then applies
    /// the identical "one extra digit, then round it away" technique
    /// `div_round` uses, generalized to a `Decimal` (not just an `i64`)
    /// divisor. Sign follows the standard XOR rule, same as every other
    /// decimal operator.
    pub fn true_div(&self, other: &Decimal, target_scale: u32) -> Option<Decimal> {
        self.div_mysql(other, target_scale.saturating_sub(self.scale))
    }

    /// Rounds to the nearest integer, ties away from zero — MySQL's
    /// decimal-to-integer conversion rule for bitwise/shift operators (which
    /// operate on integers, not decimals). `None` on overflow past `i64`.
    pub fn round_to_i64(&self) -> Option<i64> {
        if self.storage_scale == 0 {
            let mag: i64 = self.digits.parse().ok()?;
            return Some(if self.negative { -mag } else { mag });
        }
        let split = self.digits.len() - self.storage_scale as usize;
        let int_part = if split == 0 {
            "0"
        } else {
            &self.digits[..split]
        };
        let round_up = self.digits.as_bytes()[split] >= b'5';
        let mag: i64 = int_part.parse().ok()?;
        let mag = if round_up { mag.checked_add(1)? } else { mag };
        Some(if self.negative { -mag } else { mag })
    }

    /// Source `MyDecimal.ToInt`: truncates toward zero and reports a non-zero
    /// discarded fraction separately from overflow.
    pub fn to_i64_trunc(&self) -> (i64, Option<DecimalIntegerWarning>) {
        let split = self.digits.len() - self.storage_scale as usize;
        let integer = self.digits[..split].trim_start_matches('0');
        let integer = if integer.is_empty() { "0" } else { integer };
        let magnitude = integer.parse::<u64>();
        let value = match (self.negative, magnitude) {
            (false, Ok(value)) if value <= i64::MAX as u64 => value as i64,
            (true, Ok(value)) if value <= i64::MIN.unsigned_abs() => {
                if value == i64::MIN.unsigned_abs() {
                    i64::MIN
                } else {
                    -(value as i64)
                }
            }
            (false, _) => return (i64::MAX, Some(DecimalIntegerWarning::Overflow)),
            (true, _) => return (i64::MIN, Some(DecimalIntegerWarning::Overflow)),
        };
        let truncated = self.digits[split..].bytes().any(|digit| digit != b'0');
        (value, truncated.then_some(DecimalIntegerWarning::Truncated))
    }

    /// Source `MyDecimal.ToUint`: truncates toward zero, rejects negatives,
    /// and saturates positive overflow.
    pub fn to_u64_trunc(&self) -> (u64, Option<DecimalIntegerWarning>) {
        if self.negative {
            return (0, Some(DecimalIntegerWarning::Overflow));
        }
        let split = self.digits.len() - self.storage_scale as usize;
        let integer = self.digits[..split].trim_start_matches('0');
        let integer = if integer.is_empty() { "0" } else { integer };
        let Ok(value) = integer.parse::<u64>() else {
            return (u64::MAX, Some(DecimalIntegerWarning::Overflow));
        };
        let truncated = self.digits[split..].bytes().any(|digit| digit != b'0');
        (value, truncated.then_some(DecimalIntegerWarning::Truncated))
    }

    /// Like [`Decimal::round_to_i64`], but CLAMPS to `i64::MIN`/`MAX`
    /// instead of failing on overflow — `CAST(... AS SIGNED)`'s own rule
    /// (confirmed via `goeval`: `CAST(1e300 AS SIGNED)` is
    /// `9223372036854775807`, a genuine saturating clamp, not the hard
    /// `ErrOverflow` `~x`'s own bitwise conversion raises — MySQL's
    /// "truncate as warning" `SQL_MODE` downgrades the overflow to a
    /// clamp-and-warn for an explicit `CAST`, unlike an implicit bitwise
    /// coercion).
    pub fn round_to_i64_saturating(&self) -> i64 {
        self.round_to_i64()
            .unwrap_or(if self.negative { i64::MIN } else { i64::MAX })
    }

    /// `CAST(... AS UNSIGNED)`'s decimal rule: round half away from zero to an
    /// integer (Go `ModeHalfUp`, matching [`Decimal::round_to_i64`]), then Go
    /// `MyDecimal.ToUint`. A negative value is `ToUint`'s `ErrOverflow`, which
    /// the cast reports as `0`; a magnitude past `u64::MAX` saturates to
    /// `u64::MAX` (Go `ToUint` returns `MaxUint64` on positive overflow). Unlike
    /// routing through the `i64` path, this preserves values in
    /// `(i64::MAX, u64::MAX]` — the upper half of an `UNSIGNED BIGINT`.
    #[must_use]
    pub fn round_to_u64_saturating(&self) -> u64 {
        match self.rounded_magnitude_u64() {
            // A negative magnitude that does not round to zero is ToUint's
            // ErrOverflow, which the cast turns into 0; zero and positive
            // magnitudes pass through unchanged.
            Some(magnitude) => {
                if self.negative && magnitude != 0 {
                    0
                } else {
                    magnitude
                }
            }
            // Magnitude beyond u64::MAX: positive saturates to MaxUint64, a
            // negative overflow is still ErrOverflow -> 0.
            None => {
                if self.negative {
                    0
                } else {
                    u64::MAX
                }
            }
        }
    }

    /// The half-up rounded integer magnitude ignoring sign, `None` when it
    /// exceeds `u64::MAX`. The magnitude step of [`Decimal::round_to_i64`]
    /// parsed into `u64` so an `UNSIGNED` cast keeps the full range.
    fn rounded_magnitude_u64(&self) -> Option<u64> {
        if self.storage_scale == 0 {
            return self.digits.parse::<u64>().ok();
        }
        let split = self.digits.len() - self.storage_scale as usize;
        let int_part = if split == 0 {
            "0"
        } else {
            &self.digits[..split]
        };
        let round_up = self.digits.as_bytes()[split] >= b'5';
        let magnitude: u64 = int_part.parse().ok()?;
        if round_up {
            magnitude.checked_add(1)
        } else {
            Some(magnitude)
        }
    }

    /// `CAST`/`CONVERT`'s own `DECIMAL(flen, scale)` target: rounds to
    /// `scale` fractional digits (ties away from zero, same as
    /// [`Decimal::round_to_scale`]), then clamps the MAGNITUDE to the
    /// largest value representable in `flen` total digits — confirmed via
    /// `goeval`: `CAST(123456 AS DECIMAL(5,2))` is `999.99`, not an error
    /// or a silently-oversized result. `flen == 0` means unspecified (no
    /// magnitude clamp at all — see `tidb_ast::CastType::Decimal`'s own
    /// doc for why); `flen <= scale` (a malformed target nobody writes
    /// intentionally — real MySQL itself errors constructing it) is
    /// treated as "zero digits of integer part allowed", clamping any
    /// nonzero magnitude straight to the all-`9`s value rather than
    /// underflowing `flen - scale` — a deliberately narrow, not fully
    /// MySQL-faithful, fallback for a degenerate case, not a realistic
    /// query.
    pub fn cast_to_precision(&self, flen: u32, scale: u32) -> Decimal {
        let rounded = self.round_to_scale(scale as i32);
        if flen == 0 {
            return rounded;
        }
        let int_digits = rounded.digits.len() as u32 - rounded.scale;
        let max_int_digits = flen.saturating_sub(scale);
        if int_digits > max_int_digits {
            return Decimal::new(rounded.negative, "9".repeat(flen as usize), scale);
        }
        rounded
    }

    /// Converts to the nearest `f64` — MySQL's implicit `DECIMAL`-to-
    /// `FLOAT`/`DOUBLE` promotion rule when a `Decimal` operand meets a
    /// `Float` one. Lossy for precision beyond `f64`'s ~15-17 significant
    /// digits, same as MySQL's own conversion; parses this value's own
    /// canonical `Display` text, which is always valid decimal syntax.
    pub fn to_f64(&self) -> f64 {
        self.to_string()
            .parse()
            .expect("Decimal's own Display always produces valid float syntax")
    }

    /// The EXACT mathematical ceiling (`ceiling: true`) or floor
    /// (`false`), as a new `Decimal` at scale 0 — computed on the digit
    /// string directly (not via `f64`), so it's exact for arbitrary
    /// precision (unlike `round_to_i64`, this never loses precision to
    /// `i64`'s own range — `CEIL`/`FLOOR`'s own `i64`-fitting check, if
    /// any, is the CALLER's job, matching real MySQL: `CEIL`/`FLOOR`
    /// return `BIGINT` when the exact result fits, else `DECIMAL`,
    /// confirmed via `goeval`, not assumed). `CEIL` rounds toward
    /// positive infinity, `FLOOR` toward negative infinity (confirmed via
    /// `goeval`: `CEIL(-3.14)` is `-3`, `FLOOR(-3.14)` is `-4` — the
    /// magnitude rounds up in the OPPOSITE direction from the value's own
    /// sign, i.e. `CEIL` truncates a negative value's magnitude while
    /// `FLOOR` rounds it up, and vice versa for a positive value).
    pub fn ceil_floor(&self, ceiling: bool) -> Decimal {
        if self.storage_scale == 0 {
            return Decimal::new(self.negative, self.digits.clone(), 0);
        }
        let split = self.digits.len() - self.storage_scale as usize;
        let int_part = if split == 0 {
            "0"
        } else {
            &self.digits[..split]
        };
        let has_fraction = self.digits[split..].bytes().any(|b| b != b'0');
        if !has_fraction {
            return Decimal::new(self.negative, int_part.to_string(), 0);
        }
        let round_up_magnitude = ceiling != self.negative;
        let digits = if round_up_magnitude {
            digit_add(int_part, "1")
        } else {
            int_part.to_string()
        };
        Decimal::new(self.negative, digits, 0)
    }

    /// Rounds to `target_scale` fractional digits, ties away from zero
    /// (`ModeHalfUp` — MySQL's default rounding mode) — the general form of
    /// [`Decimal::round_to_i64`] (always scale 0) and [`Decimal::ceil_floor`]
    /// (always scale 0, never a caller-chosen target), used by
    /// `ROUND(decimal, frac)`. `target_scale` may be negative (rounding into
    /// the integer part, e.g. `ROUND(12345, -2)` is `12300`) or exceed
    /// `self.scale` (grows the fractional part with exact zero digits, no
    /// rounding). The caller clamps `target_scale` to MySQL's `DECIMAL` max
    /// scale (30) before calling, matching real MySQL (confirmed by reading
    /// `calculateDecimal4RoundAndTruncate` in `builtin_math.go`, not
    /// assumed): `ROUND(3.14159, 100)` does not grow to 100 fractional
    /// digits.
    pub fn round_to_scale(&self, target_scale: i32) -> Decimal {
        self.round_or_truncate_to_scale(target_scale, true)
    }

    /// Ports `MyDecimal.Round(..., ModeCeiling)` exactly.
    ///
    /// Despite its name, the source mode is not mathematical ceiling: its
    /// current behavior rounds a non-zero discarded magnitude away from zero
    /// for both signs. This is distinct from [`Self::ceil_floor`], which owns
    /// SQL `CEIL`/`FLOOR` semantics.
    pub fn round_ceiling_to_scale(&self, target_scale: i32) -> Decimal {
        let result_scale = target_scale.max(0) as u32;
        let shift = self.storage_scale as i32 - target_scale;
        if shift <= 0 {
            let digits = pad_scale(&self.digits, self.storage_scale, result_scale);
            return Decimal::new(self.negative, digits, result_scale);
        }

        let shift = shift as usize;
        let mut digits = self.digits.clone();
        if digits.len() <= shift {
            digits = format!("{}{digits}", "0".repeat(shift + 1 - digits.len())).into();
        }
        let split = digits.len() - shift;
        let kept = &digits[..split];
        let discarded_nonzero = digits[split..].bytes().any(|digit| digit != b'0');
        let mut kept = if discarded_nonzero {
            digit_add(kept, "1")
        } else {
            kept.to_owned()
        };
        if target_scale < 0 {
            kept.push_str(&"0".repeat((-target_scale) as usize));
        }
        Decimal::new(self.negative, kept, result_scale)
    }

    /// Truncates (never rounds) to `target_scale` fractional digits
    /// (`ModeTruncate`), used by `TRUNCATE(decimal, frac)`. Same shape as
    /// [`Decimal::round_to_scale`] but the digit immediately past the cut is
    /// always dropped rather than inspected.
    pub fn truncate_to_scale(&self, target_scale: i32) -> Decimal {
        self.round_or_truncate_to_scale(target_scale, false)
    }

    /// Fits this value into a `DECIMAL(precision, scale)` column: rounds to
    /// `scale` fractional digits, then checks the rounded value has at most
    /// `precision - scale` significant integer digits. Returns the rounded
    /// value if it fits, `None` if the integer part overflows. Real
    /// MySQL/TiDB rounds FIRST, so a value that only overflows AFTER
    /// rounding is rejected — `99.995` rounds to `100.00`, which overflows
    /// `DECIMAL(4,2)` (confirmed via `gorun`), while `99.994` rounds to
    /// `99.99` and fits. A value below 1 has zero significant integer
    /// digits (`0.50` fits `DECIMAL(4,2)` — the placeholder leading `0`
    /// doesn't count). Used by `tidb_exec`'s column-width validation on
    /// `INSERT`/`UPDATE`.
    pub fn fit_precision_scale(&self, precision: u32, scale: u32) -> Option<Decimal> {
        let int_budget = precision.checked_sub(scale)?;
        let rounded = self.round_to_scale(scale as i32);
        let int_len = rounded.digits.len() - rounded.scale as usize;
        let significant_int = rounded.digits[..int_len].trim_start_matches('0').len();
        (significant_int as u32 <= int_budget).then_some(rounded)
    }

    /// Shared digit-string implementation for [`Decimal::round_to_scale`] and
    /// [`Decimal::truncate_to_scale`]: `target_scale >= self.scale` just
    /// grows the fractional part with exact zeros (no digit is cut either
    /// way). Otherwise the digits past the cut are dropped; `round` decides
    /// whether the first dropped digit (`>= 5`) bumps the kept part by one.
    /// A `target_scale` deep enough to cut every digit (including the
    /// integer part) naturally falls out to `0` through the same digit math
    /// MySQL special-cases explicitly (`int(d.digitsInt)+frac < 0` in
    /// `MyDecimal.Round`) — no separate branch needed here.
    fn round_or_truncate_to_scale(&self, target_scale: i32, round: bool) -> Decimal {
        let result_scale = target_scale.max(0) as u32;
        self.round_or_truncate_to_scale_with_storage(target_scale, round, result_scale)
    }

    fn round_or_truncate_to_scale_with_storage(
        &self,
        target_scale: i32,
        round: bool,
        storage_scale: u32,
    ) -> Decimal {
        let result_scale = target_scale.max(0) as u32;
        let storage_scale = storage_scale.max(result_scale);
        let shift = self.storage_scale as i32 - target_scale;
        if shift <= 0 {
            let digits = pad_scale(&self.digits, self.storage_scale, storage_scale);
            return Decimal::new_with_storage(self.negative, digits, result_scale, storage_scale);
        }
        let shift = shift as usize;
        let mut digits = self.digits.clone();
        if digits.len() <= shift {
            digits = format!("{}{digits}", "0".repeat(shift + 1 - digits.len())).into();
        }
        let split = digits.len() - shift;
        let int_part = &digits[..split];
        let round_up = round && digits.as_bytes()[split] >= b'5';
        let mut kept = if round_up {
            digit_add(int_part, "1")
        } else {
            int_part.to_string()
        };
        if target_scale < 0 {
            kept.push_str(&"0".repeat((-target_scale) as usize));
        }
        let digits = pad_scale(&kept, result_scale, storage_scale);
        Decimal::new_with_storage(self.negative, digits, result_scale, storage_scale)
    }
}

impl std::fmt::Display for Decimal {
    /// The canonical string form (MyDecimal's `String()`): the sign (omitted
    /// for zero), then the digits with the decimal point inserted `scale`
    /// places from the right — omitted entirely when `scale == 0`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // MyDecimal retains full base-1e9 storage words, then rounds to its
        // declared `resultFrac` when it becomes a SQL value. Keep that
        // presentation rounding out of the stored payload: an enclosing AVG
        // must still consume the hidden digits.
        if self.storage_scale > self.scale {
            return write!(
                f,
                "{}",
                self.round_or_truncate_to_scale(self.scale as i32, true)
            );
        }
        let sign = if self.negative { "-" } else { "" };
        if self.scale == 0 {
            let split = self.digits.len() - self.storage_scale as usize;
            let int_part = if split == 0 {
                "0"
            } else {
                &self.digits[..split]
            };
            return write!(f, "{sign}{int_part}");
        }
        let split = self.digits.len() - self.storage_scale as usize;
        let int_part = &self.digits[..split];
        let int_part = if int_part.is_empty() { "0" } else { int_part };
        let frac_end = split + self.scale as usize;
        write!(f, "{sign}{int_part}.{}", &self.digits[split..frac_end])
    }
}

impl PartialEq for Decimal {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Decimal {}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Decimal {
    /// Value-based order, ignoring scale (`1.5` and `1.50` are equal): a
    /// sign mismatch decides it outright (zero is always non-negative, so a
    /// mismatch means one side is genuinely nonzero); otherwise the aligned
    /// magnitudes decide, reversed when both are negative.
    ///
    /// Allocation-free: sorts, hash-join probes and range filters compare
    /// decimals once per row, where the previous pad-both-sides-into-`String`
    /// form cost four heap allocations Go's word-wise `MyDecimal.Compare`
    /// does not have.
    fn cmp(&self, other: &Self) -> Ordering {
        if self.negative != other.negative {
            return if self.negative {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }
        let mag_cmp = cmp_magnitude(
            self.digits.as_str(),
            self.storage_scale,
            other.digits.as_str(),
            other.storage_scale,
        );
        if self.negative {
            mag_cmp.reverse()
        } else {
            mag_cmp
        }
    }
}

/// Compares two unsigned coefficients placed at the decimal point:
/// `digits * 10^-storage_scale` each, digit-by-digit with missing trailing
/// fraction digits read as `0`. Equivalent to right-padding both sides to
/// the common storage scale and comparing the digit strings.
fn cmp_magnitude(
    a_digits: &str,
    a_storage_scale: u32,
    b_digits: &str,
    b_storage_scale: u32,
) -> Ordering {
    let a_int_end = a_digits.len() - (a_storage_scale as usize).min(a_digits.len());
    let b_int_end = b_digits.len() - (b_storage_scale as usize).min(b_digits.len());
    let (a_int, a_frac) = (&a_digits[..a_int_end], &a_digits[a_int_end..]);
    let (b_int, b_frac) = (&b_digits[..b_int_end], &b_digits[b_int_end..]);
    // Leading zeros carry no magnitude; strip them so length decides first.
    let a_int_tz = a_int.trim_start_matches('0');
    let b_int_tz = b_int.trim_start_matches('0');
    match a_int_tz
        .len()
        .cmp(&b_int_tz.len())
        .then_with(|| a_int_tz.cmp(b_int_tz))
    {
        Ordering::Equal => {}
        non_eq => return non_eq,
    }
    // Equal-length ASCII digit strings compare numerically byte-wise.
    let common = a_frac.len().min(b_frac.len());
    match a_frac[..common].cmp(&b_frac[..common]) {
        Ordering::Equal => {}
        non_eq => return non_eq,
    }
    // A longer fraction only wins when its extra digits are not all zero.
    let (rest, sign) = if a_frac.len() > b_frac.len() {
        (&a_frac[common..], Ordering::Greater)
    } else {
        (&b_frac[common..], Ordering::Less)
    };
    if rest.bytes().any(|digit| digit != b'0') {
        sign
    } else {
        Ordering::Equal
    }
}

/// Right-pads an unsigned digit string with trailing zero digits to extend it
/// from `scale` to `target` fractional digits — exact, since a trailing
/// fractional zero never changes the value.
fn pad_scale(digits: &str, scale: u32, target: u32) -> String {
    let mut s = digits.to_string();
    s.push_str(&"0".repeat((target - scale) as usize));
    s
}

/// MyDecimal stores fractional digits in base-1e9 words. A division result's
/// hidden arithmetic precision therefore rounds up to a whole nine-digit word
/// even when its SQL-visible `resultFrac` is smaller.
fn word_scale(scale: u32) -> u32 {
    scale.div_ceil(9) * 9
}

/// Left-pads two unsigned digit strings with `0` to equal length, so they can
/// be compared or added digit-by-digit.
fn pad_equal(a: &str, b: &str) -> (String, String) {
    let len = a.len().max(b.len());
    (format!("{a:0>len$}"), format!("{b:0>len$}"))
}

/// Numerically compares two unsigned decimal digit strings of possibly
/// different lengths (equal-length numeral strings compare lexicographically
/// = numerically).
fn digit_cmp(a: &str, b: &str) -> Ordering {
    let (a, b) = pad_equal(a, b);
    a.cmp(&b)
}

/// Adds two unsigned decimal digit strings (schoolbook, with carry).
fn digit_add(a: &str, b: &str) -> String {
    let (a, b) = pad_equal(a, b);
    let mut out = Vec::with_capacity(a.len() + 1);
    let mut carry = 0u8;
    for (x, y) in a.bytes().rev().zip(b.bytes().rev()) {
        let sum = (x - b'0') + (y - b'0') + carry;
        out.push(b'0' + sum % 10);
        carry = sum / 10;
    }
    if carry > 0 {
        out.push(b'0' + carry);
    }
    out.reverse();
    String::from_utf8(out).expect("digits are ASCII")
}

/// Subtracts unsigned `b` from unsigned `a`, assuming `a >= b` (the caller
/// compares magnitudes first via `digit_cmp` and picks the operand order).
fn digit_sub(a: &str, b: &str) -> String {
    let (a, b) = pad_equal(a, b);
    let mut out = Vec::with_capacity(a.len());
    let mut borrow = 0i8;
    for (x, y) in a.bytes().rev().zip(b.bytes().rev()) {
        let mut diff = (x as i8 - b'0' as i8) - (y as i8 - b'0' as i8) - borrow;
        if diff < 0 {
            diff += 10;
            borrow = 1;
        } else {
            borrow = 0;
        }
        out.push(b'0' + diff as u8);
    }
    out.reverse();
    String::from_utf8(out).expect("digits are ASCII")
}

fn add_decimal_words(left: i32, right: i32, carry: i32) -> (i32, i32) {
    let base = CODEC_POWERS10[DIGITS_PER_WORD];
    let sum = left + right + carry;
    if sum >= base {
        (sum - base, 1)
    } else {
        (sum, 0)
    }
}

fn add_two_decimal_words(left: i32, right: i32, carry: i32) -> (i32, i32) {
    let base = i64::from(CODEC_POWERS10[DIGITS_PER_WORD]);
    let mut sum = i64::from(left) + i64::from(right) + i64::from(carry);
    let mut next_carry = 0;
    if sum >= base {
        next_carry = 1;
        sum -= base;
    }
    if sum >= base {
        next_carry += 1;
        sum -= base;
    }
    (sum as i32, next_carry)
}

/// Multiplies two unsigned decimal digit strings (schoolbook long
/// multiplication).
fn digit_mul(a: &str, b: &str) -> String {
    if a.bytes().all(|c| c == b'0') || b.bytes().all(|c| c == b'0') {
        return "0".to_string();
    }
    let a_digits: Vec<u32> = a.bytes().rev().map(|c| (c - b'0') as u32).collect();
    let b_digits: Vec<u32> = b.bytes().rev().map(|c| (c - b'0') as u32).collect();
    let mut result = vec![0u32; a_digits.len() + b_digits.len()];
    for (i, &da) in a_digits.iter().enumerate() {
        let mut carry = 0u32;
        for (j, &db) in b_digits.iter().enumerate() {
            let pos = i + j;
            let val = result[pos] + da * db + carry;
            result[pos] = val % 10;
            carry = val / 10;
        }
        let mut k = i + b_digits.len();
        while carry > 0 {
            let val = result[k] + carry;
            result[k] = val % 10;
            carry = val / 10;
            k += 1;
        }
    }
    let s: String = result
        .iter()
        .rev()
        .map(|d| (b'0' + *d as u8) as char)
        .collect();
    let trimmed = s.trim_start_matches('0');
    if trimmed.is_empty() {
        "0".to_string()
    } else {
        trimmed.to_string()
    }
}

/// Strips leading zero digits, collapsing an all-zero string to `"0"`.
fn strip_leading_zeros(s: &str) -> String {
    let trimmed = s.trim_start_matches('0');
    if trimmed.is_empty() {
        "0".to_string()
    } else {
        trimmed.to_string()
    }
}

fn parse_mysql_exponent(text: &str) -> (i64, Option<DecimalParseError>) {
    let text = text.trim();
    if text.is_empty() {
        return (0, Some(DecimalParseError::Truncated));
    }
    let bytes = text.as_bytes();
    let (negative, mut index) = match bytes[0] {
        b'-' => (true, 1),
        b'+' => (false, 1),
        _ => (false, 0),
    };
    let mut magnitude = 0_u64;
    let mut has_digit = false;
    while index < bytes.len() {
        let byte = bytes[index];
        if !byte.is_ascii_digit() {
            let bounded = magnitude.min(i64::MAX as u64) as i64;
            return (
                if negative { -bounded } else { bounded },
                Some(DecimalParseError::Truncated),
            );
        }
        has_digit = true;
        let Some(next) = magnitude
            .checked_mul(10)
            .and_then(|value| value.checked_add(u64::from(byte - b'0')))
        else {
            return (0, Some(DecimalParseError::BadNumber));
        };
        magnitude = next;
        index += 1;
    }
    if !has_digit {
        return (0, Some(DecimalParseError::Truncated));
    }
    let limit = i64::MAX as u64 + u64::from(negative);
    if magnitude > limit {
        return (
            if negative { i64::MIN } else { i64::MAX },
            Some(DecimalParseError::BadNumber),
        );
    }
    (
        if negative {
            (0_u64.wrapping_sub(magnitude)) as i64
        } else {
            magnitude as i64
        },
        None,
    )
}

/// Unsigned schoolbook long division: `a` divided by `b` (`b` assumed
/// nonzero), producing the truncated integer quotient and the remainder —
/// one digit of `a` at a time, finding each quotient digit (0-9) by repeated
/// subtraction (never more than 9 iterations per digit).
fn digit_divmod(a: &str, b: &str) -> (String, String) {
    let mut quotient = String::with_capacity(a.len());
    let mut rem = "0".to_string();
    for ch in a.bytes() {
        rem = strip_leading_zeros(&format!("{rem}{}", ch as char));
        let mut count = 0u8;
        while digit_cmp(&rem, b) != Ordering::Less {
            rem = strip_leading_zeros(&digit_sub(&rem, b));
            count += 1;
        }
        quotient.push((b'0' + count) as char);
    }
    (strip_leading_zeros(&quotient), rem)
}
mod codec;

use codec::{
    digits_to_words, fix_word_cnt_error, MyDecimalWords, CODEC_MAX_DECIMAL_SCALE, CODEC_POWERS10,
    CODEC_WORD_BUF_LEN, DIGITS_PER_WORD,
};

pub use codec::{decimal_bin_size, DecimalCodecError, DecimalCodecWarning};
