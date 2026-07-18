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
    digits: String,
    /// Fractional digits visible through `Display`/SQL result formatting.
    /// This is MyDecimal's `resultFrac` equivalent.
    scale: u32,
    /// Fractional digits retained for later decimal arithmetic. Division in
    /// TiDB stores whole base-1e9 words here even when `scale` exposes fewer
    /// digits, so an aggregate can consume precision a scalar result does
    /// not print. Normal literals and exact arithmetic keep this equal to
    /// `scale`.
    storage_scale: u32,
}

impl Decimal {
    /// The single normalization point for ordinary values, whose stored and
    /// displayed scale are identical.
    fn new(negative: bool, digits: String, scale: u32) -> Self {
        Self::new_with_storage(negative, digits, scale, scale)
    }

    /// Normalizes a value whose internal decimal payload may retain more
    /// fraction digits than its SQL-visible result scale. `storage_scale`
    /// must never be smaller than `scale`.
    fn new_with_storage(
        negative: bool,
        mut digits: String,
        scale: u32,
        storage_scale: u32,
    ) -> Self {
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
        }
    }

    /// Parses a decimal literal's original text. The expression AST supplies
    /// sign-free text; negation is represented separately by its caller.
    pub fn from_literal(text: &str) -> Self {
        let (int_part, frac_part) = text.split_once('.').unwrap_or((text, ""));
        let int_stripped = int_part.trim_start_matches('0');
        let int_norm = if int_stripped.is_empty() {
            "0"
        } else {
            int_stripped
        };
        let scale = frac_part.len() as u32;
        Decimal::new(false, format!("{int_norm}{frac_part}"), scale)
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

    /// Returns the scale of the lossless stored coefficient.
    ///
    /// This can exceed [`Self::scale`], which is the rounded SQL presentation
    /// scale. Storage and protocol codecs need this value to avoid discarding
    /// arithmetic precision.
    pub const fn storage_scale(&self) -> u32 {
        self.storage_scale
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
        if other.is_zero() {
            return None;
        }
        let common_scale = self.storage_scale.max(other.storage_scale);
        let a = pad_scale(&self.digits, self.storage_scale, common_scale);
        let b = pad_scale(&other.digits, other.storage_scale, common_scale);
        let increment = target_scale - self.scale;
        let storage_scale = word_scale(self.storage_scale + other.storage_scale + increment);
        let numerator = pad_scale(&a, 0, storage_scale);
        let (quotient, _) = digit_divmod(&numerator, &b);
        Some(Decimal::new_with_storage(
            self.negative != other.negative,
            quotient,
            target_scale,
            storage_scale,
        ))
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
            digits = format!("{}{digits}", "0".repeat(shift + 1 - digits.len()));
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
    fn cmp(&self, other: &Self) -> Ordering {
        if self.negative != other.negative {
            return if self.negative {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }
        let scale = self.storage_scale.max(other.storage_scale);
        let a = pad_scale(&self.digits, self.storage_scale, scale);
        let b = pad_scale(&other.digits, other.storage_scale, scale);
        let mag_cmp = digit_cmp(&a, &b);
        if self.negative {
            mag_cmp.reverse()
        } else {
            mag_cmp
        }
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
