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

//! `pkg/types/mydecimal.go` `MyDecimal`: the fixed-point decimal in Go's exact
//! in-memory layout.
//!
//! The layout is load-bearing: a chunk `NewDecimal` cell is the raw 40-byte
//! struct (`digitsInt`/`digitsFrac`/`resultFrac`/`negative` + 9 base-1e9
//! `int32` words), written through `unsafe.Pointer` in Go. `#[repr(C)]` with
//! the same field order reproduces it byte-for-byte on the same endianness,
//! verified by a size/round-trip test.
//!
//! Relationship to [`crate::Decimal`]: `Decimal` is the digit-string value
//! type the constant evaluator computes with; `MyDecimal` is the
//! storage/chunk-layout type. They will unify once `MyDecimal` carries the
//! full arithmetic; until then conversions bridge them as needed.
//!
//! SEED SCOPE: the struct + `FromInt`/`FromUint` + `ToString` (with
//! `removeLeadingZeros`/`digitsToWords`/`countLeadingZeroes`) and the raw
//! 40-byte round-trip, plus `FromString`/`Round`/`Shift` and their helpers
//! (`digitBounds`, `doMiniLeftShift`, `doMiniRightShift`, `maxDecimal`,
//! `fixWordCntError`, `strToInt`). DEFERRED (documented): the arithmetic
//! (add/sub/mul/div), binary `ToBin`/`FromBin` encoding, hashing, and
//! comparison. (`digitsToWords` uses Go's plain formula; Go's `div9`
//! lookup table is a performance twin with identical results.)

/// Go `maxWordBufLen`: a `MyDecimal` holds 9 words.
pub const MAX_WORD_BUF_LEN: usize = 9;
/// Go `digitsPerWord`: one word holds 9 decimal digits.
const DIGITS_PER_WORD: i32 = 9;
/// Go `wordBase` (`ten9`).
const WORD_BASE: u64 = 1_000_000_000;
/// Go `digMask` (`ten8`).
const DIG_MASK: i32 = 100_000_000;
/// Go `powers10[0..=9]`.
const POWERS10: [i32; 10] = [
    1,
    10,
    100,
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
];

/// Go `MyDecimal`: sign + digit counts + base-1e9 word buffer, in Go's exact
/// field order and layout (40 bytes).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MyDecimal {
    /// Go `digitsInt`: decimal digits before the point.
    digits_int: i8,
    /// Go `digitsFrac`: decimal digits after the point.
    digits_frac: i8,
    /// Go `resultFrac`: result fraction digits.
    result_frac: i8,
    /// Go `negative`.
    negative: bool,
    /// Go `wordBuf`: base-1e9 words (`0 <= word < wordBase`).
    word_buf: [i32; MAX_WORD_BUF_LEN],
}

/// The Go struct size (`types.MyDecimalStructSize` = 40); chunk cells store
/// exactly these bytes.
pub const MYDECIMAL_STRUCT_SIZE: usize = 40;
const _: () = assert!(std::mem::size_of::<MyDecimal>() == MYDECIMAL_STRUCT_SIZE);

/// Go `digitsToWords` (the plain formula; `div9` is its lookup twin).
fn digits_to_words(digits: i32) -> i32 {
    (digits + DIGITS_PER_WORD - 1) / DIGITS_PER_WORD
}

/// Go `countLeadingZeroes`: leading zero digits in `word`, whose digit count
/// is `i + 1`.
fn count_leading_zeroes(mut i: i32, word: i32) -> i32 {
    let mut leading = 0;
    while word < POWERS10[i as usize] {
        i -= 1;
        leading += 1;
    }
    leading
}

/// Go `wordMax`.
const WORD_MAX: i32 = (WORD_BASE as i32) - 1;

/// Go `fracMax`.
const FRAC_MAX: [i32; 8] = [
    900_000_000,
    990_000_000,
    999_000_000,
    999_900_000,
    999_990_000,
    999_999_000,
    999_999_900,
    999_999_990,
];

/// Go `RoundMode`. The values are Go's, because `Round` compares the mode's
/// numeric value against the digit after the scale (`roundDigit`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum RoundMode {
    /// Go `ModeCeiling` (no full support in Go either; see `Round`).
    Ceiling = 0,
    /// Go `ModeHalfUp`: round normally.
    HalfUp = 5,
    /// Go `ModeTruncate`: never round, just cut.
    Truncate = 10,
}

/// The decimal errors Go's `mydecimal.go` returns (`ErrTruncated`,
/// `ErrOverflow`, `ErrBadNumber`). Go returns them alongside a written result,
/// so the Rust ports return `(value, Option<DecimalError>)` shapes rather than
/// discarding the value on error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecimalError {
    /// Go `ErrTruncated`: the value did not fit and digits were dropped.
    Truncated,
    /// Go `ErrOverflow`: the value did not fit at all.
    Overflow,
    /// Go `ErrBadNumber`: the text was not a well-formed number.
    BadNumber,
}

/// Go `isSpace` (`helper.go`): only a space or a tab.
fn is_space(c: u8) -> bool {
    c == b' ' || c == b'\t'
}

/// Go `countTrailingZeroes`: trailing zero digits in `word`, starting at
/// position `i`.
fn count_trailing_zeroes(mut i: i32, word: i32) -> i32 {
    let mut trailing = 0;
    while word % POWERS10[i as usize] == 0 {
        i += 1;
        trailing += 1;
    }
    trailing
}

/// Go `fixWordCntError`: limits the word counts to `wordBufLen`.
fn fix_word_cnt_error(words_int: i32, words_frac: i32) -> (i32, i32, Option<DecimalError>) {
    let buf_len = MAX_WORD_BUF_LEN as i32;
    if words_int + words_frac > buf_len {
        if words_int > buf_len {
            return (buf_len, 0, Some(DecimalError::Overflow));
        }
        return (
            words_int,
            buf_len - words_int,
            Some(DecimalError::Truncated),
        );
    }
    (words_int, words_frac, None)
}

/// Go `add`: `a + b + carry` in base `wordBase`.
fn add_word(a: i32, b: i32, carry: i32) -> (i32, i32) {
    let sum = a + b + carry;
    if sum >= WORD_BASE as i32 {
        (sum - WORD_BASE as i32, 1)
    } else {
        (sum, 0)
    }
}

/// Go `maxDecimal`: writes the largest value of the given precision/scale.
fn max_decimal(precision: i32, frac: i32, to: &mut MyDecimal) {
    let mut digits_int = precision - frac;
    to.negative = false;
    to.digits_int = digits_int as i8;
    let mut idx = 0usize;
    if digits_int > 0 {
        let first_word_digits = digits_int % DIGITS_PER_WORD;
        if first_word_digits > 0 {
            to.word_buf[idx] = POWERS10[first_word_digits as usize] - 1; /* 9 99 999 ... */
            idx += 1;
        }
        digits_int /= DIGITS_PER_WORD;
        while digits_int > 0 {
            to.word_buf[idx] = WORD_MAX;
            idx += 1;
            digits_int -= 1;
        }
    }
    to.digits_frac = frac as i8;
    let mut frac = frac;
    if frac > 0 {
        let last_digits = frac % DIGITS_PER_WORD;
        frac /= DIGITS_PER_WORD;
        while frac > 0 {
            to.word_buf[idx] = WORD_MAX;
            idx += 1;
            frac -= 1;
        }
        if last_digits > 0 {
            to.word_buf[idx] = FRAC_MAX[(last_digits - 1) as usize];
        }
    }
}

/// Go `types.strToInt` (`helper.go`): the exponent parser `FromString` uses.
/// Returns Go's value-and-error pair; Go keeps the clamped value on
/// `ErrBadNumber`.
fn str_to_int(str: &[u8]) -> (i64, Option<DecimalError>) {
    const MAX_UINT: u64 = u64::MAX;
    const UINT_CUT_OFF: u64 = MAX_UINT / 10 + 1;
    const INT_CUT_OFF: u64 = (i64::MAX as u64) + 1;

    let trimmed = trim_ascii_space(str);
    if trimmed.is_empty() {
        return (0, Some(DecimalError::Truncated));
    }
    let mut negative = false;
    let mut i = 0usize;
    if trimmed[0] == b'-' {
        negative = true;
        i = 1;
    } else if trimmed[0] == b'+' {
        i = 1;
    }

    let mut err = None;
    let mut has_num = false;
    let mut r: u64 = 0;
    while i < trimmed.len() {
        if !trimmed[i].is_ascii_digit() {
            err = Some(DecimalError::Truncated);
            break;
        }
        has_num = true;
        if r >= UINT_CUT_OFF {
            r = 0;
            err = Some(DecimalError::BadNumber);
            break;
        }
        r *= 10;
        let r1 = r.wrapping_add(u64::from(trimmed[i] - b'0'));
        if r1 < r {
            r = 0;
            err = Some(DecimalError::BadNumber);
            break;
        }
        r = r1;
        i += 1;
    }
    if !has_num {
        err = Some(DecimalError::Truncated);
    }
    if !negative && r >= INT_CUT_OFF {
        return (i64::MAX, Some(DecimalError::BadNumber));
    }
    if negative && r > INT_CUT_OFF {
        return (i64::MIN, Some(DecimalError::BadNumber));
    }
    if negative {
        r = r.wrapping_neg();
    }
    (r as i64, err)
}

/// Go `strings.TrimSpace` restricted to the ASCII spaces `isSpace` accepts,
/// which is all `FromString` can see before the digits are validated.
fn trim_ascii_space(str: &[u8]) -> &[u8] {
    let mut start = 0;
    while start < str.len() && str[start].is_ascii_whitespace() {
        start += 1;
    }
    let mut end = str.len();
    while end > start && str[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    &str[start..end]
}

impl MyDecimal {
    /// Go `digitBounds`: `(start, end)` indexes of the first non-zero decimal
    /// digit and of the position just after the last one.
    fn digit_bounds(&self) -> (i32, i32) {
        let buf_len = digits_to_words(i32::from(self.digits_int))
            + digits_to_words(i32::from(self.digits_frac));
        let mut buf_beg = 0i32;
        let mut buf_end = buf_len - 1;

        /* find non-zero digit from number beginning */
        while buf_beg < buf_len && self.word_buf[buf_beg as usize] == 0 {
            buf_beg += 1;
        }
        if buf_beg >= buf_len {
            return (0, 0);
        }

        /* find non-zero decimal digit from number beginning */
        let mut i;
        let mut start;
        if buf_beg == 0 && self.digits_int > 0 {
            i = (i32::from(self.digits_int) - 1) % DIGITS_PER_WORD;
            start = DIGITS_PER_WORD - i - 1;
        } else {
            i = DIGITS_PER_WORD - 1;
            start = buf_beg * DIGITS_PER_WORD;
        }
        if buf_beg < buf_len {
            start += count_leading_zeroes(i, self.word_buf[buf_beg as usize]);
        }

        /* find non-zero digit at the end */
        while buf_end > buf_beg && self.word_buf[buf_end as usize] == 0 {
            buf_end -= 1;
        }
        /* find non-zero decimal digit from the end */
        let mut end;
        if buf_end == buf_len - 1 && self.digits_frac > 0 {
            i = (i32::from(self.digits_frac) - 1) % DIGITS_PER_WORD + 1;
            end = buf_end * DIGITS_PER_WORD + i;
            i = DIGITS_PER_WORD - i + 1;
        } else {
            end = (buf_end + 1) * DIGITS_PER_WORD;
            i = 1;
        }
        end -= count_trailing_zeroes(i, self.word_buf[buf_end as usize]);
        (start, end)
    }

    /// Go `doMiniLeftShift`: aligns the digits left inside the word buffer.
    /// `shift` is 1..=`digitsPerWord`-1 and the result is guaranteed to fit.
    fn do_mini_left_shift(&mut self, shift: i32, beg: i32, end: i32) {
        let mut buf_from = (beg / DIGITS_PER_WORD) as usize;
        let buf_end = ((end - 1) / DIGITS_PER_WORD) as usize;
        let c_shift = (DIGITS_PER_WORD - shift) as usize;
        if beg % DIGITS_PER_WORD < shift {
            self.word_buf[buf_from - 1] = self.word_buf[buf_from] / POWERS10[c_shift];
        }
        while buf_from < buf_end {
            self.word_buf[buf_from] = (self.word_buf[buf_from] % POWERS10[c_shift])
                * POWERS10[shift as usize]
                + self.word_buf[buf_from + 1] / POWERS10[c_shift];
            buf_from += 1;
        }
        self.word_buf[buf_from] =
            (self.word_buf[buf_from] % POWERS10[c_shift]) * POWERS10[shift as usize];
    }

    /// Go `doMiniRightShift`: aligns the digits right inside the word buffer.
    fn do_mini_right_shift(&mut self, shift: i32, beg: i32, end: i32) {
        let mut buf_from = ((end - 1) / DIGITS_PER_WORD) as usize;
        let buf_end = (beg / DIGITS_PER_WORD) as usize;
        let c_shift = (DIGITS_PER_WORD - shift) as usize;
        if DIGITS_PER_WORD - ((end - 1) % DIGITS_PER_WORD + 1) < shift {
            self.word_buf[buf_from + 1] =
                (self.word_buf[buf_from] % POWERS10[shift as usize]) * POWERS10[c_shift];
        }
        while buf_from > buf_end {
            self.word_buf[buf_from] = self.word_buf[buf_from] / POWERS10[shift as usize]
                + (self.word_buf[buf_from - 1] % POWERS10[shift as usize]) * POWERS10[c_shift];
            buf_from -= 1;
        }
        self.word_buf[buf_from] /= POWERS10[shift as usize];
    }

    /// Go `Round`: rounds `self` to `frac` digits into `to` (`frac` may be
    /// negative). Returns Go's error alongside the written result.
    ///
    /// Go allows `to == d`; the Rust signature takes `to` by value-out
    /// instead, and the in-place case is [`MyDecimal::round_in_place`].
    pub fn round(
        &self,
        to: &mut MyDecimal,
        frac: i32,
        round_mode: RoundMode,
    ) -> Option<DecimalError> {
        let same = std::ptr::eq(self, to);
        debug_assert!(!same, "use round_in_place for the aliasing case");
        to.round_from(self, frac, round_mode)
    }

    /// Go `d.Round(d, frac, mode)`: the aliasing (in-place) call.
    pub fn round_in_place(&mut self, frac: i32, round_mode: RoundMode) -> Option<DecimalError> {
        let source = *self;
        self.round_from_aliased(&source, frac, round_mode, true)
    }

    fn round_from(
        &mut self,
        d: &MyDecimal,
        frac: i32,
        round_mode: RoundMode,
    ) -> Option<DecimalError> {
        self.round_from_aliased(d, frac, round_mode, false)
    }

    /// The body of Go `Round`. `aliased` reproduces Go's `to != d` test: when
    /// the caller passed `d` as `to`, Go skips the copy-in step.
    fn round_from_aliased(
        &mut self,
        d: &MyDecimal,
        frac: i32,
        round_mode: RoundMode,
        aliased: bool,
    ) -> Option<DecimalError> {
        let buf_len = MAX_WORD_BUF_LEN as i32;
        let mut frac = frac;
        let mut err = None;
        // wordsFracTo is the number of fraction words in buffer.
        let mut words_frac_to = (frac + 1) / DIGITS_PER_WORD;
        if frac > 0 {
            words_frac_to = digits_to_words(frac);
        }
        let words_frac = digits_to_words(i32::from(d.digits_frac));
        let words_int = digits_to_words(i32::from(d.digits_int));
        let round_digit = round_mode as i32;
        /* TODO - fix this code as it won't work for CEILING mode */

        if words_int + words_frac_to > buf_len {
            words_frac_to = buf_len - words_int;
            frac = words_frac_to * DIGITS_PER_WORD;
            err = Some(DecimalError::Truncated);
        }
        if i32::from(d.digits_int) + frac < 0 {
            *self = MyDecimal::default();
            return None;
        }
        if !aliased {
            self.word_buf = d.word_buf;
            self.negative = d.negative;
            self.digits_int = (words_int.min(buf_len) * DIGITS_PER_WORD) as i8;
        }
        if words_frac_to > words_frac {
            let mut idx = (words_int + words_frac) as usize;
            while words_frac_to > words_frac {
                words_frac_to -= 1;
                self.word_buf[idx] = 0;
                idx += 1;
            }
            self.digits_frac = frac as i8;
            self.result_frac = self.digits_frac;
            return err;
        }
        if frac >= i32::from(d.digits_frac) {
            self.digits_frac = frac as i8;
            self.result_frac = self.digits_frac;
            return err;
        }

        // Do increment.
        let mut to_idx = words_int + words_frac_to - 1;
        if frac == words_frac_to * DIGITS_PER_WORD {
            let do_inc = match round_mode {
                // Notice: No support for ceiling mode now.
                RoundMode::Ceiling => {
                    // If any word after scale is not zero, do increment.
                    // e.g ceiling 3.0001 to scale 1, gets 3.1
                    let mut idx = to_idx + (words_frac - words_frac_to);
                    let mut inc = false;
                    while idx > to_idx {
                        if d.word_buf[idx as usize] != 0 {
                            inc = true;
                            break;
                        }
                        idx -= 1;
                    }
                    inc
                }
                RoundMode::HalfUp => {
                    // the first digit after scale; increment when it is >= 5.
                    d.word_buf[(to_idx + 1) as usize] / DIG_MASK >= 5
                }
                // Never round, just truncate.
                RoundMode::Truncate => false,
            };
            if do_inc {
                if to_idx >= 0 {
                    self.word_buf[to_idx as usize] += 1;
                } else {
                    to_idx += 1;
                    self.word_buf[to_idx as usize] = WORD_BASE as i32;
                }
            } else if words_int + words_frac_to == 0 {
                *self = MyDecimal::default();
                return None;
            }
        } else {
            /* TODO - fix this code as it won't work for CEILING mode */
            let pos = (words_frac_to * DIGITS_PER_WORD - frac - 1) as usize;
            let mut shifted_number = self.word_buf[to_idx as usize] / POWERS10[pos];
            let dig_after_scale = shifted_number % 10;
            if dig_after_scale > round_digit || (round_digit == 5 && dig_after_scale == 5) {
                shifted_number += 10;
            }
            self.word_buf[to_idx as usize] = POWERS10[pos] * (shifted_number - dig_after_scale);
        }
        /*
           In case we're rounding e.g. 1.5e9 to 2.0e9, the decimal words inside
           the buffer are as follows.

           Before <1, 5e8>
           After  <2, 5e8>

           Hence we need to set the 2nd field to 0.
           The same holds if we round 1.5e-9 to 2e-9.
        */
        if words_frac_to < words_frac {
            let mut idx = words_int + words_frac_to;
            if frac == 0 && words_int == 0 {
                idx = 1;
            }
            while idx < buf_len {
                self.word_buf[idx as usize] = 0;
                idx += 1;
            }
        }

        // Handle carry.
        if self.word_buf[to_idx as usize] >= WORD_BASE as i32 {
            let mut carry = 1;
            self.word_buf[to_idx as usize] -= WORD_BASE as i32;
            while carry == 1 && to_idx > 0 {
                to_idx -= 1;
                let (sum, new_carry) = add_word(self.word_buf[to_idx as usize], 0, carry);
                self.word_buf[to_idx as usize] = sum;
                carry = new_carry;
            }
            if carry > 0 {
                if words_int + words_frac_to >= buf_len {
                    words_frac_to -= 1;
                    frac = words_frac_to * DIGITS_PER_WORD;
                    err = Some(DecimalError::Truncated);
                }
                to_idx = words_int + words_frac_to.max(0);
                while to_idx > 0 {
                    if to_idx < buf_len {
                        self.word_buf[to_idx as usize] = self.word_buf[(to_idx - 1) as usize];
                    } else {
                        err = Some(DecimalError::Overflow);
                    }
                    to_idx -= 1;
                }
                self.word_buf[to_idx as usize] = 1;
                /* We cannot have more than 9 * 9 = 81 digits. */
                if i32::from(self.digits_int) < DIGITS_PER_WORD * buf_len {
                    self.digits_int += 1;
                } else {
                    err = Some(DecimalError::Overflow);
                }
            }
        } else {
            loop {
                if self.word_buf[to_idx as usize] != 0 {
                    break;
                }
                if to_idx == 0 {
                    /* making 'zero' with the proper scale */
                    let idx = words_frac_to + 1;
                    self.digits_int = 1;
                    self.digits_frac = frac.max(0) as i8;
                    self.negative = false;
                    while to_idx < idx {
                        self.word_buf[to_idx as usize] = 0;
                        to_idx += 1;
                    }
                    self.result_frac = self.digits_frac;
                    return None;
                }
                to_idx -= 1;
            }
        }
        /* Here we check 999.9 -> 1000 case when we need to increase intDigCnt */
        let first_dig = i32::from(self.digits_int) % DIGITS_PER_WORD;
        if first_dig > 0 && self.word_buf[to_idx as usize] >= POWERS10[first_dig as usize] {
            self.digits_int += 1;
        }
        if frac < 0 {
            frac = 0;
        }
        self.digits_frac = frac as i8;
        self.result_frac = self.digits_frac;
        err
    }

    /// Go `Shift`: shifts the decimal digits by `shift` places, i.e. multiplies
    /// by `10^shift` (with rounding if needed). Negative shifts move right.
    pub fn shift(&mut self, shift: i32) -> Option<DecimalError> {
        let buf_len = MAX_WORD_BUF_LEN as i32;
        let mut err = None;
        if shift == 0 {
            return None;
        }
        // point is the index of the digit position just after the point.
        let point = digits_to_words(i32::from(self.digits_int)) * DIGITS_PER_WORD;
        // new point position.
        let mut new_point = point + shift;
        let (mut digit_begin, mut digit_end) = self.digit_bounds();
        if digit_begin == digit_end {
            *self = MyDecimal::default();
            return None;
        }

        let digits_int = (new_point - digit_begin).max(0);
        let mut digits_frac = (digit_end - new_point).max(0);
        let words_int = digits_to_words(digits_int);
        let mut words_frac = digits_to_words(digits_frac);
        let new_len = words_int + words_frac;
        if new_len > buf_len {
            let lack = new_len - buf_len;
            if words_frac < lack {
                return Some(DecimalError::Overflow);
            }
            /* cut off fraction part to allow new number to fit in our buffer */
            err = Some(DecimalError::Truncated);
            words_frac -= lack;
            let diff = digits_frac - words_frac * DIGITS_PER_WORD;
            if let Some(err1) = self.round_in_place(digit_end - point - diff, RoundMode::HalfUp) {
                return Some(err1);
            }
            digit_end -= diff;
            digits_frac = words_frac * DIGITS_PER_WORD;
            if digit_end <= digit_begin {
                /*
                   We lost all digits (they will be shifted out of buffer), so we
                   can just return 0.
                */
                *self = MyDecimal::default();
                return Some(DecimalError::Truncated);
            }
        }

        if shift % DIGITS_PER_WORD != 0 {
            /*
               Calculate left/right shift to align decimal digits inside our big
               digits correctly.
            */
            let l_mini_shift;
            let r_mini_shift;
            let do_left;
            if shift > 0 {
                l_mini_shift = shift % DIGITS_PER_WORD;
                r_mini_shift = DIGITS_PER_WORD - l_mini_shift;
                do_left = l_mini_shift <= digit_begin;
            } else {
                r_mini_shift = (-shift) % DIGITS_PER_WORD;
                l_mini_shift = DIGITS_PER_WORD - r_mini_shift;
                do_left = (DIGITS_PER_WORD * buf_len - digit_end) < r_mini_shift;
            }
            let mini_shift = if do_left {
                self.do_mini_left_shift(l_mini_shift, digit_begin, digit_end);
                -l_mini_shift
            } else {
                self.do_mini_right_shift(r_mini_shift, digit_begin, digit_end);
                r_mini_shift
            };
            new_point += mini_shift;
            /*
               If number is shifted and correctly aligned in buffer we can finish.
            */
            if shift + mini_shift == 0 && (new_point - digits_int) < DIGITS_PER_WORD {
                self.digits_int = digits_int as i8;
                self.digits_frac = digits_frac as i8;
                return err; /* already shifted as it should be */
            }
            digit_begin += mini_shift;
            digit_end += mini_shift;
        }

        /* if new 'decimal front' is in first digit, we do not need move digits */
        let new_front = new_point - digits_int;
        if !(0..DIGITS_PER_WORD).contains(&new_front) {
            /* need to move digits */
            let mut word_shift;
            if new_front > 0 {
                /* move left */
                word_shift = new_front / DIGITS_PER_WORD;
                let mut to = digit_begin / DIGITS_PER_WORD - word_shift;
                let mut barier = (digit_end - 1) / DIGITS_PER_WORD - word_shift;
                while to <= barier {
                    self.word_buf[to as usize] = self.word_buf[(to + word_shift) as usize];
                    to += 1;
                }
                barier += word_shift;
                while to <= barier {
                    self.word_buf[to as usize] = 0;
                    to += 1;
                }
                word_shift = -word_shift;
            } else {
                /* move right */
                word_shift = (1 - new_front) / DIGITS_PER_WORD;
                let mut to = (digit_end - 1) / DIGITS_PER_WORD + word_shift;
                let mut barier = digit_begin / DIGITS_PER_WORD + word_shift;
                while to >= barier {
                    self.word_buf[to as usize] = self.word_buf[(to - word_shift) as usize];
                    to -= 1;
                }
                barier -= word_shift;
                while to >= barier {
                    self.word_buf[to as usize] = 0;
                    to -= 1;
                }
            }
            let digit_shift = word_shift * DIGITS_PER_WORD;
            digit_begin += digit_shift;
            digit_end += digit_shift;
            new_point += digit_shift;
        }
        /*
           If there are gaps then fill them with 0.

           Only one of following loops will work because wordIdxBegin <= wordIdxEnd.
        */
        let word_idx_begin = digit_begin / DIGITS_PER_WORD;
        let word_idx_end = (digit_end - 1) / DIGITS_PER_WORD;
        /* We don't want negative new_point below */
        let mut word_idx_new_point = 0;
        if new_point != 0 {
            word_idx_new_point = (new_point - 1) / DIGITS_PER_WORD;
        }
        if word_idx_new_point > word_idx_end {
            while word_idx_new_point > word_idx_end {
                self.word_buf[word_idx_new_point as usize] = 0;
                word_idx_new_point -= 1;
            }
        } else {
            while word_idx_new_point < word_idx_begin {
                self.word_buf[word_idx_new_point as usize] = 0;
                word_idx_new_point += 1;
            }
        }
        self.digits_int = digits_int as i8;
        self.digits_frac = digits_frac as i8;
        err
    }

    /// Go `FromString`: parses MySQL decimal text (optional sign, digits, an
    /// optional `.` fraction, an optional `e`/`E` exponent). Returns Go's
    /// error alongside the parsed value -- Go writes a result even when it
    /// reports truncation or overflow.
    ///
    /// Go's `ErrTruncatedWrongVal.FastGenByArgs("DECIMAL", str)` for empty or
    /// digit-less input becomes [`DecimalError::BadNumber`]; the wrapped
    /// warning text belongs to the statement-context tier, not here.
    pub fn from_string(str: &[u8]) -> (MyDecimal, Option<DecimalError>) {
        let mut d = MyDecimal::default();
        let err = d.set_from_string(str);
        (d, err)
    }

    fn set_from_string(&mut self, str: &[u8]) -> Option<DecimalError> {
        let buf_len = MAX_WORD_BUF_LEN as i32;
        let mut str = str;
        for i in 0..str.len() {
            if !is_space(str[i]) {
                str = &str[i..];
                break;
            }
        }
        if str.is_empty() {
            *self = MyDecimal::default();
            return Some(DecimalError::BadNumber);
        }
        match str[0] {
            b'-' => {
                self.negative = true;
                str = &str[1..];
            }
            b'+' => str = &str[1..],
            _ => {}
        }
        let mut str_idx = 0usize;
        while str_idx < str.len() && str[str_idx].is_ascii_digit() {
            str_idx += 1;
        }
        let mut digits_int = str_idx as i32;
        let mut digits_frac;
        let end_idx;
        if str_idx < str.len() && str[str_idx] == b'.' {
            let mut e = str_idx + 1;
            while e < str.len() && str[e].is_ascii_digit() {
                e += 1;
            }
            digits_frac = (e - str_idx - 1) as i32;
            end_idx = e;
        } else {
            digits_frac = 0;
            end_idx = str_idx;
        }
        if digits_int + digits_frac == 0 {
            *self = MyDecimal::default();
            return Some(DecimalError::BadNumber);
        }
        let words_int_raw = digits_to_words(digits_int);
        let words_frac_raw = digits_to_words(digits_frac);
        let (words_int, words_frac, mut err) = fix_word_cnt_error(words_int_raw, words_frac_raw);
        if err.is_some() {
            digits_frac = words_frac * DIGITS_PER_WORD;
            if err == Some(DecimalError::Overflow) {
                digits_int = words_int * DIGITS_PER_WORD;
            }
        }
        self.digits_int = digits_int as i8;
        self.digits_frac = digits_frac as i8;
        let mut word_idx = words_int;
        let str_idx_tmp = str_idx;
        let mut word: i32 = 0;
        let mut inner_idx = 0i32;
        while digits_int > 0 {
            digits_int -= 1;
            str_idx -= 1;
            word += i32::from(str[str_idx] - b'0') * POWERS10[inner_idx as usize];
            inner_idx += 1;
            if inner_idx == DIGITS_PER_WORD {
                word_idx -= 1;
                self.word_buf[word_idx as usize] = word;
                word = 0;
                inner_idx = 0;
            }
        }
        if inner_idx != 0 {
            word_idx -= 1;
            self.word_buf[word_idx as usize] = word;
        }

        word_idx = words_int;
        str_idx = str_idx_tmp;
        word = 0;
        inner_idx = 0;
        while digits_frac > 0 {
            digits_frac -= 1;
            str_idx += 1;
            word = i32::from(str[str_idx] - b'0') + word * 10;
            inner_idx += 1;
            if inner_idx == DIGITS_PER_WORD {
                self.word_buf[word_idx as usize] = word;
                word_idx += 1;
                word = 0;
                inner_idx = 0;
            }
        }
        if inner_idx != 0 {
            self.word_buf[word_idx as usize] =
                word * POWERS10[(DIGITS_PER_WORD - inner_idx) as usize];
        }
        // Go writes this as `endIdx+1 <= len(str)`.
        if end_idx < str.len() {
            if str[end_idx] == b'e' || str[end_idx] == b'E' {
                let (exponent, err1) = str_to_int(&str[end_idx + 1..]);
                if let Some(cause) = err1 {
                    err = Some(cause);
                    if cause != DecimalError::Truncated {
                        *self = MyDecimal::default();
                    }
                }
                if exponent > i64::from(i32::MAX / 2) {
                    let negative = self.negative;
                    max_decimal(buf_len * DIGITS_PER_WORD, 0, self);
                    self.negative = negative;
                    err = Some(DecimalError::Overflow);
                }
                if exponent < i64::from(i32::MIN / 2) && err != Some(DecimalError::Overflow) {
                    *self = MyDecimal::default();
                    err = Some(DecimalError::Truncated);
                }
                if err != Some(DecimalError::Overflow) {
                    // The bounds above keep the exponent inside `i32` here.
                    if let Some(shift_err) = self.shift(exponent as i32) {
                        if shift_err == DecimalError::Overflow {
                            let negative = self.negative;
                            max_decimal(buf_len * DIGITS_PER_WORD, 0, self);
                            self.negative = negative;
                        }
                        err = Some(shift_err);
                    }
                }
            } else if !trim_ascii_space(&str[end_idx..]).is_empty() {
                err = Some(DecimalError::Truncated);
            }
        }
        if self.word_buf.iter().all(|word| *word == 0) {
            self.negative = false;
        }
        self.result_frac = self.digits_frac;
        err
    }

    /// Go `FromInt`.
    #[must_use]
    pub fn from_int(val: i64) -> MyDecimal {
        let mut d = MyDecimal::default();
        let u_val = if val < 0 {
            d.negative = true;
            val.unsigned_abs()
        } else {
            val as u64
        };
        d.set_from_uint(u_val);
        d
    }

    /// Go `FromUint`.
    #[must_use]
    pub fn from_uint(val: u64) -> MyDecimal {
        let mut d = MyDecimal::default();
        d.set_from_uint(val);
        d
    }

    fn set_from_uint(&mut self, val: u64) {
        let mut x = val;
        let mut word_idx = 1usize;
        while x >= WORD_BASE {
            word_idx += 1;
            x /= WORD_BASE;
        }
        self.digits_frac = 0;
        self.digits_int = (word_idx as i32 * DIGITS_PER_WORD) as i8;
        x = val;
        while word_idx > 0 {
            word_idx -= 1;
            let y = x / WORD_BASE;
            self.word_buf[word_idx] = (x - y * WORD_BASE) as i32;
            x = y;
        }
    }

    /// Go `IsNegative`.
    #[must_use]
    pub fn is_negative(&self) -> bool {
        self.negative
    }

    /// Go `digitsFrac`: the decimal digits after the point.
    #[must_use]
    pub fn digits_frac(&self) -> i8 {
        self.digits_frac
    }

    /// Go `removeLeadingZeros`.
    fn remove_leading_zeros(&self) -> (usize, i32) {
        let mut word_idx = 0usize;
        let mut digits_int = i32::from(self.digits_int);
        let mut i = ((digits_int - 1) % DIGITS_PER_WORD) + 1;
        while digits_int > 0 && self.word_buf[word_idx] == 0 {
            digits_int -= i;
            i = DIGITS_PER_WORD;
            word_idx += 1;
        }
        if digits_int > 0 {
            digits_int -=
                count_leading_zeroes((digits_int - 1) % DIGITS_PER_WORD, self.word_buf[word_idx]);
        } else {
            digits_int = 0;
        }
        (word_idx, digits_int)
    }

    /// Go `ToString`: the decimal's plain text form (no exponent), a
    /// line-for-line port including the zero and fill-digit handling.
    #[must_use]
    pub fn to_string_bytes(&self) -> Vec<u8> {
        let digits_frac_total = i32::from(self.digits_frac);
        let mut digits_frac = digits_frac_total;
        let (word_start_idx, mut digits_int) = self.remove_leading_zeros();
        let mut word_start_idx = word_start_idx;
        if digits_int + digits_frac == 0 {
            digits_int = 1;
            word_start_idx = 0;
        }
        let digits_int_len = if digits_int == 0 { 1 } else { digits_int };
        let digits_frac_len = digits_frac;
        let mut length = digits_int_len + digits_frac_len;
        if self.negative {
            length += 1;
        }
        if digits_frac > 0 {
            length += 1;
        }
        let mut str = vec![0u8; length as usize];
        let mut str_idx = 0usize;
        if self.negative {
            str[str_idx] = b'-';
            str_idx += 1;
        }
        let mut fill;
        if digits_frac > 0 {
            let mut frac_idx = str_idx + digits_int_len as usize;
            fill = digits_frac_len - digits_frac;
            let mut word_idx = word_start_idx + digits_to_words(digits_int) as usize;
            str[frac_idx] = b'.';
            frac_idx += 1;
            while digits_frac > 0 {
                let mut x = self.word_buf[word_idx];
                word_idx += 1;
                let mut i = digits_frac.min(DIGITS_PER_WORD);
                while i > 0 {
                    let y = x / DIG_MASK;
                    str[frac_idx] = y as u8 + b'0';
                    frac_idx += 1;
                    x -= y * DIG_MASK;
                    x *= 10;
                    i -= 1;
                }
                digits_frac -= DIGITS_PER_WORD;
            }
            while fill > 0 {
                str[frac_idx] = b'0';
                frac_idx += 1;
                fill -= 1;
            }
        }
        fill = digits_int_len - digits_int;
        if digits_int == 0 {
            fill -= 1; // symbol 0 before the decimal point
        }
        while fill > 0 {
            str[str_idx] = b'0';
            str_idx += 1;
            fill -= 1;
        }
        if digits_int > 0 {
            str_idx += digits_int as usize;
            let mut word_idx = word_start_idx + digits_to_words(digits_int) as usize;
            while digits_int > 0 {
                word_idx -= 1;
                let mut x = self.word_buf[word_idx];
                let mut i = digits_int.min(DIGITS_PER_WORD);
                while i > 0 {
                    let y = x / 10;
                    str_idx -= 1;
                    str[str_idx] = b'0' + (x - y * 10) as u8;
                    x = y;
                    i -= 1;
                }
                digits_int -= DIGITS_PER_WORD;
            }
        } else {
            str[str_idx] = b'0';
        }
        str
    }

    /// The exact 40 bytes a Go chunk `NewDecimal` cell stores (Go copies the
    /// struct through `unsafe.Pointer`; this assembles the identical bytes
    /// field by field -- same layout, no `unsafe`).
    #[must_use]
    pub fn to_raw_bytes(&self) -> [u8; MYDECIMAL_STRUCT_SIZE] {
        let mut bytes = [0u8; MYDECIMAL_STRUCT_SIZE];
        bytes[0] = self.digits_int as u8;
        bytes[1] = self.digits_frac as u8;
        bytes[2] = self.result_frac as u8;
        bytes[3] = u8::from(self.negative);
        for (chunk, w) in bytes[4..].chunks_exact_mut(4).zip(&self.word_buf) {
            chunk.copy_from_slice(&w.to_ne_bytes());
        }
        bytes
    }

    /// Rebuilds a decimal from a chunk cell's raw 40 bytes.
    ///
    /// # Safety contract (checked)
    /// The `negative` byte must be 0/1 (any other value would be undefined for
    /// `bool`), so this validates and errors rather than transmuting blindly.
    pub fn from_raw_bytes(bytes: [u8; MYDECIMAL_STRUCT_SIZE]) -> Result<MyDecimal, &'static str> {
        if bytes[3] > 1 {
            return Err("invalid MyDecimal negative flag byte");
        }
        let mut d = MyDecimal {
            digits_int: bytes[0] as i8,
            digits_frac: bytes[1] as i8,
            result_frac: bytes[2] as i8,
            negative: bytes[3] == 1,
            word_buf: [0; MAX_WORD_BUF_LEN],
        };
        for (w, chunk) in d.word_buf.iter_mut().zip(bytes[4..].chunks_exact(4)) {
            *w = i32::from_ne_bytes(chunk.try_into().expect("4-byte word"));
        }
        Ok(d)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Differential fixture: every expectation is `types.MyDecimal.FromString`
    /// output captured from the Go implementation in this repository
    /// (input, `String()`, error, `digitsInt`, `digitsFrac`, `negative`).
    #[test]
    fn from_string_matches_go() {
        type Case = (
            &'static str,
            &'static str,
            Option<DecimalError>,
            i8,
            i8,
            bool,
        );
        let cases: &[Case] = &[
        ("0", "0", None, 1, 0, false),
        ("1", "1", None, 1, 0, false),
        ("-1", "-1", None, 1, 0, true),
        ("12345", "12345", None, 5, 0, false),
        ("1.5", "1.5", None, 1, 1, false),
        ("-1.50", "-1.50", None, 1, 2, true),
        ("0.000001", "0.000001", None, 1, 6, false),
        ("123456789012345678901234567890", "123456789012345678901234567890", None, 30, 0, false),
        ("  42  ", "42", None, 2, 0, false),
        ("+3.14", "3.14", None, 1, 2, false),
        (".5", "0.5", None, 0, 1, false),
        ("5.", "5", None, 1, 0, false),
        ("1e3", "1000", None, 4, 0, false),
        ("1E-3", "0.001", None, 0, 3, false),
        ("1.5e10", "15000000000", None, 11, 0, false),
        ("-1.5e-10", "-0.00000000015", None, 0, 11, true),
        ("1e100", "999999999999999999999999999999999999999999999999999999999999999999999999999999999", Some(DecimalError::Overflow), 81, 0, false),
        ("1e-100", "0", Some(DecimalError::Truncated), 0, 0, false),
        ("1e1000000000000", "999999999999999999999999999999999999999999999999999999999999999999999999999999999", Some(DecimalError::Overflow), 81, 0, false),
        ("1e-1000000000000", "0", Some(DecimalError::Truncated), 0, 0, false),
        ("1.23e5", "123000", None, 6, 0, false),
        ("999999999999999999999999999999.9999999999999999999999999999999999", "999999999999999999999999999999.9999999999999999999999999999999999", None, 30, 34, false),
        ("abc", "0", Some(DecimalError::BadNumber), 0, 0, false),
        ("", "0", Some(DecimalError::BadNumber), 0, 0, false),
        ("   ", "0", Some(DecimalError::BadNumber), 0, 0, false),
        ("1x", "1", Some(DecimalError::Truncated), 1, 0, false),
        ("1.2.3", "1.2", Some(DecimalError::Truncated), 1, 1, false),
        ("1e", "1", Some(DecimalError::Truncated), 1, 0, false),
        ("1e+", "1", Some(DecimalError::Truncated), 1, 0, false),
        ("0.0", "0.0", None, 1, 1, false),
        ("-0.0", "0.0", None, 1, 1, false),
        ("-0", "0", None, 1, 0, false),
        ("12345678901234567890.12345678901234567890", "12345678901234567890.12345678901234567890", None, 20, 20, false),
        ("1e9", "1000000000", None, 10, 0, false),
        ("1e-9", "0.000000001", None, 0, 9, false),
        ("123.456e-2", "1.23456", None, 1, 5, false),
        ("0.1e-80", "0.000000000000000000000000000000000000000000000000000000000000000000000000000000001", None, 0, 81, false),
        ("9e81", "999999999999999999999999999999999999999999999999999999999999999999999999999999999", Some(DecimalError::Overflow), 81, 0, false),
        ];
        for (input, text, want_err, digits_int, digits_frac, negative) in cases {
            let (d, err) = MyDecimal::from_string(input.as_bytes());
            assert_eq!(err, *want_err, "error for {input:?}");
            assert_eq!(
                String::from_utf8(d.to_string_bytes()).unwrap(),
                *text,
                "text for {input:?}"
            );
            assert_eq!(d.digits_int, *digits_int, "digits_int for {input:?}");
            assert_eq!(d.digits_frac, *digits_frac, "digits_frac for {input:?}");
            assert_eq!(d.negative, *negative, "negative for {input:?}");
            assert_eq!(d.result_frac, d.digits_frac, "result_frac for {input:?}");
        }
    }

    /// `from_string` and `from_int` must agree on integral text, and the
    /// parsed value must survive the raw 40-byte chunk round-trip.
    #[test]
    fn from_string_agrees_with_from_int_and_round_trips() {
        for value in [0i64, 1, -1, 12345, -987654321, i64::MAX, i64::MIN] {
            let text = value.to_string();
            let (parsed, err) = MyDecimal::from_string(text.as_bytes());
            assert_eq!(err, None, "{value} parses cleanly");
            assert_eq!(
                parsed.to_string_bytes(),
                MyDecimal::from_int(value).to_string_bytes(),
                "{value} matches from_int"
            );
            let restored = MyDecimal::from_raw_bytes(parsed.to_raw_bytes()).expect("valid bytes");
            assert_eq!(restored, parsed, "{value} survives the raw round-trip");
        }
    }

    #[test]
    fn layout_is_40_bytes() {
        assert_eq!(std::mem::size_of::<MyDecimal>(), 40);
    }

    #[test]
    fn from_int_to_string() {
        for (v, expect) in [
            (0i64, "0"),
            (1, "1"),
            (-1, "-1"),
            (42, "42"),
            (-12345, "-12345"),
            (1_000_000_000, "1000000000"),
            (i64::MAX, "9223372036854775807"),
            (i64::MIN, "-9223372036854775808"),
        ] {
            let d = MyDecimal::from_int(v);
            assert_eq!(
                String::from_utf8(d.to_string_bytes()).unwrap(),
                expect,
                "value {v}"
            );
        }
    }

    #[test]
    fn from_uint_to_string() {
        let d = MyDecimal::from_uint(u64::MAX);
        assert_eq!(
            String::from_utf8(d.to_string_bytes()).unwrap(),
            "18446744073709551615"
        );
    }

    #[test]
    fn raw_bytes_round_trip() {
        for v in [0i64, 7, -7, 123_456_789_012_345, i64::MIN] {
            let d = MyDecimal::from_int(v);
            let bytes = d.to_raw_bytes();
            let back = MyDecimal::from_raw_bytes(bytes).unwrap();
            assert_eq!(back, d, "value {v}");
            assert_eq!(back.to_string_bytes(), d.to_string_bytes());
        }
        // The negative flag byte is validated.
        let mut bytes = MyDecimal::from_int(1).to_raw_bytes();
        bytes[3] = 2;
        assert!(MyDecimal::from_raw_bytes(bytes).is_err());
    }

    #[test]
    fn negative_flag_sits_at_byte_3() {
        // Field order (and thus the chunk byte layout) matches Go.
        let bytes = MyDecimal::from_int(-5).to_raw_bytes();
        assert_eq!(bytes[3], 1);
        let bytes = MyDecimal::from_int(5).to_raw_bytes();
        assert_eq!(bytes[3], 0);
    }
}
