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

use smallvec::SmallVec;

use super::{pad_scale, Decimal, DecimalDigits, INLINE_DECIMAL_DIGITS};

// ===========================================================================
// Binary storage codec: faithful port of Go `MyDecimal` `ToBin`/`DecimalBinSize`
// (`pkg/types/mydecimal.go`). This is how a `DECIMAL` is stored in a TiKV row
// payload, and it is memcmp-comparable, so the emitted bytes must be
// byte-identical to TiDB (verified against Go `ToBin` output vectors).
//
// The algorithm operates on the base-1e9 word-buffer view of the value, exactly
// as Go does; the Rust `Decimal` keeps a normalized digit string, so
// `MyDecimalWords::from_decimal` reconstructs Go's `wordBuf`/`digitsInt`/
// `digitsFrac` view (mirroring Go `FromString`'s population), and `to_bin`
// below is then a line-for-line port of Go `WriteBin`.
//
// Public parsing applies Go's fixed nine-word bound before values reach this
// representation. The word view below therefore reconstructs the exact source
// payload rather than accepting an arbitrary-precision compatibility branch.

pub(super) const DIGITS_PER_WORD: usize = 9;
const CODEC_WORD_SIZE: usize = 4;
pub(super) const CODEC_WORD_BUF_LEN: usize = 9;
/// Largest value one 1e9 word holds (Go `wordMax` = `wordBase - 1`).
const CODEC_WORD_MAX: i32 = 999_999_999;
/// `mysql.MaxDecimalScale`.
pub(super) const CODEC_MAX_DECIMAL_SCALE: i32 = 30;
/// Bytes needed to store `k` decimal digits packed into one partial word.
const DIG2BYTES: [usize; 10] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];
/// `10^k` for `k` in `0..=9` (all fit in `i32`; `10^9 < i32::MAX`).
pub(super) const CODEC_POWERS10: [i32; 10] = [
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

/// Hard codec failure — Go `ErrBadNumber` (illegal precision/scale, or a corrupt
/// binary). Truncation/overflow are soft and reported as [`DecimalCodecWarning`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecimalCodecError {
    /// Precision/scale outside the legal range, or a corrupt binary payload.
    BadNumber,
}

/// Soft codec outcome carried beside a valid result, mirroring Go's non-fatal
/// `ErrTruncated`/`ErrOverflow` returned from `ToBin`/`FromBin`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecimalCodecWarning {
    /// Fraction digits were dropped to fit the requested scale.
    Truncated,
    /// Integer digits did not fit the requested precision.
    Overflow,
}

/// Go `digitsToWords`: number of 1e9 words needed for `digits` decimal digits.
pub(super) fn digits_to_words(digits: usize) -> usize {
    digits.div_ceil(DIGITS_PER_WORD)
}

/// Go `fixWordCntError`: clamp a word count to the nine-word buffer, reporting
/// the overflow/truncation Go would.
pub(super) fn fix_word_cnt_error(
    words_int: usize,
    words_frac: usize,
) -> (usize, usize, Option<DecimalCodecWarning>) {
    if words_int + words_frac > CODEC_WORD_BUF_LEN {
        if words_int > CODEC_WORD_BUF_LEN {
            return (CODEC_WORD_BUF_LEN, 0, Some(DecimalCodecWarning::Overflow));
        }
        return (
            words_int,
            CODEC_WORD_BUF_LEN - words_int,
            Some(DecimalCodecWarning::Truncated),
        );
    }
    (words_int, words_frac, None)
}

/// Go `writeWord`: big-endian store of the low `size` bytes of `word`.
fn write_word(b: &mut [u8], word: i32, size: usize) {
    let v = word as u32;
    match size {
        1 => b[0] = word as u8,
        2 => {
            b[0] = (v >> 8) as u8;
            b[1] = v as u8;
        }
        3 => {
            b[0] = (v >> 16) as u8;
            b[1] = (v >> 8) as u8;
            b[2] = v as u8;
        }
        4 => {
            b[0] = (v >> 24) as u8;
            b[1] = (v >> 16) as u8;
            b[2] = (v >> 8) as u8;
            b[3] = v as u8;
        }
        _ => {}
    }
}

/// Go `readWord`: sign-extending big-endian load of a `size`-byte word.
fn read_word(b: &[u8], size: usize) -> i32 {
    match size {
        1 => i32::from(b[0] as i8),
        2 => (i32::from(b[0] as i8) << 8) + i32::from(b[1]),
        3 => {
            if b[0] & 128 > 0 {
                (0xFF00_0000u32
                    | (u32::from(b[0]) << 16)
                    | (u32::from(b[1]) << 8)
                    | u32::from(b[2])) as i32
            } else {
                ((u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2])) as i32
            }
        }
        4 => {
            i32::from(b[3])
                + (i32::from(b[2]) << 8)
                + (i32::from(b[1]) << 16)
                + (i32::from(b[0] as i8) << 24)
        }
        _ => 0,
    }
}

/// Go `countLeadingZeroes(i, word)`: leading zero decimal digits of `word` when
/// laid out as `i+1` digits.
fn count_leading_zeroes(mut i: usize, word: i32) -> usize {
    let mut leading = 0;
    while word < CODEC_POWERS10[i] {
        i -= 1;
        leading += 1;
    }
    leading
}

/// Go `DecimalBinSize`: byte length of the fixed-length binary for
/// `{precision, frac}`, independent of any particular value.
pub fn decimal_bin_size(precision: i32, frac: i32) -> Result<usize, DecimalCodecError> {
    let digits_int = precision - frac;
    let words_int = digits_int / DIGITS_PER_WORD as i32;
    let words_frac = frac / DIGITS_PER_WORD as i32;
    let x_int = digits_int - words_int * DIGITS_PER_WORD as i32;
    let x_frac = frac - words_frac * DIGITS_PER_WORD as i32;
    if x_int < 0
        || x_int >= DIG2BYTES.len() as i32
        || x_frac < 0
        || x_frac >= DIG2BYTES.len() as i32
    {
        return Err(DecimalCodecError::BadNumber);
    }
    Ok(words_int as usize * CODEC_WORD_SIZE
        + DIG2BYTES[x_int as usize]
        + words_frac as usize * CODEC_WORD_SIZE
        + DIG2BYTES[x_frac as usize])
}

/// Go `MyDecimal`'s codec-relevant view: sign, integer/fraction digit counts,
/// and the base-1e9 word buffer, built from a [`Decimal`] exactly as Go
/// `FromString` builds it so the ported `ToBin` is line-for-line.
pub(super) struct MyDecimalWords {
    pub(super) negative: bool,
    pub(super) digits_int: i32,
    pub(super) digits_frac: i32,
    pub(super) word_buf: [i32; CODEC_WORD_BUF_LEN],
}

impl MyDecimalWords {
    /// Mirrors Go `FromString`'s `wordBuf` population: integer digits packed
    /// most-significant-word-first, the trailing partial fraction word
    /// left-aligned into the high digit positions, and the nine-word clamp.
    pub(super) fn from_decimal(d: &Decimal) -> Self {
        let digits = d.digits.as_bytes();
        let storage_scale = d.storage_scale as usize;
        // The Rust coefficient is left-padded to at least the storage scale, so
        // the integer digit count is the non-fraction remainder.
        let mut digits_int = digits.len() - storage_scale;
        let mut digits_frac = storage_scale;

        let words_int0 = digits_to_words(digits_int);
        let words_frac0 = digits_to_words(digits_frac);
        let (words_int, words_frac, warn) = fix_word_cnt_error(words_int0, words_frac0);
        if warn.is_some() {
            digits_frac = words_frac * DIGITS_PER_WORD;
            if warn == Some(DecimalCodecWarning::Overflow) {
                digits_int = words_int * DIGITS_PER_WORD;
            }
        }

        let mut word_buf = [0i32; CODEC_WORD_BUF_LEN];

        // Integer part: read the integer prefix right-to-left into base-1e9
        // words, filling word_buf[words_int-1] down to word_buf[0].
        let mut word_idx = words_int;
        let mut word: i32 = 0;
        let mut inner = 0usize;
        let mut remaining = digits_int;
        let mut si = digits_int;
        while remaining > 0 {
            remaining -= 1;
            si -= 1;
            word += i32::from(digits[si] - b'0') * CODEC_POWERS10[inner];
            inner += 1;
            if inner == DIGITS_PER_WORD {
                word_idx -= 1;
                word_buf[word_idx] = word;
                word = 0;
                inner = 0;
            }
        }
        if inner != 0 {
            word_idx -= 1;
            word_buf[word_idx] = word;
        }

        // Fraction part: read the fraction digits left-to-right; the final
        // partial word is left-aligned into the high digit positions.
        word_idx = words_int;
        word = 0;
        inner = 0;
        remaining = digits_frac;
        let mut fi = digits_int;
        while remaining > 0 {
            remaining -= 1;
            word = i32::from(digits[fi] - b'0') + word * 10;
            fi += 1;
            inner += 1;
            if inner == DIGITS_PER_WORD {
                word_buf[word_idx] = word;
                word_idx += 1;
                word = 0;
                inner = 0;
            }
        }
        if inner != 0 {
            word_buf[word_idx] = word * CODEC_POWERS10[DIGITS_PER_WORD - inner];
        }

        MyDecimalWords {
            negative: d.negative,
            digits_int: digits_int as i32,
            digits_frac: digits_frac as i32,
            word_buf,
        }
    }

    /// Reconstructs a normalized [`Decimal`] from the word view, extracting the
    /// coefficient digits exactly as Go `ToString` reads them: integer words
    /// least-significant-first (each digit via `/10`), fraction words
    /// most-significant-first (each digit via `/1e8`, left-aligned). The Rust
    /// `Decimal` re-normalizes and `Display` re-inserts the point/sign, matching
    /// Go `ToString`.
    pub(super) fn to_decimal(&self) -> Decimal {
        let (word_start_idx, digits_int) = self.remove_leading_zeros();
        let digits_frac = self.digits_frac;

        // Build one coefficient buffer. The previous implementation allocated
        // separate integer/fraction vectors and then copied both into a
        // String; that turns every DECIMAL cell into several heap operations.
        // Go's MyDecimal already owns a fixed word buffer, so keep the Rust
        // representation to one final coefficient allocation as well.
        let int_len = digits_int.max(0) as usize;
        let fraction_len = digits_frac.max(0) as usize;
        let mut coefficient = SmallVec::<[u8; INLINE_DECIMAL_DIGITS]>::new();
        coefficient.resize(int_len + fraction_len, b'0');
        if digits_int > 0 {
            let mut pos = int_len;
            let mut word_idx = word_start_idx + digits_to_words(digits_int as usize);
            let mut remaining = digits_int;
            while remaining > 0 {
                word_idx -= 1;
                let mut x = self.word_buf[word_idx];
                let take = remaining.min(DIGITS_PER_WORD as i32);
                for _ in 0..take {
                    let y = x / 10;
                    pos -= 1;
                    coefficient[pos] = b'0' + (x - y * 10) as u8;
                    x = y;
                }
                remaining -= DIGITS_PER_WORD as i32;
            }
        }

        // Fraction coefficient digits, built left-to-right like Go `ToString`.
        if digits_frac > 0 {
            let dig_mask = CODEC_POWERS10[DIGITS_PER_WORD - 1]; // ten8 = 10^8
            let mut word_idx = word_start_idx + digits_to_words(digits_int.max(0) as usize);
            let mut remaining = digits_frac;
            let mut offset = int_len;
            while remaining > 0 {
                let mut x = self.word_buf[word_idx];
                word_idx += 1;
                let take = remaining.min(DIGITS_PER_WORD as i32);
                for _ in 0..take {
                    let y = x / dig_mask;
                    coefficient[offset] = b'0' + y as u8;
                    offset += 1;
                    x -= y * dig_mask;
                    x *= 10;
                }
                remaining -= DIGITS_PER_WORD as i32;
            }
        }

        if coefficient.is_empty() {
            coefficient.push(b'0');
        }
        Decimal::new_with_storage(
            self.negative,
            DecimalDigits::from_ascii(coefficient),
            digits_frac.max(0) as u32,
            digits_frac.max(0) as u32,
        )
    }

    /// Go `removeLeadingZeros`: index of the first significant word and the
    /// count of significant integer digits.
    fn remove_leading_zeros(&self) -> (usize, i32) {
        let mut digits_int = self.digits_int;
        let mut word_idx = 0usize;
        // Go truncated modulo (Rust `%` matches); the value is unused when the
        // loop body never runs (digits_int <= 0).
        let mut i = ((digits_int - 1) % DIGITS_PER_WORD as i32) + 1;
        while digits_int > 0 && self.word_buf[word_idx] == 0 {
            digits_int -= i;
            i = DIGITS_PER_WORD as i32;
            word_idx += 1;
        }
        if digits_int > 0 {
            let start = ((digits_int - 1) % DIGITS_PER_WORD as i32) as usize;
            digits_int -= count_leading_zeroes(start, self.word_buf[word_idx]) as i32;
        } else {
            digits_int = 0;
        }
        (word_idx, digits_int)
    }
}

impl Decimal {
    /// Faithful port of Go `MyDecimal.ToBin`/`WriteBin`: the fixed-length,
    /// memcmp-comparable binary encoding at `{precision, frac}`. Returns the
    /// bytes plus any soft truncation/overflow, or [`DecimalCodecError`] for an
    /// illegal `{precision, frac}`.
    pub fn to_bin(
        &self,
        precision: i32,
        frac: i32,
    ) -> Result<(Vec<u8>, Option<DecimalCodecWarning>), DecimalCodecError> {
        if !(0..=(DIGITS_PER_WORD * CODEC_WORD_BUF_LEN) as i32).contains(&precision)
            || !(0..=CODEC_MAX_DECIMAL_SCALE).contains(&frac)
        {
            return Err(DecimalCodecError::BadNumber);
        }
        let d = MyDecimalWords::from_decimal(self);
        let mut warning: Option<DecimalCodecWarning> = None;
        let mut mask: i32 = if d.negative { -1 } else { 0 };

        let mut digits_int: i32 = precision - frac;
        let words_int = (digits_int / DIGITS_PER_WORD as i32) as usize;
        let leading_digits = (digits_int - words_int as i32 * DIGITS_PER_WORD as i32) as usize;
        let words_frac = (frac / DIGITS_PER_WORD as i32) as usize;
        let trailing_digits = (frac - words_frac as i32 * DIGITS_PER_WORD as i32) as usize;

        let words_frac_from0 = (d.digits_frac / DIGITS_PER_WORD as i32) as usize;
        let trailing_digits_from0 =
            (d.digits_frac - words_frac_from0 as i32 * DIGITS_PER_WORD as i32) as usize;

        let mut int_size = words_int * CODEC_WORD_SIZE + DIG2BYTES[leading_digits];
        let mut frac_size = words_frac * CODEC_WORD_SIZE + DIG2BYTES[trailing_digits];
        let frac_size_from = words_frac_from0 * CODEC_WORD_SIZE + DIG2BYTES[trailing_digits_from0];
        let origin_int_size = int_size;
        let origin_frac_size = frac_size;

        let mut bin = vec![0u8; int_size + frac_size];
        let mut bin_idx = 0usize;

        let (word_idx_from0, digits_int_from) = d.remove_leading_zeros();
        let mut word_idx_from: i64 = word_idx_from0 as i64;
        if digits_int_from + frac_size_from as i32 == 0 {
            mask = 0;
            digits_int = 1;
        }

        let mut words_int_from: i64 = (digits_int_from / DIGITS_PER_WORD as i32) as i64;
        let mut leading_digits_from =
            (digits_int_from - words_int_from as i32 * DIGITS_PER_WORD as i32) as usize;
        let i_size_from =
            words_int_from as usize * CODEC_WORD_SIZE + DIG2BYTES[leading_digits_from];

        let mut words_frac_from = words_frac_from0;
        let mut trailing_digits_from = trailing_digits_from0;

        if digits_int < digits_int_from {
            word_idx_from += words_int_from - words_int as i64;
            if leading_digits_from > 0 {
                word_idx_from += 1;
            }
            if leading_digits > 0 {
                word_idx_from -= 1;
            }
            words_int_from = words_int as i64;
            leading_digits_from = leading_digits;
            warning = Some(DecimalCodecWarning::Overflow);
        } else if int_size > i_size_from {
            while int_size > i_size_from {
                int_size -= 1;
                bin[bin_idx] = mask as u8;
                bin_idx += 1;
            }
        }

        if frac_size < frac_size_from
            || (frac_size == frac_size_from
                && (trailing_digits <= trailing_digits_from || words_frac <= words_frac_from))
        {
            if frac_size < frac_size_from
                || (frac_size == frac_size_from && trailing_digits < trailing_digits_from)
                || (frac_size == frac_size_from && words_frac < words_frac_from)
            {
                warning = Some(DecimalCodecWarning::Truncated);
            }
            words_frac_from = words_frac;
            trailing_digits_from = trailing_digits;
        } else if frac_size > frac_size_from && trailing_digits_from > 0 {
            if words_frac == words_frac_from {
                trailing_digits_from = trailing_digits;
                frac_size = frac_size_from;
            } else {
                words_frac_from += 1;
                trailing_digits_from = 0;
            }
        }

        // xIntFrom part: the leading partial integer word.
        if leading_digits_from > 0 {
            let i = DIG2BYTES[leading_digits_from];
            let x =
                (d.word_buf[word_idx_from as usize] % CODEC_POWERS10[leading_digits_from]) ^ mask;
            word_idx_from += 1;
            write_word(&mut bin[bin_idx..], x, i);
            bin_idx += i;
        }

        // wordsInt + wordsFrac full words.
        let stop = word_idx_from + words_int_from + words_frac_from as i64;
        while word_idx_from < stop {
            let x = d.word_buf[word_idx_from as usize] ^ mask;
            word_idx_from += 1;
            write_word(&mut bin[bin_idx..], x, CODEC_WORD_SIZE);
            bin_idx += CODEC_WORD_SIZE;
        }

        // xFracFrom part: the trailing partial fraction word.
        if trailing_digits_from > 0 {
            let i = DIG2BYTES[trailing_digits_from];
            let mut lim = trailing_digits;
            if words_frac_from < words_frac {
                lim = DIGITS_PER_WORD;
            }
            let mut tdf = trailing_digits_from;
            while tdf < lim && DIG2BYTES[tdf] == i {
                tdf += 1;
            }
            let x =
                (d.word_buf[word_idx_from as usize] / CODEC_POWERS10[DIGITS_PER_WORD - tdf]) ^ mask;
            write_word(&mut bin[bin_idx..], x, i);
            bin_idx += i;
        }

        if frac_size > frac_size_from {
            let bin_idx_end = origin_int_size + origin_frac_size;
            while frac_size > frac_size_from && bin_idx < bin_idx_end {
                frac_size -= 1;
                bin[bin_idx] = mask as u8;
                bin_idx += 1;
            }
        }
        bin[0] ^= 0x80;
        Ok((bin, warning))
    }

    /// Faithful port of Go `MyDecimal.FromBin`: decodes the fixed-length binary
    /// produced by [`Self::to_bin`] at `{precision, frac}` back into a
    /// [`Decimal`], returning it, the number of bytes consumed, and any soft
    /// truncation/overflow. A hard [`DecimalCodecError`] signals an illegal
    /// `{precision, frac}`, an oversized layout, or a corrupt payload.
    pub fn from_bin(
        bin: &[u8],
        precision: i32,
        frac: i32,
    ) -> Result<(Decimal, usize, Option<DecimalCodecWarning>), DecimalCodecError> {
        if bin.is_empty() {
            return Err(DecimalCodecError::BadNumber);
        }
        let digits_int = precision - frac;
        let words_int = digits_int / DIGITS_PER_WORD as i32;
        let leading_digits = digits_int - words_int * DIGITS_PER_WORD as i32;
        let mut words_frac = frac / DIGITS_PER_WORD as i32;
        let mut trailing_digits = frac - words_frac * DIGITS_PER_WORD as i32;
        let mut words_int_to = words_int;
        if leading_digits > 0 {
            words_int_to += 1;
        }
        let mut words_frac_to = words_frac;
        if trailing_digits > 0 {
            words_frac_to += 1;
        }

        // Sign lives in the top bit of the first byte (0 => negative).
        let mask: i32 = if bin[0] & 0x80 > 0 { 0 } else { -1 };
        let bin_size = decimal_bin_size(precision, frac)?;
        if bin_size > 40 {
            return Err(DecimalCodecError::BadNumber);
        }

        // Private copy with the sign bit restored (Go pads to 40 then slices;
        // only [0..bin_size] is ever read). Keep this fixed-size buffer on the
        // stack: DecodeDecimal is on the hot row-response path and the Go
        // MyDecimal decoder does not allocate a payload-sized buffer.
        let mut buf = [0u8; 40];
        let n = bin.len().min(bin_size);
        buf[..n].copy_from_slice(&bin[..n]);
        buf[0] ^= 0x80;

        let mut bin_idx = 0usize;
        let mut warning: Option<DecimalCodecWarning> = None;
        let old_words_int_to = words_int_to;
        let (fixed_int, fixed_frac, warn) =
            fix_word_cnt_error(words_int_to as usize, words_frac_to as usize);
        words_int_to = fixed_int as i32;
        words_frac_to = fixed_frac as i32;
        if warn.is_some() {
            warning = warn;
            if words_int_to < old_words_int_to {
                bin_idx += DIG2BYTES[leading_digits as usize]
                    + (words_int - words_int_to) as usize * CODEC_WORD_SIZE;
            } else {
                trailing_digits = 0;
                words_frac = words_frac_to;
            }
        }

        let mut w = MyDecimalWords {
            negative: mask != 0,
            digits_int: words_int * DIGITS_PER_WORD as i32 + leading_digits,
            digits_frac: words_frac * DIGITS_PER_WORD as i32 + trailing_digits,
            word_buf: [0i32; CODEC_WORD_BUF_LEN],
        };

        let mut word_idx = 0usize;
        if leading_digits > 0 {
            let i = DIG2BYTES[leading_digits as usize];
            let x = read_word(&buf[bin_idx..], i);
            bin_idx += i;
            w.word_buf[word_idx] = x ^ mask;
            if u64::from(w.word_buf[word_idx] as u32)
                >= u64::from(CODEC_POWERS10[leading_digits as usize + 1] as u32)
            {
                return Err(DecimalCodecError::BadNumber);
            }
            if word_idx > 0 || w.word_buf[word_idx] != 0 {
                word_idx += 1;
            } else {
                w.digits_int -= leading_digits;
            }
        }

        let stop = bin_idx + words_int as usize * CODEC_WORD_SIZE;
        while bin_idx < stop {
            w.word_buf[word_idx] = read_word(&buf[bin_idx..], CODEC_WORD_SIZE) ^ mask;
            if w.word_buf[word_idx] as u32 > CODEC_WORD_MAX as u32 {
                return Err(DecimalCodecError::BadNumber);
            }
            if word_idx > 0 || w.word_buf[word_idx] != 0 {
                word_idx += 1;
            } else {
                w.digits_int -= DIGITS_PER_WORD as i32;
            }
            bin_idx += CODEC_WORD_SIZE;
        }

        let stop = bin_idx + words_frac as usize * CODEC_WORD_SIZE;
        while bin_idx < stop {
            w.word_buf[word_idx] = read_word(&buf[bin_idx..], CODEC_WORD_SIZE) ^ mask;
            if w.word_buf[word_idx] as u32 > CODEC_WORD_MAX as u32 {
                return Err(DecimalCodecError::BadNumber);
            }
            word_idx += 1;
            bin_idx += CODEC_WORD_SIZE;
        }

        if trailing_digits > 0 {
            let i = DIG2BYTES[trailing_digits as usize];
            let x = read_word(&buf[bin_idx..], i);
            w.word_buf[word_idx] =
                (x ^ mask) * CODEC_POWERS10[DIGITS_PER_WORD - trailing_digits as usize];
            if w.word_buf[word_idx] as u32 > CODEC_WORD_MAX as u32 {
                return Err(DecimalCodecError::BadNumber);
            }
        }

        Ok((w.to_decimal(), bin_size, warning))
    }

    /// Go `MyDecimal.MarshalJSON`'s exact persistence object.
    pub fn mysql_json_value(&self) -> serde_json::Value {
        let words = MyDecimalWords::from_decimal(self);
        let mut object = serde_json::Map::new();
        object.insert(
            "DigitsInt".to_owned(),
            serde_json::Value::from(words.digits_int),
        );
        object.insert(
            "DigitsFrac".to_owned(),
            serde_json::Value::from(words.digits_frac),
        );
        object.insert("ResultFrac".to_owned(), serde_json::Value::from(self.scale));
        object.insert(
            "Negative".to_owned(),
            serde_json::Value::from(words.negative),
        );
        object.insert(
            "WordBuf".to_owned(),
            serde_json::Value::Array(
                words
                    .word_buf
                    .into_iter()
                    .map(serde_json::Value::from)
                    .collect(),
            ),
        );
        serde_json::Value::Object(object)
    }

    /// Go `MyDecimal.UnmarshalJSON`'s persistence object decoder.
    pub fn from_mysql_json_value(value: &serde_json::Value) -> Result<Self, String> {
        let object = value
            .as_object()
            .ok_or_else(|| "MyDecimal JSON must be an object".to_owned())?;
        let read_i32 = |name: &str| {
            object
                .get(name)
                .and_then(serde_json::Value::as_i64)
                .and_then(|value| i32::try_from(value).ok())
                .ok_or_else(|| format!("MyDecimal JSON is missing {name}"))
        };
        let digits_int = read_i32("DigitsInt")?;
        let digits_frac = read_i32("DigitsFrac")?;
        let result_frac = read_i32("ResultFrac")?;
        if digits_int < 0 || digits_frac < 0 || result_frac < 0 {
            return Err("MyDecimal JSON contains negative metadata".to_owned());
        }
        let negative = object
            .get("Negative")
            .and_then(serde_json::Value::as_bool)
            .ok_or_else(|| "MyDecimal JSON is missing Negative".to_owned())?;
        let encoded_words = object
            .get("WordBuf")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| "MyDecimal JSON is missing WordBuf".to_owned())?;
        if encoded_words.len() != CODEC_WORD_BUF_LEN {
            return Err("MyDecimal JSON WordBuf must contain nine words".to_owned());
        }
        let mut word_buf = [0; CODEC_WORD_BUF_LEN];
        for (output, encoded) in word_buf.iter_mut().zip(encoded_words) {
            *output = encoded
                .as_i64()
                .and_then(|value| i32::try_from(value).ok())
                .ok_or_else(|| "MyDecimal JSON contains an invalid word".to_owned())?;
        }
        let raw = MyDecimalWords {
            negative,
            digits_int,
            digits_frac,
            word_buf,
        }
        .to_decimal();
        let result_frac = result_frac as u32;
        let storage_scale = raw.storage_scale.max(result_frac);
        let digits = pad_scale(&raw.digits, raw.storage_scale, storage_scale);
        Ok(Decimal::new_with_storage(
            negative,
            digits,
            result_frac,
            storage_scale,
        ))
    }
}
