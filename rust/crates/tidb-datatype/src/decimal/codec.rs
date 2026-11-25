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
use crate::mydecimal::{parse_decimal_group, MyDecimal};

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
const MAX_U64_COEFFICIENT_DIGITS: usize = 19;
const CODEC_WORD_SIZE: usize = 4;
pub(super) const CODEC_WORD_BUF_LEN: usize = 9;
/// Largest value one 1e9 word holds (Go `wordMax` = `wordBase - 1`).
const CODEC_WORD_MAX: i32 = 999_999_999;
/// `mysql.MaxDecimalScale`.
pub(super) const CODEC_MAX_DECIMAL_SCALE: i32 = 30;
/// Bytes needed to store `k` decimal digits packed into one partial word.
const DIG2BYTES: [usize; 10] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];
static DIGIT_TRIPLETS: [[u8; 3]; 1000] = {
    let mut triplets = [[0; 3]; 1000];
    let mut index = 0;
    while index < triplets.len() {
        triplets[index] = [
            b'0' + (index / 100) as u8,
            b'0' + (index / 10 % 10) as u8,
            b'0' + (index % 10) as u8,
        ];
        index += 1;
    }
    triplets
};
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

/// Failure state returned by [`Decimal::from_bin_with_failure`].
///
/// Go's `MyDecimal.FromBin` mutates its receiver to the zero value before
/// returning `ErrBadNumber` for a corrupt payload, while still returning the
/// legal fixed payload length. Keeping those values beside the error lets row
/// decoders make the same cursor-progress decision without weakening the
/// existing [`Decimal::from_bin`] API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecimalCodecFailure {
    /// Go's post-error receiver state (`zeroMyDecimal`).
    pub value: Decimal,
    /// Fixed payload bytes consumed/available according to the requested
    /// precision and scale. Zero means the shape itself was invalid.
    pub consumed: usize,
    /// The underlying hard codec error.
    pub error: DecimalCodecError,
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
        4 => i32::from_be_bytes(*b.first_chunk().expect("four decimal word bytes")),
        _ => 0,
    }
}

#[inline]
fn read_bin_word(bin: &[u8], offset: usize, size: usize, mask: i32) -> i32 {
    // Restoring the first byte's high bit also changes that word's sign
    // extension when it occupies fewer than four bytes.
    let sign_bit = if offset == 0 {
        i32::MIN >> (32 - size * 8)
    } else {
        0
    };
    read_word(&bin[offset..], size) ^ mask ^ sign_bit
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

/// Writes all nine digits of a packed word, including leading zeros.
#[inline]
fn write_word_digits(word: u32, output: &mut [u8; DIGITS_PER_WORD]) {
    let head = word / 1_000_000;
    let middle = word / 1000 % 1000;
    let tail = word % 1000;
    output[..3].copy_from_slice(&DIGIT_TRIPLETS[head as usize]);
    output[3..6].copy_from_slice(&DIGIT_TRIPLETS[middle as usize]);
    output[6..].copy_from_slice(&DIGIT_TRIPLETS[tail as usize]);
}

/// All coefficients with at most nineteen digits fit in one u64. Combine the
/// validated words first, then format a single integer with the required scale.
#[inline]
fn decode_small_coefficient(
    bin: &[u8],
    leading_digits: usize,
    trailing_digits: usize,
    scale: usize,
    mask: i32,
) -> Option<Decimal> {
    let leading_width = DIG2BYTES[leading_digits];
    let trailing_width = DIG2BYTES[trailing_digits];
    let full_words_end = bin.len() - trailing_width;
    let mut coefficient = 0_u64;
    if leading_width != 0 {
        let word = read_bin_word(bin, 0, leading_width, mask) as u32;
        if word >= CODEC_POWERS10[leading_digits] as u32 {
            return None;
        }
        coefficient = u64::from(word);
    }

    let (words, _) = bin[leading_width..full_words_end].as_chunks::<CODEC_WORD_SIZE>();
    let mut word_mask = if leading_width == 0 { mask ^ i32::MIN } else { mask };
    for &bytes in words {
        let word = (i32::from_be_bytes(bytes) ^ word_mask) as u32;
        word_mask = mask;
        if word > CODEC_WORD_MAX as u32 {
            return None;
        }
        coefficient = coefficient * 1_000_000_000 + u64::from(word);
    }
    if trailing_width != 0 {
        let word = read_bin_word(bin, full_words_end, trailing_width, mask) as u32;
        let power = CODEC_POWERS10[trailing_digits] as u32;
        if word >= power {
            return None;
        }
        coefficient = coefficient * u64::from(power) + u64::from(word);
    }

    let mut formatted = itoa::Buffer::new();
    let text = formatted.format(coefficient).as_bytes();
    let digits = if text.len() < scale {
        let mut padded = [b'0'; MAX_U64_COEFFICIENT_DIGITS];
        padded[scale - text.len()..scale].copy_from_slice(text);
        SmallVec::from_slice(&padded[..scale])
    } else {
        SmallVec::from_slice(text)
    };
    Some(Decimal {
        negative: mask != 0 && (coefficient != 0 || scale != 0),
        digits: DecimalDigits::from_ascii(digits),
        scale: scale as u32,
        storage_scale: scale as u32,
        declared_shape: None,
    })
}

/// Decodes a complete, canonical binary payload directly into its coefficient.
fn decode_coefficient(
    bin: &[u8],
    integer_digits: usize,
    fraction_digits: usize,
    leading_digits: usize,
    trailing_digits: usize,
    mask: i32,
) -> Option<Decimal> {
    let leading_width = DIG2BYTES[leading_digits];
    let trailing_width = DIG2BYTES[trailing_digits];
    let full_words_end = bin.len() - trailing_width;
    let precision = integer_digits + fraction_digits;
    let mut digits = [b'0'; CODEC_WORD_BUF_LEN * DIGITS_PER_WORD];
    let mut formatted = itoa::Buffer::new();

    if leading_digits != 0 {
        let word = read_bin_word(bin, 0, leading_width, mask) as u32;
        if word >= CODEC_POWERS10[leading_digits] as u32 {
            return None;
        }
        if word != 0 {
            let text = formatted.format(word).as_bytes();
            digits[leading_digits - text.len()..leading_digits].copy_from_slice(text);
        }
    }

    let (words, _) = bin[leading_width..full_words_end].as_chunks::<CODEC_WORD_SIZE>();
    let (groups, _) =
        digits[leading_digits..precision - trailing_digits].as_chunks_mut::<DIGITS_PER_WORD>();
    let mut word_mask = if leading_width == 0 {
        mask ^ i32::MIN
    } else {
        mask
    };
    for (&bytes, group) in words.iter().zip(groups) {
        let word = (i32::from_be_bytes(bytes) ^ word_mask) as u32;
        word_mask = mask;
        if word > CODEC_WORD_MAX as u32 {
            return None;
        }
        if word != 0 {
            write_word_digits(word, group);
        }
    }

    if trailing_digits != 0 {
        let word = read_bin_word(bin, full_words_end, trailing_width, mask) as u32;
        if word >= CODEC_POWERS10[trailing_digits] as u32 {
            return None;
        }
        if word != 0 {
            let text = formatted.format(word).as_bytes();
            digits[precision - text.len()..precision].copy_from_slice(text);
        }
    }

    let first = digits[..integer_digits]
        .iter()
        .position(|&digit| digit != b'0')
        .unwrap_or(integer_digits);
    let coefficient = &digits[first..precision];
    Some(Decimal {
        negative: mask != 0 && !coefficient.is_empty(),
        digits: DecimalDigits::from_ascii(SmallVec::from_slice(if coefficient.is_empty() {
            b"0"
        } else {
            coefficient
        })),
        scale: fraction_digits as u32,
        storage_scale: fraction_digits as u32,
        declared_shape: None,
    })
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
    let size = words_int * CODEC_WORD_SIZE as i32
        + DIG2BYTES[x_int as usize] as i32
        + words_frac * CODEC_WORD_SIZE as i32
        + DIG2BYTES[x_frac as usize] as i32;
    usize::try_from(size).map_err(|_| DecimalCodecError::BadNumber)
}

/// Go `MyDecimal`'s codec-relevant view: sign, integer/fraction digit counts,
/// and the base-1e9 word buffer, built from a [`Decimal`] exactly as Go
/// `FromString` builds it so the ported `ToBin` is line-for-line.
pub(crate) struct MyDecimalWords {
    pub(crate) negative: bool,
    pub(crate) digits_int: i32,
    pub(crate) digits_frac: i32,
    pub(crate) word_buf: [i32; CODEC_WORD_BUF_LEN],
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
        let source_digits_int = digits.len() - storage_scale;
        let mut digits_int = source_digits_int;
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

        // Retain the low integer words and the high fractional words when
        // an input fills the nine-word buffer.
        let integer = &digits[source_digits_int - digits_int..source_digits_int];
        for (word, group) in word_buf[..words_int]
            .iter_mut()
            .rev()
            .zip(integer.rchunks(DIGITS_PER_WORD))
        {
            *word = parse_decimal_group(group);
        }

        let fraction = &digits[source_digits_int..source_digits_int + digits_frac];
        for (word, group) in word_buf[words_int..]
            .iter_mut()
            .zip(fraction.chunks(DIGITS_PER_WORD))
        {
            *word = parse_decimal_group(group) * CODEC_POWERS10[DIGITS_PER_WORD - group.len()];
        }

        MyDecimalWords {
            negative: d.negative,
            digits_int: digits_int as i32,
            digits_frac: digits_frac as i32,
            word_buf,
        }
    }

    /// Reconstructs a normalized [`Decimal`] by writing coefficient digits
    /// word by word. Fraction words are left-aligned.
    pub(super) fn to_decimal(&self) -> Decimal {
        let (word_start_idx, digits_int) = self.remove_leading_zeros();
        let digits_frac = self.digits_frac;
        let int_len = digits_int.max(0) as usize;
        let fraction_len = digits_frac.max(0) as usize;
        let mut coefficient =
            SmallVec::<[u8; INLINE_DECIMAL_DIGITS]>::from_elem(b'0', int_len + fraction_len);
        let (integer, fraction) = coefficient.split_at_mut(int_len);
        let mut buffer = itoa::Buffer::new();

        let int_words = digits_to_words(int_len);
        let (int_buf, frac_buf) = self.word_buf[word_start_idx..].split_at(int_words);
        // The integer's leading partial word can be shorter than a full word.
        for (&word, digits) in int_buf.iter().rev().zip(integer.rchunks_mut(DIGITS_PER_WORD)) {
            if word > 0 {
                let text = buffer.format(word as u32).as_bytes();
                let count = text.len().min(digits.len());
                // A partial integer may carry extra high digits in an accepted
                // noncanonical wire value. Keep the low digits, as before.
                let start = digits.len() - count;
                digits[start..].copy_from_slice(&text[text.len() - count..]);
            } else if word < 0 {
                let mut remaining = word;
                for digit in digits.iter_mut().rev() {
                    let next = remaining / 10;
                    *digit = b'0' + (remaining - next * 10) as u8;
                    remaining = next;
                }
            }
        }

        // Fraction words are left-aligned, including the last partial word.
        for (&word, digits) in frac_buf[..digits_to_words(fraction_len)]
            .iter()
            .zip(fraction.chunks_mut(DIGITS_PER_WORD))
        {
            if word != 0 {
                let text = buffer.format(word as u32).as_bytes();
                if let Some(leading_zeros) = DIGITS_PER_WORD.checked_sub(text.len()) {
                    if let Some(rest) = digits.get_mut(leading_zeros..) {
                        rest.copy_from_slice(&text[..rest.len()]);
                    }
                } else {
                    // The legacy JSON representation exposes the raw words.
                    let mut remaining = word;
                    let divisor = CODEC_POWERS10[DIGITS_PER_WORD - 1];
                    for digit in digits {
                        let next = remaining / divisor;
                        *digit = b'0' + next as u8;
                        remaining = (remaining - next * divisor) * 10;
                    }
                }
            }
        }

        if coefficient.is_empty() {
            coefficient.push(b'0');
        }
        let digits = DecimalDigits::from_ascii(coefficient);
        let scale = digits_frac.max(0) as u32;
        if digits_frac > 0 {
            Decimal::new_with_storage_preserving_zero_sign(self.negative, digits, scale, scale)
        } else {
            // Go `FromBin` resets to `zeroMyDecimal` only when both digit
            // counts are zero, clearing a non-canonical sign at scale zero.
            Decimal::new_with_storage(self.negative, digits, scale, scale)
        }
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

impl MyDecimal {
    /// Decodes Go's fixed-width decimal payload into base-1e9 words.
    ///
    /// Returns the packed value, the byte count, and Go's soft codec warning.
    #[inline]
    pub fn from_bin(
        bin: &[u8],
        precision: i32,
        frac: i32,
    ) -> Result<(Self, usize, Option<DecimalCodecWarning>), DecimalCodecError> {
        if bin.is_empty() {
            return Err(DecimalCodecError::BadNumber);
        }
        let (words, consumed, warning) = Decimal::decode_bin_words(bin, precision, frac)
            .map_err(|_| DecimalCodecError::BadNumber)?;
        Ok((Self::from_bin_words(words, frac), consumed, warning))
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
        let mut bin = vec![0u8; checked_bin_size(precision, frac)?];
        let warning = MyDecimalWords::from_decimal(self).write_bin(precision, frac, &mut bin)?;
        Ok((bin, warning))
    }

    /// Appends the fixed-length binary form to an existing buffer, like Go
    /// `MyDecimal.WriteBin`. The payload is appended even on a soft warning.
    pub fn write_bin(
        &self,
        precision: i32,
        frac: i32,
        buffer: &mut Vec<u8>,
    ) -> Result<Option<DecimalCodecWarning>, DecimalCodecError> {
        let size = checked_bin_size(precision, frac)?;
        let start = buffer.len();
        buffer.resize(start + size, 0);
        MyDecimalWords::from_decimal(self).write_bin(precision, frac, &mut buffer[start..])
    }
}

/// Go `ToBin`'s legality check plus `DecimalBinSize`: the byte length of the
/// encoding at `{precision, frac}`, or `ErrBadNumber` for a shape Go rejects.
pub(crate) fn checked_bin_size(precision: i32, frac: i32) -> Result<usize, DecimalCodecError> {
    if !(0..=(DIGITS_PER_WORD * CODEC_WORD_BUF_LEN) as i32).contains(&precision)
        || !(0..=CODEC_MAX_DECIMAL_SCALE).contains(&frac)
    {
        return Err(DecimalCodecError::BadNumber);
    }
    decimal_bin_size(precision, frac)
}

impl MyDecimalWords {
    /// Go `MyDecimal.WriteBin`: encodes the words at `{precision, frac}` into
    /// `bin`, which the caller has sized with [`checked_bin_size`]. Returns
    /// the soft truncation/overflow Go reports beside the bytes.
    pub(crate) fn write_bin(
        &self,
        precision: i32,
        frac: i32,
        bin: &mut [u8],
    ) -> Result<Option<DecimalCodecWarning>, DecimalCodecError> {
        let d = self;
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

        debug_assert_eq!(bin.len(), int_size + frac_size);
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
        Ok(warning)
    }
}

impl Decimal {
    /// Faithful port of Go `MyDecimal.FromBin`: decodes the fixed-length binary
    /// produced by [`Self::to_bin`] at `{precision, frac}` back into a
    /// [`Decimal`], returning it, the number of bytes consumed, and any soft
    /// truncation/overflow. A hard [`DecimalCodecError`] signals an illegal
    /// `{precision, frac}`, an oversized layout, or a corrupt payload.
    #[inline]
    pub fn from_bin(
        bin: &[u8],
        precision: i32,
        frac: i32,
    ) -> Result<(Decimal, usize, Option<DecimalCodecWarning>), DecimalCodecError> {
        Self::decode_bin(bin, precision, frac).map_err(|_| DecimalCodecError::BadNumber)
    }

    /// Decodes a binary decimal while retaining Go's post-error receiver and
    /// cursor state. For a corrupt payload with a legal shape, the error value
    /// contains zero and `consumed == DecimalBinSize(precision, frac)`;
    /// malformed shapes and an empty input report `consumed == 0`.
    pub fn from_bin_with_failure(
        bin: &[u8],
        precision: i32,
        frac: i32,
    ) -> Result<(Decimal, usize, Option<DecimalCodecWarning>), DecimalCodecFailure> {
        Self::decode_bin(bin, precision, frac).map_err(|consumed| DecimalCodecFailure {
            value: Decimal::from_literal("0"),
            consumed,
            error: DecimalCodecError::BadNumber,
        })
    }

    // The cursor is the only additional information needed to return Go's
    // failure state, which always has the zero value.
    #[inline]
    fn decode_bin(
        bin: &[u8],
        precision: i32,
        frac: i32,
    ) -> Result<(Decimal, usize, Option<DecimalCodecWarning>), usize> {
        let Some(&sign) = bin.first() else {
            return Err(0);
        };
        if precision >= frac && frac >= 0 {
            let integer_digits = (precision - frac) as usize;
            let fraction_digits = frac as usize;
            let words_int = integer_digits / DIGITS_PER_WORD;
            let words_frac = fraction_digits / DIGITS_PER_WORD;
            let leading = integer_digits % DIGITS_PER_WORD;
            let trailing = fraction_digits % DIGITS_PER_WORD;
            let size = (words_int + words_frac) * CODEC_WORD_SIZE
                + DIG2BYTES[leading]
                + DIG2BYTES[trailing];
            if words_int + words_frac + usize::from(leading != 0) + usize::from(trailing != 0)
                <= CODEC_WORD_BUF_LEN
            {
                let mask = if sign & 0x80 != 0 { 0 } else { -1 };
                if let Some(payload) = bin.get(..size) {
                    let value = if precision <= MAX_U64_COEFFICIENT_DIGITS as i32 {
                        decode_small_coefficient(payload, leading, trailing, fraction_digits, mask)
                    } else {
                        decode_coefficient(
                            payload,
                            integer_digits,
                            fraction_digits,
                            leading,
                            trailing,
                            mask,
                        )
                    };
                    if let Some(value) = value {
                        return Ok((value, size, None));
                    }
                }
            }
        }
        Self::decode_bin_words(bin, precision, frac)
            .map(|(words, consumed, warning)| (words.to_decimal(), consumed, warning))
    }

    // Go's word decoder also handles short payloads, noncanonical partial
    // words, and shapes needing the nine-word adjustment.
    fn decode_bin_words(
        bin: &[u8],
        precision: i32,
        frac: i32,
    ) -> Result<(MyDecimalWords, usize, Option<DecimalCodecWarning>), usize> {
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
        let bin_size = decimal_bin_size(precision, frac).map_err(|_| 0_usize)?;
        if bin_size > 40 {
            return Err(0);
        }

        // Only short or adjusted-width payloads need Go's zero-padded
        // scratch layout. The first word restores the sign bit as it loads.
        let mut padded = [0_u8; 40];
        let buf = if digits_int >= 0
            && frac >= 0
            && words_int_to + words_frac_to <= CODEC_WORD_BUF_LEN as i32
            && bin.len() >= bin_size
        {
            &bin[..bin_size]
        } else {
            let n = bin.len().min(bin_size);
            padded[..n].copy_from_slice(&bin[..n]);
            &padded[..]
        };

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
            w.word_buf[word_idx] = read_bin_word(buf, bin_idx, i, mask);
            bin_idx += i;
            if u64::from(w.word_buf[word_idx] as u32)
                >= u64::from(CODEC_POWERS10[leading_digits as usize + 1] as u32)
            {
                return Err(bin_size);
            }
            if word_idx > 0 || w.word_buf[word_idx] != 0 {
                word_idx += 1;
            } else {
                w.digits_int -= leading_digits;
            }
        }

        let stop = bin_idx + words_int as usize * CODEC_WORD_SIZE;
        while bin_idx < stop {
            w.word_buf[word_idx] = read_bin_word(buf, bin_idx, CODEC_WORD_SIZE, mask);
            if w.word_buf[word_idx] as u32 > CODEC_WORD_MAX as u32 {
                return Err(bin_size);
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
            w.word_buf[word_idx] = read_bin_word(buf, bin_idx, CODEC_WORD_SIZE, mask);
            if w.word_buf[word_idx] as u32 > CODEC_WORD_MAX as u32 {
                return Err(bin_size);
            }
            word_idx += 1;
            bin_idx += CODEC_WORD_SIZE;
        }

        if trailing_digits > 0 {
            let i = DIG2BYTES[trailing_digits as usize];
            w.word_buf[word_idx] = read_bin_word(buf, bin_idx, i, mask)
                .wrapping_mul(CODEC_POWERS10[DIGITS_PER_WORD - trailing_digits as usize]);
            if w.word_buf[word_idx] as u32 > CODEC_WORD_MAX as u32 {
                return Err(bin_size);
            }
        }

        Ok((w, bin_size, warning))
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
        Ok(Decimal::new_with_storage_preserving_zero_sign(
            negative,
            digits,
            result_frac,
            storage_scale,
        ))
    }
}
