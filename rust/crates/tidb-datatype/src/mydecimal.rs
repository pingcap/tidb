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
//! 40-byte round-trip. DEFERRED (documented): `FromString`, the arithmetic
//! (add/sub/mul/div/round/shift), binary `ToBin`/`FromBin` encoding, hashing,
//! and comparison. (`digitsToWords` uses Go's plain formula; Go's `div9`
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

impl MyDecimal {
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
