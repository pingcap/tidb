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

//! Go `encoding/hex`.

/// Go `hex.EncodeToString`.
pub fn encode_to_string(bytes: &[u8]) -> String {
    let mut encoded = Vec::new();
    append_hex(&mut encoded, bytes, b'a');
    String::from_utf8(encoded).expect("hex digits are ASCII")
}

/// Encodes bytes as uppercase hexadecimal, retaining leading zeroes.
pub fn encode_uppercase_to_string(bytes: &[u8]) -> String {
    let mut encoded = Vec::new();
    append_uppercase(&mut encoded, bytes);
    String::from_utf8(encoded).expect("hex digits are ASCII")
}

/// Appends two uppercase hexadecimal digits per byte.
pub fn append_uppercase(dst: &mut Vec<u8>, bytes: &[u8]) {
    append_hex(dst, bytes, b'A');
}

/// Go `hex.Encode`. Writes two lowercase hexadecimal digits per input byte.
///
/// # Panics
/// Panics if the destination is too short.
#[inline]
pub fn encode(dst: &mut [u8], bytes: &[u8]) -> usize {
    let len = bytes.len() * 2;
    encode_hex(&mut dst[..len], bytes, b'a');
    len
}

#[inline]
fn append_hex(dst: &mut Vec<u8>, bytes: &[u8], first_letter: u8) {
    let start = dst.len();
    let end = start
        .checked_add(bytes.len() * 2)
        .expect("hex length overflow");
    dst.resize(end, 0);
    encode_hex(&mut dst[start..], bytes, first_letter);
}

#[inline]
fn encode_hex(dst: &mut [u8], bytes: &[u8], first_letter: u8) {
    let (pairs, _) = dst.as_chunks_mut::<2>();
    let digit = |nibble| {
        b'0' + nibble
            + if nibble > 9 {
                first_letter - b'0' - 10
            } else {
                0
            }
    };
    for (&byte, pair) in bytes.iter().zip(pairs) {
        *pair = [digit(byte >> 4), digit(byte & 15)];
    }
}

const INVALID: u8 = 255;
const DIGITS: [u8; 256] = {
    let mut digits = [INVALID; 256];
    let mut i = 0;
    while i < 16 {
        if i < 10 {
            digits[b'0' as usize + i] = i as u8;
        } else {
            digits[b'a' as usize + i - 10] = i as u8;
            digits[b'A' as usize + i - 10] = i as u8;
        }
        i += 1;
    }
    digits
};

/// Decodes complete hex digit pairs and reports the first invalid input byte.
///
/// # Panics
/// Panics if the destination has fewer bytes than there are pairs.
#[inline]
pub fn decode_pairs(dst: &mut [u8], pairs: &[[u8; 2]]) -> Result<(), u8> {
    for (out, &[a, b]) in dst[..pairs.len()].iter_mut().zip(pairs) {
        let hi = DIGITS[a as usize];
        let lo = DIGITS[b as usize];
        if hi | lo == INVALID {
            return Err(if hi == INVALID { a } else { b });
        }
        *out = hi << 4 | lo;
    }
    Ok(())
}

/// Go `hex.DecodeString`, with its errors' text.
pub fn decode_string(text: &str) -> Result<Vec<u8>, String> {
    let invalid = |byte| format!("encoding/hex: invalid byte: {}", go_quote_rune(byte));
    let (pairs, tail) = text.as_bytes().as_chunks::<2>();
    let mut out = vec![0; pairs.len()];
    decode_pairs(&mut out, pairs).map_err(invalid)?;
    if let [last] = *tail {
        if DIGITS[last as usize] == INVALID {
            return Err(invalid(last));
        }
        return Err("encoding/hex: odd length hex string".to_owned());
    }
    Ok(out)
}

/// Go `%#U` of a byte as `hex.InvalidByteError` prints it.
fn go_quote_rune(byte: u8) -> String {
    let rune = char::from(byte);
    if rune.is_ascii_graphic() || rune == ' ' {
        format!("U+{:04X} '{rune}'", u32::from(byte))
    } else {
        format!("U+{:04X}", u32::from(byte))
    }
}
