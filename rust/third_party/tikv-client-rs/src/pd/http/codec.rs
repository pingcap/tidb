// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Go `codec.go`: the memcomparable byte encoding used by placement-rule key
//! ranges, plus the small Go standard-library encoders this package relies on
//! (`url.QueryEscape`, `encoding/hex`, `encoding/base64` as used by
//! `encoding/json` for `[]byte`, and `%q` for error text).

use crate::{Error, Result};

const ENC_GROUP_SIZE: usize = 8;
const ENC_MARKER: u8 = 0xFF;
const ENC_PAD: u8 = 0x0;

/// Go `encodeBytes`: the encoded value is in ascending order for comparison,
/// encoding with the following rule:
///
/// ```text
/// [group1][marker1]...[groupN][markerN]
/// group is 8 bytes slice which is padding with 0.
/// marker is `0xFF - padding 0 count`
/// ```
///
/// For example:
///
/// ```text
/// [] -> [0, 0, 0, 0, 0, 0, 0, 0, 247]
/// [1, 2, 3] -> [1, 2, 3, 0, 0, 0, 0, 0, 250]
/// [1, 2, 3, 0] -> [1, 2, 3, 0, 0, 0, 0, 0, 251]
/// [1, 2, 3, 4, 5, 6, 7, 8] -> [1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247]
/// ```
///
/// Refer: <https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format>
pub(super) fn encode_bytes(data: &[u8]) -> Vec<u8> {
    // Allocate more space to avoid unnecessary slice growing.
    let d_len = data.len();
    let mut result = Vec::with_capacity((d_len / ENC_GROUP_SIZE + 1) * (ENC_GROUP_SIZE + 1));
    let mut idx = 0;
    while idx <= d_len {
        let remain = d_len - idx;
        let mut pad_count = 0;
        if remain >= ENC_GROUP_SIZE {
            result.extend_from_slice(&data[idx..idx + ENC_GROUP_SIZE]);
        } else {
            pad_count = ENC_GROUP_SIZE - remain;
            result.extend_from_slice(&data[idx..]);
            result.extend(std::iter::repeat_n(ENC_PAD, pad_count));
        }
        result.push(ENC_MARKER - pad_count as u8);
        idx += ENC_GROUP_SIZE;
    }
    result
}

/// Go `decodeBytes`.
pub(super) fn decode_bytes(mut b: &[u8]) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(b.len());
    loop {
        if b.len() < ENC_GROUP_SIZE + 1 {
            return Err(Error::StringError(
                "insufficient bytes to decode value".to_owned(),
            ));
        }

        let group_bytes = &b[..ENC_GROUP_SIZE + 1];
        let group = &group_bytes[..ENC_GROUP_SIZE];
        let marker = group_bytes[ENC_GROUP_SIZE];

        let pad_count = (ENC_MARKER - marker) as usize;
        if pad_count > ENC_GROUP_SIZE {
            return Err(Error::StringError(format!(
                "invalid marker byte, group bytes {}",
                go_quote(group_bytes)
            )));
        }

        let real_group_size = ENC_GROUP_SIZE - pad_count;
        buf.extend_from_slice(&group[..real_group_size]);
        b = &b[ENC_GROUP_SIZE + 1..];

        if pad_count != 0 {
            // Check validity of padding bytes.
            if group[real_group_size..].iter().any(|&v| v != ENC_PAD) {
                return Err(Error::StringError(format!(
                    "invalid padding byte, group bytes {}",
                    go_quote(group_bytes)
                )));
            }
            break;
        }
    }
    Ok(buf)
}

/// Go `rawKeyToKeyHexStr`: converts a raw key to a hex string after encoding.
pub(super) fn raw_key_to_key_hex_str(key: &[u8]) -> String {
    if key.is_empty() {
        return String::new();
    }
    hex_encode(&encode_bytes(key))
}

/// Go `keyHexStrToRawKey`: converts a hex string to a raw key after decoding.
pub(super) fn key_hex_str_to_raw_key(hex_key: &str) -> Result<Vec<u8>> {
    if hex_key.is_empty() {
        return Ok(Vec::new());
    }
    let key = hex_decode(hex_key)?;
    decode_bytes(&key)
}

const LOWER_HEX: &[u8; 16] = b"0123456789abcdef";
const UPPER_HEX: &[u8; 16] = b"0123456789ABCDEF";

/// Go `hex.EncodeToString` (lower case).
pub(super) fn hex_encode(src: &[u8]) -> String {
    let mut out = String::with_capacity(src.len() * 2);
    for &b in src {
        out.push(LOWER_HEX[(b >> 4) as usize] as char);
        out.push(LOWER_HEX[(b & 0x0f) as usize] as char);
    }
    out
}

fn from_hex_char(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

// Go `hex.InvalidByteError.Error`: "encoding/hex: invalid byte: %#U".
fn hex_invalid_byte(c: u8) -> Error {
    let rune = c as char;
    let shown = if (0x20..0x7f).contains(&c) {
        format!("U+{:04X} '{}'", c, rune)
    } else {
        format!("U+{:04X}", c)
    };
    Error::StringError(format!("encoding/hex: invalid byte: {shown}"))
}

/// Go `hex.DecodeString`, including its error precedence: an invalid byte in
/// a complete pair is reported before an odd length.
pub(super) fn hex_decode(s: &str) -> Result<Vec<u8>> {
    let src = s.as_bytes();
    let mut out = Vec::with_capacity(src.len() / 2);
    let mut i = 0;
    while i + 1 < src.len() {
        let a = from_hex_char(src[i]).ok_or_else(|| hex_invalid_byte(src[i]))?;
        let b = from_hex_char(src[i + 1]).ok_or_else(|| hex_invalid_byte(src[i + 1]))?;
        out.push((a << 4) | b);
        i += 2;
    }
    if src.len() % 2 == 1 {
        let last = src[src.len() - 1];
        if from_hex_char(last).is_none() {
            return Err(hex_invalid_byte(last));
        }
        return Err(Error::StringError(
            "encoding/hex: odd length hex string".to_owned(),
        ));
    }
    Ok(out)
}

/// Go `url.QueryEscape` over raw bytes (Go converts `[]byte` with `string()`
/// without validating UTF-8, so escaping is byte-wise).
pub(super) fn query_escape(s: &[u8]) -> String {
    let mut out = String::with_capacity(s.len());
    for &c in s {
        match c {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(c as char)
            }
            b' ' => out.push('+'),
            _ => {
                out.push('%');
                out.push(UPPER_HEX[(c >> 4) as usize] as char);
                out.push(UPPER_HEX[(c & 0x0f) as usize] as char);
            }
        }
    }
    out
}

/// Go `%q` applied to a byte slice (`strconv.Quote(string(b))`).
pub(super) fn go_quote(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() + 2);
    out.push('"');
    for chunk in bytes.utf8_chunks() {
        for c in chunk.valid().chars() {
            match c {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\u{07}' => out.push_str("\\a"),
                '\u{08}' => out.push_str("\\b"),
                '\u{0c}' => out.push_str("\\f"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                '\u{0b}' => out.push_str("\\v"),
                c if (c as u32) < 0x80 && !(' '..='~').contains(&c) => {
                    out.push_str(&format!("\\x{:02x}", c as u32));
                }
                c if c.is_control() => {
                    if (c as u32) < 0x10000 {
                        out.push_str(&format!("\\u{:04x}", c as u32));
                    } else {
                        out.push_str(&format!("\\U{:08x}", c as u32));
                    }
                }
                c => out.push(c),
            }
        }
        for b in chunk.invalid() {
            out.push_str(&format!("\\x{:02x}", b));
        }
    }
    out.push('"');
    out
}

const BASE64_STD: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Go `base64.StdEncoding.EncodeToString`, which `encoding/json` uses for `[]byte`.
pub(super) fn base64_encode(src: &[u8]) -> String {
    let mut out = String::with_capacity(src.len().div_ceil(3) * 4);
    for chunk in src.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(BASE64_STD[((n >> 18) & 0x3f) as usize] as char);
        out.push(BASE64_STD[((n >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(BASE64_STD[((n >> 6) & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(BASE64_STD[(n & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

fn base64_value(c: u8) -> Option<u32> {
    match c {
        b'A'..=b'Z' => Some((c - b'A') as u32),
        b'a'..=b'z' => Some((c - b'a' + 26) as u32),
        b'0'..=b'9' => Some((c - b'0' + 52) as u32),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

/// Go `base64.StdEncoding.DecodeString` as `encoding/json` applies it to
/// `[]byte`: padding is required and `\r`/`\n` are ignored.
pub(super) fn base64_decode(s: &str) -> Result<Vec<u8>> {
    let src: Vec<u8> = s.bytes().filter(|&c| c != b'\r' && c != b'\n').collect();
    let corrupt =
        |offset: usize| Error::StringError(format!("illegal base64 data at input byte {offset}"));
    if src.len() % 4 != 0 {
        return Err(corrupt(src.len() - src.len() % 4));
    }
    let mut out = Vec::with_capacity(src.len() / 4 * 3);
    for (index, quad) in src.chunks(4).enumerate() {
        let base = index * 4;
        let last = base + 4 == src.len();
        let pads = quad.iter().rev().take_while(|&&c| c == b'=').count();
        if pads > 2 || (pads > 0 && !last) {
            return Err(corrupt(base + 4 - pads));
        }
        let mut n = 0u32;
        for (i, &c) in quad.iter().enumerate() {
            let v = if i >= 4 - pads {
                0
            } else {
                base64_value(c).ok_or_else(|| corrupt(base + i))?
            };
            n = (n << 6) | v;
        }
        out.push((n >> 16) as u8);
        if pads < 2 {
            out.push((n >> 8) as u8);
        }
        if pads < 1 {
            out.push(n as u8);
        }
    }
    Ok(out)
}

#[cfg(test)]
#[path = "codec_tests.rs"]
mod tests;
