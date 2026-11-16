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

/// Go `hex.DecodeString`, with its errors' text.
pub fn decode_string(text: &str) -> Result<Vec<u8>, String> {
    let bytes = text.as_bytes();
    let nibble = |index: usize| -> Result<u8, String> {
        let byte = bytes[index];
        (byte as char)
            .to_digit(16)
            .map(|digit| digit as u8)
            .ok_or_else(|| format!("encoding/hex: invalid byte: {}", go_quote_rune(byte)))
    };
    let mut out = Vec::with_capacity(bytes.len() / 2);
    let mut index = 0;
    while index + 1 < bytes.len() {
        out.push((nibble(index)? << 4) | nibble(index + 1)?);
        index += 2;
    }
    if bytes.len() % 2 == 1 {
        nibble(bytes.len() - 1)?;
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
