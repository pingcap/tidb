//! The part of Go `encoding/json` that `encodeSSEKMSContext` uses: decoding a
//! JSON object whose values are all strings (`json.Valid` followed by
//! `Decoder.Token`), and `json.Marshal` of the resulting `map[string]string`.

use std::collections::BTreeMap;

struct Parser<'a> {
    s: &'a [u8],
    i: usize,
}

impl Parser<'_> {
    fn ws(&mut self) {
        while self
            .s
            .get(self.i)
            .is_some_and(|c| matches!(c, b' ' | b'\t' | b'\n' | b'\r'))
        {
            self.i += 1;
        }
    }

    fn eat(&mut self, c: u8) -> bool {
        self.ws();
        if self.s.get(self.i) == Some(&c) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    /// Go `getu4`.
    fn u4(&self, at: usize) -> Option<u32> {
        let hex = self.s.get(at..at + 4)?;
        let text = std::str::from_utf8(hex).ok()?;
        if !hex.iter().all(u8::is_ascii_hexdigit) {
            return None;
        }
        u32::from_str_radix(text, 16).ok()
    }

    /// A JSON string, decoded as Go `unquoteBytes` does: a lone or
    /// mismatched UTF-16 surrogate becomes U+FFFD.
    fn string(&mut self) -> Option<String> {
        self.ws();
        if self.s.get(self.i) != Some(&b'"') {
            return None;
        }
        self.i += 1;
        let mut out = String::new();
        loop {
            let c = *self.s.get(self.i)?;
            match c {
                b'"' => {
                    self.i += 1;
                    return Some(out);
                }
                0..=0x1f => return None,
                b'\\' => {
                    let e = *self.s.get(self.i + 1)?;
                    self.i += 2;
                    match e {
                        b'"' | b'\\' | b'/' => out.push(char::from(e)),
                        b'b' => out.push('\u{8}'),
                        b'f' => out.push('\u{c}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => {
                            let r = self.u4(self.i)?;
                            self.i += 4;
                            if (0xD800..0xE000).contains(&r) {
                                let pair = (0xD800..0xDC00).contains(&r)
                                    && self.s.get(self.i..self.i + 2) == Some(b"\\u")
                                    && self
                                        .u4(self.i + 2)
                                        .is_some_and(|r1| (0xDC00..0xE000).contains(&r1));
                                if pair {
                                    let r1 = self.u4(self.i + 2).unwrap();
                                    self.i += 6;
                                    let c = 0x10000 + ((r - 0xD800) << 10) + (r1 - 0xDC00);
                                    out.push(char::from_u32(c).unwrap());
                                } else {
                                    out.push('\u{FFFD}');
                                }
                            } else {
                                out.push(char::from_u32(r).unwrap());
                            }
                        }
                        _ => return None,
                    }
                }
                _ => {
                    // The input is valid UTF-8; copy one character.
                    let len = match c {
                        0xF0.. => 4,
                        0xE0.. => 3,
                        0xC0.. => 2,
                        _ => 1,
                    };
                    out.push_str(std::str::from_utf8(self.s.get(self.i..self.i + len)?).ok()?);
                    self.i += len;
                }
            }
        }
    }
}

/// Decodes `{"k":"v",...}`, or `None` for anything else (invalid JSON, a
/// non-string value, a repeated key). The input must already be UTF-8.
pub(crate) fn parse_string_object(s: &[u8]) -> Option<BTreeMap<String, String>> {
    std::str::from_utf8(s).ok()?;
    let mut p = Parser { s, i: 0 };
    if !p.eat(b'{') {
        return None;
    }
    let mut values = BTreeMap::new();
    if !p.eat(b'}') {
        loop {
            let key = p.string()?;
            if !p.eat(b':') {
                return None;
            }
            let value = p.string()?;
            if values.insert(key, value).is_some() {
                return None;
            }
            if p.eat(b',') {
                continue;
            }
            if p.eat(b'}') {
                break;
            }
            return None;
        }
    }
    p.ws();
    (p.i == s.len()).then_some(values)
}

/// Go `appendString` with `escapeHTML` (what `json.Marshal` uses).
fn append_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '\\' | '"' => {
                out.push('\\');
                out.push(c);
            }
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\0'..='\u{1f}' | '<' | '>' | '&' | '\u{2028}' | '\u{2029}' => {
                out.push_str(&format!("\\u{:04x}", u32::from(c)));
            }
            _ => out.push(c),
        }
    }
    out.push('"');
}

/// Go `json.Marshal(map[string]string)`: keys in byte order.
pub(crate) fn marshal_string_map(values: &BTreeMap<String, String>) -> String {
    let mut out = String::from("{");
    for (i, (key, value)) in values.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        append_string(&mut out, key);
        out.push(':');
        append_string(&mut out, value);
    }
    out.push('}');
    out
}
