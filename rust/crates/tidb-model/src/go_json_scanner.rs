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

//! `boundary:` Go's `encoding/json` `checkValid` (`scanner.go`), the pass
//! `json.Unmarshal` runs before decoding. TiDB forwards its `SyntaxError`
//! text to users (for example `ErrEngineAttributeInvalidFormat`), so the
//! messages here are Go's own, byte for byte.

const MAX_NESTING_DEPTH: usize = 10_000;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Parse {
    ObjectKey,
    ObjectValue,
    ArrayValue,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    BeginValue,
    BeginValueOrEmpty,
    BeginStringOrEmpty,
    BeginString,
    EndValue,
    EndTop,
    InString,
    InStringEsc,
    InStringEscU(u8),
    Neg,
    One,
    Zero,
    Dot,
    Dot0,
    E,
    ESign,
    E0,
    Literal(&'static [u8], usize, &'static str),
}

struct Scanner {
    state: State,
    stack: Vec<Parse>,
}

const fn is_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\r' | b'\n')
}

/// Go `quoteChar`: formats a byte as a quoted character literal.
fn quote_char(c: u8) -> String {
    match c {
        b'\'' => r"'\''".to_owned(),
        b'"' => "'\"'".to_owned(),
        _ => {
            // `strconv.Quote(string(c))`: a byte converts to the rune of the
            // same value, so 0x80..=0xFF are Latin-1 code points.
            let ch = char::from(c);
            let body = match ch {
                '\x07' => r"\a".to_owned(),
                '\x08' => r"\b".to_owned(),
                '\x0c' => r"\f".to_owned(),
                '\n' => r"\n".to_owned(),
                '\r' => r"\r".to_owned(),
                '\t' => r"\t".to_owned(),
                '\x0b' => r"\v".to_owned(),
                '\\' => r"\\".to_owned(),
                ch if (ch as u32) < 0x20 || ch as u32 == 0x7f => format!(r"\x{:02x}", ch as u32),
                // `strconv.IsPrint` rejects the C1 controls, NBSP and the
                // soft hyphen in this range.
                ch if (0x80..=0xa0).contains(&(ch as u32)) || ch as u32 == 0xad => {
                    format!(r"\u{:04x}", ch as u32)
                }
                ch => ch.to_string(),
            };
            format!("'{body}'")
        }
    }
}

impl Scanner {
    fn error(c: u8, context: &str) -> String {
        format!("invalid character {} {context}", quote_char(c))
    }

    fn push(&mut self, parse: Parse) -> Result<(), String> {
        self.stack.push(parse);
        if self.stack.len() > MAX_NESTING_DEPTH {
            return Err("exceeded max depth".to_owned());
        }
        Ok(())
    }

    fn pop(&mut self) {
        self.stack.pop();
        self.state = if self.stack.is_empty() {
            State::EndTop
        } else {
            State::EndValue
        };
    }

    fn begin_value(&mut self, c: u8) -> Result<(), String> {
        if is_space(c) {
            return Ok(());
        }
        self.state = match c {
            b'{' => {
                self.push(Parse::ObjectKey)?;
                State::BeginStringOrEmpty
            }
            b'[' => {
                self.push(Parse::ArrayValue)?;
                State::BeginValueOrEmpty
            }
            b'"' => State::InString,
            b'-' => State::Neg,
            b'0' => State::Zero,
            b't' => State::Literal(b"rue", 0, "in literal true"),
            b'f' => State::Literal(b"alse", 0, "in literal false"),
            b'n' => State::Literal(b"ull", 0, "in literal null"),
            b'1'..=b'9' => State::One,
            _ => return Err(Self::error(c, "looking for beginning of value")),
        };
        Ok(())
    }

    fn end_value(&mut self, c: u8) -> Result<(), String> {
        let Some(&parse) = self.stack.last() else {
            self.state = State::EndTop;
            return self.step(c);
        };
        if is_space(c) {
            self.state = State::EndValue;
            return Ok(());
        }
        match parse {
            Parse::ObjectKey if c == b':' => {
                *self.stack.last_mut().expect("non-empty") = Parse::ObjectValue;
                self.state = State::BeginValue;
            }
            Parse::ObjectKey => return Err(Self::error(c, "after object key")),
            Parse::ObjectValue if c == b',' => {
                *self.stack.last_mut().expect("non-empty") = Parse::ObjectKey;
                self.state = State::BeginString;
            }
            Parse::ObjectValue if c == b'}' => self.pop(),
            Parse::ObjectValue => return Err(Self::error(c, "after object key:value pair")),
            Parse::ArrayValue if c == b',' => self.state = State::BeginValue,
            Parse::ArrayValue if c == b']' => self.pop(),
            Parse::ArrayValue => return Err(Self::error(c, "after array element")),
        }
        Ok(())
    }

    fn step(&mut self, c: u8) -> Result<(), String> {
        match self.state {
            State::BeginValue => self.begin_value(c),
            State::BeginValueOrEmpty => {
                if is_space(c) {
                    return Ok(());
                }
                if c == b']' {
                    return self.end_value(c);
                }
                self.begin_value(c)
            }
            State::BeginStringOrEmpty => {
                if is_space(c) {
                    return Ok(());
                }
                if c == b'}' {
                    *self.stack.last_mut().expect("inside an object") = Parse::ObjectValue;
                    return self.end_value(c);
                }
                self.state = State::BeginString;
                self.step(c)
            }
            State::BeginString => {
                if is_space(c) {
                    return Ok(());
                }
                if c == b'"' {
                    self.state = State::InString;
                    return Ok(());
                }
                Err(Self::error(c, "looking for beginning of object key string"))
            }
            State::EndValue => self.end_value(c),
            State::EndTop => {
                if is_space(c) {
                    Ok(())
                } else {
                    Err(Self::error(c, "after top-level value"))
                }
            }
            State::InString => {
                match c {
                    b'"' => self.state = State::EndValue,
                    b'\\' => self.state = State::InStringEsc,
                    c if c < 0x20 => return Err(Self::error(c, "in string literal")),
                    _ => {}
                }
                Ok(())
            }
            State::InStringEsc => {
                self.state = match c {
                    b'b' | b'f' | b'n' | b'r' | b't' | b'\\' | b'/' | b'"' => State::InString,
                    b'u' => State::InStringEscU(0),
                    _ => return Err(Self::error(c, "in string escape code")),
                };
                Ok(())
            }
            State::InStringEscU(seen) => {
                if !c.is_ascii_hexdigit() {
                    return Err(Self::error(c, "in \\u hexadecimal character escape"));
                }
                self.state = if seen == 3 {
                    State::InString
                } else {
                    State::InStringEscU(seen + 1)
                };
                Ok(())
            }
            State::Neg => {
                self.state = match c {
                    b'0' => State::Zero,
                    b'1'..=b'9' => State::One,
                    _ => return Err(Self::error(c, "in numeric literal")),
                };
                Ok(())
            }
            State::One => {
                if c.is_ascii_digit() {
                    return Ok(());
                }
                self.state = State::Zero;
                self.step(c)
            }
            State::Zero => match c {
                b'.' => {
                    self.state = State::Dot;
                    Ok(())
                }
                b'e' | b'E' => {
                    self.state = State::E;
                    Ok(())
                }
                _ => self.end_value(c),
            },
            State::Dot => {
                if c.is_ascii_digit() {
                    self.state = State::Dot0;
                    return Ok(());
                }
                Err(Self::error(c, "after decimal point in numeric literal"))
            }
            State::Dot0 => match c {
                b'0'..=b'9' => Ok(()),
                b'e' | b'E' => {
                    self.state = State::E;
                    Ok(())
                }
                _ => self.end_value(c),
            },
            State::E => {
                if c == b'+' || c == b'-' {
                    self.state = State::ESign;
                    return Ok(());
                }
                self.state = State::ESign;
                self.step(c)
            }
            State::ESign => {
                if c.is_ascii_digit() {
                    self.state = State::E0;
                    return Ok(());
                }
                Err(Self::error(c, "in exponent of numeric literal"))
            }
            State::E0 => {
                if c.is_ascii_digit() {
                    return Ok(());
                }
                self.end_value(c)
            }
            State::Literal(rest, at, context) => {
                if c != rest[at] {
                    return Err(Self::error(
                        c,
                        &format!("{context} (expecting {})", quote_char(rest[at])),
                    ));
                }
                self.state = if at + 1 == rest.len() {
                    State::EndValue
                } else {
                    State::Literal(rest, at + 1, context)
                };
                Ok(())
            }
        }
    }
}

/// Go `checkValid`: `Ok` for one well-formed JSON value, otherwise the
/// `SyntaxError` message `json.Unmarshal` would return.
pub fn check_valid(data: &[u8]) -> Result<(), String> {
    let mut scanner = Scanner {
        state: State::BeginValue,
        stack: Vec::new(),
    };
    for &c in data {
        scanner.step(c)?;
    }
    // Go `scanner.eof` feeds one virtual space, which ends a trailing number
    // and reports a truncated literal or number as an invalid ' '.
    if scanner.state == State::EndTop {
        return Ok(());
    }
    scanner.step(b' ')?;
    if scanner.state == State::EndTop {
        return Ok(());
    }
    Err("unexpected end of JSON input".to_owned())
}

#[cfg(test)]
mod tests {
    use super::check_valid;

    #[test]
    fn messages_match_go() {
        for (input, expected) in [
            ("{", "unexpected end of JSON input"),
            ("", "unexpected end of JSON input"),
            ("12", ""),
            (r#"{"a": [1, 2.5e-3, true, null, "x\u00e9"]}"#, ""),
            ("x", "invalid character 'x' looking for beginning of value"),
            (r#"{"a" 1}"#, "invalid character '1' after object key"),
            (
                r#"{"a":1 2}"#,
                "invalid character '2' after object key:value pair",
            ),
            ("[1 2]", "invalid character '2' after array element"),
            (
                "{1}",
                "invalid character '1' looking for beginning of object key string",
            ),
            ("1 2", "invalid character '2' after top-level value"),
            (
                "tru",
                "invalid character ' ' in literal true (expecting 'e')",
            ),
            ("-", "invalid character ' ' in numeric literal"),
            ("\"abc", "unexpected end of JSON input"),
            (
                "trux",
                "invalid character 'x' in literal true (expecting 'e')",
            ),
            ("-x", "invalid character 'x' in numeric literal"),
            (
                "1.x",
                "invalid character 'x' after decimal point in numeric literal",
            ),
            (
                "1ex",
                "invalid character 'x' in exponent of numeric literal",
            ),
            ("\"\\x\"", "invalid character 'x' in string escape code"),
            ("\"\n\"", "invalid character '\\n' in string literal"),
            (
                "'",
                r"invalid character '\'' looking for beginning of value",
            ),
        ] {
            assert_eq!(
                check_valid(input.as_bytes()).err().unwrap_or_default(),
                expected,
                "{input}"
            );
        }
    }
}
