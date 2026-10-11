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

//! Go's `encoding/json` syntax scanner (`scanner.go`), the state machine
//! behind `json.Valid`, which TiDB's `JSON_VALID` calls on its string
//! argument and which decides whether a document `serde_json` refused for
//! its recursion limit is valid text (Go then reports it too deep) or
//! malformed.
//!
//! The scanner checks syntax only: string contents are any bytes from 0x20
//! up (no UTF-8 or surrogate validation), and nesting is capped at
//! `maxNestingDepth` (10000).

/// Go `maxNestingDepth`.
const MAX_NESTING_DEPTH: usize = 10000;

#[derive(Clone, Copy, PartialEq, Eq)]
enum ParseState {
    ObjectKey,
    ObjectValue,
    ArrayValue,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Step {
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
    /// The remaining bytes of `true`, `false` or `null`.
    Literal(&'static [u8]),
    Error,
}

struct Scanner {
    step: Step,
    parse_state: Vec<ParseState>,
    end_top: bool,
}

fn is_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\r' | b'\n')
}

impl Scanner {
    fn new() -> Self {
        Self {
            step: Step::BeginValue,
            parse_state: Vec::new(),
            end_top: false,
        }
    }

    fn error(&mut self) -> bool {
        self.step = Step::Error;
        false
    }

    fn push_parse_state(&mut self, state: ParseState, next: Step) -> bool {
        self.parse_state.push(state);
        if self.parse_state.len() <= MAX_NESTING_DEPTH {
            self.step = next;
            true
        } else {
            self.error()
        }
    }

    fn begin_value(&mut self, c: u8) -> bool {
        if is_space(c) {
            return true;
        }
        self.step = match c {
            b'{' => return self.push_parse_state(ParseState::ObjectKey, Step::BeginStringOrEmpty),
            b'[' => return self.push_parse_state(ParseState::ArrayValue, Step::BeginValueOrEmpty),
            b'"' => Step::InString,
            b'-' => Step::Neg,
            b'0' => Step::Zero,
            b't' => Step::Literal(b"rue"),
            b'f' => Step::Literal(b"alse"),
            b'n' => Step::Literal(b"ull"),
            b'1'..=b'9' => Step::One,
            _ => return self.error(),
        };
        true
    }

    fn begin_string(&mut self, c: u8) -> bool {
        if is_space(c) {
            return true;
        }
        if c == b'"' {
            self.step = Step::InString;
            return true;
        }
        self.error()
    }

    fn end_value(&mut self, c: u8) -> bool {
        let Some(&state) = self.parse_state.last() else {
            self.step = Step::EndTop;
            self.end_top = true;
            return self.end_top_step(c);
        };
        if is_space(c) {
            self.step = Step::EndValue;
            return true;
        }
        let last = self.parse_state.len() - 1;
        match (state, c) {
            (ParseState::ObjectKey, b':') => {
                self.parse_state[last] = ParseState::ObjectValue;
                self.step = Step::BeginValue;
            }
            (ParseState::ObjectValue, b',') => {
                self.parse_state[last] = ParseState::ObjectKey;
                self.step = Step::BeginString;
            }
            (ParseState::ArrayValue, b',') => self.step = Step::BeginValue,
            (ParseState::ObjectValue, b'}') | (ParseState::ArrayValue, b']') => {
                self.parse_state.pop();
                self.step = if self.parse_state.is_empty() {
                    Step::EndTop
                } else {
                    Step::EndValue
                };
                if self.parse_state.is_empty() {
                    self.end_top = true;
                }
            }
            _ => return self.error(),
        }
        true
    }

    fn end_top_step(&mut self, c: u8) -> bool {
        if is_space(c) {
            true
        } else {
            self.error()
        }
    }

    fn step(&mut self, c: u8) -> bool {
        match self.step {
            Step::BeginValue => self.begin_value(c),
            Step::BeginValueOrEmpty => {
                if is_space(c) {
                    true
                } else if c == b']' {
                    self.end_value(c)
                } else {
                    self.begin_value(c)
                }
            }
            Step::BeginStringOrEmpty => {
                if is_space(c) {
                    true
                } else if c == b'}' {
                    let last = self.parse_state.len() - 1;
                    self.parse_state[last] = ParseState::ObjectValue;
                    self.end_value(c)
                } else {
                    self.begin_string(c)
                }
            }
            Step::BeginString => self.begin_string(c),
            Step::EndValue => self.end_value(c),
            Step::EndTop => self.end_top_step(c),
            Step::InString => {
                match c {
                    b'"' => self.step = Step::EndValue,
                    b'\\' => self.step = Step::InStringEsc,
                    0..=0x1f => return self.error(),
                    _ => {}
                }
                true
            }
            Step::InStringEsc => {
                self.step = match c {
                    b'b' | b'f' | b'n' | b'r' | b't' | b'\\' | b'/' | b'"' => Step::InString,
                    b'u' => Step::InStringEscU(0),
                    _ => return self.error(),
                };
                true
            }
            Step::InStringEscU(seen) => {
                if !c.is_ascii_hexdigit() {
                    return self.error();
                }
                self.step = if seen == 3 {
                    Step::InString
                } else {
                    Step::InStringEscU(seen + 1)
                };
                true
            }
            Step::Neg => {
                self.step = match c {
                    b'0' => Step::Zero,
                    b'1'..=b'9' => Step::One,
                    _ => return self.error(),
                };
                true
            }
            Step::One if c.is_ascii_digit() => true,
            Step::One | Step::Zero => match c {
                b'.' => {
                    self.step = Step::Dot;
                    true
                }
                b'e' | b'E' => {
                    self.step = Step::E;
                    true
                }
                _ => self.end_value(c),
            },
            Step::Dot => {
                if c.is_ascii_digit() {
                    self.step = Step::Dot0;
                    true
                } else {
                    self.error()
                }
            }
            Step::Dot0 => match c {
                b'0'..=b'9' => true,
                b'e' | b'E' => {
                    self.step = Step::E;
                    true
                }
                _ => self.end_value(c),
            },
            Step::E if matches!(c, b'+' | b'-') => {
                self.step = Step::ESign;
                true
            }
            Step::E | Step::ESign => {
                if c.is_ascii_digit() {
                    self.step = Step::E0;
                    true
                } else {
                    self.error()
                }
            }
            Step::E0 => {
                if c.is_ascii_digit() {
                    true
                } else {
                    self.end_value(c)
                }
            }
            Step::Literal(rest) => match rest.split_first() {
                Some((&expected, rest)) if expected == c => {
                    self.step = if rest.is_empty() {
                        Step::EndValue
                    } else {
                        Step::Literal(rest)
                    };
                    true
                }
                _ => self.error(),
            },
            Step::Error => false,
        }
    }

    /// Go `scanner.eof`: a value still open is completed by one space.
    fn eof(&mut self) -> bool {
        if self.step == Step::Error {
            return false;
        }
        if self.end_top {
            return true;
        }
        self.step(b' ');
        self.end_top && self.step != Step::Error
    }
}

/// Go `json.Valid`: whether `data` is exactly one JSON value, surrounding
/// whitespace aside.
#[must_use]
pub fn valid(data: &[u8]) -> bool {
    let mut scanner = Scanner::new();
    data.iter().all(|&c| scanner.step(c)) && scanner.eof()
}

/// Go `json.Indent(&dst, src, "", indent)` over a valid document, such as a
/// `BinaryJSON`'s `MarshalJSON` text: whitespace outside strings is dropped,
/// each element goes on its own line, an empty object or array stays `{}` /
/// `[]`, and a colon is followed by one space.
#[must_use]
pub fn indent(src: &str, indent: &str) -> String {
    let newline = |out: &mut String, depth: usize| {
        out.push('\n');
        for _ in 0..depth {
            out.push_str(indent);
        }
    };
    let mut out = String::with_capacity(src.len() * 2);
    let (mut need_indent, mut depth) = (false, 0usize);
    let (mut in_string, mut escaped) = (false, false);
    for c in src.chars() {
        if in_string {
            out.push(c);
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                in_string = false;
            }
            continue;
        }
        if c.is_ascii() && is_space(c as u8) {
            continue;
        }
        if need_indent && c != '}' && c != ']' {
            need_indent = false;
            depth += 1;
            newline(&mut out, depth);
        }
        match c {
            '"' => {
                in_string = true;
                out.push(c);
            }
            '{' | '[' => {
                need_indent = true;
                out.push(c);
            }
            ',' => {
                out.push(c);
                newline(&mut out, depth);
            }
            ':' => out.push_str(": "),
            '}' | ']' => {
                if need_indent {
                    need_indent = false;
                } else {
                    depth = depth.saturating_sub(1);
                    newline(&mut out, depth);
                }
                out.push(c);
            }
            _ => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{indent, valid};

    /// Every expected text was produced by Go's own `json.Indent(.., "", "  ")`.
    #[test]
    fn indent_matches_go() {
        assert_eq!(indent("[]", "  "), "[]");
        assert_eq!(indent("{}", "  "), "{}");
        assert_eq!(indent("1", "  "), "1");
        assert_eq!(indent("\"a, b: [c]\"", "  "), "\"a, b: [c]\"");
        assert_eq!(
            indent(r#"{"a": [1, {}], "b\"": []}"#, "  "),
            "{\n  \"a\": [\n    1,\n    {}\n  ],\n  \"b\\\"\": []\n}"
        );
    }

    /// Every case was answered by Go's own `json.Valid` (go1.27), including
    /// the depth cap and the string-content rules TiDB's `JSON_VALID`
    /// inherits.
    #[test]
    fn matches_go_json_valid() {
        for text in [
            "1",
            "-0",
            "0.5e+10",
            "1E-2",
            "\"x\"",
            "true",
            "false",
            "null",
            " [ ] ",
            "{}",
            "{\"a\": [1, {\"b\": null}]}",
            "\"\\u00e9\\n\"",
            "\"\\ud800\"",
            "[[]]",
        ] {
            assert!(valid(text.as_bytes()), "{text}");
        }
        for text in [
            "",
            " ",
            "01",
            "1.",
            ".5",
            "-",
            "1e",
            "+1",
            "tru",
            "nul",
            "[1,]",
            "{\"a\":1,}",
            "{1:2}",
            "[1 2]",
            "\"a",
            "\"\\x\"",
            "\"\\u12\"",
            "1 2",
            "{\"a\"}",
            "[}",
        ] {
            assert!(!valid(text.as_bytes()), "{text}");
        }
        // Raw bytes from 0x20 up, valid UTF-8 or not, may sit in a string.
        assert!(valid(b"\"\xff\xfe\""));
        assert!(!valid(b"\"\x01\""));
        let deep = |n: usize| format!("{}{}", "[".repeat(n), "]".repeat(n));
        assert!(valid(deep(200).as_bytes()));
        assert!(valid(deep(10000).as_bytes()));
        assert!(!valid(deep(10001).as_bytes()));
    }
}
