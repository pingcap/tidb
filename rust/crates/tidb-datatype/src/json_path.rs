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

use std::fmt;
use std::sync::{Mutex, OnceLock};

use tidb_kvcache::SimpleLruCache;

const PATH_CACHE_CAPACITY: usize = 1000;

#[derive(Clone, Debug, Eq, PartialEq)]
/// Selection inside one JSON array path leg.
pub enum JSONPathArraySelection {
    /// Select every element.
    Asterisk,
    /// Select one zero-based index; negative values count from the end.
    Index(i64),
    /// Select an inclusive range.
    Range {
        /// Inclusive first index.
        start: i64,
        /// Inclusive last index.
        end: i64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// One parsed JSON path leg.
pub enum JSONPathLeg {
    /// Object member name; `*` represents every member.
    Key(String),
    /// Array element selection.
    Array(JSONPathArraySelection),
    /// Recursive descent.
    DoubleAsterisk,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
/// Parsed TiDB/MySQL JSON path expression.
pub struct JSONPathExpression {
    legs: Vec<JSONPathLeg>,
    flags: u8,
}

const CONTAINS_ASTERISK: u8 = 0x01;
const CONTAINS_DOUBLE_ASTERISK: u8 = 0x02;
const CONTAINS_RANGE: u8 = 0x04;

impl JSONPathExpression {
    /// Returns the path legs after the root `$`.
    pub fn legs(&self) -> &[JSONPathLeg] {
        &self.legs
    }

    /// Returns whether the expression contains `*` or `**`.
    pub fn contains_any_asterisk(&self) -> bool {
        self.flags & (CONTAINS_ASTERISK | CONTAINS_DOUBLE_ASTERISK) != 0
    }

    /// Returns whether the expression contains an array range.
    pub fn contains_any_range(&self) -> bool {
        self.flags & CONTAINS_RANGE != 0
    }

    /// Returns whether the expression can select more than one value.
    pub fn could_match_multiple_values(&self) -> bool {
        self.contains_any_asterisk() || self.contains_any_range()
    }

    /// Returns a copy with one exact array index appended.
    pub fn push_back_index(&self, index: i64) -> Self {
        self.push_back_array_selection(JSONPathArraySelection::Index(index))
    }

    /// Returns a copy with one array selection appended.
    pub fn push_back_array_selection(&self, selection: JSONPathArraySelection) -> Self {
        let mut result = self.clone();
        match selection {
            JSONPathArraySelection::Asterisk => result.flags |= CONTAINS_ASTERISK,
            JSONPathArraySelection::Range { .. } => result.flags |= CONTAINS_RANGE,
            JSONPathArraySelection::Index(_) => {}
        }
        result.legs.push(JSONPathLeg::Array(selection));
        result
    }

    /// Returns a copy with one object key appended.
    pub fn push_back_key(&self, key: impl Into<String>) -> Self {
        let key = key.into();
        let mut result = self.clone();
        if key == "*" {
            result.flags |= CONTAINS_ASTERISK;
        }
        result.legs.push(JSONPathLeg::Key(key));
        result
    }

    /// Removes and returns the final leg while preserving flags on the parent.
    pub fn pop_last(&self) -> Option<(Self, JSONPathLeg)> {
        let mut parent = self.clone();
        let leg = parent.legs.pop()?;
        parent.recompute_flags();
        Some((parent, leg))
    }

    fn recompute_flags(&mut self) {
        self.flags = 0;
        for leg in &self.legs {
            match leg {
                JSONPathLeg::Key(key) if key == "*" => self.flags |= CONTAINS_ASTERISK,
                JSONPathLeg::Array(JSONPathArraySelection::Asterisk) => {
                    self.flags |= CONTAINS_ASTERISK;
                }
                JSONPathLeg::Array(JSONPathArraySelection::Range { .. }) => {
                    self.flags |= CONTAINS_RANGE;
                }
                JSONPathLeg::DoubleAsterisk => self.flags |= CONTAINS_DOUBLE_ASTERISK,
                _ => {}
            }
        }
    }
}

impl fmt::Display for JSONPathExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("$")?;
        for leg in &self.legs {
            match leg {
                JSONPathLeg::Key(key) => {
                    f.write_str(".")?;
                    if key == "*" {
                        f.write_str("*")?;
                    } else {
                        f.write_str(&quote_json_path_key(key))?;
                    }
                }
                JSONPathLeg::Array(JSONPathArraySelection::Asterisk) => f.write_str("[*]")?,
                JSONPathLeg::Array(JSONPathArraySelection::Index(index)) => {
                    write!(f, "[{}]", format_index(*index))?;
                }
                JSONPathLeg::Array(JSONPathArraySelection::Range { start, end }) => {
                    write!(f, "[{} to {}]", format_index(*start), format_index(*end))?;
                }
                JSONPathLeg::DoubleAsterisk => f.write_str("**")?,
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Invalid JSON path with its character position.
pub struct JSONPathError {
    /// Character position at which parsing stopped.
    pub position: usize,
}

impl fmt::Display for JSONPathError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid JSON path at position {}", self.position)
    }
}

impl std::error::Error for JSONPathError {}

struct PathCache {
    values: SimpleLruCache<String, JSONPathExpression>,
}

impl Default for PathCache {
    fn default() -> Self {
        Self {
            values: SimpleLruCache::new(PATH_CACHE_CAPACITY),
        }
    }
}

static PATH_CACHE: OnceLock<Mutex<PathCache>> = OnceLock::new();

/// Parses a TiDB/MySQL JSON path expression.
pub fn parse_json_path_expr(path: &str) -> Result<JSONPathExpression, JSONPathError> {
    let cache = PATH_CACHE.get_or_init(|| Mutex::new(PathCache::default()));
    let mut locked = cache.lock().expect("JSON path cache poisoned");
    if let Some(expression) = locked.values.get(path).cloned() {
        return Ok(expression);
    }
    drop(locked);

    let expression = Parser::new(path).parse()?;
    let mut cache = cache.lock().expect("JSON path cache poisoned");
    cache.values.put(path.to_owned(), expression.clone());
    Ok(expression)
}

/// Go `jsonPathStream` and the `parseJSONPath*` functions
/// (`pkg/types/json_path_expr.go`), step for step: the position an error
/// reports is where Go's stream stands when it gives up, including its
/// rewinds (`tryReadIndexNumber`, `tryParseArrayIndex`, `tryReadString`)
/// and the rune `read()` consumes before comparing it. Where Go would
/// `peek()` an exhausted stream (and panic) the peek answers "not a space".
struct Parser {
    chars: Vec<char>,
    position: usize,
}

const LAST: [char; 4] = ['l', 'a', 's', 't'];
const TO: [char; 2] = ['t', 'o'];

impl Parser {
    fn new(input: &str) -> Self {
        Self {
            chars: input.chars().collect(),
            position: 0,
        }
    }

    fn parse(mut self) -> Result<JSONPathExpression, JSONPathError> {
        self.skip_whitespace();
        if self.exhausted() || self.read() != '$' {
            return Err(JSONPathError { position: 1 });
        }
        self.skip_whitespace();

        let mut expression = JSONPathExpression::default();
        while !self.exhausted() {
            let ok = match self.peek() {
                Some('.') => self.parse_member(&mut expression),
                Some('[') => self.parse_array(&mut expression),
                Some('*') => self.parse_wildcard(&mut expression),
                _ => false,
            };
            if !ok {
                return self.invalid();
            }
            self.skip_whitespace();
        }
        if matches!(expression.legs.last(), Some(JSONPathLeg::DoubleAsterisk)) {
            return self.invalid();
        }
        Ok(expression)
    }

    /// Go `parseJSONPathWildcard`.
    fn parse_wildcard(&mut self, expression: &mut JSONPathExpression) -> bool {
        self.position += 1;
        if self.exhausted() || self.read() != '*' {
            return false;
        }
        if self.exhausted() || self.peek() == Some('*') {
            return false;
        }
        expression.flags |= CONTAINS_DOUBLE_ASTERISK;
        expression.legs.push(JSONPathLeg::DoubleAsterisk);
        true
    }

    /// Go `parseJSONPathArray`.
    fn parse_array(&mut self, expression: &mut JSONPathExpression) -> bool {
        self.position += 1;
        self.skip_whitespace();
        if self.exhausted() {
            return false;
        }
        if self.peek() == Some('*') {
            self.position += 1;
            expression.flags |= CONTAINS_ASTERISK;
            expression
                .legs
                .push(JSONPathLeg::Array(JSONPathArraySelection::Asterisk));
        } else {
            let Some(start) = self.try_parse_array_index() else {
                return false;
            };
            let mut selection = JSONPathArraySelection::Index(start);
            if self.peek_is_space() {
                self.skip_whitespace();
                if self.try_read_string(&TO) && self.peek_is_space() {
                    self.skip_whitespace();
                    if self.exhausted() {
                        return false;
                    }
                    let Some(end) = self.try_parse_array_index() else {
                        return false;
                    };
                    // Go `validateIndexRange`.
                    if ((start >= 0 && end >= 0) || (start < 0 && end < 0)) && start > end {
                        return false;
                    }
                    expression.flags |= CONTAINS_RANGE;
                    selection = JSONPathArraySelection::Range { start, end };
                }
            }
            expression.legs.push(JSONPathLeg::Array(selection));
        }
        self.skip_whitespace();
        !self.exhausted() && self.read() == ']'
    }

    /// Go `parseJSONPathMember`.
    fn parse_member(&mut self, expression: &mut JSONPathExpression) -> bool {
        self.position += 1;
        self.skip_whitespace();
        if self.exhausted() {
            return false;
        }
        if self.peek() == Some('*') {
            self.position += 1;
            expression.flags |= CONTAINS_ASTERISK;
            expression.legs.push(JSONPathLeg::Key("*".to_owned()));
            return true;
        }
        let (raw_key, quoted) = if self.peek() == Some('"') {
            self.position += 1;
            // Go's `readWhile` callback skips the rune after a backslash,
            // and the loop then steps past the current one.
            let start = self.position;
            loop {
                let Some(ch) = self.peek() else {
                    return false;
                };
                if ch == '\\' {
                    self.position += 1;
                } else if ch == '"' {
                    break;
                }
                self.position += 1;
            }
            let key: String = self.chars[start..self.position].iter().collect();
            self.position += 1;
            (key, true)
        } else {
            let start = self.position;
            while self
                .peek()
                .is_some_and(|ch| !(ch.is_whitespace() || ch == '.' || ch == '[' || ch == '*'))
            {
                self.position += 1;
            }
            (
                self.chars[start..self.position].iter().collect::<String>(),
                false,
            )
        };
        let wrapped = format!("\"{raw_key}\"");
        if !crate::json_scanner::valid(wrapped.as_bytes()) {
            return false;
        }
        let Ok(key) = crate::binary_json::unquote_json_string(&raw_key) else {
            return false;
        };
        if !quoted && !is_ecmascript_identifier(&key) {
            return false;
        }
        expression.legs.push(JSONPathLeg::Key(key));
        true
    }

    /// Go `tryParseArrayIndex`: a number, `last` or `last - number`; a
    /// failure rewinds to where it started, except after the leading
    /// whitespace when no index begins there.
    fn try_parse_array_index(&mut self) -> Option<i64> {
        let record = self.position;
        self.skip_whitespace();
        match self.peek()? {
            '0'..='9' => {
                let Some(index) = self.try_read_index_number() else {
                    self.position = record;
                    return None;
                };
                Some(index)
            }
            'l' => {
                if !self.try_read_string(&LAST) {
                    self.position = record;
                    return None;
                }
                self.skip_whitespace();
                if self.exhausted() || self.peek() != Some('-') {
                    return Some(-1);
                }
                self.position += 1;
                self.skip_whitespace();
                let Some(index) = self.try_read_index_number() else {
                    self.position = record;
                    return None;
                };
                Some(-1 - index)
            }
            _ => None,
        }
    }

    /// Go `tryReadIndexNumber`: digits that do not run to the end of the
    /// path and fit in a uint32.
    fn try_read_index_number(&mut self) -> Option<i64> {
        let record = self.position;
        while self.peek().is_some_and(|ch| ch.is_ascii_digit()) {
            self.position += 1;
        }
        let parsed = (!self.exhausted())
            .then(|| self.chars[record..self.position].iter().collect::<String>())
            .and_then(|digits| digits.parse::<u64>().ok())
            .filter(|&index| index <= u64::from(u32::MAX));
        match parsed {
            Some(index) => Some(index as i64),
            None => {
                self.position = record;
                None
            }
        }
    }

    /// Go `tryReadString`: the next runes are exactly `expected` and the
    /// path continues after them; otherwise nothing is consumed.
    fn try_read_string(&mut self, expected: &[char]) -> bool {
        let end = self.position + expected.len();
        if end < self.chars.len() && self.chars[self.position..end] == *expected {
            self.position = end;
            true
        } else {
            false
        }
    }

    fn read(&mut self) -> char {
        let ch = self.chars[self.position];
        self.position += 1;
        ch
    }

    fn peek_is_space(&self) -> bool {
        self.peek().is_some_and(char::is_whitespace)
    }

    fn skip_whitespace(&mut self) {
        while self.peek().is_some_and(char::is_whitespace) {
            self.position += 1;
        }
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.position).copied()
    }

    fn exhausted(&self) -> bool {
        self.position >= self.chars.len()
    }

    fn invalid<T>(&self) -> Result<T, JSONPathError> {
        Err(JSONPathError {
            position: self.position,
        })
    }
}

fn format_index(index: i64) -> String {
    if index < 0 {
        format!("last-{}", index.saturating_add(1).unsigned_abs())
    } else {
        index.to_string()
    }
}

fn is_ecmascript_identifier(value: &str) -> bool {
    let bytes = value.as_bytes();
    let Some(&first) = bytes.first() else {
        return false;
    };
    if !is_latin1_letter(first) && first != b'$' && first != b'_' {
        return false;
    }
    bytes[1..].iter().all(|&byte| {
        is_latin1_letter(byte) || byte.is_ascii_digit() || byte == b'$' || byte == b'_'
    })
}

fn is_latin1_letter(byte: u8) -> bool {
    byte.is_ascii_alphabetic()
        || matches!(
            byte,
            0xAA | 0xB5 | 0xBA | 0xC0..=0xD6 | 0xD8..=0xF6 | 0xF8..=0xFF
        )
}

fn quote_json_path_key(key: &str) -> String {
    if is_ecmascript_identifier(key) && !key.bytes().any(is_json_escape_byte) {
        return key.to_owned();
    }
    serde_json::to_string(key).expect("a Rust string is always valid JSON string input")
}

fn is_json_escape_byte(byte: u8) -> bool {
    matches!(
        byte,
        b'\\' | b'"' | b'\x08' | b'\x0c' | b'\n' | b'\r' | b'\t'
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_contains_any_asterisk() {
        for (path, expected) in [
            ("$.a[1]", false),
            ("$.a[*]", true),
            ("$.*[1]", true),
            ("$**.a[1]", true),
        ] {
            assert_eq!(
                parse_json_path_expr(path).unwrap().contains_any_asterisk(),
                expected,
                "{path}"
            );
        }
    }

    #[test]
    fn test_validate_path_expr() {
        let cases = [
            ("   $  ", true, 0),
            ("   $ .   key1  [  3  ]\t[*].*.key3", true, 5),
            ("   $ .   key1  [  3  ]**[*].*.key3", true, 6),
            ("$.\"key1 string\"[  3  ][*].*.key3", true, 5),
            (
                "$.\"hello \\\"escaped quotes\\\" world\\\\n\"[3][*].*.key3",
                true,
                5,
            ),
            ("$[1 to 5]", true, 1),
            ("$[2 to 1]", false, 0),
            ("$[last]", true, 1),
            ("$[1 to last]", true, 1),
            ("$[1to3]", false, 0),
            ("$[last - 5 to last - 10]", false, 0),
            ("$.\\\"escaped quotes\\\"[3][*].*.key3", false, 0),
            (
                "$.hello \\\"escaped quotes\\\" world[3][*].*.key3",
                false,
                0,
            ),
            ("$NoValidLegsHere", false, 0),
            ("$        No Valid Legs Here .a.b.c", false, 0),
            ("$.a[b]", false, 0),
            ("$.*[b]", false, 0),
            ("$**.a[b]", false, 0),
            ("$.b[ 1 ].", false, 0),
            ("$.performance.txn-entry-size-limit", false, 0),
            ("$.\"performance\".txn-entry-size-limit", false, 0),
            ("$.\"performance.\"txn-entry-size-limit", false, 0),
            ("$.\"performance.\"txn-entry-size-limit\"", false, 0),
            ("$[", false, 0),
            ("$a.***[3]", false, 0),
            ("$1a", false, 0),
            ("$.ѿ", false, 0),
            ("$.\"ѿ\"", true, 1),
            ("$.\"\\0\\", false, 0),
            ("$.Ѡ", false, 0),
            ("$.\"Ѡ\"", true, 1),
            ("$.µ", true, 1),
        ];
        for (path, success, legs) in cases {
            let result = parse_json_path_expr(path);
            assert_eq!(result.is_ok(), success, "{path}");
            if let Ok(expression) = result {
                assert_eq!(expression.legs().len(), legs, "{path}");
            }
        }
    }

    #[test]
    fn test_path_expr_to_string() {
        for path in [
            "$.a[1]",
            "$.a[*]",
            "$.*[2]",
            "$**.a[3]",
            "$.\"\\\"hello\\\"\"",
            "$.\"a b\"",
            "$.\"one potato\"",
        ] {
            assert_eq!(parse_json_path_expr(path).unwrap().to_string(), path);
        }
    }

    #[test]
    fn test_push_back_one_index_leg() {
        let cases = [
            ("$", 1, "$[1]", false),
            ("$.a[1]", 1, "$.a[1][1]", false),
            ("$.a[*]", 10, "$.a[*][10]", true),
            ("$.*[2]", 2, "$.*[2][2]", true),
            ("$**.a[3]", 3, "$**.a[3][3]", true),
            ("$.a[1 to 3]", 3, "$.a[1 to 3][3]", true),
            ("$.a[last-3 to last-3]", 3, "$.a[last-3 to last-3][3]", true),
            ("$**.a[3]", -3, "$**.a[3][last-2]", true),
        ];
        for (path, index, expected, multiple) in cases {
            let expression = parse_json_path_expr(path).unwrap().push_back_index(index);
            assert_eq!(expression.to_string(), expected);
            assert_eq!(expression.could_match_multiple_values(), multiple);
        }
    }

    #[test]
    fn test_push_back_one_key_leg() {
        let cases = [
            ("$", "aa", "$.aa", false),
            ("$.a[1]", "aa", "$.a[1].aa", false),
            ("$.a[1]", "*", "$.a[1].*", true),
            ("$.a[*]", "k", "$.a[*].k", true),
            ("$.*[2]", "bb", "$.*[2].bb", true),
            ("$**.a[3]", "cc", "$**.a[3].cc", true),
        ];
        for (path, key, expected, multiple) in cases {
            let expression = parse_json_path_expr(path).unwrap().push_back_key(key);
            assert_eq!(expression.to_string(), expected);
            assert_eq!(expression.could_match_multiple_values(), multiple);
        }
    }
}

#[cfg(test)]
mod go_position_tests {
    use super::parse_json_path_expr;

    /// Every position was answered by Go on this branch
    /// (`json_extract(c, path)` reporting ErrInvalidJSONPath 3143).
    #[test]
    fn invalid_paths_report_go_s_positions() {
        for (path, position) in [
            ("$[1", 2),
            ("$[", 2),
            ("$.", 2),
            ("a", 1),
            ("$.a b", 4),
            ("$[1 to", 5),
            ("$[last-", 2),
            ("$**", 3),
            ("$.**", 4),
            ("$[*", 3),
            ("$.\"a", 4),
            ("$[ 1", 3),
            (" $[1", 3),
            ("$[1]x", 4),
            ("$.a.", 4),
            ("$[-1]", 2),
            ("$[1 to 0]", 8),
            ("$[last-a]", 2),
            ("$.*.", 4),
            ("$.\"a\"b", 5),
        ] {
            assert_eq!(
                parse_json_path_expr(path).map_err(|error| error.position),
                Err(position),
                "{path}"
            );
        }
        assert!(parse_json_path_expr("$[0 to]").is_ok());
        assert!(parse_json_path_expr("$.\"*\"").is_ok());
    }
}
