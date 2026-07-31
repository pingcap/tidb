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
// See the License for the specific language governing permissions and
// limitations under the License.

//! JSON path expressions and extraction: `JSON_EXTRACT` (`->`, `->>`), the
//! `$`-path grammar, and the selection walk every other builtin reuses.
//!
//! Mirrors `pkg/types/json_path_expr.go` (`ParseJSONPathExpr`,
//! `JSONPathExpression`, `jsonPathLeg`) and `BinaryJSON.Extract` /
//! `extractTo` in `pkg/types/json_binary_functions.go`, plus
//! `builtinJSONExtractSig.evalJSON` in `pkg/expression/builtin_json.go`.
//!
//! [`JsonPath::could_match_multiple`] is Go's `CouldMatchMultipleValues`: the
//! flag that decides whether a caller may treat a selection as one value
//! (`JSON_LENGTH`, `JSON_KEYS`, `JSON_SET`) or must raise 3149.

use std::collections::HashSet;

use serde_json::Value as Json;

use super::text::format_json;
use super::value::parse_json_document_argument;
use crate::coerce::coerce_str;
use crate::{Datum, EvalError, JsonError};

/// `JSON_EXTRACT(json_doc, path [, path] ...)`, port of
/// `builtinJSONExtractSig.evalJSON` and `types.BinaryJSON.Extract`.
pub(super) fn json_extract(vals: &[Datum]) -> Result<Datum, EvalError> {
    let Some(document) = parse_json_document_argument(&vals[0])? else {
        return Ok(Datum::Null);
    };
    let mut paths = Vec::with_capacity(vals.len() - 1);
    for value in &vals[1..] {
        let Some(path) = coerce_str(value)? else {
            return Ok(Datum::Null);
        };
        paths.push(parse_path(&path)?);
    }
    match extract(&document, &paths) {
        Some(value) => Ok(Datum::new_string(format_json(&value))),
        None => Ok(Datum::Null),
    }
}

/// A parsed TiDB JSON path.  This is a direct structural port of
/// `types.JSONPathExpression` / `ParseJSONPathExpr` in
/// `pkg/types/json_path_expr.go`; ranges and wildcards carry the same
/// `CouldMatchMultipleValues` flag used by JSON_LENGTH.
#[derive(Debug)]
pub(super) struct JsonPath {
    pub(super) legs: Vec<PathLeg>,
    pub(super) could_match_multiple: bool,
}

#[derive(Debug)]
pub(super) enum PathLeg {
    Key(String),
    KeyWildcard,
    Array(ArraySelection),
    Recursive,
}

#[derive(Debug)]
pub(super) enum ArraySelection {
    All,
    Index(i64),
    Range(i64, i64),
}

/// Parses TiDB's JSON path grammar.  The argument is already a Rust `str`,
/// so its runes have the same Unicode-level behavior as Go's `[]rune` parser.
pub(super) fn parse_path(input: &str) -> Result<JsonPath, EvalError> {
    let chars: Vec<char> = input.chars().collect();
    let mut cursor = 0;
    skip_space(&chars, &mut cursor);
    if chars.get(cursor) != Some(&'$') {
        return Err(path_error(1));
    }
    cursor += 1;
    skip_space(&chars, &mut cursor);

    let mut legs = Vec::new();
    let mut could_match_multiple = false;
    while cursor < chars.len() {
        match chars[cursor] {
            '.' => {
                cursor += 1;
                skip_space(&chars, &mut cursor);
                if chars.get(cursor) == Some(&'*') {
                    cursor += 1;
                    legs.push(PathLeg::KeyWildcard);
                    could_match_multiple = true;
                } else {
                    let key = parse_member(&chars, &mut cursor)?;
                    legs.push(PathLeg::Key(key));
                }
            }
            '[' => {
                cursor += 1;
                skip_space(&chars, &mut cursor);
                let selection = if chars.get(cursor) == Some(&'*') {
                    cursor += 1;
                    could_match_multiple = true;
                    ArraySelection::All
                } else {
                    let start = parse_index(&chars, &mut cursor)?;
                    let after_start = cursor;
                    skip_space(&chars, &mut cursor);
                    if after_start != cursor && read_word(&chars, &mut cursor, "to") {
                        if cursor >= chars.len() || !chars[cursor].is_whitespace() {
                            return Err(path_error(cursor));
                        }
                        skip_space(&chars, &mut cursor);
                        let end = parse_index(&chars, &mut cursor)?;
                        if (start >= 0 && end >= 0 || start < 0 && end < 0) && start > end {
                            return Err(path_error(cursor));
                        }
                        could_match_multiple = true;
                        ArraySelection::Range(start, end)
                    } else {
                        cursor = after_start;
                        ArraySelection::Index(start)
                    }
                };
                skip_space(&chars, &mut cursor);
                if chars.get(cursor) != Some(&']') {
                    return Err(path_error(cursor));
                }
                cursor += 1;
                legs.push(PathLeg::Array(selection));
            }
            '*' => {
                if chars.get(cursor + 1) != Some(&'*') || chars.get(cursor + 2) == Some(&'*') {
                    return Err(path_error(cursor));
                }
                cursor += 2;
                legs.push(PathLeg::Recursive);
                // Go's `JSONPathExpression.CouldMatchMultipleValues` marks
                // both `*` and `**` as asterisk selections.  JSON_LENGTH
                // must reject `$**.key` before extraction, just as it does
                // for `$.*` and `$[*]`; leaving this bit clear would let a
                // recursive path silently return one arbitrary match.
                could_match_multiple = true;
            }
            _ => return Err(path_error(cursor)),
        }
        skip_space(&chars, &mut cursor);
    }
    if matches!(legs.last(), Some(PathLeg::Recursive)) {
        return Err(path_error(cursor));
    }
    Ok(JsonPath {
        legs,
        could_match_multiple,
    })
}

/// Go `ErrInvalidJSONPath` (3143) at the stream position that rejected the
/// path. `parseJSONPathExpr` reports `jsonPathStream.pos` as it stands after
/// the failing leg parser ran — this parser's `cursor` is the same rune
/// counter, advanced by the same steps — except for a path that does not
/// begin with `$`, where Go reports a literal 1.
fn path_error(position: usize) -> EvalError {
    EvalError::Json(JsonError::InvalidPath(position))
}

fn skip_space(chars: &[char], cursor: &mut usize) {
    while chars.get(*cursor).is_some_and(|ch| ch.is_whitespace()) {
        *cursor += 1;
    }
}

fn read_word(chars: &[char], cursor: &mut usize, expected: &str) -> bool {
    let saved = *cursor;
    for expected_char in expected.chars() {
        if chars.get(*cursor) != Some(&expected_char) {
            *cursor = saved;
            return false;
        }
        *cursor += 1;
    }
    true
}

/// Parses the member half of `.member` / `."quoted member"`.  Quoted keys
/// use JSON-string decoding just as Go wraps the segment in quotes then calls
/// `unquoteJSONString`; unquoted keys retain the ECMAScript-identifier gate.
fn parse_member(chars: &[char], cursor: &mut usize) -> Result<String, EvalError> {
    if chars.get(*cursor) == Some(&'"') {
        let start = *cursor;
        *cursor += 1;
        let mut escaped = false;
        while let Some(ch) = chars.get(*cursor) {
            *cursor += 1;
            if escaped {
                escaped = false;
            } else if *ch == '\\' {
                escaped = true;
            } else if *ch == '"' {
                let encoded: String = chars[start..*cursor].iter().collect();
                return serde_json::from_str(&encoded).map_err(|_| path_error(*cursor));
            }
        }
        return Err(path_error(*cursor));
    }
    let start = *cursor;
    while chars
        .get(*cursor)
        .is_some_and(|ch| !ch.is_whitespace() && *ch != '.' && *ch != '[' && *ch != '*')
    {
        *cursor += 1;
    }
    let key: String = chars[start..*cursor].iter().collect();
    if !is_ecmascript_identifier(&key) {
        return Err(path_error(*cursor));
    }
    Ok(key)
}

pub(super) fn is_ecmascript_identifier(value: &str) -> bool {
    // TiDB's predicate indexes Go string bytes rather than decoded runes.
    // This family supports its exact ASCII subset only.  A non-ASCII member
    // must be quoted (`$."宽"`), which TiDB accepts and which is faithful;
    // unquoted non-ASCII paths are an explicit capability boundary, never a
    // broadened approximation of Go's byte-level Unicode-table quirk.
    if !value.is_ascii() {
        return false;
    }
    let bytes = value.as_bytes();
    let Some(&first) = bytes.first() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == b'$' || first == b'_') {
        return false;
    }
    bytes[1..]
        .iter()
        .all(|&byte| byte.is_ascii_alphanumeric() || byte == b'$' || byte == b'_')
}

/// Parses `number`, `last`, and `last - number`, represented exactly as
/// Go's `jsonPathArrayIndex`: non-negative values count from start and
/// negative values are `len + index` (`last` is -1).
fn parse_index(chars: &[char], cursor: &mut usize) -> Result<i64, EvalError> {
    skip_space(chars, cursor);
    if read_word(chars, cursor, "last") {
        skip_space(chars, cursor);
        if chars.get(*cursor) != Some(&'-') {
            return Ok(-1);
        }
        *cursor += 1;
        skip_space(chars, cursor);
        let amount = parse_u32(chars, cursor)?;
        return Ok(-1 - i64::from(amount));
    }
    Ok(i64::from(parse_u32(chars, cursor)?))
}

fn parse_u32(chars: &[char], cursor: &mut usize) -> Result<u32, EvalError> {
    let start = *cursor;
    while chars.get(*cursor).is_some_and(char::is_ascii_digit) {
        *cursor += 1;
    }
    if start == *cursor {
        return Err(path_error(*cursor));
    }
    chars[start..*cursor]
        .iter()
        .collect::<String>()
        .parse()
        .map_err(|_| path_error(*cursor))
}

/// Port of `BinaryJSON.Extract`.  References keep the source tree intact,
/// allowing the same pointer-identity de-duplication that TiDB applies per
/// path while walking `**` recursively.
pub(super) fn extract(document: &Json, paths: &[JsonPath]) -> Option<Json> {
    let mut matches = Vec::new();
    for path in paths {
        let mut seen = HashSet::new();
        collect(document, &path.legs, &mut matches, &mut seen);
    }
    if matches.is_empty() {
        return None;
    }
    if paths.len() == 1 && matches.len() == 1 && !paths[0].could_match_multiple {
        return Some(matches.remove(0).clone());
    }
    Some(Json::Array(matches.into_iter().cloned().collect()))
}

fn collect<'a>(
    value: &'a Json,
    legs: &[PathLeg],
    output: &mut Vec<&'a Json>,
    seen: &mut HashSet<usize>,
) {
    if legs.is_empty() {
        let identity = value as *const Json as usize;
        if seen.insert(identity) {
            output.push(value);
        }
        return;
    }
    match &legs[0] {
        PathLeg::Key(key) => {
            if let Json::Object(object) = value {
                if let Some(child) = object.get(key) {
                    collect(child, &legs[1..], output, seen);
                }
            }
        }
        PathLeg::KeyWildcard => {
            if let Json::Object(object) = value {
                for child in object.values() {
                    collect(child, &legs[1..], output, seen);
                }
            }
        }
        PathLeg::Array(selection) => match value {
            Json::Array(values) => {
                let (start, end) = array_range(selection, values.len());
                if start <= end {
                    for child in &values[start..=end] {
                        collect(child, &legs[1..], output, seen);
                    }
                }
            }
            _ if select_non_array(selection) => collect(value, &legs[1..], output, seen),
            _ => {}
        },
        PathLeg::Recursive => {
            collect(value, &legs[1..], output, seen);
            match value {
                Json::Array(values) => {
                    for child in values {
                        collect(child, legs, output, seen);
                    }
                }
                Json::Object(object) => {
                    for child in object.values() {
                        collect(child, legs, output, seen);
                    }
                }
                Json::Null | Json::Bool(_) | Json::Number(_) | Json::String(_) => {}
            }
        }
    }
}

pub(super) fn array_range(selection: &ArraySelection, len: usize) -> (usize, usize) {
    let len = len as i64;
    let index = |value: i64| if value < 0 { len + value } else { value };
    let clamp_end = |value: i64| value.min(len - 1);
    let (start, end) = match *selection {
        ArraySelection::All => (0, len - 1),
        ArraySelection::Index(index_value) => (index(index_value), clamp_end(index(index_value))),
        ArraySelection::Range(start, end) => (index(start), clamp_end(index(end))),
    };
    if start < 0 || end < 0 {
        (1, 0)
    } else {
        (start as usize, end as usize)
    }
}

pub(super) fn select_non_array(selection: &ArraySelection) -> bool {
    match *selection {
        ArraySelection::Index(index) => index == 0 || index == -1,
        ArraySelection::Range(start, end) => start == 0 && end >= -1,
        ArraySelection::All => false,
    }
}
