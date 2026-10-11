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

use crate::{EvalError, JsonError};

/// A parsed TiDB JSON path.  This is a direct structural port of
/// `types.JSONPathExpression` / `ParseJSONPathExpr` in
/// `pkg/types/json_path_expr.go`; ranges and wildcards carry the same
/// `CouldMatchMultipleValues` flag used by JSON_LENGTH.
#[derive(Debug)]
pub(crate) struct JsonPath {
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

/// Go `types.ParseJSONPathExpr`, through its port
/// (`tidb_datatype::parse_json_path_expr`), in this module's tree form.
/// Go's extraction treats a key `*` as every member whether it was written
/// `.*` or `."*"`; only the unquoted form sets `CouldMatchMultipleValues`.
pub(crate) fn parse_path(input: &str) -> Result<JsonPath, EvalError> {
    use tidb_datatype::{JSONPathArraySelection, JSONPathLeg};

    let expression = tidb_datatype::parse_json_path_expr(input)
        .map_err(|error| EvalError::Json(JsonError::InvalidPath(error.position)))?;
    let legs = expression
        .legs()
        .iter()
        .map(|leg| match leg {
            JSONPathLeg::Key(key) if key == "*" => PathLeg::KeyWildcard,
            JSONPathLeg::Key(key) => PathLeg::Key(key.clone()),
            JSONPathLeg::Array(JSONPathArraySelection::Asterisk) => {
                PathLeg::Array(ArraySelection::All)
            }
            JSONPathLeg::Array(JSONPathArraySelection::Index(index)) => {
                PathLeg::Array(ArraySelection::Index(*index))
            }
            JSONPathLeg::Array(JSONPathArraySelection::Range { start, end }) => {
                PathLeg::Array(ArraySelection::Range(*start, *end))
            }
            JSONPathLeg::DoubleAsterisk => PathLeg::Recursive,
        })
        .collect();
    Ok(JsonPath {
        legs,
        could_match_multiple: expression.could_match_multiple_values(),
    })
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

/// Whether an array-selection leg names a NON-array value itself, per
/// `extractTo`'s "If the current object is not an array, still append them if
/// the selection includes 0 or last" arm.
///
/// Private on purpose: this rule belongs to `extractTo` ALONE.
/// `extractToCallback`, the walk `JSON_SEARCH` runs, deliberately has no
/// equivalent, so `super::search` must not reach for it.
fn select_non_array(selection: &ArraySelection) -> bool {
    match *selection {
        ArraySelection::Index(index) => index == 0 || index == -1,
        ArraySelection::Range(start, end) => start == 0 && end >= -1,
        ArraySelection::All => false,
    }
}
