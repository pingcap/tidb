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

//! `JSON_SEARCH`: find string leaves by LIKE pattern and report their paths.
//!
//! Mirrors `builtinJSONSearchSig.evalJSON` in
//! `pkg/expression/builtin_json.go` and `BinaryJSON.Search` /
//! `jsonWalk` in `pkg/types/json_binary_functions.go`.
//!
//! This is the one builtin that runs the walk BACKWARDS: instead of
//! extracting values at a path, it visits every string leaf and BUILDS the
//! path text that names it. That is why the traversal lives here rather than
//! reusing `super::path`'s -- the output is `$.a[0].b`, an ECMAScript-quoted
//! path string, not a value. `one` mode stops at the first hit, so the walk
//! is threaded with a stop flag rather than collecting and truncating.
//!
//! Two rules here differ from `super::path`'s extraction walk, and reusing
//! that one's rules produced wrong ROW output before:
//!
//! - an array-selection leg matches ONLY an array. `$[0].a` on the object
//!   `{"a":"foo"}` finds nothing, where `JSON_EXTRACT` would select the
//!   object itself.
//! - the same full path is reported at most ONCE across the entire walk
//!   (Go's `pathSet`), not merely deduplicated where repeats land adjacent.

use std::collections::HashSet;

use serde_json::Value as Json;

use super::path::{array_range, is_ecmascript_identifier, parse_path, PathLeg};
use super::text::format_json;
use super::value::parse_json_document_argument;
use crate::coerce::coerce_str;
use crate::{Datum, EvalError, JsonError};

/// `JSON_SEARCH(json_doc, one_or_all, search_str [, escape_char [, path] ...])`,
/// port of `builtinJSONSearchSig.evalJSON` and `BinaryJSON.Search`.
///
/// Search walks only JSON string leaves and returns their full JSON paths.  The
/// frozen evaluator carries JSON documents as text, so this keeps the same
/// representable boundary as the other JSON functions and rejects no extra
/// SQL scalar values before the shared text coercion.
pub(super) fn json_search(vals: &[Datum]) -> Result<Datum, EvalError> {
    let Some(document) = parse_json_document_argument(&vals[0])? else {
        return Ok(Datum::Null);
    };
    let Some(mode) = coerce_str(&vals[1])? else {
        return Ok(Datum::Null);
    };
    let mode = mode.to_ascii_lowercase();
    if mode != "one" && mode != "all" {
        // Go: `ErrInvalidJSONContainsPathType` (3150), NOT the 3154 the same
        // mistake raises in `JSON_CONTAINS_PATH`. A user error either way, so
        // neither may surface as `Unsupported`.
        return Err(EvalError::Json(JsonError::InvalidContainsPathType));
    }
    let Some(pattern) = coerce_str(&vals[2])? else {
        return Ok(Datum::Null);
    };
    let escape = match vals.get(3) {
        None | Some(Datum::Null) => '\\',
        Some(value) => {
            let Some(value) = coerce_str(value)? else {
                return Ok(Datum::Null);
            };
            if value.is_empty() {
                '\\'
            } else if value.chars().count() == 1 {
                value.chars().next().expect("one character is present")
            } else {
                return Err(EvalError::Unsupported("JSON_SEARCH escape length"));
            }
        }
    };

    let mut paths = Vec::new();
    if vals.len() > 4 {
        for value in &vals[4..] {
            let Some(path) = coerce_str(value)? else {
                return Ok(Datum::Null);
            };
            paths.push(parse_path(&path)?);
        }
    }

    let mut matches = Vec::new();
    if paths.is_empty() {
        walk_search(
            &document,
            "$",
            &pattern,
            escape,
            &mut matches,
            mode == "one",
        );
    } else {
        for path in &paths {
            select_search(
                &document,
                &path.legs,
                "$".to_string(),
                &pattern,
                escape,
                &mut matches,
                mode == "one",
            );
            if mode == "one" && !matches.is_empty() {
                break;
            }
        }
    }
    // Go's `BinaryJSON.Walk` carries a `pathSet` and refuses to visit a full
    // path twice for the WHOLE walk, not merely for consecutive visits: a
    // `**` leg reaches `$.a.a` once by descending from the root and again by
    // recursing into `$.a`, and two path arguments naming overlapping
    // subtrees reach shared leaves twice. `Vec::dedup` only collapses
    // ADJACENT repeats, so `$**.a` over `{"a":{"b":"x","a":"x"}}` used to
    // answer the three-element `["$.a.a", "$.a.b", "$.a.a"]`. Visiting a
    // path a second time can only re-derive results already collected, so
    // dropping the repeats from the OUTPUT is the same walk Go performs.
    let mut seen = HashSet::new();
    matches.retain(|path| seen.insert(path.clone()));
    if matches.is_empty() {
        return Ok(Datum::Null);
    }
    let result = if matches.len() == 1 {
        Json::String(matches.remove(0))
    } else {
        Json::Array(matches.into_iter().map(Json::String).collect())
    };
    Ok(Datum::new_string(format_json(&result)))
}

fn select_search(
    value: &Json,
    legs: &[PathLeg],
    path: String,
    pattern: &str,
    escape: char,
    output: &mut Vec<String>,
    stop_after_one: bool,
) {
    if stop_after_one && !output.is_empty() {
        return;
    }
    if legs.is_empty() {
        walk_search(value, &path, pattern, escape, output, stop_after_one);
        return;
    }
    match &legs[0] {
        PathLeg::Key(key) => {
            if let Json::Object(object) = value {
                if let Some(child) = object.get(key) {
                    select_search(
                        child,
                        &legs[1..],
                        append_object_path(&path, key),
                        pattern,
                        escape,
                        output,
                        stop_after_one,
                    );
                }
            }
        }
        PathLeg::KeyWildcard => {
            if let Json::Object(object) = value {
                for (key, child) in object {
                    select_search(
                        child,
                        &legs[1..],
                        append_object_path(&path, key),
                        pattern,
                        escape,
                        output,
                        stop_after_one,
                    );
                    if stop_after_one && !output.is_empty() {
                        return;
                    }
                }
            }
        }
        PathLeg::Array(selection) => {
            if let Json::Array(values) = value {
                let (start, end) = array_range(selection, values.len());
                if start <= end {
                    for (index, child) in
                        values.iter().enumerate().skip(start).take(end - start + 1)
                    {
                        select_search(
                            child,
                            &legs[1..],
                            format!("{path}[{index}]"),
                            pattern,
                            escape,
                            output,
                            stop_after_one,
                        );
                        if stop_after_one && !output.is_empty() {
                            return;
                        }
                    }
                }
            }
            // NOT the `extractTo` rule. `BinaryJSON.Extract` lets `$[0]` and
            // `$[0 to N]` name a NON-array value itself, but the callback
            // walk `JSON_SEARCH` runs does not -- `extractToCallback` enters
            // its array-selection branch only `&& bj.TypeCode ==
            // JSONTypeCodeArray`, and Go says so out loud: "NOTICE: path [0]
            // & [*] for JSON object other than array is INVALID, which is
            // different from extractTo".
        }
        PathLeg::Recursive => {
            select_search(
                value,
                &legs[1..],
                path.clone(),
                pattern,
                escape,
                output,
                stop_after_one,
            );
            if stop_after_one && !output.is_empty() {
                return;
            }
            match value {
                Json::Array(values) => {
                    for (index, child) in values.iter().enumerate() {
                        select_search(
                            child,
                            legs,
                            format!("{path}[{index}]"),
                            pattern,
                            escape,
                            output,
                            stop_after_one,
                        );
                        if stop_after_one && !output.is_empty() {
                            return;
                        }
                    }
                }
                Json::Object(object) => {
                    for (key, child) in object {
                        select_search(
                            child,
                            legs,
                            append_object_path(&path, key),
                            pattern,
                            escape,
                            output,
                            stop_after_one,
                        );
                        if stop_after_one && !output.is_empty() {
                            return;
                        }
                    }
                }
                Json::Null | Json::Bool(_) | Json::Number(_) | Json::String(_) => {}
            }
        }
    }
}

fn walk_search(
    value: &Json,
    path: &str,
    pattern: &str,
    escape: char,
    output: &mut Vec<String>,
    stop_after_one: bool,
) {
    if stop_after_one && !output.is_empty() {
        return;
    }
    match value {
        Json::String(text) => {
            if like_match(text, pattern, escape) {
                output.push(path.to_string());
            }
        }
        Json::Array(values) => {
            for (index, child) in values.iter().enumerate() {
                walk_search(
                    child,
                    &format!("{path}[{index}]"),
                    pattern,
                    escape,
                    output,
                    stop_after_one,
                );
                if stop_after_one && !output.is_empty() {
                    return;
                }
            }
        }
        Json::Object(object) => {
            for (key, child) in object {
                walk_search(
                    child,
                    &append_object_path(path, key),
                    pattern,
                    escape,
                    output,
                    stop_after_one,
                );
                if stop_after_one && !output.is_empty() {
                    return;
                }
            }
        }
        Json::Null | Json::Bool(_) | Json::Number(_) => {}
    }
}

fn append_object_path(path: &str, key: &str) -> String {
    if is_ecmascript_identifier(key) {
        format!("{path}.{key}")
    } else {
        let encoded = serde_json::to_string(key).expect("string serialization cannot fail");
        format!("{path}.{encoded}")
    }
}

/// TiDB's `DoMatch` pattern language: `%` spans any number of characters and
/// `_` spans exactly one, with the caller-selected escape character quoting
/// either wildcard or a literal escape.  Work in Unicode scalar values like
/// Go's stringutil matcher does for this expression boundary.
fn like_match(text: &str, pattern: &str, escape: char) -> bool {
    let text: Vec<char> = text.chars().collect();
    let pattern: Vec<char> = pattern.chars().collect();
    let mut memo = std::collections::HashMap::new();
    fn match_at(
        text: &[char],
        pattern: &[char],
        text_index: usize,
        pattern_index: usize,
        escape: char,
        memo: &mut std::collections::HashMap<(usize, usize), bool>,
    ) -> bool {
        if let Some(result) = memo.get(&(text_index, pattern_index)) {
            return *result;
        }
        let result = if pattern_index == pattern.len() {
            text_index == text.len()
        } else if pattern[pattern_index] == escape {
            if pattern_index + 1 >= pattern.len() {
                text_index < text.len() && text[text_index] == escape
            } else {
                text_index < text.len()
                    && text[text_index] == pattern[pattern_index + 1]
                    && match_at(
                        text,
                        pattern,
                        text_index + 1,
                        pattern_index + 2,
                        escape,
                        memo,
                    )
            }
        } else if pattern[pattern_index] == '%' {
            match_at(text, pattern, text_index, pattern_index + 1, escape, memo)
                || (text_index < text.len()
                    && match_at(text, pattern, text_index + 1, pattern_index, escape, memo))
        } else {
            text_index < text.len()
                && (pattern[pattern_index] == '_' || pattern[pattern_index] == text[text_index])
                && match_at(
                    text,
                    pattern,
                    text_index + 1,
                    pattern_index + 1,
                    escape,
                    memo,
                )
        };
        memo.insert((text_index, pattern_index), result);
        result
    }
    match_at(&text, &pattern, 0, 0, escape, &mut memo)
}
