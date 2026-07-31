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

//! Rendering a JSON value back to text: the family's one output boundary,
//! plus `JSON_PRETTY`.
//!
//! Mirrors `BinaryJSON.String` / `MarshalJSON` / `marshalFloat64To` in
//! `pkg/types/json_binary.go` and `builtinJSONSPrettySig.evalString` in
//! `pkg/expression/builtin_json.go`.
//!
//! This is NOT serde's compact form. TiDB's text sorts object keys by their
//! UTF-8 bytes, separates with `, ` and `: `, and renders a JSON DOUBLE from
//! its `f64` rather than from the input lexeme -- so `1.0` prints as `1.0`
//! and `1e0` prints as `1.0` too. Every builtin that returns a JSON result
//! goes through [`format_json`]; nowhere else may spell a JSON value.

use std::collections::BTreeMap;

use serde_json::{Number, Value as Json};

use super::value::parse_json_document_argument;
use crate::{Datum, EvalError};

/// `JSON_PRETTY(json_doc)`, port of `builtinJSONSPrettySig.evalString` in
/// `pkg/expression/builtin_json.go`.  TiDB first marshals BinaryJSON using
/// its sorted-key/space-after-separator representation, then applies
/// `encoding/json.Indent` with a two-space prefix.  Recurse directly from the
/// parsed textual value so numbers use the same `format_json_number` boundary
/// as the other JSON leaves and objects retain BinaryJSON's byte-sorted keys.
pub(super) fn json_pretty(value: &Datum) -> Result<Datum, EvalError> {
    let Some(document) = parse_json_document_argument(value)? else {
        return Ok(Datum::Null);
    };
    Ok(Datum::new_string(format_json_pretty(&document, 0)))
}

fn format_json_pretty(value: &Json, level: usize) -> String {
    let indent = |level: usize| "  ".repeat(level);
    match value {
        Json::Null | Json::Bool(_) | Json::Number(_) | Json::String(_) => format_json(value),
        Json::Array(values) => {
            if values.is_empty() {
                return "[]".to_string();
            }
            let children = values
                .iter()
                .map(|value| {
                    format!(
                        "{}{}",
                        indent(level + 1),
                        format_json_pretty(value, level + 1)
                    )
                })
                .collect::<Vec<_>>();
            format!("[\n{}\n{}]", children.join(",\n"), indent(level))
        }
        Json::Object(object) => {
            if object.is_empty() {
                return "{}".to_string();
            }
            let sorted: BTreeMap<&str, &Json> = object
                .iter()
                .map(|(key, value)| (key.as_str(), value))
                .collect();
            let children = sorted
                .into_iter()
                .map(|(key, value)| {
                    let key = serde_json::to_string(key).expect("string serialization cannot fail");
                    format!(
                        "{}{}: {}",
                        indent(level + 1),
                        key,
                        format_json_pretty(value, level + 1)
                    )
                })
                .collect::<Vec<_>>();
            format!("{{\n{}\n{}}}", children.join(",\n"), indent(level))
        }
    }
}

/// TiDB's `BinaryJSON.MarshalJSON` text form: arrays/objects have spaces,
/// object keys are sorted by UTF-8 bytes, and parsed floating JSON numbers
/// use `marshalFloat64To`'s `DOUBLE` representation (not their input lexeme).
pub(super) fn format_json(value: &Json) -> String {
    match value {
        Json::Null => "null".to_string(),
        Json::Bool(boolean) => boolean.to_string(),
        Json::Number(number) => format_json_number(number),
        Json::String(string) => {
            serde_json::to_string(string).expect("string serialization cannot fail")
        }
        Json::Array(values) => {
            let values = values.iter().map(format_json).collect::<Vec<_>>();
            format!("[{}]", values.join(", "))
        }
        Json::Object(object) => {
            let sorted: BTreeMap<&str, &Json> = object
                .iter()
                .map(|(key, value)| (key.as_str(), value))
                .collect();
            let values = sorted
                .into_iter()
                .map(|(key, value)| {
                    let key = serde_json::to_string(key).expect("string serialization cannot fail");
                    format!("{key}: {}", format_json(value))
                })
                .collect::<Vec<_>>();
            format!("{{{}}}", values.join(", "))
        }
    }
}

fn format_json_number(number: &Number) -> String {
    if let Some(integer) = number.as_i64() {
        return integer.to_string();
    }
    if let Some(integer) = number.as_u64() {
        return integer.to_string();
    }
    let float = number
        .as_f64()
        .expect("serde JSON numbers are finite f64 here");
    format_binary_json_float(float)
}

/// Port of `BinaryJSON.marshalFloat64To`'s observable decimal/exponent rule.
/// Rust's shortest `f64` display supplies the same round-trip significand;
/// the threshold and exponent cleanup are TiDB-specific.
fn format_binary_json_float(value: f64) -> String {
    let abs = value.abs();
    if abs != 0.0 && !(1e-15..1e15).contains(&abs) {
        let mut rendered = format!("{value:e}");
        if let Some(exponent) = rendered.find('e') {
            let exponent_part = &rendered[exponent + 1..];
            let cleaned = exponent_part
                .strip_prefix('+')
                .unwrap_or(exponent_part)
                .strip_prefix("-0")
                .map_or_else(
                    || exponent_part.trim_start_matches('+').to_string(),
                    |rest| format!("-{rest}"),
                );
            rendered.truncate(exponent + 1);
            rendered.push_str(&cleaned);
        }
        return rendered;
    }
    let mut rendered = value.to_string();
    if !rendered.contains('.') {
        rendered.push_str(".0");
    }
    rendered
}
