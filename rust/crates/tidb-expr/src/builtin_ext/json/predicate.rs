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

//! Predicates over JSON values: `JSON_CONTAINS`, `JSON_CONTAINS_PATH`,
//! `JSON_OVERLAPS`, `MEMBER OF`, and the value equality all four rest on.
//!
//! Mirrors `builtinJSON{Contains,ContainsPath,Overlaps,MemberOf}Sig` in
//! `pkg/expression/builtin_json.go` and `ContainsBinaryJSON` /
//! `OverlapsBinaryJSON` / `CompareBinaryJSON` in
//! `pkg/types/json_binary_functions.go`.
//!
//! [`json_equal`] is the load-bearing leaf: a wrong answer here is not a
//! wrong scalar but a wrong ROW SET, since these predicates sit in WHERE
//! clauses. It follows `CompareBinaryJSON`'s type PRECEDENCE, not serde's
//! structural `PartialEq`: numeric kinds compare by value (`1`, `1.0`, and
//! `1e0` are one value), while a JSON boolean, string, and number of
//! different precedence never compare equal however they print.

use std::cmp::Ordering;

use serde_json::{Number, Value as Json};

use super::path::{extract, parse_path};
use super::value::{json_value_argument, parse_json_document_argument, parse_json_value_argument};
use crate::coerce::coerce_str;
use crate::{Datum, EvalError, JsonError};

/// `JSON_MEMBER_OF(candidate, document)`, port of
/// `builtinJSONMemberOfSig.evalInt`.  The candidate is deliberately converted
/// as a JSON *value*: Go disables `ParseToJSONFlag` for this argument, so an
/// SQL string such as `"1"` is the JSON string `"1"`, not the JSON number
/// parsed from its text.  The document argument retains the normal JSON cast
/// and therefore parses textual JSON documents.
pub(super) fn json_member_of(vals: &[Datum]) -> Result<Datum, EvalError> {
    let [candidate, document] = vals else {
        return Err(EvalError::Unsupported("JSON_MEMBER_OF arity"));
    };
    if candidate.is_null() || document.is_null() {
        return Ok(Datum::Null);
    }
    let candidate = json_value_argument(candidate)?;
    let document = parse_json_value_argument(document)?;
    let result = match document {
        Json::Array(values) => values.iter().any(|value| json_equal(value, &candidate)),
        value => json_equal(&value, &candidate),
    };
    Ok(Datum::Int(i64::from(result)))
}

/// `JSON_CONTAINS(document, candidate [, path])`, port of
/// `builtinJSONContainsSig.evalInt` and `types.ContainsBinaryJSON`.
pub(super) fn json_contains(vals: &[Datum]) -> Result<Datum, EvalError> {
    let ([document, candidate] | [document, candidate, ..]) = vals else {
        return Err(EvalError::Unsupported("JSON_CONTAINS arity"));
    };
    if document.is_null() || candidate.is_null() {
        return Ok(Datum::Null);
    }
    let mut document = parse_json_value_argument(document)?;
    let candidate = parse_json_value_argument(candidate)?;
    if let Some(path_value) = vals.get(2) {
        let Some(path) = coerce_str(path_value)? else {
            return Ok(Datum::Null);
        };
        let path = parse_path(&path)?;
        if path.could_match_multiple {
            return Err(EvalError::Json(JsonError::InvalidPathMultipleSelection));
        }
        let Some(extracted) = extract(&document, &[path]) else {
            return Ok(Datum::Null);
        };
        document = extracted;
    }
    Ok(Datum::Int(i64::from(json_contains_value(
        &document, &candidate,
    ))))
}

/// `JSON_OVERLAPS(left, right)`, port of `builtinJSONOverlapsSig.evalInt` and
/// `types.OverlapsBinaryJSON`.
pub(super) fn json_overlaps(vals: &[Datum]) -> Result<Datum, EvalError> {
    let [left, right] = vals else {
        return Err(EvalError::Unsupported("JSON_OVERLAPS arity"));
    };
    if left.is_null() || right.is_null() {
        return Ok(Datum::Null);
    }
    let left = parse_json_value_argument(left)?;
    let right = parse_json_value_argument(right)?;
    Ok(Datum::Int(i64::from(json_overlaps_value(&left, &right))))
}

/// `JSON_CONTAINS_PATH(json_doc, one_or_all, path [, path] ...)`, port of
/// `builtinJSONContainsPathSig.evalInt`.  A path is present when TiDB's
/// BinaryJSON `Extract` operation returns at least one value; this naturally
/// handles object/array wildcards and recursive paths without treating a
/// wildcard as a scalar selection.  `ONE` returns as soon as one path is
/// present, while `ALL` returns as soon as one path is absent.
pub(super) fn json_contains_path(vals: &[Datum]) -> Result<Datum, EvalError> {
    let [document, contain_type, paths @ ..] = vals else {
        return Err(EvalError::Unsupported("JSON_CONTAINS_PATH arity"));
    };
    let Some(document) = parse_json_document_argument(document)? else {
        return Ok(Datum::Null);
    };
    let Some(contain_type) = coerce_str(contain_type)? else {
        return Ok(Datum::Null);
    };
    let contain_type = contain_type.to_ascii_lowercase();
    let one = match contain_type.as_str() {
        "one" => true,
        "all" => false,
        _ => return Err(EvalError::Unsupported("invalid JSON_CONTAINS_PATH mode")),
    };

    let mut contains = false;
    for path_value in paths {
        let Some(path) = coerce_str(path_value)? else {
            return Ok(Datum::Null);
        };
        let path = parse_path(&path)?;
        let exists = extract(&document, &[path]).is_some();
        if one {
            if exists {
                return Ok(Datum::Int(1));
            }
            contains = false;
        } else if !exists {
            return Ok(Datum::Int(0));
        } else {
            contains = true;
        }
    }
    Ok(Datum::Int(i64::from(contains)))
}

fn json_contains_value(document: &Json, candidate: &Json) -> bool {
    match document {
        Json::Object(object) => match candidate {
            Json::Object(candidate) => candidate.iter().all(|(key, value)| {
                object
                    .get(key)
                    .is_some_and(|document| json_contains_value(document, value))
            }),
            _ => false,
        },
        Json::Array(values) => match candidate {
            Json::Array(candidate) => candidate
                .iter()
                .all(|value| json_contains_value(document, value)),
            _ => values
                .iter()
                .any(|value| json_contains_value(value, candidate)),
        },
        _ => json_equal(document, candidate),
    }
}

fn json_overlaps_value(left: &Json, right: &Json) -> bool {
    if !matches!(left, Json::Array(_)) && matches!(right, Json::Array(_)) {
        return json_overlaps_value(right, left);
    }
    match left {
        Json::Object(object) => match right {
            Json::Object(right) => right
                .iter()
                .any(|(key, value)| object.get(key).is_some_and(|left| json_equal(left, value))),
            _ => false,
        },
        Json::Array(values) => match right {
            Json::Array(right) => values
                .iter()
                .any(|left| right.iter().any(|right| json_equal(left, right))),
            _ => values.iter().any(|left| json_equal(left, right)),
        },
        _ => json_equal(left, right),
    }
}

/// Equality used by TiDB's `CompareBinaryJSON`: JSON numeric kinds compare by
/// value (`1`, `1.0`, and `1e0` are equal), while strings, arrays, and objects
/// retain their JSON type and recursive structure.
fn json_equal(left: &Json, right: &Json) -> bool {
    match (left, right) {
        (Json::Null, Json::Null) => true,
        (Json::Bool(left), Json::Bool(right)) => left == right,
        (Json::Number(left), Json::Number(right)) => {
            compare_json_numbers(left, right) == Ordering::Equal
        }
        (Json::String(left), Json::String(right)) => left.as_bytes() == right.as_bytes(),
        (Json::Array(left), Json::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| json_equal(left, right))
        }
        (Json::Object(left), Json::Object(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .all(|(key, left)| right.get(key).is_some_and(|right| json_equal(left, right)))
        }
        _ => false,
    }
}

fn compare_json_numbers(left: &Number, right: &Number) -> Ordering {
    match (left.as_i64(), left.as_u64(), right.as_i64(), right.as_u64()) {
        (Some(left), _, Some(right), _) => left.cmp(&right),
        (Some(left), _, _, Some(right)) => compare_signed_unsigned(left, right),
        (_, Some(left), Some(right), _) => compare_signed_unsigned(right, left).reverse(),
        (_, Some(left), _, Some(right)) => left.cmp(&right),
        _ => left
            .as_f64()
            .zip(right.as_f64())
            .and_then(|(left, right)| left.partial_cmp(&right))
            .unwrap_or(Ordering::Equal),
    }
}

fn compare_signed_unsigned(left: i64, right: u64) -> Ordering {
    if left < 0 {
        Ordering::Less
    } else {
        (left as u64).cmp(&right)
    }
}
