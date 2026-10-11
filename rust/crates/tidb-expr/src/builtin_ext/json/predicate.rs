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
//! `JSON_OVERLAPS` and `MEMBER OF`, ports of
//! `builtinJSON{Contains,ContainsPath,Overlaps,MemberOf}Sig` in
//! `pkg/expression/builtin_json.go` on the `BinaryJSON` port of
//! `ContainsBinaryJSON` / `OverlapsBinaryJSON` / `CompareBinaryJSON`.
//! These sit in WHERE clauses, so a wrong answer is a wrong row set: equality
//! is `CompareBinaryJSON`'s type precedence (`1`, `1.0` and `1e0` are one
//! value; a boolean, string and number never are).

use super::modify::{binary_json_error, json_document, parse_json_path};
use super::value::cast_as_json_value_typed;
use crate::coerce::coerce_str;
use crate::{Datum, EvalError, JsonError};
use tidb_datatype::{compare_binary_json, BinaryJSON, FieldType, JSON_TYPE_CODE_ARRAY};

fn arg_type(arg_types: &[Option<FieldType>], index: usize) -> Option<&FieldType> {
    arg_types.get(index).and_then(Option::as_ref)
}

/// `MEMBER OF(candidate, document)`, port of `builtinJSONMemberOfSig.evalInt`:
/// the candidate is a JSON value (`DisableParseJSONFlag4Expr`, so `'1'` is the
/// string `"1"`), the document a parsed document; an array document holds the
/// candidate when one element compares equal, any other document when it
/// does itself.
pub(super) fn json_member_of(
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
) -> Result<Datum, EvalError> {
    let [candidate, document] = vals else {
        return Err(EvalError::Unsupported("JSON_MEMBER_OF arity"));
    };
    let Datum::Json(candidate) = cast_as_json_value_typed(candidate, arg_type(arg_types, 0))?
    else {
        return Ok(Datum::Null);
    };
    let Some(document) = json_document(document, arg_type(arg_types, 1))? else {
        return Ok(Datum::Null);
    };
    let equal = |value: &BinaryJSON| compare_binary_json(value, &candidate).is_eq();
    let member = if document.type_code() == JSON_TYPE_CODE_ARRAY {
        let count = document.element_count().map_err(binary_json_error)?;
        let mut found = false;
        for index in 0..count {
            if document
                .array_get(index)
                .map_err(binary_json_error)?
                .is_some_and(|element| equal(&element))
            {
                found = true;
                break;
            }
        }
        found
    } else {
        equal(&document)
    };
    Ok(Datum::Int(i64::from(member)))
}

/// `JSON_CONTAINS(document, candidate [, path])`, port of
/// `builtinJSONContainsSig.evalInt`: both arguments are parsed documents
/// (`JSON_CONTAINS('[1]', '1')` is true); a path must select one value
/// (3149), and a missing one is NULL.
pub(super) fn json_contains(
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
) -> Result<Datum, EvalError> {
    let Some(mut document) = json_document(&vals[0], arg_type(arg_types, 0))? else {
        return Ok(Datum::Null);
    };
    let Some(candidate) = json_document(&vals[1], arg_type(arg_types, 1))? else {
        return Ok(Datum::Null);
    };
    if let Some(path) = vals.get(2) {
        let Some(path) = coerce_str(path)? else {
            return Ok(Datum::Null);
        };
        let path = parse_json_path(&path)?;
        if path.could_match_multiple_values() {
            return Err(EvalError::Json(JsonError::InvalidPathMultipleSelection));
        }
        let Some(target) = document
            .extract(std::slice::from_ref(&path))
            .map_err(binary_json_error)?
        else {
            return Ok(Datum::Null);
        };
        document = target;
    }
    let contains =
        tidb_datatype::contains_binary_json(&document, &candidate).map_err(binary_json_error)?;
    Ok(Datum::Int(i64::from(contains)))
}

/// `JSON_OVERLAPS(left, right)`, port of `builtinJSONOverlapsSig.evalInt`.
pub(super) fn json_overlaps(
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
) -> Result<Datum, EvalError> {
    let Some(left) = json_document(&vals[0], arg_type(arg_types, 0))? else {
        return Ok(Datum::Null);
    };
    let Some(right) = json_document(&vals[1], arg_type(arg_types, 1))? else {
        return Ok(Datum::Null);
    };
    let overlaps = tidb_datatype::overlaps_binary_json(&left, &right).map_err(binary_json_error)?;
    Ok(Datum::Int(i64::from(overlaps)))
}

/// `JSON_CONTAINS_PATH(json_doc, one_or_all, path [, path] ...)`, port of
/// `builtinJSONContainsPathSig.evalInt`: `one` answers 1 at the first path
/// that selects something, `all` answers 0 at the first that selects nothing.
pub(super) fn json_contains_path(
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
) -> Result<Datum, EvalError> {
    let Some(document) = json_document(&vals[0], arg_type(arg_types, 0))? else {
        return Ok(Datum::Null);
    };
    let Some(contain_type) = coerce_str(&vals[1])? else {
        return Ok(Datum::Null);
    };
    let one = match contain_type.to_lowercase().as_str() {
        "one" => true,
        "all" => false,
        _ => {
            return Err(EvalError::Json(JsonError::BadOneOrAllArg {
                function: "json_contains_path",
            }))
        }
    };
    let mut contains = 1;
    for path in &vals[2..] {
        let Some(path) = coerce_str(path)? else {
            return Ok(Datum::Null);
        };
        let path = parse_json_path(&path)?;
        let exists = document
            .extract(std::slice::from_ref(&path))
            .map_err(binary_json_error)?
            .is_some();
        match (exists, one) {
            (true, true) => return Ok(Datum::Int(1)),
            (false, true) => contains = 0,
            (false, false) => return Ok(Datum::Int(0)),
            (true, false) => {}
        }
    }
    Ok(Datum::Int(contains))
}
