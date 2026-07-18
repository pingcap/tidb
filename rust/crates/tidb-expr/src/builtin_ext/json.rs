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

//! JSON scalar builtins over TiDB's textual [`Datum`] domain.
//!
//! `serde_json` deliberately parses and validates only.  TiDB's result text
//! is [`types.BinaryJSON.String`](../../../pkg/types/json_binary.go)'s text
//! form, not serde's compact form: objects are key-sorted and containers use
//! `, ` / `: ` separators.  [`format_json`] is that output boundary.
//!
//! `JSON_ARRAY` and `JSON_OBJECT` dispatch their representable scalar datum
//! rows.  The frozen evaluator has no typed boolean or BinaryJSON variant, so
//! parser-originated `TRUE/FALSE` and already-typed JSON arguments remain
//! explicit partial boundaries rather than being guessed from `Datum::Int` or
//! a text string.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};

use serde_json::{Number, Value as Json};

use crate::coerce::coerce_str;
use crate::{Datum, EvalError};

/// Dispatches the JSON family.  The match and arities are ports of the
/// function classes in `pkg/expression/builtin_json.go`:
/// `builtinJSON{Type,Extract,Unquote,Quote,Array,Object,Length,Valid,
/// ArrayAppend,ArrayInsert,SUMCRC32}Sig`.
pub(crate) fn dispatch(name: &str, vals: &[Datum]) -> Option<Result<Datum, EvalError>> {
    match (name, vals.len()) {
        ("JSON_VALID", 1) => Some(json_valid(&vals[0])),
        ("JSON_TYPE", 1) => Some(json_type(&vals[0])),
        ("JSON_QUOTE", 1) => Some(json_quote(&vals[0])),
        ("JSON_UNQUOTE", 1) => Some(json_unquote(&vals[0])),
        ("JSON_ARRAY", 0..) => Some(json_array(vals)),
        ("JSON_OBJECT", 0..) => Some(json_object(vals)),
        ("JSON_LENGTH", 1 | 2) => Some(json_length(vals)),
        ("JSON_EXTRACT", 2..) => Some(json_extract(vals)),
        ("JSON_MEMBER_OF", 2) => Some(json_member_of(vals)),
        ("JSON_CONTAINS", 2 | 3) => Some(json_contains(vals)),
        ("JSON_CONTAINS_PATH", 3..) => Some(json_contains_path(vals)),
        ("JSON_KEYS", 1 | 2) => Some(json_keys(vals)),
        ("JSON_REMOVE", 2..) => Some(json_remove(vals)),
        ("JSON_ARRAY_APPEND", 3..) => Some(json_array_append(vals)),
        ("JSON_ARRAY_INSERT", 3..) => Some(json_array_insert(vals)),
        ("JSON_SET", 3..) => Some(json_modify(vals, JsonModifyMode::Set)),
        ("JSON_INSERT", 3..) => Some(json_modify(vals, JsonModifyMode::Insert)),
        ("JSON_REPLACE", 3..) => Some(json_modify(vals, JsonModifyMode::Replace)),
        ("JSON_MERGE", 2..) => Some(json_merge(vals)),
        ("JSON_MERGE_PRESERVE", 2..) => Some(json_merge(vals)),
        ("JSON_MERGE_PATCH", 2..) => Some(json_merge_patch(vals)),
        ("JSON_SEARCH", 3..) => Some(json_search(vals)),
        ("JSON_PRETTY", 1) => Some(json_pretty(&vals[0])),
        ("JSON_SUM_CRC32", 1) => Some(json_sum_crc32(&vals[0])),
        ("JSON_OVERLAPS", 2) => Some(json_overlaps(vals)),
        _ => None,
    }
}

/// `JSON_VALID(arg)`, port of `builtinJSONValid{JSON,String,Others}Sig`.
/// String arguments are JSON documents; every non-string, non-JSON SQL value
/// is the Go `Others` signature and therefore returns zero rather than being
/// stringified.  `NULL` propagates.
fn json_valid(v: &Datum) -> Result<Datum, EvalError> {
    match v {
        Datum::Null => Ok(Datum::Null),
        Datum::String(s) => Ok(Datum::Int(i64::from(
            s.as_utf8().is_ok_and(|text| parse_json(text).is_ok()),
        ))),
        Datum::Bytes(_) | Datum::Int(_) | Datum::UInt(_) | Datum::Decimal(_) | Datum::Real(_) => {
            Ok(Datum::Int(0))
        }
        Datum::MinNotNull | Datum::MaxValue => {
            Err(EvalError::Unsupported("range sentinel JSON_VALID argument"))
        }
    }
}

/// `JSON_TYPE(json_doc)`, port of `builtinJSONTypeSig.evalString` and
/// `types.BinaryJSON.Type` (`pkg/types/json_binary_functions.go`).
fn json_type(v: &Datum) -> Result<Datum, EvalError> {
    let Some(s) = coerce_str(v)? else {
        return Ok(Datum::Null);
    };
    if !matches!(v, Datum::String(_)) {
        return Err(EvalError::Unsupported("JSON_TYPE requires JSON text"));
    }
    let json = parse_json(&s)?;
    let ty = match json {
        Json::Null => "NULL",
        Json::Bool(_) => "BOOLEAN",
        Json::Number(n) if n.is_i64() => "INTEGER",
        Json::Number(n) if n.is_u64() => "UNSIGNED INTEGER",
        Json::Number(_) => "DOUBLE",
        Json::String(_) => "STRING",
        Json::Array(_) => "ARRAY",
        Json::Object(_) => "OBJECT",
    };
    Ok(Datum::new_string(ty.to_string()))
}

/// `JSON_QUOTE(str)`, port of `builtinJSONQuoteSig.evalString`.  Go's
/// `encoding/json.Encoder` has `SetEscapeHTML(false)`; serde_json has the
/// same HTML rule for strings, while retaining Go-compatible JSON escapes.
fn json_quote(v: &Datum) -> Result<Datum, EvalError> {
    match v {
        Datum::Null => Ok(Datum::Null),
        Datum::String(s) => serde_json::to_string(
            s.as_utf8()
                .map_err(|_| EvalError::Unsupported("invalid UTF-8 string datum"))?,
        )
        .map(Datum::new_string)
        .map_err(|_| EvalError::Unsupported("JSON_QUOTE encoding")),
        _ => Err(EvalError::Unsupported("JSON_QUOTE requires string")),
    }
}

/// `JSON_UNQUOTE(str)`, port of `builtinJSONUnquoteSig.evalString` plus
/// `types.UnquoteString`.  The initial document-validity gate is important:
/// a double-quoted value followed by another root value is an error, not an
/// almost-unquoted string (`TestJSONUnquote`).
fn json_unquote(v: &Datum) -> Result<Datum, EvalError> {
    let Datum::String(s) = v else {
        return if *v == Datum::Null {
            Ok(Datum::Null)
        } else {
            Err(EvalError::Unsupported("JSON_UNQUOTE requires string"))
        };
    };
    let text = s
        .as_utf8()
        .map_err(|_| EvalError::Unsupported("invalid UTF-8 string datum"))?;
    if text.len() < 2 || !text.starts_with('"') || !text.ends_with('"') {
        return Ok(Datum::new_string(text));
    }
    let Json::String(unquoted) = parse_json(text)? else {
        return Err(EvalError::Unsupported("invalid JSON_UNQUOTE document"));
    };
    Ok(Datum::new_string(unquoted))
}

/// `JSON_ARRAY(value [, value] ...)`, port of `jsonArrayFunctionClass` and
/// `builtinJSONArraySig` in `pkg/expression/builtin_json.go`.  SQL strings
/// remain JSON strings, while numeric and NULL datums become their matching
/// JSON scalar values.  Typed boolean/BinaryJSON arguments are outside this
/// evaluator's value domain and are not inferred from an integer or string.
fn json_array(vals: &[Datum]) -> Result<Datum, EvalError> {
    let values = vals
        .iter()
        .map(json_mutation_value_argument)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Datum::new_string(format_json(&Json::Array(values))))
}

/// `JSON_OBJECT(key, value [, key, value] ...)`, port of
/// `jsonObjectFunctionClass` and `builtinJSONObjectSig` in
/// `pkg/expression/builtin_json.go`.  Keys are SQL-string-coerced, NULL keys
/// are rejected, and values follow the scalar JSON value boundary used by
/// `JSON_ARRAY`.
fn json_object(vals: &[Datum]) -> Result<Datum, EvalError> {
    if !vals.len().is_multiple_of(2) {
        return Err(EvalError::Unsupported(
            "JSON_OBJECT requires key/value pairs",
        ));
    }
    let mut object = serde_json::Map::new();
    for pair in vals.chunks_exact(2) {
        let Some(key) = coerce_str(&pair[0])? else {
            return Err(EvalError::Unsupported("JSON_OBJECT NULL key"));
        };
        let value = json_mutation_value_argument(&pair[1])?;
        object.insert(key, value);
    }
    Ok(Datum::new_string(format_json(&Json::Object(object))))
}

/// `JSON_LENGTH(json_doc [, path])`, port of `builtinJSONLengthSig.evalInt`.
/// As in TiDB, a wildcard/range path is a true SQL error rather than a length
/// of an implicitly auto-wrapped selection.
fn json_length(vals: &[Datum]) -> Result<Datum, EvalError> {
    let Some(document) = parse_json_document_argument(&vals[0])? else {
        return Ok(Datum::Null);
    };
    let target = if let Some(path_value) = vals.get(1) {
        let Some(path) = coerce_str(path_value)? else {
            return Ok(Datum::Null);
        };
        let path = parse_path(&path)?;
        if path.could_match_multiple {
            return Err(EvalError::Unsupported("JSON_LENGTH multiple selection"));
        }
        let Some(extracted) = extract(&document, &[path]) else {
            return Ok(Datum::Null);
        };
        extracted
    } else {
        document
    };
    let len = match target {
        Json::Array(values) => values.len(),
        Json::Object(values) => values.len(),
        Json::Null | Json::Bool(_) | Json::Number(_) | Json::String(_) => 1,
    };
    Ok(Datum::Int(len as i64))
}

/// `JSON_EXTRACT(json_doc, path [, path] ...)`, port of
/// `builtinJSONExtractSig.evalJSON` and `types.BinaryJSON.Extract`.
fn json_extract(vals: &[Datum]) -> Result<Datum, EvalError> {
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

/// `JSON_MEMBER_OF(candidate, document)`, port of
/// `builtinJSONMemberOfSig.evalInt`.  The candidate is deliberately converted
/// as a JSON *value*: Go disables `ParseToJSONFlag` for this argument, so an
/// SQL string such as `"1"` is the JSON string `"1"`, not the JSON number
/// parsed from its text.  The document argument retains the normal JSON cast
/// and therefore parses textual JSON documents.
fn json_member_of(vals: &[Datum]) -> Result<Datum, EvalError> {
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
fn json_contains(vals: &[Datum]) -> Result<Datum, EvalError> {
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
            return Err(EvalError::Unsupported("JSON_CONTAINS multiple selection"));
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
fn json_overlaps(vals: &[Datum]) -> Result<Datum, EvalError> {
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
fn json_contains_path(vals: &[Datum]) -> Result<Datum, EvalError> {
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

/// `JSON_KEYS(json_doc [, path])`, port of
/// `builtinJSONKeys{Sig,2ArgsSig}.evalJSON`.  The result is an array of the
/// selected object's keys, in BinaryJSON's byte-sorted object order.  A
/// scalar, array, missing path, or selected non-object is SQL NULL; a path
/// that could select more than one value is an error.
fn json_keys(vals: &[Datum]) -> Result<Datum, EvalError> {
    let Some(document) = parse_json_document_argument(&vals[0])? else {
        return Ok(Datum::Null);
    };
    let target = if let Some(path_value) = vals.get(1) {
        let Some(path) = coerce_str(path_value)? else {
            return Ok(Datum::Null);
        };
        let path = parse_path(&path)?;
        if path.could_match_multiple {
            return Err(EvalError::Unsupported("JSON_KEYS multiple selection"));
        }
        let Some(extracted) = extract(&document, &[path]) else {
            return Ok(Datum::Null);
        };
        extracted
    } else {
        document
    };
    let Json::Object(object) = target else {
        return Ok(Datum::Null);
    };

    // BinaryJSON objects are encoded with keys sorted by their UTF-8 bytes;
    // serde_json may preserve insertion order, so sort explicitly before
    // constructing the result array.
    let mut keys: Vec<&str> = object.keys().map(String::as_str).collect();
    keys.sort_unstable();
    let keys = Json::Array(
        keys.into_iter()
            .map(|key| Json::String(key.to_owned()))
            .collect(),
    );
    Ok(Datum::new_string(format_json(&keys)))
}

/// `JSON_REMOVE(json_doc, path [, path] ...)`, port of
/// `builtinJSONRemoveSig.evalJSON` and `BinaryJSON.Remove`.  Paths are
/// applied in order, so removing an earlier array element shifts the indexes
/// seen by later paths.  Wildcards, ranges, recursive paths, and the root `$`
/// path are invalid; an absent exact path is a no-op.
fn json_remove(vals: &[Datum]) -> Result<Datum, EvalError> {
    let Some(mut document) = parse_json_document_argument(&vals[0])? else {
        return Ok(Datum::Null);
    };
    for path_value in &vals[1..] {
        let Some(path) = coerce_str(path_value)? else {
            return Ok(Datum::Null);
        };
        let path = parse_path(&path)?;
        if path.legs.is_empty()
            || path.legs.iter().any(|leg| {
                !matches!(
                    leg,
                    PathLeg::Key(_) | PathLeg::Array(ArraySelection::Index(_))
                )
            })
        {
            return Err(EvalError::Unsupported("JSON_REMOVE invalid path"));
        }
        remove_path(&mut document, &path.legs);
    }
    Ok(Datum::new_string(format_json(&document)))
}

/// `JSON_ARRAY_APPEND(json_doc, path, value [, path, value] ...)`, port of
/// `builtinJSONArrayAppendSig.evalJSON` and `appendJSONArray`.
///
/// The document argument is parsed as a JSON document, while each value
/// argument has the source function's `ParseToJSONFlag` disabled: an SQL
/// string such as `'{"b": 2}'` is therefore appended as a JSON *string*, not
/// parsed as an object.  The frozen Rust datum domain has no typed BinaryJSON
/// variant, so rows whose value is an already-typed JSON object/array remain
/// an explicit boundary; every scalar value-domain row is executable here.
fn json_array_append(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.len() < 3 || vals.len().is_multiple_of(2) {
        return Err(EvalError::Unsupported("JSON_ARRAY_APPEND arity"));
    }
    let Some(mut document) = parse_json_document_argument(&vals[0])? else {
        return Ok(Datum::Null);
    };
    for pair in vals[1..].chunks_exact(2) {
        let Some(path) = coerce_str(&pair[0])? else {
            return Ok(Datum::Null);
        };
        let path = parse_path(&path)?;
        if path.could_match_multiple {
            return Err(EvalError::Unsupported(
                "JSON_ARRAY_APPEND multiple selection",
            ));
        }
        let value = json_mutation_value_argument(&pair[1])?;
        append_at_path(&mut document, &path.legs, &value);
    }
    Ok(Datum::new_string(format_json(&document)))
}

/// `JSON_ARRAY_INSERT(json_doc, path, value [, path, value] ...)`, port of
/// `builtinJSONArrayInsertSig.evalJSON` and `BinaryJSON.ArrayInsert`.
///
/// The final path leg must be an exact array index.  As in Go, a missing
/// parent or a parent that is not an array is a no-op, while an index beyond
/// the end appends.  Typed BinaryJSON value arguments are outside this
/// evaluator's public datum domain; scalar values and SQL NULL are preserved.
fn json_array_insert(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.len() < 3 || vals.len().is_multiple_of(2) {
        return Err(EvalError::Unsupported("JSON_ARRAY_INSERT arity"));
    }
    let Some(mut document) = parse_json_document_argument(&vals[0])? else {
        return Ok(Datum::Null);
    };
    for pair in vals[1..].chunks_exact(2) {
        let Some(path) = coerce_str(&pair[0])? else {
            return Ok(Datum::Null);
        };
        let path = parse_path(&path)?;
        if path.could_match_multiple {
            return Err(EvalError::Unsupported(
                "JSON_ARRAY_INSERT multiple selection",
            ));
        }
        let Some(PathLeg::Array(ArraySelection::Index(index))) = path.legs.last() else {
            return Err(EvalError::Unsupported(
                "JSON_ARRAY_INSERT invalid array cell",
            ));
        };
        if path.legs.is_empty()
            || path.legs.iter().any(|leg| {
                !matches!(
                    leg,
                    PathLeg::Key(_) | PathLeg::Array(ArraySelection::Index(_))
                )
            })
        {
            return Err(EvalError::Unsupported(
                "JSON_ARRAY_INSERT invalid array cell",
            ));
        }
        let value = json_mutation_value_argument(&pair[1])?;
        insert_at_path(&mut document, &path.legs, *index, &value);
    }
    Ok(Datum::new_string(format_json(&document)))
}

/// The three `JSON_{SET,INSERT,REPLACE}` modifiers share Go's
/// `BinaryJSON.Modify` traversal. Paths are applied sequentially, so a later
/// pair observes the document produced by earlier pairs.
#[derive(Clone, Copy)]
enum JsonModifyMode {
    Set,
    Insert,
    Replace,
}

/// `JSON_SET`, `JSON_INSERT`, and `JSON_REPLACE`, ports of
/// `builtinJSON{Set,Insert,Replace}Sig.evalJSON` and `BinaryJSON.Modify` in
/// `pkg/expression/builtin_json.go` / `pkg/types/json_binary_functions.go`.
/// Value strings remain JSON strings because the source disables
/// `ParseToJSONFlag4Expr` for every value argument.
fn json_modify(vals: &[Datum], mode: JsonModifyMode) -> Result<Datum, EvalError> {
    if vals.len() < 3 || vals.len().is_multiple_of(2) {
        return Err(EvalError::Unsupported("JSON modification arity"));
    }
    let Some(mut document) = parse_json_document_argument(&vals[0])? else {
        return Ok(Datum::Null);
    };
    for pair in vals[1..].chunks_exact(2) {
        let Some(path) = coerce_str(&pair[0])? else {
            return Ok(Datum::Null);
        };
        let path = parse_path(&path)?;
        if path.could_match_multiple
            || path.legs.iter().any(|leg| {
                !matches!(
                    leg,
                    PathLeg::Key(_) | PathLeg::Array(ArraySelection::Index(_))
                )
            })
        {
            return Err(EvalError::Unsupported("JSON modification invalid path"));
        }
        let value = json_mutation_value_argument(&pair[1])?;
        let exists = descend_exact_mut(&mut document, &path.legs).is_some();
        match mode {
            JsonModifyMode::Set => {
                if let Some(target) = descend_exact_mut(&mut document, &path.legs) {
                    *target = value;
                } else {
                    insert_missing_path(&mut document, &path.legs, &value);
                }
            }
            JsonModifyMode::Insert if !exists => {
                insert_missing_path(&mut document, &path.legs, &value);
            }
            JsonModifyMode::Replace if exists => {
                if let Some(target) = descend_exact_mut(&mut document, &path.legs) {
                    *target = value;
                }
            }
            JsonModifyMode::Insert | JsonModifyMode::Replace => {}
        }
    }
    Ok(Datum::new_string(format_json(&document)))
}

fn insert_missing_path(document: &mut Json, legs: &[PathLeg], value: &Json) {
    let Some((last, parent_legs)) = legs.split_last() else {
        *document = value.clone();
        return;
    };
    let Some(parent) = descend_exact_mut(document, parent_legs) else {
        return;
    };
    match last {
        PathLeg::Key(key) => {
            if let Json::Object(object) = parent {
                object.insert(key.clone(), value.clone());
            }
        }
        PathLeg::Array(ArraySelection::Index(_)) => match parent {
            Json::Array(values) => values.push(value.clone()),
            _ => {
                let original = std::mem::replace(parent, Json::Null);
                *parent = Json::Array(vec![original, value.clone()]);
            }
        },
        _ => {}
    }
}

/// `JSON_MERGE` and `JSON_MERGE_PRESERVE`, ports of `types.MergeBinaryJSON`.
fn json_merge(vals: &[Datum]) -> Result<Datum, EvalError> {
    let mut values = Vec::with_capacity(vals.len());
    for value in vals {
        let Some(value) = parse_json_merge_argument(value, true)? else {
            return Ok(Datum::Null);
        };
        values.push(value);
    }
    Ok(Datum::new_string(format_json(&merge_json_values(values))))
}

fn parse_json_merge_argument(
    value: &Datum,
    preserve_scalar_text: bool,
) -> Result<Option<Json>, EvalError> {
    match value {
        Datum::Null => Ok(None),
        Datum::String(s) => {
            let text = s
                .as_utf8()
                .map_err(|_| EvalError::Unsupported("invalid UTF-8 JSON value"))?;
            let parsed = parse_json(text)?;
            if preserve_scalar_text && !matches!(parsed, Json::Object(_) | Json::Array(_)) {
                Ok(Some(Json::String(text.to_owned())))
            } else {
                Ok(Some(parsed))
            }
        }
        Datum::Int(value) => Ok(Some(Json::Number((*value).into()))),
        Datum::UInt(value) => Ok(Some(Json::Number((*value).into()))),
        Datum::Real(value) => Number::from_f64(*value)
            .map(Json::Number)
            .map(Some)
            .ok_or(EvalError::FloatOverflow),
        Datum::Decimal(value) => Ok(Some(parse_json(&value.to_string())?)),
        Datum::Bytes(_) => Err(EvalError::Unsupported("JSON value requires text")),
        Datum::MinNotNull | Datum::MaxValue => {
            Err(EvalError::Unsupported("range sentinel JSON value"))
        }
    }
}

fn merge_json_values(values: Vec<Json>) -> Json {
    let mut results = Vec::new();
    let mut index = 0;
    while index < values.len() {
        if matches!(values[index], Json::Object(_)) {
            let start = index;
            while index < values.len() && matches!(values[index], Json::Object(_)) {
                index += 1;
            }
            results.push(merge_json_objects(&values[start..index]));
        } else {
            results.push(values[index].clone());
            index += 1;
        }
    }
    if results.len() == 1 {
        return results.pop().expect("one merge result");
    }
    let mut array = Vec::new();
    for value in results {
        if let Json::Array(values) = value {
            array.extend(values);
        } else {
            array.push(value);
        }
    }
    Json::Array(array)
}

fn merge_json_objects(objects: &[Json]) -> Json {
    let mut merged = BTreeMap::new();
    for object in objects {
        let Json::Object(object) = object else {
            continue;
        };
        for (key, value) in object {
            if let Some(previous) = merged.remove(key) {
                merged.insert(
                    key.clone(),
                    merge_json_values(vec![previous, value.clone()]),
                );
            } else {
                merged.insert(key.clone(), value.clone());
            }
        }
    }
    Json::Object(merged.into_iter().collect())
}

/// `JSON_MERGE_PATCH`, port of `types.MergePatchBinaryJSON` (RFC 7396).
/// SQL NULL is distinct from a JSON `null` document: the former is the source
/// nil pointer that may truncate the merge, while the latter is a JSON scalar.
fn json_merge_patch(vals: &[Datum]) -> Result<Datum, EvalError> {
    let mut values = Vec::with_capacity(vals.len());
    for value in vals {
        values.push(parse_json_merge_argument(value, false)?);
    }
    let mut start = 0;
    for index in (0..values.len()).rev() {
        if values[index]
            .as_ref()
            .is_none_or(|value| !matches!(value, Json::Object(_)))
        {
            start = index;
            break;
        }
    }
    let mut target = values[start].clone();
    for patch in &values[start + 1..] {
        target = merge_patch_value(target.as_ref(), patch.as_ref());
    }
    match target {
        Some(value) => Ok(Datum::new_string(format_json(&value))),
        None => Ok(Datum::Null),
    }
}

fn merge_patch_value(target: Option<&Json>, patch: Option<&Json>) -> Option<Json> {
    let patch = patch?;
    let Json::Object(patch_object) = patch else {
        return Some(patch.clone());
    };
    let target = target?;
    let mut merged = match target {
        Json::Object(object) => object.clone(),
        _ => serde_json::Map::new(),
    };
    for (key, value) in patch_object {
        if value.is_null() {
            merged.remove(key);
        } else {
            let missing = Json::Null;
            let previous = merged.get(key).unwrap_or(&missing);
            let replacement = merge_patch_value(Some(previous), Some(value))
                .expect("non-null merge patch value cannot produce SQL NULL");
            merged.insert(key.clone(), replacement);
        }
    }
    Some(Json::Object(merged.into_iter().collect()))
}

/// `JSON_SEARCH(json_doc, one_or_all, search_str [, escape_char [, path] ...])`,
/// port of `builtinJSONSearchSig.evalJSON` and `BinaryJSON.Search`.
///
/// Search walks only JSON string leaves and returns their full JSON paths.  The
/// frozen evaluator carries JSON documents as text, so this keeps the same
/// representable boundary as the other JSON functions and rejects no extra
/// SQL scalar values before the shared text coercion.
fn json_search(vals: &[Datum]) -> Result<Datum, EvalError> {
    let Some(document) = parse_json_document_argument(&vals[0])? else {
        return Ok(Datum::Null);
    };
    let Some(mode) = coerce_str(&vals[1])? else {
        return Ok(Datum::Null);
    };
    let mode = mode.to_ascii_lowercase();
    if mode != "one" && mode != "all" {
        return Err(EvalError::Unsupported("invalid JSON_SEARCH mode"));
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
    matches.dedup();
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
            } else if select_non_array(selection) {
                select_search(
                    value,
                    &legs[1..],
                    path,
                    pattern,
                    escape,
                    output,
                    stop_after_one,
                );
            }
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

/// `JSON_PRETTY(json_doc)`, port of `builtinJSONSPrettySig.evalString` in
/// `pkg/expression/builtin_json.go`.  TiDB first marshals BinaryJSON using
/// its sorted-key/space-after-separator representation, then applies
/// `encoding/json.Indent` with a two-space prefix.  Recurse directly from the
/// parsed textual value so numbers use the same `format_json_number` boundary
/// as the other JSON leaves and objects retain BinaryJSON's byte-sorted keys.
fn json_pretty(value: &Datum) -> Result<Datum, EvalError> {
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

/// `JSON_SUM_CRC32(json_doc)`, port of `builtinJSONSumCRC32Sig.evalInt` in
/// `pkg/expression/builtin_json.go`.  The Go signature receives a JSON array
/// plus an `ARRAY`-typed `FieldType` carried by the cast expression; the
/// frozen Rust evaluator has no typed JSON datum or FieldType metadata.  The
/// representable text-domain contract therefore accepts homogeneous scalar
/// arrays (numbers or strings), preserving Go's `fmt.Appendf("%v", item)`
/// bytes before each IEEE CRC32 and returning the int64 sum.  The target-type
/// checks (signed/unsigned range, fixed string width, and explicit JSON path
/// extraction) remain an orchestrator boundary rather than guessed defaults.
fn json_sum_crc32(value: &Datum) -> Result<Datum, EvalError> {
    let Some(document) = parse_json_document_argument(value)? else {
        return Ok(Datum::Null);
    };
    let Json::Array(values) = document else {
        return Err(EvalError::Unsupported("JSON_SUM_CRC32 requires JSON array"));
    };

    let mut saw_string = false;
    let mut saw_number = false;
    let mut sum = 0_i64;
    for value in values {
        let text = match value {
            Json::String(value) if !saw_number => {
                saw_string = true;
                value
            }
            Json::Number(value) if !saw_string => {
                saw_number = true;
                format_json_sum_number(&value)
            }
            Json::Bool(_) | Json::Null | Json::Array(_) | Json::Object(_) => {
                return Err(EvalError::Unsupported(
                    "JSON_SUM_CRC32 requires scalar array values",
                ));
            }
            Json::String(_) | Json::Number(_) => {
                return Err(EvalError::Unsupported(
                    "JSON_SUM_CRC32 requires homogeneous array values",
                ));
            }
        };
        sum = sum.wrapping_add(i64::from(crc32_ieee(text.as_bytes())));
    }
    Ok(Datum::Int(sum))
}

/// Go's `%v` formatting for the integer/ordinary-double rows used by
/// `TestJSONSumCrc32`: unlike BinaryJSON text, a float integral value is
/// rendered as `1`, not `1.0`.  Rust's shortest `f64` display has the same
/// spelling for these source vectors.
fn format_json_sum_number(number: &Number) -> String {
    if let Some(integer) = number.as_i64() {
        return integer.to_string();
    }
    if let Some(integer) = number.as_u64() {
        return integer.to_string();
    }
    let value = number
        .as_f64()
        .expect("serde JSON numbers are finite f64 here");
    if value == 0.0 {
        return "0".to_string();
    }
    let mut rendered = value.to_string();
    let abs = value.abs();
    if !(1e-4..1e6).contains(&abs) {
        // Rust and Go both provide shortest round-tripping decimals, but
        // their fixed/scientific cutover differs.  Normalize the fixed Rust
        // spelling to Go's `%g`/`%v` threshold and two-digit exponent rule.
        let negative = rendered.starts_with('-');
        if negative {
            rendered.remove(0);
        }
        let (integer, fraction) = rendered
            .split_once('.')
            .map_or((rendered.as_str(), ""), |(integer, fraction)| {
                (integer, fraction)
            });
        let digits = format!("{integer}{fraction}");
        let first = digits
            .bytes()
            .position(|digit| digit != b'0')
            .expect("nonzero float has a significant digit");
        let exponent = if integer != "0" {
            integer.len() as i32 - first as i32 - 1
        } else {
            -(first as i32 - integer.len() as i32 + 1)
        };
        let mut mantissa = digits[first..].trim_end_matches('0').to_string();
        if mantissa.len() > 1 {
            mantissa.insert(1, '.');
        }
        rendered = format!(
            "{}{}e{:+03}",
            if negative { "-" } else { "" },
            mantissa,
            exponent
        );
    }
    rendered
}

/// IEEE CRC32, matching `hash/crc32.ChecksumIEEE` used by the Go builtin.
fn crc32_ieee(bytes: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFF_u32;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

/// The value-side coercion used by both array mutation signatures.  This is
/// deliberately distinct from `parse_json_value_argument`: Go disables
/// `ParseToJSONFlag` for these arguments, so strings remain JSON strings.
fn json_mutation_value_argument(value: &Datum) -> Result<Json, EvalError> {
    match value {
        Datum::Null => Ok(Json::Null),
        Datum::String(s) => Ok(Json::String(
            s.as_utf8()
                .map_err(|_| EvalError::Unsupported("invalid UTF-8 JSON value"))?
                .to_owned(),
        )),
        // Go preserves binary strings as opaque JSON values.  There is no
        // opaque/typed JSON variant in this evaluator, so rejecting bytes is
        // safer than silently turning them into ordinary UTF-8 strings.
        Datum::Bytes(_) => Err(EvalError::Unsupported(
            "binary JSON mutation values are outside this datum domain",
        )),
        Datum::Int(value) => Ok(Json::Number((*value).into())),
        Datum::UInt(value) => Ok(Json::Number((*value).into())),
        Datum::Real(value) => Number::from_f64(*value)
            .map(Json::Number)
            .ok_or(EvalError::FloatOverflow),
        Datum::Decimal(value) => parse_json(&value.to_string()),
        Datum::MinNotNull | Datum::MaxValue => {
            Err(EvalError::Unsupported("range sentinel JSON mutation value"))
        }
    }
}

fn append_at_path(value: &mut Json, legs: &[PathLeg], appended: &Json) -> bool {
    let Some((first, rest)) = legs.split_first() else {
        append_json_value(value, appended);
        return true;
    };
    match first {
        PathLeg::Key(key) => match value {
            Json::Object(object) => object
                .get_mut(key)
                .is_some_and(|child| append_at_path(child, rest, appended)),
            _ => false,
        },
        PathLeg::Array(ArraySelection::Index(index)) => match value {
            Json::Array(values) => {
                let Some(index) = resolve_array_index(*index, values.len()) else {
                    return false;
                };
                append_at_path(&mut values[index], rest, appended)
            }
            // BinaryJSON.Extract treats [0] and [last] as selecting a scalar
            // itself.  Preserve that source behavior for nested append paths.
            _ if *index == 0 || *index == -1 => append_at_path(value, rest, appended),
            _ => false,
        },
        _ => false,
    }
}

fn append_json_value(target: &mut Json, appended: &Json) {
    if let Json::Array(values) = target {
        values.push(appended.clone());
        return;
    }
    let original = std::mem::replace(target, Json::Null);
    *target = Json::Array(vec![original, appended.clone()]);
}

fn insert_at_path(document: &mut Json, legs: &[PathLeg], index: i64, value: &Json) -> bool {
    let Some((PathLeg::Array(ArraySelection::Index(_)), parent_legs)) = legs.split_last() else {
        return false;
    };
    let Some(parent) = descend_exact_mut(document, parent_legs) else {
        return false;
    };
    let Json::Array(values) = parent else {
        return false;
    };
    let len = i64::try_from(values.len()).unwrap_or(i64::MAX);
    let index = if index < 0 {
        len.saturating_add(index).max(0)
    } else {
        index
    }
    .min(len) as usize;
    values.insert(index, value.clone());
    true
}

fn descend_exact_mut<'a>(value: &'a mut Json, legs: &[PathLeg]) -> Option<&'a mut Json> {
    let Some((first, rest)) = legs.split_first() else {
        return Some(value);
    };
    if matches!(first, PathLeg::Array(ArraySelection::Index(index)) if *index == 0 || *index == -1)
    {
        return descend_exact_mut(value, rest);
    }
    match first {
        PathLeg::Key(key) => {
            let Json::Object(object) = value else {
                return None;
            };
            object
                .get_mut(key)
                .and_then(|child| descend_exact_mut(child, rest))
        }
        PathLeg::Array(ArraySelection::Index(index)) => {
            let Json::Array(values) = value else {
                return None;
            };
            let index = resolve_array_index(*index, values.len())?;
            descend_exact_mut(&mut values[index], rest)
        }
        _ => None,
    }
}

fn remove_path(value: &mut Json, legs: &[PathLeg]) -> bool {
    let Some((first, rest)) = legs.split_first() else {
        return false;
    };
    if rest.is_empty() {
        return match (value, first) {
            (Json::Object(object), PathLeg::Key(key)) => object.remove(key).is_some(),
            (Json::Array(values), PathLeg::Array(ArraySelection::Index(index))) => {
                let Some(index) = resolve_array_index(*index, values.len()) else {
                    return false;
                };
                values.remove(index);
                true
            }
            _ => false,
        };
    }

    match (value, first) {
        (Json::Object(object), PathLeg::Key(key)) => object
            .get_mut(key)
            .is_some_and(|child| remove_path(child, rest)),
        (Json::Array(values), PathLeg::Array(ArraySelection::Index(index))) => {
            let Some(index) = resolve_array_index(*index, values.len()) else {
                return false;
            };
            remove_path(&mut values[index], rest)
        }
        _ => false,
    }
}

fn resolve_array_index(index: i64, len: usize) -> Option<usize> {
    let len = i64::try_from(len).ok()?;
    let index = if index < 0 {
        len.checked_add(index)?
    } else {
        index
    };
    (index >= 0 && index < len).then_some(index as usize)
}

/// The ordinary JSON cast used by the Go signatures.  The seed evaluator has
/// no JSON datum variant, so textual values are parsed and scalar datums are
/// lifted to their equivalent JSON scalar without pretending bytes are text.
fn parse_json_value_argument(value: &Datum) -> Result<Json, EvalError> {
    match value {
        Datum::Null => Err(EvalError::Unsupported("JSON value is NULL")),
        Datum::String(s) => parse_json(
            s.as_utf8()
                .map_err(|_| EvalError::Unsupported("invalid UTF-8 JSON value"))?,
        ),
        Datum::Int(value) => Ok(Json::Number((*value).into())),
        Datum::UInt(value) => Ok(Json::Number((*value).into())),
        Datum::Real(value) => Number::from_f64(*value)
            .map(Json::Number)
            .ok_or(EvalError::FloatOverflow),
        Datum::Decimal(value) => parse_json(&value.to_string()),
        Datum::Bytes(_) => Err(EvalError::Unsupported("JSON value requires text")),
        Datum::MinNotNull | Datum::MaxValue => {
            Err(EvalError::Unsupported("range sentinel JSON value"))
        }
    }
}

/// The candidate-side cast in `JSON_MEMBER_OF`: SQL strings become JSON
/// strings, while numeric datums retain their JSON numeric kind.
fn json_value_argument(value: &Datum) -> Result<Json, EvalError> {
    match value {
        Datum::Null => Err(EvalError::Unsupported("JSON value is NULL")),
        Datum::String(s) => Ok(Json::String(
            s.as_utf8()
                .map_err(|_| EvalError::Unsupported("invalid UTF-8 JSON value"))?
                .to_owned(),
        )),
        Datum::Int(value) => Ok(Json::Number((*value).into())),
        Datum::UInt(value) => Ok(Json::Number((*value).into())),
        Datum::Real(value) => Number::from_f64(*value)
            .map(Json::Number)
            .ok_or(EvalError::FloatOverflow),
        Datum::Decimal(value) => parse_json(&value.to_string()),
        Datum::Bytes(_) => Err(EvalError::Unsupported("JSON value requires text")),
        Datum::MinNotNull | Datum::MaxValue => {
            Err(EvalError::Unsupported("range sentinel JSON value"))
        }
    }
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

/// The `EvalJSON` coercion used by JSON document arguments in the Go
/// signatures above.  The public seed domain has no JSON variant, so only an
/// SQL string can carry a JSON document here; numeric arguments are rejected
/// honestly instead of being silently reinterpreted as JSON text.
/// Parses the seed evaluator's one representable ETJson argument domain.
///
/// TiDB's JSON signatures receive a binary JSON value. The public Rust value
/// domain has no equivalent variant, so only a SQL string may carry a JSON
/// document; keeping that restriction centralized prevents sibling JSON
/// leaves from silently coercing numeric values into different documents.
pub(crate) fn parse_json_document_argument(v: &Datum) -> Result<Option<Json>, EvalError> {
    match v {
        Datum::Null => Ok(None),
        Datum::String(s) => parse_json(
            s.as_utf8()
                .map_err(|_| EvalError::Unsupported("invalid UTF-8 string datum"))?,
        )
        .map(Some),
        Datum::Bytes(_)
        | Datum::Int(_)
        | Datum::UInt(_)
        | Datum::Decimal(_)
        | Datum::Real(_)
        | Datum::MinNotNull
        | Datum::MaxValue => Err(EvalError::Unsupported("JSON document requires string")),
    }
}

fn parse_json(s: &str) -> Result<Json, EvalError> {
    serde_json::from_str(s).map_err(|_| EvalError::Unsupported("invalid JSON document"))
}

/// A parsed TiDB JSON path.  This is a direct structural port of
/// `types.JSONPathExpression` / `ParseJSONPathExpr` in
/// `pkg/types/json_path_expr.go`; ranges and wildcards carry the same
/// `CouldMatchMultipleValues` flag used by JSON_LENGTH.
#[derive(Debug)]
struct JsonPath {
    legs: Vec<PathLeg>,
    could_match_multiple: bool,
}

#[derive(Debug)]
enum PathLeg {
    Key(String),
    KeyWildcard,
    Array(ArraySelection),
    Recursive,
}

#[derive(Debug)]
enum ArraySelection {
    All,
    Index(i64),
    Range(i64, i64),
}

/// Parses TiDB's JSON path grammar.  The argument is already a Rust `str`,
/// so its runes have the same Unicode-level behavior as Go's `[]rune` parser.
fn parse_path(input: &str) -> Result<JsonPath, EvalError> {
    let chars: Vec<char> = input.chars().collect();
    let mut cursor = 0;
    skip_space(&chars, &mut cursor);
    if chars.get(cursor) != Some(&'$') {
        return Err(EvalError::Unsupported("invalid JSON path"));
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
                            return Err(EvalError::Unsupported("invalid JSON path"));
                        }
                        skip_space(&chars, &mut cursor);
                        let end = parse_index(&chars, &mut cursor)?;
                        if (start >= 0 && end >= 0 || start < 0 && end < 0) && start > end {
                            return Err(EvalError::Unsupported("invalid JSON path"));
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
                    return Err(EvalError::Unsupported("invalid JSON path"));
                }
                cursor += 1;
                legs.push(PathLeg::Array(selection));
            }
            '*' => {
                if chars.get(cursor + 1) != Some(&'*') || chars.get(cursor + 2) == Some(&'*') {
                    return Err(EvalError::Unsupported("invalid JSON path"));
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
            _ => return Err(EvalError::Unsupported("invalid JSON path")),
        }
        skip_space(&chars, &mut cursor);
    }
    if matches!(legs.last(), Some(PathLeg::Recursive)) {
        return Err(EvalError::Unsupported("invalid JSON path"));
    }
    Ok(JsonPath {
        legs,
        could_match_multiple,
    })
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
                return serde_json::from_str(&encoded)
                    .map_err(|_| EvalError::Unsupported("invalid JSON path"));
            }
        }
        return Err(EvalError::Unsupported("invalid JSON path"));
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
        return Err(EvalError::Unsupported("invalid JSON path"));
    }
    Ok(key)
}

fn is_ecmascript_identifier(value: &str) -> bool {
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
        return Err(EvalError::Unsupported("invalid JSON path"));
    }
    chars[start..*cursor]
        .iter()
        .collect::<String>()
        .parse()
        .map_err(|_| EvalError::Unsupported("invalid JSON path"))
}

/// Port of `BinaryJSON.Extract`.  References keep the source tree intact,
/// allowing the same pointer-identity de-duplication that TiDB applies per
/// path while walking `**` recursively.
fn extract(document: &Json, paths: &[JsonPath]) -> Option<Json> {
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

fn array_range(selection: &ArraySelection, len: usize) -> (usize, usize) {
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

fn select_non_array(selection: &ArraySelection) -> bool {
    match *selection {
        ArraySelection::Index(index) => index == 0 || index == -1,
        ArraySelection::Range(start, end) => start == 0 && end >= -1,
        ArraySelection::All => false,
    }
}

/// TiDB's `BinaryJSON.MarshalJSON` text form: arrays/objects have spaces,
/// object keys are sorted by UTF-8 bytes, and parsed floating JSON numbers
/// use `marshalFloat64To`'s `DOUBLE` representation (not their input lexeme).
fn format_json(value: &Json) -> String {
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

#[cfg(test)]
mod tests {
    use super::{dispatch, format_json, parse_path};
    use crate::Datum;
    use serde_json::json;

    fn call(name: &str, vals: &[Datum]) -> Datum {
        dispatch(name, vals)
            .expect("JSON family should own name/arity")
            .expect("Go-derived valid vector should evaluate")
    }

    fn s(value: &str) -> Datum {
        Datum::new_string(value.to_string())
    }

    fn call_result(name: &str, vals: &[Datum]) -> Result<Datum, crate::EvalError> {
        dispatch(name, vals).expect("JSON family should own name/arity")
    }

    /// Vectors from `TestJSONType` and `TestJSONQuote` in
    /// `pkg/expression/builtin_json_test.go`.  `TestJSONValid` has its own
    /// source-shaped table below so its signature boundary remains visible.
    #[test]
    fn scalar_go_vectors() {
        assert_eq!(call("JSON_TYPE", &[Datum::Null]), Datum::Null);
        for (input, want) in [
            ("3", "INTEGER"),
            ("3.0", "DOUBLE"),
            ("null", "NULL"),
            ("true", "BOOLEAN"),
            ("[]", "ARRAY"),
            ("{}", "OBJECT"),
        ] {
            assert_eq!(call("JSON_TYPE", &[s(input)]), s(want));
        }
    }

    #[test]
    fn json_scalar_source_wave_boundaries() {
        // Keep the four source-owned scalar leaves' non-text and byte
        // boundaries explicit without pretending this value domain carries
        // typed BinaryJSON or charset/session warning state.
        assert!(call_result("JSON_TYPE", &[Datum::Int(3)]).is_err());
        assert!(call_result("JSON_TYPE", &[s("a")]).is_err());
        assert!(dispatch("JSON_TYPE", &[]).is_none());

        assert_eq!(call("JSON_QUOTE", &[Datum::Null]), Datum::Null);
        assert!(call_result("JSON_QUOTE", &[Datum::new_bytes([0xff])]).is_err());

        assert_eq!(call("JSON_UNQUOTE", &[Datum::Null]), Datum::Null);
        assert!(call_result("JSON_UNQUOTE", &[Datum::Int(3)]).is_err());
        assert!(call_result("JSON_UNQUOTE", &[Datum::new_bytes([0xff])]).is_err());

        assert_eq!(
            call("JSON_VALID", &[Datum::new_bytes([0xff])]),
            Datum::Int(0)
        );
        assert_eq!(call("JSON_VALID", &[Datum::Int(3)]), Datum::Int(0));
    }

    /// Source-shaped scalar rows from `TestJSONArray` and `TestJSONObject` in
    /// `pkg/expression/builtin_json_test.go:379` and `:404`.  Strings remain
    /// JSON strings, numeric/NULL values retain their scalar kinds, odd object
    /// arity and NULL keys are errors, and the typed boolean/JSON rows remain
    /// explicit value-domain boundaries.
    #[test]
    fn json_array_object_go_vectors() {
        assert_eq!(call("JSON_ARRAY", &[]), s("[]"));
        assert_eq!(call("JSON_ARRAY", &[Datum::Int(1)]), s("[1]"));
        assert_eq!(
            call(
                "JSON_ARRAY",
                &[Datum::Null, s("a"), Datum::Int(3), s(r#"{"a": "b"}"#),],
            ),
            s(r#"[null, "a", 3, "{\"a\": \"b\"}"]"#),
        );
        assert_eq!(call("JSON_ARRAY", &[Datum::UInt(2)]), s("[2]"));
        assert_eq!(call("JSON_ARRAY", &[Datum::Real(1.5)]), s("[1.5]"));
        assert!(call_result("JSON_ARRAY", &[Datum::new_bytes(b"x")]).is_err());

        assert_eq!(call("JSON_OBJECT", &[]), s("{}"));
        assert!(call_result(
            "JSON_OBJECT",
            &[Datum::Int(1), Datum::Int(2), Datum::Int(3)]
        )
        .is_err());
        assert_eq!(
            call(
                "JSON_OBJECT",
                &[Datum::Int(1), Datum::Int(2), s("hello"), Datum::Null],
            ),
            s(r#"{"1": 2, "hello": null}"#),
        );
        assert!(call_result("JSON_OBJECT", &[Datum::Null, Datum::Int(2)]).is_err());
        assert_eq!(
            call("JSON_OBJECT", &[Datum::new_bytes(b"k"), Datum::Int(2)]),
            s(r#"{"k": 2}"#),
        );
    }

    /// Complete representable table from `TestJSONValid` in
    /// `pkg/expression/builtin_json_test.go:1176`.  The Go function class has
    /// three signatures: typed JSON is already valid, strings are parsed as
    /// JSON documents, and every other SQL type returns zero.  This frozen
    /// evaluator has no typed BinaryJSON datum, so the string and "others"
    /// rows are the complete executable value-domain table; the typed JSON
    /// branch remains an explicit boundary rather than being guessed from a
    /// numeric or string value.
    #[test]
    fn json_valid_go_vectors() {
        for (input, want) in [
            ("{\"a\":1}", 1),
            ("hello", 0),
            ("\"hello\"", 1),
            ("null", 1),
            ("{}", 1),
            ("[]", 1),
            ("2", 1),
            ("2.5", 1),
            ("2019-8-19", 0),
            ("\"2019-8-19\"", 1),
        ] {
            assert_eq!(call("JSON_VALID", &[s(input)]), Datum::Int(want));
        }
        assert_eq!(call("JSON_VALID", &[Datum::Int(2)]), Datum::Int(0));
        assert_eq!(call("JSON_VALID", &[Datum::Real(2.5)]), Datum::Int(0));
        assert_eq!(call("JSON_VALID", &[Datum::Null]), Datum::Null);
    }

    /// All successful vectors in `TestJSONQuote` and `TestJSONUnquote`
    /// (`pkg/expression/builtin_json_test.go`).  The two malformed-root
    /// vectors are deliberately checked as errors rather than captured in
    /// the result corpus (where `ERR` would be skipped by the harness).
    #[test]
    fn quote_and_unquote_go_vectors() {
        for (input, want) in [
            ("", "\"\""),
            ("\"\"", "\"\\\"\\\"\""),
            ("a", "\"a\""),
            ("3", "\"3\""),
            (r#"{"a": "b"}"#, "\"{\\\"a\\\": \\\"b\\\"}\""),
            (r#"{"a":     "b"}"#, "\"{\\\"a\\\":     \\\"b\\\"}\""),
            (
                "hello,\"quoted string\",world",
                "\"hello,\\\"quoted string\\\",world\"",
            ),
            ("hello,\"宽字符\",world", "\"hello,\\\"宽字符\\\",world\""),
            (
                "Invalid Json string\tis OK",
                "\"Invalid Json string\\tis OK\"",
            ),
            (r#"1\u2232\u22322"#, "\"1\\\\u2232\\\\u22322\""),
        ] {
            assert_eq!(call("JSON_QUOTE", &[s(input)]), s(want));
        }
        assert_eq!(call("JSON_QUOTE", &[Datum::Null]), Datum::Null);

        for (input, want) in [
            ("", ""),
            ("\"\"", ""),
            ("''", "''"),
            ("3", "3"),
            (r#"{"a": "b"}"#, r#"{"a": "b"}"#),
            (r#"{"a":     "b"}"#, r#"{"a":     "b"}"#),
            (
                "\"hello,\\\"quoted string\\\",world\"",
                "hello,\"quoted string\",world",
            ),
            ("\"hello,\\\"宽字符\\\",world\"", "hello,\"宽字符\",world"),
            ("Invalid Json string\\tis OK", "Invalid Json string\\tis OK"),
            ("\"1\\\\u2232\\\\u22322\"", r#"1\u2232\u22322"#),
            (
                "\"[{\\\"x\\\":\\\"{\\\\\\\"y\\\\\\\":12}\\\"}]\"",
                r#"[{"x":"{\"y\":12}"}]"#,
            ),
            (
                r#"[{\"x\":\"{\\\"y\\\":12}\"}]"#,
                r#"[{\"x\":\"{\\\"y\\\":12}\"}]"#,
            ),
            ("\"a\"", "a"),
        ] {
            assert_eq!(call("JSON_UNQUOTE", &[s(input)]), s(want));
        }
        assert_eq!(call("JSON_UNQUOTE", &[Datum::Null]), Datum::Null);
        assert!(dispatch("JSON_UNQUOTE", &[s("\"\"a\"\"")])
            .expect("JSON_UNQUOTE is owned")
            .is_err());
    }

    /// Vectors from `TestJSONLength` and `TestBinaryJSONExtract` in TiDB's
    /// expression/types test suites.
    #[test]
    fn containers_paths_and_binary_json_format_match_go() {
        assert_eq!(
            call("JSON_LENGTH", &[s("[1,2,[1,[5,[3]]]]"), s("$[2]")]),
            Datum::Int(2)
        );
        assert_eq!(
            call(
                "JSON_EXTRACT",
                &[
                    s("{\"a\":[1,\"2\",{\"aa\":\"bb\"},4.0,{\"aa\":\"cc\"}]}"),
                    s("$.a[*].aa")
                ]
            ),
            s("[\"bb\", \"cc\"]")
        );
        assert_eq!(
            call(
                "JSON_EXTRACT",
                &[s("[[0,1],[2,3],[4,[5,6]]]"), s("$[1 to last][1 to last]")]
            ),
            s("[3, [5, 6]]")
        );
        assert_eq!(
            format_json(&json!({"b": 2, "a": 1})),
            "{\"a\": 1, \"b\": 2}"
        );
        assert!(parse_path("$**").is_err());
        assert!(parse_path("$.宽").is_err());
        assert!(parse_path("$.\"宽\"").is_ok());
    }

    /// Complete representable table from `TestJSONExtract` in
    /// `pkg/expression/builtin_json_test.go:233`.  The first row exercises
    /// Go's early NULL propagation (the NULL document is returned before its
    /// NULL path is parsed); the second row combines two exact paths into the
    /// result array; the final row preserves invalid-path errors.  Typed
    /// BinaryJSON, warning/session state, function-class construction, and
    /// vectorized execution remain outside this scalar value domain.
    #[test]
    fn json_extract_go_vectors() {
        assert_eq!(
            call("JSON_EXTRACT", &[Datum::Null, Datum::Null]),
            Datum::Null
        );
        let document = r#"{"a": [{"aa": [{"aaa": 1}]}], "aaa": 2}"#;
        assert_eq!(
            call(
                "JSON_EXTRACT",
                &[s(document), s("$.a[0].aa[0].aaa"), s("$.aaa")]
            ),
            s("[1, 2]")
        );
        assert!(call_result(
            "JSON_EXTRACT",
            &[s(document), s("$.a[0].aa[0].aaa"), s("$InvalidPath")]
        )
        .is_err());
    }

    /// The representable rows from `TestJSONMemberOf` in
    /// `pkg/expression/builtin_json_test.go`.  A candidate SQL string is a
    /// JSON string (the Go signature disables ParseToJSONFlag for arg 0),
    /// which is why `"1" MEMBER OF [1]` differs from `"1" MEMBER OF ["1"]`.
    #[test]
    fn json_member_of_go_vectors() {
        for (candidate, document, want) in [
            ("1", "[1, 2]", 0),
            ("1", "[1]", 0),
            ("1", "[0]", 0),
            ("1", "[[1]]", 0),
            ("1", "[\"1\"]", 1),
            (r#"{"a":1}"#, r#"{"a":1}"#, 0),
            (r#"{"a":1}"#, r#"[{"a":1}]"#, 0),
            (r#"{"a":1}"#, r#"[{"a":1},1]"#, 0),
            (r#"{"a":1}"#, r#"["{\"a\":1}"]"#, 1),
            (r#"{"a":1}"#, r#"["{\"a\":1}",1]"#, 1),
        ] {
            assert_eq!(
                call("JSON_MEMBER_OF", &[s(candidate), s(document)],),
                Datum::Int(want),
                "JSON_MEMBER_OF({candidate:?}, {document:?})"
            );
        }
        for (document, want) in [
            ("[1, 2]", 1),
            ("[1]", 1),
            ("[0]", 0),
            ("[1]", 1),
            ("[[1]]", 0),
        ] {
            assert_eq!(
                call("JSON_MEMBER_OF", &[Datum::Int(1), s(document)]),
                Datum::Int(want),
                "JSON_MEMBER_OF(1, {document:?})"
            );
        }
        assert_eq!(
            call("JSON_MEMBER_OF", &[Datum::Null, s("[1]")]),
            Datum::Null
        );
        assert!(call_result("JSON_MEMBER_OF", &[s("1"), s("a:1")]).is_err());
        assert_eq!(
            call("JSON_MEMBER_OF", &[Datum::Int(1), s("[1]")]),
            Datum::Int(1)
        );
    }

    /// Representable rows from `TestJSONContains`, including recursive
    /// object/array containment, scalar equality, path extraction, NULL and
    /// missing-path propagation, and wildcard rejection.
    #[test]
    fn json_contains_go_vectors() {
        for (document, candidate, want) in [
            ("[1,2,[1,[5,[3]]]]", "[1,3]", 1),
            ("[1,2,[1,[5,{\"a\":[2,3]}]]]", "[1,{\"a\":[3]}]", 1),
            ("[ {\"a\":1} ]", "{\"a\":1}", 1),
            ("{}", "{}", 1),
            ("{\"a\":1}", "{}", 1),
            ("{\"a\":1}", "1", 0),
            ("{\"a\":[1]}", "[1]", 0),
            ("{\"b\":2,\"c\":3}", "{\"c\":3}", 1),
            ("1", "1", 1),
            ("[1]", "1", 1),
            ("[1,2]", "[1]", 1),
            ("[1,2]", "[1,3]", 0),
            ("[1,2]", "[\"1\"]", 0),
            ("[1,2,[1,3]]", "[1,3]", 1),
            ("[1,2,[1,[5,[3]]]]", "[1,3]", 1),
            ("[{\"a\":1,\"b\":2}]", "{\"a\":1}", 1),
            ("[{\"a\":{\"a\":1},\"b\":2}]", "{\"a\":1}", 0),
        ] {
            assert_eq!(
                call("JSON_CONTAINS", &[s(document), s(candidate)]),
                Datum::Int(want),
                "JSON_CONTAINS({document:?}, {candidate:?})"
            );
        }
        for (document, candidate, path, want) in [
            ("[1,2,[1,[5,[3]]]]", "[1,3]", "$[2]", 1),
            ("[1,2,[1,[5,{\"a\":[2,3]}]]]", "[1,{\"a\":[3]}]", "$[2]", 1),
            ("[{\"a\":1}]", "{\"a\":1}", "$", 1),
            ("[{\"a\":1,\"b\":2}]", "{\"a\":1}", "$", 1),
        ] {
            assert_eq!(
                call("JSON_CONTAINS", &[s(document), s(candidate), s(path)]),
                Datum::Int(want),
                "JSON_CONTAINS({document:?}, {candidate:?}, {path:?})"
            );
        }
        assert_eq!(
            call("JSON_CONTAINS", &[s("{\"a\":1}"), s("1"), s("$.c")]),
            Datum::Null
        );
        // The Go test records this row as expected integer 0 but does not
        // assert non-NULL; the source evaluator returns NULL for a path that
        // misses the array root, which is the actual `Extract` contract.
        assert_eq!(
            call(
                "JSON_CONTAINS",
                &[s("[{\"a\":{\"a\":1},\"b\":2}]"), s("{\"a\":1}"), s("$.a"),],
            ),
            Datum::Null
        );
        assert_eq!(call("JSON_CONTAINS", &[Datum::Null, s("1")]), Datum::Null);
        for path in ["$.*", "$[*]", "$**.a"] {
            assert!(call_result("JSON_CONTAINS", &[s("{\"a\":1}"), s("1"), s(path)]).is_err());
        }
        assert!(call_result("JSON_CONTAINS", &[s("a:1"), s("1")]).is_err());
    }

    #[test]
    fn json_contains_source_wave_missing_rows() {
        // Exact NULL/missing-path/invalid-argument rows from TestJSONContains
        // that are separate from the main recursive containment table above.
        assert_eq!(
            call("JSON_CONTAINS", &[Datum::Null, s("1"), s("$.c")],),
            Datum::Null
        );
        assert_eq!(
            call(
                "JSON_CONTAINS",
                &[s(r#"{"a":[1,2,{"aa":"xx"}]}"#), Datum::Null, s("$.a[3]")],
            ),
            Datum::Null
        );
        assert_eq!(
            call(
                "JSON_CONTAINS",
                &[s(r#"{"a":[1,2,{"aa":"xx"}]}"#), s("1"), Datum::Null],
            ),
            Datum::Null
        );
        for (document, path) in [
            (r#"{"a":[1,2,{"aa":"xx"}]}"#, "$.c"),
            (r#"{"a":[1,2,{"aa":"xx"}]}"#, "$.a[3]"),
            (r#"{"a":[1,2,{"aa":"xx"}]}"#, "$.a[2].b"),
        ] {
            assert_eq!(
                call("JSON_CONTAINS", &[s(document), s("1"), s(path)]),
                Datum::Null,
                "JSON_CONTAINS({document:?}, 1, {path:?})",
            );
        }
        assert!(call_result("JSON_CONTAINS", &[s("[1,2,[1,3]]"), s("a:1")]).is_err());
        for args in [
            vec![Datum::Int(1), s("")],
            vec![Datum::Real(0.05), s("")],
            vec![s(""), Datum::Int(1)],
            vec![s(""), Datum::Real(0.05)],
        ] {
            assert!(call_result("JSON_CONTAINS", &args).is_err());
        }
    }

    /// Representable rows from `TestJSONOverlaps`, preserving the Go rule that
    /// arrays compare their scalar/element values while objects overlap only
    /// when a shared key has an equal value.
    #[test]
    fn json_overlaps_go_vectors() {
        for (left, right, want) in [
            ("[1,2]", "[2,3]", 1),
            ("[1,2]", "[2]", 1),
            ("[1,2]", "2", 1),
            ("[{\"a\":1}]", "{\"a\":1}", 1),
            ("[{\"a\":1}]", "{\"a\":2}", 0),
            ("{\"a\":[1,2]}", "{\"a\":[1]}", 0),
            ("[1,1,1]", "1", 1),
            ("1", "1", 1),
            ("0", "1", 0),
            ("[[1,2],3]", "[1,3]", 1),
            ("[4,5,\"6\",7]", "6", 0),
            ("[4,5,6,7]", "\"6\"", 0),
            ("[2,3]", "[1,2]", 1),
            ("2", "[1,2]", 1),
            ("{\"a\":1}", "[{\"a\":1}]", 1),
            ("{\"a\":1,\"b\":2}", "[{\"a\":1}]", 0),
            ("{\"a\":2}", "[{\"a\":1}]", 0),
            ("{\"a\":[1]}", "{\"a\":[1,2]}", 0),
            ("1", "[1,1,1]", 1),
            ("[1,[2,3]]", "[[1,2],3]", 0),
            ("[1,3]", "[[1,2],3]", 1),
            (
                "{\"a\":5,\"e\":10,\"f\":1,\"d\":20}",
                "{\"a\":1,\"b\":10,\"d\":10}",
                0,
            ),
            ("6", "[4,5,\"6\",7]", 0),
            ("\"6\"", "[4,5,6,7]", 0),
        ] {
            assert_eq!(
                call("JSON_OVERLAPS", &[s(left), s(right)]),
                Datum::Int(want),
                "JSON_OVERLAPS({left:?}, {right:?})"
            );
        }
        assert_eq!(call("JSON_OVERLAPS", &[Datum::Null, s("1")]), Datum::Null);
        assert!(call_result("JSON_OVERLAPS", &[s("a:1"), s("1")]).is_err());
    }

    /// Complete representable table from `TestJSONContainsPath` in
    /// `pkg/expression/builtin_json_test.go`.  The path-existence operation
    /// deliberately uses the same extraction walk as JSON_EXTRACT, so object
    /// and array wildcards, one/all short-circuiting, NULL propagation, and
    /// invalid-document/path errors remain observable in this scalar domain.
    #[test]
    fn json_contains_path_go_vectors() {
        let document = r#"{"a": 1, "b": 2, "c": {"d": 4}}"#;
        for (mode, path, want) in [
            ("one", "$.c.d", 1),
            ("one", "$.a.d", 0),
            ("all", "$.c.d", 1),
            ("all", "$.a.d", 0),
            ("one", "$.*", 1),
            ("one", "$[*]", 0),
            ("all", "$.*", 1),
            ("all", "$[*]", 0),
            ("ONE", "$.c.d", 1),
            ("ALL", "$.c.d", 1),
        ] {
            assert_eq!(
                call("JSON_CONTAINS_PATH", &[s(document), s(mode), s(path)]),
                Datum::Int(want),
                "JSON_CONTAINS_PATH({mode:?}, {path:?})",
            );
        }
        for (mode, paths, want) in [
            ("one", ["$.a", "$.e"].as_slice(), 1),
            ("one", ["$.a", "$.c"].as_slice(), 1),
            ("all", ["$.a", "$.e"].as_slice(), 0),
            ("all", ["$.a", "$.c"].as_slice(), 1),
            ("One", ["$.a", "$.e"].as_slice(), 1),
            ("aLl", ["$.a", "$.e"].as_slice(), 0),
        ] {
            let mut args = vec![s(document), s(mode)];
            args.extend(paths.iter().map(|path| s(path)));
            assert_eq!(
                call("JSON_CONTAINS_PATH", &args),
                Datum::Int(want),
                "JSON_CONTAINS_PATH({mode:?}, {paths:?})",
            );
        }

        for args in [
            vec![Datum::Null, s("one"), s("$.c")],
            vec![Datum::Null, s("all"), s("$.c")],
            vec![s(document), Datum::Null, s("$.a[3]")],
            vec![s(document), s("one"), Datum::Null],
            vec![s(document), s("all"), Datum::Null],
        ] {
            assert_eq!(
                call("JSON_CONTAINS_PATH", &args),
                Datum::Null,
                "JSON_CONTAINS_PATH NULL vector",
            );
        }

        for args in [
            vec![s(r#"{"a": 1"#), s("one"), s("$.a")],
            vec![s(r#"{"a": 1"#), s("all"), s("$.a")],
            vec![s(document), s("test"), s("$.a")],
        ] {
            assert!(
                call_result("JSON_CONTAINS_PATH", &args).is_err(),
                "JSON_CONTAINS_PATH invalid vector",
            );
        }
    }

    /// Complete representable table from `TestJSONKeys` in
    /// `pkg/expression/builtin_json_test.go`.  JSON keys are returned only
    /// for object roots or exact object path selections; scalar/array/missing
    /// targets are NULL and wildcard/range paths reject multiple selection.
    #[test]
    fn json_keys_go_vectors() {
        for args in [
            vec![Datum::Null],
            vec![Datum::Null, s("$.c")],
            vec![s(r#"{"a": 1}"#), Datum::Null],
            vec![Datum::Null, Datum::Null],
        ] {
            assert_eq!(call("JSON_KEYS", &args), Datum::Null);
        }
        for document in ["1", r#""str""#, "true", "null", "[1, 2]", r#"["1", "2"]"#] {
            assert_eq!(call("JSON_KEYS", &[s(document)]), Datum::Null);
        }
        for (document, want) in [
            ("{}", "[]"),
            (r#"{"a": 1}"#, r#"["a"]"#),
            (r#"{"a": 1, "b": 2}"#, r#"["a", "b"]"#),
            (r#"{"a": {"c": 3}, "b": 2}"#, r#"["a", "b"]"#),
        ] {
            assert_eq!(call("JSON_KEYS", &[s(document)]), s(want));
        }
        for (document, path, want) in [
            (r#"{"a": 1}"#, "$.a", None),
            (r#"{"a": {"c": 3}, "b": 2}"#, "$.a", Some(r#"["c"]"#)),
            (r#"{"a": {"c": 3}, "b": 2}"#, "$.a.c", None),
            (r#"{"a": 1}"#, "$.b", None),
            (r#"{"a": {"c": 3}, "b": 2}"#, "$.c", None),
            (r#"{"a": {"c": 3}, "b": 2}"#, "$.a.d", None),
            (
                r#"[{"A1": 1, "B1": 2, "C1": 3}, {"A2": 10, "B2": 20, "C2": {"D": 4}}, {"A3": 1, "B3": 2, "C3": 6}]"#,
                "$[1]",
                Some(r#"["A2", "B2", "C2"]"#),
            ),
            (
                r#"[{"A": 1, "B": 2, "C": {"D": 3}}, {"A": 10, "B": 20, "C": {"D": 4}}, {"A": 1, "B": 2, "C": [{"D": 5}, {"E": 55}]}]"#,
                "$[last].C",
                None,
            ),
            (
                r#"[{"A": 1, "B": 2, "C": {"D": 3}}, {"A": 10, "B": 20, "C": {"D": 4}}, {"A": 1, "B": 2, "C": [{"D": 5}, {"E": 55}]}]"#,
                "$[last].C[1]",
                Some(r#"["E"]"#),
            ),
        ] {
            let result = call("JSON_KEYS", &[s(document), s(path)]);
            match want {
                Some(want) => assert_eq!(result, s(want), "JSON_KEYS path {path:?}"),
                None => assert_eq!(result, Datum::Null, "JSON_KEYS path {path:?}"),
            }
        }
        for (document, path) in [
            ("{}", "$.*"),
            (r#"{"a": 1}"#, "$.*"),
            (r#"{"a": {"c": 3}, "b": 2}"#, "$.*"),
            (r#"{"a": {"c": 3}, "b": 2}"#, "$.a.*"),
            (r#"{"a": {"c": 3}, "b": 2}"#, "$[0 to 1]"),
        ] {
            assert!(
                call_result("JSON_KEYS", &[s(document), s(path)]).is_err(),
                "JSON_KEYS wildcard/range path {path:?} must reject multiple selection"
            );
        }
    }

    /// Complete representable table from `TestJSONRemove` in
    /// `pkg/expression/builtin_json_test.go`.  Exact paths are applied in
    /// order, preserving the Go behavior where later array indexes observe
    /// earlier removals; absent paths are no-ops and wildcard/range/root
    /// paths are errors.
    #[test]
    fn json_remove_go_vectors() {
        for path in ["$", "$.*", "$[*]", "$**.a"] {
            assert!(
                call_result(
                    "JSON_REMOVE",
                    &[s(r#"{"a": [1, 2, {"aa": "xx"}]}"#), s(path)]
                )
                .is_err(),
                "JSON_REMOVE invalid path {path:?}"
            );
        }
        assert_eq!(call("JSON_REMOVE", &[Datum::Null, s("$.a")]), Datum::Null);
        assert_eq!(
            call(
                "JSON_REMOVE",
                &[s(r#"{"a": [1, 2, {"aa": "xx"}]}"#), s("$.a[2].aa")]
            ),
            s(r#"{"a": [1, 2, {}]}"#)
        );
        assert_eq!(
            call(
                "JSON_REMOVE",
                &[s(r#"{"a": [1, 2, {"aa": "xx"}]}"#), s("$.a[1]")]
            ),
            s(r#"{"a": [1, {"aa": "xx"}]}"#)
        );
        for (paths, want) in [
            (vec!["$.a[2].aa", "$.a[1]"], r#"{"a": [1, {}]}"#),
            (vec!["$.a[1]", "$.a[1].aa"], r#"{"a": [1, {}]}"#),
            (vec!["$.a[3]"], r#"{"a": [1, 2, {"aa": "xx"}]}"#),
            (vec!["$.b"], r#"{"a": [1, 2, {"aa": "xx"}]}"#),
            (vec!["$.a[3]", "$.b"], r#"{"a": [1, 2, {"aa": "xx"}]}"#),
        ] {
            let mut args = vec![s(r#"{"a": [1, 2, {"aa": "xx"}]}"#)];
            args.extend(paths.into_iter().map(s));
            assert_eq!(call("JSON_REMOVE", &args), s(want), "paths {args:?}");
        }
        assert_eq!(
            call("JSON_REMOVE", &[s(r#"{"a": 1}"#), Datum::Null]),
            Datum::Null
        );
    }

    /// Complete scalar-value-domain table from `TestJSONArrayAppend` in
    /// `pkg/expression/builtin_json_test.go:949`.  SQL strings remain JSON
    /// strings because the Go signature disables ParseToJSONFlag for values;
    /// the two source rows using an already-typed `sampleJSON` object are
    /// intentionally left as a typed-BinaryJSON boundary.
    #[allow(clippy::approx_constant)]
    #[test]
    fn json_array_append_go_vectors() {
        for (args, want) in [
            (
                vec![s(r#"{"a": 1, "b": [2, 3], "c": 4}"#), s("$.d"), s("z")],
                r#"{"a": 1, "b": [2, 3], "c": 4}"#,
            ),
            (
                vec![s(r#"{"a": 1, "b": [2, 3], "c": 4}"#), s("$"), s("w")],
                r#"[{"a": 1, "b": [2, 3], "c": 4}, "w"]"#,
            ),
            (
                vec![s(r#"{"a": 1, "b": [2, 3], "c": 4}"#), s("$"), Datum::Null],
                r#"[{"a": 1, "b": [2, 3], "c": 4}, null]"#,
            ),
            (
                vec![s(r#"{"a": 1}"#), s("$"), s(r#"{"b": 2}"#)],
                r#"[{"a": 1}, "{\"b\": 2}"]"#,
            ),
            (
                vec![s(r#"{"a": 1}"#), s("$.a"), s(r#"{"b": 2}"#)],
                r#"{"a": [1, "{\"b\": 2}"]}"#,
            ),
            (
                vec![s(r#"{"a": 1}"#), s("$.a"), s("x"), s("$.a[1]"), s("y")],
                r#"{"a": [1, ["x", "y"]]}"#,
            ),
            (vec![s(r#"null"#), s("$"), Datum::Null], r#"[null, null]"#),
            (vec![s("[]"), s("$"), Datum::Null], r#"[null]"#),
            (vec![s("{}"), s("$"), Datum::Null], r#"[{}, null]"#),
            (
                vec![s(r#"["a", ["b", "c"], "d"]"#), s("$[1]"), Datum::Int(1)],
                r#"["a", ["b", "c", 1], "d"]"#,
            ),
            (
                vec![s(r#"["a", ["b", "c"], "d"]"#), s("$[0]"), Datum::Int(2)],
                r#"[["a", 2], ["b", "c"], "d"]"#,
            ),
            (
                vec![s(r#"["a", ["b", "c"], "d"]"#), s("$[1][0]"), Datum::Int(3)],
                r#"["a", [["b", 3], "c"], "d"]"#,
            ),
            (
                vec![s(r#"{"a": 1, "b": [2, 3], "c": 4}"#), s("$.b"), s("x")],
                r#"{"a": 1, "b": [2, 3, "x"], "c": 4}"#,
            ),
            (
                vec![s(r#"{"a": 1, "b": [2, 3], "c": 4}"#), s("$.c"), s("y")],
                r#"{"a": 1, "b": [2, 3], "c": [4, "y"]}"#,
            ),
            (
                vec![s(r#"[1,2,3, {"a":[4,5,6]}]"#), s("$"), Datum::Int(7)],
                r#"[1, 2, 3, {"a": [4, 5, 6]}, 7]"#,
            ),
            (
                vec![
                    s(r#"[1,2,3, {"a":[4,5,6]}]"#),
                    s("$"),
                    Datum::Int(7),
                    s("$[3].a"),
                    Datum::Real(3.14),
                ],
                r#"[1, 2, 3, {"a": [4, 5, 6, 3.14]}, 7]"#,
            ),
            (
                vec![
                    s(r#"[1,2,3, {"a":[4,5,6]}]"#),
                    s("$"),
                    Datum::Int(7),
                    s("$[3].b"),
                    Datum::Int(8),
                ],
                r#"[1, 2, 3, {"a": [4, 5, 6]}, 7]"#,
            ),
        ] {
            assert_eq!(call("JSON_ARRAY_APPEND", &args), s(want), "args {args:?}");
        }
        for args in [
            vec![Datum::Null, s("$"), Datum::Null],
            vec![Datum::Null, s("$"), s("a")],
            vec![s(r#"{"a":1}"#), Datum::Null, s("x")],
        ] {
            assert_eq!(call("JSON_ARRAY_APPEND", &args), Datum::Null);
        }
        for args in [
            vec![s("asdf"), s("$"), Datum::Null],
            vec![s(""), s("$"), Datum::Null],
            vec![s(r#"{"a":1}"#), s("asdf"), Datum::Null],
            vec![s(r#"{"a":1}"#), Datum::Int(42), Datum::Null],
            vec![s(r#"{"a":1}"#), s("$.*"), Datum::Null],
        ] {
            assert!(call_result("JSON_ARRAY_APPEND", &args).is_err());
        }
        assert!(dispatch("JSON_ARRAY_APPEND", &[s(r#"{"a":1}"#), s("$")]).is_none());
        assert!(call_result(
            "JSON_ARRAY_APPEND",
            &[s(r#"{"a":1}"#), s("$"), s("x"), s("$.a")]
        )
        .is_err());
    }

    /// Complete scalar-value-domain table from `TestJSONArrayInsert` in
    /// `pkg/expression/builtin_json_test.go:1103`.  Typed JSON value rows are
    /// not guessed from SQL strings; the representable source rows preserve
    /// string, integer, NULL, and nested-array insertion semantics exactly.
    #[test]
    fn json_array_insert_go_vectors() {
        for (args, want) in [
            (
                vec![s(r#"{"a": 1, "b": [2, 3], "c": 4}"#), s("$.b[1]"), s("z")],
                r#"{"a": 1, "b": [2, "z", 3], "c": 4}"#,
            ),
            (
                vec![s(r#"{"a": 1, "b": [2, 3], "c": 4}"#), s("$.a[1]"), s("z")],
                r#"{"a": 1, "b": [2, 3], "c": 4}"#,
            ),
            (
                vec![s(r#"{"a": 1, "b": [2, 3], "c": 4}"#), s("$.d[1]"), s("z")],
                r#"{"a": 1, "b": [2, 3], "c": 4}"#,
            ),
            (
                vec![s(r#"[{"a": 1}]"#), s("$[1]"), s("w")],
                r#"[{"a": 1}, "w"]"#,
            ),
            (
                vec![s(r#"[{"a": 1}]"#), s("$[0]"), Datum::Null],
                r#"[null, {"a": 1}]"#,
            ),
            (
                vec![s("[1, 2, 3]"), s("$[100]"), s(r#"{"b": 2}"#)],
                r#"[1, 2, 3, "{\"b\": 2}"]"#,
            ),
            (
                vec![s(r#"["a", {"b": [1, 2]}, [3, 4]]"#), s("$[1]"), s("x")],
                r#"["a", "x", {"b": [1, 2]}, [3, 4]]"#,
            ),
            (
                vec![s(r#"["a", {"b": [1, 2]}, [3, 4]]"#), s("$[1].b[0]"), s("x")],
                r#"["a", {"b": ["x", 1, 2]}, [3, 4]]"#,
            ),
            (
                vec![s(r#"["a", {"b": [1, 2]}, [3, 4]]"#), s("$[2][1]"), s("y")],
                r#"["a", {"b": [1, 2]}, [3, "y", 4]]"#,
            ),
            (
                vec![
                    s(r#"["a", {"b": [1, 2]}, [3, 4]]"#),
                    s("$[0]"),
                    s("x"),
                    s("$[2][1]"),
                    s("y"),
                ],
                r#"["x", "a", {"b": [1, 2]}, [3, 4]]"#,
            ),
            (
                vec![
                    s(r#"["a", {"b": [1, 2]}, [3, 4]]"#),
                    s("$[0]"),
                    s("x"),
                    s("$[0]"),
                    s("y"),
                ],
                r#"["y", "x", "a", {"b": [1, 2]}, [3, 4]]"#,
            ),
        ] {
            assert_eq!(call("JSON_ARRAY_INSERT", &args), s(want), "args {args:?}");
        }
        for args in [
            vec![Datum::Null, s("$"), Datum::Null],
            vec![Datum::Null, s("$"), s("a")],
            vec![s(r#"{"a": 1}"#), Datum::Null, Datum::Null],
            vec![s("[]"), s("$[0]"), Datum::Null],
            vec![s("{}"), s("$[0]"), Datum::Null],
        ] {
            let result = call_result("JSON_ARRAY_INSERT", &args);
            if args[0] == Datum::Null || args[1] == Datum::Null {
                assert_eq!(result, Ok(Datum::Null));
            } else if args[0] == s("[]") {
                assert_eq!(result, Ok(s("[null]")));
            } else {
                assert_eq!(result, Ok(s("{}")));
            }
        }
        for args in [
            vec![s("asdf"), s("$"), Datum::Null],
            vec![s(""), s("$"), Datum::Null],
            vec![s(r#"{"a":1}"#), s("asdf"), Datum::Null],
            vec![s(r#"{"a":1}"#), Datum::Int(42), Datum::Null],
            vec![s(r#"{"a":1}"#), s("$.*"), Datum::Null],
            vec![s(r#"{"a":1}"#), s("$.a"), Datum::Null],
        ] {
            assert!(call_result("JSON_ARRAY_INSERT", &args).is_err());
        }
        assert!(call_result(
            "JSON_ARRAY_INSERT",
            &[
                s(r#"{"a":1,"b":[2,3],"c":4}"#),
                s("$.b[0]"),
                Datum::Null,
                s("$.a"),
                Datum::Null,
            ]
        )
        .is_err());
        assert!(dispatch("JSON_ARRAY_INSERT", &[s(r#"{"a":1}"#), s("$")]).is_none());
        assert!(call_result(
            "JSON_ARRAY_INSERT",
            &[s(r#"{"a":1}"#), s("$[0]"), s("x"), s("$.a")]
        )
        .is_err());
        assert_eq!(
            call(
                "JSON_ARRAY_INSERT",
                &[s(r#"{"a":1}"#), s("$.a[0]"), Datum::Null]
            ),
            s(r#"{"a": 1}"#)
        );
    }

    /// Complete representable table from `TestJSONSearch` in
    /// `pkg/expression/builtin_json_test.go:1028`.  JSON paths are returned as
    /// JSON strings, matching `BinaryJSON.Search` rather than exposing the
    /// evaluator's internal path representation.
    #[test]
    fn json_search_go_vectors() {
        let document = r#"["abc", [{"k": "10"}, "def"], {"x":"abc"}, {"y":"bcd"}]"#;
        let document2 = r#"["abc", [{"k": "10"}, "def"], {"x":"ab%d"}, {"y":"abcd"}]"#;
        let cases = [
            (&[document, "one", "abc"][..], r#""$[0]""#),
            (&[document, "all", "abc"][..], r#"["$[0]", "$[2].x"]"#),
            (&[document, "all", "ghi"][..], "null"),
            (&[document, "ALL", "ghi"][..], "null"),
            (&[document, "all", "10"][..], r#""$[1][0].k""#),
            (&[document, "all", "10", "", "$[0]"][..], "null"),
            (&[document, "all", "10", "", "$[*]"][..], r#""$[1][0].k""#),
            (&[document, "all", "10", "", "$**.k"][..], r#""$[1][0].k""#),
            (
                &[document, "all", "10", "", "$[*][0].k"][..],
                r#""$[1][0].k""#,
            ),
            (&[document, "all", "10", "", "$[1]"][..], r#""$[1][0].k""#),
            (
                &[document, "all", "10", "", "$[1][0]"][..],
                r#""$[1][0].k""#,
            ),
            (&[document, "all", "abc", "", "$[2]"][..], r#""$[2].x""#),
            (
                &[document, "all", "abc", "", "$[2]", "$[0]"][..],
                r#"["$[2].x", "$[0]"]"#,
            ),
            (
                &[document, "all", "abc", "", "$[2]", "$[2]"][..],
                r#""$[2].x""#,
            ),
            (&[document, "all", "%a%"][..], r#"["$[0]", "$[2].x"]"#),
            (
                &[document, "all", "%b%"][..],
                r#"["$[0]", "$[2].x", "$[3].y"]"#,
            ),
            (&[document, "all", "%b%", "", "$[0]"][..], r#""$[0]""#),
            (&[document, "all", "%b%", "", "$[2]"][..], r#""$[2].x""#),
            (&[document, "all", "%b%", "", "$[1]"][..], "null"),
            (&[document, "all", "%b%", "", "$[3]"][..], r#""$[3].y""#),
            (&[document2, "all", "ab_d"][..], r#"["$[2].x", "$[3].y"]"#),
            (&[document2, "all", "ab%d"][..], r#"["$[2].x", "$[3].y"]"#),
            (&[document2, "all", r"ab\%d"][..], r#""$[2].x""#),
            (&[document2, "all", "ab|%d", "|"][..], r#""$[2].x""#),
        ];
        for (args, expected) in cases {
            let datums = args.iter().map(|value| s(value)).collect::<Vec<_>>();
            let actual = call("JSON_SEARCH", &datums);
            if expected == "null" {
                assert_eq!(actual, Datum::Null, "args={args:?}");
            } else {
                assert_eq!(actual, s(expected), "args={args:?}");
            }
        }
        assert_eq!(
            call("JSON_SEARCH", &[Datum::Null, s("all"), s("abc")]),
            Datum::Null
        );
        assert!(call_result("JSON_SEARCH", &[s("a"), s("all"), s("abc")]).is_err());
        assert!(call_result("JSON_SEARCH", &[s(document), s("wrong"), s("abc")]).is_err());
        assert_eq!(
            call("JSON_SEARCH", &[s(document), Datum::Null, s("abc")]),
            Datum::Null
        );
        assert_eq!(
            call("JSON_SEARCH", &[s(document), s("all"), Datum::Null]),
            Datum::Null
        );
        assert!(call_result("JSON_SEARCH", &[s(document), s("all"), s("abc"), s("??")]).is_err());
        assert_eq!(
            call(
                "JSON_SEARCH",
                &[s(document), s("all"), s("abc"), s(""), Datum::Null]
            ),
            Datum::Null
        );
        assert!(call_result(
            "JSON_SEARCH",
            &[s(document), s("all"), s("abc"), s(""), s("$xx")]
        )
        .is_err());
    }

    /// Source-shaped scalar table from `TestJSONSetInsertReplace` in
    /// `pkg/expression/builtin_json_test.go:271`.  Value strings remain JSON
    /// strings, paths are exact and sequential, SQL NULL propagates from the
    /// document, and malformed/multiple-selection paths are errors.
    #[test]
    fn json_set_insert_replace_go_vectors() {
        assert_eq!(
            call("JSON_SET", &[Datum::Null, Datum::Null, Datum::Null]),
            Datum::Null
        );
        assert_eq!(
            call("JSON_SET", &[s("{}"), s("$.a"), Datum::Int(3)]),
            s(r#"{"a": 3}"#)
        );
        assert_eq!(
            call("JSON_INSERT", &[s("{}"), s("$.a"), Datum::Int(3)]),
            s(r#"{"a": 3}"#)
        );
        assert_eq!(
            call("JSON_REPLACE", &[s("{}"), s("$.a"), Datum::Int(3)]),
            s("{}")
        );
        assert_eq!(
            call(
                "JSON_SET",
                &[s("{}"), s("$.a"), Datum::Int(3), s("$.b"), s("3"),],
            ),
            s(r#"{"a": 3, "b": "3"}"#)
        );
        assert_eq!(
            call(
                "JSON_SET",
                &[s("{}"), s("$.a"), Datum::Null, s("$.b"), s("nil"),],
            ),
            s(r#"{"a": null, "b": "nil"}"#)
        );
        assert!(dispatch("JSON_SET", &[s("{}"), s("$.a")]).is_none());
        assert!(call_result("JSON_SET", &[s("{}"), s("$InvalidPath"), Datum::Int(3)]).is_err());
        assert!(call_result("JSON_SET", &[s("{}"), s("$.*"), Datum::Int(3)]).is_err());
        assert!(dispatch("JSON_SET", &[]).is_none());
    }

    /// Source-shaped tables from `TestJSONMerge` and `TestJSONMergePreserve`
    /// in `pkg/expression/builtin_json_test.go:317` and `:348`.  JSON_MERGE
    /// is the deprecated synonym for JSON_MERGE_PRESERVE; adjacent objects
    /// combine while arrays/scalars are preserved in an output array.
    #[test]
    fn json_merge_go_vectors() {
        for name in ["JSON_MERGE", "JSON_MERGE_PRESERVE"] {
            assert_eq!(call(name, &[Datum::Null, Datum::Null]), Datum::Null);
            assert_eq!(call(name, &[s("{}"), s("[]")]), s("[{}]"));
            assert_eq!(
                call(name, &[s("{}"), s("[]"), Datum::Int(3), s("4")]),
                s(r#"[{}, 3, "4"]"#)
            );
            assert!(call_result(name, &[s("{}"), s("not-json")]).is_err());
            assert!(dispatch(name, &[s("{}")]).is_none());
        }
    }

    /// RFC 7396 and MySQL vectors from `TestJSONMergePatch` at
    /// `pkg/expression/builtin_json_test.go:1367`.  Arrays and non-object
    /// patches replace wholesale; object patches recurse, delete JSON-null
    /// keys, and retain sorted BinaryJSON object output. SQL NULL follows the
    /// source nil-pointer truncation rules; invalid text remains an error.
    #[test]
    fn json_merge_patch_go_vectors() {
        for (args, want) in [
            (vec![s(r#"{"a":"b"}"#), s(r#"{"a":"c"}"#)], r#"{"a": "c"}"#),
            (
                vec![s(r#"{"a":"b"}"#), s(r#"{"b":"c"}"#)],
                r#"{"a": "b", "b": "c"}"#,
            ),
            (vec![s(r#"{"a":"b"}"#), s(r#"{"a":null}"#)], "{}"),
            (
                vec![s(r#"{"a":"b", "b":"c"}"#), s(r#"{"a":null}"#)],
                r#"{"b": "c"}"#,
            ),
            (
                vec![s(r#"{"a":["b"]}"#), s(r#"{"a":"c"}"#)],
                r#"{"a": "c"}"#,
            ),
            (
                vec![s(r#"{"a":"c"}"#), s(r#"{"a":["b"]}"#)],
                r#"{"a": ["b"]}"#,
            ),
            (
                vec![s(r#"{"a":{"b":"c"}}"#), s(r#"{"a":{"b":"d","c":null}}"#)],
                r#"{"a": {"b": "d"}}"#,
            ),
            (
                vec![s(r#"{"a":[{"b":"c"}]}"#), s(r#"{"a":[1]}"#)],
                r#"{"a": [1]}"#,
            ),
            (
                vec![s("[\"a\",\"b\"]"), s("[\"c\",\"d\"]")],
                r#"["c", "d"]"#,
            ),
            (vec![s(r#"{"a":"b"}"#), s("[\"c\"]")], r#"["c"]"#),
            (
                vec![s(r#"{"e":null}"#), s(r#"{"a":1}"#)],
                r#"{"a": 1, "e": null}"#,
            ),
            (
                vec![s("[1,2]"), s(r#"{"a":"b","c":null}"#)],
                r#"{"a": "b"}"#,
            ),
            (
                vec![s("{}"), s(r#"{"a":{"bb":{"ccc":null}}}"#)],
                r#"{"a": {"bb": {}}}"#,
            ),
            (vec![s(r#"{"a":"foo"}"#), s("false")], "false"),
            (vec![s(r#"{"a":"foo"}"#), s("123")], "123"),
            (vec![s(r#"{"a":"foo"}"#), s("123.1")], "123.1"),
            (vec![s(r#"{"a":"foo"}"#), s("[1,2,3]")], "[1, 2, 3]"),
            (vec![s("null"), s(r#"{"a":1}"#)], r#"{"a": 1}"#),
            (vec![s(r#"{"a":1}"#), s("null")], "null"),
        ] {
            let got = call_result("JSON_MERGE_PATCH", &args).expect("valid merge patch");
            assert_eq!(got, s(want), "args {args:?}");
        }
        assert_eq!(
            call(
                "JSON_MERGE_PATCH",
                &[Datum::Null, s("null"), s(r#"{"a":1}"#)]
            ),
            s(r#"{"a": 1}"#)
        );
        assert_eq!(
            call("JSON_MERGE_PATCH", &[s("null"), s("[1,2,3]"), Datum::Null]),
            Datum::Null
        );
        for args in [
            vec![s(r#"{"a":1}"#), s("[1]}")],
            vec![s(r#"{{"a":1}"#), s("[1]"), s("null")],
            vec![s(r#"{"a":1}"#), s("jjj"), s("null")],
        ] {
            assert!(call_result("JSON_MERGE_PATCH", &args).is_err());
        }
    }

    /// Complete representable table from `TestJSONPretty` in
    /// `pkg/expression/builtin_json_test.go:1293`.  Scalar JSON values stay
    /// unchanged; arrays/objects use two-space indentation, sorted object
    /// keys, and the same nested formatting as BinaryJSON.MarshalJSON plus
    /// `encoding/json.Indent`. Invalid documents remain errors and SQL NULL
    /// propagates to SQL NULL.
    #[test]
    fn json_pretty_go_vectors() {
        for (document, want) in [
            ("true", "true"),
            ("false", "false"),
            ("2223", "2223"),
            (
                r#"{"a":1}"#,
                "{\n  \"a\": 1\n}",
            ),
            ("[1]", "[\n  1\n]"),
            (
                r#"{"a":1,"b":[{"d":1},{"e":2},{"f":3}],"c":"eee"}"#,
                "{\n  \"a\": 1,\n  \"b\": [\n    {\n      \"d\": 1\n    },\n    {\n      \"e\": 2\n    },\n    {\n      \"f\": 3\n    }\n  ],\n  \"c\": \"eee\"\n}",
            ),
            (
                r#"{"a":1,"b":"qwe","c":[1,2,3,"123",null],"d":{"d1":1,"d2":2}}"#,
                "{\n  \"a\": 1,\n  \"b\": \"qwe\",\n  \"c\": [\n    1,\n    2,\n    3,\n    \"123\",\n    null\n  ],\n  \"d\": {\n    \"d1\": 1,\n    \"d2\": 2\n  }\n}",
            ),
        ] {
            assert_eq!(call("JSON_PRETTY", &[s(document)]), s(want), "JSON_PRETTY({document:?})");
        }
        assert_eq!(call("JSON_PRETTY", &[Datum::Null]), Datum::Null);
        for document in [r#"{1}"#, r#"[1,3,4,5]]"#] {
            assert!(call_result("JSON_PRETTY", &[s(document)]).is_err());
        }
        assert!(dispatch("JSON_PRETTY", &[]).is_none());
        assert!(dispatch("JSON_PRETTY", &[s("{}"), s("extra")]).is_none());
    }

    /// Representable scalar-array rows from `TestJSONSumCrc32` in
    /// `pkg/expression/builtin_json_test.go:127`.  The Go source supplies the
    /// target array `FieldType` through `expr AS type ARRAY`; this frozen
    /// evaluator exercises the shared JSON-text portion (homogeneous numeric
    /// and string arrays, empty arrays, root/type errors, NULL, and invalid
    /// scalar/nested members) while keeping signed/unsigned/string-width
    /// conversion and path/session metadata as explicit boundaries.
    #[test]
    fn json_sum_crc32_go_vectors() {
        for (document, want) in [
            ("[-1, 2, 3]", 3_101_005_010_i64),
            ("[1, 2, 3]", 4_505_025_631_i64),
            (r#"["a", "b", "c"]"#, 5_925_539_243_i64),
            ("[1.1, 1, 3.3]", 6_204_045_883_i64),
            ("[1.1, 2.2, 3.3]", 4_453_038_788_i64),
            ("[]", 0),
        ] {
            assert_eq!(
                call_result("JSON_SUM_CRC32", &[s(document)]),
                Ok(Datum::Int(want)),
                "JSON_SUM_CRC32({document:?})",
            );
        }
        assert_eq!(call("JSON_SUM_CRC32", &[Datum::Null]), Datum::Null);

        for document in [
            "1",
            "{}",
            "true",
            "[true]",
            "[null]",
            "[[1]]",
            "[{\"a\": 1}]",
            r#"[1.1, "1.1", 3.3]"#,
        ] {
            assert!(
                call_result("JSON_SUM_CRC32", &[s(document)]).is_err(),
                "JSON_SUM_CRC32({document:?}) must reject this text-domain row",
            );
        }
        assert!(call_result("JSON_SUM_CRC32", &[s("not-json")]).is_err());
        assert!(dispatch("JSON_SUM_CRC32", &[]).is_none());
        assert!(dispatch("JSON_SUM_CRC32", &[s("[]"), s("extra")]).is_none());
    }

    /// Complete representable table from `TestJSONLength` in
    /// `pkg/expression/builtin_json_test.go`.  Keeping the scalar and path
    /// rows here makes the JSON length contract executable instead of relying
    /// on the single nested-array smoke check above: every scalar has length
    /// one, containers count direct children only, `$` selects the document,
    /// missing paths are NULL, and multiple-selection paths are errors.
    #[test]
    fn json_length_go_vectors() {
        for (document, want) in [
            ("null", 1),
            ("true", 1),
            ("false", 1),
            ("1", 1),
            ("-1", 1),
            ("1.1", 1),
            (r#""1""#, 1),
            ("{}", 0),
            (r#"{"a":1}"#, 1),
            (r#"{"a":[1]}"#, 1),
            (r#"{"b":2,"c":3}"#, 2),
            ("[1]", 1),
            ("[1,2]", 2),
            ("[1,2,[1,3]]", 3),
            ("[1,2,[1,[5,[3]]]]", 3),
            (r#"[1,2,[1,[5,{"a":[2,3]}]]]"#, 3),
            (r#"[{"a":1}]"#, 1),
            (r#"[{"a":1,"b":2}]"#, 1),
            (r#"[{"a":{"a":1},"b":2}]"#, 1),
        ] {
            assert_eq!(
                call_result("JSON_LENGTH", &[s(document)]),
                Ok(Datum::Int(want)),
                "JSON_LENGTH({document:?})",
            );
        }

        for (document, path, want) in [
            (r#"[1,2,[1,[5,[3]]]]"#, "$[2]", 2),
            (r#"[{"a":1}]"#, "$", 1),
            (r#"[{"a":1}]"#, "$[0].a", 1),
            (r#"{"a":{"a":1},"b":2}"#, "$", 2),
            (r#"{"a":{"a":1},"b":2}"#, "$.a", 1),
            (r#"{"a":{"a":1},"b":2}"#, "$.a.a", 1),
            (r#"{"a":[1,2,{"aa":"xx"}]}"#, "$.a[2].aa", 1),
        ] {
            assert_eq!(
                call_result("JSON_LENGTH", &[s(document), s(path)]),
                Ok(Datum::Int(want)),
                "JSON_LENGTH({document:?}, {path:?})",
            );
        }

        for (document, path) in [
            (r#""1""#, "$.a"),
            ("null", "$.a"),
            (r#"{"a":[1,2,{"aa":"xx"}]}"#, "$.c"),
            (r#"{"a":[1,2,{"aa":"xx"}]}"#, "$.a[3]"),
            (r#"{"a":[1,2,{"aa":"xx"}]}"#, "$.a[2].b"),
        ] {
            assert_eq!(
                call_result("JSON_LENGTH", &[s(document), s(path)]),
                Ok(Datum::Null),
                "JSON_LENGTH({document:?}, {path:?})",
            );
        }

        for path in ["$.*", "$[*]", "$**.a"] {
            assert!(
                call_result("JSON_LENGTH", &[s(r#"{"a":[1,2,{"aa":"xx"}]}"#), s(path)],).is_err(),
                "JSON_LENGTH wildcard path {path:?} must reject multiple selection",
            );
        }

        for args in [
            vec![Datum::Null],
            vec![Datum::Null, s("a")],
            vec![s(r#"{"a":1}"#), Datum::Null],
            vec![Datum::Null, Datum::Null],
        ] {
            assert_eq!(call_result("JSON_LENGTH", &args), Ok(Datum::Null));
        }
    }
}
