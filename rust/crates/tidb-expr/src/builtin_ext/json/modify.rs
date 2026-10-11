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

//! In-place document modification: `JSON_SET`, `JSON_INSERT`,
//! `JSON_REPLACE`, `JSON_REMOVE`, `JSON_ARRAY_APPEND`, `JSON_ARRAY_INSERT`.
//!
//! Ports of `builtinJSON{Set,Insert,Replace,Remove,ArrayAppend,ArrayInsert}Sig`
//! and `jsonModify` in `pkg/expression/builtin_json.go`, evaluated as Go
//! evaluates them: on the `BinaryJSON` port of `pkg/types`
//! (`BinaryJSON::modify` / `remove` / `array_insert` / `extract`,
//! `merge_binary_json`) with paths from `parse_json_path_expr`. A document
//! argument is Go's `WrapWithCastAsJSON` cast (a binary string is an opaque
//! value, any other string is parsed); a value argument has that cast's
//! `ParseToJSONFlag` disabled, and SQL NULL is JSON `null`.

use super::value::{cast_as_json_typed, cast_as_json_value_typed};
use crate::coerce::coerce_str;
use crate::{Datum, EvalError, JsonError};
use tidb_datatype::{
    BinaryJSON, BinaryJSONError, BinaryJSONValue, FieldType, JSONModifyType,
    JSONPathArraySelection, JSONPathExpression, JSONPathLeg,
};

/// Go `types.ParseJSONPathExpr`: a malformed path is `ErrInvalidJSONPath`
/// (3143) at Go's character position.
pub(super) fn parse_json_path(text: &str) -> Result<JSONPathExpression, EvalError> {
    tidb_datatype::parse_json_path_expr(text)
        .map_err(|error| EvalError::Json(JsonError::InvalidPath(error.position)))
}

/// The Go error a `BinaryJSON` refusal stands for.
pub(super) fn binary_json_error(error: BinaryJSONError) -> EvalError {
    match error {
        BinaryJSONError::TooDeep => EvalError::Json(JsonError::DocumentTooDeep),
        BinaryJSONError::EmptyDocument => EvalError::Json(JsonError::EmptyText),
        BinaryJSONError::InvalidText
        | BinaryJSONError::TrailingValues
        | BinaryJSONError::LoneSurrogate => EvalError::Json(JsonError::InvalidText),
        BinaryJSONError::InvalidPath => EvalError::Json(JsonError::InvalidPathMultipleSelection),
        BinaryJSONError::KeyTooLong | BinaryJSONError::InvalidBinary => {
            EvalError::Unsupported("binary JSON document")
        }
    }
}

/// Go `args[i].EvalJSON` for a document argument, through the
/// `WrapWithCastAsJSON` cast `newBaseBuiltinFuncWithTp` builds (with
/// `ParseToJSONFlag`). `None` is SQL NULL.
pub(crate) fn json_document(
    value: &Datum,
    field_type: Option<&FieldType>,
) -> Result<Option<BinaryJSON>, EvalError> {
    match cast_as_json_typed(value, field_type)? {
        Datum::Null => Ok(None),
        Datum::Json(json) => Ok(Some(json)),
        _ => Err(EvalError::Unsupported("datum JSON conversion")),
    }
}

/// Go `args[i].EvalJSON` for a value argument (`DisableParseJSONFlag4Expr`),
/// with SQL NULL as `types.CreateBinaryJSON(nil)`.
fn json_value(value: &Datum, field_type: Option<&FieldType>) -> Result<BinaryJSON, EvalError> {
    match cast_as_json_value_typed(value, field_type)? {
        Datum::Null => {
            BinaryJSON::from_typed_value(&BinaryJSONValue::Null).map_err(binary_json_error)
        }
        Datum::Json(json) => Ok(json),
        _ => Err(EvalError::Unsupported("datum JSON conversion")),
    }
}

fn has_multiple_selection(path: &JSONPathExpression) -> bool {
    path.contains_any_asterisk() || path.contains_any_range()
}

/// `JSON_REMOVE(json_doc, path [, path] ...)`, port of
/// `builtinJSONRemoveSig.evalJSON` and `BinaryJSON.Remove`: every path is
/// parsed first; then each is checked (`$` alone is vacuous, 3153; a
/// wildcard or range is 3149) and removed in turn, so removing an earlier
/// array element shifts what a later path names.
pub(super) fn json_remove(
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
) -> Result<Datum, EvalError> {
    let Some(mut document) = json_document(&vals[0], arg_types[0].as_ref())? else {
        return Ok(Datum::Null);
    };
    let mut paths = Vec::with_capacity(vals.len() - 1);
    for path_value in &vals[1..] {
        let Some(path) = coerce_str(path_value)? else {
            return Ok(Datum::Null);
        };
        paths.push(parse_json_path(&path)?);
    }
    for path in &paths {
        if path.legs().is_empty() {
            return Err(EvalError::Json(JsonError::VacuousPath));
        }
        if has_multiple_selection(path) {
            return Err(EvalError::Json(JsonError::InvalidPathMultipleSelection));
        }
        document = document
            .remove(std::slice::from_ref(path))
            .map_err(binary_json_error)?;
    }
    Ok(Datum::Json(document))
}

/// `JSON_ARRAY_APPEND(json_doc, path, value [, path, value] ...)`, port of
/// `builtinJSONArrayAppendSig.evalJSON` and `appendJSONArray`: the value
/// selected by each path is wrapped into an array unless it is one,
/// `MergeBinaryJSON`ed with the value wrapped in an array, and set back. An
/// absent path is a no-op.
pub(super) fn json_array_append(
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
) -> Result<Datum, EvalError> {
    if vals.len() < 3 || vals.len().is_multiple_of(2) {
        return Err(EvalError::Unsupported("JSON_ARRAY_APPEND arity"));
    }
    let Some(mut document) = json_document(&vals[0], arg_types[0].as_ref())? else {
        return Ok(Datum::Null);
    };
    for (pair, types) in vals[1..]
        .as_chunks::<2>()
        .0
        .iter()
        .zip(arg_types[1..].as_chunks::<2>().0)
    {
        let Some(path) = coerce_str(&pair[0])? else {
            return Ok(Datum::Null);
        };
        let value = json_value(&pair[1], types[1].as_ref())?;
        let path = parse_json_path(&path)?;
        if path.could_match_multiple_values() {
            return Err(EvalError::Json(JsonError::InvalidPathMultipleSelection));
        }
        let path = std::slice::from_ref(&path);
        let Some(mut target) = document.extract(path).map_err(binary_json_error)? else {
            continue;
        };
        if target.type_code() != tidb_datatype::JSON_TYPE_CODE_ARRAY {
            target = BinaryJSON::from_typed_value(&BinaryJSONValue::Array(vec![
                BinaryJSONValue::Binary(target),
            ]))
            .map_err(binary_json_error)?;
        }
        // Go wraps the value too, so an array value is appended as one
        // element (`JSON_ARRAY_APPEND('[1]', '$', JSON_ARRAY(2, 3))` is
        // `[1, [2, 3]]`).
        let value =
            BinaryJSON::from_typed_value(&BinaryJSONValue::Array(vec![BinaryJSONValue::Binary(
                value,
            )]))
            .map_err(binary_json_error)?;
        let merged =
            tidb_datatype::merge_binary_json(&[target, value]).map_err(binary_json_error)?;
        document = document
            .modify(path, &[merged], JSONModifyType::Set)
            .map_err(binary_json_error)?;
    }
    Ok(Datum::Json(document))
}

/// `JSON_ARRAY_INSERT(json_doc, path, value [, path, value] ...)`, port of
/// `builtinJSONArrayInsertSig.evalJSON` and `BinaryJSON.ArrayInsert`: the
/// last leg must name an array cell (3165); a missing parent or one that is
/// not an array is a no-op, and an index past the end appends.
pub(super) fn json_array_insert(
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
) -> Result<Datum, EvalError> {
    if vals.len() < 3 || vals.len().is_multiple_of(2) {
        return Err(EvalError::Unsupported("JSON_ARRAY_INSERT arity"));
    }
    let Some(mut document) = json_document(&vals[0], arg_types[0].as_ref())? else {
        return Ok(Datum::Null);
    };
    for (pair, types) in vals[1..]
        .as_chunks::<2>()
        .0
        .iter()
        .zip(arg_types[1..].as_chunks::<2>().0)
    {
        let Some(path) = coerce_str(&pair[0])? else {
            return Ok(Datum::Null);
        };
        let path = parse_json_path(&path)?;
        if path.could_match_multiple_values() {
            return Err(EvalError::Json(JsonError::InvalidPathMultipleSelection));
        }
        let value = json_value(&pair[1], types[1].as_ref())?;
        if !matches!(
            path.pop_last(),
            Some((_, JSONPathLeg::Array(JSONPathArraySelection::Index(_))))
        ) {
            return Err(EvalError::Json(JsonError::InvalidPathArrayCell));
        }
        document = document
            .array_insert(&path, &value)
            .map_err(binary_json_error)?;
    }
    Ok(Datum::Json(document))
}

/// The three `JSON_{SET,INSERT,REPLACE}` modifiers share Go's `jsonModify`
/// and `BinaryJSON.Modify`.
#[derive(Clone, Copy)]
pub(super) enum JsonModifyMode {
    Set,
    Insert,
    Replace,
}

impl JsonModifyMode {
    fn modify_type(self) -> JSONModifyType {
        match self {
            JsonModifyMode::Set => JSONModifyType::Set,
            JsonModifyMode::Insert => JSONModifyType::Insert,
            JsonModifyMode::Replace => JSONModifyType::Replace,
        }
    }
}

/// `jsonModify`'s path loop: every path is parsed before any value is
/// evaluated, and a NULL path makes the result NULL. Callers cache the list
/// when every path is a context-only constant.
pub(crate) fn parse_json_modify_paths(
    vals: &[Datum],
) -> Result<Option<Vec<JSONPathExpression>>, EvalError> {
    if vals.len() < 3 || vals.len().is_multiple_of(2) {
        return Err(EvalError::Unsupported("JSON modification arity"));
    }
    let mut paths = Vec::with_capacity((vals.len() - 1) / 2);
    for path_value in vals[1..].iter().step_by(2) {
        let Some(path) = coerce_str(path_value)? else {
            return Ok(None);
        };
        paths.push(parse_json_path(&path)?);
    }
    Ok(Some(paths))
}

/// `JSON_SET`, `JSON_INSERT` and `JSON_REPLACE` with a previously parsed
/// path list.
pub(super) fn json_modify_with_paths(
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
    mode: JsonModifyMode,
    paths: &[JSONPathExpression],
) -> Result<Datum, EvalError> {
    let Some(document) = json_document(&vals[0], arg_types[0].as_ref())? else {
        return Ok(Datum::Null);
    };
    json_modify_with_document(document, vals, arg_types, mode, paths)
}

/// `jsonModify` after the document and the paths: each value is evaluated,
/// then `BinaryJSON.Modify` refuses a wildcard or range path (3149) before
/// applying the pairs in order and refuses a result deeper than 100 (3157).
pub(super) fn json_modify_with_document(
    document: BinaryJSON,
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
    mode: JsonModifyMode,
    paths: &[JSONPathExpression],
) -> Result<Datum, EvalError> {
    if vals.len() < 3 || vals.len().is_multiple_of(2) {
        return Err(EvalError::Unsupported("JSON modification arity"));
    }
    let values = vals[2..]
        .iter()
        .step_by(2)
        .zip(arg_types[2..].iter().step_by(2))
        .map(|(value, field_type)| json_value(value, field_type.as_ref()))
        .collect::<Result<Vec<_>, _>>()?;
    if paths.iter().any(has_multiple_selection) {
        return Err(EvalError::Json(JsonError::InvalidPathMultipleSelection));
    }
    document
        .modify(paths, &values, mode.modify_type())
        .map(Datum::Json)
        .map_err(binary_json_error)
}

/// `JSON_SET`, `JSON_INSERT` and `JSON_REPLACE`.
pub(super) fn json_modify(
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
    mode: JsonModifyMode,
) -> Result<Datum, EvalError> {
    if vals.len() < 3 || vals.len().is_multiple_of(2) {
        return Err(EvalError::Unsupported("JSON modification arity"));
    }
    let Some(document) = json_document(&vals[0], arg_types[0].as_ref())? else {
        return Ok(Datum::Null);
    };
    let Some(paths) = parse_json_modify_paths(vals)? else {
        return Ok(Datum::Null);
    };
    json_modify_with_document(document, vals, arg_types, mode, &paths)
}
