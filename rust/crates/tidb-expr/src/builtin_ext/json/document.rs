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

//! The JSON builtins that read a document: `JSON_EXTRACT`, `JSON_TYPE`,
//! `JSON_LENGTH`, `JSON_KEYS`, `JSON_DEPTH`, `JSON_STORAGE_SIZE`,
//! `JSON_STORAGE_FREE` and `JSON_PRETTY`, ported from
//! `pkg/expression/builtin_json.go` onto the `BinaryJSON` port of
//! `pkg/types`, so an opaque, date or time value answers as itself
//! (`JSON_TYPE` BLOB, its exact storage size) rather than as text.

use super::modify::{binary_json_error, json_document, parse_json_path};
use crate::coerce::coerce_str;
use crate::{Datum, EvalError, JsonError};
use tidb_datatype::{BinaryJSON, FieldType, JSON_TYPE_CODE_ARRAY, JSON_TYPE_CODE_OBJECT};

/// The document argument, or `None` for SQL NULL.
fn document(
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
) -> Result<Option<BinaryJSON>, EvalError> {
    json_document(&vals[0], arg_types.first().and_then(Option::as_ref))
}

/// `JSON_LENGTH`'s and `JSON_KEYS`'s optional path: the one value it selects
/// (a wildcard or range is 3149), or `None` for a NULL or absent selection.
fn select_by_optional_path(
    document: BinaryJSON,
    path: Option<&Datum>,
) -> Result<Option<BinaryJSON>, EvalError> {
    let Some(path) = path else {
        return Ok(Some(document));
    };
    let Some(path) = coerce_str(path)? else {
        return Ok(None);
    };
    let path = parse_json_path(&path)?;
    if path.could_match_multiple_values() {
        return Err(EvalError::Json(JsonError::InvalidPathMultipleSelection));
    }
    document
        .extract(std::slice::from_ref(&path))
        .map_err(binary_json_error)
}

/// `JSON_EXTRACT(json_doc, path [, path] ...)`, port of
/// `builtinJSONExtractSig.evalJSON` and `BinaryJSON.Extract`.
pub(super) fn json_extract(
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
) -> Result<Datum, EvalError> {
    let Some(document) = document(vals, arg_types)? else {
        return Ok(Datum::Null);
    };
    let mut paths = Vec::with_capacity(vals.len() - 1);
    for value in &vals[1..] {
        let Some(path) = coerce_str(value)? else {
            return Ok(Datum::Null);
        };
        paths.push(parse_json_path(&path)?);
    }
    Ok(document
        .extract(&paths)
        .map_err(binary_json_error)?
        .map_or(Datum::Null, Datum::Json))
}

/// `JSON_TYPE(json_doc)`, port of `builtinJSONTypeSig.evalString`.
pub(super) fn json_type(
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
) -> Result<Datum, EvalError> {
    let Some(document) = document(vals, arg_types)? else {
        return Ok(Datum::Null);
    };
    document
        .type_name()
        .map(|name| Datum::new_string(name.to_owned()))
        .map_err(binary_json_error)
}

/// `JSON_LENGTH(json_doc [, path])`, port of `builtinJSONLengthSig.evalInt`:
/// a scalar is 1, an array or object its element count.
pub(super) fn json_length(
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
) -> Result<Datum, EvalError> {
    let Some(document) = document(vals, arg_types)? else {
        return Ok(Datum::Null);
    };
    let Some(target) = select_by_optional_path(document, vals.get(1))? else {
        return Ok(Datum::Null);
    };
    if !matches!(
        target.type_code(),
        JSON_TYPE_CODE_OBJECT | JSON_TYPE_CODE_ARRAY
    ) {
        return Ok(Datum::Int(1));
    }
    let count = target.element_count().map_err(binary_json_error)?;
    Ok(Datum::Int(i64::try_from(count).unwrap_or(i64::MAX)))
}

/// `JSON_KEYS(json_doc [, path])`, port of
/// `builtinJSONKeys{Sig,2ArgsSig}.evalJSON`: the selected object's keys, or
/// NULL for anything but an object.
pub(super) fn json_keys(
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
) -> Result<Datum, EvalError> {
    let Some(document) = document(vals, arg_types)? else {
        return Ok(Datum::Null);
    };
    let Some(target) = select_by_optional_path(document, vals.get(1))? else {
        return Ok(Datum::Null);
    };
    if target.type_code() != JSON_TYPE_CODE_OBJECT {
        return Ok(Datum::Null);
    }
    target.keys().map(Datum::Json).map_err(binary_json_error)
}

/// `JSON_DEPTH(json_doc)`, port of `builtinJSONDepthSig.evalInt`
/// (`BinaryJSON.GetElemDepth`: a scalar or empty container is 1).
pub(crate) fn json_depth(
    value: &Datum,
    field_type: Option<&FieldType>,
) -> Result<Datum, EvalError> {
    let Some(document) = json_document(value, field_type)? else {
        return Ok(Datum::Null);
    };
    let depth = document.element_depth().map_err(binary_json_error)?;
    Ok(Datum::Int(i64::try_from(depth).unwrap_or(i64::MAX)))
}

/// `JSON_STORAGE_SIZE(json_doc)`, port of `builtinJSONStorageSizeSig.evalInt`:
/// the binary value's length plus its type code.
pub(crate) fn json_storage_size(
    value: &Datum,
    field_type: Option<&FieldType>,
) -> Result<Datum, EvalError> {
    let Some(document) = json_document(value, field_type)? else {
        return Ok(Datum::Null);
    };
    Ok(Datum::Int(
        i64::try_from(document.value().len() + 1).unwrap_or(i64::MAX),
    ))
}

/// `JSON_STORAGE_FREE(json_doc)`, port of `builtinJSONStorageFreeSig.evalInt`:
/// 0 for any document, after evaluating it.
pub(crate) fn json_storage_free(
    value: &Datum,
    field_type: Option<&FieldType>,
) -> Result<Datum, EvalError> {
    Ok(match json_document(value, field_type)? {
        Some(_) => Datum::Int(0),
        None => Datum::Null,
    })
}

/// `JSON_PRETTY(json_doc)`, port of `builtinJSONSPrettySig.evalString`:
/// `MarshalJSON`, then `json.Indent` with two spaces.
pub(super) fn json_pretty(
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
) -> Result<Datum, EvalError> {
    let Some(document) = document(vals, arg_types)? else {
        return Ok(Datum::Null);
    };
    Ok(Datum::new_string(tidb_datatype::json_scanner::indent(
        &document.to_string(),
        "  ",
    )))
}
