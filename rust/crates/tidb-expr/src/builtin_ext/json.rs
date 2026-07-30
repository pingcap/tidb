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
use crate::{Datum, EvalError, JsonError};
use tidb_datatype::FieldType;

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
        ("JSON_ARRAY", 0..) => Some(json_array(vals, &no_arg_types(vals.len()))),
        ("JSON_OBJECT", 0..) => Some(json_object(vals, &no_arg_types(vals.len()))),
        ("JSON_LENGTH", 1 | 2) => Some(json_length(vals)),
        ("JSON_EXTRACT", 2..) => Some(json_extract(vals)),
        ("JSON_MEMBER_OF", 2) => Some(json_member_of(vals)),
        ("JSON_CONTAINS", 2 | 3) => Some(json_contains(vals)),
        ("JSON_CONTAINS_PATH", 3..) => Some(json_contains_path(vals)),
        ("JSON_KEYS", 1 | 2) => Some(json_keys(vals)),
        ("JSON_REMOVE", 2..) => Some(json_remove(vals)),
        ("JSON_ARRAY_APPEND", 3..) => Some(json_array_append(vals, &no_arg_types(vals.len()))),
        ("JSON_ARRAY_INSERT", 3..) => Some(json_array_insert(vals, &no_arg_types(vals.len()))),
        ("JSON_SET", 3..) => Some(json_modify(
            vals,
            &no_arg_types(vals.len()),
            JsonModifyMode::Set,
        )),
        ("JSON_INSERT", 3..) => Some(json_modify(
            vals,
            &no_arg_types(vals.len()),
            JsonModifyMode::Insert,
        )),
        ("JSON_REPLACE", 3..) => Some(json_modify(
            vals,
            &no_arg_types(vals.len()),
            JsonModifyMode::Replace,
        )),
        ("JSON_MERGE", 2..) => Some(json_merge(vals, "json_merge")),
        ("JSON_MERGE_PRESERVE", 2..) => Some(json_merge(vals, "json_merge_preserve")),
        ("JSON_MERGE_PATCH", 2..) => Some(json_merge_patch(vals)),
        ("JSON_SEARCH", 3..) => Some(json_search(vals)),
        ("JSON_PRETTY", 1) => Some(json_pretty(&vals[0])),
        ("JSON_SUM_CRC32", 1) => Some(json_sum_crc32(&vals[0])),
        ("JSON_OVERLAPS", 2) => Some(json_overlaps(vals)),
        _ => None,
    }
}

/// The typed sibling of [`dispatch`] for the function class whose value
/// arguments Go builds through an implicit `CAST(... AS JSON)` with
/// `ParseToJSONFlag` disabled (`newBaseBuiltinFuncWithTp(ctx, ..., ETJson,
/// ...)` followed by `DisableParseJSONFlag4Expr`): `JSON_ARRAY`,
/// `JSON_OBJECT`, `JSON_SET`/`JSON_INSERT`/`JSON_REPLACE`,
/// `JSON_ARRAY_APPEND`, `JSON_ARRAY_INSERT`. `arg_types[i]` is argument `i`'s
/// static `FieldType` when the caller has one (the chunk rewriter's
/// `ScalarFunction::args[i].static_type()`); `None` falls back to
/// [`json_sql_string`]'s plain-text rendering, same as the untyped
/// [`dispatch`].
///
/// Every other JSON function either takes no value-domain argument that can
/// carry a column's charset (`JSON_TYPE`, `JSON_LENGTH`, ...) or has its
/// binary-charset arguments rejected at Go's build time by
/// `verifyJSONArgsType` before evaluation ever sees them (`JSON_CONTAINS`,
/// `JSON_EXTRACT`, `JSON_MEMBER_OF`, ...) -- a plan-build-time check this
/// evaluator does not perform, and out of scope here since it never reaches
/// this Datum-only dispatch either way.
pub(crate) fn dispatch_typed(
    name: &str,
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
) -> Option<Result<Datum, EvalError>> {
    debug_assert_eq!(vals.len(), arg_types.len());
    match (name, vals.len()) {
        ("JSON_ARRAY", 0..) => Some(json_array(vals, arg_types)),
        ("JSON_OBJECT", 0..) => Some(json_object(vals, arg_types)),
        ("JSON_SET", 3..) => Some(json_modify(vals, arg_types, JsonModifyMode::Set)),
        ("JSON_INSERT", 3..) => Some(json_modify(vals, arg_types, JsonModifyMode::Insert)),
        ("JSON_REPLACE", 3..) => Some(json_modify(vals, arg_types, JsonModifyMode::Replace)),
        ("JSON_ARRAY_APPEND", 3..) => Some(json_array_append(vals, arg_types)),
        ("JSON_ARRAY_INSERT", 3..) => Some(json_array_insert(vals, arg_types)),
        _ => None,
    }
}

/// An all-`None` `arg_types` slice for [`dispatch`]'s untyped callers, so
/// [`json_array`]/[`json_object`]/[`json_modify`]/[`json_array_append`]/
/// [`json_array_insert`] share one implementation with [`dispatch_typed`]
/// instead of duplicating the plain-text path.
fn no_arg_types(len: usize) -> Vec<Option<FieldType>> {
    vec![None; len]
}

/// Whether `value` is a genuine BINARY-charset payload given its source
/// `field_type`.
///
/// NAMED BOUNDARY: unlike the JSON aggregates' `json_value`
/// (`tidb-executor`), this does NOT treat every `Datum::Bytes` as
/// unconditionally binary. In Go, a `KindBytes` datum only ever comes from a
/// genuinely BINARY-charset source, so `getRealJSONValue` can trust the datum
/// kind alone. This crate's chunk rewriter is looser: `Expr::String` literals
/// are built as `Datum::Bytes` regardless of their own (possibly non-binary)
/// static type (see `json_sql_string`'s doc), so a scalar-function value
/// argument's `Bytes`-vs-`String` shape carries no charset signal here --
/// `field_type.is_binary_string()` is the only trustworthy source, exactly as
/// Go's own `KindString` arm of `getRealJSONValue` checks
/// `ft.GetCharset() == charset.CharsetBin`. (Chunk-COLUMN reads never
/// actually produce `Bytes`: `tidb_chunk::row::Row::get_datum` always builds
/// `Datum::String` with the column's own collation, so a real BINARY column
/// reaches here as `String` with a binary collation either way.)
fn is_binary_datum(value: &Datum, field_type: Option<&FieldType>) -> bool {
    matches!(value, Datum::Bytes(_) | Datum::String(_))
        && field_type.is_some_and(FieldType::is_binary_string)
}

/// Renders `value` as the JSON `Opaque` value Go's `getRealJSONValue`
/// produces for a BINARY-charset argument, in THIS module's text-domain
/// [`Json`] model: [`Datum::to_mysql_json_with_source_type`] builds the typed
/// `BinaryJSON::Opaque`, whose `Display` is already the exact
/// `"base64:type<N>:<...>"` quoted string real TiDB prints (captured:
/// `VARBINARY`/`BLOB` render `type15`/`type252`, fixed `BINARY(n)` renders
/// `type254` padded to `n` bytes) -- reparsing that text through
/// [`parse_json`] yields the matching [`Json::String`] with no new formatting
/// logic in this crate.
fn binary_opaque_json(value: &Datum, field_type: &FieldType) -> Result<Json, EvalError> {
    let binary = value
        .to_mysql_json_with_source_type(field_type)
        .map_err(|_| EvalError::Unsupported("datum JSON conversion"))?;
    parse_json(&binary.to_string())
}

/// The SQL string an argument carries, or `None` when it is not a SQL string.
///
/// `Datum::String` and `Datum::Bytes` are the SAME SQL string value here: the
/// row evaluator builds a `String` from a parsed literal, while the chunk
/// rewriter builds `Bytes` for the identical literal (`rewriter`'s
/// `Expr::String` arm). Go draws every JSON argument boundary on the
/// argument's EvalType -- `ETString` for both -- so splitting them would make
/// `JSON_TYPE('{}')` succeed or raise 3146 depending on which evaluator ran.
///
/// NAMED BOUNDARY (GRADUATED for the typed call sites): this collapses
/// `CAST(x AS BINARY)` onto the same arm, and by itself carries no charset --
/// a binary literal is still `Datum::BinaryLiteral` and keeps its own arm;
/// only an explicit binary CAST lands here and reads as ordinary text.
///
/// The typed entry points ([`cast_as_json_typed`],
/// [`dispatch_typed`]) consult the argument's static [`FieldType`] BEFORE
/// falling into this function, so a genuine BINARY-charset column now renders
/// as a JSON `Opaque` value (`"base64:type254:..."`) via
/// [`binary_opaque_json`] instead of reaching here. This function remains the
/// plain-text fallback for callers with no `FieldType` (the row/AST evaluator
/// path in `crate::func`, which does not yet thread argument types).
fn json_sql_string(value: &Datum) -> Result<Option<&str>, EvalError> {
    let bytes = match value {
        Datum::String(text) => text.bytes(),
        Datum::Bytes(bytes) => bytes.as_slice(),
        _ => return Ok(None),
    };
    std::str::from_utf8(bytes)
        .map(Some)
        .map_err(|_| EvalError::Unsupported("invalid UTF-8 string datum"))
}

/// The DOCUMENT text of an argument Go types `ETJson`: a SQL string (which
/// carries `ParseToJSONFlag`, so it is parsed) or an already-typed JSON value
/// such as a JSON COLUMN, whose canonical text re-parses to itself.
///
/// This is deliberately narrower than [`json_sql_string`], which is also the
/// gate for signatures that demand a STRING specifically (`JSON_QUOTE`).
fn json_document_string(value: &Datum) -> Result<Option<std::borrow::Cow<'_, str>>, EvalError> {
    if let Datum::Json(document) = value {
        return Ok(Some(std::borrow::Cow::Owned(document.to_string())));
    }
    Ok(json_sql_string(value)?.map(std::borrow::Cow::Borrowed))
}

/// `JSON_VALID(arg)`, port of `builtinJSONValid{JSON,String,Others}Sig`.
/// String arguments are JSON documents; every non-string, non-JSON SQL value
/// is the Go `Others` signature and therefore returns zero rather than being
/// stringified.  `NULL` propagates.
fn json_valid(v: &Datum) -> Result<Datum, EvalError> {
    match v {
        Datum::Null => Ok(Datum::Null),
        // Non-UTF-8 bytes are simply not a JSON document, which is the zero
        // this signature reports rather than a statement error.
        Datum::String(_) | Datum::Bytes(_) => Ok(Datum::Int(i64::from(
            json_sql_string(v)
                .ok()
                .flatten()
                .is_some_and(|text| parse_json(text).is_ok()),
        ))),
        Datum::Int(_) | Datum::UInt(_) | Datum::Decimal(_) | Datum::Real(_) => Ok(Datum::Int(0)),
        Datum::Float32(_)
        | Datum::BinaryLiteral(_)
        | Datum::Duration(_)
        | Datum::Enum(_, _)
        | Datum::Bit(_)
        | Datum::Set(_, _)
        | Datum::Time(_)
        | Datum::Raw(_)
        | Datum::VectorFloat32(_) => Ok(Datum::Int(0)),
        Datum::Json(_) => Ok(Datum::Int(1)),
        Datum::MinNotNull | Datum::MaxValue => {
            Err(EvalError::Unsupported("range sentinel JSON_VALID argument"))
        }
    }
}

/// `JSON_TYPE(json_doc)`, port of `builtinJSONTypeSig.evalString` and
/// `types.BinaryJSON.Type` (`pkg/types/json_binary_functions.go`).
fn json_type(v: &Datum) -> Result<Datum, EvalError> {
    if v.is_null() {
        return Ok(Datum::Null);
    }
    let Some(s) = json_document_string(v)?.map(std::borrow::Cow::into_owned) else {
        return Err(crate::EvalError::Json(
            crate::JsonError::InvalidTypeForJson {
                argument: 1,
                function: "json_type",
            },
        ));
    };
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
    if let Some(text) = json_sql_string(v)? {
        return serde_json::to_string(text)
            .map(Datum::new_string)
            .map_err(|_| EvalError::Unsupported("JSON_QUOTE encoding"));
    }
    match v {
        Datum::Null => Ok(Datum::Null),
        _ => Err(EvalError::Json(JsonError::IncorrectType {
            argument: 1,
            function: "json_quote",
        })),
    }
}

/// `JSON_UNQUOTE(str)`, port of `builtinJSONUnquoteSig.evalString` plus
/// `types.UnquoteString`.  The initial document-validity gate is important:
/// a double-quoted value followed by another root value is an error, not an
/// almost-unquoted string (`TestJSONUnquote`).
fn json_unquote(v: &Datum) -> Result<Datum, EvalError> {
    let Some(text) = json_sql_string(v)? else {
        return if *v == Datum::Null {
            Ok(Datum::Null)
        } else {
            Err(EvalError::Unsupported("JSON_UNQUOTE requires string"))
        };
    };
    if text.len() < 2 || !text.starts_with('"') || !text.ends_with('"') {
        return Ok(Datum::new_string(text));
    }
    let Json::String(unquoted) = parse_json(text)? else {
        return Err(EvalError::Json(JsonError::InvalidText));
    };
    Ok(Datum::new_string(unquoted))
}

/// `CAST(expr AS JSON)`, port of the `builtinCast*AsJSONSig` family in
/// `pkg/expression/builtin_cast.go`.
///
/// Only the string signature carries `ParseToJSONFlag`, so a string argument
/// is PARSED as a JSON document (`CAST('abc' AS JSON)` is error 3140, not the
/// JSON string `"abc"`), while every other SQL value becomes its matching
/// JSON scalar. The result is this tier's canonical JSON text — see
/// [`format_json`] for why that is a string rather than a BinaryJSON value.
pub(crate) fn cast_as_json(value: &Datum) -> Result<Datum, EvalError> {
    if value.is_null() {
        return Ok(Datum::Null);
    }
    let json = match json_sql_string(value)? {
        Some(text) => parse_json(text)?,
        None => datum_json_scalar(value)?,
    };
    Ok(Datum::new_string(format_json(&json)))
}

/// [`cast_as_json`] with the source argument's static `FieldType`, when the
/// caller has one, consulted first: a genuine BINARY-charset argument
/// (`CAST(varbinary_col AS JSON)`) renders as the JSON `Opaque` value real
/// TiDB produces (captured: `base64:type15:...`) instead of being parsed as
/// JSON text or read as an ordinary string. `field_type: None` is exactly
/// [`cast_as_json`].
pub(crate) fn cast_as_json_typed(
    value: &Datum,
    field_type: Option<&FieldType>,
) -> Result<Datum, EvalError> {
    if value.is_null() {
        return Ok(Datum::Null);
    }
    if let Some(field_type) = field_type {
        if is_binary_datum(value, Some(field_type)) {
            return Ok(Datum::new_string(format_json(&binary_opaque_json(
                value, field_type,
            )?)));
        }
    }
    cast_as_json(value)
}

/// `JSON_ARRAY(value [, value] ...)`, port of `jsonArrayFunctionClass` and
/// `builtinJSONArraySig` in `pkg/expression/builtin_json.go`.  SQL strings
/// remain JSON strings, while numeric and NULL datums become their matching
/// JSON scalar values.  Typed boolean/BinaryJSON arguments are outside this
/// evaluator's value domain and are not inferred from an integer or string.
fn json_array(vals: &[Datum], arg_types: &[Option<FieldType>]) -> Result<Datum, EvalError> {
    let values = vals
        .iter()
        .zip(arg_types.iter())
        .map(|(v, ft)| json_mutation_value_argument(v, ft.as_ref()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Datum::new_string(format_json(&Json::Array(values))))
}

/// `JSON_OBJECT(key, value [, key, value] ...)`, port of
/// `jsonObjectFunctionClass` and `builtinJSONObjectSig` in
/// `pkg/expression/builtin_json.go`.  Keys are SQL-string-coerced, NULL keys
/// are rejected, and values follow the scalar JSON value boundary used by
/// `JSON_ARRAY`.
fn json_object(vals: &[Datum], arg_types: &[Option<FieldType>]) -> Result<Datum, EvalError> {
    if !vals.len().is_multiple_of(2) {
        return Err(EvalError::Unsupported(
            "JSON_OBJECT requires key/value pairs",
        ));
    }
    let mut object = serde_json::Map::new();
    for (pair, types) in vals.chunks_exact(2).zip(arg_types.chunks_exact(2)) {
        let Some(key) = coerce_str(&pair[0])? else {
            return Err(EvalError::Json(JsonError::NullMemberName));
        };
        let value = json_mutation_value_argument(&pair[1], types[1].as_ref())?;
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
            return Err(EvalError::Json(JsonError::InvalidPathMultipleSelection));
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
            return Err(EvalError::Json(JsonError::InvalidPathMultipleSelection));
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
        // Go checks these in order: `$` alone is vacuous (3153), and any
        // wildcard/range leg is a multiple selection (3149).
        if path.legs.is_empty() {
            return Err(EvalError::Json(JsonError::VacuousPath));
        }
        if path.legs.iter().any(|leg| {
            !matches!(
                leg,
                PathLeg::Key(_) | PathLeg::Array(ArraySelection::Index(_))
            )
        }) {
            return Err(EvalError::Json(JsonError::InvalidPathMultipleSelection));
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
fn json_array_append(vals: &[Datum], arg_types: &[Option<FieldType>]) -> Result<Datum, EvalError> {
    if vals.len() < 3 || vals.len().is_multiple_of(2) {
        return Err(EvalError::Unsupported("JSON_ARRAY_APPEND arity"));
    }
    let Some(mut document) = parse_json_document_argument(&vals[0])? else {
        return Ok(Datum::Null);
    };
    for (pair, types) in vals[1..]
        .chunks_exact(2)
        .zip(arg_types[1..].chunks_exact(2))
    {
        let Some(path) = coerce_str(&pair[0])? else {
            return Ok(Datum::Null);
        };
        let path = parse_path(&path)?;
        if path.could_match_multiple {
            return Err(EvalError::Json(JsonError::InvalidPathMultipleSelection));
        }
        let value = json_mutation_value_argument(&pair[1], types[1].as_ref())?;
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
fn json_array_insert(vals: &[Datum], arg_types: &[Option<FieldType>]) -> Result<Datum, EvalError> {
    if vals.len() < 3 || vals.len().is_multiple_of(2) {
        return Err(EvalError::Unsupported("JSON_ARRAY_INSERT arity"));
    }
    let Some(mut document) = parse_json_document_argument(&vals[0])? else {
        return Ok(Datum::Null);
    };
    for (pair, types) in vals[1..]
        .chunks_exact(2)
        .zip(arg_types[1..].chunks_exact(2))
    {
        let Some(path) = coerce_str(&pair[0])? else {
            return Ok(Datum::Null);
        };
        let path = parse_path(&path)?;
        if path.could_match_multiple {
            return Err(EvalError::Json(JsonError::InvalidPathMultipleSelection));
        }
        // The last leg must name an array CELL; `$` and a trailing object key
        // both leave nothing to insert before (3165).
        let Some(PathLeg::Array(ArraySelection::Index(index))) = path.legs.last() else {
            return Err(EvalError::Json(JsonError::InvalidPathArrayCell));
        };
        if path.legs.iter().any(|leg| {
            !matches!(
                leg,
                PathLeg::Key(_) | PathLeg::Array(ArraySelection::Index(_))
            )
        }) {
            return Err(EvalError::Json(JsonError::InvalidPathArrayCell));
        }
        let value = json_mutation_value_argument(&pair[1], types[1].as_ref())?;
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
fn json_modify(
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
    mode: JsonModifyMode,
) -> Result<Datum, EvalError> {
    if vals.len() < 3 || vals.len().is_multiple_of(2) {
        return Err(EvalError::Unsupported("JSON modification arity"));
    }
    let Some(mut document) = parse_json_document_argument(&vals[0])? else {
        return Ok(Datum::Null);
    };
    for (pair, types) in vals[1..]
        .chunks_exact(2)
        .zip(arg_types[1..].chunks_exact(2))
    {
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
            return Err(EvalError::Json(JsonError::InvalidPathMultipleSelection));
        }
        let value = json_mutation_value_argument(&pair[1], types[1].as_ref())?;
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
///
/// The deprecation warning `builtinJSONMergeSig.evalJSON` appends for the
/// `JSON_MERGE` spelling is raised by the caller, which owns the statement
/// context; this function is the value.
fn json_merge(vals: &[Datum], function: &'static str) -> Result<Datum, EvalError> {
    let mut values = Vec::with_capacity(vals.len());
    for (index, value) in vals.iter().enumerate() {
        let Some(value) = parse_json_merge_argument(value, index, function)? else {
            return Ok(Datum::Null);
        };
        values.push(value);
    }
    Ok(Datum::new_string(format_json(&merge_json_values(values))))
}

/// One document argument of the `JSON_MERGE*` family.
///
/// Go types every argument of these as `ETJson`, and `verifyJSONArgsType`
/// (`jsonMergeFunctionClass.verifyArgs`) then demands that each argument be a
/// JSON value or a STRING: `JSON_MERGE_PRESERVE('[1]', 3)` is 3146, not a
/// merge with the number 3. A string argument carries `ParseToJSONFlag`, so
/// `'1'` is the JSON number 1 and `'{}'` is the empty object -- unlike the
/// VALUE arguments of `JSON_SET`/`JSON_ARRAY_APPEND`, which stay JSON strings.
fn parse_json_merge_argument(
    value: &Datum,
    index: usize,
    function: &'static str,
) -> Result<Option<Json>, EvalError> {
    if let Some(text) = json_document_string(value)? {
        return Ok(Some(parse_json(&text)?));
    }
    match value {
        Datum::Null => Ok(None),
        _ => Err(EvalError::Json(JsonError::InvalidTypeForJson {
            argument: index + 1,
            function,
        })),
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
    for (index, value) in vals.iter().enumerate() {
        values.push(parse_json_merge_argument(value, index, "json_merge_patch")?);
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
///
/// `field_type` is the argument's static type when the caller has one (see
/// [`dispatch_typed`]); a BINARY-charset payload then renders as the JSON
/// `Opaque` value Go's implicit `CAST(... AS JSON)` produces instead of an
/// ordinary JSON string.
fn json_mutation_value_argument(
    value: &Datum,
    field_type: Option<&FieldType>,
) -> Result<Json, EvalError> {
    if let Some(field_type) = field_type {
        if is_binary_datum(value, Some(field_type)) {
            return binary_opaque_json(value, field_type);
        }
    }
    if let Some(text) = json_sql_string(value)? {
        return Ok(Json::String(text.to_owned()));
    }
    match value {
        Datum::Null => Ok(Json::Null),
        Datum::Int(value) => Ok(Json::Number((*value).into())),
        Datum::UInt(value) => Ok(Json::Number((*value).into())),
        Datum::Real(value) => Number::from_f64(*value)
            .map(Json::Number)
            .ok_or(EvalError::FloatOverflow),
        Datum::Decimal(value) => parse_json(&value.to_string()),
        Datum::MinNotNull | Datum::MaxValue => {
            Err(EvalError::Unsupported("range sentinel JSON mutation value"))
        }
        other => datum_json_scalar(other),
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
    // `BinaryJSON.Extract`: `[0]` and `[last]` select a NON-ARRAY value
    // itself. The array check has to come first -- reading it as a self
    // selection for an array too would make `$[0]` on `[1, 2]` name the
    // whole array instead of its first element.
    if !matches!(value, Json::Array(_))
        && matches!(first, PathLeg::Array(ArraySelection::Index(index)) if *index == 0 || *index == -1)
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
    if let Some(text) = json_sql_string(value)? {
        return parse_json(text);
    }
    match value {
        Datum::Null => Err(EvalError::Unsupported("JSON value is NULL")),
        Datum::Int(value) => Ok(Json::Number((*value).into())),
        Datum::UInt(value) => Ok(Json::Number((*value).into())),
        Datum::Real(value) => Number::from_f64(*value)
            .map(Json::Number)
            .ok_or(EvalError::FloatOverflow),
        Datum::Decimal(value) => parse_json(&value.to_string()),
        Datum::MinNotNull | Datum::MaxValue => {
            Err(EvalError::Unsupported("range sentinel JSON value"))
        }
        other => datum_json_scalar(other),
    }
}

/// The candidate-side cast in `JSON_MEMBER_OF`: SQL strings become JSON
/// strings, while numeric datums retain their JSON numeric kind.
fn json_value_argument(value: &Datum) -> Result<Json, EvalError> {
    if let Some(text) = json_sql_string(value)? {
        return Ok(Json::String(text.to_owned()));
    }
    match value {
        Datum::Null => Err(EvalError::Unsupported("JSON value is NULL")),
        Datum::Int(value) => Ok(Json::Number((*value).into())),
        Datum::UInt(value) => Ok(Json::Number((*value).into())),
        Datum::Real(value) => Number::from_f64(*value)
            .map(Json::Number)
            .ok_or(EvalError::FloatOverflow),
        Datum::Decimal(value) => parse_json(&value.to_string()),
        Datum::MinNotNull | Datum::MaxValue => {
            Err(EvalError::Unsupported("range sentinel JSON value"))
        }
        other => datum_json_scalar(other),
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
        Datum::String(_) | Datum::Bytes(_) => {
            parse_json(json_sql_string(v)?.unwrap_or_default()).map(Some)
        }
        Datum::Int(_)
        | Datum::UInt(_)
        | Datum::Decimal(_)
        | Datum::Real(_)
        | Datum::MinNotNull
        | Datum::MaxValue => Err(EvalError::Unsupported("JSON document requires string")),
        Datum::Json(value) => parse_json(&value.to_string()).map(Some),
        Datum::Float32(_)
        | Datum::BinaryLiteral(_)
        | Datum::Duration(_)
        | Datum::Enum(_, _)
        | Datum::Bit(_)
        | Datum::Set(_, _)
        | Datum::Time(_)
        | Datum::Raw(_)
        | Datum::VectorFloat32(_) => Err(EvalError::Unsupported(
            "JSON document requires JSON or string",
        )),
    }
}

fn datum_json_scalar(value: &Datum) -> Result<Json, EvalError> {
    let binary = value
        .to_mysql_json()
        .map_err(|_| EvalError::Unsupported("datum JSON conversion"))?;
    parse_json(&binary.to_string())
}

fn parse_json(s: &str) -> Result<Json, EvalError> {
    serde_json::from_str(s).map_err(|_| EvalError::Json(JsonError::InvalidText))
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
#[path = "json_tests.rs"]
mod tests;
