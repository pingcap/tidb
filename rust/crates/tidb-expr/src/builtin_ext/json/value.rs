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

//! Turning a SQL argument into a JSON value: the ETJson coercion boundary and
//! `CAST(expr AS JSON)`.
//!
//! Mirrors `getRealJSONValue` and the `builtinCast*AsJSONSig` family in
//! `pkg/expression/builtin_cast.go`. The whole family's behaviour hinges on
//! ONE distinction that lives here: whether an argument carries
//! `ParseToJSONFlag`. A DOCUMENT argument (`JSON_EXTRACT`'s first argument,
//! every `JSON_MERGE*` argument) parses its string, so `'1'` is the JSON
//! number 1; a VALUE argument (`JSON_SET`'s values, `JSON_ARRAY`'s elements)
//! does not, so `'1'` is the JSON string `"1"`.

use serde_json::{Number, Value as Json};

use super::text::format_json;
use crate::{Datum, EvalError, JsonError};
use tidb_datatype::{FieldType, FieldTypeFlags};

/// The integer an argument carries when it is a boolean-flagged INT, so
/// [`json_argument`] and [`cast_as_json`] can render it as a JSON `true`/`false`
/// literal, which is what Go's `builtinCastIntAsJSONSig.evalJSON` does when
/// `mysql.HasIsBooleanFlag(arg.GetType().GetFlag())` is set. Every name in Go's
/// `booleanFunctions` map (`pkg/expression/function_traits.go`) -- the
/// comparisons, the logical connectives, `IS NULL`/`IS [NOT] TRUE|FALSE`, `IN`,
/// `LIKE`/`REGEXP` and the `IS_IPV4*`/`IS_IPV6` predicates -- stamps this flag on
/// its `ETInt` result, so a value produced by one of them becomes a JSON boolean
/// rather than the integer `1`/`0`. `field_type: None` (the untyped row/AST path)
/// carries no flag and so keeps the integer rendering.
fn boolean_flagged_int(value: &Datum, field_type: Option<&FieldType>) -> Option<i64> {
    if !field_type.is_some_and(|ft| ft.has_flag(FieldTypeFlags::IS_BOOLEAN)) {
        return None;
    }
    match value {
        Datum::Int(int) => Some(*int),
        Datum::UInt(int) => Some(*int as i64),
        _ => None,
    }
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
pub(super) fn json_sql_string(value: &Datum) -> Result<Option<&str>, EvalError> {
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
pub(super) fn json_document_string(
    value: &Datum,
) -> Result<Option<std::borrow::Cow<'_, str>>, EvalError> {
    if let Datum::Json(document) = value {
        return Ok(Some(std::borrow::Cow::Owned(document.to_string())));
    }
    Ok(json_sql_string(value)?.map(std::borrow::Cow::Borrowed))
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
    // `CAST(<boolean expr> AS JSON)` is `builtinCastIntAsJSONSig.evalJSON`'s
    // boolean arm: a value from a `booleanFunctions` name becomes JSON
    // `true`/`false`, exactly as it does as a `JSON_ARRAY`/`JSON_OBJECT` element.
    if let Some(int) = boolean_flagged_int(value, field_type) {
        return Ok(Datum::new_string(format_json(&Json::Bool(int != 0))));
    }
    cast_as_json(value)
}

/// What a SQL STRING argument means to the signature receiving it -- Go's
/// `ParseToJSONFlag`, the single bit that separates the JSON family's two
/// argument kinds.
#[derive(Clone, Copy)]
pub(super) enum StringArgument {
    /// The flag is SET: the string IS a JSON document, so `'1'` is the JSON
    /// number 1 and `'{}'` the empty object. `JSON_CONTAINS`'s candidate and
    /// `JSON_OVERLAPS`'s two arguments.
    Document,
    /// `DisableParseJSONFlag4Expr`: the string is a JSON string VALUE, so
    /// `'1'` is `"1"` and `'{}'` is `"{}"`. `MEMBER OF`'s candidate and every
    /// `JSON_SET`/`JSON_ARRAY`/`JSON_ARRAY_APPEND` value.
    Value,
}

/// The one SQL-value to JSON-value coercion behind every non-document
/// argument in this family, a port of `getRealJSONValue` plus the implicit
/// `CAST(... AS JSON)` the signatures build over their arguments.
///
/// Exactly two things vary between call sites, and both are parameters here
/// rather than a third copy of the datum table:
///
/// - `string` decides a SQL string's meaning ([`StringArgument`]). This is
///   the whole reason `JSON_CONTAINS('[1]', '1')` is TRUE while
///   `JSON_ARRAY('1')` is `["1"]`.
/// - `field_type` is the argument's static type when the caller has one (see
///   [`super::dispatch_typed`]), so a genuine BINARY-charset payload renders
///   as the JSON `Opaque` value (`"base64:type15:..."`) Go produces instead
///   of an ordinary JSON string. `None` is the untyped row/AST path.
///
/// SQL NULL becomes JSON `null`: that is what the mutation signatures store
/// (`JSON_SET('{}', '$.a', NULL)` is `{"a": null}`). The predicate callers
/// answer SQL NULL for a NULL argument BEFORE calling, so they never observe
/// this arm.
pub(super) fn json_argument(
    value: &Datum,
    string: StringArgument,
    field_type: Option<&FieldType>,
) -> Result<Json, EvalError> {
    if let Some(field_type) = field_type {
        if is_binary_datum(value, Some(field_type)) {
            return binary_opaque_json(value, field_type);
        }
    }
    if let Some(int) = boolean_flagged_int(value, field_type) {
        return Ok(Json::Bool(int != 0));
    }
    if let Some(text) = json_sql_string(value)? {
        return match string {
            StringArgument::Document => parse_json(text),
            StringArgument::Value => Ok(Json::String(text.to_owned())),
        };
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
            Err(EvalError::Unsupported("range sentinel JSON value"))
        }
        other => datum_json_scalar(other),
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

pub(super) fn parse_json(s: &str) -> Result<Json, EvalError> {
    serde_json::from_str(s).map_err(|_| EvalError::Json(JsonError::InvalidText))
}
