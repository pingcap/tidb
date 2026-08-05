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

//! Building JSON out of SQL values: `JSON_ARRAY`, `JSON_OBJECT`,
//! `JSON_QUOTE`, `JSON_UNQUOTE`.
//!
//! Mirrors `builtinJSON{Array,Object,Quote,Unquote}Sig` in
//! `pkg/expression/builtin_json.go` and `types.UnquoteString` in
//! `pkg/types/json_binary_functions.go`.
//!
//! `JSON_QUOTE`/`JSON_UNQUOTE` are the string<->JSON pair and are NOT
//! inverses across the whole domain: quote demands a string argument and
//! escapes it, while unquote passes through anything that is not a complete
//! double-quoted JSON string. The constructors take VALUE arguments, so
//! `JSON_ARRAY('[1]')` is a one-element array holding the string `"[1]"`.

use serde_json::Value as Json;

use super::text::format_json;
use super::value::{json_argument, json_sql_string, parse_json, StringArgument};
use crate::coerce::coerce_str;
use crate::{Datum, EvalError, JsonError};
use tidb_datatype::FieldType;

/// `JSON_QUOTE(str)`, port of `builtinJSONQuoteSig.evalString`.  Go's
/// `encoding/json.Encoder` has `SetEscapeHTML(false)`; serde_json has the
/// same HTML rule for strings, while retaining Go-compatible JSON escapes.
pub(super) fn json_quote(v: &Datum) -> Result<Datum, EvalError> {
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
pub(super) fn json_unquote(v: &Datum) -> Result<Datum, EvalError> {
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

/// `JSON_ARRAY(value [, value] ...)`, port of `jsonArrayFunctionClass` and
/// `builtinJSONArraySig` in `pkg/expression/builtin_json.go`.  SQL strings
/// remain JSON strings, while numeric and NULL datums become their matching
/// JSON scalar values.  Typed boolean/BinaryJSON arguments are outside this
/// evaluator's value domain and are not inferred from an integer or string.
pub(super) fn json_array(
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
) -> Result<Datum, EvalError> {
    let values = vals
        .iter()
        .zip(arg_types.iter())
        .map(|(v, ft)| json_argument(v, StringArgument::Value, ft.as_ref()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Datum::new_string(format_json(&Json::Array(values))))
}

/// `JSON_OBJECT(key, value [, key, value] ...)`, port of
/// `jsonObjectFunctionClass` and `builtinJSONObjectSig` in
/// `pkg/expression/builtin_json.go`.  Keys are SQL-string-coerced, NULL keys
/// are rejected, and values follow the scalar JSON value boundary used by
/// `JSON_ARRAY`.
pub(super) fn json_object(
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
) -> Result<Datum, EvalError> {
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
        let value = json_argument(&pair[1], StringArgument::Value, types[1].as_ref())?;
        object.insert(key, value);
    }
    Ok(Datum::new_string(format_json(&Json::Object(object))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{FieldTypeCode, FieldTypeFlags};

    fn boolean_int() -> FieldType {
        let mut ft = FieldType::new(FieldTypeCode::LongLong);
        ft.set_flen(1);
        ft.add_flags(FieldTypeFlags::IS_BOOLEAN);
        ft
    }

    /// A `booleanFunctions` result (here the `IS_BOOLEAN` flag stands in for the
    /// static type of a `1<2`/`IN`/`IS NULL`/`IS_IPV4` argument) becomes a JSON
    /// `true`/`false` literal, exactly as `builtinCastIntAsJSONSig.evalJSON`
    /// does. A plain integer -- the `1+1`, `IS_UUID`, or untyped row/AST case
    /// Go leaves OUT of the boolean map -- keeps its numeric rendering, so the
    /// fix cannot silently turn every integer into a boolean.
    #[test]
    fn a_boolean_flagged_int_is_a_json_literal_and_a_plain_int_is_a_number() {
        let one = Datum::Int(1);
        let zero = Datum::Int(0);
        assert_eq!(
            json_array(
                &[one.clone(), zero.clone()],
                &[Some(boolean_int()), Some(boolean_int())],
            )
            .unwrap(),
            Datum::new_string("[true, false]".to_owned()),
        );
        // Same values, no boolean flag: the numeric rendering is unchanged.
        assert_eq!(
            json_array(
                &[one.clone(), zero.clone()],
                &[
                    Some(FieldType::new(FieldTypeCode::LongLong)),
                    Some(FieldType::new(FieldTypeCode::LongLong)),
                ],
            )
            .unwrap(),
            Datum::new_string("[1, 0]".to_owned()),
        );
        // The untyped row/AST path (no field type) also keeps the number.
        assert_eq!(
            json_array(&[one], &[None]).unwrap(),
            Datum::new_string("[1]".to_owned()),
        );
        // JSON_OBJECT threads the same value coercion for its values.
        assert_eq!(
            json_object(
                &[Datum::new_string("k".to_owned()), zero],
                &[None, Some(boolean_int())],
            )
            .unwrap(),
            Datum::new_string("{\"k\": false}".to_owned()),
        );
    }
}
