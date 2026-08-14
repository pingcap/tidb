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

//! Scalar facts ABOUT a document: `JSON_VALID`, `JSON_TYPE`, `JSON_LENGTH`,
//! `JSON_KEYS`, `JSON_SUM_CRC32`.
//!
//! Mirrors `builtinJSON{Valid*,Type,Length,Keys,Keys2Args,SumCRC32}Sig` in
//! `pkg/expression/builtin_json.go` and `BinaryJSON.Type` /
//! `GetElemCount` in `pkg/types/json_binary_functions.go`.
//!
//! `JSON_VALID` is the odd one and deliberately so: Go resolves it to one of
//! THREE signatures at plan-build time from the argument's EvalType, and the
//! `Others` signature answers 0 without ever looking at the value. Every
//! other function here demands a real document and raises rather than
//! guessing. `JSON_DEPTH` and the storage sizes live in `super::super::json2`
//! because they read BinaryJSON's encoded layout, not its value.

use serde_json::{Number, Value as Json};

use super::path::{extract, parse_path};
use super::text::format_json;
use super::value::{
    json_document_string, json_sql_string, parse_json, parse_json_document_argument,
};
use crate::coerce::coerce_str;
use crate::expression::{ConstLevel, Expression};
use crate::{Columns, Datum, EvalError, JsonError};
use tidb_chunk::row::Row;

/// Per-signature cache and lazy evaluator for `JSON_SCHEMA_VALID`.
///
/// Clone deliberately starts empty, matching Go's
/// `builtinJSONSchemaValidSig.Clone`. Only strict constants are cached because
/// Rust's evaluation context does not yet expose Go's `CtxID`; a context-only
/// parameter must never leak its compiled schema into another execution.
#[derive(Debug, Default)]
pub(crate) struct JsonSchemaCache(
    std::sync::OnceLock<Result<Option<PreparedJsonSchema>, EvalError>>,
);

#[derive(Debug)]
struct PreparedJsonSchema {
    schema: Json,
    validator: std::sync::OnceLock<Result<jsonschema::Validator, EvalError>>,
}

impl Clone for JsonSchemaCache {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl JsonSchemaCache {
    pub(crate) fn eval(
        &self,
        args: &[Expression],
        ctx: &impl Columns,
        row: Row<'_>,
    ) -> Result<Datum, EvalError> {
        let [schema_arg, document_arg] = args else {
            return Err(EvalError::WrongParameterCount("json_schema_valid"));
        };
        let schema_value = schema_arg.eval(ctx, row)?;
        if schema_value.is_null() {
            return Ok(Datum::Null);
        }

        if schema_arg.const_level() == ConstLevel::STRICT {
            let schema = match self.0.get_or_init(|| prepare_json_schema(&schema_value)) {
                Ok(Some(schema)) => schema,
                Ok(None) => return Ok(Datum::Null),
                Err(error) => return Err(error.clone()),
            };
            return validate_json_schema(schema, &document_arg.eval(ctx, row)?);
        }

        let schema = prepare_json_schema(&schema_value)?;
        let Some(schema) = schema.as_ref() else {
            return Ok(Datum::Null);
        };
        validate_json_schema(schema, &document_arg.eval(ctx, row)?)
    }
}

/// Parses and checks the schema argument without resolving external `$ref`s.
/// TiDB's qri-io validator does not fetch them until document validation, so a
/// NULL document still short-circuits without filesystem or network access.
fn prepare_json_schema(value: &Datum) -> Result<Option<PreparedJsonSchema>, EvalError> {
    let Some(schema) = parse_json_document_argument(value)? else {
        return Ok(None);
    };
    if !matches!(schema, Json::Object(_) | Json::Bool(_)) {
        return Err(EvalError::Json(JsonError::InvalidJsonType {
            argument: 1,
            function: "json_schema_valid",
            required: "object".to_owned(),
        }));
    }
    jsonschema::draft201909::meta::validate(&schema).map_err(invalid_json_schema)?;
    Ok(Some(PreparedJsonSchema {
        schema,
        validator: std::sync::OnceLock::new(),
    }))
}

fn build_json_schema(schema: &Json) -> Result<jsonschema::Validator, EvalError> {
    jsonschema::options()
        .with_draft(jsonschema::Draft::Draft201909)
        // qri-io's Draft2019_09 `Format` keyword is an assertion, while the
        // Rust validator follows the newer annotation default unless enabled.
        .should_validate_formats(true)
        .build(schema)
        .map_err(invalid_json_schema)
}

fn invalid_json_schema(error: impl ToString) -> EvalError {
    EvalError::Json(JsonError::InvalidJsonType {
        argument: 1,
        function: "json_schema_valid",
        required: error.to_string(),
    })
}

/// Validates the document, resolving external references only after the
/// document has survived the source NULL/parse boundary.
fn validate_json_schema(schema: &PreparedJsonSchema, document: &Datum) -> Result<Datum, EvalError> {
    let Some(document) = parse_json_document_argument(document)? else {
        return Ok(Datum::Null);
    };
    let validator = match schema
        .validator
        .get_or_init(|| build_json_schema(&schema.schema))
    {
        Ok(validator) => validator,
        Err(error) => return Err(error.clone()),
    };
    Ok(Datum::Int(i64::from(validator.is_valid(&document))))
}

/// Datum-level `JSON_SCHEMA_VALID(schema, document)` used by callers that do
/// not own a reusable scalar-function node.
pub(super) fn json_schema_valid(values: &[Datum]) -> Result<Datum, EvalError> {
    let Some(schema) = prepare_json_schema(&values[0])? else {
        return Ok(Datum::Null);
    };
    validate_json_schema(&schema, &values[1])
}

/// `JSON_VALID(arg)`, port of `builtinJSONValid{JSON,String,Others}Sig`.
/// String arguments are JSON documents; every non-string, non-JSON SQL value
/// is the Go `Others` signature and therefore returns zero rather than being
/// stringified.  `NULL` propagates.
pub(super) fn json_valid(v: &Datum) -> Result<Datum, EvalError> {
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
pub(super) fn json_type(v: &Datum) -> Result<Datum, EvalError> {
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

/// `JSON_LENGTH(json_doc [, path])`, port of `builtinJSONLengthSig.evalInt`.
/// As in TiDB, a wildcard/range path is a true SQL error rather than a length
/// of an implicitly auto-wrapped selection.
pub(super) fn json_length(vals: &[Datum]) -> Result<Datum, EvalError> {
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

/// `JSON_KEYS(json_doc [, path])`, port of
/// `builtinJSONKeys{Sig,2ArgsSig}.evalJSON`.  The result is an array of the
/// selected object's keys, in BinaryJSON's byte-sorted object order.  A
/// scalar, array, missing path, or selected non-object is SQL NULL; a path
/// that could select more than one value is an error.
pub(super) fn json_keys(vals: &[Datum]) -> Result<Datum, EvalError> {
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

/// `JSON_SUM_CRC32(json_doc)`, port of `builtinJSONSumCRC32Sig.evalInt` in
/// `pkg/expression/builtin_json.go`.  The Go signature receives a JSON array
/// plus an `ARRAY`-typed `FieldType` carried by the cast expression; the
/// frozen Rust evaluator has no typed JSON datum or FieldType metadata.  The
/// representable text-domain contract therefore accepts homogeneous scalar
/// arrays (numbers or strings), preserving Go's `fmt.Appendf("%v", item)`
/// bytes before each IEEE CRC32 and returning the int64 sum.  The target-type
/// checks (signed/unsigned range, fixed string width, and explicit JSON path
/// extraction) remain an orchestrator boundary rather than guessed defaults.
pub(super) fn json_sum_crc32(value: &Datum) -> Result<Datum, EvalError> {
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
