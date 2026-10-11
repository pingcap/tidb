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

//! `JSON_SEARCH`: find string leaves by LIKE pattern and report their paths,
//! a port of `builtinJSONSearchSig.evalJSON` in
//! `pkg/expression/builtin_json.go` on the `BinaryJSON` port of `Search` /
//! `Walk` (an array leg selects only an array, and a full path is reported
//! once across the whole walk).

use super::modify::{binary_json_error, json_document, parse_json_path};
use crate::coerce::coerce_str;
use crate::{Datum, EvalError, JsonError};
use tidb_datatype::{FieldType, JSONSearchMode};

/// `JSON_SEARCH(json_doc, one_or_all, search_str [, escape_char [, path] ...])`.
pub(super) fn json_search(
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
) -> Result<Datum, EvalError> {
    let Some(document) = json_document(&vals[0], arg_types.first().and_then(Option::as_ref))?
    else {
        return Ok(Datum::Null);
    };
    let Some(mode) = coerce_str(&vals[1])? else {
        return Ok(Datum::Null);
    };
    let mode = match mode.to_lowercase().as_str() {
        "one" => JSONSearchMode::One,
        "all" => JSONSearchMode::All,
        // Go's row `evalJSON` raises `ErrInvalidJSONContainsPathType`
        // (3150), but only constant folding evaluates by row and it swallows
        // the error; statements evaluate through `vecEvalJSON`, which leaves
        // the check to `BinaryJSON.Search`: `ErrJSONBadOneOrAllArg` (3154)
        // naming json_search.
        _ => {
            return Err(EvalError::Json(JsonError::BadOneOrAllArg {
                function: "json_search",
            }))
        }
    };
    let Some(pattern) = coerce_str(&vals[2])? else {
        return Ok(Datum::Null);
    };
    // A NULL or empty escape is the default backslash; longer is an error.
    let escape = match vals.get(3).map(coerce_str).transpose()?.flatten() {
        None => '\\',
        Some(escape) if escape.is_empty() => '\\',
        Some(escape) if escape.len() == 1 => char::from(escape.as_bytes()[0]),
        Some(_) => {
            return Err(EvalError::IncorrectArguments(
                "Incorrect arguments to ESCAPE".to_owned(),
            ))
        }
    };
    let mut paths = Vec::with_capacity(vals.len().saturating_sub(4));
    for path in vals.iter().skip(4) {
        let Some(path) = coerce_str(path)? else {
            return Ok(Datum::Null);
        };
        paths.push(parse_json_path(&path)?);
    }
    Ok(document
        .search(mode, &pattern, escape, &paths)
        .map_err(binary_json_error)?
        .map_or(Datum::Null, Datum::Json))
}
