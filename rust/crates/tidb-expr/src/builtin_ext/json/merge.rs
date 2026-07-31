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

//! Combining documents: `JSON_MERGE`, `JSON_MERGE_PRESERVE`, and
//! `JSON_MERGE_PATCH`.
//!
//! Mirrors `builtinJSONMerge{,PatchSig}` in
//! `pkg/expression/builtin_json.go` and `MergeBinaryJSON` /
//! `MergePatchBinaryJSON` in `pkg/types/json_binary_functions.go`.
//!
//! The two merges are NOT variants of one rule. PRESERVE (and its deprecated
//! `JSON_MERGE` spelling) keeps both sides -- duplicate object keys become an
//! array of both values, non-objects concatenate into one array.
//! PATCH is RFC 7396: the patch REPLACES, and a JSON `null` in the patch
//! DELETES the key. Unlike every other function here, PATCH also
//! distinguishes SQL NULL from JSON `null`: the former truncates the merge.

use std::collections::BTreeMap;

use serde_json::Value as Json;

use super::text::format_json;
use super::value::{json_document_string, parse_json};
use crate::{Datum, EvalError, JsonError};

/// `JSON_MERGE` and `JSON_MERGE_PRESERVE`, ports of `types.MergeBinaryJSON`.
///
/// The deprecation warning `builtinJSONMergeSig.evalJSON` appends for the
/// `JSON_MERGE` spelling is raised by the caller, which owns the statement
/// context; this function is the value.
pub(super) fn json_merge(vals: &[Datum], function: &'static str) -> Result<Datum, EvalError> {
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
pub(super) fn json_merge_patch(vals: &[Datum]) -> Result<Datum, EvalError> {
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
