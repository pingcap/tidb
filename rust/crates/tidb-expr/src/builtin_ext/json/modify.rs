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
//! Mirrors `builtinJSON{Set,Insert,Replace,Remove,ArrayAppend,ArrayInsert}Sig`
//! in `pkg/expression/builtin_json.go` and `BinaryJSON.Modify` / `Remove` /
//! `ArrayInsert` / `appendJSONArray` in
//! `pkg/types/json_binary_functions.go`.
//!
//! Every function here shares two rules. Path/value pairs apply IN ORDER, so
//! a later pair sees the document an earlier pair produced (removing `$[0]`
//! shifts what `$[1]` names). And every path must be a single exact
//! selection: a wildcard, range, or `**` leg is 3149 before any mutation
//! runs, which is why these traversals only ever handle `Key` and
//! `Array(Index)` legs.

use serde_json::Value as Json;

use super::path::{parse_path, ArraySelection, PathLeg};
use super::text::format_json;
use super::value::{json_argument, parse_json_document_argument, StringArgument};
use crate::coerce::coerce_str;
use crate::{Datum, EvalError, JsonError};
use tidb_datatype::FieldType;

/// `JSON_REMOVE(json_doc, path [, path] ...)`, port of
/// `builtinJSONRemoveSig.evalJSON` and `BinaryJSON.Remove`.  Paths are
/// applied in order, so removing an earlier array element shifts the indexes
/// seen by later paths.  Wildcards, ranges, recursive paths, and the root `$`
/// path are invalid; an absent exact path is a no-op.
pub(super) fn json_remove(vals: &[Datum]) -> Result<Datum, EvalError> {
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
pub(super) fn json_array_append(
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
) -> Result<Datum, EvalError> {
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
        let value = json_argument(&pair[1], StringArgument::Value, types[1].as_ref())?;
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
pub(super) fn json_array_insert(
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
) -> Result<Datum, EvalError> {
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
        let value = json_argument(&pair[1], StringArgument::Value, types[1].as_ref())?;
        insert_at_path(&mut document, &path.legs, *index, &value);
    }
    Ok(Datum::new_string(format_json(&document)))
}

/// The three `JSON_{SET,INSERT,REPLACE}` modifiers share Go's
/// `BinaryJSON.Modify` traversal. Paths are applied sequentially, so a later
/// pair observes the document produced by earlier pairs.
#[derive(Clone, Copy)]
pub(super) enum JsonModifyMode {
    Set,
    Insert,
    Replace,
}

/// `JSON_SET`, `JSON_INSERT`, and `JSON_REPLACE`, ports of
/// `builtinJSON{Set,Insert,Replace}Sig.evalJSON` and `BinaryJSON.Modify` in
/// `pkg/expression/builtin_json.go` / `pkg/types/json_binary_functions.go`.
/// Value strings remain JSON strings because the source disables
/// `ParseToJSONFlag4Expr` for every value argument.
pub(super) fn json_modify(
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
        let value = json_argument(&pair[1], StringArgument::Value, types[1].as_ref())?;
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
