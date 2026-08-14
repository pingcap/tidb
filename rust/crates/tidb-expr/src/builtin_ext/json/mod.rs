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
//! `serde_json` deliberately parses and validates only. TiDB's result text is
//! [`types.BinaryJSON.String`](../../../pkg/types/json_binary.go)'s text form,
//! not serde's compact form: objects are key-sorted and containers use `, ` /
//! `: ` separators. [`text::format_json`] is that output boundary.
//!
//! `JSON_ARRAY` and `JSON_OBJECT` dispatch their representable scalar datum
//! rows. The frozen evaluator has no typed boolean or BinaryJSON variant, so
//! parser-originated `TRUE/FALSE` and already-typed JSON arguments remain
//! explicit partial boundaries rather than being guessed from `Datum::Int` or
//! a text string.
//!
//! The family is split by what a function DOES to a document, each file
//! naming the Go file it was read from:
//!
//! - [`value`] -- turning a SQL argument into a JSON value, and `CAST AS
//!   JSON`. The `ParseToJSONFlag` distinction every other file depends on.
//! - [`path`] -- the `$`-path grammar and `JSON_EXTRACT`'s selection walk.
//! - [`text`] -- rendering a JSON value back to text, and `JSON_PRETTY`.
//! - [`construct`] -- `JSON_ARRAY`/`OBJECT`/`QUOTE`/`UNQUOTE`.
//! - [`report`] -- `JSON_VALID`/`TYPE`/`LENGTH`/`KEYS`/`SUM_CRC32`.
//! - [`predicate`] -- `JSON_CONTAINS`/`CONTAINS_PATH`/`OVERLAPS`/`MEMBER OF`
//!   and the value equality they share.
//! - [`modify`] -- `JSON_SET`/`INSERT`/`REPLACE`/`REMOVE`/`ARRAY_APPEND`/
//!   `ARRAY_INSERT`.
//! - [`merge`] -- `JSON_MERGE`/`MERGE_PRESERVE`/`MERGE_PATCH`.
//! - [`search`] -- `JSON_SEARCH`, the one walk that BUILDS paths.

mod construct;
mod merge;
mod modify;
mod path;
mod predicate;
mod report;
mod search;
mod text;
mod value;

use construct::{json_array, json_object, json_quote, json_unquote};
use merge::{json_merge, json_merge_patch};
use modify::{json_array_append, json_array_insert, json_modify, json_remove, JsonModifyMode};
use path::json_extract;
use predicate::{json_contains, json_contains_path, json_member_of, json_overlaps};
use report::{json_keys, json_length, json_schema_valid, json_sum_crc32, json_type, json_valid};
use search::json_search;
use text::json_pretty;

use crate::{Datum, EvalError};
use tidb_datatype::FieldType;

pub(crate) use report::JsonSchemaCache;
pub(crate) use value::{
    cast_as_json, cast_as_json_typed, cast_as_json_value_typed, parse_json_document_argument,
};

/// Dispatches the JSON family.  The match and arities are ports of the
/// function classes in `pkg/expression/builtin_json.go`:
/// `builtinJSON{Type,Extract,Unquote,Quote,Array,Object,Length,Valid,
/// ArrayAppend,ArrayInsert,SUMCRC32}Sig`.
pub(crate) fn dispatch(name: &str, vals: &[Datum]) -> Option<Result<Datum, EvalError>> {
    match (name, vals.len()) {
        ("JSON_VALID", 1) => Some(json_valid(&vals[0])),
        ("JSON_SCHEMA_VALID", 2) => Some(json_schema_valid(vals)),
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

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
