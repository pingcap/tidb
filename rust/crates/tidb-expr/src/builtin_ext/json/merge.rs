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

use super::modify::{binary_json_error, json_document};
use crate::{Datum, EvalError};
use tidb_datatype::FieldType;

/// `JSON_MERGE` and `JSON_MERGE_PRESERVE`, ports of
/// `builtinJSONMergeSig.evalJSON` and `types.MergeBinaryJSON`: any SQL NULL
/// argument makes the result NULL. The deprecation warning Go appends for
/// the `JSON_MERGE` spelling is raised by the caller, which owns the
/// statement context.
pub(super) fn json_merge(
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
) -> Result<Datum, EvalError> {
    let mut values = Vec::with_capacity(vals.len());
    for (value, field_type) in vals.iter().zip(arg_types) {
        let Some(value) = json_document(value, field_type.as_ref())? else {
            return Ok(Datum::Null);
        };
        values.push(value);
    }
    tidb_datatype::merge_binary_json(&values)
        .map(Datum::Json)
        .map_err(binary_json_error)
}

/// `JSON_MERGE_PATCH`, port of `builtinJSONMergePatchSig.evalJSON` and
/// `types.MergePatchBinaryJSON`, where a SQL NULL argument is Go's `nil`.
pub(super) fn json_merge_patch(
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
) -> Result<Datum, EvalError> {
    let values = vals
        .iter()
        .zip(arg_types)
        .map(|(value, field_type)| json_document(value, field_type.as_ref()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(tidb_datatype::merge_patch_binary_json(&values)
        .map_err(binary_json_error)?
        .map_or(Datum::Null, Datum::Json))
}
