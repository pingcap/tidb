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

use std::borrow::Cow;

use super::modify::{binary_json_error, json_document, parse_json_path};
use crate::coerce::coerce_str;
use crate::{Datum, EvalError, JsonError};
use tidb_datatype::{BinaryJSON, BinaryJSONError, EncodedJSONSelection, EvalType, FieldType, JSONPathExpression};

/// The document argument, or `None` for SQL NULL.
fn document<'a>(
    vals: &'a [Datum],
    arg_types: &[Option<FieldType>],
) -> Result<Option<Cow<'a, BinaryJSON>>, EvalError> {
    document_value(&vals[0], arg_types.first().and_then(Option::as_ref))
}

fn document_value<'a>(
    value: &'a Datum,
    field_type: Option<&FieldType>,
) -> Result<Option<Cow<'a, BinaryJSON>>, EvalError> {
    // A JSON-to-JSON cast leaves the payload unchanged. Read it directly
    // so a checked layout remains cached on the source datum.
    match value {
        Datum::Json(json) => Ok(Some(Cow::Borrowed(json))),
        _ => json_document(value, field_type).map(|document| document.map(Cow::Owned)),
    }
}

fn document_path(value: &Datum) -> Result<Option<JSONPathExpression>, EvalError> {
    let bytes = match value {
        Datum::String(path) => Some(path.bytes()),
        Datum::Bytes(path) => Some(path.as_slice()),
        _ => None,
    };
    if let Some(bytes) = bytes {
        return parse_json_path(&String::from_utf8_lossy(bytes)).map(Some);
    }
    coerce_str(value)?.map(|path| parse_json_path(&path)).transpose()
}

fn exact_document_path(value: &Datum) -> Result<Option<JSONPathExpression>, EvalError> {
    let Some(path) = document_path(value)? else {
        return Ok(None);
    };
    if path.could_match_multiple_values() {
        return Err(EvalError::Json(JsonError::InvalidPathMultipleSelection));
    }
    Ok(Some(path))
}

pub(crate) fn literal_root_path(path: &crate::expression::Expression) -> bool {
    literal_string(path) == Some(Some(b"$"))
}

/// A strict byte-string literal, with SQL NULL distinguished from a dynamic argument.
#[inline]
pub(crate) fn literal_string(arg: &crate::expression::Expression) -> Option<Option<&[u8]>> {
    let crate::expression::Expression::Constant(constant) = arg else {
        return None;
    };
    match constant.literal_value()? {
        Datum::String(value) => Some(Some(value.bytes())),
        Datum::Bytes(value) => Some(Some(value)),
        Datum::Null => Some(None),
        _ => None,
    }
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
        let Some(path) = document_path(value)? else {
            return Ok(Datum::Null);
        };
        paths.push(path);
    }
    Ok(document
        .extract_for_sql(&paths)
        .map_err(binary_json_error)?
        .map_or(Datum::Null, Datum::Json))
}

pub(crate) fn json_extract_row(
    args: &[crate::expression::Expression],
    ctx: &dyn crate::Columns,
    row: tidb_chunk::row::Row<'_>,
    cache: &crate::builtin_ext::BuiltinFuncCache<Option<Vec<JSONPathExpression>>>,
) -> Result<Option<BinaryJSON>, EvalError> {
    use crate::expression::Expression;

    let [arg, paths @ ..] = args else {
        return Err(EvalError::Unsupported("missing JSON_EXTRACT document"));
    };
    let value;
    let source = if let Some(cell) = packed_document(arg, row) {
        DocumentSource::Packed(cell)
    } else {
        value = arg.eval(ctx, row)?;
        let Some(document) = document_value(&value, arg.static_type())? else {
            return Ok(None);
        };
        DocumentSource::Document(document)
    };
    if let [path] = paths {
        if literal_root_path(path) {
            return match source {
                DocumentSource::Document(document) => Ok(Some(document.into_owned())),
                DocumentSource::Packed(cell) => {
                    let (&type_code, value) = cell.split_first().ok_or_else(|| binary_json_error(BinaryJSONError::InvalidBinary))?;
                    Ok(Some(extracted_cell(
                        row, arg, cell, EncodedJSONSelection::Borrowed { type_code, value, root: true },
                    )))
                }
            };
        }
    }
    let parse = || {
        let mut parsed = Vec::with_capacity(paths.len());
        for path in paths {
            let literal = match path {
                Expression::Constant(constant) => constant.literal_value()
                    .filter(|value| matches!(value, Datum::Null | Datum::String(_) | Datum::Bytes(_))),
                _ => None,
            };
            let parsed_path = if let Some(value) = literal {
                document_path(value)?
            } else {
                document_path(&path.eval(ctx, row)?)?
            };
            let Some(path) = parsed_path else {
                return Ok(None);
            };
            parsed.push(path);
        }
        Ok::<_, EvalError>(Some(parsed))
    };
    let cached;
    let parsed;
    let paths = if paths.iter().all(|path| {
        matches!(path, Expression::Constant(constant) if constant.literal_value().is_some())
    }) {
        cached = cache.get_or_init_cache(ctx.context_id(), parse)?;
        cached.as_deref()
    } else {
        parsed = parse()?;
        parsed.as_deref()
    };
    let Some(paths) = paths else {
        return Ok(None);
    };
    match source {
        DocumentSource::Document(document) => document.extract_for_sql(paths)
            .map_err(binary_json_error),
        DocumentSource::Packed(cell) => {
            let selected = BinaryJSON::extract_encoded_for_sql(cell, paths).map_err(binary_json_error)?;
            Ok(selected.map(|selected| extracted_cell(row, arg, cell, selected)))
        }
    }
}

fn extracted_cell(
    row: tidb_chunk::row::Row<'_>,
    arg: &crate::expression::Expression,
    cell: &[u8],
    selected: EncodedJSONSelection<'_>,
) -> BinaryJSON {
    let (type_code, value) = match selected {
        EncodedJSONSelection::Borrowed { type_code, value, .. } => (type_code, value),
        EncodedJSONSelection::Owned(value) => return value,
    };
    if value.len() >= tidb_chunk::column::Column::MIN_RETAINED_CELL_BYTES {
        if let crate::expression::Expression::Column(column) = arg {
            let index = column.index as usize;
            let start = value.as_ptr().addr() - cell.as_ptr().addr();
            if let Some((bytes, layout)) = row.try_retain_json_slice(index, start..start + value.len()) {
                return BinaryJSON::from_shared_parts(type_code, bytes)
                    .with_known_layout(selected.layout(layout));
            }
        }
    }
    BinaryJSON::from_encoded_parts(type_code, value)
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
        .map(|name| Datum::new_string_datum(name.to_owned()))
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
    let path = match vals.get(1) {
        Some(path) => match exact_document_path(path)? {
            Some(path) => Some(path),
            None => return Ok(Datum::Null),
        },
        None => None,
    };
    document.get_length(path.as_ref())
        .map(|count| count.map_or(Datum::Null, |count| Datum::Int(count as i64)))
        .map_err(binary_json_error)
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
    let path = match vals.get(1) {
        Some(path) => match exact_document_path(path)? {
            Some(path) => Some(path),
            None => return Ok(Datum::Null),
        },
        None => None,
    };
    document.get_keys(path.as_ref())
        .map(|keys| keys.map_or(Datum::Null, Datum::Json))
        .map_err(binary_json_error)
}

enum DocumentSource<'a> {
    Packed(&'a [u8]),
    Document(Cow<'a, BinaryJSON>),
}

pub(crate) fn json_contains_path_row(
    args: &[crate::expression::Expression],
    ctx: &dyn crate::Columns,
    row: tidb_chunk::row::Row<'_>,
) -> Result<Option<bool>, EvalError> {
    use super::predicate::{contains_path_mode, contains_path_mode_value};
    use crate::scalar_function::borrow_string_argument;

    let [arg, mode, paths @ ..] = args else {
        return Err(EvalError::Unsupported("missing JSON_CONTAINS_PATH argument"));
    };
    let value;
    let source = if let Some(cell) = packed_document(arg, row) {
        DocumentSource::Packed(cell)
    } else {
        value = arg.eval(ctx, row)?;
        let Some(document) = document_value(&value, arg.static_type())? else {
            return Ok(None);
        };
        DocumentSource::Document(document)
    };
    let string = |arg| {
        literal_string(arg).or_else(|| borrow_string_argument(arg, row).map(|(bytes, _)| Some(bytes)))
    };
    let one = match string(mode) {
        Some(Some(mode)) => Some(contains_path_mode(mode)?),
        Some(None) => None,
        None => contains_path_mode_value(&mode.eval(ctx, row)?)?,
    };
    let Some(one) = one else {
        return Ok(None);
    };
    let mut contains = true;
    for path in paths {
        let path = match string(path) {
            Some(Some(b"$")) => {
                contains = true;
                if one {
                    return Ok(Some(true));
                }
                continue;
            }
            Some(Some(path)) => Some(parse_json_path(&String::from_utf8_lossy(path))?),
            Some(None) => None,
            None => document_path(&path.eval(ctx, row)?)?,
        };
        let Some(path) = path else {
            return Ok(None);
        };
        contains = match &source {
            DocumentSource::Packed(cell) => BinaryJSON::encoded_path_exists_for_sql(cell, &path),
            DocumentSource::Document(document) => document.path_exists_for_sql(&path),
        }.map_err(binary_json_error)?;
        if contains == one {
            break;
        }
    }
    Ok(Some(contains))
}

pub(crate) fn json_keys_row(
    args: &[crate::expression::Expression],
    ctx: &dyn crate::Columns,
    row: tidb_chunk::row::Row<'_>,
    cache: &crate::builtin_ext::BuiltinFuncCache<Option<Vec<JSONPathExpression>>>,
) -> Result<Datum, EvalError> {
    json_metadata_row(args, ctx, row, cache, |source, path| {
        match source {
            DocumentSource::Packed(cell) => BinaryJSON::get_encoded_keys(cell, path),
            DocumentSource::Document(document) => document.get_keys(path),
        }
    })
    .map(|keys| keys.map_or(Datum::Null, Datum::Json))
}

#[inline]
pub(crate) fn json_length_row(
    args: &[crate::expression::Expression],
    ctx: &dyn crate::Columns,
    row: tidb_chunk::row::Row<'_>,
    cache: &crate::builtin_ext::BuiltinFuncCache<Option<Vec<JSONPathExpression>>>,
) -> Result<Option<usize>, EvalError> {
    if let [arg] = args {
        if let Some(cell) = packed_document(arg, row) {
            return BinaryJSON::encoded_root_count(cell).map(Some).map_err(binary_json_error);
        }
    }
    json_metadata_row(args, ctx, row, cache, |source, path| {
        match source {
            DocumentSource::Packed(cell) => BinaryJSON::get_encoded_length(cell, path),
            DocumentSource::Document(document) => document.get_length(path),
        }
    })
}

#[inline]
fn packed_document<'row>(
    arg: &crate::expression::Expression,
    row: tidb_chunk::row::Row<'row>,
) -> Option<&'row [u8]> {
    let crate::expression::Expression::Column(column) = arg else {
        return None;
    };
    if column.get_static_type()?.eval_type() != EvalType::Json {
        return None;
    }
    row.try_borrow_var_bytes(usize::try_from(column.index).ok()?)
}

/// Borrows a typed JSON cell across evaluation of the optional exact path.
fn json_metadata_row<T>(
    args: &[crate::expression::Expression],
    ctx: &dyn crate::Columns,
    row: tidb_chunk::row::Row<'_>,
    cache: &crate::builtin_ext::BuiltinFuncCache<Option<Vec<JSONPathExpression>>>,
    read: impl FnOnce(DocumentSource<'_>, Option<&JSONPathExpression>) -> Result<Option<T>, BinaryJSONError>,
) -> Result<Option<T>, EvalError> {
    use crate::expression::Expression;

    let [arg, paths @ ..] = args else {
        return Err(EvalError::Unsupported("missing JSON document"));
    };
    let value;
    let source = if let Some(bytes) = packed_document(arg, row) {
        DocumentSource::Packed(bytes)
    } else {
        value = arg.eval(ctx, row)?;
        let Some(document) = document_value(&value, arg.static_type())? else {
            return Ok(None);
        };
        DocumentSource::Document(document)
    };
    let Some(path) = paths.first() else {
        return read(source, None).map_err(binary_json_error);
    };
    if matches!(path, Expression::Constant(constant) if constant.literal_value().is_some()) {
        let paths = cache.get_or_init_cache(ctx.context_id(), || {
            exact_document_path(&path.eval(ctx, row)?).map(|path| path.map(|path| vec![path]))
        })?;
        let Some(path) = paths.as_ref().as_ref().and_then(|paths| paths.first()) else {
            return Ok(None);
        };
        return read(source, Some(path)).map_err(binary_json_error);
    }
    let Some(path) = exact_document_path(&path.eval(ctx, row)?)? else {
        return Ok(None);
    };
    read(source, Some(&path)).map_err(binary_json_error)
}

/// `JSON_DEPTH(json_doc)`, port of `builtinJSONDepthSig.evalInt`
/// (`BinaryJSON.GetElemDepth`: a scalar or empty container is 1).
pub(crate) fn json_depth(
    value: &Datum,
    field_type: Option<&FieldType>,
) -> Result<Datum, EvalError> {
    let Some(document) = document_value(value, field_type)? else {
        return Ok(Datum::Null);
    };
    let depth = document.get_elem_depth().map_err(binary_json_error)?;
    Ok(Datum::Int(i64::try_from(depth).unwrap_or(i64::MAX)))
}

/// `JSON_STORAGE_SIZE(json_doc)`, port of `builtinJSONStorageSizeSig.evalInt`:
/// the binary value's length plus its type code.
pub(crate) fn json_storage_size(
    value: &Datum,
    field_type: Option<&FieldType>,
) -> Result<Datum, EvalError> {
    let Some(document) = document_value(value, field_type)? else {
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
    Ok(match document_value(value, field_type)? {
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
    Ok(Datum::new_string_datum(tidb_datatype::json_scanner::indent(
        &document.to_string(),
        "  ",
    )))
}

#[cfg(test)]
mod tests {
    use crate::Datum;

    /// Go's builtin test door over a context-free session.
    fn eval_builtin_func(name: &str, vals: &[Datum]) -> Option<Result<Datum, crate::EvalError>> {
        crate::tests::eval_builtin_func(name, vals, &crate::NoColumns)
    }

    fn document(text: &str) -> Datum {
        Datum::new_string_datum(text.to_string())
    }

    #[test]
    fn json_depth_matches_all_go_shape_vectors() {
        // TestJSONDepth in pkg/expression/builtin_json_test.go.
        let cases = [
            ("null", 1),
            ("true", 1),
            ("false", 1),
            ("1", 1),
            ("-1", 1),
            ("1.1", 1),
            (r#""1""#, 1),
            ("{}", 1),
            ("[]", 1),
            ("[10, 20]", 2),
            ("[[], {}]", 2),
            (r#"{"Name": "Homer"}"#, 2),
            (r#"[10, {"a": 20}]"#, 3),
            (
                r#"{"Person": {"Name": "Homer", "Age": 39, "Hobbies": ["Eating", "Sleeping"]}}"#,
                4,
            ),
            (r#"{"a":1}"#, 2),
            (r#"{"a":[1]}"#, 3),
            (r#"{"b":2, "c":3}"#, 2),
            ("[1]", 2),
            ("[1,2]", 2),
            ("[1,2,[1,3]]", 3),
            (r#"[1,2,[1,[5,[3]]]]"#, 5),
            (r#"[1,2,[1,[5,{"a":[2,3]}]]]"#, 6),
            (r#"[{"a":1}]"#, 3),
            (r#"[{"a":1,"b":2}]"#, 3),
            (r#"[{"a":{"a":1},"b":2}]"#, 4),
        ];
        for (input, want) in cases {
            assert_eq!(
                eval_builtin_func("JSON_DEPTH", &[document(input)])
                    .expect("JSON_DEPTH must dispatch")
                    .expect("valid JSON must evaluate"),
                Datum::Int(want),
                "JSON_DEPTH({input})"
            );
        }
        assert_eq!(
            eval_builtin_func("JSON_DEPTH", &[Datum::Null])
                .expect("JSON_DEPTH must dispatch")
                .expect("NULL must evaluate"),
            Datum::Null
        );
        assert!(eval_builtin_func("JSON_DEPTH", &[document("a")])
            .expect("JSON_DEPTH must dispatch")
            .is_err());
        // go's implicit scalar→document cast: `JSON_DEPTH(1)` is 1 on the
        // oracle (the numeric scalar coerces to the JSON number 1), not an
        // error.
        assert_eq!(
            eval_builtin_func("JSON_DEPTH", &[Datum::Int(1)])
                .expect("JSON_DEPTH must dispatch")
                .expect("scalar depth evaluates"),
            Datum::Int(1)
        );
    }

    #[test]
    fn json_depth_uses_the_scalar_type_and_checks_container_framing() {
        use tidb_datatype::{BinaryJSON, JSON_TYPE_CODE_STRING};

        let value = BinaryJSON::from_encoded_parts(JSON_TYPE_CODE_STRING, [1, 0xff]);
        assert!(value.element_depth().is_err());
        assert_eq!(
            eval_builtin_func("JSON_DEPTH", &[Datum::Json(value)]).unwrap().unwrap(),
            Datum::Int(1),
        );

        let truncated = BinaryJSON::from_encoded_parts(JSON_TYPE_CODE_STRING, [3, b'a']);
        assert!(truncated.element_depth().is_err());
        assert_eq!(
            eval_builtin_func("JSON_DEPTH", &[Datum::Json(truncated)]).unwrap().unwrap(),
            Datum::Int(1),
        );

        let array = BinaryJSON::parse(r#"["a"]"#).unwrap();
        let mut bytes = array.value().to_vec();
        let length = bytes.len();
        bytes[length - 2] = 3;
        let truncated_child = BinaryJSON::from_encoded_parts(array.type_code(), bytes);
        assert!(eval_builtin_func("JSON_DEPTH", &[Datum::Json(truncated_child)]).unwrap().is_err());
    }

    #[test]
    fn json_storage_free_matches_go_vectors() {
        // TestJSONStorageFree in pkg/expression/builtin_json_test.go.
        for input in [
            "null",
            "true",
            "1",
            r#""1""#,
            "{}",
            r#"{"a":1}"#,
            r#"[{"a":{"a":1},"b":2}]"#,
            r#"{"a": 1000, "b": "wxyz", "c": "[1, 3, 5, 7]"}"#,
        ] {
            assert_eq!(
                eval_builtin_func("JSON_STORAGE_FREE", &[document(input)])
                    .expect("JSON_STORAGE_FREE must dispatch")
                    .expect("valid JSON must evaluate"),
                Datum::Int(0),
                "JSON_STORAGE_FREE({input})"
            );
        }
        assert_eq!(
            eval_builtin_func("JSON_STORAGE_FREE", &[Datum::Null])
                .expect("JSON_STORAGE_FREE must dispatch")
                .expect("NULL must evaluate"),
            Datum::Null
        );
        for input in [r#"[{"a":1]"#, r#"[{a":1]"#] {
            assert!(eval_builtin_func("JSON_STORAGE_FREE", &[document(input)])
                .expect("JSON_STORAGE_FREE must dispatch")
                .is_err());
        }
    }

    #[test]
    fn json_storage_size_matches_go_vectors() {
        // TestJSONStorageSize in pkg/expression/builtin_json_test.go.
        for (input, want) in [
            ("null", 2),
            ("true", 2),
            ("1", 9),
            (r#""1""#, 3),
            ("{}", 9),
            (r#"{"a":1}"#, 29),
            (r#"[{"a":{"a":1},"b":2}]"#, 82),
            (r#"{"a": 1000, "b": "wxyz", "c": "[1, 3, 5, 7]"}"#, 71),
        ] {
            assert_eq!(
                eval_builtin_func("JSON_STORAGE_SIZE", &[document(input)])
                    .expect("JSON_STORAGE_SIZE must dispatch")
                    .expect("valid JSON must evaluate"),
                Datum::Int(want),
                "JSON_STORAGE_SIZE({input})"
            );
        }
        assert_eq!(
            eval_builtin_func("JSON_STORAGE_SIZE", &[Datum::Null])
                .expect("JSON_STORAGE_SIZE must dispatch")
                .expect("NULL must evaluate"),
            Datum::Null
        );
        for input in [r#"[{"a":1]"#, r#"[{a":1]"#] {
            assert!(eval_builtin_func("JSON_STORAGE_SIZE", &[document(input)])
                .expect("JSON_STORAGE_SIZE must dispatch")
                .is_err());
        }
        assert!(eval_builtin_func("JSON_STORAGE_SIZE", &[]).is_none());
    }
}
