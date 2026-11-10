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

//! Thin builtin-family routing plus the non-family control functions and the
//! `[NOT] IN (list)` predicate. Called from `crate::eval_in`.


use crate::{Columns, Datum, EvalError};

/// Go `builtinBenchmarkSig.evalInt`'s first-argument `EvalInt` boundary.
pub(crate) fn benchmark_loop_count(
    value: Datum,
    source: Option<&tidb_datatype::FieldType>,
    ctx: &dyn Columns,
) -> Result<Option<i64>, EvalError> {
    match crate::cast::cast_arg_as_int(&value, source, ctx)? {
        Datum::Null => Ok(None),
        Datum::Int(value) => Ok(Some(value)),
        Datum::UInt(value) => Ok(Some(value as i64)),
        _ => Err(EvalError::Unsupported("BENCHMARK loop count")),
    }
}

/// Go's `builtinBenchmarkSig` switch intentionally has no vector arm.
pub(crate) fn ensure_benchmark_eval_type(
    source: Option<&tidb_datatype::FieldType>,
) -> Result<(), EvalError> {
    if source
        .is_some_and(|field_type| field_type.eval_type() == tidb_datatype::EvalType::VectorFloat32)
    {
        return Err(EvalError::Unsupported(
            "VectorFloat32 is not supported for BENCHMARK()",
        ));
    }
    Ok(())
}

/// The name path a sequence builtin's first argument carries on the CHUNK path,
/// where the rewriter replaced the parser's column reference with one string
/// constant (see `rewriter::rewrite_expr_resolved`).
///
/// The segments are joined by NUL rather than `.` so the split back is exact:
/// a backquoted identifier may contain a dot, but no identifier can contain a
/// NUL byte. That keeps the chunk path handing `Columns::sequence_nextval` the
/// SAME `&[String]` the row path hands it straight from the parser.
pub(crate) const SEQUENCE_PATH_SEPARATOR: char = '\0';

pub(crate) fn sequence_path(value: &Datum) -> Result<Vec<String>, EvalError> {
    match value {
        Datum::Bytes(bytes) => Ok(String::from_utf8(bytes.clone())
            .map_err(|_| EvalError::Unsupported("a sequence name must be text"))?
            .split(SEQUENCE_PATH_SEPARATOR)
            .map(str::to_owned)
            .collect()),
        _ => Err(EvalError::Unsupported(
            "a sequence builtin's first argument must be a name",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::RefCell;

    

    #[derive(Default)]
    struct PacketLimit {
        warnings: RefCell<Vec<(u16, String)>>,
    }

    impl Columns for PacketLimit {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn max_allowed_packet(&self) -> u64 {
            3
        }

        fn append_warning(&self, code: u16, message: &str) {
            self.warnings.borrow_mut().push((code, message.to_owned()));
        }
    }

    /// Go `TestInsertBinarySig`: all seven source rows, including the one
    /// result that crosses the signature's three-byte packet limit.
    #[test]
    fn test_insert_binary_sig() {
        let ctx = PacketLimit::default();
        let binary = |value: &str| Datum::new_bytes_datum(value.as_bytes().to_vec());
        let rows = [
            (
                [binary("abc"), Datum::Int(3), Datum::Int(-1), binary("d")],
                binary("abd"),
            ),
            (
                [binary("abc"), Datum::Int(3), Datum::Int(-1), binary("de")],
                Datum::Null,
            ),
            (
                [binary("abc"), Datum::Int(0), Datum::Int(-1), binary("d")],
                binary("abc"),
            ),
            (
                [Datum::Null, Datum::Int(3), Datum::Int(-1), binary("d")],
                Datum::Null,
            ),
            (
                [binary("abc"), Datum::Null, Datum::Int(-1), binary("d")],
                Datum::Null,
            ),
            (
                [binary("abc"), Datum::Int(3), Datum::Null, binary("d")],
                Datum::Null,
            ),
            (
                [binary("abc"), Datum::Int(3), Datum::Int(-1), Datum::Null],
                Datum::Null,
            ),
        ];

        for (args, expected) in rows {
            assert_eq!(
                crate::tests::eval_builtin_func("INSERT_FUNC", &args, &ctx)
                    .expect("INSERT must be dispatched")
                    .expect("source rows must evaluate"),
                expected
            );
        }

        assert_eq!(
            *ctx.warnings.borrow(),
            vec![(
                1301,
                "Result of insert() was larger than max_allowed_packet (3) - truncated".to_owned(),
            )]
        );
    }
}

