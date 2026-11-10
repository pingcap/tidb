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

//! Source-first completion of `pkg/expression/builtin_string_test.go::TestConvert`
//! Go `BuildToBinaryFunction` / `BuildFromBinaryFunction`
//! (`pkg/expression/builtin_convert_charset.go`), pinned against Go over
//! `mock.NewContext()`.

use crate::builtin::ColumnsBuildContext;
use crate::column::Column;
use crate::constant::Constant;
use crate::expression::Expression;
use tidb_datatype::{Datum, FieldType};
use crate::convert_charset::{build_from_binary_function, build_to_binary_function};
use tidb_charset::Collation;
use tidb_datatype::FieldTypeCode as C;
use tidb_proto::tipb::ScalarFuncSig;

const NOT_NULL: u64 = 1;
const BINARY: u64 = 128;

fn field_type(code: C, flen: i64, flags: u64, charset: &str, collation: &str) -> FieldType {
    let mut ft = FieldType::new(code);
    ft.set_flen(flen);
    ft.set_raw_flags(ft.raw_flags() | flags);
    ft.set_charset_name(charset);
    ft.set_collation_name(collation);
    ft
}

fn column(code: C, flen: i64, flags: u64, charset: &str, collation: &str) -> Expression {
    Expression::Column(Column::new(1, field_type(code, flen, flags, charset, collation)))
}

fn string_constant(bytes: &[u8], charset: &str, collation: Collation) -> Expression {
    let ft = field_type(C::VarString, bytes.len() as i64, 0, charset, collation.name());
    Expression::Constant(Constant::new(Datum::new_collation_string_datum(bytes.to_vec(), collation), ft))
}

/// name, signature, then the return type's code, flen, decimal, flags,
/// charset and collation, then the function's charset, collation,
/// coercibility and repertoire.
type Pin = (&'static str, ScalarFuncSig, C, i64, i64, u64, &'static str, &'static str, &'static str, &'static str, i32, i32);

#[track_caller]
fn assert_function(case: &str, built: &Expression, want: Pin) {
    let Expression::ScalarFunction(function) = built else {
        panic!("{case}: a function is built");
    };
    let rt = function.ret_type.as_ref().unwrap();
    let (charset, collation) = function.collation.charset_and_collation();
    let got = (
        function.func_name.lowercase(),
        function.pb_code(),
        rt.code(),
        rt.flen(),
        rt.decimal(),
        rt.raw_flags(),
        rt.charset_name(),
        rt.collation_name(),
        charset,
        collation,
        function.collation.coercibility().0,
        function.collation.repertoire().0,
    );
    assert_eq!(got, want, "{case}");
}

#[track_caller]
fn assert_constant(case: &str, built: &Expression, bytes: &[u8], want: (C, i64, i64, u64, &str, &str)) {
    let Expression::Constant(constant) = built else {
        panic!("{case}: the constant argument folds");
    };
    let rt = constant.ret_type.as_ref().unwrap();
    let value = match &constant.value {
        Datum::String(value) => value.bytes(),
        Datum::Bytes(value) => value.as_slice(),
        other => panic!("{case}: a string constant, got {other:?}"),
    };
    assert_eq!(value, bytes, "{case}");
    assert_eq!(
        (rt.code(), rt.flen(), rt.decimal(), rt.raw_flags(), rt.charset_name(), rt.collation_name()),
        want,
        "{case}"
    );
}

#[test]
fn build_to_binary_function_matches_go() {
    let ctx = ColumnsBuildContext(&crate::NoColumns);
    let to = |flen, flags| -> Pin {
        (
            "to_binary",
            ScalarFuncSig::ToBinary,
            C::VarString,
            flen,
            -1,
            flags,
            "binary",
            "binary",
            "utf8mb4",
            "utf8mb4_bin",
            4,
            3,
        )
    };
    assert_function(
        "gbk varchar",
        &build_to_binary_function(&ctx, column(C::Varchar, 20, 0, "gbk", "gbk_chinese_ci")),
        to(20, 0),
    );
    // The argument's own flags are kept; Go sets the charset and collation
    // and leaves the BINARY flag alone.
    assert_function(
        "gbk char not null",
        &build_to_binary_function(&ctx, column(C::String, 8, NOT_NULL, "gbk", "gbk_bin")),
        to(8, NOT_NULL),
    );
    assert_function(
        "utf8mb4 blob",
        &build_to_binary_function(&ctx, column(C::Blob, 65_535, 0, "utf8mb4", "utf8mb4_general_ci")),
        to(65_535, 0),
    );
    // `default: return nil, fmt.Errorf("unexpected argTp")` hands the
    // argument back.
    let int_column = column(C::LongLong, 20, BINARY, "binary", "binary");
    assert!(matches!(build_to_binary_function(&ctx, int_column), Expression::Column(_)));
    // `FoldConstant` runs on the new node: the GBK bytes of the two
    // characters, typed by the function (the argument's UTF-8 flen).
    assert_constant(
        "gbk constant",
        &build_to_binary_function(&ctx, string_constant("一二".as_bytes(), "gbk", Collation::GbkChineseCi)),
        b"\xd2\xbb\xb6\xfe",
        (C::VarString, 6, -1, NOT_NULL, "binary", "binary"),
    );
}

#[test]
fn build_from_binary_function_matches_go() {
    let ctx = ColumnsBuildContext(&crate::NoColumns);
    let from = |code, flen, charset, collation| -> Pin {
        (
            "from_binary",
            ScalarFuncSig::FromBinary,
            code,
            flen,
            -1,
            0,
            charset,
            collation,
            "utf8mb4",
            "utf8mb4_bin",
            4,
            3,
        )
    };
    assert_function(
        "binary varchar",
        &build_from_binary_function(
            &ctx,
            column(C::Varchar, 20, BINARY, "binary", "binary"),
            field_type(C::Varchar, 20, 0, "utf8mb4", "utf8mb4_general_ci"),
            false,
        ),
        from(C::Varchar, 20, "utf8mb4", "utf8mb4_general_ci"),
    );
    assert_function(
        "binary blob into gbk",
        &build_from_binary_function(
            &ctx,
            column(C::Blob, 65_535, BINARY, "binary", "binary"),
            field_type(C::Blob, 65_535, 0, "gbk", "gbk_chinese_ci"),
            true,
        ),
        from(C::Blob, 65_535, "gbk", "gbk_chinese_ci"),
    );
    let int_column = column(C::LongLong, 20, BINARY, "binary", "binary");
    assert!(matches!(
        build_from_binary_function(
            &ctx,
            int_column,
            field_type(C::Varchar, 20, 0, "utf8mb4", "utf8mb4_bin"),
            false
        ),
        Expression::Column(_)
    ));
    assert_constant(
        "binary constant",
        &build_from_binary_function(
            &ctx,
            string_constant(b"ab", "binary", Collation::Binary),
            field_type(C::VarString, 2, 0, "utf8mb4", "utf8mb4_general_ci"),
            false,
        ),
        b"ab",
        (C::VarString, 2, -1, NOT_NULL, "utf8mb4", "utf8mb4_general_ci"),
    );
    // Bytes the target charset cannot decode are an error, so the fold
    // leaves the function in place.
    assert_function(
        "undecodable constant",
        &build_from_binary_function(
            &ctx,
            string_constant(b"\xff\xfe", "binary", Collation::Binary),
            field_type(C::VarString, 2, 0, "utf8mb4", "utf8mb4_general_ci"),
            false,
        ),
        from(C::VarString, 2, "utf8mb4", "utf8mb4_general_ci"),
    );
}

/// Go `newFunctionImpl`: `case InternalFuncFromBinary` and `case
/// InternalFuncToBinary` hand the argument to the dedicated builders, the
/// caller's return type reaching `from_binary` alone.
#[test]
fn new_function_routes_the_binary_wrappers_to_their_builders() {
    let built = crate::new_function::new_function(
        &crate::NoColumns,
        "to_binary",
        FieldType::new(C::LongLong),
        vec![column(C::Varchar, 20, 0, "gbk", "gbk_chinese_ci")],
    )
    .unwrap();
    assert_function(
        "to_binary",
        &built,
        ("to_binary", ScalarFuncSig::ToBinary, C::VarString, 20, -1, 0, "binary", "binary", "utf8mb4", "utf8mb4_bin", 4, 3),
    );
    let built = crate::new_function::new_function(
        &crate::NoColumns,
        "from_binary",
        field_type(C::Varchar, 20, 0, "gbk", "gbk_bin"),
        vec![column(C::Varchar, 20, BINARY, "binary", "binary")],
    )
    .unwrap();
    assert_function(
        "from_binary",
        &built,
        ("from_binary", ScalarFuncSig::FromBinary, C::Varchar, 20, -1, 0, "gbk", "gbk_bin", "utf8mb4", "utf8mb4_bin", 4, 3),
    );
}
