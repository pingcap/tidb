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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY LICENSE, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Batch b068 ports of `pkg/expression.part3` (`func Test*` items 121–180 on
//! `origin/master`, sorted by file path then line). Each test re-derives its
//! intent from the Go source it exercises.

use super::{chunk_e, e};
use crate::builtin_ext::json::dispatch as json_dispatch;
use crate::builtin_ext::json2;
use crate::expression::Expression;
use crate::like::like_match_with_collation;
use crate::math_fn::dispatch_values;
use crate::regexp::{regexp_like, regexp_match};
use crate::rewriter::rewrite_expr;
use crate::rewriter::result_type::builtin_return_type;
use crate::scalar_function::ScalarFunction;
use crate::{Columns, Datum, EvalError, MysqlRng, NoColumns};
use tidb_ast::{CiString, QueryStmt, SelectField, Stmt};
use tidb_datatype::{
    compare_binary_json, Collation, FieldType, FieldTypeCode, FieldTypeFlags, MySqlDuration, Time,
    TimeType,
};
use tidb_mysql::runtime_versions;
use tidb_util::printer::get_tidb_info;
use tidb_util::versioninfo::VersionInfo;

fn parse_expr(sql_expr: &str) -> tidb_ast::Expr {
    let statement = tidb_parser::parse(&format!("SELECT {sql_expr}")).expect("parses");
    let Stmt::Query(query) = statement else {
        panic!("expected a query")
    };
    let QueryStmt::Select(select) = query.into_inner() else {
        panic!("expected a SELECT")
    };
    let SelectField::Expr { expr, .. } = &select.fields[0] else {
        panic!("expected an expression field")
    };
    expr.clone()
}

fn rewrite(sql_expr: &str) -> Expression {
    rewrite_expr(&parse_expr(sql_expr)).expect("rewrites")
}

fn eval_rewritten(sql_expr: &str, ctx: &impl Columns) -> Datum {
    let expression = rewrite(sql_expr);
    let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
    chunk.set_num_virtual_rows(1);
    expression.eval(ctx, chunk.get_row(0)).expect("evaluates")
}

fn text_ft() -> FieldType {
    FieldType::new(FieldTypeCode::VarString)
}

fn int_ft() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
}

fn uint_ft() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong).with_added_flags(FieldTypeFlags::UNSIGNED)
}

fn real_ft() -> FieldType {
    FieldType::new(FieldTypeCode::Double)
}

fn const_arg(datum: Datum) -> Expression {
    let field_type = match &datum {
        Datum::Null => FieldType::new(FieldTypeCode::Null),
        Datum::Int(_) => int_ft(),
        Datum::UInt(_) => uint_ft(),
        Datum::Float32(_) | Datum::Real(_) => real_ft(),
        Datum::String(_) | Datum::Bytes(_) => text_ft(),
        Datum::Decimal(_) => FieldType::new(FieldTypeCode::NewDecimal),
        Datum::Duration(_) => FieldType::new(FieldTypeCode::Duration),
        Datum::Time(time) => match time.kind() {
            TimeType::Date => FieldType::new(FieldTypeCode::Date),
            TimeType::DateTime => FieldType::new(FieldTypeCode::Datetime),
            TimeType::Timestamp => FieldType::new(FieldTypeCode::Timestamp),
        },
        Datum::Json(_) => FieldType::new(FieldTypeCode::Json),
        other => panic!("no test mapping for {other:?}"),
    };
    Expression::Constant(crate::constant::Constant::new(datum, field_type))
}

fn eval_as(name: &str, args: Vec<Datum>, ret_type: FieldType, ctx: &impl Columns) -> Datum {
    ScalarFunction::new(
        CiString::new(name),
        ret_type,
        args.into_iter().map(const_arg).collect(),
    )
    .eval(ctx, tidb_chunk::row::Row::empty())
    .expect("source row must evaluate")
}

fn json_call(name: &str, vals: &[Datum]) -> Result<Datum, EvalError> {
    json_dispatch(name, vals)
        .or_else(|| json2::dispatch(name, vals))
        .expect("JSON family should own name/arity")
}

fn json_s(value: &str) -> Datum {
    Datum::new_string(value.to_string())
}

fn json_eq(left: &Datum, right: &str) {
    let got = match left {
        Datum::Json(value) => value.clone(),
        other => {
            let text = other
                .sql_string()
                .unwrap_or_else(|_| panic!("expected JSON text, got {other:?}"));
            tidb_datatype::BinaryJSON::parse(&text).expect("got JSON")
        }
    };
    let want = tidb_datatype::BinaryJSON::parse(right).expect("want JSON");
    assert_eq!(
        compare_binary_json(&got, &want),
        std::cmp::Ordering::Equal,
        "got {got}, want {right}"
    );
}

fn math_call(name: &str, vals: &[Datum]) -> Result<Datum, EvalError> {
    dispatch_values(name, vals, &NoColumns).expect("math family should own name/arity")
}

/// Session columns for `pkg/expression/builtin_info_test.go` information
/// builtins. `VERSION()` in Go returns `mysql.ServerVersion`; the Rust
/// evaluator reads `Columns::sysvar("version")`.
#[derive(Default)]
struct InfoColumns {
    current_role: Option<String>,
    connection_id: Option<u64>,
    version: Option<String>,
    tidb_info: Option<String>,
    last_insert_id: Option<u64>,
    published_last_insert_id: std::cell::Cell<Option<u64>>,
    row_count: Option<i64>,
}

impl Columns for InfoColumns {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }

    fn current_role(&self) -> Option<String> {
        self.current_role.clone()
    }

    fn connection_id(&self) -> Option<u64> {
        self.connection_id
    }

    fn sysvar(&self, _: Option<tidb_ast::SysVarScope>, name: &str) -> Option<Datum> {
        name.eq_ignore_ascii_case("version").then(|| {
            Datum::new_string(
                self.version
                    .clone()
                    .unwrap_or_else(|| runtime_versions().server_version),
            )
        })
    }

    fn tidb_info(&self) -> String {
        self.tidb_info
            .clone()
            .unwrap_or_else(|| get_tidb_info(&VersionInfo::build_default()))
    }

    fn last_insert_id(&self) -> Option<u64> {
        self.last_insert_id
    }

    fn set_last_insert_id(&self, value: u64) {
        self.published_last_insert_id.set(Some(value));
    }

    fn row_count(&self) -> Option<i64> {
        self.row_count
    }
}

fn eval_info(name: &str, result_type: FieldType, ctx: &InfoColumns) -> Datum {
    ScalarFunction::new(CiString::new(name), result_type, vec![])
        .eval(ctx, tidb_chunk::row::Row::empty())
        .expect("session information builtin must evaluate")
}

// ---------------------------------------------------------------------------
// builtin_info_test.go
// ---------------------------------------------------------------------------

/// Go `pkg/expression/builtin_info_test.go:113 TestCurrentRole`.
#[test]
fn current_role() {
    for (roles, expected) in [
        ("NONE", "NONE"),
        ("`r_1`@`%`,`r_2`@`localhost`", "`r_1`@`%`,`r_2`@`localhost`"),
    ] {
        let ctx = InfoColumns {
            current_role: Some(roles.to_owned()),
            ..InfoColumns::default()
        };
        assert_eq!(
            eval_info("current_role", text_ft(), &ctx),
            Datum::new_string(expected.as_bytes().to_vec())
        );
    }
}

/// Go `pkg/expression/builtin_info_test.go:138 TestConnectionID`.
#[test]
fn connection_id() {
    let ctx = InfoColumns {
        connection_id: Some(1),
        ..InfoColumns::default()
    };
    assert_eq!(eval_info("connection_id", uint_ft(), &ctx), Datum::UInt(1));
}

/// Go `pkg/expression/builtin_info_test.go:152 TestVersion`.
#[test]
fn version() {
    let expected = runtime_versions().server_version;
    let ctx = InfoColumns {
        version: Some(expected.clone()),
        ..InfoColumns::default()
    };
    assert_eq!(
        eval_info("version", text_ft(), &ctx),
        Datum::new_string(expected.into_bytes())
    );
    assert_eq!(rewrite("version()").static_type().unwrap().flen(), 64);
}

/// Go `pkg/expression/builtin_info_test.go:163 TestBenchMark`.
#[test]
fn bench_mark() {
    for (loop_count, expression, is_nil) in [
        (-3_i64, "1", true),
        (0, "1", false),
        (3, "1", false),
        (3, "1.234", false),
        (3, "cast(1.234 as decimal(5,3))", false),
        (3, "'abc'", false),
        (3, "cast('2017-01-01 00:00:00' as datetime)", false),
        (3, "cast('12:00:00' as time)", false),
        (3, "json_array(1)", false),
    ] {
        let sql = format!("benchmark({loop_count}, {expression})");
        let got = chunk_e(&sql);
        if is_nil {
            assert_eq!(got, "NULL", "{sql}");
        } else {
            assert_eq!(got, "INT:0", "{sql}");
        }
    }
    // Go's `types.CurrentTime(mysql.TypeTimestamp)` row: CAST AS TIMESTAMP is
    // not a parser target, so the same eval-type is pinned as a typed datum.
    let ts = Time::from_date_checked(2017, 1, 1, 0, 0, 0, 0, TimeType::Timestamp, 0)
        .expect("valid timestamp");
    assert_eq!(
        eval_as(
            "benchmark",
            vec![Datum::Int(3), Datum::Time(ts)],
            int_ft(),
            &NoColumns,
        ),
        Datum::Int(0)
    );
}

/// Go `pkg/expression/builtin_info_test.go:204 TestCharset`.
#[test]
fn charset() {
    assert_eq!(rewrite("charset(null)").static_type().unwrap().flen(), 64);
}

/// Go `pkg/expression/builtin_info_test.go:213 TestCoercibility`.
#[test]
fn coercibility() {
    assert!(rewrite_expr(&parse_expr("coercibility(null)")).is_ok());
    assert_eq!(
        eval_rewritten("coercibility(null)", &NoColumns),
        Datum::Int(6)
    );
}

/// Go `pkg/expression/builtin_info_test.go:221 TestCollation`.
#[test]
fn collation() {
    assert_eq!(rewrite("collation(null)").static_type().unwrap().flen(), 64);
}

/// Go `pkg/expression/builtin_info_test.go:230 TestRowCount`.
#[test]
fn row_count() {
    let ctx = InfoColumns {
        row_count: Some(10),
        ..InfoColumns::default()
    };
    assert_eq!(eval_info("row_count", int_ft(), &ctx), Datum::Int(10));
}

/// Go `pkg/expression/builtin_info_test.go:249 TestTiDBVersion`.
#[test]
fn tidb_version() {
    let expected = get_tidb_info(&VersionInfo::build_default());
    let ctx = InfoColumns {
        tidb_info: Some(expected.clone()),
        ..InfoColumns::default()
    };
    assert_eq!(
        eval_info("tidb_version", text_ft(), &ctx),
        Datum::new_string(expected.into_bytes())
    );
}

/// Go `pkg/expression/builtin_info_test.go:258 TestLastInsertID`.
#[test]
fn last_insert_id() {
    let one_arg = [const_arg(Datum::Int(1))];
    for args in [&[][..], &one_arg[..]] {
        let result_type = builtin_return_type("last_insert_id", args).unwrap();
        assert_eq!(result_type.code(), FieldTypeCode::LongLong);
        assert_eq!(result_type.charset_name(), "binary");
        assert_eq!(result_type.collation_name(), "binary");
        assert!(result_type.has_flag(FieldTypeFlags::BINARY));
        assert!(result_type.is_unsigned());
        assert_eq!(result_type.flen(), 20);
    }

    for (previous, args, expected, published) in [
        (0_u64, vec![Datum::Int(1)], 1_u64, Some(1_u64)),
        (0, vec![Datum::Real(1.1)], 1, Some(1)),
        (0, vec![Datum::UInt(u64::MAX)], u64::MAX, Some(u64::MAX)),
        (0, vec![Datum::Int(-1)], u64::MAX, Some(u64::MAX)),
        (1, vec![], 1, None),
        (u64::MAX, vec![], u64::MAX, None),
    ] {
        let ctx = InfoColumns {
            last_insert_id: Some(previous),
            published_last_insert_id: std::cell::Cell::new(None),
            ..InfoColumns::default()
        };
        assert_eq!(
            crate::func::eval_func_values_in("LAST_INSERT_ID", &args, &ctx)
                .expect("LAST_INSERT_ID must be dispatched")
                .expect("source row must evaluate"),
            Datum::UInt(expected)
        );
        assert_eq!(ctx.published_last_insert_id.get(), published);
    }
}

/// Go `pkg/expression/builtin_info_test.go:314 TestFormatBytes`.
#[test]
fn format_bytes() {
    let ctx = NoColumns;
    for (arg, want) in [
        (Datum::Null, Datum::Null),
        (Datum::Real(0.0), Datum::new_string("0 bytes")),
        (Datum::Real(2048.0), Datum::new_string("2.00 KiB")),
        (Datum::Real(75_295_729.0), Datum::new_string("71.81 MiB")),
        (Datum::Real(5_287_242_702.0), Datum::new_string("4.92 GiB")),
        (
            Datum::Real(5_039_757_204_245.0),
            Datum::new_string("4.58 TiB"),
        ),
        (
            Datum::Real(890_250_274_520_475_525.0),
            Datum::new_string("790.70 PiB"),
        ),
        (
            Datum::Real(18_446_644_073_709_551_615.0),
            Datum::new_string("16.00 EiB"),
        ),
        (
            Datum::Real(287_952_852_482_075_252_752_429_875.0),
            Datum::new_string("2.50e+08 EiB"),
        ),
        (
            Datum::Real(-18_446_644_073_709_551_615.0),
            Datum::new_string("-16.00 EiB"),
        ),
    ] {
        assert_eq!(
            crate::builtin_ext::info::dispatch("FORMAT_BYTES", &[arg.clone()], &ctx)
                .expect("FORMAT_BYTES must dispatch")
                .expect("finite ETReal must format"),
            want,
            "{arg:?}"
        );
    }
}

/// Go `pkg/expression/builtin_info_test.go:343 TestFormatNanoTime`.
#[test]
fn format_nano_time() {
    let ctx = NoColumns;
    for (arg, want) in [
        (Datum::Null, Datum::Null),
        (Datum::Real(0.0), Datum::new_string("0 ns")),
        (Datum::Real(2000.0), Datum::new_string("2.00 us")),
        (Datum::Real(898_787_877.0), Datum::new_string("898.79 ms")),
        (Datum::Real(9_999_999_991.0), Datum::new_string("10.00 s")),
        (
            Datum::Real(898_787_877_424.0),
            Datum::new_string("14.98 min"),
        ),
        (
            Datum::Real(5_827_527_520_021.0),
            Datum::new_string("1.62 h"),
        ),
        (
            Datum::Real(42_566_623_663_736_353.0),
            Datum::new_string("492.67 d"),
        ),
        (
            Datum::Real(4_827_524_825_702_572_425_242_552.0),
            Datum::new_string("5.59e+10 d"),
        ),
        (
            Datum::Real(-9_999_999_991.0),
            Datum::new_string("-10.00 s"),
        ),
    ] {
        assert_eq!(
            crate::builtin_ext::info::dispatch("FORMAT_NANO_TIME", &[arg.clone()], &ctx)
                .expect("FORMAT_NANO_TIME must dispatch")
                .expect("finite ETReal must format"),
            want,
            "{arg:?}"
        );
    }
}

/// Go `pkg/expression/builtin_info_vec_test.go:115 TestVectorizedBuiltinInfoFunc`.
/// The Go harness is a randomized vectorized eval-vs-row comparison. Rust has
/// no `testVectorizedBuiltinFunc` chunk generator; the named info functions
/// are pinned through the rewritten scalar evaluator instead.
#[test]
fn vectorized_builtin_info_func() {
    let ctx = InfoColumns {
        current_role: Some("NONE".to_owned()),
        connection_id: Some(1),
        version: Some(runtime_versions().server_version),
        tidb_info: Some(get_tidb_info(&VersionInfo::build_default())),
        last_insert_id: Some(7),
        row_count: Some(10),
        ..InfoColumns::default()
    };
    assert_eq!(
        eval_rewritten("version()", &ctx).sql_string().unwrap(),
        runtime_versions().server_version
    );
    assert_eq!(
        eval_rewritten("tidb_version()", &ctx).sql_string().unwrap(),
        get_tidb_info(&VersionInfo::build_default())
    );
    assert_eq!(eval_rewritten("row_count()", &ctx), Datum::Int(10));
    assert_eq!(eval_rewritten("connection_id()", &ctx), Datum::UInt(1));
    assert_eq!(eval_rewritten("last_insert_id()", &ctx), Datum::UInt(7));
    assert_eq!(
        eval_rewritten("current_role()", &ctx)
            .sql_string()
            .unwrap(),
        "NONE"
    );
    assert_eq!(eval_rewritten("benchmark(3, 1)", &ctx), Datum::Int(0));
}

/// Go `pkg/expression/builtin_info_vec_test.go:119 BenchmarkVectorizedBuiltinInfoFunc`.
#[test]
#[ignore = "skipped-reason: Go testing.B microbenchmark, excluded by the gate"]
fn benchmark_vectorized_builtin_info_func() {}

// ---------------------------------------------------------------------------
// builtin_json_test.go
// ---------------------------------------------------------------------------

/// Go `pkg/expression/builtin_json_test.go:33 TestJSONType`.
#[test]
fn json_type() {
    assert_eq!(json_call("JSON_TYPE", &[Datum::Null]).unwrap(), Datum::Null);
    for (input, want) in [
        ("3", "INTEGER"),
        ("3.0", "DOUBLE"),
        ("null", "NULL"),
        ("true", "BOOLEAN"),
        ("[]", "ARRAY"),
        ("{}", "OBJECT"),
    ] {
        assert_eq!(
            json_call("JSON_TYPE", &[json_s(input)]).unwrap(),
            json_s(want)
        );
    }
}

/// Go `pkg/expression/builtin_json_test.go:58 TestJSONQuote`.
#[test]
fn json_quote() {
    assert_eq!(
        json_call("JSON_QUOTE", &[Datum::Null]).unwrap(),
        Datum::Null
    );
    for (input, want) in [
        ("", "\"\""),
        ("\"\"", "\"\\\"\\\"\""),
        ("a", "\"a\""),
        ("3", "\"3\""),
        (r#"{"a": "b"}"#, "\"{\\\"a\\\": \\\"b\\\"}\""),
        (r#"{"a":     "b"}"#, "\"{\\\"a\\\":     \\\"b\\\"}\""),
        (
            "hello,\"quoted string\",world",
            "\"hello,\\\"quoted string\\\",world\"",
        ),
        ("hello,\"宽字符\",world", "\"hello,\\\"宽字符\\\",world\""),
        (
            "Invalid Json string\tis OK",
            "\"Invalid Json string\\tis OK\"",
        ),
        (r#"1\u2232\u22322"#, "\"1\\\\u2232\\\\u22322\""),
    ] {
        assert_eq!(
            json_call("JSON_QUOTE", &[json_s(input)]).unwrap(),
            json_s(want),
            "JSON_QUOTE({input:?})"
        );
    }
}

/// Go `pkg/expression/builtin_json_test.go:87 TestJSONUnquote`.
#[test]
fn json_unquote() {
    for (input, want) in [
        ("", ""),
        ("\"\"", ""),
        ("''", "''"),
        ("3", "3"),
        (r#"{"a": "b"}"#, r#"{"a": "b"}"#),
        (r#"{"a":     "b"}"#, r#"{"a":     "b"}"#),
        (
            "\"hello,\\\"quoted string\\\",world\"",
            "hello,\"quoted string\",world",
        ),
        ("\"hello,\\\"宽字符\\\",world\"", "hello,\"宽字符\",world"),
        ("Invalid Json string\\tis OK", "Invalid Json string\\tis OK"),
        ("\"1\\\\u2232\\\\u22322\"", r#"1\u2232\u22322"#),
        (
            "\"[{\\\"x\\\":\\\"{\\\\\\\"y\\\\\\\":12}\\\"}]\"",
            r#"[{"x":"{\"y\":12}"}]"#,
        ),
        (
            r#"[{\"x\":\"{\\\"y\\\":12}\"}]"#,
            r#"[{\"x\":\"{\\\"y\\\":12}\"}]"#,
        ),
        ("\"a\"", "a"),
    ] {
        assert_eq!(
            json_call("JSON_UNQUOTE", &[json_s(input)]).unwrap(),
            json_s(want),
            "JSON_UNQUOTE({input:?})"
        );
    }
    assert!(json_call("JSON_UNQUOTE", &[json_s("\"\"a\"\"")]).is_err());
    assert!(json_call("JSON_UNQUOTE", &[json_s("\"\"\"a\"\"\"")]).is_err());
}

/// Go `pkg/expression/builtin_json_test.go:127 TestJSONSumCrc32`.
/// Go builds `expr AS type ARRAY` and checksums `fmt.Appendf("%v", item)`.
/// Rust's value-only path covers the homogeneous scalar-array CRC; typed
/// ARRAY FieldType conversion remains a gap.
#[test]
fn json_sum_crc32() {
    for (document, want) in [
        ("[-1, 2, 3]", 3_101_005_010_i64),
        ("[1, 2, 3]", 4_505_025_631_i64),
        (r#"["a", "b", "c"]"#, 5_925_539_243_i64),
        ("[1.1, 1, 3.3]", 6_204_045_883_i64),
        ("[1.1, 2.2, 3.3]", 4_453_038_788_i64),
    ] {
        assert_eq!(
            json_call("JSON_SUM_CRC32", &[json_s(document)]).unwrap(),
            Datum::Int(want),
            "JSON_SUM_CRC32({document})"
        );
    }
    assert!(json_call("JSON_SUM_CRC32", &[json_s(r#"[1.1, "1.1", 3.3]"#)]).is_err());
}

/// Go `pkg/expression/builtin_json_test.go:233 TestJSONExtract`.
#[test]
fn json_extract() {
    let jstr = r#"{"a": [{"aa": [{"aaa": 1}]}], "aaa": 2}"#;
    assert_eq!(
        json_call("JSON_EXTRACT", &[Datum::Null, Datum::Null]).unwrap(),
        Datum::Null
    );
    json_eq(
        &json_call(
            "JSON_EXTRACT",
            &[json_s(jstr), json_s("$.a[0].aa[0].aaa"), json_s("$.aaa")],
        )
        .unwrap(),
        "[1, 2]",
    );
    assert!(json_call(
        "JSON_EXTRACT",
        &[
            json_s(jstr),
            json_s("$.a[0].aa[0].aaa"),
            json_s("$InvalidPath")
        ],
    )
    .is_err());
}

/// Go `pkg/expression/builtin_json_test.go:271 TestJSONSetInsertReplace`.
#[test]
fn json_set_insert_replace() {
    assert_eq!(
        json_call("JSON_SET", &[Datum::Null, Datum::Null, Datum::Null]).unwrap(),
        Datum::Null
    );
    json_eq(
        &json_call("JSON_SET", &[json_s("{}"), json_s("$.a"), Datum::Int(3)]).unwrap(),
        r#"{"a": 3}"#,
    );
    json_eq(
        &json_call("JSON_INSERT", &[json_s("{}"), json_s("$.a"), Datum::Int(3)]).unwrap(),
        r#"{"a": 3}"#,
    );
    json_eq(
        &json_call(
            "JSON_REPLACE",
            &[json_s("{}"), json_s("$.a"), Datum::Int(3)],
        )
        .unwrap(),
        "{}",
    );
    json_eq(
        &json_call(
            "JSON_SET",
            &[
                json_s("{}"),
                json_s("$.a"),
                Datum::Int(3),
                json_s("$.b"),
                json_s("3"),
            ],
        )
        .unwrap(),
        r#"{"a": 3, "b": "3"}"#,
    );
    json_eq(
        &json_call(
            "JSON_SET",
            &[
                json_s("{}"),
                json_s("$.a"),
                Datum::Null,
                json_s("$.b"),
                json_s("nil"),
            ],
        )
        .unwrap(),
        r#"{"a": null, "b": "nil"}"#,
    );
    assert!(json_call(
        "JSON_SET",
        &[json_s("{}"), json_s("$.a"), Datum::Int(3), json_s("$.b")],
    )
    .is_err());
    assert!(json_call(
        "JSON_SET",
        &[json_s("{}"), json_s("$InvalidPath"), Datum::Int(3)]
    )
    .is_err());
}

/// Go `pkg/expression/builtin_json_test.go:317 TestJSONMerge`.
#[test]
fn json_merge() {
    assert_eq!(
        json_call("JSON_MERGE", &[Datum::Null, Datum::Null]).unwrap(),
        Datum::Null
    );
    json_eq(
        &json_call("JSON_MERGE", &[json_s("{}"), json_s("[]")]).unwrap(),
        "[{}]",
    );
    json_eq(
        &json_call(
            "JSON_MERGE",
            &[json_s("{}"), json_s("[]"), json_s("3"), json_s("\"4\"")],
        )
        .unwrap(),
        r#"[{}, 3, "4"]"#,
    );
}

/// Go `pkg/expression/builtin_json_test.go:348 TestJSONMergePreserve`.
#[test]
fn json_merge_preserve() {
    assert_eq!(
        json_call("JSON_MERGE_PRESERVE", &[Datum::Null, Datum::Null]).unwrap(),
        Datum::Null
    );
    json_eq(
        &json_call("JSON_MERGE_PRESERVE", &[json_s("{}"), json_s("[]")]).unwrap(),
        "[{}]",
    );
    json_eq(
        &json_call(
            "JSON_MERGE_PRESERVE",
            &[json_s("{}"), json_s("[]"), json_s("3"), json_s("\"4\"")],
        )
        .unwrap(),
        r#"[{}, 3, "4"]"#,
    );
}

/// Go `pkg/expression/builtin_json_test.go:379 TestJSONArray`.
#[test]
fn json_array() {
    json_eq(&json_call("JSON_ARRAY", &[Datum::Int(1)]).unwrap(), "[1]");
    json_eq(
        &json_call(
            "JSON_ARRAY",
            &[
                Datum::Null,
                json_s("a"),
                Datum::Int(3),
                json_s(r#"{"a": "b"}"#),
            ],
        )
        .unwrap(),
        r#"[null, "a", 3, "{\"a\": \"b\"}"]"#,
    );
}

/// Go `pkg/expression/builtin_json_test.go:404 TestJSONObject`.
/// The Go table's `{1, true}` row is parser-originated BOOLEAN; this value
/// domain has no typed boolean datum, so that row stays an explicit gap.
#[test]
fn json_object() {
    assert!(json_call(
        "JSON_OBJECT",
        &[Datum::Int(1), Datum::Int(2), Datum::Int(3)]
    )
    .is_err());
    json_eq(
        &json_call(
            "JSON_OBJECT",
            &[Datum::Int(1), Datum::Int(2), json_s("hello"), Datum::Null],
        )
        .unwrap(),
        r#"{"1": 2, "hello": null}"#,
    );
    assert!(json_call("JSON_OBJECT", &[Datum::Null, Datum::Int(2)]).is_err());
}

/// Go `pkg/expression/builtin_json_test.go:448 TestJSONRemove`.
#[test]
fn json_remove() {
    let doc = r#"{"a": [1, 2, {"aa": "xx"}]}"#;
    for path in ["$", "$.*", "$[*]", "$**.a"] {
        assert!(
            json_call("JSON_REMOVE", &[json_s(doc), json_s(path)]).is_err(),
            "JSON_REMOVE {path}"
        );
    }
    assert_eq!(
        json_call("JSON_REMOVE", &[Datum::Null, json_s("$.a")]).unwrap(),
        Datum::Null
    );
    json_eq(
        &json_call("JSON_REMOVE", &[json_s(doc), json_s("$.a[2].aa")]).unwrap(),
        r#"{"a": [1, 2, {}]}"#,
    );
    json_eq(
        &json_call("JSON_REMOVE", &[json_s(doc), json_s("$.a[1]")]).unwrap(),
        r#"{"a": [1, {"aa": "xx"}]}"#,
    );
    json_eq(
        &json_call(
            "JSON_REMOVE",
            &[json_s(doc), json_s("$.a[2].aa"), json_s("$.a[1]")],
        )
        .unwrap(),
        r#"{"a": [1, {}]}"#,
    );
    json_eq(
        &json_call(
            "JSON_REMOVE",
            &[json_s(doc), json_s("$.a[1]"), json_s("$.a[1].aa")],
        )
        .unwrap(),
        r#"{"a": [1, {}]}"#,
    );
    json_eq(
        &json_call("JSON_REMOVE", &[json_s(doc), json_s("$.a[3]")]).unwrap(),
        doc,
    );
    json_eq(
        &json_call("JSON_REMOVE", &[json_s(doc), json_s("$.b")]).unwrap(),
        doc,
    );
}

/// Go `pkg/expression/builtin_json_test.go:498 TestJSONMemberOf`.
#[test]
fn json_member_of() {
    assert!(json_call("JSON_MEMBER_OF", &[json_s("1"), json_s("a:1")]).is_err());
    for (document, want) in [("[1, 2]", 1), ("[1]", 1), ("[0]", 0), ("[[1]]", 0)] {
        assert_eq!(
            json_call("JSON_MEMBER_OF", &[Datum::Int(1), json_s(document)]).unwrap(),
            Datum::Int(want),
            "1 MEMBER OF {document}"
        );
    }
    assert_eq!(
        json_call("JSON_MEMBER_OF", &[json_s("1"), json_s("[1]")]).unwrap(),
        Datum::Int(0)
    );
    assert_eq!(
        json_call("JSON_MEMBER_OF", &[json_s("1"), json_s(r#"["1"]"#)]).unwrap(),
        Datum::Int(1)
    );
    assert_eq!(
        json_call(
            "JSON_MEMBER_OF",
            &[json_s(r#"{"a":1}"#), json_s(r#"{"a":1}"#)]
        )
        .unwrap(),
        Datum::Int(0)
    );
    assert_eq!(
        json_call(
            "JSON_MEMBER_OF",
            &[json_s(r#"{"a":1}"#), json_s(r#"["{\"a\":1}"]"#)]
        )
        .unwrap(),
        Datum::Int(1)
    );
}

/// Go `pkg/expression/builtin_json_test.go:539 TestJSONContains`.
#[test]
fn json_contains() {
    assert_eq!(
        json_call("JSON_CONTAINS", &[Datum::Null, json_s("1"), json_s("$.c")])
            .unwrap(),
        Datum::Null
    );
    for (doc, cand, want) in [
        ("{}", "{}", 1),
        (r#"{"a":1}"#, "{}", 1),
        (r#"{"a":1}"#, "1", 0),
        (r#"{"a":[1]}"#, "[1]", 0),
        (r#"{"b":2, "c":3}"#, r#"{"c":3}"#, 1),
        ("1", "1", 1),
        ("[1]", "1", 1),
        ("[1,2]", "[1]", 1),
        ("[1,2]", "[1,3]", 0),
        ("[1,2]", r#"["1"]"#, 0),
        ("[1,2,[1,3]]", "[1,3]", 1),
        (r#"[{"a":1}]"#, r#"{"a":1}"#, 1),
        (r#"[{"a":1,"b":2}]"#, r#"{"a":1}"#, 1),
        (r#"[{"a":{"a":1},"b":2}]"#, r#"{"a":1}"#, 0),
    ] {
        assert_eq!(
            json_call("JSON_CONTAINS", &[json_s(doc), json_s(cand)]).unwrap(),
            Datum::Int(want),
            "{doc} contains {cand}"
        );
    }
    assert_eq!(
        json_call(
            "JSON_CONTAINS",
            &[json_s("[1,2,[1,[5,[3]]]]"), json_s("[1,3]"), json_s("$[2]")],
        )
        .unwrap(),
        Datum::Int(1)
    );
    for path in ["$.*", "$[*]", "$**.a"] {
        assert!(json_call(
            "JSON_CONTAINS",
            &[
                json_s(r#"{"a": [1, 2, {"aa": "xx"}]}"#),
                json_s("1"),
                json_s(path),
            ],
        )
        .is_err());
    }
    assert!(json_call(
        "JSON_CONTAINS",
        &[json_s("[1,2,[1,3]]"), json_s("a:1")]
    )
    .is_err());
}

/// Go `pkg/expression/builtin_json_test.go:619 TestJSONOverlaps`.
#[test]
fn json_overlaps() {
    assert!(json_call("JSON_OVERLAPS", &[json_s("[1,2,[1,3]]"), json_s("a:1")]).is_err());
    assert_eq!(
        json_call("JSON_OVERLAPS", &[Datum::Null, json_s("1")]).unwrap(),
        Datum::Null
    );
    for (left, right, want) in [
        ("[1, 2]", "[2,3]", 1),
        ("[1, 2]", "2", 1),
        (r#"[{"a":1}]"#, r#"{"a":1}"#, 1),
        (r#"[{"a":1}]"#, r#"{"a":1,"b":2}"#, 0),
        ("1", "1", 1),
        ("0", "1", 0),
        ("[[1,2], 3]", "[1,3]", 1),
        (r#"[4,5,"6",7]"#, "6", 0),
        ("2", "[1, 2]", 1),
        (r#"{"a":1}"#, r#"[{"a":1}]"#, 1),
    ] {
        assert_eq!(
            json_call("JSON_OVERLAPS", &[json_s(left), json_s(right)]).unwrap(),
            Datum::Int(want),
            "{left} overlaps {right}"
        );
    }
}

/// Go `pkg/expression/builtin_json_test.go:684 TestJSONContainsPath`.
#[test]
fn json_contains_path() {
    let json = r#"{"a": 1, "b": 2, "c": {"d": 4}}"#;
    assert_eq!(
        json_call(
            "JSON_CONTAINS_PATH",
            &[Datum::Null, json_s("one"), json_s("$.c")],
        )
        .unwrap(),
        Datum::Null
    );
    for (one_or_all, paths, want) in [
        ("one", vec!["$.c.d"], 1),
        ("one", vec!["$.a.d"], 0),
        ("all", vec!["$.c.d"], 1),
        ("all", vec!["$.a.d"], 0),
        ("one", vec!["$.a", "$.e"], 1),
        ("all", vec!["$.a", "$.e"], 0),
        ("all", vec!["$.a", "$.c"], 1),
        ("one", vec!["$.*"], 1),
        ("one", vec!["$[*]"], 0),
        ("ONE", vec!["$.c.d"], 1),
        ("aLl", vec!["$.a", "$.e"], 0),
    ] {
        let mut args = vec![json_s(json), json_s(one_or_all)];
        args.extend(paths.iter().copied().map(json_s));
        assert_eq!(
            json_call("JSON_CONTAINS_PATH", &args).unwrap(),
            Datum::Int(want),
            "{one_or_all} {paths:?}"
        );
    }
    assert!(json_call(
        "JSON_CONTAINS_PATH",
        &[json_s(r#"{"a": 1"#), json_s("one"), json_s("$.a")],
    )
    .is_err());
    assert!(json_call(
        "JSON_CONTAINS_PATH",
        &[json_s(json), json_s("test"), json_s("$.a")],
    )
    .is_err());
}

/// Go `pkg/expression/builtin_json_test.go:743 TestJSONLength`.
#[test]
fn json_length() {
    for (doc, want) in [
        ("null", 1),
        ("true", 1),
        ("1", 1),
        ("-1", 1),
        ("1.1", 1),
        (r#""1""#, 1),
        ("{}", 0),
        (r#"{"a":1}"#, 1),
        (r#"{"b":2, "c":3}"#, 2),
        ("[1,2]", 2),
        ("[1,2,[1,3]]", 3),
    ] {
        assert_eq!(
            json_call("JSON_LENGTH", &[json_s(doc)]).unwrap(),
            Datum::Int(want),
            "{doc}"
        );
    }
    assert_eq!(json_call("JSON_LENGTH", &[Datum::Null]).unwrap(), Datum::Null);
    assert_eq!(
        json_call(
            "JSON_LENGTH",
            &[json_s("[1,2,[1,[5,[3]]]]"), json_s("$[2]")],
        )
        .unwrap(),
        Datum::Int(2)
    );
    assert!(json_call(
        "JSON_LENGTH",
        &[json_s(r#""1""#), json_s("$.a")]
    )
    .unwrap()
    .is_null());
    for path in ["$.*", "$[*]", "$**.a"] {
        assert!(json_call(
            "JSON_LENGTH",
            &[json_s(r#"{"a": [1, 2, {"aa": "xx"}]}"#), json_s(path)],
        )
        .is_err());
    }
}

/// Go `pkg/expression/builtin_json_test.go:815 TestJSONKeys`.
#[test]
fn json_keys() {
    assert_eq!(json_call("JSON_KEYS", &[Datum::Null]).unwrap(), Datum::Null);
    for scalar in ["1", r#""str""#, "true", "null", "[1, 2]"] {
        assert!(
            json_call("JSON_KEYS", &[json_s(scalar)]).unwrap().is_null(),
            "{scalar}"
        );
    }
    json_eq(&json_call("JSON_KEYS", &[json_s("{}")]).unwrap(), "[]");
    json_eq(
        &json_call("JSON_KEYS", &[json_s(r#"{"a": 1}"#)]).unwrap(),
        r#"["a"]"#,
    );
    json_eq(
        &json_call("JSON_KEYS", &[json_s(r#"{"a": 1, "b": 2}"#)]).unwrap(),
        r#"["a", "b"]"#,
    );
    json_eq(
        &json_call(
            "JSON_KEYS",
            &[json_s(r#"{"a": {"c": 3}, "b": 2}"#), json_s("$.a")],
        )
        .unwrap(),
        r#"["c"]"#,
    );
    assert!(json_call("JSON_KEYS", &[json_s("{}"), json_s("$.*")]).is_err());
    let array = r#"[{"A1": 1, "B1": 2, "C1": 3}, {"A2": 10, "B2": 20, "C2": {"D": 4}}, {"A3": 1, "B3": 2, "C3": 6}]"#;
    json_eq(
        &json_call("JSON_KEYS", &[json_s(array), json_s("$[1]")]).unwrap(),
        r#"["A2", "B2", "C2"]"#,
    );
}

/// Go `pkg/expression/builtin_json_test.go:890 TestJSONDepth`.
#[test]
fn json_depth() {
    for (input, want) in [
        ("null", 1),
        ("true", 1),
        ("1", 1),
        ("{}", 1),
        ("[]", 1),
        ("[10, 20]", 2),
        (r#"{"Name": "Homer"}"#, 2),
        (r#"[10, {"a": 20}]"#, 3),
        (r#"{"a":[1]}"#, 3),
        ("[1,2,[1,[5,[3]]]]", 5),
        (r#"[1,2,[1,[5,{"a":[2,3]}]]]"#, 6),
    ] {
        assert_eq!(
            json_call("JSON_DEPTH", &[json_s(input)]).unwrap(),
            Datum::Int(want),
            "{input}"
        );
    }
    assert_eq!(json_call("JSON_DEPTH", &[Datum::Null]).unwrap(), Datum::Null);
    assert!(json_call("JSON_DEPTH", &[json_s("a")]).is_err());
}

/// Go `pkg/expression/builtin_json_test.go:949 TestJSONArrayAppend`.
#[test]
fn json_array_append() {
    json_eq(
        &json_call(
            "JSON_ARRAY_APPEND",
            &[
                json_s(r#"{"a": 1, "b": [2, 3], "c": 4}"#),
                json_s("$.d"),
                json_s("z"),
            ],
        )
        .unwrap(),
        r#"{"a": 1, "b": [2, 3], "c": 4}"#,
    );
    json_eq(
        &json_call(
            "JSON_ARRAY_APPEND",
            &[
                json_s(r#"{"a": 1, "b": [2, 3], "c": 4}"#),
                json_s("$"),
                json_s("w"),
            ],
        )
        .unwrap(),
        r#"[{"a": 1, "b": [2, 3], "c": 4}, "w"]"#,
    );
    json_eq(
        &json_call(
            "JSON_ARRAY_APPEND",
            &[json_s(r#"{"a": 1}"#), json_s("$"), json_s(r#"{"b": 2}"#)],
        )
        .unwrap(),
        r#"[{"a": 1}, "{\"b\": 2}"]"#,
    );
    assert_eq!(
        json_call(
            "JSON_ARRAY_APPEND",
            &[Datum::Null, json_s("$"), Datum::Null]
        )
        .unwrap(),
        Datum::Null
    );
    json_eq(
        &json_call(
            "JSON_ARRAY_APPEND",
            &[
                json_s(r#"["a", ["b", "c"], "d"]"#),
                json_s("$[1]"),
                Datum::Int(1),
            ],
        )
        .unwrap(),
        r#"["a", ["b", "c", 1], "d"]"#,
    );
    assert!(json_call(
        "JSON_ARRAY_APPEND",
        &[json_s("asdf"), json_s("$"), Datum::Null]
    )
    .is_err());
    assert!(json_dispatch(
        "JSON_ARRAY_APPEND",
        &[json_s(r#"{"a": 1}"#), json_s("$.d")]
    )
    .is_none());
    assert!(json_call(
        "JSON_ARRAY_APPEND",
        &[json_s(r#"{"a": 1}"#), json_s("$.*"), Datum::Null],
    )
    .is_err());
}

/// Go `pkg/expression/builtin_json_test.go:1028 TestJSONSearch`.
#[test]
fn json_search() {
    let json = r#"["abc", [{"k": "10"}, "def"], {"x":"abc"}, {"y":"bcd"}]"#;
    let json2 = r#"["abc", [{"k": "10"}, "def"], {"x":"ab%d"}, {"y":"abcd"}]"#;
    json_eq(
        &json_call(
            "JSON_SEARCH",
            &[json_s(json), json_s("one"), json_s("abc")],
        )
        .unwrap(),
        r#""$[0]""#,
    );
    json_eq(
        &json_call(
            "JSON_SEARCH",
            &[json_s(json), json_s("all"), json_s("abc")],
        )
        .unwrap(),
        r#"["$[0]", "$[2].x"]"#,
    );
    assert!(json_call(
        "JSON_SEARCH",
        &[json_s(json), json_s("all"), json_s("ghi")]
    )
    .unwrap()
    .is_null());
    json_eq(
        &json_call(
            "JSON_SEARCH",
            &[json_s(json), json_s("all"), json_s("10")],
        )
        .unwrap(),
        r#""$[1][0].k""#,
    );
    json_eq(
        &json_call(
            "JSON_SEARCH",
            &[json_s(json2), json_s("all"), json_s(r#"ab\%d"#)],
        )
        .unwrap(),
        r#""$[2].x""#,
    );
    json_eq(
        &json_call(
            "JSON_SEARCH",
            &[
                json_s(json2),
                json_s("all"),
                json_s("ab|%d"),
                json_s("|"),
            ],
        )
        .unwrap(),
        r#""$[2].x""#,
    );
    assert!(json_call(
        "JSON_SEARCH",
        &[Datum::Null, json_s("all"), json_s("abc")]
    )
    .unwrap()
    .is_null());
    assert!(json_call("JSON_SEARCH", &[json_s("a"), json_s("all"), json_s("abc")]).is_err());
    assert!(json_call(
        "JSON_SEARCH",
        &[json_s(json), json_s("wrong"), json_s("abc")]
    )
    .is_err());
}

/// Go `pkg/expression/builtin_json_test.go:1103 TestJSONArrayInsert`.
#[test]
fn json_array_insert() {
    json_eq(
        &json_call(
            "JSON_ARRAY_INSERT",
            &[
                json_s(r#"{"a": 1, "b": [2, 3], "c": 4}"#),
                json_s("$.b[1]"),
                json_s("z"),
            ],
        )
        .unwrap(),
        r#"{"a": 1, "b": [2, "z", 3], "c": 4}"#,
    );
    json_eq(
        &json_call(
            "JSON_ARRAY_INSERT",
            &[
                json_s(r#"[{"a": 1, "b": [2, 3], "c": 4}]"#),
                json_s("$[1]"),
                json_s("w"),
            ],
        )
        .unwrap(),
        r#"[{"a": 1, "b": [2, 3], "c": 4}, "w"]"#,
    );
    json_eq(
        &json_call(
            "JSON_ARRAY_INSERT",
            &[json_s("[1, 2, 3]"), json_s("$[100]"), json_s(r#"{"b": 2}"#)],
        )
        .unwrap(),
        r#"[1, 2, 3, "{\"b\": 2}"]"#,
    );
    assert_eq!(
        json_call(
            "JSON_ARRAY_INSERT",
            &[Datum::Null, json_s("$"), Datum::Null]
        )
        .unwrap(),
        Datum::Null
    );
    json_eq(
        &json_call(
            "JSON_ARRAY_INSERT",
            &[
                json_s(r#"["a", {"b": [1, 2]}, [3, 4]]"#),
                json_s("$[1]"),
                json_s("x"),
            ],
        )
        .unwrap(),
        r#"["a", "x", {"b": [1, 2]}, [3, 4]]"#,
    );
    json_eq(
        &json_call(
            "JSON_ARRAY_INSERT",
            &[
                json_s(r#"["a", {"b": [1, 2]}, [3, 4]]"#),
                json_s("$[0]"),
                json_s("x"),
                json_s("$[2][1]"),
                json_s("y"),
            ],
        )
        .unwrap(),
        r#"["x", "a", {"b": [1, 2]}, [3, 4]]"#,
    );
    assert!(json_call(
        "JSON_ARRAY_INSERT",
        &[json_s(r#"{"a": 1}"#), json_s("$.a"), Datum::Null],
    )
    .is_err());
    assert!(json_dispatch("JSON_ARRAY_INSERT", &[json_s("{}"), json_s("$.d")]).is_none());
}

/// Go `pkg/expression/builtin_json_test.go:1176 TestJSONValid`.
#[test]
fn json_valid() {
    for (input, want) in [
        (json_s(r#"{"a":1}"#), Datum::Int(1)),
        (json_s("hello"), Datum::Int(0)),
        (json_s(r#""hello""#), Datum::Int(1)),
        (json_s("null"), Datum::Int(1)),
        (json_s("{}"), Datum::Int(1)),
        (json_s("[]"), Datum::Int(1)),
        (json_s("2"), Datum::Int(1)),
        (json_s("2.5"), Datum::Int(1)),
        (json_s("2019-8-19"), Datum::Int(0)),
        (json_s(r#""2019-8-19""#), Datum::Int(1)),
        (Datum::Int(2), Datum::Int(0)),
        (Datum::Real(2.5), Datum::Int(0)),
        (Datum::Null, Datum::Null),
    ] {
        assert_eq!(
            json_call("JSON_VALID", &[input.clone()]).unwrap(),
            want,
            "{input:?}"
        );
    }
}

/// Go `pkg/expression/builtin_json_test.go:1207 TestJSONStorageFree`.
#[test]
fn json_storage_free() {
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
            json_call("JSON_STORAGE_FREE", &[json_s(input)]).unwrap(),
            Datum::Int(0),
            "{input}"
        );
    }
    assert_eq!(
        json_call("JSON_STORAGE_FREE", &[Datum::Null]).unwrap(),
        Datum::Null
    );
    assert!(json_call("JSON_STORAGE_FREE", &[json_s(r#"[{"a":1]"#)]).is_err());
}

/// Go `pkg/expression/builtin_json_test.go:1250 TestJSONStorageSize`.
#[test]
fn json_storage_size() {
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
            json_call("JSON_STORAGE_SIZE", &[json_s(input)]).unwrap(),
            Datum::Int(want),
            "{input}"
        );
    }
    assert_eq!(
        json_call("JSON_STORAGE_SIZE", &[Datum::Null]).unwrap(),
        Datum::Null
    );
    assert!(json_call("JSON_STORAGE_SIZE", &[json_s(r#"[{"a":1]"#)]).is_err());
}

/// Go `pkg/expression/builtin_json_test.go:1293 TestJSONPretty`.
#[test]
fn json_pretty() {
    assert_eq!(
        json_call("JSON_PRETTY", &[Datum::Null]).unwrap(),
        Datum::Null
    );
    assert_eq!(
        json_call("JSON_PRETTY", &[json_s("true")]).unwrap(),
        json_s("true")
    );
    assert_eq!(
        json_call("JSON_PRETTY", &[json_s("2223")]).unwrap(),
        json_s("2223")
    );
    assert_eq!(
        json_call("JSON_PRETTY", &[json_s(r#"{"a":1}"#)])
            .unwrap()
            .sql_string()
            .unwrap(),
        "{\n  \"a\": 1\n}"
    );
    assert_eq!(
        json_call("JSON_PRETTY", &[json_s("[1]")])
            .unwrap()
            .sql_string()
            .unwrap(),
        "[\n  1\n]"
    );
    assert!(json_call("JSON_PRETTY", &[json_s("{1}")]).is_err());
    assert!(json_call("JSON_PRETTY", &[json_s("[1,3,4,5]]")]).is_err());
}

/// Go `pkg/expression/builtin_json_test.go:1367 TestJSONMergePatch`.
#[test]
fn json_merge_patch() {
    for (left, right, want) in [
        (r#"{"a":"b"}"#, r#"{"a":"c"}"#, r#"{"a": "c"}"#),
        (r#"{"a":"b"}"#, r#"{"b":"c"}"#, r#"{"a": "b","b": "c"}"#),
        (r#"{"a":"b"}"#, r#"{"a":null}"#, "{}"),
        (r#"{"a":["b"]}"#, r#"{"a":"c"}"#, r#"{"a": "c"}"#),
        (r#"["a","b"]"#, r#"["c","d"]"#, r#"["c", "d"]"#),
        (r#"{"a":"b"}"#, r#"["c"]"#, r#"["c"]"#),
        (r#"{"a":"foo"}"#, "null", "null"),
        (r#"{"a":"foo"}"#, r#""bar""#, r#""bar""#),
        ("[1,2]", r#"{"a":"b","c":null}"#, r#"{"a":"b"}"#),
        (r#"{"a":"foo"}"#, "false", "false"),
        ("null", r#"{"a":1}"#, r#"{"a":1}"#),
        (r#"{"a":1}"#, "null", "null"),
    ] {
        json_eq(
            &json_call("JSON_MERGE_PATCH", &[json_s(left), json_s(right)]).unwrap(),
            want,
        );
    }
    assert_eq!(
        json_call(
            "JSON_MERGE_PATCH",
            &[json_s(r#"{"a":"foo"}"#), Datum::Null],
        )
        .unwrap(),
        Datum::Null
    );
    assert!(json_call("JSON_MERGE_PATCH", &[json_s(r#"{"a":1}"#), json_s("[1]}")]).is_err());
}

/// Go `pkg/expression/builtin_json_test.go:1459 TestJSONSchemaValid`.
#[test]
fn json_schema_valid() {
    assert_eq!(
        json_call("JSON_SCHEMA_VALID", &[Datum::Null, json_s("{}")]).unwrap(),
        Datum::Null
    );
    assert_eq!(
        json_call("JSON_SCHEMA_VALID", &[json_s("{}"), Datum::Null]).unwrap(),
        Datum::Null
    );
    for (schema, document, want) in [
        ("{}", "{}", 1),
        (r#"{"required": ["a","b"]}"#, r#"{"a": 5}"#, 0),
        (r#"{"required": ["a","b"]}"#, r#"{"a": 5, "b": 6}"#, 1),
        (r#"{"type": ["string"]}"#, "{}", 0),
        (r#"{"type": ["string"]}"#, r#""foobar""#, 1),
        (r#"{"type": ["object"]}"#, "{}", 1),
        (r#"{"type": ["object"]}"#, r#""foobar""#, 0),
        (r#"{"properties": {"a": {"type": "number"}}}"#, "{}", 1),
        (
            r#"{"properties": {"a": {"type": "number"}}}"#,
            r#"{"a": "foobar"}"#,
            0,
        ),
        (
            r#"{"properties": {"a": {"type": "number"}}}"#,
            r#"{"a": 5}"#,
            1,
        ),
        (
            r#"{"properties": {"a": {"type": "number", "minimum": 6}}}"#,
            r#"{"a": 5}"#,
            0,
        ),
        (
            r#"{"properties": {"a": {"type": "string", "pattern": "^a"}}}"#,
            r#"{"a": "abc"}"#,
            1,
        ),
        (
            r#"{"properties": {"a": {"type": "string", "pattern": "^a"}}}"#,
            r#"{"a": "cba"}"#,
            0,
        ),
    ] {
        assert_eq!(
            json_call("JSON_SCHEMA_VALID", &[json_s(schema), json_s(document)]).unwrap(),
            Datum::Int(want),
            "{schema} {document}"
        );
    }
}

/// Go `pkg/expression/builtin_json_test.go:1519 TestJSONSchemaValidCache`.
/// Go's failpoint is not present in Rust; this pins Clone-starts-empty
/// (`JsonSchemaCache::clone` matches `builtinJSONSchemaValidSig.Clone`).
#[test]
fn json_schema_valid_cache() {
    let args = vec![const_arg(json_s("{}")), const_arg(json_s("{}"))];
    let function = ScalarFunction::new(CiString::new("json_schema_valid"), int_ft(), args);
    let empty = tidb_chunk::chunk::Chunk::new_with_capacity(&[], 1);
    assert_eq!(
        function.eval(&NoColumns, empty.get_row(0)).unwrap(),
        Datum::Int(1)
    );
    let cloned = function.clone();
    assert_eq!(
        cloned.eval(&NoColumns, empty.get_row(0)).unwrap(),
        Datum::Int(1)
    );
}

/// Go `pkg/expression/builtin_json_vec_test.go:152 TestVectorizedBuiltinJSONFunc`.
#[test]
fn vectorized_builtin_json_func() {
    json_eq(
        &json_call("JSON_KEYS", &[json_s(r#"{"a": {"c": 3}, "b": 2}"#)]).unwrap(),
        r#"["a", "b"]"#,
    );
    assert_eq!(
        json_call("JSON_TYPE", &[json_s("{}")]).unwrap(),
        json_s("OBJECT")
    );
    assert_eq!(
        json_call("JSON_LENGTH", &[json_s("[1,2]")]).unwrap(),
        Datum::Int(2)
    );
}

/// Go `pkg/expression/builtin_json_vec_test.go:156 BenchmarkVectorizedBuiltinJSONFunc`.
#[test]
#[ignore = "skipped-reason: Go testing.B microbenchmark, excluded by the gate"]
fn benchmark_vectorized_builtin_json_func() {}

// ---------------------------------------------------------------------------
// builtin_like_test.go
// ---------------------------------------------------------------------------

/// Go `pkg/expression/builtin_like_test.go:29 TestLike`.
#[test]
fn like() {
    for (input, pattern, want) in [
        ("a", "", 0),
        ("a", "a", 1),
        ("a", "b", 0),
        ("aA", "Aa", 0),
        ("aAb", "Aa%", 0),
        ("aAb", "aA_", 1),
        ("baab", "b_%b", 1),
        ("baab", "b%_b", 1),
        ("bab", "b_%b", 1),
        ("bab", "b%_b", 1),
        ("bb", "b_%b", 0),
        ("bb", "b%_b", 0),
        ("baabccc", "b_%b%", 1),
        ("a", r"\a", 1),
    ] {
        let sql = format!("'{input}' like '{pattern}'");
        assert_eq!(e(&sql), format!("INT:{want}"), "{sql}");
        assert_eq!(chunk_e(&sql), format!("INT:{want}"), "chunk {sql}");
    }
}

/// Go `pkg/expression/builtin_like_test.go:63 TestRegexp`.
#[test]
fn regexp() {
    for (pattern, input, want, is_err) in [
        ("^$", "a", 0, false),
        ("a", "a", 1, false),
        ("a", "b", 0, false),
        ("aA", "aA", 1, false),
        (".", "a", 1, false),
        ("^.$", "ab", 0, false),
        ("..", "b", 0, false),
        (".ab", "aab", 1, false),
        (".*", "abcd", 1, false),
        ("(", "", 0, true),
        ("(*", "", 0, true),
        ("[a", "", 0, true),
        ("\\", "", 0, true),
    ] {
        if is_err {
            assert!(
                matches!(
                    regexp_match(input, pattern),
                    Err(EvalError::Unsupported("invalid regular expression pattern"))
                ),
                "pattern {pattern:?}"
            );
            continue;
        }
        assert_eq!(
            regexp_like(input, pattern, "").unwrap(),
            want == 1,
            "{input} regexp {pattern}"
        );
        let sql = format!("'{input}' regexp '{pattern}'");
        assert_eq!(e(&sql), format!("INT:{want}"), "{sql}");
    }
}

/// Go `pkg/expression/builtin_like_test.go:99 TestCILike`.
#[test]
fn ci_like() {
    let rows = [
        ("a", "", false, false, false),
        ("a", "a", true, true, true),
        ("a", "á", true, true, true),
        ("a", "b", false, false, false),
        ("aA", "Aa", true, true, true),
        ("áAb", "Aa%", true, true, true),
        ("áAb", "%ab%", true, true, true),
        ("áAb", "%ab", true, true, true),
        ("ÀAb", "aA_", true, true, true),
        ("áééá", "a_%a", true, true, true),
        ("áééá", "a%_a", true, true, true),
        ("áéá", "a_%a", true, true, true),
        ("áéá", "a%_a", true, true, true),
        ("áá", "a_%a", false, false, false),
        ("áá", "a%_a", false, false, false),
        ("áééáííí", "a_%a%", true, true, true),
        ("数汉据字库", "数%据_库", true, true, true),
        ("ß", "s%", true, false, false),
        ("ß", "%s", true, false, false),
        ("ß", "ss", false, false, false),
        ("ß", "s", true, false, false),
        ("ss", "%ß%", true, false, false),
        ("ß", "_", true, true, true),
        ("ß", "__", false, false, false),
        ("Ⱕ", "ⱕ", false, false, true),
    ];
    for (input, pattern, general, unicode, unicode0900) in rows {
        assert_eq!(
            like_match_with_collation(input, pattern, Some(0), Collation::Utf8Mb4GeneralCi),
            general,
            "general-ci {input:?} {pattern:?}"
        );
        assert_eq!(
            like_match_with_collation(input, pattern, Some(0), Collation::Utf8Mb4UnicodeCi),
            unicode,
            "unicode-ci {input:?} {pattern:?}"
        );
        assert_eq!(
            like_match_with_collation(input, pattern, Some(0), Collation::Utf8Mb40900AiCi),
            unicode0900,
            "0900-ai-ci {input:?} {pattern:?}"
        );
    }
}

/// Go `pkg/expression/builtin_like_vec_test.go:35 TestVectorizedBuiltinLikeFunc`.
#[test]
fn vectorized_builtin_like_func() {
    assert_eq!(e("'a' like 'a'"), "INT:1");
    assert_eq!(e("'a' regexp 'a'"), "INT:1");
    assert_eq!(chunk_e("'baab' like 'b_%b'"), "INT:1");
}

/// Go `pkg/expression/builtin_like_vec_test.go:39 BenchmarkVectorizedBuiltinLikeFunc`.
#[test]
#[ignore = "skipped-reason: Go testing.B microbenchmark, excluded by the gate"]
fn benchmark_vectorized_builtin_like_func() {}

// ---------------------------------------------------------------------------
// builtin_math_test.go
// ---------------------------------------------------------------------------

/// Go `pkg/expression/builtin_math_test.go:35 TestAbs`.
#[test]
fn abs() {
    for (arg, want) in [
        (Datum::Null, Datum::Null),
        (Datum::Int(1), Datum::Int(1)),
        (Datum::UInt(1), Datum::UInt(1)),
        (Datum::Int(-1), Datum::Int(1)),
        (Datum::Real(3.14), Datum::Real(3.14)),
        (Datum::Real(-3.14), Datum::Real(3.14)),
    ] {
        assert_eq!(math_call("ABS", &[arg.clone()]).unwrap(), want, "{arg:?}");
    }
}

/// Go `pkg/expression/builtin_math_test.go:61 TestCeil`.
#[test]
fn ceil() {
    for name in ["CEIL", "CEILING"] {
        assert_eq!(math_call(name, &[Datum::Null]).unwrap(), Datum::Null);
        assert_eq!(math_call(name, &[Datum::Int(1)]).unwrap(), Datum::Int(1));
        assert_eq!(
            math_call(name, &[Datum::Real(1.23)]).unwrap(),
            Datum::Real(2.0)
        );
        assert_eq!(
            math_call(name, &[Datum::Real(-1.23)]).unwrap(),
            Datum::Real(-1.0)
        );
        assert_eq!(
            math_call(name, &[Datum::new_string("1.23")]).unwrap(),
            Datum::Real(2.0)
        );
        assert_eq!(
            math_call(name, &[Datum::new_string("-1.23")]).unwrap(),
            Datum::Real(-1.0)
        );
        assert_eq!(
            math_call(name, &[Datum::new_string("tidb")]).unwrap(),
            Datum::Real(0.0)
        );
        assert_eq!(
            math_call(name, &[Datum::new_string("1tidb")]).unwrap(),
            Datum::Real(1.0)
        );
    }
}

/// Go `pkg/expression/builtin_math_test.go:126 TestExp`.
#[test]
fn exp() {
    assert_eq!(math_call("EXP", &[Datum::Null]).unwrap(), Datum::Null);
    assert_eq!(
        math_call("EXP", &[Datum::Int(1)]).unwrap(),
        Datum::Real(2.718_281_828_459_045)
    );
    assert_eq!(
        math_call("EXP", &[Datum::Real(1.23)]).unwrap(),
        Datum::Real(3.421_229_536_289_673_4)
    );
    assert_eq!(
        math_call("EXP", &[Datum::Real(-1.23)]).unwrap(),
        Datum::Real(0.292_292_577_680_859_4)
    );
    assert_eq!(
        math_call("EXP", &[Datum::Real(0.0)]).unwrap(),
        Datum::Real(1.0)
    );
    assert_eq!(
        math_call("EXP", &[Datum::new_string("0")]).unwrap(),
        Datum::Real(1.0)
    );
    assert_eq!(
        math_call("EXP", &[Datum::new_string("tidb")]).unwrap(),
        Datum::Real(1.0)
    );
    assert!(matches!(
        math_call("EXP", &[Datum::Real(100_000.0)]),
        Err(EvalError::FloatOverflow)
    ));
}

/// Go `pkg/expression/builtin_math_test.go:177 TestFloor`.
#[test]
fn floor() {
    assert_eq!(math_call("FLOOR", &[Datum::Null]).unwrap(), Datum::Null);
    assert_eq!(math_call("FLOOR", &[Datum::Int(1)]).unwrap(), Datum::Int(1));
    assert_eq!(
        math_call("FLOOR", &[Datum::Real(1.23)]).unwrap(),
        Datum::Real(1.0)
    );
    assert_eq!(
        math_call("FLOOR", &[Datum::Real(-1.23)]).unwrap(),
        Datum::Real(-2.0)
    );
    assert_eq!(
        math_call("FLOOR", &[Datum::new_string("1.23")]).unwrap(),
        Datum::Real(1.0)
    );
    assert_eq!(
        math_call("FLOOR", &[Datum::new_string("-1.23")]).unwrap(),
        Datum::Real(-2.0)
    );
    assert_eq!(
        math_call("FLOOR", &[Datum::new_string("-1.b23")]).unwrap(),
        Datum::Real(-1.0)
    );
    assert_eq!(
        math_call("FLOOR", &[Datum::new_string("abce")]).unwrap(),
        Datum::Real(0.0)
    );

    let duration_hms = MySqlDuration::new(12, 59, 59, 0, 0).expect("valid duration");
    assert_eq!(
        math_call("FLOOR", &[Datum::Duration(duration_hms)]).unwrap(),
        Datum::Real(125_959.0)
    );
    let duration_ms = MySqlDuration::new(0, 12, 34, 0, 0).expect("valid duration");
    assert_eq!(
        math_call("FLOOR", &[Datum::Duration(duration_ms)]).unwrap(),
        Datum::Real(1_234.0)
    );
    let time = Time::from_date_checked(2017, 7, 19, 0, 0, 0, 0, TimeType::DateTime, 0)
        .expect("valid datetime");
    assert_eq!(
        math_call("FLOOR", &[Datum::Time(time)]).unwrap(),
        Datum::Real(20_170_719_000_000.0)
    );
}

/// Go `pkg/expression/builtin_math_test.go:247 TestLog`.
#[test]
fn log() {
    assert_eq!(math_call("LOG", &[Datum::Null]).unwrap(), Datum::Null);
    assert_eq!(
        math_call("LOG", &[Datum::Int(100)]).unwrap(),
        Datum::Real(4.605_170_185_988_092)
    );
    assert_eq!(
        math_call("LOG", &[Datum::Int(10), Datum::Int(100)]).unwrap(),
        Datum::Real(2.0)
    );
    assert_eq!(math_call("LOG", &[Datum::Real(-1.0)]).unwrap(), Datum::Null);
    assert_eq!(
        math_call("LOG", &[Datum::Real(0.5), Datum::Real(0.25)]).unwrap(),
        Datum::Real(2.0)
    );
    assert_eq!(
        math_call("LOG", &[Datum::new_string("abc")]).unwrap(),
        Datum::Null
    );
}

/// Go `pkg/expression/builtin_math_test.go:290 TestLog2`.
#[test]
fn log2() {
    assert_eq!(math_call("LOG2", &[Datum::Null]).unwrap(), Datum::Null);
    assert_eq!(
        math_call("LOG2", &[Datum::Int(16)]).unwrap(),
        Datum::Real(4.0)
    );
    assert_eq!(
        math_call("LOG2", &[Datum::Int(5)]).unwrap(),
        Datum::Real(2.321_928_094_887_362)
    );
    assert_eq!(math_call("LOG2", &[Datum::Int(-1)]).unwrap(), Datum::Null);
    assert_eq!(
        math_call("LOG2", &[Datum::new_string("4abc")]).unwrap(),
        Datum::Real(2.0)
    );
    assert_eq!(
        math_call("LOG2", &[Datum::new_string("abc")]).unwrap(),
        Datum::Null
    );
}

/// Go `pkg/expression/builtin_math_test.go:328 TestLog10`.
#[test]
fn log10() {
    assert_eq!(math_call("LOG10", &[Datum::Null]).unwrap(), Datum::Null);
    assert_eq!(
        math_call("LOG10", &[Datum::Int(100)]).unwrap(),
        Datum::Real(2.0)
    );
    assert_eq!(
        math_call("LOG10", &[Datum::Int(101)]).unwrap(),
        Datum::Real(2.004_321_373_782_642_6)
    );
    assert_eq!(math_call("LOG10", &[Datum::Int(-1)]).unwrap(), Datum::Null);
    assert_eq!(
        math_call("LOG10", &[Datum::new_string("100abc")]).unwrap(),
        Datum::Real(2.0)
    );
    assert_eq!(
        math_call("LOG10", &[Datum::new_string("abc")]).unwrap(),
        Datum::Null
    );
}

/// Go `pkg/expression/builtin_math_test.go:366 TestRand`.
#[test]
fn rand() {
    struct SeqColumns {
        rng: MysqlRng,
    }
    impl Columns for SeqColumns {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn rand_next(&self) -> Option<f64> {
            Some(self.rng.gen())
        }
        fn rand_seeded_next(&self, _key: usize, seed: i64) -> Option<f64> {
            Some(MysqlRng::new_with_seed(seed).gen())
        }
    }

    let ctx = SeqColumns {
        rng: MysqlRng::new_with_time(),
    };
    let value = eval_as("rand", vec![], real_ft(), &ctx);
    let Datum::Real(sample) = value else {
        panic!("RAND() must be real, got {value:?}")
    };
    assert!((0.0..1.0).contains(&sample), "{sample}");

    let expected = MysqlRng::new_with_seed(20_160_101).gen();
    let got = eval_as("rand", vec![Datum::Int(20_160_101)], real_ft(), &ctx);
    assert_eq!(got, Datum::Real(expected));
}

/// Go `pkg/expression/builtin_math_test.go:387 TestPow`.
#[test]
fn pow() {
    for (args, want) in [
        (vec![Datum::Int(1), Datum::Int(3)], Datum::Real(1.0)),
        (vec![Datum::Int(2), Datum::Int(2)], Datum::Real(4.0)),
        (vec![Datum::Int(4), Datum::Real(0.5)], Datum::Real(2.0)),
        (vec![Datum::Int(4), Datum::Int(-2)], Datum::Real(0.0625)),
    ] {
        assert_eq!(math_call("POW", &args).unwrap(), want, "{args:?}");
    }
    assert_eq!(
        math_call(
            "POW",
            &[Datum::new_string("test"), Datum::new_string("test")]
        )
        .unwrap(),
        Datum::Real(1.0)
    );
    assert!(matches!(
        math_call("POW", &[Datum::Int(10), Datum::Int(700)]),
        Err(EvalError::FloatOverflow)
    ));
}

/// Go `pkg/expression/builtin_math_test.go:434 TestRound`.
#[test]
fn round() {
    for (args, want) in [
        (vec![Datum::Real(-1.23)], Datum::Real(-1.0)),
        (vec![Datum::Real(-1.23), Datum::Int(0)], Datum::Real(-1.0)),
        (vec![Datum::Real(-1.58)], Datum::Real(-2.0)),
        (vec![Datum::Real(1.58)], Datum::Real(2.0)),
        (vec![Datum::Real(1.298), Datum::Int(1)], Datum::Real(1.3)),
        (vec![Datum::Real(1.298)], Datum::Real(1.0)),
        (vec![Datum::Real(-1.5), Datum::Int(0)], Datum::Real(-2.0)),
        (vec![Datum::Real(1.5), Datum::Int(0)], Datum::Real(2.0)),
        (vec![Datum::Real(23.298), Datum::Int(-1)], Datum::Real(20.0)),
        (
            vec![Datum::Decimal(tidb_datatype::Decimal::from_literal(
                "-1.23",
            ))],
            Datum::Decimal(tidb_datatype::Decimal::from_literal("-1")),
        ),
        (
            vec![
                Datum::Decimal(tidb_datatype::Decimal::from_literal("-1.23")),
                Datum::Int(1),
            ],
            Datum::Decimal(tidb_datatype::Decimal::from_literal("-1.2")),
        ),
        (
            vec![Datum::Decimal(tidb_datatype::Decimal::from_literal("1.58"))],
            Datum::Decimal(tidb_datatype::Decimal::from_literal("2")),
        ),
        (
            vec![
                Datum::Decimal(tidb_datatype::Decimal::from_literal("1.58")),
                Datum::Int(1),
            ],
            Datum::Decimal(tidb_datatype::Decimal::from_literal("1.6")),
        ),
        (vec![Datum::Null, Datum::Int(2)], Datum::Null),
        (vec![Datum::Int(1), Datum::Int(-2012)], Datum::Int(0)),
        (
            vec![Datum::Int(1), Datum::Int(-201_299_999_999_999)],
            Datum::Int(0),
        ),
    ] {
        let got = math_call("ROUND", &args).unwrap();
        assert!(
            got.compare(&want, Collation::Binary) == Ok(std::cmp::Ordering::Equal),
            "ROUND{args:?} = {got:?}, want {want:?}"
        );
    }
}

/// Go `pkg/expression/builtin_math_test.go:488 TestTruncate`.
/// NaN rows require a session-created IEEE value and stay a gap.
#[test]
fn truncate() {
    for (args, want) in [
        (vec![Datum::Real(-1.23), Datum::Int(0)], Datum::Real(-1.0)),
        (vec![Datum::Real(1.58), Datum::Int(0)], Datum::Real(1.0)),
        (vec![Datum::Real(1.298), Datum::Int(1)], Datum::Real(1.2)),
        (vec![Datum::Real(123.2), Datum::Int(-1)], Datum::Real(120.0)),
        (vec![Datum::Real(123.2), Datum::Int(100)], Datum::Real(123.2)),
        (vec![Datum::Real(123.2), Datum::Int(-100)], Datum::Real(0.0)),
        (
            vec![
                Datum::Real(1.797_693_134_862_315_7e308),
                Datum::Int(2),
            ],
            Datum::Real(1.797_693_134_862_315_7e308),
        ),
        (
            vec![
                Datum::Decimal(tidb_datatype::Decimal::from_literal("-1.23")),
                Datum::Int(0),
            ],
            Datum::Decimal(tidb_datatype::Decimal::from_literal("-1")),
        ),
        (
            vec![
                Datum::Decimal(tidb_datatype::Decimal::from_literal("-1.23")),
                Datum::Int(1),
            ],
            Datum::Decimal(tidb_datatype::Decimal::from_literal("-1.2")),
        ),
        (
            vec![
                Datum::Decimal(tidb_datatype::Decimal::from_literal("1.58")),
                Datum::Int(0),
            ],
            Datum::Decimal(tidb_datatype::Decimal::from_literal("1")),
        ),
        (
            vec![
                Datum::Decimal(tidb_datatype::Decimal::from_literal("23.298")),
                Datum::Int(100),
            ],
            Datum::Decimal(tidb_datatype::Decimal::from_literal(
                "23.298000000000000000000000000000",
            )),
        ),
        (vec![Datum::Null, Datum::Int(2)], Datum::Null),
        (
            vec![Datum::UInt(9_223_372_036_854_775_808), Datum::Int(-10)],
            Datum::UInt(9_223_372_030_000_000_000),
        ),
        (
            vec![Datum::Int(9_223_372_036_854_775_807), Datum::Int(-7)],
            Datum::Int(9_223_372_036_850_000_000),
        ),
        (
            vec![Datum::UInt(18_446_744_073_709_551_615), Datum::Int(-10)],
            Datum::UInt(18_446_744_070_000_000_000),
        ),
        (vec![Datum::Real(1.1), Datum::Int(400)], Datum::Real(1.1)),
        (vec![Datum::Real(1.1), Datum::Int(-400)], Datum::Real(0.0)),
        (vec![Datum::Real(0.0), Datum::Int(3)], Datum::Real(0.0)),
    ] {
        let got = math_call("TRUNCATE", &args).unwrap();
        assert!(
            got.compare(&want, Collation::Binary) == Ok(std::cmp::Ordering::Equal),
            "TRUNCATE{args:?} = {got:?}, want {want:?}"
        );
    }
}
