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

//! Tests for the JSON builtin family (`super::dispatch`), split out of the
//! family's `mod.rs` purely for file size; every assertion, Go citation, and
//! doc comment is unchanged from its prior home inside `mod tests` there.

use super::dispatch;
use super::path::parse_path;
use super::text::format_json;
use crate::Datum;
use serde_json::json;

fn call(name: &str, vals: &[Datum]) -> Datum {
    dispatch(name, vals)
        .expect("JSON family should own name/arity")
        .expect("Go-derived valid vector should evaluate")
}

fn s(value: &str) -> Datum {
    Datum::new_string(value.to_string())
}

fn call_result(name: &str, vals: &[Datum]) -> Result<Datum, crate::EvalError> {
    dispatch(name, vals).expect("JSON family should own name/arity")
}

/// Vectors from `TestJSONType` and `TestJSONQuote` in
/// `pkg/expression/builtin_json_test.go`.  `TestJSONValid` has its own
/// source-shaped table below so its signature boundary remains visible.
#[test]
fn scalar_go_vectors() {
    assert_eq!(call("JSON_TYPE", &[Datum::Null]), Datum::Null);
    for (input, want) in [
        ("3", "INTEGER"),
        ("3.0", "DOUBLE"),
        ("null", "NULL"),
        ("true", "BOOLEAN"),
        ("[]", "ARRAY"),
        ("{}", "OBJECT"),
    ] {
        assert_eq!(call("JSON_TYPE", &[s(input)]), s(want));
    }
}

#[test]
fn json_scalar_source_wave_boundaries() {
    // Keep the four source-owned scalar leaves' non-text and byte
    // boundaries explicit without pretending this value domain carries
    // typed BinaryJSON or charset/session warning state.
    assert!(call_result("JSON_TYPE", &[Datum::Int(3)]).is_err());
    assert!(call_result("JSON_TYPE", &[s("a")]).is_err());
    assert!(dispatch("JSON_TYPE", &[]).is_none());

    assert_eq!(call("JSON_QUOTE", &[Datum::Null]), Datum::Null);
    assert!(call_result("JSON_QUOTE", &[Datum::new_bytes([0xff])]).is_err());

    assert_eq!(call("JSON_UNQUOTE", &[Datum::Null]), Datum::Null);
    assert!(call_result("JSON_UNQUOTE", &[Datum::Int(3)]).is_err());
    assert!(call_result("JSON_UNQUOTE", &[Datum::new_bytes([0xff])]).is_err());

    assert_eq!(
        call("JSON_VALID", &[Datum::new_bytes([0xff])]),
        Datum::Int(0)
    );
    assert_eq!(call("JSON_VALID", &[Datum::Int(3)]), Datum::Int(0));
}

/// Source-shaped scalar rows from `TestJSONArray` and `TestJSONObject` in
/// `pkg/expression/builtin_json_test.go:379` and `:404`.  Strings remain
/// JSON strings, numeric/NULL values retain their scalar kinds, odd object
/// arity and NULL keys are errors, and the typed boolean/JSON rows remain
/// explicit value-domain boundaries.
#[test]
fn json_array_object_go_vectors() {
    assert_eq!(call("JSON_ARRAY", &[]), s("[]"));
    assert_eq!(call("JSON_ARRAY", &[Datum::Int(1)]), s("[1]"));
    assert_eq!(
        call(
            "JSON_ARRAY",
            &[Datum::Null, s("a"), Datum::Int(3), s(r#"{"a": "b"}"#),],
        ),
        s(r#"[null, "a", 3, "{\"a\": \"b\"}"]"#),
    );
    assert_eq!(call("JSON_ARRAY", &[Datum::UInt(2)]), s("[2]"));
    assert_eq!(call("JSON_ARRAY", &[Datum::Real(1.5)]), s("[1.5]"));
    // A `Datum::Bytes` is the chunk rewriter's spelling of the SAME SQL
    // string literal the row evaluator spells `Datum::String`, so it is
    // the JSON string "x" -- see [`json_sql_string`] for the named
    // binary-charset boundary this collapses.
    assert_eq!(
        call("JSON_ARRAY", &[Datum::new_bytes(b"x".to_vec())]),
        s(r#"["x"]"#)
    );

    assert_eq!(call("JSON_OBJECT", &[]), s("{}"));
    assert!(call_result(
        "JSON_OBJECT",
        &[Datum::Int(1), Datum::Int(2), Datum::Int(3)]
    )
    .is_err());
    assert_eq!(
        call(
            "JSON_OBJECT",
            &[Datum::Int(1), Datum::Int(2), s("hello"), Datum::Null],
        ),
        s(r#"{"1": 2, "hello": null}"#),
    );
    assert!(call_result("JSON_OBJECT", &[Datum::Null, Datum::Int(2)]).is_err());
    assert_eq!(
        call("JSON_OBJECT", &[Datum::new_bytes(b"k"), Datum::Int(2)]),
        s(r#"{"k": 2}"#),
    );
}

/// Complete representable table from `TestJSONValid` in
/// `pkg/expression/builtin_json_test.go:1176`.  The Go function class has
/// three signatures: typed JSON is already valid, strings are parsed as
/// JSON documents, and every other SQL type returns zero.  This frozen
/// evaluator has no typed BinaryJSON datum, so the string and "others"
/// rows are the complete executable value-domain table; the typed JSON
/// branch remains an explicit boundary rather than being guessed from a
/// numeric or string value.
#[test]
fn json_valid_go_vectors() {
    for (input, want) in [
        ("{\"a\":1}", 1),
        ("hello", 0),
        ("\"hello\"", 1),
        ("null", 1),
        ("{}", 1),
        ("[]", 1),
        ("2", 1),
        ("2.5", 1),
        ("2019-8-19", 0),
        ("\"2019-8-19\"", 1),
    ] {
        assert_eq!(call("JSON_VALID", &[s(input)]), Datum::Int(want));
    }
    assert_eq!(call("JSON_VALID", &[Datum::Int(2)]), Datum::Int(0));
    assert_eq!(call("JSON_VALID", &[Datum::Real(2.5)]), Datum::Int(0));
    assert_eq!(call("JSON_VALID", &[Datum::Null]), Datum::Null);
}

/// Complete source table from `TestJSONSchemaValid` in
/// `pkg/expression/builtin_json_test.go`.  The schema is a draft-2019-09
/// document because qri-io/jsonschema v0.2.1 loads that draft by default.
#[test]
fn json_schema_valid_go_vectors() {
    assert_eq!(
        call("JSON_SCHEMA_VALID", &[Datum::Null, s("{}")]),
        Datum::Null
    );
    assert_eq!(
        call("JSON_SCHEMA_VALID", &[s("{}"), Datum::Null]),
        Datum::Null
    );
    for (schema, document, want) in [
        ("{}", "{}", 1),
        (r#"{"required":["a","b"]}"#, r#"{"a":5}"#, 0),
        (r#"{"required":["a","b"]}"#, r#"{"a":5,"b":6}"#, 1),
        (r#"{"type":["string"]}"#, "{}", 0),
        (r#"{"type":["string"]}"#, r#""foobar""#, 1),
        (r#"{"type":["object"]}"#, "{}", 1),
        (r#"{"type":["object"]}"#, r#""foobar""#, 0),
        (r#"{"properties":{"a":{"type":"number"}}}"#, "{}", 1),
        (
            r#"{"properties":{"a":{"type":"number"}}}"#,
            r#"{"a":"foobar"}"#,
            0,
        ),
        (r#"{"properties":{"a":{"type":"number"}}}"#, r#"{"a":5}"#, 1),
        (
            r#"{"properties":{"a":{"type":"number","minimum":6}}}"#,
            r#"{"a":5}"#,
            0,
        ),
        (
            r#"{"properties":{"a":{"type":"string","pattern":"^a"}}}"#,
            r#"{"a":"abc"}"#,
            1,
        ),
        (
            r#"{"properties":{"a":{"type":"string","pattern":"^a"}}}"#,
            r#"{"a":"cba"}"#,
            0,
        ),
    ] {
        assert_eq!(
            call("JSON_SCHEMA_VALID", &[s(schema), s(document)]),
            Datum::Int(want),
            "schema={schema}, document={document}",
        );
    }

    // qri-io's Schema.UnmarshalJSON accepts the draft's boolean-schema form.
    assert_eq!(
        call("JSON_SCHEMA_VALID", &[s("true"), s("42")]),
        Datum::Int(1)
    );
    assert_eq!(
        call("JSON_SCHEMA_VALID", &[s("false"), s("42")]),
        Datum::Int(0)
    );
    assert_eq!(
        call(
            "JSON_SCHEMA_VALID",
            &[s(r#"{"format":"ipv4"}"#), s(r#""999.1.1.1""#)],
        ),
        Datum::Int(0),
    );

    let error = call_result("JSON_SCHEMA_VALID", &[s("[]"), s("{}")])
        .expect_err("a schema root must be an object or boolean");
    assert!(matches!(
        error,
        crate::EvalError::Json(crate::JsonError::InvalidJsonType {
            argument: 1,
            function: "json_schema_valid",
            ref required,
        }) if required == "object"
    ));

    let error = call_result("JSON_SCHEMA_VALID", &[s(r#"{"type":7}"#), s("{}")])
        .expect_err("a schema keyword with the wrong JSON kind must be rejected");
    assert!(matches!(
        error,
        crate::EvalError::Json(crate::JsonError::InvalidJsonType {
            argument: 1,
            function: "json_schema_valid",
            ..
        })
    ));

    assert!(matches!(
        call_result("JSON_SCHEMA_VALID", &[s("{}"), s("")]),
        Err(crate::EvalError::Json(crate::JsonError::EmptyText))
    ));
}

#[test]
fn json_schema_valid_resolves_file_and_http_references() {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::{Duration, Instant};

    let directory = tempfile::tempdir().expect("create schema directory");
    let missing_reference = format!(
        r#"{{"$ref":"file://{}"}}"#,
        directory.path().join("missing.json").display(),
    );
    assert_eq!(
        call("JSON_SCHEMA_VALID", &[s(&missing_reference), Datum::Null],),
        Datum::Null,
    );

    let schema_path = directory.path().join("integer.json");
    std::fs::write(&schema_path, r#"{"type":"integer"}"#).expect("write referenced file schema");
    let file_reference = format!(r#"{{"$ref":"file://{}"}}"#, schema_path.display());
    assert_eq!(
        call("JSON_SCHEMA_VALID", &[s(&file_reference), s("7")]),
        Datum::Int(1),
    );
    assert_eq!(
        call("JSON_SCHEMA_VALID", &[s(&file_reference), s(r#""seven""#)],),
        Datum::Int(0),
    );

    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind schema server");
    listener
        .set_nonblocking(true)
        .expect("make schema server bounded");
    let address = listener.local_addr().expect("read schema server address");
    let server = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(5);
        let (mut stream, _) = loop {
            match listener.accept() {
                Ok(connection) => break connection,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < deadline, "schema request did not arrive");
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("accept schema request: {error}"),
            }
        };
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("bound schema request read");
        let mut request = [0_u8; 1024];
        let read = stream.read(&mut request).expect("read schema request");
        assert!(std::str::from_utf8(&request[..read])
            .expect("HTTP request is text")
            .starts_with("GET /integer.json "),);
        let body = r#"{"type":"integer"}"#;
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len(),
        )
        .expect("write schema response");
    });
    let http_reference = format!(r#"{{"$ref":"http://{address}/integer.json"}}"#);
    assert_eq!(
        call("JSON_SCHEMA_VALID", &[s(&http_reference), s("7")]),
        Datum::Int(1),
    );
    server.join().expect("schema server completed");
}

/// All successful vectors in `TestJSONQuote` and `TestJSONUnquote`
/// (`pkg/expression/builtin_json_test.go`).  The two malformed-root
/// vectors are deliberately checked as errors rather than captured in
/// the result corpus (where `ERR` would be skipped by the harness).
#[test]
fn quote_and_unquote_go_vectors() {
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
        assert_eq!(call("JSON_QUOTE", &[s(input)]), s(want));
    }
    assert_eq!(call("JSON_QUOTE", &[Datum::Null]), Datum::Null);

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
        assert_eq!(call("JSON_UNQUOTE", &[s(input)]), s(want));
    }
    assert_eq!(call("JSON_UNQUOTE", &[Datum::Null]), Datum::Null);
    assert!(dispatch("JSON_UNQUOTE", &[s("\"\"a\"\"")])
        .expect("JSON_UNQUOTE is owned")
        .is_err());
}

/// Vectors from `TestJSONLength` and `TestBinaryJSONExtract` in TiDB's
/// expression/types test suites.
#[test]
fn containers_paths_and_binary_json_format_match_go() {
    assert_eq!(
        call("JSON_LENGTH", &[s("[1,2,[1,[5,[3]]]]"), s("$[2]")]),
        Datum::Int(2)
    );
    assert_eq!(
        call(
            "JSON_EXTRACT",
            &[
                s("{\"a\":[1,\"2\",{\"aa\":\"bb\"},4.0,{\"aa\":\"cc\"}]}"),
                s("$.a[*].aa")
            ]
        ),
        s("[\"bb\", \"cc\"]")
    );
    assert_eq!(
        call(
            "JSON_EXTRACT",
            &[s("[[0,1],[2,3],[4,[5,6]]]"), s("$[1 to last][1 to last]")]
        ),
        s("[3, [5, 6]]")
    );
    assert_eq!(
        format_json(&json!({"b": 2, "a": 1})),
        "{\"a\": 1, \"b\": 2}"
    );
    assert!(parse_path("$**").is_err());
    assert!(parse_path("$.宽").is_err());
    assert!(parse_path("$.\"宽\"").is_ok());
}

/// Complete representable table from `TestJSONExtract` in
/// `pkg/expression/builtin_json_test.go:233`.  The first row exercises
/// Go's early NULL propagation (the NULL document is returned before its
/// NULL path is parsed); the second row combines two exact paths into the
/// result array; the final row preserves invalid-path errors.  Typed
/// BinaryJSON, warning/session state, function-class construction, and
/// vectorized execution remain outside this scalar value domain.
#[test]
fn json_extract_go_vectors() {
    assert_eq!(
        call("JSON_EXTRACT", &[Datum::Null, Datum::Null]),
        Datum::Null
    );
    let document = r#"{"a": [{"aa": [{"aaa": 1}]}], "aaa": 2}"#;
    assert_eq!(
        call(
            "JSON_EXTRACT",
            &[s(document), s("$.a[0].aa[0].aaa"), s("$.aaa")]
        ),
        s("[1, 2]")
    );
    assert!(call_result(
        "JSON_EXTRACT",
        &[s(document), s("$.a[0].aa[0].aaa"), s("$InvalidPath")]
    )
    .is_err());
}

/// The representable rows from `TestJSONMemberOf` in
/// `pkg/expression/builtin_json_test.go`.  A candidate SQL string is a
/// JSON string (the Go signature disables ParseToJSONFlag for arg 0),
/// which is why `"1" MEMBER OF [1]` differs from `"1" MEMBER OF ["1"]`.
#[test]
fn json_member_of_go_vectors() {
    for (candidate, document, want) in [
        ("1", "[1, 2]", 0),
        ("1", "[1]", 0),
        ("1", "[0]", 0),
        ("1", "[[1]]", 0),
        ("1", "[\"1\"]", 1),
        (r#"{"a":1}"#, r#"{"a":1}"#, 0),
        (r#"{"a":1}"#, r#"[{"a":1}]"#, 0),
        (r#"{"a":1}"#, r#"[{"a":1},1]"#, 0),
        (r#"{"a":1}"#, r#"["{\"a\":1}"]"#, 1),
        (r#"{"a":1}"#, r#"["{\"a\":1}",1]"#, 1),
    ] {
        assert_eq!(
            call("JSON_MEMBER_OF", &[s(candidate), s(document)],),
            Datum::Int(want),
            "JSON_MEMBER_OF({candidate:?}, {document:?})"
        );
    }
    for (document, want) in [
        ("[1, 2]", 1),
        ("[1]", 1),
        ("[0]", 0),
        ("[1]", 1),
        ("[[1]]", 0),
    ] {
        assert_eq!(
            call("JSON_MEMBER_OF", &[Datum::Int(1), s(document)]),
            Datum::Int(want),
            "JSON_MEMBER_OF(1, {document:?})"
        );
    }
    assert_eq!(
        call("JSON_MEMBER_OF", &[Datum::Null, s("[1]")]),
        Datum::Null
    );
    assert!(call_result("JSON_MEMBER_OF", &[s("1"), s("a:1")]).is_err());
    assert_eq!(
        call("JSON_MEMBER_OF", &[Datum::Int(1), s("[1]")]),
        Datum::Int(1)
    );
}

/// Representable rows from `TestJSONContains`, including recursive
/// object/array containment, scalar equality, path extraction, NULL and
/// missing-path propagation, and wildcard rejection.
#[test]
fn json_contains_go_vectors() {
    for (document, candidate, want) in [
        ("[1,2,[1,[5,[3]]]]", "[1,3]", 1),
        ("[1,2,[1,[5,{\"a\":[2,3]}]]]", "[1,{\"a\":[3]}]", 1),
        ("[ {\"a\":1} ]", "{\"a\":1}", 1),
        ("{}", "{}", 1),
        ("{\"a\":1}", "{}", 1),
        ("{\"a\":1}", "1", 0),
        ("{\"a\":[1]}", "[1]", 0),
        ("{\"b\":2,\"c\":3}", "{\"c\":3}", 1),
        ("1", "1", 1),
        ("[1]", "1", 1),
        ("[1,2]", "[1]", 1),
        ("[1,2]", "[1,3]", 0),
        ("[1,2]", "[\"1\"]", 0),
        ("[1,2,[1,3]]", "[1,3]", 1),
        ("[1,2,[1,[5,[3]]]]", "[1,3]", 1),
        ("[{\"a\":1,\"b\":2}]", "{\"a\":1}", 1),
        ("[{\"a\":{\"a\":1},\"b\":2}]", "{\"a\":1}", 0),
    ] {
        assert_eq!(
            call("JSON_CONTAINS", &[s(document), s(candidate)]),
            Datum::Int(want),
            "JSON_CONTAINS({document:?}, {candidate:?})"
        );
    }
    for (document, candidate, path, want) in [
        ("[1,2,[1,[5,[3]]]]", "[1,3]", "$[2]", 1),
        ("[1,2,[1,[5,{\"a\":[2,3]}]]]", "[1,{\"a\":[3]}]", "$[2]", 1),
        ("[{\"a\":1}]", "{\"a\":1}", "$", 1),
        ("[{\"a\":1,\"b\":2}]", "{\"a\":1}", "$", 1),
    ] {
        assert_eq!(
            call("JSON_CONTAINS", &[s(document), s(candidate), s(path)]),
            Datum::Int(want),
            "JSON_CONTAINS({document:?}, {candidate:?}, {path:?})"
        );
    }
    assert_eq!(
        call("JSON_CONTAINS", &[s("{\"a\":1}"), s("1"), s("$.c")]),
        Datum::Null
    );
    // The Go test records this row as expected integer 0 but does not
    // assert non-NULL; the source evaluator returns NULL for a path that
    // misses the array root, which is the actual `Extract` contract.
    assert_eq!(
        call(
            "JSON_CONTAINS",
            &[s("[{\"a\":{\"a\":1},\"b\":2}]"), s("{\"a\":1}"), s("$.a"),],
        ),
        Datum::Null
    );
    assert_eq!(call("JSON_CONTAINS", &[Datum::Null, s("1")]), Datum::Null);
    for path in ["$.*", "$[*]", "$**.a"] {
        assert!(call_result("JSON_CONTAINS", &[s("{\"a\":1}"), s("1"), s(path)]).is_err());
    }
    assert!(call_result("JSON_CONTAINS", &[s("a:1"), s("1")]).is_err());
}

#[test]
fn json_contains_source_wave_missing_rows() {
    // Exact NULL/missing-path/invalid-argument rows from TestJSONContains
    // that are separate from the main recursive containment table above.
    assert_eq!(
        call("JSON_CONTAINS", &[Datum::Null, s("1"), s("$.c")],),
        Datum::Null
    );
    assert_eq!(
        call(
            "JSON_CONTAINS",
            &[s(r#"{"a":[1,2,{"aa":"xx"}]}"#), Datum::Null, s("$.a[3]")],
        ),
        Datum::Null
    );
    assert_eq!(
        call(
            "JSON_CONTAINS",
            &[s(r#"{"a":[1,2,{"aa":"xx"}]}"#), s("1"), Datum::Null],
        ),
        Datum::Null
    );
    for (document, path) in [
        (r#"{"a":[1,2,{"aa":"xx"}]}"#, "$.c"),
        (r#"{"a":[1,2,{"aa":"xx"}]}"#, "$.a[3]"),
        (r#"{"a":[1,2,{"aa":"xx"}]}"#, "$.a[2].b"),
    ] {
        assert_eq!(
            call("JSON_CONTAINS", &[s(document), s("1"), s(path)]),
            Datum::Null,
            "JSON_CONTAINS({document:?}, 1, {path:?})",
        );
    }
    assert!(call_result("JSON_CONTAINS", &[s("[1,2,[1,3]]"), s("a:1")]).is_err());
    for args in [
        vec![Datum::Int(1), s("")],
        vec![Datum::Real(0.05), s("")],
        vec![s(""), Datum::Int(1)],
        vec![s(""), Datum::Real(0.05)],
    ] {
        assert!(call_result("JSON_CONTAINS", &args).is_err());
    }
}

/// Representable rows from `TestJSONOverlaps`, preserving the Go rule that
/// arrays compare their scalar/element values while objects overlap only
/// when a shared key has an equal value.
#[test]
fn json_overlaps_go_vectors() {
    for (left, right, want) in [
        ("[1,2]", "[2,3]", 1),
        ("[1,2]", "[2]", 1),
        ("[1,2]", "2", 1),
        ("[{\"a\":1}]", "{\"a\":1}", 1),
        ("[{\"a\":1}]", "{\"a\":2}", 0),
        ("{\"a\":[1,2]}", "{\"a\":[1]}", 0),
        ("[1,1,1]", "1", 1),
        ("1", "1", 1),
        ("0", "1", 0),
        ("[[1,2],3]", "[1,3]", 1),
        ("[4,5,\"6\",7]", "6", 0),
        ("[4,5,6,7]", "\"6\"", 0),
        ("[2,3]", "[1,2]", 1),
        ("2", "[1,2]", 1),
        ("{\"a\":1}", "[{\"a\":1}]", 1),
        ("{\"a\":1,\"b\":2}", "[{\"a\":1}]", 0),
        ("{\"a\":2}", "[{\"a\":1}]", 0),
        ("{\"a\":[1]}", "{\"a\":[1,2]}", 0),
        ("1", "[1,1,1]", 1),
        ("[1,[2,3]]", "[[1,2],3]", 0),
        ("[1,3]", "[[1,2],3]", 1),
        (
            "{\"a\":5,\"e\":10,\"f\":1,\"d\":20}",
            "{\"a\":1,\"b\":10,\"d\":10}",
            0,
        ),
        ("6", "[4,5,\"6\",7]", 0),
        ("\"6\"", "[4,5,6,7]", 0),
    ] {
        assert_eq!(
            call("JSON_OVERLAPS", &[s(left), s(right)]),
            Datum::Int(want),
            "JSON_OVERLAPS({left:?}, {right:?})"
        );
    }
    assert_eq!(call("JSON_OVERLAPS", &[Datum::Null, s("1")]), Datum::Null);
    assert!(call_result("JSON_OVERLAPS", &[s("a:1"), s("1")]).is_err());
}

/// Complete representable table from `TestJSONContainsPath` in
/// `pkg/expression/builtin_json_test.go`.  The path-existence operation
/// deliberately uses the same extraction walk as JSON_EXTRACT, so object
/// and array wildcards, one/all short-circuiting, NULL propagation, and
/// invalid-document/path errors remain observable in this scalar domain.
#[test]
fn json_contains_path_go_vectors() {
    let document = r#"{"a": 1, "b": 2, "c": {"d": 4}}"#;
    for (mode, path, want) in [
        ("one", "$.c.d", 1),
        ("one", "$.a.d", 0),
        ("all", "$.c.d", 1),
        ("all", "$.a.d", 0),
        ("one", "$.*", 1),
        ("one", "$[*]", 0),
        ("all", "$.*", 1),
        ("all", "$[*]", 0),
        ("ONE", "$.c.d", 1),
        ("ALL", "$.c.d", 1),
    ] {
        assert_eq!(
            call("JSON_CONTAINS_PATH", &[s(document), s(mode), s(path)]),
            Datum::Int(want),
            "JSON_CONTAINS_PATH({mode:?}, {path:?})",
        );
    }
    for (mode, paths, want) in [
        ("one", ["$.a", "$.e"].as_slice(), 1),
        ("one", ["$.a", "$.c"].as_slice(), 1),
        ("all", ["$.a", "$.e"].as_slice(), 0),
        ("all", ["$.a", "$.c"].as_slice(), 1),
        ("One", ["$.a", "$.e"].as_slice(), 1),
        ("aLl", ["$.a", "$.e"].as_slice(), 0),
    ] {
        let mut args = vec![s(document), s(mode)];
        args.extend(paths.iter().map(|path| s(path)));
        assert_eq!(
            call("JSON_CONTAINS_PATH", &args),
            Datum::Int(want),
            "JSON_CONTAINS_PATH({mode:?}, {paths:?})",
        );
    }

    for args in [
        vec![Datum::Null, s("one"), s("$.c")],
        vec![Datum::Null, s("all"), s("$.c")],
        vec![s(document), Datum::Null, s("$.a[3]")],
        vec![s(document), s("one"), Datum::Null],
        vec![s(document), s("all"), Datum::Null],
    ] {
        assert_eq!(
            call("JSON_CONTAINS_PATH", &args),
            Datum::Null,
            "JSON_CONTAINS_PATH NULL vector",
        );
    }

    for args in [
        vec![s(r#"{"a": 1"#), s("one"), s("$.a")],
        vec![s(r#"{"a": 1"#), s("all"), s("$.a")],
        vec![s(document), s("test"), s("$.a")],
    ] {
        assert!(
            call_result("JSON_CONTAINS_PATH", &args).is_err(),
            "JSON_CONTAINS_PATH invalid vector",
        );
    }
}

/// Complete representable table from `TestJSONKeys` in
/// `pkg/expression/builtin_json_test.go`.  JSON keys are returned only
/// for object roots or exact object path selections; scalar/array/missing
/// targets are NULL and wildcard/range paths reject multiple selection.
#[test]
fn json_keys_go_vectors() {
    for args in [
        vec![Datum::Null],
        vec![Datum::Null, s("$.c")],
        vec![s(r#"{"a": 1}"#), Datum::Null],
        vec![Datum::Null, Datum::Null],
    ] {
        assert_eq!(call("JSON_KEYS", &args), Datum::Null);
    }
    for document in ["1", r#""str""#, "true", "null", "[1, 2]", r#"["1", "2"]"#] {
        assert_eq!(call("JSON_KEYS", &[s(document)]), Datum::Null);
    }
    for (document, want) in [
        ("{}", "[]"),
        (r#"{"a": 1}"#, r#"["a"]"#),
        (r#"{"a": 1, "b": 2}"#, r#"["a", "b"]"#),
        (r#"{"a": {"c": 3}, "b": 2}"#, r#"["a", "b"]"#),
    ] {
        assert_eq!(call("JSON_KEYS", &[s(document)]), s(want));
    }
    for (document, path, want) in [
        (r#"{"a": 1}"#, "$.a", None),
        (r#"{"a": {"c": 3}, "b": 2}"#, "$.a", Some(r#"["c"]"#)),
        (r#"{"a": {"c": 3}, "b": 2}"#, "$.a.c", None),
        (r#"{"a": 1}"#, "$.b", None),
        (r#"{"a": {"c": 3}, "b": 2}"#, "$.c", None),
        (r#"{"a": {"c": 3}, "b": 2}"#, "$.a.d", None),
        (
            r#"[{"A1": 1, "B1": 2, "C1": 3}, {"A2": 10, "B2": 20, "C2": {"D": 4}}, {"A3": 1, "B3": 2, "C3": 6}]"#,
            "$[1]",
            Some(r#"["A2", "B2", "C2"]"#),
        ),
        (
            r#"[{"A": 1, "B": 2, "C": {"D": 3}}, {"A": 10, "B": 20, "C": {"D": 4}}, {"A": 1, "B": 2, "C": [{"D": 5}, {"E": 55}]}]"#,
            "$[last].C",
            None,
        ),
        (
            r#"[{"A": 1, "B": 2, "C": {"D": 3}}, {"A": 10, "B": 20, "C": {"D": 4}}, {"A": 1, "B": 2, "C": [{"D": 5}, {"E": 55}]}]"#,
            "$[last].C[1]",
            Some(r#"["E"]"#),
        ),
    ] {
        let result = call("JSON_KEYS", &[s(document), s(path)]);
        match want {
            Some(want) => assert_eq!(result, s(want), "JSON_KEYS path {path:?}"),
            None => assert_eq!(result, Datum::Null, "JSON_KEYS path {path:?}"),
        }
    }
    for (document, path) in [
        ("{}", "$.*"),
        (r#"{"a": 1}"#, "$.*"),
        (r#"{"a": {"c": 3}, "b": 2}"#, "$.*"),
        (r#"{"a": {"c": 3}, "b": 2}"#, "$.a.*"),
        (r#"{"a": {"c": 3}, "b": 2}"#, "$[0 to 1]"),
    ] {
        assert!(
            call_result("JSON_KEYS", &[s(document), s(path)]).is_err(),
            "JSON_KEYS wildcard/range path {path:?} must reject multiple selection"
        );
    }
}

/// Complete representable table from `TestJSONRemove` in
/// `pkg/expression/builtin_json_test.go`.  Exact paths are applied in
/// order, preserving the Go behavior where later array indexes observe
/// earlier removals; absent paths are no-ops and wildcard/range/root
/// paths are errors.
#[test]
fn json_remove_go_vectors() {
    for path in ["$", "$.*", "$[*]", "$**.a"] {
        assert!(
            call_result(
                "JSON_REMOVE",
                &[s(r#"{"a": [1, 2, {"aa": "xx"}]}"#), s(path)]
            )
            .is_err(),
            "JSON_REMOVE invalid path {path:?}"
        );
    }
    assert_eq!(call("JSON_REMOVE", &[Datum::Null, s("$.a")]), Datum::Null);
    assert_eq!(
        call(
            "JSON_REMOVE",
            &[s(r#"{"a": [1, 2, {"aa": "xx"}]}"#), s("$.a[2].aa")]
        ),
        s(r#"{"a": [1, 2, {}]}"#)
    );
    assert_eq!(
        call(
            "JSON_REMOVE",
            &[s(r#"{"a": [1, 2, {"aa": "xx"}]}"#), s("$.a[1]")]
        ),
        s(r#"{"a": [1, {"aa": "xx"}]}"#)
    );
    for (paths, want) in [
        (vec!["$.a[2].aa", "$.a[1]"], r#"{"a": [1, {}]}"#),
        (vec!["$.a[1]", "$.a[1].aa"], r#"{"a": [1, {}]}"#),
        (vec!["$.a[3]"], r#"{"a": [1, 2, {"aa": "xx"}]}"#),
        (vec!["$.b"], r#"{"a": [1, 2, {"aa": "xx"}]}"#),
        (vec!["$.a[3]", "$.b"], r#"{"a": [1, 2, {"aa": "xx"}]}"#),
    ] {
        let mut args = vec![s(r#"{"a": [1, 2, {"aa": "xx"}]}"#)];
        args.extend(paths.into_iter().map(s));
        assert_eq!(call("JSON_REMOVE", &args), s(want), "paths {args:?}");
    }
    assert_eq!(
        call("JSON_REMOVE", &[s(r#"{"a": 1}"#), Datum::Null]),
        Datum::Null
    );
}

/// Complete scalar-value-domain table from `TestJSONArrayAppend` in
/// `pkg/expression/builtin_json_test.go:949`.  SQL strings remain JSON
/// strings because the Go signature disables ParseToJSONFlag for values;
/// the two source rows using an already-typed `sampleJSON` object are
/// intentionally left as a typed-BinaryJSON boundary.
#[allow(clippy::approx_constant)]
#[test]
fn json_array_append_go_vectors() {
    for (args, want) in [
        (
            vec![s(r#"{"a": 1, "b": [2, 3], "c": 4}"#), s("$.d"), s("z")],
            r#"{"a": 1, "b": [2, 3], "c": 4}"#,
        ),
        (
            vec![s(r#"{"a": 1, "b": [2, 3], "c": 4}"#), s("$"), s("w")],
            r#"[{"a": 1, "b": [2, 3], "c": 4}, "w"]"#,
        ),
        (
            vec![s(r#"{"a": 1, "b": [2, 3], "c": 4}"#), s("$"), Datum::Null],
            r#"[{"a": 1, "b": [2, 3], "c": 4}, null]"#,
        ),
        (
            vec![s(r#"{"a": 1}"#), s("$"), s(r#"{"b": 2}"#)],
            r#"[{"a": 1}, "{\"b\": 2}"]"#,
        ),
        (
            vec![s(r#"{"a": 1}"#), s("$.a"), s(r#"{"b": 2}"#)],
            r#"{"a": [1, "{\"b\": 2}"]}"#,
        ),
        (
            vec![s(r#"{"a": 1}"#), s("$.a"), s("x"), s("$.a[1]"), s("y")],
            r#"{"a": [1, ["x", "y"]]}"#,
        ),
        (vec![s(r#"null"#), s("$"), Datum::Null], r#"[null, null]"#),
        (vec![s("[]"), s("$"), Datum::Null], r#"[null]"#),
        (vec![s("{}"), s("$"), Datum::Null], r#"[{}, null]"#),
        (
            vec![s(r#"["a", ["b", "c"], "d"]"#), s("$[1]"), Datum::Int(1)],
            r#"["a", ["b", "c", 1], "d"]"#,
        ),
        (
            vec![s(r#"["a", ["b", "c"], "d"]"#), s("$[0]"), Datum::Int(2)],
            r#"[["a", 2], ["b", "c"], "d"]"#,
        ),
        (
            vec![s(r#"["a", ["b", "c"], "d"]"#), s("$[1][0]"), Datum::Int(3)],
            r#"["a", [["b", 3], "c"], "d"]"#,
        ),
        (
            vec![s(r#"{"a": 1, "b": [2, 3], "c": 4}"#), s("$.b"), s("x")],
            r#"{"a": 1, "b": [2, 3, "x"], "c": 4}"#,
        ),
        (
            vec![s(r#"{"a": 1, "b": [2, 3], "c": 4}"#), s("$.c"), s("y")],
            r#"{"a": 1, "b": [2, 3], "c": [4, "y"]}"#,
        ),
        (
            vec![s(r#"[1,2,3, {"a":[4,5,6]}]"#), s("$"), Datum::Int(7)],
            r#"[1, 2, 3, {"a": [4, 5, 6]}, 7]"#,
        ),
        (
            vec![
                s(r#"[1,2,3, {"a":[4,5,6]}]"#),
                s("$"),
                Datum::Int(7),
                s("$[3].a"),
                Datum::Real(3.14),
            ],
            r#"[1, 2, 3, {"a": [4, 5, 6, 3.14]}, 7]"#,
        ),
        (
            vec![
                s(r#"[1,2,3, {"a":[4,5,6]}]"#),
                s("$"),
                Datum::Int(7),
                s("$[3].b"),
                Datum::Int(8),
            ],
            r#"[1, 2, 3, {"a": [4, 5, 6]}, 7]"#,
        ),
    ] {
        assert_eq!(call("JSON_ARRAY_APPEND", &args), s(want), "args {args:?}");
    }
    for args in [
        vec![Datum::Null, s("$"), Datum::Null],
        vec![Datum::Null, s("$"), s("a")],
        vec![s(r#"{"a":1}"#), Datum::Null, s("x")],
    ] {
        assert_eq!(call("JSON_ARRAY_APPEND", &args), Datum::Null);
    }
    for args in [
        vec![s("asdf"), s("$"), Datum::Null],
        vec![s(""), s("$"), Datum::Null],
        vec![s(r#"{"a":1}"#), s("asdf"), Datum::Null],
        vec![s(r#"{"a":1}"#), Datum::Int(42), Datum::Null],
        vec![s(r#"{"a":1}"#), s("$.*"), Datum::Null],
    ] {
        assert!(call_result("JSON_ARRAY_APPEND", &args).is_err());
    }
    assert!(dispatch("JSON_ARRAY_APPEND", &[s(r#"{"a":1}"#), s("$")]).is_none());
    assert!(call_result(
        "JSON_ARRAY_APPEND",
        &[s(r#"{"a":1}"#), s("$"), s("x"), s("$.a")]
    )
    .is_err());
}

/// Complete scalar-value-domain table from `TestJSONArrayInsert` in
/// `pkg/expression/builtin_json_test.go:1103`.  Typed JSON value rows are
/// not guessed from SQL strings; the representable source rows preserve
/// string, integer, NULL, and nested-array insertion semantics exactly.
#[test]
fn json_array_insert_go_vectors() {
    for (args, want) in [
        (
            vec![s(r#"{"a": 1, "b": [2, 3], "c": 4}"#), s("$.b[1]"), s("z")],
            r#"{"a": 1, "b": [2, "z", 3], "c": 4}"#,
        ),
        (
            vec![s(r#"{"a": 1, "b": [2, 3], "c": 4}"#), s("$.a[1]"), s("z")],
            r#"{"a": 1, "b": [2, 3], "c": 4}"#,
        ),
        (
            vec![s(r#"{"a": 1, "b": [2, 3], "c": 4}"#), s("$.d[1]"), s("z")],
            r#"{"a": 1, "b": [2, 3], "c": 4}"#,
        ),
        (
            vec![s(r#"[{"a": 1}]"#), s("$[1]"), s("w")],
            r#"[{"a": 1}, "w"]"#,
        ),
        (
            vec![s(r#"[{"a": 1}]"#), s("$[0]"), Datum::Null],
            r#"[null, {"a": 1}]"#,
        ),
        (
            vec![s("[1, 2, 3]"), s("$[100]"), s(r#"{"b": 2}"#)],
            r#"[1, 2, 3, "{\"b\": 2}"]"#,
        ),
        (
            vec![s(r#"["a", {"b": [1, 2]}, [3, 4]]"#), s("$[1]"), s("x")],
            r#"["a", "x", {"b": [1, 2]}, [3, 4]]"#,
        ),
        (
            vec![s(r#"["a", {"b": [1, 2]}, [3, 4]]"#), s("$[1].b[0]"), s("x")],
            r#"["a", {"b": ["x", 1, 2]}, [3, 4]]"#,
        ),
        (
            vec![s(r#"["a", {"b": [1, 2]}, [3, 4]]"#), s("$[2][1]"), s("y")],
            r#"["a", {"b": [1, 2]}, [3, "y", 4]]"#,
        ),
        (
            vec![
                s(r#"["a", {"b": [1, 2]}, [3, 4]]"#),
                s("$[0]"),
                s("x"),
                s("$[2][1]"),
                s("y"),
            ],
            r#"["x", "a", {"b": [1, 2]}, [3, 4]]"#,
        ),
        (
            vec![
                s(r#"["a", {"b": [1, 2]}, [3, 4]]"#),
                s("$[0]"),
                s("x"),
                s("$[0]"),
                s("y"),
            ],
            r#"["y", "x", "a", {"b": [1, 2]}, [3, 4]]"#,
        ),
    ] {
        assert_eq!(call("JSON_ARRAY_INSERT", &args), s(want), "args {args:?}");
    }
    for args in [
        vec![Datum::Null, s("$"), Datum::Null],
        vec![Datum::Null, s("$"), s("a")],
        vec![s(r#"{"a": 1}"#), Datum::Null, Datum::Null],
        vec![s("[]"), s("$[0]"), Datum::Null],
        vec![s("{}"), s("$[0]"), Datum::Null],
    ] {
        let result = call_result("JSON_ARRAY_INSERT", &args);
        if args[0] == Datum::Null || args[1] == Datum::Null {
            assert_eq!(result, Ok(Datum::Null));
        } else if args[0] == s("[]") {
            assert_eq!(result, Ok(s("[null]")));
        } else {
            assert_eq!(result, Ok(s("{}")));
        }
    }
    for args in [
        vec![s("asdf"), s("$"), Datum::Null],
        vec![s(""), s("$"), Datum::Null],
        vec![s(r#"{"a":1}"#), s("asdf"), Datum::Null],
        vec![s(r#"{"a":1}"#), Datum::Int(42), Datum::Null],
        vec![s(r#"{"a":1}"#), s("$.*"), Datum::Null],
        vec![s(r#"{"a":1}"#), s("$.a"), Datum::Null],
    ] {
        assert!(call_result("JSON_ARRAY_INSERT", &args).is_err());
    }
    assert!(call_result(
        "JSON_ARRAY_INSERT",
        &[
            s(r#"{"a":1,"b":[2,3],"c":4}"#),
            s("$.b[0]"),
            Datum::Null,
            s("$.a"),
            Datum::Null,
        ]
    )
    .is_err());
    assert!(dispatch("JSON_ARRAY_INSERT", &[s(r#"{"a":1}"#), s("$")]).is_none());
    assert!(call_result(
        "JSON_ARRAY_INSERT",
        &[s(r#"{"a":1}"#), s("$[0]"), s("x"), s("$.a")]
    )
    .is_err());
    assert_eq!(
        call(
            "JSON_ARRAY_INSERT",
            &[s(r#"{"a":1}"#), s("$.a[0]"), Datum::Null]
        ),
        s(r#"{"a": 1}"#)
    );
}

/// Complete representable table from `TestJSONSearch` in
/// `pkg/expression/builtin_json_test.go:1028`.  JSON paths are returned as
/// JSON strings, matching `BinaryJSON.Search` rather than exposing the
/// evaluator's internal path representation.
#[test]
fn json_search_go_vectors() {
    let document = r#"["abc", [{"k": "10"}, "def"], {"x":"abc"}, {"y":"bcd"}]"#;
    let document2 = r#"["abc", [{"k": "10"}, "def"], {"x":"ab%d"}, {"y":"abcd"}]"#;
    let cases = [
        (&[document, "one", "abc"][..], r#""$[0]""#),
        (&[document, "all", "abc"][..], r#"["$[0]", "$[2].x"]"#),
        (&[document, "all", "ghi"][..], "null"),
        (&[document, "ALL", "ghi"][..], "null"),
        (&[document, "all", "10"][..], r#""$[1][0].k""#),
        (&[document, "all", "10", "", "$[0]"][..], "null"),
        (&[document, "all", "10", "", "$[*]"][..], r#""$[1][0].k""#),
        (&[document, "all", "10", "", "$**.k"][..], r#""$[1][0].k""#),
        (
            &[document, "all", "10", "", "$[*][0].k"][..],
            r#""$[1][0].k""#,
        ),
        (&[document, "all", "10", "", "$[1]"][..], r#""$[1][0].k""#),
        (
            &[document, "all", "10", "", "$[1][0]"][..],
            r#""$[1][0].k""#,
        ),
        (&[document, "all", "abc", "", "$[2]"][..], r#""$[2].x""#),
        (
            &[document, "all", "abc", "", "$[2]", "$[0]"][..],
            r#"["$[2].x", "$[0]"]"#,
        ),
        (
            &[document, "all", "abc", "", "$[2]", "$[2]"][..],
            r#""$[2].x""#,
        ),
        (&[document, "all", "%a%"][..], r#"["$[0]", "$[2].x"]"#),
        (
            &[document, "all", "%b%"][..],
            r#"["$[0]", "$[2].x", "$[3].y"]"#,
        ),
        (&[document, "all", "%b%", "", "$[0]"][..], r#""$[0]""#),
        (&[document, "all", "%b%", "", "$[2]"][..], r#""$[2].x""#),
        (&[document, "all", "%b%", "", "$[1]"][..], "null"),
        (&[document, "all", "%b%", "", "$[3]"][..], r#""$[3].y""#),
        (&[document2, "all", "ab_d"][..], r#"["$[2].x", "$[3].y"]"#),
        (&[document2, "all", "ab%d"][..], r#"["$[2].x", "$[3].y"]"#),
        (&[document2, "all", r"ab\%d"][..], r#""$[2].x""#),
        (&[document2, "all", "ab|%d", "|"][..], r#""$[2].x""#),
    ];
    for (args, expected) in cases {
        let datums = args.iter().map(|value| s(value)).collect::<Vec<_>>();
        let actual = call("JSON_SEARCH", &datums);
        if expected == "null" {
            assert_eq!(actual, Datum::Null, "args={args:?}");
        } else {
            assert_eq!(actual, s(expected), "args={args:?}");
        }
    }
    assert_eq!(
        call("JSON_SEARCH", &[Datum::Null, s("all"), s("abc")]),
        Datum::Null
    );
    assert!(call_result("JSON_SEARCH", &[s("a"), s("all"), s("abc")]).is_err());
    assert!(call_result("JSON_SEARCH", &[s(document), s("wrong"), s("abc")]).is_err());
    assert_eq!(
        call("JSON_SEARCH", &[s(document), Datum::Null, s("abc")]),
        Datum::Null
    );
    assert_eq!(
        call("JSON_SEARCH", &[s(document), s("all"), Datum::Null]),
        Datum::Null
    );
    assert!(call_result("JSON_SEARCH", &[s(document), s("all"), s("abc"), s("??")]).is_err());
    assert_eq!(
        call(
            "JSON_SEARCH",
            &[s(document), s("all"), s("abc"), s(""), Datum::Null]
        ),
        Datum::Null
    );
    assert!(call_result(
        "JSON_SEARCH",
        &[s(document), s("all"), s("abc"), s(""), s("$xx")]
    )
    .is_err());
}

/// The two ways `JSON_SEARCH`'s walk differs from `JSON_EXTRACT`'s, both of
/// which this evaluator got wrong by reusing the extraction rules.
///
/// Go runs `extractToCallback`, not `extractTo`. It enters its
/// array-selection branch only `&& bj.TypeCode == JSONTypeCodeArray` ("NOTICE:
/// path [0] & [*] for JSON object other than array is INVALID, which is
/// different from extractTo"), and `Walk` carries a `pathSet` that refuses to
/// visit any full path twice for the whole walk.
#[test]
fn json_search_walk_is_not_the_extract_walk() {
    // Captured from this tree's TiDB (`goeval`): both answer NULL, where
    // `JSON_EXTRACT('{"a":"foo"}', '$[0].a')` selects "foo".
    assert_eq!(
        call(
            "JSON_SEARCH",
            &[
                s(r#"{"a":"foo"}"#),
                s("all"),
                s("foo"),
                Datum::Null,
                s("$[0].a")
            ]
        ),
        Datum::Null
    );
    assert_eq!(
        call(
            "JSON_SEARCH",
            &[
                s(r#"{"a":"foo"}"#),
                s("all"),
                s("foo"),
                Datum::Null,
                s("$[0 to 1].a")
            ]
        ),
        Datum::Null
    );
    // An array-selection leg still applies to a real array.
    assert_eq!(
        call(
            "JSON_SEARCH",
            &[
                s(r#"[{"a":"foo"}]"#),
                s("all"),
                s("foo"),
                Datum::Null,
                s("$[0].a")
            ]
        ),
        s(r#""$[0].a""#)
    );

    // `$**.a` reaches `$.a.a` twice -- once descending from the root and once
    // recursing into `$.a` -- with `$.a.b` collected in between, so the
    // repeat is NOT adjacent. Go's pathSet reports it once.
    assert_eq!(
        call(
            "JSON_SEARCH",
            &[
                s(r#"{"a":{"b":"x","a":"x"}}"#),
                s("all"),
                s("x"),
                Datum::Null,
                s("$**.a")
            ]
        ),
        s(r#"["$.a.a", "$.a.b"]"#)
    );
    // Two path arguments naming the same leaf, with a third path between
    // them: again a non-adjacent repeat.
    assert_eq!(
        call(
            "JSON_SEARCH",
            &[
                s(r#"{"a":"x","b":"x"}"#),
                s("all"),
                s("x"),
                Datum::Null,
                s("$.a"),
                s("$.b"),
                s("$.a")
            ]
        ),
        s(r#"["$.a", "$.b"]"#)
    );
}

/// `one_or_all` is a USER error, not an evaluator gap, and Go raises a
/// different code per function: 3154 `ErrJSONBadOneOrAllArg` from
/// `builtinJSONContainsPathSig.evalInt`, 3150
/// `ErrInvalidJSONContainsPathType` from `builtinJSONSearchSig.evalJSON`.
#[test]
fn json_one_or_all_argument_errors() {
    use crate::{EvalError, JsonError};

    assert_eq!(
        call_result(
            "JSON_CONTAINS_PATH",
            &[s(r#"{"a":1}"#), s("bogus"), s("$.a")]
        ),
        Err(EvalError::Json(JsonError::BadOneOrAllArg {
            function: "json_contains_path"
        }))
    );
    assert_eq!(
        call_result("JSON_SEARCH", &[s(r#"{"a":"x"}"#), s("bogus"), s("x")]),
        Err(EvalError::Json(JsonError::InvalidContainsPathType))
    );
    assert_eq!(
        JsonError::BadOneOrAllArg {
            function: "json_contains_path"
        }
        .code(),
        3154
    );
    assert_eq!(JsonError::InvalidContainsPathType.code(), 3150);
}

/// Source-shaped scalar table from `TestJSONSetInsertReplace` in
/// `pkg/expression/builtin_json_test.go:271`.  Value strings remain JSON
/// strings, paths are exact and sequential, SQL NULL propagates from the
/// document, and malformed/multiple-selection paths are errors.
#[test]
fn json_set_insert_replace_go_vectors() {
    assert_eq!(
        call("JSON_SET", &[Datum::Null, Datum::Null, Datum::Null]),
        Datum::Null
    );
    assert_eq!(
        call("JSON_SET", &[s("{}"), s("$.a"), Datum::Int(3)]),
        s(r#"{"a": 3}"#)
    );
    assert_eq!(
        call("JSON_INSERT", &[s("{}"), s("$.a"), Datum::Int(3)]),
        s(r#"{"a": 3}"#)
    );
    assert_eq!(
        call("JSON_REPLACE", &[s("{}"), s("$.a"), Datum::Int(3)]),
        s("{}")
    );
    assert_eq!(
        call(
            "JSON_SET",
            &[s("{}"), s("$.a"), Datum::Int(3), s("$.b"), s("3"),],
        ),
        s(r#"{"a": 3, "b": "3"}"#)
    );
    assert_eq!(
        call(
            "JSON_SET",
            &[s("{}"), s("$.a"), Datum::Null, s("$.b"), s("nil"),],
        ),
        s(r#"{"a": null, "b": "nil"}"#)
    );
    // `[0]` selects a NON-ARRAY value itself, but an ARRAY's `[0]` is
    // its FIRST ELEMENT. Reading the shortcut as unconditional made
    // `$[0]` on an array name the whole document, so the second pair
    // here replaced everything with `8` instead of the first element.
    assert_eq!(
        call(
            "JSON_SET",
            &[
                s("[1,2]"),
                s("$[0]"),
                Datum::Int(9),
                s("$[0][0]"),
                Datum::Int(8)
            ],
        ),
        s("[8, 2]")
    );
    assert_eq!(
        call("JSON_SET", &[s("[1,2]"), s("$[1]"), Datum::Int(9)]),
        s("[1, 9]")
    );
    assert_eq!(
        call("JSON_SET", &[s("1"), s("$[0]"), Datum::Int(2)]),
        s("2")
    );
    assert!(dispatch("JSON_SET", &[s("{}"), s("$.a")]).is_none());
    assert!(call_result("JSON_SET", &[s("{}"), s("$InvalidPath"), Datum::Int(3)]).is_err());
    assert!(call_result("JSON_SET", &[s("{}"), s("$.*"), Datum::Int(3)]).is_err());
    assert!(dispatch("JSON_SET", &[]).is_none());
}

/// Source-shaped tables from `TestJSONMerge` and `TestJSONMergePreserve`
/// in `pkg/expression/builtin_json_test.go:317` and `:348`, re-captured
/// through SQL because the two disagree on what an argument IS: the Go
/// unit test hands the signature Go strings (JSON strings), while
/// `jsonMergeFunctionClass` types every SQL argument `ETJson`, so a
/// string LITERAL is PARSED (`'1'` merges as the number 1) and a
/// non-string scalar is rejected outright (3146) rather than merged.
///
/// Adjacent objects combine; arrays and scalars are preserved in an
/// output array. JSON_MERGE is the deprecated synonym for
/// JSON_MERGE_PRESERVE and differs only in the 1681 warning its caller
/// raises (see `func::eval_func_values_in`).
#[test]
fn json_merge_go_vectors() {
    for name in ["JSON_MERGE", "JSON_MERGE_PRESERVE"] {
        assert_eq!(call(name, &[Datum::Null, Datum::Null]), Datum::Null);
        assert_eq!(call(name, &[s("{}"), s("[]")]), s("[{}]"));
        assert_eq!(call(name, &[s("1"), s("2")]), s("[1, 2]"));
        assert_eq!(call(name, &[s(r#""a""#), s(r#""b""#)]), s(r#"["a", "b"]"#));
        assert_eq!(
            call(name, &[s(r#"{"a":1}"#), s(r#"{"a":2}"#)]),
            s(r#"{"a": [1, 2]}"#)
        );
        assert_eq!(
            call(name, &[s("{}"), s("[]"), s("3"), s(r#""4""#)]),
            s(r#"[{}, 3, "4"]"#)
        );
        assert!(call_result(name, &[s("{}"), s("not-json")]).is_err());
        assert!(dispatch(name, &[s("{}")]).is_none());
    }
    // A non-string, non-JSON argument is 3146 at its own 1-based index,
    // named for the function the user wrote.
    for (name, function) in [
        ("JSON_MERGE", "json_merge"),
        ("JSON_MERGE_PRESERVE", "json_merge_preserve"),
        ("JSON_MERGE_PATCH", "json_merge_patch"),
    ] {
        assert_eq!(
            call_result(name, &[s("[1]"), Datum::Int(3)]),
            Err(crate::EvalError::Json(
                crate::JsonError::InvalidTypeForJson {
                    argument: 2,
                    function,
                }
            ))
        );
    }
}

/// The `json`-class codes the mutation family reports for a path it
/// cannot use, captured from real TiDB: `JSON_REMOVE` calls a bare `$`
/// vacuous (3153), `JSON_ARRAY_INSERT` needs an array CELL as its last
/// leg (3165), and every mutation refuses a wildcard leg (3149).
#[test]
fn json_mutation_path_error_codes() {
    let code = |name: &str, args: &[Datum]| match call_result(name, args) {
        Err(crate::EvalError::Json(error)) => error.code(),
        other => panic!("expected a json error from {name}, got {other:?}"),
    };
    assert_eq!(code("JSON_REMOVE", &[s(r#"{"a":1}"#), s("$")]), 3153);
    assert_eq!(
        code("JSON_ARRAY_INSERT", &[s("[1]"), s("$"), Datum::Int(1)]),
        3165
    );
    assert_eq!(
        code(
            "JSON_ARRAY_INSERT",
            &[s(r#"{"a":1}"#), s("$.a"), Datum::Int(1)]
        ),
        3165
    );
    for (name, args) in [
        ("JSON_REMOVE", vec![s("[1]"), s("$[*]")]),
        ("JSON_SET", vec![s(r#"{"a":1}"#), s("$[*]"), Datum::Int(1)]),
        ("JSON_SET", vec![s(r#"{"a":1}"#), s("$.*"), Datum::Int(1)]),
        ("JSON_SET", vec![s(r#"{"a":1}"#), s("$**.a"), Datum::Int(1)]),
        (
            "JSON_INSERT",
            vec![s(r#"{"a":1}"#), s("$.*"), Datum::Int(1)],
        ),
        (
            "JSON_REPLACE",
            vec![s(r#"{"a":1}"#), s("$.*"), Datum::Int(1)],
        ),
        (
            "JSON_ARRAY_APPEND",
            vec![s("[1]"), s("$[*]"), Datum::Int(1)],
        ),
    ] {
        assert_eq!(code(name, &args), 3149, "{name} {args:?}");
    }
}

/// RFC 7396 and MySQL vectors from `TestJSONMergePatch` at
/// `pkg/expression/builtin_json_test.go:1367`.  Arrays and non-object
/// patches replace wholesale; object patches recurse, delete JSON-null
/// keys, and retain sorted BinaryJSON object output. SQL NULL follows the
/// source nil-pointer truncation rules; invalid text remains an error.
#[test]
fn json_merge_patch_go_vectors() {
    for (args, want) in [
        (vec![s(r#"{"a":"b"}"#), s(r#"{"a":"c"}"#)], r#"{"a": "c"}"#),
        (
            vec![s(r#"{"a":"b"}"#), s(r#"{"b":"c"}"#)],
            r#"{"a": "b", "b": "c"}"#,
        ),
        (vec![s(r#"{"a":"b"}"#), s(r#"{"a":null}"#)], "{}"),
        (
            vec![s(r#"{"a":"b", "b":"c"}"#), s(r#"{"a":null}"#)],
            r#"{"b": "c"}"#,
        ),
        (
            vec![s(r#"{"a":["b"]}"#), s(r#"{"a":"c"}"#)],
            r#"{"a": "c"}"#,
        ),
        (
            vec![s(r#"{"a":"c"}"#), s(r#"{"a":["b"]}"#)],
            r#"{"a": ["b"]}"#,
        ),
        (
            vec![s(r#"{"a":{"b":"c"}}"#), s(r#"{"a":{"b":"d","c":null}}"#)],
            r#"{"a": {"b": "d"}}"#,
        ),
        (
            vec![s(r#"{"a":[{"b":"c"}]}"#), s(r#"{"a":[1]}"#)],
            r#"{"a": [1]}"#,
        ),
        (
            vec![s("[\"a\",\"b\"]"), s("[\"c\",\"d\"]")],
            r#"["c", "d"]"#,
        ),
        (vec![s(r#"{"a":"b"}"#), s("[\"c\"]")], r#"["c"]"#),
        (
            vec![s(r#"{"e":null}"#), s(r#"{"a":1}"#)],
            r#"{"a": 1, "e": null}"#,
        ),
        (
            vec![s("[1,2]"), s(r#"{"a":"b","c":null}"#)],
            r#"{"a": "b"}"#,
        ),
        (
            vec![s("{}"), s(r#"{"a":{"bb":{"ccc":null}}}"#)],
            r#"{"a": {"bb": {}}}"#,
        ),
        (vec![s(r#"{"a":"foo"}"#), s("false")], "false"),
        (vec![s(r#"{"a":"foo"}"#), s("123")], "123"),
        (vec![s(r#"{"a":"foo"}"#), s("123.1")], "123.1"),
        (vec![s(r#"{"a":"foo"}"#), s("[1,2,3]")], "[1, 2, 3]"),
        (vec![s("null"), s(r#"{"a":1}"#)], r#"{"a": 1}"#),
        (vec![s(r#"{"a":1}"#), s("null")], "null"),
    ] {
        let got = call_result("JSON_MERGE_PATCH", &args).expect("valid merge patch");
        assert_eq!(got, s(want), "args {args:?}");
    }
    assert_eq!(
        call(
            "JSON_MERGE_PATCH",
            &[Datum::Null, s("null"), s(r#"{"a":1}"#)]
        ),
        s(r#"{"a": 1}"#)
    );
    assert_eq!(
        call("JSON_MERGE_PATCH", &[s("null"), s("[1,2,3]"), Datum::Null]),
        Datum::Null
    );
    for args in [
        vec![s(r#"{"a":1}"#), s("[1]}")],
        vec![s(r#"{{"a":1}"#), s("[1]"), s("null")],
        vec![s(r#"{"a":1}"#), s("jjj"), s("null")],
    ] {
        assert!(call_result("JSON_MERGE_PATCH", &args).is_err());
    }
}

/// Complete representable table from `TestJSONPretty` in
/// `pkg/expression/builtin_json_test.go:1293`.  Scalar JSON values stay
/// unchanged; arrays/objects use two-space indentation, sorted object
/// keys, and the same nested formatting as BinaryJSON.MarshalJSON plus
/// `encoding/json.Indent`. Invalid documents remain errors and SQL NULL
/// propagates to SQL NULL.
#[test]
fn json_pretty_go_vectors() {
    for (document, want) in [
        ("true", "true"),
        ("false", "false"),
        ("2223", "2223"),
        (
            r#"{"a":1}"#,
            "{\n  \"a\": 1\n}",
        ),
        ("[1]", "[\n  1\n]"),
        (
            r#"{"a":1,"b":[{"d":1},{"e":2},{"f":3}],"c":"eee"}"#,
            "{\n  \"a\": 1,\n  \"b\": [\n    {\n      \"d\": 1\n    },\n    {\n      \"e\": 2\n    },\n    {\n      \"f\": 3\n    }\n  ],\n  \"c\": \"eee\"\n}",
        ),
        (
            r#"{"a":1,"b":"qwe","c":[1,2,3,"123",null],"d":{"d1":1,"d2":2}}"#,
            "{\n  \"a\": 1,\n  \"b\": \"qwe\",\n  \"c\": [\n    1,\n    2,\n    3,\n    \"123\",\n    null\n  ],\n  \"d\": {\n    \"d1\": 1,\n    \"d2\": 2\n  }\n}",
        ),
    ] {
        assert_eq!(call("JSON_PRETTY", &[s(document)]), s(want), "JSON_PRETTY({document:?})");
    }
    assert_eq!(call("JSON_PRETTY", &[Datum::Null]), Datum::Null);
    for document in [r#"{1}"#, r#"[1,3,4,5]]"#] {
        assert!(call_result("JSON_PRETTY", &[s(document)]).is_err());
    }
    assert!(dispatch("JSON_PRETTY", &[]).is_none());
    assert!(dispatch("JSON_PRETTY", &[s("{}"), s("extra")]).is_none());
}

/// Representable scalar-array rows from `TestJSONSumCrc32` in
/// `pkg/expression/builtin_json_test.go:127`.  The Go source supplies the
/// target array `FieldType` through `expr AS type ARRAY`; this frozen
/// evaluator exercises the shared JSON-text portion (homogeneous numeric
/// and string arrays, empty arrays, root/type errors, NULL, and invalid
/// scalar/nested members) while keeping signed/unsigned/string-width
/// conversion and path/session metadata as explicit boundaries.
#[test]
fn json_sum_crc32_go_vectors() {
    for (document, want) in [
        ("[-1, 2, 3]", 3_101_005_010_i64),
        ("[1, 2, 3]", 4_505_025_631_i64),
        (r#"["a", "b", "c"]"#, 5_925_539_243_i64),
        ("[1.1, 1, 3.3]", 6_204_045_883_i64),
        ("[1.1, 2.2, 3.3]", 4_453_038_788_i64),
        ("[]", 0),
    ] {
        assert_eq!(
            call_result("JSON_SUM_CRC32", &[s(document)]),
            Ok(Datum::Int(want)),
            "JSON_SUM_CRC32({document:?})",
        );
    }
    assert_eq!(call("JSON_SUM_CRC32", &[Datum::Null]), Datum::Null);

    for document in [
        "1",
        "{}",
        "true",
        "[true]",
        "[null]",
        "[[1]]",
        "[{\"a\": 1}]",
        r#"[1.1, "1.1", 3.3]"#,
    ] {
        assert!(
            call_result("JSON_SUM_CRC32", &[s(document)]).is_err(),
            "JSON_SUM_CRC32({document:?}) must reject this text-domain row",
        );
    }
    assert!(call_result("JSON_SUM_CRC32", &[s("not-json")]).is_err());
    assert!(dispatch("JSON_SUM_CRC32", &[]).is_none());
    assert!(dispatch("JSON_SUM_CRC32", &[s("[]"), s("extra")]).is_none());
}

/// Complete representable table from `TestJSONLength` in
/// `pkg/expression/builtin_json_test.go`.  Keeping the scalar and path
/// rows here makes the JSON length contract executable instead of relying
/// on the single nested-array smoke check above: every scalar has length
/// one, containers count direct children only, `$` selects the document,
/// missing paths are NULL, and multiple-selection paths are errors.
#[test]
fn json_length_go_vectors() {
    for (document, want) in [
        ("null", 1),
        ("true", 1),
        ("false", 1),
        ("1", 1),
        ("-1", 1),
        ("1.1", 1),
        (r#""1""#, 1),
        ("{}", 0),
        (r#"{"a":1}"#, 1),
        (r#"{"a":[1]}"#, 1),
        (r#"{"b":2,"c":3}"#, 2),
        ("[1]", 1),
        ("[1,2]", 2),
        ("[1,2,[1,3]]", 3),
        ("[1,2,[1,[5,[3]]]]", 3),
        (r#"[1,2,[1,[5,{"a":[2,3]}]]]"#, 3),
        (r#"[{"a":1}]"#, 1),
        (r#"[{"a":1,"b":2}]"#, 1),
        (r#"[{"a":{"a":1},"b":2}]"#, 1),
    ] {
        assert_eq!(
            call_result("JSON_LENGTH", &[s(document)]),
            Ok(Datum::Int(want)),
            "JSON_LENGTH({document:?})",
        );
    }

    for (document, path, want) in [
        (r#"[1,2,[1,[5,[3]]]]"#, "$[2]", 2),
        (r#"[{"a":1}]"#, "$", 1),
        (r#"[{"a":1}]"#, "$[0].a", 1),
        (r#"{"a":{"a":1},"b":2}"#, "$", 2),
        (r#"{"a":{"a":1},"b":2}"#, "$.a", 1),
        (r#"{"a":{"a":1},"b":2}"#, "$.a.a", 1),
        (r#"{"a":[1,2,{"aa":"xx"}]}"#, "$.a[2].aa", 1),
    ] {
        assert_eq!(
            call_result("JSON_LENGTH", &[s(document), s(path)]),
            Ok(Datum::Int(want)),
            "JSON_LENGTH({document:?}, {path:?})",
        );
    }

    for (document, path) in [
        (r#""1""#, "$.a"),
        ("null", "$.a"),
        (r#"{"a":[1,2,{"aa":"xx"}]}"#, "$.c"),
        (r#"{"a":[1,2,{"aa":"xx"}]}"#, "$.a[3]"),
        (r#"{"a":[1,2,{"aa":"xx"}]}"#, "$.a[2].b"),
    ] {
        assert_eq!(
            call_result("JSON_LENGTH", &[s(document), s(path)]),
            Ok(Datum::Null),
            "JSON_LENGTH({document:?}, {path:?})",
        );
    }

    for path in ["$.*", "$[*]", "$**.a"] {
        assert!(
            call_result("JSON_LENGTH", &[s(r#"{"a":[1,2,{"aa":"xx"}]}"#), s(path)],).is_err(),
            "JSON_LENGTH wildcard path {path:?} must reject multiple selection",
        );
    }

    for args in [
        vec![Datum::Null],
        vec![Datum::Null, s("a")],
        vec![s(r#"{"a":1}"#), Datum::Null],
        vec![Datum::Null, Datum::Null],
    ] {
        assert_eq!(call_result("JSON_LENGTH", &args), Ok(Datum::Null));
    }
}

/// `BinaryJSON.MarshalJSON` orders object keys by PLAIN BYTE comparison
/// (`slices.SortFunc(fields, cmp.Compare)` in `buildBinaryJSONObject`,
/// `pkg/types/json_binary.go`), NOT by key length first.
///
/// Captured from real TiDB (`testkit.CreateMockStore`):
///   SELECT CAST('{"z":1,"B":2,"a":3,"A":4,"_":5,"0":6}' AS JSON)
///     -> {"0": 6, "A": 4, "B": 2, "_": 5, "a": 3, "z": 1}
///   SELECT CAST('{"bb":1,"a":2}' AS JSON)  -> {"a": 2, "bb": 1}
/// A length-first order would have put `bb` before `ccc` and `a` last.
#[test]
fn object_keys_print_in_plain_byte_order() {
    assert_eq!(
        call(
            "JSON_EXTRACT",
            &[s(r#"{"z":1,"B":2,"a":3,"A":4,"_":5,"0":6}"#), s("$")]
        ),
        s(r#"{"0": 6, "A": 4, "B": 2, "_": 5, "a": 3, "z": 1}"#)
    );
    assert_eq!(
        call("JSON_KEYS", &[s(r#"{"bb":1,"a":2,"ccc":3,"dd":4}"#)]),
        s(r#"["a", "bb", "ccc", "dd"]"#)
    );
    // The separators are `, ` and `: `, not serde's compact form.
    assert_eq!(
        call("JSON_EXTRACT", &[s(r#"{"b":1,"aa":2}"#), s("$")]),
        s(r#"{"aa": 2, "b": 1}"#)
    );
}

/// `marshalFloat64To`'s scientific-notation cutoffs, captured from
/// `SELECT CAST('[...]' AS JSON)`: a double keeps at least one fractional
/// digit inside `[1e-15, 1e15)` and switches to a `+`-free, zero-padding-
/// free exponent outside it. An integer JSON number keeps no `.0`.
#[test]
fn json_numbers_print_as_binary_json_does() {
    assert_eq!(
        call(
            "JSON_EXTRACT",
            &[s("[1.0, 1.5, 1e3, 100000000000000000000, -0.0]"), s("$")]
        ),
        s("[1.0, 1.5, 1000.0, 1e20, -0.0]")
    );
    assert_eq!(
        call(
            "JSON_EXTRACT",
            &[s("[0.1,2.5e-10,1e100,3,-3,1.7976931348623157e308]"), s("$")]
        ),
        s("[0.1, 0.00000000025, 1e100, 3, -3, 1.7976931348623157e308]")
    );
    // Beyond int64: `JSON_TYPE` reports the unsigned kind and the value
    // prints without a decimal point.
    assert_eq!(
        call("JSON_TYPE", &[s("18446744073709551615")]),
        s("UNSIGNED INTEGER")
    );
}

/// A duplicate `JSON_OBJECT` key keeps the LAST value, captured from
/// `SELECT JSON_OBJECT('k',1,'k',2,'k',3)` -> `{"k": 3}`.
#[test]
fn json_object_duplicate_key_keeps_last() {
    assert_eq!(
        call(
            "JSON_OBJECT",
            &[s("k"), Datum::Int(1), s("k"), Datum::Int(2)]
        ),
        s(r#"{"k": 2}"#)
    );
    assert_eq!(
        call(
            "JSON_OBJECT",
            &[
                s("k"),
                Datum::Int(1),
                s("k"),
                Datum::Int(2),
                s("k"),
                Datum::Int(3)
            ]
        ),
        s(r#"{"k": 3}"#)
    );
}

/// `parseJSONPathExpr` reports `jsonPathStream.pos` for a rejected leg and
/// a literal 1 when the expression does not start with `$`. Captured:
///   JSON_EXTRACT('{"a":1}','xx')  -> 3143 ... character position 1.
///   JSON_EXTRACT('{"a":1}','$.')  -> 3143 ... character position 2.
#[test]
fn path_errors_carry_go_code_and_position() {
    use crate::{EvalError, JsonError};
    assert_eq!(
        parse_path("xx").unwrap_err(),
        EvalError::Json(JsonError::InvalidPath(1))
    );
    assert_eq!(
        parse_path("$.").unwrap_err(),
        EvalError::Json(JsonError::InvalidPath(2))
    );
    // A trailing `**` is rejected after the whole stream is consumed.
    assert!(matches!(
        parse_path("$**").unwrap_err(),
        EvalError::Json(JsonError::InvalidPath(_))
    ));
    assert!(parse_path("$.a").is_ok());
    assert!(parse_path(r#"$."a b""#).is_ok());
    assert!(parse_path("$[*]").is_ok());
    assert!(parse_path("$**.b").is_ok());
}

/// The `json` error class each in-scope signature raises, with the code
/// TiDB reports. Every pairing below is a captured TiDB error string.
#[test]
fn json_errors_carry_go_codes() {
    use crate::{EvalError, JsonError};
    let code = |name: &str, args: &[Datum]| match call_result(name, args) {
        Err(EvalError::Json(error)) => error.code(),
        other => panic!("expected a json-class error, got {other:?}"),
    };
    assert_eq!(code("JSON_EXTRACT", &[s("x"), s("$.a")]), 3140);
    assert_eq!(code("JSON_EXTRACT", &[s(r#"{"a":1}"#), s("xx")]), 3143);
    assert_eq!(code("JSON_LENGTH", &[s("[1,2]"), s("$[*]")]), 3149);
    assert_eq!(code("JSON_KEYS", &[s(r#"{"a":1}"#), s("$[*]")]), 3149);
    assert_eq!(code("JSON_TYPE", &[Datum::Int(1)]), 3146);
    assert_eq!(code("JSON_QUOTE", &[Datum::Int(1)]), 3064);
    assert_eq!(code("JSON_OBJECT", &[Datum::Null, Datum::Int(1)]), 3158);
    assert_eq!(
        JsonError::InvalidPath(7).message(),
        "Invalid JSON path expression. The error is around character position 7."
    );
}

/// A SQL string reaches this family as `Datum::String` from the row
/// evaluator and as `Datum::Bytes` from the chunk rewriter's `Expr::String`
/// arm. Both are the same `ETString` argument to Go, so every signature
/// must answer identically -- see [`json_sql_string`].
#[test]
fn string_and_bytes_arguments_agree() {
    let bytes = |value: &str| Datum::new_bytes(value.as_bytes().to_vec());
    for (name, text, path) in [
        ("JSON_TYPE", r#"{"a":1}"#, None),
        ("JSON_VALID", r#"{"a":1}"#, None),
        ("JSON_KEYS", r#"{"b":1,"a":2}"#, None),
        ("JSON_QUOTE", "a\"b", None),
        ("JSON_UNQUOTE", r#""a""#, None),
        ("JSON_EXTRACT", r#"{"a":1}"#, Some("$.a")),
        ("JSON_LENGTH", r#"{"a":1,"b":2}"#, Some("$")),
    ] {
        let (string_args, byte_args) = match path {
            Some(path) => (vec![s(text), s(path)], vec![bytes(text), bytes(path)]),
            None => (vec![s(text)], vec![bytes(text)]),
        };
        assert_eq!(
            call_result(name, &string_args),
            call_result(name, &byte_args),
            "{name} disagreed between its string and bytes spellings"
        );
    }
}

/// `dispatch_typed`'s BINARY-charset value rendering. Every expected
/// string is captured verbatim from a real TiDB server
/// (`zz_dump_frozjson_test.go`, `TestZZDumpFrozJSONBinaryOpaque`) over a
/// one-row table with `vb varbinary(8)`, `b binary(3)`, `bl blob`, `vc
/// varchar(8)`, each holding `'ab'`.
#[test]
fn dispatch_typed_renders_binary_charset_arguments_as_opaque() {
    use super::dispatch_typed;
    use tidb_datatype::{Collation, FieldType, FieldTypeCode};

    let varbinary = FieldType::new(FieldTypeCode::Varchar).with_collation(Collation::Binary);
    let mut binary = FieldType::new(FieldTypeCode::String).with_collation(Collation::Binary);
    binary.set_flen(3);
    let blob = FieldType::new(FieldTypeCode::Blob);
    let varchar = FieldType::new(FieldTypeCode::Varchar);

    // `JSON_ARRAY(vb)`, `JSON_ARRAY(b)`, `JSON_ARRAY(bl)`, `JSON_ARRAY(vc)`.
    for (field_type, bytes, expected) in [
        (
            varbinary.clone(),
            b"ab".as_slice(),
            r#"["base64:type15:YWI="]"#,
        ),
        (
            binary.clone(),
            b"ab".as_slice(),
            r#"["base64:type254:YWIA"]"#,
        ),
        (blob.clone(), b"ab".as_slice(), r#"["base64:type252:YWI="]"#),
    ] {
        let got = dispatch_typed(
            "JSON_ARRAY",
            &[Datum::Bytes(bytes.to_vec())],
            &[Some(field_type)],
        )
        .expect("JSON_ARRAY is owned")
        .expect("valid vector");
        assert_eq!(got, s(expected));
    }
    // An ordinary (non-binary-charset) STRING datum is unaffected.
    let plain = dispatch_typed(
        "JSON_ARRAY",
        &[Datum::new_string("ab".to_string())],
        &[Some(varchar)],
    )
    .expect("JSON_ARRAY is owned")
    .expect("valid vector");
    assert_eq!(plain, s(r#"["ab"]"#));

    // `JSON_OBJECT('k', vb)`.
    let object = dispatch_typed(
        "JSON_OBJECT",
        &[s("k"), Datum::Bytes(b"ab".to_vec())],
        &[None, Some(varbinary.clone())],
    )
    .expect("JSON_OBJECT is owned")
    .expect("valid vector");
    assert_eq!(object, s(r#"{"k": "base64:type15:YWI="}"#));

    // `JSON_INSERT('{}', '$.a', vb)`.
    let inserted = dispatch_typed(
        "JSON_INSERT",
        &[s("{}"), s("$.a"), Datum::Bytes(b"ab".to_vec())],
        &[None, None, Some(varbinary)],
    )
    .expect("JSON_INSERT is owned")
    .expect("valid vector");
    assert_eq!(inserted, s(r#"{"a": "base64:type15:YWI="}"#));
}

/// [`cast_as_json_typed`]'s BINARY-charset rendering, the same capture as
/// `dispatch_typed_renders_binary_charset_arguments_as_opaque`:
/// `SELECT CAST(vb AS JSON) FROM t`.
#[test]
fn cast_as_json_typed_renders_binary_charset_argument_as_opaque() {
    use super::cast_as_json_typed;
    use tidb_datatype::{Collation, FieldType, FieldTypeCode};

    let varbinary = FieldType::new(FieldTypeCode::Varchar).with_collation(Collation::Binary);
    let got =
        cast_as_json_typed(&Datum::Bytes(b"ab".to_vec()), Some(&varbinary)).expect("valid vector");
    let Datum::Json(got) = got else {
        panic!("CAST AS JSON did not retain the JSON domain")
    };
    let opaque = got.opaque().expect("opaque binary JSON");
    assert_eq!(opaque.type_code, 15);
    assert_eq!(opaque.bytes, b"ab");

    // `field_type: None` is exactly the untyped `cast_as_json`: a bare
    // (non-JSON) string is PARSED as a JSON document and `ab` is not
    // valid JSON text, so this errors instead of guessing Opaque.
    assert!(cast_as_json_typed(&Datum::Bytes(b"ab".to_vec()), None).is_err());
}
