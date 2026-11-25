//! Go 1.27 `net/url` behavior captured from Go's own test tables.
//!
//! `testdata/net_url_go.jsonl` was produced by a throwaway Go program that
//! copied `urltests`, `stringURLTests`, `unescapeTests`, `queryEscapeTests`,
//! `pathEscapeTests`, `encodeQueryTests` and `parseTests` out of Go 1.27.3's
//! `net/url/url_test.go` (with go/ast) and the inputs of `TestParseErrors`,
//! `TestRejectControlCharacters`, `TestInvalidUserPassword`,
//! `TestParseFailure` and `TestURLHostnameAndPort`, plus netip error paths,
//! the strict-colon hosts and TiDB storage URLs, and printed what the real
//! package answered (`GOTOOLCHAIN=go1.27.3 go run .`).

use super::*;
use serde_json::Value;

fn bytes(v: &Value) -> Vec<u8> {
    match v {
        Value::String(s) => s.as_bytes().to_vec(),
        Value::Object(o) => hex::decode(o["hex"].as_str().unwrap()).unwrap(),
        other => panic!("not a string: {other}"),
    }
}

fn err_text(err: Option<&Error>) -> Value {
    err.map_or(Value::Null, |e| Value::String(e.to_string()))
}

fn url_of(v: &Value) -> URL {
    URL {
        scheme: bytes(&v["scheme"]),
        opaque: bytes(&v["opaque"]),
        user: v["user"].as_object().map(|u| Userinfo {
            username: bytes(&u["username"]),
            password: bytes(&u["password"]),
            password_set: u["password_set"].as_bool().unwrap(),
        }),
        host: bytes(&v["host"]),
        path: bytes(&v["path"]),
        raw_path: bytes(&v["raw_path"]),
        omit_host: v["omit_host"].as_bool().unwrap(),
        force_query: v["force_query"].as_bool().unwrap(),
        raw_query: bytes(&v["raw_query"]),
        fragment: bytes(&v["fragment"]),
        raw_fragment: bytes(&v["raw_fragment"]),
    }
}

fn values_of(v: &Value) -> Values {
    let mut values = Values::default();
    for pair in v.as_array().unwrap() {
        let key = bytes(&pair[0]);
        values
            .0
            .insert(key, pair[1].as_array().unwrap().iter().map(bytes).collect());
    }
    values
}

fn check_derived(url: &URL, want: &Value, context: &str) {
    assert_eq!(url.to_bytes(), bytes(&want["string"]), "String {context}");
    assert_eq!(
        url.escaped_path(),
        bytes(&want["escaped_path"]),
        "EscapedPath {context}"
    );
    assert_eq!(
        url.escaped_fragment(),
        bytes(&want["escaped_fragment"]),
        "EscapedFragment {context}"
    );
    assert_eq!(
        url.redacted().into_bytes(),
        bytes(&want["redacted"]),
        "Redacted {context}"
    );
    assert_eq!(
        url.request_uri(),
        bytes(&want["request_uri"]),
        "RequestURI {context}"
    );
    assert_eq!(
        url.hostname(),
        bytes(&want["hostname"]),
        "Hostname {context}"
    );
    assert_eq!(url.port(), bytes(&want["port"]), "Port {context}");
    assert_eq!(
        url.is_abs(),
        want["is_abs"].as_bool().unwrap(),
        "IsAbs {context}"
    );
    assert_eq!(url.query(), values_of(&want["query"]), "Query {context}");
}

/// Go `TestParse`, `TestURLString`, `TestParseErrors`, `TestUnescape`,
/// `TestQueryEscape`, `TestPathEscape`, `TestEncodeQuery`, `TestParseQuery`
/// and `TestURLHostnameAndPort`, against the captured Go answers.
#[test]
fn net_url_matches_go_capture() {
    let capture = include_str!("../testdata/net_url_go.jsonl");
    let mut counts = std::collections::BTreeMap::<String, usize>::new();
    for line in capture.lines() {
        let rec: Value = serde_json::from_str(line).unwrap();
        let kind = rec["kind"].as_str().unwrap();
        *counts.entry(kind.to_owned()).or_default() += 1;
        match kind {
            "parse" => {
                let input = bytes(&rec["in"]);
                let context = format!("for {:?}", String::from_utf8_lossy(&input));
                match parse(&input) {
                    Ok(url) => {
                        assert_eq!(rec["err"], Value::Null, "Parse {context} succeeded");
                        assert_eq!(url, url_of(&rec["url"]), "fields {context}");
                        check_derived(&url, &rec["derived"], &context);
                    }
                    Err(err) => assert_eq!(err_text(Some(&err)), rec["err"], "error {context}"),
                }
            }
            "string" => {
                let url = url_of(&rec["url"]);
                check_derived(&url, &rec["derived"], &format!("of {url:?}"));
            }
            "unescape" => {
                let input = bytes(&rec["in"]);
                let query = query_unescape(&input);
                assert_eq!(
                    err_text(query.as_ref().err()),
                    rec["query_err"],
                    "{input:?}"
                );
                if let Ok(out) = query {
                    assert_eq!(out, bytes(&rec["query"]), "QueryUnescape {input:?}");
                }
                let path = path_unescape(&input);
                assert_eq!(err_text(path.as_ref().err()), rec["path_err"], "{input:?}");
                if let Ok(out) = path {
                    assert_eq!(out, bytes(&rec["path"]), "PathUnescape {input:?}");
                }
            }
            "escape" => {
                let input = bytes(&rec["in"]);
                assert_eq!(
                    query_escape(&input),
                    bytes(&rec["query"]),
                    "QueryEscape {input:?}"
                );
                assert_eq!(
                    path_escape(&input),
                    bytes(&rec["path"]),
                    "PathEscape {input:?}"
                );
            }
            "encode" => assert_eq!(values_of(&rec["values"]).encode(), bytes(&rec["out"])),
            "parse_query" => {
                let (values, err) = parse_query(bytes(&rec["in"]));
                assert_eq!(values, values_of(&rec["values"]), "{}", rec["in"]);
                assert_eq!(err_text(err.as_ref()), rec["err"], "{}", rec["in"]);
            }
            "hostport" => {
                let url = URL {
                    host: bytes(&rec["in"]),
                    ..URL::default()
                };
                assert_eq!(url.hostname(), bytes(&rec["host"]), "{}", rec["in"]);
                assert_eq!(url.port(), bytes(&rec["port"]), "{}", rec["in"]);
            }
            other => panic!("unknown capture kind {other}"),
        }
    }
    let want = [
        ("encode", 6),
        ("escape", 12),
        ("hostport", 21),
        ("parse", 192),
        ("parse_query", 26),
        ("string", 66),
        ("unescape", 13),
    ];
    assert_eq!(counts, want.map(|(k, n)| (k.to_owned(), n)).into());
}

/// Go `TestParseFailure`: the first query error is returned.
#[test]
fn test_parse_failure() {
    let (_, err) = parse_query("%gh&%ij");
    assert!(err.unwrap().to_string().contains("%gh"));
}

/// Go `TestRejectControlCharacters`.
#[test]
fn test_reject_control_characters() {
    for s in [
        "http://foo.com/?foo\nbar",
        "http\r://foo.com/",
        "http://foo\x7f.com/",
    ] {
        let err = parse(s).unwrap_err().to_string();
        assert!(
            err.contains("net/url: invalid control character in URL"),
            "{err}"
        );
    }
    assert!(parse(b"http://foo.com/ctl\x80").is_ok());
}

#[test]
fn url_escaped_path_borrows_only_matching_hints() {
    let mut url = crate::parse("https://example.invalid/a%2f%66").unwrap();
    assert!(matches!(url.escaped_path_cow(), std::borrow::Cow::Borrowed(bytes)
        if std::ptr::eq(bytes, url.raw_path.as_slice())));

    url.path = b"/new path".to_vec();
    assert_eq!(url.escaped_path_cow().as_ref(), b"/new%20path");
    url.raw_path = b"/%0G".to_vec();
    assert_eq!(url.string(), "https://example.invalid/new%20path");
}

#[test]
fn redacted_url_keeps_escaped_components_and_masks_empty_password() {
    let url = parse("https://user%40name:@example.invalid/a%2fb?x=y#f%20g").unwrap();
    assert_eq!(
        url.redacted(),
        "https://user%40name:xxxxx@example.invalid/a%2fb?x=y#f%20g"
    );
    assert!(matches!(url.user.as_ref().unwrap().password(), ([], true)));

    let url = parse("https://user%40name@example.invalid/a%2fb?x=y#f%20g").unwrap();
    assert_eq!(url.redacted(), url.string());
}

#[test]
fn parse_query_groups_decoded_keys_and_keeps_the_parameter_limit() {
    let (values, error) = parse_query("name=first&%6Eame=with+space&name=third%2B");
    assert_eq!(error, None);
    assert_eq!(
        values.0.get(b"name".as_slice()).unwrap(),
        &[
            b"first".to_vec(),
            b"with space".to_vec(),
            b"third+".to_vec()
        ]
    );

    let mut query = format!("name=value{}", "&".repeat(9999));
    let (values, error) = parse_query(&query);
    assert_eq!(error, None);
    assert_eq!(values.get("name"), b"value");

    query.push('&');
    let (values, error) = parse_query(&query);
    assert!(values.0.is_empty());
    assert_eq!(
        error,
        Some(Error::Other(
            "number of URL query parameters exceeded limit".into()
        ))
    );
}
