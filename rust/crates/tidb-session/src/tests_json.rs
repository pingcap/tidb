#![cfg(test)]

use crate::tests_support::*;
use crate::*;

/// `JSON_TABLE` is REFUSED, and this test records WHY rather than
/// leaving it looking like an unfinished port.
///
/// The Go side of this branch does not parse it AT ALL. Captured from
/// `testkit.CreateMockStore` on `hparser-integration`:
///
/// ```text
/// SQL:  select * from json_table('[1,2]', '$[*]' columns (v int path '$')) jt
/// ERR:  [parser:1064]You have an error in your SQL syntax; ... near
///       "'[1,2]', '$[*]' columns (v int path '$')) jt"
/// ```
///
/// The `FOR ORDINALITY` and lateral (`FROM t, JSON_TABLE(t.j, ...)`)
/// forms fail the same way, and `grep -rni json_table pkg/` finds only
/// the UNRELATED statistics-dump `JSONTable` struct -- no grammar rule,
/// no AST node, no executor. There is therefore no Go source to
/// transcreate; this is a HARD SKIP, not a deferral.
#[test]
fn json_table_is_unsupported_upstream() {
    let mut session = Session::new();
    assert!(
        session
            .run(r#"SELECT * FROM JSON_TABLE('[1]', '$[*]' COLUMNS (v INT PATH '$')) t"#)
            .is_err(),
        "JSON_TABLE does not parse in Go either -- it must stay refused"
    );
    assert!(
            session
                .run(
                    r#"SELECT * FROM JSON_TABLE('[{"a":1}]', '$[*]' COLUMNS (o FOR ORDINALITY, a INT PATH '$.a')) AS jt"#
                )
                .is_err(),
            "FOR ORDINALITY form is a Go parse error too"
        );
}

/// `JSON_ARRAYAGG` / `JSON_OBJECTAGG` / `APPROX_COUNT_DISTINCT` /
/// `APPROX_PERCENTILE`, as GROUP BY aggregates and as window functions.
///
/// Every expectation is captured from TiDB (`pkg/executor`, mock store).
#[test]
fn json_and_approximate_aggregates() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (id BIGINT, g BIGINT, i BIGINT, s VARCHAR(20), j JSON)")
        .unwrap();
    session
        .run(
            "INSERT INTO t VALUES \
                 (1,1,10,'a','[1,2]'),(2,1,20,'b','{\"k\":1}'),(3,1,NULL,NULL,NULL),\
                 (4,2,30,'a','\"str\"'),(5,2,30,'a','1'),(6,2,40,'c','null')",
        )
        .unwrap();

    // JSON_ARRAYAGG keeps ROW ORDER and includes a NULL input as JSON
    // `null` -- it does not skip the row the way SUM/COUNT do.
    assert_eq!(
        row_text(session.run("SELECT JSON_ARRAYAGG(i) FROM t")),
        [["[10, 20, null, 30, 30, 40]"]]
    );
    assert_eq!(
        row_text(session.run("SELECT JSON_ARRAYAGG(s) FROM t")),
        [["[\"a\", \"b\", null, \"a\", \"a\", \"c\"]"]]
    );
    // A JSON column's own value is carried through unchanged, including
    // the JSON `null` document, which is indistinguishable from the
    // SQL NULL row here.
    assert_eq!(
        row_text(session.run("SELECT JSON_ARRAYAGG(j) FROM t")),
        [["[[1, 2], {\"k\": 1}, null, \"str\", 1, null]"]]
    );
    assert_eq!(
        row_text(session.run("SELECT g, JSON_ARRAYAGG(i) FROM t GROUP BY g ORDER BY g")),
        [["1", "[10, 20, null]"], ["2", "[30, 30, 40]"]]
    );
    // An empty group is SQL NULL, not `[]`.
    assert_eq!(
        row_text(session.run("SELECT JSON_ARRAYAGG(i) FROM t WHERE id < 0")),
        [["NULL"]]
    );

    // JSON_OBJECTAGG: a repeated key keeps the LAST row's value (`a` is
    // written by id 1 then overwritten by id 4 and again by id 5), and
    // the encoded object sorts its keys.
    assert_eq!(
        row_text(session.run("SELECT JSON_OBJECTAGG(s, i) FROM t WHERE s IS NOT NULL")),
        [["{\"a\": 30, \"b\": 20, \"c\": 40}"]]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT g, JSON_OBJECTAGG(s, i) FROM t WHERE s IS NOT NULL \
                 GROUP BY g ORDER BY g"
        )),
        [
            ["1", "{\"a\": 10, \"b\": 20}"],
            ["2", "{\"a\": 30, \"c\": 40}"]
        ]
    );
    // A non-string key is stringified, and a NULL VALUE is kept as JSON
    // `null` (only a NULL KEY is an error).
    assert_eq!(
        row_text(session.run("SELECT JSON_OBJECTAGG(id, j) FROM t")),
        [[
            "{\"1\": [1, 2], \"2\": {\"k\": 1}, \"3\": null, \"4\": \"str\", \
               \"5\": 1, \"6\": null}"
        ]]
    );
    assert_eq!(
        row_text(session.run("SELECT JSON_OBJECTAGG(s, i) FROM t WHERE id < 0")),
        [["NULL"]]
    );
    // Captured: "[json:3158]JSON documents may not contain NULL member
    // names." -- raised while folding the group, so it fails the
    // statement even though one non-NULL key was already written.
    assert!(matches!(
        session.run("SELECT JSON_OBJECTAGG(s, i) FROM t"),
        Err(DriverError::JsonDocumentNullKey)
    ));
    assert!(matches!(
        session.run("SELECT JSON_OBJECTAGG(s, i) FROM t WHERE id IN (1,3)"),
        Err(DriverError::JsonDocumentNullKey)
    ));
    // Neither JSON aggregate accepts DISTINCT -- Go's parser rejects it.
    assert!(matches!(
        session.run("SELECT JSON_ARRAYAGG(DISTINCT i) FROM t"),
        Err(DriverError::Parse(_))
    ));
    assert!(matches!(
        session.run("SELECT JSON_OBJECTAGG(DISTINCT s, i) FROM t"),
        Err(DriverError::Parse(_))
    ));

    // APPROX_COUNT_DISTINCT is EXACT at this cardinality: Go's BJKST
    // sketch discards nothing below 65536 distinct values.
    assert_eq!(
        row_text(session.run("SELECT APPROX_COUNT_DISTINCT(i) FROM t")),
        [["4"]]
    );
    assert_eq!(
        row_text(session.run("SELECT APPROX_COUNT_DISTINCT(s) FROM t")),
        [["3"]]
    );
    // The multi-argument form counts distinct TUPLES, dropping a row
    // with any NULL argument.
    assert_eq!(
        row_text(session.run("SELECT APPROX_COUNT_DISTINCT(i, s) FROM t")),
        [["4"]]
    );
    assert_eq!(
        row_text(session.run("SELECT g, APPROX_COUNT_DISTINCT(i) FROM t GROUP BY g ORDER BY g")),
        [["1", "2"], ["2", "2"]]
    );
    // An empty (or all-NULL) input is 0, never NULL -- the result column
    // is NOT NULL, like COUNT's.
    assert_eq!(
        row_text(session.run("SELECT APPROX_COUNT_DISTINCT(i) FROM t WHERE id < 0")),
        [["0"]]
    );
    // DISTINCT is legal and cannot change the answer.
    assert_eq!(
        row_text(session.run("SELECT APPROX_COUNT_DISTINCT(DISTINCT i) FROM t")),
        [["4"]]
    );

    // APPROX_PERCENTILE ranks the group's values at ordinal rank
    // ceil(pct/100 * N) and returns THAT element: over (1,2,3,4) the
    // median is 2, not 2.5.
    session
        .run("CREATE TABLE p (g BIGINT, i BIGINT, d DOUBLE, s VARCHAR(20))")
        .unwrap();
    session
        .run(
            "INSERT INTO p VALUES (1,1,1.0,'x'),(1,2,2.0,'y'),(1,3,3.0,'z'),(1,4,4.0,'w'),\
                 (2,10,10.0,'a'),(2,20,20.0,'b'),(2,30,30.0,'c')",
        )
        .unwrap();
    for (pct, even, odd) in [
        (1, "1", "10"),
        (25, "1", "10"),
        (50, "2", "20"),
        (75, "3", "30"),
        (99, "4", "30"),
        (100, "4", "30"),
    ] {
        assert_eq!(
            row_text(session.run(&format!(
                "SELECT APPROX_PERCENTILE(i, {pct}) FROM p WHERE g = 1"
            ))),
            [[even]],
            "even-sized group at {pct}%"
        );
        assert_eq!(
            row_text(session.run(&format!(
                "SELECT APPROX_PERCENTILE(i, {pct}) FROM p WHERE g = 2"
            ))),
            [[odd]],
            "odd-sized group at {pct}%"
        );
    }
    assert_eq!(
        row_text(session.run("SELECT APPROX_PERCENTILE(d, 50) FROM p WHERE g = 1")),
        [["2"]]
    );
    // A STRING argument gets Go's no-op accumulator: always NULL.
    assert_eq!(
        row_text(session.run("SELECT APPROX_PERCENTILE(s, 50) FROM p WHERE g = 1")),
        [["NULL"]]
    );
    assert_eq!(
        row_text(session.run("SELECT APPROX_PERCENTILE(i, 50) FROM p WHERE g = 99")),
        [["NULL"]]
    );
    assert_eq!(
        row_text(session.run("SELECT APPROX_PERCENTILE(DISTINCT i, 50) FROM p WHERE g = 1")),
        [["2"]]
    );
    // The percentage is validated at PLAN time, against [1, 100].
    assert!(matches!(
        session.run("SELECT APPROX_PERCENTILE(i, 0) FROM p"),
        Err(DriverError::PercentageOutOfRange(0))
    ));
    assert!(matches!(
        session.run("SELECT APPROX_PERCENTILE(i, 101) FROM p"),
        Err(DriverError::PercentageOutOfRange(101))
    ));
    assert!(matches!(
        session.run("SELECT APPROX_PERCENTILE(i, -1) FROM p"),
        Err(DriverError::PercentageOutOfRange(-1))
    ));
    // A DECIMAL literal reads as 0 and a FLOAT literal as its IEEE-754
    // bit pattern, because Go's `Constant.EvalInt` reads the datum's
    // int64 field UNCONVERTED for both (captured verbatim).
    assert!(matches!(
        session.run("SELECT APPROX_PERCENTILE(i, 50.5) FROM p"),
        Err(DriverError::PercentageOutOfRange(0))
    ));
    assert!(matches!(
        session.run("SELECT APPROX_PERCENTILE(i, 50e0) FROM p"),
        Err(DriverError::PercentageOutOfRange(4632233691727265792))
    ));
    // A STRING literal, though, takes Go's converting branch.
    assert_eq!(
        row_text(session.run("SELECT APPROX_PERCENTILE(i, '50') FROM p")),
        [["4"]]
    );
    assert!(matches!(
        session.run("SELECT APPROX_PERCENTILE(i, NULL) FROM p"),
        Err(DriverError::ApproxPercentileArgument(message))
            if message.contains("cannot be NULL")
    ));
    assert!(matches!(
        session.run("SELECT APPROX_PERCENTILE(i) FROM p"),
        Err(DriverError::ApproxPercentileArgument(
            "APPROX_PERCENTILE should take 2 arguments"
        ))
    ));
    assert!(matches!(
        session.run("SELECT APPROX_PERCENTILE(i, i) FROM p"),
        Err(DriverError::ApproxPercentileArgument(message))
            if message.contains("constant expression")
    ));

    // All four answer OVER a window, and the FRAME applies.
    assert_eq!(
        row_text(session.run("SELECT id, JSON_ARRAYAGG(i) OVER (ORDER BY id) FROM t ORDER BY id")),
        [
            ["1", "[10]"],
            ["2", "[10, 20]"],
            ["3", "[10, 20, null]"],
            ["4", "[10, 20, null, 30]"],
            ["5", "[10, 20, null, 30, 30]"],
            ["6", "[10, 20, null, 30, 30, 40]"],
        ]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT id, JSON_ARRAYAGG(i) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING \
                 AND CURRENT ROW) FROM t ORDER BY id"
        )),
        [
            ["1", "[10]"],
            ["2", "[10, 20]"],
            ["3", "[20, null]"],
            ["4", "[null, 30]"],
            ["5", "[30, 30]"],
            ["6", "[30, 40]"],
        ]
    );
    // The default RANGE frame ends at the last PEER, so tied ORDER BY
    // keys all see the whole peer group (id 4 and 5 share i = 30).
    assert_eq!(
        row_text(session.run(
            "SELECT id, JSON_ARRAYAGG(i) OVER (PARTITION BY g ORDER BY i) FROM t \
                 WHERE g = 2 ORDER BY id"
        )),
        [["4", "[30, 30]"], ["5", "[30, 30]"], ["6", "[30, 30, 40]"],]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT id, JSON_OBJECTAGG(s, i) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING \
                 AND CURRENT ROW) FROM t WHERE s IS NOT NULL ORDER BY id"
        )),
        [
            ["1", "{\"a\": 10}"],
            ["2", "{\"a\": 10, \"b\": 20}"],
            ["4", "{\"a\": 30, \"b\": 20}"],
            ["5", "{\"a\": 30}"],
            ["6", "{\"a\": 30, \"c\": 40}"],
        ]
    );
    assert_eq!(
        row_text(
            session
                .run("SELECT id, APPROX_COUNT_DISTINCT(i) OVER (ORDER BY id) FROM t ORDER BY id")
        ),
        [
            ["1", "1"],
            ["2", "2"],
            ["3", "2"],
            ["4", "3"],
            ["5", "3"],
            ["6", "4"],
        ]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT id, APPROX_COUNT_DISTINCT(i) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING \
                 AND CURRENT ROW) FROM t ORDER BY id"
        )),
        [
            ["1", "1"],
            ["2", "2"],
            ["3", "1"],
            ["4", "1"],
            ["5", "1"],
            ["6", "2"],
        ]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT i, APPROX_PERCENTILE(i, 50) OVER (PARTITION BY g ORDER BY i) FROM p \
                 WHERE g = 1 ORDER BY i"
        )),
        [["1", "1"], ["2", "1"], ["3", "2"], ["4", "2"]]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT i, APPROX_PERCENTILE(i, 50) OVER (PARTITION BY g ORDER BY i \
                 ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM p WHERE g = 1 ORDER BY i"
        )),
        [["1", "1"], ["2", "1"], ["3", "2"], ["4", "3"]]
    );
    // A window call still refuses DISTINCT, whatever the function.
    assert!(matches!(
        session.run("SELECT APPROX_COUNT_DISTINCT(DISTINCT i) OVER () FROM t"),
        Err(DriverError::NotSupportedYet(
            "<window function>(DISTINCT ..)"
        ))
    ));
}

/// `JSON_ARRAYAGG`/`JSON_OBJECTAGG` over a BINARY-charset value: Go wraps it
/// in a JSON `Opaque` value (rendered `"base64:type<N>:<data>"`) tagged with
/// the source column's own MySQL type code, rather than an ordinary JSON
/// string. A BINARY-charset KEY is rejected with 3144. Every expectation is
/// captured verbatim from a real TiDB server (`zz_dump_opaque_test.go`,
/// `TestZZDumpOpaque`).
#[test]
fn json_aggregates_wrap_binary_charset_values_as_opaque() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE t (\
                 c_varbin VARBINARY(10), c_bin BINARY(3), c_tinyblob TINYBLOB, \
                 c_blob BLOB, c_mediumblob MEDIUMBLOB, c_longblob LONGBLOB)",
        )
        .unwrap();
    session
        .run("INSERT INTO t VALUES ('ab','ab','ab','ab','ab','ab')")
        .unwrap();

    // mysql.TypeVarchar (15).
    assert_eq!(
        row_text(session.run("SELECT JSON_ARRAYAGG(c_varbin) FROM t")),
        [["[\"base64:type15:YWI=\"]"]]
    );
    // mysql.TypeString (254), fixed-length and zero-padded to `flen` before
    // encoding -- `YWIA` decodes to `61 62 00` (`ab\0`), the tailing pad
    // byte included.
    assert_eq!(
        row_text(session.run("SELECT JSON_ARRAYAGG(c_bin) FROM t")),
        [["[\"base64:type254:YWIA\"]"]]
    );
    // mysql.Type{Tiny,Medium,Long}Blob / mysql.TypeBlob (249/250/251/252).
    assert_eq!(
        row_text(session.run("SELECT JSON_ARRAYAGG(c_tinyblob) FROM t")),
        [["[\"base64:type249:YWI=\"]"]]
    );
    assert_eq!(
        row_text(session.run("SELECT JSON_ARRAYAGG(c_blob) FROM t")),
        [["[\"base64:type252:YWI=\"]"]]
    );
    assert_eq!(
        row_text(session.run("SELECT JSON_ARRAYAGG(c_mediumblob) FROM t")),
        [["[\"base64:type250:YWI=\"]"]]
    );
    assert_eq!(
        row_text(session.run("SELECT JSON_ARRAYAGG(c_longblob) FROM t")),
        [["[\"base64:type251:YWI=\"]"]]
    );

    // JSON_OBJECTAGG's VALUE side follows the same rule.
    assert_eq!(
        row_text(session.run("SELECT JSON_OBJECTAGG('k', c_varbin) FROM t")),
        [["{\"k\": \"base64:type15:YWI=\"}"]]
    );
    assert_eq!(
        row_text(session.run("SELECT JSON_OBJECTAGG('k', c_bin) FROM t")),
        [["{\"k\": \"base64:type254:YWIA\"}"]]
    );

    // A BINARY-charset KEY fails the statement with 3144 -- captured:
    // "[json:3144]Cannot create a JSON value from a string with CHARACTER
    // SET 'binary'.".
    assert!(matches!(
        session.run("SELECT JSON_OBJECTAGG(c_varbin, 1) FROM t"),
        Err(DriverError::InvalidJsonCharset { charset }) if charset == "binary"
    ));
}

/// `APPROX_COUNT_DISTINCT` past the 65536-distinct-value threshold, where
/// Go's `BJKST` sketch (`func_count_distinct.go`) starts discarding samples
/// and extrapolating rather than counting exactly.
///
/// Captured from Go (`testkit.CreateMockStore`,
/// `pkg/executor/zz_dump_approxcount_test.go`, `-tags=intest`): a `BIGINT`
/// column loaded with 0..100000 (then, over the same table, 0..70000 via
/// `WHERE v < 70000`) distinct values, `SELECT APPROX_COUNT_DISTINCT(v)
/// FROM t`. Go answered 101048 and 70697; matching bit for bit end to end
/// (through the real row path -- `appendInt64`'s encoding, FarmHash, and
/// the sketch's skip/resize/rehash arithmetic all in series) is the proof
/// the port is faithful rather than merely plausible.
#[test]
fn approx_count_distinct_matches_go_above_the_sketch_threshold() {
    let mut session = Session::new();
    session.run("CREATE TABLE big (v BIGINT)").unwrap();
    const TOTAL: i64 = 100_000;
    const BATCH: i64 = 1000;
    let mut start = 0;
    while start < TOTAL {
        let end = (start + BATCH).min(TOTAL);
        let values: Vec<String> = (start..end).map(|i| format!("({i})")).collect();
        session
            .run(&format!("INSERT INTO big VALUES {}", values.join(",")))
            .unwrap();
        start = end;
    }

    assert_eq!(
        row_text(session.run("SELECT APPROX_COUNT_DISTINCT(v) FROM big")),
        [["101048"]]
    );
    assert_eq!(
        row_text(session.run("SELECT APPROX_COUNT_DISTINCT(v) FROM big WHERE v < 70000")),
        [["70697"]]
    );
}

/// The JSON family's first slice: JSON evaluated as VALUES.
///
/// Every expectation below is a `testkit.CreateMockStore` capture of real
/// TiDB on the same statements. Two facts are worth naming because they
/// are easy to assume wrong:
///
///  * object keys print in PLAIN BYTE order, not length-then-bytes
///    (`buildBinaryJSONObject`'s `cmp.Compare`), so `{"b":1,"aa":2}`
///    prints `aa` first;
///  * a duplicate `JSON_OBJECT` key keeps the LAST value.
///
/// DOCUMENTED DIVERGENCE: TiDB reports a JSON-returning column as type
/// `JSON` (245); this tier has no BinaryJSON value, so the column is a
/// string carrying `BinaryJSON.MarshalJSON`'s exact text. The VALUES here
/// are byte-identical to TiDB's -- only the reported column type differs,
/// the same trade the temporal casts make (see `tidb_expr::rewriter`).
#[test]
fn json_value_functions() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY, j VARCHAR(200))")
        .unwrap();
    session
        .run(r#"INSERT INTO t VALUES (1, '{"b":1,"aa":2,"c":{"d q":"v"}}')"#)
        .unwrap();

    macro_rules! check {
        ($sql:expr, $want:expr) => {
            assert_eq!(
                row_text(session.run($sql)),
                vec![vec![($want).to_owned()]],
                "{}",
                $sql
            );
        };
    }

    // JSON_EXTRACT: one path returns the element, several wrap in an
    // array, an unmatched path is NULL, and a wildcard always wraps.
    check!(r#"SELECT JSON_EXTRACT('{"a":1,"b":"x"}', '$.a')"#, "1");
    check!(r#"SELECT JSON_EXTRACT('{"a":1,"b":"x"}', '$.b')"#, r#""x""#);
    check!(
        r#"SELECT JSON_EXTRACT('{"a":1,"b":"x"}', '$.a', '$.b')"#,
        r#"[1, "x"]"#
    );
    check!(r#"SELECT JSON_EXTRACT('{"a":1}', '$.zzz')"#, "NULL");
    check!("SELECT JSON_EXTRACT('[1,2,3]', '$[*]')", "[1, 2, 3]");
    check!("SELECT JSON_EXTRACT('[1,2,3]', '$[1]')", "2");
    check!(r#"SELECT JSON_EXTRACT('{"a":1,"b":2}', '$.*')"#, "[1, 2]");
    // A scalar auto-wraps for `$[0]`, and `**` walks recursively.
    check!("SELECT JSON_EXTRACT('3', '$[0]')", "3");
    check!(
        r#"SELECT JSON_EXTRACT('{"a":{"b":[1,2]}}', '$**.b')"#,
        "[[1, 2]]"
    );
    check!(r#"SELECT JSON_EXTRACT('{"a b":1}', '$."a b"')"#, "1");
    check!("SELECT JSON_EXTRACT(NULL, '$.a')", "NULL");
    check!(r#"SELECT JSON_EXTRACT('{"a":1}', NULL)"#, "NULL");

    // `->` is JSON_EXTRACT and `->>` wraps it in JSON_UNQUOTE, so `->>`
    // differs ONLY when the extracted value is a JSON string.
    check!("SELECT j->'$.b' FROM t", "1");
    check!("SELECT j->>'$.b' FROM t", "1");
    check!("SELECT j->'$.c' FROM t", r#"{"d q": "v"}"#);
    check!("SELECT j->>'$.c' FROM t", r#"{"d q": "v"}"#);
    check!(r#"SELECT j->'$.c."d q"' FROM t"#, r#""v""#);
    check!(r#"SELECT j->>'$.c."d q"' FROM t"#, "v");
    check!("SELECT j->'$.zz' FROM t", "NULL");
    check!("SELECT j->>'$.zz' FROM t", "NULL");
    check!(
        "SELECT j->'$' FROM t",
        r#"{"aa": 2, "b": 1, "c": {"d q": "v"}}"#
    );

    // JSON_TYPE names the BinaryJSON kind, which is why `1.0` is DOUBLE
    // and a value past int64 is UNSIGNED INTEGER.
    for (document, want) in [
        ("1", "INTEGER"),
        ("1.0", "DOUBLE"),
        ("-1", "INTEGER"),
        ("1e3", "DOUBLE"),
        (r#""s""#, "STRING"),
        ("true", "BOOLEAN"),
        ("false", "BOOLEAN"),
        ("null", "NULL"),
        ("{}", "OBJECT"),
        ("[]", "ARRAY"),
        ("18446744073709551615", "UNSIGNED INTEGER"),
        (r#"  {"a":1}  "#, "OBJECT"),
    ] {
        let sql = format!("SELECT JSON_TYPE('{document}')");
        assert_eq!(
            row_text(session.run(&sql)),
            vec![vec![want.to_owned()]],
            "{sql}"
        );
    }
    check!("SELECT JSON_TYPE(NULL)", "NULL");

    // JSON_OBJECT / JSON_ARRAY. The duplicate key keeps the LAST value
    // and the printed key order is plain byte order.
    check!("SELECT JSON_OBJECT('k',1,'k',2)", r#"{"k": 2}"#);
    check!("SELECT JSON_OBJECT('k',1,'k',2,'k',3)", r#"{"k": 3}"#);
    check!(
        "SELECT JSON_OBJECT('b',1,'aa',2,'c',3)",
        r#"{"aa": 2, "b": 1, "c": 3}"#
    );
    check!("SELECT JSON_OBJECT('k',NULL)", r#"{"k": null}"#);
    check!("SELECT JSON_OBJECT()", "{}");
    check!("SELECT JSON_OBJECT(1,1)", r#"{"1": 1}"#);
    check!(
        "SELECT JSON_ARRAY(1,'x',NULL,1.5)",
        r#"[1, "x", null, 1.5]"#
    );
    check!("SELECT JSON_ARRAY()", "[]");

    // JSON_QUOTE / JSON_UNQUOTE. Only a fully double-quoted document is
    // unquoted; anything else comes back unchanged.
    check!(r#"SELECT JSON_QUOTE('a"b')"#, r#""a\"b""#);
    check!("SELECT JSON_QUOTE('中')", r#""中""#);
    check!("SELECT JSON_QUOTE(NULL)", "NULL");
    check!(r#"SELECT JSON_UNQUOTE('"a\\"b"')"#, r#"a"b"#);
    check!(r#"SELECT JSON_UNQUOTE('"\\u4e2d"')"#, "中");
    check!(r#"SELECT JSON_UNQUOTE('"a\\/b"')"#, "a/b");
    check!("SELECT JSON_UNQUOTE('abc')", "abc");
    check!("SELECT JSON_UNQUOTE('[1,2]')", "[1,2]");
    check!(r#"SELECT JSON_UNQUOTE('"x')"#, r#""x"#);
    check!(r#"SELECT JSON_UNQUOTE(JSON_QUOTE('a"b'))"#, r#"a"b"#);
    check!("SELECT JSON_UNQUOTE(NULL)", "NULL");

    // JSON_CONTAINS: containment, not equality, plus the optional path.
    check!("SELECT JSON_CONTAINS('[1,2,3]','2')", "1");
    check!("SELECT JSON_CONTAINS('[1,2,3]','[1,3]')", "1");
    check!(r#"SELECT JSON_CONTAINS('{"a":1,"b":2}','{"a":1}')"#, "1");
    check!(r#"SELECT JSON_CONTAINS('{"a":{"b":1}}','1','$.a.b')"#, "1");
    check!(r#"SELECT JSON_CONTAINS('{"a":[1,2]}','2','$.a')"#, "1");
    check!("SELECT JSON_CONTAINS('[[1,2]]','[1]')", "1");
    check!("SELECT JSON_CONTAINS('1','1')", "1");
    check!("SELECT JSON_CONTAINS('[1]','2')", "0");
    check!("SELECT JSON_CONTAINS(NULL,'1')", "NULL");
    check!(r#"SELECT JSON_CONTAINS('{"a":1}','1','$.zz')"#, "NULL");

    // JSON_LENGTH / JSON_KEYS / JSON_DEPTH. Every scalar has length one
    // and depth one; JSON_KEYS is NULL for a non-object.
    check!(r#"SELECT JSON_LENGTH('{"a":1,"b":2}')"#, "2");
    check!("SELECT JSON_LENGTH('[1,2,3]')", "3");
    check!("SELECT JSON_LENGTH('1')", "1");
    check!("SELECT JSON_LENGTH('null')", "1");
    check!("SELECT JSON_LENGTH(NULL)", "NULL");
    check!(r#"SELECT JSON_LENGTH('{"a":{"b":1,"c":2}}','$.a')"#, "2");
    check!(r#"SELECT JSON_LENGTH('{"a":1}','$.zz')"#, "NULL");
    check!(
        r#"SELECT JSON_KEYS('{"z":1,"B":2,"a":3,"A":4,"_":5,"0":6}')"#,
        r#"["0", "A", "B", "_", "a", "z"]"#
    );
    check!(
        r#"SELECT JSON_KEYS('{"bb":1,"a":2,"ccc":3,"dd":4}')"#,
        r#"["a", "bb", "ccc", "dd"]"#
    );
    check!("SELECT JSON_KEYS('{}')", "[]");
    check!("SELECT JSON_KEYS('[1,2]')", "NULL");
    check!("SELECT JSON_KEYS('1')", "NULL");
    check!(
        r#"SELECT JSON_KEYS('{"a":{"z":1,"y":2}}','$.a')"#,
        r#"["y", "z"]"#
    );
    check!("SELECT JSON_DEPTH('1')", "1");
    check!("SELECT JSON_DEPTH('[]')", "1");
    check!("SELECT JSON_DEPTH('{}')", "1");
    check!("SELECT JSON_DEPTH('[1,[2,[3]]]')", "4");
    check!(r#"SELECT JSON_DEPTH('{"a":{"b":{"c":1}}}')"#, "4");
    check!("SELECT JSON_DEPTH(NULL)", "NULL");

    // JSON_VALID never raises: a malformed document, and every non-string
    // SQL value, is simply zero.
    check!("SELECT JSON_VALID('{}')", "1");
    check!("SELECT JSON_VALID('{')", "0");
    check!("SELECT JSON_VALID('abc')", "0");
    check!("SELECT JSON_VALID(' ')", "0");
    check!("SELECT JSON_VALID(1)", "0");
    check!("SELECT JSON_VALID(NULL)", "NULL");

    // CAST(x AS JSON): only the STRING signature parses, so `'abc'` is
    // error 3140 rather than the JSON string "abc" (asserted below).
    check!(
        r#"SELECT CAST('{"b":1,"aa":2,"c":3,"a":4}' AS JSON)"#,
        r#"{"a": 4, "aa": 2, "b": 1, "c": 3}"#
    );
    check!("SELECT CAST(1 AS JSON)", "1");
    check!("SELECT CAST(1.5 AS JSON)", "1.5");
    check!("SELECT CAST(NULL AS JSON)", "NULL");
    // `marshalFloat64To`'s cutoffs: at least one fractional digit inside
    // [1e-15, 1e15), a bare exponent outside it.
    check!(
        "SELECT CAST('[1.0, 1.5, 1e3, 100000000000000000000, -0.0]' AS JSON)",
        "[1.0, 1.5, 1000.0, 1e20, -0.0]"
    );
    check!(
        "SELECT CAST('[0.1,2.5e-10,1e100,3,-3,1.7976931348623157e308]' AS JSON)",
        "[0.1, 0.00000000025, 1e100, 3, -3, 1.7976931348623157e308]"
    );

    // The `json` error class reaches the wire with TiDB's own code.
    let mut code = |sql: &str| match session.run(sql) {
        Err(error) => error.to_mysql_error().code,
        Ok(output) => panic!("expected an error from {sql}, got {output:?}"),
    };
    assert_eq!(code("SELECT JSON_EXTRACT('x','$.a')"), 3140);
    assert_eq!(code("SELECT CAST('abc' AS JSON)"), 3140);
    assert_eq!(code("SELECT JSON_LENGTH('nope')"), 3140);
    assert_eq!(code(r#"SELECT JSON_EXTRACT('{"a":1}','xx')"#), 3143);
    assert_eq!(code(r#"SELECT JSON_EXTRACT('{"a":1}','$.')"#), 3143);
    assert_eq!(code("SELECT JSON_CONTAINS('[1,2]','1','$[*]')"), 3149);
    assert_eq!(code("SELECT JSON_TYPE(1)"), 3146);
    assert_eq!(code("SELECT JSON_QUOTE(1)"), 3064);
    assert_eq!(code("SELECT JSON_OBJECT(NULL,1)"), 3158);
    assert_eq!(
        session
            .run(r#"SELECT JSON_EXTRACT('{"a":1}','xx')"#)
            .unwrap_err()
            .to_mysql_error()
            .message,
        "Invalid JSON path expression. The error is around character position 1."
    );

    // REFUSED because UPSTREAM GO DOES NOT PARSE IT: `JSON_TABLE` has no
    // grammar rule, AST node, or executor anywhere in `pkg/`, so there is
    // no source to transcreate. Evidence and the captured Go parse error
    // live in `json_table_is_unsupported_upstream` below. The mutation
    // family graduated -- see `json_mutation_functions` and
    // `json_column_type` below.
    assert!(
        session
            .run(r#"SELECT * FROM JSON_TABLE('[1]', '$[*]' COLUMNS (v INT PATH '$')) t"#)
            .is_err(),
        "JSON_TABLE should still be refused"
    );
}

/// The JSON MUTATION family, captured from real TiDB through
/// `testkit.CreateMockStore`.
///
/// The rule that is easiest to get wrong -- and that many cases below
/// exist to pin -- is that a mutation's path/value pairs are applied
/// SEQUENTIALLY to the document the previous pair produced, not all
/// against the original. `JSON_REMOVE('[1,2,3]','$[0]','$[0]')` therefore
/// removes two DIFFERENT elements and leaves `[3]`.
///
/// DOCUMENTED DIVERGENCE, unchanged from slice 1: a JSON-returning
/// BUILTIN reports column type `VarString` where TiDB says `JSON`,
/// because this tier's expression datum domain is textual. The VALUES are
/// byte-identical. A JSON COLUMN is a different story -- see
/// `json_column_type`.
#[test]
fn json_mutation_functions() {
    let mut session = Session::new();
    macro_rules! check {
        ($sql:expr, $want:expr) => {
            assert_eq!(
                row_text(session.run($sql)),
                vec![vec![($want).to_owned()]],
                "{}",
                $sql
            );
        };
    }

    // JSON_SET replaces an existing path and creates a missing one;
    // JSON_INSERT only creates; JSON_REPLACE only replaces.
    check!(r#"SELECT JSON_SET('{"a":1}','$.a',2)"#, r#"{"a": 2}"#);
    check!(
        r#"SELECT JSON_SET('{"a":1}','$.b',2)"#,
        r#"{"a": 1, "b": 2}"#
    );
    check!(r#"SELECT JSON_INSERT('{"a":1}','$.a',2)"#, r#"{"a": 1}"#);
    check!(
        r#"SELECT JSON_INSERT('{"a":1}','$.b',2)"#,
        r#"{"a": 1, "b": 2}"#
    );
    check!(r#"SELECT JSON_REPLACE('{"a":1}','$.a',2)"#, r#"{"a": 2}"#);
    check!(r#"SELECT JSON_REPLACE('{"a":1}','$.b',2)"#, r#"{"a": 1}"#);
    // `$` alone replaces the whole document.
    check!(r#"SELECT JSON_SET('{"a":1}','$',2)"#, "2");
    // A VALUE argument does NOT carry ParseToJSONFlag, so an SQL string
    // becomes a JSON STRING rather than a parsed document.
    check!(
        r#"SELECT JSON_SET('{}','$.a','{"x":1}')"#,
        r#"{"a": "{\"x\":1}"}"#
    );
    // NAMED BOUNDARY, and the reason the value rule above matters: a
    // JSON-typed value argument keeps its STRUCTURE in TiDB
    // (`JSON_SET('{}','$.a',CAST('{"x":1}' AS JSON))` is
    // `{"a": {"x": 1}}`), but this tier's CAST produces canonical TEXT,
    // which is indistinguishable from a string literal here and so
    // nests as a JSON string. A JSON COLUMN carries a real BinaryJSON
    // and does keep its structure -- see `json_column_type`.
    // DOCUMENTED DIVERGENCE (the `builtin_ext::json` module doc's typed
    // boolean boundary): TiDB reads `TRUE` through the argument's
    // `IsBooleanFlag` and stores the JSON boolean `true`. This tier's
    // value domain has no boolean datum, so `TRUE` arrives as the
    // integer 1 -- the same value a JSON COLUMN stores for `TRUE` in
    // TiDB itself (`json_column_type` captures that).
    check!(r#"SELECT JSON_SET('{"a":1}','$.a',TRUE)"#, r#"{"a": 1}"#);
    check!(r#"SELECT JSON_SET('{"a":1}','$.a',1.5)"#, r#"{"a": 1.5}"#);
    // An out-of-range array index appends rather than padding.
    check!("SELECT JSON_SET('[1,2,3]','$[5]',9)", "[1, 2, 3, 9]");
    // A scalar document indexes as a one-element array.
    check!("SELECT JSON_SET('1','$.a',2)", "1");
    check!("SELECT JSON_SET('1','$[0]',2)", "2");
    // A missing INTERMEDIATE leg is a no-op: only the LAST leg is
    // created, never a whole missing branch.
    check!(
        r#"SELECT JSON_SET('{"a":{"b":1}}','$.a.c.d',1)"#,
        r#"{"a": {"b": 1}}"#
    );

    // SEQUENTIAL evaluation: the second pair sees the first pair's
    // document. `$.b` does not exist for the first pair, so `$.b.c` is
    // reachable only because the first pair created `$.b` -- and when it
    // created a SCALAR there, `$.b.c` finds no object and does nothing.
    check!(
        r#"SELECT JSON_SET('{"a":1}','$.b',2,'$.b.c',3)"#,
        r#"{"a": 1, "b": 2}"#
    );
    check!("SELECT JSON_SET('[1,2]','$[0]',9,'$[0][0]',8)", "[8, 2]");

    // JSON_REMOVE, whose paths are also sequential: two identical `$[0]`
    // paths remove the FIRST and then the SECOND original element.
    check!("SELECT JSON_REMOVE('[1,2,3]','$[0]')", "[2, 3]");
    check!("SELECT JSON_REMOVE('[1,2,3]','$[0]','$[0]')", "[3]");
    check!("SELECT JSON_REMOVE('[1,2,3]','$[0]','$[1]')", "[2]");
    check!(r#"SELECT JSON_REMOVE('{"a":1,"b":2}','$.a','$.b')"#, "{}");
    check!(r#"SELECT JSON_REMOVE('{"a":1}','$.zz')"#, r#"{"a": 1}"#);
    check!("SELECT JSON_REMOVE('[1,2,3]','$[9]')", "[1, 2, 3]");

    // JSON_ARRAY_APPEND wraps a non-array target in an array first;
    // JSON_ARRAY_INSERT needs an existing array CELL.
    check!("SELECT JSON_ARRAY_APPEND('[1,2]','$',3)", "[1, 2, 3]");
    check!(
        r#"SELECT JSON_ARRAY_APPEND('{"a":[1]}','$.a',2)"#,
        r#"{"a": [1, 2]}"#
    );
    check!(
        r#"SELECT JSON_ARRAY_APPEND('{"a":1}','$.a',2)"#,
        r#"{"a": [1, 2]}"#
    );
    check!("SELECT JSON_ARRAY_APPEND('1','$',2)", "[1, 2]");
    check!(
        "SELECT JSON_ARRAY_APPEND('[[1],[2]]','$[0]',9)",
        "[[1, 9], [2]]"
    );
    check!(
        r#"SELECT JSON_ARRAY_APPEND('{"a":1}','$.zz',2)"#,
        r#"{"a": 1}"#
    );
    // Sequential again: `$` appended 3 first, and `$[0]` then wrapped
    // the ORIGINAL first element.
    check!(
        "SELECT JSON_ARRAY_APPEND('[1,2]','$',3,'$[0]',4)",
        "[[1, 4], 2, 3]"
    );
    check!(
        "SELECT JSON_ARRAY_INSERT('[1,2,3]','$[1]',9)",
        "[1, 9, 2, 3]"
    );
    check!(
        "SELECT JSON_ARRAY_INSERT('[1,2,3]','$[0]',9,'$[0]',8)",
        "[8, 9, 1, 2, 3]"
    );
    check!(
        "SELECT JSON_ARRAY_INSERT('[1,2,3]','$[9]',9)",
        "[1, 2, 3, 9]"
    );
    check!(
        "SELECT JSON_ARRAY_INSERT('[[1,2]]','$[0][1]',9)",
        "[[1, 9, 2]]"
    );

    // MERGE_PATCH deletes a key whose patch value is JSON null;
    // MERGE_PRESERVE wraps two values for the same key in an array.
    check!(
        r#"SELECT JSON_MERGE_PATCH('{"a":1,"b":2}','{"a":null}')"#,
        r#"{"b": 2}"#
    );
    check!(
        r#"SELECT JSON_MERGE_PATCH('{"a":1}','{"b":2}')"#,
        r#"{"a": 1, "b": 2}"#
    );
    check!("SELECT JSON_MERGE_PATCH('[1,2]','[3]')", "[3]");
    check!("SELECT JSON_MERGE_PRESERVE('[1,2]','[3]')", "[1, 2, 3]");
    check!(
        r#"SELECT JSON_MERGE_PRESERVE('{"a":1}','{"a":2}')"#,
        r#"{"a": [1, 2]}"#
    );
    // A MERGE argument IS parsed (unlike a mutation VALUE argument).
    check!("SELECT JSON_MERGE_PRESERVE('1','2')", "[1, 2]");
    check!(
        r#"SELECT JSON_MERGE('{"a":1}','{"b":2}')"#,
        r#"{"a": 1, "b": 2}"#
    );

    // NULL propagation, which differs PER ARGUMENT ROLE:
    //  * a NULL DOCUMENT or a NULL PATH makes the whole call NULL;
    //  * a NULL VALUE is the JSON null scalar and is stored;
    //  * JSON_MERGE* is NULL as soon as ANY argument is NULL.
    check!("SELECT JSON_SET(NULL,'$.a',1)", "NULL");
    check!(r#"SELECT JSON_SET('{"a":1}',NULL,1)"#, "NULL");
    check!(r#"SELECT JSON_SET('{"a":1}','$.a',NULL)"#, r#"{"a": null}"#);
    check!(
        r#"SELECT JSON_INSERT('{"a":1}','$.b',NULL)"#,
        r#"{"a": 1, "b": null}"#
    );
    check!(
        r#"SELECT JSON_REPLACE('{"a":1}','$.a',NULL)"#,
        r#"{"a": null}"#
    );
    check!("SELECT JSON_REMOVE(NULL,'$.a')", "NULL");
    check!(r#"SELECT JSON_REMOVE('{"a":1}',NULL)"#, "NULL");
    check!("SELECT JSON_ARRAY_APPEND(NULL,'$',1)", "NULL");
    check!("SELECT JSON_ARRAY_APPEND('[1]',NULL,1)", "NULL");
    check!("SELECT JSON_ARRAY_APPEND('[1]','$',NULL)", "[1, null]");
    check!("SELECT JSON_ARRAY_INSERT('[1]','$[0]',NULL)", "[null, 1]");
    check!("SELECT JSON_ARRAY_INSERT('[1]',NULL,1)", "NULL");
    check!("SELECT JSON_MERGE(NULL,'[1]')", "NULL");
    check!(r#"SELECT JSON_MERGE_PATCH('{"a":1}',NULL)"#, "NULL");
    check!(r#"SELECT JSON_MERGE_PATCH(NULL,'{"a":1}')"#, "NULL");
    check!(
        r#"SELECT JSON_MERGE_PATCH('{"a":1}',NULL,'{"b":2}')"#,
        "NULL"
    );
    check!(r#"SELECT JSON_MERGE_PRESERVE('{"a":1}',NULL)"#, "NULL");

    // The `json` error class, with TiDB's own codes.
    let mut code = |sql: &str| match session.run(sql) {
        Err(error) => error.to_mysql_error().code,
        Ok(output) => panic!("expected an error from {sql}, got {output:?}"),
    };
    assert_eq!(code(r#"SELECT JSON_SET('{"a":1}','xx',1)"#), 3143);
    assert_eq!(code(r#"SELECT JSON_SET('{"a":1}','$[*]',1)"#), 3149);
    assert_eq!(code(r#"SELECT JSON_SET('{"a":1}','$.*',1)"#), 3149);
    assert_eq!(code(r#"SELECT JSON_SET('{"a":1}','$**.a',1)"#), 3149);
    assert_eq!(code("SELECT JSON_REMOVE('[1]','$[*]')"), 3149);
    assert_eq!(code("SELECT JSON_ARRAY_APPEND('[1]','$[*]',1)"), 3149);
    // `$` is vacuous for REMOVE and not an array cell for ARRAY_INSERT.
    assert_eq!(code(r#"SELECT JSON_REMOVE('{"a":1}','$')"#), 3153);
    assert_eq!(code("SELECT JSON_ARRAY_INSERT('[1]','$',1)"), 3165);
    assert_eq!(code(r#"SELECT JSON_ARRAY_INSERT('{"a":1}','$.a',2)"#), 3165);
    assert_eq!(code(r#"SELECT JSON_SET('nope','$.a',1)"#), 3140);
    assert_eq!(code(r#"SELECT JSON_MERGE_PATCH('nope','{}')"#), 3140);
    // A MERGE argument must be a JSON string or a JSON value.
    assert_eq!(code("SELECT JSON_MERGE_PRESERVE('[1]',3)"), 3146);
    assert_eq!(code(r#"SELECT JSON_MERGE_PATCH('{"a":1}',3)"#), 3146);

    // JSON_MERGE is deprecated: it computes the same value as
    // JSON_MERGE_PRESERVE and adds warning 1681.
    assert_eq!(
        row_text(session.run("SELECT JSON_MERGE('[1]','[2]')")),
        vec![vec!["[1, 2]".to_owned()]]
    );
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        vec![vec![
            "Warning".to_owned(),
            "1681".to_owned(),
            "JSON_MERGE is deprecated and will be removed in a future release.".to_owned(),
        ]]
    );
    // A NULL argument returns before Go appends the warning.
    assert_eq!(
        row_text(session.run("SELECT JSON_MERGE(NULL,'[1]')")),
        vec![vec!["NULL".to_owned()]]
    );
    assert!(row_text(session.run("SHOW WARNINGS")).is_empty());
}

/// The JSON COLUMN TYPE, captured from real TiDB.
///
/// NOT a divergence, unlike the JSON-returning BUILTINS above: a JSON
/// column stores a real `BinaryJSON` in its row and its chunk cell, so
/// the wire reports type `JSON` (245) with the binary charset exactly as
/// TiDB does. The write path is Go `table.CastValue`, which PARSES and
/// CANONICALIZES the written text -- which is why `{"b":2,"a":1}` reads
/// back key-sorted and spaced.
#[test]
fn json_column_type() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE tj (id BIGINT PRIMARY KEY, j JSON)")
        .unwrap();
    for sql in [
        r#"INSERT INTO tj VALUES (1,'{"b":2,"a":1}')"#,
        "INSERT INTO tj VALUES (2,'[1,2,3]')",
        "INSERT INTO tj VALUES (3,NULL)",
        "INSERT INTO tj VALUES (4,'null')",
        r#"INSERT INTO tj VALUES (5,'"str"')"#,
        // A non-string SQL value becomes its own JSON scalar; TRUE is
        // the INTEGER 1, not the JSON boolean.
        "INSERT INTO tj VALUES (6, 7)",
        "INSERT INTO tj VALUES (7, TRUE)",
        "INSERT INTO tj VALUES (8, 1.5)",
    ] {
        session.run(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }

    assert_eq!(
        row_text(session.run("SELECT id, j FROM tj ORDER BY id")),
        vec![
            vec!["1".to_owned(), r#"{"a": 1, "b": 2}"#.to_owned()],
            vec!["2".to_owned(), "[1, 2, 3]".to_owned()],
            vec!["3".to_owned(), "NULL".to_owned()],
            vec!["4".to_owned(), "null".to_owned()],
            vec!["5".to_owned(), r#""str""#.to_owned()],
            vec!["6".to_owned(), "7".to_owned()],
            vec!["7".to_owned(), "1".to_owned()],
            vec!["8".to_owned(), "1.5".to_owned()],
        ]
    );
    assert_eq!(
        row_text(session.run("SELECT JSON_TYPE(j) FROM tj ORDER BY id")),
        ["OBJECT", "ARRAY", "NULL", "NULL", "STRING", "INTEGER", "INTEGER", "DOUBLE",]
            .map(|t| vec![t.to_owned()])
            .to_vec()
    );
    // A JSON column feeds the JSON builtins as a document.
    assert_eq!(
        row_text(session.run(r#"SELECT JSON_SET(j,'$.c',3) FROM tj WHERE id=1"#)),
        vec![vec![r#"{"a": 1, "b": 2, "c": 3}"#.to_owned()]]
    );
    assert_eq!(
        row_text(session.run(r#"SELECT JSON_EXTRACT(j,'$.a') FROM tj WHERE id=1"#)),
        vec![vec!["1".to_owned()]]
    );
    // A column VALUE argument keeps its structure, because it really is
    // a JSON value rather than the canonical text a CAST produces here.
    assert_eq!(
        row_text(session.run(r#"SELECT JSON_SET('{}','$.a',j) FROM tj WHERE id=1"#)),
        vec![vec![r#"{"a": {"a": 1, "b": 2}}"#.to_owned()]]
    );
    assert_eq!(
        row_text(session.run(r#"SELECT id FROM tj WHERE JSON_EXTRACT(j,'$.a') = 1"#)),
        vec![vec!["1".to_owned()]]
    );
    assert_eq!(
        row_text(session.run("SELECT id FROM tj WHERE j IS NULL")),
        vec![vec!["3".to_owned()]]
    );

    // The wire type: `JSON` (245), binary charset, like TiDB.
    let StmtOutput::Rows { columns, .. } = session
        .run_with_columns("SELECT j FROM tj WHERE id=1")
        .unwrap()
    else {
        panic!("expected rows");
    };
    assert_eq!(columns[0].1.code(), tidb_datatype::FieldTypeCode::Json);
    assert_eq!(columns[0].1.charset_name(), "binary");

    // A malformed document is the PARSER's own 3140, not the generic
    // 1366 every other bad column value reports -- and it stays an
    // error, because there is no truncated document to store.
    macro_rules! failure {
        ($sql:expr) => {
            match session.run($sql) {
                Err(error) => error.to_mysql_error(),
                Ok(output) => panic!("expected an error from {}, got {output:?}", $sql),
            }
        };
    }
    assert_eq!(failure!("INSERT INTO tj VALUES (9,'nope')").code, 3140);
    assert_eq!(
        failure!("INSERT INTO tj VALUES (10,'')").message,
        "Invalid JSON text: The document is empty"
    );
    // A JSON column can be neither indexed nor given a default.
    assert_eq!(failure!("CREATE TABLE tj3 (j JSON, KEY(j))").code, 3152);
    assert_eq!(failure!("CREATE TABLE tj4 (j JSON PRIMARY KEY)").code, 3152);
    assert_eq!(
        failure!(r#"CREATE TABLE tj9 (j JSON DEFAULT '{}')"#).code,
        1101
    );
    // DEFAULT NULL is the one default a JSON column may carry.
    session
        .run("CREATE TABLE tj2 (j JSON DEFAULT NULL, k JSON NOT NULL)")
        .unwrap();

    // UPDATE writes a mutated document back through the same cast.
    session
        .run(r#"UPDATE tj SET j = JSON_SET(j,'$.z',1) WHERE id=1"#)
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT j FROM tj WHERE id=1")),
        vec![vec![r#"{"a": 1, "b": 2, "z": 1}"#.to_owned()]]
    );

    // SHOW reports the declared type.
    assert_eq!(
        row_text(session.run("SHOW COLUMNS FROM tj"))[1][..2],
        ["j".to_owned(), "json".to_owned()]
    );
    assert!(row_text(session.run("SHOW CREATE TABLE tj"))[0][1].contains("`j` json DEFAULT NULL"));
}
