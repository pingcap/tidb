//! The charset TRANSCODING boundary, checked against the TiDB capture
//! (`pkg/executor/zz_dump_transcode_test.go`).
//!
//! Every expectation here is a captured `ROWS`/`ERR` line, not a guess. The
//! architecture they pin down is documented in
//! `tidb_expr::convert_charset`: a character string is ALWAYS stored and
//! evaluated as UTF-8, whatever its declared charset, and the only implicit
//! transcode is the `to_binary` wrap Go's `HandleBinaryLiteral` puts on a
//! non-legacy-charset argument of a binary-aware function.
#![cfg(test)]

use crate::tests_support::row_text;
use crate::*;

fn charset_session() -> Session {
    let mut session = Session::new();
    for sql in [
        "CREATE TABLE g1 (a VARCHAR(20) CHARSET gbk)",
        "CREATE TABLE g3 (a VARCHAR(20) CHARSET gbk COLLATE gbk_chinese_ci)",
        "CREATE TABLE u1 (a VARCHAR(20) CHARSET utf8mb4)",
        "CREATE TABLE l1 (a VARCHAR(20) CHARSET latin1)",
        "CREATE TABLE a1 (a VARCHAR(20) CHARSET ascii)",
        "CREATE TABLE b1 (a VARCHAR(20) CHARSET gb18030)",
    ] {
        session.run(sql).unwrap();
    }
    session
}

fn one(session: &mut Session, sql: &str) -> String {
    row_text(session.run(sql))[0][0].clone()
}

fn error_code(session: &mut Session, sql: &str) -> u16 {
    session.run(sql).unwrap_err().to_mysql_error().code
}

/// The storage verdict. A `gbk` column holds the UTF-8 bytes of its value:
/// `HEX` reports the GBK form only because it transcodes at the boundary,
/// while `CHAR_LENGTH` -- which does NOT transcode -- counts the same two
/// characters for both columns. Captured: `D2BBC1D0`/4/2 for the gbk column
/// and `E4B880E58897`/6/2 for the utf8mb4 one.
#[test]
fn gbk_column_stores_utf8_and_transcodes_at_the_boundary() {
    let mut session = charset_session();
    session
        .run("INSERT INTO g1 VALUES ('\u{4e00}\u{5217}')")
        .unwrap();
    session
        .run("INSERT INTO u1 VALUES ('\u{4e00}\u{5217}')")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT HEX(a), LENGTH(a), CHAR_LENGTH(a), ASCII(a) FROM g1")),
        vec![vec!["D2BBC1D0", "4", "2", "210"]]
    );
    assert_eq!(
        row_text(session.run("SELECT HEX(a), LENGTH(a), CHAR_LENGTH(a) FROM u1")),
        vec![vec!["E4B880E58897", "6", "2"]]
    );
    // The value itself still reads back as the original characters.
    assert_eq!(one(&mut session, "SELECT a FROM g1"), "\u{4e00}\u{5217}");
    // `CAST(x AS BINARY)` is the same wrap through Go's `funcPropAuto` arm.
    assert_eq!(
        row_text(session.run("SELECT HEX(CAST(a AS BINARY)), LENGTH(CAST(a AS BINARY)) FROM g1")),
        vec![vec!["D2BBC1D0", "4"]]
    );
}

/// A character the column's charset cannot represent is 1366 at write time,
/// not mangled bytes. Captured: `Incorrect string value '\xF0\x9F\x98\x89'`
/// for `😉` and `'\xE2\x82\xAC'` for `€` (which GBK rejects through the
/// `customGBKEncoder` special case), while `一列` is accepted.
#[test]
fn unrepresentable_character_is_rejected_at_write_time() {
    let mut session = charset_session();
    assert_eq!(
        error_code(&mut session, "INSERT INTO g1 VALUES ('\u{1f609}')"),
        1366
    );
    assert_eq!(
        error_code(&mut session, "INSERT INTO g1 VALUES ('\u{20ac}')"),
        1366
    );
    assert_eq!(
        error_code(&mut session, "INSERT INTO a1 VALUES ('\u{4e00}')"),
        1366
    );
    session
        .run("INSERT INTO g1 VALUES ('\u{4e00}\u{5217}')")
        .unwrap();
    session
        .run("INSERT INTO b1 VALUES ('\u{4e00}\u{5217}')")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT HEX(a), LENGTH(a) FROM b1")),
        vec![vec!["D2BBC1D0", "4"]]
    );
}

/// `CONVERT(x USING cs)` RETAGS rather than transcodes: converting to gbk and
/// straight back to utf8mb4 returns the identical bytes, and an
/// unrepresentable character becomes `?` with no error. Captured:
/// `D2BBC1D0`, `E4B880E58897`, `?`.
#[test]
fn convert_using_retags_and_replaces() {
    let mut session = charset_session();
    assert_eq!(
        row_text(session.run(
            "SELECT HEX(CONVERT('\u{4e00}\u{5217}' USING gbk)), \
             LENGTH(CONVERT('\u{4e00}\u{5217}' USING gbk))"
        )),
        vec![vec!["D2BBC1D0", "4"]]
    );
    assert_eq!(
        one(
            &mut session,
            "SELECT HEX(CONVERT(CONVERT('\u{4e00}\u{5217}' USING gbk) USING utf8mb4))"
        ),
        "E4B880E58897"
    );
    assert_eq!(
        one(&mut session, "SELECT CONVERT('\u{1f609}' USING gbk)"),
        "?"
    );
    assert_eq!(
        one(&mut session, "SELECT HEX(CONVERT('\u{4e00}' USING ascii))"),
        "3F"
    );
}

/// TiDB's `latin1` is a byte-preserving alias for UTF-8, not ISO-8859-1
/// (`pkg/parser/charset/encoding_latin1.go` builds it on `encoding.Nop` with
/// an always-true `IsValid`), so it is a LEGACY charset that never transcodes.
/// Captured: `HEX('é')` in a latin1 column is `C3A9`, a raw `0xE9` stays
/// `E9`, and even a character outside Latin-1 survives untouched.
#[test]
fn latin1_is_a_byte_preserving_utf8_alias() {
    let mut session = charset_session();
    session.run("INSERT INTO l1 VALUES ('\u{e9}')").unwrap();
    assert_eq!(
        row_text(session.run("SELECT HEX(a), LENGTH(a), CHAR_LENGTH(a) FROM l1")),
        vec![vec!["C3A9", "2", "1"]]
    );
    session.run("INSERT INTO l1 VALUES (0xE9)").unwrap();
    session.run("INSERT INTO l1 VALUES ('\u{4e00}')").unwrap();
    assert_eq!(
        row_text(session.run("SELECT HEX(a) FROM l1")),
        vec![vec!["C3A9"], vec!["E9"], vec!["E4B880"]]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT HEX(CONVERT('\u{e9}' USING latin1)), HEX(CONVERT('\u{4e00}' USING latin1))"
        )),
        vec![vec!["C3A9", "E4B880"]]
    );
}

/// A statement as the client's raw bytes: the wire carries a byte that is not
/// UTF-8 as its `raw_bytes` escape (`decode_client_sql`).
fn run_bytes(session: &mut Session, sql: &[u8]) -> Result<StmtResult, DriverError> {
    session.run(&tidb_hack::raw_bytes::bytes_to_str(sql))
}

/// Go scans a latin1, binary, or utf8mb4 client's SQL bytes raw
/// (`encodingLatin1`/`encodingBin` are `encoding.Nop`, and a UTF-8 client's
/// literal is kept as written), converting a string literal only into the
/// connection charset. Every expectation is a TiDB capture (testkit, the same
/// statements in order): a utf8mb4 client stores a raw `0xE9` in a latin1
/// column and is refused 1366 by a utf8mb4 one; a latin1 client on a utf8mb4
/// connection (the stock `mysql --default-character-set=latin1` handshake)
/// sends `0xE9` as `?` and terminal UTF-8 unchanged -- never double-encoded;
/// `SET NAMES latin1`/`binary` keep `0xE9` as is.
#[test]
fn noop_client_charsets_keep_the_raw_literal_bytes() {
    let mut session = charset_session();
    let glyphs = "\u{2514}\u{2500}\u{e9}".as_bytes();
    let with_glyphs = |prefix: &[u8], suffix: &[u8]| [prefix, glyphs, suffix].concat();
    let incorrect = |session: &mut Session, sql: &[u8]| {
        let error = run_bytes(session, sql).unwrap_err().to_mysql_error();
        assert_eq!(
            (error.code, error.message.as_str()),
            (1366, "Incorrect string value '\\xE9' for column 'a'"),
            "{sql:?}"
        );
    };

    // utf8mb4 client, strict mode.
    incorrect(&mut session, b"INSERT INTO u1 VALUES ('\xe9')");
    incorrect(&mut session, b"INSERT INTO u1 VALUES ('a\xe9b')");
    run_bytes(&mut session, b"INSERT INTO l1 VALUES ('\xe9')").unwrap();
    assert_eq!(
        row_text(run_bytes(
            &mut session,
            b"SELECT HEX('\xe9'), HEX('a\xe9b')"
        )),
        vec![vec!["E9", "61E962"]]
    );
    session.run("SET sql_mode = ''").unwrap();
    run_bytes(&mut session, b"INSERT INTO u1 VALUES ('a\xe9b')").unwrap();
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        vec![vec![
            "Warning",
            "1366",
            "Incorrect string value '\\xE9' for column 'a'"
        ]]
    );
    assert_eq!(
        row_text(session.run("SELECT HEX(a) FROM u1")),
        vec![vec!["613F62"]]
    );
    session.run("SET sql_mode = DEFAULT").unwrap();

    // latin1 client, utf8mb4 connection.
    session.run("SET character_set_client = latin1").unwrap();
    assert_eq!(
        row_text(run_bytes(
            &mut session,
            &with_glyphs(b"SELECT HEX('\xe9'), HEX('", b"')")
        )),
        vec![vec!["3F", "E29494E29480C3A9"]]
    );
    run_bytes(&mut session, b"INSERT INTO l1 VALUES ('\xe9')").unwrap();
    run_bytes(
        &mut session,
        &with_glyphs(b"INSERT INTO u1 VALUES ('", b"')"),
    )
    .unwrap();

    // SET NAMES latin1.
    session.run("SET NAMES latin1").unwrap();
    assert_eq!(
        row_text(run_bytes(
            &mut session,
            &with_glyphs(b"SELECT HEX('\xe9'), HEX('", b"'), COLLATION('\xe9')")
        )),
        vec![vec!["E9", "E29494E29480C3A9", "latin1_bin"]]
    );
    run_bytes(&mut session, b"INSERT INTO l1 VALUES ('\xe9')").unwrap();
    run_bytes(
        &mut session,
        &with_glyphs(b"INSERT INTO l1 VALUES ('", b"')"),
    )
    .unwrap();
    incorrect(&mut session, b"INSERT INTO u1 VALUES ('\xe9')");
    run_bytes(
        &mut session,
        &with_glyphs(b"INSERT INTO u1 VALUES ('", b"')"),
    )
    .unwrap();

    // SET NAMES binary.
    session.run("SET NAMES binary").unwrap();
    assert_eq!(
        row_text(run_bytes(
            &mut session,
            b"SELECT HEX('\xe9'), COLLATION('\xe9')"
        )),
        vec![vec!["E9", "binary"]]
    );
    incorrect(&mut session, b"INSERT INTO u1 VALUES ('\xe9')");
    run_bytes(&mut session, b"INSERT INTO l1 VALUES ('\xff')").unwrap();

    session.run("SET NAMES utf8mb4").unwrap();
    assert_eq!(
        row_text(session.run("SELECT HEX(a) FROM l1")),
        vec![
            vec!["E9"],
            vec!["3F"],
            vec!["E9"],
            vec!["E29494E29480C3A9"],
            vec!["FF"]
        ]
    );
    assert_eq!(
        row_text(session.run("SELECT HEX(a) FROM u1")),
        vec![
            vec!["613F62"],
            vec!["E29494E29480C3A9"],
            vec!["E29494E29480C3A9"]
        ]
    );
}

/// Go `Scanner.convert2System`: a utf8mb4 client's identifier turns an
/// invalid byte into `?` with warning 1300; a latin1 client's keeps it with
/// no warning. Captured: `select 1 as \`a\xE9\`` warns
/// `Invalid utf8mb4 character string: 'E9'` only under the utf8mb4 client.
#[test]
fn client_identifier_bytes_convert_like_go() {
    let mut session = Session::new();
    run_bytes(&mut session, b"SELECT 1 AS `a\xe9`").unwrap();
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        vec![vec![
            "Warning",
            "1300",
            "Invalid utf8mb4 character string: 'E9'"
        ]]
    );
    session.run("SET character_set_client = latin1").unwrap();
    run_bytes(&mut session, b"SELECT 1 AS `a\xe9`").unwrap();
    assert!(row_text(session.run("SHOW WARNINGS")).is_empty());
}

/// Runs a client's SQL bytes the way the wire carries them for the
/// session's `@@character_set_client`.
fn run_client_bytes(session: &mut Session, sql: &[u8]) -> Result<StmtResult, DriverError> {
    let client = session.vars.build_parser_config().charset_client;
    session.run(&tidb_parser::carry_client_sql(sql, client))
}

fn mysql_error(result: Result<StmtResult, DriverError>) -> (u16, String) {
    let error = result.unwrap_err().to_mysql_error();
    (error.code, error.message)
}

/// Go `Scanner.convert2Connection` for an ascii client, live TiDB captures
/// in order: under the default strict `sql_mode` and `SET NAMES ascii` a
/// non-ASCII literal is `ERROR 1300`; without strict mode it is `?` with a
/// 1300 warning naming the first group; a utf8mb4 connection only warns; an
/// identifier becomes `?` (`Unknown column '?'`), and the failed statement's
/// parser warning is dropped (Go `handleStmt` appends parser warnings only
/// after success).
#[test]
fn ascii_client_literals_replace_or_refuse_like_go() {
    let mut session = Session::new();
    session.run("SET NAMES ascii").unwrap();
    assert_eq!(
        mysql_error(run_client_bytes(&mut session, "SELECT HEX('é')".as_bytes())),
        (1300, "Invalid ascii character string: 'C3A9'".to_owned())
    );
    session.run("SET sql_mode = ''").unwrap();
    assert_eq!(
        row_text(run_client_bytes(
            &mut session,
            "SELECT HEX('éx\u{e9}')".as_bytes()
        )),
        vec![vec!["3F783F"]]
    );
    let warning = vec![vec![
        "Warning",
        "1300",
        "Invalid ascii character string: 'C3A9'",
    ]];
    assert_eq!(row_text(session.run("SHOW WARNINGS")), warning);
    session.run("SET sql_mode = DEFAULT").unwrap();
    session
        .run("SET character_set_connection = utf8mb4")
        .unwrap();
    assert_eq!(
        row_text(run_client_bytes(&mut session, "SELECT HEX('é')".as_bytes())),
        vec![vec!["3F"]]
    );
    assert_eq!(row_text(session.run("SHOW WARNINGS")), warning);
    assert_eq!(
        mysql_error(run_client_bytes(&mut session, "SELECT é".as_bytes())),
        (1054, "Unknown column '?' in 'field list'".to_owned())
    );
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        vec![vec!["Error", "1054", "Unknown column '?' in 'field list'"]]
    );
}

/// A gbk client's bytes are scanned raw, live TiDB captures in order:
/// `SET NAMES gbk; SELECT HEX('\xE9')` is `ERROR 1300 (HY000): Invalid gbk
/// character string: 'E9'` (it was `1064 COM_QUERY is not valid UTF-8`
/// here), `HEX('\xD6\xD0')` is `D6D0`, and `HEX('\x95\x5C')` -- whose trail
/// byte is no backslash escape -- `955C`; on a utf8mb4 connection
/// `HEX('\xE9')` is `3F` with the warning and `HEX('\x95\x5C')` `E6989E`.
#[test]
fn gbk_client_literals_like_go() {
    let mut session = Session::new();
    session.run("SET NAMES gbk").unwrap();
    assert_eq!(
        mysql_error(run_client_bytes(&mut session, b"SELECT HEX('\xe9')")),
        (1300, "Invalid gbk character string: 'E9'".to_owned())
    );
    assert_eq!(
        row_text(run_client_bytes(
            &mut session,
            b"SELECT HEX('\xd6\xd0'), HEX('\x95\x5c')"
        )),
        vec![vec!["D6D0", "955C"]]
    );
    session
        .run("SET character_set_connection = utf8mb4")
        .unwrap();
    assert_eq!(
        row_text(run_client_bytes(
            &mut session,
            b"SELECT HEX('\xe9'), HEX('\x95\x5c')"
        )),
        vec![vec!["3F", "E6989E"]]
    );
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        vec![vec![
            "Warning",
            "1300",
            "Invalid gbk character string: 'E9'"
        ]]
    );
}

/// Go publishes a statement's `Text()` to the process list and keeps it as
/// `StmtCtx.OriginalSQL` (slow log, statement summary): the client's bytes
/// decoded with `@@character_set_client`, a non-printable literal in hex.
/// Live TiDB `information_schema.PROCESSLIST.INFO` captures: a latin1
/// client's `SELECT SLEEP(3), '\xE9', 'a<TAB>b', '\xC3\xA9' /* x\xE9 */` is
/// `SELECT SLEEP(3), 0xe9, 0x610962, '\xC3\xA9' /* x\xE9 */` (the comment's
/// byte raw), a utf8mb4 client's `'a<TAB>b', "q<LF>"` is `0x610962,
/// 0x710a`, and a gbk client's `'\xD6\xD0', '\x95\x5C'` is UTF-8
/// `'中', '昞'`.
#[test]
fn statement_text_is_go_text() {
    let mut session = Session::new();
    let text = |session: &Session, sql: &[u8]| {
        let client = session.vars.build_parser_config().charset_client;
        let carried = tidb_parser::carry_client_sql(sql, client);
        tidb_hack::raw_bytes::str_to_bytes(&session.statement_text(&carried)).into_owned()
    };
    assert_eq!(text(&session, b"SELECT 1"), b"SELECT 1");
    assert_eq!(
        text(&session, b"SELECT SLEEP(2), 'a\tb', \"q\n\" , 'ok'"),
        b"SELECT SLEEP(2), 0x610962, 0x710a , 'ok'"
    );
    session.run("SET NAMES latin1").unwrap();
    assert_eq!(
        text(
            &session,
            b"SELECT SLEEP(3), '\xe9', 'a\tb', '\xc3\xa9' /* x\xe9 */"
        ),
        b"SELECT SLEEP(3), 0xe9, 0x610962, '\xc3\xa9' /* x\xe9 */"
    );
    session.run("SET NAMES gbk").unwrap();
    assert_eq!(
        text(&session, b"SELECT SLEEP(2), '\xd6\xd0', '\x95\x5c'"),
        "SELECT SLEEP(2), '中', '昞'".as_bytes()
    );
}

/// `CHARSET gbk` with no `COLLATE` resolves to `gbk_chinese_ci`, not
/// `gbk_bin` -- TiDB leaves the Chinese charsets at their registry default.
/// Captured: `ORDER BY` on the gbk column follows GBK weights (`列` before
/// `一`, the reverse of Unicode code-point order), and the `_ci` comparison
/// folds case.
#[test]
fn gbk_defaults_to_chinese_ci_and_orders_by_gbk_weight() {
    let mut session = charset_session();
    session
        .run("INSERT INTO g1 VALUES ('\u{4e00}'),('\u{5217}'),('b'),('A')")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT a, HEX(a) FROM g1 ORDER BY a")),
        vec![
            vec!["A", "41"],
            vec!["b", "62"],
            vec!["\u{5217}", "C1D0"],
            vec!["\u{4e00}", "D2BB"],
        ]
    );
    session.run("INSERT INTO g3 VALUES ('a'),('A')").unwrap();
    assert_eq!(
        row_text(session.run("SELECT COUNT(*) FROM g3 WHERE a = 'A'")),
        vec![vec!["2"]]
    );
}

/// Go `ResetContextOfStmt` writes `tidb_skip_utf8_check` and
/// `tidb_skip_ascii_check` into the statement's TypeFlags, which a column
/// write casts under: with the check skipped, strict mode stores the invalid
/// bytes as written (oracle-captured, `executor/statement_context`).
#[test]
fn skip_string_checks_reach_column_writes() {
    let mut session = Session::new();
    for sql in [
        "set sql_mode = 'STRICT_TRANS_TABLES'",
        "create table sc2 (a varchar(255))",
        "create table sc3 (a varchar(255)) charset ascii",
    ] {
        session.run(sql).unwrap();
    }
    for (table, variable) in [
        ("sc2", "tidb_skip_utf8_check"),
        ("sc3", "tidb_skip_ascii_check"),
    ] {
        let error = session
            .run(&format!("insert {table} values (unhex('4040ffff'))"))
            .unwrap_err()
            .to_mysql_error();
        assert_eq!(error.code, 1366, "{table}: {}", error.message);
        session.run(&format!("set @@{variable} = '1'")).unwrap();
        session
            .run(&format!("insert {table} values (unhex('4040ffff'))"))
            .unwrap();
        session.run(&format!("set @@{variable} = '0'")).unwrap();
        assert_eq!(
            crate::tests_support::row_text(session.run(&format!("select length(a) from {table}"))),
            [["4"]],
            "{table}"
        );
    }
}

/// The client's SQL bytes as the wire carries them for the session's
/// `@@character_set_client`.
fn client_sql(session: &Session, sql: &[u8]) -> String {
    let client = session.vars.build_parser_config().charset_client;
    tidb_parser::carry_client_sql(sql, client).into_owned()
}

/// Go evaluates `EXECUTE ... USING @v` through the variable's `getvar`
/// function, whose string result carries the variable's type's collation:
/// live Go with a gbk client, `SET @v = '中'; PREPARE s FROM 'SELECT HEX(?),
/// COLLATION(?)'; EXECUTE s USING @v, @v` answers `D6D0 | gbk_chinese_ci`
/// (`E4B8AD | utf8mb4_bin` here before).
#[test]
fn execute_using_binds_the_user_variable_collation() {
    let mut session = Session::new();
    session.run("SET NAMES gbk").unwrap();
    let set = client_sql(&session, b"SET @v = '\xd6\xd0'");
    session.run(&set).unwrap();
    session
        .run("PREPARE s FROM 'SELECT HEX(?), COLLATION(?)'")
        .unwrap();
    assert_eq!(
        row_text(session.run("EXECUTE s USING @v, @v")),
        [["D6D0", "gbk_chinese_ci"]]
    );
}

/// Go compares identifiers by `CIStr.L`, `strings.ToLower` of what was
/// written, which folds non-ASCII letters and reads each byte that is not
/// UTF-8 as U+FFFD. Live Go: `SELECT COUNT(*) FROM t AS Σ WHERE σ.a = 1`,
/// a latin1 client's `... AS \xC9 WHERE \xE9.a = 1` and `... AS \xC4 WHERE
/// \xE4.a = 1`, and `SELECT a AS Ä FROM t GROUP BY ä` all resolve.
#[test]
fn identifier_comparisons_fold_like_go_to_lower() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE fold_t (a INT PRIMARY KEY)")
        .unwrap();
    session.run("INSERT INTO fold_t VALUES (1), (2)").unwrap();
    for sql in [
        "SELECT COUNT(*) FROM fold_t AS Σ WHERE σ.a = 1",
        "SELECT COUNT(*) FROM fold_t AS x WHERE test.X.a = 1",
        "SELECT COUNT(*) FROM (SELECT a AS ä FROM fold_t) AS Ä WHERE Ä.Ä = 1",
    ] {
        assert_eq!(row_text(session.run(sql)), [["1"]], "{sql}");
    }
    assert_eq!(
        row_text(session.run("SELECT a AS Ä FROM fold_t GROUP BY ä ORDER BY 1")),
        [["1"], ["2"]]
    );
    session.run("SET NAMES latin1").unwrap();
    for sql in [
        &b"SELECT COUNT(*) FROM fold_t AS \xc9 WHERE \xe9.a = 1"[..],
        b"SELECT COUNT(*) FROM fold_t AS \xc4 WHERE \xe4.a = 1",
    ] {
        let sql = client_sql(&session, sql);
        assert_eq!(row_text(session.run(&sql)), [["1"]], "{sql}");
    }
}

/// A latin1 client's `'d\xE9'` is the Go string `d`, `E9`. Go oracle
/// (testkit, `SET NAMES latin1`): a utf8mb4 VARCHAR's `DEFAULT 'd\xE9'` is
/// 1067 (the bytes are not utf8mb4), an ENUM/SET whose member is that same
/// literal takes it as its default, and a default naming no member is 1067.
#[test]
fn latin1_client_enum_members_keep_their_bytes() {
    let mut session = Session::new();
    session.run("SET NAMES latin1").unwrap();
    for (sql, expected) in [
        (
            &b"CREATE TABLE l1 (c VARCHAR(10) DEFAULT 'd\xe9')"[..],
            Some(1067),
        ),
        (
            b"CREATE TABLE l2 (c ENUM('d\xe9', 'x') DEFAULT 'd\xe9')",
            None,
        ),
        (
            b"CREATE TABLE l3 (c SET('d\xe9', 'x') DEFAULT 'd\xe9')",
            None,
        ),
        (
            b"CREATE TABLE l4 (c ENUM('a', 'b') DEFAULT 'd\xe9')",
            Some(1067),
        ),
    ] {
        let sql = client_sql(&session, sql);
        match expected {
            None => {
                session.run(&sql).unwrap();
            }
            Some(code) => {
                assert_eq!(error_code(&mut session, &sql), code, "{sql}");
            }
        }
    }
    session.run("INSERT INTO l2 VALUES ()").unwrap();
    assert_eq!(row_text(session.run("SELECT c + 0 FROM l2")), [["1"]]);
}

/// Go reloads a created table from its stored meta JSON, and `json.Marshal`
/// writes each byte that is not UTF-8 as U+FFFD, so the ENUM/SET member and
/// default a latin1 client wrote as `'d\xE9'` are `d\u{FFFD}` once the DDL
/// returns -- validated against the raw bytes first, stored replaced. A LOCAL
/// temporary table never goes through meta and keeps the raw bytes. Go
/// oracle, the same statements on one connection.
#[test]
fn latin1_client_enum_members_reload_from_stored_json() {
    let mut session = Session::new();
    session.run("SET NAMES latin1").unwrap();
    let sql = client_sql(
        &session,
        b"CREATE TABLE e (c ENUM('d\xe9','x') DEFAULT 'd\xe9', s SET('d\xe9','y'))",
    );
    session.run(&sql).unwrap();
    assert_eq!(
        row_text(session.run(
            "SELECT HEX(column_type), HEX(column_default) FROM information_schema.columns \
             WHERE table_schema = 'test' AND table_name = 'e' ORDER BY ordinal_position"
        )),
        [
            ["656E756D282764EFBFBD272C27782729", "64EFBFBD"],
            ["736574282764EFBFBD272C27792729", "NULL"]
        ]
    );
    session.run("INSERT INTO e VALUES ('x', 'y')").unwrap();
    session.run("INSERT INTO e VALUES (DEFAULT, DEFAULT)").unwrap();
    assert_eq!(
        row_text(session.run("SELECT HEX(c), HEX(s) FROM e")),
        [["78", "79"], ["64EFBFBD", "NULL"]]
    );
    let sql = client_sql(&session, b"SELECT HEX(c) FROM e WHERE c = 'd\xe9'");
    assert!(row_text(session.run(&sql)).is_empty());

    // ALTER goes through a job and meta as well.
    let sql = client_sql(
        &session,
        b"ALTER TABLE e ADD COLUMN c2 ENUM('d\xe9','z') DEFAULT 'd\xe9'",
    );
    session.run(&sql).unwrap();
    assert_eq!(
        row_text(session.run("SELECT HEX(c2), c2 + 0 FROM e")),
        [["64EFBFBD", "1"], ["64EFBFBD", "1"]]
    );

    let sql = client_sql(
        &session,
        b"CREATE TEMPORARY TABLE te (c ENUM('d\xe9','x') DEFAULT 'd\xe9')",
    );
    session.run(&sql).unwrap();
    session.run("INSERT INTO te VALUES (DEFAULT)").unwrap();
    assert_eq!(
        row_text(session.run("SELECT HEX(c), c + 0 FROM te")),
        [["64E9", "1"]]
    );
    let sql = client_sql(&session, b"SELECT HEX(c) FROM te WHERE c = 'd\xe9'");
    assert_eq!(row_text(session.run(&sql)), [["64E9"]]);
}

/// Go reloads the WHOLE `TableInfo` a DDL job installs from its stored meta
/// JSON, not only its columns: a latin1 client's column name `d\xE9`, index
/// name and comment, table comment, partition name and comment and generated
/// expression text all read back with U+FFFD for the byte, at CREATE TABLE
/// and at ALTER TABLE ... ADD INDEX alike. Go oracle, the same statements on
/// one connection.
#[test]
fn latin1_client_identifiers_and_comments_reload_from_stored_json() {
    let mut session = Session::new();
    session.run("SET NAMES latin1").unwrap();
    let sql = client_sql(
        &session,
        b"CREATE TABLE nu (a INT, d\xe9 INT, b VARCHAR(10), g VARCHAR(20) AS (CONCAT(b, 'x\xe9')), \
          KEY i\xe9 (a) COMMENT 'k\xe9') COMMENT 't\xe9' PARTITION BY RANGE (a) (PARTITION p\xe9 \
          VALUES LESS THAN (10) COMMENT 'pc\xe9', PARTITION p1 VALUES LESS THAN MAXVALUE)",
    );
    session.run(&sql).unwrap();
    session.run("SET NAMES utf8mb4").unwrap();
    // Go's generated text is `concat(`b`, _latin1'x\u{FFFD}')`; this crate
    // still restores the literal's introducer as `_utf8mb4`, a separate
    // divergence, so only the replaced literal bytes are pinned here.
    let columns = row_text(session.run(
        "SELECT HEX(COLUMN_NAME), HEX(GENERATION_EXPRESSION) FROM information_schema.columns \
         WHERE table_schema = 'test' AND table_name = 'nu' ORDER BY ORDINAL_POSITION",
    ));
    assert_eq!(
        columns
            .iter()
            .map(|row| row[0].as_str())
            .collect::<Vec<_>>(),
        ["61", "64EFBFBD", "62", "67"]
    );
    assert!(columns[3][1].ends_with("2778EFBFBD2729"), "{columns:?}");
    assert_eq!(
        row_text(session.run(
            "SELECT HEX(TABLE_COMMENT) FROM information_schema.tables WHERE table_schema = \
             'test' AND table_name = 'nu'"
        )),
        [["74EFBFBD"]]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT HEX(PARTITION_NAME), HEX(PARTITION_COMMENT) FROM \
             information_schema.partitions WHERE table_schema = 'test' AND table_name = 'nu'"
        )),
        [["70EFBFBD", "7063EFBFBD"], ["7031", ""]]
    );
    session.run("SET NAMES latin1").unwrap();
    let sql = client_sql(
        &session,
        b"ALTER TABLE nu ADD INDEX j\xe9 (b) COMMENT 'jc\xe9'",
    );
    session.run(&sql).unwrap();
    session.run("SET NAMES utf8mb4").unwrap();
    assert_eq!(
        row_text(session.run(
            "SELECT HEX(INDEX_NAME), HEX(INDEX_COMMENT) FROM information_schema.statistics \
             WHERE table_schema = 'test' AND table_name = 'nu' ORDER BY INDEX_NAME"
        )),
        [["69EFBFBD", "6BEFBFBD"], ["6AEFBFBD", "6A63EFBFBD"]]
    );
}

/// Go's parser types a plain string literal with the session's
/// `character_set_connection`, so the generated expression a latin1 client
/// wrote restores as `concat(`b`, _latin1'x\u{FFFD}')` (the byte replaced
/// by the meta JSON reload). Go oracle, the same statements.
#[test]
fn latin1_client_generated_text_carries_the_connection_introducer() {
    let mut session = Session::new();
    session.run("SET NAMES latin1").unwrap();
    let sql = client_sql(
        &session,
        b"CREATE TABLE g1 (b VARCHAR(10), g VARCHAR(20) AS (CONCAT(b, 'x\xe9')))",
    );
    session.run(&sql).unwrap();
    session.run("SET NAMES utf8mb4").unwrap();
    assert_eq!(
        row_text(session.run(
            "SELECT HEX(GENERATION_EXPRESSION) FROM information_schema.columns WHERE \
             table_schema = 'test' AND table_name = 'g1' AND column_name = 'g'"
        )),
        [["636F6E636174286062602C205F6C6174696E312778EFBFBD2729"]]
    );
}

/// Go reloads a partitioned table's `PartitionInfo` from meta JSON and
/// rebuilds its routing from the reloaded bound TEXT: a latin1 client's LIST
/// COLUMNS bound `'x\xE9'` becomes `'x\u{FFFD}'`, so the row `'x\xE9'` no
/// longer has a partition (1526), and the RANGE COLUMNS bound `'m\xE9'`
/// routes by `'m\u{FFFD}'`. Go oracle, the same statements.
#[test]
fn latin1_client_partition_bounds_route_by_their_reloaded_text() {
    let mut session = Session::new();
    session.run("SET NAMES latin1").unwrap();
    for sql in [
        &b"CREATE TABLE p1 (a INT, b VARCHAR(10)) CHARSET latin1 PARTITION BY LIST COLUMNS (b) \
           (PARTITION p0 VALUES IN ('x\xe9', 'q'), PARTITION p1 VALUES IN ('z'))"[..],
        &b"CREATE TABLE p2 (a INT, b VARCHAR(10)) CHARSET latin1 PARTITION BY RANGE COLUMNS (b) \
           (PARTITION p0 VALUES LESS THAN ('m\xe9'), PARTITION p1 VALUES LESS THAN (MAXVALUE))"[..],
    ] {
        let sql = client_sql(&session, sql);
        session.run(&sql).unwrap();
    }
    session.run("SET NAMES utf8mb4").unwrap();
    assert_eq!(
        row_text(session.run(
            "SELECT TABLE_NAME, PARTITION_NAME, HEX(PARTITION_DESCRIPTION) FROM \
             information_schema.partitions WHERE table_schema = 'test' ORDER BY 1, 2"
        )),
        [
            ["p1", "p0", "2778EFBFBD272C277127"],
            ["p1", "p1", "277A27"],
            ["p2", "p0", "276DEFBFBD27"],
            ["p2", "p1", "4D415856414C5545"]
        ]
    );
    session.run("SET NAMES latin1").unwrap();
    let sql = client_sql(&session, b"INSERT INTO p1 VALUES (1, 'x\xe9')");
    let error = session.run(&sql).unwrap_err().to_mysql_error();
    assert_eq!(error.code, 1526, "{}", error.message);
    session.run("INSERT INTO p1 VALUES (2, 'q')").unwrap();
    let sql = client_sql(
        &session,
        b"INSERT INTO p2 VALUES (1, 'm'), (2, 'm\xe9'), (3, 'n')",
    );
    session.run(&sql).unwrap();
    session.run("SET NAMES utf8mb4").unwrap();
    assert_eq!(
        row_text(session.run("SELECT a, HEX(b) FROM p1 PARTITION (p0) ORDER BY a")),
        [["2", "71"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a, HEX(b) FROM p2 PARTITION (p0) ORDER BY a")),
        [["1", "6D"], ["2", "6DE9"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a, HEX(b) FROM p2 PARTITION (p1) ORDER BY a")),
        [["3", "6E"]]
    );
}

/// Go parses a statement once, with the client's encodings, and binds a
/// prepared statement's values onto that tree; nothing re-lexes it in
/// another encoding. A gbk client's identifier `中` (bytes D6 D0) must name
/// the same column through the prepared-parameter door and through the
/// executor entries that take the statement text (ALTER TABLE, CREATE INDEX,
/// RENAME TABLE, TRUNCATE TABLE).
#[test]
fn gbk_client_identifiers_survive_binding_and_ddl_reparse() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE gbk_t (id INT PRIMARY KEY, `中` INT)")
        .unwrap();
    session.run("INSERT INTO gbk_t VALUES (1, 7)").unwrap();
    session.run("SET NAMES gbk").unwrap();
    let select = client_sql(&session, b"SELECT `\xd6\xd0` FROM gbk_t WHERE id = ?");
    match session.run_with_params(&select, &[Datum::Int(1)]).unwrap() {
        StmtOutput::Rows { rows, .. } => assert_eq!(rows, vec![vec![Datum::Int(7)]]),
        other => panic!("expected rows: {other:?}"),
    }
    for sql in [
        &b"ALTER TABLE gbk_t ADD COLUMN `\xd6\xd0x` INT"[..],
        b"CREATE INDEX `\xd6\xd0i` ON gbk_t (`\xd6\xd0`)",
        b"RENAME TABLE gbk_t TO `\xd6\xd0t`",
        b"TRUNCATE TABLE `\xd6\xd0t`",
    ] {
        let sql = client_sql(&session, sql);
        session.run(&sql).unwrap();
    }
    session.run("SET NAMES utf8mb4").unwrap();
    assert_eq!(
        row_text(session.run(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_name = '中t' ORDER BY ordinal_position"
        )),
        [["id"], ["中"], ["中x"]]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT DISTINCT index_name FROM information_schema.statistics \
             WHERE table_name = '中t' AND index_name <> 'PRIMARY'"
        )),
        [["中i"]]
    );
    assert_eq!(row_text(session.run("SELECT COUNT(*) FROM `中t`")), [["0"]]);
}
