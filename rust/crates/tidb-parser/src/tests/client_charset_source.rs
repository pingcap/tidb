// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Exact byte-level client-charset owners from `pkg/parser/parser_test.go`.

use super::*;

fn encoded_create(encoding: &'static encoding_rs::Encoding, label: &str) -> Vec<u8> {
    let sql = format!("create table 测试表 (测试列 varchar(255) default '{label}测试用例');");
    let (encoded, _, had_errors) = encoding.encode(&sql);
    assert!(!had_errors);
    encoded.into_owned()
}

fn assert_client_charset(encoding: &'static encoding_rs::Encoding, charset: &str, label: &str) {
    let sql = encoded_create(encoding, label);
    let unconfigured = parse_bytes(&sql, "").expect("binary client accepts source bytes");
    assert!(!unconfigured.restore().contains("`测试表`"));

    let statement = parse_bytes(&sql, charset).expect("configured client charset decodes SQL");
    let restored = statement.restore();
    assert!(restored.contains("`测试表`"), "{restored}");
    assert!(restored.contains("`测试列`"), "{restored}");
    assert!(
        restored.contains(&format!("'{label}测试用例'")),
        "{restored}"
    );

    let mut quoted = b"select '".to_vec();
    quoted.extend_from_slice(&[0xC6, 0x5C]);
    quoted.extend_from_slice(b"' from dual;");
    assert!(parse_bytes(&quoted, charset).is_ok());

    let mut invalid_escape = b"select '".to_vec();
    invalid_escape.extend_from_slice(&[0x65, 0x5C]);
    invalid_escape.extend_from_slice(b"'");
    assert!(parse_bytes(&invalid_escape, charset).is_err());

    let mut introduced = b"select _gbk '".to_vec();
    introduced.extend_from_slice(&[0xC6, 0x5C]);
    introduced.extend_from_slice(b"' from dual;");
    assert!(parse_bytes(&introduced, "").is_err());

    let mut quoted_identifier = b"select '".to_vec();
    quoted_identifier.extend_from_slice(&[0xC6, 0x5C]);
    quoted_identifier.extend_from_slice(b"' from `");
    quoted_identifier.extend_from_slice(&[0xAB, 0x60]);
    quoted_identifier.extend_from_slice(b"`;");

    let mut backslash_pair = b"select '".to_vec();
    backslash_pair.extend_from_slice(&[0xA5, 0x5C]);
    backslash_pair.push(b'\'');

    let mut quote_pair = b"select '''".to_vec();
    quote_pair.extend_from_slice(&[0xA5, 0x5C]);
    quote_pair.push(b'\'');

    let mut backtick_pair = b"select ```".to_vec();
    backtick_pair.extend_from_slice(&[0xA5, 0x5C]);
    backtick_pair.push(b'`');

    for source in [
        quoted_identifier.as_slice(),
        r#"prepare p1 from "insert into t values ('中文');";"#.as_bytes(),
        "select '啊';".as_bytes(),
        "create table t1(s set('a一','b二','c三'));".as_bytes(),
        "insert into t3 values('一a');".as_bytes(),
        backslash_pair.as_slice(),
        quote_pair.as_slice(),
        backtick_pair.as_slice(),
    ] {
        assert!(
            parse_bytes(source, charset).is_ok(),
            "client charset {charset}, SQL bytes {source:?}"
        );
    }
}

#[test]
fn test_gbk_encoding() {
    assert_client_charset(encoding_rs::GBK, "gbk", "GBK");
}

#[test]
fn test_gb18030_encoding() {
    assert_client_charset(encoding_rs::GB18030, "gb18030", "GB18030");
}

#[test]
fn charset_client_api_covers_the_complete_go_encoding_map() {
    assert!(parse_bytes(b"select 1; select 2", "utf8mb4").is_err());
    assert_eq!(
        parse_multi_bytes(b"select 1; select 2", "utf8mb4")
            .unwrap()
            .len(),
        2
    );
    // Go's utf8 client keeps a literal's bytes, valid UTF-8 or not
    // (`convert2Connection` returns `lit` for a UTF-8 client).
    let utf8 = parse_bytes(b"select '\xFF'", "utf8").unwrap().restore();
    assert_eq!(
        raw_bytes::str_to_bytes(&utf8).as_ref(),
        b"SELECT _UTF8MB4'\xFF'"
    );
    assert!(parse_bytes(b"select '\xFF'", "ascii").is_err());
    assert!(parse_bytes(b"select 'ascii'", "ascii").is_ok());

    // latin1/binary clients under the default utf8mb4 connection: the bytes
    // are scanned raw, and `OpReplaceNoErr` turns the group utf8mb4 cannot
    // hold into `?` -- captured from TiDB as `HEX('\xE9')` = `3F` -- while
    // UTF-8 the terminal sent stays UTF-8.
    for charset in ["latin1", "binary", "not-a-charset"] {
        let replaced = parse_bytes(b"select '\xE9'", charset).unwrap().restore();
        assert_eq!(replaced, "SELECT _UTF8MB4'?'", "{charset}");
        let utf8 = parse_bytes("select '└─é'".as_bytes(), charset)
            .unwrap()
            .restore();
        assert_eq!(utf8, "SELECT _UTF8MB4'└─é'", "{charset}");
    }
}

fn config(client: &str, connection: &str) -> ParserConfig {
    ParserConfig {
        charset_client: tidb_datatype::find_encoding(client),
        charset_connection: tidb_datatype::find_encoding(connection),
        ..ParserConfig::default()
    }
}

/// Go `Scanner.convert2Connection` over the client's raw bytes, captured from
/// TiDB through `HEX()`: a latin1 or binary connection keeps a non-UTF-8 byte
/// (`SET NAMES latin1; SELECT HEX('\xE9')` = `E9`), a utf8mb4 one turns it
/// into `?` for a non-UTF-8 client (`3F`), and a utf8mb4 client keeps it
/// whatever the connection (`E9`). An introducer does not change the
/// literal's conversion: `_binary'\xE9'` is `3F` under a latin1 client and
/// utf8mb4 connection.
#[test]
fn string_literals_convert_from_client_bytes_to_the_connection() {
    let cases: [(&str, &str, &[u8], &[u8]); 8] = [
        (
            "latin1",
            "latin1",
            b"select '\xe9'",
            b"SELECT _UTF8MB4'\xe9'",
        ),
        (
            "binary",
            "binary",
            b"select 'a\xe9b'",
            b"SELECT _UTF8MB4'a\xe9b'",
        ),
        ("latin1", "utf8mb4", b"select '\xe9'", b"SELECT _UTF8MB4'?'"),
        (
            "binary",
            "utf8mb4",
            b"select 'a\xe9\xffb'",
            b"SELECT _UTF8MB4'a??b'",
        ),
        (
            "utf8mb4",
            "latin1",
            b"select '\xe9'",
            b"SELECT _UTF8MB4'\xe9'",
        ),
        (
            "utf8mb4",
            "utf8mb4",
            b"select 'a\xe9b'",
            b"SELECT _UTF8MB4'a\xe9b'",
        ),
        (
            "latin1",
            "utf8mb4",
            b"select _binary'\xe9'",
            b"SELECT _BINARY'?'",
        ),
        (
            "latin1",
            "latin1",
            b"select _binary'\xe9'",
            b"SELECT _BINARY'\xe9'",
        ),
    ];
    for (client, connection, sql, restored) in cases {
        let statement = parse_with_sql_mode_and_config(
            &raw_bytes::bytes_to_str(sql),
            SqlMode::default(),
            config(client, connection),
        )
        .unwrap();
        assert_eq!(
            raw_bytes::str_to_bytes(&statement.restore()).as_ref(),
            restored,
            "client {client}, connection {connection}"
        );
    }
}

/// Go `Scanner.convert2System`: a utf8 client's identifier replaces an
/// invalid byte with `?` and warns 1300 (captured: `select 1 as \`a\xE9\``
/// names the column `a?` with `Invalid utf8mb4 character string: 'E9'`); a
/// latin1 client's identifier keeps the byte with no warning.
#[test]
fn identifiers_convert_from_client_bytes_to_utf8() {
    let sql = raw_bytes::bytes_to_str(b"select 1 as `a\xe9`");
    let utf8 = parse_with_sql_mode_config_and_warnings(
        &sql,
        SqlMode::default(),
        config("utf8mb4", "utf8mb4"),
    )
    .unwrap();
    assert_eq!(utf8.statement.restore(), "SELECT 1 AS `a?`");
    assert_eq!(
        utf8.warnings
            .iter()
            .map(|warning| warning.message.as_str())
            .collect::<Vec<_>>(),
        ["[parser:1300]Invalid utf8mb4 character string: 'E9'"]
    );
    let latin1 = parse_with_sql_mode_config_and_warnings(
        &sql,
        SqlMode::default(),
        config("latin1", "utf8mb4"),
    )
    .unwrap();
    assert_eq!(
        raw_bytes::str_to_bytes(&latin1.statement.restore()).as_ref(),
        b"SELECT 1 AS `a\xe9`"
    );
    assert!(latin1.warnings.is_empty());
}

/// Go `SelectField.Text()` decodes the field's source bytes with the client
/// encoding and spells a non-printable string literal as a hex literal: the
/// column of `SELECT HEX('\xE9')` is `HEX(0xe9)` and of `SELECT HEX('a<TAB>b')`
/// is `HEX(0x610962)` (captured), while `HEX('a\tb')` -- printable source --
/// keeps its spelling.
#[test]
fn select_field_text_hexes_non_printable_literals() {
    for (client, connection) in [
        ("utf8mb4", "utf8mb4"),
        ("latin1", "latin1"),
        ("binary", "binary"),
    ] {
        let sql = raw_bytes::bytes_to_str(
            b"select hex('\xe9'), hex('a\tb'), hex('a\\tb'), _binary'\xe9'",
        );
        let Stmt::Query(query) =
            parse_with_sql_mode_and_config(&sql, SqlMode::default(), config(client, connection))
                .unwrap()
        else {
            panic!("a query");
        };
        let tidb_ast::QueryStmt::Select(select) = query.as_ref() else {
            panic!("a select");
        };
        let texts: Vec<&[u8]> = (0..4)
            .map(|index| select.fields.text(index).unwrap())
            .collect();
        assert_eq!(
            texts,
            [
                &b"hex(0xe9)"[..],
                b"hex(0x610962)",
                b"hex('a\\tb')",
                b"_binary 0xe9"
            ],
            "{client}"
        );
    }
}
