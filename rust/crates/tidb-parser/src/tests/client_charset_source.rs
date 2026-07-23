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
    assert!(parse_bytes(b"select '\xFF'", "utf8").is_err());
    assert!(parse_bytes(b"select '\xFF'", "ascii").is_err());
    assert!(parse_bytes(b"select 'ascii'", "ascii").is_ok());

    let latin1 = parse_bytes(b"select '\xE9'", "latin1").unwrap().restore();
    assert!(latin1.contains('é'), "{latin1}");

    assert!(parse_bytes(b"select '\xFF'", "binary").is_ok());
    assert!(parse_bytes(b"select '\xFF'", "not-a-charset").is_ok());
}
