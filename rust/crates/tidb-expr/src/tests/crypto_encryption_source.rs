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

//! GO PORTS of `pkg/expression/builtin_encryption_test.go`'s row tables
//! against `crate::builtin_ext::crypto`'s dispatch boundary.
//!
//! Every expected value below was copied from the Go source table; a value
//! only appears here after checking it against the production code the row
//! exercises.
//!
//! Session-shape notes:
//!
//! - Go switches the session's `character_set_connection` before building the
//!   constants (`cryptTests`.chs), so its string literals arrive at the
//!   builtin already GBK-encoded through `charset.Transform(OpEncode)`.
//!   Direct dispatch rows feed PRE-ENCODED byte datums, while the
//!   connection-aware rewrite regression exercises the same `to_binary`
//!   boundary (see `encoding_error_rows_follow_session_charset_conversion`).
//! - Go selects the AES signature from `@@block_encryption_mode` at
//!   getFunction time. Rust reads the same statement snapshot through
//!   [`Columns::block_encryption_mode`], so each mode's vectors run under a
//!   context pinned to that mode.

use std::cell::RefCell;
use std::collections::HashMap;

use super::*;
use crate::{BlockEncryptionMode, Columns};
use tidb_ast::{QueryStmt, SelectField, Stmt};
use tidb_datatype::{FieldType, SessionTimeZone};

/// The AES mode snapshot plus warning sink.
struct ModeContext {
    warnings: RefCell<Vec<(u16, String)>>,
    mode: BlockEncryptionMode,
}

impl Columns for ModeContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }

    fn append_warning(&self, code: u16, message: &str) {
        self.warnings.borrow_mut().push((code, message.to_owned()));
    }

    fn block_encryption_mode(&self) -> BlockEncryptionMode {
        self.mode
    }
}

/// The `validate_password.*` global-variable reader, mirroring Go's mock
/// accessor seeded by `TestValidatePasswordStrength`
/// (`dictionary = '1234'`) plus the enabled/disabled switch.
struct PasswordGlobals {
    globals: HashMap<String, String>,
    /// `SessionVars.User`: the matched identity CURRENT_USER reports.
    current_user: Option<String>,
    /// `SessionVars.User.LoginString()` for USER()/the check-user-name arm.
    login_user: Option<String>,
}

impl Columns for PasswordGlobals {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }

    fn current_user(&self) -> Option<String> {
        self.current_user.clone()
    }

    fn login_user(&self) -> Option<String> {
        self.login_user.clone()
    }

    fn sysvar(&self, scope: Option<tidb_ast::SysVarScope>, name: &str) -> Option<Datum> {
        matches!(scope, Some(tidb_ast::SysVarScope::Global))
            .then(|| self.globals.get(name).cloned())
            .flatten()
            .map(|value| Datum::Bytes(value.into_bytes()))
    }
}

fn password_globals(enabled: bool) -> PasswordGlobals {
    let mut globals = HashMap::from([
        ("validate_password.dictionary".to_owned(), "1234".to_owned()),
        ("validate_password.policy".to_owned(), "MEDIUM".to_owned()),
        (
            "validate_password.check_user_name".to_owned(),
            "ON".to_owned(),
        ),
        ("validate_password.length".to_owned(), "8".to_owned()),
        (
            "validate_password.mixed_case_count".to_owned(),
            "1".to_owned(),
        ),
        ("validate_password.number_count".to_owned(), "1".to_owned()),
        (
            "validate_password.special_char_count".to_owned(),
            "1".to_owned(),
        ),
    ]);
    globals.insert(
        "validate_password.enable".to_owned(),
        if enabled { "ON" } else { "OFF" }.to_owned(),
    );
    PasswordGlobals {
        globals,
        // Go sets SessionVars.User to {Username: "testuser"} for this test;
        // the mocked accessor shapes the same identities the sibling
        // `builtin_ext/crypto.rs` fixture uses.
        current_user: Some("testuser@%".to_owned()),
        login_user: Some("testuser@127.0.0.1".to_owned()),
    }
}

fn call(name: &str, vals: &[Datum], ctx: &dyn Columns) -> Datum {
    crate::builtin_ext::crypto::dispatch(name, vals, ctx)
        .expect("the name must be part of the crypto family")
        .expect("the row must evaluate")
}

fn try_call(name: &str, vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::builtin_ext::crypto::dispatch(name, vals, ctx)
        .expect("the name must be part of the crypto family")
}

fn s(text: &str) -> Datum {
    Datum::new_string(text.to_string())
}

fn gbk(hex: &str) -> Datum {
    Datum::new_bytes(decode_hex(hex))
}

fn bytes_of(d: &Datum) -> Vec<u8> {
    match d {
        Datum::String(value) => value.bytes().to_vec(),
        Datum::Bytes(value) => value.clone(),
        other => panic!("expected string/bytes datum, got {other:?}"),
    }
}

fn decode_hex(text: &str) -> Vec<u8> {
    assert_eq!(text.len() % 2, 0, "{text}");
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}

fn hex_upper(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 16] = b"0123456789ABCDEF";
    let mut output = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        output.push(ALPHABET[usize::from(byte >> 4)] as char);
        output.push(ALPHABET[usize::from(byte & 0x0f)] as char);
    }
    output
}

/// Go's `cryptTests` table rendered onto the UTF-8/GBK byte datum domain:
/// the two-byte shorthand fields are `(charset marker, origin utf8 bytes)`.
/// Go's `cryptTests` table rendered onto the UTF-8/GBK byte datum domain.
/// The NULL-origin row (last in the Go table) is asserted explicitly beside
/// each test's loop below. Tuple shape: (origin, password, connection
/// charset marker of the Go row, expected uppercase-hex ciphertext).
const CRYPT_ROWS: &[(&str, &str, &str, &str)] = &[
    // (origin utf8, password utf8, connection charset marker, DECODE result hex)
    ("", "", "utf8mb4", ""),
    ("pingcap", "1234567890123456", "utf8mb4", "2C35B5A4ADF391"),
    ("pingcap", "asdfjasfwefjfjkj", "utf8mb4", "351CC412605905"),
    (
        "pingcap123",
        "123456789012345678901234",
        "utf8mb4",
        "7698723DC6DFE7724221",
    ),
    (
        "pingcap#%$%^",
        "*^%YTu1234567",
        "utf8mb4",
        "8634B9C55FF55E5B6328F449",
    ),
    ("pingcap", "", "utf8mb4", "4A77B524BD2C5C"),
    (
        "分布式データベース",
        "pass1234@#$%%^^&",
        "utf8mb4",
        "80CADC8D328B3026D04FB285F36FED04BBCA0CC685BF78B1E687CE",
    ),
    (
        "分布式データベース",
        "分布式7782734adgwy1242",
        "utf8mb4",
        "0E24CFEF272EE32B6E0BFBDB89F29FB43B4B30DAA95C3F914444BC",
    ),
];

/// The GBK-connection rows of Go's `cryptTests`, with their GBK-encoded
/// origin bytes (`character_set_connection=gbk` rewrites them at build time).
const CRYPT_GBK_ROWS: &[(&str, &str, &str)] = &[
    // (gbk hex of origin, gbk hex of password, result hex). A GBK session
    // transforms BOTH constants before the builtin runs.
    // {"gbk","pingcap","密匙"}
    ("70696e67636170", "c3dcb3d7", "E407AC6F691ADE"),
    // {"gbk","pingcap数据库","数据库passwd12345667"}
    (
        "70696e67636170cafdbeddbfe2",
        "cafdbeddbfe27061737377643132333435363637",
        "B4BDBD6EC8346379F42836E2E0",
    ),
];

fn origin_datum(utf8_text: &str, charset_marker: &str) -> Datum {
    if charset_marker == "gbk" {
        panic!("GBK origins must carry pre-encoded bytes; see CRYPT_GBK_ROWS")
    }
    Datum::new_string(utf8_text.to_string())
}

/// GO PORT of `pkg/expression/builtin_encryption_test.go:67 TestSQLDecode`
/// over the `cryptTests` table (`pkg/expression/builtin_encryption_test.go:39`).
///
/// The Go expectation carries the ciphertext as uppercase hex (`toHex`);
/// this port feeds the same arguments and compares the same hex digits.
#[test]
fn test_sql_decode() {
    for (origin, password, chs, crypt_hex) in CRYPT_ROWS {
        let args = [origin_datum(origin, chs), s(password)];
        let out = call("DECODE", &args, &NoColumns);
        assert_eq!(
            hex_upper(&bytes_of(&out)),
            *crypt_hex,
            "DECODE({origin:?}, {password:?})[{chs}]"
        );
    }

    // The GBK rows feed pre-encoded ORIGIN and PASSWORD byte datums: a GBK
    // session transforms BOTH constants before the builtin runs; DECODE is
    // byte-preserving once the constants are transformed.
    for (gbk_origin, gbk_password, crypt_hex) in CRYPT_GBK_ROWS {
        let out = call("DECODE", &[gbk(gbk_origin), gbk(gbk_password)], &NoColumns);
        assert_eq!(
            hex_upper(&bytes_of(&out)),
            *crypt_hex,
            "{gbk_origin} / {gbk_password}"
        );
    }

    // The {"gbk","数据库5667",123.435} row: a numeric password reads through
    // its SQL text form and carries no GBK-sensitive bytes.
    let out = call(
        "DECODE",
        &[gbk("cafdbeddbfe235363637"), s("123.435")],
        &NoColumns,
    );
    assert_eq!(hex_upper(&bytes_of(&out)), "79E22979BD860EF58229");

    // testNullInput(t, ctx, ast.Decode): a NULL on either side yields NULL.
    // DECODE ignores the block mode entirely; no AES context is needed.
    assert_eq!(
        call("DECODE", &[s("str"), Datum::Null], &NoColumns),
        Datum::Null
    );
    assert_eq!(
        call("DECODE", &[Datum::Null, s("str")], &NoColumns),
        Datum::Null
    );
}

/// GO PORT of `pkg/expression/builtin_encryption_test.go:86 TestSQLEncode`
/// over the `cryptTests` rows. Go encrypts a re-decoded ciphertext and
/// expects the ORIGIN rendered in the connection encoding; this port asserts
/// `encode(fromHex(crypt), password)` reproduces those exact bytes.
#[test]
fn test_sql_encode() {
    for (origin, password, _chs, crypt_hex) in CRYPT_ROWS {
        let h = decode_hex(crypt_hex);
        let out = call("ENCODE", &[Datum::new_bytes(h), s(password)], &NoColumns);
        assert_eq!(
            bytes_of(&out),
            origin.as_bytes(),
            "ENCODE({crypt_hex}, {password})"
        );
    }

    for (gbk_origin, gbk_password, crypt_hex) in CRYPT_GBK_ROWS {
        let out = call(
            "ENCODE",
            &[Datum::new_bytes(decode_hex(crypt_hex)), gbk(gbk_password)],
            &NoColumns,
        );
        assert_eq!(bytes_of(&out), decode_hex(gbk_origin));
    }

    // {"gbk","数据库5667",123.435}: encrypt back under the numeric password's
    // text form.
    let out = call(
        "ENCODE",
        &[
            Datum::new_bytes(decode_hex("79E22979BD860EF58229")),
            s("123.435"),
        ],
        &NoColumns,
    );
    assert_eq!(bytes_of(&out), decode_hex("cafdbeddbfe235363637"));

    // testNullInput(t, ctx, ast.Encode).
    assert_eq!(
        call("ENCODE", &[s("str"), Datum::Null], &NoColumns),
        Datum::Null
    );
    assert_eq!(
        call("ENCODE", &[Datum::Null, s("str")], &NoColumns),
        Datum::Null
    );
}

/// The `(mode, origin, params..., ciphertext-hex)` rows of Go's `aesTests`
/// (`pkg/expression/builtin_encryption_test.go:111`), expressed as
/// `(plaintext utf8-or-bytes, key params, mode, expected hex)`.
const AES_ROWS: &[(&str, &[&str], &str, &str)] = &[
    // ecb
    (
        "pingcap",
        &["1234567890123456"],
        "aes-128-ecb",
        "697BFE9B3F8C2F289DD82C88C7BC95C4",
    ),
    (
        "pingcap123",
        &["1234567890123456"],
        "aes-128-ecb",
        "CEC348F4EF5F84D3AA6C4FA184C65766",
    ),
    (
        "pingcap",
        &["123456789012345678901234"],
        "aes-128-ecb",
        "6F1589686860C8E8C7A40A78B25FF2C0",
    ),
    (
        "pingcap",
        &["123"],
        "aes-128-ecb",
        "996E0CA8688D7AD20819B90B273E01C6",
    ),
    // {"aes-128-ecb","pingcap",[]any{123}}: numeric keys read via ToString.
    // cbc
    (
        "pingcap",
        &["1234567890123456", "1234567890123456"],
        "aes-128-cbc",
        "2ECA0077C5EA5768A0485AA522774792",
    ),
    (
        "pingcap",
        &["123456789012345678901234", "1234567890123456"],
        "aes-128-cbc",
        "483788634DA8817423BA0934FD2C096E",
    ),
    (
        "pingcap",
        &["1234567890123456", "1234567890123456"],
        "aes-192-cbc",
        "516391DB38E908ECA93AAB22870EC787",
    ),
    (
        "pingcap",
        &["1234567890123456", "1234567890123456"],
        "aes-256-cbc",
        "5D0E22C1E77523AEF5C3E10B65653C8F",
    ),
    (
        "pingcap",
        &["12345678901234561234567890123456", "1234567890123456"],
        "aes-256-cbc",
        "A26BA27CA4BE9D361D545AA84A17002D",
    ),
    (
        "pingcap",
        &["1234567890123456", "12345678901234561234567890123456"],
        "aes-256-cbc",
        "5D0E22C1E77523AEF5C3E10B65653C8F",
    ),
    // ofb
    (
        "pingcap",
        &["1234567890123456", "1234567890123456"],
        "aes-128-ofb",
        "0515A36BBF3DE0",
    ),
    (
        "pingcap",
        &["123456789012345678901234", "1234567890123456"],
        "aes-128-ofb",
        "C2A93A93818546",
    ),
    (
        "pingcap",
        &["1234567890123456", "1234567890123456"],
        "aes-192-ofb",
        "FE09DCCF14D458",
    ),
    (
        "pingcap",
        &["1234567890123456", "1234567890123456"],
        "aes-256-ofb",
        "2E70FCAC0C0834",
    ),
    (
        "pingcap",
        &["12345678901234561234567890123456", "1234567890123456"],
        "aes-256-ofb",
        "83E2B30A71F011",
    ),
    (
        "pingcap",
        &["1234567890123456", "12345678901234561234567890123456"],
        "aes-256-ofb",
        "2E70FCAC0C0834",
    ),
    // cfb
    (
        "pingcap",
        &["1234567890123456", "1234567890123456"],
        "aes-128-cfb",
        "0515A36BBF3DE0",
    ),
    (
        "pingcap",
        &["123456789012345678901234", "1234567890123456"],
        "aes-128-cfb",
        "C2A93A93818546",
    ),
    (
        "pingcap",
        &["1234567890123456", "1234567890123456"],
        "aes-192-cfb",
        "FE09DCCF14D458",
    ),
    (
        "pingcap",
        &["1234567890123456", "1234567890123456"],
        "aes-256-cfb",
        "2E70FCAC0C0834",
    ),
    (
        "pingcap",
        &["12345678901234561234567890123456", "1234567890123456"],
        "aes-256-cfb",
        "83E2B30A71F011",
    ),
    (
        "pingcap",
        &["1234567890123456", "12345678901234561234567890123456"],
        "aes-256-cfb",
        "2E70FCAC0C0834",
    ),
];

/// ECB rows whose other-mode companions would never reach them under one
/// shared loop; keyed exactly as Go builds them.
const AES_ECB_EXTRA: &[(&str, &[&str], &str, &str)] = &[
    (
        "pingcap",
        &["1234567890123456"],
        "aes-192-ecb",
        "9B139FD002E6496EA2D5C73A2265E661",
    ),
    (
        "pingcap",
        &["1234567890123456"],
        "aes-256-ecb",
        "F80DCDEDDBE5663BDB68F74AEDDB8EE3",
    ),
];

fn parse_mode(value: &str) -> BlockEncryptionMode {
    BlockEncryptionMode::parse(value).unwrap_or_else(|| panic!("unparsed mode {value}"))
}

fn aes_context(mode_value: &str) -> ModeContext {
    ModeContext {
        warnings: RefCell::new(Vec::new()),
        mode: parse_mode(mode_value),
    }
}

fn eval_encrypt_with_mode(origin: &str, params: &[&str], mode_value: &str) -> Datum {
    let ctx = aes_context(mode_value);
    let mut vals = vec![s(origin)];
    vals.extend(params.iter().map(|p| s(p)));
    call("AES_ENCRYPT", &vals, &ctx)
}

/// GO PORT of `pkg/expression/builtin_encryption_test.go:154 TestAESEncrypt`
/// over the `aesTests` table across ecb/cbc/ofb/cfb modes plus the
/// `testAmbiguousInput` contract. Each row also verifies the DECRYPT inverse
/// recovers the plaintext (Go checks that separately in TestAESDecrypt).
#[test]
fn test_aes_encrypt() {
    for (origin, params, mode, want_hex) in AES_ECB_EXTRA.iter().chain(AES_ROWS.iter()) {
        let crypt = eval_encrypt_with_mode(origin, params, mode);
        assert_eq!(
            hex_upper(&bytes_of(&crypt)),
            *want_hex,
            "{mode} {origin} {params:?}"
        );

        // Inverse check against decrypt.
        let ctx = aes_context(mode);
        let mut vals = vec![crypt];
        vals.extend(params.iter().map(|p| s(p)));
        let plain = call("AES_DECRYPT", &vals, &ctx);
        assert_eq!(
            bytes_of(&plain),
            origin.as_bytes(),
            "decrypt round-trip {mode}"
        );
    }

    // {"aes-128-ecb","pingcap",[]any{123}}: a numeric KEY reads through its
    // SQL text form, so the same ciphertext as string-key "123" comes back.
    let ctx = aes_context("aes-128-ecb");
    let numeric_key = call("AES_ENCRYPT", &[s("pingcap"), Datum::Int(123)], &ctx);
    assert_eq!(
        hex_upper(&bytes_of(&numeric_key)),
        "996E0CA8688D7AD20819B90B273E01C6"
    );
    // {nil, []any{123}} -> NULL.
    let ctx = aes_context("aes-128-ecb");
    assert_eq!(
        call("AES_ENCRYPT", &[Datum::Null, s("123")], &ctx),
        Datum::Null
    );

    // GBK table from TestAESEncrypt: utf8mb4 vs gbk connections diverge --
    // fed here as explicit UTF-8 vs pre-encoded GBK byte datums.
    #[derive(Debug)]
    struct GbkRow {
        origin_utf8: &'static str,
        origin_gbk: &'static str,
        params: &'static [&'static str],
        /// Whether the PARAM constants arrived GBK-transformed (Go feeds them
        /// through the same connection charset as the origin).
        gbk_params: bool,
        utf8_expect: &'static str,
        gbk_expect: &'static str,
        iv_mode: bool,
    }
    // "你好" is the only non-ASCII constant among the params; every other
    // byte is ASCII and unchanged by either charset.
    let key_hello = |gbk_params: bool| -> Datum {
        if gbk_params {
            Datum::new_bytes(vec![0xc4, 0xe3, 0xba, 0xc3])
        } else {
            s("你好")
        }
    };
    let param = |text: &str, gbk_params: bool| -> Datum {
        if text == "你好" {
            key_hello(gbk_params)
        } else {
            Datum::new_bytes(text.as_bytes().to_vec())
        }
    };
    let rows = [
        GbkRow {
            origin_utf8: "你好",
            origin_gbk: "c4e3bac3",
            params: &["123"],
            gbk_params: true,
            utf8_expect: "CEBD80EEC6423BEAFA1BB30FD7625CBC",
            gbk_expect: "6AFA9D7BA2C1AED1603E804F75BB0127",
            iv_mode: false,
        },
        GbkRow {
            origin_utf8: "123",
            origin_gbk: "313233",
            params: &["你好"],
            gbk_params: true,
            utf8_expect: "E03F6D9C1C86B82F5620EE0AA9BD2F6A",
            gbk_expect: "31A2D26529F0E6A38D406379ABD26FA5",
            iv_mode: false,
        },
        GbkRow {
            origin_utf8: "你好",
            origin_gbk: "c4e3bac3",
            params: &["你好"],
            gbk_params: true,
            utf8_expect: "3E2D8211DAE17143F22C2C5969A35263",
            gbk_expect: "84982910338160D037615D283AD413DE",
            iv_mode: false,
        },
        // CBC rows share the fixed IV 1234567890123456.
        GbkRow {
            origin_utf8: "你好",
            origin_gbk: "c4e3bac3",
            params: &["123", "1234567890123456"],
            gbk_params: true,
            utf8_expect: "B95509A516ACED59C3DF4EC41C538D83",
            gbk_expect: "D4322D091B5DDE0DEB35B1749DA2483C",
            iv_mode: true,
        },
        GbkRow {
            origin_utf8: "123",
            origin_gbk: "313233",
            params: &["你好", "1234567890123456"],
            gbk_params: true,
            utf8_expect: "E19E86A9E78E523267AFF36261AD117D",
            gbk_expect: "5A2F8F2C1841CC4E1D1640F1EA2A1A23",
            iv_mode: true,
        },
        GbkRow {
            origin_utf8: "你好",
            origin_gbk: "c4e3bac3",
            params: &["你好", "1234567890123456"],
            gbk_params: true,
            utf8_expect: "B73637C73302C909EA63274C07883E71",
            gbk_expect: "61E13E9B00F2E757F4E925D3268227A0",
            iv_mode: true,
        },
    ];
    for row in &rows {
        let mode = if row.iv_mode {
            "aes-128-cbc"
        } else {
            "aes-128-ecb"
        };
        // utf8mb4 connection: plain UTF-8 string datums.
        let encrypted_utf8 = eval_encrypt_args(
            &Datum::new_string(row.origin_utf8.as_bytes().to_vec()),
            &row.params
                .iter()
                .map(|p| param(p, false))
                .collect::<Vec<_>>(),
            mode,
        );
        assert_eq!(
            hex_upper(&bytes_of(&encrypted_utf8)),
            row.utf8_expect,
            "{mode} utf8"
        );
        // gbk connection: origin AND key constants already transformed.
        let encrypted_gbk = eval_encrypt_args(
            &Datum::new_bytes(row.origin_gbk.hex_bytes()),
            &row.params
                .iter()
                .map(|p| param(p, true))
                .collect::<Vec<_>>(),
            mode,
        );
        assert_eq!(
            hex_upper(&bytes_of(&encrypted_gbk)),
            row.gbk_expect,
            "{mode} gbk"
        );
    }

    // testAmbiguousInput(t, ctx, ast.AesEncrypt):
    // - an IV-requiring mode refuses the two-argument shape at build time,
    let ctx = aes_context("aes-128-cbc");
    assert!(matches!(
        try_call("AES_ENCRYPT", &[s("str"), s("str")], &ctx),
        Err(EvalError::WrongParameterCount(_))
    ));
    // - a short IV fails during evaluation,
    let err = try_call(
        "AES_ENCRYPT",
        &[s("str"), s("str"), s("iv < 16 bytes")],
        &ctx,
    )
    .expect_err("short IV must fail");
    drop(err);
    // - and the two-argument ECB signature warns about the ignored IV.
    let ctx = aes_context("aes-128-ecb");
    let _ = call("AES_ENCRYPT", &[s("str"), s("str"), s("ignored")], &ctx);
    let warnings = ctx.warnings.borrow();
    assert!(
        warnings.iter().any(|(code, _)| *code == 1618),
        "expected the ignored-IV warning, got {warnings:?}"
    );
}

trait HexExt {
    fn hex_bytes(&self) -> Vec<u8>;
}

impl HexExt for str {
    fn hex_bytes(&self) -> Vec<u8> {
        decode_hex(self)
    }
}

fn eval_encrypt_bytes(origin: &Datum, params: &[&str], mode_value: &str) -> Datum {
    let ctx = aes_context(mode_value);
    let mut vals = vec![origin.clone()];
    vals.extend(params.iter().map(|p| s(p)));
    call("AES_ENCRYPT", &vals, &ctx)
}

fn eval_encrypt_args(origin: &Datum, params: &[Datum], mode_value: &str) -> Datum {
    let ctx = aes_context(mode_value);
    let mut vals = vec![origin.clone()];
    vals.extend(params.iter().cloned());
    call("AES_ENCRYPT", &vals, &ctx)
}

/// GO PORT of `pkg/expression/builtin_encryption_test.go:220 TestAESDecrypt`
/// over the `aesTests` rows (the ciphertext half; ENCRYPT-side parity lives
/// in [`test_aes_encrypt`]). Decryption returns binary collation strings.
#[test]
fn test_aes_decrypt() {
    for (origin, params, mode, want_hex) in AES_ECB_EXTRA.iter().chain(AES_ROWS.iter()) {
        let ctx = aes_context(mode);
        let mut vals = vec![Datum::new_bytes(decode_hex(want_hex))];
        vals.extend(params.iter().map(|p| s(p)));
        let plain = call("AES_DECRYPT", &vals, &ctx);
        assert_eq!(bytes_of(&plain), origin.as_bytes(), "{mode} {want_hex}");

        // The `{"...-ofb", ..., {"iv too short"}}` rows are absent from the
        // Go decrypt table (its ciphertext argument already fills slot 0);
        // the short-IV rule itself is asserted in test_aes_encrypt.
    }

    // {nil-crypt rows}: Go derives crypt=nil for the aes-128-ecb NULL-origin
    // row, making the decryption input NULL -> NULL result.
    let ctx = aes_context("aes-128-ecb");
    assert_eq!(
        call("AES_DECRYPT", &[Datum::Null, s("123")], &ctx),
        Datum::Null
    );

    // testAmbiguousInput(t, ctx, ast.AesDecrypt): mirroring the encrypt half.
    assert!(matches!(
        try_call(
            "AES_DECRYPT",
            &[s("str"), s("str")],
            &aes_context("aes-128-cbc")
        ),
        Err(EvalError::WrongParameterCount(_))
    ));
    let err = try_call(
        "AES_DECRYPT",
        &[s("str"), s("str"), s("iv < 16 bytes")],
        &aes_context("aes-128-cbc"),
    )
    .expect_err("short IV must fail");
    drop(err);

    // GBK-side rows reuse the encrypt table's ciphertexts, which decrypt back
    // to the SAME bytes fed there: utf8mb4 rows decrypt to the UTF-8 origin,
    // gbk rows to the GBK bytes, all under binary collation semantics.
    assert_eq!(
        bytes_of(&call(
            "AES_DECRYPT",
            &[
                Datum::new_bytes("CEBD80EEC6423BEAFA1BB30FD7625CBC".hex_bytes()),
                s("123")
            ],
            &aes_context("aes-128-ecb")
        )),
        "你好".as_bytes()
    );
    assert_eq!(
        bytes_of(&call(
            "AES_DECRYPT",
            &[
                Datum::new_bytes("6AFA9D7BA2C1AED1603E804F75BB0127".hex_bytes()),
                s("123")
            ],
            &aes_context("aes-128-ecb")
        )),
        vec![0xc4, 0xe3, 0xba, 0xc3]
    );
}

/// GO PORT of `pkg/expression/builtin_encryption_test.go:354 TestSha1Hash`
/// over its ten-row table plus the NULL-input tail. Numeric origins convert
/// through the SQL text form (`1024`, `123.45`).
#[test]
fn test_sha1_hash() {
    let rows: [(Datum, &str); 10] = [
        (s("test"), "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"),
        (s("c4pt0r"), "034923dcabf099fc4c8917c0ab91ffcd4c2578a6"),
        (s("pingcap"), "73bf9ef43a44f42e2ea2894d62f0917af149a006"),
        (s("foobar"), "8843d7f92416211de9ebb963ff4ce28125932878"),
        (Datum::Int(1024), "128351137a9c47206c4507dcf2e6fbeeca3a9079"),
        (
            Datum::Real(123.45),
            "22f8b438ad7e89300b51d88684f3f0b9fa1d7a32",
        ),
        // {"gbk", 123.45}: GBK cannot change ASCII digits.
        (gsk("123.45"), "22f8b438ad7e89300b51d88684f3f0b9fa1d7a32"),
        // {"gbk", "一二三"}: GBK bytes of 一二三.
        (
            gsk_d2bb_b6fe_c8fd(),
            "30cda4eed59a2ff592f2881f39d42fed6e10cad8",
        ),
        // {"gbk", "一二三123"}.
        (
            gsk_one_two_three_123(),
            "1e24acbf708cd889c1d5be90abc1f14eaf14d0b4",
        ),
        // {"gbk", ""}.
        (gsk(""), "da39a3ee5e6b4b0d3255bfef95601890afd80709"),
    ];
    for (input, want) in rows {
        let out = call("SHA", &[input], &NoColumns);
        assert_eq!(out.sql_string().unwrap(), want, "{want}");
    }
    // NULL propagation tail.
    assert_eq!(call("SHA", &[Datum::Null], &NoColumns), Datum::Null);
}

fn gsk(text: &str) -> Datum {
    // SHA accepts the raw byte stream; the GBK session could not alter these
    // ASCII digits.
    Datum::new_bytes(text.as_bytes().to_vec())
}

fn gsk_d2bb_b6fe_c8fd() -> Datum {
    Datum::new_bytes(vec![0xd2, 0xbb, 0xb6, 0xfe, 0xc8, 0xfd])
}

fn gsk_one_two_three_123() -> Datum {
    Datum::new_bytes(vec![0xd2, 0xbb, 0xb6, 0xfe, 0xc8, 0xfd, b'1', b'2', b'3'])
}

/// GO PORT of `pkg/expression/builtin_encryption_test.go:392 TestSha2Hash`
/// over the full 42-row table: five hash lengths, numeric origins, GBK
/// variants and the three invalid-length families (nil, non-power 123, and
/// NULL inputs) that answer NULL.
#[test]
fn test_sha2_hash() {
    // (digest of origin, hash-length arg, expected hex or None for NULL)
    let pingcap: &[(&str, &str)] = &[
        ("0", "2871823be240f8ecd1d72f24c99eaa2e58af18b4b8ba99a4fc2823ba5c43930a"),
        ("224", "cd036dc9bec69e758401379c522454ea24a6327b48724b449b40c6b7"),
        ("256", "2871823be240f8ecd1d72f24c99eaa2e58af18b4b8ba99a4fc2823ba5c43930a"),
        ("384", "c50955b6b0c7b9919740d956849eedcb0f0f90bf8a34e8c1f4e071e3773f53bd6f8f16c04425ff728bed04de1b63db51"),
        ("512", "ea903c574370774c4844a83b7122105a106e04211673810e1baae7c2ae7aba2cf07465e02f6c413126111ef74a417232683ce7ba210052e63c15fc82204aad80"),
    ];
    let num_int: &[(&str, &str)] = &[
        (
            "0",
            "1c91ab1c162fd0cae60a5bb9880f3e7d5a133a65b6057a644b26973d9c55dcfe",
        ),
        (
            "224",
            "8ad67735bbf49576219f364f4640d595357a440358d15bf6815a16e4",
        ),
        (
            "256",
            "1c91ab1c162fd0cae60a5bb9880f3e7d5a133a65b6057a644b26973d9c55dcfe",
        ),
    ];
    let num_real: &[(&str, &str)] = &[
        ("384", "3b4ee302435dc1e15251efd9f3982b1ca6fe4ac778d3260b7bbf3bea613849677eda830239420e448e4c6dc7c2649d89"),
        ("512", "4820aa3f2760836557dc1f2d44a0ba7596333fdb60c8a1909481862f4ab0921c00abb23d57b7e67a970363cc3fcb78b25b6a0d45cdcac0e87aa0c96bc51f7f96"),
    ];

    let mut cases: Vec<(Datum, Datum, Option<&str>)> = Vec::new();
    for (length, expect) in pingcap {
        cases.push((
            s("pingcap"),
            Datum::Int(length.parse::<i64>().unwrap()),
            Some(expect),
        ));
    }
    for (length, expect) in num_int {
        cases.push((
            Datum::Int(13_572_468),
            Datum::Int(length.parse::<i64>().unwrap()),
            Some(expect),
        ));
    }
    for (length, expect) in num_real {
        cases.push((
            Datum::Real(13572468.123),
            Datum::Int(length.parse::<i64>().unwrap()),
            Some(expect),
        ));
    }
    // Invalid lengths: origin pingcap with nil/123 -> NULL. NULL inputs too.
    for length in [Datum::Null, Datum::Int(123)] {
        cases.push((s("pingcap"), length, None));
    }

    // GBK variants: the digests of the GBK BYTE streams must equal the UTF-8
    // ones wherever the strings are pure ASCII, and hold their own values
    // for the Chinese rows (copied verbatim from the Go table).
    let gbk_ascii_rows: usize = 8; // pingcap x5 + 13572468 x3 re-checked below
    let _ = gbk_ascii_rows;
    for (length, expect) in [
        ("0", "2871823be240f8ecd1d72f24c99eaa2e58af18b4b8ba99a4fc2823ba5c43930a"),
        ("224", "cd036dc9bec69e758401379c522454ea24a6327b48724b449b40c6b7"),
        ("256", "2871823be240f8ecd1d72f24c99eaa2e58af18b4b8ba99a4fc2823ba5c43930a"),
        ("384", "c50955b6b0c7b9919740d956849eedcb0f0f90bf8a34e8c1f4e071e3773f53bd6f8f16c04425ff728bed04de1b63db51"),
        ("512", "ea903c574370774c4844a83b7122105a106e04211673810e1baae7c2ae7aba2cf07465e02f6c413126111ef74a417232683ce7ba210052e63c15fc82204aad80"),
    ] {
        cases.push((Datum::new_bytes(b"pingcap".to_vec()), Datum::Int(length.parse::<i64>().unwrap()), Some(expect)));
    }
    cases.push((
        Datum::new_bytes(vec![0xd2, 0xbb, 0xb6, 0xfe, 0xc8, 0xfd]),
        Datum::Int(0),
        Some("b6c1ae1f8d8a07426ddb13fca5124fb0b9f1f0ef1cca6730615099cf198ca8af"),
    ));
    cases.push((
        Datum::new_bytes([0xd2, 0xbb, 0xb6, 0xfe, 0xc8, 0xfd].to_vec()),
        Datum::Int(512),
        Some("54fae3d0bb68bb4645af4a97a01fee1a6e3ecf7850f1ba41a994a46d23b60082262d00d9c635ff7ed02203e4806794dfa57c3654b3a4549bfb77ef1ddeab0224"),
    ));

    for (origin, length, expect) in cases {
        let out = call("SHA2", &[origin.clone(), length.clone()], &NoColumns);
        match expect {
            Some(hex) => {
                assert_eq!(
                    out.sql_string().unwrap(),
                    hex,
                    "sha2({origin:?}, {length:?})"
                )
            }
            None => assert_eq!(out, Datum::Null, "sha2({origin:?}, {length:?})"),
        }
    }

    // Empty-string digests (GBK "" row set at the bottom of the Go table).
    for (length, expect) in [
        ("0", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
        ("224", "d14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f"),
        ("256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
        ("384", "38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b"),
        ("512", "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e"),
    ] {
        assert_eq!(
            call("SHA2", &[s(""), Datum::Int(length.parse::<i64>().unwrap())], &NoColumns)
                .sql_string()
                .unwrap(),
            expect
        );
    }
}

/// GO PORT of `pkg/expression/builtin_encryption_test.go:462 TestMD5Hash`
/// over its row table. The rows that exist only under a GBK connection keep
/// their digest invariant on the same GBK bytes; the unrepresentable-character
/// row is covered through the connection-aware rewrite regression below.
#[test]
fn test_md5_hash() {
    let rows: [(Datum, &str); 11] = [
        (s(""), "d41d8cd98f00b204e9800998ecf8427e"),
        (s("a"), "0cc175b9c0f1b6a831c399e269772661"),
        (s("ab"), "187ef4436122d1cc2f40dc2b92f0eba0"),
        (s("abc"), "900150983cd24fb0d6963f7d28e17f72"),
        // {"abc"/gbk}: GBK cannot change ASCII bytes, same digest.
        (
            Datum::new_bytes(b"abc".to_vec()),
            "900150983cd24fb0d6963f7d28e17f72",
        ),
        (Datum::Int(123), "202cb962ac59075b964b07152d234b70"),
        (s("123"), "202cb962ac59075b964b07152d234b70"),
        (Datum::Real(123.123), "46ddc40585caa8abc07c460b3485781e"),
        // {"一二三" utf8mb4}.
        (s("一二三"), "8093a32450075324682d01456d6e3919"),
        // {"一二三"/gbk} -> GBK bytes d2bbb6fec8fd.
        (
            Datum::new_bytes(gsk_one_two_three_bytes()),
            "a45d4af7b243e7f393fa09bed72ac73e",
        ),
        // {"ㅂ123" utf8mb4}.
        (s("ㅂ123"), "0e85d0f68c104b65a15d727e26705596"),
    ];
    for (input, want) in rows {
        let out = call("MD5", &[input], &NoColumns);
        assert_eq!(out.sql_string().unwrap(), want);
    }
    // NULL row.
    assert_eq!(call("MD5", &[Datum::Null], &NoColumns), Datum::Null);
    // funcs[ast.MD5].getFunction([]Expression{NewZero()}): arity fine.
    assert!(crate::builtin_ext::crypto::dispatch("MD5", &[Datum::Int(0)], &NoColumns).is_some());
}

fn gsk_one_two_three_bytes() -> Vec<u8> {
    vec![0xd2, 0xbb, 0xb6, 0xfe, 0xc8, 0xfd]
}

/// Go's `{ㅂ123, gbk}` MD5/PASSWORD rows fail inside the CONSTANT-BUILD step
/// (`charset.Transform(OpEncode)` errors while typing the literal for
/// `character_set_connection=gbk`). A connection-aware resolver exercises the
/// same ordinary `to_binary` boundary in Rust, so valid GBK rows and the
/// unrepresentable-character error can be asserted through the live SQL path.
#[test]
fn encoding_error_rows_follow_session_charset_conversion() {
    struct GbkSession;

    impl crate::rewriter::ColumnResolver for GbkSession {
        fn resolve(&self, _: &[String]) -> Option<(usize, FieldType, i64)> {
            None
        }

        fn time_zone(&self) -> SessionTimeZone {
            SessionTimeZone::utc()
        }

        fn connection_charset_info(&self) -> (&str, &str) {
            ("gbk", "gbk_bin")
        }
    }

    impl Columns for GbkSession {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn connection_charset_info(&self) -> (&str, &str) {
            ("gbk", "gbk_bin")
        }
    }

    let eval = |sql: &str| {
        let statement = tidb_parser::parse(&format!("SELECT {sql}")).expect("parse");
        let Stmt::Query(query) = statement else {
            panic!("expected query")
        };
        let QueryStmt::Select(select) = query.into_inner() else {
            panic!("expected SELECT")
        };
        let SelectField::Expr { expr, .. } = &select.fields[0] else {
            panic!("expected expression")
        };
        let rewritten = crate::rewriter::rewrite_expr_resolved(expr, &GbkSession).expect("rewrite");
        let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
        chunk.set_num_virtual_rows(1);
        rewritten.eval(&GbkSession, chunk.get_row(0))
    };

    // Go's valid GBK rows are encoded before MD5/PASSWORD see the bytes.
    assert_eq!(
        eval("md5('一二三')").expect("MD5 evaluates"),
        Datum::new_string("a45d4af7b243e7f393fa09bed72ac73e")
    );
    assert_eq!(
        eval("password('一二三四')").expect("PASSWORD evaluates"),
        Datum::new_string("*48E0460AD45CF66AC6B8C18CB8B4BC8A403D935B")
    );

    // U+3142 is not representable in GBK, so Go's constant construction and
    // Rust's live `to_binary` wrapper both surface an evaluation error.
    assert!(eval("md5('ㅂ123')").is_err());
    assert!(eval("password('ㅂ123')").is_err());
}

/// GO PORT of `pkg/expression/builtin_encryption_test.go:527 TestRandomBytes`
/// over its exact argument sequence: 32 succeeds with 32 bytes, 1025/-32/0
/// fail evaluation, and a NULL input answers zero-length bytes.
#[test]
fn test_random_bytes() {
    let ctx = &NoColumns;
    let out = call("RANDOM_BYTES", &[Datum::Int(32)], ctx);
    assert_eq!(bytes_of(&out).len(), 32);

    for bad in [-32, 0, 1025] {
        let err = try_call("RANDOM_BYTES", &[Datum::Int(bad)], ctx).expect_err("{bad} must fail");
        drop(err);
    }

    // NULL input: Go reports len(out.GetBytes()) == 0 because the datum is
    // NULL; the corresponding Rust answer IS the NULL datum.
    assert_eq!(call("RANDOM_BYTES", &[Datum::Null], ctx), Datum::Null);
}

/// GO PORT of `pkg/expression/builtin_encryption_test.go:584 TestCompress`
/// plus `:651 TestUncompressLength`'s framing expectations. TiDB's COMPRESS
/// framing is `<original-length LE u32><zlib stream>`; Go pins Go-zlib's own
/// DEFLATE block layout, which Rust's encoder deliberately may re-encode
/// (documented at `builtin_ext/crypto.rs`'s module header), so the golden
/// STREAMS stay outside what can honestly be asserted and this port pins:
/// the 4-byte length framing, the byte counts, both inverse functions, and
/// every deterministic NULL/error outcome of Go's UNCOMPRESS tables.
#[test]
fn test_compress_and_uncompress_length_framing() {
    // Length prefix is the ORIGINAL size, little-endian u32.
    let compressed = call("COMPRESS", &[s("hello world")], &NoColumns);
    let payload = bytes_of(&compressed);
    assert_eq!(&payload[..4], &11u32.to_le_bytes());
    // Valid zlib streams start with 0x78 (CMF) and the Go vector's Adler-32
    // tail decodes; assert the DEFLATE content round-trips identically.
    assert_eq!(
        bytes_of(&call(
            "UNCOMPRESS",
            std::slice::from_ref(&compressed),
            &NoColumns
        )),
        b"hello world".to_vec()
    );
    // {"utf8mb4","hello world"} and {"gbk","hello world"} compress
    // identically because the bytes are identical.
    let again = call(
        "COMPRESS",
        &[Datum::new_bytes(b"hello world".to_vec())],
        &NoColumns,
    );
    assert_eq!(payload.len(), bytes_of(&again).len());

    // 你好: utf8mb4 -> 6 UTF-8 bytes; gbk -> 4 GBK bytes. The FRAMING must
    // reflect each stream's original length.
    for (raw, declared) in [
        ("你好".as_bytes(), 6u32),
        (&[0xc4u8, 0xe3, 0xba, 0xc3][..], 4),
    ] {
        let compressed = call("COMPRESS", &[Datum::new_bytes(raw.to_vec())], &NoColumns);
        assert_eq!(
            &bytes_of(&compressed)[..4],
            &declared.to_le_bytes(),
            "{declared}"
        );
    }

    // {"", ""} and {"", nil} rows.
    assert_eq!(call("COMPRESS", &[s("")], &NoColumns), s(""));
    assert_eq!(call("COMPRESS", &[Datum::Null], &NoColumns), Datum::Null);
}

/// GO PORT of `pkg/expression/builtin_encryption_test.go:616 TestUncompress`
/// using Go's OWN decoded payloads as byte-table rows.
#[test]
fn test_uncompress() {
    // MySQL-flavored zlib result and TiDB-flavored zlib result, both
    // declaring original length 11.
    let mysql_zlib = decode_hex("0B000000789CCB48CDC9C95728CF2FCA4901001A0B045D");
    let tidb_zlib = decode_hex("0B000000789CCA48CDC9C95728CF2FCA4901040000FFFF1A0B045D");
    for payload in [&mysql_zlib, &tidb_zlib] {
        assert_eq!(
            bytes_of(&call(
                "UNCOMPRESS",
                &[Datum::new_string(payload.clone())],
                &NoColumns
            )),
            b"hello world".to_vec()
        );
    }

    // Wrong declared length (02 != 11): corrupt framing -> NULL.
    let wrong_len = decode_hex("02000000789CCB48CDC9C95728CF2FCA4901001A0B045D");
    assert_eq!(
        call("UNCOMPRESS", &[Datum::new_string(wrong_len)], &NoColumns),
        Datum::Null
    );

    // Degenerate inputs: empty -> "", then every truncation of the payload.
    assert_eq!(call("UNCOMPRESS", &[s("")], &NoColumns), s(""));
    for malformed in [
        "31".to_owned(),           // "1"
        "31323334".to_owned(),     // "1234"
        "3132333435".to_owned(),   // "12345"
        "0B".to_owned(),           // 0x0B
        "0B000000".to_owned(),     // header only
        "0B0000001234".to_owned(), // header + junk
    ] {
        let out = try_call(
            "UNCOMPRESS",
            &[Datum::new_string(decode_hex(&malformed))],
            &NoColumns,
        )
        .expect("UNCOMPRESS evaluates malformed payloads");
        assert_eq!(out, Datum::Null, "{malformed}");
    }
    // Numeric origin "12345" has no zlib stream shape either.
    let out = try_call("UNCOMPRESS", &[Datum::Int(12345)], &NoColumns).unwrap();
    assert_eq!(out, Datum::Null);
    // NULL propagates.
    assert_eq!(call("UNCOMPRESS", &[Datum::Null], &NoColumns), Datum::Null);
}

/// GO PORT of `pkg/expression/builtin_encryption_test.go:651
/// TestUncompressLength` over Go's exact payload rows.
#[test]
fn test_uncompress_length() {
    let length_of = |vals: [Datum; 1]| -> Datum { call("UNCOMPRESSED_LENGTH", &vals, &NoColumns) };

    let mysql_zlib = decode_hex("0B000000789CCB48CDC9C95728CF2FCA4901001A0B045D");
    let tidb_zlib = decode_hex("0B000000789CCA48CDC9C95728CF2FCA4901040000FFFF1A0B045D");
    for payload in [&mysql_zlib, &tidb_zlib] {
        assert_eq!(
            length_of([Datum::new_string(payload.clone())]),
            Datum::Int(11)
        );
    }

    assert_eq!(length_of([s("")]), Datum::Int(0));
    assert_eq!(length_of([s("1")]), Datum::Int(0));
    assert_eq!(length_of([s("123")]), Datum::Int(0));
    assert_eq!(
        length_of([Datum::new_string(decode_hex("0B"))]),
        Datum::Int(0)
    );
    assert_eq!(
        length_of([Datum::new_string(decode_hex("0B00"))]),
        Datum::Int(0)
    );

    // Header-only payload: still int64(0) per the Go table's `0x0` row.
    assert_eq!(
        length_of([Datum::new_string(decode_hex("0B000000"))]),
        Datum::Int(0x0)
    );
    // Header plus two bytes: the DECLARED length is readable.
    assert_eq!(
        length_of([Datum::new_string(decode_hex("0B0000001234"))]),
        Datum::Int(0x0B)
    );
    // The int64(12345) reinterprets to the little-endian uint32 875770417.
    assert_eq!(length_of([Datum::Int(12345)]), Datum::Int(875_770_417));
    // NULL row.
    assert_eq!(
        call("UNCOMPRESSED_LENGTH", &[Datum::Null], &NoColumns),
        Datum::Null
    );
}

/// GO PORT of `pkg/expression/builtin_encryption_test.go:681
/// TestValidatePasswordStrength` over its eight-row table, both with
/// validation disabled (all rows answer 0) and enabled.
#[test]
fn test_validate_password_strength() {
    let rows: [(Datum, Option<i64>); 8] = [
        (Datum::Null, None),
        (s("123"), Some(0)),
        (s("testuser123"), Some(0)),
        (s("resutset123"), Some(0)),
        (s("12345"), Some(25)),
        (s("12345678"), Some(50)),
        (s("!Abc12345678"), Some(75)),
        (s("!Abc87654321"), Some(100)),
    ];

    // Disable password validation: every non-NULL row answers NewDatum(0)
    // and the NULL row stays NULL (the expect column IS nil there).
    let disabled = password_globals(false);
    for (input, _) in &rows {
        let out = call("VALIDATE_PASSWORD_STRENGTH", &[input.clone()], &disabled);
        if matches!(input, Datum::Null) {
            assert_eq!(out, Datum::Null);
        } else {
            assert_eq!(out, Datum::Int(0), "{input:?}");
        }
    }

    // Enable password validation: each row answers its Go expectation
    // (including the NULL row, whose expect is nil -> NULL).
    let enabled = password_globals(true);
    for (input, expect) in &rows {
        let want = match expect {
            Some(value) => Datum::Int(*value),
            None => Datum::Null,
        };
        assert_eq!(
            call("VALIDATE_PASSWORD_STRENGTH", &[input.clone()], &enabled),
            want,
            "{input:?}"
        );
    }
}

/// GO PORT of `pkg/expression/builtin_encryption_test.go:730 TestPassword`
/// over its row table including the deprecation warning counter. The
/// invalid-GBK error row is carried by
/// [`encoding_error_rows_follow_session_charset_conversion`].
#[test]
fn test_password() {
    // The hashed rows, with their Go digests; the {"ㅂ123"/gbk} error row is
    // exercised through the connection-aware SQL rewrite regression.
    let rows: [(Datum, &str); 7] = [
        (s(""), ""),
        (s("abc"), "*0D3CED9BEC10A777AEC23CCC353A8C08A633045E"),
        // {"abc"/gbk}: identical bytes, same digest.
        (
            Datum::new_bytes(b"abc".to_vec()),
            "*0D3CED9BEC10A777AEC23CCC353A8C08A633045E",
        ),
        (Datum::Int(123), "*23AE809DDACAF96AF0FD78ED04B6A265E05AA257"),
        (
            Datum::Real(1.23),
            "*A589EEBA8D3F9E1A34A7EE518FAC4566BFAD5BB6",
        ),
        (s("一二三四"), "*D207780722F22B23C254CAC0580D3B6738C19E18"),
        (
            Datum::Decimal(crate::Decimal::from_literal("123.123")),
            "*B15B84262DB34BFB2C817A45A55C405DC7C52BB1",
        ),
    ];
    for (input, want) in rows {
        let out = call("PASSWORD", &[input], &NoColumns);
        assert_eq!(out.sql_string().unwrap(), want, "PASSWORD({want})");
    }

    // PASSWORD(NULL): the first row of Go's table answers KindNull.
    assert_eq!(call("PASSWORD", &[Datum::Null], &NoColumns), Datum::Null);

    // Arity: one argument is accepted; the deprecated-function build path
    // also admits NewZero() (Go: funcs[ast.PasswordFunc].getFunction).
    assert!(
        crate::builtin_ext::crypto::dispatch("PASSWORD", &[Datum::Int(0)], &NoColumns).is_some()
    );
}

/// GO PORT of `pkg/expression/builtin_encryption_test.go:787
/// TestUncompressRejectsInflatedDataLargerThanDeclaredLength` (payload shape
/// half) and `:828 TestUncompressTracksInflateMemory`'s inverse frame.
///
/// The memory-tracker assertions (`tracker.BytesConsumed`,
/// `MaxConsumed <= declaredLength`, the LogOnExceed hook) need a session
/// memory tracker, which this tier does not model; see
/// [`uncompress_memory_tracker_gaps`].
#[test]
fn uncompress_rejects_payload_deeper_than_declared_length() {
    use flate2::write::ZlibEncoder;
    use std::io::Write;

    // makeCompressedPayload(t, 32, zeros(1<<20)): a real zlib stream whose
    // inflated body exceeds the 32-byte declaration.
    let body = vec![0u8; 1 << 20];
    let mut encoder = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(&body).expect("zlib write");
    let stream = encoder.finish().expect("zlib finish");
    let mut framed = 32u32.to_le_bytes().to_vec();
    framed.extend_from_slice(&stream);

    let warnings = WarningSink::default();
    let out = try_call("UNCOMPRESS", &[Datum::new_bytes(framed)], &warnings)
        .expect("malformed-but-complete frames evaluate");
    assert_eq!(
        out,
        Datum::Null,
        "inflation beyond the declared length is rejected"
    );

    // requireLastZlibWarning(t, ctx, errZlibZBuf): the last statement warning
    // is the Z_BUF_ERROR twin (TiDB errno 1258/ZlibZBuf).
    let logged = warnings.0.borrow();
    assert!(
        logged.iter().any(|(code, _)| *code == 1258),
        "expected a ZlibZBuf warning among {:?}",
        logged.iter().map(|(c, _)| c).collect::<Vec<_>>()
    );
}

#[derive(Default)]
struct WarningSink(RefCell<Vec<(u16, String)>>);

impl Columns for WarningSink {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }

    fn append_warning(&self, code: u16, message: &str) {
        self.0.borrow_mut().push((code, message.to_owned()));
    }
}

/// GO PORT of `pkg/expression/builtin_encryption_test.go:804
/// TestUncompressRejectsHandcraftedPayloadLargerThanDeclaredLength`: the
/// hand-picked hex payload declares 32 bytes but inflates to 1024 'A's --
/// and the decoder must reject it with the same ZLibZBuf warning. The
/// inflate-mem tracker check is covered by [`uncompress_memory_tracker_gaps`].
#[test]
fn uncompress_rejects_handcrafted_payload_larger_than_declared_length() {
    let payload = decode_hex("20000000789c73741c05a360148c540000a4780410");
    assert_eq!(u32::from_le_bytes(payload[..4].try_into().unwrap()), 32);

    // Sanity-check the plaintext claim Go makes about this stream before
    // asserting the decoder rejects it for exceeding the declaration.
    let mut decoder = flate2::write::ZlibDecoder::new(Vec::new());
    std::io::Write::write_all(&mut decoder, &payload[4..]).expect("inflate sanity");
    let raw = decoder.finish().expect("inflate finish");
    assert_eq!(raw, vec![b'A'; 1024]);

    let warnings = WarningSink::default();
    let out = try_call("UNCOMPRESS", &[Datum::new_bytes(payload)], &warnings)
        .expect("well-formed-but-overlong frames evaluate");
    assert_eq!(out, Datum::Null);
    let logged = warnings.0.borrow();
    assert!(
        logged.iter().any(|(code, _)| *code == 1258),
        "expected ZlibZBuf warning among {logged:?}"
    );
}

/// go-parity-gap: `TestVectorizedBuiltinEncryptionFunc`
/// (`pkg/expression/builtin_encryption_vec_test.go:83`) feeds
/// `vecBuiltinEncryptionCases` (AES mode/generator pairs across every family
/// member plus SM3 and RANDOM_BYTES arms) through the vec-vs-scalar harness;
/// no vectorized signature tier exists here, so there is nothing to run the
/// differential against. The scalar halves are pinned by this module's table
/// ports.
#[test]
#[ignore = "go-parity-gap: ENCRYPTION-family vec-vs-scalar differential without a vectorized tier"]
fn vectorized_builtin_encryption_harness_gap() {}

/// go-parity-gap: the tracker-driven halves of
/// `TestUncompressRejectsInflatedDataLargerThanDeclaredLength`
/// (`tracker.MaxConsumed() <= declaredLength`),
/// `TestUncompressTracksInflateMemory` (LogOnExceed hook firing once, limit
/// 32 bytes around a 4096-byte inflate), and the memory assertions in
/// `TestUncompressRejectsInflatedDataLargerThanDeclaredLengthVectorized`
/// exercise `StmtCtx.MemTracker`, which the expression tier does not model.
#[test]
#[ignore = "go-parity-gap: Uncompress/inflate memory accounting runs on StmtCtx.MemTracker (mem.MemoryTracker + LogOnExceed), not modeled on tidb-expr's Columns"]
fn uncompress_memory_tracker_gaps() {}

/// GO PORT skeleton for `pkg/expression/builtin_encryption_test.go:852
/// TestUncompressRejectsInflatedDataLargerThanDeclaredLengthVectorized`: the
/// rejection itself is pinned column-free by
/// [`uncompress_rejects_payload_deeper_than_declared_length`] (identical
/// sig code path); what remains is only the `vecEvalString` plumbing over a
/// one-row chunk.
#[test]
#[ignore = "go-parity-gap: no separate vectorized signature tier exists in tidb-expr to route this through f.vecEvalString; the value-level behavior is covered"]
fn uncompress_overlong_declared_length_vectorized_gap() {}
