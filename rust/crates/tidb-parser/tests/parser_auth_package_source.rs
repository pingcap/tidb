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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Complete source-test translation for `pkg/parser/auth`.

use tidb_ast::{RestoreCtx, RestoreFlags};
use tidb_mysql::{AuthCachingSha2Password, AuthTiDBSM3Password};
use tidb_parser::auth::{
    check_hashing_password, check_hashing_password_bytes, check_scrambled_password,
    decode_password, encode_password, encode_password_bytes, new_hash_password,
    new_hash_password_bytes, sha1_hash, sm3_hash, AuthError, RoleIdentity, Sm3, UserIdentity,
    HOST_NAME_MAX_LENGTH, USER_NAME_MAX_LENGTH,
};

fn hex_decode(text: &str) -> Vec<u8> {
    let encoded = format!("*{text}");
    decode_password(&encoded).expect("test vector is valid hex")
}

const SHA2_VECTOR: &str = "24412430303524031A69251C34295C4B35167C7F1E5A7B63091349503974624D34504B5A424679354856336868686F52485A736E4A733368786E427575516C73446469496537";
const SM3_VECTOR: &str = "24412430303524031a69251c34295c4b35167c7f1e5a7b63091349536c72627066426a635061762e556e6c63533159414d7762317261324a5a3047756b4244664177434e3043";

#[test]
fn auth_identity_production_contract_is_complete() {
    assert_eq!(USER_NAME_MAX_LENGTH, 32);
    assert_eq!(HOST_NAME_MAX_LENGTH, 255);
    let mut user = UserIdentity {
        username: "login`name".to_owned(),
        hostname: "Host".to_owned(),
        auth_username: "matched".to_owned(),
        auth_hostname: "%".to_owned(),
        auth_plugin: "plugin".to_owned(),
        current_user: false,
    };
    let mut context = RestoreCtx::new(RestoreFlags::DEFAULT, String::new());
    user.restore(&mut context);
    assert_eq!(context.into_inner(), "`login``name`@`Host`");
    assert_eq!(user.to_string(), "matched@%");
    assert_eq!(user.login_string(), "login`name@Host");

    user.current_user = true;
    let mut context = RestoreCtx::new(RestoreFlags::DEFAULT, String::new());
    user.restore(&mut context);
    assert_eq!(context.into_inner(), "CURRENT_USER");

    let role = RoleIdentity {
        username: "admin".to_owned(),
        hostname: String::new(),
    };
    let mut context = RestoreCtx::new(RestoreFlags::DEFAULT, String::new());
    role.restore(&mut context);
    assert_eq!(context.into_inner(), "`admin`");
    assert_eq!(role.to_string(), "`admin`@``");
}

#[test]
fn test_encode_password() {
    let expected = "*23AE809DDACAF96AF0FD78ED04B6A265E05AA257";
    assert_eq!(encode_password("123"), expected);
    assert_eq!(encode_password_bytes(b"123"), expected);
    assert_eq!(encode_password(""), "");
}

#[test]
fn test_decode_password() {
    let decoded = decode_password(&encode_password("123")).unwrap();
    assert_eq!(decoded, sha1_hash(&sha1_hash(b"123")));
    assert!(matches!(decode_password("*0"), Err(AuthError::Hex(_))));
    assert!(matches!(decode_password("*xx"), Err(AuthError::Hex(_))));
}

#[test]
fn test_check_scramble() {
    let salt = [
        85, 92, 45, 22, 58, 79, 107, 6, 122, 125, 58, 80, 12, 90, 103, 32, 90, 10, 74, 82,
    ];
    let auth = [
        24, 180, 183, 225, 166, 6, 81, 102, 70, 248, 199, 143, 91, 204, 169, 9, 161, 171, 203, 33,
    ];
    let password_hash = decode_password(&encode_password("abc")).unwrap();
    assert!(check_scrambled_password(&salt, &password_hash, &auth));
    assert!(!check_scrambled_password(&salt, &password_hash, b"xxyyzz"));
}

#[test]
fn test_check_sha_password_good() {
    assert!(
        check_hashing_password(&hex_decode(SHA2_VECTOR), "foobar", AuthCachingSha2Password,)
            .unwrap()
    );
}

#[test]
fn test_check_sha_password_bad() {
    assert!(!check_hashing_password(
        &hex_decode(SHA2_VECTOR),
        "not_foobar",
        AuthCachingSha2Password,
    )
    .unwrap());
}

#[test]
fn test_check_sha_password_short() {
    assert_eq!(
        check_hashing_password(b"aaaaaaaa", "not_foobar", AuthCachingSha2Password),
        Err(AuthError::HashParts)
    );
}

#[test]
fn test_check_sha_password_digest_type_incompatible() {
    let incompatible = SHA2_VECTOR.replacen("2441", "2442", 1);
    assert_eq!(
        check_hashing_password(
            &hex_decode(&incompatible),
            "not_foobar",
            AuthCachingSha2Password,
        ),
        Err(AuthError::DigestType)
    );
}

#[test]
fn test_check_sha_password_iterations_invalid() {
    let invalid = SHA2_VECTOR.replacen("303035", "303047", 1);
    assert_eq!(
        check_hashing_password(&hex_decode(&invalid), "not_foobar", AuthCachingSha2Password,),
        Err(AuthError::Iterations)
    );
}

#[test]
fn test_new_sha2_password() {
    let password_hash = new_hash_password("testpwd", AuthCachingSha2Password).unwrap();
    assert!(
        check_hashing_password(password_hash.as_bytes(), "testpwd", AuthCachingSha2Password,)
            .unwrap()
    );
    let salt = &password_hash.as_bytes()[7..27];
    assert!(salt
        .iter()
        .all(|byte| *byte < 128 && *byte != 0 && *byte != b'$'));
}

#[test]
fn hashing_password_preserves_the_full_go_string_byte_domain() {
    let password = b"not-utf8-\xff";
    let password_hash =
        new_hash_password_bytes(password, AuthCachingSha2Password).expect("random salt");
    assert!(
        check_hashing_password_bytes(&password_hash, password, AuthCachingSha2Password,).unwrap()
    );
}

#[test]
fn benchmark_sha_password_obligation_executes_one_round() {
    test_check_sha_password_good();
}

#[test]
fn test_sm3() {
    assert_eq!(
        hex::encode(sm3_hash(b"abc")),
        "66c7f0f462eeedd9d1f2d46bdc10e4e24167c4875cf2f7a2297da02b8f4ba8e0"
    );
    assert_eq!(
        hex::encode(sm3_hash(
            b"abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd"
        )),
        "debe9ff92275b8a138604889c18e5a4d6fdb70e5387e5765293dcba39c0c5732"
    );
    let mut incremental = Sm3::default();
    incremental.write(b"a");
    incremental.write(b"bc");
    assert_eq!(incremental.sum(&[]), sm3_hash(b"abc"));
}

#[test]
fn test_check_sm3_password_good() {
    assert!(
        check_hashing_password(&hex_decode(SM3_VECTOR), "foobar", AuthTiDBSM3Password,).unwrap()
    );
}

#[test]
fn test_check_sm3_password_bad() {
    let password_hash = hex_decode("24412430303524031a69251c34295c4b35167c7f1e5a7b6309134956387565426743446d3643446176712f6c4b63323667346e48624872776f39512e4342416a693656676f2f");
    assert!(!check_hashing_password(&password_hash, "not_foobar", AuthTiDBSM3Password).unwrap());
}

#[test]
fn test_check_sm3_password_short() {
    assert_eq!(
        check_hashing_password(b"aaaaaaaa", "not_foobar", AuthTiDBSM3Password),
        Err(AuthError::HashParts)
    );
}

#[test]
fn test_check_sm3_password_digest_type_incompatible() {
    let incompatible = SHA2_VECTOR.replacen("2441", "2443", 1);
    assert_eq!(
        check_hashing_password(
            &hex_decode(&incompatible),
            "not_foobar",
            AuthTiDBSM3Password,
        ),
        Err(AuthError::DigestType)
    );
}

#[test]
fn test_check_sm3_password_iterations_invalid() {
    let invalid = SHA2_VECTOR.replacen("303035", "303047", 1);
    assert_eq!(
        check_hashing_password(&hex_decode(&invalid), "not_foobar", AuthTiDBSM3Password,),
        Err(AuthError::Iterations)
    );
}

#[test]
fn test_new_sm3_password() {
    let password_hash = new_hash_password("testpwd", AuthTiDBSM3Password).unwrap();
    assert!(
        check_hashing_password(password_hash.as_bytes(), "testpwd", AuthTiDBSM3Password,).unwrap()
    );
    let salt = &password_hash.as_bytes()[7..27];
    assert!(salt
        .iter()
        .all(|byte| *byte < 128 && *byte != 0 && *byte != b'$'));
}

#[test]
fn benchmark_sm3_password_obligation_executes_one_round() {
    test_check_sm3_password_good();
}

mod hex {
    pub fn encode(bytes: impl AsRef<[u8]>) -> String {
        bytes
            .as_ref()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }
}
