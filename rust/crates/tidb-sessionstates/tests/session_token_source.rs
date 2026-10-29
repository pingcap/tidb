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

//! Go `session_token_test.go`.

use std::path::Path;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use openssl::asn1::Asn1Time;
use openssl::bn::BigNum;
use openssl::ec::{EcGroup, EcKey};
use openssl::hash::MessageDigest;
use openssl::nid::Nid;
use openssl::pkey::{PKey, Private};
use openssl::rsa::Rsa;
use openssl::x509::{X509Builder, X509NameBuilder};
use tidb_sessionstates::{
    create_session_token, reload_signing_cert, set_cert_path, set_key_path,
    validate_session_token, SessionToken, LOAD_CERT_INTERVAL,
};

const TOKEN_LIFETIME: Duration = Duration::from_secs(60);
const OLD_CERT_VALID_TIME: Duration = Duration::from_secs(15 * 60);
const MINUTE: Duration = Duration::from_secs(60);

/// The signing certificate is process-wide, as Go's is; the tests take
/// turns with it.
static SIGNING_CERT: Mutex<()> = Mutex::new(());

fn signing_cert() -> MutexGuard<'static, ()> {
    let guard = SIGNING_CERT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    set_key_path("");
    set_cert_path("");
    guard
}

enum KeyAlgorithm {
    Rsa(u32),
    Ecdsa,
    Ed25519,
}

/// Go `util.CreateCertificates` for one key algorithm and signature digest.
fn create_certificates(cert_path: &Path, key_path: &Path, key: &KeyAlgorithm, digest: MessageDigest) {
    let key: PKey<Private> = match key {
        KeyAlgorithm::Rsa(bits) => PKey::from_rsa(Rsa::generate(*bits).unwrap()).unwrap(),
        KeyAlgorithm::Ecdsa => PKey::from_ec_key(
            EcKey::generate(&EcGroup::from_curve_name(Nid::X9_62_PRIME256V1).unwrap()).unwrap(),
        )
        .unwrap(),
        KeyAlgorithm::Ed25519 => PKey::generate_ed25519().unwrap(),
    };
    let mut name = X509NameBuilder::new().unwrap();
    name.append_entry_by_nid(Nid::COMMONNAME, "TiDB_Server").unwrap();
    let name = name.build();
    let mut builder = X509Builder::new().unwrap();
    builder.set_version(2).unwrap();
    builder
        .set_serial_number(&BigNum::from_u32(1).unwrap().to_asn1_integer().unwrap())
        .unwrap();
    builder.set_subject_name(&name).unwrap();
    builder.set_issuer_name(&name).unwrap();
    builder.set_pubkey(&key).unwrap();
    builder.set_not_before(&Asn1Time::days_from_now(0).unwrap()).unwrap();
    builder.set_not_after(&Asn1Time::days_from_now(90).unwrap()).unwrap();
    builder.sign(&key, digest).unwrap();
    std::fs::write(cert_path, builder.build().to_pem().unwrap()).unwrap();
    std::fs::write(key_path, key.private_key_to_pem_pkcs8().unwrap()).unwrap();
}

fn create_rsa_cert(cert_path: &Path, key_path: &Path) {
    create_certificates(cert_path, key_path, &KeyAlgorithm::Rsa(2048), MessageDigest::sha256());
}

fn create_new_token(username: &str) -> (SessionToken, Vec<u8>) {
    let token = create_session_token(username).unwrap();
    let bytes = tidb_stringutil::go_json_marshal(&token).into_bytes();
    (token, bytes)
}

fn setup_signing_cert_for_test(dir: &Path) {
    let cert_path = dir.join("test1_cert.pem");
    let key_path = dir.join("test1_key.pem");
    create_rsa_cert(&cert_path, &key_path);
    set_key_path(key_path.to_str().unwrap());
    set_cert_path(cert_path.to_str().unwrap());
}

/// Go `failpoint.Enable(mockNowOffset, "return(...)")`.
fn mock_now_offset(offset: Duration) {
    fail::cfg("mockNowOffset", &format!("return({})", offset.as_nanos())).unwrap();
}

#[test]
fn set_cert_and_key() {
    let _guard = signing_cert();
    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("test1_cert.pem");
    let key_path = dir.path().join("test1_key.pem");
    create_rsa_cert(&cert_path, &key_path);

    // no cert and no key
    let error = create_session_token("test_user").unwrap_err();
    assert!(error.to_string().contains("no certificate or key file"), "{error}");
    // no cert
    set_key_path(key_path.to_str().unwrap());
    let error = create_session_token("test_user").unwrap_err();
    assert!(error.to_string().contains("no certificate or key file"), "{error}");
    // no key
    set_key_path("");
    set_cert_path(cert_path.to_str().unwrap());
    let error = create_session_token("test_user").unwrap_err();
    assert!(error.to_string().contains("no certificate or key file"), "{error}");
    // both configured
    set_key_path(key_path.to_str().unwrap());
    create_session_token("test_user").unwrap();
    // When the key and cert don't match, it will still use the old pair.
    let cert_path2 = dir.path().join("test2_cert.pem");
    let key_path2 = dir.path().join("test2_key.pem");
    create_rsa_cert(&cert_path2, &key_path2);
    set_key_path(key_path2.to_str().unwrap());
    create_session_token("test_user").unwrap();
}

#[test]
fn sign_algo() {
    let _guard = signing_cert();
    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("test1_cert.pem");
    let key_path = dir.path().join("test1_key.pem");
    set_key_path(key_path.to_str().unwrap());
    set_cert_path(cert_path.to_str().unwrap());
    let digests = || [MessageDigest::sha256(), MessageDigest::sha384(), MessageDigest::sha512()];
    let mut cases = Vec::new();
    for bits in [2048, 4096] {
        for digest in digests() {
            cases.push((KeyAlgorithm::Rsa(bits), digest));
        }
    }
    for digest in digests() {
        cases.push((KeyAlgorithm::Ecdsa, digest));
    }
    // Ed25519 signs without a separate digest.
    cases.push((KeyAlgorithm::Ed25519, MessageDigest::null()));
    for (key, digest) in &cases {
        create_certificates(&cert_path, &key_path, key, *digest);
        reload_signing_cert();
        let (_, token_bytes) = create_new_token("test_user");
        validate_session_token(&token_bytes, "test_user").unwrap();
    }
}

#[test]
fn verify_token() {
    let _guard = signing_cert();
    let dir = tempfile::tempdir().unwrap();
    setup_signing_cert_for_test(dir.path());

    // check succeeds
    let (mut token, token_bytes) = create_new_token("test_user");
    validate_session_token(&token_bytes, "test_user").unwrap();
    // the token expires
    mock_now_offset(TOKEN_LIFETIME + MINUTE);
    let error = validate_session_token(&token_bytes, "test_user").unwrap_err();
    fail::remove("mockNowOffset");
    assert!(error.contains("token expired"), "{error}");
    // the current user is different with the token
    let error = validate_session_token(&token_bytes, "another_user").unwrap_err();
    assert!(error.contains("username does not match"), "{error}");
    // forge the user name
    token.username = "another_user".to_owned();
    let forged = tidb_stringutil::go_json_marshal(&token);
    let error = validate_session_token(forged.as_bytes(), "another_user").unwrap_err();
    assert!(error.contains("verification error"), "{error}");
    // forge the expire time
    token.username = "test_user".to_owned();
    token.expire_time = chrono::Local::now().fixed_offset() - chrono::Duration::minutes(1);
    let forged = tidb_stringutil::go_json_marshal(&token);
    let error = validate_session_token(forged.as_bytes(), "test_user").unwrap_err();
    assert!(error.contains("verification error"), "{error}");
}

#[test]
fn cert_expire() {
    let _guard = signing_cert();
    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("test1_cert.pem");
    let key_path = dir.path().join("test1_key.pem");
    create_rsa_cert(&cert_path, &key_path);
    set_key_path(key_path.to_str().unwrap());
    set_cert_path(cert_path.to_str().unwrap());

    let (_, token_bytes) = create_new_token("test_user");
    validate_session_token(&token_bytes, "test_user").unwrap();
    // replace the cert, but the old cert is still valid for a while
    let cert_path2 = dir.path().join("test2_cert.pem");
    let key_path2 = dir.path().join("test2_key.pem");
    create_rsa_cert(&cert_path2, &key_path2);
    set_key_path(key_path2.to_str().unwrap());
    set_cert_path(cert_path2.to_str().unwrap());
    validate_session_token(&token_bytes, "test_user").unwrap();
    // the old cert expires and the original token is invalid
    let mut offset = LOAD_CERT_INTERVAL;
    mock_now_offset(offset);
    reload_signing_cert();
    offset += OLD_CERT_VALID_TIME + MINUTE;
    mock_now_offset(offset);
    let error = validate_session_token(&token_bytes, "test_user").unwrap_err();
    assert!(error.contains("verification error"), "{error}");
    // the new cert is not rotated but is reloaded
    let (_, token_bytes) = create_new_token("test_user");
    reload_signing_cert();
    validate_session_token(&token_bytes, "test_user").unwrap();
    // the cert is rotated but is still valid
    create_rsa_cert(&cert_path2, &key_path2);
    offset += LOAD_CERT_INTERVAL;
    mock_now_offset(offset);
    reload_signing_cert();
    let error = validate_session_token(&token_bytes, "test_user").unwrap_err();
    assert!(error.contains("token expired"), "{error}");
    // after some time, it's not valid
    offset += OLD_CERT_VALID_TIME + MINUTE;
    mock_now_offset(offset);
    let error = validate_session_token(&token_bytes, "test_user").unwrap_err();
    fail::remove("mockNowOffset");
    assert!(error.contains("verification error"), "{error}");
}

#[test]
fn load_and_read_concurrently() {
    let _guard = signing_cert();
    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("test1_cert.pem");
    let key_path = dir.path().join("test1_key.pem");
    create_rsa_cert(&cert_path, &key_path);
    set_key_path(key_path.to_str().unwrap());
    set_cert_path(cert_path.to_str().unwrap());

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    std::thread::scope(|scope| {
        // the writer
        scope.spawn(|| {
            while std::time::Instant::now() < deadline {
                create_rsa_cert(&cert_path, &key_path);
                std::thread::sleep(Duration::from_secs(1));
            }
        });
        // the loader
        for _ in 0..2 {
            scope.spawn(|| {
                while std::time::Instant::now() < deadline {
                    reload_signing_cert();
                    std::thread::sleep(Duration::from_millis(500));
                }
            });
        }
        // the reader
        for id in 0..3 {
            scope.spawn(move || {
                let username = format!("test_user_{id}");
                while std::time::Instant::now() < deadline {
                    let (_, token_bytes) = create_new_token(&username);
                    std::thread::sleep(Duration::from_millis(10));
                    validate_session_token(&token_bytes, &username).unwrap();
                    std::thread::sleep(Duration::from_millis(10));
                }
            });
        }
    });
}

/// The token's JSON is Go's: kebab-case times in `time.RFC3339Nano` and a
/// base64 signature, so a token one server signs verifies on the other.
#[test]
fn a_token_go_encoded_decodes_and_re_encodes_byte_for_byte() {
    let go = br#"{"username":"test_user","sign-time":"2026-10-27T16:03:31.123456789+08:00","expire-time":"2026-10-27T16:04:31.12345678+08:00","signature":"AQID"}"#;
    let token: SessionToken = serde_json::from_slice(go).unwrap();
    assert_eq!(token.signature, vec![1, 2, 3]);
    assert_eq!(tidb_stringutil::go_json_marshal(&token).as_bytes(), go);
    let utc = br#"{"username":"u","sign-time":"2026-10-27T08:03:31Z","expire-time":"2026-10-27T08:04:31Z"}"#;
    let token: SessionToken = serde_json::from_slice(utc).unwrap();
    assert!(token.signature.is_empty());
    assert_eq!(tidb_stringutil::go_json_marshal(&token).as_bytes(), utc);
}
