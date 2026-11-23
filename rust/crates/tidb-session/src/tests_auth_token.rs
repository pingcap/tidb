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

//! Go `ConnectionVerification`'s `tidb_auth_token` branch: the JWT the
//! client sends (NUL-terminated) is verified against `GlobalJWKS` and its
//! claims against the account's name, `metadata.email` and `Token_issuer`.

use base64::Engine as _;
use openssl::hash::MessageDigest;
use openssl::pkey::PKey;
use openssl::rsa::Rsa;

use crate::domain::configured_user_store::ConfiguredUserStore;
use crate::domain::mock_store::MockStoreDomain;
use crate::domain::secure_transport::TransportKind;

fn token(key: &Rsa<openssl::pkey::Private>, kid: &str, sub: &str, email: &str, iss: &str) -> Vec<u8> {
    let b64 = |bytes: &[u8]| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    let header = serde_json::json!({ "alg": "RS256", "kid": kid, "typ": "JWT" });
    let payload = serde_json::json!({ "sub": sub, "email": email, "iss": iss, "iat": now, "exp": now + 3600 });
    let input = format!("{}.{}", b64(header.to_string().as_bytes()), b64(payload.to_string().as_bytes()));
    let pkey = PKey::from_rsa(key.clone()).unwrap();
    let mut signer = openssl::sign::Signer::new(MessageDigest::sha256(), &pkey).unwrap();
    signer.update(input.as_bytes()).unwrap();
    // Go reads `authentication[:len(authentication)-1]`: the token is sent
    // NUL-terminated.
    let mut response = format!("{input}.{}", b64(&signer.sign_to_vec().unwrap())).into_bytes();
    response.push(0);
    response
}

#[test]
fn tidb_auth_token_login_verifies_the_jwt_and_its_claims() {
    let key = Rsa::generate(2048).unwrap();
    let jwks = serde_json::json!({ "keys": [{
        "kty": "RSA", "alg": "RS256", "kid": "the-key-id-0", "use": "sig",
        "n": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(key.n().to_vec()),
        "e": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(key.e().to_vec()),
    }]});
    let path = std::env::temp_dir().join(format!("tidb-session-jwks-{}.json", std::process::id()));
    std::fs::write(&path, jwks.to_string()).unwrap();
    tidb_privilege::tidb_auth_token::GLOBAL_JWKS.load_jwks_4_auth_token(None, path.to_str().unwrap()).unwrap();

    let store = MockStoreDomain::create().unwrap();
    let users = ConfiguredUserStore::from_accounts(store.new_session().unwrap().privilege_manager().unwrap());
    let mut root = store.new_session().unwrap();
    root.run(
        r#"CREATE USER `user1@pingcap.com`@'localhost' IDENTIFIED WITH 'tidb_auth_token' REQUIRE token_issuer 'issuer1' ATTRIBUTE '{"email": "user1@pingcap.com"}'"#,
    )
    .unwrap();
    let login = |response: &[u8]| {
        let admission = users.admit_transport(TransportKind::PlainTcp).unwrap();
        users.authenticate_with_plugin("user1@pingcap.com", "localhost", &[], response, "tidb_auth_token", admission, None, None)
    };
    let user = "user1@pingcap.com";
    login(&token(&key, "the-key-id-0", user, user, "issuer1")).expect("a valid token logs in");
    // Each claim names the account, and the key must be the set's.
    assert!(login(&token(&key, "the-key-id-0", user, user, "issuer2")).is_err(), "wrong iss");
    assert!(login(&token(&key, "the-key-id-0", user, "other@pingcap.com", "issuer1")).is_err(), "wrong email");
    assert!(login(&token(&key, "the-key-id-0", "other", user, "issuer1")).is_err(), "wrong sub");
    assert!(login(&token(&key, "the-key-id-1", user, user, "issuer1")).is_err(), "unknown kid");
    let other_key = Rsa::generate(2048).unwrap();
    assert!(login(&token(&other_key, "the-key-id-0", user, user, "issuer1")).is_err(), "wrong signature");
    assert!(login(b"").is_err(), "empty authentication");
}
