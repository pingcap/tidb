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

//! Go `pkg/privilege/privileges/tidb_auth_token_test.go`.

use std::sync::LazyLock;

use openssl::pkey::Private;
use openssl::rsa::Rsa;

use super::*;

const EMAIL1: &str = "user1@pingcap.com";
const EMAIL2: &str = "user2@pingcap.com";
const ISSUER1: &str = "issuer1";
const ISSUER2: &str = "issuer2";

/// Go's three RSA keys, each published with `alg` RS256, `kid`
/// `the-key-id-i` and `use` sig; `path[0]` holds key 0, `path[1]` keys 0 and
/// 1, `path[2]` key 2.
struct Fixture {
    pri_keys: Vec<Rsa<Private>>,
    path: [String; 3],
}

static FIXTURE: LazyLock<Fixture> = LazyLock::new(|| {
    let pri_keys: Vec<Rsa<Private>> = (0..3).map(|_| Rsa::generate(2048).expect("rsa key")).collect();
    let jwk = |i: usize| {
        let key = &pri_keys[i];
        serde_json::json!({
            "kty": "RSA",
            "alg": "RS256",
            "kid": format!("the-key-id-{i}"),
            "use": "sig",
            "n": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(key.n().to_vec()),
            "e": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(key.e().to_vec()),
        })
    };
    let dir = std::env::temp_dir().join(format!("tidb-auth-token-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let path: [String; 3] = std::array::from_fn(|i| {
        let keys: Vec<serde_json::Value> = if i == 2 { vec![jwk(2)] } else { (0..=i).map(jwk).collect() };
        let file = dir.join(format!("jwks{i}.json"));
        std::fs::write(&file, serde_json::to_vec_pretty(&serde_json::json!({ "keys": keys })).unwrap()).unwrap();
        file.to_string_lossy().into_owned()
    });
    Fixture { pri_keys, path }
});

/// Go `getSignedTokenString`: an RS256 JWT whose header carries `kid` and
/// whose payload carries the registered claims among `pairs`.
fn get_signed_token_string(pri_key: &Rsa<Private>, pairs: &serde_json::Map<String, serde_json::Value>) -> String {
    let mut header = serde_json::json!({ "alg": "RS256", "typ": "JWT" });
    let mut payload = serde_json::Map::new();
    for (k, v) in pairs {
        match k.as_str() {
            "kid" => header["kid"] = v.clone(),
            "sub" | "iat" | "exp" | "iss" | "email" => {
                payload.insert(k.clone(), v.clone());
            }
            _ => {}
        }
    }
    let b64 = |bytes: &[u8]| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
    let signing_input = format!("{}.{}", b64(header.to_string().as_bytes()), b64(serde_json::Value::Object(payload).to_string().as_bytes()));
    let pkey = PKey::from_rsa(pri_key.clone()).unwrap();
    let mut signer = openssl::sign::Signer::new(MessageDigest::sha256(), &pkey).unwrap();
    signer.update(signing_input.as_bytes()).unwrap();
    format!("{signing_input}.{}", b64(&signer.sign_to_vec().unwrap()))
}

fn now_unix() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}

fn claims_of(kid: &str, now: i64) -> serde_json::Map<String, serde_json::Value> {
    let mut claims = serde_json::Map::new();
    claims.insert("kid".into(), kid.into());
    claims.insert("sub".into(), EMAIL1.into());
    claims.insert("email".into(), EMAIL1.into());
    claims.insert("iat".into(), now.into());
    claims.insert("exp".into(), (now + 100 * 3600).into());
    claims.insert("iss".into(), ISSUER1.into());
    claims
}

fn jwks_at(path: &str) -> &'static JwksImpl {
    let jwks: &'static JwksImpl = Box::leak(Box::default());
    jwks.load_jwks_4_auth_token(None, path).expect(path);
    jwks
}

fn unix(time: &Claim) -> i64 {
    match time {
        Claim::Time(time) => time.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
        other => panic!("{other:?} is not a time"),
    }
}

/// Go `TestAuthTokenClaims`.
#[test]
fn test_auth_token_claims() {
    let fixture = &*FIXTURE;
    let jwks = jwks_at(&fixture.path[0]);
    let now = now_unix();
    let claims = claims_of("the-key-id-0", now);
    let signed = get_signed_token_string(&fixture.pri_keys[0], &claims);
    let mut verified = jwks.check_sig_with_retry(&signed, 0).expect("verifies");
    for k in ["sub", "email", "iss"] {
        assert_eq!(verified[k], Claim::Value(claims[k].clone()));
    }
    for k in ["iat", "exp"] {
        assert_eq!(unix(&verified[k]), claims[k].as_i64().unwrap());
    }
    let mut record = AuthTokenRecord { user: EMAIL1, email: EMAIL1, auth_token_issuer: ISSUER1 };

    // Success
    check_auth_token_claims(&verified, &record, DEFAULT_TOKEN_LIFE).unwrap();

    let string = |s: &str| Claim::Value(s.into());
    let expect_err = |verified: &BTreeMap<String, Claim>, record: &AuthTokenRecord<'_>, life: Duration, want: &str| {
        let err = check_auth_token_claims(verified, record, life).unwrap_err();
        assert!(err.contains(want), "{err} does not contain {want}");
    };

    // test 'sub'
    verified.insert("sub".into(), string(EMAIL2));
    expect_err(&verified, &record, DEFAULT_TOKEN_LIFE, "Wrong 'sub'");
    verified.remove("sub");
    expect_err(&verified, &record, DEFAULT_TOKEN_LIFE, "lack 'sub'");
    verified.insert("sub".into(), string(EMAIL1));

    // test 'email'
    verified.insert("email".into(), string(EMAIL2));
    expect_err(&verified, &record, DEFAULT_TOKEN_LIFE, "Wrong 'email'");
    verified.remove("email");
    expect_err(&verified, &record, DEFAULT_TOKEN_LIFE, "lack 'email'");
    verified.insert("email".into(), string(EMAIL1));

    // test 'iat'
    let now_time = UNIX_EPOCH + Duration::from_secs(now as u64);
    verified.remove("iat");
    expect_err(&verified, &record, DEFAULT_TOKEN_LIFE, "lack 'iat'");
    verified.insert("iat".into(), string("abc"));
    expect_err(&verified, &record, DEFAULT_TOKEN_LIFE, "iat: abc is not a value of time.Time");
    std::thread::sleep(Duration::from_secs(2));
    verified.insert("iat".into(), Claim::Time(now_time));
    expect_err(&verified, &record, Duration::from_secs(1), "the token has been out of its life time");
    verified.insert("iat".into(), Claim::Time(now_time + Duration::from_secs(3600)));
    expect_err(&verified, &record, DEFAULT_TOKEN_LIFE, "the token is issued at a future time");
    verified.insert("iat".into(), Claim::Time(now_time));

    // test 'exp'
    verified.remove("exp");
    expect_err(&verified, &record, DEFAULT_TOKEN_LIFE, "lack 'exp'");
    verified.insert("exp".into(), string("abc"));
    expect_err(&verified, &record, DEFAULT_TOKEN_LIFE, "exp: abc is not a value of time.Time");
    verified.insert("exp".into(), Claim::Time(now_time));
    expect_err(&verified, &record, DEFAULT_TOKEN_LIFE, "the token has been expired");
    verified.insert("exp".into(), Claim::Time(now_time + Duration::from_secs(100 * 3600)));

    // test token_issuer
    verified.remove("iss");
    expect_err(&verified, &record, DEFAULT_TOKEN_LIFE, "lack 'iss'");
    verified.insert("iss".into(), string(ISSUER1));
    record.auth_token_issuer = ISSUER2;
    expect_err(&verified, &record, DEFAULT_TOKEN_LIFE, "Wrong 'iss");
}

/// Go `TestJWKSImpl`.
#[test]
fn test_jwks_impl() {
    let fixture = &*FIXTURE;
    let jwks: &'static JwksImpl = Box::leak(Box::default());

    // Set wrong path of JWKS
    assert!(jwks.load_jwks_4_auth_token(None, "wrong-jwks-path").is_err());
    assert!(jwks.load().is_err());
    assert!(jwks.check_sig_with_retry("invalid tokenString", 4).is_err());
    assert!(jwks.verify("invalid tokenString").is_err());

    jwks.load_jwks_4_auth_token(None, &fixture.path[0]).unwrap();
    let now = now_unix();
    let mut claims = claims_of("the-key-id-0", now);
    let signed = get_signed_token_string(&fixture.pri_keys[0], &claims);
    let parts: Vec<&str> = signed.split('.').collect();

    // Wrong encoded JWT format
    let err = jwks.check_sig_with_retry(&format!("{}.{}", parts[0], parts[1]), 0).unwrap_err();
    assert!(err.contains("Invalid JWT"), "{err}");
    let err = jwks.check_sig_with_retry(&format!("{signed}.{}", parts[1]), 0).unwrap_err();
    assert!(err.contains("Invalid JWT"), "{err}");

    // Wrong signature
    let err = jwks.check_sig_with_retry(&format!("{signed}A"), 0).unwrap_err();
    assert!(err.contains("could not verify message using any of the signatures or keys"), "{err}");

    // Wrong signature, and fail to reload JWKS
    *jwks.filepath.lock().unwrap() = "wrong-path".to_owned();
    let err = jwks.check_sig_with_retry(&format!("{signed}A"), 0).unwrap_err();
    assert!(err.contains("open wrong-path: no such file or directory"), "{err}");
    *jwks.filepath.lock().unwrap() = fixture.path[0].clone();

    jwks.load_jwks_4_auth_token(None, &fixture.path[0]).unwrap();
    jwks.check_sig_with_retry(&signed, 0).unwrap();

    // Wrong kid
    claims.insert("kid".into(), "the-key-id-1".into());
    let signed = get_signed_token_string(&fixture.pri_keys[0], &claims);
    assert!(jwks.check_sig_with_retry(&signed, 0).is_err());
    claims.insert("kid".into(), "the-key-id-0".into());
    let signed = get_signed_token_string(&fixture.pri_keys[0], &claims);
    jwks.load_jwks_4_auth_token(None, &fixture.path[1]).unwrap();
    jwks.check_sig_with_retry(&signed, 0).unwrap();
    jwks.load_jwks_4_auth_token(None, &fixture.path[2]).unwrap();
    assert!(jwks.check_sig_with_retry(&signed, 0).is_err());
}
