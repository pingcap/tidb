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

//! Go `pkg/privilege/privileges/tidb_auth_token.go` and
//! `privileges.go`'s `checkAuthTokenClaims`: a `tidb_auth_token` account
//! logs in with a JWT whose signature a JSON Web Key Set verifies
//! (`security.auth-token-jwks`, reloaded every
//! `security.auth-token-refresh-interval`) and whose claims name the account.
//!
//! Go verifies with `lestrrat-go/jwx`'s `jws.Verify(token,
//! jws.WithKeySet(set))`: the token's protected `kid` must name a key of the
//! set, a key whose `use` is not `sig` or that has no `alg` is skipped, and
//! the key's `alg` selects the verifier.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use openssl::bn::BigNum;
use openssl::ec::{EcGroup, EcKey};
use openssl::ecdsa::EcdsaSig;
use openssl::hash::MessageDigest;
use openssl::nid::Nid;
use openssl::pkey::{PKey, Public};
use openssl::rsa::{Padding, Rsa};
use openssl::sign::{RsaPssSaltlen, Verifier};

/// Go `defaultTokenLife`.
pub const DEFAULT_TOKEN_LIFE: Duration = Duration::from_secs(15 * 60);

/// One key of a JWKS, as `jws.Verify` uses it.
struct Jwk {
    kid: String,
    alg: String,
    usage: String,
    key: KeyMaterial,
}

enum KeyMaterial {
    Public(PKey<Public>),
    Symmetric(Vec<u8>),
}

/// Go `JWKSImpl` contains a JSON Web Key Set (JWKS), and a filepath that
/// stores the JWKS.
#[derive(Default)]
pub struct JwksImpl {
    set: RwLock<Option<Arc<Vec<Jwk>>>>,
    filepath: Mutex<String>,
}

/// Go `GlobalJWKS` is the global JWKS for tidb-server.
pub static GLOBAL_JWKS: std::sync::LazyLock<JwksImpl> = std::sync::LazyLock::new(JwksImpl::default);

/// A claim of a verified token (Go `jwt.Token.AsMap`): `iat`, `exp` and
/// `nbf` are times, every other claim its JSON value.
#[derive(Clone, Debug, PartialEq)]
pub enum Claim {
    /// Go `time.Time`.
    Time(SystemTime),
    /// Any other JSON value.
    Value(serde_json::Value),
}

impl Claim {
    fn as_str(&self) -> Option<&str> {
        match self {
            Claim::Value(serde_json::Value::String(value)) => Some(value),
            _ => None,
        }
    }

    /// Go `%s`/`%v` of the claim.
    fn display(&self) -> String {
        match self {
            Claim::Value(serde_json::Value::String(value)) => value.clone(),
            Claim::Value(value) => value.to_string(),
            Claim::Time(time) => format!("{time:?}"),
        }
    }
}

fn b64url(input: &str) -> Result<Vec<u8>, String> {
    let trimmed = input.trim_end_matches('=');
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(trimmed)
        .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(trimmed))
        .map_err(|error| format!("failed to decode base64: {error}"))
}

fn big(field: &serde_json::Value, name: &str) -> Result<BigNum, String> {
    let text = field.get(name).and_then(serde_json::Value::as_str).ok_or_else(|| format!("missing {name}"))?;
    BigNum::from_slice(&b64url(text)?).map_err(|error| error.to_string())
}

/// Go `jwk.ParseKey` of one key of the set.
fn parse_jwk(value: &serde_json::Value) -> Result<Jwk, String> {
    let text = |name: &str| value.get(name).and_then(serde_json::Value::as_str).unwrap_or_default().to_owned();
    let key = match text("kty").as_str() {
        "RSA" => {
            let rsa = Rsa::from_public_components(big(value, "n")?, big(value, "e")?).map_err(|error| error.to_string())?;
            KeyMaterial::Public(PKey::from_rsa(rsa).map_err(|error| error.to_string())?)
        }
        "EC" => {
            let nid = match text("crv").as_str() {
                "P-256" => Nid::X9_62_PRIME256V1,
                "P-384" => Nid::SECP384R1,
                "P-521" => Nid::SECP521R1,
                other => return Err(format!("invalid curve algorithm {other}")),
            };
            let group = EcGroup::from_curve_name(nid).map_err(|error| error.to_string())?;
            let (x, y) = (big(value, "x")?, big(value, "y")?);
            let ec = EcKey::from_public_key_affine_coordinates(&group, &x, &y)
                .map_err(|error| error.to_string())?;
            KeyMaterial::Public(PKey::from_ec_key(ec).map_err(|error| error.to_string())?)
        }
        "OKP" => {
            let x = b64url(&text("x"))?;
            match text("crv").as_str() {
                "Ed25519" => KeyMaterial::Public(
                    PKey::public_key_from_raw_bytes(&x, openssl::pkey::Id::ED25519).map_err(|error| error.to_string())?,
                ),
                other => return Err(format!("unsupported curve {other}")),
            }
        }
        "oct" => KeyMaterial::Symmetric(b64url(&text("k"))?),
        other => return Err(format!("unsupported key type {other}")),
    };
    Ok(Jwk { kid: text("kid"), alg: text("alg"), usage: text("use"), key })
}

/// Go `jwk.ReadFile`: the set's keys.
fn read_file(path: &str) -> Result<Vec<Jwk>, String> {
    let bytes = std::fs::read(path).map_err(|error| go_open_error(path, &error))?;
    let value: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|error| format!("failed to unmarshal JWK set: {error}"))?;
    let keys = match value.get("keys") {
        Some(serde_json::Value::Array(keys)) => keys.iter().map(parse_jwk).collect::<Result<Vec<_>, _>>()?,
        // A single key is a set of one.
        _ => vec![parse_jwk(&value)?],
    };
    Ok(keys)
}

/// Go `os.Open`'s `*PathError` text.
fn go_open_error(path: &str, error: &std::io::Error) -> String {
    let detail = match error.kind() {
        std::io::ErrorKind::NotFound => "no such file or directory".to_owned(),
        std::io::ErrorKind::PermissionDenied => "permission denied".to_owned(),
        _ => error.to_string(),
    };
    format!("open {path}: {detail}")
}

fn digest(alg: &str) -> Option<MessageDigest> {
    Some(match &alg[2..] {
        "256" => MessageDigest::sha256(),
        "384" => MessageDigest::sha384(),
        "512" => MessageDigest::sha512(),
        _ => return None,
    })
}

/// Go `jws.NewVerifier(alg).Verify(payload, signature, key)`.
fn verify_signature(alg: &str, key: &KeyMaterial, payload: &[u8], signature: &[u8]) -> Result<(), String> {
    let failed = || "failed to verify signature".to_owned();
    match (alg, key) {
        ("RS256" | "RS384" | "RS512" | "PS256" | "PS384" | "PS512", KeyMaterial::Public(pkey)) => {
            let md = digest(alg).ok_or_else(failed)?;
            let mut verifier = Verifier::new(md, pkey).map_err(|error| error.to_string())?;
            if alg.starts_with("PS") {
                verifier.set_rsa_padding(Padding::PKCS1_PSS).map_err(|error| error.to_string())?;
                verifier.set_rsa_pss_saltlen(RsaPssSaltlen::DIGEST_LENGTH).map_err(|error| error.to_string())?;
            }
            verifier.update(payload).map_err(|error| error.to_string())?;
            verifier.verify(signature).map_err(|error| error.to_string())?.then_some(()).ok_or_else(failed)
        }
        ("ES256" | "ES384" | "ES512", KeyMaterial::Public(pkey)) => {
            let md = digest(alg).ok_or_else(failed)?;
            let ec = pkey.ec_key().map_err(|error| error.to_string())?;
            let size = signature.len() / 2;
            if size == 0 || signature.len() % 2 != 0 {
                return Err(failed());
            }
            let r = BigNum::from_slice(&signature[..size]).map_err(|error| error.to_string())?;
            let s = BigNum::from_slice(&signature[size..]).map_err(|error| error.to_string())?;
            let sig = EcdsaSig::from_private_components(r, s).map_err(|error| error.to_string())?;
            let hashed = openssl::hash::hash(md, payload).map_err(|error| error.to_string())?;
            sig.verify(&hashed, &ec).map_err(|error| error.to_string())?.then_some(()).ok_or_else(failed)
        }
        ("EdDSA", KeyMaterial::Public(pkey)) => {
            let mut verifier = Verifier::new_without_digest(pkey).map_err(|error| error.to_string())?;
            verifier.verify_oneshot(signature, payload).map_err(|error| error.to_string())?.then_some(()).ok_or_else(failed)
        }
        ("HS256" | "HS384" | "HS512", KeyMaterial::Symmetric(secret)) => {
            let md = digest(alg).ok_or_else(failed)?;
            let pkey = PKey::hmac(secret).map_err(|error| error.to_string())?;
            let mut signer = openssl::sign::Signer::new(md, &pkey).map_err(|error| error.to_string())?;
            signer.update(payload).map_err(|error| error.to_string())?;
            let mac = signer.sign_to_vec().map_err(|error| error.to_string())?;
            openssl::memcmp::eq(&mac, signature).then_some(()).ok_or_else(failed)
        }
        _ => Err(format!("failed to create verifier for algorithm {alg:?}")),
    }
}

impl JwksImpl {
    /// Go `load`.
    fn load(&self) -> Result<(), String> {
        let path = self.filepath.lock().unwrap_or_else(PoisonError::into_inner).clone();
        let cur = read_file(&path)?;
        *self.set.write().unwrap_or_else(PoisonError::into_inner) = Some(Arc::new(cur));
        Ok(())
    }

    /// Go `verify`: `jws.Verify(tokenBytes, jws.WithKeySet(*s))`.
    fn verify(&self, token: &str) -> Result<Vec<u8>, String> {
        let Some(set) = self.set.read().unwrap_or_else(PoisonError::into_inner).clone() else {
            return Err("No valid JWKS yet".to_owned());
        };
        let parts: Vec<&str> = token.split('.').collect();
        let [header, payload, signature] = parts.as_slice() else {
            return Err("failed to parse jws: invalid number of segments".to_owned());
        };
        let protected: serde_json::Value = b64url(header)
            .and_then(|bytes| serde_json::from_slice(&bytes).map_err(|error| error.to_string()))
            .map_err(|error| format!("failed to parse jws: {error}"))?;
        let payload_bytes = b64url(payload).map_err(|error| format!("failed to parse jws: {error}"))?;
        let signature = b64url(signature).map_err(|error| format!("failed to parse jws: {error}"))?;
        let wanted_kid = protected.get("kid").and_then(serde_json::Value::as_str).unwrap_or_default();
        if wanted_kid.is_empty() {
            return Err(r#"key provider 0 failed: failed to find matching key: no key ID ("kid") specified in token"#.to_owned());
        }
        let Some(key) = set.iter().find(|key| key.kid == wanted_kid) else {
            return Err(format!("key provider 0 failed: failed to find key with key ID {wanted_kid:?} in key set"));
        };
        let signing_input = format!("{header}.{payload}");
        // `selectKey`: a key for another use, or with no algorithm, offers
        // nothing to verify with.
        if (key.usage.is_empty() || key.usage == "sig") && !key.alg.is_empty() {
            if verify_signature(&key.alg, &key.key, signing_input.as_bytes(), &signature).is_ok() {
                return Ok(payload_bytes);
            }
        }
        Err("could not verify message using any of the signatures or keys".to_owned())
    }

    /// Go `LoadJWKS4AuthToken` reloads the JWKS every `interval` while
    /// `stop` is unset, after loading it once.
    pub fn load_jwks_4_auth_token(
        &'static self,
        reload: Option<(Duration, Arc<std::sync::atomic::AtomicBool>)>,
        jwks_path: &str,
    ) -> Result<(), String> {
        *self.filepath.lock().unwrap_or_else(PoisonError::into_inner) = jwks_path.to_owned();
        if let Some((interval, stop)) = reload {
            let path = jwks_path.to_owned();
            std::thread::Builder::new()
                .name("jwks-reload".to_owned())
                .spawn(move || loop {
                    let deadline = std::time::Instant::now() + interval;
                    while std::time::Instant::now() < deadline {
                        if stop.load(std::sync::atomic::Ordering::Acquire) {
                            return;
                        }
                        std::thread::sleep(Duration::from_millis(100).min(interval));
                    }
                    if self.load().is_err() {
                        tidb_logutil::bg_logger().error(
                            "Fail to load JWKS",
                            &[
                                tidb_log::Field::new("path", tidb_log::Value::Str(path.clone())),
                                tidb_log::Field::new("interval", tidb_log::Value::Str(format!("{interval:?}"))),
                            ],
                        );
                    }
                })
                .map_err(|error| error.to_string())?;
        }
        self.load()
    }

    /// Go `checkSigWithRetry` verifies the signature in the jwt, and returns
    /// the claims.
    pub fn check_sig_with_retry(&self, token_string: &str, mut retry_time: i32) -> Result<BTreeMap<String, Claim>, String> {
        if token_string.split('.').count() != 3 {
            return Err("Invalid JWT".to_owned());
        }
        let mut err = String::new();
        while retry_time >= 0 {
            retry_time -= 1;
            // verify signature
            let verified_payload = match self.verify(token_string) {
                Ok(payload) => payload,
                Err(error) => {
                    err = error;
                    self.load()?;
                    continue;
                }
            };
            match parse_claims(&verified_payload) {
                Ok(claims) => return Ok(claims),
                Err(error) => err = error,
            }
        }
        Err(format!("Retry time has been spent out: {err}"))
    }
}

/// Go `jwt.New().UnmarshalJSON(payload)` then `AsMap`: the registered time
/// claims become times.
fn parse_claims(payload: &[u8]) -> Result<BTreeMap<String, Claim>, String> {
    let value: serde_json::Value = serde_json::from_slice(payload).map_err(|error| error.to_string())?;
    let serde_json::Value::Object(object) = value else {
        return Err("failed to unmarshal JWT payload".to_owned());
    };
    let mut claims = BTreeMap::new();
    for (name, value) in object {
        let claim = match name.as_str() {
            "iat" | "exp" | "nbf" => {
                let seconds = match &value {
                    serde_json::Value::Number(number) => number.as_f64(),
                    serde_json::Value::String(text) => text.parse::<f64>().ok(),
                    _ => None,
                }
                .ok_or_else(|| format!("failed to accept value for {name}: invalid numeric date"))?;
                let time = if seconds >= 0.0 {
                    UNIX_EPOCH + Duration::from_secs_f64(seconds)
                } else {
                    UNIX_EPOCH - Duration::from_secs_f64(-seconds)
                };
                Claim::Time(time)
            }
            _ => Claim::Value(value),
        };
        claims.insert(name, claim);
    }
    Ok(claims)
}

/// The account fields Go's `checkAuthTokenClaims` reads off `UserRecord`.
pub struct AuthTokenRecord<'a> {
    /// Go `User`.
    pub user: &'a str,
    /// Go `Email` (`User_attributes->>"$.metadata.email"`).
    pub email: &'a str,
    /// Go `AuthTokenIssuer` (`mysql.user.Token_issuer`).
    pub auth_token_issuer: &'a str,
}

/// Go `checkAuthTokenClaims`.
pub fn check_auth_token_claims(
    claims: &BTreeMap<String, Claim>,
    record: &AuthTokenRecord<'_>,
    token_life: Duration,
) -> Result<(), String> {
    match claims.get("sub") {
        None => return Err("lack 'sub'".to_owned()),
        Some(sub) if sub.as_str() != Some(record.user) => return Err(format!("Wrong 'sub': {}", sub.display())),
        Some(_) => {}
    }
    match claims.get("email") {
        None => return Err("lack 'email'".to_owned()),
        Some(email) if email.as_str() != Some(record.email) => {
            return Err(format!("Wrong 'email': {}", email.display()));
        }
        Some(_) => {}
    }
    let now = SystemTime::now();
    match claims.get("iat") {
        None => return Err("lack 'iat'".to_owned()),
        Some(Claim::Time(iat)) => {
            if now > *iat + token_life {
                return Err("the token has been out of its life time".to_owned());
            } else if now < *iat {
                return Err("the token is issued at a future time".to_owned());
            }
        }
        Some(other) => return Err(format!("iat: {} is not a value of time.Time", other.display())),
    }
    match claims.get("exp") {
        None => return Err("lack 'exp'".to_owned()),
        Some(Claim::Time(exp)) => {
            if now > *exp {
                return Err("the token has been expired".to_owned());
            }
        }
        Some(other) => return Err(format!("exp: {} is not a value of time.Time", other.display())),
    }
    // `iss` is not required if `token_issuer` is empty in `mysql.user`
    match claims.get("iss") {
        Some(iss) if iss.as_str() != Some(record.auth_token_issuer) => Err(format!("Wrong 'iss': {}", iss.display())),
        None if !record.auth_token_issuer.is_empty() => Err("lack 'iss'".to_owned()),
        _ => Ok(()),
    }
}

#[cfg(test)]
#[path = "tidb_auth_token_tests.rs"]
mod tests;
