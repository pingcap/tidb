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

//! Go `session_token.go`: the token `SHOW SESSION_STATES` signs and a
//! `tidb_session_token` login presents, so a proxy can move a session to
//! another TiDB without the user's password.

use std::sync::{LazyLock, RwLock};
use std::time::Duration;

use chrono::{DateTime, FixedOffset};
use openssl::ecdsa::EcdsaSig;
use openssl::hash::MessageDigest;
use openssl::pkey::{Id, PKey, Private};
use openssl::rsa::Padding;
use openssl::sign::{RsaPssSaltlen, Signer, Verifier};
use openssl::x509::X509;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tidb_error::mysql::FormatArg;
use tidb_error::terror::TerrorError;

use crate::session_states::{deserialize_go_bytes, serialize_go_bytes};

/// A token needs a lifetime to avoid brute force attack.
const TOKEN_LIFETIME: Duration = Duration::from_secs(60);
/// Starter uses a longer lifetime because session migration can span a
/// longer idle window in zero-backend deployments.
const STARTER_TOKEN_LIFETIME: Duration = Duration::from_secs(8 * 60 * 60);
/// Go `LoadCertInterval`: the certificate should be rotated periodically.
pub const LOAD_CERT_INTERVAL: Duration = Duration::from_secs(10 * 60);
const STARTER_LOAD_CERT_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
/// After a certificate is replaced, it's still valid for this long. It must
/// be a little longer than [`LOAD_CERT_INTERVAL`]: a server may sign with the
/// old cert while another already checks with the new one.
const OLD_CERT_VALID_TIME: Duration = Duration::from_secs(15 * 60);
const STARTER_OLD_CERT_VALID_TIME: Duration = Duration::from_secs(36 * 60 * 60);

fn current_token_lifetime() -> Duration {
    if tidb_config::deploymode::is_starter() {
        STARTER_TOKEN_LIFETIME
    } else {
        TOKEN_LIFETIME
    }
}

/// Go `GetLoadCertInterval`.
#[must_use]
pub fn get_load_cert_interval() -> Duration {
    if tidb_config::deploymode::is_starter() {
        STARTER_LOAD_CERT_INTERVAL
    } else {
        LOAD_CERT_INTERVAL
    }
}

fn current_old_cert_valid_time() -> Duration {
    if tidb_config::deploymode::is_starter() {
        STARTER_OLD_CERT_VALID_TIME
    } else {
        OLD_CERT_VALID_TIME
    }
}

/// Go `SessionToken`: the token used to authenticate with the new server.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionToken {
    /// The user the token logs in as.
    #[serde(default)]
    pub username: String,
    /// When the token was signed.
    #[serde(rename = "sign-time", default = "go_zero_time", with = "go_time")]
    pub sign_time: DateTime<FixedOffset>,
    /// When the token stops being accepted.
    #[serde(rename = "expire-time", default = "go_zero_time", with = "go_time")]
    pub expire_time: DateTime<FixedOffset>,
    /// The signature over the token's JSON without it.
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "serialize_go_bytes",
        deserialize_with = "deserialize_go_bytes"
    )]
    pub signature: Vec<u8>,
}

/// Go `CreateSessionToken`: creates a token for the user.
///
/// # Errors
///
/// Go's `ErrCannotMigrateSession` when no certificate can sign it.
pub fn create_session_token(username: &str) -> Result<SessionToken, TerrorError> {
    let now = get_now();
    let mut token = SessionToken {
        username: username.to_owned(),
        sign_time: now,
        expire_time: add(now, current_token_lifetime()),
        signature: Vec::new(),
    };
    let token_bytes = tidb_stringutil::go_json_marshal(&token);
    token.signature = GLOBAL_SIGNING_CERT
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .sign(token_bytes.as_bytes())
        .map_err(|error| cannot_migrate(&[error.as_str()]))?;
    Ok(token)
}

/// Go `ValidateSessionToken`: validates a token for the user.
///
/// # Errors
///
/// The token does not decode, its signature does not verify against a
/// current certificate, it expired, its lifetime is too long, or it names
/// another user.
pub fn validate_session_token(token_bytes: &[u8], username: &str) -> Result<(), String> {
    let mut token: SessionToken =
        serde_json::from_slice(token_bytes).map_err(|error| error.to_string())?;
    let signature = std::mem::take(&mut token.signature);
    // Clear the signature and marshal it again to get the original content.
    let content = tidb_stringutil::go_json_marshal(&token);
    GLOBAL_SIGNING_CERT
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .check_signature(content.as_bytes(), &signature)
        .map_err(|error| cannot_migrate(&[error.as_str()]).to_string())?;
    let now = get_now();
    if now > token.expire_time {
        return Err(cannot_migrate(&["token expired", &go_time_string(&token.expire_time)])
            .to_string());
    }
    // An attacker may forge a very long lifetime to brute force, so the sign
    // time is checked too, tolerating a lifetime that changed between
    // versions and clock bias between instances.
    if add(token.sign_time, current_token_lifetime()) < now {
        return Err(cannot_migrate(&[
            "token lifetime is too long",
            &go_time_string(&token.sign_time),
        ])
        .to_string());
    }
    if !tidb_mysql::equal_fold(username, &token.username) {
        return Err(
            cannot_migrate(&["username does not match", username, &token.username]).to_string(),
        );
    }
    Ok(())
}

/// Go `SetKeyPath`.
pub fn set_key_path(key_path: &str) {
    GLOBAL_SIGNING_CERT
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .set_key_path(key_path);
}

/// Go `SetCertPath`.
pub fn set_cert_path(cert_path: &str) {
    GLOBAL_SIGNING_CERT
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .set_cert_path(cert_path);
}

/// Go `ReloadSigningCert`.
pub fn reload_signing_cert() {
    GLOBAL_SIGNING_CERT
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .check_and_load_cert();
}

fn cannot_migrate(arguments: &[&str]) -> TerrorError {
    let arguments: Vec<FormatArg> = arguments.iter().map(|argument| FormatArg::from(*argument)).collect();
    crate::ERR_CANNOT_MIGRATE_SESSION.generate_with_stack_by_args(&arguments)
}

static GLOBAL_SIGNING_CERT: LazyLock<RwLock<SigningCert>> = LazyLock::new(RwLock::default);

/// Go `signingCert`.
#[derive(Default)]
struct SigningCert {
    cert_path: String,
    key_path: String,
    /// The cert file may happen to be rotated between signing and checking,
    /// so the old cert is kept for a while. Every certificate here is not
    /// expired yet, in descending order of expiry.
    certs: Vec<CertInfo>,
}

struct CertInfo {
    cert: X509,
    /// Go `cert.SignatureAlgorithm`.
    algorithm: SignatureAlgorithm,
    priv_key: PKey<Private>,
    expire_time: DateTime<FixedOffset>,
}

impl SigningCert {
    fn set_cert_path(&mut self, cert_path: &str) {
        if cert_path != self.cert_path {
            self.cert_path = cert_path.to_owned();
            // It may fail expectedly because the key path is not set yet.
            self.check_and_load_cert();
        }
    }

    fn set_key_path(&mut self, key_path: &str) {
        if key_path != self.key_path {
            self.key_path = key_path.to_owned();
            // It may fail expectedly because the cert path is not set yet.
            self.check_and_load_cert();
        }
    }

    fn check_and_load_cert(&mut self) {
        if self.cert_path.is_empty() || self.key_path.is_empty() {
            return;
        }
        let paths = [
            tidb_log::Field::new("cert path", tidb_log::Value::Str(self.cert_path.clone())),
            tidb_log::Field::new("key path", tidb_log::Value::Str(self.key_path.clone())),
        ];
        match self.load_cert() {
            Err(error) => {
                let mut fields = paths.to_vec();
                fields.push(tidb_log::Field::new("error", tidb_log::Value::Str(error)));
                tidb_log::warn("loading signing cert failed", &fields);
            }
            Ok(()) => tidb_log::info("signing cert is loaded successfully", &paths),
        }
    }

    fn load_cert(&mut self) -> Result<(), String> {
        let (cert, priv_key) = load_x509_key_pair(&self.cert_path, &self.key_path)?;
        let algorithm = SignatureAlgorithm::of(&cert).map_err(|error| {
            format!(
                "parse x509 cert failed, cert path: {}, key path: {}: {error}",
                self.cert_path, self.key_path
            )
        })?;
        // Rotate certs, keeping their expire times in descending order.
        let now = get_now();
        let mut certs = Vec::with_capacity(self.certs.len() + 1);
        certs.push(CertInfo {
            cert,
            algorithm,
            priv_key,
            expire_time: add(now, get_load_cert_interval() + current_old_cert_valid_time()),
        });
        // Discard the certs that are already expired.
        certs.extend(
            std::mem::take(&mut self.certs)
                .into_iter()
                .take_while(|info| now <= info.expire_time),
        );
        self.certs = certs;
        Ok(())
    }

    fn sign(&self, content: &[u8]) -> Result<Vec<u8>, String> {
        // Always sign the token with the latest cert.
        let Some(info) = self.certs.first() else {
            return Err("no certificate or key file to sign the data".to_owned());
        };
        let unsupported =
            || format!("not supported private key type '{}' for signing", info.algorithm.name());
        let key = &info.priv_key;
        let openssl = |error: openssl::error::ErrorStack| error.to_string();
        match key.id() {
            Id::ED25519 => Signer::new_without_digest(key)
                .and_then(|mut signer| signer.sign_oneshot_to_vec(content))
                .map_err(openssl),
            Id::RSA => {
                let (digest, pss) = match info.algorithm {
                    SignatureAlgorithm::Sha256WithRsaPss => (MessageDigest::sha256(), true),
                    SignatureAlgorithm::Sha384WithRsaPss => (MessageDigest::sha384(), true),
                    SignatureAlgorithm::Sha512WithRsaPss => (MessageDigest::sha512(), true),
                    SignatureAlgorithm::Sha256WithRsa => (MessageDigest::sha256(), false),
                    SignatureAlgorithm::Sha384WithRsa => (MessageDigest::sha384(), false),
                    SignatureAlgorithm::Sha512WithRsa => (MessageDigest::sha512(), false),
                    _ => return Err(unsupported()),
                };
                let mut signer = Signer::new(digest, key).map_err(openssl)?;
                if pss {
                    signer.set_rsa_padding(Padding::PKCS1_PSS).map_err(openssl)?;
                    signer
                        .set_rsa_pss_saltlen(RsaPssSaltlen::DIGEST_LENGTH)
                        .map_err(openssl)?;
                    signer.set_rsa_mgf1_md(digest).map_err(openssl)?;
                }
                signer.sign_oneshot_to_vec(content).map_err(openssl)
            }
            // Go's `ecdsa.PrivateKey.Sign` takes the content itself as the
            // digest, truncated to the curve order as OpenSSL truncates it.
            Id::EC => key
                .ec_key()
                .and_then(|ec_key| EcdsaSig::sign(content, &ec_key))
                .and_then(|signature| signature.to_der())
                .map_err(openssl),
            _ => Err(unsupported()),
        }
    }

    /// Go `checkSignature`: the signature and the content against every
    /// certificate that has not expired.
    fn check_signature(&self, content: &[u8], signature: &[u8]) -> Result<(), String> {
        let now = get_now();
        let mut error = None;
        for info in &self.certs {
            // The expire times descend, so the rest are expired too.
            if now > info.expire_time {
                break;
            }
            let result = match info.priv_key.id() {
                // ECDSA is special: `PrivateKey.Sign` doesn't match with
                // `Certificate.CheckSignature`.
                Id::EC => verify_ecdsa(&info.cert, content, signature)
                    .then_some(())
                    .ok_or_else(|| "x509: ECDSA verification failure".to_owned()),
                _ => check_certificate_signature(&info.cert, info.algorithm, content, signature),
            };
            match result {
                Ok(()) => return Ok(()),
                Err(failure) => error = Some(failure),
            }
        }
        // No certs (possible) or all certs are expired (impossible).
        Err(error.unwrap_or_else(|| {
            format!(
                "no valid certificate to check the signature, cached certificates: {}",
                self.certs.len()
            )
        }))
    }
}

/// Go `tls.LoadX509KeyPair`: the first certificate of the chain and the
/// private key, which must belong to it.
fn load_x509_key_pair(cert_path: &str, key_path: &str) -> Result<(X509, PKey<Private>), String> {
    let wrap = |error: &dyn std::fmt::Display| {
        format!("load x509 failed, cert path: {cert_path}, key path: {key_path}: {error}")
    };
    let cert_pem = std::fs::read(cert_path).map_err(|error| wrap(&error))?;
    let key_pem = std::fs::read(key_path).map_err(|error| wrap(&error))?;
    let cert = X509::stack_from_pem(&cert_pem)
        .map_err(|error| wrap(&error))?
        .into_iter()
        .next()
        .ok_or_else(|| wrap(&"tls: failed to find any PEM data in certificate input"))?;
    let key = PKey::private_key_from_pem(&key_pem).map_err(|error| wrap(&error))?;
    let public = cert.public_key().map_err(|error| wrap(&error))?;
    if !public.public_eq(&key) {
        return Err(wrap(&"tls: private key does not match public key"));
    }
    Ok((cert, key))
}

fn verify_ecdsa(cert: &X509, content: &[u8], signature: &[u8]) -> bool {
    let Ok(signature) = EcdsaSig::from_der(signature) else {
        return false;
    };
    cert.public_key()
        .and_then(|public| public.ec_key())
        .and_then(|ec_key| signature.verify(content, &ec_key))
        .unwrap_or(false)
}

/// Go `Certificate.CheckSignature(c.SignatureAlgorithm, ...)` for a key
/// that is not ECDSA.
fn check_certificate_signature(
    cert: &X509,
    algorithm: SignatureAlgorithm,
    content: &[u8],
    signature: &[u8],
) -> Result<(), String> {
    let (digest, public_algorithm, pss) = algorithm.details().ok_or_else(unsupported_algorithm)?;
    let digest = match digest {
        None if public_algorithm != PublicKeyAlgorithm::Ed25519 => {
            return Err(unsupported_algorithm())
        }
        None => None,
        Some(Hash::Md5 | Hash::Sha1) => {
            return Err(format!(
                "x509: cannot verify signature: insecure algorithm {}",
                algorithm.name()
            ))
        }
        Some(Hash::Sha256) => Some(MessageDigest::sha256()),
        Some(Hash::Sha384) => Some(MessageDigest::sha384()),
        Some(Hash::Sha512) => Some(MessageDigest::sha512()),
    };
    let public = cert.public_key().map_err(|error| error.to_string())?;
    let have = match public.id() {
        Id::RSA => PublicKeyAlgorithm::Rsa,
        Id::ED25519 => PublicKeyAlgorithm::Ed25519,
        _ => return Err(unsupported_algorithm()),
    };
    if have != public_algorithm {
        return Err(format!(
            "x509: signature algorithm specifies an {} public key, but have public key of type {}",
            public_algorithm.name(),
            have.go_type()
        ));
    }
    let openssl = |error: openssl::error::ErrorStack| error.to_string();
    let verified = match digest {
        None => Verifier::new_without_digest(&public)
            .map_err(openssl)?
            .verify_oneshot(signature, content)
            .unwrap_or(false),
        Some(digest) => {
            let mut verifier = Verifier::new(digest, &public).map_err(openssl)?;
            if pss {
                verifier.set_rsa_padding(Padding::PKCS1_PSS).map_err(openssl)?;
                verifier
                    .set_rsa_pss_saltlen(RsaPssSaltlen::DIGEST_LENGTH)
                    .map_err(openssl)?;
                verifier.set_rsa_mgf1_md(digest).map_err(openssl)?;
            }
            verifier.verify_oneshot(signature, content).unwrap_or(false)
        }
    };
    if verified {
        Ok(())
    } else if have == PublicKeyAlgorithm::Ed25519 {
        Err("x509: Ed25519 verification failure".to_owned())
    } else {
        Err("crypto/rsa: verification error".to_owned())
    }
}

fn unsupported_algorithm() -> String {
    "x509: cannot verify signature: algorithm unimplemented".to_owned()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Hash {
    Md5,
    Sha1,
    Sha256,
    Sha384,
    Sha512,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PublicKeyAlgorithm {
    Rsa,
    Dsa,
    Ecdsa,
    Ed25519,
}

impl PublicKeyAlgorithm {
    fn name(self) -> &'static str {
        match self {
            Self::Rsa => "RSA",
            Self::Dsa => "DSA",
            Self::Ecdsa => "ECDSA",
            Self::Ed25519 => "Ed25519",
        }
    }

    fn go_type(self) -> &'static str {
        match self {
            Self::Rsa => "*rsa.PublicKey",
            Self::Dsa => "*dsa.PublicKey",
            Self::Ecdsa => "*ecdsa.PublicKey",
            Self::Ed25519 => "ed25519.PublicKey",
        }
    }
}

/// Go `x509.SignatureAlgorithm`, as `getSignatureAlgorithmFromAI` reads it
/// from a certificate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SignatureAlgorithm {
    Unknown,
    Md5WithRsa,
    Sha1WithRsa,
    Sha256WithRsa,
    Sha384WithRsa,
    Sha512WithRsa,
    DsaWithSha1,
    DsaWithSha256,
    EcdsaWithSha1,
    EcdsaWithSha256,
    EcdsaWithSha384,
    EcdsaWithSha512,
    Sha256WithRsaPss,
    Sha384WithRsaPss,
    Sha512WithRsaPss,
    PureEd25519,
}

impl SignatureAlgorithm {
    /// Go `signatureAlgorithmDetails`: the hash (none for Ed25519), the
    /// public-key algorithm and whether it is RSA-PSS.
    fn details(self) -> Option<(Option<Hash>, PublicKeyAlgorithm, bool)> {
        use PublicKeyAlgorithm::{Dsa, Ecdsa, Ed25519, Rsa};
        Some(match self {
            Self::Unknown => return None,
            Self::Md5WithRsa => (Some(Hash::Md5), Rsa, false),
            Self::Sha1WithRsa => (Some(Hash::Sha1), Rsa, false),
            Self::Sha256WithRsa => (Some(Hash::Sha256), Rsa, false),
            Self::Sha384WithRsa => (Some(Hash::Sha384), Rsa, false),
            Self::Sha512WithRsa => (Some(Hash::Sha512), Rsa, false),
            Self::Sha256WithRsaPss => (Some(Hash::Sha256), Rsa, true),
            Self::Sha384WithRsaPss => (Some(Hash::Sha384), Rsa, true),
            Self::Sha512WithRsaPss => (Some(Hash::Sha512), Rsa, true),
            Self::DsaWithSha1 => (Some(Hash::Sha1), Dsa, false),
            Self::DsaWithSha256 => (Some(Hash::Sha256), Dsa, false),
            Self::EcdsaWithSha1 => (Some(Hash::Sha1), Ecdsa, false),
            Self::EcdsaWithSha256 => (Some(Hash::Sha256), Ecdsa, false),
            Self::EcdsaWithSha384 => (Some(Hash::Sha384), Ecdsa, false),
            Self::EcdsaWithSha512 => (Some(Hash::Sha512), Ecdsa, false),
            Self::PureEd25519 => (None, Ed25519, false),
        })
    }

    /// Go `SignatureAlgorithm.String`.
    fn name(self) -> &'static str {
        match self {
            Self::Unknown => "0",
            Self::Md5WithRsa => "MD5-RSA",
            Self::Sha1WithRsa => "SHA1-RSA",
            Self::Sha256WithRsa => "SHA256-RSA",
            Self::Sha384WithRsa => "SHA384-RSA",
            Self::Sha512WithRsa => "SHA512-RSA",
            Self::Sha256WithRsaPss => "SHA256-RSAPSS",
            Self::Sha384WithRsaPss => "SHA384-RSAPSS",
            Self::Sha512WithRsaPss => "SHA512-RSAPSS",
            Self::DsaWithSha1 => "DSA-SHA1",
            Self::DsaWithSha256 => "DSA-SHA256",
            Self::EcdsaWithSha1 => "ECDSA-SHA1",
            Self::EcdsaWithSha256 => "ECDSA-SHA256",
            Self::EcdsaWithSha384 => "ECDSA-SHA384",
            Self::EcdsaWithSha512 => "ECDSA-SHA512",
            Self::PureEd25519 => "Ed25519",
        }
    }

    /// Go `getSignatureAlgorithmFromAI` over the certificate's own
    /// signature algorithm.
    fn of(cert: &X509) -> Result<Self, String> {
        use x509_parser::prelude::FromDer;
        let der = cert.to_der().map_err(|error| error.to_string())?;
        let (_, parsed) = x509_parser::certificate::X509Certificate::from_der(&der)
            .map_err(|error| error.to_string())?;
        Ok(Self::from_algorithm_identifier(&parsed.signature_algorithm))
    }

    fn from_algorithm_identifier(ai: &x509_parser::x509::AlgorithmIdentifier<'_>) -> Self {
        let oid = ai.algorithm.to_id_string();
        match oid.as_str() {
            // RFC 8410, Section 3: for all of the OIDs, the parameters MUST
            // be absent.
            "1.3.101.112" if ai.parameters.is_some() => Self::Unknown,
            "1.3.101.112" => Self::PureEd25519,
            "1.2.840.113549.1.1.4" => Self::Md5WithRsa,
            "1.2.840.113549.1.1.5" | "1.3.14.3.2.29" => Self::Sha1WithRsa,
            "1.2.840.113549.1.1.11" => Self::Sha256WithRsa,
            "1.2.840.113549.1.1.12" => Self::Sha384WithRsa,
            "1.2.840.113549.1.1.13" => Self::Sha512WithRsa,
            "1.2.840.10040.4.3" => Self::DsaWithSha1,
            "2.16.840.1.101.3.4.3.2" => Self::DsaWithSha256,
            "1.2.840.10045.4.1" => Self::EcdsaWithSha1,
            "1.2.840.10045.4.3.2" => Self::EcdsaWithSha256,
            "1.2.840.10045.4.3.3" => Self::EcdsaWithSha384,
            "1.2.840.10045.4.3.4" => Self::EcdsaWithSha512,
            "1.2.840.113549.1.1.10" => Self::rsa_pss(ai),
            _ => Self::Unknown,
        }
    }

    /// RSA-PSS forced into three buckets: the MGF1 hash matches the message
    /// hash, the salt length matches the hash length, and the trailer field
    /// has the default value.
    fn rsa_pss(ai: &x509_parser::x509::AlgorithmIdentifier<'_>) -> Self {
        use x509_parser::der_parser::asn1_rs::{Any, FromDer, Oid, Tag};
        let null_or_absent =
            |parameters: Option<&Any<'_>>| parameters.is_none_or(|any| any.tag() == Tag::Null && any.data.is_empty());
        let Some(parameters) = ai.parameters.as_ref() else {
            return Self::Unknown;
        };
        let Ok(params) = x509_parser::signature_algorithm::RsaSsaPssParams::try_from(parameters.clone())
        else {
            return Self::Unknown;
        };
        // Go's `pssParameters` requires the hash, the mask generation
        // function and the salt length to be present.
        let (Some(hash), Some(mgf)) = (params.hash_algorithm(), params.mask_gen_algorithm_raw())
        else {
            return Self::Unknown;
        };
        let Some(mgf_hash) = mgf.parameters.as_ref() else {
            return Self::Unknown;
        };
        // The MGF1 parameter is an AlgorithmIdentifier: the hash's OID and
        // its own (absent or NULL) parameters.
        let Ok((rest, mgf_hash_oid)) = Oid::from_der(mgf_hash.data) else {
            return Self::Unknown;
        };
        if !null_or_absent(hash.parameters.as_ref())
            || mgf.algorithm.to_id_string() != "1.2.840.113549.1.1.8"
            || mgf_hash.tag() != Tag::Sequence
            || mgf_hash_oid != hash.algorithm
            || !(rest.is_empty() || rest == [0x05, 0x00])
            || params.trailer_field() != 1
        {
            return Self::Unknown;
        }
        match (hash.algorithm.to_id_string().as_str(), params.salt_length()) {
            ("2.16.840.1.101.3.4.2.1", 32) => Self::Sha256WithRsaPss,
            ("2.16.840.1.101.3.4.2.2", 48) => Self::Sha384WithRsaPss,
            ("2.16.840.1.101.3.4.2.3", 64) => Self::Sha512WithRsaPss,
            _ => Self::Unknown,
        }
    }
}

/// Go `getNow`, with its `mockNowOffset` failpoint (nanoseconds).
fn get_now() -> DateTime<FixedOffset> {
    let now = chrono::Local::now().fixed_offset();
    #[cfg(feature = "failpoints")]
    {
        let offset = fail::eval("mockNowOffset", |value| {
            value.and_then(|value| value.parse::<u64>().ok()).unwrap_or(0)
        });
        if let Some(offset) = offset.filter(|offset| *offset != 0) {
            return add(now, Duration::from_nanos(offset));
        }
    }
    now
}

fn add(time: DateTime<FixedOffset>, duration: Duration) -> DateTime<FixedOffset> {
    time + chrono::Duration::from_std(duration).expect("token durations fit chrono")
}

fn go_zero_time() -> DateTime<FixedOffset> {
    DateTime::parse_from_rfc3339("0001-01-01T00:00:00Z").expect("Go's zero time is valid RFC3339")
}

/// Go `Time.String` without the zone abbreviation, which a fixed offset
/// does not carry; it only reaches logs.
fn go_time_string(time: &DateTime<FixedOffset>) -> String {
    let fraction = format!("{:09}", time.timestamp_subsec_nanos());
    let fraction = fraction.trim_end_matches('0');
    let dot = if fraction.is_empty() { "" } else { "." };
    format!("{}{dot}{fraction} {}", time.format("%Y-%m-%d %H:%M:%S"), time.format("%z"))
}

/// Go `time.Time`'s JSON: `time.RFC3339Nano`, trailing fraction zeros
/// trimmed, `Z` for UTC.
mod go_time {
    use super::*;

    pub(super) fn serialize<S: Serializer>(
        time: &DateTime<FixedOffset>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let fraction = format!("{:09}", time.timestamp_subsec_nanos());
        let fraction = fraction.trim_end_matches('0');
        let dot = if fraction.is_empty() { "" } else { "." };
        let zone = if time.offset().local_minus_utc() == 0 {
            "Z".to_owned()
        } else {
            time.format("%:z").to_string()
        };
        serializer.serialize_str(&format!(
            "{}{dot}{fraction}{zone}",
            time.format("%Y-%m-%dT%H:%M:%S")
        ))
    }

    /// `null` leaves Go's zero time.
    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<DateTime<FixedOffset>, D::Error> {
        match Option::<String>::deserialize(deserializer)? {
            None => Ok(go_zero_time()),
            Some(text) => DateTime::parse_from_rfc3339(&text).map_err(serde::de::Error::custom),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go's `pssParametersSHA256/384/512`, RFC 3447 Appendix A.2.3.
    fn pss_parameters(hash: u8, salt: u8) -> Vec<u8> {
        vec![
            48, 52, 160, 15, 48, 13, 6, 9, 96, 134, 72, 1, 101, 3, 4, 2, hash, 5, 0, 161, 28, 48,
            26, 6, 9, 42, 134, 72, 134, 247, 13, 1, 1, 8, 48, 13, 6, 9, 96, 134, 72, 1, 101, 3, 4,
            2, hash, 5, 0, 162, 3, 2, 1, salt,
        ]
    }

    fn pss_algorithm(hash: u8, salt: u8) -> SignatureAlgorithm {
        use x509_parser::prelude::FromDer;
        // SEQUENCE { id-RSASSA-PSS, params }
        let oid = [6, 9, 42, 134, 72, 134, 247, 13, 1, 1, 10];
        let params = pss_parameters(hash, salt);
        let mut der = vec![48, (oid.len() + params.len()) as u8];
        der.extend_from_slice(&oid);
        der.extend_from_slice(&params);
        let (_, ai) = x509_parser::x509::AlgorithmIdentifier::from_der(&der).unwrap();
        SignatureAlgorithm::from_algorithm_identifier(&ai)
    }

    #[test]
    fn rsa_pss_parameters_fall_into_gos_three_buckets() {
        assert_eq!(pss_algorithm(1, 32), SignatureAlgorithm::Sha256WithRsaPss);
        assert_eq!(pss_algorithm(2, 48), SignatureAlgorithm::Sha384WithRsaPss);
        assert_eq!(pss_algorithm(3, 64), SignatureAlgorithm::Sha512WithRsaPss);
        // The salt length must match the hash length.
        assert_eq!(pss_algorithm(1, 20), SignatureAlgorithm::Unknown);
        assert_eq!(pss_algorithm(2, 32), SignatureAlgorithm::Unknown);
    }

    fn rsa_cert() -> (X509, PKey<Private>) {
        let key = PKey::from_rsa(openssl::rsa::Rsa::generate(2048).unwrap()).unwrap();
        let mut builder = openssl::x509::X509Builder::new().unwrap();
        builder.set_pubkey(&key).unwrap();
        builder.sign(&key, MessageDigest::sha256()).unwrap();
        (builder.build(), key)
    }

    /// Go signs with the PSS hash the certificate names, salt as long as
    /// the hash, and checks the same way.
    #[test]
    fn rsa_pss_signatures_verify() {
        for algorithm in [
            SignatureAlgorithm::Sha256WithRsaPss,
            SignatureAlgorithm::Sha384WithRsaPss,
            SignatureAlgorithm::Sha512WithRsaPss,
        ] {
            let (cert, priv_key) = rsa_cert();
            let signing = SigningCert {
                certs: vec![CertInfo {
                    cert,
                    algorithm,
                    priv_key,
                    expire_time: add(get_now(), LOAD_CERT_INTERVAL),
                }],
                ..SigningCert::default()
            };
            let signature = signing.sign(b"content").unwrap();
            signing.check_signature(b"content", &signature).unwrap();
            let error = signing.check_signature(b"forged", &signature).unwrap_err();
            assert!(error.contains("verification error"), "{error}");
        }
    }

    /// An RSA key whose certificate names no RSA hash cannot sign, as Go's.
    #[test]
    fn an_rsa_key_under_an_unknown_algorithm_cannot_sign() {
        let (cert, priv_key) = rsa_cert();
        let signing = SigningCert {
            certs: vec![CertInfo {
                cert,
                algorithm: SignatureAlgorithm::Unknown,
                priv_key,
                expire_time: add(get_now(), LOAD_CERT_INTERVAL),
            }],
            ..SigningCert::default()
        };
        assert_eq!(
            signing.sign(b"content").unwrap_err(),
            "not supported private key type '0' for signing"
        );
    }
}
