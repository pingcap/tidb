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

//! Authentication-string encoding: the stage-two hash each supported
//! authentication plugin stores in `mysql.user.authentication_string`.
//!
//! Mirrors Go `pkg/parser/auth/mysql_native_password.go`
//! (`EncodePassword`), `pkg/parser/auth/caching_sha2.go` (`NewSha2Password`
//! and its SHA-crypt implementation) and the SM3 variant of
//! `pkg/parser/auth`.

use sha1::{Digest, Sha1};

use tidb_executor::DriverError;
use tidb_mysql::consts::{
    AuthCachingSha2Password, AuthLDAPSASL, AuthLDAPSimple, AuthNativePassword, AuthSocket,
    AuthTiDBAuthToken, AuthTiDBSM3Password, PWDHashLen, SHAPWDHashLen, SM3PWDHashLen,
};

/// Go `pkg/parser/auth.EncodePassword`: the
/// `mysql.user.authentication_string` of a `mysql_native_password` account is
/// `*` followed by the UPPERCASE hexadecimal SHA-1 of the SHA-1 of the
/// plaintext. An empty password encodes to the empty string, NOT to a hash of
/// the empty string.
#[must_use]
pub fn encode_password(password: &str) -> String {
    if password.is_empty() {
        return String::new();
    }
    let stage_one = Sha1::digest(password.as_bytes());
    let stage_two = Sha1::digest(stage_one);
    let mut encoded = String::with_capacity(1 + stage_two.len() * 2);
    encoded.push('*');
    for byte in stage_two {
        use std::fmt::Write;
        write!(encoded, "{byte:02X}").expect("writing to a String cannot fail");
    }
    encoded
}

/// Plugins `CREATE`/`ALTER USER ... IDENTIFIED WITH` accepts without a
/// registered extension auth plugin -- Go `simple.go`'s account executor
/// switch (`executor/simple.go`'s `executeCreateUser`). Any other name is
/// Go's `ErrPluginIsNotLoaded` (1524), `Plugin '<name>' is not loaded`,
/// since this tier registers no extension auth plugins to fall back to.
pub const CREATE_USER_PLUGINS: &[&str] = &[
    AuthNativePassword,
    AuthCachingSha2Password,
    AuthTiDBSM3Password,
    AuthSocket,
    AuthTiDBAuthToken,
    AuthLDAPSimple,
    AuthLDAPSASL,
];

/// Whether `plugin` is one [`CREATE_USER_PLUGINS`] accepts.
#[must_use]
pub fn is_create_user_plugin(plugin: &str) -> bool {
    CREATE_USER_PLUGINS.contains(&plugin)
}

/// How a login over a NETWORK connection may be verified for an account
/// whose `mysql.user.plugin` is `plugin`.
///
/// Go picks the wire plugin from the account row (`server/conn.go`'s
/// `checkAuthPlugin`, around line 1027, auth-switches the client to
/// `userplugin`) and then dispatches on that same row in
/// `privileges.ConnectionVerification` (`privileges.go` around line 666).
/// This tier's wire front end always auth-switches to
/// `mysql_native_password`, so the account's plugin cannot select a
/// handshake -- but it MUST still select the verifier, because most of
/// Go's plugin arms accept credentials a native scramble can never carry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoginPluginVerification {
    /// Compare the client's `mysql_native_password` scramble against the
    /// stored native hash, and accept an empty response for an empty
    /// stored string. Go's `mysql.AuthNativePassword` arm.
    NativeHash,
    /// Re-derive the stored SHA-crypt envelope from the CLEARTEXT the client
    /// sent and compare -- Go's `caching_sha2_password` and
    /// `tidb_sm3_password` arms, which share one
    /// [`check_hashing_password`] driven by SHA-256 or SM3.
    ///
    /// The payload is the plugin name, because the two plugins differ only
    /// in that digest.
    HashingPlugin(&'static str),
    /// Only a passwordless login -- empty stored string AND empty client
    /// response -- can succeed.
    ///
    /// Go reaches its password arms only through
    /// `else if len(pwd) > 0 || len(authentication) > 0`, so an account with
    /// an empty `authentication_string` that receives an empty response
    /// authenticates whatever its plugin is; this arm reproduces exactly
    /// that, and denies the rest because Go's `default` arm -- an
    /// authentication plugin it does not know -- logs "unknown
    /// authentication plugin" and denies.
    PasswordlessOnly,
    /// No network login can succeed, whatever the client sends.
    ///
    /// `auth_socket` is refused outright off a Unix socket
    /// (`server/conn.go` around line 988, and again at line 878);
    /// `tidb_auth_token` needs a JWT this tier has no JWKS for
    /// (`privileges.go` around line 670, which also denies an empty token);
    /// and both LDAP plugins need a bind against a directory server
    /// (`privileges.go` around lines 690 and 699). All three report Go's
    /// generic `ErrAccessDenied` (1045).
    Deny,
}

/// Selects the verifier Go's `ConnectionVerification` would use for an
/// account whose `mysql.user.plugin` column holds `plugin`.
///
/// An empty `plugin` is Go's "no user plugin set" case (`server/conn.go`
/// around line 1008), which assumes `mysql_native_password`.
#[must_use]
pub fn login_plugin_verification(plugin: &str) -> LoginPluginVerification {
    if plugin.is_empty() || plugin == AuthNativePassword {
        LoginPluginVerification::NativeHash
    } else if plugin == AuthCachingSha2Password {
        LoginPluginVerification::HashingPlugin(AuthCachingSha2Password)
    } else if plugin == AuthTiDBSM3Password {
        LoginPluginVerification::HashingPlugin(AuthTiDBSM3Password)
    } else if plugin == AuthSocket
        || plugin == AuthTiDBAuthToken
        || plugin == AuthLDAPSimple
        || plugin == AuthLDAPSASL
    {
        LoginPluginVerification::Deny
    } else {
        // Any plugin name this tier does not know -- Go's `default` arm.
        LoginPluginVerification::PasswordlessOnly
    }
}

/// Whether an account on `plugin` needs the CLEARTEXT password rather than a
/// `mysql_native_password` scramble, which is what makes the wire front end
/// run Go's `authSha`/`authSM3` full-authentication exchange for it.
#[must_use]
pub fn plugin_needs_cleartext(plugin: &str) -> bool {
    matches!(
        login_plugin_verification(plugin),
        LoginPluginVerification::HashingPlugin(_)
    )
}

/// One account specification's `IDENTIFIED WITH <plugin> [BY '<password>' |
/// AS '<hash>']` credential, already split into the shape Go's
/// `encodedPassword` switches on.
pub enum PluginCredential<'a> {
    /// `BY '<password>'`: the plaintext the plugin hashes.
    By(&'a str),
    /// `AS '<hash>'`: an already-computed hash, validated for shape only.
    As(&'a str),
    /// Neither clause: a passwordless account.
    None,
}

/// Go `executor/utils.go`'s `encodedPassword`, minus the extension-plugin
/// branch: this tier registers no extension auth plugins, so
/// `encodePasswordWithPlugin` always falls to this path.
///
/// Returns the `authentication_string` to store, or
/// [`DriverError::PasswordFormat`] for an `AS` hash that does not match the
/// plugin's expected shape (Go's `ErrPasswordFormat`, 1827).
///
/// Every plugin's `BY`/`AS` form is captured and implemented exactly,
/// including `tidb_sm3_password`'s `BY` form (hashed with
/// [`hash_tidb_sm3`], the same SHA-crypt envelope as `caching_sha2_password`
/// driven by SM3 instead of SHA-256) and its `AS` form (a length check
/// only, no hashing needed) -- ORDER matters: an LDAP plugin's `AS` form
/// stores the `dn` verbatim before the general empty/length rules apply,
/// but an LDAP plugin's `BY` form is NOT special (Go's `switch` only
/// special-cases it in the `AS`/plugin-only arm) and falls to the same
/// native SHA1 hash every other unlisted plugin's `BY` form does.
pub fn encode_password_for_plugin(
    plugin: &str,
    credential: &PluginCredential<'_>,
) -> Result<String, DriverError> {
    match credential {
        PluginCredential::By(password) => {
            if plugin == AuthCachingSha2Password {
                Ok(hash_caching_sha2(password))
            } else if plugin == AuthTiDBSM3Password {
                Ok(hash_tidb_sm3(password))
            } else if plugin == AuthSocket {
                Ok(String::new())
            } else {
                // Go's `default` arm: every other accepted plugin (native,
                // both LDAP forms, the token plugins) hashes a `BY`
                // password the native way.
                Ok(encode_password(password))
            }
        }
        PluginCredential::As(hash) => {
            if plugin == AuthLDAPSimple || plugin == AuthLDAPSASL {
                return Ok((*hash).to_owned());
            }
            if hash.is_empty() {
                return Ok(String::new());
            }
            let shaped = if plugin == AuthCachingSha2Password {
                hash.len() == SHAPWDHashLen as usize
            } else if plugin == AuthTiDBSM3Password {
                hash.len() == SM3PWDHashLen as usize
            } else if plugin == AuthNativePassword {
                hash.len() == PWDHashLen as usize + 1 && hash.starts_with('*')
            } else {
                plugin == AuthSocket
            };
            if shaped {
                Ok((*hash).to_owned())
            } else {
                Err(DriverError::PasswordFormat)
            }
        }
        PluginCredential::None => Ok(String::new()),
    }
}

/// SHA-crypt mixing width: Go's `MIXCHARS`, and also `sha256::Sum256`'s
/// output width, which is why the loops below can reuse a whole digest as
/// one "chunk".
pub(super) const SHA_CRYPT_MIXCHARS: usize = 32;
/// Go's `SALT_LENGTH`.
pub(super) const SHA_CRYPT_SALT_LEN: usize = 20;
/// Go's `ITERATION_MULTIPLIER`.
pub(super) const SHA_CRYPT_ITERATION_MULTIPLIER: u32 = 1000;
/// Go's custom base64 alphabet for `b64From24bit`, distinct from RFC 4648.
pub(super) const SHA_CRYPT_B64_ALPHABET: &[u8; 64] =
    b"./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

/// Go `pkg/parser/auth.b64From24bit`: packs three bytes into 24 bits and
/// emits `n` base64 digits, LEAST-significant 6 bits first.
pub(super) fn sha_crypt_b64_from_24bit(bytes: [u8; 3], n: usize, out: &mut String) {
    let mut word = (u32::from(bytes[0]) << 16) | (u32::from(bytes[1]) << 8) | u32::from(bytes[2]);
    for _ in 0..n {
        out.push(SHA_CRYPT_B64_ALPHABET[(word & 0x3f) as usize] as char);
        word >>= 6;
    }
}

/// Go `pkg/parser/auth.NewHashPassword` for `caching_sha2_password`: a
/// SHA256-crypt-family hash (<https://www.akkadia.org/drepper/SHA-crypt.txt>)
/// with a random 20-byte salt and 5000 iterations, stored
/// `$A$005$<20-byte salt><43-char digest>` -- 70 bytes total,
/// `SHAPWDHashLen`. The salt excludes NUL and `$` exactly as Go's generator
/// does (see [`tidb_util::fastrand::buf`]).
pub(super) fn hash_caching_sha2(password: &str) -> String {
    let salt = tidb_util::fastrand::buf(SHA_CRYPT_SALT_LEN);
    sha_crypt(
        password,
        &salt,
        5 * SHA_CRYPT_ITERATION_MULTIPLIER,
        |input| Sha256Hash::digest(input).into(),
    )
}

/// `tidb_sm3_password`'s `IDENTIFIED WITH ... BY '<password>'` hash: the
/// same SHA-crypt-shaped envelope as `hash_caching_sha2`, but driven by
/// `tidb_parser::auth::sm3_hash` (Go drives the identical `hashCrypt` with
/// SM3 instead of SHA-256 for this plugin; see
/// `pkg/parser/auth/caching_sha2.go`'s `NewHashPassword`).
pub(super) fn hash_tidb_sm3(password: &str) -> String {
    let salt = tidb_util::fastrand::buf(SHA_CRYPT_SALT_LEN);
    sha_crypt(
        password,
        &salt,
        5 * SHA_CRYPT_ITERATION_MULTIPLIER,
        tidb_parser::auth::sm3_hash,
    )
}

/// Go `pkg/parser/auth.CheckHashingPassword` (`caching_sha2.go` line 193):
/// re-derives the stored hash from `password` using the SALT AND ITERATION
/// COUNT the stored hash itself carries, and compares the whole envelope.
///
/// `password` is the CLEARTEXT, not a scramble: TiDB never implements the
/// caching fast path, so both `caching_sha2_password` and
/// `tidb_sm3_password` always drive the client through full authentication
/// and hand `ConnectionVerification` the plaintext (`server/conn.go`'s
/// `authSha`/`authSM3`, and `checkPasswordForPlugin` passing
/// `string(authentication)` straight in).
///
/// `false` for every malformed stored hash, which is Go's answer too: its
/// three error returns (wrong part count, digest type other than `A`,
/// unparsable iteration count) all reach a caller that logs and treats the
/// check as failed.
#[must_use]
pub fn check_hashing_password(stored: &str, password: &[u8], plugin: &str) -> bool {
    let hash: fn(&[u8]) -> [u8; 32] = if plugin == AuthCachingSha2Password {
        |input| Sha256Hash::digest(input).into()
    } else if plugin == AuthTiDBSM3Password {
        tidb_parser::auth::sm3_hash
    } else {
        // Go's `switch hash` leaves `newHash` empty for an unrecognized
        // plugin and then compares it to the stored hash, which no real
        // stored hash equals.
        return false;
    };
    // Go splits on `$`, so a valid hash is exactly `["", "A", "<iters>",
    // "<salt><digest>"]`.
    let parts: Vec<&str> = stored.split('$').collect();
    let [_, "A", iterations, tail] = parts.as_slice() else {
        return false;
    };
    let Ok(iterations) = u32::from_str_radix(iterations, 16) else {
        return false;
    };
    let Some(salt) = tail.as_bytes().get(..SHA_CRYPT_SALT_LEN) else {
        return false;
    };
    let Ok(password) = std::str::from_utf8(password) else {
        return false;
    };
    sha_crypt(
        password,
        salt,
        iterations * SHA_CRYPT_ITERATION_MULTIPLIER,
        hash,
    ) == stored
}

/// Go `pkg/parser/auth.hashCrypt`, ported 1:1 (see the numbered steps in
/// Go's own comment referencing the akkadia.org SHA-crypt description).
/// `hash` must be a 32-byte digest function: SHA-256 for
/// `caching_sha2_password` ([`hash_caching_sha2`]), SM3 for
/// `tidb_sm3_password` ([`hash_tidb_sm3`]).
pub(super) fn sha_crypt(
    plaintext: &str,
    salt: &[u8],
    iterations: u32,
    hash: impl Fn(&[u8]) -> [u8; 32],
) -> String {
    let pt = plaintext.as_bytes();

    // Steps 4-8: sumB = hash(pt + salt + pt).
    let mut buf_b = Vec::with_capacity(pt.len() * 2 + salt.len());
    buf_b.extend_from_slice(pt);
    buf_b.extend_from_slice(salt);
    buf_b.extend_from_slice(pt);
    let sum_b = hash(&buf_b);

    // Steps 1-3, 9-11: bufA = pt + salt, then sumB chunks and pt/sumB
    // alternating by the bits of len(pt).
    let mut buf_a = Vec::new();
    buf_a.extend_from_slice(pt);
    buf_a.extend_from_slice(salt);
    let mut i = pt.len();
    while i > SHA_CRYPT_MIXCHARS {
        buf_a.extend_from_slice(&sum_b[..SHA_CRYPT_MIXCHARS]);
        i -= SHA_CRYPT_MIXCHARS;
    }
    buf_a.extend_from_slice(&sum_b[..i]);
    let mut i = pt.len();
    while i > 0 {
        if i.is_multiple_of(2) {
            buf_a.extend_from_slice(pt);
        } else {
            buf_a.extend_from_slice(&sum_b);
        }
        i >>= 1;
    }
    // Step 12: sumA.
    let mut sum_a = hash(&buf_a);

    // Steps 13-16: sumDP = hash(pt repeated len(pt) times), then `p` built
    // from sumDP chunks sized by len(pt).
    let mut buf_dp = Vec::with_capacity(pt.len() * pt.len());
    for _ in 0..pt.len() {
        buf_dp.extend_from_slice(pt);
    }
    let sum_dp = hash(&buf_dp);
    let mut p = Vec::new();
    let mut i = pt.len();
    while i > 0 {
        if i > SHA_CRYPT_MIXCHARS {
            p.extend_from_slice(&sum_dp);
        } else {
            p.extend_from_slice(&sum_dp[..i]);
        }
        i = i.saturating_sub(SHA_CRYPT_MIXCHARS);
    }

    // Steps 17-20: sumDS = hash(salt repeated 16+sumA[0] times), then `s`
    // built from sumDS chunks sized by len(salt).
    let mut buf_ds = Vec::new();
    for _ in 0..(16 + usize::from(sum_a[0])) {
        buf_ds.extend_from_slice(salt);
    }
    let sum_ds = hash(&buf_ds);
    let mut s = Vec::new();
    let mut i = salt.len();
    while i > 0 {
        if i > SHA_CRYPT_MIXCHARS {
            s.extend_from_slice(&sum_ds);
        } else {
            s.extend_from_slice(&sum_ds[..i]);
        }
        i = i.saturating_sub(SHA_CRYPT_MIXCHARS);
    }

    // Step 21: the iterated mixing loop.
    for round in 0..iterations {
        let mut buf_c = Vec::new();
        if round & 1 != 0 {
            buf_c.extend_from_slice(&p);
        } else {
            buf_c.extend_from_slice(&sum_a);
        }
        if round % 3 != 0 {
            buf_c.extend_from_slice(&s);
        }
        if round % 7 != 0 {
            buf_c.extend_from_slice(&p);
        }
        if round & 1 != 0 {
            buf_c.extend_from_slice(&sum_a);
        } else {
            buf_c.extend_from_slice(&p);
        }
        sum_a = hash(&buf_c);
    }
    let sum_c = sum_a;

    // Step 22: `$A$<rounds>$<salt><permuted base64 of sumC>`.
    let mut out = String::with_capacity(SHAPWDHashLen as usize);
    out.push_str("$A$");
    out.push_str(&format!(
        "{:03X}",
        iterations / SHA_CRYPT_ITERATION_MULTIPLIER
    ));
    out.push('$');
    for &byte in salt {
        out.push(byte as char);
    }
    sha_crypt_b64_from_24bit([sum_c[0], sum_c[10], sum_c[20]], 4, &mut out);
    sha_crypt_b64_from_24bit([sum_c[21], sum_c[1], sum_c[11]], 4, &mut out);
    sha_crypt_b64_from_24bit([sum_c[12], sum_c[22], sum_c[2]], 4, &mut out);
    sha_crypt_b64_from_24bit([sum_c[3], sum_c[13], sum_c[23]], 4, &mut out);
    sha_crypt_b64_from_24bit([sum_c[24], sum_c[4], sum_c[14]], 4, &mut out);
    sha_crypt_b64_from_24bit([sum_c[15], sum_c[25], sum_c[5]], 4, &mut out);
    sha_crypt_b64_from_24bit([sum_c[6], sum_c[16], sum_c[26]], 4, &mut out);
    sha_crypt_b64_from_24bit([sum_c[27], sum_c[7], sum_c[17]], 4, &mut out);
    sha_crypt_b64_from_24bit([sum_c[18], sum_c[28], sum_c[8]], 4, &mut out);
    sha_crypt_b64_from_24bit([sum_c[9], sum_c[19], sum_c[29]], 4, &mut out);
    sha_crypt_b64_from_24bit([0, sum_c[31], sum_c[30]], 3, &mut out);
    out
}

pub(super) type Sha256Hash = sha2::Sha256;
