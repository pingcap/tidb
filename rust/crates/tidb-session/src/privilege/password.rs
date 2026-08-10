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

//! Account-plugin policy for values stored in
//! `mysql.user.authentication_string`. Cryptographic byte formats are owned
//! by [`tidb_parser::auth`]; this module selects the plugin and maps account
//! statement forms onto that single implementation.

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
    tidb_parser::auth::encode_password(password)
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

/// Go `pkg/parser/auth.NewHashPassword` for `caching_sha2_password`: a
/// SHA256-crypt-family hash (<https://www.akkadia.org/drepper/SHA-crypt.txt>)
/// with a random 20-byte salt and 5000 iterations, stored
/// `$A$005$<20-byte salt><43-char digest>` -- 70 bytes total,
/// `SHAPWDHashLen`. The salt excludes NUL and `$` exactly as Go's generator
/// does (see [`tidb_util::fastrand::buf`]).
pub(super) fn hash_caching_sha2(password: &str) -> String {
    hash_password(password, AuthCachingSha2Password)
}

/// `tidb_sm3_password`'s `IDENTIFIED WITH ... BY '<password>'` hash: the
/// same SHA-crypt-shaped envelope as `hash_caching_sha2`, but driven by
/// `tidb_parser::auth::sm3_hash` (Go drives the identical `hashCrypt` with
/// SM3 instead of SHA-256 for this plugin; see
/// `pkg/parser/auth/caching_sha2.go`'s `NewHashPassword`).
pub(super) fn hash_tidb_sm3(password: &str) -> String {
    hash_password(password, AuthTiDBSM3Password)
}

fn hash_password(password: &str, plugin: &str) -> String {
    let salt = tidb_util::fastrand::buf(tidb_parser::auth::SALT_LENGTH);
    let encoded =
        tidb_parser::auth::hash_password_with_salt_bytes(password.as_bytes(), &salt, plugin);
    String::from_utf8(encoded).expect("generated authentication strings are ASCII")
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
    tidb_parser::auth::check_hashing_password_bytes(stored.as_bytes(), password, plugin)
        .unwrap_or(false)
}
