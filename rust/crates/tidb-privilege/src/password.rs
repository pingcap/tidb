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

use tidb_error::mysql::MysqlError;
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

/// Go `UserPrivileges.isValidHash` for the built-in plugins (this server
/// registers no extension auth plugin): whether an account's stored
/// `authentication_string` has its plugin's hash shape. An empty string is
/// always valid; a plugin Go does not know is never valid with a string.
#[must_use]
pub fn is_valid_hash(plugin: &str, authentication_string: &str) -> bool {
    if authentication_string.is_empty() {
        return true;
    }
    if plugin == AuthNativePassword {
        authentication_string.len() == PWDHashLen as usize + 1
    } else if plugin == AuthCachingSha2Password {
        authentication_string.len() == SHAPWDHashLen as usize
    } else if plugin == AuthTiDBSM3Password {
        authentication_string.len() == SM3PWDHashLen as usize
    } else {
        plugin == AuthSocket
            || plugin == AuthTiDBAuthToken
            || plugin == AuthLDAPSimple
            || plugin == AuthLDAPSASL
    }
}

/// Go `UserPrivileges.GetAuthPluginForConnection` after its skip-grant arm:
/// the plugin `checkAuthPlugin` switches the connection to. LDAP and
/// `tidb_auth_token` keep theirs; an account with no authentication string
/// has none (a native-compatible empty password), except `auth_socket`, whose
/// empty string means the OS user maps one-to-one; an invalid hash is Go's
/// error, which `checkAuthPlugin` only logs, so it has none either.
#[must_use]
pub fn auth_plugin_for_connection<'a>(plugin: &'a str, authentication_string: &str) -> &'a str {
    if plugin == AuthTiDBAuthToken || plugin == AuthLDAPSASL || plugin == AuthLDAPSimple {
        return plugin;
    }
    if authentication_string.is_empty() && plugin != AuthSocket {
        return "";
    }
    if is_valid_hash(plugin, authentication_string) {
        plugin
    } else {
        ""
    }
}

/// Go `checkPasswordForPlugin` (`privileges.go:613`): whether the client's
/// `authentication` matches a password plugin's `stored` hash -- a native
/// scramble against the stage-two hash, or the cleartext re-hashed for
/// `caching_sha2_password` / `tidb_sm3_password`. An empty stored hash never
/// matches; one that does not decode is the error.
pub fn check_password_for_plugin(
    plugin: &str,
    stored: &str,
    salt: &[u8],
    authentication: &[u8],
) -> Result<bool, tidb_parser::auth::AuthError> {
    if stored.is_empty() {
        return Ok(false);
    }
    if plugin == AuthNativePassword {
        let hash = tidb_parser::auth::decode_password(stored)?;
        Ok(tidb_parser::auth::check_scrambled_password(
            salt,
            &hash,
            authentication,
        ))
    } else if plugin == AuthCachingSha2Password || plugin == AuthTiDBSM3Password {
        tidb_parser::auth::check_hashing_password_bytes(stored.as_bytes(), authentication, plugin)
    } else {
        Ok(false)
    }
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
/// Go `ErrPasswordFormat` (`ErrPasswordFormat`) for an `AS` hash that does not match the
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
) -> Result<String, MysqlError> {
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
                Err(tidb_error::mysql::MysqlError::from_terror(
                    &tidb_util::dbterror::exeerrors::ERR_PASSWORD_FORMAT
                        .generate_with_stack_by_args(&[]),
                ))
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
