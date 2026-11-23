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

//! Go `privileges.go`'s `UserPrivileges.ConnectionVerification`: whether the
//! matched account (`authUser`@`authHost`) may log in with what the client
//! sent -- its TLS policy (`checkSSL`), a well-formed stored string, the
//! plugin's verification (session token, `tidb_auth_token`, LDAP SASL /
//! simple, `auth_socket`, an extension plugin, or the password plugins with
//! the retained secondary password), the account lock, and the password
//! expiry. `session.Auth` resolves the identity before it and tracks failed
//! logins around it.

use tidb_mysql::consts::{
    AuthCachingSha2Password, AuthLDAPSASL, AuthLDAPSimple, AuthNativePassword, AuthSocket, AuthTiDBAuthToken,
    AuthTiDBSM3Password,
};

use crate::{check_password_for_plugin, TlsPeerIdentity, UserPrivileges};

/// Go `privilege.VerificationInfo`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct VerificationInfo {
    /// Go `InSandBoxMode`: the password expired and the session starts in
    /// sandbox mode.
    pub in_sandbox_mode: bool,
    /// Go `FailedDueToWrongPassword`: the only failure that counts toward
    /// `FAILED_LOGIN_ATTEMPTS`.
    pub failed_due_to_wrong_password: bool,
}

/// The errors Go's `ConnectionVerification` returns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VerificationError {
    /// Go `ErrAccessDenied` (1045).
    AccessDenied,
    /// Go `errAccountHasBeenLocked` (3118).
    AccountLocked,
    /// Go `CheckPasswordExpired`'s `ErrMustChangePasswordLogin` (1862).
    PasswordExpired,
}

/// What Go's `ConnectionVerification` is given.
pub struct VerificationRequest<'a> {
    /// Go `user.Username`: the name the client sent.
    pub username: &'a str,
    /// Go `authUser`.
    pub auth_user: &'a str,
    /// Go `authHost`.
    pub auth_host: &'a str,
    /// Go `authentication`.
    pub response: &'a [u8],
    /// Go `salt`.
    pub salt: &'a [u8],
    /// Go `user.AuthPlugin == mysql.AuthTiDBSessionToken`.
    pub session_token: bool,
    /// Go `sessionVars.TLSConnectionState != nil`, as the account policy
    /// reads it.
    pub is_tls: bool,
    /// Whether the TLS peer presented a verified client certificate.
    pub verified_client_certificate: bool,
    /// The verified peer certificate's identity, for `checkSSL`'s properties.
    pub tls_peer: Option<&'a TlsPeerIdentity>,
    /// Go `sessionVars.TLSConnectionState`, which an extension plugin reads.
    pub conn_state: Option<&'a tidb_variable::vars::TlsConnectionState>,
    /// Go `sessionVars`' `default_password_lifetime`, which
    /// `CheckPasswordExpired` reads for an account without its own lifetime.
    pub default_password_lifetime: i64,
}

impl UserPrivileges<'_> {
    /// Go `UserPrivileges.ConnectionVerification`.
    pub fn connection_verification(
        &self,
        request: VerificationRequest<'_>,
        auth_conn: Option<&mut dyn tidb_extension::AuthConn>,
    ) -> (VerificationInfo, Result<(), VerificationError>) {
        let mut info = VerificationInfo::default();
        let (user, host, username, response, salt, conn_state) =
            (request.auth_user, request.auth_host, request.username, request.response, request.salt, request.conn_state);
        let session_token = request.session_token;
        // Go `ConnectionVerification` from here. `checkSSL` runs on the
        // matched account's `mysql.global_priv` row before the password is
        // compared, and reports the generic access-denied on failure.
        if !self.handle.admits_account_tls_peer(
            user,
            host,
            request.is_tls,
            request.verified_client_certificate,
            request.tls_peer,
        ) {
            return (info, Err(VerificationError::AccessDenied));
        }
        let (primary, plugin, secondary) = self
            .handle
            .login_credentials(user, host)
            .expect("a matched identity has its account row");
        // A stored string that is not its plugin's hash shape is refused
        // before any password.
        if !crate::is_valid_hash(&plugin, &primary) {
            return (info, Err(VerificationError::AccessDenied));
        }
        if session_token {
            // Go skips the account's own plugin for a session token, and a
            // token that does not verify is no wrong password.
            if let Err(error) = tidb_sessionstates::validate_session_token(response, username) {
                tidb_log::warn(
                    "verify session token failed",
                    &[
                        tidb_log::Field::new("username", tidb_log::Value::Str(username.to_owned())),
                        tidb_log::Field::new("error", tidb_log::Value::Str(error)),
                    ],
                );
                return (info, Err(VerificationError::AccessDenied));
            }
        } else if plugin == AuthTiDBAuthToken {
            if response.is_empty() {
                tidb_logutil::bg_logger().warn("empty authentication", &[]);
                return (info, Err(VerificationError::AccessDenied));
            }
            let token_string = String::from_utf8_lossy(&response[..response.len() - 1]);
            let claims = match crate::tidb_auth_token::GLOBAL_JWKS.check_sig_with_retry(&token_string, 1) {
                Ok(claims) => claims,
                Err(error) => {
                    tidb_logutil::bg_logger()
                        .warn("verify JWT failed", &[tidb_log::Field::new("error", tidb_log::Value::Str(error))]);
                    return (info, Err(VerificationError::AccessDenied));
                }
            };
            let (auth_token_issuer, email) = self.handle.auth_token_record(user, host).unwrap_or_default();
            let record = crate::tidb_auth_token::AuthTokenRecord {
                user,
                email: &email,
                auth_token_issuer: &auth_token_issuer,
            };
            if let Err(error) = crate::tidb_auth_token::check_auth_token_claims(
                &claims,
                &record,
                crate::tidb_auth_token::DEFAULT_TOKEN_LIFE,
            ) {
                tidb_logutil::bg_logger()
                    .warn("check claims failed", &[tidb_log::Field::new("error", tidb_log::Value::Str(error))]);
                return (info, Err(VerificationError::AccessDenied));
            }
        } else if plugin == AuthLDAPSASL {
            let Some(auth_conn) = auth_conn else {
                return (info, Err(VerificationError::AccessDenied));
            };
            if let Err(error) =
                tidb_privilege_ldap::LDAP_SASL_AUTH_IMPL.auth_ldap_sasl(user, &primary, response, auth_conn)
            {
                // though the pwd stores only `dn` for LDAP SASL, it could be
                // unsafe to print it out. for example, someone may alter the
                // auth plugin name but forgot to change the password...
                tidb_logutil::bg_logger().warn(
                    "verify through LDAP SASL failed",
                    &[
                        tidb_log::Field::new("username", tidb_log::Value::Str(username.to_owned())),
                        tidb_log::Field::new("error", tidb_log::Value::Str(error)),
                    ],
                );
                return (info, Err(VerificationError::AccessDenied));
            }
        } else if plugin == AuthLDAPSimple {
            if let Err(error) =
                tidb_privilege_ldap::LDAP_SIMPLE_AUTH_IMPL.auth_ldap_simple(user, &primary, response)
            {
                tidb_logutil::bg_logger().warn(
                    "verify through LDAP Simple failed",
                    &[
                        tidb_log::Field::new("username", tidb_log::Value::Str(username.to_owned())),
                        tidb_log::Field::new("error", tidb_log::Value::Str(error)),
                    ],
                );
                return (info, Err(VerificationError::AccessDenied));
            }
        } else if plugin == AuthSocket {
            // The response is the peer's OS user, which must be the
            // account's user or its authentication string.
            if response != user.as_bytes() && response != primary.as_bytes() {
                return (info, Err(VerificationError::AccessDenied));
            }
        } else if let Some(p) = match tidb_extension::get_extensions() {
            Ok(extensions) => extensions.and_then(|es| es.get_auth_plugins().remove(&plugin)),
            Err(_) => return (info, Err(VerificationError::AccessDenied)),
        } {
            let authenticate = p.authenticate_user.as_ref().expect("validated auth plugin");
            // Never surface the callback's error: it may contain credentials.
            if authenticate(tidb_extension::AuthenticateRequest {
                user: username,
                stored_auth_string: &primary,
                input_auth_string: response,
                salt,
                conn_state,
                auth_conn,
            })
            .is_err()
            {
                return (info, Err(VerificationError::AccessDenied));
            }
        } else if !primary.is_empty() || !response.is_empty() {
            // The password plugins. Deliberately reachable with an EMPTY
            // stored primary when the client supplied a password, so a
            // retained secondary can still authenticate; a passwordless login
            // (empty primary AND empty response) never enters here.
            if plugin != AuthNativePassword
                && plugin != AuthCachingSha2Password
                && plugin != AuthTiDBSM3Password
            {
                tidb_log::warn(
                    "unknown authentication plugin",
                    &[
                        tidb_log::Field::new("authUser", tidb_log::Value::Str(user.to_owned())),
                        tidb_log::Field::new("plugin", tidb_log::Value::Str(plugin.clone())),
                    ],
                );
                return (info, Err(VerificationError::AccessDenied));
            }
            let primary_ok = check_password_for_plugin(&plugin, &primary, salt, response);
            // A malformed stored native primary is a wrong password that no
            // secondary can rescue.
            let verified = match primary_ok {
                Err(_) if plugin == AuthNativePassword => false,
                Ok(true) => true,
                _ => secondary.as_deref().is_some_and(|secondary| {
                    check_password_for_plugin(&plugin, secondary, salt, response).unwrap_or(false)
                }),
            };
            if !verified {
                // Go's `FailedDueToWrongPassword`: the only failure that
                // counts toward `FAILED_LOGIN_ATTEMPTS`.
                info.failed_due_to_wrong_password = true;
                return (info, Err(VerificationError::AccessDenied));
            }
        }
        // Login a locked account is not allowed: a ROLE is a `mysql.user`
        // row with `account_locked = 'Y'` and an empty password.
        if self.handle.is_role(user, host) {
            return (info, Err(VerificationError::AccountLocked));
        }
        // Skip checking password expiration if the session is migrated from
        // another session. Otherwise, the user cannot log in or execute
        // statements after migration.
        if !session_token {
            match self.handle.check_password_expired(user, host, request.default_password_lifetime) {
                Ok(in_sandbox_mode) => info.in_sandbox_mode = in_sandbox_mode,
                Err(_) => return (info, Err(VerificationError::PasswordExpired)),
            }
        }
        (info, Ok(()))
    }
}
