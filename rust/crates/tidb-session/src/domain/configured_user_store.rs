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

//! The deployable SQL node's `mysql.user` table: startup account
//! provisioning plus the login path that verifies against it.
//!
//! The account rows themselves live in the shared
//! [`PrivilegeRegistry`][tidb_privilege::PrivilegeRegistry] -- the
//! same one `CREATE USER`/`GRANT`/`DROP USER` write and `SHOW GRANTS` reads,
//! exactly as Go has ONE `mysql.user` holding both the
//! `authentication_string` and the privilege columns. This file's strict TSV
//! is therefore only a *provisioning* format (the operator's initial rows),
//! not a separate, immutable catalog: an account `CREATE USER ... IDENTIFIED
//! BY` adds at runtime can log in immediately, and one `DROP USER` removes
//! can no longer log in at all. Process-wide skip-grant-table is the explicit
//! recovery exception: it retains this registry as statement storage but
//! admits the requested identity without reading a row or checking a secret.

use std::collections::HashSet;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use tidb_privilege::{check_password_for_plugin, AccountLockout, PrivilegeRegistry};
use crate::GlobalSysvars;
use tidb_mysql::consts::{
    AuthCachingSha2Password, AuthLDAPSASL, AuthLDAPSimple, AuthNativePassword, AuthSocket,
    AuthTiDBAuthToken, AuthTiDBSM3Password,
};

use crate::domain::auth_identity::{
    IdentityCatalog, IdentityLookupPolicy, IdentityLookupRequest, IdentityLookupResult,
    MatchedIdentity, DEFAULT_AUTH_PLUGIN,
};
use crate::domain::native_password::NativePasswordHash;
use crate::domain::secure_transport::{SecureTransportError, SecureTransportPolicy, TransportKind};

/// Canonical identity established after either ordinary account/password
/// verification or the validated process-wide skip-grant-table admission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthenticatedIdentity {
    identity: MatchedIdentity,
    in_sandbox_mode: bool,
    privilege_bypassed: bool,
    /// Domain's internal system-session pool admission. Go's internal
    /// sessions never enter the server's client list, so they never appear
    /// in `SHOW PROCESSLIST`.
    internal: bool,
}

/// Proof that the process-wide secure-transport gate admitted this socket.
///
/// The fields are deliberately private: the MySQL connection owner obtains
/// this value before any auth-plugin exchange, then hands the same decision
/// back to the account verifier so the live global policy is read exactly
/// once, in Go's order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransportAdmission {
    secure_for_account_policy: bool,
    verified_client_certificate: bool,
    tls_peer: Option<tidb_privilege::TlsPeerIdentity>,
}

impl TransportAdmission {
    pub fn with_verified_client_certificate(mut self, verified: bool) -> Self {
        self.verified_client_certificate = self.secure_for_account_policy && verified;
        self
    }
    pub fn with_tls_peer(
        mut self,
        peer: Option<tidb_privilege::TlsPeerIdentity>,
    ) -> Self {
        self.tls_peer = if self.verified_client_certificate {
            peer
        } else {
            None
        };
        self
    }
}

/// Why [`ConfiguredUserStore::authenticate_native`] refused a login. Go's
/// `pkg/privilege/privileges.ConnectionVerification` checks
/// `record.AccountLocked` BEFORE comparing the password (`privileges.go`
/// around line 762) and reports a distinct errno for it,
/// `mysql.ErrAccountHasBeenLocked` (3118, `"Access denied for user '%s'@'%s'.
/// Account is locked."`), rather than the generic `ErrAccessDenied` (1045) a
/// bad password or unknown user gets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthenticationFailure {
    /// The account exists and is locked -- `ACCOUNT LOCK`, or a ROLE (which
    /// Go stores as a locked account with no password). Errno 3118.
    AccountLocked,
    /// Unknown user/host, or a known unlocked account whose password did
    /// not match. Errno 1045; deliberately indistinguishable from each
    /// other, matching Go's account-existence-hiding behavior.
    AccessDenied,
    /// The account auto-locked itself after `FAILED_LOGIN_ATTEMPTS`
    /// consecutive wrong passwords and is still inside its
    /// `PASSWORD_LOCK_TIME` window. Errno 3955, distinct from both of the
    /// above -- Go's message names the remaining lock time, so this is the
    /// one failure that tells the client something specific.
    AutoLocked(AccountLockout),
    /// The account's password has expired and the server refuses expired
    /// logins (`disconnect_on_expired_password`, Go's default). Errno 1862.
    PasswordExpired,
    /// The process-wide `require_secure_transport` is ON and the connection
    /// is plaintext TCP -- Go's `conn.go:669` gate, which fires BEFORE any
    /// account is looked up, so it is the one failure that does not depend
    /// on who the client claims to be. Errno 3159.
    SecureTransportRequired,
    /// Authoritative login tracking could not complete its storage transaction.
    Storage(tidb_store_driver::sql_query_error::SqlQueryError),
}

impl AuthenticatedIdentity {
    /// Identity used by Domain's internal system-session pool. Go's
    /// `SysSessionPool` runs restricted internal SQL without a client login.
    pub fn internal() -> Self {
        Self {
            identity: MatchedIdentity::new("root", "%"),
            in_sandbox_mode: false,
            privilege_bypassed: true,
            internal: true,
        }
    }

    /// Whether this admission came from Domain's internal system-session
    /// pool rather than a client login.
    #[must_use]
    pub fn is_internal(&self) -> bool {
        self.internal
    }

    /// Canonical matched username for ordinary authentication, or the
    /// requested username retained by skip-grant admission.
    #[must_use]
    pub fn username(&self) -> &str {
        self.identity.username()
    }

    /// Canonical matched host pattern for ordinary authentication, or the
    /// presented peer host retained by skip-grant admission.
    #[must_use]
    pub fn host(&self) -> &str {
        self.identity.host()
    }

    /// The verified configured native plugin for an ordinary account, or the
    /// fixed native-plugin handoff Go uses before bypass admission.
    #[must_use]
    pub const fn auth_plugin(&self) -> &'static str {
        DEFAULT_AUTH_PLUGIN
    }

    /// Canonical identity for session metadata owners.
    #[must_use]
    pub const fn matched_identity(&self) -> &MatchedIdentity {
        &self.identity
    }

    /// Whether this login was admitted with an EXPIRED password and must
    /// therefore start in sandbox mode -- Go's `ConnectionVerification`
    /// returning `info.InSandBoxMode`, which `session.Auth` turns into
    /// `EnableSandBoxMode()`.
    #[must_use]
    pub const fn in_sandbox_mode(&self) -> bool {
        self.in_sandbox_mode
    }

    /// Whether startup enabled TiDB's process-wide skip-grant-table policy
    /// for this login. Both privilege-enforcing front-end session factories
    /// (pipeline and cluster) carry this admission fact into the statement
    /// authorization layer.
    #[must_use]
    pub const fn privilege_bypassed(&self) -> bool {
        self.privilege_bypassed
    }
}

/// The live `mysql.user` table, plus the login verifier that reads it.
///
/// Cheaply cloneable: every clone is the SAME table (the registry is an
/// `Arc`-shared map), which is what lets the wire front end's authenticator
/// and the session factory's `CREATE USER`/`GRANT` executor be one store.
#[derive(Clone)]
pub struct ConfiguredUserStore {
    login_storage: Arc<std::sync::RwLock<Option<Arc<dyn LoginPolicyStorage>>>>,
    accounts: PrivilegeRegistry,
    /// The shared `SET GLOBAL`-scope sysvar table, so a `default_password_lifetime`
    /// a session sets is what this login path reads back for the NEXT login
    /// (Go reads the same one process-wide `GlobalVarsAccessor`). Starts as
    /// this store's own private table until [`Self::with_global_vars`]
    /// points it at the one the session factory shares.
    global_vars: GlobalSysvars,
    /// Immutable startup policy corresponding to Go's process-global
    /// `privileges.SkipWithGrant` switch.
    identity_policy: IdentityLookupPolicy,
}

pub trait LoginPolicyStorage: Send + Sync {
    fn apply(
        &self,
        user: &str,
        host: &str,
        action: tidb_privilege::LoginPolicyAction,
    ) -> Result<(), AuthenticationFailure>;
}

impl ConfiguredUserStore {
    pub fn attach_login_storage(&self, storage: Arc<dyn LoginPolicyStorage>) {
        *self
            .login_storage
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(storage);
    }

    fn apply_login_policy(
        &self,
        user: &str,
        host: &str,
        action: tidb_privilege::LoginPolicyAction,
    ) -> Result<(), AuthenticationFailure> {
        use tidb_privilege::LoginPolicyAction;
        let Some(mut cached) = self
            .accounts
            .password_locking(user, host)
            .filter(|policy| policy.tracking_enabled())
        else {
            return Ok(());
        };
        let storage = self
            .login_storage
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(storage) = storage {
            // Go consults the cache before opening the unlock transaction. The
            // post-password operations always read the authoritative locked row.
            if action == LoginPolicyAction::Verify {
                let was_locked = cached.auto_account_locked;
                action
                    .apply(&mut cached, user, host, self.accounts.clock().now_unix())
                    .map_err(AuthenticationFailure::AutoLocked)?;
                if !was_locked {
                    return Ok(());
                }
            }
            return storage.apply(user, host, action);
        }
        match action {
            LoginPolicyAction::Verify => self.accounts.verify_account_auto_lock(user, host),
            LoginPolicyAction::Failure => self.accounts.record_failed_login(user, host),
            LoginPolicyAction::Success => self.accounts.clear_failed_login_count(user, host),
        }
        .map_err(AuthenticationFailure::AutoLocked)
    }

    /// Opens and parses one strict TSV auth file.
    ///
    /// Each record is
    /// `username<TAB>host<TAB>mysql_native_password<TAB>*40HEX`. On Unix the
    /// already-opened descriptor must be a regular file with mode exactly
    /// `0600`; validation and reading use the same descriptor.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfiguredUserStoreError> {
        let mut file = File::open(path).map_err(ConfiguredUserStoreError::Io)?;
        let metadata = file.metadata().map_err(ConfiguredUserStoreError::Io)?;
        if !metadata.file_type().is_file() {
            return Err(ConfiguredUserStoreError::NotRegularFile);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, PermissionsExt};
            if metadata.permissions().mode() & 0o7777 != 0o600 {
                return Err(ConfiguredUserStoreError::InvalidPermissions);
            }
            if metadata.uid() != rustix::process::getuid().as_raw() {
                return Err(ConfiguredUserStoreError::InvalidOwner);
            }
        }

        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .map_err(ConfiguredUserStoreError::Io)?;
        Self::parse(&contents)
    }

    /// Parses strict TSV contents after the file owner validates its
    /// metadata, provisioning one `mysql.user` row per record.
    ///
    /// A record's stored hash must be a strict `*40HEX` stage-two value or
    /// EMPTY, the latter provisioning a passwordless account exactly as
    /// `CREATE USER 'u'@'%'` without `IDENTIFIED BY` does (captured: Go
    /// leaves `authentication_string` empty for such an account).
    pub fn parse(contents: &str) -> Result<Self, ConfiguredUserStoreError> {
        if contents.is_empty() {
            return Err(ConfiguredUserStoreError::EmptyStore);
        }

        let mut seen = HashSet::new();
        let mut provisioned = Vec::new();
        for (index, line) in contents.lines().enumerate() {
            let line_number = index + 1;
            let fields: Vec<_> = line.split('\t').collect();
            if fields.len() != 4 || fields[0].is_empty() || fields[1].is_empty() {
                return Err(ConfiguredUserStoreError::MalformedRecord { line: line_number });
            }
            if fields[2] != DEFAULT_AUTH_PLUGIN {
                return Err(ConfiguredUserStoreError::UnsupportedPlugin { line: line_number });
            }
            if fields.iter().any(|field| {
                field
                    .as_bytes()
                    .iter()
                    .any(|byte| matches!(byte, b'\r' | b'\n' | b'\0'))
            }) {
                return Err(ConfiguredUserStoreError::MalformedRecord { line: line_number });
            }

            let duplicate_key = (fields[0].to_owned(), fields[1].to_owned());
            if !seen.insert(duplicate_key) {
                return Err(ConfiguredUserStoreError::DuplicateIdentity { line: line_number });
            }
            if !fields[3].is_empty() && NativePasswordHash::parse(fields[3]).is_err() {
                return Err(ConfiguredUserStoreError::InvalidPasswordHash { line: line_number });
            }
            provisioned.push((
                fields[0].to_owned(),
                fields[1].to_owned(),
                fields[3].to_owned(),
            ));
        }
        if provisioned.is_empty() {
            return Err(ConfiguredUserStoreError::EmptyStore);
        }

        // The file IS this node's bootstrap, so the table holds exactly the
        // accounts it lists: a node whose file omits `root`@`%` has no root
        // row, and therefore no implicitly passwordless root login.
        Ok(Self {
            accounts: PrivilegeRegistry::bootstrapped_from(provisioned),
            global_vars: GlobalSysvars::new(),
            identity_policy: IdentityLookupPolicy::default(),
            login_storage: Arc::new(std::sync::RwLock::new(None)),
        })
    }

    /// Adopts an already-populated account table as this store's own.
    ///
    /// The TSV file is one way to provision the rows; a cluster whose
    /// `mysql.*` a Go TiDB already wrote is another
    /// ([`tidb_privilege::load::registry_from_cluster`]). Both end at the
    /// same live table, so the login path, `CREATE USER`, and `SHOW GRANTS`
    /// cannot tell which one filled it.
    #[must_use]
    pub fn from_accounts(accounts: PrivilegeRegistry) -> Self {
        Self {
            accounts,
            global_vars: GlobalSysvars::new(),
            identity_policy: IdentityLookupPolicy::default(),
            login_storage: Arc::new(std::sync::RwLock::new(None)),
        }
    }

    /// Creates the deliberately empty account storage used only by a server
    /// admitted under skip-grant-table.
    ///
    /// Go does not load privilege tables in this recovery mode. The registry
    /// still exists so administrative statements have a real storage owner,
    /// but startup neither needs nor fabricates an account row.
    #[must_use]
    pub fn empty_for_skip_grant_table() -> Self {
        Self::from_accounts(PrivilegeRegistry::bootstrapped_from([])).with_skip_grant_table(true)
    }

    /// Applies TiDB's process-wide skip-grant-table identity policy.
    ///
    /// The policy is fixed before the store is shared with a listener. Login
    /// and every session opened from it therefore observe one startup value.
    #[must_use]
    pub fn with_skip_grant_table(mut self, enabled: bool) -> Self {
        self.identity_policy = IdentityLookupPolicy::new(enabled);
        self
    }

    /// The live account table, to be shared with the session factory so that
    /// `CREATE USER`/`DROP USER` and the login path see one set of rows.
    ///
    /// Also the handle a [`tidb_domain_core::cluster_privileges::PrivilegeReloader`]
    /// publishes into: because every clone shares the same underlying
    /// `Mutex`es, a reload thread given this clone keeps this store (and
    /// every other clone) current without either side holding the other.
    #[must_use]
    pub fn accounts(&self) -> PrivilegeRegistry {
        self.accounts.clone()
    }

    /// The `SET GLOBAL`-scope sysvar table this store's login path reads
    /// `default_password_lifetime` from, to be shared with the session
    /// factory ([`crate::domain::pipeline_session::PipelineSessionFactory`]) so a
    /// `SET GLOBAL default_password_lifetime = n` on one connection is what
    /// the NEXT connection's login sees.
    #[must_use]
    pub fn global_vars(&self) -> GlobalSysvars {
        self.global_vars.clone()
    }

    /// Points this store's login path at the session factory's shared
    /// GLOBAL-scope sysvar table instead of its own private one, so
    /// `default_password_lifetime` is one setting rather than two.
    #[must_use]
    pub fn with_global_vars(mut self, global_vars: GlobalSysvars) -> Self {
        self.global_vars = global_vars;
        self
    }

    /// Returns the number of account rows currently in the table.
    #[must_use]
    pub fn len(&self) -> usize {
        self.accounts.accounts().len()
    }

    /// Returns whether the table has no account rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Go `MatchIdentity` + `AuthPluginForUser` (`GetAuthPluginForConnection`),
    /// which `checkAuthPlugin` (`server/conn.go:939`) reads before any auth
    /// exchange: `None` when no account matches the user and host -- Go
    /// answers 1045 at once -- else the plugin the connection is switched to,
    /// empty for "no user plugin set, assuming MySQL Native Password". In
    /// skip-grant mode every request is native password.
    #[must_use]
    pub fn match_identity_plugin(&self, username: &str, remote_host: &str) -> Option<String> {
        if self.identity_policy.skip_with_grant() {
            return Some(DEFAULT_AUTH_PLUGIN.to_owned());
        }
        let catalog = IdentityCatalog::new(
            self.accounts
                .accounts()
                .into_iter()
                .map(|(user, host)| MatchedIdentity::new(&user, &host)),
        );
        let request = IdentityLookupRequest::new(username, remote_host, true);
        let IdentityLookupResult::Matched(identity) = catalog.resolve(&request, &[]) else {
            return None;
        };
        let (authentication_string, plugin, _) = self
            .accounts
            .login_credentials(identity.username(), identity.host())?;
        Some(
            tidb_privilege::auth_plugin_for_connection(&plugin, &authentication_string)
                .to_owned(),
        )
    }

    /// Go `writeInitialHandshake`'s advertised plugin:
    /// `@@global.default_authentication_plugin`.
    pub fn default_authentication_plugin(&self) -> String {
        self.global_vars
            .get("default_authentication_plugin")
            .unwrap_or_else(|_| DEFAULT_AUTH_PLUGIN.to_owned())
    }

    /// Resolves and verifies one native-password response against the LIVE
    /// account table, except that validated skip-grant mode admits the
    /// requested user/peer identity without consulting rows or the response.
    ///
    /// An unknown user or host still executes the native verifier against a
    /// dummy hash before returning [`AuthenticationFailure::AccessDenied`].
    /// An ordinary successful result contains the canonical stored host
    /// pattern; a bypass result deliberately keeps the client-supplied host.
    ///
    /// A passwordless account (empty `authentication_string`) authenticates
    /// only on an empty auth response, which is what a client sends when the
    /// user supplies no password -- except the plugins Go verifies outside
    /// its password arms (`auth_socket`, `tidb_auth_token`, LDAP), which
    /// store an empty string without being passwordless.
    ///
    /// The failed-login lockout and password-expiry checks run in Go's order
    /// (`pkg/session.(*session).Auth`): the auto-lock window is checked and
    /// possibly expired FIRST, before any password comparison, so an account
    /// inside its `PASSWORD_LOCK_TIME` reports 3955 for the right password
    /// too (captured); the `ACCOUNT LOCK` / role check and the password
    /// comparison follow; a wrong password then bumps the failure counter
    /// and may lock the account; a right password clears it; and only a
    /// login that got all the way through is asked whether its password has
    /// expired.
    pub fn authenticate_native(
        &self,
        username: &str,
        remote_host: &str,
        salt: &[u8],
        response: &[u8],
    ) -> Result<AuthenticatedIdentity, AuthenticationFailure> {
        self.authenticate(
            username,
            remote_host,
            salt,
            response,
            TransportKind::PlainTcp,
        )
    }

    /// [`Self::authenticate_native`] told what the connection's transport
    /// actually is, which is what the TLS rules need.
    ///
    /// Go applies them at two different places and this keeps both:
    ///
    /// * `require_secure_transport` (`server/conn.go` line 669) is
    ///   process-wide and fires BEFORE the account is known, so a plaintext
    ///   connection is refused whoever it claims to be. It is read from the
    ///   process atomic, so a `SET GLOBAL` takes effect on the next login.
    /// * the account's own `REQUIRE` clause (`privileges.go`'s `checkSSL`,
    ///   line 795) is per-account and fires INSIDE `ConnectionVerification`,
    ///   after the account is matched and before the password is compared --
    ///   and reports the SAME generic `ErrAccessDenied` a wrong password
    ///   does, so it tells a client nothing about why.
    ///
    /// Skip-grant mode still enforces the process-wide rule first, then skips
    /// the per-account `REQUIRE` rule together with every other account-row
    /// policy because no account row is selected.
    pub fn authenticate(
        &self,
        username: &str,
        remote_host: &str,
        salt: &[u8],
        response: &[u8],
        transport: TransportKind,
    ) -> Result<AuthenticatedIdentity, AuthenticationFailure> {
        let admission = self
            .admit_transport(transport)
            .map_err(|_| AuthenticationFailure::SecureTransportRequired)?;
        self.authenticate_admitted(
            username,
            remote_host,
            salt,
            response,
            tidb_mysql::consts::AuthNativePassword,
            admission,
        )
    }

    /// Applies the process-wide transport gate before any auth-plugin
    /// negotiation and returns a token for the later account verifier.
    pub fn admit_transport(
        &self,
        transport: TransportKind,
    ) -> Result<TransportAdmission, SecureTransportError> {
        let policy = SecureTransportPolicy::new(
            tidb_util::tls::REQUIRE_SECURE_TRANSPORT.load(std::sync::atomic::Ordering::SeqCst),
        );
        policy.admit(transport)?;
        Ok(TransportAdmission {
            secure_for_account_policy: matches!(
                transport,
                TransportKind::DirectTls | TransportKind::GatewayTls
            ),
            verified_client_certificate: false,
            tls_peer: None,
        })
    }

    /// Verifies the account after [`Self::admit_transport`] succeeded.
    ///
    /// The opaque token prevents the wire owner from accidentally reaching
    /// password/account policy while skipping the earlier process-wide gate.
    ///
    /// `auth_plugin` is the plugin the client authenticated with (Go
    /// `UserIdentity.AuthPlugin`): a `tidb_session_token` login presents a
    /// token another TiDB signed instead of a password.
    pub fn authenticate_admitted(
        &self,
        username: &str,
        remote_host: &str,
        salt: &[u8],
        response: &[u8],
        auth_plugin: &str,
        admission: TransportAdmission,
    ) -> Result<AuthenticatedIdentity, AuthenticationFailure> {
        let is_tls = admission.secure_for_account_policy;
        let session_token = auth_plugin == tidb_mysql::consts::AuthTiDBSessionToken;
        let request = IdentityLookupRequest::new(username, remote_host, true);
        if let IdentityLookupResult::Bypassed(identity) =
            IdentityCatalog::default().resolve_with_policy(self.identity_policy, &request, &[])
        {
            return Ok(AuthenticatedIdentity {
                identity,
                in_sandbox_mode: false,
                privilege_bypassed: true,
                internal: false,
            });
        }
        // The source bypass returned above without taking any account-table
        // lock. Only ordinary authentication constructs and sorts the live
        // identity catalog.
        let catalog = IdentityCatalog::new(
            self.accounts
                .accounts()
                .into_iter()
                .map(|(user, host)| MatchedIdentity::new(&user, &host)),
        );
        // Go `session.Auth`: an identity `MatchIdentity` cannot find is
        // access denied before anything else.
        let identity = match catalog.resolve(&request, &[]) {
            IdentityLookupResult::Matched(identity) => identity,
            IdentityLookupResult::Bypassed(_) => unreachable!("ordinary resolution cannot bypass"),
            IdentityLookupResult::NotFound => return Err(AuthenticationFailure::AccessDenied),
        };
        let (user, host) = (identity.username(), identity.host());
        // Go runs `verifyAccountAutoLock` before `ConnectionVerification`,
        // which is what makes a locked account report 3955 rather than 1045
        // even when the password that arrived was correct -- and what
        // auto-unlocks an account whose `PASSWORD_LOCK_TIME` has run out, so
        // the very next correct password works.
        self.apply_login_policy(user, host, tidb_privilege::LoginPolicyAction::Verify)?;
        // Go `ConnectionVerification` from here. `checkSSL` runs on the
        // matched account's `mysql.global_priv` row before the password is
        // compared, and reports the generic access-denied on failure.
        if !self.accounts.admits_account_tls_peer(
            user,
            host,
            is_tls,
            admission.verified_client_certificate,
            admission.tls_peer.as_ref(),
        ) {
            return Err(AuthenticationFailure::AccessDenied);
        }
        let (primary, plugin, secondary) = self
            .accounts
            .login_credentials(user, host)
            .expect("a matched identity has its account row");
        // A stored string that is not its plugin's hash shape is refused
        // before any password.
        if !tidb_privilege::is_valid_hash(&plugin, &primary) {
            return Err(AuthenticationFailure::AccessDenied);
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
                return Err(AuthenticationFailure::AccessDenied);
            }
        } else if plugin == AuthTiDBAuthToken || plugin == AuthLDAPSASL || plugin == AuthLDAPSimple
        {
            // Go verifies a JWT against `GlobalJWKS` and binds to the LDAP
            // server; this server has neither configured, which Go answers
            // with access denied.
            return Err(AuthenticationFailure::AccessDenied);
        } else if plugin == AuthSocket {
            // The response is the peer's OS user, which must be the
            // account's user or its authentication string.
            if response != user.as_bytes() && response != primary.as_bytes() {
                return Err(AuthenticationFailure::AccessDenied);
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
                return Err(AuthenticationFailure::AccessDenied);
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
                self.apply_login_policy(user, host, tidb_privilege::LoginPolicyAction::Failure)?;
                return Err(AuthenticationFailure::AccessDenied);
            }
        }
        // Login to a locked account is not allowed: a ROLE is a `mysql.user`
        // row with `account_locked = 'Y'` and an empty password.
        if self.accounts.is_role(user, host) {
            return Err(AuthenticationFailure::AccountLocked);
        }
        // Go reads the global `default_password_lifetime` for an account
        // whose own `Password_lifetime` is NULL. `default_password_lifetime`
        // is TypeInt (see `sysvar.rs`), so a value the registry's own
        // validation admitted always parses; anything that somehow does not
        // falls back to 0 -- "no cluster-wide expiry" -- rather than
        // panicking the login path.
        let default_password_lifetime = self
            .global_vars
            .get("default_password_lifetime")
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0);
        // A session migrated from another TiDB skips the expiration check,
        // or it could neither log in nor run statements after migration.
        let in_sandbox_mode = !session_token
            && self
                .accounts
                .check_password_expired(user, host, default_password_lifetime)
                .map_err(|_| AuthenticationFailure::PasswordExpired)?;
        self.apply_login_policy(user, host, tidb_privilege::LoginPolicyAction::Success)?;
        Ok(AuthenticatedIdentity {
            identity,
            in_sandbox_mode,
            privilege_bypassed: false,
            internal: false,
        })
    }
}

impl std::fmt::Debug for ConfiguredUserStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConfiguredUserStore")
            .field("account_count", &self.len())
            .finish_non_exhaustive()
    }
}

/// Safe startup auth-file failure categories.
#[derive(Debug)]
pub enum ConfiguredUserStoreError {
    /// The file could not be opened, inspected, or read.
    Io(std::io::Error),
    /// The opened descriptor is not a regular file.
    NotRegularFile,
    /// Unix secret-file permissions are not exactly `0600`.
    InvalidPermissions,
    /// The Unix secret file is not owned by the effective process user.
    InvalidOwner,
    /// No account rows were present.
    EmptyStore,
    /// A record does not have four nonempty required fields.
    MalformedRecord {
        /// One-based record line.
        line: usize,
    },
    /// A record selects a plugin outside this milestone.
    UnsupportedPlugin {
        /// One-based record line.
        line: usize,
    },
    /// An exact username/host row occurs more than once.
    DuplicateIdentity {
        /// One-based record line.
        line: usize,
    },
    /// A stored password is neither empty nor a strict native stage-two
    /// hash.
    InvalidPasswordHash {
        /// One-based record line.
        line: usize,
    },
}

impl std::fmt::Display for ConfiguredUserStoreError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "could not read authentication file: {error}"),
            Self::NotRegularFile => formatter.write_str("authentication file is not regular"),
            Self::InvalidPermissions => {
                formatter.write_str("authentication file permissions must be 0600")
            }
            Self::InvalidOwner => {
                formatter.write_str("authentication file must be owned by the process user")
            }
            Self::EmptyStore => formatter.write_str("authentication file contains no accounts"),
            Self::MalformedRecord { line } => {
                write!(formatter, "authentication file record {line} is malformed")
            }
            Self::UnsupportedPlugin { line } => write!(
                formatter,
                "authentication file record {line} uses an unsupported plugin"
            ),
            Self::DuplicateIdentity { line } => write!(
                formatter,
                "authentication file record {line} duplicates an identity"
            ),
            Self::InvalidPasswordHash { line } => write!(
                formatter,
                "authentication file record {line} has an invalid password hash"
            ),
        }
    }
}

impl std::error::Error for ConfiguredUserStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod account_tls_batch_admission_controls {
    use tidb_privilege::PrivilegeRegistry;
    use crate::domain::configured_user_store::{AuthenticationFailure, ConfiguredUserStore};
    use crate::domain::secure_transport::TransportKind;
    use tidb_privilege::cluster_privilege_load::LoadedGlobalPriv;
    use tidb_privilege::SslType;

    #[test]
    fn account_tls_batch_authentication_uses_go_canonical_account_host() {
        let registry = PrivilegeRegistry::default();
        registry.load_global_priv(vec![LoadedGlobalPriv {
            user: "root".into(),
            host: "127.%".into(),
            priv_json: r#"{"ssl_type":2}"#.into(),
        }]);
        let users = ConfiguredUserStore::from_accounts(registry);
        // MatchIdentity selected root@%; ConnectionVerification uses that
        // authHost, rather than performing another match against the peer IP.
        assert!(users
            .authenticate("root", "127.0.0.1", &[0; 20], &[], TransportKind::DirectTls)
            .is_ok());
    }

    #[test]
    fn account_tls_batch_require_ssl_does_not_treat_unix_as_tls() {
        let registry = PrivilegeRegistry::default();
        registry.set_ssl_type("root", "%", SslType::Any);
        let users = ConfiguredUserStore::from_accounts(registry);
        assert_eq!(
            users
                .authenticate(
                    "root",
                    "localhost",
                    &[0; 20],
                    &[],
                    TransportKind::UnixSocket
                )
                .unwrap_err(),
            AuthenticationFailure::AccessDenied
        );
    }
}
