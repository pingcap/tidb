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

//! Account management: `CREATE`/`DROP`/`ALTER`/`RENAME USER`, `SET PASSWORD`,
//! and `GRANT`/`REVOKE`/`SHOW GRANTS`.
//!
//! These are the arms `Session::apply_schema_statement` and
//! `Session::dispatch_admin_stmt` delegate to; grouped together because they
//! all read and write the same `mysql.user` / privilege-registry state the
//! `privilege` module models.

use crate::show::string_column_output;
use crate::*;

/// Go's `passwordOrLockOptionsInfo` after `loadOptions`: the account-state
/// changes ONE `CREATE`/`ALTER USER` statement's `PASSWORD ...` /
/// `ACCOUNT ...` / `FAILED_LOGIN_ATTEMPTS` / `PASSWORD_LOCK_TIME` clauses
/// add up to. Every field is `None` when the statement wrote no clause of
/// that kind, which is the distinction `ALTER USER` needs: an unwritten
/// option keeps the account's current value rather than resetting it.
#[derive(Default)]
pub(crate) struct PasswordOrLockOptions {
    /// `ACCOUNT LOCK` / `ACCOUNT UNLOCK`.
    locked: Option<bool>,
    /// `FAILED_LOGIN_ATTEMPTS n`, clamped as Go clamps it.
    failed_login_attempts: Option<i64>,
    /// `PASSWORD_LOCK_TIME n | UNBOUNDED`; `UNBOUNDED` is `-1`.
    password_lock_time_days: Option<i64>,
    /// `PASSWORD EXPIRE [DEFAULT | NEVER | INTERVAL n DAY]`.
    expire: Option<privilege::PasswordExpireSetting>,
}

/// Go clamps `FAILED_LOGIN_ATTEMPTS` and `PASSWORD_LOCK_TIME` to
/// `math.MaxInt16` rather than rejecting a larger count.
const MAX_PASSWORD_LOCK_COUNT: i64 = i16::MAX as i64;

/// Go rejects `PASSWORD EXPIRE INTERVAL n DAY` outside `1 ..= MaxUint16`
/// with `ErrWrongValue2("DAY", n)`.
const MAX_PASSWORD_EXPIRE_INTERVAL_DAYS: i64 = u16::MAX as i64;

impl PasswordOrLockOptions {
    /// Go's `passwordOrLockOptionsInfo.loadOptions`.
    ///
    /// Go reads the expiry clauses BACKWARD (stopping at the first one it
    /// finds) and every other clause forward, overwriting as it goes -- two
    /// directions, one rule: the LAST clause of each kind wins. A single
    /// forward pass that overwrites therefore reproduces both, with no
    /// direction to special-case (captured:
    /// `FAILED_LOGIN_ATTEMPTS 1 PASSWORD_LOCK_TIME unbounded
    /// FAILED_LOGIN_ATTEMPTS 5 PASSWORD_LOCK_TIME 5` stores 5 and 5).
    pub(crate) fn load(
        options: &[tidb_ast::CreateUserPasswordOption],
    ) -> Result<Self, DriverError> {
        use tidb_ast::AlterUserPasswordExpire as Expire;
        use tidb_ast::CreateUserPasswordOption as Option_;
        let mut loaded = Self::default();
        for option in options {
            match option {
                Option_::AccountLock => loaded.locked = Some(true),
                Option_::AccountUnlock => loaded.locked = Some(false),
                Option_::FailedLoginAttempts(count) => {
                    loaded.failed_login_attempts = Some((*count).min(MAX_PASSWORD_LOCK_COUNT));
                }
                Option_::PasswordLockTime(days) => {
                    loaded.password_lock_time_days = Some((*days).min(MAX_PASSWORD_LOCK_COUNT));
                }
                Option_::PasswordLockTimeUnbounded => loaded.password_lock_time_days = Some(-1),
                Option_::Expire(Expire::Expire) => {
                    loaded.expire = Some(privilege::PasswordExpireSetting::Now);
                }
                Option_::Expire(Expire::Default) => {
                    loaded.expire = Some(privilege::PasswordExpireSetting::Default);
                }
                Option_::Expire(Expire::Never) => {
                    loaded.expire = Some(privilege::PasswordExpireSetting::Never);
                }
                Option_::Expire(Expire::Interval(days)) => {
                    if *days <= 0 || *days > MAX_PASSWORD_EXPIRE_INTERVAL_DAYS {
                        return Err(DriverError::PasswordExpireIntervalOutOfRange { days: *days });
                    }
                    loaded.expire = Some(privilege::PasswordExpireSetting::Interval(*days));
                }
                Option_::History(_)
                | Option_::HistoryDefault
                | Option_::ReuseInterval(_)
                | Option_::ReuseDefault
                | Option_::RequireCurrentDefault => {
                    return Err(DriverError::Unsupported(
                        "PASSWORD HISTORY / PASSWORD REUSE INTERVAL / PASSWORD REQUIRE CURRENT are not supported yet",
                    ));
                }
            }
        }
        Ok(loaded)
    }

    /// Whether the statement wrote no clause at all, which is what lets
    /// `CREATE USER` leave a brand-new row exactly as it was bootstrapped.
    fn is_empty(&self) -> bool {
        self.locked.is_none()
            && self.failed_login_attempts.is_none()
            && self.password_lock_time_days.is_none()
            && self.expire.is_none()
    }

    /// Writes this statement's options onto one existing account row.
    ///
    /// Order matters: `set_locked(.., false)` is the clause that clears the
    /// failed-login counter (Go's `alterUserFailedLoginJSON` resets it
    /// whenever `lockAccount` is `"N"`), so `ACCOUNT UNLOCK` is applied AFTER
    /// the new policy, leaving the captured
    /// `ALTER USER u5 ACCOUNT UNLOCK FAILED_LOGIN_ATTEMPTS 3
    /// PASSWORD_LOCK_TIME 6` -> policy 3/6 with count 0.
    fn apply(&self, registry: &privilege::PrivilegeRegistry, user: &str, host: &str) {
        if self.failed_login_attempts.is_some() || self.password_lock_time_days.is_some() {
            registry.set_password_locking_options(
                user,
                host,
                self.failed_login_attempts,
                self.password_lock_time_days,
            );
        }
        if let Some(expire) = self.expire {
            registry.set_password_expire(user, host, expire);
        }
        if let Some(locked) = self.locked {
            registry.set_locked(user, host, locked);
        }
    }
}

/// The account identity a written role names. Go's role grammar defaults the
/// omitted host to `%`, the same wildcard host `CREATE USER r` gets, which is
/// what makes `CREATE ROLE r` and `CREATE USER r` collide.
fn role_identity(spec: &tidb_ast::RoleSpec) -> privilege::Account {
    let host = if spec.host.is_empty() {
        "%".to_owned()
    } else {
        spec.host.clone()
    };
    (spec.role.clone(), host)
}

impl Session {
    /// `CREATE USER [IF NOT EXISTS] <account> [IDENTIFIED BY '<password>']`.
    /// Go `simple.go`'s `executeCreateUser`, minus resource limits and
    /// account annotations, which this tier has no storage for and therefore
    /// refuses rather than silently drops.
    ///
    /// `IDENTIFIED BY` stores the account's
    /// `mysql.user.authentication_string` (see
    /// [`privilege::encode_password`]), which is the same row the wire front
    /// end verifies a login against -- so an account created here can
    /// immediately log in with that password.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_user_stmt(
        &mut self,
        if_not_exists: bool,
        users: &[tidb_ast::CreateUserSpec],
        tls_options: &[tidb_ast::AlterUserTlsOption],
        resource_options: &[tidb_ast::AlterUserResourceOption],
        password_options: &[tidb_ast::CreateUserPasswordOption],
        comment_or_attribute: &Option<tidb_ast::CreateUserCommentOrAttribute>,
        resource_group: &Option<String>,
    ) -> Result<StmtOutput, DriverError> {
        if !tls_options.is_empty()
            || !resource_options.is_empty()
            || comment_or_attribute.is_some()
            || resource_group.is_some()
        {
            return Err(DriverError::Unsupported(
                "CREATE USER options beyond the account list are not supported yet",
            ));
        }
        // Go validates every statement-level option BEFORE writing any row,
        // so a bad `PASSWORD EXPIRE INTERVAL 0 DAY` creates no account.
        let options = PasswordOrLockOptions::load(password_options)?;
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::Unsupported(
                "CREATE USER requires a server front end with a privilege registry",
            ));
        };
        for spec in users {
            let (auth_string, plugin) = Self::resolve_auth_string_and_plugin(spec.auth.as_ref())?;
            if spec.dual_password.is_some() {
                return Err(DriverError::Unsupported(
                    "CREATE USER ... RETAIN CURRENT PASSWORD is not supported yet",
                ));
            }
            let user = spec.user.user.as_str();
            let host = spec.user.host.as_str();
            // Go processes each account in source order and fails on the
            // FIRST duplicate rather than batching, unlike DROP USER below.
            if registry.create_user_with_plugin(user, host, &auth_string, &plugin) {
                options.apply(&registry, user, host);
            } else if !if_not_exists {
                return Err(DriverError::CreateUserAlreadyExists {
                    user: user.to_owned(),
                    host: host.to_owned(),
                });
            }
        }
        Ok(StmtOutput::Affected(0))
    }

    /// `CREATE ROLE [IF NOT EXISTS] <role> [, ...]` -- Go's `executeCreateUser`
    /// reached with `IsCreateRole`, which writes the same `mysql.user` row
    /// with `account_locked = 'Y'` and no password.
    ///
    /// Roles and users share ONE namespace, so this reports `ErrCannotUser`
    /// against a name already taken by either kind (captured both ways); the
    /// only thing that differs from `CREATE USER`'s message is the operation
    /// name it prints.
    pub(crate) fn create_role_stmt(
        &mut self,
        if_not_exists: bool,
        roles: &[tidb_ast::RoleSpec],
    ) -> Result<StmtOutput, DriverError> {
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::Unsupported(
                "CREATE ROLE requires a server front end with a privilege registry",
            ));
        };
        for spec in roles {
            let (role, host) = role_identity(spec);
            if !registry.create_role(&role, &host) && !if_not_exists {
                return Err(DriverError::CannotUserRole {
                    operation: "CREATE ROLE",
                    target: format!("'{role}'@'{host}'"),
                });
            }
        }
        Ok(StmtOutput::Affected(0))
    }

    /// `GRANT <role> [, ...] TO <account> [, ...]`: one `mysql.role_edges`
    /// row per pair. Go validates every ROLE first (`ErrGrantRole`/3523 for a
    /// role with no account row) and only then every target account
    /// (`ErrCannotUser`/1396), so a statement naming both an unknown role and
    /// an unknown user reports the ROLE (captured order).
    ///
    /// Nothing rejects a self-grant or a cycle: `GRANT r1 TO r1` reports OK
    /// (captured).
    pub(crate) fn grant_role_stmt(
        &mut self,
        grant: &tidb_ast::GrantRoleStmt,
    ) -> Result<StmtOutput, DriverError> {
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::Unsupported(
                "GRANT <role> requires a server front end with a privilege registry",
            ));
        };
        let roles =
            self.resolve_roles(&grant.roles, |role, host| DriverError::GrantUnknownRole {
                role: role.to_owned(),
                host: host.to_owned(),
            })?;
        let grantees = self.resolve_role_grantees(&grant.users, "GRANT ROLE")?;
        for grantee in &grantees {
            for role in &roles {
                registry.grant_role(role, grantee);
            }
        }
        Ok(StmtOutput::Affected(0))
    }

    /// `REVOKE <role> [, ...] FROM <account> [, ...]`. Unlike `GRANT`, a
    /// missing ROLE here reports `ErrCannotUser`/1396 -- backtick-quoted, as
    /// `auth.RoleIdentity.String` prints it -- rather than 3523. Revoking a
    /// role the account never held is a silent no-op (captured).
    pub(crate) fn revoke_role_stmt(
        &mut self,
        revoke: &tidb_ast::RevokeRoleStmt,
    ) -> Result<StmtOutput, DriverError> {
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::Unsupported(
                "REVOKE <role> requires a server front end with a privilege registry",
            ));
        };
        let roles =
            self.resolve_roles(&revoke.roles, |role, host| DriverError::CannotUserRole {
                operation: "REVOKE ROLE",
                target: format!("`{role}`@`{host}`"),
            })?;
        let grantees = self.resolve_role_grantees(&revoke.users, "REVOKE ROLE")?;
        for grantee in &grantees {
            for role in &roles {
                registry.revoke_role(role, grantee);
            }
        }
        // Go drops a revoked role from the CURRENT session's active set in
        // the same statement, so a session cannot keep using privileges it
        // no longer holds.
        if let Some((user, host)) = self.current_identity() {
            let account = (user.to_owned(), host.to_owned());
            if grantees.contains(&account) {
                self.active_roles.retain(|role| !roles.contains(role));
            }
        }
        Ok(StmtOutput::Affected(0))
    }

    /// `SET ROLE <selection>`: replaces the session's active-role set.
    ///
    /// Activation is DIRECT-ONLY at every form -- `ALL` offers exactly the
    /// roles granted to the account, and naming a role held only through
    /// another role reports `ErrRoleNotGranted`/3530 (captured). What the
    /// activated roles then CONFER is transitive, but that is a question for
    /// the privilege check, not for this set.
    ///
    /// A rejected `SET ROLE` leaves the previous set untouched (captured).
    pub(crate) fn set_role_stmt(
        &mut self,
        set_role: &tidb_ast::SetRoleStmt,
    ) -> Result<StmtOutput, DriverError> {
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::Unsupported(
                "SET ROLE requires a server front end with a privilege registry",
            ));
        };
        let account = self.own_account()?;
        let active = match &set_role.selection {
            tidb_ast::SetRoleSelection::None => Vec::new(),
            tidb_ast::SetRoleSelection::All => registry.granted_roles(&account),
            tidb_ast::SetRoleSelection::Default => registry.default_roles(&account),
            tidb_ast::SetRoleSelection::AllExcept(excluded) => {
                let excluded = self.granted_roles_or_error(&registry, &account, excluded)?;
                registry
                    .granted_roles(&account)
                    .into_iter()
                    .filter(|role| !excluded.contains(role))
                    .collect()
            }
            tidb_ast::SetRoleSelection::Roles(roles) => {
                self.granted_roles_or_error(&registry, &account, roles)?
            }
        };
        self.active_roles = active;
        Ok(StmtOutput::Affected(0))
    }

    /// `SET DEFAULT ROLE <selection> TO <account> [, ...]`: replaces each
    /// account's `mysql.default_roles` rows (never merges -- captured:
    /// `SET DEFAULT ROLE r1` after `ALL` leaves r1 alone).
    ///
    /// `ALL` means every role granted to THAT account, resolved per account.
    /// A named role that the account does not hold reports 3530, the same
    /// gate `SET ROLE` uses.
    pub(crate) fn set_default_role_stmt(
        &mut self,
        set_default: &tidb_ast::SetDefaultRoleStmt,
    ) -> Result<StmtOutput, DriverError> {
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::Unsupported(
                "SET DEFAULT ROLE requires a server front end with a privilege registry",
            ));
        };
        let accounts = self.resolve_role_grantees(&set_default.users, "SET DEFAULT ROLE")?;
        for account in &accounts {
            let roles = match &set_default.selection {
                tidb_ast::DefaultRoleSelection::None => Vec::new(),
                tidb_ast::DefaultRoleSelection::All => registry.granted_roles(account),
                tidb_ast::DefaultRoleSelection::Roles(roles) => {
                    self.granted_roles_or_error(&registry, account, roles)?
                }
            };
            registry.set_default_roles(account, &roles);
        }
        Ok(StmtOutput::Affected(0))
    }

    /// Resolves written role identities to accounts, requiring each to have
    /// an account row. `missing` builds the error, because `GRANT` and
    /// `REVOKE` report a missing role differently (3523 vs 1396).
    fn resolve_roles(
        &self,
        roles: &[tidb_ast::RoleSpec],
        missing: impl Fn(&str, &str) -> DriverError,
    ) -> Result<Vec<privilege::Account>, DriverError> {
        let Some(registry) = &self.privileges else {
            return Err(DriverError::Unsupported(
                "roles require a server front end with a privilege registry",
            ));
        };
        roles
            .iter()
            .map(|spec| {
                let (role, host) = role_identity(spec);
                if registry.user_exists(&role, &host) {
                    Ok((role, host))
                } else {
                    Err(missing(&role, &host))
                }
            })
            .collect()
    }

    /// Resolves the accounts a role statement targets, requiring each to
    /// exist (`ErrCannotUser` naming `operation`, with the account printed
    /// bare as `user@host`).
    fn resolve_role_grantees(
        &self,
        users: &[tidb_ast::UserSpec],
        operation: &'static str,
    ) -> Result<Vec<privilege::Account>, DriverError> {
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::Unsupported(
                "roles require a server front end with a privilege registry",
            ));
        };
        users
            .iter()
            .map(|spec| {
                let (user, host) = self.resolve_account(spec)?;
                if registry.user_exists(&user, &host) {
                    Ok((user, host))
                } else {
                    Err(DriverError::CannotUserRole {
                        operation,
                        target: format!("{user}@{host}"),
                    })
                }
            })
            .collect()
    }

    /// Resolves roles that must be granted DIRECTLY to `account`, which is
    /// the gate `SET ROLE` and `SET DEFAULT ROLE` share.
    fn granted_roles_or_error(
        &self,
        registry: &privilege::PrivilegeRegistry,
        account: &privilege::Account,
        roles: &[tidb_ast::RoleSpec],
    ) -> Result<Vec<privilege::Account>, DriverError> {
        roles
            .iter()
            .map(|spec| {
                let role = role_identity(spec);
                if registry.has_role(account, &role) {
                    Ok(role)
                } else {
                    Err(DriverError::RoleNotGranted {
                        role: role.0.clone(),
                        role_host: role.1.clone(),
                        user: account.0.clone(),
                        host: account.1.clone(),
                    })
                }
            })
            .collect()
    }

    /// `CREATE USER`'s
    /// `IDENTIFIED WITH <plugin> [BY '<password>' | AS '<hash>']` form.
    ///
    /// Returns the account's `(authentication_string, plugin)` pair. An
    /// unrecognized plugin name is Go's `ErrPluginIsNotLoaded` (1524): this
    /// tier registers no extension auth plugins, so any name outside
    /// [`privilege::CREATE_USER_PLUGINS`] can never be loaded. A missing
    /// `IDENTIFIED` clause defaults to `mysql_native_password`, empty
    /// (passwordless) -- Go's default when `CREATE USER` writes neither
    /// `BY` nor `WITH`.
    fn resolve_auth_string_and_plugin(
        auth: Option<&tidb_ast::CreateUserAuth>,
    ) -> Result<(String, String), DriverError> {
        const DEFAULT_PLUGIN: &str = tidb_mysql::consts::AuthNativePassword;
        match auth {
            None => Ok((String::new(), DEFAULT_PLUGIN.to_owned())),
            Some(tidb_ast::CreateUserAuth::By(password)) => Ok((
                privilege::encode_password(password),
                DEFAULT_PLUGIN.to_owned(),
            )),
            Some(tidb_ast::CreateUserAuth::With { plugin, credential }) => {
                if !privilege::is_create_user_plugin(plugin) {
                    return Err(DriverError::PluginIsNotLoaded {
                        plugin: plugin.clone(),
                    });
                }
                let credential = match credential {
                    None => privilege::PluginCredential::None,
                    Some(tidb_ast::CreateUserCredential::By(password)) => {
                        privilege::PluginCredential::By(password)
                    }
                    Some(tidb_ast::CreateUserCredential::As(hash)) => {
                        privilege::PluginCredential::As(hash)
                    }
                };
                let auth_string = privilege::encode_password_for_plugin(plugin, &credential)?;
                Ok((auth_string, plugin.clone()))
            }
        }
    }

    /// `ALTER USER [IF EXISTS] <account> [IDENTIFIED [WITH '<plugin>'] BY
    /// '<password>'] [ACCOUNT LOCK | ACCOUNT UNLOCK]`, the `ALTER USER`
    /// actions this tier stores: a password/plugin change rewrites the
    /// account's `mysql.user.authentication_string` (and `plugin`) in place,
    /// and `ACCOUNT LOCK`/`UNLOCK` flips `account_locked` (Go
    /// `executeAlterUser`). Every other statement-level option (TLS,
    /// resource limits, comment/attribute, resource group, and all other
    /// `PASSWORD ...` clauses) remains unsupported.
    pub(crate) fn alter_user_stmt(
        &mut self,
        alter: &tidb_ast::AlterUserStmt,
    ) -> Result<StmtOutput, DriverError> {
        if alter.user_function_auth.is_some()
            || alter.user_function_dual_password.is_some()
            || !alter.tls_options.is_empty()
            || !alter.resource_options.is_empty()
            || alter.comment_or_attribute.is_some()
            || alter.resource_group.is_some()
        {
            return Err(DriverError::Unsupported(
                "ALTER USER options beyond IDENTIFIED [WITH] BY / ACCOUNT LOCK|UNLOCK are not supported yet",
            ));
        }
        let options = PasswordOrLockOptions::load(&alter.password_options)?;
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::Unsupported(
                "ALTER USER requires a server front end with a privilege registry",
            ));
        };
        for spec in &alter.users {
            if spec.dual_password.is_some() {
                return Err(DriverError::Unsupported(
                    "ALTER USER options beyond IDENTIFIED [WITH] BY / ACCOUNT LOCK|UNLOCK are not supported yet",
                ));
            }
            let (user, host) = self.resolve_account(&spec.user)?;
            if let Some(auth) = spec.auth.as_ref() {
                // A bare `IDENTIFIED BY` (no `WITH <plugin>`) keeps the
                // account's CURRENT plugin (Go backfills
                // `spec.AuthOpt.AuthPlugin` from `currentAuthPlugin` rather
                // than resetting it to `mysql_native_password`); only an
                // explicit `IDENTIFIED WITH` changes it.
                let (auth_string, plugin) = match auth {
                    tidb_ast::CreateUserAuth::By(password) => {
                        let current_plugin = registry
                            .plugin(&user, &host)
                            .unwrap_or_else(|| tidb_mysql::consts::AuthNativePassword.to_owned());
                        (privilege::encode_password(password), current_plugin)
                    }
                    tidb_ast::CreateUserAuth::With { .. } => {
                        Self::resolve_auth_string_and_plugin(Some(auth))?
                    }
                };
                if registry.set_auth_string_and_plugin(&user, &host, &auth_string, &plugin) {
                    // Go writes `password_expired='N'` and a fresh
                    // `Password_last_changed` in the same UPDATE as the new
                    // hash, which is what lets an expired account recover by
                    // setting a password (captured: after
                    // `ALTER USER e5 IDENTIFIED BY 'pw2'`, `SHOW CREATE USER`
                    // reports `PASSWORD EXPIRE DEFAULT` again).
                    registry.mark_password_changed(&user, &host);
                } else if !alter.if_exists {
                    return Err(DriverError::AlterUserMissing { user, host });
                }
            } else if options.is_empty() {
                return Err(DriverError::Unsupported(
                    "ALTER USER options beyond IDENTIFIED [WITH] BY / password-and-lock options are not supported yet",
                ));
            } else if !registry.user_exists(&user, &host) && !alter.if_exists {
                return Err(DriverError::AlterUserMissing { user, host });
            }
            // Go applies the statement's options in the same UPDATE that
            // writes the password, so a statement doing both lands both.
            options.apply(&registry, &user, &host);
        }
        // A sandboxed session escapes by giving ITSELF a new password, which
        // is the only thing it was allowed in here to do (Go's
        // `executeAlterUser` -> `checkSandboxMode`, whose gate ran before the
        // statement reached this driver).
        if self.sandbox_mode && alter.users.iter().any(|spec| spec.auth.is_some()) {
            self.sandbox_mode = false;
        }
        Ok(StmtOutput::Affected(0))
    }

    /// `SHOW CREATE USER <account>`. Go's `fetchShowCreateUser`
    /// (`pkg/executor/show.go`) reads `mysql.user`/`mysql.global_priv`
    /// columns this tier has no storage for beyond `authentication_string`,
    /// `plugin`, and `account_locked` (the `ACCOUNT LOCK`/`UNLOCK` flag
    /// `set_locked` writes) -- every other clause therefore prints its
    /// Go-observed DEFAULT rather than a tracked value:
    /// - `REQUIRE NONE` always (no TLS/`REQUIRE` storage; `CREATE`/`ALTER
    ///   USER` already reject `tls_options`, so no account can differ here).
    /// - `PASSWORD HISTORY DEFAULT` / `PASSWORD REUSE INTERVAL DEFAULT`
    ///   always (no `Password_reuse_history`/`Password_reuse_time` storage).
    /// - No ` token_issuer`, ` WITH MAX_USER_CONNECTIONS`, or ` ATTRIBUTE`
    ///   suffix (no storage for any of them; Go omits each when its column is
    ///   NULL/empty too, so a freshly created account's line matches byte for
    ///   byte).
    ///
    /// The `PASSWORD EXPIRE ...` clause and the
    /// ` FAILED_LOGIN_ATTEMPTS n PASSWORD_LOCK_TIME n|UNBOUNDED` suffix DO
    /// reflect real stored columns, including the bare `PASSWORD EXPIRE` a
    /// `CREATE ROLE` account prints (Go's `CREATE ROLE` writes
    /// `Password_expired='Y'`) -- the divergence noted here before.
    ///
    /// The `IDENTIFIED WITH '<plugin>' AS '<hash>'` clause DOES reflect the
    /// account's real plugin and stored hash, and `ACCOUNT LOCK`/`UNLOCK`
    /// DOES reflect the real `account_locked` flag (shared with `is_role`;
    /// a `CREATE ROLE` account therefore prints `ACCOUNT LOCK` here, matching
    /// Go).
    pub(crate) fn show_create_user_stmt(
        &mut self,
        spec: &tidb_ast::UserSpec,
    ) -> Result<StmtOutput, DriverError> {
        let (user, host) = self.resolve_account(spec)?;
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::Unsupported(
                "SHOW CREATE USER requires a server front end with a privilege registry",
            ));
        };
        if !registry.user_exists(&user, &host) {
            return Err(DriverError::CannotUserRole {
                operation: "SHOW CREATE USER",
                target: format!("'{user}'@'{host}'"),
            });
        }
        let plugin = registry
            .plugin(&user, &host)
            .unwrap_or_else(|| tidb_mysql::consts::AuthNativePassword.to_owned());
        let auth_string = registry.auth_string(&user, &host).unwrap_or_default();
        // Go: `authStr` is empty ONLY for `auth_socket` with no stored data;
        // every other plugin (including a native/sha2/sm3 account with an
        // empty, passwordless hash) still prints ` AS '<possibly empty>'`.
        let auth_clause = if plugin == tidb_mysql::consts::AuthSocket && auth_string.is_empty() {
            String::new()
        } else {
            format!(" AS '{auth_string}'")
        };
        let account_clause = if registry.is_role(&user, &host) {
            "LOCK"
        } else {
            "UNLOCK"
        };
        // Go picks ONE expiry clause from the two columns, in this order
        // (all four captured): `Password_expired='Y'` prints a bare
        // `PASSWORD EXPIRE` whatever the lifetime is, then a zero lifetime
        // prints `NEVER`, then a positive one prints `INTERVAL n DAY`, and a
        // NULL lifetime prints `DEFAULT`.
        let expiry = registry.password_expiry(&user, &host).unwrap_or_default();
        let expire_clause = if expiry.expired {
            "PASSWORD EXPIRE".to_owned()
        } else {
            match expiry.lifetime {
                Some(0) => "PASSWORD EXPIRE NEVER".to_owned(),
                Some(days) if days > 0 => format!("PASSWORD EXPIRE INTERVAL {days} DAY"),
                _ => "PASSWORD EXPIRE DEFAULT".to_owned(),
            }
        };
        // Both suffixes appear together or not at all, because Go reads them
        // from one `Password_locking` object that exists only when at least
        // one of the two options is nonzero (captured:
        // `FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 3` prints both, a plain
        // account prints neither, and `PASSWORD_LOCK_TIME 6` alone still
        // prints ` FAILED_LOGIN_ATTEMPTS 0 PASSWORD_LOCK_TIME 6`).
        let locking_clause = registry
            .password_locking(&user, &host)
            .map(|locking| {
                let lock_time = if locking.password_lock_time_days == -1 {
                    "UNBOUNDED".to_owned()
                } else {
                    locking.password_lock_time_days.to_string()
                };
                format!(
                    " FAILED_LOGIN_ATTEMPTS {} PASSWORD_LOCK_TIME {lock_time}",
                    locking.failed_login_attempts
                )
            })
            .unwrap_or_default();
        let show_str = format!(
            "CREATE USER '{user}'@'{host}' IDENTIFIED WITH '{plugin}'{auth_clause} REQUIRE NONE {expire_clause} ACCOUNT {account_clause} PASSWORD HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT{locking_clause}"
        );
        // Go: `fmt.Sprintf("CREATE USER for %s", s.User)` -- `s.User.String()`
        // is unquoted `user@host` (same shape `SHOW GRANTS`'s header uses).
        Ok(string_column_output(
            &format!("CREATE USER for {user}@{host}"),
            vec![show_str],
        ))
    }

    /// `SET PASSWORD [FOR <account>] = '<password>'`: the same
    /// `authentication_string` write as `ALTER USER ... IDENTIFIED BY`
    /// (captured: both leave the identical `*HEX` value), defaulting to the
    /// session's own account.
    pub(crate) fn set_password_stmt(
        &mut self,
        set_password: &tidb_ast::SetPasswordStmt,
    ) -> Result<StmtOutput, DriverError> {
        if set_password.retain_current_password {
            return Err(DriverError::Unsupported(
                "SET PASSWORD ... RETAIN CURRENT PASSWORD is not supported yet",
            ));
        }
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::Unsupported(
                "SET PASSWORD requires a server front end with a privilege registry",
            ));
        };
        let (user, host) = match &set_password.user {
            Some(spec) => self.resolve_account(spec)?,
            None => self.own_account()?,
        };
        let auth_string = privilege::encode_password(&set_password.password);
        if !registry.set_auth_string(&user, &host, &auth_string) {
            return Err(DriverError::SetPasswordNoMatchingRow);
        }
        // Same UPDATE as `ALTER USER ... IDENTIFIED BY`: a stored password
        // is an unexpired password.
        registry.mark_password_changed(&user, &host);
        self.sandbox_mode = false;
        Ok(StmtOutput::Affected(0))
    }

    /// `RENAME USER <old> TO <new> [, ...]`. Go's `executeRenameUser` moves
    /// the `mysql.user` row -- authentication string included -- along with
    /// every `mysql.db`/`mysql.tables_priv` row keyed by the old identity
    /// (captured: after the rename the new account holds all three scoped
    /// grant lines and the old one reports `ErrNonexistingGrant`), and
    /// reports `ErrCannotUser` for a missing source or an occupied target.
    pub(crate) fn rename_user_stmt(
        &mut self,
        pairs: &[tidb_ast::RenameUserPair],
    ) -> Result<StmtOutput, DriverError> {
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::Unsupported(
                "RENAME USER requires a server front end with a privilege registry",
            ));
        };
        for pair in pairs {
            let (old_user, old_host) = self.resolve_account(&pair.old_user)?;
            let (new_user, new_host) = self.resolve_account(&pair.new_user)?;
            let old_missing = !registry.user_exists(&old_user, &old_host);
            if !registry.rename_user(&old_user, &old_host, &new_user, &new_host) {
                return Err(DriverError::RenameUserFailed {
                    old_user,
                    old_host,
                    new_user,
                    new_host,
                    old_missing,
                });
            }
        }
        Ok(StmtOutput::Affected(0))
    }

    /// Resolves one written account identity, expanding the `CURRENT_USER`
    /// pseudo-user to the session's own identity as Go does.
    pub(crate) fn resolve_account(
        &self,
        spec: &tidb_ast::UserSpec,
    ) -> Result<(String, String), DriverError> {
        if spec.current_user {
            return self.own_account();
        }
        Ok((spec.user.clone(), spec.host.clone()))
    }

    /// The session's own account identity. A session with no authenticated
    /// identity is an in-process one with no front end, which has no account
    /// to name.
    pub(crate) fn own_account(&self) -> Result<(String, String), DriverError> {
        let (user, host) = self.current_identity().ok_or(DriverError::Unsupported(
            "CURRENT_USER requires a session with an authenticated identity",
        ))?;
        Ok((user.to_owned(), host.to_owned()))
    }

    /// `DROP USER` / `DROP ROLE` at the GLOBAL scope this tier models. Go's
    /// `executeDropUser` checks every named account exists BEFORE dropping
    /// any of them, rolling the whole statement back and reporting every
    /// missing account together if one is missing.
    ///
    /// `is_role` selects the operation name the failure message prints and
    /// NOTHING else: Go does not check that the account is really a role, so
    /// `DROP ROLE` on a plain user and `DROP USER` on a role both succeed
    /// (captured). One row, one delete.
    pub(crate) fn drop_user_stmt(
        &mut self,
        is_role: bool,
        if_exists: bool,
        users: &[tidb_ast::UserSpec],
    ) -> Result<StmtOutput, DriverError> {
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::Unsupported(
                "DROP USER requires a server front end with a privilege registry",
            ));
        };
        if !if_exists {
            let missing: Vec<String> = users
                .iter()
                .filter(|spec| !registry.user_exists(&spec.user, &spec.host))
                .map(|spec| format!("{}@{}", spec.user, spec.host))
                .collect();
            if !missing.is_empty() {
                let accounts = missing.join(",");
                return Err(if is_role {
                    DriverError::CannotUserRole {
                        operation: "DROP ROLE",
                        target: accounts,
                    }
                } else {
                    DriverError::DropUserMissing { accounts }
                });
            }
        }
        for spec in users {
            registry.drop_user(&spec.user, &spec.host);
        }
        // A dropped role stops being active in THIS session too; the edge it
        // was activated through is gone, so keeping it would confer
        // privileges from a row that no longer exists.
        self.active_roles.retain(|(role, host)| {
            !users
                .iter()
                .any(|spec| &spec.user == role && &spec.host == host)
        });
        Ok(StmtOutput::Affected(0))
    }

    /// `GRANT <static privs> ON <level> TO <user>... [WITH GRANT OPTION]` --
    /// Go's `grant.go` GLOBAL/DATABASE/TABLE scopes. Roles, dynamic
    /// privileges, and column lists are refused rather than silently
    /// accepted or dropped.
    ///
    /// `WITH GRANT OPTION` is just `mysql.GrantPriv` ORed into the same
    /// scope's privilege mask, which is why it works identically at all
    /// three scopes and why `REVOKE GRANT OPTION ON <level>` (an ordinary
    /// privilege name) clears exactly that scope's bit.
    pub(crate) fn grant_stmt(
        &mut self,
        grant: &tidb_ast::GrantStmt,
    ) -> Result<StmtOutput, DriverError> {
        if grant.object_type.is_some() {
            return Err(DriverError::Unsupported(
                "GRANT ... ON FUNCTION/PROCEDURE is not supported yet",
            ));
        }
        let with_grant = if grant.with_grant {
            privilege::GlobalPriv::GrantOption.bit()
        } else {
            0
        };
        if !grant.tls_options.is_empty() {
            return Err(DriverError::Unsupported(
                "GRANT ... REQUIRE is not supported yet",
            ));
        }
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::Unsupported(
                "GRANT requires a server front end with a privilege registry",
            ));
        };
        match &grant.level {
            tidb_ast::GrantLevel::Global => {
                let (static_mask, dynamic) = self.split_global_privs(&grant.privileges, true)?;
                // Go `containsNonDynamicPriv`: `WITH GRANT OPTION` sets the
                // account's `mysql.user.Grant_priv` only when the statement
                // named at least one NON-dynamic privilege. A grant of
                // dynamic privileges alone records the grant option on each
                // `global_grants` row instead, leaving the account's own
                // `GRANT OPTION` untouched -- "with DYNAMIC privileges the
                // GRANT OPTION is individually grantable, and not a global
                // property of the user".
                let names_static = grant.privileges.iter().any(|privilege| !privilege.dynamic);
                let mask = static_mask | if names_static { with_grant } else { 0 };
                for spec in &grant.users {
                    let user = spec.user.user.as_str();
                    let host = spec.user.host.as_str();
                    // Go's default sql_mode forbids GRANT from implicitly
                    // creating the target account (captured:
                    // `ErrCantCreateUserWithGrant`, 1410).
                    if !registry.user_exists(user, host) {
                        return Err(DriverError::GrantToUnknownUser);
                    }
                    registry.grant(user, host, mask);
                    for name in &dynamic {
                        registry.grant_dynamic(user, host, name, grant.with_grant);
                    }
                }
            }
            tidb_ast::GrantLevel::Database(database) => {
                let database = self.resolve_grant_database(database.as_deref())?;
                let privs = self.resolve_scoped_privs(&grant.privileges, ScopeKind::Database)?;
                let mask = privs.iter().fold(0u64, |mask, priv_| mask | priv_.bit()) | with_grant;
                for spec in &grant.users {
                    let user = spec.user.user.as_str();
                    let host = spec.user.host.as_str();
                    if !registry.user_exists(user, host) {
                        return Err(DriverError::GrantToUnknownUser);
                    }
                    registry.grant_db(user, host, &database, mask);
                }
            }
            tidb_ast::GrantLevel::Table { database, table } => {
                let database = self.resolve_grant_database(database.as_deref())?;
                let TableScopePrivs {
                    table: privs,
                    columns,
                } = self.resolve_table_scope_privs(&grant.privileges)?;
                let mask = privs.iter().fold(0u64, |mask, priv_| mask | priv_.bit()) | with_grant;
                // Go allows granting on a table that does not exist only
                // when the privilege list includes `CREATE` (captured:
                // issues #28533/#29268); otherwise it reports
                // `ErrTableNotExists` (1146).
                let table_exists = self.lock_catalog()?.table_in(&database, table).is_some();
                if !table_exists && !privs.contains(&privilege::GlobalPriv::Create) {
                    return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
                        "{database}.{table}"
                    ))));
                }
                // Go's `checkAndInitColumnPriv` resolves every named column
                // against the table and reports `Unknown column: <name>`
                // when it is absent, so a `GRANT` naming a bad column stores
                // nothing at all (captured). Resolving up front also
                // normalises the spelling to the table's own: `GRANT SELECT
                // (A)` prints back as `SELECT(a)`.
                let columns = self.resolve_grant_columns(&database, table, &columns)?;
                for spec in &grant.users {
                    let user = spec.user.user.as_str();
                    let host = spec.user.host.as_str();
                    if !registry.user_exists(user, host) {
                        return Err(DriverError::GrantToUnknownUser);
                    }
                    // The TABLE row is created even by a column-only grant
                    // (Go's `checkAndInitTablePriv` runs for every TABLE-level
                    // GRANT), which is what later lets `REVOKE` on that table
                    // get past its "no such grant" check.
                    registry.grant_table(user, host, &database, table, mask);
                    for (column, column_mask) in &columns {
                        registry.grant_column(user, host, &database, table, column, *column_mask);
                    }
                }
            }
        }
        Ok(StmtOutput::Affected(0))
    }

    /// `REVOKE <static privs> ON <level> FROM <user>...`. Go's `revoke.go`
    /// requires every named account to already exist (`errors.Errorf("Unknown
    /// user: %s", ...)`, captured); this tier does too.
    pub(crate) fn revoke_stmt(
        &mut self,
        revoke: &tidb_ast::RevokeStmt,
    ) -> Result<StmtOutput, DriverError> {
        if revoke.object_type.is_some() {
            return Err(DriverError::Unsupported(
                "REVOKE ... ON FUNCTION/PROCEDURE is not supported yet",
            ));
        }
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::Unsupported(
                "REVOKE requires a server front end with a privilege registry",
            ));
        };
        // Go's `checkDynamicPrivilegeUsage` runs before any row is touched
        // and names EVERY dynamic privilege in the statement, comma-joined,
        // in the one 3619 it raises.
        if !matches!(revoke.level, tidb_ast::GrantLevel::Global) {
            let dynamic: Vec<String> = revoke
                .privileges
                .iter()
                .filter(|privilege| privilege.dynamic)
                .map(|privilege| privilege.name.to_ascii_uppercase())
                .collect();
            if !dynamic.is_empty() {
                return Err(DriverError::IllegalPrivilegeLevel(dynamic.join(",")));
            }
        }
        match &revoke.level {
            tidb_ast::GrantLevel::Global => {
                let (mask, dynamic) = self.split_global_privs(&revoke.privileges, false)?;
                let revoke_all_dynamic = revoke
                    .privileges
                    .iter()
                    .any(|privilege| privilege.name == "ALL");
                let unregistered: Vec<String> = dynamic
                    .iter()
                    .filter(|name| !privilege::is_dynamic_privilege(name))
                    .cloned()
                    .collect();
                for spec in &revoke.users {
                    let user = spec.user.user.as_str();
                    let host = spec.user.host.as_str();
                    if !registry.user_exists(user, host) {
                        return Err(DriverError::RevokeUnknownUser {
                            user: user.to_owned(),
                            host: host.to_owned(),
                        });
                    }
                    registry.revoke(user, host, mask);
                    if revoke_all_dynamic {
                        registry.revoke_all_dynamic(user, host);
                    }
                    for name in &dynamic {
                        registry.revoke_dynamic(user, host, name);
                    }
                }
                // An unregistered name is a WARNING here, not the error
                // `GRANT` raises for it, and the delete still runs
                // (captured: the statement reports OK with a 3929 warning).
                for name in unregistered {
                    self.warnings.push(SqlWarning {
                        level: WarningLevel::Warning,
                        code: 3929,
                        message: format!(
                            "Dynamic privilege '{name}' is not registered with the server."
                        ),
                    });
                }
            }
            tidb_ast::GrantLevel::Database(database) => {
                let database = self.resolve_grant_database(database.as_deref())?;
                let privs = self.resolve_scoped_privs(&revoke.privileges, ScopeKind::Database)?;
                let mask = privs.iter().fold(0u64, |mask, priv_| mask | priv_.bit());
                for spec in &revoke.users {
                    let user = spec.user.user.as_str();
                    let host = spec.user.host.as_str();
                    if !registry.user_exists(user, host) {
                        return Err(DriverError::RevokeUnknownUser {
                            user: user.to_owned(),
                            host: host.to_owned(),
                        });
                    }
                    if !registry.db_grant_row_exists(user, host, &database) {
                        return Err(DriverError::RevokeNoDbGrant {
                            user: user.to_owned(),
                            host: host.to_owned(),
                            database: database.clone(),
                        });
                    }
                    registry.revoke_db(user, host, &database, mask);
                }
            }
            tidb_ast::GrantLevel::Table { database, table } => {
                let database = self.resolve_grant_database(database.as_deref())?;
                let TableScopePrivs {
                    table: privs,
                    columns,
                } = self.resolve_table_scope_privs(&revoke.privileges)?;
                let mask = privs.iter().fold(0u64, |mask, priv_| mask | priv_.bit());
                for spec in &revoke.users {
                    let user = spec.user.user.as_str();
                    let host = spec.user.host.as_str();
                    if !registry.user_exists(user, host) {
                        return Err(DriverError::RevokeUnknownUser {
                            user: user.to_owned(),
                            host: host.to_owned(),
                        });
                    }
                    // Go checks the TABLE row's existence for every account
                    // BEFORE touching any row and before resolving a column
                    // name, so revoking a nonexistent column from an account
                    // with no grant on the table reports this and not
                    // `Unknown column` (captured).
                    if !registry.table_grant_row_exists(user, host, &database, table) {
                        return Err(DriverError::RevokeNoTableGrant {
                            user: user.to_owned(),
                            host: host.to_owned(),
                            database: database.clone(),
                            table: table.clone(),
                        });
                    }
                }
                // `REVOKE` resolves columns through `table.FindCol` too, but
                // only once the table rows are known to exist.
                let columns = self.resolve_grant_columns(&database, table, &columns)?;
                for spec in &revoke.users {
                    let user = spec.user.user.as_str();
                    let host = spec.user.host.as_str();
                    registry.revoke_table(user, host, &database, table, mask);
                    for (column, column_mask) in &columns {
                        registry.revoke_column(user, host, &database, table, column, *column_mask);
                    }
                }
            }
        }
        Ok(StmtOutput::Affected(0))
    }

    /// Resolves a DB/TABLE-scope `GRANT`/`REVOKE`'s database qualifier: the
    /// written name, or (Go's `getTargetSchemaName`) the session's current
    /// database when the statement wrote a bare `*`/table name.
    pub(crate) fn resolve_grant_database(
        &self,
        database: Option<&str>,
    ) -> Result<String, DriverError> {
        match database {
            Some(database) => Ok(database.to_owned()),
            None if !self.current_db.is_empty() => Ok(self.current_db.clone()),
            None => Err(DriverError::Schema(SchemaErrorKind::NoDatabaseSelected)),
        }
    }

    /// Resolves a TABLE-scope `GRANT`/`REVOKE` privilege list, splitting the
    /// privileges that carry a column list from those that do not.
    ///
    /// Go validates each element against the scope its column list selects:
    /// without one, `mysql.AllTablePrivs` (see [`Self::resolve_scoped_privs`]);
    /// with one, `mysql.AllColumnPrivs` -- `SELECT`, `INSERT`, `UPDATE`,
    /// `REFERENCES` and `ALL`/`USAGE` only. Everything else, `GRANT OPTION`
    /// included, is the captured `ErrWrongUsage`/1221 "Incorrect usage of
    /// COLUMN GRANT and NON-COLUMN PRIVILEGES". `ALL (col)` expands to all
    /// four; `USAGE (col)` contributes no privilege at all.
    fn resolve_table_scope_privs(
        &self,
        privileges: &[tidb_ast::GrantPrivilege],
    ) -> Result<TableScopePrivs, DriverError> {
        let mut result = TableScopePrivs::default();
        let without_columns: Vec<tidb_ast::GrantPrivilege> = privileges
            .iter()
            .filter(|privilege| privilege.columns.is_empty())
            .cloned()
            .collect();
        result.table = self.resolve_scoped_privs(&without_columns, ScopeKind::Table)?;
        for privilege in privileges {
            if privilege.columns.is_empty() {
                continue;
            }
            let mask = match privilege.name.as_str() {
                "ALL" => privilege::all_column_privs_mask(),
                "USAGE" => 0,
                _ => {
                    let priv_ = privilege::GlobalPriv::from_grant_name(&privilege.name)
                        .filter(|priv_| priv_.is_valid_at_column_scope())
                        .ok_or(DriverError::ColumnGrantNonColumnPriv)?;
                    priv_.bit()
                }
            };
            for column in &privilege.columns {
                match result
                    .columns
                    .iter_mut()
                    .find(|(named, _)| named.eq_ignore_ascii_case(column))
                {
                    Some((_, existing)) => *existing |= mask,
                    None => result.columns.push((column.clone(), mask)),
                }
            }
        }
        Ok(result)
    }

    /// Resolves each written column name against the table, returning the
    /// table's own spelling of it (Go's `table.FindCol`, which matches on the
    /// lowercased name -- captured: `GRANT SELECT (A)` prints back as
    /// `SELECT(a)`). A name the table does not have is Go's plain
    /// `Unknown column: <name>`.
    fn resolve_grant_columns(
        &self,
        database: &str,
        table: &str,
        columns: &[(String, u64)],
    ) -> Result<Vec<(String, u64)>, DriverError> {
        if columns.is_empty() {
            return Ok(Vec::new());
        }
        let catalog = self.lock_catalog()?;
        let names = catalog
            .table_in(database, table)
            .map(tidb_executor::TableEntry::column_names)
            .unwrap_or_default();
        columns
            .iter()
            .map(|(column, mask)| {
                names
                    .iter()
                    .find(|name| name.eq_ignore_ascii_case(column))
                    .map(|name| (name.clone(), *mask))
                    .ok_or_else(|| DriverError::UnknownGrantColumn(column.clone()))
            })
            .collect()
    }

    /// Resolves a `GRANT`/`REVOKE` privilege list at DB or TABLE scope,
    /// validating that every privilege is one Go's `mysql.AllDBPrivs`/
    /// `mysql.AllTablePrivs` allows there. `ALL [PRIVILEGES]` expands to
    /// every privilege valid at that scope. A global-only privilege at DB
    /// scope is refused with the captured `ErrWrongUsage`/1221; any
    /// privilege outside the TABLE-scope set is refused with the captured
    /// `ErrIllegalGrantForTable`/1144 (Go checks the TABLE-scope validity
    /// before the table-existence check, so this runs first here too).
    pub(crate) fn resolve_scoped_privs(
        &self,
        privileges: &[tidb_ast::GrantPrivilege],
        scope: ScopeKind,
    ) -> Result<Vec<privilege::GlobalPriv>, DriverError> {
        let all_scoped: &[privilege::GlobalPriv] = match scope {
            ScopeKind::Database => privilege::ALL_DB_PRIVS,
            ScopeKind::Table => privilege::ALL_TABLE_PRIVS,
        };
        let mut result = Vec::new();
        for privilege in privileges {
            if privilege.name == "ALL" {
                result.extend_from_slice(all_scoped);
                continue;
            }
            if !privilege.columns.is_empty() {
                return Err(DriverError::Unsupported(
                    "GRANT/REVOKE with a column list is not supported yet",
                ));
            }
            // A DYNAMIC privilege is refused for being at the wrong LEVEL
            // before anything asks whether it is registered, so an
            // unregistered name outside `*.*` reports 3619 and not 3929
            // (Go: `grantDynamicPriv`'s level check precedes its registry
            // check; `REVOKE`'s `checkDynamicPrivilegeUsage` runs even
            // earlier).
            if privilege.dynamic {
                return Err(DriverError::IllegalPrivilegeLevel(privilege.name.clone()));
            }
            let Some(priv_) = privilege::GlobalPriv::from_grant_name(&privilege.name) else {
                return Err(DriverError::DynamicPrivilegeNotRegistered(
                    privilege.name.clone(),
                ));
            };
            let valid = match scope {
                ScopeKind::Database => priv_.is_valid_at_db_scope(),
                ScopeKind::Table => priv_.is_valid_at_table_scope(),
            };
            if !valid {
                return Err(match scope {
                    ScopeKind::Database => DriverError::DbGrantGlobalOnlyPriv,
                    ScopeKind::Table => DriverError::IllegalGrantForTable,
                });
            }
            result.push(priv_);
        }
        Ok(result)
    }

    /// Resolves a `GRANT`/`REVOKE` privilege list to the bitmask this tier's
    /// registry stores. `ALL [PRIVILEGES]` expands to every modeled global
    /// privilege (Go: `mysql.AllGlobalPrivs`, minus the roles/GRANT OPTION
    /// this tier does not model). A name that is not one of the standard
    /// privileges this tier recognizes is refused with the same error Go
    /// raises for an unregistered dynamic privilege (captured: 3929),
    /// because `tidb-parser` accepts any bare identifier there through its
    /// `ExtendedPriv`/dynamic-privilege grammar branch.
    /// Splits a GLOBAL-scope privilege list into the static bitmask and the
    /// DYNAMIC privilege names, which live in different tables
    /// (`mysql.user.Priv` vs `mysql.global_grants`) and so are applied
    /// separately.
    ///
    /// `ALL [PRIVILEGES]` expands to the static mask only: Go's `GRANT ALL`
    /// never confers a dynamic privilege. (`REVOKE ALL` DOES clear them, but
    /// through its own unqualified delete rather than through this list --
    /// see [`privilege::PrivilegeRegistry::revoke_all_dynamic`].)
    ///
    /// `reject_unregistered` distinguishes the two consumers: `GRANT` fails
    /// with `ErrDynamicPrivilegeNotRegistered`/3929 on an unknown name,
    /// while `REVOKE` only WARNS with the same error and proceeds, so it
    /// asks for the names unfiltered and warns itself.
    pub(crate) fn split_global_privs(
        &self,
        privileges: &[tidb_ast::GrantPrivilege],
        reject_unregistered: bool,
    ) -> Result<(u64, Vec<String>), DriverError> {
        let mut mask = 0u64;
        let mut dynamic = Vec::new();
        for privilege in privileges {
            if privilege.name == "ALL" {
                mask |= privilege::all_privs_mask();
                continue;
            }
            if !privilege.columns.is_empty() {
                return Err(DriverError::Unsupported(
                    "GRANT/REVOKE with a column list is not supported yet",
                ));
            }
            if privilege.dynamic {
                if reject_unregistered && !privilege::is_dynamic_privilege(&privilege.name) {
                    return Err(DriverError::DynamicPrivilegeNotRegistered(
                        privilege.name.clone(),
                    ));
                }
                dynamic.push(privilege.name.to_ascii_uppercase());
                continue;
            }
            match privilege::GlobalPriv::from_grant_name(&privilege.name) {
                Some(priv_) => mask |= priv_.bit(),
                None => {
                    return Err(DriverError::DynamicPrivilegeNotRegistered(
                        privilege.name.clone(),
                    ));
                }
            }
        }
        Ok((mask, dynamic))
    }

    /// `SHOW GRANTS [FOR <account> [USING <role>...]]`.
    ///
    /// Which roles' privileges are folded in depends on the form (Go passes
    /// exactly one role list to `showGrants`): the bare form and
    /// `FOR CURRENT_USER` use the session's ACTIVE roles, `USING` names the
    /// roles explicitly, and `SHOW GRANTS FOR <someone else>` folds in
    /// nothing (captured: root's `SHOW GRANTS FOR 'u1'@'%'` omits the
    /// database line that u1's own `SHOW GRANTS` shows through its active
    /// role).
    pub(crate) fn show_grants_stmt(
        &mut self,
        show: &tidb_ast::ShowGrantsStmt,
    ) -> Result<StmtOutput, DriverError> {
        let is_own = match &show.user {
            None => true,
            Some(spec) => {
                spec.current_user
                    || self
                        .current_identity()
                        .is_some_and(|(user, host)| spec.user == user && spec.host == host)
            }
        };
        let (user, host) = match &show.user {
            None => {
                let Some((user, host)) = self.current_identity() else {
                    return Err(DriverError::Unsupported(
                        "SHOW GRANTS requires an authenticated session",
                    ));
                };
                (user.to_owned(), host.to_owned())
            }
            Some(spec) if spec.current_user => {
                let Some((user, host)) = self.current_identity() else {
                    return Err(DriverError::Unsupported(
                        "SHOW GRANTS requires an authenticated session",
                    ));
                };
                (user.to_owned(), host.to_owned())
            }
            Some(spec) => (spec.user.clone(), spec.host.clone()),
        };
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::Unsupported(
                "SHOW GRANTS requires a server front end with a privilege registry",
            ));
        };
        let roles = if show.roles.is_empty() {
            if is_own {
                self.active_roles.clone()
            } else {
                Vec::new()
            }
        } else {
            // `USING` names roles through the account grammar, so the same
            // omitted-host default applies.
            show.roles
                .iter()
                .map(|spec| {
                    let host = if spec.host.is_empty() {
                        "%".to_owned()
                    } else {
                        spec.host.clone()
                    };
                    (spec.user.clone(), host)
                })
                .collect()
        };
        let Some(lines) = registry.show_grants(&user, &host, &roles) else {
            return Err(DriverError::NonexistingGrant { user, host });
        };
        // Go: `fmt.Sprintf("Grants for %s", s.User)` -- `s.User.String()` is
        // unquoted `user@host`. One row per GLOBAL/DB/TABLE-scope line, in
        // that order (`registry.show_grants`'s captured ordering).
        Ok(string_column_output(
            &format!("Grants for {user}@{host}"),
            lines.split('\n').map(str::to_owned).collect(),
        ))
    }
}
