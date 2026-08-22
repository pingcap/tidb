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
use tidb_util::password_validation::{self, GlobalVarAccessor, PasswordUser, PwdError};

struct SessionPasswordGlobals<'a>(&'a SessionVars);

impl GlobalVarAccessor for SessionPasswordGlobals<'_> {
    type Error = VarError;

    fn get_global_sys_var(&self, name: &str) -> Result<String, Self::Error> {
        self.0.get_global(name)
    }
}

fn password_validation_error(error: PwdError<VarError>) -> DriverError {
    match error {
        PwdError::Accessor(error) => crate::variables::var_error(error),
        PwdError::ParseInt(error) => DriverError::Exec(tidb_executor::ExecError::internal(
            format!("invalid validate_password numeric setting: {error}"),
        )),
        PwdError::NotValid(reason) => DriverError::NotValidPassword { reason },
    }
}

fn identity_username(identity: &str) -> &str {
    identity.rsplit_once('@').map_or(identity, |(user, _)| user)
}

/// Which non-global `GRANT`/`REVOKE` scope a privilege list is being
/// validated against -- selects between Go's `mysql.AllDBPrivs` and
/// `mysql.AllTablePrivs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScopeKind {
    /// `ON db.*`.
    Database,
    /// `ON db.t`.
    Table,
}

/// A TABLE-scope `GRANT`/`REVOKE` privilege list split into the two kinds of
/// row it writes: the `mysql.Tables_priv` privileges (those written without a
/// column list) and the `mysql.Columns_priv` ones. One statement may carry
/// both (captured: `GRANT SELECT, INSERT (a), UPDATE ON cg.t` writes a table
/// row of `SELECT,UPDATE` and a column row of `INSERT` on `a`, and
/// `SHOW GRANTS` prints them as two lines).
#[derive(Debug, Default)]
struct TableScopePrivs {
    /// The privileges written without a column list.
    table: Vec<privilege::GlobalPriv>,
    /// `(column, mask)` in the order the columns were first named, merged so
    /// that a column named by several privileges appears once.
    columns: Vec<(String, u64)>,
}

/// Go's `passwordOrLockOptionsInfo` after `loadOptions`: the account-state
/// changes ONE `CREATE`/`ALTER USER` statement's `PASSWORD ...` /
/// `ACCOUNT ...` / `FAILED_LOGIN_ATTEMPTS` / `PASSWORD_LOCK_TIME` clauses
/// add up to. Every field is `None` when the statement wrote no clause of
/// that kind, which is the distinction `ALTER USER` needs: an unwritten
/// option keeps the account's current value rather than resetting it.
#[derive(Default)]
pub(crate) struct PasswordOrLockOptions {
    /// `ACCOUNT LOCK` / `ACCOUNT UNLOCK`.
    pub(crate) locked: Option<bool>,
    /// `FAILED_LOGIN_ATTEMPTS n`, clamped as Go clamps it.
    failed_login_attempts: Option<i64>,
    /// `PASSWORD_LOCK_TIME n | UNBOUNDED`; `UNBOUNDED` is `-1`.
    password_lock_time_days: Option<i64>,
    /// `PASSWORD EXPIRE [DEFAULT | NEVER | INTERVAL n DAY]`.
    pub(crate) expire: Option<privilege::PasswordExpireSetting>,
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
                    return Err(DriverError::unsupported(
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
    pub(crate) fn apply(&self, registry: &privilege::PrivilegeRegistry, user: &str, host: &str) {
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

/// Go `executor/grant.go`'s `tlsOption2GlobalPriv` (around line 431): the
/// `ssl_type` a `REQUIRE` clause list resolves to. Go folds a LIST, with the
/// LAST option of each kind winning, and starts from `SslTypeNotSpecified`
/// -- which stores and admits identically to `NONE`.
///
/// REFUSED, by name rather than silently accepted: `X509`, `CIPHER`,
/// `ISSUER`, `SUBJECT`, `SAN`. Each demands a VERIFIED CLIENT CERTIFICATE
/// CHAIN (Go's `checkSSL` reads `tlsState.VerifiedChains`), and this
/// server's TLS is configured `with_no_client_auth()` -- it never requests a
/// client certificate, so it can never have one to verify. Storing the
/// requirement anyway would leave an account that Go refuses on this
/// transport being ADMITTED here over ordinary TLS, which is the fail-OPEN
/// direction. `TOKEN_ISSUER` is refused for the same reason one level over:
/// it belongs to `tidb_auth_token`, whose login this tier does not serve.
pub(crate) fn ssl_type_of(
    tls_options: &[tidb_ast::AlterUserTlsOption],
) -> Result<privilege::SslType, DriverError> {
    let mut ssl_type = privilege::SslType::None;
    for option in tls_options {
        ssl_type = match option {
            tidb_ast::AlterUserTlsOption::None => privilege::SslType::None,
            tidb_ast::AlterUserTlsOption::Ssl => privilege::SslType::Any,
            other => {
                let clause = match other {
                    tidb_ast::AlterUserTlsOption::X509 => "X509",
                    tidb_ast::AlterUserTlsOption::Cipher(_) => "CIPHER",
                    tidb_ast::AlterUserTlsOption::Issuer(_) => "ISSUER",
                    tidb_ast::AlterUserTlsOption::Subject(_) => "SUBJECT",
                    tidb_ast::AlterUserTlsOption::San(_) => "SAN",
                    tidb_ast::AlterUserTlsOption::TokenIssuer(_) => "TOKEN_ISSUER",
                    tidb_ast::AlterUserTlsOption::None | tidb_ast::AlterUserTlsOption::Ssl => {
                        unreachable!("handled above")
                    }
                };
                return Err(DriverError::unsupported(format!(
                    "REQUIRE {clause} needs a verified client certificate, which this server \
                     does not request; only REQUIRE NONE and REQUIRE SSL are supported"
                )));
            }
        };
    }
    Ok(ssl_type)
}

impl Session {
    pub(crate) fn validate_password_if_enabled(&self, password: &str) -> Result<(), DriverError> {
        let globals = SessionPasswordGlobals(&self.vars);
        if !password_validation::validation_enabled(&globals).map_err(password_validation_error)? {
            return Ok(());
        }
        let user = self.current_user.as_deref().map(|current| PasswordUser {
            auth_username: identity_username(current),
            username: self
                .login_user
                .as_deref()
                .map(identity_username)
                .unwrap_or_else(|| identity_username(current)),
        });
        password_validation::validate_password(password, user, &globals)
            .map_err(password_validation_error)
    }

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
    /// Go's `executeCreateUser` gate (`executor/simple.go` around line 1051):
    /// `INSERT` on `mysql.user`, else the global `CREATE USER` privilege.
    ///
    /// The role form accepts `CreateRolePriv` as well (`simple.go` line
    /// 1060), and it is a real `mysql.user` column (`Create_role_priv`), so
    /// it is a privilege here like any other. Measured: an account holding
    /// only `GRANT CREATE ROLE ON *.*` creates a ROLE and is refused
    /// `CREATE USER` with `CREATE User`.
    ///
    /// The privilege text is Go's own argument VERBATIM, capitalization
    /// quirk included: `CREATE USER` reports `CREATE User`.
    fn require_create_user_privilege(&self, is_role: bool) -> Result<(), DriverError> {
        if self.has_scoped_privilege(
            tidb_mysql::consts::SystemDB,
            tidb_mysql::consts::UserTable,
            privilege::GlobalPriv::Insert,
        ) || self.has_scoped_privilege("", "", privilege::GlobalPriv::CreateUser)
            || (is_role && self.has_scoped_privilege("", "", privilege::GlobalPriv::CreateRole))
        {
            return Ok(());
        }
        Err(DriverError::SpecificAccessDenied(
            if is_role {
                "CREATE ROLE or CREATE USER"
            } else {
                "CREATE User"
            }
            .to_owned(),
        ))
    }

    /// Go's `executeDropUser` gate (`executor/simple.go` around line 2519):
    /// `DELETE` on `mysql.user`, else the global `CREATE USER` privilege, and
    /// for the role form `DROP ROLE` as well (measured: an account holding
    /// only `GRANT DROP ROLE ON *.*` drops a role and is refused
    /// `DROP USER`).
    fn require_drop_user_privilege(&self, is_role: bool) -> Result<(), DriverError> {
        if self.has_scoped_privilege(
            tidb_mysql::consts::SystemDB,
            tidb_mysql::consts::UserTable,
            privilege::GlobalPriv::Delete,
        ) || self.has_scoped_privilege("", "", privilege::GlobalPriv::CreateUser)
            || (is_role && self.has_scoped_privilege("", "", privilege::GlobalPriv::DropRole))
        {
            return Ok(());
        }
        Err(DriverError::SpecificAccessDenied(
            if is_role {
                "DROP ROLE or CREATE USER"
            } else {
                "CREATE USER"
            }
            .to_owned(),
        ))
    }

    /// Go's shared `SYSTEM_USER` guard on `DROP USER`/`ALTER USER`
    /// (`executor/simple.go` around lines 2563 and 1958): an account that
    /// itself holds `SYSTEM_USER` may only be modified by a caller who holds
    /// `SYSTEM_USER` (or `RESTRICTED_USER_ADMIN`) too. Because SUPER is the
    /// fallback for every dynamic privilege, this reads as "only a SUPER may
    /// touch a SUPER", which is why Go's message names both.
    pub(crate) fn require_system_user_privilege_over(
        &self,
        user: &str,
        host: &str,
    ) -> Result<(), DriverError> {
        if self.privilege_checks_bypassed()
            || self.privileges.is_none()
            || self.current_identity().is_none()
        {
            return Ok(());
        }
        if self.has_dynamic_privilege("SYSTEM_USER", false)
            || self.has_dynamic_privilege("RESTRICTED_USER_ADMIN", false)
        {
            return Ok(());
        }
        if self.target_has_dynamic_privilege(user, host, "SYSTEM_USER") {
            return Err(DriverError::SpecificAccessDenied(
                "SYSTEM_USER or SUPER".to_owned(),
            ));
        }
        Ok(())
    }

    /// Go's `GRANT`/`REVOKE` gate: "to GRANT, you must have the privileges
    /// you are granting, plus the GRANT OPTION"
    /// (`planner/core/planbuilder.go`'s `collectVisitInfoFromGrantStmt`
    /// around line 3946 and `collectVisitInfoFromRevokeStmt` around line
    /// 3878, checked by `optimizer.go`'s `CheckPrivilege`).
    ///
    /// Every named STATIC privilege is verified at the statement's own scope,
    /// then `GRANT OPTION` at that same scope when the statement named any
    /// static privilege at all. A DYNAMIC privilege is instead verified as
    /// itself WITH its own grant option, which is why a dynamic-only
    /// statement never consults the account's global `GRANT OPTION`.
    ///
    /// The two statements report a denial differently: `GRANT` names the
    /// missing `GRANT OPTION` for a dynamic privilege (1227), while every
    /// other denial -- and every `REVOKE` denial -- carries no
    /// statement-specific error and falls to Go's generic
    /// `ErrPrivilegeCheckFail` (8121).
    ///
    /// One deliberate divergence: an explicit privilege LIST is verified in
    /// `privilege::ALL_GLOBAL_PRIVS` order rather than in written order,
    /// because the caller has already folded the list into a mask. That
    /// changes only WHICH privilege a denial names when several are missing,
    /// never whether the statement is denied, and it matches Go exactly for
    /// `ALL PRIVILEGES` (whose expansion is that same list).
    fn require_grant_privileges(
        &self,
        database: &str,
        table: &str,
        static_mask: u64,
        dynamic: &[String],
        is_grant: bool,
    ) -> Result<(), DriverError> {
        let denied = |priv_: privilege::GlobalPriv| {
            DriverError::PrivilegeCheckFail(priv_.check_fail_name().to_owned())
        };
        for priv_ in privilege::ALL_GLOBAL_PRIVS {
            if static_mask & priv_.bit() != 0 && !self.has_scoped_privilege(database, table, *priv_)
            {
                return Err(denied(*priv_));
            }
        }
        for name in dynamic {
            if !self.has_dynamic_privilege(name, true) {
                return Err(if is_grant {
                    DriverError::SpecificAccessDenied("GRANT OPTION".to_owned())
                } else {
                    // Go interpolates the `[]string` of dynamic privileges,
                    // so its message keeps the slice brackets.
                    DriverError::PrivilegeCheckFail(format!("[{name}]"))
                });
            }
        }
        // Go appends the scope's `GRANT OPTION` LAST, and only when the
        // statement named at least one non-dynamic privilege.
        if static_mask != 0
            && !self.has_scoped_privilege(database, table, privilege::GlobalPriv::GrantOption)
        {
            return Err(denied(privilege::GlobalPriv::GrantOption));
        }
        Ok(())
    }

    /// Whether `(user, host)` is the account this session authenticated as --
    /// Go's `alterCurrentUser`, which keys on the AUTHENTICATED identity so a
    /// statement naming that account explicitly is still self-service.
    pub(crate) fn is_own_account(&self, user: &str, host: &str) -> bool {
        self.current_identity() == Some((user, host))
    }

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
        if !resource_options.is_empty() || resource_group.is_some() {
            return Err(DriverError::unsupported(
                "CREATE USER options beyond the account list are not supported yet",
            ));
        }
        let ssl_type = ssl_type_of(tls_options)?;
        // Go `executeCreateUser`'s `userAttributes`: a COMMENT clause is
        // wrapped as `{"metadata": {"comment": "<text>"}}`, an ATTRIBUTE
        // clause embeds the caller's JSON as `{"metadata": <json>}`, and a
        // statement with neither stores the literal empty object `'{}'` --
        // NOT NULL (captured: a fresh account's `User_attributes` prints
        // `{}`; only the bootstrap root row is NULL).
        let user_attributes_json = match comment_or_attribute {
            None => "{}".to_owned(),
            Some(tidb_ast::CreateUserCommentOrAttribute::Comment(text)) => {
                format!("{{\"metadata\": {{\"comment\": \"{text}\"}}}}")
            }
            Some(tidb_ast::CreateUserCommentOrAttribute::Attribute(json)) => {
                format!("{{\"metadata\": {json}}}")
            }
        };
        // Go validates every statement-level option BEFORE writing any row,
        // so a bad `PASSWORD EXPIRE INTERVAL 0 DAY` creates no account.
        let options = PasswordOrLockOptions::load(password_options)?;
        self.require_create_user_privilege(false)?;
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::unsupported(
                "CREATE USER requires a server front end with a privilege registry",
            ));
        };
        let default_plugin = self
            .vars
            .get_global("default_authentication_plugin")
            .map_err(crate::variables::var_error)?;
        if !privilege::is_create_user_plugin(&default_plugin) {
            return Err(DriverError::PluginIsNotLoaded {
                plugin: default_plugin,
            });
        }
        for spec in users {
            // TiDB's grammar rejects RETAIN/DISCARD on CREATE USER with 1064
            // (the corpus asserts it), and this tier's parser does too, so
            // this arm is a belt matching Go `executeCreateUser`'s own guard
            // for AST-built statements.
            if spec.dual_password.is_some() {
                return Err(DriverError::unsupported(
                    "RETAIN CURRENT PASSWORD / DISCARD OLD PASSWORD clause is not supported in \
                     CREATE USER statement",
                ));
            }
            let user = spec.user.user.as_str();
            let host = spec.user.host.as_str();
            if registry.user_exists(user, host) {
                if !if_not_exists {
                    return Err(DriverError::CreateUserAlreadyExists {
                        user: user.to_owned(),
                        host: host.to_owned(),
                    });
                }
                continue;
            }
            let (validation_plugin, validation_text) = match spec.auth.as_ref() {
                None => (default_plugin.as_str(), ""),
                Some(tidb_ast::CreateUserAuth::By(password)) => {
                    (default_plugin.as_str(), password.as_str())
                }
                Some(tidb_ast::CreateUserAuth::With { plugin, credential }) => (
                    plugin.as_str(),
                    match credential {
                        None => "",
                        Some(tidb_ast::CreateUserCredential::By(password))
                        | Some(tidb_ast::CreateUserCredential::As(password)) => password.as_str(),
                    },
                ),
            };
            if tidb_mysql::is_auth_plugin_clear_text(validation_plugin) {
                self.validate_password_if_enabled(validation_text)?;
            }
            let (auth_string, plugin) =
                Self::resolve_auth_string_and_plugin(spec.auth.as_ref(), &default_plugin)?;
            // Go processes each account in source order and fails on the
            // FIRST duplicate rather than batching, unlike DROP USER below.
            if registry.create_user_with_plugin(user, host, &auth_string, &plugin) {
                options.apply(&registry, user, host);
                registry.set_ssl_type(user, host, ssl_type);
                // The `mysql.user` row Go's INSERT writes for this account.
                // Column values come from the statement's own clauses, the
                // way Go's `plOptions` feeds the VALUES list.
                let (password_expired, password_lifetime) = match options.expire {
                    Some(privilege::PasswordExpireSetting::Now) => (true, None),
                    Some(privilege::PasswordExpireSetting::Never) => (false, Some(0)),
                    Some(privilege::PasswordExpireSetting::Interval(days)) => (false, Some(days)),
                    Some(privilege::PasswordExpireSetting::Default) | None => (false, None),
                };
                let (user, host) = (user.to_owned(), host.to_owned());
                self.mirror_create_user_row(
                    &user,
                    &host,
                    &auth_string,
                    &plugin,
                    &user_attributes_json,
                    matches!(options.locked, Some(true)),
                    password_expired,
                    password_lifetime,
                )?;
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
        self.require_create_user_privilege(true)?;
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::unsupported(
                "CREATE ROLE requires a server front end with a privilege registry",
            ));
        };
        // Go `executeCreateUser` (reached with `IsCreateRole`) resolves the
        // row's plugin from `default_authentication_plugin` exactly as it
        // does for a user with no `IDENTIFIED WITH`.
        let default_plugin = self
            .vars
            .get_global("default_authentication_plugin")
            .unwrap_or_else(|_| tidb_mysql::consts::AuthNativePassword.to_owned());
        for spec in roles {
            let (role, host) = role_identity(spec);
            if registry.create_role(&role, &host) {
                // The same INSERT as CREATE USER's, with `IsCreateRole`'s
                // overrides: `Account_locked='Y'`, `Password_expired='Y'`,
                // empty password, `'{}'` attributes.
                self.mirror_create_user_row(
                    &role,
                    &host,
                    "",
                    &default_plugin,
                    "{}",
                    true,
                    true,
                    None,
                )?;
            } else if !if_not_exists {
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
        // Go `planbuilder.go`'s `*ast.GrantRoleStmt` case (around line 3775):
        // the DYNAMIC `ROLE_ADMIN`, whose SUPER fallback is why the message
        // names both.
        if !self.has_dynamic_privilege("ROLE_ADMIN", false) {
            return Err(DriverError::SpecificAccessDenied(
                "SUPER or ROLE_ADMIN".to_owned(),
            ));
        }
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::unsupported(
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
        // Go `planbuilder.go`'s `*ast.RevokeRoleStmt` case (around line 3783),
        // the same `ROLE_ADMIN` gate `GRANT <role>` uses.
        if !self.has_dynamic_privilege("ROLE_ADMIN", false) {
            return Err(DriverError::SpecificAccessDenied(
                "SUPER or ROLE_ADMIN".to_owned(),
            ));
        }
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::unsupported(
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
        if self.privilege_checks_bypassed() {
            return Ok(StmtOutput::Affected(0));
        }
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::unsupported(
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
            return Err(DriverError::unsupported(
                "SET DEFAULT ROLE requires a server front end with a privilege registry",
            ));
        };
        let sole_current = set_default.users.len() == 1
            && set_default
                .users
                .first()
                .zip(self.current_identity())
                .is_some_and(|(spec, current)| spec.user == current.0 && spec.host == current.1);
        // Go `executeSetDefaultRole` (`executor/simple.go` around line 445):
        // setting one's OWN default roles needs no privilege at all;
        // anything else needs `UPDATE` on `mysql.default_roles`, else the
        // global `CREATE USER` privilege.
        if !sole_current
            && !self.has_scoped_privilege(
                tidb_mysql::consts::SystemDB,
                tidb_mysql::consts::DefaultRoleTable,
                privilege::GlobalPriv::Update,
            )
            && !self.has_scoped_privilege("", "", privilege::GlobalPriv::CreateUser)
        {
            return Err(DriverError::SpecificAccessDenied("CREATE USER".to_owned()));
        }
        // Only after the authorization gate does Go disclose whether a
        // non-current target or explicit role exists.
        //
        // Two storage-only paths do not require a `mysql.user` cache row:
        // NONE in the ordinary deployment mode, and every form targeting the
        // sole current user by its written, explicit identity. Go deliberately
        // keeps the CURRENT_USER AST pseudo-user's empty username/hostname in
        // this statement: NONE deletes `@` as a no-op, while ALL/regular fail
        // the ordinary target-existence check for `@`.
        let accounts = if sole_current
            || matches!(&set_default.selection, tidb_ast::DefaultRoleSelection::None)
        {
            set_default
                .users
                .iter()
                .map(|spec| (spec.user.clone(), spec.host.clone()))
                .collect()
        } else {
            set_default
                .users
                .iter()
                .map(|spec| {
                    if registry.user_exists(&spec.user, &spec.host) {
                        Ok((spec.user.clone(), spec.host.clone()))
                    } else {
                        Err(DriverError::CannotUserRole {
                            operation: "SET DEFAULT ROLE",
                            target: format!("{}@{}", spec.user, spec.host),
                        })
                    }
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        if matches!(
            &set_default.selection,
            tidb_ast::DefaultRoleSelection::Roles(_)
        ) && !sole_current
        {
            let tidb_ast::DefaultRoleSelection::Roles(roles) = &set_default.selection else {
                unreachable!("the selection was matched above")
            };
            for role in roles {
                let (role, host) = role_identity(role);
                if !registry.user_exists(&role, &host) {
                    return Err(DriverError::CannotUserRole {
                        operation: "SET DEFAULT ROLE",
                        target: format!("`{role}`@`{host}`"),
                    });
                }
            }
        }
        // Go performs this replacement in one transaction. Resolve every
        // account's complete replacement first so a later 3530 cannot publish
        // an earlier account's default roles before the statement fails.
        let mut replacements = Vec::with_capacity(accounts.len());
        for account in &accounts {
            let roles = match &set_default.selection {
                tidb_ast::DefaultRoleSelection::None => Vec::new(),
                tidb_ast::DefaultRoleSelection::All => registry.granted_roles(account),
                tidb_ast::DefaultRoleSelection::Roles(roles) => {
                    // Go's skip-grant manager makes `FindEdge` return false:
                    // an explicit role therefore reports 3530 even when the
                    // storage row exists. `ALL` and `NONE` remain storage
                    // operations and deliberately take the arms above.
                    if self.privilege_checks_bypassed() {
                        let role = roles
                            .first()
                            .map(role_identity)
                            .expect("the parser requires at least one explicit default role");
                        return Err(DriverError::RoleNotGranted {
                            role: role.0,
                            role_host: role.1,
                            user: account.0.clone(),
                            host: account.1.clone(),
                        });
                    }
                    self.granted_roles_or_error(&registry, account, roles)?
                }
            };
            replacements.push(roles);
        }
        registry.replace_default_roles(
            accounts
                .iter()
                .zip(&replacements)
                .map(|(account, roles)| (account, roles.as_slice())),
        );
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
            return Err(DriverError::unsupported(
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
            return Err(DriverError::unsupported(
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
    /// `IDENTIFIED` clause uses the live `default_authentication_plugin` and
    /// an empty authentication string, as Go's account executor does.
    pub(crate) fn resolve_auth_string_and_plugin(
        auth: Option<&tidb_ast::CreateUserAuth>,
        default_plugin: &str,
    ) -> Result<(String, String), DriverError> {
        match auth {
            None => Ok((String::new(), default_plugin.to_owned())),
            Some(tidb_ast::CreateUserAuth::By(password)) => Ok((
                privilege::encode_password_for_plugin(
                    default_plugin,
                    &privilege::PluginCredential::By(password),
                )?,
                default_plugin.to_owned(),
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
        // Go `planbuilder.go`'s `*ast.RenameUserStmt` case (around line 3743).
        if !self.has_scoped_privilege("", "", privilege::GlobalPriv::CreateUser) {
            return Err(DriverError::SpecificAccessDenied("CREATE USER".to_owned()));
        }
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::unsupported(
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
            // Go `renameUserHostInSystemTable` on `mysql.user`: the row moves
            // with the account, authentication string included.
            self.mirror_rename_user_row(&old_user, &old_host, &new_user, &new_host)?;
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
        let (user, host) = self.current_identity().ok_or(DriverError::unsupported(
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
        self.require_drop_user_privilege(is_role)?;
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::unsupported(
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
        // Go rolls the whole statement back when ONE target turns out to be
        // a `SYSTEM_USER`, so the guard has to clear every target before the
        // first delete rather than inside the delete loop.
        for spec in users {
            self.require_system_user_privilege_over(&spec.user, &spec.host)?;
        }
        for spec in users {
            if registry.drop_user(&spec.user, &spec.host) {
                // Go `executeDropUser` deletes the account's `mysql.user`
                // row in the same transaction; a target that never existed
                // (reachable only under IF EXISTS) deletes nothing.
                self.mirror_drop_user_row(&spec.user, &spec.host)?;
            }
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
            return Err(DriverError::unsupported(
                "GRANT ... ON FUNCTION/PROCEDURE is not supported yet",
            ));
        }
        let with_grant = if grant.with_grant {
            privilege::GlobalPriv::GrantOption.bit()
        } else {
            0
        };
        if !grant.tls_options.is_empty() {
            return Err(DriverError::unsupported(
                "GRANT ... REQUIRE is not supported yet",
            ));
        }
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::unsupported(
                "GRANT requires a server front end with a privilege registry",
            ));
        };
        // Go's GRANT is ATOMIC across its user list: the executor writes
        // every row inside one transaction and rolls the whole statement
        // back when a grantee is missing (captured, `executor/grant`'s
        // TestGrantPrivilegeAtomic: `grant ... to r1, r2, r4` with `r4`
        // absent leaves r1 and r2 at 'N'). Each arm below verifies every
        // grantee AFTER its privilege gate (Go's plan-time check precedes
        // the executor's user lookup) and BEFORE its first registry write,
        // which reproduces the rollback without one.
        let all_grantees_exist = |users: &[tidb_ast::CreateUserSpec]| -> Result<(), DriverError> {
            for spec in users {
                if !registry.user_exists(&spec.user.user, &spec.user.host) {
                    return Err(DriverError::GrantToUnknownUser);
                }
            }
            Ok(())
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
                self.require_grant_privileges("", "", static_mask, &dynamic, true)?;
                all_grantees_exist(&grant.users)?;
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
                    // Go `grant.go` `composeGlobalPrivUpdate`: the same bits
                    // flip the account's `mysql.user` privilege columns to
                    // `'Y'`. Dynamic privileges live in `global_grants`, not
                    // here, so only the static mask is mirrored.
                    let (user, host) = (user.to_owned(), host.to_owned());
                    self.mirror_global_priv_columns(&user, &host, mask, true)?;
                }
            }
            tidb_ast::GrantLevel::Database(database) => {
                let database = self.resolve_grant_database(database.as_deref())?;
                let privs = self.resolve_scoped_privs(&grant.privileges, ScopeKind::Database)?;
                let mask = privs.iter().fold(0u64, |mask, priv_| mask | priv_.bit()) | with_grant;
                self.require_grant_privileges(&database, "", mask, &[], true)?;
                all_grantees_exist(&grant.users)?;
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
                // Go passes an EMPTY column to every `GRANT`-collected
                // `visitInfo`, so a column list is verified at TABLE scope
                // like any other privilege in the same statement.
                let column_mask = columns.iter().fold(mask, |mask, (_, bits)| mask | bits);
                self.require_grant_privileges(&database, table, column_mask, &[], true)?;
                all_grantees_exist(&grant.users)?;
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
            return Err(DriverError::unsupported(
                "REVOKE ... ON FUNCTION/PROCEDURE is not supported yet",
            ));
        }
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::unsupported(
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
        // Go's REVOKE is ATOMIC across its user list, like GRANT's rollback
        // (captured, `executor/grant`: `revoke all ... from r1, r2, r4, r3`
        // with `r4` absent reports "Unknown user" and r1/r2/r3 keep every
        // bit). Verified after each arm's privilege gate, before its first
        // registry write.
        let all_revokees_exist = |users: &[tidb_ast::CreateUserSpec]| -> Result<(), DriverError> {
            for spec in users {
                if !registry.user_exists(&spec.user.user, &spec.user.host) {
                    return Err(DriverError::RevokeUnknownUser {
                        user: spec.user.user.clone(),
                        host: spec.user.host.clone(),
                    });
                }
            }
            Ok(())
        };
        match &revoke.level {
            tidb_ast::GrantLevel::Global => {
                let (mask, dynamic) = self.split_global_privs(&revoke.privileges, false)?;
                self.require_grant_privileges("", "", mask, &dynamic, false)?;
                all_revokees_exist(&revoke.users)?;
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
                    // Go `revoke.go` `composeGlobalPrivUpdate(.., "N")`: the
                    // revoked bits flip the same `mysql.user` columns back.
                    let (user, host) = (user.to_owned(), host.to_owned());
                    self.mirror_global_priv_columns(&user, &host, mask, false)?;
                }
                // An unregistered name is a WARNING here, not the error
                // `GRANT` raises for it, and the delete still runs
                // (captured: the statement reports OK with a 3929 warning).
                for name in unregistered {
                    self.append_warning(
                        WarningLevel::Warning,
                        3929,
                        format!("Dynamic privilege '{name}' is not registered with the server."),
                    );
                }
            }
            tidb_ast::GrantLevel::Database(database) => {
                let database = self.resolve_grant_database(database.as_deref())?;
                let privs = self.resolve_scoped_privs(&revoke.privileges, ScopeKind::Database)?;
                let mask = privs.iter().fold(0u64, |mask, priv_| mask | priv_.bit());
                self.require_grant_privileges(&database, "", mask, &[], false)?;
                all_revokees_exist(&revoke.users)?;
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
                let column_mask = columns.iter().fold(mask, |mask, (_, bits)| mask | bits);
                self.require_grant_privileges(&database, table, column_mask, &[], false)?;
                all_revokees_exist(&revoke.users)?;
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
                        .ok_or(DriverError::WrongUsage {
                            first: "COLUMN GRANT",
                            second: "NON-COLUMN PRIVILEGES",
                        })?;
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
    fn resolve_scoped_privs(
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
                return Err(DriverError::unsupported(
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
                    ScopeKind::Database => DriverError::WrongUsage {
                        first: "DB GRANT",
                        second: "GLOBAL PRIVILEGES",
                    },
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
                return Err(DriverError::unsupported(
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
                    return Err(DriverError::unsupported(
                        "SHOW GRANTS requires an authenticated session",
                    ));
                };
                (user.to_owned(), host.to_owned())
            }
            Some(spec) if spec.current_user => {
                let Some((user, host)) = self.current_identity() else {
                    return Err(DriverError::unsupported(
                        "SHOW GRANTS requires an authenticated session",
                    ));
                };
                (user.to_owned(), host.to_owned())
            }
            Some(spec) => (spec.user.clone(), spec.host.clone()),
        };
        if self.privilege_checks_bypassed() {
            // Go validates every explicit USING role through `FindEdge`
            // before calling the SkipWithGrant-aware `ShowGrants`. The
            // former deliberately returns false in bypass mode, so USING
            // reports 3530 while the role-less form reaches the fixed 1141.
            if let Some(spec) = show.roles.first() {
                let role_host = if spec.host.is_empty() {
                    "%".to_owned()
                } else {
                    spec.host.clone()
                };
                return Err(DriverError::RoleNotGranted {
                    role: spec.user.clone(),
                    role_host,
                    user,
                    host,
                });
            }
            return Err(DriverError::NonexistingGrant {
                user: "root".to_owned(),
                host: "%".to_owned(),
            });
        }
        // Go `executor/show.go`'s `fetchShowGrants` (around line 2018):
        // reading ANOTHER account's grants needs `SELECT` on the whole
        // `mysql` schema, because that is what reading the grant tables
        // would need. Without it, one statement enumerates every account's
        // privileges.
        if !is_own
            && !self.has_scoped_privilege(
                tidb_mysql::consts::SystemDB,
                "",
                privilege::GlobalPriv::Select,
            )
        {
            let (caller, caller_host) = self.own_account()?;
            return Err(DriverError::DbAccessDenied {
                user: caller,
                host: caller_host,
                database: tidb_mysql::consts::SystemDB.to_owned(),
            });
        }
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::unsupported(
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
