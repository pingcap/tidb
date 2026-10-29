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
//! These are the arms `Session::dispatch_admin_stmt` delegates to; grouped together because they
//! all read and write the same `mysql.user` / privilege-registry state the
//! `privilege` module models.

use std::sync::Arc;

use tidb_datatype::Datum;
use tidb_variable::{SessionVars, VarError};

use crate::DriverError;
use crate::KvTableReader;
use tidb_privilege::PrivilegeTypeExt;
use tidb_hack::GoCaseCompare;
use tidb_password_validation::{self as password_validation, GlobalVarAccessor, PasswordUser, PwdError};

struct SessionPasswordGlobals<'a>(&'a SessionVars);

impl GlobalVarAccessor for SessionPasswordGlobals<'_> {
    type Error = VarError;

    fn get_global_sys_var(&self, name: &str) -> Result<String, Self::Error> {
        self.0.get_global(name)
    }
}

fn password_validation_error(error: PwdError<VarError>) -> DriverError {
    match error {
        PwdError::Accessor(error) => DriverError::from(error),
        PwdError::ParseInt(error) => DriverError::Exec(crate::ExecError::internal(format!(
            "invalid validate_password numeric setting: {error}"
        ))),
        PwdError::NotValid(reason) => DriverError::from(
            tidb_error::catalog::variable::ERR_NOT_VALID_PASSWORD
                .generate_with_stack_by_args(&[reason.into()]),
        ),
    }
}

fn identity_username(identity: &str) -> &str {
    identity.rsplit_once('@').map_or(identity, |(user, _)| user)
}

/// Which non-global `GRANT`/`REVOKE` scope a privilege list is being
/// validated against -- selects between Go's `mysql.AllDBPrivs` and
/// `mysql.AllTablePrivs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeKind {
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
pub struct TableScopePrivs {
    /// The privileges written without a column list.
    table: Vec<tidb_mysql::PrivilegeType>,
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
pub struct PasswordOrLockOptions {
    /// `ACCOUNT LOCK` / `ACCOUNT UNLOCK`.
    pub(crate) locked: Option<bool>,
    /// `FAILED_LOGIN_ATTEMPTS n`, clamped as Go clamps it.
    failed_login_attempts: Option<i64>,
    /// `PASSWORD_LOCK_TIME n | UNBOUNDED`; `UNBOUNDED` is `-1`.
    password_lock_time_days: Option<i64>,
    /// `PASSWORD EXPIRE [DEFAULT | NEVER | INTERVAL n DAY]`.
    pub(crate) expire: Option<tidb_privilege::PasswordExpireSetting>,
    /// Outer None means omitted; inner None means DEFAULT.
    pub(crate) history: Option<Option<i64>>,
    pub(crate) reuse_days: Option<Option<i64>>,
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
                    loaded.expire = Some(tidb_privilege::PasswordExpireSetting::Now);
                }
                Option_::Expire(Expire::Default) => {
                    loaded.expire = Some(tidb_privilege::PasswordExpireSetting::Default);
                }
                Option_::Expire(Expire::Never) => {
                    loaded.expire = Some(tidb_privilege::PasswordExpireSetting::Never);
                }
                Option_::Expire(Expire::Interval(days)) => {
                    if *days <= 0 || *days > MAX_PASSWORD_EXPIRE_INTERVAL_DAYS {
                        return Err(DriverError::from(
                            tidb_error::catalog::types::ERR_WRONG_VALUE2
                                .generate_with_stack_by_args(&[
                                    "DAY".into(),
                                    (*days).to_string().into(),
                                ]),
                        ));
                    }
                    loaded.expire = Some(tidb_privilege::PasswordExpireSetting::Interval(*days));
                }
                Option_::History(count) => {
                    loaded.history = Some(Some((*count).min(u16::MAX as i64)))
                }
                Option_::HistoryDefault => loaded.history = Some(None),
                Option_::ReuseInterval(days) => {
                    loaded.reuse_days = Some(Some((*days).min(u16::MAX as i64)))
                }
                Option_::ReuseDefault => loaded.reuse_days = Some(None),
                // This grammar exposes only DEFAULT, which is the source default.
                Option_::RequireCurrentDefault => {}
            }
        }
        Ok(loaded)
    }

    /// Writes this statement's options onto one existing account row.
    ///
    /// Order matters: `set_locked(.., false)` is the clause that clears the
    /// failed-login counter (Go's `alterUserFailedLoginJSON` resets it
    /// whenever `lockAccount` is `"N"`), so `ACCOUNT UNLOCK` is applied AFTER
    /// the new policy, leaving the captured
    /// `ALTER USER u5 ACCOUNT UNLOCK FAILED_LOGIN_ATTEMPTS 3
    /// PASSWORD_LOCK_TIME 6` -> policy 3/6 with count 0.
    pub(crate) fn apply(&self, registry: &tidb_privilege::PrivilegeRegistry, user: &str, host: &str) {
        registry.set_password_reuse_policy(user, host, self.history, self.reuse_days);
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
fn role_identity(spec: &tidb_ast::RoleSpec) -> tidb_privilege::Account {
    let host = if spec.host.is_empty() {
        "%".to_owned()
    } else {
        spec.host.clone()
    };
    (spec.role.clone(), host)
}

/// Go executor/grant.go's tlsOption2GlobalPriv: reject duplicate or invalid
/// clauses before publishing the complete REQUIRE image. An omitted clause
/// produces the empty NONE policy; specified properties retain their text.
pub fn tls_policy_of(
    options: &[tidb_ast::AlterUserTlsOption],
) -> Result<String, DriverError> {
    use tidb_ast::AlterUserTlsOption as T;
    let invalid = |message: String| DriverError::Exec(crate::ExecError::internal(message));
    let mut fields = serde_json::Map::new();
    let mut seen = std::collections::HashSet::new();
    for option in options {
        let (kind, key, value) = match option {
            T::None => ("NONE", None, None),
            T::Ssl => ("SSL", None, None),
            T::X509 => ("X509", None, None),
            T::Cipher(v) => ("CIPHER", Some("ssl_cipher"), Some(v)),
            T::Issuer(v) => ("ISSUER", Some("x509_issuer"), Some(v)),
            T::Subject(v) => ("SUBJECT", Some("x509_subject"), Some(v)),
            T::San(v) => ("SAN", Some("san"), Some(v)),
            T::TokenIssuer(_) => {
                return Err(DriverError::unsupported(
                    "REQUIRE TOKEN_ISSUER needs token authentication",
                ))
            }
        };
        if !seen.insert(kind) {
            return Err(invalid(format!("Duplicate require {kind} clause")));
        }
        let policy = match option {
            T::None => 0,
            T::Ssl => 1,
            T::X509 => 2,
            _ => 3,
        };
        if policy == 0 {
            fields.remove("ssl_type");
        } else {
            fields.insert("ssl_type".into(), policy.into());
        }
        if let (Some(key), Some(value)) = (key, value) {
            match option {
                T::Cipher(_)
                    if !value.is_empty()
                        && !tidb_tls::SUPPORT_CIPHER.contains(value.as_str()) =>
                {
                    return Err(invalid(format!("Unsupported cipher suite: {value}")))
                }
                T::Issuer(_) | T::Subject(_) => {
                    for entry in value.split('/').filter(|s| !s.is_empty()) {
                        let parts: Vec<_> = entry.split('=').collect();
                        if parts.len() != 2 {
                            return Err(invalid(format!("invalid X509_NAME input: {value}")));
                        }
                        if !matches!(
                            parts[0],
                            "C" | "O"
                                | "OU"
                                | "CN"
                                | "SERIALNUMBER"
                                | "L"
                                | "ST"
                                | "STREET"
                                | "POSTALCODE"
                                | "emailAddress"
                        ) {
                            return Err(invalid(format!(
                                "Unsupport check '{}' in current version TiDB",
                                parts[0]
                            )));
                        }
                    }
                }
                T::San(_) => {
                    tidb_privilege::parse_policy_sans(value).map_err(invalid)?;
                }

                _ => {}
            }
            if !value.is_empty() {
                fields.insert(key.into(), value.clone().into());
            }
        }
    }
    // Priv is LONGTEXT, so Go's struct field order and encoding/json string
    // escaping are observable through SELECT Priv as well as reload.
    #[derive(serde::Serialize)]
    struct OrderedPolicy<'a> {
        #[serde(skip_serializing_if = "Option::is_none")]
        ssl_type: Option<&'a serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        ssl_cipher: Option<&'a serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        x509_issuer: Option<&'a serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        x509_subject: Option<&'a serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        san: Option<&'a serde_json::Value>,
    }
    let policy = OrderedPolicy {
        ssl_type: fields.get("ssl_type"),
        ssl_cipher: fields.get("ssl_cipher"),
        x509_issuer: fields.get("x509_issuer"),
        x509_subject: fields.get("x509_subject"),
        san: fields.get("san"),
    };
    Ok(serde_json::to_string(&policy)
        .map_err(|error| invalid(error.to_string()))?
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029"))
}


/// Go `resourceOptionsInfo.loadResourceOptions`' MAX_USER_CONNECTIONS: the
/// last one written, capped at `math.MaxInt16`; `None` when absent.
pub fn max_user_connections_option(
    options: &[tidb_ast::AlterUserResourceOption],
) -> Option<i64> {
    options
        .iter()
        .rev()
        .find(|option| option.kind == tidb_ast::AlterUserResourceKind::MaxUserConnections)
        .map(|option| option.count.min(i64::from(i16::MAX)))
}

/// Go folds the ATTRIBUTE text into `{"metadata": <text>}` and stores the whole
/// document in the `User_attributes` JSON column, so a malformed text is that
/// column's `Invalid JSON text` (3140).
pub fn invalid_user_attribute_json() -> DriverError {
    DriverError::Mysql(crate::MysqlError::from_terror(
        &tidb_expr::JsonError::InvalidText.to_terror(),
    ))
}

/// Go's executor arms, over the session [`super::account_tables::AccountSession`] gives them.
pub trait AccountExec: super::account_tables::UserTableExec {
    /// Go `checkPasswordHistoryRule` + `addHistoricalData`: judges a new
    /// password against the account's reuse policy (the statement's own
    /// clauses, else the account's, else the globals) and records it.
    #[allow(clippy::too_many_arguments)]
    fn maintain_password_history(
        &mut self,
        registry: &tidb_privilege::PrivilegeRegistry,
        user: &str,
        host: &str,
        encoded: &str,
        plaintext: Option<&str>,
        plugin: &str,
        options: &PasswordOrLockOptions,
        creating: bool,
        plugin_changed: bool,
    ) -> Result<(), DriverError> {
        self.reload_password_history(registry, user, host)?;
        let (count, days) = registry.password_reuse_policy(user, host);
        let global = |name| {
            self.session_vars()
                .get_global(name)
                .map_err(DriverError::from)?
                .parse::<i64>()
                .map_err(|error| {
                    DriverError::Exec(crate::ExecError::internal(format!(
                        "invalid {name}: {error}"
                    )))
                })
        };
        let count = options
            .history
            .unwrap_or(count)
            .map_or_else(|| global("password_history"), Ok)?;
        let days = options
            .reuse_days
            .unwrap_or(days)
            .map_or_else(|| global("password_reuse_interval"), Ok)?;
        registry.record_password_history(
            user,
            host,
            encoded,
            plaintext,
            plugin,
            count,
            days,
            creating,
            plugin_changed,
            &self.session_vars().session_time_zone(),
        )
        .map_err(DriverError::from)
    }

    /// Refreshes the account's `mysql.password_history` rows into the
    /// registry before the reuse rule reads them.
    fn reload_password_history(
        &mut self,
        registry: &tidb_privilege::PrivilegeRegistry,
        user: &str,
        host: &str,
    ) -> Result<(), DriverError> {
        if !self.user_table_present() || self.in_account_statement() {
            return Ok(());
        }
        // The registry keeps UTC timestamps, which is how the TIMESTAMP
        // column is stored.
        let ctx = self
            .statement_context(false)
            .with_time_zone(tidb_datatype::SessionTimeZone::utc());
        let decode_context = crate::RowDecodeContext::for_query(&ctx);
        let rows = self.with_catalog_mut(|catalog| {
            let Some(crate::TableEntry::Kv(table)) =
                catalog.table_mut_in("mysql", "password_history")
            else {
                return Ok(Vec::new());
            };
            let table = std::sync::Arc::make_mut(table);
            let text = |datum: &Datum| tidb_datatype::datum_text::sysvar_datum_text(datum).unwrap_or_default();
            let rows = table
                .scan_rows_with_context(&decode_context)
                .map_err(crate::ddl::index_backfill_error)?;
            Ok(rows
                .into_iter()
                .filter_map(|row| {
                    let [stored_host, stored_user, timestamp, password] = row.as_slice() else {
                        return None;
                    };
                    let Datum::Time(timestamp) = timestamp else {
                        return None;
                    };
                    (text(stored_user) == user && text(stored_host) == host).then(|| {
                        tidb_privilege::loaded::LoadedPasswordHistory {
                            host: host.to_owned(),
                            user: user.to_owned(),
                            timestamp: timestamp.clone(),
                            password: (!password.is_null()).then(|| text(password)),
                        }
                    })
                })
                .collect::<Vec<_>>())
        })?;
        registry.replace_account_password_history(user, host, rows);
        Ok(())
    }

    /// Go `pwdValidator.ValidatePassword` when `validate_password.enable`
    /// is on; a no-op otherwise.
    fn validate_password_if_enabled(&self, password: &str) -> Result<(), DriverError> {
        let globals = SessionPasswordGlobals(self.session_vars());
        let enabled = globals
            .get_global_sys_var("validate_password.enable")
            .is_ok_and(|value| value.eq_ignore_ascii_case("ON") || value == "1");
        if !enabled {
            return Ok(());
        }
        let user = self.session_vars().current_user.as_deref().map(|current| PasswordUser {
            auth_username: identity_username(current).into(),
            username: self
                .session_vars().login_user
                .as_deref()
                .map(identity_username)
                .unwrap_or_else(|| identity_username(current))
                .into(),
        });
        let password = tidb_datatype::GoString::from(password);
        password_validation::validate_password(&password, user.as_ref(), &globals)
            .map_err(password_validation_error)
    }

    /// `CREATE USER [IF NOT EXISTS] <account> [IDENTIFIED BY '<password>']`.
    /// Go `simple.go`'s `executeCreateUser`, minus resource limits and
    /// account annotations, which this tier has no storage for and therefore
    /// refuses rather than silently drops.
    ///
    /// `IDENTIFIED BY` stores the account's
    /// `mysql.user.authentication_string` (see
    /// [`tidb_privilege::encode_password`]), which is the same row the wire front
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
            tidb_mysql::InsertPriv,
        ) || self.has_scoped_privilege("", "", tidb_mysql::CreateUserPriv)
            || (is_role && self.has_scoped_privilege("", "", tidb_mysql::CreateRolePriv))
        {
            return Ok(());
        }
        Err(DriverError::from(
            tidb_error::plannererrors::ERR_SPECIFIC_ACCESS_DENIED.generate_with_stack_by_args(&[
                (if is_role {
                    "CREATE ROLE or CREATE USER"
                } else {
                    "CREATE User"
                }
                .to_owned())
                .into(),
            ]),
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
            tidb_mysql::DeletePriv,
        ) || self.has_scoped_privilege("", "", tidb_mysql::CreateUserPriv)
            || (is_role && self.has_scoped_privilege("", "", tidb_mysql::DropRolePriv))
        {
            return Ok(());
        }
        Err(DriverError::from(
            tidb_error::plannererrors::ERR_SPECIFIC_ACCESS_DENIED.generate_with_stack_by_args(&[
                (if is_role {
                    "DROP ROLE or CREATE USER"
                } else {
                    "CREATE USER"
                }
                .to_owned())
                .into(),
            ]),
        ))
    }

    /// Go's shared `SYSTEM_USER` guard on `DROP USER`/`ALTER USER`
    /// (`executor/simple.go` around lines 2563 and 1958): an account that
    /// itself holds `SYSTEM_USER` may only be modified by a caller who holds
    /// `SYSTEM_USER` (or `RESTRICTED_USER_ADMIN`) too. Because SUPER is the
    /// fallback for every dynamic privilege, this reads as "only a SUPER may
    /// touch a SUPER", which is why Go's message names both.
    fn require_system_user_privilege_over(
        &self,
        user: &str,
        host: &str,
    ) -> Result<(), DriverError> {
        if self.privilege_checks_bypassed()
            || self.privilege_registry().is_none()
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
            return Err(DriverError::from(
                tidb_error::plannererrors::ERR_SPECIFIC_ACCESS_DENIED
                    .generate_with_stack_by_args(&["SYSTEM_USER or SUPER".into()]),
            ));
        }
        Ok(())
    }

    /// Whether `(user, host)` is the account this session authenticated as --
    /// Go's `alterCurrentUser`, which keys on the AUTHENTICATED identity so a
    /// statement naming that account explicitly is still self-service.
    fn is_own_account(&self, user: &str, host: &str) -> bool {
        self.current_identity() == Some((user, host))
    }

    /// Go `SimpleExec.executeCreateUser` for `CREATE USER`.
    #[allow(clippy::too_many_arguments)]
    fn create_user_stmt(
        &mut self,
        if_not_exists: bool,
        users: &[tidb_ast::CreateUserSpec],
        tls_options: &[tidb_ast::AlterUserTlsOption],
        resource_options: &[tidb_ast::AlterUserResourceOption],
        password_options: &[tidb_ast::CreateUserPasswordOption],
        comment_or_attribute: &Option<tidb_ast::CreateUserCommentOrAttribute>,
        resource_group: &Option<String>,
    ) -> Result<(), DriverError> {
        // Go `loadResourceOptions`: only MAX_USER_CONNECTIONS reaches the
        // stored row, capped at `math.MaxInt16`.
        let max_user_connections = max_user_connections_option(resource_options).unwrap_or(0);
        let tls_policy = tls_policy_of(tls_options)?;
        // Go `executeCreateUser`'s `userAttributes`: a COMMENT clause is
        // wrapped as `{"metadata": {"comment": "<text>"}}`, an ATTRIBUTE
        // clause embeds the caller's JSON as `{"metadata": <json>}`, and a
        // statement with neither stores the literal empty object `'{}'` --
        // NOT NULL (captured: a fresh account's `User_attributes` prints
        // `{}`; only the bootstrap root row is NULL).
        let user_attributes_json = match comment_or_attribute {
            None => "{}".to_owned(),
            Some(tidb_ast::CreateUserCommentOrAttribute::Comment(text)) => {
                serde_json::json!({"metadata": {"comment": text}}).to_string()
            }
            Some(tidb_ast::CreateUserCommentOrAttribute::Attribute(json)) => {
                serde_json::json!({"metadata": serde_json::from_str::<serde_json::Value>(json).map_err(|_| invalid_user_attribute_json())?}).to_string()
            }
        };
        // Go then appends `"resource_group": "<name>"`.
        let user_attributes_json = match resource_group {
            None => user_attributes_json,
            Some(name) => {
                let name = self.user_resource_group_attribute(name, false)?;
                let mut attributes: serde_json::Map<String, serde_json::Value> =
                    serde_json::from_str(&user_attributes_json).expect("composed above");
                attributes.insert("resource_group".to_owned(), serde_json::Value::String(name));
                serde_json::Value::Object(attributes).to_string()
            }
        };
        // Go validates every statement-level option BEFORE writing any row,
        // so a bad `PASSWORD EXPIRE INTERVAL 0 DAY` creates no account.
        let options = PasswordOrLockOptions::load(password_options)?;
        self.require_create_user_privilege(false)?;
        let Some(registry) = self.privilege_registry().cloned() else {
            return Err(DriverError::unsupported(
                "CREATE USER requires a server front end with a privilege registry",
            ));
        };
        let default_plugin = self
            .session_vars()
            .get_global("default_authentication_plugin")
            .map_err(DriverError::from)?;
        if !tidb_privilege::is_create_user_plugin(&default_plugin) {
            return Err(DriverError::from(
                tidb_dbterror::exeerrors::ERR_PLUGIN_IS_NOT_LOADED
                    .generate_with_stack_by_args(&[default_plugin.into()]),
            ));
        }
        // Go `executeCreateUser` validates every account -- existence,
        // plugin, password strength -- while composing ONE INSERT, and runs
        // it only after the loop: a failure on any account creates none.
        let mut planned = Vec::with_capacity(users.len());
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
            // Go `executeCreateUser` checks the name length first, then
            // refuses to expire the anonymous account's password, then the
            // host length -- all before it looks the account up.
            if user.len() > tidb_parser::auth::USER_NAME_MAX_LENGTH {
                return Err(tidb_planner::preprocess::wrong_string_length_error(
                    user,
                    "user name",
                    tidb_parser::auth::USER_NAME_MAX_LENGTH,
                ).into());
            }
            if user.is_empty()
                && matches!(options.expire, Some(tidb_privilege::PasswordExpireSetting::Now))
            {
                return Err(DriverError::from(
                    tidb_dbterror::exeerrors::ERR_PASSWORD_EXPIRE_ANONYMOUS_USER
                        .generate_with_stack_by_args(&[]),
                ));
            }
            if host.len() > tidb_parser::auth::HOST_NAME_MAX_LENGTH {
                return Err(tidb_planner::preprocess::wrong_string_length_error(
                    host,
                    "host name",
                    tidb_parser::auth::HOST_NAME_MAX_LENGTH,
                ).into());
            }
            if registry.user_exists(user, host) {
                if !if_not_exists {
                    return Err(DriverError::from(
                        tidb_dbterror::exeerrors::ERR_CANNOT_USER
                            .generate_with_stack_by_args(&[
                                "CREATE USER".into(),
                                format!("'{}'@'{}'", user.to_owned(), host.to_owned()).into(),
                            ]),
                    ));
                }
                // go `executeCreateUser` demotes the duplicate to
                // ErrUserAlreadyExists (3163) as a NOTE under IF NOT EXISTS.
                self.append_sql_warning(tidb_context::SqlWarn::raised(
                    tidb_context::WarningLevel::Note,
                    tidb_error::catalog::infoschema::ERR_USER_ALREADY_EXISTS
                        .generate_with_stack_by_args(&[format!("'{}'@'{}'", user, host).into()]),
                ));
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
            planned.push((user, host, auth_string, plugin));
        }
        for (user, host, auth_string, plugin) in planned {
            if registry.create_user_with_plugin_and_tls_policy(
                user,
                host,
                &auth_string,
                &plugin,
                &tls_policy,
            ) {
                registry.merge_user_attributes(
                    user,
                    host,
                    serde_json::from_str(&user_attributes_json).expect("validated metadata"),
                    false,
                );
                options.apply(&registry, user, host);
                registry.set_max_user_connections(user, host, max_user_connections);
                self.maintain_password_history(
                    &registry,
                    user,
                    host,
                    &auth_string,
                    None,
                    &plugin,
                    &options,
                    true,
                    false,
                )?;
                // The `mysql.user` row Go's INSERT writes for this account.
                // Column values come from the statement's own clauses, the
                // way Go's `plOptions` feeds the VALUES list.
                let (password_expired, password_lifetime) = match options.expire {
                    Some(tidb_privilege::PasswordExpireSetting::Now) => (true, None),
                    Some(tidb_privilege::PasswordExpireSetting::Never) => (false, Some(0)),
                    Some(tidb_privilege::PasswordExpireSetting::Interval(days)) => (false, Some(days)),
                    Some(tidb_privilege::PasswordExpireSetting::Default) | None => (false, None),
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
                    max_user_connections,
                )?;
                self.mirror_account_policy_and_history(&registry, &user, &host)?;
                // Go `executeCreateUser`: `INSERT IGNORE INTO
                // mysql.global_priv`, whose `tlsOption2GlobalPriv` is `{}`
                // for a statement without REQUIRE.
                self.mirror_global_priv(
                    &user,
                    &host,
                    &tls_policy,
                    super::account_tables::GlobalPrivWrite::CreateUser,
                )?;
            } else if !if_not_exists {
                return Err(DriverError::from(
                    tidb_dbterror::exeerrors::ERR_CANNOT_USER.generate_with_stack_by_args(&[
                        "CREATE USER".into(),
                        format!("'{}'@'{}'", user.to_owned(), host.to_owned()).into(),
                    ]),
                ));
            }
        }
        Ok(())
    }

    /// `CREATE ROLE [IF NOT EXISTS] <role> [, ...]` -- Go's `executeCreateUser`
    /// reached with `IsCreateRole`, which writes the same `mysql.user` row
    /// with `account_locked = 'Y'` and no password.
    ///
    /// Roles and users share ONE namespace, so this reports `ErrCannotUser`
    /// against a name already taken by either kind (captured both ways); the
    /// only thing that differs from `CREATE USER`'s message is the operation
    /// name it prints.
    fn create_role_stmt(
        &mut self,
        if_not_exists: bool,
        roles: &[tidb_ast::RoleSpec],
    ) -> Result<(), DriverError> {
        self.require_create_user_privilege(true)?;
        let Some(registry) = self.privilege_registry().cloned() else {
            return Err(DriverError::unsupported(
                "CREATE ROLE requires a server front end with a privilege registry",
            ));
        };
        // Go `executeCreateUser` (reached with `IsCreateRole`) resolves the
        // row's plugin from `default_authentication_plugin` exactly as it
        // does for a user with no `IDENTIFIED WITH`.
        let default_plugin = self
            .session_vars()
            .get_global("default_authentication_plugin")
            .unwrap_or_else(|_| tidb_mysql::consts::AuthNativePassword.to_owned());
        // Go `executeCreateUser` checks every role before its one INSERT, so
        // `CREATE ROLE r1, r2, r3` with r2 taken creates none of them.
        let mut planned = Vec::with_capacity(roles.len());
        for spec in roles {
            let (role, host) = role_identity(spec);
            if !registry.user_exists(&role, &host) {
                planned.push((role, host));
            } else if !if_not_exists {
                return Err(DriverError::from(
                    tidb_dbterror::exeerrors::ERR_CANNOT_USER.generate_with_stack_by_args(&[
                        "CREATE ROLE".into(),
                        (format!("'{role}'@'{host}'")).into(),
                    ]),
                ));
            } else {
                // go demotes the duplicate to Note 3163 under IF NOT
                // EXISTS, same as CREATE USER.
                self.append_sql_warning(tidb_context::SqlWarn::raised(
                    tidb_context::WarningLevel::Note,
                    tidb_error::catalog::infoschema::ERR_USER_ALREADY_EXISTS
                        .generate_with_stack_by_args(&[format!("'{}'@'{}'", role, host).into()]),
                ));
            }
        }
        for (role, host) in planned {
            if registry.create_role_with_plugin(&role, &host, &default_plugin) {
                registry.merge_user_attributes(&role, &host, serde_json::json!({}), false);
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
                    0,
                )?;
                self.mirror_account_policy_and_history(&registry, &role, &host)?;
                // Go `executeCreateUser` with `IsCreateRole` writes the
                // same `{}` policy row.
                self.mirror_global_priv(
                    &role,
                    &host,
                    "{}",
                    super::account_tables::GlobalPrivWrite::CreateUser,
                )?;
            } else if !if_not_exists {
                return Err(DriverError::from(
                    tidb_dbterror::exeerrors::ERR_CANNOT_USER.generate_with_stack_by_args(&[
                        "CREATE ROLE".into(),
                        (format!("'{role}'@'{host}'")).into(),
                    ]),
                ));
            }
        }
        Ok(())
    }

    /// `GRANT <role> [, ...] TO <account> [, ...]`: one `mysql.role_edges`
    /// row per pair. Go validates every ROLE first (`ErrGrantRole`/3523 for a
    /// role with no account row) and only then every target account
    /// (`ErrCannotUser`/1396), so a statement naming both an unknown role and
    /// an unknown user reports the ROLE (captured order).
    ///
    /// Nothing rejects a self-grant or a cycle: `GRANT r1 TO r1` reports OK
    /// (captured).
    fn grant_role_stmt(
        &mut self,
        grant: &tidb_ast::GrantRoleStmt,
    ) -> Result<(), DriverError> {
        let Some(registry) = self.privilege_registry().cloned() else {
            return Err(DriverError::unsupported(
                "GRANT <role> requires a server front end with a privilege registry",
            ));
        };
        let roles = self.resolve_roles(&grant.roles, |role, host| {
            DriverError::from(
                tidb_dbterror::exeerrors::ERR_GRANT_ROLE.generate_with_stack_by_args(&[
                    format!("`{}`@`{}`", role.to_owned(), host.to_owned()).into(),
                ]),
            )
        })?;
        let grantees = self.resolve_role_grantees(&grant.users, "GRANT ROLE")?;
        for grantee in &grantees {
            for role in &roles {
                registry.grant_role(role, grantee);
                self.mirror_role_edge((&role.0, &role.1), (&grantee.0, &grantee.1), true)?;
            }
        }
        Ok(())
    }

    /// `REVOKE <role> [, ...] FROM <account> [, ...]`. Unlike `GRANT`, a
    /// missing ROLE here reports `ErrCannotUser`/1396 -- backtick-quoted, as
    /// `auth.RoleIdentity.String` prints it -- rather than 3523. Revoking a
    /// role the account never held is a silent no-op (captured).
    fn revoke_role_stmt(
        &mut self,
        revoke: &tidb_ast::RevokeRoleStmt,
    ) -> Result<(), DriverError> {
        let Some(registry) = self.privilege_registry().cloned() else {
            return Err(DriverError::unsupported(
                "REVOKE <role> requires a server front end with a privilege registry",
            ));
        };
        let roles = self.resolve_roles(&revoke.roles, |role, host| {
            DriverError::from(
                tidb_dbterror::exeerrors::ERR_CANNOT_USER.generate_with_stack_by_args(&[
                    "REVOKE ROLE".into(),
                    (format!("`{role}`@`{host}`")).into(),
                ]),
            )
        })?;
        let grantees = self.resolve_role_grantees(&revoke.users, "REVOKE ROLE")?;
        for grantee in &grantees {
            for role in &roles {
                registry.revoke_role(role, grantee);
                self.mirror_role_edge((&role.0, &role.1), (&grantee.0, &grantee.1), false)?;
            }
        }
        // Go drops a revoked role from the CURRENT session's active set in
        // the same statement, so a session cannot keep using privileges it
        // no longer holds.
        if let Some((user, host)) = self.current_identity() {
            let account = (user.to_owned(), host.to_owned());
            if grantees.contains(&account) {
                Arc::make_mut(&mut self.session_vars_mut().active_roles).retain(|role| !roles.contains(role));
            }
        }
        Ok(())
    }

    /// `SET DEFAULT ROLE <selection> TO <account> [, ...]`: replaces each
    /// account's `mysql.default_roles` rows (never merges -- captured:
    /// `SET DEFAULT ROLE r1` after `ALL` leaves r1 alone).
    ///
    /// `ALL` means every role granted to THAT account, resolved per account.
    /// A named role that the account does not hold reports 3530, the same
    /// gate `SET ROLE` uses.
    fn set_default_role_stmt(
        &mut self,
        set_default: &tidb_ast::SetDefaultRoleStmt,
    ) -> Result<(), DriverError> {
        let Some(registry) = self.privilege_registry().cloned() else {
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
                tidb_mysql::UpdatePriv,
            )
            && !self.has_scoped_privilege("", "", tidb_mysql::CreateUserPriv)
        {
            return Err(DriverError::from(
                tidb_error::plannererrors::ERR_SPECIFIC_ACCESS_DENIED
                    .generate_with_stack_by_args(&["CREATE USER".into()]),
            ));
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
                        Err(DriverError::from(
                            tidb_dbterror::exeerrors::ERR_CANNOT_USER
                                .generate_with_stack_by_args(&[
                                    "SET DEFAULT ROLE".into(),
                                    (format!("{}@{}", spec.user, spec.host)).into(),
                                ]),
                        ))
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
                    return Err(DriverError::from(
                        tidb_dbterror::exeerrors::ERR_CANNOT_USER
                            .generate_with_stack_by_args(&[
                                "SET DEFAULT ROLE".into(),
                                (format!("`{role}`@`{host}`")).into(),
                            ]),
                    ));
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
                        return Err(DriverError::from(
                            tidb_dbterror::exeerrors::ERR_ROLE_NOT_GRANTED
                                .generate_with_stack_by_args(&[
                                    format!("`{}`@`{}`", role.0, role.1).into(),
                                    format!("{}@{}", account.0.clone(), account.1.clone()).into(),
                                ]),
                        ));
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
        // Go `setDefaultRoleNone` / `setDefaultRoleRegular` /
        // `setDefaultRoleAll`.
        for (account, roles) in accounts.iter().zip(&replacements) {
            let roles = match &set_default.selection {
                tidb_ast::DefaultRoleSelection::All => None,
                _ => Some(roles.as_slice()),
            };
            self.mirror_default_roles(&account.0, &account.1, roles)?;
        }
        Ok(())
    }

    /// Resolves written role identities to accounts, requiring each to have
    /// an account row. `missing` builds the error, because `GRANT` and
    /// `REVOKE` report a missing role differently (3523 vs 1396).
    fn resolve_roles(
        &self,
        roles: &[tidb_ast::RoleSpec],
        missing: impl Fn(&str, &str) -> DriverError,
    ) -> Result<Vec<tidb_privilege::Account>, DriverError> {
        let Some(registry) = self.privilege_registry() else {
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
    ) -> Result<Vec<tidb_privilege::Account>, DriverError> {
        let Some(registry) = self.privilege_registry().cloned() else {
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
                    Err(DriverError::from(
                        tidb_dbterror::exeerrors::ERR_CANNOT_USER
                            .generate_with_stack_by_args(&[
                                operation.into(),
                                (format!("{user}@{host}")).into(),
                            ]),
                    ))
                }
            })
            .collect()
    }

    /// Resolves roles that must be granted DIRECTLY to `account`, which is
    /// the gate `SET ROLE` and `SET DEFAULT ROLE` share.
    fn granted_roles_or_error(
        &self,
        registry: &tidb_privilege::PrivilegeRegistry,
        account: &tidb_privilege::Account,
        roles: &[tidb_ast::RoleSpec],
    ) -> Result<Vec<tidb_privilege::Account>, DriverError> {
        roles
            .iter()
            .map(|spec| {
                let role = role_identity(spec);
                // Go `checker.FindEdge` reads the privilege cache: a role edge
                // written into `mysql.role_edges` directly counts only after
                // FLUSH PRIVILEGES (asked of the Go server: 3530 before it).
                let cache = self.account_statement_privilege_cache()
                    .unwrap_or(registry);
                if cache.has_role(account, &role) {
                    Ok(role)
                } else {
                    Err(DriverError::from(
                        tidb_dbterror::exeerrors::ERR_ROLE_NOT_GRANTED
                            .generate_with_stack_by_args(&[
                                format!("`{}`@`{}`", role.0.clone(), role.1.clone()).into(),
                                format!("{}@{}", account.0.clone(), account.1.clone()).into(),
                            ]),
                    ))
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
    /// [`tidb_privilege::CREATE_USER_PLUGINS`] can never be loaded. A missing
    /// `IDENTIFIED` clause uses the live `default_authentication_plugin` and
    /// an empty authentication string, as Go's account executor does.
    fn resolve_auth_string_and_plugin(
        auth: Option<&tidb_ast::CreateUserAuth>,
        default_plugin: &str,
    ) -> Result<(String, String), DriverError> {
        match auth {
            None => Ok((String::new(), default_plugin.to_owned())),
            Some(tidb_ast::CreateUserAuth::By(password)) => Ok((
                tidb_privilege::encode_password_for_plugin(
                    default_plugin,
                    &tidb_privilege::PluginCredential::By(password),
                )?,
                default_plugin.to_owned(),
            )),
            Some(tidb_ast::CreateUserAuth::With { plugin, credential }) => {
                if !tidb_privilege::is_create_user_plugin(plugin) {
                    return Err(DriverError::from(
                        tidb_dbterror::exeerrors::ERR_PLUGIN_IS_NOT_LOADED
                            .generate_with_stack_by_args(&[plugin.clone().into()]),
                    ));
                }
                let credential = match credential {
                    None => tidb_privilege::PluginCredential::None,
                    Some(tidb_ast::CreateUserCredential::By(password)) => {
                        tidb_privilege::PluginCredential::By(password)
                    }
                    Some(tidb_ast::CreateUserCredential::As(hash)) => {
                        tidb_privilege::PluginCredential::As(hash)
                    }
                };
                let auth_string = tidb_privilege::encode_password_for_plugin(plugin, &credential)?;
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
    fn rename_user_stmt(
        &mut self,
        pairs: &[tidb_ast::RenameUserPair],
    ) -> Result<(), DriverError> {
        let Some(registry) = self.privilege_registry().cloned() else {
            return Err(DriverError::unsupported(
                "RENAME USER requires a server front end with a privilege registry",
            ));
        };
        let mut resolved = Vec::with_capacity(pairs.len());
        for pair in pairs {
            resolved.push((
                self.resolve_account(&pair.old_user)?,
                self.resolve_account(&pair.new_user)?,
            ));
        }
        // Go renames every pair inside ONE transaction and rolls all of them
        // back at the first pair whose source is missing or whose target is
        // taken -- each pair seeing the renames before it. Walk the pairs
        // over that running view first, so a failing statement renames
        // nothing.
        {
            let mut gone: Vec<(String, String)> = Vec::new();
            let mut added: Vec<(String, String)> = Vec::new();
            let exists = |account: &(String, String),
                          gone: &[(String, String)],
                          added: &[(String, String)]| {
                added.contains(account)
                    || (!gone.contains(account) && registry.user_exists(&account.0, &account.1))
            };
            for (old, new) in &resolved {
                // Go `executeRenameUser` checks the NEW name's lengths before
                // it looks either account up.
                if new.0.len() > tidb_parser::auth::USER_NAME_MAX_LENGTH {
                    return Err(tidb_planner::preprocess::wrong_string_length_error(
                        &new.0,
                        "user name",
                        tidb_parser::auth::USER_NAME_MAX_LENGTH,
                    ).into());
                }
                if new.1.len() > tidb_parser::auth::HOST_NAME_MAX_LENGTH {
                    return Err(tidb_planner::preprocess::wrong_string_length_error(
                        &new.1,
                        "host name",
                        tidb_parser::auth::HOST_NAME_MAX_LENGTH,
                    ).into());
                }
                let old_missing = !exists(old, &gone, &added);
                if old_missing || exists(new, &gone, &added) {
                    return Err(DriverError::from(
                        tidb_dbterror::exeerrors::ERR_CANNOT_USER
                            .generate_with_stack_by_args(&[
                                "RENAME USER".into(),
                                format!(
                                    "{}@{} TO {}@{} {}",
                                    old.0.clone(),
                                    old.1.clone(),
                                    new.0.clone(),
                                    new.1.clone(),
                                    if old_missing {
                                        "old did not exist"
                                    } else {
                                        "new did exist"
                                    }
                                )
                                .into(),
                            ]),
                    ));
                }
                added.retain(|account| account != old);
                gone.push(old.clone());
                gone.retain(|account| account != new);
                added.push(new.clone());
            }
        }
        for ((old_user, old_host), (new_user, new_host)) in resolved {
            if !registry.rename_user(&old_user, &old_host, &new_user, &new_host) {
                return Err(DriverError::from(
                    tidb_dbterror::exeerrors::ERR_CANNOT_USER.generate_with_stack_by_args(&[
                        "RENAME USER".into(),
                        format!(
                            "{}@{} TO {}@{} {}",
                            old_user,
                            old_host,
                            new_user,
                            new_host,
                            if !registry.user_exists(&old_user, &old_host) {
                                "old did not exist"
                            } else {
                                "new did exist"
                            }
                        )
                        .into(),
                    ]),
                ));
            }
            // Go `renameUserHostInSystemTable` on `mysql.user`: the row moves
            // with the account, authentication string included.
            self.mirror_rename_user_row(&old_user, &old_host, &new_user, &new_host)?;
            self.mirror_rename_password_history(&old_user, &old_host, &new_user, &new_host)?;
            self.mirror_rename_user_privileges(&old_user, &old_host, &new_user, &new_host)?;
        }
        Ok(())
    }

    /// Resolves one written account identity, expanding the `CURRENT_USER`
    /// pseudo-user to the session's own identity as Go does.
    fn resolve_account(
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
    fn own_account(&self) -> Result<(String, String), DriverError> {
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
    fn drop_user_stmt(
        &mut self,
        is_role: bool,
        if_exists: bool,
        users: &[tidb_ast::UserSpec],
    ) -> Result<(), DriverError> {
        self.require_drop_user_privilege(is_role)?;
        let Some(registry) = self.privilege_registry().cloned() else {
            return Err(DriverError::unsupported(
                "DROP USER requires a server front end with a privilege registry",
            ));
        };
        // Go walks the accounts in order inside one transaction: the first
        // missing account (without IF EXISTS) stops the walk and is the only
        // one the error names, a missing one under IF EXISTS is Note 3162,
        // and a `SYSTEM_USER` target rolls everything back. Checking all of
        // it before the first delete reproduces that rollback.
        for spec in users {
            if !registry.user_exists(&spec.user, &spec.host) {
                let account = format!("{}@{}", spec.user, spec.host);
                if !if_exists {
                    return Err(if is_role {
                        DriverError::from(
                            tidb_dbterror::exeerrors::ERR_CANNOT_USER
                                .generate_with_stack_by_args(&["DROP ROLE".into(), account.into()]),
                        )
                    } else {
                        DriverError::from(
                            tidb_dbterror::exeerrors::ERR_CANNOT_USER
                                .generate_with_stack_by_args(&["DROP USER".into(), account.into()]),
                        )
                    });
                }
                self.append_sql_warning(tidb_context::SqlWarn::raised(
                    tidb_context::WarningLevel::Note,
                    tidb_error::catalog::infoschema::ERR_USER_DROP_EXISTS
                        .generate_with_stack_by_args(&[account.into()]),
                ));
            }
            self.require_system_user_privilege_over(&spec.user, &spec.host)?;
        }
        for spec in users {
            if registry.drop_user(&spec.user, &spec.host) {
                // Go `executeDropUser` deletes the account's `mysql.user`
                // row in the same transaction; a target that never existed
                // (reachable only under IF EXISTS) deletes nothing.
                self.mirror_drop_user_row(&spec.user, &spec.host)?;
                self.mirror_delete_password_history(&spec.user, &spec.host)?;
                self.mirror_drop_user_privileges(&spec.user, &spec.host)?;
            }
        }
        // A dropped role stops being active in THIS session too; the edge it
        // was activated through is gone, so keeping it would confer
        // privileges from a row that no longer exists.
        Arc::make_mut(&mut self.session_vars_mut().active_roles).retain(|(role, host)| {
            !users
                .iter()
                .any(|spec| &spec.user == role && &spec.host == host)
        });
        Ok(())
    }

    /// Go's `grant.go` GLOBAL/DATABASE/TABLE privileges and shared REQUIRE
    /// policy. Dynamic privileges are global; columns use the table owner.
    ///
    /// `WITH GRANT OPTION` is just `mysql.GrantPriv` ORed into the same
    /// scope's privilege mask, which is why it works identically at all
    /// three scopes and why `REVOKE GRANT OPTION ON <level>` (an ordinary
    /// privilege name) clears exactly that scope's bit.
    fn grant_stmt(
        &mut self,
        grant: &tidb_ast::GrantStmt,
    ) -> Result<(), DriverError> {
        if grant.object_type.is_some() {
            return Err(DriverError::unsupported(
                "GRANT ... ON FUNCTION/PROCEDURE is not supported yet",
            ));
        }
        let with_grant = if grant.with_grant {
            tidb_mysql::GrantPriv.bit()
        } else {
            0
        };
        // Go grantGlobalPriv shares CREATE/ALTER USER's policy producer.
        // Privilege/target validation precedes policy validation and mutation.
        // An omitted REQUIRE leaves the account's existing policy untouched.
        let load_tls_policy = || {
            (!grant.tls_options.is_empty())
                .then(|| tls_policy_of(&grant.tls_options))
                .transpose()
        };
        let tls_policy;
        let Some(registry) = self.privilege_registry().cloned() else {
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
        //
        // Go `GrantExec.Next` creates a missing grantee inside that same
        // transaction when `NO_AUTO_CREATE_USER` is unset (5.7 compatibility):
        // `INSERT INTO mysql.user (Host, User, authentication_string, plugin)`
        // with `default_authentication_plugin`. The accounts to create are
        // collected here and written only after every check has passed.
        let no_auto_create_user = self.session_vars().sql_mode().has_no_auto_create_user_mode();
        let all_grantees_exist =
            |users: &[tidb_ast::CreateUserSpec]| -> Result<Vec<usize>, DriverError> {
                let mut missing = Vec::new();
                for (position, spec) in users.iter().enumerate() {
                    if !registry.user_exists(&spec.user.user, &spec.user.host) {
                        if no_auto_create_user {
                            return Err(DriverError::from(
                                tidb_dbterror::exeerrors::ERR_CANT_CREATE_USER_WITH_GRANT
                                    .generate_with_stack_by_args(&[]),
                            ));
                        }
                        missing.push(position);
                    }
                }
                Ok(missing)
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
                let missing = all_grantees_exist(&grant.users)?;
                tls_policy = load_tls_policy()?;
                self.create_grantees(&registry, &grant.users, &missing)?;
                for spec in &grant.users {
                    let user = spec.user.user.as_str();
                    let host = spec.user.host.as_str();
                    // Go's default sql_mode forbids GRANT from implicitly
                    // creating the target account (captured:
                    // `ErrCantCreateUserWithGrant`, 1410).
                    if !registry.user_exists(user, host) {
                        return Err(DriverError::from(
                            tidb_dbterror::exeerrors::ERR_CANT_CREATE_USER_WITH_GRANT
                                .generate_with_stack_by_args(&[]),
                        ));
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
                    // Go `grantDynamicPriv`: one `mysql.global_grants` row
                    // per dynamic privilege.
                    for name in &dynamic {
                        self.mirror_dynamic_priv(&user, &host, name, Some(grant.with_grant))?;
                    }
                }
            }
            tidb_ast::GrantLevel::Database(database) => {
                let database = self.resolve_grant_database(database.as_deref())?;
                let privs = self.resolve_scoped_privs(&grant.privileges, ScopeKind::Database)?;
                let mask = privs.iter().fold(0u64, |mask, priv_| mask | priv_.bit()) | with_grant;
                let missing = all_grantees_exist(&grant.users)?;
                tls_policy = load_tls_policy()?;
                self.create_grantees(&registry, &grant.users, &missing)?;
                for spec in &grant.users {
                    let user = spec.user.user.as_str();
                    let host = spec.user.host.as_str();
                    if !registry.user_exists(user, host) {
                        return Err(DriverError::from(
                            tidb_dbterror::exeerrors::ERR_CANT_CREATE_USER_WITH_GRANT
                                .generate_with_stack_by_args(&[]),
                        ));
                    }
                    registry.grant_db(user, host, &database, mask);
                    let (user, host) = (user.to_owned(), host.to_owned());
                    self.mirror_db_privs(&user, &host, &database, mask, true)?;
                }
            }
            tidb_ast::GrantLevel::Table { database, table } => {
                let database = self.resolve_grant_database(database.as_deref())?;
                let TableScopePrivs {
                    table: privs,
                    columns,
                } = self.resolve_table_scope_privs(&grant.privileges)?;
                let mask = privs.iter().fold(0u64, |mask, priv_| mask | priv_.bit()) | with_grant;
                let missing = all_grantees_exist(&grant.users)?;
                // Go allows granting on a table that does not exist only
                // when the privilege list includes `CREATE` (captured:
                // issues #28533/#29268); otherwise it reports
                // `ErrTableNotExists` (1146).
                let table_exists = self.lock_catalog()?.table_in(&database, table).is_some();
                if !table_exists && !privs.contains(&tidb_mysql::CreatePriv) {
                    return Err(DriverError::from(
                        tidb_error::catalog::infoschema::ERR_TABLE_NOT_EXISTS
                            .generate_with_stack_by_args(&[(&database).into(), table.into()]),
                    ));
                }
                // Go's `checkAndInitColumnPriv` resolves every named column
                // against the table and reports `Unknown column: <name>`
                // when it is absent, so a `GRANT` naming a bad column stores
                // nothing at all (captured). Resolving up front also
                // normalises the spelling to the table's own: `GRANT SELECT
                // (A)` prints back as `SELECT(a)`.
                let columns = self.resolve_grant_columns(&database, table, &columns)?;
                tls_policy = load_tls_policy()?;
                // Go `GrantExec.Next` appends a GRANT element under `WITH
                // GRANT OPTION`; `ALL` is told apart because Go's
                // `composeTablePrivUpdateForGrant` replaces the sets for it.
                let table_all = grant
                    .privileges
                    .iter()
                    .any(|privilege| privilege.name == "ALL" && privilege.columns.is_empty());
                let mut table_privs_written = privs.clone();
                if with_grant != 0 {
                    table_privs_written.push(tidb_mysql::GrantPriv);
                }
                self.create_grantees(&registry, &grant.users, &missing)?;
                for spec in &grant.users {
                    let user = spec.user.user.as_str();
                    let host = spec.user.host.as_str();
                    if !registry.user_exists(user, host) {
                        return Err(DriverError::from(
                            tidb_dbterror::exeerrors::ERR_CANT_CREATE_USER_WITH_GRANT
                                .generate_with_stack_by_args(&[]),
                        ));
                    }
                    // The TABLE row is created even by a column-only grant
                    // (Go's `checkAndInitTablePriv` runs for every TABLE-level
                    // GRANT), which is what later lets `REVOKE` on that table
                    // get past its "no such grant" check.
                    registry.grant_table(user, host, &database, table, mask);
                    for (column, column_mask) in &columns {
                        registry.grant_column(user, host, &database, table, column, *column_mask);
                    }
                    let (user, host) = (user.to_owned(), host.to_owned());
                    self.mirror_table_privs(
                        &user,
                        &host,
                        &database,
                        table,
                        &table_privs_written,
                        table_all,
                        true,
                    )?;
                    self.mirror_column_privs(&user, &host, &database, table, &columns, true)?;
                }
            }
        }
        if let Some(policy) = tls_policy {
            for spec in &grant.users {
                registry.set_tls_policy(&spec.user.user, &spec.user.host, &policy);
                self.mirror_global_priv(
                    &spec.user.user,
                    &spec.user.host,
                    &policy,
                    super::account_tables::GlobalPrivWrite::Grant,
                )?;
            }
        }
        Ok(())
    }

    /// Go `GrantExec.Next`'s implicit account creation: each missing grantee
    /// gets a `mysql.user` row carrying only its host, name, encoded password
    /// and plugin (`IDENTIFIED BY` in the GRANT, else
    /// `default_authentication_plugin`).
    fn create_grantees(
        &mut self,
        registry: &tidb_privilege::PrivilegeRegistry,
        users: &[tidb_ast::CreateUserSpec],
        missing: &[usize],
    ) -> Result<(), DriverError> {
        if missing.is_empty() {
            return Ok(());
        }
        let default_plugin = self
            .session_vars()
            .get_global("default_authentication_plugin")
            .map_err(DriverError::from)?;
        for &position in missing {
            let spec = &users[position];
            let (auth_string, plugin) =
                Self::resolve_auth_string_and_plugin(spec.auth.as_ref(), &default_plugin)?;
            let (user, host) = (spec.user.user.as_str(), spec.user.host.as_str());
            if registry.create_user_with_plugin(user, host, &auth_string, &plugin) {
                let (user, host) = (user.to_owned(), host.to_owned());
                self.mirror_grant_created_user_row(&user, &host, &auth_string, &plugin)?;
            }
        }
        Ok(())
    }

    /// `REVOKE <static privs> ON <level> FROM <user>...`. Go's `revoke.go`
    /// requires every named account to already exist (`errors.Errorf("Unknown
    /// user: %s", ...)`, captured); this tier does too.
    fn revoke_stmt(
        &mut self,
        revoke: &tidb_ast::RevokeStmt,
    ) -> Result<(), DriverError> {
        if revoke.object_type.is_some() {
            return Err(DriverError::unsupported(
                "REVOKE ... ON FUNCTION/PROCEDURE is not supported yet",
            ));
        }
        let Some(registry) = self.privilege_registry().cloned() else {
            return Err(DriverError::unsupported(
                "REVOKE requires a server front end with a privilege registry",
            ));
        };
        // Go `RevokeExec.Next` names the session's own account for
        // `CURRENT_USER` (`sessVars.User.AuthUsername`/`AuthHostname`) before
        // it checks the account exists.
        if revoke.users.iter().any(|spec| spec.user.current_user) {
            let mut resolved = revoke.clone();
            for spec in &mut resolved.users {
                if spec.user.current_user {
                    let (user, host) = self.own_account()?;
                    spec.user = tidb_ast::UserSpec {
                        current_user: false,
                        user,
                        host,
                    };
                }
            }
            return self.revoke_stmt(&resolved);
        }
        // Go's `checkDynamicPrivilegeUsage` runs before any row is touched
        // and names EVERY dynamic privilege in the statement, comma-joined,
        // in the one 3619 it raises.
        if !matches!(revoke.level, tidb_ast::GrantLevel::Global) {
            let dynamic: Vec<String> = revoke
                .privileges
                .iter()
                .filter(|privilege| privilege.dynamic)
                .map(|privilege| tidb_hack::go_to_upper(&privilege.name))
                .collect();
            if !dynamic.is_empty() {
                return Err(DriverError::from(
                    tidb_dbterror::exeerrors::ERR_ILLEGAL_PRIVILEGE_LEVEL
                        .generate_with_stack_by_args(&[(dynamic.join(",")).into()]),
                ));
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
                    return Err(DriverError::from(tidb_error::mysql::MysqlError::unknown(
                        format!(
                            "Unknown user: {}@{}",
                            spec.user.user.clone(),
                            spec.user.host.clone()
                        ),
                    )));
                }
            }
            Ok(())
        };
        match &revoke.level {
            tidb_ast::GrantLevel::Global => {
                let (mask, dynamic) = self.split_global_privs(&revoke.privileges, false)?;
                all_revokees_exist(&revoke.users)?;
                let revoke_all_dynamic = revoke
                    .privileges
                    .iter()
                    .any(|privilege| privilege.name == "ALL");
                let unregistered: Vec<String> = dynamic
                    .iter()
                    .filter(|name| !tidb_privilege::is_dynamic_privilege(name))
                    .cloned()
                    .collect();
                for spec in &revoke.users {
                    let user = spec.user.user.as_str();
                    let host = spec.user.host.as_str();
                    if !registry.user_exists(user, host) {
                        return Err(DriverError::from(tidb_error::mysql::MysqlError::unknown(
                            format!("Unknown user: {}@{}", user.to_owned(), host.to_owned()),
                        )));
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
                    // Go `revokeGlobalPriv` / `revokeDynamicPriv`.
                    if revoke_all_dynamic {
                        self.mirror_revoke_all_dynamic(&user, &host)?;
                    }
                    for name in &dynamic {
                        self.mirror_dynamic_priv(&user, &host, name, None)?;
                    }
                }
                // An unregistered name is a WARNING here, not the error
                // `GRANT` raises for it, and the delete still runs
                // (captured: the statement reports OK with a 3929 warning).
                for name in unregistered {
                    self.append_sql_warning(tidb_context::SqlWarn::raised(
                        tidb_context::WarningLevel::Warning,
                        tidb_dbterror::exeerrors::ERR_DYNAMIC_PRIVILEGE_NOT_REGISTERED
                            .generate_with_stack_by_args(&[name.into()]),
                    ));
                }
            }
            tidb_ast::GrantLevel::Database(database) => {
                let database = self.resolve_grant_database(database.as_deref())?;
                let privs = self.resolve_scoped_privs(&revoke.privileges, ScopeKind::Database)?;
                let mask = privs.iter().fold(0u64, |mask, priv_| mask | priv_.bit());
                all_revokees_exist(&revoke.users)?;
                for spec in &revoke.users {
                    let user = spec.user.user.as_str();
                    let host = spec.user.host.as_str();
                    if !registry.user_exists(user, host) {
                        return Err(DriverError::from(tidb_error::mysql::MysqlError::unknown(
                            format!("Unknown user: {}@{}", user.to_owned(), host.to_owned()),
                        )));
                    }
                    if !registry.db_grant_row_exists(user, host, &database) {
                        return Err(DriverError::from(tidb_error::mysql::MysqlError::unknown(format!("There is no such grant defined for user '{}' on host '{}' on database {}", user.to_owned(), host.to_owned(), database.clone()))));
                    }
                    registry.revoke_db(user, host, &database, mask);
                    let (user, host) = (user.to_owned(), host.to_owned());
                    self.mirror_db_privs(&user, &host, &database, mask, false)?;
                }
            }
            tidb_ast::GrantLevel::Table { database, table } => {
                let database = self.resolve_grant_database(database.as_deref())?;
                let TableScopePrivs {
                    table: privs,
                    columns,
                } = self.resolve_table_scope_privs(&revoke.privileges)?;
                let mask = privs.iter().fold(0u64, |mask, priv_| mask | priv_.bit());
                all_revokees_exist(&revoke.users)?;
                for spec in &revoke.users {
                    let user = spec.user.user.as_str();
                    let host = spec.user.host.as_str();
                    if !registry.user_exists(user, host) {
                        return Err(DriverError::from(tidb_error::mysql::MysqlError::unknown(
                            format!("Unknown user: {}@{}", user.to_owned(), host.to_owned()),
                        )));
                    }
                    // Go checks the TABLE row's existence for every account
                    // BEFORE touching any row and before resolving a column
                    // name, so revoking a nonexistent column from an account
                    // with no grant on the table reports this and not
                    // `Unknown column` (captured).
                    if !registry.table_grant_row_exists(user, host, &database, table) {
                        return Err(DriverError::from(tidb_error::mysql::MysqlError::unknown(format!("There is no such grant defined for user '{}' on host '{}' on table {}.{}", user.to_owned(), host.to_owned(), database.clone(), table.clone()))));
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
                    let (user, host) = (user.to_owned(), host.to_owned());
                    self.mirror_table_privs(&user, &host, &database, table, &privs, false, false)?;
                    self.mirror_column_privs(&user, &host, &database, table, &columns, false)?;
                }
            }
        }
        Ok(())
    }

    /// Resolves a DB/TABLE-scope `GRANT`/`REVOKE`'s database qualifier: the
    /// written name, or (Go's `getTargetSchemaName`) the session's current
    /// database when the statement wrote a bare `*`/table name.
    fn resolve_grant_database(
        &self,
        database: Option<&str>,
    ) -> Result<String, DriverError> {
        match database {
            Some(database) => Ok(database.to_owned()),
            None if !self.session_vars().current_db.is_empty() => {
                Ok(self.session_vars().current_db.clone())
            }
            None => Err(DriverError::from(
                tidb_error::plannererrors::ERR_NO_DB.generate_with_stack_by_args(&[]),
            )),
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
                "ALL" => tidb_privilege::all_column_privs_mask(),
                "USAGE" => 0,
                _ => {
                    let priv_ = tidb_privilege::privilege_from_grant_name(&privilege.name)
                        .filter(|priv_| priv_.is_valid_at_column_scope())
                        .ok_or(DriverError::from(
                            tidb_dbterror::exeerrors::ERR_WRONG_USAGE
                                .generate_with_stack_by_args(&[
                                    "COLUMN GRANT".into(),
                                    "NON-COLUMN PRIVILEGES".into(),
                                ]),
                        ))?;
                    priv_.bit()
                }
            };
            for column in &privilege.columns {
                match result
                    .columns
                    .iter_mut()
                    .find(|(named, _)| named.go_equal_fold(column))
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
            .map(crate::TableEntry::column_names)
            .unwrap_or_default();
        columns
            .iter()
            .map(|(column, mask)| {
                names
                    .iter()
                    .find(|name| name.go_equal_fold(column))
                    .map(|name| (name.clone(), *mask))
                    .ok_or_else(|| {
                        DriverError::from(tidb_error::mysql::MysqlError::unknown(format!(
                            "Unknown column: {}",
                            column.clone()
                        )))
                    })
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
    ) -> Result<Vec<tidb_mysql::PrivilegeType>, DriverError> {
        let all_scoped: &[tidb_mysql::PrivilegeType] = match scope {
            ScopeKind::Database => tidb_mysql::ALL_DB_PRIVS,
            ScopeKind::Table => tidb_mysql::ALL_TABLE_PRIVS,
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
            if !privilege.dynamic && privilege.name == "USAGE" {
                // Go grantLevelPriv: no privilege bit, but REQUIRE still
                // updates the account through grantGlobalPriv.
                continue;
            }
            if privilege.dynamic {
                return Err(DriverError::from(
                    tidb_dbterror::exeerrors::ERR_ILLEGAL_PRIVILEGE_LEVEL
                        .generate_with_stack_by_args(&[privilege.name.clone().into()]),
                ));
            }
            let Some(priv_) = tidb_privilege::privilege_from_grant_name(&privilege.name) else {
                return Err(DriverError::from(
                    tidb_dbterror::exeerrors::ERR_DYNAMIC_PRIVILEGE_NOT_REGISTERED
                        .generate_with_stack_by_args(&[privilege.name.clone().into()]),
                ));
            };
            let valid = match scope {
                ScopeKind::Database => priv_.is_valid_at_db_scope(),
                ScopeKind::Table => priv_.is_valid_at_table_scope(),
            };
            if !valid {
                return Err(match scope {
                    ScopeKind::Database => DriverError::from(
                        tidb_dbterror::exeerrors::ERR_WRONG_USAGE
                            .generate_with_stack_by_args(&[
                                "DB GRANT".into(),
                                "GLOBAL PRIVILEGES".into(),
                            ]),
                    ),
                    ScopeKind::Table => DriverError::from(
                        tidb_dbterror::exeerrors::ERR_ILLEGAL_GRANT_FOR_TABLE
                            .generate_with_stack_by_args(&[]),
                    ),
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
    /// see [`tidb_privilege::PrivilegeRegistry::revoke_all_dynamic`].)
    ///
    /// `reject_unregistered` distinguishes the two consumers: `GRANT` fails
    /// with `ErrDynamicPrivilegeNotRegistered`/3929 on an unknown name,
    /// while `REVOKE` only WARNS with the same error and proceeds, so it
    /// asks for the names unfiltered and warns itself.
    fn split_global_privs(
        &self,
        privileges: &[tidb_ast::GrantPrivilege],
        reject_unregistered: bool,
    ) -> Result<(u64, Vec<String>), DriverError> {
        let mut mask = 0u64;
        let mut dynamic = Vec::new();
        for privilege in privileges {
            if privilege.name == "ALL" {
                // Go composeGlobalPrivUpdate expands mysql.AllGlobalPrivs,
                // including the static CREATE ROLE and DROP ROLE columns.
                mask |= tidb_privilege::all_privs_mask();
                continue;
            }
            if !privilege.columns.is_empty() {
                return Err(DriverError::unsupported(
                    "GRANT/REVOKE with a column list is not supported yet",
                ));
            }
            if privilege.dynamic {
                if reject_unregistered && !tidb_privilege::is_dynamic_privilege(&privilege.name) {
                    return Err(DriverError::from(
                        tidb_dbterror::exeerrors::ERR_DYNAMIC_PRIVILEGE_NOT_REGISTERED
                            .generate_with_stack_by_args(&[privilege.name.clone().into()]),
                    ));
                }
                dynamic.push(tidb_hack::go_to_upper(&privilege.name));
                continue;
            }
            if privilege.name == "USAGE" {
                continue;
            }
            match tidb_privilege::privilege_from_grant_name(&privilege.name) {
                Some(priv_) => mask |= priv_.bit(),
                None => {
                    return Err(DriverError::from(
                        tidb_dbterror::exeerrors::ERR_DYNAMIC_PRIVILEGE_NOT_REGISTERED
                            .generate_with_stack_by_args(&[privilege.name.clone().into()]),
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
    fn show_grants_rows(
        &mut self,
        show_user: Option<&tidb_ast::UserSpec>,
        show_roles: &[tidb_ast::UserSpec],
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        let is_own = match show_user {
            None => true,
            Some(spec) => {
                spec.current_user
                    || self
                        .current_identity()
                        .is_some_and(|(user, host)| spec.user == user && spec.host == host)
            }
        };
        let (user, host) = match show_user {
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
            if let Some(spec) = show_roles.first() {
                let role_host = if spec.host.is_empty() {
                    "%".to_owned()
                } else {
                    spec.host.clone()
                };
                return Err(DriverError::from(
                    tidb_dbterror::exeerrors::ERR_ROLE_NOT_GRANTED
                        .generate_with_stack_by_args(&[
                            format!("`{}`@`{}`", spec.user.clone(), role_host).into(),
                            format!("{}@{}", user, host).into(),
                        ]),
                ));
            }
            return Err(DriverError::from(
                tidb_error::catalog::privileges::ERR_NONEXISTING_GRANT
                    .fast_generate_by_args(&["root".into(), "%".into()]),
            ));
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
                tidb_mysql::SelectPriv,
            )
        {
            let (caller, caller_host) = self.own_account()?;
            return Err(DriverError::from(
                tidb_error::plannererrors::ERR_DBACCESS_DENIED.generate_with_stack_by_args(&[
                    caller.into(),
                    caller_host.into(),
                    (tidb_mysql::consts::SystemDB.to_owned()).into(),
                ]),
            ));
        }
        let Some(registry) = self.privilege_registry().cloned() else {
            return Err(DriverError::unsupported(
                "SHOW GRANTS requires a server front end with a privilege registry",
            ));
        };
        // Go `fetchShowGrants` merges the session's active roles only for a
        // bare SHOW GRANTS or FOR CURRENT_USER(); naming the account -- even
        // one's own -- shows its own grants.
        let names_current_user = show_user.is_none_or(|spec| spec.current_user);
        let roles = if show_roles.is_empty() {
            if names_current_user {
                self.session_vars().active_roles.to_vec()
            } else {
                Vec::new()
            }
        } else {
            // `USING` names roles through the account grammar, so the same
            // omitted-host default applies.
            show_roles
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
        // Go `fetchShowGrants`: every role the output merges -- the USING
        // list, or the session's active roles -- must be granted to the
        // account (`FindEdge`).
        let account = (user.clone(), host.clone());
        for role in &roles {
            if !registry.has_role(&account, role) {
                return Err(DriverError::from(
                    tidb_dbterror::exeerrors::ERR_ROLE_NOT_GRANTED
                        .generate_with_stack_by_args(&[
                            format!("`{}`@`{}`", role.0.clone(), role.1.clone()).into(),
                            format!("{}@{}", user, host).into(),
                        ]),
                ));
            }
        }
        let Some(lines) =
            registry.show_grants(&user, &host, &roles, self.session_vars().sql_mode())
        else {
            return Err(DriverError::from(
                tidb_error::catalog::privileges::ERR_NONEXISTING_GRANT
                    .fast_generate_by_args(&[user.into(), host.into()]),
            ));
        };
        // Go `fetchShowGrants`: one row per GLOBAL/DB/TABLE-scope line, in
        // that order (`registry.show_grants`'s captured ordering), under the
        // `Grants for <User>` column `buildShowSchema` names.
        Ok(lines
            .split('\n')
            .map(|line| vec![Datum::Bytes(line.as_bytes().to_vec())])
            .collect())
    }
}

impl<S: super::account_tables::AccountSession > AccountExec for S {}
