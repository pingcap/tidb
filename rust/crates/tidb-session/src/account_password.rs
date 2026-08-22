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

//! The password half of account management: `ALTER USER` (Go
//! `executeAlterUser`, MySQL 8.0 dual passwords included), `SET PASSWORD`
//! (Go `executeSetPwd`), and `SHOW CREATE USER` (Go `fetchShowCreateUser`).
//!
//! Split from `crate::account` along Go's own seam -- these three all read
//! and write the SAME per-account credential state (`authentication_string`,
//! `plugin`, the `User_attributes` dual-password/metadata JSON the
//! `crate::user_table` mirror maintains) -- so the account-list statements
//! (`CREATE`/`DROP`/`RENAME USER`, grants, roles) stay in `account.rs` and
//! the credential statements live here.

use crate::account::{ssl_type_of, PasswordOrLockOptions};
use crate::show::string_column_output;
use crate::*;

impl Session {
    /// `ALTER USER [IF EXISTS] <account | USER()> [IDENTIFIED [WITH
    /// '<plugin>'] BY '<password>' [RETAIN CURRENT PASSWORD] | DISCARD OLD
    /// PASSWORD] [ACCOUNT LOCK | UNLOCK] [PASSWORD ...] [COMMENT|ATTRIBUTE]`,
    /// Go `executeAlterUser`: a password/plugin change rewrites the account's
    /// `mysql.user.authentication_string` (and `plugin`), the MySQL 8.0
    /// dual-password clauses maintain `User_attributes ->
    /// '$.additional_password'`, and a COMMENT/ATTRIBUTE clause merges into
    /// `$.metadata`. Resource limits, RESOURCE GROUP, and the `PASSWORD
    /// HISTORY`/`REUSE`/`REQUIRE CURRENT` policies remain unsupported.
    pub(crate) fn alter_user_stmt(
        &mut self,
        alter: &tidb_ast::AlterUserStmt,
    ) -> Result<StmtOutput, DriverError> {
        if !alter.resource_options.is_empty() || alter.resource_group.is_some() {
            return Err(DriverError::unsupported(
                "ALTER USER resource limits / RESOURCE GROUP are not supported yet",
            ));
        }
        let options = PasswordOrLockOptions::load(&alter.password_options)?;
        let ssl_type = ssl_type_of(&alter.tls_options)?;
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::unsupported(
                "ALTER USER requires a server front end with a privilege registry",
            ));
        };
        // Go `executeAlterUser`'s USER() preamble: `CurrentAuth` /
        // `CurrentDualPasswordOption` collapse into ONE synthetic spec keyed
        // on the AUTHENTICATED account, so the per-spec loop below sees the
        // `ALTER USER USER() ...` forms as an ordinary self-targeting spec.
        let synthetic_specs;
        let specs: &[tidb_ast::CreateUserSpec] =
            if alter.user_function_auth.is_some() || alter.user_function_dual_password.is_some() {
                let (user, host) = self.own_account()?;
                synthetic_specs = [tidb_ast::CreateUserSpec {
                    user: tidb_ast::UserSpec {
                        current_user: false,
                        user,
                        host,
                    },
                    auth: alter
                        .user_function_auth
                        .clone()
                        .map(tidb_ast::CreateUserAuth::By),
                    dual_password: alter.user_function_dual_password,
                }];
                &synthetic_specs
            } else {
                &alter.users
            };
        // Go `alterUserHasPrivilegedOptions`: statement-level options beyond
        // the per-spec password / dual-password clauses. Resource limits and
        // RESOURCE GROUP -- the other two members of Go's allowlist -- were
        // refused above, so the three that can still be present are these.
        let has_other_stmt_options = !alter.tls_options.is_empty()
            || !alter.password_options.is_empty()
            || alter.comment_or_attribute.is_some();
        // Go resolves `default_authentication_plugin` once per statement and
        // treats a read failure as "" (-> mysql_native_password); it feeds
        // `effectiveAuthPlugin`, which normalizes plugin names for the
        // plugin-change comparisons below.
        let default_plugin = self
            .vars
            .get_global("default_authentication_plugin")
            .unwrap_or_default();
        let effective = |plugin: &str| -> String {
            if !plugin.is_empty() {
                plugin.to_owned()
            } else if default_plugin.is_empty() {
                tidb_mysql::consts::AuthNativePassword.to_owned()
            } else {
                default_plugin.clone()
            }
        };
        for spec in specs {
            // Go `dualPasswordOption`.
            let (spec_retain, spec_discard) = match spec.dual_password {
                Some(tidb_ast::AlterUserDualPassword::RetainCurrent) => (true, false),
                Some(tidb_ast::AlterUserDualPassword::DiscardOld) => (false, true),
                None => (false, false),
            };
            let spec_dual_requested = spec_retain || spec_discard;
            let (user, host) = self.resolve_account(&spec.user)?;
            let alter_current_user = spec.user.current_user || self.is_own_account(&user, &host);
            // Go's `alterPassword`: a bare self password change --
            // `IDENTIFIED BY` with no plugin change and no statement-level
            // option -- which MySQL allows without CREATE USER.
            let alter_password = matches!(spec.auth, Some(tidb_ast::CreateUserAuth::By(_)))
                && !has_other_stmt_options;
            // Go reads the target row BEFORE the privilege gate (the
            // "no plugin change" classification needs the current plugin);
            // the missing-user outcome still surfaces only after the checks.
            let exists = registry.user_exists(&user, &host);
            let current_plugin = registry.plugin(&user, &host).unwrap_or_default();
            let current_auth_string = registry.auth_string(&user, &host).unwrap_or_default();
            let same_or_unspecified_plugin = match &spec.auth {
                None | Some(tidb_ast::CreateUserAuth::By(_)) => true,
                Some(tidb_ast::CreateUserAuth::With { plugin, .. }) => {
                    effective(plugin) == effective(&current_plugin)
                }
            };
            // Go's `selfServiceDualPwd`: a dual-password change to the
            // caller's OWN account with no other privileged option and no
            // plugin change is governed by APPLICATION_PASSWORD_ADMIN, not
            // the CREATE USER admin check.
            let self_service_dual = alter_current_user
                && spec_dual_requested
                && !has_other_stmt_options
                && same_or_unspecified_plugin;
            let need_admin_check = !(alter_current_user && alter_password) && !self_service_dual;
            if need_admin_check {
                // Static half of Go's gate (CREATE USER, or UPDATE on the
                // mysql schema); the SYSTEM_USER half runs after the exists
                // test below, in Go's own order.
                if !(self.has_scoped_privilege("", "", privilege::GlobalPriv::CreateUser)
                    || self.has_scoped_privilege(
                        tidb_mysql::consts::SystemDB,
                        tidb_mysql::consts::UserTable,
                        privilege::GlobalPriv::Update,
                    ))
                {
                    return Err(DriverError::SpecificAccessDenied("CREATE USER".to_owned()));
                }
            }
            // Go: self-service dual-password additionally requires
            // APPLICATION_PASSWORD_ADMIN (CREATE USER / UPDATE-mysql being
            // accepted as supersets).
            if self_service_dual
                && !(self.has_scoped_privilege("", "", privilege::GlobalPriv::CreateUser)
                    || self.has_scoped_privilege(
                        tidb_mysql::consts::SystemDB,
                        tidb_mysql::consts::UserTable,
                        privilege::GlobalPriv::Update,
                    )
                    || self.has_dynamic_privilege("APPLICATION_PASSWORD_ADMIN", false))
            {
                return Err(DriverError::SpecificAccessDenied(
                    "APPLICATION_PASSWORD_ADMIN".to_owned(),
                ));
            }
            if !exists {
                if alter.if_exists {
                    // Go collects the miss into `failedUsers` and reports it
                    // as a note under IF EXISTS; the skip is the observable
                    // part.
                    continue;
                }
                return Err(DriverError::AlterUserMissing { user, host });
            }
            if need_admin_check {
                self.require_system_user_privilege_over(&user, &host)?;
            }
            // Go's RETAIN CURRENT PASSWORD validation, in its order: plugin
            // capability, then "a new password must be set", then "same
            // plugin", then "and it must be non-empty".
            if spec_retain {
                let resolved = effective(&current_plugin);
                if !tidb_mysql::is_auth_plugin_clear_text(&resolved) {
                    return Err(DriverError::DualPasswordUnsupportedForPlugin { plugin: resolved });
                }
                match &spec.auth {
                    None => {
                        return Err(DriverError::CurrentPasswordCannotBeRetained { user, host });
                    }
                    Some(tidb_ast::CreateUserAuth::By(password)) => {
                        if password.is_empty() {
                            return Err(DriverError::CurrentPasswordCannotBeRetained {
                                user,
                                host,
                            });
                        }
                    }
                    Some(tidb_ast::CreateUserAuth::With { plugin, credential }) => {
                        let Some(credential) = credential else {
                            // `IDENTIFIED WITH p` alone carries no new
                            // password (Go: neither ByAuthString nor
                            // ByHashString).
                            return Err(DriverError::CurrentPasswordCannotBeRetained {
                                user,
                                host,
                            });
                        };
                        if !plugin.is_empty() && effective(plugin) != resolved {
                            return Err(DriverError::PasswordCannotBeRetainedOnPluginChange {
                                user,
                                host,
                            });
                        }
                        let text = match credential {
                            tidb_ast::CreateUserCredential::By(text)
                            | tidb_ast::CreateUserCredential::As(text) => text,
                        };
                        if text.is_empty() {
                            return Err(DriverError::CurrentPasswordCannotBeRetained {
                                user,
                                host,
                            });
                        }
                    }
                }
            }
            // The `mysql.user` UPDATE Go composes as `fields`; rendered SQL
            // fragments in Go's own order.
            let mut mirror_fields: Vec<String> = Vec::new();
            let mut plugin_changed = false;
            if let Some(auth) = spec.auth.as_ref() {
                mirror_fields.push("password_last_changed=current_timestamp()".to_owned());
                // A bare `IDENTIFIED BY` (no `WITH <plugin>`) keeps the
                // account's CURRENT plugin (Go backfills
                // `spec.AuthOpt.AuthPlugin` from `currentAuthPlugin` rather
                // than resetting it to `mysql_native_password`); only an
                // explicit `IDENTIFIED WITH` changes it.
                let (auth_string, plugin, plaintext) = match auth {
                    tidb_ast::CreateUserAuth::By(password) => {
                        let current_plugin = if current_plugin.is_empty() {
                            tidb_mysql::consts::AuthNativePassword.to_owned()
                        } else {
                            current_plugin.clone()
                        };
                        (
                            privilege::encode_password_for_plugin(
                                &current_plugin,
                                &privilege::PluginCredential::By(password),
                            )?,
                            current_plugin,
                            Some(password.as_str()),
                        )
                    }
                    tidb_ast::CreateUserAuth::With { credential, .. } => {
                        let (auth_string, plugin) = Self::resolve_auth_string_and_plugin(
                            Some(auth),
                            tidb_mysql::consts::AuthNativePassword,
                        )?;
                        let plaintext = match credential {
                            Some(tidb_ast::CreateUserCredential::By(password)) => {
                                Some(password.as_str())
                            }
                            None | Some(tidb_ast::CreateUserCredential::As(_)) => None,
                        };
                        (auth_string, plugin, plaintext)
                    }
                };
                plugin_changed = effective(&plugin) != effective(&current_plugin);
                if plaintext.is_some_and(|_| tidb_mysql::is_auth_plugin_clear_text(&plugin)) {
                    self.validate_password_if_enabled(plaintext.expect("checked above"))?;
                }
                // Go `buildAdditionalPasswordEntry`'s gate, raised while the
                // fields are composed and therefore BEFORE any row is
                // touched: an empty current primary cannot become the
                // secondary.
                if spec_retain && current_auth_string.is_empty() {
                    return Err(DriverError::SecondPasswordCannotBeEmpty { user, host });
                }
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
                mirror_fields.push(format!(
                    "authentication_string={}",
                    crate::user_table::sql_str(&auth_string)
                ));
                mirror_fields.push(format!("plugin={}", crate::user_table::sql_str(&plugin)));
            }
            // Go applies the statement's options in the same UPDATE that
            // writes the password, so a statement doing both lands both.
            options.apply(&registry, &user, &host);
            if let Some(locked) = options.locked {
                mirror_fields.push(format!(
                    "account_locked='{}'",
                    if locked { "Y" } else { "N" }
                ));
            }
            // Go's `plOptions.passwordExpired`: `'Y'` for a bare `PASSWORD
            // EXPIRE`, `'N'` backfilled when the spec stores a new password
            // and no expire clause was written; the lifetime column moves
            // only for the DEFAULT/NEVER/INTERVAL forms.
            match options.expire {
                Some(privilege::PasswordExpireSetting::Now) => {
                    mirror_fields.push("password_expired='Y'".to_owned());
                }
                _ => {
                    if spec.auth.is_some() {
                        mirror_fields.push("password_expired='N'".to_owned());
                    }
                }
            }
            match options.expire {
                Some(privilege::PasswordExpireSetting::Default) => {
                    mirror_fields.push("password_lifetime=null".to_owned());
                }
                Some(privilege::PasswordExpireSetting::Never) => {
                    mirror_fields.push("password_lifetime=0".to_owned());
                }
                Some(privilege::PasswordExpireSetting::Interval(days)) => {
                    mirror_fields.push(format!("password_lifetime={days}"));
                }
                Some(privilege::PasswordExpireSetting::Now) | None => {}
            }
            // Go's `newAttributes`: COMMENT/ATTRIBUTE metadata, then the
            // RETAIN secondary. (`$.Password_locking` is deliberately not
            // mirrored -- see `crate::user_table`'s module doc.)
            let mut new_attributes: Vec<String> = Vec::new();
            if let Some(annotation) = &alter.comment_or_attribute {
                match annotation {
                    tidb_ast::CreateUserCommentOrAttribute::Comment(text) => {
                        new_attributes.push(format!("\"metadata\": {{\"comment\": \"{text}\"}}"))
                    }
                    tidb_ast::CreateUserCommentOrAttribute::Attribute(json) => {
                        new_attributes.push(format!("\"metadata\": {json}"));
                    }
                }
            }
            if spec_retain {
                // Go `buildAdditionalPasswordEntry`: the pre-change primary
                // hash, JSON-encoded (`json.Marshal`).
                new_attributes.push(format!(
                    "\"additional_password\": {}",
                    crate::user_table::json_string_literal(&current_auth_string)
                ));
            }
            // Go: DISCARD removes the secondary, and a plugin change drops
            // it silently; RETAIN always writes a fresh one, so it wins.
            let drop_secondary = (spec_discard || plugin_changed) && !spec_retain;
            // Go emits ONE `user_attributes` assignment so merge-then-remove
            // is a single SQL expression, and the DISCARD-only form collapses
            // a now-empty object back to NULL (not `'{}'`) via NULLIF.
            match (new_attributes.is_empty(), drop_secondary) {
                (false, true) => {
                    let object = format!("{{{}}}", new_attributes.join(","));
                    mirror_fields.push(format!(
                        "user_attributes=json_remove(json_merge_patch(coalesce(user_attributes, \
                         '{{}}'), {}), '$.additional_password')",
                        crate::user_table::sql_str(&object)
                    ));
                }
                (false, false) => {
                    let object = format!("{{{}}}", new_attributes.join(","));
                    mirror_fields.push(format!(
                        "user_attributes=json_merge_patch(coalesce(user_attributes, '{{}}'), {})",
                        crate::user_table::sql_str(&object)
                    ));
                }
                (true, true) => {
                    mirror_fields.push(
                        "user_attributes=nullif(json_remove(coalesce(user_attributes, '{}'), \
                         '$.additional_password'), cast('{}' as json))"
                            .to_owned(),
                    );
                }
                (true, false) => {}
            }
            // Go REPLACES the whole `mysql.global_priv` PRIV JSON when the
            // statement carries any `REQUIRE` clause, and leaves the row
            // untouched when it carries none -- so `ALTER USER ... REQUIRE
            // NONE` clears an earlier `REQUIRE SSL` (captured:
            // `{"ssl_type":1}` becomes `{}`) while a password-only ALTER
            // keeps it.
            if !alter.tls_options.is_empty() {
                registry.set_ssl_type(&user, &host, ssl_type);
            }
            if !mirror_fields.is_empty() && self.user_table_present() {
                let sql = format!(
                    "UPDATE mysql.user SET {} WHERE Host={} and User={}",
                    mirror_fields.join(","),
                    crate::user_table::sql_str(&host),
                    crate::user_table::sql_str(&user),
                );
                self.run_user_table_write(&sql)?;
            }
        }
        // A sandboxed session escapes by giving ITSELF a new password, which
        // is the only thing it was allowed in here to do (Go's
        // `executeAlterUser` -> `checkSandboxMode`, whose gate ran before the
        // statement reached this driver).
        if self.sandbox_mode
            && (alter.user_function_auth.is_some()
                || alter.users.iter().any(|spec| spec.auth.is_some()))
        {
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
        // Go `executor/show.go`'s `fetchShowCreateUser` (around line 1873):
        // this statement renders `IDENTIFIED WITH '<plugin>' AS '<hash>'`,
        // so naming ANOTHER account needs `SELECT` on `mysql.user` -- the
        // privilege that would let the caller read the stored hash directly.
        if !spec.current_user
            && !self.is_own_account(&user, &host)
            && !self.has_scoped_privilege(
                tidb_mysql::consts::SystemDB,
                tidb_mysql::consts::UserTable,
                privilege::GlobalPriv::Select,
            )
        {
            let (caller, caller_host) = self.own_account()?;
            return Err(DriverError::TableAccessDenied {
                privilege: "SELECT",
                user: caller,
                host: caller_host,
                table: tidb_mysql::consts::UserTable.to_owned(),
            });
        }
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::unsupported(
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
        // Go's `fetchShowCreateUser` reads the `mysql.global_priv` PRIV
        // JSON's `ssl_type` for this clause (captured: `REQUIRE SSL` for an
        // account created with it, `REQUIRE NONE` for one without).
        let require_clause = registry.ssl_type(&user, &host).show_create_user_clause();
        let show_str = format!(
            "CREATE USER '{user}'@'{host}' IDENTIFIED WITH '{plugin}'{auth_clause} REQUIRE {require_clause} {expire_clause} ACCOUNT {account_clause} PASSWORD HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT{locking_clause}"
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
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::unsupported(
                "SET PASSWORD requires a server front end with a privilege registry",
            ));
        };
        let (user, host) = match &set_password.user {
            Some(spec) => self.resolve_account(spec)?,
            None => self.own_account()?,
        };
        let set_for_self = self.is_own_account(&user, &host)
            || set_password
                .user
                .as_ref()
                .is_none_or(|spec| spec.current_user);
        // Go `executeSetPwd` (`executor/simple.go` around line 2905):
        // changing ANOTHER account's password is TiDB's long-standing
        // SUPER-only operation, and the refusal names the `mysql` schema
        // rather than the privilege. APPLICATION_PASSWORD_ADMIN covers only
        // SELF-account secondary passwords, so RETAIN must not relax this
        // check (the corpus's `dpsetap` case: 1044, not success).
        if !set_for_self && !self.has_scoped_privilege("", "", privilege::GlobalPriv::Super) {
            let (caller, caller_host) = self.own_account()?;
            return Err(DriverError::DbAccessDenied {
                user: caller,
                host: caller_host,
                database: tidb_mysql::consts::SystemDB.to_owned(),
            });
        }
        // Go: self-service `SET PASSWORD ... RETAIN CURRENT PASSWORD`
        // requires APPLICATION_PASSWORD_ADMIN (CREATE USER / UPDATE on the
        // mysql schema suffice as supersets), matching `executeAlterUser`'s
        // self-service dual-password gate.
        if set_for_self
            && set_password.retain_current_password
            && !(self.has_scoped_privilege("", "", privilege::GlobalPriv::CreateUser)
                || self.has_dynamic_privilege("APPLICATION_PASSWORD_ADMIN", false)
                || self.has_scoped_privilege(
                    tidb_mysql::consts::SystemDB,
                    tidb_mysql::consts::UserTable,
                    privilege::GlobalPriv::Update,
                ))
        {
            return Err(DriverError::SpecificAccessDenied(
                "APPLICATION_PASSWORD_ADMIN".to_owned(),
            ));
        }
        let Some(plugin) = registry.plugin(&user, &host) else {
            return Err(DriverError::SetPasswordNoMatchingRow);
        };
        let current_auth_string = registry.auth_string(&user, &host).unwrap_or_default();
        // Go's RETAIN validation for SET PASSWORD: plugin capability, then
        // "the new password must be non-empty". The registry's plugin column
        // is always concrete, so `effectiveAuthPlugin`'s legacy empty-column
        // resolution has nothing to resolve here.
        if set_password.retain_current_password {
            if !tidb_mysql::is_auth_plugin_clear_text(&plugin) {
                return Err(DriverError::DualPasswordUnsupportedForPlugin { plugin });
            }
            if set_password.password.is_empty() {
                return Err(DriverError::CurrentPasswordCannotBeRetained { user, host });
            }
        }
        self.validate_password_if_enabled(&set_password.password)?;
        let auth_string = privilege::encode_password_for_plugin(
            &plugin,
            &privilege::PluginCredential::By(&set_password.password),
        )?;
        // Go `buildAdditionalPasswordEntry`'s gate, before the UPDATE runs:
        // an empty current primary cannot be retained as the secondary.
        if set_password.retain_current_password && current_auth_string.is_empty() {
            return Err(DriverError::SecondPasswordCannotBeEmpty { user, host });
        }
        if !registry.set_auth_string(&user, &host, &auth_string) {
            return Err(DriverError::SetPasswordNoMatchingRow);
        }
        // Same UPDATE as `ALTER USER ... IDENTIFIED BY`: a stored password
        // is an unexpired password.
        registry.mark_password_changed(&user, &host);
        // Go's UPDATE against mysql.user, with the RETAIN form promoting the
        // old primary into `$.additional_password` in the SAME statement.
        if self.user_table_present() {
            let attrs_clause = if set_password.retain_current_password {
                let object = format!(
                    "{{\"additional_password\": {}}}",
                    crate::user_table::json_string_literal(&current_auth_string)
                );
                format!(
                    ",user_attributes=json_merge_patch(coalesce(user_attributes, '{{}}'), {})",
                    crate::user_table::sql_str(&object)
                )
            } else {
                String::new()
            };
            let sql = format!(
                "UPDATE mysql.user SET authentication_string={},password_expired='N',\
                 password_last_changed=current_timestamp(){attrs_clause} WHERE User={} AND \
                 Host={}",
                crate::user_table::sql_str(&auth_string),
                crate::user_table::sql_str(&user),
                crate::user_table::sql_str(&host.to_lowercase()),
            );
            self.run_user_table_write(&sql)?;
        }
        self.sandbox_mode = false;
        Ok(StmtOutput::Affected(0))
    }
}
