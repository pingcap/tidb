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

use tidb_datatype::Datum;

use super::account::{tls_policy_of, PasswordOrLockOptions};
use crate::DriverError;
use tidb_hack::GoToLower;
use tidb_hack::go_to_lower;

/// The `mysql.user` columns Go `fetchShowCreateUser` reads, in its order.
pub struct ShowCreateUserRow {
    plugin: String,
    account_locked: bool,
    /// `user_attributes->>'$.metadata'`.
    metadata: String,
    token_issuer: String,
    password_reuse_history: Option<String>,
    password_reuse_time: Option<String>,
    password_expired: bool,
    /// `-1` for NULL.
    password_lifetime: i64,
    failed_login_attempts: String,
    password_lock_time_days: String,
    auth_data: String,
    max_user_connections: i64,
}

/// Go's executor arms, over the session [`super::account_tables::AccountSession`] gives them.
pub trait AccountPasswordExec: super::account::AccountExec {
    /// The per-account admission gate of Go `executeAlterUser`: whether the
    /// statement needs the CREATE USER administrator check for this account,
    /// and the refusal when the caller lacks it. It runs for every spec,
    /// including the accounts that turn out not to exist, because Go checks
    /// privileges before it reports a missing account.
    #[allow(clippy::too_many_arguments)]
    fn alter_user_admin_gate(
        &self,
        registry: &tidb_privilege::PrivilegeRegistry,
        spec: &tidb_ast::CreateUserSpec,
        user: &str,
        host: &str,
        has_other_stmt_options: bool,
        effective: &dyn Fn(&str) -> String,
    ) -> Result<bool, DriverError> {
        let (spec_retain, spec_discard) = match spec.dual_password {
            Some(tidb_ast::AlterUserDualPassword::RetainCurrent) => (true, false),
            Some(tidb_ast::AlterUserDualPassword::DiscardOld) => (false, true),
            None => (false, false),
        };
        let spec_dual_requested = spec_retain || spec_discard;
        let current_plugin = registry.plugin(user, host).unwrap_or_default();
            let alter_current_user = spec.user.current_user || self.is_own_account(user, host);
            // Go's `alterPassword`: a bare self password change --
            // `IDENTIFIED BY` with no plugin change and no statement-level
            // option -- which MySQL allows without CREATE USER.
            let alter_password = matches!(spec.auth, Some(tidb_ast::CreateUserAuth::By(_)))
                && !has_other_stmt_options;
            // Go reads the target row BEFORE the privilege gate (the
            // "no plugin change" classification needs the current plugin);
            // the missing-user outcome still surfaces only after the checks.
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
                if !(self.has_scoped_privilege("", "", tidb_mysql::CreateUserPriv)
                    || self.has_scoped_privilege(
                        tidb_mysql::consts::SystemDB,
                        tidb_mysql::consts::UserTable,
                        tidb_mysql::UpdatePriv,
                    ))
                {
                    return Err(DriverError::from(tidb_error::plannererrors::ERR_SPECIFIC_ACCESS_DENIED.generate_with_stack_by_args(&["CREATE USER".into()])));
                }
            }
            // Go: self-service dual-password additionally requires
            // APPLICATION_PASSWORD_ADMIN (CREATE USER / UPDATE-mysql being
            // accepted as supersets).
            if self_service_dual
                && !(self.has_scoped_privilege("", "", tidb_mysql::CreateUserPriv)
                    || self.has_scoped_privilege(
                        tidb_mysql::consts::SystemDB,
                        tidb_mysql::consts::UserTable,
                        tidb_mysql::UpdatePriv,
                    )
                    || self.has_dynamic_privilege("APPLICATION_PASSWORD_ADMIN", false))
            {
                return Err(DriverError::from(tidb_error::plannererrors::ERR_SPECIFIC_ACCESS_DENIED.generate_with_stack_by_args(&["APPLICATION_PASSWORD_ADMIN".into()])));
            }
        Ok(need_admin_check)
    }

    /// `ALTER USER [IF EXISTS] <account | USER()> [IDENTIFIED [WITH
    /// '<plugin>'] BY '<password>' [RETAIN CURRENT PASSWORD] | DISCARD OLD
    /// PASSWORD] [ACCOUNT LOCK | UNLOCK] [PASSWORD ...] [COMMENT|ATTRIBUTE]`,
    /// Go `executeAlterUser`: a password/plugin change rewrites the account's
    /// `mysql.user.authentication_string` (and `plugin`), the MySQL 8.0
    /// dual-password clauses maintain `User_attributes ->
    /// '$.additional_password'`, and a COMMENT/ATTRIBUTE clause merges into
    /// `$.metadata`, and RESOURCE GROUP records `$.resource_group`. Resource
    /// limits and the `PASSWORD HISTORY`/`REUSE`/`REQUIRE CURRENT` policies
    /// remain unsupported.
    fn alter_user_stmt(
        &mut self,
        alter: &tidb_ast::AlterUserStmt,
    ) -> Result<(), DriverError> {
        let options = PasswordOrLockOptions::load(&alter.password_options)?;
        let tls_policy = tls_policy_of(&alter.tls_options)?;
        let Some(registry) = self.privilege_registry().cloned() else {
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
            .session_vars()
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
        // Go runs the statement in one internal transaction: a missing
        // account is collected and the loop continues, and a statement
        // without IF EXISTS then fails with every collected account named
        // (1396) and rolls back what the others changed -- so nothing is
        // written when any account is missing.
        if !alter.if_exists {
            let mut missing = Vec::new();
            for spec in specs {
                let (user, host) = self.resolve_account(&spec.user)?;
                if !registry.user_exists(&user, &host) {
                    // Go checks the caller's privileges before it reports an
                    // account as missing.
                    self.alter_user_admin_gate(
                        &registry,
                        spec,
                        &user,
                        &host,
                        has_other_stmt_options,
                        &effective,
                    )?;
                    missing.push((user, host));
                }
            }
            match missing.len() {
                0 => {}
                1 => {
                    let (user, host) = missing.remove(0);
                    return Err(DriverError::from(tidb_util::dbterror::exeerrors::ERR_CANNOT_USER.generate_with_stack_by_args(&["ALTER USER".into(), format!("'{}'@'{}'", user, host).into()])));
                }
                _ => {
                    return Err(DriverError::from(tidb_util::dbterror::exeerrors::ERR_CANNOT_USER.generate_with_stack_by_args(&["ALTER USER".into(), (missing
                            .iter()
                            .map(|(user, host)| format!("'{user}'@'{host}'"))
                            .collect::<Vec<_>>()
                            .join(",")).into()])));
                }
            }
        }
        let mut skipped = Vec::new();
        for spec in specs {
            // Go `dualPasswordOption`.
            let (spec_retain, spec_discard) = match spec.dual_password {
                Some(tidb_ast::AlterUserDualPassword::RetainCurrent) => (true, false),
                Some(tidb_ast::AlterUserDualPassword::DiscardOld) => (false, true),
                None => (false, false),
            };
            let (user, host) = self.resolve_account(&spec.user)?;
            let need_admin_check = self.alter_user_admin_gate(
                &registry,
                spec,
                &user,
                &host,
                has_other_stmt_options,
                &effective,
            )?;
            let exists = registry.user_exists(&user, &host);
            let current_plugin = registry.plugin(&user, &host).unwrap_or_default();
            let current_auth_string = registry.auth_string(&user, &host).unwrap_or_default();
            if !exists {
                if alter.if_exists {
                    // Go collects the miss into `failedUsers` and, after the
                    // loop, reports each as Note 3162 under IF EXISTS.
                    skipped.push(format!("'{user}'@'{host}'"));
                    continue;
                }
                return Err(DriverError::from(tidb_util::dbterror::exeerrors::ERR_CANNOT_USER.generate_with_stack_by_args(&["ALTER USER".into(), format!("'{}'@'{}'", user, host).into()])));
            }
            if need_admin_check {
                self.require_system_user_privilege_over(&user, &host)?;
            }
            // Go `executeAlterUser` refuses, while composing the account's
            // UPDATE (before any write), to expire the anonymous account.
            if user.is_empty()
                && matches!(options.expire, Some(tidb_privilege::PasswordExpireSetting::Now))
            {
                return Err(DriverError::from(tidb_util::dbterror::exeerrors::ERR_PASSWORD_EXPIRE_ANONYMOUS_USER.generate_with_stack_by_args(&[])));
            }
            // Go's RETAIN CURRENT PASSWORD validation, in its order: plugin
            // capability, then "a new password must be set", then "same
            // plugin", then "and it must be non-empty".
            if spec_retain {
                let resolved = effective(&current_plugin);
                if !tidb_mysql::is_auth_plugin_clear_text(&resolved) {
                    return Err(DriverError::from(tidb_error::mysql::MysqlError::unknown(format!("Dual password is not supported for users authenticating with plugin '{}'", resolved))));
                }
                match &spec.auth {
                    None => {
                        return Err(DriverError::from(tidb_util::dbterror::exeerrors::ERR_CURRENT_PASSWORD_CANNOT_BE_RETAINED.generate_with_stack_by_args(&[user.into(), host.into()])));
                    }
                    Some(tidb_ast::CreateUserAuth::By(password)) => {
                        if password.is_empty() {
                            return Err(DriverError::from(tidb_util::dbterror::exeerrors::ERR_CURRENT_PASSWORD_CANNOT_BE_RETAINED.generate_with_stack_by_args(&[user.into(), host.into()])));
                        }
                    }
                    Some(tidb_ast::CreateUserAuth::With { plugin, credential }) => {
                        let Some(credential) = credential else {
                            // `IDENTIFIED WITH p` alone carries no new
                            // password (Go: neither ByAuthString nor
                            // ByHashString).
                            return Err(DriverError::from(tidb_util::dbterror::exeerrors::ERR_CURRENT_PASSWORD_CANNOT_BE_RETAINED.generate_with_stack_by_args(&[user.into(), host.into()])));
                        };
                        if !plugin.is_empty() && effective(plugin) != resolved {
                            return Err(DriverError::from(tidb_util::dbterror::exeerrors::ERR_PASSWORD_CANNOT_BE_RETAINED_ON_PLUGIN_CHANGE.generate_with_stack_by_args(&[user.into(), host.into()])));
                        }
                        let text = match credential {
                            tidb_ast::CreateUserCredential::By(text)
                            | tidb_ast::CreateUserCredential::As(text) => text,
                        };
                        if text.is_empty() {
                            return Err(DriverError::from(tidb_util::dbterror::exeerrors::ERR_CURRENT_PASSWORD_CANNOT_BE_RETAINED.generate_with_stack_by_args(&[user.into(), host.into()])));
                        }
                    }
                }
            }
            // The `mysql.user` UPDATE Go composes as `fields`; rendered SQL
            // fragments in Go's own order.
            let mut mirror_fields: Vec<String> = Vec::new();
            // Go: MAX_USER_CONNECTIONS needs CREATE USER even on one's own
            // account.
            if let Some(count) =
                super::account::max_user_connections_option(&alter.resource_options)
            {
                if !self.has_scoped_privilege("", "", tidb_mysql::CreateUserPriv) {
                    return Err(DriverError::from(tidb_error::plannererrors::ERR_SPECIFIC_ACCESS_DENIED.generate_with_stack_by_args(&["CREATE USER".into()])));
                }
                mirror_fields.push(format!("max_user_connections={count}"));
                registry.set_max_user_connections(&user, &host, count);
            }
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
                        let current_plugin = effective(&current_plugin);
                        (
                            tidb_privilege::encode_password_for_plugin(
                                &current_plugin,
                                &tidb_privilege::PluginCredential::By(password),
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
                    return Err(DriverError::from(tidb_util::dbterror::exeerrors::ERR_SECOND_PASSWORD_CANNOT_BE_EMPTY.generate_with_stack_by_args(&[user.into(), host.into()])));
                }
                self.maintain_password_history(
                    &registry,
                    &user,
                    &host,
                    &auth_string,
                    plaintext,
                    &plugin,
                    &options,
                    false,
                    plugin_changed,
                )?;
                if registry.set_auth_string_and_plugin(&user, &host, &auth_string, &plugin) {
                    // Go writes `password_expired='N'` and a fresh
                    // `Password_last_changed` in the same UPDATE as the new
                    // hash, which is what lets an expired account recover by
                    // setting a password (captured: after
                    // `ALTER USER e5 IDENTIFIED BY 'pw2'`, `SHOW CREATE USER`
                    // reports `PASSWORD EXPIRE DEFAULT` again).
                    registry.mark_password_changed(&user, &host);
                } else if !alter.if_exists {
                    return Err(DriverError::from(tidb_util::dbterror::exeerrors::ERR_CANNOT_USER.generate_with_stack_by_args(&["ALTER USER".into(), format!("'{}'@'{}'", user, host).into()])));
                }
                mirror_fields.push(format!(
                    "authentication_string={}",
                    super::account_tables::sql_str(&auth_string)
                ));
                mirror_fields.push(format!("plugin={}", super::account_tables::sql_str(&plugin)));
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
                Some(tidb_privilege::PasswordExpireSetting::Now) => {
                    mirror_fields.push("password_expired='Y'".to_owned());
                }
                _ => {
                    if spec.auth.is_some() {
                        mirror_fields.push("password_expired='N'".to_owned());
                    }
                }
            }
            match options.expire {
                Some(tidb_privilege::PasswordExpireSetting::Default) => {
                    mirror_fields.push("password_lifetime=null".to_owned());
                }
                Some(tidb_privilege::PasswordExpireSetting::Never) => {
                    mirror_fields.push("password_lifetime=0".to_owned());
                }
                Some(tidb_privilege::PasswordExpireSetting::Interval(days)) => {
                    mirror_fields.push(format!("password_lifetime={days}"));
                }
                Some(tidb_privilege::PasswordExpireSetting::Now) | None => {}
            }
            // Go's `newAttributes`: COMMENT/ATTRIBUTE metadata, then the
            // RETAIN secondary, preserving the locking object and other metadata.
            let mut new_attributes: Vec<String> = Vec::new();
            if let Some(annotation) = &alter.comment_or_attribute {
                match annotation {
                    tidb_ast::CreateUserCommentOrAttribute::Comment(text) => new_attributes.push(
                        format!("\"metadata\": {}", serde_json::json!({"comment": text})),
                    ),
                    tidb_ast::CreateUserCommentOrAttribute::Attribute(json) => {
                        new_attributes.push(format!("\"metadata\": {json}"));
                    }
                }
            }
            if let Some(name) = &alter.resource_group {
                let name =
                    self.user_resource_group_attribute(name, registry.is_role(&user, &host))?;
                new_attributes.push(format!(
                    "\"resource_group\": {}",
                    super::account_tables::json_string_literal(&name)
                ));
            }
            if spec_retain {
                // Go `buildAdditionalPasswordEntry`: the pre-change primary
                // hash, JSON-encoded (`json.Marshal`).
                new_attributes.push(format!(
                    "\"additional_password\": {}",
                    super::account_tables::json_string_literal(&current_auth_string)
                ));
            }
            // Go: DISCARD removes the secondary, and a plugin change drops
            // it silently; RETAIN always writes a fresh one, so it wins.
            let drop_secondary = (spec_discard || plugin_changed) && !spec_retain;
            if !new_attributes.is_empty() || drop_secondary {
                let patch = format!("{{{}}}", new_attributes.join(","));
                registry.merge_user_attributes(
                    &user,
                    &host,
                    serde_json::from_str(&patch)
                        .map_err(|_| super::account::invalid_user_attribute_json())?,
                    drop_secondary,
                );
            }
            // Go `executeAlterUser` REPLACES the whole `mysql.global_priv`
            // PRIV JSON on every ALTER USER: `tlsOption2GlobalPriv` answers
            // `{}` for a statement without REQUIRE, so a password-only ALTER
            // clears an earlier `REQUIRE SSL` too (captured on this branch:
            // `SHOW CREATE USER` then prints `REQUIRE NONE`), through
            // `INSERT ... ON DUPLICATE KEY UPDATE Priv = values(Priv)`.
            registry.set_tls_policy(&user, &host, &tls_policy);
            self.mirror_global_priv(
                &user,
                &host,
                &tls_policy,
                super::account_tables::GlobalPrivWrite::AlterUser,
            )?;
            if !mirror_fields.is_empty() && self.user_table_present() {
                let sql = format!(
                    "UPDATE mysql.user SET {} WHERE Host={} and User={}",
                    mirror_fields.join(","),
                    super::account_tables::sql_str(&host),
                    super::account_tables::sql_str(&user),
                );
                self.run_user_table_write(&sql)?;
            }
            self.mirror_account_policy_and_history(&registry, &user, &host)?;
        }
        for account in skipped {
            self.append_sql_warning(tidb_util::context::SqlWarn::raised(
                tidb_util::context::WarningLevel::Note,
                tidb_error::catalog::infoschema::ERR_USER_DROP_EXISTS
                    .generate_with_stack_by_args(&[account.into()]),
            ));
        }
        // A sandboxed session escapes by giving ITSELF a new password, which
        // is the only thing it was allowed in here to do (Go's
        // `executeAlterUser` -> `checkSandboxMode`, whose gate ran before the
        // statement reached this driver).
        if self.session_vars().sandbox_mode
            && (alter.user_function_auth.is_some()
                || alter.users.iter().any(|spec| spec.auth.is_some()))
        {
            self.session_vars_mut().sandbox_mode = false;
        }
        Ok(())
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
    fn show_create_user_rows(
        &mut self,
        spec: &tidb_ast::UserSpec,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
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
                tidb_mysql::SelectPriv,
            )
        {
            let (caller, caller_host) = self.own_account()?;
            return Err(DriverError::from(tidb_error::plannererrors::ERR_TABLEACCESS_DENIED.generate_with_stack_by_args(&["SELECT".into(), caller.into(), caller_host.into(), (tidb_mysql::consts::UserTable.to_owned()).into()])));
        }
        let Some(registry) = self.privilege_registry().cloned() else {
            return Err(DriverError::unsupported(
                "SHOW CREATE USER requires a server front end with a privilege registry",
            ));
        };
        let row = if self.user_table_present() {
            self.show_create_user_row(&user, &host)?
        } else {
            // A bare catalog has no `mysql.user`; the registry holds the
            // same account columns.
            registry.user_exists(&user, &host).then(|| {
                let expiry = registry.password_expiry(&user, &host).unwrap_or_default();
                let locking = registry.password_locking(&user, &host);
                let (history, reuse_days) = registry.password_reuse_policy(&user, &host);
                ShowCreateUserRow {
                    plugin: registry.plugin(&user, &host).unwrap_or_default(),
                    account_locked: registry.is_role(&user, &host),
                    metadata: String::new(),
                    token_issuer: String::new(),
                    password_reuse_history: history.map(|n| n.to_string()),
                    password_reuse_time: reuse_days.map(|n| n.to_string()),
                    password_expired: expiry.expired,
                    password_lifetime: expiry.lifetime.map_or(-1, i64::from),
                    failed_login_attempts: locking
                        .as_ref()
                        .map(|locking| locking.failed_login_attempts.to_string())
                        .unwrap_or_default(),
                    password_lock_time_days: locking
                        .map(|locking| locking.password_lock_time_days.to_string())
                        .unwrap_or_default(),
                    auth_data: registry.auth_string(&user, &host).unwrap_or_default(),
                    max_user_connections: 0,
                }
            })
        };
        let Some(row) = row else {
            return Err(DriverError::from(tidb_util::dbterror::exeerrors::ERR_CANNOT_USER.generate_with_stack_by_args(&["SHOW CREATE USER".into(), (format!("'{user}'@'{host}'")).into()])));
        };
        // Go starts from the global `default_authentication_plugin` and takes
        // the row's plugin only when it is non-empty.
        let plugin = if row.plugin.is_empty() {
            self.session_vars()
                .get_global("default_authentication_plugin")
                .map_err(DriverError::from)?
        } else {
            row.plugin
        };
        let account_clause = if row.account_locked { "LOCK" } else { "UNLOCK" };
        let attribute_clause = if row.metadata.is_empty() {
            String::new()
        } else {
            format!(" ATTRIBUTE '{}'", row.metadata)
        };
        let token_issuer = if row.token_issuer.is_empty() {
            String::new()
        } else {
            format!(" token_issuer {}", row.token_issuer)
        };
        let history = row
            .password_reuse_history
            .unwrap_or_else(|| "DEFAULT".to_owned());
        let reuse_interval = row
            .password_reuse_time
            .map_or_else(|| "DEFAULT".to_owned(), |days| format!("{days} DAY"));
        let expire_clause = if row.password_expired {
            "PASSWORD EXPIRE".to_owned()
        } else if row.password_lifetime == 0 {
            "PASSWORD EXPIRE NEVER".to_owned()
        } else if row.password_lifetime > 0 {
            format!("PASSWORD EXPIRE INTERVAL {} DAY", row.password_lifetime)
        } else {
            "PASSWORD EXPIRE DEFAULT".to_owned()
        };
        let failed_login_attempts = if row.failed_login_attempts.is_empty() {
            String::new()
        } else {
            format!(" FAILED_LOGIN_ATTEMPTS {}", row.failed_login_attempts)
        };
        let password_lock_time = match row.password_lock_time_days.as_str() {
            "" => String::new(),
            "-1" => " PASSWORD_LOCK_TIME UNBOUNDED".to_owned(),
            days => format!(" PASSWORD_LOCK_TIME {days}"),
        };
        let max_user_connections = if row.max_user_connections > 0 {
            format!(" WITH MAX_USER_CONNECTIONS {}", row.max_user_connections)
        } else {
            String::new()
        };
        // Go: `authStr` is empty ONLY for `auth_socket` with no stored data;
        // every other plugin (including a native/sha2/sm3 account with an
        // empty, passwordless hash) still prints ` AS '<possibly empty>'`.
        let auth_clause = if plugin == tidb_mysql::consts::AuthSocket && row.auth_data.is_empty() {
            String::new()
        } else {
            format!(" AS '{}'", row.auth_data)
        };
        // Go's `fetchShowCreateUser` reads the `mysql.global_priv` PRIV
        // JSON's `ssl_type` for this clause (captured: `REQUIRE SSL` for an
        // account created with it, `REQUIRE NONE` for one without).
        let require_clause = registry.tls_require_clause(&user, &host);
        let show_str = format!(
            "CREATE USER '{user}'@'{host}' IDENTIFIED WITH '{plugin}'{auth_clause} REQUIRE {require_clause}{token_issuer}{max_user_connections} {expire_clause} ACCOUNT {account_clause} PASSWORD HISTORY {history} PASSWORD REUSE INTERVAL {reuse_interval}{failed_login_attempts}{password_lock_time}{attribute_clause}"
        );
        // The column is `buildShowSchema`'s `CREATE USER for <User>`.
        Ok(vec![vec![Datum::Bytes(show_str.into_bytes())]])
    }

    /// Go `fetchShowCreateUser`'s `ExecRestrictedSQL` over `mysql.user`,
    /// column for column; `None` when the account has no row.
    fn show_create_user_row(
        &mut self,
        user: &str,
        host: &str,
    ) -> Result<Option<ShowCreateUserRow>, DriverError> {
        let sql = format!(
            "SELECT plugin, Account_locked, user_attributes->>'$.metadata', Token_issuer, \
             Password_reuse_history, Password_reuse_time, Password_expired, Password_lifetime, \
             user_attributes->>'$.Password_locking.failed_login_attempts', \
             user_attributes->>'$.Password_locking.password_lock_time_days', \
             authentication_string, Max_user_connections \
             FROM mysql.user WHERE User={} AND Host={}",
            super::account_tables::sql_str(user),
            super::account_tables::sql_str(&host.go_to_lower()),
        );
        let rows = self.exec_restricted_sql(&sql)?;
        let Some(row) = rows.into_iter().next() else {
            return Ok(None);
        };
        // Go `Row.GetString`: a NULL cell reads as "".
        let text = |index: usize| -> String {
            row.get(index)
                .filter(|value| !value.is_null())
                .and_then(|value| value.to_bytes().ok())
                .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
                .unwrap_or_default()
        };
        let nullable = |index: usize| row.get(index).is_some_and(|value| !value.is_null());
        Ok(Some(ShowCreateUserRow {
            plugin: text(0),
            account_locked: !text(1).ends_with('N'),
            metadata: text(2),
            token_issuer: text(3),
            password_reuse_history: nullable(4).then(|| text(4)),
            password_reuse_time: nullable(5).then(|| text(5)),
            password_expired: text(6) == "Y",
            password_lifetime: if nullable(7) { row[7].get_int64() } else { -1 },
            failed_login_attempts: text(8),
            password_lock_time_days: text(9),
            auth_data: text(10),
            max_user_connections: row.get(11).map_or(0, Datum::get_int64),
        }))
    }

    /// `SET PASSWORD [FOR <account>] = '<password>'`: the same
    /// `authentication_string` write as `ALTER USER ... IDENTIFIED BY`
    /// (captured: both leave the identical `*HEX` value), defaulting to the
    /// session's own account.
    fn set_password_stmt(
        &mut self,
        set_password: &tidb_ast::SetPasswordStmt,
    ) -> Result<(), DriverError> {
        let Some(registry) = self.privilege_registry().cloned() else {
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
        if !set_for_self && !self.has_scoped_privilege("", "", tidb_mysql::SuperPriv) {
            let (caller, caller_host) = self.own_account()?;
            return Err(DriverError::from(tidb_error::plannererrors::ERR_DBACCESS_DENIED.generate_with_stack_by_args(&[caller.into(), caller_host.into(), (tidb_mysql::consts::SystemDB.to_owned()).into()])));
        }
        // Go: self-service `SET PASSWORD ... RETAIN CURRENT PASSWORD`
        // requires APPLICATION_PASSWORD_ADMIN (CREATE USER / UPDATE on the
        // mysql schema suffice as supersets), matching `executeAlterUser`'s
        // self-service dual-password gate.
        if set_for_self
            && set_password.retain_current_password
            && !(self.has_scoped_privilege("", "", tidb_mysql::CreateUserPriv)
                || self.has_dynamic_privilege("APPLICATION_PASSWORD_ADMIN", false)
                || self.has_scoped_privilege(
                    tidb_mysql::consts::SystemDB,
                    tidb_mysql::consts::UserTable,
                    tidb_mysql::UpdatePriv,
                ))
        {
            return Err(DriverError::from(tidb_error::plannererrors::ERR_SPECIFIC_ACCESS_DENIED.generate_with_stack_by_args(&["APPLICATION_PASSWORD_ADMIN".into()])));
        }
        let Some(plugin) = registry.plugin(&user, &host) else {
            return Err(DriverError::from(tidb_util::dbterror::exeerrors::ERR_PASSWORD_NO_MATCH.generate_with_stack_by_args(&[])));
        };
        let current_auth_string = registry.auth_string(&user, &host).unwrap_or_default();
        // Go's RETAIN validation for SET PASSWORD: plugin capability, then
        // "the new password must be non-empty". The registry's plugin column
        // is always concrete, so `effectiveAuthPlugin`'s legacy empty-column
        // resolution has nothing to resolve here.
        if set_password.retain_current_password {
            if !tidb_mysql::is_auth_plugin_clear_text(&plugin) {
                return Err(DriverError::from(tidb_error::mysql::MysqlError::unknown(format!("Dual password is not supported for users authenticating with plugin '{}'", plugin))));
            }
            if set_password.password.is_empty() {
                return Err(DriverError::from(tidb_util::dbterror::exeerrors::ERR_CURRENT_PASSWORD_CANNOT_BE_RETAINED.generate_with_stack_by_args(&[user.into(), host.into()])));
            }
        }
        self.validate_password_if_enabled(&set_password.password)?;
        let auth_string = tidb_privilege::encode_password_for_plugin(
            &plugin,
            &tidb_privilege::PluginCredential::By(&set_password.password),
        )?;
        // Go `buildAdditionalPasswordEntry`'s gate, before the UPDATE runs:
        // an empty current primary cannot be retained as the secondary.
        if set_password.retain_current_password && current_auth_string.is_empty() {
            return Err(DriverError::from(tidb_util::dbterror::exeerrors::ERR_SECOND_PASSWORD_CANNOT_BE_EMPTY.generate_with_stack_by_args(&[user.into(), host.into()])));
        }
        self.maintain_password_history(
            &registry,
            &user,
            &host,
            &auth_string,
            Some(&set_password.password),
            &plugin,
            &super::account::PasswordOrLockOptions::default(),
            false,
            false,
        )?;
        if set_password.retain_current_password {
            registry.merge_user_attributes(
                &user,
                &host,
                serde_json::json!({"additional_password": current_auth_string}),
                false,
            );
        }
        if !registry.set_auth_string(&user, &host, &auth_string) {
            return Err(DriverError::from(tidb_util::dbterror::exeerrors::ERR_PASSWORD_NO_MATCH.generate_with_stack_by_args(&[])));
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
                    super::account_tables::json_string_literal(&current_auth_string)
                );
                format!(
                    ",user_attributes=json_merge_patch(coalesce(user_attributes, '{{}}'), {})",
                    super::account_tables::sql_str(&object)
                )
            } else {
                String::new()
            };
            let sql = format!(
                "UPDATE mysql.user SET authentication_string={},password_expired='N',\
                 password_last_changed=current_timestamp(){attrs_clause} WHERE User={} AND \
                 Host={}",
                super::account_tables::sql_str(&auth_string),
                super::account_tables::sql_str(&user),
                super::account_tables::sql_str(&go_to_lower(&host)),
            );
            self.run_user_table_write(&sql)?;
        }
        self.mirror_account_policy_and_history(&registry, &user, &host)?;
        self.session_vars_mut().sandbox_mode = false;
        Ok(())
    }
}

impl<S: super::account_tables::AccountSession > AccountPasswordExec for S {}
