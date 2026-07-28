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
            || !password_options.is_empty()
            || comment_or_attribute.is_some()
            || resource_group.is_some()
        {
            return Err(DriverError::Unsupported(
                "CREATE USER options beyond the account list are not supported yet",
            ));
        }
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::Unsupported(
                "CREATE USER requires a server front end with a privilege registry",
            ));
        };
        for spec in users {
            let auth_string = Self::resolve_auth_string(spec.auth.as_ref())?;
            if spec.dual_password.is_some() {
                return Err(DriverError::Unsupported(
                    "CREATE USER ... RETAIN CURRENT PASSWORD is not supported yet",
                ));
            }
            let user = spec.user.user.as_str();
            let host = spec.user.host.as_str();
            // Go processes each account in source order and fails on the
            // FIRST duplicate rather than batching, unlike DROP USER below.
            if !registry.create_user(user, host, &auth_string) && !if_not_exists {
                return Err(DriverError::CreateUserAlreadyExists {
                    user: user.to_owned(),
                    host: host.to_owned(),
                });
            }
        }
        Ok(StmtOutput::Affected(0))
    }

    /// The `mysql.user.authentication_string` one account specification's
    /// authentication clause stores. `IDENTIFIED WITH <plugin>` is refused
    /// rather than silently downgraded, because only
    /// `mysql_native_password` is modelled; a missing clause means a
    /// passwordless account, whose `authentication_string` is empty.
    pub(crate) fn resolve_auth_string(
        auth: Option<&tidb_ast::CreateUserAuth>,
    ) -> Result<String, DriverError> {
        match auth {
            None => Ok(String::new()),
            Some(tidb_ast::CreateUserAuth::By(password)) => {
                Ok(privilege::encode_password(password))
            }
            Some(tidb_ast::CreateUserAuth::With { .. }) => Err(DriverError::Unsupported(
                "CREATE/ALTER USER ... IDENTIFIED WITH is not supported yet",
            )),
        }
    }

    /// `ALTER USER [IF EXISTS] <account> IDENTIFIED BY '<password>'`, the one
    /// `ALTER USER` action this tier stores: it rewrites the account's
    /// `mysql.user.authentication_string` in place, so the NEXT login uses
    /// the new password (Go `executeAlterUser`).
    pub(crate) fn alter_user_stmt(
        &mut self,
        alter: &tidb_ast::AlterUserStmt,
    ) -> Result<StmtOutput, DriverError> {
        if alter.user_function_auth.is_some()
            || alter.user_function_dual_password.is_some()
            || !alter.tls_options.is_empty()
            || !alter.resource_options.is_empty()
            || !alter.password_options.is_empty()
            || alter.comment_or_attribute.is_some()
            || alter.resource_group.is_some()
        {
            return Err(DriverError::Unsupported(
                "ALTER USER options beyond IDENTIFIED BY are not supported yet",
            ));
        }
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::Unsupported(
                "ALTER USER requires a server front end with a privilege registry",
            ));
        };
        for spec in &alter.users {
            if spec.auth.is_none() || spec.dual_password.is_some() {
                return Err(DriverError::Unsupported(
                    "ALTER USER options beyond IDENTIFIED BY are not supported yet",
                ));
            }
            let auth_string = Self::resolve_auth_string(spec.auth.as_ref())?;
            let (user, host) = self.resolve_account(&spec.user)?;
            if !registry.set_auth_string(&user, &host, &auth_string) && !alter.if_exists {
                return Err(DriverError::AlterUserMissing { user, host });
            }
        }
        Ok(StmtOutput::Affected(0))
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

    /// `DROP USER` at the GLOBAL scope this tier models. Go's
    /// `executeDropUser` checks every named account exists BEFORE dropping
    /// any of them, rolling the whole statement back and reporting every
    /// missing account together if one is missing.
    pub(crate) fn drop_user_stmt(
        &mut self,
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
                return Err(DriverError::DropUserMissing {
                    accounts: missing.join(","),
                });
            }
        }
        for spec in users {
            registry.drop_user(&spec.user, &spec.host);
        }
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
                let privs = self.resolve_scoped_privs(&grant.privileges, ScopeKind::Table)?;
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
                for spec in &grant.users {
                    let user = spec.user.user.as_str();
                    let host = spec.user.host.as_str();
                    if !registry.user_exists(user, host) {
                        return Err(DriverError::GrantToUnknownUser);
                    }
                    registry.grant_table(user, host, &database, table, mask);
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
                let privs = self.resolve_scoped_privs(&revoke.privileges, ScopeKind::Table)?;
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
                    if !registry.table_grant_row_exists(user, host, &database, table) {
                        return Err(DriverError::RevokeNoTableGrant {
                            user: user.to_owned(),
                            host: host.to_owned(),
                            database: database.clone(),
                            table: table.clone(),
                        });
                    }
                    registry.revoke_table(user, host, &database, table, mask);
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

    /// `SHOW GRANTS [FOR <user>]` at GLOBAL scope. `USING <roles>` is refused
    /// rather than silently ignored, since active-role expansion is not
    /// modeled here.
    pub(crate) fn show_grants_stmt(
        &mut self,
        show: &tidb_ast::ShowGrantsStmt,
    ) -> Result<StmtOutput, DriverError> {
        if !show.roles.is_empty() {
            return Err(DriverError::Unsupported(
                "SHOW GRANTS ... USING is not supported yet",
            ));
        }
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
        let Some(lines) = registry.show_grants(&user, &host) else {
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
