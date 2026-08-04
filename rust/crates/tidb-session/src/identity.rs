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

//! Who the session is, and what a front end attaches to make that answerable:
//! the authenticated identity, the active roles, the process-list registration,
//! the privilege registry, the shared GLOBAL sysvar table, and sandbox mode.
//!
//! Go sets all of this on `SessionVars` (plus the `Domain`-owned managers) once
//! a connection authenticates. Everything here is either a front end's setter
//! or a read the privilege checks and the identity builtins make -- a session
//! with no front end has none of it, which is why every check falls back to
//! "unrestricted" rather than "denied".

use tidb_ast::{SessionStmt, Stmt};
use tidb_datatype::Datum;

use crate::{privilege, process, vars, DriverError, Session};

impl Session {
    /// Go's privilege gate on `SET GLOBAL`: SUPER, or the dynamic
    /// `SYSTEM_VARIABLES_ADMIN` privilege (which `has_dynamic_priv` already
    /// falls back to SUPER for, so this one call covers "at least one of").
    /// No attached privilege registry (an in-process session with no
    /// front end) is treated as unrestricted, matching every other
    /// privilege check this session performs before a registry is attached.
    pub(crate) fn require_set_global_privilege(&self) -> Result<(), DriverError> {
        let Some(registry) = &self.privileges else {
            return Ok(());
        };
        let Some((user, host)) = self.current_identity() else {
            return Ok(());
        };
        if registry.has_dynamic_priv_with_roles(
            user,
            host,
            self.active_roles(),
            "SYSTEM_VARIABLES_ADMIN",
            false,
        ) {
            Ok(())
        } else {
            Err(DriverError::Var(
                tidb_executor::VarErrorKind::SetGlobalAccessDenied,
            ))
        }
    }

    /// The account table plus this session's authenticated identity -- what
    /// every privilege gate needs, and `None` for a session that is
    /// UNRESTRICTED because it has no front end (no registry attached, or no
    /// authenticated identity). Go's executors treat a missing privilege
    /// manager as "miss privilege checker" and refuse; this tier's in-process
    /// sessions legitimately have none, so they pass instead -- the rule
    /// every check here already followed, now stated once.
    fn privilege_context(&self) -> Option<(&privilege::PrivilegeRegistry, &str, &str)> {
        let registry = self.privileges.as_ref()?;
        let (user, host) = self.current_identity()?;
        Some((registry, user, host))
    }

    /// Go `RequestVerification(activeRoles, database, table, "", priv)`:
    /// whether this session holds `global_priv` at that scope, through its
    /// own grants or a role. An EMPTY `database` and `table` is Go's GLOBAL
    /// (`*.*`) scope; an empty `table` alone is schema scope.
    ///
    /// The *error* a denied caller reports is the caller's, not this
    /// method's: Go words it per statement (`ANALYZE` reports
    /// `ErrTableaccessDenied` naming INSERT or SELECT), and only the caller
    /// knows which privilege it was asking about.
    #[must_use]
    pub fn has_scoped_privilege(
        &self,
        database: &str,
        table: &str,
        global_priv: privilege::GlobalPriv,
    ) -> bool {
        let Some((registry, user, host)) = self.privilege_context() else {
            return true;
        };
        registry.has_table_priv_with_roles(
            user,
            host,
            self.active_roles(),
            database,
            table,
            global_priv,
        )
    }

    /// [`Self::has_scoped_privilege`] at table scope, the name the executor
    /// arms that predate the general form call it by.
    #[must_use]
    pub fn has_table_privilege(
        &self,
        database: &str,
        table: &str,
        global_priv: privilege::GlobalPriv,
    ) -> bool {
        self.has_scoped_privilege(database, table, global_priv)
    }

    /// Go `RequestVerification(activeRoles, database, table, "", mask)` for a
    /// multi-bit `mask`, which Go reads as "ANY of these privileges".
    ///
    /// The virtual schemas answer before any grant is read, exactly as they
    /// do for the single-privilege form
    /// ([`crate::table_privilege::mem_db_verdict_mask`]).
    #[must_use]
    pub fn has_any_scoped_privilege(&self, database: &str, table: &str, mask: u64) -> bool {
        let Some((registry, user, host)) = self.privilege_context() else {
            return true;
        };
        if let Some(verdict) = crate::table_privilege::mem_db_verdict_mask(database, mask) {
            return verdict;
        }
        registry.has_priv_mask_with_roles(user, host, self.active_roles(), database, table, mask)
    }

    /// The owned snapshot of this session's identity that the
    /// `information_schema` retrievers filter their rows with, since they run
    /// with the catalog borrowed and cannot ask the session anything.
    #[must_use]
    pub(crate) fn schema_visibility(&self) -> crate::infoschema::SchemaVisibility {
        match self.privilege_context() {
            Some((registry, user, host)) => crate::infoschema::SchemaVisibility::for_session(
                registry.clone(),
                user,
                host,
                self.active_roles(),
            ),
            None => crate::infoschema::SchemaVisibility::unrestricted(),
        }
    }

    /// Go `UserPrivileges.DBIsVisible` (`privileges.go` around line 935): may
    /// this session SEE the schema at all.
    ///
    /// A session with no front end is unrestricted, exactly as every other
    /// check here is, and Go's own callers guard on
    /// `checker != nil && SessionVars.User != nil` for the same reason.
    #[must_use]
    pub fn database_is_visible(&self, database: &str) -> bool {
        let Some((registry, user, host)) = self.privilege_context() else {
            return true;
        };
        registry.db_is_visible_with_roles(user, host, self.active_roles(), database)
    }

    /// The `ErrDBaccessDenied` (1044) gate Go's `executeUse`,
    /// `fetchShowTables`, `fetchShowTableStatus` and
    /// `fetchShowCreateDatabase` all apply BEFORE they look the schema up --
    /// which is why naming a schema that does not exist reports 1044 and not
    /// `ErrBadDB` for an account that could not have seen it either way
    /// (measured: `SHOW TABLES IN nosuchdb` as an unprivileged user is
    /// 1044).
    pub(crate) fn require_visible_database(&self, database: &str) -> Result<(), DriverError> {
        if self.database_is_visible(database) {
            return Ok(());
        }
        let (user, host) = self.current_identity().unwrap_or_default();
        Err(DriverError::DbAccessDenied {
            user: user.to_owned(),
            host: host.to_owned(),
            database: database.to_owned(),
        })
    }

    /// Go `RequestDynamicVerification(activeRoles, name, grantable)`. The
    /// registry's own SUPER fallback is part of this, which is why Go's
    /// denial messages name `SUPER or <PRIVILEGE>`.
    #[must_use]
    pub fn has_dynamic_privilege(&self, name: &str, with_grant: bool) -> bool {
        let Some((registry, user, host)) = self.privilege_context() else {
            return true;
        };
        registry.has_dynamic_priv_with_roles(user, host, self.active_roles(), name, with_grant)
    }

    /// Go `RequestDynamicVerificationWithUser` (`privileges.go` around line
    /// 118): whether the NAMED account holds a dynamic privilege, evaluated
    /// over that account's own DEFAULT roles rather than the caller's active
    /// ones. `DROP USER` and `ALTER USER` ask this about their target, which
    /// is how a `SYSTEM_USER` account is protected from a merely
    /// `CREATE USER`-privileged caller.
    #[must_use]
    pub(crate) fn target_has_dynamic_privilege(&self, user: &str, host: &str, name: &str) -> bool {
        let Some(registry) = self.privileges.as_ref() else {
            return false;
        };
        let account = (user.to_owned(), host.to_owned());
        let roles = registry.default_roles(&account);
        registry.has_dynamic_priv_with_roles(user, host, &roles, name, false)
    }

    /// Go's `CheckPrivilege` (`planner/core/optimizer.go` around line 187)
    /// over the `visitInfo` one statement collects
    /// ([`crate::table_privilege::required_table_privileges`]).
    ///
    /// Entries are checked in the order the builder appended them, and the
    /// FIRST failure is reported -- with the statement's own
    /// `ErrTableaccessDenied` (1142) where Go attached one, and with the
    /// generic `ErrPrivilegeCheckFail` (8121) where it did not.
    pub(crate) fn require_statement_table_privileges(
        &self,
        stmt: &tidb_ast::Stmt,
    ) -> Result<(), DriverError> {
        let Some((_, user, host)) = self.privilege_context() else {
            return Ok(());
        };
        let (user, host) = (user.to_owned(), host.to_owned());
        for request in crate::table_privilege::required_table_privileges(stmt, &self.current_db) {
            // Go answers a virtual schema from fixed rules before it reads a
            // single grant, so `SELECT ... FROM information_schema.*` needs
            // nothing and a write there is refused whatever is granted.
            let granted = match crate::table_privilege::mem_db_verdict(
                &request.database,
                request.privilege,
            ) {
                Some(verdict) => verdict,
                None => {
                    self.has_scoped_privilege(&request.database, &request.table, request.privilege)
                }
            };
            if granted {
                continue;
            }
            return Err(if request.table_named_in_error {
                DriverError::TableAccessDenied {
                    // Go's `authErr` spells the verb as the uppercase
                    // command name and the table as `tableInfo.Name.L`.
                    privilege: request.privilege.print_name(),
                    user,
                    host,
                    table: request.table.to_lowercase(),
                }
            } else {
                DriverError::PrivilegeCheckFail(request.privilege.check_fail_name().to_owned())
            });
        }
        Ok(())
    }

    /// The `user@host` this session authenticated as, as Go's
    /// `AuthUsername`/`AuthHostname` pair. `None` for a session with no front
    /// end.
    #[must_use]
    pub fn authenticated_identity(&self) -> Option<(&str, &str)> {
        self.current_identity()
    }

    /// Records the authenticated identity, which the builtins report.
    ///
    /// Go sets `SessionVars.User` once the connection authenticates; a
    /// front end that has no user leaves it unset and the builtins answer
    /// NULL, which is what Go does for a session without one.
    pub fn set_user(&mut self, current_user: String, login_user: String) {
        self.current_user = Some(current_user);
        self.login_user = Some(login_user);
    }

    /// Grants or revokes this session's `PROCESS` privilege.
    ///
    /// See the [`Session::has_process_priv`] field doc for why this exists
    /// as a direct setter rather than a `GRANT PROCESS ON *.* TO ...`
    /// statement: `GRANT` is not implemented in this tier yet.
    pub fn set_process_privilege(&mut self, granted: bool) {
        self.has_process_priv = granted;
    }

    /// Joins this session to the server's process list under `connection_id`.
    ///
    /// Go's server registers each connection with the `sessmgr.Manager` right
    /// after authentication; `guard` is that registration, and dropping the
    /// session removes the row.
    pub fn attach_process(&mut self, connection_id: u64, guard: process::ProcessGuard) {
        self.connection_id = Some(connection_id);
        self.process = Some(guard);
    }

    /// Joins this session to the server's account/global-privilege registry.
    ///
    /// Go's session reads `privilege.Manager` off the `Domain` every
    /// connection shares; this is the equivalent handle, installed by the
    /// front end the same way [`Session::attach_process`] installs the
    /// process-list registry.
    pub fn attach_privileges(&mut self, registry: privilege::PrivilegeRegistry) {
        // Go's `Auth` activates the account's DEFAULT roles the moment the
        // connection is authenticated (captured: a fresh session reports its
        // default roles from `CURRENT_ROLE()` with no `SET ROLE` at all).
        // The registry is what makes that answerable, so attaching it is the
        // one place that can do it -- the front end installs the identity
        // first and the registry second.
        if let Some((user, host)) = self.current_identity() {
            self.active_roles = registry.default_roles(&(user.to_owned(), host.to_owned()));
        }
        self.privileges = Some(registry);
    }

    /// Points this session at a different account table for one statement,
    /// answering the one it was using.
    ///
    /// Unlike [`Self::attach_privileges`] this touches nothing else: the
    /// session's active roles are its own state, and a front end that runs an
    /// account statement against a scratch copy of the table -- which is how
    /// a node whose registry is a read of somebody else's `mysql.*` validates
    /// the statement before persisting it -- must be able to put the live
    /// table back without a `SET ROLE` silently reverting to the defaults.
    pub fn swap_privileges(
        &mut self,
        registry: privilege::PrivilegeRegistry,
    ) -> Option<privilege::PrivilegeRegistry> {
        self.privileges.replace(registry)
    }

    /// Joins this session to the server's shared GLOBAL-scope sysvar table
    /// and snapshots its current overrides into this session's own copy --
    /// see [`vars::SessionVars::seed_from_globals`] for why that snapshot
    /// happens exactly once, here, rather than on every read.
    pub fn attach_globals(&mut self, globals: vars::GlobalSysvars) {
        self.vars.seed_from_globals(globals);
    }

    /// Points this session at a different shared GLOBAL-scope sysvar table
    /// for one statement, answering the one it was using.
    ///
    /// Unlike [`Self::attach_globals`] this does not reseed the session's own
    /// `@@x` copies: a front end running a `SET GLOBAL` against a scratch
    /// table read from the cluster (validate-then-persist, exactly like
    /// [`Self::swap_privileges`]) must be able to put the live table back
    /// unconditionally if the statement fails, without disturbing anything
    /// else this session has already seeded from it.
    pub fn swap_globals(&mut self, globals: vars::GlobalSysvars) -> vars::GlobalSysvars {
        self.vars.swap_globals(globals)
    }

    /// Go `SessionVars.ActiveRoles`, for the privilege checks and the
    /// `CURRENT_ROLE()` builtin.
    pub(crate) fn active_roles(&self) -> &[privilege::Account] {
        &self.active_roles
    }

    /// The text `CURRENT_ROLE()` reports: Go's `builtinCurrentRoleSig` joins
    /// each active role's `RoleIdentity.String()` (backtick-quoted
    /// ``\`role\`@\`host\```) with a bare comma, and answers the literal
    /// `NONE` when no role is active (captured, both forms).
    pub(crate) fn current_role_text(&self) -> String {
        if self.active_roles.is_empty() {
            return "NONE".to_owned();
        }
        self.active_roles
            .iter()
            .map(|(role, host)| format!("`{role}`@`{host}`"))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Splits the `CURRENT_USER()` identity (`user@host`) this session
    /// authenticated as, for privilege-registry lookups. `None` for a
    /// session with no front end.
    pub(crate) fn current_identity(&self) -> Option<(&str, &str)> {
        let identity = self.current_user.as_deref()?;
        identity.split_once('@')
    }

    /// Records the connection identifier `CONNECTION_ID()` reports, which Go
    /// sets on `SessionVars.ConnectionID` when the front end opens the
    /// connection. `attach_process` sets it too; this exists for a front end
    /// that has an id but no process registry.
    pub fn set_connection_id(&mut self, connection_id: u64) {
        self.connection_id = Some(connection_id);
    }

    /// Go `SessionVars.ConnectionID`, which `CONNECTION_ID()` reports; zero
    /// for a session no front end opened.
    #[must_use]
    pub fn connection_id(&self) -> u64 {
        self.connection_id.unwrap_or(0)
    }

    /// Whether this session logged in with an expired password and is
    /// therefore restricted to fixing it -- Go's `session.InSandBoxMode`.
    #[must_use]
    pub const fn in_sandbox_mode(&self) -> bool {
        self.sandbox_mode
    }

    /// Puts this session in sandbox mode. The front end calls this when the
    /// login reported an expired password the server chose to admit.
    pub const fn enable_sandbox_mode(&mut self) {
        self.sandbox_mode = true;
    }

    /// Go's `TiDBContext.checkSandBoxMode` (`pkg/server/driver_tidb.go`): a
    /// sandboxed session may run `SET PASSWORD` and `ALTER USER` and nothing
    /// else; everything else reports 1820. Go gates on the PARSED statement
    /// (its front end hands `ExecuteStmt` an `ast.StmtNode`), so a syntax
    /// error still surfaces as a syntax error -- which is why this parses
    /// first and lets a parse failure fall through to the normal path that
    /// reports it. The extra parse is paid only while sandboxed, a state one
    /// statement ends.
    pub(crate) fn check_sandbox_mode(&self, sql: &str) -> Result<(), DriverError> {
        if !self.sandbox_mode {
            return Ok(());
        }
        let Ok(stmt) = self.parse(sql) else {
            return Ok(());
        };
        match stmt {
            Stmt::Session(session) if matches!(*session, SessionStmt::SetPassword(_)) => Ok(()),
            Stmt::Ddl(ddl) if matches!(*ddl, tidb_ast::DdlStmt::AlterUser(_)) => Ok(()),
            _ => Err(DriverError::MustChangePassword),
        }
    }

    /// `SELECT * FROM information_schema.USER_PRIVILEGES` rows, in Go's
    /// `MySQLPrivilege.UserPrivilegesTable` order: EVERY account's static
    /// privileges first (one row per privilege, in `mysql.AllGlobalPrivs`
    /// print order, or a single `USAGE` row for an account with none), then
    /// EVERY account's DYNAMIC privileges. Accounts are visited in username
    /// order, since Go walks a B-tree keyed by username.
    ///
    /// `IS_GRANTABLE` means different things in the two halves (captured):
    /// a static row reports the account's `GRANT OPTION`, while a dynamic
    /// row reports that one privilege's own `with_grant_option`.
    ///
    /// Visibility (Go: "Seeing all users requires SELECT ON * FROM mysql.*.
    /// The SUPER privilege (or any other dynamic privilege) doesn't help
    /// here. This is verified against MySQL."): without global `SELECT`, a
    /// session sees only its own account's rows.
    pub(crate) fn user_privileges_table_rows(&self) -> Vec<Vec<Datum>> {
        let Some(registry) = &self.privileges else {
            return Vec::new();
        };
        let identity = self
            .current_identity()
            .map(|(user, host)| (user.to_owned(), host.to_owned()));
        let show_all = identity.as_ref().is_none_or(|(user, host)| {
            registry.has_global_priv(user, host, privilege::GlobalPriv::Select)
        });
        let visible = |account: &(String, String)| show_all || identity.as_ref() == Some(account);

        let grantee = |(user, host): &(String, String)| format!("'{user}'@'{host}'");
        let cell = |value: &str| Datum::Bytes(value.as_bytes().to_vec());
        let flag = |grantable: bool| cell(if grantable { "YES" } else { "NO" });

        let mut static_accounts = registry.global_priv_masks();
        static_accounts.sort_by(|(left, _), (right, _)| left.cmp(right));
        let mut rows = Vec::new();
        for (account, privs) in &static_accounts {
            if !visible(account) {
                continue;
            }
            let grantable = flag(privs & privilege::GlobalPriv::GrantOption.bit() != 0);
            let named: Vec<&privilege::GlobalPriv> = privilege::ALL_GLOBAL_PRIVS
                .iter()
                .filter(|priv_| privs & priv_.bit() != 0)
                .collect();
            if named.is_empty() {
                rows.push(vec![
                    cell(&grantee(account)),
                    cell("def"),
                    cell("USAGE"),
                    grantable.clone(),
                ]);
                continue;
            }
            for priv_ in named {
                rows.push(vec![
                    cell(&grantee(account)),
                    cell("def"),
                    cell(priv_.print_name()),
                    grantable.clone(),
                ]);
            }
        }

        let mut dynamic_accounts = registry.accounts_with_dynamic_privs();
        dynamic_accounts.sort();
        for account in &dynamic_accounts {
            if !visible(account) {
                continue;
            }
            for (name, grantable) in registry.dynamic_priv_rows(&account.0, &account.1) {
                rows.push(vec![
                    cell(&grantee(account)),
                    cell("def"),
                    cell(&name),
                    flag(grantable),
                ]);
            }
        }
        rows
    }
}
