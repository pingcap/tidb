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

//! `mysql.user` as a REAL table: the internal DML every account statement
//! runs against it, transcreating the SQL Go's `executor/simple.go`
//! (`executeCreateUser` / `executeAlterUser` / `executeSetPwd` /
//! `executeDropUser` / `executeRenameUser`) and `executor/grant.go` /
//! `executor/revoke.go` issue through their system session.
//!
//! # Deviation from Go, stated once
//!
//! Go's `mysql.user` IS the storage: the privilege cache
//! (`privileges.MySQLPrivilege`) is rebuilt FROM it (`LoadUserTable`), so a
//! direct `UPDATE mysql.user` plus `FLUSH PRIVILEGES` changes logins. This
//! tier's [`crate::privilege::PrivilegeRegistry`] remains the authoritative,
//! cross-session account store (it also serves sessions that never
//! bootstrapped a catalog), and every account-mutating statement writes BOTH:
//! the registry as before, and the table rows exactly as Go writes them. The
//! full inversion -- registry as a cache reloaded from the table -- is the
//! documented follow-up; what this module guarantees meanwhile is that the
//! table is never fabricated at read time: a `SELECT ... FROM mysql.user`
//! reads only what bootstrap and the statements above actually wrote.
//!
//! Consequences kept deliberately Go-shaped:
//!  * a session whose catalog was never bootstrapped (no `mysql.user` table)
//!    skips the mirror -- reads there keep answering `UnknownTable`, exactly
//!    what they answered before this module existed;
//!  * direct DML against `mysql.user` executes (it is a real table) but does
//!    not reach the registry, which is Go's own behavior BEFORE
//!    `FLUSH PRIVILEGES`;
//!  * the `$.Password_locking` member of `User_attributes` is NOT mirrored
//!    (Go's `readPasswordLockingInfo`/`alterUserFailedLoginJSON` pipeline,
//!    whose `auto_locked_last_changed` carries a wall-clock `time.UnixDate`
//!    string): the registry models the whole policy and no recording in the
//!    suite ever reads that JSON member back. Named gap, not an
//!    approximation.

use crate::*;

/// Renders one SQL string literal under the default (backslash-escaping)
/// SQL mode, for values interpolated into the internal DML below -- the role
/// Go's `sqlescape` `%?` placeholder plays.
pub(crate) fn sql_str(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('\'');
    for ch in value.chars() {
        match ch {
            '\'' => out.push_str("''"),
            '\\' => out.push_str("\\\\"),
            _ => out.push(ch),
        }
    }
    out.push('\'');
    out
}

/// Renders one JSON string literal -- the role Go's `json.Marshal(oldPwd)`
/// plays in `buildAdditionalPasswordEntry`. Escapes exactly what RFC 8259
/// requires (quote, backslash, control characters); a stored hash is ASCII,
/// but a `caching_sha2_password` salt byte range makes the control-character
/// arm reachable in principle.
pub(crate) fn json_string_literal(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            control if (control as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", control as u32));
            }
            _ => out.push(ch),
        }
    }
    out.push('"');
    out
}

impl Session {
    /// Whether the shared catalog holds `mysql.user` -- true after
    /// [`Session::bootstrap_fresh_store`], false for a bare catalog a caller
    /// installed without bootstrapping (see the module doc's deviation
    /// note).
    pub(crate) fn user_table_present(&mut self) -> bool {
        self.with_catalog_mut(|catalog| Ok(catalog.contains_in("mysql", "user")))
            .unwrap_or(false)
    }

    /// Runs one internal `INSERT`/`UPDATE`/`DELETE` against the `mysql`
    /// schema, the way Go's account executors run theirs through the system
    /// session's `sqlExecutor`. A failure fails the statement, as Go's does;
    /// callers only reach this once [`Self::user_table_present`] said the
    /// table exists, so an error here is a real defect rather than a missing
    /// bootstrap.
    pub(crate) fn run_user_table_write(&mut self, sql: &str) -> Result<(), DriverError> {
        // The session's own DML context: it carries the statement clock
        // (`password_last_changed=current_timestamp()` and the column's
        // default both evaluate NOW), the session time zone, and the JSON
        // machinery's sql-mode inputs.
        let ctx = self.statement_context(true);
        let head = sql.trim_start();
        self.with_catalog_mut(|catalog| {
            if head.len() >= 6 && head[..6].eq_ignore_ascii_case("INSERT") {
                tidb_executor::run_insert_in(sql, catalog, "mysql", &ctx).map(|_| ())
            } else if head.len() >= 6 && head[..6].eq_ignore_ascii_case("UPDATE") {
                tidb_executor::run_update_in(sql, catalog, "mysql", &ctx).map(|_| ())
            } else if head.len() >= 6 && head[..6].eq_ignore_ascii_case("DELETE") {
                tidb_executor::run_delete_in(sql, catalog, "mysql", &ctx).map(|_| ())
            } else {
                Err(DriverError::unsupported(
                    "internal mysql.user write must be INSERT/UPDATE/DELETE",
                ))
            }
        })
    }

    /// Mirrors one freshly created account into `mysql.user` -- Go
    /// `executeCreateUser`'s `INSERT INTO mysql.user (Host, User,
    /// authentication_string, plugin, user_attributes, Account_locked,
    /// Token_issuer, Password_expired, Password_lifetime,
    /// Max_user_connections, Password_reuse_time, Password_reuse_history)`,
    /// with the columns this tier's `CREATE USER` can actually produce.
    /// Columns Go omits take their `CreateUserTable` defaults, including
    /// `Password_last_changed`'s `CURRENT_TIMESTAMP()`.
    ///
    /// `user_attributes_json` is Go's `userAttributesStr` -- `{}` for a
    /// statement with no COMMENT/ATTRIBUTE clause, which is why a plain
    /// account reads back `{}` rather than NULL (captured in
    /// `privilege/privileges`: `SELECT User_attributes ...` prints `{}`),
    /// while the BOOTSTRAP root row is NULL (`doDMLWorks` writes `null`).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn mirror_create_user_row(
        &mut self,
        user: &str,
        host: &str,
        auth_string: &str,
        plugin: &str,
        user_attributes_json: &str,
        account_locked: bool,
        password_expired: bool,
        password_lifetime: Option<i64>,
    ) -> Result<(), DriverError> {
        if !self.user_table_present() {
            return Ok(());
        }
        let lifetime = match password_lifetime {
            // Go's `plOptions.passwordLifetime` starts nil for CREATE USER,
            // and PASSWORD EXPIRE NEVER / INTERVAL n DAY store 0 / n.
            None => "null".to_owned(),
            Some(days) => days.to_string(),
        };
        let sql = format!(
            "INSERT INTO mysql.user (Host, User, authentication_string, plugin, \
             user_attributes, Account_locked, Token_issuer, Password_expired, \
             Password_lifetime, Max_user_connections, Password_reuse_time, \
             Password_reuse_history) VALUES ({host}, {user}, {auth}, {plugin}, {attrs}, \
             {locked}, '', {expired}, {lifetime}, 0, null, null)",
            // Go lowercases the stored Host (`hostName :=
            // strings.ToLower(spec.User.Hostname)`).
            host = sql_str(&host.to_lowercase()),
            user = sql_str(user),
            auth = sql_str(auth_string),
            plugin = sql_str(plugin),
            attrs = sql_str(user_attributes_json),
            locked = if account_locked { "'Y'" } else { "'N'" },
            expired = if password_expired { "'Y'" } else { "'N'" },
        );
        self.run_user_table_write(&sql)
    }

    /// Mirrors `DROP USER`'s row removal -- Go `executeDropUser`'s
    /// `DELETE FROM mysql.user WHERE Host = %? and User = %?`.
    pub(crate) fn mirror_drop_user_row(
        &mut self,
        user: &str,
        host: &str,
    ) -> Result<(), DriverError> {
        if !self.user_table_present() {
            return Ok(());
        }
        let sql = format!(
            "DELETE FROM mysql.user WHERE Host = {} and User = {}",
            sql_str(&host.to_lowercase()),
            sql_str(user),
        );
        self.run_user_table_write(&sql)
    }

    /// Mirrors `RENAME USER`'s row move -- Go `executeRenameUser` ->
    /// `renameUserHostInSystemTable` on `mysql.user`: `UPDATE %n.%n SET
    /// User=%?, Host=%? WHERE User=%? AND Host=%?` (host lowercased on both
    /// sides).
    pub(crate) fn mirror_rename_user_row(
        &mut self,
        old_user: &str,
        old_host: &str,
        new_user: &str,
        new_host: &str,
    ) -> Result<(), DriverError> {
        if !self.user_table_present() {
            return Ok(());
        }
        let sql = format!(
            "UPDATE mysql.user SET User={}, Host={} WHERE User={} AND Host={}",
            sql_str(new_user),
            sql_str(&new_host.to_lowercase()),
            sql_str(old_user),
            sql_str(&old_host.to_lowercase()),
        );
        self.run_user_table_write(&sql)
    }

    /// Mirrors a GLOBAL `GRANT`/`REVOKE`'s static-privilege bits into the
    /// account's `mysql.user` privilege columns -- Go `grant.go`
    /// `composeGlobalPrivUpdate` (`%n='Y'` per named privilege, every
    /// `AllGlobalPrivs` column for `ALL`) and `revoke.go`'s `'N'` twin. The
    /// caller passes the same mask it handed the registry, so `WITH GRANT
    /// OPTION`'s `Grant_priv` rides in the mask exactly as it does there.
    pub(crate) fn mirror_global_priv_columns(
        &mut self,
        user: &str,
        host: &str,
        mask: u64,
        grant: bool,
    ) -> Result<(), DriverError> {
        if mask == 0 || !self.user_table_present() {
            return Ok(());
        }
        let value = if grant { "'Y'" } else { "'N'" };
        let mut assignments = Vec::new();
        for priv_ in privilege::ALL_GLOBAL_PRIVS
            .iter()
            .chain(std::iter::once(&privilege::GlobalPriv::GrantOption))
        {
            if mask & priv_.bit() != 0 {
                assignments.push(format!("{}={value}", priv_.user_table_column()));
            }
        }
        let sql = format!(
            "UPDATE mysql.user SET {} WHERE Host={} AND User={}",
            assignments.join(","),
            sql_str(host),
            sql_str(user),
        );
        self.run_user_table_write(&sql)
    }
}
