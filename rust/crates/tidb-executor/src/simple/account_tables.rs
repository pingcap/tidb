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

//! The `mysql.*` privilege-table writes of Go's account executors
//! (`pkg/executor/simple.go`'s user arms, `grant.go`, `revoke.go`): the
//! internal SQL each one runs on its system session, as builders over the
//! session primitives [`AccountSession`] names.

use tidb_datatype::Datum;
use tidb_hack::GoToLower;
use tidb_privilege::PrivilegeTypeExt;
use tidb_util::stringutil::go_to_lower;

use crate::sessionctx::SessionContext;
use crate::DriverError;

/// What an account executor reaches through its session: Go
/// `sessionctx.Context` plus the system session's `sqlExecutor`.
pub trait AccountSession: SessionContext + Sized {
    /// Whether the `mysql` privilege tables exist to write (a session whose
    /// catalog was never bootstrapped skips the rows).
    fn user_table_present(&mut self) -> bool;
    /// Runs one internal write against the `mysql` schema.
    fn run_user_table_write(&mut self, sql: &str) -> Result<(), DriverError>;
    /// Runs one internal read against the `mysql` schema.
    fn user_table_rows(&mut self, sql: &str) -> Result<Vec<Vec<Datum>>, DriverError>;
    /// The authenticated `(user, host)` the session runs as.
    fn current_identity(&self) -> Option<(&str, &str)>;
    /// The privilege cache the statement reads (Go `MySQLPrivilege`), when
    /// the session has one.
    fn privilege_registry(&self) -> Option<&tidb_privilege::PrivilegeRegistry>;
    /// The live cache while an account statement runs against stored
    /// accounts: Go's `FindEdge` reads the cache, not the tables.
    fn account_statement_privilege_cache(&self) -> Option<&tidb_privilege::PrivilegeRegistry>;
    /// Whether the statement runs on the system session Go's executor takes
    /// (`GetSysSession`), inside its own transaction.
    fn in_account_statement(&self) -> bool;
    /// Go `RequestVerification` at a scope (`""` for the global one).
    fn has_scoped_privilege(
        &self,
        database: &str,
        table: &str,
        global_priv: tidb_privilege::PrivilegeType,
    ) -> bool;
    /// Go `RequestDynamicVerification`.
    fn has_dynamic_privilege(&self, name: &str, with_grant: bool) -> bool;
    /// Whether another account holds a dynamic privilege.
    fn target_has_dynamic_privilege(&self, user: &str, host: &str, name: &str) -> bool;
    /// Go `ExecRestrictedSQL`.
    fn exec_restricted_sql(&mut self, sql: &str) -> Result<Vec<Vec<Datum>>, DriverError>;
    /// The statement's infoschema.
    fn lock_catalog(&self) -> Result<std::sync::MutexGuard<'_, crate::Catalog>, DriverError>;
    /// The statement's infoschema, mutably (the in-process tables).
    fn with_catalog_mut<T>(
        &mut self,
        body: impl FnOnce(&mut crate::Catalog) -> Result<T, DriverError>,
    ) -> Result<T, DriverError>;
    /// The `RESOURCE_GROUP` user attribute after Go's resource-group checks.
    fn user_resource_group_attribute(&self, name: &str, is_role: bool) -> Result<String, DriverError>;
    /// The statement context an internal read decodes under.
    fn statement_context(&self, is_dml: bool) -> crate::StmtContext;
}

/// Renders one SQL string literal under the default (backslash-escaping)
/// SQL mode, for values interpolated into the internal DML below -- the role
/// Go's `sqlescape` `%?` placeholder plays.
pub fn sql_str(value: &str) -> String {
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
/// plays in `buildAdditionalPasswordEntry` and `GlobalPrivValue`. Escapes what
/// `encoding/json` escapes: quote, backslash, control characters (`\b`, `\f`,
/// `\n`, `\r`, `\t` short forms), and the HTML-safe set `<`, `>`, `&`,
/// U+2028 and U+2029.
pub fn json_string_literal(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '<' | '>' | '&' | '\u{2028}' | '\u{2029}' => {
                out.push_str(&format!("\\u{:04x}", ch as u32));
            }
            control if (control as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", control as u32));
            }
            _ => out.push(ch),
        }
    }
    out.push('"');
    out
}

/// Which statement writes an account's `mysql.global_priv` row: Go's GRANT,
/// CREATE USER and ALTER USER each use their own statement form.
pub enum GlobalPrivWrite {
    /// `GRANT`.
    Grant,
    /// `CREATE USER`.
    CreateUser,
    /// `ALTER USER`.
    AlterUser,
}

/// Go `json.Marshal(&privileges.GlobalPrivValue{...})`: the policy's fields
/// compact and in the struct's order (`ssl_type`, `ssl_cipher`,
/// `x509_issuer`, `x509_subject`, `san`), each omitted when empty.
fn go_global_priv_json(json: &str) -> String {
    let value: serde_json::Value = serde_json::from_str(json).unwrap_or_default();
    let mut fields = Vec::new();
    for key in [
        "ssl_type",
        "ssl_cipher",
        "x509_issuer",
        "x509_subject",
        "san",
    ] {
        match value.get(key) {
            Some(serde_json::Value::Number(number)) if number.as_i64() != Some(0) => {
                fields.push(format!("\"{key}\":{number}"));
            }
            Some(serde_json::Value::String(text)) if !text.is_empty() => {
                fields.push(format!("\"{key}\":{}", json_string_literal(text)));
            }
            _ => {}
        }
    }
    format!("{{{}}}", fields.join(","))
}

/// Go `SetFromString`.
fn set_from_string(value: &str) -> Vec<String> {
    if value.is_empty() {
        return Vec::new();
    }
    value.split(',').map(str::to_owned).collect()
}

/// Go `addToSet`.
fn add_to_set(set: &mut Vec<String>, value: &str) {
    if !set.iter().any(|existing| existing == value) {
        set.push(value.to_owned());
    }
}

/// Go `deleteFromSet`.
fn delete_from_set(set: &mut Vec<String>, value: &str) {
    if let Some(position) = set.iter().position(|existing| existing == value) {
        set.remove(position);
    }
}

/// A privilege-table cell as text (Go `Row.GetString`, "" for NULL).
pub fn datum_text(value: &Datum) -> String {
    if value.is_null() {
        return String::new();
    }
    value
        .to_bytes()
        .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
        .unwrap_or_default()
}

/// The privilege-table writes, over [`AccountSession`].
pub trait UserTableExec: AccountSession {
    /// Deletes the account's `mysql.password_history` rows.
    fn mirror_delete_password_history(
        &mut self,
        user: &str,
        host: &str,
    ) -> Result<(), DriverError> {
        if !self.user_table_present() {
            return Ok(());
        }
        self.run_user_table_write(&format!(
            "DELETE FROM mysql.password_history WHERE User={} AND Host={}",
            sql_str(user),
            sql_str(host)
        ))
    }

    /// Moves the account's `mysql.password_history` rows to its new name.
    fn mirror_rename_password_history(
        &mut self,
        old_user: &str,
        old_host: &str,
        new_user: &str,
        new_host: &str,
    ) -> Result<(), DriverError> {
        if !self.user_table_present() {
            return Ok(());
        }
        self.run_user_table_write(&format!(
            "UPDATE mysql.password_history SET User={}, Host={} WHERE User={} AND Host={}",
            sql_str(new_user),
            sql_str(new_host),
            sql_str(old_user),
            sql_str(old_host)
        ))
    }

    /// Writes the account's reuse policy and `User_attributes` to
    /// `mysql.user`, and replaces its `mysql.password_history` rows with the
    /// registry's.
    fn mirror_account_policy_and_history(
        &mut self,
        registry: &tidb_privilege::PrivilegeRegistry,
        user: &str,
        host: &str,
    ) -> Result<(), DriverError> {
        if !self.user_table_present() {
            return Ok(());
        }
        let (count, days) = registry.password_reuse_policy(user, host);
        let count = count.map_or_else(|| "NULL".into(), |n| n.to_string());
        let days = days.map_or_else(|| "NULL".into(), |n| n.to_string());
        let attrs = registry
            .user_attributes(user, host)
            .as_deref()
            .map_or_else(|| "NULL".into(), sql_str);
        self.run_user_table_write(&format!("UPDATE mysql.user SET Password_reuse_history={count}, Password_reuse_time={days}, User_attributes={attrs} WHERE User={} AND Host={}", sql_str(user), sql_str(host)))?;
        self.mirror_delete_password_history(user, host)?;
        for row in registry.account_password_history(user, host) {
            // Registry timestamps are UTC; SQL TIMESTAMP text belongs to this session.
            let mut timestamp = row.timestamp;
            timestamp
                .convert_time_zone(
                    &tidb_datatype::SessionTimeZone::utc(),
                    &self.session_vars().session_time_zone(),
                )
                .map_err(|error| {
                    DriverError::Exec(crate::ExecError::internal(error.to_string()))
                })?;
            self.run_user_table_write(&format!("INSERT INTO mysql.password_history (Host, User, Password_timestamp, Password) VALUES ({}, {}, {}, {})", sql_str(&row.host), sql_str(&row.user), sql_str(&timestamp.to_string()), row.password.as_deref().map_or_else(|| "NULL".into(), sql_str)))?;
        }
        Ok(())
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
    fn mirror_create_user_row(
        &mut self,
        user: &str,
        host: &str,
        auth_string: &str,
        plugin: &str,
        user_attributes_json: &str,
        account_locked: bool,
        password_expired: bool,
        password_lifetime: Option<i64>,
        max_user_connections: i64,
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
             {locked}, '', {expired}, {lifetime}, {max_user_connections}, null, null)",
            // Go lowercases the stored Host (`hostName :=
            // strings.ToLower(spec.User.Hostname)`).
            host = sql_str(&go_to_lower(host)),
            user = sql_str(user),
            auth = sql_str(auth_string),
            plugin = sql_str(plugin),
            attrs = sql_str(user_attributes_json),
            locked = if account_locked { "'Y'" } else { "'N'" },
            expired = if password_expired { "'Y'" } else { "'N'" },
        );
        self.run_user_table_write(&sql)
    }

    /// Mirrors the account GRANT creates without `NO_AUTO_CREATE_USER` --
    /// Go `GrantExec.Next`'s `INSERT INTO mysql.user (Host, User,
    /// authentication_string, plugin)`, every other column at its default
    /// and the host stored as written.
    fn mirror_grant_created_user_row(
        &mut self,
        user: &str,
        host: &str,
        auth_string: &str,
        plugin: &str,
    ) -> Result<(), DriverError> {
        if !self.user_table_present() {
            return Ok(());
        }
        let sql = format!(
            "INSERT INTO mysql.user (Host, User, authentication_string, plugin) VALUES ({}, {}, {}, {})",
            sql_str(host),
            sql_str(user),
            sql_str(auth_string),
            sql_str(plugin),
        );
        self.run_user_table_write(&sql)
    }

    /// Mirrors `DROP USER`'s row removal -- Go `executeDropUser`'s
    /// `DELETE FROM mysql.user WHERE Host = %? and User = %?`.
    fn mirror_drop_user_row(
        &mut self,
        user: &str,
        host: &str,
    ) -> Result<(), DriverError> {
        if !self.user_table_present() {
            return Ok(());
        }
        let sql = format!(
            "DELETE FROM mysql.user WHERE Host = {} and User = {}",
            sql_str(&go_to_lower(host)),
            sql_str(user),
        );
        self.run_user_table_write(&sql)
    }

    /// Mirrors `RENAME USER`'s row move -- Go `executeRenameUser` ->
    /// `renameUserHostInSystemTable` on `mysql.user`: `UPDATE %n.%n SET
    /// User=%?, Host=%? WHERE User=%? AND Host=%?` (host lowercased on both
    /// sides).
    fn mirror_rename_user_row(
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
            sql_str(&go_to_lower(new_host)),
            sql_str(old_user),
            sql_str(&go_to_lower(old_host)),
        );
        self.run_user_table_write(&sql)
    }

    /// Mirrors a GLOBAL `GRANT`/`REVOKE`'s static-privilege bits into the
    /// account's `mysql.user` privilege columns -- Go `grant.go`
    /// `composeGlobalPrivUpdate` (`%n='Y'` per named privilege, every
    /// `AllGlobalPrivs` column for `ALL`) and `revoke.go`'s `'N'` twin. The
    /// caller passes the same mask it handed the registry, so `WITH GRANT
    /// OPTION`'s `Grant_priv` rides in the mask exactly as it does there.
    fn mirror_global_priv_columns(
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
        for priv_ in tidb_mysql::ALL_GLOBAL_PRIVS
            .iter()
            .chain(std::iter::once(&tidb_mysql::GrantPriv))
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

    /// Go `ctx.GetSessionVars().User.String()`: the Grantor Go records.
    fn grantor(&self) -> String {
        self.current_identity()
            .map_or_else(String::new, |(user, host)| format!("{user}@{host}"))
    }

    /// Go `grant.go` `checkAndInitDBPriv` + `grantDBLevel` and `revoke.go`
    /// `revokeDBPriv`: the bits in `mask` on the account's `mysql.db` row set
    /// to `'Y'` (the row inserted first when absent) or `'N'` (the row deleted
    /// once every DB privilege and `Grant_priv` is `'N'`).
    fn mirror_db_privs(
        &mut self,
        user: &str,
        host: &str,
        db: &str,
        mask: u64,
        grant: bool,
    ) -> Result<(), DriverError> {
        // Go's `revokePriv` returns before `revokeDBPriv` for USAGE.
        if !self.user_table_present() || (!grant && mask == 0) {
            return Ok(());
        }
        let key = format!(
            "User={} AND Host={} AND DB={}",
            sql_str(user),
            sql_str(host),
            sql_str(db)
        );
        let scope = tidb_mysql::ALL_DB_PRIVS
            .iter()
            .chain(std::iter::once(&tidb_mysql::GrantPriv));
        let value = if grant { "'Y'" } else { "'N'" };
        let assignments: Vec<String> = scope
            .clone()
            .filter(|priv_| mask & priv_.bit() != 0)
            .map(|priv_| format!("{}={value}", priv_.user_table_column()))
            .collect();
        if grant
            && self
                .user_table_rows(&format!("SELECT 1 FROM mysql.db WHERE {key}"))?
                .is_empty()
        {
            self.run_user_table_write(&format!(
                "INSERT INTO mysql.db (Host, User, DB) VALUES ({}, {}, {})",
                sql_str(host),
                sql_str(user),
                sql_str(db)
            ))?;
        }
        if !assignments.is_empty() {
            self.run_user_table_write(&format!(
                "UPDATE mysql.db SET {} WHERE {key}",
                assignments.join(",")
            ))?;
        }
        if !grant {
            let all_no: Vec<String> = scope
                .map(|priv_| format!(" AND {}='N'", priv_.user_table_column()))
                .collect();
            self.run_user_table_write(&format!(
                "DELETE FROM mysql.db WHERE {key}{}",
                all_no.concat()
            ))?;
        }
        Ok(())
    }

    /// Go `checkAndInitTablePriv` + `composeTablePrivUpdateForGrant` and
    /// `composeTablePrivUpdateForRevoke`: the account's `mysql.tables_priv`
    /// row, its `Table_priv`/`Column_priv` sets edited as Go edits the
    /// strings it reads back. A grant of `ALL` replaces both sets with
    /// `AllTablePrivs`/`AllColumnPrivs`; a revoke deletes the row once
    /// `Table_priv` is empty.
    fn mirror_table_privs(
        &mut self,
        user: &str,
        host: &str,
        db: &str,
        table: &str,
        privs: &[tidb_privilege::PrivilegeType],
        all: bool,
        grant: bool,
    ) -> Result<(), DriverError> {
        if !self.user_table_present() {
            return Ok(());
        }
        let key = format!(
            "User={} AND Host={} AND DB={} AND Table_name={}",
            sql_str(user),
            sql_str(host),
            sql_str(db),
            sql_str(table)
        );
        let rows = self.user_table_rows(&format!(
            "SELECT Table_priv, Column_priv FROM mysql.tables_priv WHERE {key}"
        ))?;
        // A statement naming only column privileges reaches
        // `checkAndInitTablePriv` (GRANT) but never `grantTableLevel` /
        // `revokeTablePriv`: the row exists, its sets and Grantor untouched.
        if privs.is_empty() && !all {
            if grant && rows.is_empty() {
                self.run_user_table_write(&format!(
                    "INSERT INTO mysql.tables_priv (Host, User, DB, Table_name, Table_priv, \
                     Column_priv) VALUES ({}, {}, {}, {}, '', '')",
                    sql_str(host),
                    sql_str(user),
                    sql_str(db),
                    sql_str(table)
                ))?;
            }
            return Ok(());
        }
        let (mut table_privs, mut column_privs) = match rows.first() {
            Some(row) => (
                set_from_string(&datum_text(&row[0])),
                set_from_string(&datum_text(&row[1])),
            ),
            None if grant => {
                self.run_user_table_write(&format!(
                    "INSERT INTO mysql.tables_priv (Host, User, DB, Table_name, Table_priv, \
                     Column_priv) VALUES ({}, {}, {}, {}, '', '')",
                    sql_str(host),
                    sql_str(user),
                    sql_str(db),
                    sql_str(table)
                ))?;
                (Vec::new(), Vec::new())
            }
            None => return Ok(()),
        };
        if grant {
            if all {
                table_privs = tidb_mysql::ALL_TABLE_PRIVS
                    .iter()
                    .map(|priv_| priv_.set_name().to_owned())
                    .collect();
                column_privs = tidb_mysql::ALL_COLUMN_PRIVS
                    .iter()
                    .map(|priv_| priv_.set_name().to_owned())
                    .collect();
            }
            for priv_ in privs {
                add_to_set(&mut table_privs, priv_.set_name());
                if tidb_mysql::ALL_COLUMN_PRIVS.contains(priv_) {
                    add_to_set(&mut column_privs, priv_.set_name());
                }
            }
        } else {
            for priv_ in privs {
                delete_from_set(&mut table_privs, priv_.set_name());
                delete_from_set(&mut column_privs, priv_.set_name());
            }
        }
        self.run_user_table_write(&format!(
            "UPDATE mysql.tables_priv SET Table_priv={}, Column_priv={}, Grantor={} WHERE {key}",
            sql_str(&table_privs.join(",")),
            sql_str(&column_privs.join(",")),
            sql_str(&self.grantor())
        ))?;
        if !grant && table_privs.is_empty() {
            self.run_user_table_write(&format!("DELETE FROM mysql.tables_priv WHERE {key}"))?;
        }
        Ok(())
    }

    /// Go `checkAndInitColumnPriv` + `composeColumnPrivUpdateForGrant` and
    /// `revokeColumnPriv`: each column's `mysql.columns_priv` row, its
    /// `Column_priv` set edited as Go edits it; a revoke deletes a row once
    /// the set is empty.
    fn mirror_column_privs(
        &mut self,
        user: &str,
        host: &str,
        db: &str,
        table: &str,
        columns: &[(String, u64)],
        grant: bool,
    ) -> Result<(), DriverError> {
        if !self.user_table_present() {
            return Ok(());
        }
        for (column, mask) in columns {
            let key = format!(
                "User={} AND Host={} AND DB={} AND Table_name={} AND Column_name={}",
                sql_str(user),
                sql_str(host),
                sql_str(db),
                sql_str(table),
                sql_str(column)
            );
            let rows = self.user_table_rows(&format!(
                "SELECT Column_priv FROM mysql.columns_priv WHERE {key}"
            ))?;
            let mut column_privs = match rows.first() {
                Some(row) => set_from_string(&datum_text(&row[0])),
                None if grant => {
                    self.run_user_table_write(&format!(
                        "INSERT INTO mysql.columns_priv (Host, User, DB, Table_name, \
                         Column_name, Column_priv) VALUES ({}, {}, {}, {}, {}, '')",
                        sql_str(host),
                        sql_str(user),
                        sql_str(db),
                        sql_str(table),
                        sql_str(column)
                    ))?;
                    Vec::new()
                }
                None => continue,
            };
            for priv_ in tidb_mysql::ALL_COLUMN_PRIVS {
                if mask & priv_.bit() == 0 {
                    continue;
                }
                if grant {
                    add_to_set(&mut column_privs, priv_.set_name());
                } else {
                    delete_from_set(&mut column_privs, priv_.set_name());
                }
            }
            self.run_user_table_write(&format!(
                "UPDATE mysql.columns_priv SET Column_priv={} WHERE {key}",
                sql_str(&column_privs.join(","))
            ))?;
            if !grant && column_privs.is_empty() {
                self.run_user_table_write(&format!("DELETE FROM mysql.columns_priv WHERE {key}"))?;
            }
        }
        Ok(())
    }

    /// Go `grantDynamicPriv` (`REPLACE INTO mysql.global_grants`) and
    /// `revokeDynamicPriv` (`DELETE`); `name` is upper-cased as Go stores it.
    fn mirror_dynamic_priv(
        &mut self,
        user: &str,
        host: &str,
        name: &str,
        grant_option: Option<bool>,
    ) -> Result<(), DriverError> {
        if !self.user_table_present() {
            return Ok(());
        }
        let name = tidb_hack::go_to_upper(name);
        match grant_option {
            Some(with_grant) => self.run_user_table_write(&format!(
                "REPLACE INTO mysql.global_grants (user,host,priv,with_grant_option) VALUES \
                 ({}, {}, {}, {})",
                sql_str(user),
                sql_str(host),
                sql_str(&name),
                if with_grant { "'Y'" } else { "'N'" }
            )),
            None => self.run_user_table_write(&format!(
                "DELETE FROM mysql.global_grants WHERE user = {} AND host = {} AND priv = {}",
                sql_str(user),
                sql_str(host),
                sql_str(&name)
            )),
        }
    }

    /// Go `revokeGlobalPriv` for `ALL`: every dynamic privilege of the
    /// account goes with it.
    fn mirror_revoke_all_dynamic(
        &mut self,
        user: &str,
        host: &str,
    ) -> Result<(), DriverError> {
        if !self.user_table_present() {
            return Ok(());
        }
        self.run_user_table_write(&format!(
            "DELETE FROM mysql.global_grants WHERE user = {} AND host = {}",
            sql_str(user),
            sql_str(host)
        ))
    }

    /// The account's `mysql.global_priv` row as Go writes it: `GRANT ...
    /// REQUIRE` (`checkAndInitGlobalPriv` + `grantGlobalPriv`), `CREATE USER`
    /// (`INSERT IGNORE`) and `ALTER USER` (`ON DUPLICATE KEY UPDATE`). `json`
    /// is the registry's policy, re-encoded in Go's `GlobalPrivValue` field
    /// order.
    fn mirror_global_priv(
        &mut self,
        user: &str,
        host: &str,
        json: &str,
        write: GlobalPrivWrite,
    ) -> Result<(), DriverError> {
        if !self.user_table_present() {
            return Ok(());
        }
        let privilege = sql_str(&go_global_priv_json(json));
        let (host_lit, user_lit) = (sql_str(host), sql_str(user));
        match write {
            GlobalPrivWrite::Grant => {
                if self
                    .user_table_rows(&format!(
                        "SELECT 1 FROM mysql.global_priv WHERE User={user_lit} AND Host={host_lit}"
                    ))?
                    .is_empty()
                {
                    self.run_user_table_write(&format!(
                        "INSERT INTO mysql.global_priv (Host, User, PRIV) VALUES ({host_lit}, \
                         {user_lit}, '{{}}')"
                    ))?;
                }
                self.run_user_table_write(&format!(
                    "UPDATE mysql.global_priv SET PRIV={privilege} WHERE User={user_lit} AND \
                     Host={host_lit}"
                ))
            }
            GlobalPrivWrite::CreateUser => self.run_user_table_write(&format!(
                "INSERT IGNORE INTO mysql.global_priv (Host, User, Priv) VALUES ({host_lit}, \
                 {user_lit}, {privilege})"
            )),
            GlobalPrivWrite::AlterUser => self.run_user_table_write(&format!(
                "INSERT INTO mysql.global_priv (Host, User, Priv) VALUES ({host_lit}, \
                 {user_lit}, {privilege}) ON DUPLICATE KEY UPDATE Priv = values(Priv)"
            )),
        }
    }

    /// Go `executeGrantRole` (`INSERT IGNORE INTO mysql.role_edges`) and
    /// `executeRevokeRole` (the edge and the matching default role).
    fn mirror_role_edge(
        &mut self,
        role: (&str, &str),
        user: (&str, &str),
        grant: bool,
    ) -> Result<(), DriverError> {
        if !self.user_table_present() {
            return Ok(());
        }
        let (role_user, role_host) = (sql_str(role.0), sql_str(role.1));
        let (to_user, to_host) = (sql_str(user.0), sql_str(user.1));
        if grant {
            return self.run_user_table_write(&format!(
                "INSERT IGNORE INTO mysql.role_edges (FROM_HOST, FROM_USER, TO_HOST, TO_USER) \
                 VALUES ({role_host},{role_user},{to_host},{to_user})"
            ));
        }
        self.run_user_table_write(&format!(
            "DELETE IGNORE FROM mysql.role_edges WHERE FROM_HOST={role_host} and \
             FROM_USER={role_user} and TO_HOST={to_host} and TO_USER={to_user}"
        ))?;
        self.run_user_table_write(&format!(
            "DELETE IGNORE FROM mysql.default_roles WHERE DEFAULT_ROLE_HOST={role_host} and \
             DEFAULT_ROLE_USER={role_user} and HOST={to_host} and USER={to_user}"
        ))
    }

    /// Go `setDefaultRoleNone` / `setDefaultRoleRegular` / `setDefaultRoleAll`:
    /// the account's `mysql.default_roles` rows replaced.
    fn mirror_default_roles(
        &mut self,
        user: &str,
        host: &str,
        roles: Option<&[(String, String)]>,
    ) -> Result<(), DriverError> {
        if !self.user_table_present() {
            return Ok(());
        }
        let (user_lit, host_lit) = (sql_str(user), sql_str(host));
        self.run_user_table_write(&format!(
            "DELETE IGNORE FROM mysql.default_roles WHERE USER={user_lit} AND HOST={host_lit}"
        ))?;
        match roles {
            Some(roles) => {
                for (role_user, role_host) in roles {
                    self.run_user_table_write(&format!(
                        "INSERT IGNORE INTO mysql.default_roles values({host_lit}, {user_lit}, \
                         {}, {})",
                        sql_str(role_host),
                        sql_str(role_user)
                    ))?;
                }
                Ok(())
            }
            // `SET DEFAULT ROLE ALL`: every role granted to the account.
            None => self.run_user_table_write(&format!(
                "INSERT IGNORE INTO mysql.default_roles(HOST,USER,DEFAULT_ROLE_HOST,\
                 DEFAULT_ROLE_USER) SELECT TO_HOST,TO_USER,FROM_HOST,FROM_USER FROM \
                 mysql.role_edges WHERE TO_HOST={host_lit} AND TO_USER={user_lit}"
            )),
        }
    }

    /// Go `executeDropUser`'s deletes after `mysql.user` and
    /// `mysql.password_history`, in its order and with its spelling.
    fn mirror_drop_user_privileges(
        &mut self,
        user: &str,
        host: &str,
    ) -> Result<(), DriverError> {
        if !self.user_table_present() {
            return Ok(());
        }
        let (user, host) = (sql_str(user), sql_str(host));
        for (table, user_column, host_column) in [
            ("global_priv", "User", "Host"),
            ("db", "User", "Host"),
            ("tables_priv", "User", "Host"),
            ("columns_priv", "User", "Host"),
            ("role_edges", "TO_USER", "TO_HOST"),
            ("role_edges", "FROM_USER", "FROM_HOST"),
            ("default_roles", "DEFAULT_ROLE_USER", "DEFAULT_ROLE_HOST"),
            ("default_roles", "USER", "HOST"),
            ("global_grants", "User", "Host"),
        ] {
            self.run_user_table_write(&format!(
                "DELETE FROM mysql.{table} WHERE {host_column} = {host} and {user_column} = {user}"
            ))?;
        }
        Ok(())
    }

    /// Go `executeRenameUser`'s `renameUserHostInSystemTable` calls after
    /// `mysql.user`: hosts lowercased on both sides, and -- as in Go --
    /// `mysql.columns_priv` left alone.
    fn mirror_rename_user_privileges(
        &mut self,
        old_user: &str,
        old_host: &str,
        new_user: &str,
        new_host: &str,
    ) -> Result<(), DriverError> {
        if !self.user_table_present() {
            return Ok(());
        }
        let (old_user, old_host) = (sql_str(old_user), sql_str(&old_host.go_to_lower()));
        let (new_user, new_host) = (sql_str(new_user), sql_str(&new_host.go_to_lower()));
        for (table, user_column, host_column) in [
            ("global_priv", "User", "Host"),
            ("db", "User", "Host"),
            ("tables_priv", "User", "Host"),
            ("role_edges", "TO_USER", "TO_HOST"),
            ("role_edges", "FROM_USER", "FROM_HOST"),
            ("default_roles", "DEFAULT_ROLE_USER", "DEFAULT_ROLE_HOST"),
            ("default_roles", "USER", "HOST"),
            ("global_grants", "User", "Host"),
        ] {
            self.run_user_table_write(&format!(
                "UPDATE mysql.{table} SET {user_column} = {new_user}, {host_column} = {new_host} \
                 WHERE {user_column} = {old_user} and {host_column} = {old_host}"
            ))?;
        }
        Ok(())
    }
}

impl<S: AccountSession> UserTableExec for S {}

pub use tidb_datatype::datum_text::sysvar_datum_text;
