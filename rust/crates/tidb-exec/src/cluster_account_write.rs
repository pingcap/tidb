// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Writing a cluster's accounts and grants back into `mysql.*`.
//!
//! This is the write half of the bridge [`crate::cluster_privilege_load`]
//! reads: whatever a `CREATE USER`/`GRANT` did to this node's account table
//! is written back as the very rows a Go TiDB reads.
//!
//! # Why this plans a whole image rather than one statement's delta
//!
//! An account statement's *meaning* already lives in
//! [`tidb_session::privilege::PrivilegeRegistry`] -- it validates `GRANT
//! SELECT ON *.*`, decides that `CREATE USER IF NOT EXISTS` is a no-op, knows
//! that `DROP USER` also drops the grantee's role edges. Re-deriving all of
//! that from the AST here would be a second, divergent implementation of the
//! same rules.
//!
//! So this path takes the *result* instead: the caller hands over the account
//! table as it should now be, and this module makes the cluster's rows match
//! it. Every statement shape collapses into one operation, and the invariant
//! is exact and testable: after applying this plan,
//! [`crate::cluster_privilege_load::load_cluster_privileges`] reads back the
//! image that was planned. Because the desired image is itself built from a
//! registry seeded from *this same snapshot*, the diff is precisely the
//! statement's own effect and never reverts a change another node made.
//!
//! # What it writes, and what it refuses
//!
//! `mysql.user`, `mysql.db`, `mysql.global_grants`, `mysql.role_edges` and
//! `mysql.default_roles` are written. `mysql.tables_priv` and
//! `mysql.columns_priv` are only ever *deleted from* (when the account they
//! belong to is gone); a plan that would have to write one is refused by name,
//! because their `SET` privilege columns are a value shape this writer does not
//! encode yet, and a table-scoped grant silently dropped is worse than a
//! refused statement.

use std::collections::BTreeMap;

use tidb_datatype::{Datum, Time};
use tidb_meta::{key, value};
use tidb_model::table_info::TableInfo;
use tidb_txnkv::transaction::OptimisticMutation;

use crate::cluster_catalog::{ClusterCatalog, MetaSnapshot};
use crate::cluster_privilege_load::{
    ClusterPrivileges, DB_PRIVILEGE_COLUMNS, USER_PRIVILEGE_COLUMNS,
};
use crate::mysql_system_tables::{
    scan_system_table_keyed, SystemTableError, SystemTableView, SYSTEM_DB,
};
use crate::system_row_write::{
    defaults_row, delete_row, indexed_columns, insert_row, row_id_of, update_row, RowEncodeError,
    RowValues, NO, YES,
};

/// Why an account image could not be written.
#[derive(Debug)]
pub enum AccountWriteError {
    /// The cluster has no such `mysql.*` table, or no `mysql` schema at all.
    MissingTable(String),
    /// A stored row could not be read.
    Read(SystemTableError),
    /// A row could not be encoded.
    Encode(RowEncodeError),
    /// The change is one this writer does not express; the text names it.
    Unsupported(String),
}

impl std::fmt::Display for AccountWriteError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingTable(name) => write!(formatter, "the cluster has no {name}"),
            Self::Read(error) => write!(formatter, "reading the stored accounts failed: {error}"),
            Self::Encode(error) => write!(formatter, "encoding an account row failed: {error}"),
            Self::Unsupported(detail) => formatter.write_str(detail),
        }
    }
}

impl std::error::Error for AccountWriteError {}

impl From<SystemTableError> for AccountWriteError {
    fn from(error: SystemTableError) -> Self {
        Self::Read(error)
    }
}

impl From<RowEncodeError> for AccountWriteError {
    fn from(error: RowEncodeError) -> Self {
        Self::Encode(error)
    }
}

impl From<crate::cluster_catalog::ClusterCatalogError> for AccountWriteError {
    fn from(error: crate::cluster_catalog::ClusterCatalogError) -> Self {
        Self::Read(SystemTableError::Snapshot(error.to_string()))
    }
}

/// One planned account change: the mutations that make the cluster's rows
/// match the desired image, and who they are about.
#[derive(Debug, Default)]
pub struct AccountWritePlan {
    /// The mutations, in no particular order (they touch distinct keys).
    pub mutations: Vec<OptimisticMutation>,
    /// The `'user'@'host'` identities whose rows this plan touches, which is
    /// what the etcd notification names so peers can reload just them.
    pub changed_users: Vec<String>,
}

impl AccountWritePlan {
    /// Whether the desired image already matches what the cluster stores.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.mutations.is_empty()
    }
}

/// One `mysql.*` table this writer maintains, as data.
struct AccountTable {
    /// The table name under `mysql.`.
    name: &'static str,
    /// The lowercase columns that identify a row, in the order they are keyed.
    key_columns: &'static [&'static str],
    /// The lowercase columns whose values this writer owns. Every other
    /// column of a stored row is left exactly as it was found, and a new row
    /// takes its declared `DEFAULT`.
    value_columns: &'static [&'static str],
}

/// One logical row of an account table: its identity and the values this
/// writer owns.
type LogicalRows = BTreeMap<Vec<String>, BTreeMap<&'static str, String>>;

const USER_TABLE: &str = "user";
const DB_TABLE: &str = "db";
const GLOBAL_GRANTS_TABLE: &str = "global_grants";
const ROLE_EDGES_TABLE: &str = "role_edges";
const DEFAULT_ROLES_TABLE: &str = "default_roles";
/// The two tables this writer only ever deletes from.
const SCOPED_GRANT_TABLES: &[&str] = &["tables_priv", "columns_priv"];

/// Plans the mutations that make the cluster's `mysql.*` rows equal `desired`.
///
/// `snapshot` must be the *same* snapshot the caller built `desired` from, and
/// the mutations must be committed on that snapshot's transaction: that is
/// what makes a concurrent writer a write conflict at prewrite rather than a
/// silent overwrite of somebody else's `GRANT`.
pub fn plan_account_write<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    desired: &ClusterPrivileges,
    now: Time,
) -> Result<AccountWritePlan, AccountWriteError> {
    let mut plan = AccountWritePlan::default();
    let mut changed = std::collections::BTreeSet::new();

    for (table, desired_rows) in [
        (user_table(), user_rows(desired)),
        (db_table(), db_rows(desired)),
        (global_grants_table(), dynamic_rows(desired)),
        (role_edges_table(), role_edge_rows(desired)),
        (default_roles_table(), default_role_rows(desired)),
    ] {
        reconcile(
            snapshot,
            catalog,
            &table,
            &desired_rows,
            now,
            &mut plan,
            &mut changed,
        )?;
    }

    // The scoped-grant tables are not modelled, so the only change they may
    // take is losing the rows of an account that no longer exists. Anything
    // else -- a granted, revoked or altered table/column privilege -- is
    // refused by name, because silently dropping one is far worse.
    let live: std::collections::BTreeSet<(String, String)> = desired
        .users
        .iter()
        .map(|user| (user.host.clone(), user.user.clone()))
        .collect();
    for name in SCOPED_GRANT_TABLES {
        reconcile_scoped_grants(
            snapshot,
            catalog,
            name,
            desired,
            &live,
            &mut plan,
            &mut changed,
        )?;
    }

    plan.changed_users = changed.into_iter().collect();
    Ok(plan)
}

fn user_table() -> AccountTable {
    AccountTable {
        name: USER_TABLE,
        key_columns: &["host", "user"],
        value_columns: USER_VALUE_COLUMNS,
    }
}

fn db_table() -> AccountTable {
    AccountTable {
        name: DB_TABLE,
        key_columns: &["host", "user", "db"],
        value_columns: DB_VALUE_COLUMNS,
    }
}

fn global_grants_table() -> AccountTable {
    AccountTable {
        name: GLOBAL_GRANTS_TABLE,
        key_columns: &["host", "user", "priv"],
        value_columns: &["with_grant_option"],
    }
}

fn role_edges_table() -> AccountTable {
    AccountTable {
        name: ROLE_EDGES_TABLE,
        key_columns: &["from_host", "from_user", "to_host", "to_user"],
        value_columns: &[],
    }
}

fn default_roles_table() -> AccountTable {
    AccountTable {
        name: DEFAULT_ROLES_TABLE,
        key_columns: &["host", "user", "default_role_host", "default_role_user"],
        value_columns: &[],
    }
}

/// The `mysql.user` columns this writer owns: exactly the non-key columns
/// [`crate::cluster_privilege_load`] reads back, so the round trip is closed.
/// A column outside this list is a column the account table does not model,
/// and an existing row keeps whatever it holds there.
const USER_VALUE_COLUMNS: &[&str] = &[
    "authentication_string",
    "plugin",
    "account_locked",
    "password_expired",
    "select_priv",
    "insert_priv",
    "update_priv",
    "delete_priv",
    "create_priv",
    "drop_priv",
    "process_priv",
    "grant_priv",
    "references_priv",
    "alter_priv",
    "show_db_priv",
    "super_priv",
    "create_tmp_table_priv",
    "lock_tables_priv",
    "execute_priv",
    "create_view_priv",
    "show_view_priv",
    "create_routine_priv",
    "alter_routine_priv",
    "index_priv",
    "create_user_priv",
    "event_priv",
    "repl_slave_priv",
    "repl_client_priv",
    "trigger_priv",
    "shutdown_priv",
    "reload_priv",
    "file_priv",
    "config_priv",
    "create_tablespace_priv",
];

/// The `mysql.db` columns this writer owns, likewise mirroring the loader.
const DB_VALUE_COLUMNS: &[&str] = &[
    "select_priv",
    "insert_priv",
    "update_priv",
    "delete_priv",
    "create_priv",
    "drop_priv",
    "grant_priv",
    "references_priv",
    "index_priv",
    "alter_priv",
    "create_tmp_table_priv",
    "lock_tables_priv",
    "create_view_priv",
    "show_view_priv",
    "create_routine_priv",
    "alter_routine_priv",
    "execute_priv",
    "event_priv",
    "trigger_priv",
];

fn yes_no(granted: bool) -> String {
    if granted { YES } else { NO }.to_owned()
}

fn user_rows(desired: &ClusterPrivileges) -> LogicalRows {
    let mut rows = LogicalRows::new();
    for user in &desired.users {
        let mut values = BTreeMap::new();
        values.insert("authentication_string", user.authentication_string.clone());
        values.insert("plugin", user.plugin.clone());
        values.insert("account_locked", yes_no(user.account_locked));
        values.insert("password_expired", yes_no(user.password_expired));
        for (column, printed) in USER_PRIVILEGE_COLUMNS {
            values.insert(
                *column,
                yes_no(user.privileges.iter().any(|held| held == printed)),
            );
        }
        rows.insert(vec![user.host.clone(), user.user.clone()], values);
    }
    rows
}

fn db_rows(desired: &ClusterPrivileges) -> LogicalRows {
    let mut rows = LogicalRows::new();
    for grant in &desired.db_grants {
        let mut values = BTreeMap::new();
        for (column, printed) in DB_PRIVILEGE_COLUMNS {
            values.insert(
                *column,
                yes_no(grant.privileges.iter().any(|held| held == printed)),
            );
        }
        rows.insert(
            vec![
                grant.host.clone(),
                grant.user.clone(),
                grant.database.clone(),
            ],
            values,
        );
    }
    rows
}

fn dynamic_rows(desired: &ClusterPrivileges) -> LogicalRows {
    let mut rows = LogicalRows::new();
    for grant in &desired.dynamic_grants {
        rows.insert(
            vec![
                grant.host.clone(),
                grant.user.clone(),
                grant.privilege.clone(),
            ],
            BTreeMap::from([("with_grant_option", yes_no(grant.with_grant_option))]),
        );
    }
    rows
}

fn role_edge_rows(desired: &ClusterPrivileges) -> LogicalRows {
    let mut rows = LogicalRows::new();
    for edge in &desired.role_edges {
        rows.insert(
            vec![
                edge.role_host.clone(),
                edge.role_user.clone(),
                edge.grantee_host.clone(),
                edge.grantee_user.clone(),
            ],
            BTreeMap::new(),
        );
    }
    rows
}

fn default_role_rows(desired: &ClusterPrivileges) -> LogicalRows {
    let mut rows = LogicalRows::new();
    for role in &desired.default_roles {
        rows.insert(
            vec![
                role.host.clone(),
                role.user.clone(),
                role.role_host.clone(),
                role.role_user.clone(),
            ],
            BTreeMap::new(),
        );
    }
    rows
}

/// One stored row: where it lives and everything it holds.
struct StoredRow {
    key: Vec<u8>,
    values: RowValues,
}

fn locate<'catalog>(
    catalog: &'catalog ClusterCatalog,
    table: &str,
) -> Result<&'catalog TableInfo, AccountWriteError> {
    catalog
        .databases
        .iter()
        .find(|database| database.info.name.lowercase() == SYSTEM_DB)
        .and_then(|database| {
            database
                .tables
                .iter()
                .find(|stored| stored.name.lowercase() == table)
        })
        .ok_or_else(|| AccountWriteError::MissingTable(format!("{SYSTEM_DB}.{table}")))
}

/// Every public column of one table, so a row is decoded whole rather than
/// projected: a rewrite must preserve the columns this writer does not own.
fn full_view(table: &TableInfo) -> SystemTableView {
    let columns: Vec<&str> = table
        .cols()
        .iter()
        .map(|column| column.name.lowercase())
        .collect();
    SystemTableView::project(
        &format!("{SYSTEM_DB}.{}", table.name.original()),
        table,
        &columns,
    )
}

fn read_rows<S: MetaSnapshot>(
    snapshot: &mut S,
    table: &TableInfo,
) -> Result<Vec<StoredRow>, AccountWriteError> {
    let view = full_view(table);
    let mut rows = Vec::new();
    for (key, value) in scan_system_table_keyed(snapshot, &view)? {
        let values = tidb_tablecodec::decode_table_row_to_map(&value, &column_types(table), None)
            .map_err(|error| {
            AccountWriteError::Read(SystemTableError::Decode {
                name: format!("{SYSTEM_DB}.{}", table.name.original()),
                detail: error.to_string(),
            })
        })?;
        rows.push(StoredRow { key, values });
    }
    Ok(rows)
}

fn column_types(table: &TableInfo) -> BTreeMap<i64, tidb_datatype::FieldType> {
    table
        .cols()
        .iter()
        .map(|column| (column.id, column.field_type.clone()))
        .collect()
}

/// The text one stored column holds, in the spelling the loader reads it as.
fn stored_text(values: &RowValues, column_id: i64) -> String {
    match values.get(&column_id) {
        Some(Datum::Bytes(bytes)) => String::from_utf8_lossy(bytes).into_owned(),
        Some(Datum::String(string)) => String::from_utf8_lossy(string.bytes()).into_owned(),
        Some(Datum::Enum(member, _)) => member.name().to_owned(),
        _ => String::new(),
    }
}

fn column_id(table: &TableInfo, column: &str) -> Result<i64, AccountWriteError> {
    table
        .cols()
        .iter()
        .find(|stored| stored.name.lowercase() == column)
        .map(|stored| stored.id)
        .ok_or_else(|| {
            AccountWriteError::MissingTable(format!(
                "{SYSTEM_DB}.{}.{column}",
                table.name.original()
            ))
        })
}

/// Makes one table's rows match `desired`.
#[allow(clippy::too_many_arguments)]
fn reconcile<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    account_table: &AccountTable,
    desired: &LogicalRows,
    now: Time,
    plan: &mut AccountWritePlan,
    changed: &mut std::collections::BTreeSet<String>,
) -> Result<(), AccountWriteError> {
    let table = match locate(catalog, account_table.name) {
        Ok(table) => table,
        // A cluster old enough to lack `mysql.global_grants` cannot hold the
        // rows it would store either, so an empty desired set is satisfied and
        // a non-empty one is refused by name rather than silently dropped.
        Err(missing) if desired.is_empty() => {
            if account_table.name == USER_TABLE {
                return Err(missing);
            }
            return Ok(());
        }
        Err(missing) => return Err(missing),
    };
    // An update rewrites the record alone, so a value column an index covers
    // would leave a stale entry behind. None of these tables declares one --
    // their indexes cover only identity columns -- and checking says so out
    // loud instead of relying on it.
    let indexed = indexed_columns(table);
    if let Some(column) = account_table
        .value_columns
        .iter()
        .find(|column| indexed.iter().any(|name| name == *column))
    {
        return Err(AccountWriteError::Unsupported(format!(
            "{SYSTEM_DB}.{} indexes `{column}`, which this writer updates in place",
            account_table.name
        )));
    }

    let key_ids: Vec<i64> = account_table
        .key_columns
        .iter()
        .map(|column| column_id(table, column))
        .collect::<Result<_, _>>()?;
    let value_ids: Vec<(&'static str, i64)> = account_table
        .value_columns
        .iter()
        .map(|column| column_id(table, column).map(|id| (*column, id)))
        .collect::<Result<_, _>>()?;

    let stored = read_rows(snapshot, table)?;
    let mut by_key: BTreeMap<Vec<String>, StoredRow> = BTreeMap::new();
    for row in stored {
        let identity = key_ids
            .iter()
            .map(|id| stored_text(&row.values, *id))
            .collect();
        by_key.insert(identity, row);
    }

    let mut next_row_id: Option<i64> = None;
    for (identity, values) in desired {
        match by_key.remove(identity) {
            Some(mut row) => {
                let mut moved = false;
                for (column, id) in &value_ids {
                    let wanted = values.get(column).cloned().unwrap_or_default();
                    if stored_text(&row.values, *id) != wanted {
                        row.values.insert(*id, Datum::Bytes(wanted.into_bytes()));
                        moved = true;
                    }
                }
                if moved {
                    plan.mutations
                        .push(update_row(table, &row.key, &row.values)?);
                    note_changed(changed, account_table, identity);
                }
            }
            None => {
                let row_id = match next_row_id {
                    Some(next) => next,
                    None => first_free_row_id(snapshot, catalog, table)?,
                };
                next_row_id = Some(row_id + 1);
                let mut fresh = defaults_row(table, now)?;
                for (position, id) in key_ids.iter().enumerate() {
                    fresh.insert(*id, Datum::Bytes(identity[position].clone().into_bytes()));
                }
                for (column, id) in &value_ids {
                    let wanted = values.get(column).cloned().unwrap_or_default();
                    fresh.insert(*id, Datum::Bytes(wanted.into_bytes()));
                }
                plan.mutations.extend(insert_row(table, row_id, &fresh)?);
                note_changed(changed, account_table, identity);
            }
        }
    }
    // Whatever the desired image did not claim is a row the change removed.
    for (identity, row) in by_key {
        plan.mutations
            .extend(delete_row(table, &row.key, &row.values)?);
        note_changed(changed, account_table, &identity);
    }
    if let Some(next) = next_row_id {
        publish_row_id_watermark(catalog, table, next - 1, &mut plan.mutations)?;
    }
    Ok(())
}

/// Records the `'user'@'host'` a changed row is about.
///
/// `mysql.role_edges` keys the ROLE first and the grantee second, so its
/// account is the second pair; every other table names its own account first.
fn note_changed(
    changed: &mut std::collections::BTreeSet<String>,
    table: &AccountTable,
    identity: &[String],
) {
    let (host, user) = if table.name == ROLE_EDGES_TABLE {
        (&identity[2], &identity[3])
    } else {
        (&identity[0], &identity[1])
    };
    changed.insert(format!("'{user}'@'{host}'"));
}

/// Reserves the next `_tidb_rowid` for one table by advancing its allocator
/// key, and answers the first ID reserved.
///
/// The key holds the max USED id, so the increment IS the allocation, exactly
/// as [`crate::cluster_ddl`]'s global-ID allocator works. Reserving from the
/// value this snapshot read is what makes a competing allocation a write
/// conflict rather than a duplicate handle.
///
/// One row per statement is the realistic case, but a statement that inserts
/// several (a `CREATE USER` naming three accounts) counts on from this one
/// call and publishes the final watermark once.
fn first_free_row_id<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table: &TableInfo,
) -> Result<i64, AccountWriteError> {
    let stored = snapshot.get(&key::auto_table_id_kv_key(system_db_id(catalog)?, table.id))?;
    let current = match stored {
        Some(bytes) => value::parse_int_value(&bytes).map_err(|error| {
            AccountWriteError::Encode(RowEncodeError(format!(
                "{}'s row-ID allocator: {error}",
                table.name.original()
            )))
        })?,
        // Go's `Inc` treats a missing key as zero, and a table whose rows were
        // all seeded by a bootstrap that wrote no watermark starts here.
        None => 0,
    };
    // A cluster whose rows outran its allocator key (a bootstrap that seeded
    // rows without one) would otherwise hand out a handle that already exists,
    // which TiKV rejects as an `Insert` assertion failure -- a confusing
    // report of a real problem. Starting past the highest stored handle is the
    // honest repair.
    let highest = read_rows(snapshot, table)?
        .iter()
        .map(|row| row_id_of(&row.key))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .max()
        .unwrap_or(0);
    Ok(current.max(highest) + 1)
}

fn publish_row_id_watermark(
    catalog: &ClusterCatalog,
    table: &TableInfo,
    last_used: i64,
    mutations: &mut Vec<OptimisticMutation>,
) -> Result<(), AccountWriteError> {
    mutations.push(
        OptimisticMutation::meta_put(
            key::auto_table_id_kv_key(system_db_id(catalog)?, table.id),
            value::encode_int_value(last_used),
        )
        .map_err(|error| AccountWriteError::Encode(RowEncodeError(error.to_string())))?,
    );
    Ok(())
}

fn system_db_id(catalog: &ClusterCatalog) -> Result<i64, AccountWriteError> {
    catalog
        .databases
        .iter()
        .find(|database| database.info.name.lowercase() == SYSTEM_DB)
        .map(|database| database.info.id)
        .ok_or_else(|| AccountWriteError::MissingTable(SYSTEM_DB.to_owned()))
}

/// Removes every `mysql.tables_priv`/`mysql.columns_priv` row whose account no
/// longer exists -- the one change to those tables this writer makes -- and
/// refuses any other difference by name.
///
/// The comparison is exact: what the cluster stores for accounts that survive
/// must already equal what the desired image holds. That is true for every
/// statement whose effect is global- or database-scoped, and false for exactly
/// the table- and column-scoped grants this writer cannot encode.
#[allow(clippy::too_many_arguments)]
fn reconcile_scoped_grants<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    name: &str,
    desired: &ClusterPrivileges,
    live: &std::collections::BTreeSet<(String, String)>,
    plan: &mut AccountWritePlan,
    changed: &mut std::collections::BTreeSet<String>,
) -> Result<(), AccountWriteError> {
    let Ok(table) = locate(catalog, name) else {
        return Ok(());
    };
    let host_id = column_id(table, "host")?;
    let user_id = column_id(table, "user")?;
    let mut surviving = std::collections::BTreeSet::new();
    for row in read_rows(snapshot, table)? {
        let identity = (
            stored_text(&row.values, host_id),
            stored_text(&row.values, user_id),
        );
        if !live.contains(&identity) {
            plan.mutations
                .extend(delete_row(table, &row.key, &row.values)?);
            changed.insert(format!("'{}'@'{}'", identity.1, identity.0));
            continue;
        }
        surviving.insert(scoped_identity(table, &row.values, name)?);
    }
    let wanted: std::collections::BTreeSet<Vec<String>> = if name == "tables_priv" {
        desired
            .table_grants
            .iter()
            .map(|grant| {
                vec![
                    grant.host.clone(),
                    grant.user.clone(),
                    grant.database.clone(),
                    grant.table.clone(),
                ]
            })
            .collect()
    } else {
        desired
            .column_grants
            .iter()
            .map(|grant| {
                vec![
                    grant.host.clone(),
                    grant.user.clone(),
                    grant.database.clone(),
                    grant.table.clone(),
                    grant.column.clone(),
                ]
            })
            .collect()
    };
    if surviving != wanted {
        return Err(AccountWriteError::Unsupported(format!(
            "this node cannot change {SYSTEM_DB}.{name}: it stores privileges in a SET column \
             this node does not encode; run this table- or column-scoped grant on a TiDB server"
        )));
    }
    Ok(())
}

/// One scoped-grant row's identity, in the column order the desired image
/// spells it.
fn scoped_identity(
    table: &TableInfo,
    values: &RowValues,
    name: &str,
) -> Result<Vec<String>, AccountWriteError> {
    let mut columns = vec!["host", "user", "db", "table_name"];
    if name == "columns_priv" {
        columns.push("column_name");
    }
    columns
        .into_iter()
        .map(|column| column_id(table, column).map(|id| stored_text(values, id)))
        .collect()
}
