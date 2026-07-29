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

//! Writing a cluster's `SET GLOBAL` overrides back into
//! `mysql.global_variables`.
//!
//! This is the write half of the bridge [`crate::cluster_sysvar_load`] reads,
//! and it follows [`crate::cluster_account_write`]'s whole-image pattern
//! exactly, for the same reason: the caller hands over the overrides table as
//! it should now be (every variable a `SET GLOBAL` has ever set on this
//! cluster, at its current value), built from a
//! [`tidb_session::vars::GlobalSysvars`] scratch copy seeded from *this same
//! snapshot* -- so the diff against the stored rows is precisely one
//! statement's own effect and can never revert a change another node made
//! concurrently, and a `SET GLOBAL x = DEFAULT` (which removes `x` from the
//! scratch table's overrides) is naturally a delete of its stored row rather
//! than a case this module has to special-case.

use std::collections::BTreeMap;

use tidb_datatype::{Datum, Time};
use tidb_meta::{key, value};
use tidb_model::table_info::TableInfo;
use tidb_txnkv::transaction::OptimisticMutation;

use crate::cluster_catalog::ClusterCatalog;
use crate::cluster_catalog::MetaSnapshot;
use crate::mysql_system_tables::{
    scan_system_table_keyed, SystemTableError, SystemTableView, SYSTEM_DB,
};
use crate::system_row_write::{
    defaults_row, delete_row, insert_row, row_id_of, update_row, RowEncodeError, RowValues,
};

const GLOBAL_VARIABLES_TABLE: &str = "global_variables";

/// Why a `SET GLOBAL` could not be persisted.
#[derive(Debug)]
pub enum SysvarWriteError {
    /// The cluster has no `mysql.global_variables` table at all -- a cluster
    /// older than the schema this node expects.
    MissingTable,
    /// A stored row could not be read.
    Read(SystemTableError),
    /// A row could not be encoded.
    Encode(RowEncodeError),
}

impl std::fmt::Display for SysvarWriteError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingTable => {
                formatter.write_str("this cluster has no mysql.global_variables table")
            }
            Self::Read(error) => write!(formatter, "reading the stored sysvars failed: {error}"),
            Self::Encode(error) => write!(formatter, "encoding a sysvar row failed: {error}"),
        }
    }
}

impl std::error::Error for SysvarWriteError {}

impl From<SystemTableError> for SysvarWriteError {
    fn from(error: SystemTableError) -> Self {
        Self::Read(error)
    }
}

impl From<RowEncodeError> for SysvarWriteError {
    fn from(error: RowEncodeError) -> Self {
        Self::Encode(error)
    }
}

/// The mutations that make `mysql.global_variables` equal `desired`.
#[derive(Debug, Default)]
pub struct SysvarWritePlan {
    /// The mutations, in no particular order (they touch distinct keys).
    pub mutations: Vec<OptimisticMutation>,
    /// The variable names this plan actually changed (a stored value that
    /// already equals what `desired` wants is not listed).
    pub changed: Vec<String>,
}

impl SysvarWritePlan {
    /// Whether the desired image already matches what the cluster stores.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.mutations.is_empty()
    }
}

/// Plans the mutations that make `mysql.global_variables` equal `desired`:
/// every named variable is upserted to its value, and every stored row
/// `desired` does not name is deleted (that is what a `SET GLOBAL x =
/// DEFAULT` means once it has removed `x` from the caller's overrides table).
///
/// `snapshot` must be the same snapshot `desired` was built from, and the
/// mutations must commit on that snapshot's transaction: a concurrent
/// `SET GLOBAL` of the same variable is then a write conflict at prewrite,
/// never a silent overwrite.
pub fn plan_sysvar_write<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    desired: &BTreeMap<String, String>,
    now: Time,
) -> Result<SysvarWritePlan, SysvarWriteError> {
    let mut plan = SysvarWritePlan::default();
    let table = locate(catalog)?;
    let name_id = column_id(table, "variable_name")?;
    let value_id = column_id(table, "variable_value")?;

    let view = full_view(table);
    let mut by_name: BTreeMap<String, StoredRow> = BTreeMap::new();
    for (key, value) in scan_system_table_keyed(snapshot, &view)? {
        let values = tidb_tablecodec::decode_table_row_to_map(&value, &column_types(table), None)
            .map_err(|error| {
            SysvarWriteError::Read(SystemTableError::Decode {
                name: format!("{SYSTEM_DB}.{GLOBAL_VARIABLES_TABLE}"),
                detail: error.to_string(),
            })
        })?;
        let name = stored_text(&values, name_id);
        by_name.insert(name, StoredRow { key, values });
    }

    let mut next_row_id: Option<i64> = None;
    for (name, value) in desired {
        let key = name.to_ascii_lowercase();
        match by_name.remove(&key) {
            Some(mut row) => {
                if stored_text(&row.values, value_id) != *value {
                    row.values
                        .insert(value_id, Datum::Bytes(value.clone().into_bytes()));
                    plan.mutations
                        .push(update_row(table, &row.key, &row.values)?);
                    plan.changed.push(key);
                }
            }
            None => {
                let row_id = match next_row_id {
                    Some(next) => next,
                    None => first_free_row_id(snapshot, catalog, table)?,
                };
                next_row_id = Some(row_id + 1);
                let mut fresh = defaults_row(table, now)?;
                fresh.insert(name_id, Datum::Bytes(key.clone().into_bytes()));
                fresh.insert(value_id, Datum::Bytes(value.clone().into_bytes()));
                plan.mutations.extend(insert_row(table, row_id, &fresh)?);
                plan.changed.push(key);
            }
        }
    }
    // Whatever `desired` did not claim is a row a `SET GLOBAL ... = DEFAULT`
    // removed.
    for (name, row) in by_name {
        plan.mutations
            .extend(delete_row(table, &row.key, &row.values)?);
        plan.changed.push(name);
    }
    if let Some(next) = next_row_id {
        publish_row_id_watermark(catalog, table, next - 1, &mut plan.mutations)?;
    }
    Ok(plan)
}

struct StoredRow {
    key: Vec<u8>,
    values: RowValues,
}

fn locate(catalog: &ClusterCatalog) -> Result<&TableInfo, SysvarWriteError> {
    catalog
        .databases
        .iter()
        .find(|database| database.info.name.lowercase() == SYSTEM_DB)
        .and_then(|database| {
            database
                .tables
                .iter()
                .find(|stored| stored.name.lowercase() == GLOBAL_VARIABLES_TABLE)
        })
        .ok_or(SysvarWriteError::MissingTable)
}

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

fn column_types(table: &TableInfo) -> BTreeMap<i64, tidb_datatype::FieldType> {
    table
        .cols()
        .iter()
        .map(|column| (column.id, column.field_type.clone()))
        .collect()
}

fn stored_text(values: &RowValues, column_id: i64) -> String {
    match values.get(&column_id) {
        Some(Datum::Bytes(bytes)) => String::from_utf8_lossy(bytes).into_owned(),
        Some(Datum::String(string)) => String::from_utf8_lossy(string.bytes()).into_owned(),
        _ => String::new(),
    }
}

fn column_id(table: &TableInfo, column: &str) -> Result<i64, SysvarWriteError> {
    table
        .cols()
        .iter()
        .find(|stored| stored.name.lowercase() == column)
        .map(|stored| stored.id)
        .ok_or(SysvarWriteError::MissingTable)
}

/// Reserves the next `_tidb_rowid` for `mysql.global_variables`, mirroring
/// [`crate::cluster_account_write`]'s allocator use exactly (same table
/// shape: `NONCLUSTERED` primary key, so a row needs an explicit handle).
fn first_free_row_id<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table: &TableInfo,
) -> Result<i64, SysvarWriteError> {
    let stored = snapshot
        .get(&key::auto_table_id_kv_key(system_db_id(catalog)?, table.id))
        .map_err(|error| SysvarWriteError::Read(SystemTableError::Snapshot(error.to_string())))?;
    let current = match stored {
        Some(bytes) => value::parse_int_value(&bytes).map_err(|error| {
            SysvarWriteError::Encode(RowEncodeError(format!(
                "mysql.global_variables's row-ID allocator: {error}"
            )))
        })?,
        None => 0,
    };
    let highest = {
        let view = full_view(table);
        scan_system_table_keyed(snapshot, &view)?
            .into_iter()
            .map(|(key, _)| row_id_of(&key))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .max()
            .unwrap_or(0)
    };
    Ok(current.max(highest) + 1)
}

fn publish_row_id_watermark(
    catalog: &ClusterCatalog,
    table: &TableInfo,
    last_used: i64,
    mutations: &mut Vec<OptimisticMutation>,
) -> Result<(), SysvarWriteError> {
    mutations.push(
        OptimisticMutation::meta_put(
            key::auto_table_id_kv_key(system_db_id(catalog)?, table.id),
            value::encode_int_value(last_used),
        )
        .map_err(|error| SysvarWriteError::Encode(RowEncodeError(error.to_string())))?,
    );
    Ok(())
}

fn system_db_id(catalog: &ClusterCatalog) -> Result<i64, SysvarWriteError> {
    catalog
        .databases
        .iter()
        .find(|database| database.info.name.lowercase() == SYSTEM_DB)
        .map(|database| database.info.id)
        .ok_or(SysvarWriteError::MissingTable)
}
