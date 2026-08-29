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

//! Go `pkg/util/domainutil` (`repair_vars.go`, the package's only file).
//!
//! `ADMIN REPAIR TABLE`'s shared state: which tables startup marked as
//! damaged (`repair-table-list` in the config), the databases holding their
//! quarantined metadata, and whether the node is in repair mode at all. The
//! infoschema loader calls [`RepairInfo::check_and_fetch_repaired_table`] to
//! DIVERT a damaged table out of the visible schema, and the DDL executor
//! reads/removes entries as `ADMIN REPAIR TABLE` statements complete.
//!
//! Go holds one package-level `RepairInfo` guarded by an embedded `RWMutex`;
//! the same shape here is the process-wide [`REPAIR_INFO`].

use std::collections::{HashMap, HashSet};
use std::sync::{LazyLock, RwLock};

use tidb_model::{DBInfo, GoShared, TableInfo};
use tidb_mysql::to_lowercase as go_simple_lowercase;

/// Go `repairInfo`: the fields the package-level `RepairInfo` guards.
#[derive(Debug, Default)]
struct RepairState {
    repair_db_info_map: HashMap<i64, GoShared<DBInfo>>,
    repair_table_list: Vec<String>,
    repair_mode: bool,
}

/// Go's `repairInfo` value with its embedded lock.
#[derive(Debug)]
pub struct RepairInfo {
    state: RwLock<RepairState>,
}

/// Go's package-level `RepairInfo` variable.
pub static REPAIR_INFO: LazyLock<RepairInfo> = LazyLock::new(RepairInfo::new);

impl RepairInfo {
    fn new() -> Self {
        Self {
            state: RwLock::new(RepairState::default()),
        }
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, RepairState> {
        self.state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, RepairState> {
        self.state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    /// Go `InRepairMode`.
    #[must_use]
    pub fn in_repair_mode(&self) -> bool {
        self.read().repair_mode
    }

    /// Go `SetRepairMode`.
    pub fn set_repair_mode(&self, mode: bool) {
        self.write().repair_mode = mode;
    }

    /// Go `GetRepairTableList`.
    #[must_use]
    pub fn get_repair_table_list(&self) -> Vec<String> {
        self.read().repair_table_list.clone()
    }

    /// Go `GetMustLoadRepairTableListByDB`: the ids of this database's
    /// repairing tables. The list is matched lowercased; `table_name2id` "is
    /// case sensitive and needs to be traversed to match the table id".
    #[must_use]
    pub fn get_must_load_repair_table_list_by_db(
        &self,
        db_name: &str,
        table_name2id: &HashMap<String, i64>,
    ) -> Vec<i64> {
        let db_name_prefix = format!("{db_name}.");
        let state = self.read();
        let repair_table_set: HashSet<String> = state
            .repair_table_list
            .iter()
            .map(|full_table_name| go_simple_lowercase(full_table_name))
            .filter(|lower| lower.starts_with(&db_name_prefix))
            .collect();
        let mut table_id_list = Vec::new();
        for (table_name, id) in table_name2id {
            let full_name = go_simple_lowercase(&format!("{db_name}.{table_name}"));
            if repair_table_set.contains(&full_name) {
                table_id_list.push(*id);
            }
        }
        table_id_list
    }

    /// Go `SetRepairTableList`: stored LOWERCASED, mutating the caller's
    /// list in place exactly as Go does before taking the lock.
    pub fn set_repair_table_list(&self, mut list: Vec<String>) {
        for entry in &mut list {
            *entry = go_simple_lowercase(entry);
        }
        self.write().repair_table_list = list;
    }

    /// Go `CheckAndFetchRepairedTable`: outside repair mode nothing is
    /// fetched; inside it, a `db.table` on the repairing list is RECORDED —
    /// its database shallow-copied on first sight (`di.Copy()`), the table
    /// appended to that copy's quarantined list — and `true` says the
    /// caller must divert the table out of the visible schema.
    pub fn check_and_fetch_repaired_table(
        &self,
        di: &GoShared<DBInfo>,
        tbl: &GoShared<TableInfo>,
    ) -> bool {
        let mut state = self.write();
        if !state.repair_mode {
            return false;
        }
        let (db_id, full_name) = {
            let db = di.read();
            let full = format!("{}.{}", db.name.lowercase(), tbl.read().name.lowercase());
            (db.id, full)
        };
        let is_repair = state
            .repair_table_list
            .iter()
            .any(|tn| go_simple_lowercase(tn) == full_name);
        if !is_repair {
            return false;
        }
        if let Some(repaired_db) = state.repair_db_info_map.get(&db_id) {
            repaired_db
                .write()
                .deprecated_tables
                .push_handle_go(Some(tbl.clone()));
        } else {
            // Shallow copy the DBInfo, clean the tables, and set the
            // repaired table.
            let mut repaired_db = di.read().copy_like_go();
            repaired_db.deprecated_tables =
                tidb_model::GoSharedPointerSlice::from_handles(vec![Some(tbl.clone())]);
            state
                .repair_db_info_map
                .insert(db_id, GoShared::new(repaired_db));
        }
        true
    }

    /// Go `GetRepairedTableInfoByTableName` ("exported for test"): the
    /// quarantined table and its database, by lowercased names. Go's loop
    /// RETURNS `(nil, db)` after inspecting the FIRST database whose name
    /// matches — reproduced, quirk included.
    #[must_use]
    pub fn get_repaired_table_info_by_table_name(
        &self,
        schema_lower_name: &str,
        table_lower_name: &str,
    ) -> (Option<GoShared<TableInfo>>, Option<GoShared<DBInfo>>) {
        let state = self.read();
        for db in state.repair_db_info_map.values() {
            if db.read().name.lowercase() != schema_lower_name {
                continue;
            }
            for table in db.read().deprecated_tables.iter_deref() {
                if table.read().name.lowercase() == table_lower_name {
                    return (Some(table), Some(db.clone()));
                }
            }
            return (None, Some(db.clone()));
        }
        (None, None)
    }

    /// Go `RemoveFromRepairInfo`: drop the repaired table from the list and
    /// the map; an emptied database leaves the map, and an emptied map ends
    /// repair mode.
    pub fn remove_from_repair_info(&self, schema_lower_name: &str, table_lower_name: &str) {
        let repaired_lower_name = format!("{schema_lower_name}.{table_lower_name}");
        let mut state = self.write();
        if let Some(index) = state
            .repair_table_list
            .iter()
            .position(|rt| go_simple_lowercase(rt) == repaired_lower_name)
        {
            state.repair_table_list.remove(index);
        }
        let mut empty_db_id = None;
        for (db_id, db) in &state.repair_db_info_map {
            if db.read().name.lowercase() != schema_lower_name {
                continue;
            }
            let mut db = db.write();
            let position = db
                .deprecated_tables
                .iter_deref()
                .position(|table| table.read().name.lowercase() == table_lower_name);
            if let Some(position) = position {
                db.deprecated_tables.delete_go(position, position + 1);
            }
            if db.deprecated_tables.iter_handles().count() == 0 {
                empty_db_id = Some(*db_id);
            }
            break;
        }
        if let Some(db_id) = empty_db_id {
            state.repair_db_info_map.remove(&db_id);
        }
        if state.repair_db_info_map.is_empty() {
            state.repair_mode = false;
        }
    }
}

/// Go `repairKeyType`: the session-context keys `ADMIN REPAIR TABLE` caches
/// its target under.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RepairKeyType {
    /// Go `RepairedTable`.
    RepairedTable,
    /// Go `RepairedDatabase`.
    RepairedDatabase,
}

impl std::fmt::Display for RepairKeyType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::RepairedTable => "RepairedTable",
            Self::RepairedDatabase => "RepairedDatabase",
        })
    }
}
