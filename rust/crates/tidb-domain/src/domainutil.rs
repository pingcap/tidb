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

//! Go `pkg/util/domainutil` (`repair_vars.go`, the package's only file),
//! COMPLETE.
//!
//! `ADMIN REPAIR TABLE`'s shared state: which tables startup marked as
//! damaged (`repair-table-list` in the config), the databases holding their
//! quarantined metadata, and whether the node is in repair mode at all. The
//! infoschema loader calls [`RepairInfo::check_and_fetch_repaired_table`] to
//! DIVERT a damaged table out of the visible schema, and the DDL executor
//! reads/removes entries as `ADMIN REPAIR TABLE` statements complete.
//!
//! Go holds one package-level `RepairInfo` guarded by an embedded `RWMutex`;
//! the same shape here is a process-wide [`RwLock`] behind [`repair_info`].
//! Upstream coverage lives in `pkg/ddl/repair_table_test.go`, which is
//! testkit-bound (a live DDL pipeline); the focused tests below pin each
//! method against the Go bodies instead.

use std::collections::BTreeMap;
use std::sync::{OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard};

use tidb_model::{DBInfo, GoShared, TableInfo};

/// Go `repairInfo`: the fields the package-level `RepairInfo` guards.
#[derive(Debug, Default)]
pub struct RepairInfo {
    repair_db_info_map: BTreeMap<i64, GoShared<DBInfo>>,
    repair_table_list: Vec<String>,
    repair_mode: bool,
}

/// Go's package-level `RepairInfo` variable.
pub fn repair_info() -> &'static RwLock<RepairInfo> {
    static REPAIR_INFO: OnceLock<RwLock<RepairInfo>> = OnceLock::new();
    REPAIR_INFO.get_or_init(|| RwLock::new(RepairInfo::default()))
}

/// A read lock on the shared instance (poison-tolerant, as Go's mutex is).
pub fn repair_info_read() -> RwLockReadGuard<'static, RepairInfo> {
    repair_info()
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// A write lock on the shared instance.
pub fn repair_info_write() -> RwLockWriteGuard<'static, RepairInfo> {
    repair_info()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl RepairInfo {
    /// Go `InRepairMode`.
    #[must_use]
    pub fn in_repair_mode(&self) -> bool {
        self.repair_mode
    }

    /// Go `SetRepairMode`.
    pub fn set_repair_mode(&mut self, mode: bool) {
        self.repair_mode = mode;
    }

    /// Go `GetRepairTableList`.
    #[must_use]
    pub fn get_repair_table_list(&self) -> Vec<String> {
        self.repair_table_list.clone()
    }

    /// Go `GetMustLoadRepairTableListByDB`: the ids of this database's
    /// repairing tables. The list is matched lowercased; `table_name2id` "is
    /// case sensitive and needs to be traversed to match the table id".
    #[must_use]
    pub fn get_must_load_repair_table_list_by_db(
        &self,
        db_name: &str,
        table_name2id: &BTreeMap<String, i64>,
    ) -> Vec<i64> {
        let db_name_prefix = format!("{db_name}.");
        let repair_table_set: std::collections::BTreeSet<String> = self
            .repair_table_list
            .iter()
            .map(|full_table_name| full_table_name.to_lowercase())
            .filter(|lower| lower.starts_with(&db_name_prefix))
            .collect();
        let mut table_id_list = Vec::new();
        for (table_name, id) in table_name2id {
            let full_name = format!("{db_name}.{table_name}").to_lowercase();
            if repair_table_set.contains(&full_name) {
                table_id_list.push(*id);
            }
        }
        table_id_list
    }

    /// Go `SetRepairTableList`: stored LOWERCASED, mutating the caller's
    /// list in place exactly as Go does before taking the lock.
    pub fn set_repair_table_list(&mut self, mut list: Vec<String>) {
        for entry in &mut list {
            *entry = entry.to_lowercase();
        }
        self.repair_table_list = list;
    }

    /// Go `CheckAndFetchRepairedTable`: outside repair mode nothing is
    /// fetched; inside it, a `db.table` on the repairing list is RECORDED —
    /// its database shallow-copied on first sight (`di.Copy()`), the table
    /// appended to that copy's quarantined list — and `true` says the
    /// caller must divert the table out of the visible schema.
    pub fn check_and_fetch_repaired_table(
        &mut self,
        di: &GoShared<DBInfo>,
        tbl: &GoShared<TableInfo>,
    ) -> bool {
        if !self.repair_mode {
            return false;
        }
        let (db_id, full_name) = {
            let db = di.read();
            let full = format!("{}.{}", db.name.lowercase(), tbl.read().name.lowercase());
            (db.id, full)
        };
        let is_repair = self
            .repair_table_list
            .iter()
            .any(|tn| tn.to_lowercase() == full_name);
        if !is_repair {
            return false;
        }
        if let Some(repaired_db) = self.repair_db_info_map.get(&db_id) {
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
            self.repair_db_info_map
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
        for db in self.repair_db_info_map.values() {
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
    pub fn remove_from_repair_info(&mut self, schema_lower_name: &str, table_lower_name: &str) {
        let repaired_lower_name = format!("{schema_lower_name}.{table_lower_name}");
        if let Some(index) = self
            .repair_table_list
            .iter()
            .position(|rt| rt.to_lowercase() == repaired_lower_name)
        {
            self.repair_table_list.remove(index);
        }
        let mut empty_db_id = None;
        for (db_id, db) in &self.repair_db_info_map {
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
            self.repair_db_info_map.remove(&db_id);
        }
        if self.repair_db_info_map.is_empty() {
            self.repair_mode = false;
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

impl RepairKeyType {
    /// Go `repairKeyType.String`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RepairedTable => "RepairedTable",
            Self::RepairedDatabase => "RepairedDatabase",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_ast::CiString;

    fn db(id: i64, name: &str) -> GoShared<DBInfo> {
        GoShared::new(DBInfo {
            id,
            name: CiString::new(name),
            ..DBInfo::default()
        })
    }

    fn table(name: &str) -> GoShared<TableInfo> {
        GoShared::new(TableInfo {
            name: CiString::new(name),
            ..TableInfo::default()
        })
    }

    /// The fetch/divert/repair round trip over the Go bodies: outside
    /// repair mode nothing diverts; inside it a listed table is quarantined
    /// under a SHALLOW database copy, found by name, and its removal ends
    /// repair mode when nothing else is quarantined.
    #[test]
    fn a_listed_table_diverts_and_its_repair_ends_the_mode() {
        let mut info = RepairInfo::default();
        let source_db = db(7, "RepDB");
        let damaged = table("Broken");

        // `SetRepairTableList` lowercases what the config wrote.
        info.set_repair_table_list(vec!["RepDB.Broken".to_owned()]);
        assert_eq!(info.get_repair_table_list(), ["repdb.broken"]);

        // Outside repair mode, nothing is fetched.
        assert!(!info.check_and_fetch_repaired_table(&source_db, &damaged));

        info.set_repair_mode(true);
        assert!(info.in_repair_mode());
        assert!(info.check_and_fetch_repaired_table(&source_db, &damaged));
        // An unlisted table is untouched.
        assert!(!info.check_and_fetch_repaired_table(&source_db, &table("fine")));

        // The quarantined copy answers by LOWERCASED names, and the source
        // database's own table list was never touched (shallow copy).
        let (found, found_db) = info.get_repaired_table_info_by_table_name("repdb", "broken");
        assert!(found.is_some());
        assert_eq!(found_db.expect("the copy").read().id, 7);
        assert_eq!(source_db.read().deprecated_tables.iter_handles().count(), 0);
        // Go's first-match-database quirk: a missing table in a MATCHING
        // database answers `(None, Some(db))`.
        let (missing, still_db) = info.get_repaired_table_info_by_table_name("repdb", "other");
        assert!(missing.is_none() && still_db.is_some());
        let (none_table, none_db) = info.get_repaired_table_info_by_table_name("nodb", "broken");
        assert!(none_table.is_none() && none_db.is_none());

        // `GetMustLoadRepairTableListByDB` matches case-insensitively on the
        // list but traverses the case-sensitive id map.
        let ids: BTreeMap<String, i64> = [("Broken".to_owned(), 31)].into_iter().collect();
        assert_eq!(
            info.get_must_load_repair_table_list_by_db("repdb", &ids),
            [31]
        );
        assert!(info
            .get_must_load_repair_table_list_by_db("otherdb", &ids)
            .is_empty());

        // Repairing the table empties the map and ENDS repair mode.
        info.remove_from_repair_info("repdb", "broken");
        assert!(info.get_repair_table_list().is_empty());
        assert!(!info.in_repair_mode());
        let (gone, gone_db) = info.get_repaired_table_info_by_table_name("repdb", "broken");
        assert!(gone.is_none() && gone_db.is_none());
    }

    /// Go's key-type strings.
    #[test]
    fn repair_key_types_spell_like_go() {
        assert_eq!(RepairKeyType::RepairedTable.as_str(), "RepairedTable");
        assert_eq!(RepairKeyType::RepairedDatabase.as_str(), "RepairedDatabase");
    }
}
