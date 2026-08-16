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

//! Complete transcreation of Go `pkg/util/domainutil` (`repair_vars.go`):
//! the process-global `ADMIN REPAIR TABLE` state.
//!
//! While repair mode is on, schema loading routes tables named in the repair
//! list into a side map of shallow-copied `DBInfo`s instead of the live
//! catalog; repairing the last table turns the mode off again. Go keeps this
//! in one package-global (`RepairInfo`) behind an `RWMutex`, mirrored here by
//! [`REPAIR_INFO`].
//!
//! The package ships no Go tests; the tests below pin each observable rule,
//! including the case rules — the repair list is lowercased when stored, the
//! must-load lookup matches its `tableName2ID` keys case-insensitively, and
//! removal clears repair mode only when the map empties.

use std::collections::BTreeMap;
use std::sync::{LazyLock, RwLock};

use tidb_model::db::DBInfo;
use tidb_model::table_info::TableInfo;
use tidb_model::GoShared;

#[derive(Debug, Default)]
struct RepairState {
    repair_db_info_map: BTreeMap<i64, GoShared<DBInfo>>,
    repair_table_list: Vec<String>,
    repair_mode: bool,
}

/// Go's `repairInfo` struct behind its `RWMutex`.
#[derive(Debug, Default)]
pub struct RepairInfo {
    state: RwLock<RepairState>,
}

/// Go `RepairInfo`, the package-global instance.
pub static REPAIR_INFO: LazyLock<RepairInfo> = LazyLock::new(RepairInfo::default);

impl RepairInfo {
    fn read(&self) -> std::sync::RwLockReadGuard<'_, RepairState> {
        self.state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, RepairState> {
        self.state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
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
    pub fn repair_table_list(&self) -> Vec<String> {
        self.read().repair_table_list.clone()
    }

    /// Go `SetRepairTableList`: entries are stored lowercased.
    pub fn set_repair_table_list(&self, list: Vec<String>) {
        let list = list.into_iter().map(|entry| entry.to_lowercase()).collect();
        self.write().repair_table_list = list;
    }

    /// Go `GetMustLoadRepairTableListByDB`: the IDs of `db_name`'s tables
    /// that are on the repair list.
    ///
    /// `db_name` is expected lowercased (Go builds the prefix from it as
    /// given), while `table_name_to_id`'s keys are case-sensitive catalog
    /// names matched case-insensitively against the list.
    #[must_use]
    pub fn must_load_repair_table_list_by_db(
        &self,
        db_name: &str,
        table_name_to_id: &BTreeMap<String, i64>,
    ) -> Vec<i64> {
        let state = self.read();
        let db_name_prefix = format!("{db_name}.");
        let repair_table_set: std::collections::HashSet<String> = state
            .repair_table_list
            .iter()
            .map(|full_name| full_name.to_lowercase())
            .filter(|lower| lower.starts_with(&db_name_prefix))
            .collect();

        let mut table_ids = Vec::new();
        for (table_name, id) in table_name_to_id {
            let full_name = format!("{db_name}.{table_name}");
            if repair_table_set.contains(&full_name.to_lowercase()) {
                table_ids.push(*id);
            }
        }
        table_ids
    }

    /// Go `CheckAndFetchRepairedTable`: in repair mode, a table on the list
    /// is captured into the side map — under a shallow copy of its schema
    /// holding only the repaired tables — and `true` says the caller should
    /// keep it out of the live catalog.
    pub fn check_and_fetch_repaired_table(&self, db: &DBInfo, table: &TableInfo) -> bool {
        let mut state = self.write();
        if !state.repair_mode {
            return false;
        }
        let full_name = format!("{}.{}", db.name.lowercase(), table.name.lowercase());
        let is_repair = state
            .repair_table_list
            .iter()
            .any(|entry| entry.to_lowercase() == full_name);
        if !is_repair {
            return false;
        }

        if let Some(repaired_db) = state.repair_db_info_map.get(&db.id) {
            repaired_db.write().deprecated_tables.push_go(table.clone());
        } else {
            // Go shallow-copies the DBInfo, then replaces its table list with
            // just the repaired table.
            let mut repaired_db = db.copy_like_go();
            repaired_db.deprecated_tables = vec![table.clone()].into();
            state
                .repair_db_info_map
                .insert(db.id, GoShared::new(repaired_db));
        }
        true
    }

    /// Go `GetRepairedTableInfoByTableName`.
    ///
    /// Mirrors the source's shape: the first schema whose name matches
    /// answers, with `(None, Some(db))` when it holds no such table.
    #[must_use]
    pub fn repaired_table_info_by_table_name(
        &self,
        schema_lower_name: &str,
        table_lower_name: &str,
    ) -> (Option<TableInfo>, Option<GoShared<DBInfo>>) {
        let state = self.read();
        for db in state.repair_db_info_map.values() {
            let guard = db.read();
            if guard.name.lowercase() != schema_lower_name {
                continue;
            }
            for table in guard.deprecated_tables.iter_deref() {
                let table = table.read();
                if table.name.lowercase() == table_lower_name {
                    return (Some(table.clone()), Some(db.clone()));
                }
            }
            drop(guard);
            return (None, Some(db.clone()));
        }
        (None, None)
    }

    /// Go `RemoveFromRepairInfo`: drops the repaired table from the list and
    /// the side map, dropping the schema entry when its last table goes and
    /// leaving repair mode when the map empties.
    pub fn remove_from_repair_info(&self, schema_lower_name: &str, table_lower_name: &str) {
        let repaired_lower_name = format!("{schema_lower_name}.{table_lower_name}");
        let mut state = self.write();

        if let Some(position) = state
            .repair_table_list
            .iter()
            .position(|entry| entry.to_lowercase() == repaired_lower_name)
        {
            state.repair_table_list.remove(position);
        }

        let mut emptied_db_id = None;
        for (db_id, db) in &state.repair_db_info_map {
            let guard = db.read();
            if guard.name.lowercase() != schema_lower_name {
                continue;
            }
            let position = guard
                .deprecated_tables
                .iter_deref()
                .position(|table| table.read().name.lowercase() == table_lower_name);
            drop(guard);
            if let Some(position) = position {
                let mut guard = db.write();
                guard.deprecated_tables.delete_go(position, position + 1);
            }
            if db.read().deprecated_tables.len() == 0 {
                emptied_db_id = Some(*db_id);
            }
            break;
        }
        if let Some(db_id) = emptied_db_id {
            state.repair_db_info_map.remove(&db_id);
        }

        if state.repair_db_info_map.is_empty() {
            state.repair_mode = false;
        }
    }

    /// Resets the global for tests, mirroring Go's `init`.
    pub fn reset_for_test(&self) {
        let mut state = self.write();
        *state = RepairState::default();
    }
}

/// Go `repairKeyType`: the session-context keys for a repair in progress.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tidb_ast::CiString;

    // The state is process-global; serialize the tests that touch it.
    static GLOBAL: Mutex<()> = Mutex::new(());

    fn db(id: i64, name: &str) -> DBInfo {
        DBInfo {
            id,
            name: CiString::new(name),
            ..Default::default()
        }
    }

    fn table(id: i64, name: &str) -> TableInfo {
        TableInfo {
            id,
            name: CiString::new(name),
            ..Default::default()
        }
    }

    // The repair list is lowercased when stored.
    #[test]
    fn the_repair_list_is_stored_lowercased() {
        let _guard = GLOBAL.lock().unwrap_or_else(|e| e.into_inner());
        REPAIR_INFO.reset_for_test();

        REPAIR_INFO.set_repair_table_list(vec!["Test.T1".to_owned(), "OTHER.T2".to_owned()]);
        assert_eq!(
            REPAIR_INFO.repair_table_list(),
            vec!["test.t1".to_owned(), "other.t2".to_owned()]
        );
    }

    // Must-load matches catalog names case-insensitively and only within the
    // named schema.
    #[test]
    fn must_load_matches_listed_tables_case_insensitively() {
        let _guard = GLOBAL.lock().unwrap_or_else(|e| e.into_inner());
        REPAIR_INFO.reset_for_test();
        REPAIR_INFO.set_repair_table_list(vec!["test.t1".to_owned(), "other.t9".to_owned()]);

        let mut names = BTreeMap::new();
        names.insert("T1".to_owned(), 11);
        names.insert("t2".to_owned(), 12);
        let ids = REPAIR_INFO.must_load_repair_table_list_by_db("test", &names);
        assert_eq!(ids, vec![11]);

        // A schema with no listed table yields nothing.
        assert!(REPAIR_INFO
            .must_load_repair_table_list_by_db("absent", &names)
            .is_empty());
    }

    // Outside repair mode nothing is captured, whatever the list says.
    #[test]
    fn nothing_is_captured_outside_repair_mode() {
        let _guard = GLOBAL.lock().unwrap_or_else(|e| e.into_inner());
        REPAIR_INFO.reset_for_test();
        REPAIR_INFO.set_repair_table_list(vec!["test.t1".to_owned()]);

        assert!(!REPAIR_INFO.check_and_fetch_repaired_table(&db(1, "test"), &table(10, "t1")));
    }

    // In repair mode a listed table is captured under a shallow-copied schema
    // holding only the repaired tables, and later captures append to it.
    #[test]
    fn listed_tables_are_captured_into_the_side_map() {
        let _guard = GLOBAL.lock().unwrap_or_else(|e| e.into_inner());
        REPAIR_INFO.reset_for_test();
        REPAIR_INFO.set_repair_mode(true);
        REPAIR_INFO.set_repair_table_list(vec!["test.t1".to_owned(), "test.t2".to_owned()]);

        let schema = db(1, "Test");
        assert!(REPAIR_INFO.check_and_fetch_repaired_table(&schema, &table(10, "T1")));
        // An unlisted table passes through.
        assert!(!REPAIR_INFO.check_and_fetch_repaired_table(&schema, &table(11, "t3")));
        assert!(REPAIR_INFO.check_and_fetch_repaired_table(&schema, &table(12, "t2")));

        let (found, found_db) = REPAIR_INFO.repaired_table_info_by_table_name("test", "t1");
        assert_eq!(found.expect("captured").id, 10);
        let found_db = found_db.expect("schema captured");
        assert_eq!(found_db.read().id, 1);
        // The side copy holds exactly the repaired tables.
        assert_eq!(found_db.read().deprecated_tables.len(), 2);

        // A matching schema without the table answers (None, Some(db)).
        let (missing, still_db) = REPAIR_INFO.repaired_table_info_by_table_name("test", "t9");
        assert!(missing.is_none());
        assert!(still_db.is_some());

        // An unknown schema answers (None, None).
        let (none, no_db) = REPAIR_INFO.repaired_table_info_by_table_name("nope", "t1");
        assert!(none.is_none());
        assert!(no_db.is_none());
    }

    // Removing the last repaired table drops the schema entry and leaves
    // repair mode.
    #[test]
    fn removing_the_last_table_leaves_repair_mode() {
        let _guard = GLOBAL.lock().unwrap_or_else(|e| e.into_inner());
        REPAIR_INFO.reset_for_test();
        REPAIR_INFO.set_repair_mode(true);
        REPAIR_INFO.set_repair_table_list(vec!["test.t1".to_owned(), "test.t2".to_owned()]);

        let schema = db(1, "test");
        assert!(REPAIR_INFO.check_and_fetch_repaired_table(&schema, &table(10, "t1")));
        assert!(REPAIR_INFO.check_and_fetch_repaired_table(&schema, &table(12, "t2")));

        REPAIR_INFO.remove_from_repair_info("test", "t1");
        assert!(REPAIR_INFO.in_repair_mode());
        assert_eq!(REPAIR_INFO.repair_table_list(), vec!["test.t2".to_owned()]);
        let (still, _) = REPAIR_INFO.repaired_table_info_by_table_name("test", "t2");
        assert!(still.is_some());

        REPAIR_INFO.remove_from_repair_info("test", "t2");
        assert!(!REPAIR_INFO.in_repair_mode());
        assert!(REPAIR_INFO.repair_table_list().is_empty());
        let (gone, no_db) = REPAIR_INFO.repaired_table_info_by_table_name("test", "t2");
        assert!(gone.is_none());
        assert!(no_db.is_none());
    }

    // Go `repairKeyType.String`.
    #[test]
    fn repair_key_types_render_their_go_names() {
        assert_eq!(RepairKeyType::RepairedTable.to_string(), "RepairedTable");
        assert_eq!(
            RepairKeyType::RepairedDatabase.to_string(),
            "RepairedDatabase"
        );
    }
}
