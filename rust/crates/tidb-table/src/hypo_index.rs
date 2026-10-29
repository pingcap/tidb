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

//! Hypothetical indexes and TiFlash replicas for the Index Advisor
//! (`pkg/ddl/executor.go` `addHypoIndexIntoCtx` / `dropHypoIndexFromCtx` /
//! `setHypoTiFlashReplica`).
//!
//! Neither is schema: Go keeps them in `SessionVars.HypoIndexes` and
//! `SessionVars.HypoTiFlashReplicas`, keyed by the lowercase database and
//! table NAMES, so they outlive a dropped table and apply to a new table of
//! the same name. Only `EXPLAIN` planning (`getPossibleAccessPaths`,
//! `UsedHypoTiFlashReplicas`) and `SHOW CREATE TABLE` read them.

use std::collections::{BTreeMap, BTreeSet};

use tidb_hack::GoToLower;

use tidb_model::index::IndexInfo;
use crate::kv_table::{KvIndex, KvTable};

/// Go `SessionVars.HypoIndexes`: `dbName -> tblName -> idxName -> idxInfo`,
/// every key lowercase.
#[derive(Clone, Debug, Default)]
pub struct HypoIndexes {
    by_table: BTreeMap<String, BTreeMap<String, BTreeMap<String, IndexInfo>>>,
    /// How many ids [`Self::add`] has handed out.
    allocated_ids: i64,
}

use tidb_sessionstates::HypoIndexMap;

impl HypoIndexes {
    /// Go `EncodeSessionStates`: `sessionStates.HypoIndexes = s.HypoIndexes`.
    #[must_use]
    pub fn session_state(&self) -> &HypoIndexMap {
        &self.by_table
    }

    /// Go `DecodeSessionStates`: `s.HypoIndexes = sessionStates.HypoIndexes`,
    /// each index taking its own planning id (see [`Self::add`]).
    #[must_use]
    pub fn from_session_state(mut by_table: HypoIndexMap) -> Self {
        let mut allocated_ids = 0;
        for index in by_table
            .values_mut()
            .flat_map(BTreeMap::values_mut)
            .flat_map(BTreeMap::values_mut)
        {
            allocated_ids += 1;
            index.id = -allocated_ids;
        }
        Self {
            by_table,
            allocated_ids,
        }
    }

    /// Whether no hypothetical index is defined anywhere.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_table
            .values()
            .all(|tables| tables.values().all(BTreeMap::is_empty))
    }

    /// The hypothetical indexes of `database`.`table`, ordered by Go's
    /// `Name.O` -- the order `SHOW CREATE TABLE` sorts them into "to make the
    /// result stable" (`show.go`). Go's access-path loop walks the map in
    /// random order; this one order serves both.
    #[must_use]
    pub fn table_indexes(&self, database: &str, table: &str) -> Vec<&IndexInfo> {
        let mut indexes: Vec<&IndexInfo> = self
            .by_table
            .get(&database.go_to_lower())
            .and_then(|tables| tables.get(&table.go_to_lower()))
            .map(|indexes| indexes.values().collect())
            .unwrap_or_default();
        indexes.sort_by(|left, right| left.name.original().cmp(right.name.original()));
        indexes
    }

    /// [`Self::table_indexes`] resolved against `table`'s columns, for the
    /// readers that work over the loaded table (EXPLAIN's access objects,
    /// `SHOW CREATE TABLE`). An index naming a column the table no longer
    /// has is left out.
    #[must_use]
    pub fn table_kv_indexes(&self, database: &str, table: &KvTable) -> Vec<KvIndex> {
        self.table_indexes(database, &table.name())
            .into_iter()
            .filter_map(|index| crate::table_from_meta::kv_index(index, &table.columns).ok())
            .collect()
    }

    /// Go `addHypoIndexIntoCtx`.
    ///
    /// Go leaves a hypothetical index's id 0 and every plan carries the
    /// `*IndexInfo` itself. Plans here name their index by id, so each one
    /// takes its own negative id, which no stored index can have; ids never
    /// reach EXPLAIN's text.
    pub fn add(
        &mut self,
        database: &str,
        table: &str,
        mut index: IndexInfo,
    ) -> Result<(), tidb_error::mysql::MysqlError> {
        let indexes = self
            .by_table
            .entry(database.go_to_lower())
            .or_default()
            .entry(table.go_to_lower())
            .or_default();
        let name = index.name.lowercase().to_owned();
        if indexes.contains_key(&name) {
            // Go `errors.Errorf("conflict hypo index name %s", indexName.L)`.
            return Err(tidb_error::mysql::MysqlError::unknown(format!(
                "conflict hypo index name {name}"
            )));
        }
        self.allocated_ids += 1;
        index.id = -self.allocated_ids;
        indexes.insert(name, index);
        Ok(())
    }

    /// Go `dropHypoIndexFromCtx`.
    pub fn remove(
        &mut self,
        database: &str,
        table: &str,
        index: &str,
        if_exists: bool,
    ) -> Result<(), tidb_error::mysql::MysqlError> {
        let removed = self
            .by_table
            .get_mut(&database.go_to_lower())
            .and_then(|tables| tables.get_mut(&table.go_to_lower()))
            .and_then(|indexes| indexes.remove(&index.go_to_lower()));
        if removed.is_none() && !if_exists {
            // Go `ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist")`.
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_CANT_DROP_FIELD_OR_KEY
                    .generate_with_stack(format!("index {index} doesn't exist")),
            ));
        }
        Ok(())
    }
}

/// Go `SessionVars.HypoTiFlashReplicas`: `dbName -> tblName`, lowercase.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HypoTiFlashReplicas {
    by_database: BTreeMap<String, BTreeSet<String>>,
}

impl HypoTiFlashReplicas {
    /// Go `setHypoTiFlashReplica`'s map write, by lowercase schema and table
    /// name: a positive count adds the replica, zero deletes it.
    pub fn set(&mut self, schema: &str, table: &str, add: bool) {
        let tables = self.by_database.entry(schema.to_owned()).or_default();
        if add {
            tables.insert(table.to_owned());
        } else {
            tables.remove(table);
        }
    }

    /// Go `EncodeSessionStates`' `sessionStates.HypoTiFlashReplicas =
    /// s.HypoTiFlashReplicas`.
    #[must_use]
    pub fn session_state(&self) -> tidb_sessionstates::HypoTiFlashReplicaMap {
        self.by_database
            .iter()
            .map(|(database, tables)| {
                let tables = tables
                    .iter()
                    .map(|table| (table.clone(), tidb_sessionstates::Empty {}))
                    .collect();
                (database.clone(), tables)
            })
            .collect()
    }

    /// Go `DecodeSessionStates`' `s.HypoTiFlashReplicas =
    /// sessionStates.HypoTiFlashReplicas`.
    #[must_use]
    pub fn from_session_state(state: tidb_sessionstates::HypoTiFlashReplicaMap) -> Self {
        Self {
            by_database: state
                .into_iter()
                .map(|(database, tables)| (database, tables.into_keys().collect()))
                .collect(),
        }
    }

    /// Whether no hypothetical replica is defined anywhere.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_database.values().all(BTreeSet::is_empty)
    }

    /// The membership test of Go `UsedHypoTiFlashReplicas`.
    #[must_use]
    pub fn contains(&self, database: &str, table: &str) -> bool {
        self.by_database
            .get(&database.go_to_lower())
            .is_some_and(|tables| tables.contains(&table.go_to_lower()))
    }
}
