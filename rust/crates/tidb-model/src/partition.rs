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

//! `PartitionInfo` and friends from `pkg/meta/model/table.go`.
//!
//! DEFERRED: `PartitionDefinition::MemoryUsage` (Go memory accounting) and
//! `PartitionInfo`'s overlapping-dropping-partition / default-list-partition
//! methods, which encode more of the DDL reorg machinery.

use std::collections::BTreeMap;

use tidb_ast::{CiString, PartitionType};

use crate::action_type::ActionType;
use crate::engine_attribute::{build_storage_class_string, StorageClassTransitRule};
use crate::placement::PolicyRefInfo;
use crate::schema_state::SchemaState;

/// Go `PartitionState`: the online-DDL state of one partition.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PartitionState {
    /// The partition ID.
    pub id: i64,
    /// The online-DDL state.
    pub state: SchemaState,
}

/// Go `UpdateIndexInfo`: an index touched by a partition DDL.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UpdateIndexInfo {
    /// The index name.
    pub index_name: String,
    /// Whether it is a global index.
    pub global: bool,
}

/// Go `PartitionDefinition`: one partition's definition.
#[derive(Clone, Debug, Default)]
pub struct PartitionDefinition {
    /// The partition ID.
    pub id: i64,
    /// The partition name.
    pub name: CiString,
    /// RANGE partition upper bounds.
    pub less_than: Vec<String>,
    /// LIST partition value sets.
    pub in_values: Vec<Vec<String>>,
    /// The placement policy reference.
    pub placement_policy_ref: Option<PolicyRefInfo>,
    /// The partition comment.
    pub comment: String,
    /// The storage-class tier.
    pub storage_class_tier: String,
    /// The storage-class transitions.
    pub storage_class_transitions: Vec<StorageClassTransitRule>,
}

impl PartitionDefinition {
    /// Go `StorageClassString`: the JSON string describing the storage class.
    #[must_use]
    pub fn storage_class_string(&self) -> String {
        build_storage_class_string(&self.storage_class_tier, &self.storage_class_transitions)
    }
}

/// Go `PartitionInfo`: a table's partitioning metadata.
#[derive(Clone, Debug, Default)]
pub struct PartitionInfo {
    /// The partition method.
    pub partition_type: PartitionType,
    /// The partition expression.
    pub expr: String,
    /// The partition columns.
    pub columns: Vec<CiString>,
    /// Whether partitioning is enabled.
    pub enable: bool,
    /// Whether the column list is empty.
    pub is_empty_columns: bool,
    /// The partition definitions.
    pub definitions: Vec<PartitionDefinition>,
    /// Definitions being added.
    pub adding_definitions: Vec<PartitionDefinition>,
    /// Definitions being dropped.
    pub dropping_definitions: Vec<PartitionDefinition>,
    /// New partition IDs from a reorg.
    pub new_partition_ids: Vec<i64>,
    /// The original partition-ID order.
    pub original_partition_ids_order: Vec<i64>,
    /// Per-partition online-DDL states.
    pub states: Vec<PartitionState>,
    /// The partition count.
    pub num: u64,
    /// The in-progress DDL action.
    pub ddl_action: ActionType,
    /// The in-progress DDL state.
    pub ddl_state: SchemaState,
    /// The new table ID during a reorg.
    pub new_table_id: i64,
    /// The in-progress DDL partition type.
    pub ddl_type: PartitionType,
    /// The in-progress DDL expression.
    pub ddl_expr: String,
    /// The in-progress DDL columns.
    pub ddl_columns: Vec<CiString>,
    /// The indexes updated by the in-progress DDL.
    pub ddl_update_indexes: Vec<UpdateIndexInfo>,
    /// The indexes changed by the in-progress DDL.
    pub ddl_changed_index: BTreeMap<i64, bool>,
}

impl PartitionInfo {
    /// Go `GetNameByID`: the (original-case) name of the partition with `id`.
    #[must_use]
    pub fn get_name_by_id(&self, id: i64) -> String {
        self.definitions
            .iter()
            .find(|d| d.id == id)
            .map_or(String::new(), |d| d.name.original().to_owned())
    }

    /// Go `GetStateByID`: the state of partition `id` (default `StatePublic`).
    #[must_use]
    pub fn get_state_by_id(&self, id: i64) -> SchemaState {
        self.states
            .iter()
            .find(|s| s.id == id)
            .map_or(SchemaState::PUBLIC, |s| s.state)
    }

    /// Go `SetStateByID`: set (or insert) the state of partition `id`.
    pub fn set_state_by_id(&mut self, id: i64, state: SchemaState) {
        if let Some(s) = self.states.iter_mut().find(|s| s.id == id) {
            s.state = state;
            return;
        }
        self.states.push(PartitionState { id, state });
    }

    /// Go `GCPartitionStates`: drop states with no matching definition.
    pub fn gc_partition_states(&mut self) {
        if self.states.is_empty() {
            return;
        }
        let ids: std::collections::BTreeSet<i64> = self.definitions.iter().map(|d| d.id).collect();
        self.states.retain(|s| ids.contains(&s.id));
    }

    /// Go `ClearReorgIntermediateInfo`: reset the in-progress DDL fields.
    pub fn clear_reorg_intermediate_info(&mut self) {
        self.ddl_action = ActionType::ACTION_NONE;
        self.ddl_state = SchemaState::NONE;
        self.ddl_type = PartitionType::None;
        self.ddl_expr = String::new();
        self.ddl_columns = Vec::new();
        self.new_table_id = 0;
        self.ddl_changed_index = BTreeMap::new();
    }

    /// Go `FindPartitionDefinitionByName`: index of the partition named
    /// `name` (case-insensitive), or `-1`.
    #[must_use]
    pub fn find_partition_definition_by_name(&self, name: &str) -> i64 {
        let low = name.to_lowercase();
        self.definitions
            .iter()
            .position(|d| d.name.lowercase() == low)
            .map_or(-1, |i| i as i64)
    }

    /// Go `GetPartitionIDByName`: the ID of the partition named `name`
    /// (case-insensitive), or `-1`.
    #[must_use]
    pub fn get_partition_id_by_name(&self, name: &str) -> i64 {
        let low = name.to_lowercase();
        self.definitions
            .iter()
            .find(|d| d.name.lowercase() == low)
            .map_or(-1, |d| d.id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn def(id: i64, name: &str) -> PartitionDefinition {
        PartitionDefinition {
            id,
            name: CiString::new(name),
            ..Default::default()
        }
    }

    #[test]
    fn name_and_id_lookup() {
        let pi = PartitionInfo {
            definitions: vec![def(10, "p0"), def(20, "P1")],
            ..Default::default()
        };
        assert_eq!(pi.get_name_by_id(20), "P1");
        assert_eq!(pi.get_name_by_id(99), "");
        // Case-insensitive name lookup.
        assert_eq!(pi.find_partition_definition_by_name("p1"), 1);
        assert_eq!(pi.find_partition_definition_by_name("nope"), -1);
        assert_eq!(pi.get_partition_id_by_name("P0"), 10);
        assert_eq!(pi.get_partition_id_by_name("x"), -1);
    }

    #[test]
    fn state_get_set_gc() {
        let mut pi = PartitionInfo {
            definitions: vec![def(10, "p0"), def(20, "p1")],
            ..Default::default()
        };
        // Default state is Public.
        assert_eq!(pi.get_state_by_id(10), SchemaState::PUBLIC);
        pi.set_state_by_id(10, SchemaState::DELETE_ONLY);
        assert_eq!(pi.get_state_by_id(10), SchemaState::DELETE_ONLY);
        // Update-in-place, no duplicate.
        pi.set_state_by_id(10, SchemaState::WRITE_ONLY);
        assert_eq!(pi.states.len(), 1);
        // A state for a nonexistent definition is GC'd.
        pi.set_state_by_id(999, SchemaState::PUBLIC);
        assert_eq!(pi.states.len(), 2);
        pi.gc_partition_states();
        assert_eq!(pi.states.len(), 1);
        assert_eq!(pi.get_state_by_id(10), SchemaState::WRITE_ONLY);
    }

    #[test]
    fn clear_reorg_and_storage_class() {
        let mut pi = PartitionInfo {
            new_table_id: 7,
            ddl_expr: "x".into(),
            ..Default::default()
        };
        pi.clear_reorg_intermediate_info();
        assert_eq!(pi.new_table_id, 0);
        assert!(pi.ddl_expr.is_empty());
        assert_eq!(pi.ddl_type, PartitionType::None);

        let d = PartitionDefinition {
            storage_class_tier: "STANDARD".into(),
            ..Default::default()
        };
        assert_eq!(d.storage_class_string(), "STANDARD");
    }
}
