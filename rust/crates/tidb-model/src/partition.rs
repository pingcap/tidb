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
//! Go runtime-specific `unsafe.Sizeof` memory accounting is recorded as an
//! explicit package-lockdown decline; the persisted and DDL transition rules
//! are implemented here.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use tidb_ast::{CiString, PartitionType};

use crate::action_type::ActionType;
use crate::engine_attribute::{build_storage_class_string, StorageClassTransitRule};
use crate::placement::PolicyRefInfo;
use crate::schema_state::SchemaState;

/// Go marshals `ast.PartitionType` (an `int`) and `ActionType` (a `byte`) as
/// plain JSON numbers: neither has a `MarshalJSON`. These adapters reproduce
/// that, since neither Rust type carries serde impls of its own here.
mod partition_type_json {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use tidb_ast::PartitionType;

    /// The raw integer is carried through unchanged. Collapsing an unnamed
    /// value to `NONE` would relabel a partitioned table "not partitioned"
    /// while its `definitions` and `num` stay populated.
    pub fn serialize<S: Serializer>(t: &PartitionType, s: S) -> Result<S::Ok, S::Error> {
        t.0.serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<PartitionType, D::Error> {
        Ok(PartitionType(Option::<i64>::deserialize(d)?.unwrap_or(0)))
    }

    /// Go `omitempty` on a `PartitionType`: the zero value is `PartitionTypeNone`.
    pub fn is_none(t: &PartitionType) -> bool {
        t.0 == 0
    }
}

/// `ActionType` as the JSON number Go writes for its underlying `byte`.
mod action_type_json {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::action_type::ActionType;

    pub fn serialize<S: Serializer>(a: &ActionType, s: S) -> Result<S::Ok, S::Error> {
        a.0.serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<ActionType, D::Error> {
        Ok(ActionType(Option::<u8>::deserialize(d)?.unwrap_or(0)))
    }

    /// Go `omitempty` on an `ActionType`: the zero value is `ActionNone`.
    pub fn is_none(a: &ActionType) -> bool {
        a.0 == 0
    }
}

/// Go `PartitionState`: the online-DDL state of one partition.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionState {
    /// The partition ID.
    #[serde(rename = "id", default)]
    pub id: i64,
    /// The online-DDL state.
    #[serde(rename = "state", default)]
    pub state: SchemaState,
}

/// Go `UpdateIndexInfo`: an index touched by a partition DDL.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateIndexInfo {
    /// The index name.
    #[serde(
        rename = "index_name",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub index_name: String,
    /// Whether it is a global index.
    #[serde(rename = "global", default)]
    pub global: bool,
}

/// Go `PartitionDefinition`: one partition's definition.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PartitionDefinition {
    /// The partition ID.
    #[serde(rename = "id", default)]
    pub id: i64,
    /// The partition name.
    #[serde(rename = "name", default)]
    pub name: CiString,
    /// RANGE partition upper bounds.
    #[serde(
        rename = "less_than",
        default,
        deserialize_with = "crate::serde_helpers::null_default",
        serialize_with = "crate::serde_helpers::null_if_empty"
    )]
    pub less_than: Vec<String>,
    /// LIST partition value sets.
    #[serde(
        rename = "in_values",
        default,
        deserialize_with = "crate::serde_helpers::null_default",
        serialize_with = "crate::serde_helpers::null_if_empty"
    )]
    pub in_values: Vec<Vec<String>>,
    /// The placement policy reference.
    #[serde(rename = "policy_ref_info", default)]
    pub placement_policy_ref: Option<PolicyRefInfo>,
    /// The partition comment.
    #[serde(
        rename = "comment",
        default,
        skip_serializing_if = "crate::serde_helpers::is_empty_str",
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub comment: String,
    /// The storage-class tier.
    #[serde(
        rename = "storage_class_tier",
        default,
        skip_serializing_if = "crate::serde_helpers::is_empty_str",
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub storage_class_tier: String,
    /// The storage-class transitions.
    #[serde(
        rename = "storage_class_transitions",
        default,
        skip_serializing_if = "crate::serde_helpers::is_empty_vec",
        deserialize_with = "crate::serde_helpers::null_default"
    )]
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
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PartitionInfo {
    /// The partition method.
    #[serde(rename = "type", default, with = "partition_type_json")]
    pub partition_type: PartitionType,
    /// The partition expression.
    #[serde(
        rename = "expr",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub expr: String,
    /// The partition columns.
    #[serde(
        rename = "columns",
        default,
        deserialize_with = "crate::serde_helpers::null_default",
        serialize_with = "crate::serde_helpers::null_if_empty"
    )]
    pub columns: Vec<CiString>,
    /// Whether partitioning is enabled.
    #[serde(rename = "enable", default)]
    pub enable: bool,
    /// Whether the column list is empty.
    #[serde(rename = "is_empty_columns", default)]
    pub is_empty_columns: bool,
    /// The partition definitions.
    #[serde(
        rename = "definitions",
        default,
        deserialize_with = "crate::serde_helpers::null_default",
        serialize_with = "crate::serde_helpers::null_if_empty"
    )]
    pub definitions: Vec<PartitionDefinition>,
    /// Definitions being added.
    #[serde(
        rename = "adding_definitions",
        default,
        deserialize_with = "crate::serde_helpers::null_default",
        serialize_with = "crate::serde_helpers::null_if_empty"
    )]
    pub adding_definitions: Vec<PartitionDefinition>,
    /// Definitions being dropped.
    #[serde(
        rename = "dropping_definitions",
        default,
        deserialize_with = "crate::serde_helpers::null_default",
        serialize_with = "crate::serde_helpers::null_if_empty"
    )]
    pub dropping_definitions: Vec<PartitionDefinition>,
    /// New partition IDs from a reorg.
    #[serde(
        rename = "new_partition_ids",
        default,
        skip_serializing_if = "crate::serde_helpers::is_empty_vec",
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub new_partition_ids: Vec<i64>,
    /// The original partition-ID order.
    #[serde(
        rename = "original_partition_ids_order",
        default,
        skip_serializing_if = "crate::serde_helpers::is_empty_vec",
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub original_partition_ids_order: Vec<i64>,
    /// Per-partition online-DDL states.
    #[serde(
        rename = "states",
        default,
        deserialize_with = "crate::serde_helpers::null_default",
        serialize_with = "crate::serde_helpers::null_if_empty"
    )]
    pub states: Vec<PartitionState>,
    /// The partition count.
    #[serde(rename = "num", default)]
    pub num: u64,
    /// The in-progress DDL action.
    #[serde(
        rename = "ddl_action",
        default,
        with = "action_type_json",
        skip_serializing_if = "action_type_json::is_none"
    )]
    pub ddl_action: ActionType,
    /// The in-progress DDL state.
    #[serde(rename = "ddl_state", default)]
    pub ddl_state: SchemaState,
    /// The new table ID during a reorg.
    #[serde(
        rename = "new_table_id",
        default,
        skip_serializing_if = "crate::serde_helpers::is_zero_i64"
    )]
    pub new_table_id: i64,
    /// The in-progress DDL partition type.
    #[serde(
        rename = "ddl_type",
        default,
        with = "partition_type_json",
        skip_serializing_if = "partition_type_json::is_none"
    )]
    pub ddl_type: PartitionType,
    /// The in-progress DDL expression.
    #[serde(
        rename = "ddl_expr",
        default,
        skip_serializing_if = "crate::serde_helpers::is_empty_str",
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub ddl_expr: String,
    /// The in-progress DDL columns.
    #[serde(
        rename = "ddl_columns",
        default,
        skip_serializing_if = "crate::serde_helpers::is_empty_vec",
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub ddl_columns: Vec<CiString>,
    /// The indexes updated by the in-progress DDL.
    #[serde(
        rename = "ddl_update_indexes",
        default,
        skip_serializing_if = "crate::serde_helpers::is_empty_vec",
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub ddl_update_indexes: Vec<UpdateIndexInfo>,
    /// The indexes changed by the in-progress DDL.
    ///
    /// Go writes a `map[int64]bool` with the keys formatted as JSON strings and
    /// sorted by that string form, which `go_int_key_map` reproduces.
    #[serde(
        rename = "ddl_changed_index",
        default,
        skip_serializing_if = "BTreeMap::is_empty",
        deserialize_with = "crate::serde_helpers::null_default",
        serialize_with = "crate::serde_helpers::go_int_key_map"
    )]
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
        self.ddl_type = PartitionType::NONE;
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

    /// Go `GetDefaultListPartition`: the first empty/default LIST partition.
    #[must_use]
    pub fn get_default_list_partition(&self) -> isize {
        if self.partition_type != PartitionType::LIST {
            return -1;
        }
        self.definitions
            .iter()
            .position(|definition| {
                definition.in_values.is_empty()
                    || definition
                        .in_values
                        .iter()
                        .any(|values| values.len() == 1 && values[0] == "DEFAULT")
            })
            .map_or(-1, |index| index as isize)
    }

    /// Go `CanHaveOverlappingDroppingPartition`.
    #[must_use]
    pub fn can_have_overlapping_dropping_partition(&self) -> bool {
        self.ddl_action == ActionType::ACTION_DROP_TABLE_PARTITION
            && self.ddl_state == SchemaState::WRITE_ONLY
    }

    /// Go `ReplaceWithOverlappingPartitionIdx`. `Some(error)` is the Rust
    /// equivalent of a non-nil Go error and is cleared only when a valid
    /// replacement exists.
    pub fn replace_with_overlapping_partition_idx<E>(
        &self,
        mut index: isize,
        mut error: Option<E>,
    ) -> (isize, Option<E>) {
        if error.is_some() && index >= 0 {
            index = self.get_overlapping_dropping_partition_idx(index);
            if index >= 0 {
                error = None;
            }
        }
        (index, error)
    }

    /// Go `GetOverlappingDroppingPartitionIdx`.
    #[must_use]
    pub fn get_overlapping_dropping_partition_idx(&self, index: isize) -> isize {
        if index < 0 || index as usize >= self.definitions.len() {
            return -1;
        }
        if !self.can_have_overlapping_dropping_partition() {
            return index;
        }
        match self.partition_type {
            PartitionType::RANGE => {
                for candidate in index as usize..self.definitions.len() {
                    if !self.is_dropping(candidate) {
                        return candidate as isize;
                    }
                }
                -1
            }
            PartitionType::LIST => {
                if !self.is_dropping(index as usize) {
                    return index;
                }
                let default_index = self.get_default_list_partition();
                if default_index == index {
                    -1
                } else {
                    default_index
                }
            }
            _ => index,
        }
    }

    /// Go `IsDropping`. As in Go, `index` must identify an existing
    /// definition; invalid metadata is an invariant violation and panics.
    #[must_use]
    pub fn is_dropping(&self, index: usize) -> bool {
        let id = self.definitions[index].id;
        self.dropping_definitions
            .iter()
            .any(|definition| definition.id == id)
    }

    /// Go `SetOriginalPartitionIDs`.
    pub fn set_original_partition_ids(&mut self) {
        self.original_partition_ids_order = self
            .definitions
            .iter()
            .map(|definition| definition.id)
            .collect();
    }

    /// Go `IDsInDDLToIgnore`. A zero-length result covers Go's nil slice;
    /// callers consume the IDs, not allocation identity.
    #[must_use]
    pub fn ids_in_ddl_to_ignore(&self) -> Vec<i64> {
        match self.ddl_action {
            ActionType::ACTION_TRUNCATE_TABLE_PARTITION => match self.ddl_state {
                SchemaState::WRITE_ONLY => self.new_partition_ids.clone(),
                SchemaState::DELETE_ONLY | SchemaState::DELETE_REORGANIZATION => self
                    .dropping_definitions
                    .iter()
                    .map(|definition| definition.id)
                    .collect(),
                _ => Vec::new(),
            },
            ActionType::ACTION_DROP_TABLE_PARTITION => self
                .dropping_definitions
                .iter()
                .map(|definition| definition.id)
                .collect(),
            ActionType::ACTION_ADD_TABLE_PARTITION => self
                .adding_definitions
                .iter()
                .map(|definition| definition.id)
                .collect(),
            _ => Vec::new(),
        }
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
    fn default_and_overlapping_partition_boundaries() {
        let mut range = PartitionInfo {
            partition_type: PartitionType::RANGE,
            definitions: vec![def(1, "p0"), def(2, "p1"), def(3, "p2")],
            dropping_definitions: vec![def(1, "p0"), def(2, "p1")],
            ddl_action: ActionType::ACTION_DROP_TABLE_PARTITION,
            ddl_state: SchemaState::WRITE_ONLY,
            ..Default::default()
        };
        assert_eq!(range.get_default_list_partition(), -1);
        assert_eq!(range.get_overlapping_dropping_partition_idx(-1), -1);
        assert_eq!(range.get_overlapping_dropping_partition_idx(0), 2);
        assert_eq!(range.get_overlapping_dropping_partition_idx(2), 2);
        let (index, error) = range.replace_with_overlapping_partition_idx(0, Some("dropped"));
        assert_eq!(index, 2);
        assert!(error.is_none());
        range.dropping_definitions.push(def(3, "p2"));
        let (index, error) = range.replace_with_overlapping_partition_idx(0, Some("dropped"));
        assert_eq!(index, -1);
        assert_eq!(error, Some("dropped"));

        let list = PartitionInfo {
            partition_type: PartitionType::LIST,
            definitions: vec![
                PartitionDefinition {
                    id: 1,
                    name: CiString::new("p0"),
                    in_values: vec![vec!["1".to_owned()]],
                    ..Default::default()
                },
                PartitionDefinition {
                    id: 2,
                    name: CiString::new("pd"),
                    in_values: vec![vec!["DEFAULT".to_owned()]],
                    ..Default::default()
                },
            ],
            dropping_definitions: vec![def(1, "p0")],
            ddl_action: ActionType::ACTION_DROP_TABLE_PARTITION,
            ddl_state: SchemaState::WRITE_ONLY,
            ..Default::default()
        };
        assert_eq!(list.get_default_list_partition(), 1);
        assert_eq!(list.get_overlapping_dropping_partition_idx(0), 1);
    }

    #[test]
    fn partition_id_visibility_boundaries() {
        let mut partition = PartitionInfo {
            definitions: vec![def(1, "p0"), def(2, "p1")],
            adding_definitions: vec![def(3, "p2")],
            dropping_definitions: vec![def(1, "p0")],
            new_partition_ids: vec![10, 20],
            ..Default::default()
        };
        partition.set_original_partition_ids();
        assert_eq!(partition.original_partition_ids_order, vec![1, 2]);

        partition.ddl_action = ActionType::ACTION_TRUNCATE_TABLE_PARTITION;
        partition.ddl_state = SchemaState::WRITE_ONLY;
        assert_eq!(partition.ids_in_ddl_to_ignore(), vec![10, 20]);
        partition.ddl_state = SchemaState::DELETE_ONLY;
        assert_eq!(partition.ids_in_ddl_to_ignore(), vec![1]);
        partition.ddl_action = ActionType::ACTION_DROP_TABLE_PARTITION;
        assert_eq!(partition.ids_in_ddl_to_ignore(), vec![1]);
        partition.ddl_action = ActionType::ACTION_ADD_TABLE_PARTITION;
        assert_eq!(partition.ids_in_ddl_to_ignore(), vec![3]);
        partition.ddl_action = ActionType::ACTION_NONE;
        assert!(partition.ids_in_ddl_to_ignore().is_empty());
    }

    // Field order, tag names and bytes compared against Go's json.Marshal.
    // An empty Vec re-serializes as `null`, which is what Go's nil slice
    // produces, so the round trip is byte-exact.
    #[test]
    fn partition_info_json_matches_go() {
        let go = r#"{"type":1,"expr":"a","columns":[{"O":"c","L":"c"}],"enable":true,"is_empty_columns":false,"definitions":[{"id":1,"name":{"O":"p0","L":"p0"},"less_than":["10"],"in_values":null,"policy_ref_info":null}],"adding_definitions":null,"dropping_definitions":null,"states":[{"id":1,"state":5}],"num":2,"ddl_state":0,"ddl_changed_index":{"10":false,"2":true}}"#;
        let pi: PartitionInfo = serde_json::from_str(go).unwrap();
        assert_eq!(pi.partition_type, PartitionType::RANGE);
        assert_eq!(pi.definitions.len(), 1);
        assert_eq!(pi.definitions[0].name.original(), "p0");
        assert!(pi.definitions[0].in_values.is_empty());
        assert!(pi.adding_definitions.is_empty());
        assert_eq!(pi.states[0].state, SchemaState::PUBLIC);
        assert_eq!(pi.num, 2);
        assert_eq!(pi.ddl_changed_index.get(&10), Some(&false));

        assert_eq!(serde_json::to_string(&pi).unwrap(), go);

        // Every omitempty field is absent from the zero value, as in Go.
        let empty = serde_json::to_string(&PartitionInfo::default()).unwrap();
        assert_eq!(
            empty,
            r#"{"type":0,"expr":"","columns":null,"enable":false,"is_empty_columns":false,"definitions":null,"adding_definitions":null,"dropping_definitions":null,"states":null,"num":0,"ddl_state":0}"#
        );
    }

    // Go's `PartitionType` is a plain `int`: any number survives Unmarshal and
    // Marshal untouched. Collapsing an unnamed value to `PartitionTypeNone`
    // would relabel this table "not partitioned" while `definitions` and `num`
    // stay populated -- an internally contradictory TableInfo, written back to
    // every node in the cluster on the first Rust DDL.
    #[test]
    fn unknown_partition_type_survives_round_trip() {
        let go = r#"{"type":9,"expr":"a","columns":null,"enable":true,"is_empty_columns":false,"definitions":[{"id":1,"name":{"O":"p0","L":"p0"},"less_than":["10"],"in_values":null,"policy_ref_info":null}],"adding_definitions":null,"dropping_definitions":null,"states":null,"num":2,"ddl_state":0}"#;
        let pi: PartitionInfo = serde_json::from_str(go).unwrap();
        assert_eq!(pi.partition_type, PartitionType(9));
        assert_eq!(pi.partition_type.sql(), "");
        assert_eq!(pi.definitions.len(), 1);
        assert_eq!(serde_json::to_string(&pi).unwrap(), go);
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
        assert_eq!(pi.ddl_type, PartitionType::NONE);

        let d = PartitionDefinition {
            storage_class_tier: "STANDARD".into(),
            ..Default::default()
        };
        assert_eq!(d.storage_class_string(), "STANDARD");
    }
}
