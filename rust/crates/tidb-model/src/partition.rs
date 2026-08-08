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

use serde::{Deserialize, Serialize, Serializer};
use tidb_ast::{CiString, PartitionType};

use crate::action_type::ActionType;
use crate::engine_attribute::{build_storage_class_string, StorageClassTransitRule};
use crate::go_runtime::{GoShared, GoSharedSlice, GoSliceElementLayout};
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
    #[serde(rename = "less_than", default)]
    pub less_than: GoSharedSlice<String>,
    /// LIST partition value sets.
    #[serde(rename = "in_values", default)]
    pub in_values: GoSharedSlice<GoSharedSlice<String>>,
    /// The placement policy reference.
    #[serde(rename = "policy_ref_info", default)]
    pub placement_policy_ref: Option<GoShared<PolicyRefInfo>>,
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
        skip_serializing_if = "GoSharedSlice::is_empty"
    )]
    pub storage_class_transitions: GoSharedSlice<StorageClassTransitRule>,
}

impl PartitionDefinition {
    /// Go `PartitionDefinition.Clone`: `LessThan` and storage transitions use
    /// `slices.Clone`; `InValues` and the policy pointer remain shallow.
    #[must_use]
    pub fn clone_like_go(&self) -> Self {
        Self {
            id: self.id,
            name: self.name.clone(),
            less_than: self
                .less_than
                .slices_clone(16, GoSliceElementLayout::PointerBearing),
            in_values: self.in_values.clone(),
            placement_policy_ref: self.placement_policy_ref.clone(),
            comment: self.comment.clone(),
            storage_class_tier: self.storage_class_tier.clone(),
            storage_class_transitions: self
                .storage_class_transitions
                .slices_clone(32, GoSliceElementLayout::PointerBearing),
        }
    }
    /// Nil-receiver-capable Go clone call boundary. The source method
    /// dereferences its receiver before returning a value, so nil panics.
    #[must_use]
    pub fn clone_pointer(definition: Option<&Self>) -> Self {
        definition
            .expect("nil *PartitionDefinition")
            .clone_like_go()
    }

    /// Go `MemoryUsage` on a non-nil partition definition.
    ///
    /// The constants are the owning Go source's 64-bit `unsafe.Sizeof`
    /// results: `PartitionState` is 16 bytes, a string header is 16 bytes,
    /// and `PolicyRefInfo.ID` is 8 bytes. String payloads use byte length.
    #[must_use]
    pub fn memory_usage(&self) -> i64 {
        const GO_PARTITION_STATE_SIZE: i64 = 16;
        const GO_CI_STRING_HEADERS_SIZE: i64 = 32;
        const GO_POLICY_ID_SIZE: i64 = 8;

        let ci_string_usage = |value: &CiString| {
            GO_CI_STRING_HEADERS_SIZE
                + i64::try_from(value.original().len() + value.lowercase().len())
                    .expect("CiString byte length exceeds Go int64")
        };
        let mut sum = GO_PARTITION_STATE_SIZE + ci_string_usage(&self.name);
        if let Some(policy) = &self.placement_policy_ref {
            let policy = policy.read();
            sum += GO_POLICY_ID_SIZE + ci_string_usage(&policy.name);
        }
        let less_than = self.less_than.snapshot();
        let in_values = self.in_values.snapshot();
        let less_than_bytes = less_than
            .iter()
            .map(|value| i64::try_from(value.len()).expect("partition value exceeds Go int64"))
            .sum::<i64>();
        let in_value_bytes = in_values
            .iter()
            .flat_map(GoSharedSlice::snapshot)
            .map(|value| i64::try_from(value.len()).expect("partition value exceeds Go int64"))
            .sum::<i64>();
        sum + less_than_bytes + in_value_bytes
    }

    /// Nil-receiver-capable Go `(*PartitionDefinition).MemoryUsage` boundary.
    /// The source method explicitly returns zero before dereferencing a nil
    /// receiver.
    #[must_use]
    pub fn memory_usage_pointer(definition: Option<&Self>) -> i64 {
        definition.map_or(0, Self::memory_usage)
    }

    /// Go `StorageClassString`: the JSON string describing the storage class.
    #[must_use]
    pub fn storage_class_string(&self) -> String {
        build_storage_class_string(
            &self.storage_class_tier,
            &self.storage_class_transitions.snapshot(),
        )
    }
}

fn shared_int_bool_map_is_empty(value: &Option<GoShared<BTreeMap<i64, bool>>>) -> bool {
    value.as_ref().is_none_or(|map| map.read().is_empty())
}

fn serialize_shared_int_bool_map<S>(
    value: &Option<GoShared<BTreeMap<i64, bool>>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        None => serializer.serialize_none(),
        Some(value) => crate::serde_helpers::go_int_key_map(&value.read(), serializer),
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
    #[serde(rename = "columns", default)]
    pub columns: GoSharedSlice<CiString>,
    /// Whether partitioning is enabled.
    #[serde(rename = "enable", default)]
    pub enable: bool,
    /// Whether the column list is empty.
    #[serde(rename = "is_empty_columns", default)]
    pub is_empty_columns: bool,
    /// The partition definitions.
    #[serde(rename = "definitions", default)]
    pub definitions: GoSharedSlice<PartitionDefinition>,
    /// Definitions being added.
    #[serde(rename = "adding_definitions", default)]
    pub adding_definitions: GoSharedSlice<PartitionDefinition>,
    /// Definitions being dropped.
    #[serde(rename = "dropping_definitions", default)]
    pub dropping_definitions: GoSharedSlice<PartitionDefinition>,
    /// New partition IDs from a reorg.
    #[serde(
        rename = "new_partition_ids",
        default,
        skip_serializing_if = "GoSharedSlice::is_empty"
    )]
    pub new_partition_ids: GoSharedSlice<i64>,
    /// The original partition-ID order.
    #[serde(
        rename = "original_partition_ids_order",
        default,
        skip_serializing_if = "GoSharedSlice::is_empty"
    )]
    pub original_partition_ids_order: GoSharedSlice<i64>,
    /// Per-partition online-DDL states.
    #[serde(rename = "states", default)]
    pub states: GoSharedSlice<PartitionState>,
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
        skip_serializing_if = "GoSharedSlice::is_empty"
    )]
    pub ddl_columns: GoSharedSlice<CiString>,
    /// The indexes updated by the in-progress DDL.
    #[serde(
        rename = "ddl_update_indexes",
        default,
        skip_serializing_if = "GoSharedSlice::is_empty"
    )]
    pub ddl_update_indexes: GoSharedSlice<UpdateIndexInfo>,
    /// The indexes changed by the in-progress DDL.
    ///
    /// Go writes a `map[int64]bool` with the keys formatted as JSON strings and
    /// sorted by that string form, which `go_int_key_map` reproduces.
    #[serde(
        rename = "ddl_changed_index",
        default,
        skip_serializing_if = "shared_int_bool_map_is_empty",
        serialize_with = "serialize_shared_int_bool_map"
    )]
    pub ddl_changed_index: Option<GoShared<BTreeMap<i64, bool>>>,
}

impl PartitionInfo {
    /// Go `PartitionInfo.Clone`: Columns uses `slices.Clone`; definition
    /// containers are always freshly `make`d and element-cloned; all other
    /// slice/map headers deliberately remain shallow aliases.
    #[must_use]
    pub fn clone_like_go(&self) -> Self {
        let clone_definitions = |source: &GoSharedSlice<PartitionDefinition>| {
            GoSharedSlice::from_vec(source.map_visible(PartitionDefinition::clone_like_go))
        };
        Self {
            partition_type: self.partition_type,
            expr: self.expr.clone(),
            columns: self
                .columns
                .slices_clone(32, GoSliceElementLayout::PointerBearing),
            enable: self.enable,
            is_empty_columns: self.is_empty_columns,
            definitions: clone_definitions(&self.definitions),
            adding_definitions: clone_definitions(&self.adding_definitions),
            dropping_definitions: clone_definitions(&self.dropping_definitions),
            new_partition_ids: self.new_partition_ids.clone(),
            original_partition_ids_order: self.original_partition_ids_order.clone(),
            states: self.states.clone(),
            num: self.num,
            ddl_action: self.ddl_action,
            ddl_state: self.ddl_state,
            new_table_id: self.new_table_id,
            ddl_type: self.ddl_type,
            ddl_expr: self.ddl_expr.clone(),
            ddl_columns: self.ddl_columns.clone(),
            ddl_update_indexes: self.ddl_update_indexes.clone(),
            ddl_changed_index: self.ddl_changed_index.clone(),
        }
    }
    /// Nil-receiver-capable Go clone call boundary.
    #[must_use]
    pub fn clone_pointer(partition: Option<&Self>) -> GoShared<Self> {
        GoShared::new(partition.expect("nil *PartitionInfo").clone_like_go())
    }

    /// Go `GetNameByID`: the (original-case) name of the partition with `id`.
    #[must_use]
    pub fn get_name_by_id(&self, id: i64) -> String {
        self.definitions
            .snapshot()
            .into_iter()
            .find(|d| d.id == id)
            .map_or(String::new(), |d| d.name.original().to_owned())
    }

    /// Go `GetStateByID`: the state of partition `id` (default `StatePublic`).
    #[must_use]
    pub fn get_state_by_id(&self, id: i64) -> SchemaState {
        self.states
            .snapshot()
            .into_iter()
            .find(|s| s.id == id)
            .map_or(SchemaState::PUBLIC, |s| s.state)
    }

    /// Go `SetStateByID`: set (or insert) the state of partition `id`.
    pub fn set_state_by_id(&mut self, id: i64, state: SchemaState) {
        if let Some(index) = self
            .states
            .snapshot()
            .iter()
            .position(|existing| existing.id == id)
        {
            self.states.set(index, PartitionState { id, state });
            return;
        }
        self.states.push_go(
            PartitionState { id, state },
            16,
            GoSliceElementLayout::NoPointers,
        );
    }

    /// Go `GCPartitionStates`: drop states with no matching definition.
    pub fn gc_partition_states(&mut self) {
        if self.states.is_empty() {
            return;
        }
        let definitions = self.definitions.snapshot();
        let ids: std::collections::BTreeSet<i64> =
            definitions.iter().map(|definition| definition.id).collect();
        let mut states = GoSharedSlice::from_vec_with_capacity(Vec::new(), definitions.len());
        for state in self.states.snapshot() {
            if ids.contains(&state.id) {
                states.push_go(state, 16, GoSliceElementLayout::NoPointers);
            }
        }
        self.states = states;
    }

    /// Go `ClearReorgIntermediateInfo`: reset the in-progress DDL fields.
    pub fn clear_reorg_intermediate_info(&mut self) {
        self.ddl_action = ActionType::ACTION_NONE;
        self.ddl_state = SchemaState::NONE;
        self.ddl_type = PartitionType::NONE;
        self.ddl_expr = String::new();
        self.ddl_columns = GoSharedSlice::default();
        self.new_table_id = 0;
        self.ddl_changed_index = None;
    }

    /// Go `FindPartitionDefinitionByName`: index of the partition named
    /// `name` (case-insensitive), or `-1`.
    #[must_use]
    pub fn find_partition_definition_by_name(&self, name: &str) -> i64 {
        let low = tidb_mysql::to_lowercase(name);
        self.definitions
            .snapshot()
            .into_iter()
            .position(|d| d.name.lowercase() == low)
            .map_or(-1, |i| i as i64)
    }

    /// Go `GetPartitionIDByName`: the ID of the partition named `name`
    /// (case-insensitive), or `-1`.
    #[must_use]
    pub fn get_partition_id_by_name(&self, name: &str) -> i64 {
        let low = tidb_mysql::to_lowercase(name);
        self.definitions
            .snapshot()
            .into_iter()
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
            .snapshot()
            .into_iter()
            .position(|definition| {
                definition.in_values.is_empty()
                    || definition
                        .in_values
                        .snapshot()
                        .into_iter()
                        .any(|values| values.len() == 1 && values.get(0) == "DEFAULT")
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
                    if !self.is_dropping(candidate as isize) {
                        return candidate as isize;
                    }
                }
                -1
            }
            PartitionType::LIST => {
                if !self.is_dropping(index) {
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
    pub fn is_dropping(&self, index: isize) -> bool {
        let id = self.definitions.get(index as usize).id;
        self.dropping_definitions
            .snapshot()
            .into_iter()
            .any(|definition| definition.id == id)
    }

    /// Go `SetOriginalPartitionIDs`.
    pub fn set_original_partition_ids(&mut self) {
        let definitions = self.definitions.snapshot();
        let ids = definitions
            .iter()
            .map(|definition| definition.id)
            .collect::<Vec<_>>();
        self.original_partition_ids_order =
            GoSharedSlice::from_vec_with_capacity(ids, definitions.len());
    }

    /// Go `IDsInDDLToIgnore`, including the returned slice header's nil,
    /// capacity, and backing-alias behavior.
    #[must_use]
    pub fn ids_in_ddl_to_ignore(&self) -> GoSharedSlice<i64> {
        let ids_from_definitions = |definitions: &GoSharedSlice<PartitionDefinition>,
                                    capacity: usize| {
            let mut ids = GoSharedSlice::from_vec_with_capacity(Vec::new(), capacity);
            for definition in definitions.snapshot() {
                ids.push_go(definition.id, 8, GoSliceElementLayout::NoPointers);
            }
            ids
        };
        match self.ddl_action {
            ActionType::ACTION_TRUNCATE_TABLE_PARTITION => match self.ddl_state {
                SchemaState::WRITE_ONLY => self.new_partition_ids.clone(),
                SchemaState::DELETE_ONLY | SchemaState::DELETE_REORGANIZATION
                    if !self.dropping_definitions.is_empty() =>
                {
                    ids_from_definitions(
                        &self.dropping_definitions,
                        self.dropping_definitions.len(),
                    )
                }
                _ => GoSharedSlice::default(),
            },
            ActionType::ACTION_DROP_TABLE_PARTITION if !self.dropping_definitions.is_empty() => {
                ids_from_definitions(&self.dropping_definitions, self.dropping_definitions.len())
            }
            ActionType::ACTION_ADD_TABLE_PARTITION if !self.adding_definitions.is_empty() => {
                ids_from_definitions(&self.adding_definitions, self.dropping_definitions.len())
            }
            _ => GoSharedSlice::default(),
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
    fn partition_definition_go_memory_usage() {
        assert_eq!(PartitionDefinition::memory_usage_pointer(None), 0);
        assert_eq!(PartitionDefinition::default().memory_usage(), 48);
        let definition = PartitionDefinition {
            name: CiString::new("Part"),
            less_than: vec!["abc".to_owned()].into(),
            in_values: vec![GoSharedSlice::from(vec!["x".to_owned(), "yz".to_owned()])].into(),
            placement_policy_ref: Some(GoShared::new(PolicyRefInfo {
                id: 1,
                name: CiString::new("Policy"),
            })),
            ..Default::default()
        };
        assert_eq!(definition.memory_usage(), 114);
    }

    #[test]
    fn partition_definition_clone_preserves_source_alias_policy() {
        assert!(std::panic::catch_unwind(|| PartitionDefinition::clone_pointer(None)).is_err());

        let empty_with_spare = GoSharedSlice::<String>::from_vec_with_capacity(Vec::new(), 4);
        let empty_clone = PartitionDefinition {
            less_than: empty_with_spare.clone(),
            ..Default::default()
        }
        .clone_like_go();
        assert!(empty_clone.less_than.is_allocated());
        assert_eq!(empty_clone.less_than.capacity(), 0);
        assert!(!empty_clone.less_than.backing_ptr_eq(&empty_with_spare));

        let less_than = GoSharedSlice::from_vec_with_capacity(
            (0..17).map(|index| index.to_string()).collect(),
            24,
        );
        let transitions = GoSharedSlice::from_vec_with_capacity(
            (0..18)
                .map(|index| StorageClassTransitRule {
                    tier: format!("tier-{index}"),
                    ..Default::default()
                })
                .collect(),
            24,
        );
        let inner = GoSharedSlice::from_vec(vec!["one".to_owned()]);
        let in_values = GoSharedSlice::from_vec_with_capacity(vec![inner], 3);
        let policy = GoShared::new(PolicyRefInfo {
            id: 7,
            name: CiString::new("p"),
        });
        let source = PartitionDefinition {
            less_than: less_than.clone(),
            in_values: in_values.clone(),
            placement_policy_ref: Some(policy.clone()),
            storage_class_transitions: transitions.clone(),
            ..Default::default()
        };
        let structural = source.clone();
        assert!(structural.less_than.backing_ptr_eq(&source.less_than));
        assert!(structural.in_values.backing_ptr_eq(&source.in_values));
        assert!(structural
            .placement_policy_ref
            .as_ref()
            .unwrap()
            .ptr_eq(&policy));
        let cloned = source.clone_like_go();

        assert!(!cloned.less_than.backing_ptr_eq(&less_than));
        assert_eq!(cloned.less_than.capacity(), 18);
        cloned.less_than.set(0, "clone".to_owned());
        assert_eq!(less_than.get(0), "0");
        assert!(!cloned
            .storage_class_transitions
            .backing_ptr_eq(&transitions));
        assert_eq!(cloned.storage_class_transitions.capacity(), 19);
        cloned
            .storage_class_transitions
            .update(0, |rule| rule.tier = "clone".to_owned());
        assert_eq!(transitions.get(0).tier, "tier-0");

        assert!(cloned.in_values.backing_ptr_eq(&in_values));
        let replacement = GoSharedSlice::from_vec(vec!["replacement".to_owned()]);
        cloned.in_values.set(0, replacement.clone());
        assert!(source.in_values.get(0).backing_ptr_eq(&replacement));
        assert!(cloned
            .placement_policy_ref
            .as_ref()
            .unwrap()
            .ptr_eq(&policy));
        cloned.placement_policy_ref.as_ref().unwrap().write().id = 8;
        assert_eq!(source.placement_policy_ref.as_ref().unwrap().read().id, 8);
    }

    #[test]
    fn partition_info_clone_preserves_source_container_policies() {
        assert!(std::panic::catch_unwind(|| PartitionInfo::clone_pointer(None)).is_err());
        let serialized_pointer =
            serde_json::to_value(GoShared::new(PartitionInfo::default())).unwrap();
        assert_eq!(serialized_pointer["definitions"], serde_json::Value::Null);

        let nil = PartitionInfo::default().clone_like_go();
        assert!(!nil.columns.is_allocated());
        assert!(nil.definitions.is_allocated());
        assert!(nil.adding_definitions.is_allocated());
        assert!(nil.dropping_definitions.is_allocated());

        let allocated_empty = GoSharedSlice::<PartitionDefinition>::from_vec(Vec::new());
        let empty_source = PartitionInfo {
            definitions: allocated_empty.clone(),
            ..Default::default()
        };
        let empty_clone = empty_source.clone_like_go();
        assert!(empty_clone.definitions.is_allocated());
        assert!(!empty_clone.definitions.backing_ptr_eq(&allocated_empty));
        assert_eq!(empty_clone.definitions.capacity(), 0);

        let columns = GoSharedSlice::from_vec_with_capacity(
            (0..18)
                .map(|index| CiString::new(&format!("c{index}")))
                .collect(),
            24,
        );
        let definitions = GoSharedSlice::from_vec_with_capacity(
            vec![PartitionDefinition {
                id: 1,
                less_than: vec!["10".to_owned()].into(),
                in_values: vec![GoSharedSlice::from(vec!["one".to_owned()])].into(),
                ..Default::default()
            }],
            4,
        );
        let new_ids = GoSharedSlice::from_vec_with_capacity(vec![10_i64], 3);
        let original_ids = GoSharedSlice::from_vec(vec![1_i64]);
        let states = GoSharedSlice::from_vec(vec![PartitionState {
            id: 1,
            state: SchemaState::PUBLIC,
        }]);
        let ddl_columns = GoSharedSlice::from_vec(vec![CiString::new("a")]);
        let ddl_updates = GoSharedSlice::from_vec(vec![UpdateIndexInfo {
            index_name: "i".to_owned(),
            global: false,
        }]);
        let changed = GoShared::new(BTreeMap::from([(1, true)]));
        let source = PartitionInfo {
            columns: columns.clone(),
            definitions: definitions.clone(),
            adding_definitions: definitions.clone(),
            dropping_definitions: definitions.clone(),
            new_partition_ids: new_ids.clone(),
            original_partition_ids_order: original_ids.clone(),
            states: states.clone(),
            ddl_columns: ddl_columns.clone(),
            ddl_update_indexes: ddl_updates.clone(),
            ddl_changed_index: Some(changed.clone()),
            ..Default::default()
        };
        let structural = source.clone();
        assert!(structural.columns.backing_ptr_eq(&source.columns));
        assert!(structural.definitions.backing_ptr_eq(&source.definitions));
        assert!(structural.new_partition_ids.backing_ptr_eq(&new_ids));
        let cloned = source.clone_like_go();

        assert!(!cloned.columns.backing_ptr_eq(&columns));
        assert_eq!(cloned.columns.capacity(), 19);
        for cloned_definitions in [
            &cloned.definitions,
            &cloned.adding_definitions,
            &cloned.dropping_definitions,
        ] {
            assert!(!cloned_definitions.backing_ptr_eq(&definitions));
            assert_eq!(cloned_definitions.capacity(), definitions.len());
        }
        cloned.definitions.update(0, |definition| definition.id = 2);
        assert_eq!(definitions.get(0).id, 1);
        cloned
            .definitions
            .update(0, |definition| definition.less_than.set(0, "20".to_owned()));
        assert_eq!(definitions.get(0).less_than.get(0), "10");
        let replacement = GoSharedSlice::from(vec!["replacement".to_owned()]);
        cloned
            .definitions
            .get(0)
            .in_values
            .set(0, replacement.clone());
        assert!(definitions
            .get(0)
            .in_values
            .get(0)
            .backing_ptr_eq(&replacement));

        assert!(cloned.new_partition_ids.backing_ptr_eq(&new_ids));
        assert!(cloned
            .original_partition_ids_order
            .backing_ptr_eq(&original_ids));
        assert!(cloned.states.backing_ptr_eq(&states));
        assert!(cloned.ddl_columns.backing_ptr_eq(&ddl_columns));
        assert!(cloned.ddl_update_indexes.backing_ptr_eq(&ddl_updates));
        assert!(cloned.ddl_changed_index.as_ref().unwrap().ptr_eq(&changed));
        cloned.new_partition_ids.set(0, 11);
        cloned
            .states
            .update(0, |state| state.state = SchemaState::WRITE_ONLY);
        cloned
            .ddl_update_indexes
            .update(0, |index| index.global = true);
        cloned
            .ddl_changed_index
            .as_ref()
            .unwrap()
            .write()
            .insert(2, false);
        assert_eq!(source.new_partition_ids.get(0), 11);
        assert_eq!(source.states.get(0).state, SchemaState::WRITE_ONLY);
        assert!(source.ddl_update_indexes.get(0).global);
        assert_eq!(
            source.ddl_changed_index.as_ref().unwrap().read().get(&2),
            Some(&false)
        );
    }

    #[test]
    fn name_and_id_lookup() {
        let pi = PartitionInfo {
            definitions: vec![def(10, "p0"), def(20, "P1")].into(),
            ..Default::default()
        };
        assert_eq!(pi.get_name_by_id(20), "P1");
        assert_eq!(pi.get_name_by_id(99), "");
        // Case-insensitive name lookup.
        assert_eq!(pi.find_partition_definition_by_name("p1"), 1);
        assert_eq!(pi.find_partition_definition_by_name("nope"), -1);
        assert_eq!(pi.get_partition_id_by_name("P0"), 10);
        assert_eq!(pi.get_partition_id_by_name("x"), -1);

        let simple_case = PartitionInfo {
            definitions: vec![def(30, "i")].into(),
            ..Default::default()
        };
        assert_eq!(simple_case.find_partition_definition_by_name("\u{130}"), 0);
        assert_eq!(simple_case.get_partition_id_by_name("\u{130}"), 30);
    }

    #[test]
    fn state_get_set_gc() {
        let mut pi = PartitionInfo {
            definitions: vec![def(10, "p0"), def(20, "p1")].into(),
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
            definitions: vec![def(1, "p0"), def(2, "p1"), def(3, "p2")].into(),
            dropping_definitions: vec![def(1, "p0"), def(2, "p1")].into(),
            ddl_action: ActionType::ACTION_DROP_TABLE_PARTITION,
            ddl_state: SchemaState::WRITE_ONLY,
            ..Default::default()
        };
        assert_eq!(range.get_default_list_partition(), -1);
        assert_eq!(range.get_overlapping_dropping_partition_idx(-1), -1);
        assert!(std::panic::catch_unwind(|| range.is_dropping(-1)).is_err());
        assert!(range.is_dropping(0));
        assert_eq!(range.get_overlapping_dropping_partition_idx(0), 2);
        assert_eq!(range.get_overlapping_dropping_partition_idx(2), 2);
        let (index, error) = range.replace_with_overlapping_partition_idx(0, Some("dropped"));
        assert_eq!(index, 2);
        assert!(error.is_none());
        range.dropping_definitions = vec![def(1, "p0"), def(2, "p1"), def(3, "p2")].into();
        let (index, error) = range.replace_with_overlapping_partition_idx(0, Some("dropped"));
        assert_eq!(index, -1);
        assert_eq!(error, Some("dropped"));

        let list = PartitionInfo {
            partition_type: PartitionType::LIST,
            definitions: vec![
                PartitionDefinition {
                    id: 1,
                    name: CiString::new("p0"),
                    in_values: vec![GoSharedSlice::from(vec!["1".to_owned()])].into(),
                    ..Default::default()
                },
                PartitionDefinition {
                    id: 2,
                    name: CiString::new("pd"),
                    in_values: vec![GoSharedSlice::from(vec!["DEFAULT".to_owned()])].into(),
                    ..Default::default()
                },
            ]
            .into(),
            dropping_definitions: vec![def(1, "p0")].into(),
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
            definitions: vec![def(1, "p0"), def(2, "p1")].into(),
            adding_definitions: vec![def(3, "p2")].into(),
            dropping_definitions: vec![def(1, "p0")].into(),
            new_partition_ids: vec![10, 20].into(),
            ..Default::default()
        };
        partition.set_original_partition_ids();
        assert_eq!(
            partition.original_partition_ids_order.snapshot(),
            vec![1, 2]
        );

        partition.ddl_action = ActionType::ACTION_TRUNCATE_TABLE_PARTITION;
        partition.ddl_state = SchemaState::WRITE_ONLY;
        let write_only = partition.ids_in_ddl_to_ignore();
        assert!(write_only.backing_ptr_eq(&partition.new_partition_ids));
        assert_eq!(write_only.snapshot(), vec![10, 20]);
        partition.ddl_state = SchemaState::DELETE_ONLY;
        let truncating = partition.ids_in_ddl_to_ignore();
        assert_eq!(truncating.snapshot(), vec![1]);
        assert_eq!(truncating.capacity(), 1);
        partition.ddl_action = ActionType::ACTION_DROP_TABLE_PARTITION;
        let dropping = partition.ids_in_ddl_to_ignore();
        assert_eq!(dropping.snapshot(), vec![1]);
        assert_eq!(dropping.capacity(), 1);
        partition.ddl_action = ActionType::ACTION_ADD_TABLE_PARTITION;
        partition.adding_definitions = vec![def(3, "p2"), def(4, "p3"), def(5, "p4")].into();
        let adding = partition.ids_in_ddl_to_ignore();
        assert_eq!(adding.snapshot(), vec![3, 4, 5]);
        // The source seeds this result with DroppingDefinitions' length (1),
        // then grows the noscan []int64 header while appending three IDs.
        assert_eq!(adding.capacity(), 4);
        partition.ddl_action = ActionType::ACTION_NONE;
        let none = partition.ids_in_ddl_to_ignore();
        assert!(!none.is_allocated());
    }

    // Field order, tag names, and nil/allocated slice bytes compared against
    // Go's json.Marshal.
    #[test]
    fn partition_info_json_matches_go() {
        let go = r#"{"type":1,"expr":"a","columns":[{"O":"c","L":"c"}],"enable":true,"is_empty_columns":false,"definitions":[{"id":1,"name":{"O":"p0","L":"p0"},"less_than":["10"],"in_values":null,"policy_ref_info":null}],"adding_definitions":null,"dropping_definitions":null,"states":[{"id":1,"state":5}],"num":2,"ddl_state":0,"ddl_changed_index":{"10":false,"2":true}}"#;
        let pi: PartitionInfo = serde_json::from_str(go).unwrap();
        assert_eq!(pi.partition_type, PartitionType::RANGE);
        assert_eq!(pi.definitions.len(), 1);
        assert_eq!(pi.definitions.get(0).name.original(), "p0");
        assert!(pi.definitions.get(0).in_values.is_empty());
        assert!(pi.adding_definitions.is_empty());
        assert_eq!(pi.states.get(0).state, SchemaState::PUBLIC);
        assert_eq!(pi.num, 2);
        assert_eq!(
            pi.ddl_changed_index.as_ref().unwrap().read().get(&10),
            Some(&false)
        );

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
        assert!(!pi.ddl_columns.is_allocated());
        assert!(pi.ddl_changed_index.is_none());

        let d = PartitionDefinition {
            storage_class_tier: "STANDARD".into(),
            ..Default::default()
        };
        assert_eq!(d.storage_class_string(), "STANDARD");
    }
}
