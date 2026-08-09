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

//! `SchemaDiff` and `AffectedOption` from `pkg/meta/model/job.go`.
//!
//! One diff is the whole record of what one schema version changed, stored
//! under the meta key `Diff:<version>`. A reader that already holds version
//! `v` can reach version `w` by reading `Diff:v+1 .. Diff:w` and replaying
//! them, instead of re-reading the whole catalog.

use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};

use crate::action_type::ActionType;
use crate::go_runtime::GoSharedPointerSlice;
use crate::serde_helpers::{
    go_json_field_matches, ignore_unknown, impl_go_json_deserialize, impl_go_json_merge_object,
    GoValueSlice, NullNoopSeed, OptionValueSliceSeed, SharedPointerSliceSeed,
};

/// Go `AffectedOption`: one extra (schema, table) pair a diff touches beyond
/// its own `SchemaID`/`TableID`, used by DDLs that change several tables.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct AffectedOption {
    /// The affected database ID.
    #[serde(rename = "schema_id", default)]
    pub schema_id: i64,
    /// The affected table ID.
    #[serde(rename = "table_id", default)]
    pub table_id: i64,
    /// The table ID before the change, when the DDL replaces a table.
    #[serde(rename = "old_table_id", default)]
    pub old_table_id: i64,
    /// The database ID before the change, when the DDL moves a table.
    #[serde(rename = "old_schema_id", default)]
    pub old_schema_id: i64,
}

impl_go_json_merge_object!(AffectedOption, destination, map, key, {
    if go_json_field_matches(&key, "schema_id") {
        map.next_value_seed(NullNoopSeed(&mut destination.schema_id))?;
    } else if go_json_field_matches(&key, "table_id") {
        map.next_value_seed(NullNoopSeed(&mut destination.table_id))?;
    } else if go_json_field_matches(&key, "old_table_id") {
        map.next_value_seed(NullNoopSeed(&mut destination.old_table_id))?;
    } else if go_json_field_matches(&key, "old_schema_id") {
        map.next_value_seed(NullNoopSeed(&mut destination.old_schema_id))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(AffectedOption);

/// Go `SchemaDiff`: the schema modification at one particular schema version.
///
/// `is_refresh_meta` is Go's `json:"-"` field: it is set in memory by the
/// BR-only `refreshMeta` path and is never stored, so it is skipped by serde
/// (fresh decodes are `false`, while receiver merges leave it unchanged).
#[derive(Clone, Debug, Default)]
pub struct SchemaDiff {
    /// The schema version this diff produces.
    pub version: i64,
    /// The DDL action that produced it.
    pub action_type: ActionType,
    /// The database the action targeted.
    pub schema_id: i64,
    /// The table the action targeted, `0` for a database-level action.
    pub table_id: i64,
    /// The action list of a multi-schema-change step; empty otherwise.
    pub sub_action_types: GoValueSlice<ActionType>,
    /// The table ID before `TRUNCATE TABLE`.
    pub old_table_id: i64,
    /// The database ID before `RENAME TABLE`.
    pub old_schema_id: i64,
    /// Whether applying this diff requires rebuilding the whole schema map.
    pub regenerate_schema_map: bool,
    /// Whether the diff was too large to store and the reader must consult the
    /// meta store for the new table definition directly.
    pub read_table_from_meta: bool,
    /// Extra tables the same DDL touched.
    pub affected_options: GoSharedPointerSlice<AffectedOption>,
    /// In-memory only (Go `json:"-"`): set by BR's `refreshMeta` DDL.
    pub is_refresh_meta: bool,
}

impl PartialEq for SchemaDiff {
    fn eq(&self, other: &Self) -> bool {
        self.version == other.version
            && self.action_type == other.action_type
            && self.schema_id == other.schema_id
            && self.table_id == other.table_id
            && self.sub_action_types == other.sub_action_types
            && self.old_table_id == other.old_table_id
            && self.old_schema_id == other.old_schema_id
            && self.regenerate_schema_map == other.regenerate_schema_map
            && self.read_table_from_meta == other.read_table_from_meta
            && self.is_refresh_meta == other.is_refresh_meta
            && pointer_slice_values_eq(&self.affected_options, &other.affected_options)
    }
}

impl Eq for SchemaDiff {}

fn pointer_slice_values_eq(
    left: &GoSharedPointerSlice<AffectedOption>,
    right: &GoSharedPointerSlice<AffectedOption>,
) -> bool {
    left.is_allocated() == right.is_allocated()
        && left.len() == right.len()
        && left
            .iter_handles()
            .zip(right.iter_handles())
            .all(|(left, right)| match (left, right) {
                (None, None) => true,
                (Some(left), Some(right)) => *left.read() == *right.read(),
                _ => false,
            })
}

impl_go_json_merge_object!(SchemaDiff, destination, map, key, {
    if go_json_field_matches(&key, "version") {
        map.next_value_seed(NullNoopSeed(&mut destination.version))?;
    } else if go_json_field_matches(&key, "type") {
        map.next_value_seed(NullNoopSeed(&mut destination.action_type))?;
    } else if go_json_field_matches(&key, "schema_id") {
        map.next_value_seed(NullNoopSeed(&mut destination.schema_id))?;
    } else if go_json_field_matches(&key, "table_id") {
        map.next_value_seed(NullNoopSeed(&mut destination.table_id))?;
    } else if go_json_field_matches(&key, "sub_action_types") {
        map.next_value_seed(OptionValueSliceSeed(destination.sub_action_types.raw_mut()))?;
    } else if go_json_field_matches(&key, "old_table_id") {
        map.next_value_seed(NullNoopSeed(&mut destination.old_table_id))?;
    } else if go_json_field_matches(&key, "old_schema_id") {
        map.next_value_seed(NullNoopSeed(&mut destination.old_schema_id))?;
    } else if go_json_field_matches(&key, "regenerate_schema_map") {
        map.next_value_seed(NullNoopSeed(&mut destination.regenerate_schema_map))?;
    } else if go_json_field_matches(&key, "read_table_from_meta") {
        map.next_value_seed(NullNoopSeed(&mut destination.read_table_from_meta))?;
    } else if go_json_field_matches(&key, "affected_options") {
        map.next_value_seed(SharedPointerSliceSeed(&mut destination.affected_options))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(SchemaDiff);

impl SchemaDiff {
    /// Iterates affected options at the same dereference boundary as Go
    /// consumers. A null element panics instead of being silently skipped.
    pub fn affected_options_iter(&self) -> impl Iterator<Item = crate::GoShared<AffectedOption>> {
        self.affected_options.iter_deref()
    }
}

impl Serialize for SchemaDiff {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // Field order is Go's declaration order, which `encoding/json`
        // preserves; the two `omitempty` fields disappear when zero.
        let omitted =
            usize::from(self.sub_action_types.is_empty()) + usize::from(!self.read_table_from_meta);
        let mut value = serializer.serialize_struct("SchemaDiff", 10 - omitted)?;
        value.serialize_field("version", &self.version)?;
        value.serialize_field("type", &self.action_type)?;
        value.serialize_field("schema_id", &self.schema_id)?;
        value.serialize_field("table_id", &self.table_id)?;
        if self.sub_action_types.is_empty() {
            value.skip_field("sub_action_types")?;
        } else {
            value.serialize_field("sub_action_types", &self.sub_action_types)?;
        }
        value.serialize_field("old_table_id", &self.old_table_id)?;
        value.serialize_field("old_schema_id", &self.old_schema_id)?;
        value.serialize_field("regenerate_schema_map", &self.regenerate_schema_map)?;
        if self.read_table_from_meta {
            value.serialize_field("read_table_from_meta", &self.read_table_from_meta)?;
        } else {
            value.skip_field("read_table_from_meta")?;
        }
        // No `omitempty`: preserve nil (`null`), allocated empty (`[]`), and
        // nil elements in Go's []*AffectedOption independently.
        value.serialize_field("affected_options", &self.affected_options)?;
        value.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde_helpers::to_go_json;

    /// Bytes in the exact shape TiDB stores for `CREATE TABLE`.
    const STORED_CREATE_TABLE: &str = r#"{"version":53,"type":3,"schema_id":2,"table_id":104,"old_table_id":0,"old_schema_id":0,"regenerate_schema_map":false,"affected_options":null}"#;

    #[test]
    fn a_stored_create_table_diff_round_trips_byte_for_byte() {
        let diff: SchemaDiff = serde_json::from_str(STORED_CREATE_TABLE).unwrap();
        assert_eq!(diff.version, 53);
        assert_eq!(diff.action_type, ActionType::ACTION_CREATE_TABLE);
        assert_eq!(diff.schema_id, 2);
        assert_eq!(diff.table_id, 104);
        assert!(!diff.affected_options.is_allocated());
        assert!(!diff.is_refresh_meta);
        assert_eq!(
            String::from_utf8(to_go_json(&diff).unwrap()).unwrap(),
            STORED_CREATE_TABLE
        );
    }

    #[test]
    fn omitempty_fields_appear_only_when_set() {
        let stored = r#"{"version":9,"type":61,"schema_id":2,"table_id":0,"sub_action_types":[3,4],"old_table_id":7,"old_schema_id":1,"regenerate_schema_map":true,"read_table_from_meta":true,"affected_options":[{"schema_id":2,"table_id":105,"old_table_id":0,"old_schema_id":0}]}"#;
        let diff: SchemaDiff = serde_json::from_str(stored).unwrap();
        assert_eq!(
            diff.sub_action_types.iter().copied().collect::<Vec<_>>(),
            vec![ActionType(3), ActionType(4)]
        );
        assert!(diff.regenerate_schema_map);
        assert!(diff.read_table_from_meta);
        assert_eq!(diff.affected_options.len(), 1);
        assert_eq!(diff.affected_options.get(0).unwrap().read().table_id, 105);
        assert_eq!(
            String::from_utf8(to_go_json(&diff).unwrap()).unwrap(),
            stored
        );
    }

    #[test]
    fn null_affected_options_decode_as_nil_none() {
        let diff: SchemaDiff = serde_json::from_str(
            r#"{"version":1,"sub_action_types":null,"affected_options":null}"#,
        )
        .unwrap();
        assert!(diff.sub_action_types.is_empty());
        assert!(!diff.sub_action_types.is_allocated());
        assert!(!diff.affected_options.is_allocated());
        assert_eq!(diff.action_type, ActionType::ACTION_NONE);
    }

    #[test]
    fn sub_actions_preserve_runtime_nil_empty_and_null_elements() {
        let nil = SchemaDiff::default();
        let empty: SchemaDiff = serde_json::from_str(r#"{"sub_action_types":[]}"#).unwrap();
        let nullable: SchemaDiff =
            serde_json::from_str(r#"{"sub_action_types":[3,null,4]}"#).unwrap();
        assert!(!nil.sub_action_types.is_allocated());
        assert!(empty.sub_action_types.is_allocated());
        assert_eq!(nullable.sub_action_types[0], ActionType(3));
        assert_eq!(nullable.sub_action_types[1], ActionType::ACTION_NONE);
        assert_eq!(nullable.sub_action_types[2], ActionType(4));
        // omitempty suppresses nil and allocated-empty equally on the wire.
        assert_eq!(to_go_json(&nil).unwrap(), to_go_json(&empty).unwrap());
    }

    #[test]
    fn schema_diff_uses_go_fold_duplicate_null_and_partial_merge_rules() {
        use crate::serde_helpers::GoJsonMerge;

        let decoded: SchemaDiff = serde_json::from_str(
            r#"{"VERSION":7,"version":8,"version":null,"TYPE":3,"type":null,"unknown":1}"#,
        )
        .unwrap();
        assert_eq!(decoded.version, 8);
        assert_eq!(decoded.action_type, ActionType(3));

        let mut diff = SchemaDiff {
            version: 5,
            is_refresh_meta: true,
            affected_options: GoSharedPointerSlice::from_nullable(vec![Some(AffectedOption {
                schema_id: 1,
                table_id: 2,
                ..Default::default()
            })]),
            ..Default::default()
        };
        let mut decoder = serde_json::Deserializer::from_str(
            r#"{"version":"bad","affected_options":[{"schema_id":"bad","table_id":9},null],"old_schema_id":4}"#,
        );
        assert!(diff.go_json_merge(&mut decoder).is_err());
        assert_eq!(diff.version, 5);
        assert!(diff.is_refresh_meta);
        assert_eq!(diff.affected_options.len(), 2);
        assert_eq!(diff.affected_options.get(0).unwrap().read().schema_id, 1);
        assert_eq!(diff.affected_options.get(0).unwrap().read().table_id, 9);
        assert!(diff.affected_options.get(1).is_none());
        assert_eq!(diff.old_schema_id, 4);
    }

    #[test]
    fn affected_options_preserve_nil_empty_and_nil_elements() {
        let nil = serde_json::to_value(SchemaDiff::default()).unwrap();
        assert!(nil["affected_options"].is_null());

        let empty = serde_json::to_value(SchemaDiff {
            affected_options: GoSharedPointerSlice::from_nullable(Vec::new()),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(empty["affected_options"], serde_json::json!([]));

        let nullable = SchemaDiff {
            affected_options: GoSharedPointerSlice::from_nullable(vec![
                None,
                Some(AffectedOption {
                    schema_id: i64::MIN,
                    table_id: i64::MAX,
                    ..Default::default()
                }),
            ]),
            ..Default::default()
        };
        let encoded = serde_json::to_value(&nullable).unwrap();
        assert_eq!(encoded["affected_options"][0], serde_json::Value::Null);
        assert_eq!(encoded["affected_options"][1]["schema_id"], i64::MIN);
        assert_eq!(encoded["affected_options"][1]["table_id"], i64::MAX);
        let encoded = serde_json::to_vec(&encoded).unwrap();
        assert_eq!(
            serde_json::from_slice::<SchemaDiff>(&encoded).unwrap(),
            nullable
        );
    }

    #[test]
    #[should_panic(expected = "nil pointer in Go slice")]
    fn affected_options_iterator_panics_at_go_dereference_boundary() {
        let diff = SchemaDiff {
            affected_options: GoSharedPointerSlice::from_nullable(vec![None]),
            ..Default::default()
        };
        let _ = diff.affected_options_iter().next();
    }
}
