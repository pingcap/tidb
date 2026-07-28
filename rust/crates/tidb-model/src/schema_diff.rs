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
use serde::{Deserialize, Serialize, Serializer};

use crate::action_type::ActionType;

/// Go `AffectedOption`: one extra (schema, table) pair a diff touches beyond
/// its own `SchemaID`/`TableID`, used by DDLs that change several tables.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
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

/// Go `SchemaDiff`: the schema modification at one particular schema version.
///
/// `is_refresh_meta` is Go's `json:"-"` field: it is set in memory by the
/// BR-only `refreshMeta` path and is never stored, so it is skipped by serde
/// and always decodes `false`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
pub struct SchemaDiff {
    /// The schema version this diff produces.
    #[serde(rename = "version", default)]
    pub version: i64,
    /// The DDL action that produced it.
    #[serde(rename = "type", default)]
    pub action_type: ActionType,
    /// The database the action targeted.
    #[serde(rename = "schema_id", default)]
    pub schema_id: i64,
    /// The table the action targeted, `0` for a database-level action.
    #[serde(rename = "table_id", default)]
    pub table_id: i64,
    /// The action list of a multi-schema-change step; empty otherwise.
    #[serde(
        rename = "sub_action_types",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub sub_action_types: Vec<ActionType>,
    /// The table ID before `TRUNCATE TABLE`.
    #[serde(rename = "old_table_id", default)]
    pub old_table_id: i64,
    /// The database ID before `RENAME TABLE`.
    #[serde(rename = "old_schema_id", default)]
    pub old_schema_id: i64,
    /// Whether applying this diff requires rebuilding the whole schema map.
    #[serde(rename = "regenerate_schema_map", default)]
    pub regenerate_schema_map: bool,
    /// Whether the diff was too large to store and the reader must consult the
    /// meta store for the new table definition directly.
    #[serde(rename = "read_table_from_meta", default)]
    pub read_table_from_meta: bool,
    /// Extra tables the same DDL touched.
    #[serde(
        rename = "affected_options",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub affected_options: Vec<AffectedOption>,
    /// In-memory only (Go `json:"-"`): set by BR's `refreshMeta` DDL.
    #[serde(skip)]
    pub is_refresh_meta: bool,
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
        // No `omitempty`: an empty Go slice is nil here and marshals as null.
        if self.affected_options.is_empty() {
            value.serialize_field("affected_options", &Option::<Vec<AffectedOption>>::None)?;
        } else {
            value.serialize_field("affected_options", &self.affected_options)?;
        }
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
        assert!(diff.affected_options.is_empty());
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
        assert_eq!(diff.sub_action_types, vec![ActionType(3), ActionType(4)]);
        assert!(diff.regenerate_schema_map);
        assert!(diff.read_table_from_meta);
        assert_eq!(diff.affected_options.len(), 1);
        assert_eq!(diff.affected_options[0].table_id, 105);
        assert_eq!(
            String::from_utf8(to_go_json(&diff).unwrap()).unwrap(),
            stored
        );
    }

    #[test]
    fn a_null_slice_decodes_as_empty_rather_than_failing() {
        let diff: SchemaDiff = serde_json::from_str(
            r#"{"version":1,"sub_action_types":null,"affected_options":null}"#,
        )
        .unwrap();
        assert!(diff.sub_action_types.is_empty());
        assert!(diff.affected_options.is_empty());
        assert_eq!(diff.action_type, ActionType::ACTION_NONE);
    }
}
