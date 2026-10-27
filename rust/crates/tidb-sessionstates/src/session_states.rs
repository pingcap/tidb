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

//! Go `pkg/sessionctx/sessionstates/session_states.go`.
//!
//! Every field decodes as Go's `encoding/json` does: a JSON `null` leaves
//! the zero value, and unknown fields are ignored.

use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;
use tidb_datatype::{Datum, FieldType};
use tidb_model::index::IndexInfo;
use tidb_util::context::SqlWarn;

/// Go `SessionStateType`: the type of a session state the connection
/// layer (not the session) owns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionStateType {
    /// Go `StatePrepareStmt`: prepared statements.
    PrepareStmt,
    /// Go `StateBinding`: session SQL bindings.
    Binding,
}

/// Go `PreparedStmtInfo`: what restores a prepared statement. Plans are
/// rebuilt at the destination.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedStmtInfo {
    /// The SQL PREPARE name; empty for a binary protocol statement.
    #[serde(default, deserialize_with = "null_default", skip_serializing_if = "String::is_empty")]
    pub name: String,
    /// The statement text.
    #[serde(rename = "text", default, deserialize_with = "null_default")]
    pub stmt_text: String,
    /// The database selected when it was prepared.
    #[serde(
        rename = "db",
        default,
        deserialize_with = "null_default",
        skip_serializing_if = "String::is_empty"
    )]
    pub stmt_db: String,
    /// The remembered binary parameter types, Go `[]byte` (base64 in JSON).
    #[serde(
        rename = "types",
        default,
        serialize_with = "serialize_go_bytes",
        deserialize_with = "deserialize_go_bytes",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub param_types: Vec<u8>,
}

/// Go `QueryInfo`: diagnostics of the last query, `@@tidb_last_query_info`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct QueryInfo {
    /// The transaction scope.
    #[serde(default, deserialize_with = "null_default")]
    pub txn_scope: String,
    /// The transaction's start timestamp.
    #[serde(default, deserialize_with = "null_default")]
    pub start_ts: u64,
    /// The transaction's for-update timestamp.
    #[serde(default, deserialize_with = "null_default")]
    pub for_update_ts: u64,
    /// The statement's read and write RU.
    #[serde(default, deserialize_with = "null_default", serialize_with = "serialize_go_float")]
    pub ru_consumption: f64,
    /// The statement's error, `err.Error()`.
    #[serde(
        rename = "error",
        default,
        deserialize_with = "null_default",
        skip_serializing_if = "String::is_empty"
    )]
    pub err_msg: String,
}

/// Go `LastDDLInfo`: the last DDL job a session waited for,
/// `@@tidb_last_ddl_info`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LastDDLInfo {
    /// The DDL statement.
    #[serde(default, deserialize_with = "null_default")]
    pub query: String,
    /// The job's sequence number.
    #[serde(default, deserialize_with = "null_default")]
    pub seq_num: u64,
}

/// Go `SessionStates.HypoIndexes`: hypothetical indexes by schema, table and
/// index name.
pub type HypoIndexMap = BTreeMap<String, BTreeMap<String, BTreeMap<String, IndexInfo>>>;

/// Go `SessionStates.HypoTiFlashReplicas`, a `map[string]map[string]struct{}`.
pub type HypoTiFlashReplicaMap = BTreeMap<String, BTreeMap<String, Empty>>;

/// Go's `struct{}`, which JSON spells `{}`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Empty {}

/// Go `*types.Datum` in JSON: `Datum.MarshalJSON` / `UnmarshalJSON`.
#[derive(Clone, Debug, PartialEq)]
pub struct JsonDatum(pub Datum);

impl Serialize for JsonDatum {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let bytes = self.0.marshal_json().map_err(serde::ser::Error::custom)?;
        let text = String::from_utf8(bytes).map_err(serde::ser::Error::custom)?;
        RawValue::from_string(text)
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for JsonDatum {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        Datum::unmarshal_json(raw.get().as_bytes())
            .map(JsonDatum)
            .map_err(serde::de::Error::custom)
    }
}

/// Go `SessionStates`: the session state that is migrated.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SessionStates {
    /// User variable values.
    #[serde(
        rename = "user-var-values",
        default,
        deserialize_with = "null_default",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub user_vars: BTreeMap<String, JsonDatum>,
    /// User variable types.
    #[serde(
        rename = "user-var-types",
        default,
        deserialize_with = "null_default",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub user_var_types: BTreeMap<String, FieldType>,
    /// Session system variables.
    #[serde(
        rename = "sys-vars",
        default,
        deserialize_with = "null_default",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub system_vars: BTreeMap<String, String>,
    /// Prepared statements, by statement id.
    #[serde(
        rename = "prepared-stmts",
        default,
        deserialize_with = "null_default",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub prepared_stmts: BTreeMap<u32, PreparedStmtInfo>,
    /// The last allocated prepared statement id.
    #[serde(
        rename = "prepared-stmt-id",
        default,
        deserialize_with = "null_default",
        skip_serializing_if = "is_zero_u32"
    )]
    pub prepared_stmt_id: u32,
    /// The server status flags.
    #[serde(rename = "status", default, deserialize_with = "null_default", skip_serializing_if = "is_zero_u32")]
    pub status: u32,
    /// The current database.
    #[serde(
        rename = "current-db",
        default,
        deserialize_with = "null_default",
        skip_serializing_if = "String::is_empty"
    )]
    pub current_db: String,
    /// The last transaction's info, `@@tidb_last_txn_info`.
    #[serde(
        rename = "txn-info",
        default,
        deserialize_with = "null_default",
        skip_serializing_if = "String::is_empty"
    )]
    pub last_txn_info: String,
    /// The last query's info, `@@tidb_last_query_info`.
    #[serde(rename = "query-info", default, skip_serializing_if = "Option::is_none")]
    pub last_query_info: Option<QueryInfo>,
    /// The last DDL's info, `@@tidb_last_ddl_info`.
    #[serde(rename = "ddl-info", default, skip_serializing_if = "Option::is_none")]
    pub last_ddl_info: Option<LastDDLInfo>,
    /// `FOUND_ROWS()`.
    #[serde(
        rename = "found-rows",
        default,
        deserialize_with = "null_default",
        skip_serializing_if = "is_zero_u64"
    )]
    pub last_found_rows: u64,
    /// `@@last_plan_from_cache`.
    #[serde(
        rename = "in-plan-cache",
        default,
        deserialize_with = "null_default",
        skip_serializing_if = "is_false"
    )]
    pub found_in_plan_cache: bool,
    /// `@@last_plan_from_binding`.
    #[serde(
        rename = "in-binding",
        default,
        deserialize_with = "null_default",
        skip_serializing_if = "is_false"
    )]
    pub found_in_binding: bool,
    /// `LASTVAL()` of each sequence, by sequence id.
    #[serde(
        rename = "seq-values",
        default,
        deserialize_with = "null_default",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub sequence_latest_values: BTreeMap<i64, i64>,
    /// `ROW_COUNT()`.
    #[serde(
        rename = "affected-rows",
        default,
        deserialize_with = "null_default",
        skip_serializing_if = "is_zero_i64"
    )]
    pub last_affected_rows: i64,
    /// `LAST_INSERT_ID()`.
    #[serde(
        rename = "last-insert-id",
        default,
        deserialize_with = "null_default",
        skip_serializing_if = "is_zero_u64"
    )]
    pub last_insert_id: u64,
    /// The last statement's warnings.
    #[serde(
        rename = "warnings",
        default,
        deserialize_with = "null_default",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub warnings: Vec<SqlWarn>,
    /// The session SQL bindings, as their own JSON text.
    #[serde(
        rename = "bindings",
        default,
        deserialize_with = "null_default",
        skip_serializing_if = "String::is_empty"
    )]
    pub bindings: String,
    /// The resource group.
    #[serde(
        rename = "rs-group",
        default,
        deserialize_with = "null_default",
        skip_serializing_if = "String::is_empty"
    )]
    pub resource_group_name: String,
    /// Hypothetical indexes.
    #[serde(
        rename = "hypo-indexes",
        default,
        deserialize_with = "null_default",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub hypo_indexes: HypoIndexMap,
    /// Hypothetical TiFlash replicas.
    #[serde(
        rename = "hypo-tiflash-replicas",
        default,
        deserialize_with = "null_default",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub hypo_tiflash_replicas: HypoTiFlashReplicaMap,
}

/// Go `executeSetSessionStates`' `json.NewDecoder(...).Decode(&sessionStates)`:
/// the first JSON value of `text`; a JSON `null` decodes to no state.
///
/// # Errors
///
/// When the text is not JSON or a field has another type than Go's.
pub fn decode_session_states(text: &str) -> Result<SessionStates, serde_json::Error> {
    let mut decoder = serde_json::Deserializer::from_str(text);
    Option::<SessionStates>::deserialize(&mut decoder).map(Option::unwrap_or_default)
}

/// Go `encoding/json` decoding into a non-pointer field: `null` leaves the
/// zero value.
fn null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Option::<T>::deserialize(deserializer).map(Option::unwrap_or_default)
}

/// Go `encoding/json`'s `[]byte`: standard base64.
pub(crate) fn serialize_go_bytes<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
    use base64::Engine as _;
    serializer.serialize_str(&base64::engine::general_purpose::STANDARD.encode(bytes))
}

/// Go `encoding/json`'s `[]byte`: standard base64, `null` for none.
pub(crate) fn deserialize_go_bytes<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
    use base64::Engine as _;
    let Some(text) = Option::<String>::deserialize(deserializer)? else {
        return Ok(Vec::new());
    };
    base64::engine::general_purpose::STANDARD
        .decode(text)
        .map_err(serde::de::Error::custom)
}

/// Go `encoding/json`'s `float64`.
fn serialize_go_float<S: Serializer>(value: &f64, serializer: S) -> Result<S::Ok, S::Error> {
    RawValue::from_string(tidb_util::stringutil::go_json_float(*value))
        .map_err(serde::ser::Error::custom)?
        .serialize(serializer)
}

fn is_zero_u32(value: &u32) -> bool {
    *value == 0
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

fn is_zero_i64(value: &i64) -> bool {
    *value == 0
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_null_and_absent_fields_as_go_zero_values() {
        assert!(decode_session_states("null").unwrap().system_vars.is_empty());
        let states = decode_session_states(
            r#"{"status": null, "current-db": "d", "prepared-stmts": {"3": {"text": "select ?", "db": null, "types": "CAA="}}, "unknown": 1}"#,
        )
        .unwrap();
        assert_eq!(states.status, 0);
        assert_eq!(states.current_db, "d");
        assert_eq!(
            states.prepared_stmts[&3],
            PreparedStmtInfo {
                name: String::new(),
                stmt_text: "select ?".to_owned(),
                stmt_db: String::new(),
                param_types: vec![8, 0],
            }
        );
    }

    #[test]
    fn refuses_a_field_of_another_type_than_gos() {
        for text in [r#"{"status": "x"}"#, r#"{"status": -1}"#, r#"{"found-rows": 1.5}"#] {
            assert!(decode_session_states(text).is_err(), "{text}");
        }
    }

    #[test]
    fn encodes_go_omitempty_and_query_info() {
        let states = SessionStates {
            last_query_info: Some(QueryInfo {
                txn_scope: "global".to_owned(),
                start_ts: 7,
                for_update_ts: 7,
                ru_consumption: 0.0,
                err_msg: String::new(),
            }),
            hypo_tiflash_replicas: BTreeMap::from([(
                "test".to_owned(),
                BTreeMap::from([("t".to_owned(), Empty {})]),
            )]),
            ..SessionStates::default()
        };
        assert_eq!(
            serde_json::to_string(&states).unwrap(),
            r#"{"query-info":{"txn_scope":"global","start_ts":7,"for_update_ts":7,"ru_consumption":0},"hypo-tiflash-replicas":{"test":{"t":{}}}}"#
        );
    }
}
