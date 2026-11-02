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

//! Go `pkg/meta/model/job_args.go`'s RECOVER TABLE / RECOVER SCHEMA
//! arguments: `AutoIDGroup`, `RecoverTableInfo`, `RecoverSchemaInfo` and
//! `RecoverArgs`.

use super::*;

/// Go `AutoIDGroup`: a table's three allocator values. Untagged, so its JSON
/// keys are the Go field names.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct AutoIdGroup {
    /// `_tidb_rowid` allocator value.
    #[serde(rename = "RowID", default)]
    pub row_id: i64,
    /// Separate `AUTO_INCREMENT` allocator value.
    #[serde(rename = "IncrementID", default)]
    pub increment_id: i64,
    /// `AUTO_RANDOM` allocator value.
    #[serde(rename = "RandomID", default)]
    pub random_id: i64,
}

/// Go `RecoverTableInfo`: what RECOVER / FLASHBACK TABLE brings back.
/// Untagged, so its JSON keys are the Go field names.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RecoverTableInfo {
    /// The schema the table is recovered into.
    #[serde(rename = "SchemaID", default)]
    pub schema_id: i64,
    /// The table as it was before the drop.
    #[serde(rename = "TableInfo", default)]
    pub table_info: Option<GoShared<crate::TableInfo>>,
    /// The DROP / TRUNCATE job.
    #[serde(rename = "DropJobID", default)]
    pub drop_job_id: i64,
    /// The snapshot the table is read at (Go `GetRecoverSnapshotTS`).
    #[serde(rename = "SnapshotTS", default)]
    pub snapshot_ts: u64,
    /// The allocators at that snapshot.
    #[serde(rename = "AutoIDs", default)]
    pub auto_ids: AutoIdGroup,
    /// The schema name of the drop job.
    #[serde(rename = "OldSchemaName", default)]
    pub old_schema_name: String,
    /// The table's name before FLASHBACK TABLE ... TO renames it.
    #[serde(rename = "OldTableName", default)]
    pub old_table_name: String,
}

/// Go `RecoverSchemaInfo`, whose `*DBInfo` is embedded: `encoding/json`
/// writes the database's own keys first, and none of them when it is nil.
#[derive(Clone, Debug, Default)]
pub struct RecoverSchemaInfo {
    /// The embedded `*DBInfo`.
    pub db_info: Option<GoShared<crate::db::DBInfo>>,
    /// Go `RecoverTableInfos`; `None` is a nil slice.
    pub recover_table_infos: Option<Vec<GoShared<RecoverTableInfo>>>,
    /// Go `LoadTablesOnExecute`: the owner, not the submitter, lists the
    /// tables.
    pub load_tables_on_execute: bool,
    /// The DROP DATABASE job.
    pub drop_job_id: i64,
    /// The snapshot the schema is read at.
    pub snapshot_ts: u64,
    /// The schema's name at the drop.
    pub old_schema_name: CiString,
}

const RECOVER_SCHEMA_INFO_KEYS: [&str; 5] = [
    "RecoverTableInfos",
    "LoadTablesOnExecute",
    "DropJobID",
    "SnapshotTS",
    "OldSchemaName",
];

/// [`RecoverSchemaInfo`]'s own fields, in Go's declaration order.
#[derive(Serialize)]
struct RecoverSchemaInfoFields<'a> {
    #[serde(rename = "RecoverTableInfos")]
    recover_table_infos: &'a Option<Vec<GoShared<RecoverTableInfo>>>,
    #[serde(rename = "LoadTablesOnExecute")]
    load_tables_on_execute: bool,
    #[serde(rename = "DropJobID")]
    drop_job_id: i64,
    #[serde(rename = "SnapshotTS")]
    snapshot_ts: u64,
    #[serde(rename = "OldSchemaName")]
    old_schema_name: &'a CiString,
}

impl Serialize for RecoverSchemaInfo {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::Error as _;

        let own = serde_json::to_string(&RecoverSchemaInfoFields {
            recover_table_infos: &self.recover_table_infos,
            load_tables_on_execute: self.load_tables_on_execute,
            drop_job_id: self.drop_job_id,
            snapshot_ts: self.snapshot_ts,
            old_schema_name: &self.old_schema_name,
        })
        .map_err(S::Error::custom)?;
        // Go writes the embedded struct's keys first, in its own order.
        let text = match &self.db_info {
            Some(db_info) => {
                let embedded = serde_json::to_string(&*db_info.read()).map_err(S::Error::custom)?;
                let embedded = &embedded[1..embedded.len() - 1];
                if embedded.is_empty() {
                    own
                } else {
                    format!("{{{embedded},{}", &own[1..])
                }
            }
            None => own,
        };
        RawValue::from_string(text)
            .map_err(S::Error::custom)?
            .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RecoverSchemaInfo {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error as _;

        let mut fields = serde_json::Map::<String, serde_json::Value>::deserialize(deserializer)?;
        let mut take = |key: &str| fields.remove(key).filter(|value| !value.is_null());
        // From text: the metadata keeps raw JSON members, which only a
        // text deserializer can hand over.
        fn decode<T: DeserializeOwned, E: serde::de::Error>(value: serde_json::Value) -> Result<T, E> {
            serde_json::from_str(&value.to_string()).map_err(E::custom)
        }
        let mut info = Self::default();
        if let Some(value) = take("RecoverTableInfos") {
            info.recover_table_infos = Some(decode::<_, D::Error>(value)?);
        }
        if let Some(value) = take("LoadTablesOnExecute") {
            info.load_tables_on_execute = decode::<_, D::Error>(value)?;
        }
        if let Some(value) = take("DropJobID") {
            info.drop_job_id = decode::<_, D::Error>(value)?;
        }
        if let Some(value) = take("SnapshotTS") {
            info.snapshot_ts = decode::<_, D::Error>(value)?;
        }
        if let Some(value) = take("OldSchemaName") {
            info.old_schema_name = decode::<_, D::Error>(value)?;
        }
        // Go allocates the embedded pointer once one of its keys appears.
        fields.retain(|key, _| !RECOVER_SCHEMA_INFO_KEYS.contains(&key.as_str()));
        if !fields.is_empty() {
            info.db_info = Some(decode::<_, D::Error>(serde_json::Value::Object(fields))?);
        }
        Ok(info)
    }
}

/// Go `RecoverArgs`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RecoverArgs {
    /// What is recovered; RECOVER TABLE carries one table.
    #[serde(rename = "recover_info", default, skip_serializing_if = "Option::is_none")]
    pub recover_info: Option<GoShared<RecoverSchemaInfo>>,
    /// Go `recoverCheckFlag*`: whether GC was on when the job began.
    #[serde(rename = "check_flag", default, skip_serializing_if = "crate::serde_helpers::is_zero_i64")]
    pub check_flag: i64,
    /// Used during runtime (Go `json:"-"`).
    #[serde(skip)]
    pub affected_physical_ids: Vec<i64>,
}

impl RecoverArgs {
    /// Go `RecoverTableInfos`.
    #[must_use]
    pub fn recover_table_infos(&self) -> Vec<GoShared<RecoverTableInfo>> {
        self.recover_info
            .as_ref()
            .and_then(|info| info.read().recover_table_infos.clone())
            .unwrap_or_default()
    }
}

impl JobArgs for RecoverArgs {
    job_args_identity_methods!(Recover);

    fn get_args_v1(value: Option<&GoShared<Self>>, job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *RecoverArgs receiver").read();
        let check_flag: GoAny = ColumnDefaultValue::Int(value.check_flag).into();
        GoSharedSlice::from_vec(if job.type_ == ActionType::ACTION_RECOVER_TABLE {
            vec![
                typed_pointer_any(
                    model_type("RecoverTableInfo", GoTypeKind::Struct),
                    value.recover_table_infos().into_iter().next(),
                ),
                check_flag,
            ]
        } else {
            vec![
                typed_pointer_any(
                    model_type("RecoverSchemaInfo", GoTypeKind::Struct),
                    value.recover_info.clone(),
                ),
                check_flag,
            ]
        })
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let mut decoder = V1Decoder::new(job)?;
        let recover_info = if job.type_ == ActionType::ACTION_RECOVER_TABLE {
            let table = GoField::<Option<GoShared<RecoverTableInfo>>>::default();
            decoder.decode(&table, model_type("RecoverTableInfo", GoTypeKind::Struct).pointer_to())?;
            RecoverSchemaInfo {
                recover_table_infos: Some(table.get().into_iter().collect()),
                ..RecoverSchemaInfo::default()
            }
        } else {
            let schema = GoField::<Option<GoShared<RecoverSchemaInfo>>>::default();
            decoder.decode(&schema, model_type("RecoverSchemaInfo", GoTypeKind::Struct).pointer_to())?;
            schema.get().map(|schema| schema.read().clone()).unwrap_or_default()
        };
        let check_flag = GoField::<i64>::default();
        decoder.decode(&check_flag, builtin_type("int64", GoTypeKind::Int64))?;
        decoder.finish(job);
        Ok(Some(GoShared::new(Self {
            recover_info: Some(GoShared::new(recover_info)),
            check_flag: check_flag.get(),
            affected_physical_ids: Vec::new(),
        })))
    }
}

/// Go `GetRecoverArgs`.
pub fn get_recover_args(job: &mut Job) -> Result<Option<GoShared<RecoverArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}
