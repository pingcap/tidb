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

//! Go master's materialized view job arguments (`job_args.go`).

use super::*;
use crate::serde_helpers::{is_false, is_zero_i64};

/// Go `mysql.SQLMode` (`int64`).
fn mysql_sql_mode_type() -> GoTypeIdentity {
    GoTypeIdentity::defined("github.com/pingcap/tidb/pkg/parser/mysql", "SQLMode", "mysql.SQLMode", GoTypeKind::Int64)
}

/// Go `CreateMaterializedViewLogArgs`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CreateMaterializedViewLogArgs {
    /// Go `TableInfo`: the MV log table.
    #[serde(rename = "table_info", default, skip_serializing_if = "Option::is_none")]
    pub table_info: Option<GoShared<TableInfo>>,
}

impl JobArgs for CreateMaterializedViewLogArgs {
    job_args_identity_methods!(CreateMaterializedViewLog);

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *CreateMaterializedViewLogArgs receiver").read();
        GoSharedSlice::from_vec(vec![typed_pointer_any(
            model_type("TableInfo", GoTypeKind::Struct),
            value.table_info.clone(),
        )])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let table_info = GoShared::new(TableInfo::default());
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode_pointee(&table_info, model_type("TableInfo", GoTypeKind::Struct))?;
        decoder.finish(job);
        Ok(Some(GoShared::new(Self { table_info: Some(table_info) })))
    }
}

/// Go `GetCreateMaterializedViewLogArgs`.
pub fn get_create_materialized_view_log_args(
    job: &mut Job,
) -> Result<Option<GoShared<CreateMaterializedViewLogArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `CreateMaterializedViewArgs`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CreateMaterializedViewArgs {
    /// Go `TableInfo`: the materialized view table.
    #[serde(rename = "table_info", default, skip_serializing_if = "Option::is_none")]
    pub table_info: Option<GoShared<TableInfo>>,
    /// Go `MLogTableIDs`.
    #[serde(rename = "mlog_table_ids", default, skip_serializing_if = "Vec::is_empty")]
    pub mlog_table_ids: Vec<i64>,
}

impl JobArgs for CreateMaterializedViewArgs {
    job_args_identity_methods!(CreateMaterializedView);

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *CreateMaterializedViewArgs receiver").read();
        GoSharedSlice::from_vec(vec![
            typed_pointer_any(model_type("TableInfo", GoTypeKind::Struct), value.table_info.clone()),
            typed_value_any(
                builtin_type("[]int64", GoTypeKind::Slice),
                GoSharedSlice::from_vec(value.mlog_table_ids.clone()),
            ),
        ])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let table_info = GoShared::new(TableInfo::default());
        let mlog_table_ids = GoField::<GoSharedSlice<i64>>::default();
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode_pointee(&table_info, model_type("TableInfo", GoTypeKind::Struct))?;
        decoder.decode(&mlog_table_ids, builtin_type("[]int64", GoTypeKind::Slice))?;
        decoder.finish(job);
        Ok(Some(GoShared::new(Self {
            table_info: Some(table_info),
            mlog_table_ids: mlog_table_ids.get().snapshot(),
        })))
    }
}

/// Go `GetCreateMaterializedViewArgs`.
pub fn get_create_materialized_view_args(
    job: &mut Job,
) -> Result<Option<GoShared<CreateMaterializedViewArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `AlterMaterializedViewRefreshArgs`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AlterMaterializedViewRefreshArgs {
    /// Go `RefreshMethod`.
    #[serde(rename = "refresh_method", default, skip_serializing_if = "String::is_empty")]
    pub refresh_method: String,
    /// Go `RefreshStartWith`.
    #[serde(rename = "refresh_start_with", default, skip_serializing_if = "String::is_empty")]
    pub refresh_start_with: String,
    /// Go `RefreshNext`.
    #[serde(rename = "refresh_next", default, skip_serializing_if = "String::is_empty")]
    pub refresh_next: String,
    /// Go `RefreshScheduleSQLMode`.
    #[serde(rename = "refresh_schedule_sql_mode", default, skip_serializing_if = "is_zero_i64")]
    pub refresh_schedule_sql_mode: i64,
    /// Go `UpdateRefreshSchedule`.
    #[serde(rename = "update_refresh_schedule", default, skip_serializing_if = "is_false")]
    pub update_refresh_schedule: bool,
}

impl JobArgs for AlterMaterializedViewRefreshArgs {
    job_args_identity_methods!(AlterMaterializedViewRefresh);

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *AlterMaterializedViewRefreshArgs receiver").read();
        GoSharedSlice::from_vec(vec![
            ColumnDefaultValue::Str(value.refresh_method.as_str().into()).into(),
            ColumnDefaultValue::Str(value.refresh_start_with.as_str().into()).into(),
            ColumnDefaultValue::Str(value.refresh_next.as_str().into()).into(),
            typed_value_any(mysql_sql_mode_type(), value.refresh_schedule_sql_mode),
            ColumnDefaultValue::Bool(value.update_refresh_schedule).into(),
        ])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let (method, start_with, next) =
            (GoField::<String>::default(), GoField::<String>::default(), GoField::<String>::default());
        let (sql_mode, update) = (GoField::<i64>::default(), GoField::<bool>::default());
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(&method, builtin_type("string", GoTypeKind::String))?;
        decoder.decode(&start_with, builtin_type("string", GoTypeKind::String))?;
        decoder.decode(&next, builtin_type("string", GoTypeKind::String))?;
        decoder.decode(&sql_mode, mysql_sql_mode_type())?;
        decoder.decode(&update, builtin_type("bool", GoTypeKind::Bool))?;
        decoder.finish(job);
        Ok(Some(GoShared::new(Self {
            refresh_method: method.get(),
            refresh_start_with: start_with.get(),
            refresh_next: next.get(),
            refresh_schedule_sql_mode: sql_mode.get(),
            update_refresh_schedule: update.get(),
        })))
    }
}

/// Go `GetAlterMaterializedViewRefreshArgs`.
pub fn get_alter_materialized_view_refresh_args(
    job: &mut Job,
) -> Result<Option<GoShared<AlterMaterializedViewRefreshArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `AlterMaterializedViewAttributesArgs`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AlterMaterializedViewAttributesArgs {
    /// Go `AlertWarningSec`.
    #[serde(rename = "alert_warning_sec", default, skip_serializing_if = "is_zero_i64")]
    pub alert_warning_sec: i64,
    /// Go `AlertOverdueSec`.
    #[serde(rename = "alert_overdue_sec", default, skip_serializing_if = "is_zero_i64")]
    pub alert_overdue_sec: i64,
    /// Go `AlertRefreshFailed`.
    #[serde(rename = "alert_refresh_failed", default, skip_serializing_if = "is_false")]
    pub alert_refresh_failed: bool,
}

impl JobArgs for AlterMaterializedViewAttributesArgs {
    job_args_identity_methods!(AlterMaterializedViewAttributes);

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *AlterMaterializedViewAttributesArgs receiver").read();
        GoSharedSlice::from_vec(vec![
            ColumnDefaultValue::Int(value.alert_warning_sec).into(),
            ColumnDefaultValue::Int(value.alert_overdue_sec).into(),
            ColumnDefaultValue::Bool(value.alert_refresh_failed).into(),
        ])
    }

    /// Go: the three arguments, else the two a job from before
    /// `AlertRefreshFailed` carries.
    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let decode = |job: &mut Job, with_failed: bool| -> Result<Self, serde_json::Error> {
            let (warning, overdue, failed) =
                (GoField::<i64>::default(), GoField::<i64>::default(), GoField::<bool>::default());
            let mut decoder = V1Decoder::new(job)?;
            decoder.decode(&warning, builtin_type("int64", GoTypeKind::Int64))?;
            decoder.decode(&overdue, builtin_type("int64", GoTypeKind::Int64))?;
            if with_failed {
                decoder.decode(&failed, builtin_type("bool", GoTypeKind::Bool))?;
            }
            decoder.finish(job);
            Ok(Self { alert_warning_sec: warning.get(), alert_overdue_sec: overdue.get(), alert_refresh_failed: failed.get() })
        };
        let args = match decode(job, true) {
            Ok(args) => args,
            Err(_) => decode(job, false)?,
        };
        Ok(Some(GoShared::new(args)))
    }
}

/// Go `GetAlterMaterializedViewAttributesArgs`.
pub fn get_alter_materialized_view_attributes_args(
    job: &mut Job,
) -> Result<Option<GoShared<AlterMaterializedViewAttributesArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `AlterMaterializedViewLogPurgeArgs`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AlterMaterializedViewLogPurgeArgs {
    /// Go `PurgeMethod`.
    #[serde(rename = "purge_method", default, skip_serializing_if = "String::is_empty")]
    pub purge_method: String,
    /// Go `PurgeStartWith`.
    #[serde(rename = "purge_start_with", default, skip_serializing_if = "String::is_empty")]
    pub purge_start_with: String,
    /// Go `PurgeNext`.
    #[serde(rename = "purge_next", default, skip_serializing_if = "String::is_empty")]
    pub purge_next: String,
    /// Go `PurgeScheduleSQLMode`.
    #[serde(rename = "purge_schedule_sql_mode", default, skip_serializing_if = "is_zero_i64")]
    pub purge_schedule_sql_mode: i64,
    /// Go `UpdatePurgeSchedule`.
    #[serde(rename = "update_purge_schedule", default, skip_serializing_if = "is_false")]
    pub update_purge_schedule: bool,
}

impl JobArgs for AlterMaterializedViewLogPurgeArgs {
    job_args_identity_methods!(AlterMaterializedViewLogPurge);

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *AlterMaterializedViewLogPurgeArgs receiver").read();
        GoSharedSlice::from_vec(vec![
            ColumnDefaultValue::Str(value.purge_method.as_str().into()).into(),
            ColumnDefaultValue::Str(value.purge_start_with.as_str().into()).into(),
            ColumnDefaultValue::Str(value.purge_next.as_str().into()).into(),
            typed_value_any(mysql_sql_mode_type(), value.purge_schedule_sql_mode),
            ColumnDefaultValue::Bool(value.update_purge_schedule).into(),
        ])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let (method, start_with, next) =
            (GoField::<String>::default(), GoField::<String>::default(), GoField::<String>::default());
        let (sql_mode, update) = (GoField::<i64>::default(), GoField::<bool>::default());
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(&method, builtin_type("string", GoTypeKind::String))?;
        decoder.decode(&start_with, builtin_type("string", GoTypeKind::String))?;
        decoder.decode(&next, builtin_type("string", GoTypeKind::String))?;
        decoder.decode(&sql_mode, mysql_sql_mode_type())?;
        decoder.decode(&update, builtin_type("bool", GoTypeKind::Bool))?;
        decoder.finish(job);
        Ok(Some(GoShared::new(Self {
            purge_method: method.get(),
            purge_start_with: start_with.get(),
            purge_next: next.get(),
            purge_schedule_sql_mode: sql_mode.get(),
            update_purge_schedule: update.get(),
        })))
    }
}

/// Go `GetAlterMaterializedViewLogPurgeArgs`.
pub fn get_alter_materialized_view_log_purge_args(
    job: &mut Job,
) -> Result<Option<GoShared<AlterMaterializedViewLogPurgeArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}
