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

//! Schema, table and partition job arguments.

use super::*;

/// Go `EmptyArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct EmptyArgs {}

impl JobArgs for EmptyArgs {
    fn into_job_args_value(value: Option<GoShared<Self>>) -> JobArgsValue {
        JobArgsValue::Empty(value)
    }

    fn from_job_args_value(value: &JobArgsValue) -> Option<Option<GoShared<Self>>> {
        match value {
            JobArgsValue::Empty(value) => Some(value.clone()),
            _ => None,
        }
    }

    fn get_args_v1(_value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        GoSharedSlice::default()
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        assert_eq!(job.version, JobVersion::V1, "job version is not v1");
        Ok(Some(GoShared::new(Self {})))
    }
}

/// Go `CreateSchemaArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct CreateSchemaArgs {
    /// Database metadata pointer.
    #[serde(
        rename = "db_info",
        default,
        skip_serializing_if = "field_shared_pointer_is_none"
    )]
    pub db_info: GoField<Option<GoShared<DBInfo>>>,
}

impl JobArgs for CreateSchemaArgs {
    fn into_job_args_value(value: Option<GoShared<Self>>) -> JobArgsValue {
        JobArgsValue::CreateSchema(value)
    }

    fn from_job_args_value(value: &JobArgsValue) -> Option<Option<GoShared<Self>>> {
        match value {
            JobArgsValue::CreateSchema(value) => Some(value.clone()),
            _ => None,
        }
    }

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *CreateSchemaArgs receiver").read();
        GoSharedSlice::from_vec(vec![typed_pointer_any(
            model_type("DBInfo", GoTypeKind::Struct),
            value.db_info.get(),
        )])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let database = GoShared::new(DBInfo::default());
        let value = GoShared::new(Self {
            db_info: GoField::new(Some(database.clone())),
        });
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode_pointee(&database, model_type("DBInfo", GoTypeKind::Struct))?;
        decoder.finish(job);
        Ok(Some(value))
    }
}

/// Go `GetCreateSchemaArgs`.
pub fn get_create_schema_args(
    job: &mut Job,
) -> Result<Option<GoShared<CreateSchemaArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

impl JobArgs for RenameTableArgs {
    fn into_job_args_value(value: Option<GoShared<Self>>) -> JobArgsValue {
        JobArgsValue::RenameTable(value)
    }

    fn from_job_args_value(value: &JobArgsValue) -> Option<Option<GoShared<Self>>> {
        match value {
            JobArgsValue::RenameTable(value) => Some(value.clone()),
            _ => None,
        }
    }

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *RenameTableArgs receiver").read();
        GoSharedSlice::from_vec(vec![
            ColumnDefaultValue::Int(value.old_schema_id).into(),
            typed_value_any(
                ast_type("CIStr", GoTypeKind::Struct),
                value.new_table_name.clone(),
            ),
            typed_value_any(
                ast_type("CIStr", GoTypeKind::Struct),
                value.old_schema_name.clone(),
            ),
        ])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let old_schema_id = GoField::<i64>::default();
        let new_table_name = GoField::<CiString>::default();
        let old_schema_name = GoField::<CiString>::default();
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(&old_schema_id, builtin_type("int64", GoTypeKind::Int64))?;
        decoder.decode(&new_table_name, ast_type("CIStr", GoTypeKind::Struct))?;
        decoder.decode(&old_schema_name, ast_type("CIStr", GoTypeKind::Struct))?;
        decoder.finish(job);
        Ok(Some(GoShared::new(RenameTableArgs {
            old_schema_id: old_schema_id.get(),
            old_schema_name: old_schema_name.get(),
            new_table_name: new_table_name.get(),
            new_schema_id: job.schema_id,
            ..Default::default()
        })))
    }
}

/// Go `GetRenameTableArgs`.
pub fn get_rename_table_args(
    job: &mut Job,
) -> Result<Option<GoShared<RenameTableArgs>>, serde_json::Error> {
    let args = get_or_decode_args::<RenameTableArgs>(job)?;
    if let Some(args) = &args {
        args.write().new_schema_id = job.schema_id;
    }
    Ok(args)
}

/// Go `RenameTablesArgs`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RenameTablesArgs {
    /// Rename entries in statement order.
    #[serde(
        rename = "rename_table_infos",
        default,
        skip_serializing_if = "field_shared_pointer_slice_is_empty"
    )]
    pub rename_table_infos: GoField<GoSharedPointerSlice<RenameTableArgs>>,
}

impl JobArgs for RenameTablesArgs {
    fn into_job_args_value(value: Option<GoShared<Self>>) -> JobArgsValue {
        JobArgsValue::RenameTables(value)
    }

    fn from_job_args_value(value: &JobArgsValue) -> Option<Option<GoShared<Self>>> {
        match value {
            JobArgsValue::RenameTables(value) => Some(value.clone()),
            _ => None,
        }
    }

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let infos = value
            .expect("nil *RenameTablesArgs receiver")
            .read()
            .rename_table_infos
            .get();
        let mut old_schema_ids = Vec::with_capacity(infos.len());
        let mut old_schema_names = Vec::with_capacity(infos.len());
        let mut old_table_names = Vec::with_capacity(infos.len());
        let mut new_schema_ids = Vec::with_capacity(infos.len());
        let mut new_table_names = Vec::with_capacity(infos.len());
        let mut table_ids = Vec::with_capacity(infos.len());
        for info in infos.iter_deref() {
            let info = info.read();
            old_schema_ids.push(info.old_schema_id);
            old_schema_names.push(info.old_schema_name.clone());
            old_table_names.push(info.old_table_name.clone());
            new_schema_ids.push(info.new_schema_id);
            new_table_names.push(info.new_table_name.clone());
            table_ids.push(info.table_id);
        }
        GoSharedSlice::from_vec(vec![
            typed_value_any(
                builtin_type("[]int64", GoTypeKind::Slice),
                GoSharedSlice::from_vec(old_schema_ids),
            ),
            typed_value_any(
                builtin_type("[]int64", GoTypeKind::Slice),
                GoSharedSlice::from_vec(new_schema_ids),
            ),
            typed_value_any(
                ast_type("[]CIStr", GoTypeKind::Slice),
                GoSharedSlice::from_vec(new_table_names),
            ),
            typed_value_any(
                builtin_type("[]int64", GoTypeKind::Slice),
                GoSharedSlice::from_vec(table_ids),
            ),
            typed_value_any(
                ast_type("[]CIStr", GoTypeKind::Slice),
                GoSharedSlice::from_vec(old_schema_names),
            ),
            typed_value_any(
                ast_type("[]CIStr", GoTypeKind::Slice),
                GoSharedSlice::from_vec(old_table_names),
            ),
        ])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let old_schema_ids = GoField::<GoSharedSlice<i64>>::default();
        let new_schema_ids = GoField::<GoSharedSlice<i64>>::default();
        let new_table_names = GoField::<GoSharedSlice<CiString>>::default();
        let table_ids = GoField::<GoSharedSlice<i64>>::default();
        let old_schema_names = GoField::<GoSharedSlice<CiString>>::default();
        let old_table_names = GoField::<GoSharedSlice<CiString>>::default();
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(&old_schema_ids, builtin_type("[]int64", GoTypeKind::Slice))?;
        decoder.decode(&new_schema_ids, builtin_type("[]int64", GoTypeKind::Slice))?;
        decoder.decode(&new_table_names, ast_type("[]CIStr", GoTypeKind::Slice))?;
        decoder.decode(&table_ids, builtin_type("[]int64", GoTypeKind::Slice))?;
        decoder.decode(&old_schema_names, ast_type("[]CIStr", GoTypeKind::Slice))?;
        decoder.decode(&old_table_names, ast_type("[]CIStr", GoTypeKind::Slice))?;
        decoder.finish(job);

        let old_schema_ids = old_schema_ids.get();
        let old_table_names = if old_table_names.read().is_empty() && !old_schema_ids.is_empty() {
            GoSharedSlice::from_vec(vec![CiString::default(); old_schema_ids.len()])
        } else {
            old_table_names.get()
        };
        let infos = (0..old_schema_ids.len())
            .map(|index| {
                Some(GoShared::new(RenameTableArgs {
                    old_schema_id: old_schema_ids.get(index),
                    old_schema_name: old_schema_names.read().get(index),
                    old_table_name: old_table_names.get(index),
                    new_schema_id: new_schema_ids.read().get(index),
                    new_table_name: new_table_names.read().get(index),
                    table_id: table_ids.read().get(index),
                    ..Default::default()
                }))
            })
            .collect();
        Ok(Some(GoShared::new(Self {
            rename_table_infos: GoField::new(GoSharedPointerSlice::from_handles(infos)),
        })))
    }
}

/// Go `GetRenameTablesArgs`.
pub fn get_rename_tables_args(
    job: &mut Job,
) -> Result<Option<GoShared<RenameTablesArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `ResourceGroupArgs`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ResourceGroupArgs {
    /// Resource-group metadata pointer. DROP uses only its name.
    #[serde(
        rename = "rg_info",
        default,
        skip_serializing_if = "field_shared_pointer_is_none"
    )]
    pub resource_group_info: GoField<Option<GoShared<ResourceGroupInfo>>>,
}

impl JobArgs for ResourceGroupArgs {
    fn into_job_args_value(value: Option<GoShared<Self>>) -> JobArgsValue {
        JobArgsValue::ResourceGroup(value)
    }

    fn from_job_args_value(value: &JobArgsValue) -> Option<Option<GoShared<Self>>> {
        match value {
            JobArgsValue::ResourceGroup(value) => Some(value.clone()),
            _ => None,
        }
    }

    fn get_args_v1(value: Option<&GoShared<Self>>, job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *ResourceGroupArgs receiver").read();
        let info = value.resource_group_info.get();
        match job.type_ {
            ActionType::ACTION_CREATE_RESOURCE_GROUP => GoSharedSlice::from_vec(vec![
                typed_pointer_any(model_type("ResourceGroupInfo", GoTypeKind::Struct), info),
                ColumnDefaultValue::Bool(false).into(),
            ]),
            ActionType::ACTION_ALTER_RESOURCE_GROUP => {
                GoSharedSlice::from_vec(vec![typed_pointer_any(
                    model_type("ResourceGroupInfo", GoTypeKind::Struct),
                    info,
                )])
            }
            ActionType::ACTION_DROP_RESOURCE_GROUP => {
                GoSharedSlice::from_vec(vec![ColumnDefaultValue::Str(
                    info.expect("nil ResourceGroupArgs.RGInfo")
                        .read()
                        .name
                        .original()
                        .into(),
                )
                .into()])
            }
            _ => GoSharedSlice::default(),
        }
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let info = GoShared::new(ResourceGroupInfo::default());
        let value = GoShared::new(Self {
            resource_group_info: GoField::new(Some(info.clone())),
        });
        let mut decoder = V1Decoder::new(job)?;
        match job.type_ {
            ActionType::ACTION_CREATE_RESOURCE_GROUP | ActionType::ACTION_ALTER_RESOURCE_GROUP => {
                decoder
                    .decode_pointee(&info, model_type("ResourceGroupInfo", GoTypeKind::Struct))?;
            }
            ActionType::ACTION_DROP_RESOURCE_GROUP => {
                let name = GoField::<GoString>::default();
                decoder.decode(&name, builtin_type("string", GoTypeKind::String))?;
                info.write().name = CiString::new(name.get().to_utf8_lossy_go());
            }
            _ => {}
        }
        decoder.finish(job);
        Ok(Some(value))
    }
}

/// Go `GetResourceGroupArgs`.
pub fn get_resource_group_args(
    job: &mut Job,
) -> Result<Option<GoShared<ResourceGroupArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `DropSchemaArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct DropSchemaArgs {
    /// Submission-time foreign-key check flag.
    #[serde(rename = "fk_check", default, skip_serializing_if = "field_is_default")]
    pub fk_check: GoField<bool>,
    /// Finished-job physical table identifiers.
    #[serde(
        rename = "all_dropped_table_ids",
        default,
        skip_serializing_if = "field_shared_slice_is_empty"
    )]
    pub all_dropped_table_ids: GoField<GoSharedSlice<i64>>,
}

impl JobArgs for DropSchemaArgs {
    fn into_job_args_value(value: Option<GoShared<Self>>) -> JobArgsValue {
        JobArgsValue::DropSchema(value)
    }

    fn from_job_args_value(value: &JobArgsValue) -> Option<Option<GoShared<Self>>> {
        match value {
            JobArgsValue::DropSchema(value) => Some(value.clone()),
            _ => None,
        }
    }

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *DropSchemaArgs receiver").read();
        GoSharedSlice::from_vec(vec![ColumnDefaultValue::Bool(value.fk_check.get()).into()])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let value = GoShared::new(Self::default());
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(
            &value.read().fk_check,
            builtin_type("bool", GoTypeKind::Bool),
        )?;
        decoder.finish(job);
        Ok(Some(value))
    }
}

impl FinishedJobArgs for DropSchemaArgs {
    fn get_finished_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *DropSchemaArgs receiver").read();
        GoSharedSlice::from_vec(vec![typed_value_any(
            builtin_type("[]int64", GoTypeKind::Slice),
            value.all_dropped_table_ids.get(),
        )])
    }
}

/// Go `GetDropSchemaArgs`.
pub fn get_drop_schema_args(
    job: &mut Job,
) -> Result<Option<GoShared<DropSchemaArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `GetFinishedDropSchemaArgs`.
pub fn get_finished_drop_schema_args(
    job: &mut Job,
) -> Result<Option<GoShared<DropSchemaArgs>>, serde_json::Error> {
    if job.version == JobVersion::V1 {
        let value = GoShared::new(DropSchemaArgs::default());
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(
            &value.read().all_dropped_table_ids,
            builtin_type("[]int64", GoTypeKind::Slice),
        )?;
        decoder.finish(job);
        Ok(Some(value))
    } else {
        get_or_decode_args_v2(job)
    }
}

/// Go `ModifySchemaArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ModifySchemaArgs {
    /// Destination charset.
    #[serde(
        rename = "to_charset",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub to_charset: GoField<GoString>,
    /// Destination collation.
    #[serde(
        rename = "to_collate",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub to_collate: GoField<GoString>,
    /// Nullable placement policy reference.
    #[serde(
        rename = "policy_ref",
        default,
        skip_serializing_if = "field_shared_pointer_is_none"
    )]
    pub policy_ref: GoField<Option<GoShared<PolicyRefInfo>>>,
}

impl JobArgs for ModifySchemaArgs {
    fn into_job_args_value(value: Option<GoShared<Self>>) -> JobArgsValue {
        JobArgsValue::ModifySchema(value)
    }

    fn from_job_args_value(value: &JobArgsValue) -> Option<Option<GoShared<Self>>> {
        match value {
            JobArgsValue::ModifySchema(value) => Some(value.clone()),
            _ => None,
        }
    }

    fn get_args_v1(value: Option<&GoShared<Self>>, job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *ModifySchemaArgs receiver").read();
        if job.type_ == ActionType::ACTION_MODIFY_SCHEMA_CHARSET_AND_COLLATE {
            return GoSharedSlice::from_vec(vec![
                ColumnDefaultValue::Str(value.to_charset.get()).into(),
                ColumnDefaultValue::Str(value.to_collate.get()).into(),
            ]);
        }
        GoSharedSlice::from_vec(vec![typed_pointer_any(
            model_type("PolicyRefInfo", GoTypeKind::Struct),
            value.policy_ref.get(),
        )])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let value = GoShared::new(Self::default());
        let mut decoder = V1Decoder::new(job)?;
        if job.type_ == ActionType::ACTION_MODIFY_SCHEMA_CHARSET_AND_COLLATE {
            decoder.decode(
                &value.read().to_charset,
                builtin_type("string", GoTypeKind::String),
            )?;
            decoder.decode(
                &value.read().to_collate,
                builtin_type("string", GoTypeKind::String),
            )?;
        } else {
            decoder.decode(
                &value.read().policy_ref,
                model_type("PolicyRefInfo", GoTypeKind::Struct).pointer_to(),
            )?;
        }
        decoder.finish(job);
        Ok(Some(value))
    }
}

/// Go `GetModifySchemaArgs`.
pub fn get_modify_schema_args(
    job: &mut Job,
) -> Result<Option<GoShared<ModifySchemaArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `CreateTableArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct CreateTableArgs {
    /// Table metadata pointer.
    #[serde(
        rename = "table_info",
        default,
        skip_serializing_if = "field_shared_pointer_is_none"
    )]
    pub table_info: GoField<Option<GoShared<TableInfo>>>,
    /// Create-view replace flag.
    #[serde(
        rename = "on_exist_replace",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub on_exist_replace: GoField<bool>,
    /// Replaced view identifier.
    #[serde(
        rename = "old_view_tbl_id",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub old_view_table_id: GoField<i64>,
    /// Submission-time foreign-key check flag.
    #[serde(rename = "fk_check", default, skip_serializing_if = "field_is_default")]
    pub fk_check: GoField<bool>,
}

impl JobArgs for CreateTableArgs {
    fn into_job_args_value(value: Option<GoShared<Self>>) -> JobArgsValue {
        JobArgsValue::CreateTable(value)
    }

    fn from_job_args_value(value: &JobArgsValue) -> Option<Option<GoShared<Self>>> {
        match value {
            JobArgsValue::CreateTable(value) => Some(value.clone()),
            _ => None,
        }
    }

    fn get_args_v1(value: Option<&GoShared<Self>>, job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *CreateTableArgs receiver").read();
        let table = || {
            typed_pointer_any(
                model_type("TableInfo", GoTypeKind::Struct),
                value.table_info.get(),
            )
        };
        match job.type_ {
            ActionType::ACTION_CREATE_TABLE => GoSharedSlice::from_vec(vec![
                table(),
                ColumnDefaultValue::Bool(value.fk_check.get()).into(),
            ]),
            ActionType::ACTION_CREATE_VIEW => GoSharedSlice::from_vec(vec![
                table(),
                ColumnDefaultValue::Bool(value.on_exist_replace.get()).into(),
                ColumnDefaultValue::Int(value.old_view_table_id.get()).into(),
            ]),
            ActionType::ACTION_CREATE_SEQUENCE => GoSharedSlice::from_vec(vec![table()]),
            _ => GoSharedSlice::default(),
        }
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let table_info = GoShared::new(TableInfo::default());
        let value = GoShared::new(Self {
            table_info: GoField::new(Some(table_info.clone())),
            ..Default::default()
        });
        let mut decoder = V1Decoder::new(job)?;
        match job.type_ {
            ActionType::ACTION_CREATE_TABLE => {
                decoder.decode_pointee(&table_info, model_type("TableInfo", GoTypeKind::Struct))?;
                decoder.decode(
                    &value.read().fk_check,
                    builtin_type("bool", GoTypeKind::Bool),
                )?;
            }
            ActionType::ACTION_CREATE_VIEW => {
                decoder.decode_pointee(&table_info, model_type("TableInfo", GoTypeKind::Struct))?;
                decoder.decode(
                    &value.read().on_exist_replace,
                    builtin_type("bool", GoTypeKind::Bool),
                )?;
                decoder.decode(
                    &value.read().old_view_table_id,
                    builtin_type("int64", GoTypeKind::Int64),
                )?;
            }
            ActionType::ACTION_CREATE_SEQUENCE => {
                decoder.decode_pointee(&table_info, model_type("TableInfo", GoTypeKind::Struct))?;
            }
            _ => {}
        }
        decoder.finish(job);
        Ok(Some(value))
    }
}

/// Go `GetCreateTableArgs`.
pub fn get_create_table_args(
    job: &mut Job,
) -> Result<Option<GoShared<CreateTableArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `CreateMaterializedViewLogArgs`: the arguments for a create materialized
/// view log job.
#[derive(Clone, Debug, Default, Serialize)]
pub struct CreateMaterializedViewLogArgs {
    /// Table metadata pointer.
    #[serde(
        rename = "table_info",
        default,
        skip_serializing_if = "field_shared_pointer_is_none"
    )]
    pub table_info: GoField<Option<GoShared<TableInfo>>>,
}

impl JobArgs for CreateMaterializedViewLogArgs {
    job_args_identity_methods!(CreateMaterializedViewLog);

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value
            .expect("nil *CreateMaterializedViewLogArgs receiver")
            .read();
        GoSharedSlice::from_vec(vec![typed_pointer_any(
            model_type("TableInfo", GoTypeKind::Struct),
            value.table_info.get(),
        )])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let table_info = GoShared::new(TableInfo::default());
        let value = GoShared::new(Self {
            table_info: GoField::new(Some(table_info.clone())),
        });
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode_pointee(&table_info, model_type("TableInfo", GoTypeKind::Struct))?;
        decoder.finish(job);
        Ok(Some(value))
    }
}

/// Go `GetCreateMaterializedViewLogArgs`.
pub fn get_create_materialized_view_log_args(
    job: &mut Job,
) -> Result<Option<GoShared<CreateMaterializedViewLogArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `CreateMaterializedViewArgs`: the arguments for a create materialized
/// view job.
#[derive(Clone, Debug, Default, Serialize)]
pub struct CreateMaterializedViewArgs {
    /// Table metadata pointer.
    #[serde(
        rename = "table_info",
        default,
        skip_serializing_if = "field_shared_pointer_is_none"
    )]
    pub table_info: GoField<Option<GoShared<TableInfo>>>,
    /// The materialized view log table identifiers created alongside the view.
    #[serde(
        rename = "mlog_table_ids",
        default,
        skip_serializing_if = "field_shared_slice_is_empty"
    )]
    pub mlog_table_ids: GoField<GoSharedSlice<i64>>,
}

impl JobArgs for CreateMaterializedViewArgs {
    job_args_identity_methods!(CreateMaterializedView);

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value
            .expect("nil *CreateMaterializedViewArgs receiver")
            .read();
        GoSharedSlice::from_vec(vec![
            typed_pointer_any(
                model_type("TableInfo", GoTypeKind::Struct),
                value.table_info.get(),
            ),
            typed_value_any(
                builtin_type("[]int64", GoTypeKind::Slice),
                value.mlog_table_ids.get(),
            ),
        ])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let table_info = GoShared::new(TableInfo::default());
        let value = GoShared::new(Self {
            table_info: GoField::new(Some(table_info.clone())),
            ..Default::default()
        });
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode_pointee(&table_info, model_type("TableInfo", GoTypeKind::Struct))?;
        decoder.decode(
            &value.read().mlog_table_ids,
            builtin_type("[]int64", GoTypeKind::Slice),
        )?;
        decoder.finish(job);
        Ok(Some(value))
    }
}

/// Go `GetCreateMaterializedViewArgs`.
pub fn get_create_materialized_view_args(
    job: &mut Job,
) -> Result<Option<GoShared<CreateMaterializedViewArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `BatchCreateTableArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct BatchCreateTableArgs {
    /// Create-table argument pointers in source order.
    #[serde(
        rename = "tables",
        default,
        skip_serializing_if = "field_shared_pointer_slice_is_empty"
    )]
    pub tables: GoField<GoSharedPointerSlice<CreateTableArgs>>,
}

impl JobArgs for BatchCreateTableArgs {
    fn into_job_args_value(value: Option<GoShared<Self>>) -> JobArgsValue {
        JobArgsValue::BatchCreateTable(value)
    }

    fn from_job_args_value(value: &JobArgsValue) -> Option<Option<GoShared<Self>>> {
        match value {
            JobArgsValue::BatchCreateTable(value) => Some(value.clone()),
            _ => None,
        }
    }

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *BatchCreateTableArgs receiver").read();
        let tables = value.tables.get();
        let infos = GoSharedPointerSlice::from_handles(
            tables
                .iter_handles()
                .map(|table| {
                    table
                        .expect("nil *CreateTableArgs in BatchCreateTableArgs.Tables")
                        .read()
                        .table_info
                        .get()
                })
                .collect(),
        );
        let fk_check = tables
            .get(0)
            .expect("BatchCreateTableArgs.Tables[0] is nil or missing")
            .read()
            .fk_check
            .get();
        GoSharedSlice::from_vec(vec![
            typed_value_any(builtin_type("[]*model.TableInfo", GoTypeKind::Slice), infos),
            ColumnDefaultValue::Bool(fk_check).into(),
        ])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let table_infos = GoField::<GoSharedPointerSlice<TableInfo>>::default();
        let fk_check = GoField::<bool>::default();
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(
            &table_infos,
            builtin_type("[]*model.TableInfo", GoTypeKind::Slice),
        )?;
        decoder.decode(&fk_check, builtin_type("bool", GoTypeKind::Bool))?;
        decoder.finish(job);

        let table_infos = table_infos.get();
        let fk_check = fk_check.get();
        let tables = GoSharedPointerSlice::from_handles(
            table_infos
                .iter_handles()
                .map(|table_info| {
                    Some(GoShared::new(CreateTableArgs {
                        table_info: GoField::new(table_info),
                        fk_check: GoField::new(fk_check),
                        ..Default::default()
                    }))
                })
                .collect(),
        );
        Ok(Some(GoShared::new(Self {
            tables: GoField::new(tables),
        })))
    }
}

/// Go `GetBatchCreateTableArgs`.
pub fn get_batch_create_table_args(
    job: &mut Job,
) -> Result<Option<GoShared<BatchCreateTableArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `TruncateTableArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct TruncateTableArgs {
    /// Submission-time foreign-key check flag.
    #[serde(rename = "fk_check", default, skip_serializing_if = "field_is_default")]
    pub fk_check: GoField<bool>,
    /// New physical table identifier.
    #[serde(
        rename = "new_table_id",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub new_table_id: GoField<i64>,
    /// Replacement partition identifiers.
    #[serde(
        rename = "new_partition_ids",
        default,
        skip_serializing_if = "field_shared_slice_is_empty"
    )]
    pub new_partition_ids: GoField<GoSharedSlice<i64>>,
    /// Previous partition identifiers.
    #[serde(
        rename = "old_partition_ids",
        default,
        skip_serializing_if = "field_shared_slice_is_empty"
    )]
    pub old_partition_ids: GoField<GoSharedSlice<i64>>,
    /// Runtime-only policy-bearing new partitions.
    #[serde(skip)]
    pub new_part_ids_with_policy: GoField<GoSharedSlice<i64>>,
    /// Runtime-only policy-bearing old partitions.
    #[serde(skip)]
    pub old_part_ids_with_policy: GoField<GoSharedSlice<i64>>,
    /// Runtime-only affected-partition update switch.
    #[serde(skip)]
    pub should_update_affected_partitions: GoField<bool>,
}

impl JobArgs for TruncateTableArgs {
    fn into_job_args_value(value: Option<GoShared<Self>>) -> JobArgsValue {
        JobArgsValue::TruncateTable(value)
    }

    fn from_job_args_value(value: &JobArgsValue) -> Option<Option<GoShared<Self>>> {
        match value {
            JobArgsValue::TruncateTable(value) => Some(value.clone()),
            _ => None,
        }
    }

    fn get_args_v1(value: Option<&GoShared<Self>>, job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *TruncateTableArgs receiver").read();
        if job.type_ == ActionType::ACTION_TRUNCATE_TABLE {
            return GoSharedSlice::from_vec(vec![
                ColumnDefaultValue::Int(value.new_table_id.get()).into(),
                ColumnDefaultValue::Bool(value.fk_check.get()).into(),
                typed_value_any(
                    builtin_type("[]int64", GoTypeKind::Slice),
                    value.new_partition_ids.get(),
                ),
                typed_value_any(
                    builtin_type("int", GoTypeKind::Int64),
                    value.old_partition_ids.read().len() as i64,
                ),
            ]);
        }
        GoSharedSlice::from_vec(vec![
            typed_value_any(
                builtin_type("[]int64", GoTypeKind::Slice),
                value.old_partition_ids.get(),
            ),
            typed_value_any(
                builtin_type("[]int64", GoTypeKind::Slice),
                value.new_partition_ids.get(),
            ),
        ])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let value = GoShared::new(Self::default());
        let mut decoder = V1Decoder::new(job)?;
        if job.type_ == ActionType::ACTION_TRUNCATE_TABLE {
            decoder.decode(
                &value.read().new_table_id,
                builtin_type("int64", GoTypeKind::Int64),
            )?;
            decoder.decode(
                &value.read().fk_check,
                builtin_type("bool", GoTypeKind::Bool),
            )?;
            decoder.decode(
                &value.read().new_partition_ids,
                builtin_type("[]int64", GoTypeKind::Slice),
            )?;
        } else {
            decoder.decode(
                &value.read().old_partition_ids,
                builtin_type("[]int64", GoTypeKind::Slice),
            )?;
            decoder.decode(
                &value.read().new_partition_ids,
                builtin_type("[]int64", GoTypeKind::Slice),
            )?;
        }
        decoder.finish(job);
        Ok(Some(value))
    }
}

impl FinishedJobArgs for TruncateTableArgs {
    fn get_finished_args_v1(value: Option<&GoShared<Self>>, job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *TruncateTableArgs receiver").read();
        let old_ids = || {
            typed_value_any(
                builtin_type("[]int64", GoTypeKind::Slice),
                value.old_partition_ids.get(),
            )
        };
        if job.type_ == ActionType::ACTION_TRUNCATE_TABLE {
            return GoSharedSlice::from_vec(vec![
                ColumnDefaultValue::Bytes(GoAnyBytes::from_vec(Vec::new())).into(),
                old_ids(),
            ]);
        }
        GoSharedSlice::from_vec(vec![old_ids()])
    }
}

/// Go `GetTruncateTableArgs`.
pub fn get_truncate_table_args(
    job: &mut Job,
) -> Result<Option<GoShared<TruncateTableArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `GetFinishedTruncateTableArgs`.
pub fn get_finished_truncate_table_args(
    job: &mut Job,
) -> Result<Option<GoShared<TruncateTableArgs>>, serde_json::Error> {
    if job.version != JobVersion::V1 {
        return get_or_decode_args_v2(job);
    }
    let value = GoShared::new(TruncateTableArgs::default());
    let mut decoder = V1Decoder::new(job)?;
    if job.type_ == ActionType::ACTION_TRUNCATE_TABLE {
        let start_key = GoField::<GoByteSlice>::default();
        decoder.decode(&start_key, builtin_type("[]uint8", GoTypeKind::Slice))?;
    }
    decoder.decode(
        &value.read().old_partition_ids,
        builtin_type("[]int64", GoTypeKind::Slice),
    )?;
    decoder.finish(job);
    Ok(Some(value))
}

/// Go `TableIDIndexID`: one table/index pair whose index range is deleted.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct TableIDIndexID {
    /// Physical table identifier.
    #[serde(rename = "TableID")]
    pub table_id: i64,
    /// Index identifier.
    #[serde(rename = "IndexID")]
    pub index_id: i64,
}

/// Go `TablePartitionArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct TablePartitionArgs {
    /// Affected partition names.
    #[serde(
        rename = "part_names",
        default,
        skip_serializing_if = "field_shared_slice_is_empty"
    )]
    pub part_names: GoField<GoSharedSlice<GoString>>,
    /// Partition metadata. V1 decoding always allocates this pointee.
    #[serde(
        rename = "part_info",
        default,
        skip_serializing_if = "field_shared_pointer_is_none"
    )]
    pub part_info: GoField<Option<GoShared<PartitionInfo>>>,
    /// Finished-job physical table identifiers.
    #[serde(
        rename = "old_physical_tbl_ids",
        default,
        skip_serializing_if = "field_shared_slice_is_empty"
    )]
    pub old_physical_table_ids: GoField<GoSharedSlice<i64>>,
    /// Finished-job global index identifiers.
    #[serde(
        rename = "old_global_indexes",
        default,
        skip_serializing_if = "field_shared_slice_is_empty"
    )]
    pub old_global_indexes: GoField<GoSharedSlice<TableIDIndexID>>,
    /// Runtime-only replacement partition identifiers.
    #[serde(skip)]
    pub new_partition_ids: GoField<GoSharedSlice<i64>>,
}

impl JobArgs for TablePartitionArgs {
    fn into_job_args_value(value: Option<GoShared<Self>>) -> JobArgsValue {
        JobArgsValue::TablePartition(value)
    }

    fn from_job_args_value(value: &JobArgsValue) -> Option<Option<GoShared<Self>>> {
        match value {
            JobArgsValue::TablePartition(value) => Some(value.clone()),
            _ => None,
        }
    }

    fn get_args_v1(value: Option<&GoShared<Self>>, job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *TablePartitionArgs receiver").read();
        if job.type_ == ActionType::ACTION_ADD_TABLE_PARTITION {
            return GoSharedSlice::from_vec(vec![typed_pointer_any(
                model_type("PartitionInfo", GoTypeKind::Struct),
                value.part_info.get(),
            )]);
        }
        let part_names = || {
            typed_value_any(
                builtin_type("[]string", GoTypeKind::Slice),
                value.part_names.get(),
            )
        };
        if job.type_ == ActionType::ACTION_DROP_TABLE_PARTITION {
            return GoSharedSlice::from_vec(vec![part_names()]);
        }
        GoSharedSlice::from_vec(vec![
            part_names(),
            typed_pointer_any(
                model_type("PartitionInfo", GoTypeKind::Struct),
                value.part_info.get(),
            ),
        ])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let part_names = GoField::<GoSharedSlice<GoString>>::default();
        let part_info = GoShared::new(PartitionInfo::default());
        let mut decoder = V1Decoder::new(job)?;
        if job.type_ == ActionType::ACTION_ADD_TABLE_PARTITION {
            if job.state == JobState::ROLLINGBACK {
                decoder.decode(&part_names, builtin_type("[]string", GoTypeKind::Slice))?;
            } else {
                decoder
                    .decode_pointee(&part_info, model_type("PartitionInfo", GoTypeKind::Struct))?;
            }
        } else if job.type_ == ActionType::ACTION_DROP_TABLE_PARTITION {
            decoder.decode(&part_names, builtin_type("[]string", GoTypeKind::Slice))?;
        } else {
            decoder.decode(&part_names, builtin_type("[]string", GoTypeKind::Slice))?;
            decoder.decode_pointee(&part_info, model_type("PartitionInfo", GoTypeKind::Struct))?;
        }
        decoder.finish(job);
        Ok(Some(GoShared::new(Self {
            part_names: GoField::new(part_names.get()),
            part_info: GoField::new(Some(part_info)),
            ..Default::default()
        })))
    }
}

impl FinishedJobArgs for TablePartitionArgs {
    fn get_finished_args_v1(value: Option<&GoShared<Self>>, job: &Job) -> GoSharedSlice<GoAny> {
        assert!(
            job.type_ != ActionType::ACTION_ADD_TABLE_PARTITION
                || job.state == JobState::ROLLBACK_DONE,
            "add table partition job should not call getFinishedArgsV1 if not rollback"
        );
        let value = value.expect("nil *TablePartitionArgs receiver").read();
        GoSharedSlice::from_vec(vec![
            typed_value_any(
                builtin_type("[]int64", GoTypeKind::Slice),
                value.old_physical_table_ids.get(),
            ),
            typed_value_any(
                model_type("TableIDIndexID", GoTypeKind::Struct).slice_of(),
                value.old_global_indexes.get(),
            ),
        ])
    }
}

/// Go `GetTablePartitionArgs`.
pub fn get_table_partition_args(
    job: &mut Job,
) -> Result<Option<GoShared<TablePartitionArgs>>, serde_json::Error> {
    let value =
        get_or_decode_args::<TablePartitionArgs>(job)?.expect("nil *TablePartitionArgs receiver");
    if value.read().part_info.get().is_none() {
        value
            .read()
            .part_info
            .set(Some(GoShared::new(PartitionInfo::default())));
    }
    Ok(Some(value))
}

/// Go `GetFinishedTablePartitionArgs`.
pub fn get_finished_table_partition_args(
    job: &mut Job,
) -> Result<Option<GoShared<TablePartitionArgs>>, serde_json::Error> {
    if job.version != JobVersion::V1 {
        return get_or_decode_args_v2(job);
    }
    let old_physical_table_ids = GoField::<GoSharedSlice<i64>>::default();
    let old_global_indexes = GoField::<GoSharedSlice<TableIDIndexID>>::default();
    let mut decoder = V1Decoder::new(job)?;
    decoder.decode(
        &old_physical_table_ids,
        builtin_type("[]int64", GoTypeKind::Slice),
    )?;
    decoder.decode(
        &old_global_indexes,
        model_type("TableIDIndexID", GoTypeKind::Struct).slice_of(),
    )?;
    decoder.finish(job);
    Ok(Some(GoShared::new(TablePartitionArgs {
        old_physical_table_ids: GoField::new(old_physical_table_ids.get()),
        old_global_indexes: GoField::new(old_global_indexes.get()),
        ..Default::default()
    })))
}

/// Go `FillRollbackArgsForAddPartition`.
pub fn fill_rollback_args_for_add_partition(
    job: &mut Job,
    args: Option<&GoShared<TablePartitionArgs>>,
) {
    assert_eq!(
        job.type_,
        ActionType::ACTION_ADD_TABLE_PARTITION,
        "only for add partition job"
    );
    let part_names = args
        .expect("nil *TablePartitionArgs receiver")
        .read()
        .part_names
        .get();
    let mut fake = Job {
        version: job.version,
        type_: ActionType::ACTION_DROP_TABLE_PARTITION,
        ..Default::default()
    };
    fake.fill_args(Some(GoShared::new(TablePartitionArgs {
        part_names: GoField::new(part_names),
        ..Default::default()
    })));
    job.set_v1_decoded_args(fake.decoded_args());
}

/// Go `ExchangeTablePartitionArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ExchangeTablePartitionArgs {
    /// Exchanged partition identifier.
    #[serde(
        rename = "partition_id",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub partition_id: GoField<i64>,
    /// Partitioned-table schema identifier.
    #[serde(
        rename = "pt_schema_id",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub partitioned_table_schema_id: GoField<i64>,
    /// Partitioned-table identifier.
    #[serde(
        rename = "pt_table_id",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub partitioned_table_id: GoField<i64>,
    /// Exchanged partition name.
    #[serde(
        rename = "partition_name",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub partition_name: GoField<GoString>,
    /// Whether row validation is required.
    #[serde(
        rename = "with_validation",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub with_validation: GoField<bool>,
}

impl JobArgs for ExchangeTablePartitionArgs {
    fn into_job_args_value(value: Option<GoShared<Self>>) -> JobArgsValue {
        JobArgsValue::ExchangeTablePartition(value)
    }

    fn from_job_args_value(value: &JobArgsValue) -> Option<Option<GoShared<Self>>> {
        match value {
            JobArgsValue::ExchangeTablePartition(value) => Some(value.clone()),
            _ => None,
        }
    }

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value
            .expect("nil *ExchangeTablePartitionArgs receiver")
            .read();
        GoSharedSlice::from_vec(vec![
            ColumnDefaultValue::Int(value.partition_id.get()).into(),
            ColumnDefaultValue::Int(value.partitioned_table_schema_id.get()).into(),
            ColumnDefaultValue::Int(value.partitioned_table_id.get()).into(),
            ColumnDefaultValue::Str(value.partition_name.get()).into(),
            ColumnDefaultValue::Bool(value.with_validation.get()).into(),
        ])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let value = GoShared::new(Self::default());
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(
            &value.read().partition_id,
            builtin_type("int64", GoTypeKind::Int64),
        )?;
        decoder.decode(
            &value.read().partitioned_table_schema_id,
            builtin_type("int64", GoTypeKind::Int64),
        )?;
        decoder.decode(
            &value.read().partitioned_table_id,
            builtin_type("int64", GoTypeKind::Int64),
        )?;
        decoder.decode(
            &value.read().partition_name,
            builtin_type("string", GoTypeKind::String),
        )?;
        decoder.decode(
            &value.read().with_validation,
            builtin_type("bool", GoTypeKind::Bool),
        )?;
        decoder.finish(job);
        Ok(Some(value))
    }
}

/// Go `GetExchangeTablePartitionArgs`.
pub fn get_exchange_table_partition_args(
    job: &mut Job,
) -> Result<Option<GoShared<ExchangeTablePartitionArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `AlterTablePartitionArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct AlterTablePartitionArgs {
    /// Partition identifier.
    #[serde(
        rename = "partition_id",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub partition_id: GoField<i64>,
    /// Label rule for `ActionAlterTablePartitionAttributes`.
    #[serde(
        rename = "label_rule",
        default,
        skip_serializing_if = "field_shared_pointer_is_none"
    )]
    pub label_rule: GoField<Option<GoShared<serde_json::Value>>>,
    /// Placement policy reference for `ActionAlterTablePartitionPlacement`.
    #[serde(
        rename = "policy_ref_info",
        default,
        skip_serializing_if = "field_shared_pointer_is_none"
    )]
    pub policy_ref_info: GoField<Option<GoShared<PolicyRefInfo>>>,
}

impl JobArgs for AlterTablePartitionArgs {
    job_args_identity_methods!(AlterTablePartition);

    fn get_args_v1(value: Option<&GoShared<Self>>, job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *AlterTablePartitionArgs receiver").read();
        let partition_id = ColumnDefaultValue::Int(value.partition_id.get()).into();
        if job.type_ == ActionType::ACTION_ALTER_TABLE_PARTITION_ATTRIBUTES {
            return GoSharedSlice::from_vec(vec![
                partition_id,
                typed_pointer_any(
                    pdhttp_type("LabelRule", GoTypeKind::Struct),
                    value.label_rule.get(),
                ),
            ]);
        }
        GoSharedSlice::from_vec(vec![
            partition_id,
            typed_pointer_any(
                model_type("PolicyRefInfo", GoTypeKind::Struct),
                value.policy_ref_info.get(),
            ),
        ])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let value = GoShared::new(Self::default());
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(
            &value.read().partition_id,
            builtin_type("int64", GoTypeKind::Int64),
        )?;
        if job.type_ == ActionType::ACTION_ALTER_TABLE_PARTITION_ATTRIBUTES {
            decoder.decode(
                &value.read().label_rule,
                pdhttp_type("LabelRule", GoTypeKind::Struct),
            )?;
        } else {
            decoder.decode(
                &value.read().policy_ref_info,
                model_type("PolicyRefInfo", GoTypeKind::Struct),
            )?;
        }
        decoder.finish(job);
        Ok(Some(value))
    }
}

/// Go `GetAlterTablePartitionArgs`.
pub fn get_alter_table_partition_args(
    job: &mut Job,
) -> Result<Option<GoShared<AlterTablePartitionArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}
