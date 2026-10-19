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

//! Table alteration and index job arguments.

use super::*;

/// Go `RebaseAutoIDArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct RebaseAutoIDArgs {
    /// Replacement auto-ID base.
    #[serde(rename = "new_base", default, skip_serializing_if = "field_is_default")]
    pub new_base: GoField<i64>,
    /// Whether the requested base is forced.
    #[serde(rename = "force", default, skip_serializing_if = "field_is_default")]
    pub force: GoField<bool>,
}

impl JobArgs for RebaseAutoIDArgs {
    fn into_job_args_value(value: Option<GoShared<Self>>) -> JobArgsValue {
        JobArgsValue::RebaseAutoId(value)
    }

    fn from_job_args_value(value: &JobArgsValue) -> Option<Option<GoShared<Self>>> {
        match value {
            JobArgsValue::RebaseAutoId(value) => Some(value.clone()),
            _ => None,
        }
    }

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *RebaseAutoIDArgs receiver").read();
        GoSharedSlice::from_vec(vec![
            ColumnDefaultValue::Int(value.new_base.get()).into(),
            ColumnDefaultValue::Bool(value.force.get()).into(),
        ])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let value = GoShared::new(Self::default());
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(
            &value.read().new_base,
            builtin_type("int64", GoTypeKind::Int64),
        )?;
        decoder.decode(&value.read().force, builtin_type("bool", GoTypeKind::Bool))?;
        decoder.finish(job);
        Ok(Some(value))
    }
}

/// Go `GetRebaseAutoIDArgs`.
pub fn get_rebase_auto_id_args(
    job: &mut Job,
) -> Result<Option<GoShared<RebaseAutoIDArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `ModifyTableCommentArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ModifyTableCommentArgs {
    /// Replacement table comment as arbitrary Go-string bytes.
    #[serde(rename = "comment", default, skip_serializing_if = "field_is_default")]
    pub comment: GoField<GoString>,
}

impl JobArgs for ModifyTableCommentArgs {
    fn into_job_args_value(value: Option<GoShared<Self>>) -> JobArgsValue {
        JobArgsValue::ModifyTableComment(value)
    }

    fn from_job_args_value(value: &JobArgsValue) -> Option<Option<GoShared<Self>>> {
        match value {
            JobArgsValue::ModifyTableComment(value) => Some(value.clone()),
            _ => None,
        }
    }

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *ModifyTableCommentArgs receiver").read();
        GoSharedSlice::from_vec(vec![ColumnDefaultValue::Str(value.comment.get()).into()])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let value = GoShared::new(Self::default());
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(
            &value.read().comment,
            builtin_type("string", GoTypeKind::String),
        )?;
        decoder.finish(job);
        Ok(Some(value))
    }
}

/// Go `GetModifyTableCommentArgs`.
pub fn get_modify_table_comment_args(
    job: &mut Job,
) -> Result<Option<GoShared<ModifyTableCommentArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `ModifyTableCharsetAndCollateArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ModifyTableCharsetAndCollateArgs {
    /// Destination charset as arbitrary Go-string bytes.
    #[serde(
        rename = "to_charset",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub to_charset: GoField<GoString>,
    /// Destination collation as arbitrary Go-string bytes.
    #[serde(
        rename = "to_collate",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub to_collate: GoField<GoString>,
    /// Whether existing column metadata is overwritten.
    #[serde(
        rename = "needs_overwrite_cols",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub needs_overwrite_columns: GoField<bool>,
}

impl JobArgs for ModifyTableCharsetAndCollateArgs {
    fn into_job_args_value(value: Option<GoShared<Self>>) -> JobArgsValue {
        JobArgsValue::ModifyTableCharsetAndCollate(value)
    }

    fn from_job_args_value(value: &JobArgsValue) -> Option<Option<GoShared<Self>>> {
        match value {
            JobArgsValue::ModifyTableCharsetAndCollate(value) => Some(value.clone()),
            _ => None,
        }
    }

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value
            .expect("nil *ModifyTableCharsetAndCollateArgs receiver")
            .read();
        GoSharedSlice::from_vec(vec![
            ColumnDefaultValue::Str(value.to_charset.get()).into(),
            ColumnDefaultValue::Str(value.to_collate.get()).into(),
            ColumnDefaultValue::Bool(value.needs_overwrite_columns.get()).into(),
        ])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let value = GoShared::new(Self::default());
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(
            &value.read().to_charset,
            builtin_type("string", GoTypeKind::String),
        )?;
        decoder.decode(
            &value.read().to_collate,
            builtin_type("string", GoTypeKind::String),
        )?;
        decoder.decode(
            &value.read().needs_overwrite_columns,
            builtin_type("bool", GoTypeKind::Bool),
        )?;
        decoder.finish(job);
        Ok(Some(value))
    }
}

/// Go `GetModifyTableCharsetAndCollateArgs`.
pub fn get_modify_table_charset_and_collate_args(
    job: &mut Job,
) -> Result<Option<GoShared<ModifyTableCharsetAndCollateArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

fn shared_slice_is_empty<T>(value: &GoSharedSlice<T>) -> bool {
    value.is_empty()
}

fn shared_pointer_slice_is_empty<T>(value: &GoSharedPointerSlice<T>) -> bool {
    value.is_empty()
}

/// Go `IndexArgSplitOpt`, the V2-only index pre-split payload.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct IndexArgSplitOpt {
    /// Lower bounds.
    #[serde(default, skip_serializing_if = "shared_slice_is_empty")]
    pub lower: GoSharedSlice<String>,
    /// Upper bounds.
    #[serde(default, skip_serializing_if = "shared_slice_is_empty")]
    pub upper: GoSharedSlice<String>,
    /// Region count.
    #[serde(default, skip_serializing_if = "crate::serde_helpers::is_zero_i64")]
    pub num: i64,
    /// Explicit split values.
    #[serde(default, skip_serializing_if = "shared_slice_is_empty")]
    pub value_lists: GoSharedSlice<GoSharedSlice<String>>,
}

/// Go `IndexArg`, shared by add, drop, rename, primary-key and columnar-index
/// jobs. Runtime-only fields retain Go's `json:"-"` behavior.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct IndexArg {
    /// Deprecated runtime-only global marker used by V1 layouts.
    #[serde(skip)]
    pub global: bool,
    /// Unique-index marker.
    #[serde(default, skip_serializing_if = "crate::serde_helpers::is_false")]
    pub unique: bool,
    /// Index name. Go's struct-valued `CIStr` is not omitted at its zero value.
    #[serde(default)]
    pub index_name: CiString,
    /// Ordered index parts. This field has no `omitempty` in Go.
    #[serde(default)]
    pub index_part_specifications: GoSharedPointerSlice<tidb_ast::IndexPartSpecification>,
    /// Index options pointer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_option: Option<GoShared<tidb_ast::IndexOption>>,
    /// Hidden generated columns.
    #[serde(default, skip_serializing_if = "shared_pointer_slice_is_empty")]
    pub hidden_cols: GoSharedPointerSlice<ColumnInfo>,
    /// Vector/columnar functional expression text.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub func_expr: String,
    /// Columnar-index compatibility marker (`json:"is_vector"`).
    #[serde(
        rename = "is_vector",
        default,
        skip_serializing_if = "crate::serde_helpers::is_false"
    )]
    pub is_columnar: bool,
    /// Columnar-index kind.
    #[serde(default, skip_serializing_if = "columnar_index_type_is_na")]
    pub columnar_index_type: crate::ColumnarIndexType,
    /// Primary-key marker.
    #[serde(default, skip_serializing_if = "crate::serde_helpers::is_false")]
    pub is_pk: bool,
    /// Numeric MySQL SQL mode.
    #[serde(default, skip_serializing_if = "crate::serde_helpers::is_zero_u64")]
    pub sql_mode: u64,
    /// Index ID used by completed/drop jobs.
    #[serde(default, skip_serializing_if = "crate::serde_helpers::is_zero_i64")]
    pub index_id: i64,
    /// `IF EXISTS` marker.
    #[serde(default, skip_serializing_if = "crate::serde_helpers::is_false")]
    pub if_exist: bool,
    /// Finished-job global-index marker.
    #[serde(default, skip_serializing_if = "crate::serde_helpers::is_false")]
    pub is_global: bool,
    /// Best-effort automatic pre-split marker for add-index jobs.
    ///
    /// This stays separate from `split_opt` for rolling upgrades: an older
    /// owner may ignore this unknown field, but must not mistake AUTO for an
    /// empty manual split payload.
    #[serde(
        rename = "auto_presplit",
        default,
        skip_serializing_if = "crate::serde_helpers::is_false"
    )]
    pub auto_pre_split: bool,
    /// V2-only pre-split payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub split_opt: Option<GoShared<IndexArgSplitOpt>>,
    /// Partial-index condition string.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub condition_string: String,
}

fn columnar_index_type_is_na(value: &crate::ColumnarIndexType) -> bool {
    *value == crate::ColumnarIndexType::NA
}

impl IndexArg {
    /// Go `IndexArg.GetColumnarIndexType`.
    #[must_use]
    pub fn get_columnar_index_type(&self) -> crate::ColumnarIndexType {
        crate::index_arg_columnar_index_type(self.columnar_index_type, self.is_columnar)
    }
}

/// Go `ModifyIndexArgs`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ModifyIndexArgs {
    /// One or more index operations.
    #[serde(default, skip_serializing_if = "shared_pointer_slice_is_empty")]
    pub index_args: GoSharedPointerSlice<IndexArg>,
    /// Partition IDs stored by completed jobs.
    #[serde(default, skip_serializing_if = "shared_slice_is_empty")]
    pub partition_ids: GoSharedSlice<i64>,
    /// V1 completed-argument discriminator; never persisted in V2 JSON.
    #[serde(skip)]
    pub op_type: crate::IndexOp,
}

impl ModifyIndexArgs {
    /// The exact `IndexArgs[0].Unique` access used by Go BDR admission.
    /// Missing input retains Go's index-out-of-range panic boundary.
    #[must_use]
    pub fn first_index_unique(&self) -> bool {
        self.index_args
            .get(0)
            .expect("index out of range [0] with length 0")
            .read()
            .unique
    }
}

impl JobArgs for ModifyIndexArgs {
    job_args_identity_methods!(ModifyIndex);

    fn get_args_v1(value: Option<&GoShared<Self>>, job: &Job) -> GoSharedSlice<GoAny> {
        let args = value.expect("nil *ModifyIndexArgs receiver").read();
        if job.type_ == ActionType::ACTION_RENAME_INDEX {
            return GoSharedSlice::from_vec(vec![
                typed_value_any(
                    ast_type("CIStr", GoTypeKind::Struct),
                    args.index_args
                        .get(0)
                        .expect("missing source index")
                        .read()
                        .index_name
                        .clone(),
                ),
                typed_value_any(
                    ast_type("CIStr", GoTypeKind::Struct),
                    args.index_args
                        .get(1)
                        .expect("missing target index")
                        .read()
                        .index_name
                        .clone(),
                ),
            ]);
        }

        if job.type_ == ActionType::ACTION_DROP_INDEX
            || job.type_ == ActionType::ACTION_DROP_PRIMARY_KEY
        {
            if args.index_args.len() == 1 {
                let index = args.index_args.get(0).unwrap();
                let index = index.read();
                return GoSharedSlice::from_vec(vec![
                    typed_value_any(
                        ast_type("CIStr", GoTypeKind::Struct),
                        index.index_name.clone(),
                    ),
                    ColumnDefaultValue::Bool(index.if_exist).into(),
                ]);
            }
            let mut names = Vec::with_capacity(args.index_args.len());
            let mut if_exists = Vec::with_capacity(args.index_args.len());
            for index in args.index_args.iter_deref() {
                let index = index.read();
                names.push(index.index_name.clone());
                if_exists.push(index.if_exist);
            }
            return GoSharedSlice::from_vec(vec![
                typed_value_any(ast_type("[]CIStr", GoTypeKind::Slice), names),
                typed_value_any(builtin_type("[]bool", GoTypeKind::Slice), if_exists),
            ]);
        }

        let index = args.index_args.get(0).expect("missing index argument");
        let index = index.read();
        if job.type_ == ActionType::ACTION_ADD_COLUMNAR_INDEX {
            return GoSharedSlice::from_vec(vec![
                typed_value_any(
                    ast_type("CIStr", GoTypeKind::Struct),
                    index.index_name.clone(),
                ),
                typed_pointer_any(
                    ast_type("IndexPartSpecification", GoTypeKind::Struct),
                    index.index_part_specifications.get(0),
                ),
                typed_pointer_any(
                    ast_type("IndexOption", GoTypeKind::Struct),
                    index.index_option.clone(),
                ),
                ColumnDefaultValue::str(&index.func_expr).into(),
                typed_value_any(
                    model_type("ColumnarIndexType", GoTypeKind::Byte),
                    index.columnar_index_type,
                ),
            ]);
        }

        if job.type_ == ActionType::ACTION_ADD_PRIMARY_KEY {
            return GoSharedSlice::from_vec(vec![
                ColumnDefaultValue::Bool(index.unique).into(),
                typed_value_any(
                    ast_type("CIStr", GoTypeKind::Struct),
                    index.index_name.clone(),
                ),
                typed_value_any(
                    ast_type("[]*IndexPartSpecification", GoTypeKind::Slice),
                    index.index_part_specifications.clone(),
                ),
                typed_pointer_any(
                    ast_type("IndexOption", GoTypeKind::Struct),
                    index.index_option.clone(),
                ),
                typed_value_any(
                    GoTypeIdentity::defined(
                        "github.com/pingcap/tidb/pkg/parser/mysql",
                        "SQLMode",
                        "mysql.SQLMode",
                        GoTypeKind::Uint64,
                    ),
                    index.sql_mode,
                ),
                GoAny::nil(),
                ColumnDefaultValue::Bool(index.global).into(),
            ]);
        }

        let count = args.index_args.len();
        if count == 1 {
            return GoSharedSlice::from_vec(vec![
                ColumnDefaultValue::Bool(index.unique).into(),
                typed_value_any(
                    ast_type("CIStr", GoTypeKind::Struct),
                    index.index_name.clone(),
                ),
                typed_value_any(
                    ast_type("[]*IndexPartSpecification", GoTypeKind::Slice),
                    index.index_part_specifications.clone(),
                ),
                typed_pointer_any(
                    ast_type("IndexOption", GoTypeKind::Struct),
                    index.index_option.clone(),
                ),
                typed_value_any(
                    builtin_type("[]*model.ColumnInfo", GoTypeKind::Slice),
                    index.hidden_cols.clone(),
                ),
                ColumnDefaultValue::Bool(index.global).into(),
            ]);
        }

        let mut unique = Vec::with_capacity(count);
        let mut names = Vec::with_capacity(count);
        let mut parts = Vec::with_capacity(count);
        let mut options = Vec::with_capacity(count);
        let mut hidden = Vec::with_capacity(count);
        let mut global = Vec::with_capacity(count);
        drop(index);
        for index in args.index_args.iter_deref() {
            let index = index.read();
            unique.push(index.unique);
            names.push(index.index_name.clone());
            parts.push(index.index_part_specifications.clone());
            options.push(index.index_option.clone());
            hidden.push(index.hidden_cols.clone());
            global.push(index.global);
        }
        GoSharedSlice::from_vec(vec![
            typed_value_any(builtin_type("[]bool", GoTypeKind::Slice), unique),
            typed_value_any(ast_type("[]CIStr", GoTypeKind::Slice), names),
            typed_value_any(
                ast_type("[][]*IndexPartSpecification", GoTypeKind::Slice),
                parts,
            ),
            typed_value_any(ast_type("[]*IndexOption", GoTypeKind::Slice), options),
            typed_value_any(
                builtin_type("[][]*model.ColumnInfo", GoTypeKind::Slice),
                hidden,
            ),
            typed_value_any(builtin_type("[]bool", GoTypeKind::Slice), global),
        ])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        match job.type_ {
            ActionType::ACTION_RENAME_INDEX => decode_rename_index_v1(job),
            ActionType::ACTION_ADD_INDEX => decode_add_index_v1(job),
            ActionType::ACTION_ADD_COLUMNAR_INDEX => decode_add_columnar_index_v1(job),
            ActionType::ACTION_ADD_PRIMARY_KEY => decode_add_primary_key_v1(job),
            _ => Err(serde_json::Error::io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid job type for decoding {}", job.type_.0),
            ))),
        }
    }
}

impl FinishedJobArgs for ModifyIndexArgs {
    fn get_finished_args_v1(value: Option<&GoShared<Self>>, job: &Job) -> GoSharedSlice<GoAny> {
        let args = value.expect("nil *ModifyIndexArgs receiver").read();
        if args.op_type == crate::IndexOp::ADD_INDEX {
            if job.type_ == ActionType::ACTION_ADD_COLUMNAR_INDEX {
                let index = args.index_args.get(0).expect("missing index argument");
                let index = index.read();
                return GoSharedSlice::from_vec(vec![
                    ColumnDefaultValue::Int(index.index_id).into(),
                    ColumnDefaultValue::Bool(index.if_exist).into(),
                    typed_value_any(
                        builtin_type("[]int64", GoTypeKind::Slice),
                        args.partition_ids.clone(),
                    ),
                    ColumnDefaultValue::Bool(index.is_global).into(),
                ]);
            }
            let mut ids = Vec::with_capacity(args.index_args.len());
            let mut if_exists = Vec::with_capacity(args.index_args.len());
            let mut globals = Vec::with_capacity(args.index_args.len());
            for index in args.index_args.iter_deref() {
                let index = index.read();
                ids.push(index.index_id);
                if_exists.push(index.if_exist);
                globals.push(index.global);
            }
            return GoSharedSlice::from_vec(vec![
                typed_value_any(builtin_type("[]int64", GoTypeKind::Slice), ids),
                typed_value_any(builtin_type("[]bool", GoTypeKind::Slice), if_exists),
                typed_value_any(
                    builtin_type("[]int64", GoTypeKind::Slice),
                    args.partition_ids.clone(),
                ),
                typed_value_any(builtin_type("[]bool", GoTypeKind::Slice), globals),
            ]);
        }

        if args.op_type == crate::IndexOp::ROLLBACK_ADD_INDEX {
            let mut names = Vec::with_capacity(args.index_args.len());
            let mut if_exists = Vec::with_capacity(args.index_args.len());
            for index in args.index_args.iter_deref() {
                let index = index.read();
                names.push(index.index_name.clone());
                if_exists.push(index.if_exist);
            }
            return GoSharedSlice::from_vec(vec![
                typed_value_any(ast_type("[]CIStr", GoTypeKind::Slice), names),
                typed_value_any(builtin_type("[]bool", GoTypeKind::Slice), if_exists),
                typed_value_any(
                    builtin_type("[]int64", GoTypeKind::Slice),
                    args.partition_ids.clone(),
                ),
            ]);
        }

        let index = args.index_args.get(0).expect("missing index argument");
        let index = index.read();
        GoSharedSlice::from_vec(vec![
            typed_value_any(
                ast_type("CIStr", GoTypeKind::Struct),
                index.index_name.clone(),
            ),
            ColumnDefaultValue::Bool(index.if_exist).into(),
            ColumnDefaultValue::Int(index.index_id).into(),
            typed_value_any(
                builtin_type("[]int64", GoTypeKind::Slice),
                args.partition_ids.clone(),
            ),
            ColumnDefaultValue::Bool(index.is_columnar).into(),
        ])
    }
}

fn decode_rename_index_v1(
    job: &mut Job,
) -> Result<Option<GoShared<ModifyIndexArgs>>, serde_json::Error> {
    let from = GoField::<CiString>::default();
    let to = GoField::<CiString>::default();
    let mut decoder = V1Decoder::new(job)?;
    decoder.decode(&from, ast_type("CIStr", GoTypeKind::Struct))?;
    decoder.decode(&to, ast_type("CIStr", GoTypeKind::Struct))?;
    decoder.finish(job);
    Ok(Some(GoShared::new(ModifyIndexArgs {
        index_args: GoSharedPointerSlice::from_handles(vec![
            Some(GoShared::new(IndexArg {
                index_name: from.get(),
                ..Default::default()
            })),
            Some(GoShared::new(IndexArg {
                index_name: to.get(),
                ..Default::default()
            })),
        ]),
        ..Default::default()
    })))
}

fn decode_add_index_v1(
    job: &mut Job,
) -> Result<Option<GoShared<ModifyIndexArgs>>, serde_json::Error> {
    match decode_multi_add_index_v1(job) {
        Ok(value) => Ok(Some(value)),
        Err(_) => decode_single_add_index_v1(job).map(Some),
    }
}

fn decode_multi_add_index_v1(
    job: &mut Job,
) -> Result<GoShared<ModifyIndexArgs>, serde_json::Error> {
    let unique = GoField::<GoSharedSlice<bool>>::default();
    let names = GoField::<GoSharedSlice<CiString>>::default();
    let parts =
        GoField::<GoSharedSlice<GoSharedPointerSlice<tidb_ast::IndexPartSpecification>>>::default();
    let options = GoField::<GoSharedSlice<Option<GoShared<tidb_ast::IndexOption>>>>::default();
    let hidden = GoField::<GoSharedSlice<GoSharedPointerSlice<ColumnInfo>>>::default();
    let global = GoField::<GoSharedSlice<bool>>::default();
    let mut decoder = V1Decoder::new(job)?;
    decoder.decode(&unique, builtin_type("[]bool", GoTypeKind::Slice))?;
    decoder.decode(&names, ast_type("[]CIStr", GoTypeKind::Slice))?;
    decoder.decode(
        &parts,
        ast_type("[][]*IndexPartSpecification", GoTypeKind::Slice),
    )?;
    decoder.decode(&options, ast_type("[]*IndexOption", GoTypeKind::Slice))?;
    decoder.decode(
        &hidden,
        builtin_type("[][]*model.ColumnInfo", GoTypeKind::Slice),
    )?;
    decoder.decode(&global, builtin_type("[]bool", GoTypeKind::Slice))?;
    decoder.finish(job);

    let values: Vec<IndexArg> = (0..unique.read().len())
        .map(|index| IndexArg {
            unique: unique.read().get(index),
            index_name: names.read().get(index),
            index_part_specifications: parts.read().get(index),
            index_option: options.read().get(index),
            hidden_cols: hidden.read().get(index),
            global: global.read().get(index),
            ..Default::default()
        })
        .collect();
    Ok(GoShared::new(ModifyIndexArgs {
        index_args: values.into(),
        ..Default::default()
    }))
}

fn decode_single_add_index_v1(
    job: &mut Job,
) -> Result<GoShared<ModifyIndexArgs>, serde_json::Error> {
    let unique = GoField::<bool>::default();
    let name = GoField::<CiString>::default();
    let parts = GoField::<GoSharedPointerSlice<tidb_ast::IndexPartSpecification>>::default();
    let option = GoField::<Option<GoShared<tidb_ast::IndexOption>>>::default();
    let hidden = GoField::<GoSharedPointerSlice<ColumnInfo>>::default();
    let global = GoField::<bool>::default();
    let mut decoder = V1Decoder::new(job)?;
    decoder.decode(&unique, builtin_type("bool", GoTypeKind::Bool))?;
    decoder.decode(&name, ast_type("CIStr", GoTypeKind::Struct))?;
    decoder.decode(
        &parts,
        ast_type("[]*IndexPartSpecification", GoTypeKind::Slice),
    )?;
    decoder.decode(
        &option,
        ast_type("IndexOption", GoTypeKind::Struct).pointer_to(),
    )?;
    decoder.decode(
        &hidden,
        builtin_type("[]*model.ColumnInfo", GoTypeKind::Slice),
    )?;
    decoder.decode(&global, builtin_type("bool", GoTypeKind::Bool))?;
    decoder.finish(job);
    Ok(GoShared::new(ModifyIndexArgs {
        index_args: vec![IndexArg {
            unique: unique.get(),
            index_name: name.get(),
            index_part_specifications: parts.get(),
            index_option: option.get(),
            hidden_cols: hidden.get(),
            global: global.get(),
            ..Default::default()
        }]
        .into(),
        ..Default::default()
    }))
}

fn decode_add_primary_key_v1(
    job: &mut Job,
) -> Result<Option<GoShared<ModifyIndexArgs>>, serde_json::Error> {
    let unique = GoField::<bool>::default();
    let name = GoField::<CiString>::default();
    let parts = GoField::<GoSharedPointerSlice<tidb_ast::IndexPartSpecification>>::default();
    let option = GoField::<Option<GoShared<tidb_ast::IndexOption>>>::default();
    let sql_mode = GoField::<u64>::default();
    let unused = GoField::<serde_json::Value>::default();
    let global = GoField::<bool>::default();
    let mut decoder = V1Decoder::new(job)?;
    decoder.decode(&unique, builtin_type("bool", GoTypeKind::Bool))?;
    decoder.decode(&name, ast_type("CIStr", GoTypeKind::Struct))?;
    decoder.decode(
        &parts,
        ast_type("[]*IndexPartSpecification", GoTypeKind::Slice),
    )?;
    decoder.decode(
        &option,
        ast_type("IndexOption", GoTypeKind::Struct).pointer_to(),
    )?;
    decoder.decode(
        &sql_mode,
        GoTypeIdentity::defined(
            "github.com/pingcap/tidb/pkg/parser/mysql",
            "SQLMode",
            "mysql.SQLMode",
            GoTypeKind::Uint64,
        ),
    )?;
    decoder.decode(&unused, builtin_type("interface {}", GoTypeKind::Other))?;
    decoder.decode(&global, builtin_type("bool", GoTypeKind::Bool))?;
    decoder.finish(job);
    Ok(Some(GoShared::new(ModifyIndexArgs {
        index_args: vec![IndexArg {
            unique: unique.get(),
            index_name: name.get(),
            index_part_specifications: parts.get(),
            index_option: option.get(),
            is_pk: true,
            sql_mode: sql_mode.get(),
            global: global.get(),
            ..Default::default()
        }]
        .into(),
        ..Default::default()
    })))
}

fn decode_add_columnar_index_v1(
    job: &mut Job,
) -> Result<Option<GoShared<ModifyIndexArgs>>, serde_json::Error> {
    let name = GoField::<CiString>::default();
    let part = GoField::<Option<GoShared<tidb_ast::IndexPartSpecification>>>::default();
    let option = GoField::<Option<GoShared<tidb_ast::IndexOption>>>::default();
    let func_expr = GoField::<String>::default();
    let kind = GoField::<crate::ColumnarIndexType>::default();
    let mut decoder = V1Decoder::new(job)?;
    decoder.decode(&name, ast_type("CIStr", GoTypeKind::Struct))?;
    decoder.decode(
        &part,
        ast_type("IndexPartSpecification", GoTypeKind::Struct).pointer_to(),
    )?;
    decoder.decode(
        &option,
        ast_type("IndexOption", GoTypeKind::Struct).pointer_to(),
    )?;
    decoder.decode(&func_expr, builtin_type("string", GoTypeKind::String))?;
    decoder.decode(&kind, model_type("ColumnarIndexType", GoTypeKind::Byte))?;
    decoder.finish(job);
    Ok(Some(GoShared::new(ModifyIndexArgs {
        index_args: vec![IndexArg {
            index_name: name.get(),
            index_part_specifications: GoSharedPointerSlice::from_handles(vec![part.get()]),
            index_option: option.get(),
            func_expr: func_expr.get(),
            is_columnar: true,
            columnar_index_type: kind.get(),
            ..Default::default()
        }]
        .into(),
        ..Default::default()
    })))
}

/// Go `GetModifyIndexArgs`.
pub fn get_modify_index_args(
    job: &mut Job,
) -> Result<Option<GoShared<ModifyIndexArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

fn decode_drop_index_v1(
    job: &mut Job,
) -> Result<Option<GoShared<ModifyIndexArgs>>, serde_json::Error> {
    let scalar_name = GoField::<CiString>::default();
    let scalar_if_exists = GoField::<bool>::default();
    let scalar = (|| {
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(&scalar_name, ast_type("CIStr", GoTypeKind::Struct))?;
        decoder.decode(&scalar_if_exists, builtin_type("bool", GoTypeKind::Bool))?;
        decoder.finish(job);
        Ok::<_, serde_json::Error>(())
    })();

    let (names, if_exists) = if scalar.is_ok() {
        (vec![scalar_name.get()], vec![scalar_if_exists.get()])
    } else {
        let names = GoField::<GoSharedSlice<CiString>>::default();
        let if_exists = GoField::<GoSharedSlice<bool>>::default();
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(&names, ast_type("[]CIStr", GoTypeKind::Slice))?;
        decoder.decode(&if_exists, builtin_type("[]bool", GoTypeKind::Slice))?;
        decoder.finish(job);
        (names.get().snapshot(), if_exists.get().snapshot())
    };
    Ok(Some(GoShared::new(ModifyIndexArgs {
        index_args: names
            .into_iter()
            .enumerate()
            .map(|(index, name)| IndexArg {
                index_name: name,
                if_exist: if_exists[index],
                ..Default::default()
            })
            .collect::<Vec<_>>()
            .into(),
        ..Default::default()
    })))
}

/// Go `GetDropIndexArgs`.
pub fn get_drop_index_args(
    job: &mut Job,
) -> Result<Option<GoShared<ModifyIndexArgs>>, serde_json::Error> {
    if job.version == JobVersion::V2 {
        get_or_decode_args_v2(job)
    } else {
        decode_drop_index_v1(job)
    }
}

/// Go `GetFinishedModifyIndexArgs`.
pub fn get_finished_modify_index_args(
    job: &mut Job,
) -> Result<Option<GoShared<ModifyIndexArgs>>, serde_json::Error> {
    if job.version == JobVersion::V2 {
        return get_or_decode_args_v2(job);
    }

    if job.is_rollingback()
        || job.type_ == ActionType::ACTION_DROP_INDEX
        || job.type_ == ActionType::ACTION_DROP_PRIMARY_KEY
    {
        if job.is_rollingback() {
            let names = GoField::<GoSharedSlice<CiString>>::default();
            let if_exists = GoField::<GoSharedSlice<bool>>::default();
            let partition_ids = GoField::<GoSharedSlice<i64>>::default();
            let is_columnar = GoField::<bool>::default();
            let mut decoder = V1Decoder::new(job)?;
            decoder.decode(&names, ast_type("[]CIStr", GoTypeKind::Slice))?;
            decoder.decode(&if_exists, builtin_type("[]bool", GoTypeKind::Slice))?;
            decoder.decode(&partition_ids, builtin_type("[]int64", GoTypeKind::Slice))?;
            decoder.decode(&is_columnar, builtin_type("bool", GoTypeKind::Bool))?;
            decoder.finish(job);
            let values = names
                .get()
                .snapshot()
                .into_iter()
                .enumerate()
                .map(|(index, name)| IndexArg {
                    index_name: name,
                    if_exist: if_exists.read().get(index),
                    is_columnar: is_columnar.get(),
                    ..Default::default()
                })
                .collect::<Vec<_>>();
            return Ok(Some(GoShared::new(ModifyIndexArgs {
                index_args: values.into(),
                partition_ids: partition_ids.get(),
                ..Default::default()
            })));
        } else {
            let name = GoField::<CiString>::default();
            let if_exists = GoField::<bool>::default();
            let index_id = GoField::<i64>::default();
            let partition_ids = GoField::<GoSharedSlice<i64>>::default();
            let is_columnar = GoField::<bool>::default();
            let mut decoder = V1Decoder::new(job)?;
            decoder.decode(&name, ast_type("CIStr", GoTypeKind::Struct))?;
            decoder.decode(&if_exists, builtin_type("bool", GoTypeKind::Bool))?;
            decoder.decode(&index_id, builtin_type("int64", GoTypeKind::Int64))?;
            decoder.decode(&partition_ids, builtin_type("[]int64", GoTypeKind::Slice))?;
            decoder.decode(&is_columnar, builtin_type("bool", GoTypeKind::Bool))?;
            decoder.finish(job);
            return Ok(Some(GoShared::new(ModifyIndexArgs {
                index_args: vec![IndexArg {
                    index_name: name.get(),
                    if_exist: if_exists.get(),
                    index_id: index_id.get(),
                    is_columnar: is_columnar.get(),
                    ..Default::default()
                }]
                .into(),
                partition_ids: partition_ids.get(),
                ..Default::default()
            })));
        }
    }

    match decode_finished_add_index_scalar_v1(job) {
        Ok(value) => Ok(Some(value)),
        Err(_) => decode_finished_add_index_slice_v1(job).map(Some),
    }
}

fn decode_finished_add_index_scalar_v1(
    job: &mut Job,
) -> Result<GoShared<ModifyIndexArgs>, serde_json::Error> {
    let id = GoField::<i64>::default();
    let if_exists = GoField::<bool>::default();
    let partition_ids = GoField::<GoSharedSlice<i64>>::default();
    let is_global = GoField::<bool>::default();
    let mut decoder = V1Decoder::new(job)?;
    decoder.decode(&id, builtin_type("int64", GoTypeKind::Int64))?;
    decoder.decode(&if_exists, builtin_type("bool", GoTypeKind::Bool))?;
    decoder.decode(&partition_ids, builtin_type("[]int64", GoTypeKind::Slice))?;
    decoder.decode(&is_global, builtin_type("bool", GoTypeKind::Bool))?;
    decoder.finish(job);
    Ok(GoShared::new(ModifyIndexArgs {
        index_args: vec![IndexArg {
            index_id: id.get(),
            if_exist: if_exists.get(),
            is_global: is_global.get(),
            ..Default::default()
        }]
        .into(),
        partition_ids: partition_ids.get(),
        ..Default::default()
    }))
}

fn decode_finished_add_index_slice_v1(
    job: &mut Job,
) -> Result<GoShared<ModifyIndexArgs>, serde_json::Error> {
    let ids = GoField::<GoSharedSlice<i64>>::default();
    let if_exists = GoField::<GoSharedSlice<bool>>::default();
    let partition_ids = GoField::<GoSharedSlice<i64>>::default();
    let globals = GoField::<GoSharedSlice<bool>>::default();
    let mut decoder = V1Decoder::new(job)?;
    decoder.decode(&ids, builtin_type("[]int64", GoTypeKind::Slice))?;
    decoder.decode(&if_exists, builtin_type("[]bool", GoTypeKind::Slice))?;
    decoder.decode(&partition_ids, builtin_type("[]int64", GoTypeKind::Slice))?;
    decoder.decode(&globals, builtin_type("[]bool", GoTypeKind::Slice))?;
    decoder.finish(job);
    let values = ids
        .get()
        .snapshot()
        .into_iter()
        .enumerate()
        .map(|(index, id)| IndexArg {
            index_id: id,
            if_exist: if_exists.read().get(index),
            is_global: globals.read().get(index),
            ..Default::default()
        })
        .collect::<Vec<_>>();
    Ok(GoShared::new(ModifyIndexArgs {
        index_args: values.into(),
        partition_ids: partition_ids.get(),
        ..Default::default()
    }))
}

/// Go `AlterIndexVisibilityArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct AlterIndexVisibilityArgs {
    /// Index name. Go's struct-valued `CIStr` is never omitted by `omitempty`.
    #[serde(rename = "index_name", default)]
    pub index_name: GoField<CiString>,
    /// Whether the index is invisible.
    #[serde(
        rename = "invisible",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub invisible: GoField<bool>,
}

impl JobArgs for AlterIndexVisibilityArgs {
    job_args_identity_methods!(AlterIndexVisibility);

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value
            .expect("nil *AlterIndexVisibilityArgs receiver")
            .read();
        GoSharedSlice::from_vec(vec![
            typed_value_any(
                ast_type("CIStr", GoTypeKind::Struct),
                value.index_name.get(),
            ),
            ColumnDefaultValue::Bool(value.invisible.get()).into(),
        ])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let value = GoShared::new(Self::default());
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(
            &value.read().index_name,
            ast_type("CIStr", GoTypeKind::Struct),
        )?;
        decoder.decode(
            &value.read().invisible,
            builtin_type("bool", GoTypeKind::Bool),
        )?;
        decoder.finish(job);
        Ok(Some(value))
    }
}

/// Go `GetAlterIndexVisibilityArgs`.
pub fn get_alter_index_visibility_args(
    job: &mut Job,
) -> Result<Option<GoShared<AlterIndexVisibilityArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `AlterTTLInfoArgs`: at least one of the three is set.
#[derive(Clone, Debug, Default, Serialize, serde::Deserialize)]
pub struct AlterTTLInfoArgs {
    /// The new TTL definition.
    #[serde(rename = "ttl_info", default, skip_serializing_if = "Option::is_none")]
    pub ttl_info: Option<GoShared<crate::table::TTLInfo>>,
    /// `TTL_ENABLE`.
    #[serde(
        rename = "ttl_enable",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub ttl_enable: Option<bool>,
    /// `TTL_JOB_INTERVAL`.
    #[serde(
        rename = "ttl_cron_job_schedule",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub ttl_cron_job_schedule: Option<String>,
}

impl JobArgs for AlterTTLInfoArgs {
    job_args_identity_methods!(AlterTtlInfo);

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *AlterTTLInfoArgs receiver").read();
        GoSharedSlice::from_vec(vec![
            typed_pointer_any(
                model_type("TTLInfo", GoTypeKind::Struct),
                value.ttl_info.clone(),
            ),
            typed_value_any(
                builtin_type("bool", GoTypeKind::Bool).pointer_to(),
                value.ttl_enable,
            ),
            typed_value_any(
                builtin_type("string", GoTypeKind::String).pointer_to(),
                value.ttl_cron_job_schedule.clone(),
            ),
        ])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let ttl_info = GoField::<Option<GoShared<crate::table::TTLInfo>>>::default();
        let ttl_enable = GoField::<Option<bool>>::default();
        let schedule = GoField::<Option<String>>::default();
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(
            &ttl_info,
            model_type("TTLInfo", GoTypeKind::Struct).pointer_to(),
        )?;
        decoder.decode(
            &ttl_enable,
            builtin_type("bool", GoTypeKind::Bool).pointer_to(),
        )?;
        decoder.decode(
            &schedule,
            builtin_type("string", GoTypeKind::String).pointer_to(),
        )?;
        decoder.finish(job);
        Ok(Some(GoShared::new(Self {
            ttl_info: ttl_info.get(),
            ttl_enable: ttl_enable.get(),
            ttl_cron_job_schedule: schedule.get(),
        })))
    }
}

/// Go `GetAlterTTLInfoArgs`.
pub fn get_alter_ttl_info_args(
    job: &mut Job,
) -> Result<Option<GoShared<AlterTTLInfoArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `AlterTablePlacementArgs`: `None` is the `default` policy.
#[derive(Clone, Debug, Default, Serialize, serde::Deserialize)]
pub struct AlterTablePlacementArgs {
    /// The policy the table takes.
    #[serde(
        rename = "placement_policy_ref",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub placement_policy_ref: Option<GoShared<crate::PolicyRefInfo>>,
}

impl JobArgs for AlterTablePlacementArgs {
    job_args_identity_methods!(AlterTablePlacement);

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *AlterTablePlacementArgs receiver").read();
        GoSharedSlice::from_vec(vec![typed_pointer_any(
            model_type("PolicyRefInfo", GoTypeKind::Struct),
            value.placement_policy_ref.clone(),
        )])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let reference = GoField::<Option<GoShared<crate::PolicyRefInfo>>>::default();
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(
            &reference,
            model_type("PolicyRefInfo", GoTypeKind::Struct).pointer_to(),
        )?;
        decoder.finish(job);
        Ok(Some(GoShared::new(Self {
            placement_policy_ref: reference.get(),
        })))
    }
}

/// Go `GetAlterTablePlacementArgs`.
pub fn get_alter_table_placement_args(
    job: &mut Job,
) -> Result<Option<GoShared<AlterTablePlacementArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `AlterTableAttributesArgs`: PD's label rule, kept as its JSON.
#[derive(Clone, Debug, Default, Serialize, serde::Deserialize)]
pub struct AlterTableAttributesArgs {
    /// Go `*pdhttp.LabelRule`.
    #[serde(
        rename = "label_rule",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub label_rule: Option<GoShared<serde_json::Value>>,
}

impl JobArgs for AlterTableAttributesArgs {
    job_args_identity_methods!(AlterTableAttributes);

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value
            .expect("nil *AlterTableAttributesArgs receiver")
            .read();
        GoSharedSlice::from_vec(vec![typed_pointer_any(
            builtin_type("http.LabelRule", GoTypeKind::Struct),
            value.label_rule.clone(),
        )])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let rule = GoShared::new(serde_json::Value::Object(Default::default()));
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode_pointee(&rule, builtin_type("http.LabelRule", GoTypeKind::Struct))?;
        decoder.finish(job);
        Ok(Some(GoShared::new(Self {
            label_rule: Some(rule),
        })))
    }
}

/// Go `GetAlterTableAttributesArgs`.
pub fn get_alter_table_attributes_args(
    job: &mut Job,
) -> Result<Option<GoShared<AlterTableAttributesArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `AlterTableAffinityArgs`: `None` removes the affinity.
#[derive(Clone, Debug, Default, Serialize, serde::Deserialize)]
pub struct AlterTableAffinityArgs {
    /// The table's new affinity.
    #[serde(rename = "affinity", default, skip_serializing_if = "Option::is_none")]
    pub affinity: Option<GoShared<crate::table::TableAffinityInfo>>,
}

impl JobArgs for AlterTableAffinityArgs {
    job_args_identity_methods!(AlterTableAffinity);

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        GoSharedSlice::from_vec(vec![typed_pointer_any(
            model_type("AlterTableAffinityArgs", GoTypeKind::Struct),
            value.cloned(),
        )])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let value = GoShared::new(Self::default());
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode_pointee(
            &value,
            model_type("AlterTableAffinityArgs", GoTypeKind::Struct),
        )?;
        decoder.finish(job);
        Ok(Some(value))
    }
}

/// Go `GetAlterTableAffinityArgs`.
pub fn get_alter_table_affinity_args(
    job: &mut Job,
) -> Result<Option<GoShared<AlterTableAffinityArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `AlterTableSetRegionSplitPolicyArgs` (untagged fields, written by
/// their Go names).
#[derive(Clone, Debug, Default, Serialize, serde::Deserialize)]
pub struct AlterTableSetRegionSplitPolicyArgs {
    /// The index, empty for the table's own policy.
    #[serde(rename = "IndexName", default)]
    pub index_name: String,
    /// The policy, `None` to remove it.
    #[serde(rename = "Policy", default)]
    pub policy: Option<GoShared<crate::index::RegionSplitPolicy>>,
}

impl JobArgs for AlterTableSetRegionSplitPolicyArgs {
    job_args_identity_methods!(AlterTableSetRegionSplitPolicy);

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value
            .expect("nil *AlterTableSetRegionSplitPolicyArgs receiver")
            .read();
        GoSharedSlice::from_vec(vec![
            typed_value_any(
                builtin_type("string", GoTypeKind::String),
                value.index_name.clone(),
            ),
            typed_pointer_any(
                model_type("RegionSplitPolicy", GoTypeKind::Struct),
                value.policy.clone(),
            ),
        ])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let index_name = GoField::<String>::default();
        let policy = GoField::<Option<GoShared<crate::index::RegionSplitPolicy>>>::default();
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(&index_name, builtin_type("string", GoTypeKind::String))?;
        decoder.decode(
            &policy,
            model_type("RegionSplitPolicy", GoTypeKind::Struct).pointer_to(),
        )?;
        decoder.finish(job);
        Ok(Some(GoShared::new(Self {
            index_name: index_name.get(),
            policy: policy.get(),
        })))
    }
}

/// Go `GetAlterTableSetRegionSplitPolicyArgs`.
pub fn get_alter_table_set_region_split_policy_args(
    job: &mut Job,
) -> Result<Option<GoShared<AlterTableSetRegionSplitPolicyArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `ast.TiFlashReplicaSpec` as a job persists it (untagged fields).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct JobTiFlashReplicaSpec {
    /// The replica count.
    #[serde(rename = "Count", default)]
    pub count: u64,
    /// The location labels; Go's nil slice is `null`.
    #[serde(rename = "Labels", default)]
    pub labels: Option<Vec<String>>,
    /// A hypothetical replica (index advisor), never a job's.
    #[serde(rename = "Hypo", default)]
    pub hypo: bool,
}

/// Go `SetTiFlashReplicaArgs`.
#[derive(Clone, Debug, Default, Serialize, serde::Deserialize)]
pub struct SetTiFlashReplicaArgs {
    /// The replica spec. Go's struct value is never omitted.
    #[serde(rename = "tiflash_replica", default)]
    pub tiflash_replica: JobTiFlashReplicaSpec,
    /// Native BR's repair: the replica turns unavailable again (v2 only).
    #[serde(
        rename = "reset_available",
        default,
        skip_serializing_if = "std::ops::Not::not"
    )]
    pub reset_available: bool,
    /// Internal repairs skip the columnar-storage gate (v2 only).
    #[serde(
        rename = "skip_columnar_storage_gate",
        default,
        skip_serializing_if = "std::ops::Not::not"
    )]
    pub skip_columnar_storage_gate: bool,
}

impl JobArgs for SetTiFlashReplicaArgs {
    job_args_identity_methods!(SetTiFlashReplica);

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *SetTiFlashReplicaArgs receiver").read();
        GoSharedSlice::from_vec(vec![typed_value_any(
            ast_type("TiFlashReplicaSpec", GoTypeKind::Struct),
            value.tiflash_replica.clone(),
        )])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let spec = GoField::<JobTiFlashReplicaSpec>::default();
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(&spec, ast_type("TiFlashReplicaSpec", GoTypeKind::Struct))?;
        decoder.finish(job);
        Ok(Some(GoShared::new(Self {
            tiflash_replica: spec.get(),
            ..Self::default()
        })))
    }
}

/// Go `GetSetTiFlashReplicaArgs`.
pub fn get_set_tiflash_replica_args(
    job: &mut Job,
) -> Result<Option<GoShared<SetTiFlashReplicaArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `UpdateTiFlashReplicaStatusArgs`.
#[derive(Clone, Debug, Default, Serialize, serde::Deserialize)]
pub struct UpdateTiFlashReplicaStatusArgs {
    /// Whether the replica is now available.
    #[serde(
        rename = "available",
        default,
        skip_serializing_if = "std::ops::Not::not"
    )]
    pub available: bool,
    /// The table or partition the status is about.
    #[serde(
        rename = "physical_id",
        default,
        skip_serializing_if = "is_zero_i64_value"
    )]
    pub physical_id: i64,
}

const fn is_zero_i64_value(value: &i64) -> bool {
    *value == 0
}

impl JobArgs for UpdateTiFlashReplicaStatusArgs {
    job_args_identity_methods!(UpdateTiFlashReplicaStatus);

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value
            .expect("nil *UpdateTiFlashReplicaStatusArgs receiver")
            .read();
        GoSharedSlice::from_vec(vec![
            ColumnDefaultValue::Bool(value.available).into(),
            ColumnDefaultValue::Int(value.physical_id).into(),
        ])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let available = GoField::<bool>::default();
        let physical_id = GoField::<i64>::default();
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(&available, builtin_type("bool", GoTypeKind::Bool))?;
        decoder.decode(&physical_id, builtin_type("int64", GoTypeKind::Int64))?;
        decoder.finish(job);
        Ok(Some(GoShared::new(Self {
            available: available.get(),
            physical_id: physical_id.get(),
        })))
    }
}

/// Go `GetUpdateTiFlashReplicaStatusArgs`.
pub fn get_update_tiflash_replica_status_args(
    job: &mut Job,
) -> Result<Option<GoShared<UpdateTiFlashReplicaStatusArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `LockTablesArgs`, shared by LOCK TABLES, UNLOCK TABLES and ADMIN
/// CLEANUP TABLE LOCK.
#[derive(Clone, Debug, Default, Serialize, serde::Deserialize)]
pub struct LockTablesArgs {
    /// The tables to lock, in statement order.
    #[serde(rename = "lock_tables", default, skip_serializing_if = "Vec::is_empty")]
    pub lock_tables: Vec<crate::table::TableLockTpInfo>,
    /// How many of them the job has locked.
    #[serde(
        rename = "index_of_lock",
        default,
        skip_serializing_if = "is_zero_i64_value"
    )]
    pub index_of_lock: i64,
    /// The tables to unlock first (the session's current locks).
    #[serde(
        rename = "unlock_tables",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub unlock_tables: Vec<crate::table::TableLockTpInfo>,
    /// How many of them the job has unlocked.
    #[serde(
        rename = "index_of_unlock",
        default,
        skip_serializing_if = "is_zero_i64_value"
    )]
    pub index_of_unlock: i64,
    /// The session the locks belong to. Go's struct value is never omitted.
    #[serde(rename = "session_info", default)]
    pub session_info: crate::table::SessionInfo,
    /// ADMIN CLEANUP TABLE LOCK: every holder's lock goes. Go's tag is
    /// `json:"is_cleanup:omitempty"`, so that string is the key and the
    /// field is always written.
    #[serde(rename = "is_cleanup:omitempty", default)]
    pub is_cleanup: bool,
}

impl JobArgs for LockTablesArgs {
    job_args_identity_methods!(LockTables);

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        GoSharedSlice::from_vec(vec![typed_pointer_any(
            model_type("LockTablesArgs", GoTypeKind::Struct),
            value.cloned(),
        )])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let value = GoShared::new(Self::default());
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode_pointee(&value, model_type("LockTablesArgs", GoTypeKind::Struct))?;
        decoder.finish(job);
        Ok(Some(value))
    }
}

/// Go `GetLockTablesArgs`.
pub fn get_lock_tables_args(
    job: &mut Job,
) -> Result<Option<GoShared<LockTablesArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `AddForeignKeyArgs`.
#[derive(Clone, Debug, Default, Serialize, serde::Deserialize)]
pub struct AddForeignKeyArgs {
    /// The key the job adds.
    #[serde(rename = "fk_info", default, skip_serializing_if = "Option::is_none")]
    pub fk_info: Option<GoShared<crate::table::FKInfo>>,
    /// The submitting session's `foreign_key_checks`.
    #[serde(
        rename = "fk_check",
        default,
        skip_serializing_if = "std::ops::Not::not"
    )]
    pub fk_check: bool,
}

impl JobArgs for AddForeignKeyArgs {
    job_args_identity_methods!(AddForeignKey);

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *AddForeignKeyArgs receiver").read();
        GoSharedSlice::from_vec(vec![
            typed_pointer_any(
                model_type("FKInfo", GoTypeKind::Struct),
                value.fk_info.clone(),
            ),
            ColumnDefaultValue::Bool(value.fk_check).into(),
        ])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let fk_info = GoShared::new(crate::table::FKInfo::default());
        let fk_check = GoField::<bool>::default();
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode_pointee(&fk_info, model_type("FKInfo", GoTypeKind::Struct))?;
        decoder.decode(&fk_check, builtin_type("bool", GoTypeKind::Bool))?;
        decoder.finish(job);
        Ok(Some(GoShared::new(Self {
            fk_info: Some(fk_info),
            fk_check: fk_check.get(),
        })))
    }
}

/// Go `GetAddForeignKeyArgs`.
pub fn get_add_foreign_key_args(
    job: &mut Job,
) -> Result<Option<GoShared<AddForeignKeyArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `DropForeignKeyArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct DropForeignKeyArgs {
    /// Foreign-key name. Go's struct-valued `CIStr` is never omitted.
    #[serde(rename = "fk_name", default)]
    pub foreign_key_name: GoField<CiString>,
}

impl JobArgs for DropForeignKeyArgs {
    job_args_identity_methods!(DropForeignKey);

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *DropForeignKeyArgs receiver").read();
        GoSharedSlice::from_vec(vec![typed_value_any(
            ast_type("CIStr", GoTypeKind::Struct),
            value.foreign_key_name.get(),
        )])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let value = GoShared::new(Self::default());
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(
            &value.read().foreign_key_name,
            ast_type("CIStr", GoTypeKind::Struct),
        )?;
        decoder.finish(job);
        Ok(Some(value))
    }
}

/// Go `GetDropForeignKeyArgs`.
pub fn get_drop_foreign_key_args(
    job: &mut Job,
) -> Result<Option<GoShared<DropForeignKeyArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `CheckConstraintArgs`, shared by ALTER and DROP CHECK jobs.
#[derive(Clone, Debug, Default, Serialize)]
pub struct CheckConstraintArgs {
    /// Constraint name. Go's struct-valued `CIStr` is never omitted.
    #[serde(rename = "constraint_name", default)]
    pub constraint_name: GoField<CiString>,
    /// Requested enforcement state; DROP leaves the zero value.
    #[serde(rename = "enforced", default, skip_serializing_if = "field_is_default")]
    pub enforced: GoField<bool>,
}

impl JobArgs for CheckConstraintArgs {
    job_args_identity_methods!(CheckConstraint);

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *CheckConstraintArgs receiver").read();
        GoSharedSlice::from_vec(vec![
            typed_value_any(
                ast_type("CIStr", GoTypeKind::Struct),
                value.constraint_name.get(),
            ),
            ColumnDefaultValue::Bool(value.enforced.get()).into(),
        ])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let value = GoShared::new(Self::default());
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(
            &value.read().constraint_name,
            ast_type("CIStr", GoTypeKind::Struct),
        )?;
        decoder.decode(
            &value.read().enforced,
            builtin_type("bool", GoTypeKind::Bool),
        )?;
        decoder.finish(job);
        Ok(Some(value))
    }
}

/// Go `GetCheckConstraintArgs`.
pub fn get_check_constraint_args(
    job: &mut Job,
) -> Result<Option<GoShared<CheckConstraintArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `AddCheckConstraintArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct AddCheckConstraintArgs {
    /// Constraint metadata submitted with the job.
    #[serde(rename = "constraint_info", default)]
    pub constraint: GoField<Option<GoShared<ConstraintInfo>>>,
}

impl JobArgs for AddCheckConstraintArgs {
    job_args_identity_methods!(AddCheckConstraint);

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *AddCheckConstraintArgs receiver").read();
        GoSharedSlice::from_vec(vec![typed_pointer_any(
            model_type("ConstraintInfo", GoTypeKind::Struct),
            value.constraint.get(),
        )])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let constraint = GoShared::new(ConstraintInfo::default());
        let value = GoShared::new(Self {
            constraint: GoField::new(Some(constraint.clone())),
        });
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode_pointee(
            &constraint,
            model_type("ConstraintInfo", GoTypeKind::Struct),
        )?;
        decoder.finish(job);
        Ok(Some(value))
    }
}

/// Go `GetAddCheckConstraintArgs`.
pub fn get_add_check_constraint_args(
    job: &mut Job,
) -> Result<Option<GoShared<AddCheckConstraintArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `ModifyTableAutoIDCacheArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ModifyTableAutoIDCacheArgs {
    /// Replacement cache size.
    #[serde(
        rename = "new_cache",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub new_cache: GoField<i64>,
}

impl JobArgs for ModifyTableAutoIDCacheArgs {
    job_args_identity_methods!(ModifyTableAutoIdCache);

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value
            .expect("nil *ModifyTableAutoIDCacheArgs receiver")
            .read();
        GoSharedSlice::from_vec(vec![ColumnDefaultValue::Int(value.new_cache.get()).into()])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let value = GoShared::new(Self::default());
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(
            &value.read().new_cache,
            builtin_type("int64", GoTypeKind::Int64),
        )?;
        decoder.finish(job);
        Ok(Some(value))
    }
}

/// Go `GetModifyTableAutoIDCacheArgs`.
pub fn get_modify_table_auto_id_cache_args(
    job: &mut Job,
) -> Result<Option<GoShared<ModifyTableAutoIDCacheArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `ShardRowIDArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ShardRowIDArgs {
    /// Shard-row-ID bit width.
    #[serde(
        rename = "shard_row_id_bits",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub shard_row_id_bits: GoField<u64>,
}

impl JobArgs for ShardRowIDArgs {
    job_args_identity_methods!(ShardRowId);

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *ShardRowIDArgs receiver").read();
        GoSharedSlice::from_vec(vec![ColumnDefaultValue::Uint(
            value.shard_row_id_bits.get(),
        )
        .into()])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let value = GoShared::new(Self::default());
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(
            &value.read().shard_row_id_bits,
            builtin_type("uint64", GoTypeKind::Uint64),
        )?;
        decoder.finish(job);
        Ok(Some(value))
    }
}

/// Go `GetShardRowIDArgs`.
pub fn get_shard_row_id_args(
    job: &mut Job,
) -> Result<Option<GoShared<ShardRowIDArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `SetDefaultValueArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct SetDefaultValueArgs {
    /// Column metadata pointer.
    #[serde(
        rename = "column_info",
        default,
        skip_serializing_if = "field_shared_pointer_is_none"
    )]
    pub column: GoField<Option<GoShared<ColumnInfo>>>,
}

impl JobArgs for SetDefaultValueArgs {
    job_args_identity_methods!(SetDefaultValue);

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *SetDefaultValueArgs receiver").read();
        GoSharedSlice::from_vec(vec![typed_pointer_any(
            model_type("ColumnInfo", GoTypeKind::Struct),
            value.column.get(),
        )])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        // Go preallocates `&ColumnInfo{}` and decodes into the pointee, so a
        // JSON null retains a non-nil zero column and caches that field pointer.
        let column = GoShared::new(ColumnInfo::default());
        let value = GoShared::new(Self {
            column: GoField::new(Some(column.clone())),
        });
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode_pointee(&column, model_type("ColumnInfo", GoTypeKind::Struct))?;
        decoder.finish(job);
        Ok(Some(value))
    }
}

/// Go `GetSetDefaultValueArgs`.
pub fn get_set_default_value_args(
    job: &mut Job,
) -> Result<Option<GoShared<SetDefaultValueArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `RefreshMetaArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct RefreshMetaArgs {
    /// Schema identifier.
    #[serde(
        rename = "schema_id",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub schema_id: GoField<i64>,
    /// Table identifier.
    #[serde(rename = "table_id", default, skip_serializing_if = "field_is_default")]
    pub table_id: GoField<i64>,
    /// Involved database as arbitrary Go-string bytes.
    #[serde(
        rename = "involved_db",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub involved_database: GoField<GoString>,
    /// Involved table as arbitrary Go-string bytes.
    #[serde(
        rename = "involved_table",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub involved_table: GoField<GoString>,
}

impl JobArgs for RefreshMetaArgs {
    job_args_identity_methods!(RefreshMeta);

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *RefreshMetaArgs receiver");
        GoSharedSlice::from_vec(vec![typed_pointer_any(
            model_type("RefreshMetaArgs", GoTypeKind::Struct),
            Some(value.clone()),
        )])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let value = GoShared::new(Self::default());
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode_pointee(&value, model_type("RefreshMetaArgs", GoTypeKind::Struct))?;
        decoder.finish(job);
        Ok(Some(value))
    }
}

/// Go `GetRefreshMetaArgs`.
pub fn get_refresh_meta_args(
    job: &mut Job,
) -> Result<Option<GoShared<RefreshMetaArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `ModifyTableEngineAttributeArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ModifyTableEngineAttributeArgs {
    /// Replacement engine attribute as arbitrary Go-string bytes.
    #[serde(
        rename = "engine_attribute",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub engine_attribute: GoField<GoString>,
}

impl JobArgs for ModifyTableEngineAttributeArgs {
    job_args_identity_methods!(ModifyTableEngineAttribute);

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value
            .expect("nil *ModifyTableEngineAttributeArgs receiver")
            .read();
        GoSharedSlice::from_vec(vec![
            ColumnDefaultValue::Str(value.engine_attribute.get()).into()
        ])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let value = GoShared::new(Self::default());
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(
            &value.read().engine_attribute,
            builtin_type("string", GoTypeKind::String),
        )?;
        decoder.finish(job);
        Ok(Some(value))
    }
}

/// Go `GetModifyTableEngineAttributeArgs`.
pub fn get_modify_table_engine_attribute_args(
    job: &mut Job,
) -> Result<Option<GoShared<ModifyTableEngineAttributeArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `AlterTableModeArgs`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct AlterTableModeArgs {
    /// New table mode.
    #[serde(
        rename = "table_mode",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub table_mode: GoField<TableMode>,
    /// Schema identifier.
    #[serde(
        rename = "schema_id",
        default,
        skip_serializing_if = "field_is_default"
    )]
    pub schema_id: GoField<i64>,
    /// Table identifier.
    #[serde(rename = "table_id", default, skip_serializing_if = "field_is_default")]
    pub table_id: GoField<i64>,
}

impl JobArgs for AlterTableModeArgs {
    job_args_identity_methods!(AlterTableMode);

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *AlterTableModeArgs receiver");
        GoSharedSlice::from_vec(vec![typed_pointer_any(
            model_type("AlterTableModeArgs", GoTypeKind::Struct),
            Some(value.clone()),
        )])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let value = GoShared::new(Self::default());
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode_pointee(&value, model_type("AlterTableModeArgs", GoTypeKind::Struct))?;
        decoder.finish(job);
        Ok(Some(value))
    }
}

/// Go `GetAlterTableModeArgs`.
pub fn get_alter_table_mode_args(
    job: &mut Job,
) -> Result<Option<GoShared<AlterTableModeArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}
