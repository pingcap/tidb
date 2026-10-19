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

//! Go `TableColumnArgs` and `ModifyColumnArgs`: the column jobs' arguments.

use super::*;
use crate::IndexInfo;

/// Go `TableColumnArgs`: the arguments of ADD COLUMN and DROP COLUMN (and of
/// an ADD COLUMN rolling back, which runs as a drop).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TableColumnArgs {
    /// The column (only its name for a drop).
    #[serde(
        rename = "column_info",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub col: Option<GoShared<ColumnInfo>>,
    /// Where an added column goes.
    #[serde(rename = "position", default, skip_serializing_if = "Option::is_none")]
    pub pos: Option<GoShared<tidb_ast::JobColumnPosition>>,
    /// Go `Offset`, kept for older readers.
    #[serde(rename = "offset", default, skip_serializing_if = "is_zero_i64")]
    pub offset: i64,
    /// IF NOT EXISTS / IF EXISTS.
    #[serde(
        rename = "ignore_existence_err",
        default,
        skip_serializing_if = "is_false_bool"
    )]
    pub ignore_existence_err: bool,
    /// A drop's indexes over the column, filled while it runs.
    #[serde(rename = "index_ids", default, skip_serializing_if = "Vec::is_empty")]
    pub index_ids: Vec<i64>,
    /// A finished drop's partitions, effective when `index_ids` is not empty.
    #[serde(
        rename = "partition_ids",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub partition_ids: Vec<i64>,
}

const fn is_zero_i64(value: &i64) -> bool {
    *value == 0
}

const fn is_zero_u8(value: &u8) -> bool {
    *value == 0
}

const fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

const fn is_false_bool(value: &bool) -> bool {
    !*value
}

impl TableColumnArgs {
    /// The column name the args carry (`Col.Name`).
    #[must_use]
    pub fn col_name(&self) -> CiString {
        self.col
            .as_ref()
            .map(|col| col.read().name.clone())
            .unwrap_or_default()
    }
}

fn column_name_only(name: CiString) -> GoShared<ColumnInfo> {
    GoShared::new(ColumnInfo {
        name,
        ..ColumnInfo::default()
    })
}

impl JobArgs for TableColumnArgs {
    job_args_identity_methods!(TableColumn);

    fn get_args_v1(value: Option<&GoShared<Self>>, job: &Job) -> GoSharedSlice<GoAny> {
        let args = value.expect("nil *TableColumnArgs receiver").read();
        if job.type_ == ActionType::ACTION_DROP_COLUMN {
            let mut values = vec![
                typed_value_any(ast_type("CIStr", GoTypeKind::Struct), args.col_name()),
                ColumnDefaultValue::Bool(args.ignore_existence_err).into(),
            ];
            // An older owner appends these at runtime, so they are encoded
            // only once there are indexes to record.
            if !args.index_ids.is_empty() {
                values.push(typed_value_any(
                    builtin_type("[]int64", GoTypeKind::Slice),
                    args.index_ids.clone(),
                ));
                values.push(typed_value_any(
                    builtin_type("[]int64", GoTypeKind::Slice),
                    args.partition_ids.clone(),
                ));
            }
            return GoSharedSlice::from_vec(values);
        }
        GoSharedSlice::from_vec(vec![
            typed_pointer_any(
                model_type("ColumnInfo", GoTypeKind::Struct),
                args.col.clone(),
            ),
            typed_pointer_any(
                ast_type("ColumnPosition", GoTypeKind::Struct),
                args.pos.clone(),
            ),
            ColumnDefaultValue::Int(args.offset).into(),
            ColumnDefaultValue::Bool(args.ignore_existence_err).into(),
        ])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let mut decoder = V1Decoder::new(job)?;
        // Go decodes into a fresh `&ColumnInfo{}` and `&ast.ColumnPosition{}`.
        let args =
            if job.type_ == ActionType::ACTION_DROP_COLUMN || job.state == JobState::ROLLINGBACK {
                let name = GoField::<CiString>::default();
                let ignore = GoField::<bool>::default();
                let index_ids = GoField::<Vec<i64>>::default();
                let partition_ids = GoField::<Vec<i64>>::default();
                decoder.decode(&name, ast_type("CIStr", GoTypeKind::Struct))?;
                decoder.decode(&ignore, builtin_type("bool", GoTypeKind::Bool))?;
                decoder.decode(&index_ids, builtin_type("[]int64", GoTypeKind::Slice))?;
                decoder.decode(&partition_ids, builtin_type("[]int64", GoTypeKind::Slice))?;
                Self {
                    col: Some(column_name_only(name.get())),
                    pos: Some(GoShared::new(tidb_ast::JobColumnPosition::default())),
                    ignore_existence_err: ignore.get(),
                    index_ids: index_ids.get(),
                    partition_ids: partition_ids.get(),
                    ..Self::default()
                }
            } else {
                let col = GoShared::new(ColumnInfo::default());
                let pos = GoShared::new(tidb_ast::JobColumnPosition::default());
                let offset = GoField::<i64>::default();
                let ignore = GoField::<bool>::default();
                decoder.decode_pointee(&col, model_type("ColumnInfo", GoTypeKind::Struct))?;
                decoder.decode_pointee(&pos, ast_type("ColumnPosition", GoTypeKind::Struct))?;
                decoder.decode(&offset, builtin_type("int", GoTypeKind::Int64))?;
                decoder.decode(&ignore, builtin_type("bool", GoTypeKind::Bool))?;
                Self {
                    col: Some(col),
                    pos: Some(pos),
                    offset: offset.get(),
                    ignore_existence_err: ignore.get(),
                    ..Self::default()
                }
            };
        decoder.finish(job);
        Ok(Some(GoShared::new(args)))
    }
}

/// Go `GetTableColumnArgs`.
pub fn get_table_column_args(
    job: &mut Job,
) -> Result<Option<GoShared<TableColumnArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `FillRollBackArgsForAddColumn`: a rolling-back ADD COLUMN carries the
/// arguments a DROP COLUMN job would.
pub fn fill_roll_back_args_for_add_column(job: &mut Job, args: TableColumnArgs) {
    debug_assert_eq!(job.type_, ActionType::ACTION_ADD_COLUMN);
    let mut fake = Job {
        version: job.version,
        type_: ActionType::ACTION_DROP_COLUMN,
        ..Job::default()
    };
    fake.fill_args(Some(GoShared::new(args)));
    // Go `job.args = fakeJob.args`.
    job.set_v1_decoded_args(fake.decoded_args());
}

/// Go `ModifyColumnArgs`: the arguments of MODIFY / CHANGE / RENAME COLUMN.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ModifyColumnArgs {
    /// The column as the statement defines it.
    #[serde(rename = "column", default, skip_serializing_if = "Option::is_none")]
    pub column: Option<GoShared<ColumnInfo>>,
    /// The changed column's id, persisted after the first step.
    #[serde(rename = "old_column_id", default, skip_serializing_if = "is_zero_i64")]
    pub old_column_id: i64,
    /// The changed column's name (Go's `omitempty` keeps a struct).
    #[serde(rename = "old_column_name", default)]
    pub old_column_name: CiString,
    /// Where the column goes.
    #[serde(rename = "position", default, skip_serializing_if = "Option::is_none")]
    pub position: Option<GoShared<tidb_ast::JobColumnPosition>>,
    /// Go `ModifyColumnType` ([`crate::modify_type`]).
    #[serde(
        rename = "modify_column_type",
        default,
        skip_serializing_if = "is_zero_u8"
    )]
    pub modify_column_type: u8,
    /// A changed AUTO_RANDOM column's shard bits.
    #[serde(
        rename = "new_shard_bits",
        default,
        skip_serializing_if = "is_zero_u64"
    )]
    pub new_shard_bits: u64,
    /// The temporary column a data reorganization writes.
    #[serde(
        rename = "changing_column",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub changing_column: Option<GoShared<ColumnInfo>>,
    /// The temporary indexes; never persisted in V2 (`json:"-"`).
    #[serde(skip)]
    pub changing_idxs: Vec<IndexInfo>,
    /// Temporary indexes an earlier job created that this one replaced.
    #[serde(
        rename = "removed_idxs",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub redundant_idxs: Vec<i64>,
    /// Finished: the index ids the delete range removes.
    #[serde(rename = "index_ids", default, skip_serializing_if = "Vec::is_empty")]
    pub index_ids: Vec<i64>,
    /// Finished: the indexes the job created.
    #[serde(
        rename = "new_index_ids",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub new_index_ids: Vec<i64>,
    /// Finished: the partitions.
    #[serde(
        rename = "partition_ids",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub partition_ids: Vec<i64>,
}

impl JobArgs for ModifyColumnArgs {
    job_args_identity_methods!(ModifyColumn);

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let args = value.expect("nil *ModifyColumnArgs receiver").read();
        let mut values = vec![
            typed_pointer_any(
                model_type("ColumnInfo", GoTypeKind::Struct),
                args.column.clone(),
            ),
            typed_value_any(
                ast_type("CIStr", GoTypeKind::Struct),
                args.old_column_name.clone(),
            ),
            typed_pointer_any(
                ast_type("ColumnPosition", GoTypeKind::Struct),
                args.position.clone(),
            ),
            typed_value_any(
                builtin_type("uint8", GoTypeKind::Byte),
                args.modify_column_type,
            ),
            typed_value_any(
                builtin_type("uint64", GoTypeKind::Uint64),
                args.new_shard_bits,
            ),
        ];
        // An older owner expects five, appending the rest at runtime.
        if args.changing_column.is_some() {
            values.push(typed_pointer_any(
                model_type("ColumnInfo", GoTypeKind::Struct),
                args.changing_column.clone(),
            ));
            values.push(typed_value_any(
                builtin_type("[]*model.IndexInfo", GoTypeKind::Slice),
                args.changing_idxs.clone(),
            ));
            values.push(typed_value_any(
                builtin_type("[]int64", GoTypeKind::Slice),
                args.redundant_idxs.clone(),
            ));
            values.push(ColumnDefaultValue::Int(args.old_column_id).into());
        }
        GoSharedSlice::from_vec(values)
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let column = GoField::<Option<GoShared<ColumnInfo>>>::default();
        let old_column_name = GoField::<CiString>::default();
        let position = GoField::<Option<GoShared<tidb_ast::JobColumnPosition>>>::default();
        let modify_column_type = GoField::<u8>::default();
        let new_shard_bits = GoField::<u64>::default();
        let changing_column = GoField::<Option<GoShared<ColumnInfo>>>::default();
        let changing_idxs = GoField::<Vec<IndexInfo>>::default();
        let redundant_idxs = GoField::<Vec<i64>>::default();
        let old_column_id = GoField::<i64>::default();
        let mut decoder = V1Decoder::new(job)?;
        decoder.decode(
            &column,
            model_type("ColumnInfo", GoTypeKind::Struct).pointer_to(),
        )?;
        decoder.decode(&old_column_name, ast_type("CIStr", GoTypeKind::Struct))?;
        decoder.decode(
            &position,
            ast_type("ColumnPosition", GoTypeKind::Struct).pointer_to(),
        )?;
        decoder.decode(&modify_column_type, builtin_type("uint8", GoTypeKind::Byte))?;
        decoder.decode(&new_shard_bits, builtin_type("uint64", GoTypeKind::Uint64))?;
        decoder.decode(
            &changing_column,
            model_type("ColumnInfo", GoTypeKind::Struct).pointer_to(),
        )?;
        decoder.decode(
            &changing_idxs,
            builtin_type("[]*model.IndexInfo", GoTypeKind::Slice),
        )?;
        decoder.decode(&redundant_idxs, builtin_type("[]int64", GoTypeKind::Slice))?;
        decoder.decode(&old_column_id, builtin_type("int64", GoTypeKind::Int64))?;
        decoder.finish(job);
        Ok(Some(GoShared::new(Self {
            column: column.get(),
            old_column_name: old_column_name.get(),
            position: position.get(),
            modify_column_type: modify_column_type.get(),
            new_shard_bits: new_shard_bits.get(),
            changing_column: changing_column.get(),
            changing_idxs: changing_idxs.get(),
            redundant_idxs: redundant_idxs.get(),
            old_column_id: old_column_id.get(),
            ..Self::default()
        })))
    }
}

impl FinishedJobArgs for ModifyColumnArgs {
    fn get_finished_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let args = value.expect("nil *ModifyColumnArgs receiver").read();
        GoSharedSlice::from_vec(vec![
            typed_value_any(
                builtin_type("[]int64", GoTypeKind::Slice),
                args.index_ids.clone(),
            ),
            typed_value_any(
                builtin_type("[]int64", GoTypeKind::Slice),
                args.partition_ids.clone(),
            ),
            typed_value_any(
                builtin_type("[]int64", GoTypeKind::Slice),
                args.new_index_ids.clone(),
            ),
        ])
    }
}

/// Go `GetModifyColumnArgs`.
pub fn get_modify_column_args(
    job: &mut Job,
) -> Result<Option<GoShared<ModifyColumnArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}

/// Go `GetFinishedModifyColumnArgs`.
pub fn get_finished_modify_column_args(
    job: &mut Job,
) -> Result<Option<GoShared<ModifyColumnArgs>>, serde_json::Error> {
    if job.version != JobVersion::V1 {
        return get_or_decode_args_v2(job);
    }
    let index_ids = GoField::<Vec<i64>>::default();
    let partition_ids = GoField::<Vec<i64>>::default();
    let new_index_ids = GoField::<Vec<i64>>::default();
    let mut decoder = V1Decoder::new(job)?;
    decoder.decode(&index_ids, builtin_type("[]int64", GoTypeKind::Slice))?;
    decoder.decode(&partition_ids, builtin_type("[]int64", GoTypeKind::Slice))?;
    decoder.decode(&new_index_ids, builtin_type("[]int64", GoTypeKind::Slice))?;
    decoder.finish(job);
    Ok(Some(GoShared::new(ModifyColumnArgs {
        index_ids: index_ids.get(),
        partition_ids: partition_ids.get(),
        new_index_ids: new_index_ids.get(),
        ..ModifyColumnArgs::default()
    })))
}

#[cfg(test)]
mod tests {
    use super::super::tests::{encoded_finished_job, encoded_job};
    use super::*;

    const EMPTY_COLUMN_TAIL: &str = r#""offset":0,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":0,"Flag":0,"Flen":0,"Decimal":0,"Charset":"","Collate":"","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":0,"comment":"","hidden":false,"change_state_info":null,"version":0}"#;

    fn column(id: i64, name: &str) -> GoShared<ColumnInfo> {
        GoShared::new(ColumnInfo {
            id,
            name: CiString::new(name),
            ..ColumnInfo::default()
        })
    }

    fn raw(job: &Job) -> String {
        job.raw_args.as_ref().unwrap().get().to_owned()
    }

    /// The wire shapes Go's `Job.Encode` writes for these arguments,
    /// captured from the Go oracle.
    #[test]
    fn column_args_encode_like_go() {
        let after_a = || {
            GoShared::new(tidb_ast::JobColumnPosition::from_statement(
                &tidb_ast::ColumnPosition::After("a".to_owned()),
            ))
        };
        let add = |version| {
            encoded_job(
                version,
                ActionType::ACTION_ADD_COLUMN,
                GoShared::new(TableColumnArgs {
                    col: Some(column(3, "D")),
                    pos: Some(after_a()),
                    ignore_existence_err: true,
                    ..TableColumnArgs::default()
                }),
            )
        };
        let position = r#"{"Tp":2,"RelativeColumn":{"Schema":{"O":"","L":""},"Table":{"O":"","L":""},"Name":{"O":"a","L":"a"}}}"#;
        let added = format!(r#"{{"id":3,"name":{{"O":"D","L":"d"}},{EMPTY_COLUMN_TAIL}"#);
        assert_eq!(
            raw(&add(JobVersion::V1)),
            format!("[{added},{position},0,true]")
        );
        assert_eq!(
            raw(&add(JobVersion::V2)),
            format!(
                r#"{{"column_info":{added},"position":{position},"ignore_existence_err":true}}"#
            )
        );

        let drop = |version, index_ids: Vec<i64>| {
            encoded_job(
                version,
                ActionType::ACTION_DROP_COLUMN,
                GoShared::new(TableColumnArgs {
                    col: Some(column(0, "B")),
                    ignore_existence_err: index_ids.is_empty(),
                    partition_ids: if index_ids.is_empty() {
                        Vec::new()
                    } else {
                        vec![7]
                    },
                    index_ids,
                    ..TableColumnArgs::default()
                }),
            )
        };
        assert_eq!(
            raw(&drop(JobVersion::V1, Vec::new())),
            r#"[{"O":"B","L":"b"},true]"#
        );
        assert_eq!(
            raw(&drop(JobVersion::V1, vec![1, 2])),
            r#"[{"O":"B","L":"b"},false,[1,2],[7]]"#
        );
        let dropped = format!(r#"{{"id":0,"name":{{"O":"B","L":"b"}},{EMPTY_COLUMN_TAIL}"#);
        assert_eq!(
            raw(&drop(JobVersion::V2, Vec::new())),
            format!(r#"{{"column_info":{dropped},"ignore_existence_err":true}}"#)
        );
        assert_eq!(
            raw(&drop(JobVersion::V2, vec![1, 2])),
            format!(r#"{{"column_info":{dropped},"index_ids":[1,2],"partition_ids":[7]}}"#)
        );

        let modify = |version| {
            encoded_job(
                version,
                ActionType::ACTION_MODIFY_COLUMN,
                GoShared::new(ModifyColumnArgs {
                    column: Some(column(2, "c")),
                    old_column_name: CiString::new("b"),
                    position: Some(GoShared::new(tidb_ast::JobColumnPosition::from_statement(
                        &tidb_ast::ColumnPosition::First,
                    ))),
                    modify_column_type: crate::job_enums::modify_type::NO_REORG,
                    ..ModifyColumnArgs::default()
                }),
            )
        };
        let modified = format!(r#"{{"id":2,"name":{{"O":"c","L":"c"}},{EMPTY_COLUMN_TAIL}"#);
        let first = r#"{"Tp":1,"RelativeColumn":null}"#;
        assert_eq!(
            raw(&modify(JobVersion::V1)),
            format!(r#"[{modified},{{"O":"b","L":"b"}},{first},1,0]"#)
        );
        assert_eq!(
            raw(&modify(JobVersion::V2)),
            format!(
                r#"{{"column":{modified},"old_column_name":{{"O":"b","L":"b"}},"position":{first},"modify_column_type":1}}"#
            )
        );

        let finished = |version| {
            encoded_finished_job(
                version,
                ActionType::ACTION_MODIFY_COLUMN,
                GoShared::new(ModifyColumnArgs {
                    index_ids: vec![4],
                    partition_ids: vec![8],
                    new_index_ids: vec![9],
                    ..ModifyColumnArgs::default()
                }),
            )
        };
        assert_eq!(raw(&finished(JobVersion::V1)), "[[4],[8],[9]]");
        assert_eq!(
            raw(&finished(JobVersion::V2)),
            r#"{"old_column_name":{"O":"","L":""},"index_ids":[4],"new_index_ids":[9],"partition_ids":[8]}"#
        );
    }

    /// What the delete range and the owner read back.
    #[test]
    fn column_args_decode_what_they_encode() {
        for version in [JobVersion::V1, JobVersion::V2] {
            let mut job = encoded_job(
                version,
                ActionType::ACTION_DROP_COLUMN,
                GoShared::new(TableColumnArgs {
                    col: Some(column(0, "B")),
                    index_ids: vec![1, 2],
                    partition_ids: vec![7],
                    ..TableColumnArgs::default()
                }),
            );
            let args = get_table_column_args(&mut job).unwrap().unwrap();
            let args = args.read();
            assert_eq!(args.col_name().original(), "B");
            assert_eq!(
                (args.index_ids.clone(), args.partition_ids.clone()),
                (vec![1, 2], vec![7])
            );

            let mut job = encoded_finished_job(
                version,
                ActionType::ACTION_MODIFY_COLUMN,
                GoShared::new(ModifyColumnArgs {
                    index_ids: vec![4],
                    partition_ids: vec![8],
                    new_index_ids: vec![9],
                    ..ModifyColumnArgs::default()
                }),
            );
            let args = get_finished_modify_column_args(&mut job).unwrap().unwrap();
            let args = args.read();
            assert_eq!(
                (
                    args.index_ids.clone(),
                    args.partition_ids.clone(),
                    args.new_index_ids.clone()
                ),
                (vec![4], vec![8], vec![9])
            );
        }
        // A rolling-back ADD COLUMN carries drop-shaped arguments.
        for version in [JobVersion::V1, JobVersion::V2] {
            let mut job = Job {
                version,
                type_: ActionType::ACTION_ADD_COLUMN,
                ..Job::default()
            };
            fill_roll_back_args_for_add_column(
                &mut job,
                TableColumnArgs {
                    col: Some(column(0, "d")),
                    ..TableColumnArgs::default()
                },
            );
            job.state = JobState::ROLLINGBACK;
            let bytes = job.encode(true).unwrap();
            let mut decoded = Job::default();
            decoded.decode(&bytes).unwrap();
            if version == JobVersion::V1 {
                assert_eq!(raw(&decoded), r#"[{"O":"d","L":"d"},false]"#);
            }
            let args = get_table_column_args(&mut decoded).unwrap().unwrap();
            assert_eq!(args.read().col_name().original(), "d");
        }
    }
}
