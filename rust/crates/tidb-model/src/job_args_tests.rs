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

use super::*;
use crate::ColumnarIndexType;

pub(crate) fn encoded_job<T: JobArgs>(
    version: JobVersion,
    action: ActionType,
    value: GoShared<T>,
) -> Job {
    let mut job = Job {
        version,
        type_: action,
        ..Default::default()
    };
    job.fill_args(Some(value));
    let bytes = job.encode(true).expect("encode job arguments");
    let mut decoded = Job::default();
    decoded.decode(&bytes).expect("decode job envelope");
    decoded
}

pub(crate) fn encoded_finished_job<T: FinishedJobArgs>(
    version: JobVersion,
    action: ActionType,
    value: GoShared<T>,
) -> Job {
    let mut job = Job {
        version,
        type_: action,
        ..Default::default()
    };
    job.fill_finished_args(Some(value));
    let bytes = job.encode(true).expect("encode finished job arguments");
    let mut decoded = Job::default();
    decoded
        .decode(&bytes)
        .expect("decode finished job envelope");
    decoded
}

#[test]
pub(crate) fn v2_getter_reuses_the_exact_typed_pointer() {
    let database = GoShared::new(DBInfo::default());
    let args = GoShared::new(CreateSchemaArgs {
        db_info: GoField::new(Some(database.clone())),
    });
    let mut job = Job {
        version: JobVersion::V2,
        ..Default::default()
    };
    job.fill_args(Some(args.clone()));

    let fetched = get_create_schema_args(&mut job)
        .expect("cached V2 args")
        .expect("non-nil CreateSchemaArgs");
    assert!(fetched.ptr_eq(&args));
    assert!(fetched
        .read()
        .db_info
        .get()
        .expect("DBInfo pointer")
        .ptr_eq(&database));
    assert_eq!(
        job.decoded_args()
            .get(0)
            .dynamic_type()
            .expect("typed pointer")
            .display_name(),
        "*model.CreateSchemaArgs"
    );

    let wrong = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        get_drop_schema_args(&mut job)
    }));
    assert!(wrong.is_err(), "Go type assertion must panic");

    job.encode(true).expect("persist V2 typed args");
    job.clear_decoded_args();
    let rebuilt = get_create_schema_args(&mut job)
        .expect("decode V2 raw args")
        .expect("non-nil rebuilt args");
    assert!(!rebuilt.ptr_eq(&args));
}

#[test]
fn v2_null_retains_a_non_nil_interface_with_a_typed_nil_pointer() {
    let mut job = Job {
        version: JobVersion::V2,
        raw_args: Some(crate::PersistedRawJson::from_bytes(b"null".to_vec())),
        ..Default::default()
    };
    assert!(get_create_schema_args(&mut job)
        .expect("typed null decode")
        .is_none());
    let cached = job.decoded_args().get(0);
    assert!(!cached.is_nil(), "typed nil is a non-nil interface");
    assert_eq!(
        cached.dynamic_type().unwrap().display_name(),
        "*model.CreateSchemaArgs"
    );
    assert!(get_create_schema_args(&mut job)
        .expect("cached typed null")
        .is_none());
}

#[test]
fn create_schema_v1_and_v2_round_trip_their_distinct_wire_shapes() {
    for version in [JobVersion::V1, JobVersion::V2] {
        let database = GoShared::new(DBInfo::default());
        let args = GoShared::new(CreateSchemaArgs {
            db_info: GoField::new(Some(database)),
        });
        let mut decoded = encoded_job(version, ActionType::ACTION_CREATE_SCHEMA, args);
        let raw = decoded.raw_args.as_ref().unwrap().get();
        if version == JobVersion::V1 {
            assert!(raw.starts_with("[{"), "{raw}");
        } else {
            assert!(raw.starts_with("{"), "{raw}");
        }

        let fetched = get_create_schema_args(&mut decoded)
            .expect("decode typed schema args")
            .expect("non-nil CreateSchemaArgs");
        assert!(fetched.read().db_info.get().is_some());
        if version == JobVersion::V1 {
            assert_eq!(decoded.decoded_args().len(), 1);
            assert_eq!(
                decoded
                    .decoded_args()
                    .get(0)
                    .dynamic_type()
                    .unwrap()
                    .display_name(),
                "*model.DBInfo"
            );
        } else {
            assert_eq!(decoded.decoded_args().len(), 1);
        }
    }
}

#[test]
fn v1_decode_uses_minimum_length_and_accepts_null_argument_arrays() {
    let mut null = Job {
        version: JobVersion::V1,
        raw_args: Some(crate::PersistedRawJson::from_bytes(b"null".to_vec())),
        ..Default::default()
    };
    let args = get_create_schema_args(&mut null)
        .expect("null V1 args")
        .expect("allocated receiver");
    assert!(args.read().db_info.get().is_some());
    assert!(!null.decoded_args().is_allocated());

    let mut extra = Job {
        version: JobVersion::V1,
        raw_args: Some(crate::PersistedRawJson::from_bytes(
            br#"[null,true]"#.to_vec(),
        )),
        ..Default::default()
    };
    let args = get_create_schema_args(&mut extra)
        .expect("extra V1 args")
        .expect("allocated receiver");
    assert!(args.read().db_info.get().is_some());
    assert_eq!(
        extra.decoded_args().len(),
        1,
        "extra raw values are ignored"
    );
}

#[test]
fn drop_schema_submission_and_finished_shapes_are_independent() {
    let args = GoShared::new(DropSchemaArgs {
        fk_check: GoField::new(true),
        all_dropped_table_ids: GoField::new(GoSharedSlice::from_vec_with_capacity(vec![3, 5], 4)),
    });
    let mut submission = Job {
        version: JobVersion::V1,
        ..Default::default()
    };
    submission.fill_args(Some(args.clone()));
    submission.encode(true).unwrap();
    assert_eq!(submission.raw_args.as_ref().unwrap().get(), "[true]");

    let mut finished = Job {
        version: JobVersion::V1,
        ..Default::default()
    };
    finished.fill_finished_args(Some(args));
    finished.encode(true).unwrap();
    assert_eq!(finished.raw_args.as_ref().unwrap().get(), "[[3,5]]");

    let bytes = finished.encode(false).unwrap();
    let mut decoded = Job::default();
    decoded.decode(&bytes).unwrap();
    let decoded = get_finished_drop_schema_args(&mut decoded)
        .unwrap()
        .unwrap();
    assert_eq!(
        decoded.read().all_dropped_table_ids.get().snapshot(),
        [3, 5]
    );
}

#[test]
fn modify_schema_v1_switches_only_on_charset_action() {
    let policy = GoShared::new(PolicyRefInfo::default());
    let args = GoShared::new(ModifySchemaArgs {
        to_charset: GoField::new(GoString::from("utf8mb4")),
        to_collate: GoField::new(GoString::from("utf8mb4_bin")),
        policy_ref: GoField::new(Some(policy)),
    });
    let mut charset = Job {
        version: JobVersion::V1,
        type_: ActionType::ACTION_MODIFY_SCHEMA_CHARSET_AND_COLLATE,
        ..Default::default()
    };
    charset.fill_args(Some(args.clone()));
    charset.encode(true).unwrap();
    assert_eq!(
        charset.raw_args.as_ref().unwrap().get(),
        r#"["utf8mb4","utf8mb4_bin"]"#
    );

    let mut placement = Job {
        version: JobVersion::V1,
        type_: ActionType::ACTION_MODIFY_SCHEMA_DEFAULT_PLACEMENT,
        ..Default::default()
    };
    placement.fill_args(Some(args));
    placement.encode(true).unwrap();
    assert!(placement.raw_args.as_ref().unwrap().get().starts_with("[{"));
}

#[test]
fn create_table_v1_action_matrix_matches_source_switch() {
    let args = GoShared::new(CreateTableArgs {
        table_info: GoField::new(Some(GoShared::new(TableInfo::default()))),
        on_exist_replace: GoField::new(true),
        old_view_table_id: GoField::new(91),
        fk_check: GoField::new(true),
    });
    let rows = [
        (ActionType::ACTION_CREATE_TABLE, 2_usize),
        (ActionType::ACTION_CREATE_VIEW, 3),
        (ActionType::ACTION_CREATE_SEQUENCE, 1),
    ];
    for (action, expected_len) in rows {
        let mut job = Job {
            version: JobVersion::V1,
            type_: action,
            ..Default::default()
        };
        job.fill_args(Some(args.clone()));
        assert_eq!(job.decoded_args().len(), expected_len);
        job.encode(true).unwrap();
        let raw: Vec<Box<RawValue>> =
            serde_json::from_str(&job.raw_args.as_ref().unwrap().get()).unwrap();
        assert_eq!(raw.len(), expected_len);
    }

    let mut unknown = Job {
        version: JobVersion::V1,
        type_: ActionType(u8::MAX),
        ..Default::default()
    };
    unknown.fill_args(Some(args));
    assert!(!unknown.decoded_args().is_allocated());
    unknown.encode(true).unwrap();
    assert_eq!(unknown.raw_args.as_ref().unwrap().get(), "null");
}

#[test]
pub(crate) fn batch_create_table_v1_shares_one_fk_flag_and_v2_keeps_each_value() {
    let tables = GoSharedPointerSlice::from_handles(vec![
        Some(GoShared::new(CreateTableArgs {
            table_info: GoField::new(Some(GoShared::new(TableInfo {
                id: 100,
                ..Default::default()
            }))),
            fk_check: GoField::new(true),
            ..Default::default()
        })),
        Some(GoShared::new(CreateTableArgs {
            table_info: GoField::new(Some(GoShared::new(TableInfo {
                id: 101,
                ..Default::default()
            }))),
            fk_check: GoField::new(false),
            ..Default::default()
        })),
    ]);
    let args = GoShared::new(BatchCreateTableArgs {
        tables: GoField::new(tables),
    });

    for version in [JobVersion::V1, JobVersion::V2] {
        let mut job = encoded_job(version, ActionType::ACTION_CREATE_TABLES, args.clone());
        let decoded = get_batch_create_table_args(&mut job).unwrap().unwrap();
        let decoded = decoded.read().tables.get();
        assert_eq!(decoded.len(), 2);
        assert_eq!(
            decoded
                .get(0)
                .unwrap()
                .read()
                .table_info
                .get()
                .unwrap()
                .read()
                .id,
            100
        );
        assert_eq!(
            decoded
                .get(1)
                .unwrap()
                .read()
                .table_info
                .get()
                .unwrap()
                .read()
                .id,
            101
        );
        assert!(decoded.get(0).unwrap().read().fk_check.get());
        assert_eq!(
            decoded.get(1).unwrap().read().fk_check.get(),
            version == JobVersion::V1
        );
    }

    for tables in [
        GoSharedPointerSlice::<CreateTableArgs>::from_handles(vec![]),
        GoSharedPointerSlice::from_handles(vec![None]),
    ] {
        let args = GoShared::new(BatchCreateTableArgs {
            tables: GoField::new(tables),
        });
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut job = Job {
                version: JobVersion::V1,
                type_: ActionType::ACTION_CREATE_TABLES,
                ..Default::default()
            };
            job.fill_args(Some(args));
        }));
        assert!(panic.is_err());
    }
}

#[test]
pub(crate) fn truncate_table_submission_and_finished_action_matrix_matches_source() {
    let args = GoShared::new(TruncateTableArgs {
        new_table_id: GoField::new(1),
        fk_check: GoField::new(true),
        old_partition_ids: GoField::new(GoSharedSlice::from_vec(vec![11, 2])),
        new_partition_ids: GoField::new(GoSharedSlice::from_vec(vec![2, 3])),
        ..Default::default()
    });
    for action in [
        ActionType::ACTION_TRUNCATE_TABLE,
        ActionType::ACTION_TRUNCATE_TABLE_PARTITION,
    ] {
        for version in [JobVersion::V1, JobVersion::V2] {
            let mut job = encoded_job(version, action, args.clone());
            let decoded = get_truncate_table_args(&mut job).unwrap().unwrap();
            let decoded = decoded.read();
            if action == ActionType::ACTION_TRUNCATE_TABLE {
                assert_eq!(decoded.new_table_id.get(), 1);
                assert!(decoded.fk_check.get());
            } else {
                assert_eq!(decoded.old_partition_ids.get().snapshot(), [11, 2]);
            }
            assert_eq!(decoded.new_partition_ids.get().snapshot(), [2, 3]);
        }
    }

    let finished = GoShared::new(TruncateTableArgs {
        old_partition_ids: GoField::new(GoSharedSlice::from_vec(vec![5, 6])),
        ..Default::default()
    });
    for action in [
        ActionType::ACTION_TRUNCATE_TABLE,
        ActionType::ACTION_TRUNCATE_TABLE_PARTITION,
    ] {
        for version in [JobVersion::V1, JobVersion::V2] {
            let mut job = encoded_finished_job(version, action, finished.clone());
            if version == JobVersion::V1 && action == ActionType::ACTION_TRUNCATE_TABLE {
                assert_eq!(job.raw_args.as_ref().unwrap().get(), r#"["",[5,6]]"#);
            }
            let decoded = get_finished_truncate_table_args(&mut job).unwrap().unwrap();
            assert_eq!(decoded.read().old_partition_ids.get().snapshot(), [5, 6]);
        }
    }
}

fn table_partition_names(value: &TablePartitionArgs) -> Vec<Vec<u8>> {
    value
        .part_names
        .get()
        .snapshot()
        .into_iter()
        .map(|name| name.as_bytes().to_vec())
        .collect()
}

fn assert_table_partition_submission_action_matrix_matches_source() {
    let args = GoShared::new(TablePartitionArgs {
        part_names: GoField::new(GoSharedSlice::from_vec(vec![
            GoString::from("a"),
            GoString::from("b"),
        ])),
        part_info: GoField::new(Some(GoShared::new(PartitionInfo {
            new_table_id: 91,
            ..Default::default()
        }))),
        new_partition_ids: GoField::new(GoSharedSlice::from_vec(vec![7, 8])),
        ..Default::default()
    });
    for action in [
        ActionType::ACTION_ALTER_TABLE_PARTITIONING,
        ActionType::ACTION_REMOVE_PARTITIONING,
        ActionType::ACTION_REORGANIZE_PARTITION,
        ActionType::ACTION_ADD_TABLE_PARTITION,
        ActionType::ACTION_DROP_TABLE_PARTITION,
    ] {
        for version in [JobVersion::V1, JobVersion::V2] {
            let mut job = encoded_job(version, action, args.clone());
            assert!(
                !job.raw_args
                    .as_ref()
                    .unwrap()
                    .get()
                    .contains("new_partition_ids"),
                "runtime-only field entered persisted JSON"
            );
            let decoded = get_table_partition_args(&mut job).unwrap().unwrap();
            let decoded = decoded.read();
            let names = table_partition_names(&decoded);
            let info_id = decoded
                .part_info
                .get()
                .expect("GetTablePartitionArgs repairs PartInfo")
                .read()
                .new_table_id;
            if version == JobVersion::V2 {
                assert_eq!(names, [b"a".to_vec(), b"b".to_vec()]);
                assert_eq!(info_id, 91);
            } else {
                if action == ActionType::ACTION_ADD_TABLE_PARTITION {
                    assert!(names.is_empty());
                } else {
                    assert_eq!(names, [b"a".to_vec(), b"b".to_vec()]);
                }
                if action == ActionType::ACTION_DROP_TABLE_PARTITION {
                    assert_eq!(info_id, 0);
                } else {
                    assert_eq!(info_id, 91);
                }
            }
        }
    }

    let missing_info = GoShared::new(TablePartitionArgs {
        part_names: GoField::new(GoSharedSlice::from_vec(vec![GoString::from("a")])),
        ..Default::default()
    });
    let mut job = encoded_job(
        JobVersion::V2,
        ActionType::ACTION_DROP_TABLE_PARTITION,
        missing_info,
    );
    assert!(job.raw_args.as_ref().unwrap().get().contains("part_names"));
    assert!(!job.raw_args.as_ref().unwrap().get().contains("part_info"));
    assert!(get_table_partition_args(&mut job)
        .unwrap()
        .unwrap()
        .read()
        .part_info
        .get()
        .is_some());

    let nil = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut job = Job {
            version: JobVersion::V2,
            raw_args: Some(crate::PersistedRawJson::from_bytes(b"null".to_vec())),
            ..Default::default()
        };
        get_table_partition_args(&mut job)
    }));
    assert!(nil.is_err(), "Go dereferences the decoded nil receiver");
}

fn assert_add_partition_rollback_reencodes_drop_shape_and_shares_names() {
    for version in [JobVersion::V1, JobVersion::V2] {
        let mut job = Job {
            version,
            type_: ActionType::ACTION_ADD_TABLE_PARTITION,
            ..Default::default()
        };
        job.fill_args(Some(GoShared::new(TablePartitionArgs::default())));
        job.encode(true).unwrap();

        let names = GoSharedSlice::from_vec(vec![GoString::from("aaaa"), GoString::from("bbb")]);
        let rollback = GoShared::new(TablePartitionArgs {
            part_names: GoField::new(names.clone()),
            part_info: GoField::new(Some(GoShared::new(PartitionInfo {
                new_table_id: 123,
                ..Default::default()
            }))),
            ..Default::default()
        });
        fill_rollback_args_for_add_partition(&mut job, Some(&rollback));
        assert_eq!(job.decoded_args().len(), 1);
        assert_eq!(
            job.decoded_args()
                .get(0)
                .dynamic_type()
                .unwrap()
                .display_name(),
            if version == JobVersion::V1 {
                "[]string"
            } else {
                "*model.TablePartitionArgs"
            }
        );

        names.set(0, GoString::from("changed"));
        job.state = JobState::ROLLINGBACK;
        let bytes = job.encode(true).unwrap();
        if version == JobVersion::V1 {
            assert_eq!(
                job.raw_args.as_ref().unwrap().get(),
                r#"[["changed","bbb"]]"#
            );
        } else {
            assert_eq!(
                job.raw_args.as_ref().unwrap().get(),
                r#"{"part_names":["changed","bbb"]}"#
            );
        }
        let mut decoded = Job::default();
        decoded.decode(&bytes).unwrap();
        let decoded = get_table_partition_args(&mut decoded).unwrap().unwrap();
        let decoded = decoded.read();
        assert_eq!(
            table_partition_names(&decoded),
            [b"changed".to_vec(), b"bbb".to_vec()]
        );
        assert_eq!(decoded.part_info.get().unwrap().read().new_table_id, 0);
    }

    let wrong_action = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut job = Job {
            version: JobVersion::V1,
            type_: ActionType::ACTION_DROP_TABLE_PARTITION,
            ..Default::default()
        };
        let args = GoShared::new(TablePartitionArgs::default());
        fill_rollback_args_for_add_partition(&mut job, Some(&args));
    }));
    assert!(wrong_action.is_err());
}

#[test]
pub(crate) fn table_partition_args_match_source() {
    assert_table_partition_submission_action_matrix_matches_source();
    assert_add_partition_rollback_reencodes_drop_shape_and_shares_names();
}

#[test]
pub(crate) fn finished_table_partition_matrix_and_add_assertion_match_source() {
    let args = GoShared::new(TablePartitionArgs {
        old_physical_table_ids: GoField::new(GoSharedSlice::from_vec(vec![1, 2])),
        old_global_indexes: GoField::new(GoSharedSlice::from_vec(vec![TableIDIndexID {
            table_id: 3,
            index_id: 4,
        }])),
        ..Default::default()
    });
    for action in [
        ActionType::ACTION_ALTER_TABLE_PARTITIONING,
        ActionType::ACTION_REMOVE_PARTITIONING,
        ActionType::ACTION_REORGANIZE_PARTITION,
        ActionType::ACTION_DROP_TABLE_PARTITION,
    ] {
        for version in [JobVersion::V1, JobVersion::V2] {
            let mut job = encoded_finished_job(version, action, args.clone());
            let decoded = get_finished_table_partition_args(&mut job)
                .unwrap()
                .unwrap();
            assert_eq!(
                decoded.read().old_physical_table_ids.get().snapshot(),
                [1, 2]
            );
            assert_eq!(
                decoded.read().old_global_indexes.get().snapshot(),
                [TableIDIndexID {
                    table_id: 3,
                    index_id: 4
                }]
            );
            if version == JobVersion::V1 {
                assert_eq!(
                    job.decoded_args()
                        .get(1)
                        .dynamic_type()
                        .unwrap()
                        .display_name(),
                    "*[]model.TableIDIndexID"
                );
            }
        }
    }

    for version in [JobVersion::V1, JobVersion::V2] {
        let mut job = Job {
            version,
            type_: ActionType::ACTION_ADD_TABLE_PARTITION,
            state: JobState::ROLLBACK_DONE,
            ..Default::default()
        };
        job.fill_finished_args(Some(args.clone()));
        let bytes = job.encode(true).unwrap();
        let mut decoded = Job::default();
        decoded.decode(&bytes).unwrap();
        assert_eq!(
            get_finished_table_partition_args(&mut decoded)
                .unwrap()
                .unwrap()
                .read()
                .old_physical_table_ids
                .get()
                .snapshot(),
            [1, 2]
        );
    }

    let invalid_v1 = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut job = Job {
            version: JobVersion::V1,
            type_: ActionType::ACTION_ADD_TABLE_PARTITION,
            state: JobState::DONE,
            ..Default::default()
        };
        job.fill_finished_args(Some(args.clone()));
    }));
    assert!(invalid_v1.is_err());

    let mut v2 = Job {
        version: JobVersion::V2,
        type_: ActionType::ACTION_ADD_TABLE_PARTITION,
        state: JobState::DONE,
        ..Default::default()
    };
    v2.fill_finished_args(Some(args));
    assert_eq!(v2.decoded_args().len(), 1);

    assert_eq!(
        serde_json::to_string(&TableIDIndexID::default()).unwrap(),
        r#"{"TableID":0,"IndexID":0}"#
    );
}

#[test]
pub(crate) fn exchange_table_partition_args_match_source() {
    let args = GoShared::new(ExchangeTablePartitionArgs {
        partition_id: GoField::new(100),
        partitioned_table_schema_id: GoField::new(123),
        partitioned_table_id: GoField::new(345),
        partition_name: GoField::new(GoString::from("c")),
        with_validation: GoField::new(true),
    });
    for version in [JobVersion::V1, JobVersion::V2] {
        let mut job = encoded_job(
            version,
            ActionType::ACTION_EXCHANGE_TABLE_PARTITION,
            args.clone(),
        );
        assert_eq!(
            job.raw_args.as_ref().unwrap().get(),
            if version == JobVersion::V1 {
                r#"[100,123,345,"c",true]"#
            } else {
                r#"{"partition_id":100,"pt_schema_id":123,"pt_table_id":345,"partition_name":"c","with_validation":true}"#
            }
        );
        let decoded = get_exchange_table_partition_args(&mut job)
            .unwrap()
            .unwrap();
        let decoded = decoded.read();
        assert_eq!(decoded.partition_id.get(), 100);
        assert_eq!(decoded.partitioned_table_schema_id.get(), 123);
        assert_eq!(decoded.partitioned_table_id.get(), 345);
        assert_eq!(decoded.partition_name.get().as_bytes(), b"c");
        assert!(decoded.with_validation.get());
    }

    assert_eq!(
        crate::serde_helpers::to_go_json(&ExchangeTablePartitionArgs::default()).unwrap(),
        b"{}"
    );
    let mut typed_nil = Job {
        version: JobVersion::V2,
        raw_args: Some(crate::PersistedRawJson::from_bytes(b"null".to_vec())),
        ..Default::default()
    };
    assert!(get_exchange_table_partition_args(&mut typed_nil)
        .unwrap()
        .is_none());
}

#[test]
pub(crate) fn alter_table_partition_args_match_source() {
    let label_rule = GoShared::new(serde_json::json!({
        "id": "ss",
        "index": 0,
        "labels": null,
        "rule_type": "",
        "data": null,
    }));
    let policy_ref_info = GoShared::new(PolicyRefInfo {
        id: 462,
        ..Default::default()
    });
    let args = GoShared::new(AlterTablePartitionArgs {
        partition_id: GoField::new(123),
        label_rule: GoField::new(Some(label_rule.clone())),
        policy_ref_info: GoField::new(Some(policy_ref_info.clone())),
    });

    for action in [
        ActionType::ACTION_ALTER_TABLE_PARTITION_ATTRIBUTES,
        ActionType::ACTION_ALTER_TABLE_PARTITION_PLACEMENT,
    ] {
        for version in [JobVersion::V1, JobVersion::V2] {
            let mut job = encoded_job(version, action, args.clone());
            let decoded = get_alter_table_partition_args(&mut job).unwrap().unwrap();
            let decoded = decoded.read();
            assert_eq!(decoded.partition_id.get(), 123);
            if action == ActionType::ACTION_ALTER_TABLE_PARTITION_ATTRIBUTES {
                let actual = decoded.label_rule.get();
                let actual = actual.as_ref().unwrap();
                assert_eq!(*actual.read(), *label_rule.read());
            } else {
                let actual = decoded.policy_ref_info.get();
                let actual = actual.as_ref().unwrap();
                assert_eq!(*actual.read(), *policy_ref_info.read());
            }
        }
    }
}

#[test]
pub(crate) fn rebase_auto_id_args_match_source_matrix() {
    let args = GoShared::new(RebaseAutoIDArgs {
        new_base: GoField::new(9527),
        force: GoField::new(true),
    });
    for action in [
        ActionType::ACTION_REBASE_AUTO_ID,
        ActionType::ACTION_REBASE_AUTO_RANDOM_BASE,
    ] {
        for version in [JobVersion::V1, JobVersion::V2] {
            let mut job = encoded_job(version, action, args.clone());
            assert_eq!(
                job.raw_args.as_ref().unwrap().get(),
                if version == JobVersion::V1 {
                    r#"[9527,true]"#
                } else {
                    r#"{"new_base":9527,"force":true}"#
                }
            );
            let decoded = get_rebase_auto_id_args(&mut job).unwrap().unwrap();
            assert_eq!(decoded.read().new_base.get(), 9527);
            assert!(decoded.read().force.get());
        }
    }
}

#[test]
pub(crate) fn modify_table_comment_args_match_source_matrix() {
    let args = GoShared::new(ModifyTableCommentArgs {
        comment: GoField::new(GoString::from("TiDB is great")),
    });
    for version in [JobVersion::V1, JobVersion::V2] {
        let mut job = encoded_job(
            version,
            ActionType::ACTION_MODIFY_TABLE_COMMENT,
            args.clone(),
        );
        assert_eq!(
            job.raw_args.as_ref().unwrap().get(),
            if version == JobVersion::V1 {
                r#"["TiDB is great"]"#
            } else {
                r#"{"comment":"TiDB is great"}"#
            }
        );
        assert_eq!(
            get_modify_table_comment_args(&mut job)
                .unwrap()
                .unwrap()
                .read()
                .comment
                .get()
                .as_bytes(),
            b"TiDB is great"
        );
    }
}

#[test]
pub(crate) fn modify_table_charset_and_collate_args_pin_every_field() {
    let args = GoShared::new(ModifyTableCharsetAndCollateArgs {
        to_charset: GoField::new(GoString::from("utf8mb4")),
        to_collate: GoField::new(GoString::from("utf8mb4_bin")),
        needs_overwrite_columns: GoField::new(true),
    });
    for version in [JobVersion::V1, JobVersion::V2] {
        let mut job = encoded_job(
            version,
            ActionType::ACTION_MODIFY_TABLE_CHARSET_AND_COLLATE,
            args.clone(),
        );
        assert_eq!(
            job.raw_args.as_ref().unwrap().get(),
            if version == JobVersion::V1 {
                r#"["utf8mb4","utf8mb4_bin",true]"#
            } else {
                r#"{"to_charset":"utf8mb4","to_collate":"utf8mb4_bin","needs_overwrite_cols":true}"#
            }
        );
        let decoded = get_modify_table_charset_and_collate_args(&mut job)
            .unwrap()
            .unwrap();
        assert_eq!(decoded.read().to_charset.get().as_bytes(), b"utf8mb4");
        assert_eq!(decoded.read().to_collate.get().as_bytes(), b"utf8mb4_bin");
        assert!(decoded.read().needs_overwrite_columns.get());
    }
    assert_eq!(
        crate::serde_helpers::to_go_json(&ModifyTableCharsetAndCollateArgs::default()).unwrap(),
        b"{}"
    );
}

#[test]
pub(crate) fn v2_job_args_decode_uses_go_object_stream_rules() {
    let mut table = Job {
        version: JobVersion::V2,
        raw_args: Some(crate::PersistedRawJson::from_bytes(
            br#"{"table_info":{"id":7},"table_info":{"comment":"kept"},"unknown":1}"#.to_vec(),
        )),
        ..Default::default()
    };
    let table = get_create_table_args(&mut table).unwrap().unwrap();
    let table = table.read().table_info.get().unwrap();
    assert_eq!(table.read().id, 7, "duplicate pointer members merge");
    assert_eq!(table.read().comment, "kept");

    let mut scalar = Job {
        version: JobVersion::V2,
        raw_args: Some(crate::PersistedRawJson::from_bytes(
            br#"{"new_base":1,"new_ba\u017fe":2,"new_base":null}"#.to_vec(),
        )),
        ..Default::default()
    };
    assert_eq!(
        get_rebase_auto_id_args(&mut scalar)
            .unwrap()
            .unwrap()
            .read()
            .new_base
            .get(),
        2,
        "Unicode SimpleFold matches and scalar null is a no-op"
    );

    let mut slice = Job {
        version: JobVersion::V2,
        raw_args: Some(crate::PersistedRawJson::from_bytes(
            br#"{"new_partition_ids":[1,2,3],"new_partition_ids":[9]}"#.to_vec(),
        )),
        ..Default::default()
    };
    let slice = get_truncate_table_args(&mut slice).unwrap().unwrap();
    let header = slice.read().new_partition_ids.get();
    assert_eq!(header.snapshot(), [9]);
    assert_eq!(header.capacity(), 4, "later duplicate reuses Go backing");

    let mut object_slice = Job {
        version: JobVersion::V2,
        raw_args: Some(crate::PersistedRawJson::from_bytes(
            br#"{"old_global_indexes":[{"TableID":1,"tableid":2,"IndexID":3}]}"#.to_vec(),
        )),
        ..Default::default()
    };
    assert_eq!(
        get_finished_table_partition_args(&mut object_slice)
            .unwrap()
            .unwrap()
            .read()
            .old_global_indexes
            .get()
            .snapshot(),
        [TableIDIndexID {
            table_id: 2,
            index_id: 3
        }]
    );
}

#[test]
pub(crate) fn first_source_getter_matrix_round_trips_values_in_both_versions() {
    for version in [JobVersion::V1, JobVersion::V2] {
        let schema_args = GoShared::new(CreateSchemaArgs {
            db_info: GoField::new(Some(GoShared::new(DBInfo {
                id: 100,
                ..Default::default()
            }))),
        });
        let mut schema_job = encoded_job(
            version,
            ActionType::ACTION_CREATE_SCHEMA,
            schema_args.clone(),
        );
        let decoded_schema = get_create_schema_args(&mut schema_job).unwrap().unwrap();
        assert!(!decoded_schema.ptr_eq(&schema_args));
        assert_eq!(decoded_schema.read().db_info.get().unwrap().read().id, 100);

        let drop_args = GoShared::new(DropSchemaArgs {
            fk_check: GoField::new(true),
            all_dropped_table_ids: GoField::new(GoSharedSlice::from_vec(vec![1, 2])),
        });
        let mut drop_job = encoded_job(version, ActionType::ACTION_DROP_SCHEMA, drop_args.clone());
        assert!(get_drop_schema_args(&mut drop_job)
            .unwrap()
            .unwrap()
            .read()
            .fk_check
            .get());

        let mut finished_job =
            encoded_finished_job(version, ActionType::ACTION_DROP_SCHEMA, drop_args);
        assert_eq!(
            get_finished_drop_schema_args(&mut finished_job)
                .unwrap()
                .unwrap()
                .read()
                .all_dropped_table_ids
                .get()
                .snapshot(),
            [1, 2]
        );

        let charset_args = GoShared::new(ModifySchemaArgs {
            to_charset: GoField::new(GoString::from("aa")),
            to_collate: GoField::new(GoString::from("bb")),
            policy_ref: GoField::default(),
        });
        let mut charset_job = encoded_job(
            version,
            ActionType::ACTION_MODIFY_SCHEMA_CHARSET_AND_COLLATE,
            charset_args,
        );
        let charset = get_modify_schema_args(&mut charset_job).unwrap().unwrap();
        assert_eq!(charset.read().to_charset.get().as_bytes(), b"aa");
        assert_eq!(charset.read().to_collate.get().as_bytes(), b"bb");

        for policy in [
            Some(GoShared::new(PolicyRefInfo {
                id: 123,
                ..Default::default()
            })),
            None,
        ] {
            let expected_id = policy.as_ref().map(|policy| policy.read().id);
            let policy_args = GoShared::new(ModifySchemaArgs {
                policy_ref: GoField::new(policy),
                ..Default::default()
            });
            let mut policy_job = encoded_job(
                version,
                ActionType::ACTION_MODIFY_SCHEMA_DEFAULT_PLACEMENT,
                policy_args,
            );
            let decoded = get_modify_schema_args(&mut policy_job).unwrap().unwrap();
            assert_eq!(
                decoded
                    .read()
                    .policy_ref
                    .get()
                    .map(|policy| policy.read().id),
                expected_id
            );
        }

        for (action, id, replace, old_id, fk_check) in [
            (ActionType::ACTION_CREATE_TABLE, 100, false, 0, true),
            (ActionType::ACTION_CREATE_VIEW, 122, true, 123, false),
            (ActionType::ACTION_CREATE_SEQUENCE, 22, false, 0, false),
        ] {
            let table_args = GoShared::new(CreateTableArgs {
                table_info: GoField::new(Some(GoShared::new(TableInfo {
                    id,
                    ..Default::default()
                }))),
                on_exist_replace: GoField::new(replace),
                old_view_table_id: GoField::new(old_id),
                fk_check: GoField::new(fk_check),
            });
            let mut table_job = encoded_job(version, action, table_args);
            let decoded = get_create_table_args(&mut table_job).unwrap().unwrap();
            let decoded = decoded.read();
            assert_eq!(decoded.table_info.get().unwrap().read().id, id);
            assert_eq!(decoded.on_exist_replace.get(), replace);
            assert_eq!(decoded.old_view_table_id.get(), old_id);
            assert_eq!(decoded.fk_check.get(), fk_check);
        }
    }
}

#[test]
fn columnar_index_type_preserves_all_source_boundaries() {
    assert_eq!(
        index_arg_columnar_index_type(ColumnarIndexType::NA, false),
        ColumnarIndexType::NA
    );
    assert_eq!(
        index_arg_columnar_index_type(ColumnarIndexType::NA, true),
        ColumnarIndexType::VECTOR
    );
    for explicit in [
        ColumnarIndexType::INVERTED,
        ColumnarIndexType::VECTOR,
        ColumnarIndexType::FULLTEXT,
        ColumnarIndexType(255),
    ] {
        assert_eq!(index_arg_columnar_index_type(explicit, false), explicit);
        assert_eq!(index_arg_columnar_index_type(explicit, true), explicit);
    }
}

#[test]
fn rename_parallel_slices_keep_source_order_and_json_boundaries() {
    let args = rename_tables_args_from_v1(
        &[11, i64::MIN],
        &[CiString::new("OldDB"), CiString::new("Σ")],
        &[CiString::new("OldT"), CiString::new("İ")],
        &[22, i64::MAX],
        &[CiString::new("NewT"), CiString::new("")],
        &[33, -1],
    );
    assert_eq!(args.len(), 2);
    assert_eq!(args[0].old_schema_name.original(), "OldDB");
    assert_eq!(args[1].old_schema_name.lowercase(), "σ");
    assert_eq!(args[1].old_table_name.lowercase(), "i");
    assert_eq!(args[1].new_schema_id, i64::MAX);
    assert_eq!(args[1].table_id, -1);

    let encoded = serde_json::to_value(&args[0]).expect("rename args serialize");
    assert_eq!(encoded["old_schema_id"], 11);
    assert_eq!(encoded["old_schema_name"]["O"], "OldDB");
    assert_eq!(encoded["old_schema_name"]["L"], "olddb");
    assert!(encoded.get("old_schema_id_for_schema_diff").is_none());

    let zero =
        serde_json::to_value(RenameTableArgs::default()).expect("zero rename args serialize");
    assert!(zero.get("old_schema_id").is_none());
    assert_eq!(zero["old_schema_name"], serde_json::json!({"O":"","L":""}));
}

#[test]
fn rename_empty_and_mismatched_parallel_slices_match_go() {
    assert!(rename_tables_args_from_v1(&[], &[], &[], &[], &[], &[]).is_empty());
    let name = CiString::new("x");
    for missing in 0..5 {
        let old_schema_names = (missing != 0)
            .then_some(name.clone())
            .into_iter()
            .collect::<Vec<_>>();
        let old_table_names = (missing != 1)
            .then_some(name.clone())
            .into_iter()
            .collect::<Vec<_>>();
        let new_schema_ids = (missing != 2).then_some(2).into_iter().collect::<Vec<_>>();
        let new_table_names = (missing != 3)
            .then_some(name.clone())
            .into_iter()
            .collect::<Vec<_>>();
        let table_ids = (missing != 4).then_some(3).into_iter().collect::<Vec<_>>();
        let missing_parallel = std::panic::catch_unwind(|| {
            rename_tables_args_from_v1(
                &[1],
                &old_schema_names,
                &old_table_names,
                &new_schema_ids,
                &new_table_names,
                &table_ids,
            )
        });
        assert!(missing_parallel.is_err(), "parallel slice {missing}");
    }
}

#[test]
fn index_operation_values_keep_go_iota_and_byte_width() {
    assert_eq!(IndexOp::ADD_INDEX.0, 0);
    assert_eq!(IndexOp::DROP_INDEX.0, 1);
    assert_eq!(IndexOp::ROLLBACK_ADD_INDEX.0, 2);
    assert_eq!(serde_json::to_string(&IndexOp(255)).unwrap(), "255");
}

#[test]
pub(crate) fn scalar_and_existing_model_args_match_the_source_v1_v2_matrix() {
    for version in [JobVersion::V1, JobVersion::V2] {
        let mut job = encoded_job(
            version,
            ActionType::ACTION_ALTER_INDEX_VISIBILITY,
            GoShared::new(AlterIndexVisibilityArgs {
                index_name: GoField::new(CiString::new("index-name")),
                invisible: GoField::new(true),
            }),
        );
        let args = get_alter_index_visibility_args(&mut job).unwrap().unwrap();
        assert_eq!(args.read().index_name.get().original(), "index-name");
        assert!(args.read().invisible.get());

        let mut job = encoded_job(
            version,
            ActionType::ACTION_DROP_FOREIGN_KEY,
            GoShared::new(DropForeignKeyArgs {
                foreign_key_name: GoField::new(CiString::new("fk-name")),
            }),
        );
        let args = get_drop_foreign_key_args(&mut job).unwrap().unwrap();
        assert_eq!(args.read().foreign_key_name.get().original(), "fk-name");

        let mut job = encoded_job(
            version,
            ActionType::ACTION_MODIFY_TABLE_AUTO_IDCACHE,
            GoShared::new(ModifyTableAutoIDCacheArgs {
                new_cache: GoField::new(7_527),
            }),
        );
        assert_eq!(
            get_modify_table_auto_id_cache_args(&mut job)
                .unwrap()
                .unwrap()
                .read()
                .new_cache
                .get(),
            7_527
        );

        let mut job = encoded_job(
            version,
            ActionType::ACTION_SHARD_ROW_ID,
            GoShared::new(ShardRowIDArgs {
                shard_row_id_bits: GoField::new(101),
            }),
        );
        assert_eq!(
            get_shard_row_id_args(&mut job)
                .unwrap()
                .unwrap()
                .read()
                .shard_row_id_bits
                .get(),
            101
        );

        let column = GoShared::new(ColumnInfo {
            id: 7_527,
            name: CiString::new("col_name"),
            ..Default::default()
        });
        let mut job = encoded_job(
            version,
            ActionType::ACTION_SET_DEFAULT_VALUE,
            GoShared::new(SetDefaultValueArgs {
                column: GoField::new(Some(column)),
            }),
        );
        let args = get_set_default_value_args(&mut job).unwrap().unwrap();
        let column = args.read().column.get().unwrap();
        assert_eq!(column.read().id, 7_527);
        assert_eq!(column.read().name.original(), "col_name");

        let mut job = encoded_job(
            version,
            ActionType::ACTION_REFRESH_META,
            GoShared::new(RefreshMetaArgs {
                schema_id: GoField::new(i64::MIN),
                table_id: GoField::new(i64::MAX),
                involved_database: GoField::new(GoString::from("db")),
                involved_table: GoField::new(GoString::from("table")),
            }),
        );
        let args = get_refresh_meta_args(&mut job).unwrap().unwrap();
        assert_eq!(args.read().schema_id.get(), i64::MIN);
        assert_eq!(args.read().table_id.get(), i64::MAX);
        assert_eq!(args.read().involved_database.get().as_bytes(), b"db");
        assert_eq!(args.read().involved_table.get().as_bytes(), b"table");

        let mut job = encoded_job(
            version,
            ActionType::ACTION_MODIFY_ENGINE_ATTRIBUTE,
            GoShared::new(ModifyTableEngineAttributeArgs {
                engine_attribute: GoField::new(GoString::from("attribute")),
            }),
        );
        assert_eq!(
            get_modify_table_engine_attribute_args(&mut job)
                .unwrap()
                .unwrap()
                .read()
                .engine_attribute
                .get()
                .as_bytes(),
            b"attribute"
        );

        let mut job = encoded_job(
            version,
            ActionType::ACTION_ALTER_TABLE_MODE,
            GoShared::new(AlterTableModeArgs {
                table_mode: GoField::new(TableMode(255)),
                schema_id: GoField::new(-1),
                table_id: GoField::new(2),
            }),
        );
        let args = get_alter_table_mode_args(&mut job).unwrap().unwrap();
        assert_eq!(args.read().table_mode.get(), TableMode(255));
        assert_eq!(args.read().schema_id.get(), -1);
        assert_eq!(args.read().table_id.get(), 2);
    }
}

#[test]
pub(crate) fn new_native_args_preserve_go_null_duplicate_and_pointer_rules() {
    let visibility: AlterIndexVisibilityArgs = serde_json::from_str(
        r#"{"index_name":{"O":"first","L":"first"},"INDEX_NAME":"last","invisible":true,"INVISIBLE":null}"#,
    )
    .unwrap();
    assert_eq!(visibility.index_name.get().original(), "last");
    assert_eq!(visibility.index_name.get().lowercase(), "last");
    assert!(visibility.invisible.get(), "scalar null is a no-op");

    let default: SetDefaultValueArgs =
        serde_json::from_str(r#"{"column_info":{"id":7},"COLUMN_INFO":{"name":"Col"}}"#).unwrap();
    let column = default.column.get().unwrap();
    assert_eq!(column.read().id, 7);
    assert_eq!(column.read().name.original(), "Col");

    let refresh: RefreshMetaArgs = serde_json::from_str(
        r#"{"schema_id":1,"SCHEMA_ID":null,"involved_db":"first","INVOLVED_DB":"last"}"#,
    )
    .unwrap();
    assert_eq!(refresh.schema_id.get(), 1);
    assert_eq!(refresh.involved_database.get().as_bytes(), b"last");

    let zero_visibility = serde_json::to_value(AlterIndexVisibilityArgs::default()).unwrap();
    assert_eq!(
        zero_visibility,
        serde_json::json!({"index_name":{"O":"","L":""}})
    );

    let mut v1_null = Job {
        version: JobVersion::V1,
        raw_args: Some(crate::PersistedRawJson::from_bytes(b"[null]".to_vec())),
        ..Default::default()
    };
    let default = get_set_default_value_args(&mut v1_null).unwrap().unwrap();
    assert!(default.read().column.get().is_some());
    assert_eq!(
        v1_null
            .decoded_args()
            .get(0)
            .dynamic_type()
            .unwrap()
            .display_name(),
        "*model.ColumnInfo"
    );
}
