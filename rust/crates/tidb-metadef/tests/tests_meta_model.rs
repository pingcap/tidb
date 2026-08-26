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

//! Ports of `pkg/meta/model` unit tests from Go (`origin/master`) onto the
//! Rust rewrite surface (`tidb-model`). Each test cites its Go source file
//! and function.

use tidb_model::job::{INVOLVING_ALL, INVOLVING_NONE, JOB_RESUME_REASON_KV_DISK_FULL};
use tidb_model::table::{DEFAULT_TTL_JOB_INTERVAL, OLD_DEFAULT_TTL_JOB_INTERVAL};

use tidb_ast::CiString;
use tidb_datatype::{FieldType, FieldTypeFlags};
use tidb_model::go_runtime::{GoShared, GoSharedSlice, GoSharedPointerSlice};
use tidb_model::table::FKInfo;
use tidb_model::{
    get_job_ver_in_use, ts_convert_2_time, ActionType, BackfillMeta, ColumnInfo, ColumnDefaultValue,
    DBInfo, DDLReorgMeta, GoAny, GoField, HistoryInfo, IndexColumn, IndexInfo,
    InvolvingSchemaInfo, Job, JobMeta, JobResumeReason, JobState, JobVersion, PartitionInfo,
    PlacementSettings, PolicyInfo, ResolvedTimeZone, SchemaState, SubJob, TableInfo, TableMode,
    TimeZoneLocation, TruncateTableArgs, TTLInfo, ACTION_BDR_MAP, BDR_ACTION_MAP,
};
use tidb_parser::parse_config_duration;

/// Go `TestActionBDRMap` (bdr_test.go:23).
#[test]
fn action_bdr_map() {
    let actions_by_role = BDR_ACTION_MAP.read();
    let role_by_action = ACTION_BDR_MAP.read();
    assert_eq!(tidb_model::ACTION_MAP.len(), role_by_action.len());

    let mut total_actions = 0;
    for (bdr_type, actions) in actions_by_role.iter() {
        for action in actions.snapshot() {
            assert_eq!(
                role_by_action.get(&action),
                Some(bdr_type),
                "action {action}"
            );
        }
        total_actions += actions.len();
    }
    assert_eq!(total_actions, role_by_action.len());
}

fn base_col(name: &str, code: tidb_datatype::FieldTypeCode) -> ColumnInfo {
    ColumnInfo {
        id: 1,
        name: CiString::new(name),
        field_type: FieldType::new(code),
        ..Default::default()
    }
}

/// Go `TestDefaultValue` (column_test.go:28): plain and BIT default values
/// round-trip through set/get and JSON marshal/unmarshal with Go's BIT
/// string-vs-bytes divergence preserved.
#[test]
fn default_value() {
    let rand_plain = "random_plain_string";

    let mut old_plain_col = base_col("oldPlainCol", tidb_datatype::FieldTypeCode::Long);
    old_plain_col.default_value = GoAny::from(ColumnDefaultValue::str(rand_plain));
    old_plain_col.origin_default_value = GoAny::from(ColumnDefaultValue::str(rand_plain));

    let mut new_plain_col = base_col("newPlainCol", tidb_datatype::FieldTypeCode::Long);
    new_plain_col
        .set_default_value(ColumnDefaultValue::Int(1))
        .unwrap();
    assert_eq!(new_plain_col.get_default_value(), GoAny::from(ColumnDefaultValue::Int(1)));
    new_plain_col
        .set_default_value(ColumnDefaultValue::str(rand_plain))
        .unwrap();
    assert_eq!(
        new_plain_col.get_default_value(),
        GoAny::from(ColumnDefaultValue::str(rand_plain))
    );

    // Only string type is allowed in a BIT column.
    let mut new_bit_col = base_col("newBitCol", tidb_datatype::FieldTypeCode::Bit);
    let err = new_bit_col
        .set_default_value(ColumnDefaultValue::Int(1))
        .unwrap_err();
    assert!(
        err.to_string().contains("Invalid default value"),
        "unexpected error: {err}"
    );
    // Go's SetDefaultValue writes DefaultValue before the BIT check, so the
    // rejected int remains visible -- exactly like the source.
    assert_eq!(new_bit_col.get_default_value(), GoAny::from(ColumnDefaultValue::Int(1)));
    // Go: randBitStr := string([]byte{25, 185}) -- raw bytes as a Go string.
    new_bit_col
        .set_default_value(ColumnDefaultValue::string_bytes(vec![25u8, 185]))
        .unwrap();
    assert_eq!(
        new_bit_col.get_default_value(),
        GoAny::from(ColumnDefaultValue::string_bytes(vec![25u8, 185]))
    );

    let mut null_bit_col = base_col("nullBitCol", tidb_datatype::FieldTypeCode::Bit);
    null_bit_col.set_origin_default_value(GoAny::nil()).unwrap();
    assert_eq!(null_bit_col.get_origin_default_value(), GoAny::nil());

    let mut old_bit_col = base_col("oldBitCol", tidb_datatype::FieldTypeCode::Bit);
    old_bit_col.default_value = GoAny::from(ColumnDefaultValue::string_bytes(vec![25u8, 185]));
    old_bit_col.origin_default_value = old_bit_col.default_value.clone();

    let cases: Vec<(ColumnInfo, bool)> = vec![
        (old_plain_col, true),
        (old_bit_col, false),
        (new_plain_col, true),
        (new_bit_col, true),
        (null_bit_col, true),
    ];
    for (col, is_consistent) in &cases {
        let comment = format!("{} assertion failed", col.name);
        let bytes = serde_json::to_vec(col).expect(&comment);
        let new_col: ColumnInfo = serde_json::from_slice(&bytes).expect(&comment);
        if *is_consistent {
            assert_eq!(col.get_default_value(), new_col.get_default_value(), "{comment}");
            assert_eq!(
                col.get_origin_default_value(),
                new_col.get_origin_default_value(),
                "{comment}"
            );
        } else {
            assert_ne!(col.get_default_value(), new_col.get_default_value(), "{comment}");
            assert_ne!(
                col.get_origin_default_value(),
                new_col.get_origin_default_value(),
                "{comment}"
            );
        }
    }

    let extra_phys_tbl_id_col = ColumnInfo::new_extra_phys_tbl_id_col_info();
    assert_eq!(extra_phys_tbl_id_col.get_flag(), u64::from(FieldTypeFlags::NOT_NULL));
    assert_eq!(
        extra_phys_tbl_id_col.get_type(),
        tidb_datatype::FieldTypeCode::LongLong
    );
}

/// Go `TestPlacementSettingsString` (placement_test.go:24).
#[test]
fn placement_settings_string() {
    let settings = PlacementSettings {
        primary_region: "us-east-1".into(),
        regions: "us-east-1,us-east-2".into(),
        schedule: "EVEN".into(),
        ..Default::default()
    };
    assert_eq!(
        settings.to_string(),
        "PRIMARY_REGION=\"us-east-1\" REGIONS=\"us-east-1,us-east-2\" SCHEDULE=\"EVEN\""
    );

    let settings = PlacementSettings {
        leader_constraints: "[+region=bj]".into(),
        ..Default::default()
    };
    assert_eq!(settings.to_string(), "LEADER_CONSTRAINTS=\"[+region=bj]\"");

    let settings = PlacementSettings {
        voters: 1,
        voter_constraints: "[+region=us-east-1]".into(),
        followers: 2,
        follower_constraints: "[+disk=ssd]".into(),
        learners: 3,
        learner_constraints: "[+region=us-east-2]".into(),
        ..Default::default()
    };
    assert_eq!(
        settings.to_string(),
        "VOTERS=1 VOTER_CONSTRAINTS=\"[+region=us-east-1]\" \
         FOLLOWERS=2 FOLLOWER_CONSTRAINTS=\"[+disk=ssd]\" \
         LEARNERS=3 LEARNER_CONSTRAINTS=\"[+region=us-east-2]\""
    );

    let settings = PlacementSettings {
        voters: 3,
        followers: 2,
        learners: 1,
        constraints: "{\"+us-east-1\":1,+us-east-2:1}".into(),
        ..Default::default()
    };
    assert_eq!(
        settings.to_string(),
        "CONSTRAINTS=\"{\\\"+us-east-1\\\":1,+us-east-2:1}\" VOTERS=3 FOLLOWERS=2 LEARNERS=1"
    );
}

/// Go `TestPlacementSettingsClone` (placement_test.go:56): mutating the clone
/// leaves the original zero value untouched.
#[test]
fn placement_settings_clone() {
    let settings = PlacementSettings::default();
    let mut cloned_settings = settings.clone();
    cloned_settings.primary_region = "r1".into();
    cloned_settings.regions = "r1,r2".into();
    cloned_settings.voters = 2;
    cloned_settings.followers = 3;
    cloned_settings.constraints = "[+zone=z1]".into();
    cloned_settings.learner_constraints = "[+region=r1]".into();
    cloned_settings.follower_constraints = "[+disk=ssd]".into();
    cloned_settings.leader_constraints = "[+region=r2]".into();
    cloned_settings.voter_constraints = "[+zone=z2]".into();
    cloned_settings.schedule = "even".into();
    assert_eq!(settings, PlacementSettings::default());
}

/// Go `TestPlacementPolicyClone` (placement_test.go:73).
#[test]
fn placement_policy_clone() {
    let policy = PolicyInfo {
        placement_settings: Some(GoShared::new(PlacementSettings::default())),
        ..Default::default()
    };
    let mut cloned_policy = policy.clone_like_go();
    cloned_policy.id = 100;
    cloned_policy.name = CiString::new("p2");
    cloned_policy.state = SchemaState::DELETE_ONLY;
    cloned_policy.placement_settings.as_ref().unwrap().write().followers = 10;

    assert_eq!(policy.id, 0);
    assert_eq!(policy.name.to_string(), "");
    assert_eq!(policy.state, SchemaState::NONE);
    assert_eq!(*policy.placement_settings.as_ref().unwrap().read(), PlacementSettings::default());
}

/// Go `TestTableModeCanTransitionTo` (table_mode_test.go:23).
#[test]
fn table_mode_can_transition_to() {
    for (from, to, expect) in [
        (TableMode::NORMAL, TableMode::NORMAL, true),
        (TableMode::NORMAL, TableMode::IMPORT, true),
        (TableMode::NORMAL, TableMode::RESTORE, true),
        (TableMode::IMPORT, TableMode::NORMAL, true),
        (TableMode::IMPORT, TableMode::IMPORT, true),
        (TableMode::IMPORT, TableMode::RESTORE, false),
        (TableMode::RESTORE, TableMode::NORMAL, true),
        (TableMode::RESTORE, TableMode::IMPORT, false),
        (TableMode::RESTORE, TableMode::RESTORE, true),
    ] {
        assert_eq!(from.can_transition_to(to), expect, "{from:?} -> {to:?}");
    }
}

/// Go `TestJobStartTime` (job_test.go:38).
///
/// The start-time fragment of `Job.String()` renders through the process
/// local zone exactly as Go does, so only its stable neighbors are pinned.
#[test]
fn job_start_time() {
    let mut job: Job = Default::default();
    job.version = JobVersion::V1;
    job.id = 123;
    job.binlog_info = Some(GoShared::new(HistoryInfo::default()));
    assert_eq!(ts_convert_2_time(job.start_ts).unix_millis(), 0);
    let rendered = job.to_string();
    assert!(
        rendered.starts_with(
            "ID:123, Type:none, State:none, SchemaState:none, SchemaID:0, TableID:0, \
             RowCount:0, ArgLen:0, start time: "
        ),
        "unexpected: {rendered}"
    );
    assert!(
        rendered.ends_with(", Err:<nil>, ErrCount:0, SnapshotVersion:0, Version: v1"),
        "unexpected: {rendered}"
    );
}

/// Go `TestState` (job_test.go:48): every live job state renders a name.
#[test]
fn state_strings() {
    for state in [
        JobState::RUNNING,
        JobState::DONE,
        JobState::CANCELLED,
        JobState::ROLLINGBACK,
        JobState::ROLLBACK_DONE,
        JobState::SYNCED,
    ] {
        assert!(!state.to_string().is_empty());
    }
}

/// Go `TestJobCodec` (job_test.go:63): encode/decode preserves binlog info,
/// reorg location and resume reason; clean + encode(true) drops binlog info;
/// state predicates and row count behave as in Go. The Go test fills
/// `RenameTableArgs`; this port uses `TruncateTableArgs` because the Rust
/// `RenameTableArgs` is a compat boundary without a `JobArgs` impl -- the
/// codec path under test is identical.
#[test]
fn job_codec() {
    // Go: tzName, tzOffset := time.Now().In(time.UTC).Zone() => ("UTC", 0).
    let tz_name = "UTC";
    let tz_offset: i64 = 0;
    let mut job: Job = Default::default();
    job.version = JobVersion::V1;
    job.id = 1;
    job.table_id = 2;
    job.schema_id = 1;
    job.binlog_info = Some(GoShared::new(HistoryInfo::default()));
    let mut reorg_meta: DDLReorgMeta = Default::default();
    let mut location: TimeZoneLocation = Default::default();
    location.name = tz_name.into();
    location.offset = tz_offset;
    reorg_meta.location = Some(GoShared::new(location));
    job.reorg_meta = Some(GoShared::new(reorg_meta));
    job.fill_args(Some(GoShared::new(TruncateTableArgs {
        fk_check: GoField::new(true),
        ..Default::default()
    })));
    {
        let binlog = job.binlog_info.as_ref().unwrap().write();
        let mut binlog = binlog;
        binlog.add_db_info(
            123,
            Some(GoShared::new(DBInfo {
                id: 1,
                name: CiString::new("test_history_db"),
                ..Default::default()
            })),
        );
        binlog.add_table_info(
            123,
            Some(GoShared::new(TableInfo {
                id: 1,
                name: CiString::new("test_history_tbl"),
                ..Default::default()
            })),
        );
    }
    job.set_resume_reason(JOB_RESUME_REASON_KV_DISK_FULL);

    assert!(!job.is_cancelled());
    let b = job.encode(false).unwrap();
    let mut new_job = Job::default();
    new_job.decode(&b).unwrap();
    {
        let original_binlog = job.binlog_info.as_ref().unwrap().read();
        let decoded_binlog = new_job.binlog_info.as_ref().unwrap().read();
        assert_eq!(original_binlog.schema_version, decoded_binlog.schema_version);
        let original_db = original_binlog.db_info.as_ref().unwrap().read();
        let decoded_db = decoded_binlog.db_info.as_ref().unwrap().read();
        assert_eq!(original_db.id, decoded_db.id);
        assert_eq!(original_db.name.to_string(), decoded_db.name.to_string());
        let original_tbl = original_binlog.table_info.as_ref().unwrap().read();
        let decoded_tbl = decoded_binlog.table_info.as_ref().unwrap().read();
        assert_eq!(original_tbl.id, decoded_tbl.id);
        assert_eq!(original_tbl.name.to_string(), decoded_tbl.name.to_string());
    }
    assert!(!new_job.to_string().is_empty());
    {
        let reorg_meta = new_job.reorg_meta.as_ref().unwrap().read();
        let location = reorg_meta.location.as_ref().unwrap().read();
        assert_eq!(location.name.to_string(), tz_name);
        assert_eq!(location.offset, tz_offset);
    }
    assert!(new_job.has_resume_reason(JOB_RESUME_REASON_KV_DISK_FULL));

    job.binlog_info.as_ref().unwrap().write().clean();
    let b1 = job.encode(true).unwrap();
    let mut new_job = Job::default();
    new_job.decode(&b1).unwrap();
    {
        let decoded_binlog = new_job.binlog_info.as_ref().unwrap().read();
        assert!(decoded_binlog.db_info.is_none());
        assert!(decoded_binlog.table_info.is_none());
    }
    assert!(!new_job.to_string().is_empty());

    let b2 = job.encode(true).unwrap();
    let mut new_job = Job::default();
    new_job.decode(&b2).unwrap();
    assert!(!new_job.to_string().is_empty());

    job.state = JobState::DONE;
    assert!(job.is_done());
    assert!(job.is_finished());
    assert!(!job.is_running());
    assert!(!job.is_synced());
    assert!(!job.is_rollback_done());
    job.set_row_count(3);
    assert_eq!(job.get_row_count(), 3);
}

/// Go `TestDDLReorgMetaUseNewCollate` (job_test.go:120).
#[test]
fn ddl_reorg_meta_use_new_collate() {
    let mut meta = DDLReorgMeta::default();
    assert!(meta.get_use_new_collate_or_default(true));
    assert!(!meta.get_use_new_collate_or_default(false));

    meta.set_use_new_collate(false);
    assert!(!meta.get_use_new_collate_or_default(true));

    let data = serde_json::to_string(&meta).unwrap();
    assert!(
        data.contains("\"use_new_collate\":false"),
        "unexpected JSON: {data}"
    );

    let mut decoded: DDLReorgMeta = serde_json::from_str(&data).unwrap();
    assert!(!decoded.get_use_new_collate_or_default(true));

    decoded.set_use_new_collate(true);
    assert!(decoded.get_use_new_collate_or_default(false));
}

/// Go `TestLocation` (job_test.go:140).
#[test]
fn location() {
    use tidb_model::TimeZoneLocation;

    // test offset = 0
    let mut loc: TimeZoneLocation = Default::default();
    let n_loc = loc.get_location().unwrap();
    assert_eq!(n_loc.read().name(), "UTC");
    // test loc.location != nil: the resolved location is cached on the
    // receiver, so mutating the fields afterwards keeps returning the cache.
    loc.name = "Asia/Shanghai".into();
    let n_loc = loc.get_location().unwrap();
    assert_eq!(n_loc.read().name(), "UTC");

    // timezone +05:00 JSON round-trip
    let mut loc1: TimeZoneLocation = Default::default();
    loc1.name = "UTC".into();
    loc1.offset = 18000;
    let loc1_byte = serde_json::to_vec(&loc1).unwrap();
    let loc2: TimeZoneLocation = serde_json::from_slice(&loc1_byte).unwrap();
    assert_eq!(loc2.offset, loc1.offset);
    assert_eq!(loc2.name.to_string(), loc1.name.to_string());
    let n_loc = loc2.get_location().unwrap();
    let n_loc = n_loc.read();
    assert_eq!(n_loc.name(), "UTC");
    match &*n_loc {
        ResolvedTimeZone::Fixed { name, offset_seconds } => {
            assert_eq!(name.to_string(), "UTC");
            assert_eq!(*offset_seconds, 18000);
        }
        other => panic!("expected fixed zone, got {other:?}"),
    }
}

/// Go `TestJobClone` (job_test.go:167).
#[test]
fn job_clone() {
    let mut job: Job = Default::default();
    job.version = JobVersion::V1;
    job.id = 100;
    job.type_ = ActionType::ACTION_CREATE_TABLE;
    job.schema_id = 101;
    job.table_id = 102;
    job.schema_name = "test".into();
    job.table_name = "t".into();
    job.state = JobState::DONE;
    job.multi_schema_info = None;
    job.resume_reason = Some(GoShared::new(JobResumeReason { type_: JOB_RESUME_REASON_KV_DISK_FULL.into() }));
    let clone = std::clone::Clone::clone(&job).deep_clone().unwrap();
    assert_eq!(clone.id, job.id);
    assert_eq!(clone.type_, job.type_);
    assert_eq!(clone.schema_id, job.schema_id);
    assert_eq!(clone.table_id, job.table_id);
    assert_eq!(clone.schema_name.to_string(), job.schema_name.to_string());
    assert_eq!(clone.table_name.to_string(), job.table_name.to_string());
    assert_eq!(clone.state, job.state);
    assert!(clone.multi_schema_info.is_none());
    assert_eq!(
        clone.resume_reason.as_ref().unwrap().read().type_.to_string(),
        JOB_RESUME_REASON_KV_DISK_FULL
    );
}

/// Go `TestSubJobToProxyJobWithResumeReason` (job_test.go:192).
#[test]
fn sub_job_to_proxy_job_with_resume_reason() {
    let mut parent_job: Job = Default::default();
    parent_job.id = 100;
    parent_job.resume_reason = Some(GoShared::new(JobResumeReason { type_: JOB_RESUME_REASON_KV_DISK_FULL.into() }));
    let mut sub_job: SubJob = Default::default();
    sub_job.type_ = ActionType::ACTION_ADD_INDEX;
    sub_job.state = JobState::QUEUEING;
    let proxy_job = sub_job.to_proxy_job(&parent_job, 0);
    assert!(proxy_job.has_resume_reason(JOB_RESUME_REASON_KV_DISK_FULL));
}

/// Go `TestBackfillMetaCodec` (job_test.go:214).
#[test]
fn backfill_meta_codec() {
    use tidb_error::terror::{TerrorClass, TerrorCode, ERR_RESULT_UNDETERMINED};

    let jm = JobMeta {
        schema_id: 1,
        table_id: 2,
        query: "alter table t add index idx(a)".into(),
        priority: 1,
        ..Default::default()
    };
    // Go: Error: terror.ErrResultUndetermined.
    let bm = BackfillMeta {
        end_include: true,
        error: Some(GoShared::new(ERR_RESULT_UNDETERMINED.clone())),
        job_meta: Some(GoShared::new(jm)),
        ..Default::default()
    };
    let bm_bytes = bm.encode().unwrap();
    let mut bm_ret = BackfillMeta::default();
    bm_ret.decode(&bm_bytes).unwrap();
    assert_eq!(bm_ret.end_include, bm.end_include);
    let ret_meta = bm_ret.job_meta.as_ref().unwrap().read();
    assert_eq!(ret_meta.schema_id, 1);
    assert_eq!(ret_meta.table_id, 2);
    assert_eq!(ret_meta.query.to_string(), "alter table t add index idx(a)");
    assert_eq!(ret_meta.priority, 1);
    let ret_error = bm_ret.error.as_ref().unwrap().read();
    assert_eq!(ret_error.code(), TerrorCode::new(2));
    assert_eq!(ret_error.class(), TerrorClass::Global);
}

/// Go `TestMayNeedReorg` (job_test.go:233).
#[test]
fn may_need_reorg() {
    let reorg_job_types = [
        ActionType::ACTION_REORGANIZE_PARTITION,
        ActionType::ACTION_REMOVE_PARTITIONING,
        ActionType::ACTION_ALTER_TABLE_PARTITIONING,
        ActionType::ACTION_ADD_INDEX,
        ActionType::ACTION_ADD_PRIMARY_KEY,
    ];
    let general_job_types = [
        ActionType::ACTION_CREATE_TABLE,
        ActionType::ACTION_DROP_TABLE,
    ];
    let mut job: Job = Default::default();
    job.version = JobVersion::V1;
    job.id = 100;
    job.type_ = ActionType::ACTION_CREATE_TABLE;
    job.schema_id = 101;
    job.table_id = 102;
    job.schema_name = "test".into();
    job.table_name = "t".into();
    job.state = JobState::DONE;
    job.multi_schema_info = None;
    for job_type in reorg_job_types {
        job.type_ = job_type;
        assert!(job.may_need_reorg(), "{job_type:?}");
    }
    for job_type in general_job_types {
        job.type_ = job_type;
        assert!(!job.may_need_reorg(), "{job_type:?}");
    }
}

/// Go `TestInFinalState` (job_test.go:267).
#[test]
fn in_final_state() {
    for (state, v) in [
        (JobState::SYNCED, true),
        (JobState::CANCELLED, true),
        (JobState::PAUSED, true),
        (JobState::CANCELLING, false),
        (JobState::ROLLBACK_DONE, false),
    ] {
        let mut job: Job = Default::default();
        job.state = state;
        assert_eq!(job.in_final_state(), v, "{state:?}");
    }
}

/// Go `TestSchemaState` (job_test.go:279): every reorganization state renders
/// a name.
#[test]
fn schema_state_strings() {
    for state in [
        SchemaState::DELETE_ONLY,
        SchemaState::WRITE_ONLY,
        SchemaState::WRITE_REORGANIZATION,
        SchemaState::DELETE_REORGANIZATION,
        SchemaState::PUBLIC,
        SchemaState::GLOBAL_TXN_ONLY,
    ] {
        assert!(!state.to_string().is_empty());
    }
}

/// Go `TestActionTypeReserved` (job_test.go:294): no action type falls into
/// the reserved range [200, 256). The Go test re-parses job.go source to
/// enumerate constants; this port pins the same invariant against the ported
/// `ActionType` constant table (the single source of truth here).
#[test]
fn action_type_reserved() {
    const RESERVED_START: i64 = 200;
    const RESERVED_END: i64 = 256;
    for (action, _name) in tidb_model::ACTION_MAP {
        let value = i64::from(action.0);
        assert!(
            !(value >= RESERVED_START && value < RESERVED_END),
            "action {_name} must not be in reserved range \
             [{RESERVED_START}, {RESERVED_END}), but got {value}"
        );
    }
}

/// Go `TestString` (job_test.go:334).
#[test]
fn action_type_string() {
    let acts = [
        (ActionType::ACTION_NONE, "none"),
        (ActionType::ACTION_ADD_FOREIGN_KEY, "add foreign key"),
        (ActionType::ACTION_DROP_FOREIGN_KEY, "drop foreign key"),
        (ActionType::ACTION_TRUNCATE_TABLE, "truncate table"),
        (ActionType::ACTION_MODIFY_COLUMN, "modify column"),
        (ActionType::ACTION_RENAME_TABLE, "rename table"),
        (ActionType::ACTION_RENAME_TABLES, "rename tables"),
        (ActionType::ACTION_SET_DEFAULT_VALUE, "set default value"),
        (ActionType::ACTION_CREATE_SCHEMA, "create schema"),
        (ActionType::ACTION_DROP_SCHEMA, "drop schema"),
        (ActionType::ACTION_CREATE_TABLE, "create table"),
        (ActionType::ACTION_DROP_TABLE, "drop table"),
        (ActionType::ACTION_ADD_INDEX, "add index"),
        (ActionType::ACTION_DROP_INDEX, "drop index"),
        (ActionType::ACTION_ADD_COLUMN, "add column"),
        (ActionType::ACTION_DROP_COLUMN, "drop column"),
        (
            ActionType::ACTION_MODIFY_SCHEMA_CHARSET_AND_COLLATE,
            "modify schema charset and collate",
        ),
        (
            ActionType::ACTION_ALTER_TABLE_PLACEMENT,
            "alter table placement",
        ),
        (
            ActionType::ACTION_ALTER_TABLE_PARTITION_PLACEMENT,
            "alter table partition placement",
        ),
        (ActionType::ACTION_ALTER_NO_CACHE_TABLE, "alter table nocache"),
        (
            ActionType::ACTION_ALTER_TABLE_AFFINITY,
            "alter table affinity",
        ),
        (
            ActionType::ACTION_ALTER_TABLE_SOFT_DELETE_INFO,
            "alter soft delete info",
        ),
        (
            ActionType::ACTION_MODIFY_SCHEMA_SOFT_DELETE_AND_ACTIVE_ACTIVE,
            "modify schema soft delete and active active",
        ),
    ];
    for (act, result) in acts {
        assert_eq!(act.to_string(), result, "{act:?}");
    }
}

/// Go `TestJobEncodeV2` (job_test.go:410).
#[test]
fn job_encode_v2() {
    let mut j: Job = Default::default();
    j.version = JobVersion::V2;
    j.type_ = ActionType::ACTION_TRUNCATE_TABLE;
    j.fill_args(Some(GoShared::new(TruncateTableArgs {
        fk_check: GoField::new(true),
        ..Default::default()
    })));
    j.encode(false).unwrap();
    assert!(j.raw_args.is_none());
    j.encode(true).unwrap();
    assert!(j.raw_args.is_some());
    let args: TruncateTableArgs = serde_json::from_str(&j.raw_args.as_ref().unwrap().get()).unwrap();
    assert!(*args.fk_check.read());
    assert_eq!(j.decoded_args().len(), 1, "v2 job retains its decoded argument");
}

/// Go `TestJobVerInUse` (job_test.go:429). This workspace targets the classic
/// kernel build, whose in-use job version is v1.
#[test]
fn job_ver_in_use() {
    assert_eq!(get_job_ver_in_use(), JobVersion::V1);
}

fn explicit(infos: Vec<InvolvingSchemaInfo>) -> Job {
    let mut job = Job::default();
    job.involving_schema_info = GoSharedSlice::from_vec(infos);
    job
}

fn involving(database: &str, table: &str) -> InvolvingSchemaInfo {
    InvolvingSchemaInfo {
        database: database.into(),
        table: table.into(),
        ..Default::default()
    }
}

/// Go `TestJobCheckInvolvingSchemaInfo` (job_test.go:437).
#[test]
fn job_check_involving_schema_info() {
    fn bare(schema_name: &str, table_name: &str) -> Job {
        let mut job: Job = Default::default();
        job.schema_name = schema_name.into();
        job.table_name = table_name.into();
        job
    }
    let cases: Vec<(Job, Option<&str>)> = vec![
        // cases without explicit InvolvingSchemaInfo
        (bare("", ""), Some("must involve only one type of object")),
        (bare("", "t1"), Some("must have non-empty name set")),
        (bare("", "*"), Some("must have non-empty name set")),
        // GetInvolvingSchemaInfo converts these into test.* automatically.
        (bare("test", ""), None),
        (bare("test", "t"), None),
        (bare("test", "*"), None),
        // GetInvolvingSchemaInfo converts these into *.* automatically.
        (bare("*", ""), None),
        (bare("*", "t"), Some("operating on all databases, must not set table name")),
        (bare("*", "*"), None),
        // cases with explicit InvolvingSchemaInfo
        (
            explicit(vec![InvolvingSchemaInfo { policy: "p".into(), ..Default::default() }]),
            None,
        ),
        (
            explicit(vec![InvolvingSchemaInfo { policy: "*".into(), ..Default::default() }]),
            None,
        ),
        (
            explicit(vec![InvolvingSchemaInfo { resource_group: "r".into(), ..Default::default() }]),
            None,
        ),
        (
            explicit(vec![InvolvingSchemaInfo { resource_group: "*".into(), ..Default::default() }]),
            None,
        ),
        (
            explicit(vec![InvolvingSchemaInfo {
                policy: "p".into(),
                resource_group: "r".into(),
                ..Default::default()
            }]),
            Some("must involve only one type of object"),
        ),
        (
            explicit(vec![InvolvingSchemaInfo {
                policy: "p".into(),
                database: "d".into(),
                ..Default::default()
            }]),
            Some("must involve only one type of object"),
        ),
        (
            explicit(vec![InvolvingSchemaInfo {
                database: "d".into(),
                resource_group: "r".into(),
                ..Default::default()
            }]),
            Some("must involve only one type of object"),
        ),
        (
            explicit(vec![InvolvingSchemaInfo {
                policy: "p".into(),
                database: "d".into(),
                resource_group: "r".into(),
                ..Default::default()
            }]),
            Some("must involve only one type of object"),
        ),
        (explicit(vec![involving("", "")]), Some("must involve only one type of object")),
        (explicit(vec![involving("", "t")]), Some("must have non-empty name set")),
        (explicit(vec![involving("", "*")]), Some("must have non-empty name set")),
        (explicit(vec![involving("d", "")]), Some("must have non-empty name set")),
        (explicit(vec![involving("d", "t")]), None),
        (explicit(vec![involving("d", "*")]), None),
        // note: we won't adjust for explicit InvolvingSchemaInfo in this case.
        (explicit(vec![involving("*", "")]), Some("must have non-empty name set")),
        (
            explicit(vec![involving("*", "t")]),
            Some("operating on all databases, must not set table name"),
        ),
        (explicit(vec![involving("*", "*")]), None),
    ];
    for (i, (job, expected)) in cases.into_iter().enumerate() {
        let err = job.check_involving_schema_info();
        match expected {
            None => assert!(err.is_ok(), "case-{i}: {err:?}"),
            Some(fragment) => {
                let message = err.unwrap_err();
                assert!(message.contains(fragment), "case-{i}: {message}");
            }
        }
    }
}

/// Go `TestJobCheckInvolvingSchemaInfo` subtest "normalize scheduler names"
/// (job_test.go:495).
#[test]
fn normalize_scheduler_names() {
    let mut job: Job = Default::default();
    job.schema_name = "TestDB".into();
    job.table_name = "T1".into();
    job.involving_schema_info = GoSharedSlice::from_vec(vec![
        InvolvingSchemaInfo { database: "TestDB".into(), table: "T1".into(), ..Default::default() },
        InvolvingSchemaInfo {
            database: "AnotherDB".into(),
            table: INVOLVING_ALL.into(),
            ..Default::default()
        },
        InvolvingSchemaInfo {
            database: INVOLVING_ALL.into(),
            table: INVOLVING_ALL.into(),
            ..Default::default()
        },
        InvolvingSchemaInfo {
            database: INVOLVING_NONE.into(),
            table: INVOLVING_NONE.into(),
            ..Default::default()
        },
        InvolvingSchemaInfo { policy: "PolicyName".into(), ..Default::default() },
        InvolvingSchemaInfo { resource_group: "ResourceGroupName".into(), ..Default::default() },
    ]);

    job.normalize_involving_schema_info();

    assert_eq!(job.schema_name.to_string(), "testdb");
    assert_eq!(job.table_name.to_string(), "t1");
    let infos = job.involving_schema_info.snapshot();
    assert_eq!(infos[0].database.to_string(), "testdb");
    assert_eq!(infos[0].table.to_string(), "t1");
    assert_eq!(infos[1].database.to_string(), "anotherdb");
    assert_eq!(infos[1].table.to_string(), INVOLVING_ALL);
    assert_eq!(infos[2].database.to_string(), INVOLVING_ALL);
    assert_eq!(infos[2].table.to_string(), INVOLVING_ALL);
    assert_eq!(infos[3].database.to_string(), INVOLVING_NONE);
    assert_eq!(infos[3].table.to_string(), INVOLVING_NONE);
    assert_eq!(infos[4].policy.to_string(), "policyname");
    assert_eq!(infos[5].resource_group.to_string(), "resourcegroupname");
}

// ---------------------------------------------------------------------------
// table_test.go
// ---------------------------------------------------------------------------

fn new_column_for_test(id: usize) -> ColumnInfo {
    ColumnInfo {
        id: id as i64,
        name: CiString::new(&format!("c_{id}")),
        offset: id as i64,
        field_type: FieldType::new(tidb_datatype::FieldTypeCode::Long),
        ..Default::default()
    }
}

fn new_index_for_test(id: usize, columns: &[GoShared<ColumnInfo>]) -> IndexInfo {
    IndexInfo {
        name: CiString::new(&format!("i_{id}")),
        table: CiString::new("t"),
        columns: columns
            .iter()
            .map(|c| IndexColumn {
                name: c.read().name.clone(),
                offset: c.read().offset,
                length: 0,
                ..Default::default()
            })
            .collect::<Vec<_>>()
            .into(),
        ..Default::default()
    }
}

fn check_offsets(tbl: &TableInfo, ids: &[usize]) {
    assert_eq!(ids.len(), tbl.columns.len());
    for (i, id) in ids.iter().enumerate() {
        let col_handle = tbl.columns.get(i).unwrap();
        let col = col_handle.read();
        assert_eq!(format!("c_{id}"), col.name.lowercase());
        assert_eq!(i as i64, col.offset);
    }
    for handle in tbl.columns.iter_handles() {
        if let Some(col_handle) = handle {
            let col = col_handle.read();
            for idx in tbl.indices.iter_handles().flatten() {
                let idx = idx.read();
                let idx_cols = idx.columns.handles();
                    for idx_col_handle in idx_cols {
                        let idx_col_handle = idx_col_handle.expect("nil IndexColumn");
                        let idx_col = idx_col_handle.read();
                    if col.name.lowercase() == idx_col.name.lowercase() {
                        // Columns with the same name should have a same offset.
                        assert_eq!(col.offset, idx_col.offset);
                    }
                }
            }
        }
    }
}

/// Go `TestMoveColumnInfo` (table_test.go:50).
#[test]
fn move_column_info() {
    let cols: Vec<GoShared<ColumnInfo>> =
        (0..5usize).map(|i| GoShared::new(new_column_for_test(i))).collect();

    let i0 = new_index_for_test(0, &cols);
    let i1 = new_index_for_test(1, &[cols[4].clone(), cols[2].clone()]);
    let i2 = new_index_for_test(2, &[cols[0].clone(), cols[4].clone()]);
    let i3 = new_index_for_test(3, &[cols[1].clone(), cols[2].clone(), cols[3].clone()]);
    let i4 = new_index_for_test(4, &[cols[3].clone(), cols[2].clone(), cols[1].clone()]);

    let mut tbl = TableInfo {
        id: 1,
        name: CiString::new("t"),
        columns: GoSharedPointerSlice::from_handles(
            (0..5usize).map(|i| Some(cols[i].clone())).collect(),
        ),
        indices: vec![i0, i1, i2, i3, i4].into(),
        ..Default::default()
    };

    // Original offsets: [0, 1, 2, 3, 4]
    tbl.move_column_info(4, 0);
    check_offsets(&tbl, &[4, 0, 1, 2, 3]);
    tbl.move_column_info(2, 3);
    check_offsets(&tbl, &[4, 0, 2, 1, 3]);
    tbl.move_column_info(3, 2);
    check_offsets(&tbl, &[4, 0, 1, 2, 3]);
    tbl.move_column_info(0, 4);
    check_offsets(&tbl, &[0, 1, 2, 3, 4]);
    tbl.move_column_info(2, 2);
    check_offsets(&tbl, &[0, 1, 2, 3, 4]);
    tbl.move_column_info(0, 0);
    check_offsets(&tbl, &[0, 1, 2, 3, 4]);
    tbl.move_column_info(1, 4);
    check_offsets(&tbl, &[0, 2, 3, 4, 1]);
    tbl.move_column_info(3, 0);
    check_offsets(&tbl, &[4, 0, 2, 3, 1]);
}

/// Go `TestModelBasic` (table_test.go:89).
#[test]
fn model_basic() {
    let mut column = ColumnInfo {
        id: 1,
        name: CiString::new("c"),
        offset: 0,
        default_value: ColumnDefaultValue::Int(0).into(),
        field_type: FieldType::new(tidb_datatype::FieldTypeCode::Unspecified),
        hidden: true,
        ..Default::default()
    };
    column.add_flag(u64::from(FieldTypeFlags::PRI_KEY));

    let index = IndexInfo {
        name: CiString::new("key"),
        table: CiString::new("t"),
        columns: vec![IndexColumn {
            name: CiString::new("c"),
            offset: 0,
            length: 10,
            ..Default::default()
        }]
        .into(),
        unique: true,
        primary: true,
        ..Default::default()
    };

    let seq = tidb_model::table::SequenceInfo {
        increment: 1,
        min_value: 1,
        max_value: 100,
        ..Default::default()
    };

    let table = TableInfo {
        id: 1,
        name: CiString::new("t"),
        charset: "utf8".into(),
        collate: "utf8_bin".into(),
        columns: vec![column.clone()].into(),
        indices: vec![index].into(),
        foreign_keys: vec![FKInfo {
            ref_cols: vec![CiString::new("a")].into(),
            cols: vec![CiString::new("a")].into(),
            ..Default::default()
        }]
        .into(),
        pk_is_handle: true,
        ..Default::default()
    };

    let table2 = TableInfo {
        id: 2,
        name: CiString::new("s"),
        sequence: Some(GoShared::new(seq)),
        ..Default::default()
    };

    let table_handle = GoShared::new(table.clone());

    let db_info = DBInfo {
        id: 1,
        name: CiString::new("test"),
        charset: "utf8".into(),
        collate: "utf8_bin".into(),
        deprecated_tables: GoSharedPointerSlice::from_handles(vec![Some(table_handle.clone())]),
        ..Default::default()
    };

    let n = db_info.clone_like_go();
    assert_eq!(n.id, db_info.id);
    assert_eq!(n.name.to_string(), db_info.name.to_string());
    assert_eq!(n.charset, db_info.charset);
    assert_eq!(n.collate, db_info.collate);
    assert_eq!(
        n.deprecated_tables.get(0).map(|t| t.read().id),
        db_info.deprecated_tables.get(0).map(|t| t.read().id)
    );
    assert!(!n.deprecated_tables.get(0).unwrap().ptr_eq(&table_handle));

    let pk_name = table.get_pk_name();
    assert_eq!(pk_name.to_string(), "c");
    let new_column = table.get_pk_col_info().unwrap();
    assert!(new_column.read().hidden);
    let first_column = table.columns.get(0).unwrap();
    assert_eq!(new_column.read().id, first_column.read().id);
    assert_eq!(new_column.read().name.to_string(), first_column.read().name.to_string());
    let in_idx = table.column_is_in_index(Some(&column));
    assert!(in_idx);
    assert_eq!(tidb_ast::IndexType::BTREE.sql(), "BTREE");
    assert_eq!(tidb_ast::IndexType::HASH.sql(), "HASH");
    assert_eq!(tidb_ast::IndexType(100_000).sql(), "");
    let has = table.indices.get(0).unwrap().read().has_prefix_index();
    assert!(has);
    assert_eq!(
        ts_convert_2_time(table.update_ts).unix_millis(),
        table.get_update_time().unix_millis()
    );
    assert!(table2.is_sequence());
    assert!(!table2.is_base_table());

    // Corner cases
    let mut toggled = table.columns.get(0).unwrap().read().clone();
    toggled.toggle_flag(u64::from(FieldTypeFlags::PRI_KEY));
    table.columns.set(0, Some(GoShared::new(toggled)));
    let pk_name = table.get_pk_name();
    assert_eq!(pk_name.to_string(), "");
    assert!(table.get_pk_col_info().is_none());
    let an_col = ColumnInfo {
        name: CiString::new("d"),
        ..Default::default()
    };
    let ex_idx = table.column_is_in_index(Some(&an_col));
    assert!(!ex_idx);
    let an_index = IndexInfo {
        columns: Vec::<IndexColumn>::new().into(),
        ..Default::default()
    };
    assert!(!an_index.has_prefix_index());

    let extra_pk = ColumnInfo::new_extra_handle_col_info();
    assert_eq!(
        extra_pk.get_flag(),
        u64::from(FieldTypeFlags::NOT_NULL | FieldTypeFlags::PRI_KEY)
    );
    assert_eq!(extra_pk.get_charset(), "binary");
    assert_eq!(extra_pk.get_collate(), "binary");
}

/// Go `TestTTLInfoClone` (table_test.go:194).
#[test]
fn ttl_info_clone() {
    let ttl_info = TTLInfo {
        column_name: CiString::new("test"),
        interval_expr_str: "test_expr".into(),
        interval_time_unit: 5,
        enable: true,
        ..Default::default()
    };

    let mut cloned_ttl_info = ttl_info.clone();
    cloned_ttl_info.column_name = CiString::new("test_2");
    cloned_ttl_info.interval_expr_str = "test_expr_2".into();
    cloned_ttl_info.interval_time_unit = 9;
    cloned_ttl_info.enable = false;

    assert_eq!(ttl_info.column_name.to_string(), "test");
    assert_eq!(ttl_info.interval_expr_str, "test_expr");
    assert_eq!(ttl_info.interval_time_unit, 5);
    assert!(ttl_info.enable);
}

const HOUR_NANOS: i64 = 3600 * 1_000_000_000;

/// Go `TestTTLJobInterval` (table_test.go:214).
#[test]
fn ttl_job_interval() {
    let ttl_info = TTLInfo::default();
    let interval = ttl_info.get_job_interval().unwrap();
    assert_eq!(interval, HOUR_NANOS); // time.Hour

    let ttl_info = TTLInfo {
        job_interval: "200h".into(),
        ..Default::default()
    };
    let interval = ttl_info.get_job_interval().unwrap();
    assert_eq!(interval, 200 * HOUR_NANOS);
}

/// Go `TestClearReorgIntermediateInfo` (table_test.go:227).
#[test]
fn clear_reorg_intermediate_info() {
    let mut pt_info = PartitionInfo::default();
    pt_info.ddl_type = tidb_ast::PartitionType::HASH;
    pt_info.ddl_expr = "Test DDL Expr".into();
    pt_info.new_table_id = 1111;

    pt_info.clear_reorg_intermediate_info();
    assert_eq!(pt_info.ddl_type, tidb_ast::PartitionType::NONE);
    assert_eq!(pt_info.ddl_expr, "");
    assert!(!pt_info.ddl_columns.is_allocated());
    assert_eq!(pt_info.new_table_id, 0);
}

/// Go `TestTTLDefaultJobInterval` (table_test.go:240): both package-level
/// defaults parse as valid Go durations.
#[test]
fn ttl_default_job_interval() {
    let d = parse_config_duration(DEFAULT_TTL_JOB_INTERVAL).unwrap();
    assert_eq!(d, 24 * HOUR_NANOS);
    let d = parse_config_duration(OLD_DEFAULT_TTL_JOB_INTERVAL).unwrap();
    assert_eq!(d, HOUR_NANOS);
}
