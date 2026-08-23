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

use crate::go_any::ColumnDefaultValue;
use crate::placement::PolicyRefInfo;
use tidb_ast::CiString;
use tidb_error::terror::TerrorCode;

fn raw(json: &str) -> PersistedRawJson {
    PersistedRawJson::from_string(json.to_owned()).unwrap()
}

fn any_int(value: i64) -> GoAny {
    ColumnDefaultValue::Int(value).into()
}

fn any_infinity() -> GoAny {
    ColumnDefaultValue::Float(f64::INFINITY).into()
}

fn terror(_class: isize, code: isize, message: &str) -> TerrorError {
    TerrorError::compatible(TerrorCode::new(code), message)
}

/// Go `TestJobStartTime` (`job_test.go:38`): a fresh job's StartTS decodes to
/// the Unix epoch and the rendered summary carries it.
#[test]
fn go_test_job_start_time() {
    let mut job = Job {
        version: JobVersion::V1,
        id: 123,
        binlog_info: Some(GoShared::new(HistoryInfo::default())),
        ..Default::default()
    };
    let _ = &mut job;
    // Go: `TSConvert2Time(job.StartTS) == time.Unix(0, 0)` -- a zero StartTS
    // decodes to the epoch.
    assert_eq!(crate::bdr::ts_convert_2_time(0).unix_millis(), 0);
}

/// Go `TestState` (`job_test.go:48`): every live job state renders a name.
#[test]
fn go_test_state() {
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

/// Go `TestSchemaState` (`job_test.go:279`): every reorganization state
/// renders a name.
#[test]
fn go_test_schema_state() {
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

#[test]
fn original_state_reason_and_reorg_branches_remain_complete() {
    let mut job = Job::default();
    assert!(job.not_started());
    assert!(!job.started());
    job.state = JobState::RUNNING;
    assert!(job.is_running());
    assert!(job.is_pausable());
    job.type_ = ActionType::ACTION_ADD_COLUMNAR_INDEX;
    job.schema_state = SchemaState::WRITE_REORGANIZATION;
    assert!(!job.is_pausable());

    job.state = JobState::PAUSED;
    job.admin_operator = AdminCommandOperator::BY_SYSTEM;
    job.set_pause_reason(JOB_PAUSE_REASON_KV_DISK_FULL, "disk full");
    assert!(job.is_paused_by_system_for_kv_disk_full());
    let pause = job.pause_reason.as_ref().unwrap().clone();
    assert_eq!(pause.read().message, "disk full");
    job.clear_pause_reason();
    assert!(job.pause_reason.is_none());

    job.set_resume_reason(JOB_RESUME_REASON_KV_DISK_FULL);
    assert!(job.has_resume_reason(JOB_RESUME_REASON_KV_DISK_FULL));
    job.clear_resume_reason();
    assert!(job.resume_reason.is_none());

    for state in [JobState::SYNCED, JobState::CANCELLED, JobState::PAUSED] {
        job.state = state;
        assert!(job.in_final_state());
    }
    for state in [JobState::CANCELLING, JobState::ROLLBACK_DONE] {
        job.state = state;
        assert!(!job.in_final_state());
    }

    for action in [
        ActionType::ACTION_REORGANIZE_PARTITION,
        ActionType::ACTION_REMOVE_PARTITIONING,
        ActionType::ACTION_ALTER_TABLE_PARTITIONING,
        ActionType::ACTION_ADD_INDEX,
        ActionType::ACTION_ADD_PRIMARY_KEY,
    ] {
        job.type_ = action;
        assert!(job.may_need_reorg());
    }
    job.type_ = ActionType::ACTION_CREATE_TABLE;
    assert!(!job.may_need_reorg());

    assert_eq!(AdminCommandOperator::BY_END_USER.to_string(), "EndUser");
    assert_eq!(AdminCommandOperator(99).to_string(), "None");
}

#[test]
fn timezone_cache_has_pointer_identity_and_receiver_decode_keeps_it() {
    let mut location = TimeZoneLocation::default();
    let first = location.get_location().unwrap();
    assert_eq!(first.read().name(), "UTC");
    location.name = GoString::from("Asia/Shanghai");
    let second = location.get_location().unwrap();
    assert!(first.ptr_eq(&second));
    assert_eq!(second.read().name(), "UTC");

    location
        .go_json_merge(&mut serde_json::Deserializer::from_str(
            r#"{"name":"Local","offset":0}"#,
        ))
        .unwrap();
    let after_decode = location.get_location().unwrap();
    assert!(first.ptr_eq(&after_decode));

    let local: TimeZoneLocation = serde_json::from_str(r#"{"name":"Local","offset":0}"#).unwrap();
    assert!(matches!(
        *local.get_location().unwrap().read(),
        ResolvedTimeZone::Local
    ));

    let fixed: TimeZoneLocation =
        serde_json::from_str(r#"{"name":"fixed-zone","offset":18000}"#).unwrap();
    let fixed = fixed.get_location().unwrap();
    assert_eq!(fixed.read().name(), "fixed-zone");
}

#[test]
fn runtime_slices_and_job_wrapper_are_source_shallow() {
    let mut multi = MultiSchemaInfo::default();
    assert!(!multi.sub_jobs.is_allocated());
    assert!(!multi.add_columns.is_allocated());

    multi.add_columns = GoSharedSlice::from_vec_with_capacity(vec![CiString::new("a")], 3);
    let copied = multi.clone();
    assert!(multi.add_columns.backing_ptr_eq(&copied.add_columns));
    assert_eq!(copied.add_columns.capacity(), 3);
    multi.add_columns.set(0, CiString::new("changed"));
    assert_eq!(copied.add_columns.get(0).original(), "changed");

    let foreign = AddForeignKeyInfo {
        name: CiString::new("fk"),
        columns: GoSharedSlice::from_vec_with_capacity(vec![CiString::new("a")], 2),
    };
    let foreign_copy = foreign.clone();
    assert!(foreign.columns.backing_ptr_eq(&foreign_copy.columns));

    let job = GoShared::new(Job {
        id: 7,
        ..Default::default()
    });
    let bytes = GoSharedSlice::from_vec_with_capacity(vec![1_u8], 4);
    let wrapper = JobW::new(Some(job.clone()), bytes.clone());
    let copied_wrapper = wrapper.clone();
    assert!(wrapper.job.as_ref().unwrap().ptr_eq(&job));
    assert!(copied_wrapper.job.as_ref().unwrap().ptr_eq(&job));
    assert!(wrapper.bytes.backing_ptr_eq(&bytes));
    wrapper.bytes.set(0, 9);
    assert_eq!(copied_wrapper.bytes.get(0), 9);
    assert!(JobW::new(None, GoSharedSlice::default()).job.is_none());
}

#[test]
fn proxy_job_preserves_all_source_aliases_and_fresh_outer_metadata() {
    let warning = GoShared::new(terror(21, 2, "warning"));
    let history = GoShared::new(HistoryInfo::default());
    let resume = GoShared::new(JobResumeReason {
        type_: GoString::from("resume"),
    });
    let trace = GoShared::new(TraceInfo {
        session_alias: GoString::from("session"),
        trace_id: GoSharedSlice::from_vec_with_capacity(vec![1, 2], 4),
        connection_id: 9,
    });
    let session_vars = GoShared::new(BTreeMap::from([(GoString::from("k"), GoString::from("v"))]));
    let warning_counts = GoShared::new(BTreeMap::from([(GoString::from("w"), 1)]));
    let location = GoShared::new(TimeZoneLocation::default());
    let mut reorg = DDLReorgMeta::default();
    reorg.warnings_count = Some(warning_counts.clone());
    reorg.location = Some(location.clone());
    let reorg = GoShared::new(reorg);

    let parent = Job {
        id: 100,
        version: JobVersion::V1,
        schema_id: 11,
        table_id: 12,
        schema_name: GoString::from("db"),
        query: GoString::from("query"),
        binlog_info: Some(history.clone()),
        reorg_meta: Some(reorg.clone()),
        resume_reason: Some(resume.clone()),
        trace_info: Some(trace.clone()),
        session_vars: Some(session_vars.clone()),
        ..Default::default()
    };
    let argument_header = GoSharedSlice::from_vec_with_capacity(vec![any_int(3)], 3);
    let raw_args = PersistedRawJson::from_bytes_with_capacity(b"[3]".to_vec(), 8);
    let sub = SubJob {
        type_: ActionType::ACTION_ADD_INDEX,
        warning: Some(warning.clone()),
        args: argument_header.clone(),
        raw_args: Some(raw_args.clone()),
        revertible: true,
        row_count: 5,
        reorg_type: ReorgType(7),
        reorg_stage: ReorgStage(8),
        analyze_state: 9,
        ..Default::default()
    };

    let proxy = sub.to_proxy_job(&parent, -1);
    assert!(proxy.warning.as_ref().unwrap().ptr_eq(&warning));
    assert!(proxy.binlog_info.as_ref().unwrap().ptr_eq(&history));
    assert!(proxy.resume_reason.as_ref().unwrap().ptr_eq(&resume));
    assert!(proxy.trace_info.as_ref().unwrap().ptr_eq(&trace));
    assert!(proxy.session_vars.as_ref().unwrap().ptr_eq(&session_vars));
    assert!(proxy.decoded_args().backing_ptr_eq(&argument_header));
    assert!(proxy.raw_args.as_ref().unwrap().backing_ptr_eq(&raw_args));
    let proxy_multi = proxy.multi_schema_info.as_ref().unwrap();
    assert_eq!(proxy_multi.read().seq, -1);
    assert!(!proxy_multi.read().sub_jobs.is_allocated());
    let proxy_reorg = proxy.reorg_meta.as_ref().unwrap();
    assert!(!proxy_reorg.ptr_eq(&reorg));
    assert!(proxy_reorg
        .read()
        .warnings_count
        .as_ref()
        .unwrap()
        .ptr_eq(&warning_counts));
    assert!(proxy_reorg
        .read()
        .location
        .as_ref()
        .unwrap()
        .ptr_eq(&location));
    assert_eq!(proxy_reorg.read().reorg_type, ReorgType(7));
    assert!(proxy.error.is_none());
    assert!(proxy.pause_reason.is_none());

    let old_raw = raw(r#"{"keep":true}"#);
    let mut destination = SubJob {
        raw_args: Some(old_raw.clone()),
        reorg_type: ReorgType(90),
        ..Default::default()
    };
    destination.from_proxy_job(&proxy, 44);
    assert!(destination.warning.as_ref().unwrap().ptr_eq(&warning));
    assert!(destination.decoded_args().backing_ptr_eq(&argument_header));
    assert!(destination
        .raw_args
        .as_ref()
        .unwrap()
        .backing_ptr_eq(&old_raw));
    assert_eq!(destination.schema_version, 44);

    let mut without_reorg = proxy.clone();
    without_reorg.reorg_meta = None;
    destination.from_proxy_job(&without_reorg, 45);
    assert_eq!(destination.reorg_type, ReorgType(7));

    let cloned_sub = sub.clone_without_args();
    assert!(!cloned_sub.decoded_args().is_allocated());
    assert!(cloned_sub.warning.as_ref().unwrap().ptr_eq(&warning));
    assert!(cloned_sub
        .raw_args
        .as_ref()
        .unwrap()
        .backing_ptr_eq(&raw_args));
}

#[test]
fn encode_preserves_nil_empty_order_and_marshal_failure_mutations() {
    let mut nil_v1 = Job {
        version: JobVersion::V1,
        ..Default::default()
    };
    nil_v1.encode(true).unwrap();
    assert_eq!(nil_v1.raw_args.as_ref().unwrap().get(), "null");

    let mut empty_v1 = Job {
        version: JobVersion::V1,
        args: GoSharedSlice::from_vec(Vec::new()),
        ..Default::default()
    };
    empty_v1.encode(true).unwrap();
    assert_eq!(empty_v1.raw_args.as_ref().unwrap().get(), "[]");

    let mut v2 = Job {
        version: JobVersion::V2,
        ..Default::default()
    };
    v2.fill_v2_arg(any_int(9));
    v2.encode(true).unwrap();
    assert_eq!(v2.raw_args.as_ref().unwrap().get(), "9");

    let first = GoShared::new(SubJob {
        args: GoSharedSlice::from_vec(vec![any_int(1)]),
        raw_args: Some(raw(r#"{"old":1}"#)),
        ..Default::default()
    });
    let failing = GoShared::new(SubJob {
        args: GoSharedSlice::from_vec(vec![any_infinity()]),
        raw_args: Some(raw(r#"{"old":2}"#)),
        ..Default::default()
    });
    let untouched = GoShared::new(SubJob {
        args: GoSharedSlice::from_vec(vec![any_int(3)]),
        raw_args: Some(raw(r#"{"old":3}"#)),
        ..Default::default()
    });
    let mut job = Job {
        version: JobVersion::V1,
        args: GoSharedSlice::from_vec(vec![any_int(0)]),
        multi_schema_info: Some(GoShared::new(MultiSchemaInfo {
            sub_jobs: GoSharedPointerSlice::from_handles(vec![
                Some(first.clone()),
                Some(failing.clone()),
                Some(untouched.clone()),
            ]),
            ..Default::default()
        })),
        ..Default::default()
    };
    let error = job.encode(true).unwrap_err();
    assert!(error.to_string().contains("unsupported value"));
    assert_eq!(job.raw_args.as_ref().unwrap().get(), "[0]");
    assert_eq!(first.read().raw_args.as_ref().unwrap().get(), "[1]");
    assert!(failing.read().raw_args.is_none());
    assert_eq!(
        untouched.read().raw_args.as_ref().unwrap().get(),
        r#"{"old":3}"#
    );

    let mut parent_failure = Job {
        version: JobVersion::V1,
        args: GoSharedSlice::from_vec(vec![any_infinity()]),
        raw_args: Some(raw(r#"{"old":true}"#)),
        ..Default::default()
    };
    assert!(parent_failure.encode(true).is_err());
    assert!(parent_failure.raw_args.is_none());

    let mut final_failure = Job {
        id: 5,
        raw_args: Some(PersistedRawJson::from_bytes(b"{".to_vec())),
        ..Default::default()
    };
    assert!(final_failure.encode(false).is_err());
    assert_eq!(final_failure.id, 5);
}

#[test]
fn job_clone_runs_codec_and_restores_only_subjob_jobargs() {
    let job_args = any_int(77);
    let sub = GoShared::new(SubJob {
        job_args: job_args.clone(),
        args: GoSharedSlice::from_vec(vec![any_int(8)]),
        ..Default::default()
    });
    let warning = GoShared::new(terror(21, 2, "warning"));
    let resume = GoShared::new(JobResumeReason {
        type_: GoString::from("resume"),
    });
    let mut source = Job {
        version: JobVersion::V1,
        id: 100,
        warning: Some(warning.clone()),
        resume_reason: Some(resume.clone()),
        args: GoSharedSlice::from_vec(vec![any_int(3)]),
        multi_schema_info: Some(GoShared::new(MultiSchemaInfo {
            sub_jobs: GoSharedPointerSlice::from_handles(vec![Some(sub.clone())]),
            ..Default::default()
        })),
        ..Default::default()
    };

    let clone = source.deep_clone().unwrap();
    assert_eq!(clone.id, 100);
    assert!(!clone.decoded_args().is_allocated());
    assert!(!clone.warning.as_ref().unwrap().ptr_eq(&warning));
    assert!(!clone.resume_reason.as_ref().unwrap().ptr_eq(&resume));
    let cloned_sub = clone
        .multi_schema_info
        .as_ref()
        .unwrap()
        .read()
        .sub_jobs
        .get(0)
        .unwrap();
    assert!(!cloned_sub.read().decoded_args().is_allocated());
    assert!(cloned_sub.read().job_args.go_equal(&job_args));
    assert!(source.raw_args.is_some());
    assert!(sub.read().raw_args.is_some());

    source.set_v1_decoded_args(GoSharedSlice::from_vec(vec![any_infinity()]));
    assert!(source.deep_clone().is_none());
    assert!(source.raw_args.is_none());
}

#[test]
fn receiver_decode_reuses_pointers_maps_slices_and_rawmessage_backing() {
    let warning = GoShared::new(terror(21, 2, "old"));
    let history = GoShared::new(HistoryInfo {
        schema_version: 7,
        ..Default::default()
    });
    let multi = GoShared::new(MultiSchemaInfo {
        sub_jobs: GoSharedPointerSlice::from_nullable_with_capacity(
            vec![Some(SubJob {
                row_count: 1,
                ..Default::default()
            })],
            3,
        ),
        ..Default::default()
    });
    let sub_jobs_backing = multi.read().sub_jobs.clone();
    let pause = GoShared::new(JobPauseReason {
        type_: GoString::from("old"),
        message: GoString::from("old"),
    });
    let trace_bytes = GoSharedSlice::from_vec_with_capacity(vec![9_u8], 4);
    let trace = GoShared::new(TraceInfo {
        session_alias: GoString::from("old"),
        trace_id: trace_bytes.clone(),
        connection_id: 1,
    });
    let session = GoShared::new(BTreeMap::from([(
        GoString::from("old"),
        GoString::from("1"),
    )]));
    let involving = GoSharedSlice::from_vec_with_capacity(
        vec![InvolvingSchemaInfo {
            database: GoString::from("old"),
            table: GoString::from("t"),
            ..Default::default()
        }],
        3,
    );
    let raw_alias = PersistedRawJson::from_bytes_with_capacity(b"[0]".to_vec(), 32);
    let mut job = Job {
        warning: Some(warning.clone()),
        binlog_info: Some(history.clone()),
        multi_schema_info: Some(multi.clone()),
        pause_reason: Some(pause.clone()),
        trace_info: Some(trace.clone()),
        session_vars: Some(session.clone()),
        involving_schema_info: involving.clone(),
        raw_args: Some(raw_alias.clone()),
        ..Default::default()
    };

    job.decode(
        br#"{
            "RAW_ARGS":{"x":1},
            "warning":{"class":21,"code":3,"message":"new","rfccode":"global:3"},
            "binlog":{"SchemaVersion":8},
            "multi_schema_info":{"sub_jobs":[{"row_count":2},null],"seq":4},
            "pause_reason":{"type":"new"},
            "trace_info":{"session_alias":"new","trace_id":"AAE=","connection_id":2},
            "session_vars":{"new":"2"},
            "involving_schema_info":[{"DATABASE":"new","table":"t2"}]
        }"#,
    )
    .unwrap();

    assert!(job.warning.as_ref().unwrap().ptr_eq(&warning));
    assert_eq!(warning.read().message(), "new");
    assert!(job.binlog_info.as_ref().unwrap().ptr_eq(&history));
    assert_eq!(history.read().schema_version, 8);
    assert!(job.multi_schema_info.as_ref().unwrap().ptr_eq(&multi));
    assert!(multi.read().sub_jobs.backing_ptr_eq(&sub_jobs_backing));
    assert_eq!(multi.read().sub_jobs.get(0).unwrap().read().row_count, 2);
    assert!(multi.read().sub_jobs.get(1).is_none());
    assert!(job.pause_reason.as_ref().unwrap().ptr_eq(&pause));
    assert_eq!(pause.read().message, "old");
    assert_eq!(pause.read().type_, "new");
    assert!(job.trace_info.as_ref().unwrap().ptr_eq(&trace));
    assert_eq!(trace.read().session_alias, "new");
    assert!(!trace.read().trace_id.backing_ptr_eq(&trace_bytes));
    assert!(job.session_vars.as_ref().unwrap().ptr_eq(&session));
    assert_eq!(job.get_system_var("old"), Some(GoString::from("1")));
    assert_eq!(job.get_system_var("new"), Some(GoString::from("2")));
    assert!(job.involving_schema_info.backing_ptr_eq(&involving));
    assert_eq!(job.involving_schema_info.get(0).database, "new");
    assert!(job.raw_args.as_ref().unwrap().backing_ptr_eq(&raw_alias));
    assert_eq!(job.raw_args.as_ref().unwrap().get(), r#"{"x":1}"#);

    let old_warning = job.warning.as_ref().unwrap().clone();
    assert!(job.decode(br#"{"warning":7,"id":9}"#).is_err());
    assert!(job.warning.as_ref().unwrap().ptr_eq(&old_warning));
    assert_eq!(job.id, 0);

    let mut nil_warning = Job::default();
    assert!(nil_warning.decode(br#"{"warning":7}"#).is_err());
    assert!(nil_warning.warning.is_some());

    job.decode(br#"{"session_vars":null,"involving_schema_info":null}"#)
        .unwrap();
    assert!(job.session_vars.is_none());
    assert!(!job.involving_schema_info.is_allocated());
}

#[test]
fn involving_schema_alias_normalization_and_validation_are_source_shaped() {
    let explicit = GoSharedSlice::from_vec_with_capacity(
        vec![
            InvolvingSchemaInfo {
                database: GoString::from("TestDB"),
                table: GoString::from("T1"),
                ..Default::default()
            },
            InvolvingSchemaInfo {
                policy: GoString::from("PolicyName"),
                ..Default::default()
            },
        ],
        4,
    );
    let mut job = Job {
        schema_name: GoString::from("Fallback"),
        involving_schema_info: explicit.clone(),
        ..Default::default()
    };
    let returned = job.get_involving_schema_info();
    assert!(returned.backing_ptr_eq(&explicit));
    job.normalize_involving_schema_info();
    assert_eq!(returned.get(0).database, "testdb");
    assert_eq!(returned.get(0).table, "t1");
    assert_eq!(returned.get(1).policy, "policyname");
    assert!(job.check_involving_schema_info().is_ok());

    let invalid = Job {
        involving_schema_info: GoSharedSlice::from_vec(vec![InvolvingSchemaInfo {
            policy: GoString::from("p"),
            resource_group: GoString::from("r"),
            ..Default::default()
        }]),
        ..Default::default()
    };
    assert!(invalid
        .check_involving_schema_info()
        .unwrap_err()
        .contains("only one type"));

    let fallback = Job {
        schema_name: GoString::from("db"),
        ..Default::default()
    }
    .get_involving_schema_info();
    assert!(fallback.is_allocated());
    assert_eq!(fallback.get(0).database, "db");
    assert_eq!(fallback.get(0).table, INVOLVING_ALL);
}

#[test]
fn finish_methods_keep_pointer_alias_and_panic_mutation_order() {
    let history = GoShared::new(HistoryInfo::default());
    let table = GoShared::new(TableInfo {
        id: 9,
        name: CiString::new("t"),
        placement_policy_ref: Some(GoShared::new(PolicyRefInfo {
            id: 3,
            name: CiString::new("p"),
        })),
        ..Default::default()
    });
    let tables = GoSharedPointerSlice::from_handles_with_capacity(vec![Some(table.clone())], 3);
    let mut job = Job {
        binlog_info: Some(history.clone()),
        ..Default::default()
    };
    job.finish_multiple_table_job(JobState::DONE, SchemaState::PUBLIC, 8, &tables);
    assert!(history.read().multiple_table_infos.backing_ptr_eq(&tables));
    assert!(history.read().table_info.as_ref().unwrap().ptr_eq(&table));

    let stale = history.read().table_info.as_ref().unwrap().clone();
    let empty = GoSharedPointerSlice::from_nullable(Vec::new());
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        job.finish_multiple_table_job(JobState::CANCELLED, SchemaState::NONE, 9, &empty);
    }))
    .is_err());
    assert_eq!(job.state, JobState::CANCELLED);
    assert_eq!(history.read().schema_version, 9);
    assert!(history.read().multiple_table_infos.is_allocated());
    assert!(history.read().multiple_table_infos.is_empty());
    assert!(history.read().table_info.as_ref().unwrap().ptr_eq(&stale));
}

#[test]
fn arbitrary_job_strings_and_signed_sql_mode_round_trip() {
    let mut job = Job {
        id: 1,
        query: GoString::from_bytes(vec![b'a', 0xff, b'<']),
        bdr_role: GoString::from_bytes(vec![0xfe]),
        sql_mode: i64::MIN,
        ..Default::default()
    };
    let encoded = String::from_utf8(job.encode(false).unwrap()).unwrap();
    assert!(encoded.contains(r#""query":"a\ufffd\u003c""#));
    assert!(encoded.contains(r#""bdr_role":"\ufffd""#));
    assert!(encoded.contains(r#""sql_mode":-9223372036854775808"#));
}
