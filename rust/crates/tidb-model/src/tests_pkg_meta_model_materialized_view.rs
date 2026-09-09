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

//! Go-derived regressions for the materialized-view metadata drift in
//! `pkg/meta/model` (Go master `94a9cbedab`): action types 85/86, BDR safe
//! classification, `MayNeedReorg`/`IsRollbackable`, the `TableInfo`
//! materialized-view fields and their JSON/clone contracts, `TimeZoneLocation`
//! clone reuse, the job-args v1/v2 round trips, and the `SubJob`
//! `InvolvingSchemaInfo` propagation.

use super::*;
use crate::serde_helpers::GoJsonMerge;
use tidb_ast::CiString;
use tidb_datatype::GoString;

fn encoded_job<T: JobArgs>(version: JobVersion, action: ActionType, value: GoShared<T>) -> Job {
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

/// Go `TestMayNeedReorg` general/dedicated additions: action 86 belongs to the
/// reorganization set.
#[test]
fn create_materialized_view_may_need_reorg() {
    let job = Job {
        type_: ActionType::ACTION_CREATE_MATERIALIZED_VIEW,
        ..Default::default()
    };
    assert!(job.may_need_reorg());
    let log_job = Job {
        type_: ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG,
        ..Default::default()
    };
    assert!(!log_job.may_need_reorg());
}

/// Go `TestCreateMaterializedViewRollbackable`: rollbackable in `StateNone`
/// and `StateWriteReorganization`, not rollbackable in `StatePublic`.
#[test]
fn create_materialized_view_rollbackable_states() {
    let mut job = Job {
        type_: ActionType::ACTION_CREATE_MATERIALIZED_VIEW,
        schema_state: SchemaState::NONE,
        ..Default::default()
    };
    assert!(job.is_rollbackable());
    job.schema_state = SchemaState::WRITE_REORGANIZATION;
    assert!(job.is_rollbackable());
    job.schema_state = SchemaState::PUBLIC;
    assert!(!job.is_rollbackable());
}

/// Go `ActionMap` names for the two new actions, and the BDR `SafeDDL`
/// membership added by Go `bdr.go`.
#[test]
fn mv_action_names_and_bdr_safe_classification() {
    let action_map: std::collections::HashMap<u16, &str> = ACTION_MAP
        .iter()
        .map(|(action, name)| (u16::from(action.0), *name))
        .collect();
    assert_eq!(
        action_map
            .get(&u16::from(
                ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG.0
            ))
            .copied(),
        Some("create materialized view log")
    );
    assert_eq!(
        action_map
            .get(&u16::from(ActionType::ACTION_CREATE_MATERIALIZED_VIEW.0))
            .copied(),
        Some("create materialized view")
    );

    let safe = BDR_ACTION_MAP
        .read()
        .get(&DDLBDRType::SAFE_DDL)
        .cloned()
        .expect("SafeDDL entry");
    let safe_values: Vec<u16> = safe
        .snapshot()
        .iter()
        .map(|action| u16::from(action.0))
        .collect();
    assert!(safe_values.contains(&u16::from(
        ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG.0
    )));
    assert!(safe_values.contains(&u16::from(ActionType::ACTION_CREATE_MATERIALIZED_VIEW.0)));
}

/// Go `TestCreateTableArgs` "create materialized view" subtest: the job args
/// round trip through both job versions, preserving the table info and the
/// materialized view log table identifiers.
#[test]
fn create_materialized_view_args_roundtrip() {
    let mut table_info = TableInfo::default();
    table_info.id = 102;
    let in_args = GoShared::new(CreateMaterializedViewArgs {
        table_info: GoField::new(Some(GoShared::new(table_info))),
        mlog_table_ids: GoField::new(GoSharedSlice::from_vec(vec![99])),
    });
    for version in [JobVersion::V1, JobVersion::V2] {
        let mut job = encoded_job(
            version,
            ActionType::ACTION_CREATE_MATERIALIZED_VIEW,
            in_args.clone(),
        );
        let args = get_create_materialized_view_args(&mut job)
            .expect("decode job arguments")
            .expect("typed args present");
        let table = args.read().table_info.get().expect("table info present");
        assert_eq!(table.read().id, 102);
        assert_eq!(
            args.read().mlog_table_ids.get().snapshot(),
            vec![99],
            "mlog table ids survive the round trip"
        );
    }
}

/// Go `GetCreateMaterializedViewLogArgs`: the log-job args carry only the
/// table info on both job versions.
#[test]
fn create_materialized_view_log_args_roundtrip() {
    let mut table_info = TableInfo::default();
    table_info.id = 88;
    let in_args = GoShared::new(CreateMaterializedViewLogArgs {
        table_info: GoField::new(Some(GoShared::new(table_info))),
    });
    for version in [JobVersion::V1, JobVersion::V2] {
        let mut job = encoded_job(
            version,
            ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG,
            in_args.clone(),
        );
        let args = get_create_materialized_view_log_args(&mut job)
            .expect("decode job arguments")
            .expect("typed args present");
        let table = args.read().table_info.get().expect("table info present");
        assert_eq!(table.read().id, 88);
    }
}

/// Go `table.go` JSON shape: absent materialized-view metadata is omitted
/// (`omitempty` pointers), present metadata round trips through the Go-exact
/// JSON merge, and `TableInfo.Clone` deep-copies the metadata slices.
#[test]
fn table_info_mv_fields_omitempty_merge_and_clone() {
    let plain = TableInfo::default();
    let text = serde_json::to_string(&plain).expect("serialize plain table");
    assert!(!text.contains("materialized_view_base"));
    assert!(!text.contains("\"materialized_view\""));
    assert!(!text.contains("materialized_view_log"));

    let mut base_info = MaterializedViewBaseInfo::default();
    base_info.mlog_id = 7;
    base_info.mview_ids = vec![1, 2].into();
    let mut mv_info = MaterializedViewInfo::default();
    mv_info.sql_content = "select 1".to_string();
    mv_info.base_table_ids = vec![88].into();
    mv_info.init_build_state = MViewInitBuildState::INIT_BUILD_DEFERRED;
    let mut log_info = MaterializedViewLogInfo::default();
    log_info.base_table_id = 88;
    log_info.log_accumulation_alert_rows = Some(5000);

    let mut table = TableInfo::default();
    table.materialized_view_base = Some(GoShared::new(base_info));
    table.materialized_view = Some(GoShared::new(mv_info));
    table.materialized_view_log = Some(GoShared::new(log_info));

    let text = serde_json::to_string(&table).expect("serialize decorated table");
    assert!(text.contains("\"materialized_view_base\""));
    assert!(text.contains("\"mlog_id\":7"));
    assert!(text.contains("\"materialized_view\""));
    // `init_build_state` is 1 (deferred), so omitempty keeps it.
    assert!(text.contains("\"init_build_state\":1"));
    assert!(text.contains("\"sql_content\":\"select 1\""));
    assert!(text.contains("\"materialized_view_log\""));
    assert!(text.contains("\"log_accumulation_alert_rows\":5000"));

    let mut decoded = TableInfo::default();
    decoded.decode(text.as_bytes()).expect("merge decode");
    let decoded_view = decoded
        .materialized_view
        .as_ref()
        .expect("decoded materialized view")
        .read()
        .clone();
    assert_eq!(decoded_view.sql_content, "select 1");
    assert_eq!(
        decoded_view.init_build_state,
        MViewInitBuildState::INIT_BUILD_DEFERRED
    );

    let clone = decoded.clone_like_go();
    clone
        .materialized_view_base
        .as_ref()
        .expect("cloned base info")
        .write()
        .mview_ids
        .iter_mut()
        .for_each(|value| *value = 999);
    let original_ids: Vec<i64> = decoded
        .materialized_view_base
        .as_ref()
        .expect("original base info")
        .read()
        .mview_ids
        .iter()
        .copied()
        .collect();
    assert_eq!(original_ids, vec![1, 2], "Clone deep-copies the id slice");
}

/// Go `TestMaterializedViewInfoClone` plus the ready/deferred/building display
/// and access-error contracts from `table.go`.
#[test]
fn mv_info_display_access_error_and_timezone_clone() {
    assert_eq!(MViewInitBuildState::INIT_BUILD_READY.to_string(), "ready");
    assert_eq!(
        MViewInitBuildState::INIT_BUILD_DEFERRED.to_string(),
        "deferred"
    );
    assert_eq!(
        MViewInitBuildState::INIT_BUILD_BUILDING.to_string(),
        "building"
    );
    assert_eq!(MViewInitBuildState(9).to_string(), "unknown(9)");
    assert!(MViewInitBuildState::INIT_BUILD_READY.is_ready());
    assert!(!MViewInitBuildState::INIT_BUILD_DEFERRED.is_ready());

    assert_eq!(
        MViewInitBuildState::INIT_BUILD_DEFERRED.access_error_message("mview1"),
        "materialized view mview1 is not ready: initial build has not completed"
    );
    assert_eq!(
        MViewInitBuildState::INIT_BUILD_BUILDING.access_error_message("mview1"),
        "materialized view mview1 initial build is in progress"
    );
    assert_eq!(
        MViewInitBuildState::INIT_BUILD_READY.access_error_message("mview1"),
        ""
    );

    let mut info = MaterializedViewInfo::default();
    info.base_table_ids = vec![1, 2].into();
    info.sql_content = "select 1".to_string();
    info.definition_div_precision_increment = 4;
    info.definition_time_zone.name = GoString::from("UTC");
    info.refresh_schedule_time_zone.name = GoString::from("Asia/Shanghai");
    info.definition_time_zone
        .get_location()
        .expect("resolve definition time zone");

    let clone = info.clone_like_go();
    assert_eq!(clone.sql_content, "select 1");
    assert_eq!(clone.definition_div_precision_increment, 4);
    assert_eq!(
        clone.definition_time_zone.name.as_utf8().expect("utf8"),
        "UTC"
    );
    assert_eq!(
        clone
            .refresh_schedule_time_zone
            .name
            .as_utf8()
            .expect("utf8"),
        "Asia/Shanghai"
    );
    let location = clone
        .definition_time_zone
        .get_location()
        .expect("clone re-resolves the time zone cache");
    match &*location.read() {
        ResolvedTimeZone::Named(tz) => assert_eq!(tz.to_string(), "UTC"),
        other => panic!("expected named UTC zone, got {other:?}"),
    }

    // Go `GetInitBuildState`: nil metadata is ready.
    assert_eq!(
        MaterializedViewInfo::get_init_build_state(None),
        MViewInitBuildState::INIT_BUILD_READY
    );
    assert_eq!(
        MaterializedViewInfo::get_init_build_state(Some(&info)),
        info.init_build_state
    );
}

/// Go `MaterializedViewLogInfo.EffectiveLogAccumulationAlertRows` and the
/// physical log-table-name derivation with Go's rune budget.
#[test]
fn mv_log_rows_threshold_and_table_name() {
    assert_eq!(
        MaterializedViewLogInfo::effective_log_accumulation_alert_rows(None),
        None
    );
    let mut log = MaterializedViewLogInfo::default();
    assert_eq!(
        MaterializedViewLogInfo::effective_log_accumulation_alert_rows(Some(&log)),
        None,
        "a nil alert pointer is disabled"
    );
    log.log_accumulation_alert_rows = Some(0);
    assert_eq!(
        MaterializedViewLogInfo::effective_log_accumulation_alert_rows(Some(&log)),
        None,
        "a zero threshold is disabled"
    );
    log.log_accumulation_alert_rows = Some(4);
    assert_eq!(
        MaterializedViewLogInfo::effective_log_accumulation_alert_rows(Some(&log)),
        Some(4)
    );

    let short = materialized_view_log_table_name(&CiString::new("orders"));
    assert_eq!(short.original(), "$mlog$orders");
    assert_eq!(short.lowercase(), "$mlog$orders");

    let long_name = "a".repeat(80);
    let truncated = materialized_view_log_table_name(&CiString::new(&long_name));
    // 64 rune budget minus the 6-rune prefix leaves 58 base-name runes.
    let expected = format!("$mlog${}", "a".repeat(58));
    assert_eq!(truncated.original(), expected);
    assert_eq!(
        truncated.original().chars().count(),
        tidb_mysql::consts::MaxTableNameLength
    );
}

/// Go `SubJob.InvolvingSchemaInfo`: omitempty persistence, proxy-job
/// propagation in both directions, and the non-serialized
/// `MultiSchemaInfo.InvolvingSchemaInfo` runtime field.
#[test]
fn subjob_involving_schema_info_persists_and_propagates() {
    let mut sub_job = SubJob {
        type_: ActionType::ACTION_CREATE_TABLE,
        ..Default::default()
    };
    let text = serde_json::to_string(&sub_job).expect("serialize bare sub-job");
    assert!(!text.contains("involving_schema_info"));

    sub_job.involving_schema_info = GoSharedSlice::from_vec(vec![InvolvingSchemaInfo {
        database: GoString::from("test"),
        table: GoString::from("t"),
        mode: InvolvingSchemaInfoMode::SHARED,
        ..Default::default()
    }]);
    let text = serde_json::to_string(&sub_job).expect("serialize decorated sub-job");
    assert!(text.contains("\"involving_schema_info\""));
    assert!(text.contains("\"mode\":1"));

    let mut decoded = SubJob::default();
    let mut deserializer = serde_json::Deserializer::from_str(&text);
    decoded
        .go_json_merge(&mut deserializer)
        .expect("merge decode sub-job");
    deserializer.end().expect("trailing characters");
    assert_eq!(
        decoded.involving_schema_info.snapshot()[0]
            .database
            .as_utf8()
            .expect("utf8"),
        "test"
    );

    let mut parent = Job {
        type_: ActionType::ACTION_MULTI_SCHEMA_CHANGE,
        ..Default::default()
    };
    parent.multi_schema_info = Some(GoShared::new(MultiSchemaInfo {
        sub_jobs: GoSharedPointerSlice::from_handles(vec![Some(GoShared::new(sub_job.clone()))]),
        ..Default::default()
    }));
    let proxy = sub_job.to_proxy_job(&parent, 0);
    assert_eq!(
        proxy.involving_schema_info.snapshot()[0]
            .table
            .as_utf8()
            .expect("utf8"),
        "t",
        "ToProxyJob carries the sub-job's involving schema info"
    );

    let mut back = SubJob::default();
    back.from_proxy_job(&proxy, 3);
    assert_eq!(
        back.involving_schema_info.snapshot()[0]
            .table
            .as_utf8()
            .expect("utf8"),
        "t",
        "FromProxyJob copies the involving schema info back"
    );
    assert_eq!(back.schema_version, 3);

    // Go tags the runtime field `json:"-"`: it must not serialize.
    let mut info = MultiSchemaInfo::default();
    info.involving_schema_info = GoSharedSlice::from_vec(vec![InvolvingSchemaInfo::default()]);
    let text = serde_json::to_string(&info).expect("serialize multi-schema info");
    assert!(!text.contains("involving_schema_info"));
}
