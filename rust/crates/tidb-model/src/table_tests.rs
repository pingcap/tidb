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

fn go_json<T: serde::Serialize>(value: &T) -> String {
    String::from_utf8(crate::serde_helpers::to_go_json(value).unwrap()).unwrap()
}

// The five `TableInfo` sub-struct enums that used to reject an
// unrecognised ordinal outright. Go declares all five as plain
// `int`/`byte`, so a document written by a newer TiDB decodes there and
// must decode here: an unknown `column_choice` may not take out the whole
// table. Each value is preserved byte for byte across the cycle.
#[test]
fn unknown_ast_enum_ordinals_survive_round_trip() {
    let view = r#"{"view_algorithm":7,"view_definer":{"Username":"root","Hostname":"%","CurrentUser":false,"AuthUsername":"","AuthHostname":"","AuthPlugin":""},"view_security":5,"view_select":"SELECT 1","view_checkoption":9,"view_cols":null}"#;
    let decoded: ViewInfo = serde_json::from_str(view).unwrap();
    assert_eq!(decoded.algorithm, ViewAlgorithm(7));
    assert_eq!(decoded.security, ViewSecurity(5));
    assert_eq!(decoded.check_option, ViewCheckOption(9));
    // Go's `String` falls through to these defaults rather than erroring.
    assert_eq!(decoded.algorithm.sql(), "UNDEFINED");
    assert_eq!(decoded.security.sql(), "DEFINER");
    assert_eq!(decoded.check_option.sql(), "CASCADED");
    assert_eq!(go_json(&decoded), view);

    let stats = r#"{"auto_recalc":true,"column_choice":4,"column_list":[],"sample_num":0,"sample_rate":0,"buckets":0,"topn":0,"concurrency":0}"#;
    let decoded: StatsOptions = serde_json::from_str(stats).unwrap();
    assert_eq!(decoded.column_choice, ColumnChoice(4));
    assert_eq!(decoded.column_choice.sql(), "DEFAULT");
    assert_eq!(go_json(&decoded), stats);

    let lock = r#"{"Tp":6,"Sessions":null,"State":0,"TS":0}"#;
    let decoded: TableLockInfo = serde_json::from_str(lock).unwrap();
    assert_eq!(decoded.tp, TableLockType(6));
    assert_eq!(decoded.tp.sql(), "");
    assert_eq!(go_json(&decoded), lock);
}

#[test]
fn enum_strings() {
    assert_eq!(TableCacheStatusType::DISABLE.to_string(), "disable");
    assert_eq!(TableCacheStatusType::ENABLE.to_string(), "enable");
    assert_eq!(TableCacheStatusType::SWITCHING.to_string(), "switching");
    assert_eq!(TableCacheStatusType(9).to_string(), "");

    assert_eq!(TempTableType::NONE.to_string(), "");
    assert_eq!(TempTableType::GLOBAL.to_string(), "global");
    assert_eq!(TempTableType::LOCAL.to_string(), "local");

    assert_eq!(TableLockState::NONE.to_string(), "none");
    assert_eq!(TableLockState::PRE_LOCK.to_string(), "pre-lock");
    assert_eq!(TableLockState::PUBLIC.to_string(), "public");
    assert_eq!(TableLockState(9).to_string(), "none");
}

#[test]
fn fk_string() {
    let fk = FKInfo {
        name: CiString::new("fk1"),
        ref_schema: CiString::new("db2"),
        ref_table: CiString::new("parent"),
        ref_cols: vec![CiString::new("id"), CiString::new("x")].into(),
        cols: vec![CiString::new("a"), CiString::new("b")].into(),
        on_delete: 2, // CASCADE
        on_update: 0, // NoOption
        ..Default::default()
    };
    assert_eq!(
        fk.string("db1", "child"),
        "`db1`.`child`, CONSTRAINT `fk1` FOREIGN KEY (`a`, `b`) REFERENCES \
         `db2`.`parent` (`id`, `x`) ON DELETE CASCADE"
    );

    // Same-schema reference omits the schema; ON UPDATE included.
    let fk = FKInfo {
        name: CiString::new("fk2"),
        ref_schema: CiString::new("db1"),
        ref_table: CiString::new("parent"),
        ref_cols: vec![CiString::new("id")].into(),
        cols: vec![CiString::new("pid")].into(),
        on_delete: 0,
        on_update: 1, // RESTRICT
        ..Default::default()
    };
    assert_eq!(
        fk.string("db1", "child"),
        "`db1`.`child`, CONSTRAINT `fk2` FOREIGN KEY (`pid`) REFERENCES \
         `parent` (`id`) ON UPDATE RESTRICT"
    );

    let foreign_keys: GoSharedPointerSlice<FKInfo> = vec![fk].into();
    assert!(find_fk_info_by_name(&foreign_keys, "fk2").is_some());
    // Source requires the caller to supply the lower-case lookup key.
    assert!(find_fk_info_by_name(&foreign_keys, "FK2").is_none());

    find_fk_info_by_name_mut(&foreign_keys, "fk2")
        .unwrap()
        .write()
        .version = FK_VERSION1;
    assert_eq!(foreign_keys.get(0).unwrap().read().version, FK_VERSION1);

    let nullable = GoSharedPointerSlice::from_nullable(vec![None, Some(FKInfo::default())]);
    assert!(std::panic::catch_unwind(|| find_fk_info_by_name(&nullable, "fk2")).is_err());
}

#[test]
fn fk_clone_uses_slices_clone_and_nil_receiver_panics() {
    assert!(std::panic::catch_unwind(|| FKInfo::clone_pointer(None)).is_err());
    let empty = GoSharedSlice::<CiString>::from_vec_with_capacity(Vec::new(), 4);
    let values = GoSharedSlice::from_vec_with_capacity(
        (0..18)
            .map(|index| CiString::new(format!("c{index}")))
            .collect(),
        24,
    );
    let source = FKInfo {
        ref_cols: empty.clone(),
        cols: values.clone(),
        ..Default::default()
    };
    let structural = source.clone();
    assert!(structural.ref_cols.backing_ptr_eq(&empty));
    assert!(structural.cols.backing_ptr_eq(&values));

    let cloned = source.clone_like_go();
    assert!(!cloned.ref_cols.backing_ptr_eq(&empty));
    assert_eq!(cloned.ref_cols.capacity(), 0);
    assert!(!cloned.cols.backing_ptr_eq(&values));
    assert_eq!(cloned.cols.capacity(), 19);
    cloned
        .cols
        .update(0, |column| *column = CiString::new("clone"));
    assert_eq!(values.get(0).original(), "c0");

    let nil = FKInfo::default().clone_like_go();
    assert!(!nil.ref_cols.is_allocated());
    assert!(!nil.cols.is_allocated());
}

#[test]
fn changing_field_type_boundary() {
    let mut column = ColumnInfo {
        field_type: tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Long),
        changing_field_type: Some(GoShared::new(tidb_datatype::FieldType::new(
            tidb_datatype::FieldTypeCode::Varchar,
        ))),
        ..Default::default()
    };
    let mut index_column = IndexColumn::default();
    assert_eq!(
        get_idx_changing_field_type(&index_column, &column).code(),
        tidb_datatype::FieldTypeCode::Long
    );
    index_column.use_changing_type = true;
    assert_eq!(
        get_idx_changing_field_type(&index_column, &column).code(),
        tidb_datatype::FieldTypeCode::Varchar
    );
    column.changing_field_type = None;
    assert_eq!(
        get_idx_changing_field_type(&index_column, &column).code(),
        tidb_datatype::FieldTypeCode::Long
    );

    column.changing_field_type = Some(GoShared::new(tidb_datatype::FieldType::new(
        tidb_datatype::FieldTypeCode::Varchar,
    )));
    *get_idx_changing_field_type_mut(&index_column, &mut column) =
        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Double);
    assert_eq!(
        column.changing_field_type.as_ref().unwrap().read().code(),
        tidb_datatype::FieldTypeCode::Double
    );
    index_column.use_changing_type = false;
    *get_idx_changing_field_type_mut(&index_column, &mut column) =
        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Float);
    assert_eq!(
        column.field_type.code(),
        tidb_datatype::FieldTypeCode::Float
    );
}

#[test]
fn statistics_keys_ignore_only_the_source_excluded_bit() {
    let item = TableItemID {
        table_id: 12,
        id: 34,
        is_index: true,
        is_sync_load_failed: false,
    };
    assert_eq!(item.key(), "34#12#true");
    assert_eq!(
        TableItemID {
            is_sync_load_failed: true,
            ..item
        }
        .key(),
        item.key()
    );
    assert_eq!(
        StatsLoadItem {
            table_item_id: item,
            full_load: false,
        }
        .key(),
        "34#12#true#false"
    );
}

#[test]
fn ttl_interval_and_affinity_boundaries() {
    assert_eq!(
        TTLInfo::default().get_job_interval().unwrap(),
        3_600_000_000_000
    );
    assert_eq!(
        TTLInfo {
            job_interval: "24h".to_owned(),
            ..Default::default()
        }
        .get_job_interval()
        .unwrap(),
        86_400_000_000_000
    );
    assert!(TTLInfo {
        job_interval: "bad".to_owned(),
        ..Default::default()
    }
    .get_job_interval()
    .is_err());

    assert!(new_table_affinity_info_with_level("").unwrap().is_none());
    assert!(new_table_affinity_info_with_level("NONE")
        .unwrap()
        .is_none());
    assert_eq!(
        new_table_affinity_info_with_level("PaRtItIoN")
            .unwrap()
            .unwrap()
            .level,
        "partition"
    );
    assert_eq!(
        new_table_affinity_info_with_level("bogus").unwrap_err(),
        "invalid table affinity level: 'bogus'"
    );
}

#[test]
fn stats_options_and_window() {
    assert_eq!(WindowRepeatType::NEVER.to_string(), "Never");
    assert_eq!(WindowRepeatType::DAY.to_string(), "Day");
    assert_eq!(WindowRepeatType::WEEK.to_string(), "Week");
    assert_eq!(WindowRepeatType::MONTH.to_string(), "Month");
    assert_eq!(WindowRepeatType(9).to_string(), "");

    let opts = StatsOptions::new();
    assert!(opts.auto_recalc);
    assert_eq!(opts.column_choice, ColumnChoice::DEFAULT);
    assert_eq!(opts.column_list, Some(Vec::new()));
    assert!(opts.stats_window_settings.is_none());
    // Default (not the constructor) has auto_recalc false.
    let zero = StatsOptions::default();
    assert!(!zero.auto_recalc);
    assert!(zero.column_list.is_none());
    assert!(go_json(&zero).contains(r#""column_list":null"#));
}

#[test]
fn view_info_basic() {
    let v = ViewInfo {
        select_stmt: "SELECT 1".to_owned(),
        cols: vec![CiString::new("a")].into(),
        definer: Some(Box::new(UserIdentity {
            username: "root".to_owned(),
            ..Default::default()
        })),
        ..Default::default()
    };
    assert_eq!(v.select_stmt, "SELECT 1");
    assert_eq!(v.definer.as_ref().unwrap().username, "root");
    // Clone is a deep copy.
    let c = v.clone();
    assert_eq!(c.cols[0].original(), "a");
}

#[test]
fn view_info_merges_pointer_fields_and_slices_like_go() {
    use crate::serde_helpers::GoJsonMerge;

    let nil_cols: ViewInfo = serde_json::from_str(r#"{"view_cols":null}"#).unwrap();
    let empty_cols: ViewInfo = serde_json::from_str(r#"{"view_cols":[]}"#).unwrap();
    assert!(!nil_cols.cols.is_allocated());
    assert!(empty_cols.cols.is_allocated());

    let mut view = ViewInfo {
        definer: Some(Box::new(UserIdentity {
            username: "before".to_owned(),
            ..Default::default()
        })),
        select_stmt: "before".to_owned(),
        cols: vec![CiString::new("old")].into(),
        ..Default::default()
    };
    let mut decoder = serde_json::Deserializer::from_str(
        r#"{"VIEW_DEFINER":{"Username":"root"},"view_definer":{"Hostname":"%","Username":7},"view_ſelect":"after","view_cols":[{"L":"merged"},null]}"#,
    );
    assert!(view.go_json_merge(&mut decoder).is_err());
    let definer = view.definer.as_ref().unwrap();
    assert_eq!(definer.username, "root");
    assert_eq!(definer.hostname, "%");
    assert_eq!(view.select_stmt, "after");
    assert_eq!(view.cols[0].original(), "old");
    assert_eq!(view.cols[0].lowercase(), "merged");
    assert_eq!(view.cols[1].original(), "");

    let mut decoder = serde_json::Deserializer::from_str(
        r#"{"view_definer":null,"view_algorithm":7,"view_algorithm":null}"#,
    );
    view.go_json_merge(&mut decoder).unwrap();
    assert!(view.definer.is_none());
    assert_eq!(view.algorithm, ViewAlgorithm(7));
}

#[test]
fn session_info_string() {
    let s = SessionInfo {
        server_id: "s1".to_owned(),
        session_id: 42,
    };
    assert_eq!(s.to_string(), "server: s1_session: 42");
}

#[test]
fn data_structs_clone() {
    let ttl = TTLInfo {
        column_name: CiString::new("t"),
        enable: true,
        ..Default::default()
    };
    assert_eq!(ttl.clone(), ttl);

    let seq = SequenceInfo {
        start: 1,
        max_value: 100,
        ..Default::default()
    };
    assert_eq!(seq.clone().max_value, 100);

    let ep = ExchangePartitionInfo {
        exchange_partition_table_id: 5,
        ..Default::default()
    };
    assert_eq!(ep, ep.clone());

    let rfk = ReferredFKInfo {
        child_table: CiString::new("child"),
        ..Default::default()
    };
    assert_eq!(rfk.child_table.original(), "child");
}

// Every json tag from pkg/meta/model/table.go, in Go's field order. The
// expected bytes were captured from encoding/json on the same values.
//
// The assertions use the crate's Go-compatible formatter so HTML-sensitive
// strings and integral floats are pinned in addition to field order/tags.
#[test]
fn json_tags_match_go() {
    let view = ViewInfo {
        algorithm: ViewAlgorithm::MERGE,
        definer: Some(Box::new(UserIdentity {
            username: "root".to_owned(),
            hostname: "%".to_owned(),
            ..Default::default()
        })),
        security: ViewSecurity::INVOKER,
        select_stmt: "SELECT 1".to_owned(),
        check_option: ViewCheckOption::CASCADED,
        cols: vec![CiString::new("A")].into(),
    };
    assert_eq!(
        go_json(&view),
        r#"{"view_algorithm":1,"view_definer":{"Username":"root","Hostname":"%","CurrentUser":false,"AuthUsername":"","AuthHostname":"","AuthPlugin":""},"view_security":1,"view_select":"SELECT 1","view_checkoption":1,"view_cols":[{"O":"A","L":"a"}]}"#
    );

    let constraint = ConstraintInfo {
        id: 1,
        name: CiString::new("c1"),
        table: CiString::new("t"),
        constraint_cols: vec![CiString::new("a")].into(),
        enforced: true,
        in_column: false,
        expr_string: "a < 1 && b > 0".to_owned(),
        state: SchemaState::PUBLIC,
    };
    assert_eq!(
        go_json(&constraint),
        r#"{"id":1,"constraint_name":{"O":"c1","L":"c1"},"tbl_name":{"O":"t","L":"t"},"constraint_cols":[{"O":"a","L":"a"}],"enforced":true,"in_column":false,"expr_string":"a \u003c 1 \u0026\u0026 b \u003e 0","state":5}"#
    );

    let sequence = SequenceInfo {
        start: 1,
        cache: true,
        cycle: false,
        min_value: 1,
        max_value: 10,
        increment: 1,
        cache_value: 1000,
        comment: "c".to_owned(),
    };
    assert_eq!(
        go_json(&sequence),
        r#"{"sequence_start":1,"sequence_cache":true,"sequence_cycle":false,"sequence_min_value":1,"sequence_max_value":10,"sequence_increment":1,"sequence_cache_value":1000,"sequence_comment":"c"}"#
    );

    let ttl = TTLInfo {
        column_name: CiString::new("t"),
        interval_expr_str: "1".to_owned(),
        interval_time_unit: 4,
        enable: true,
        job_interval: "1h".to_owned(),
    };
    assert_eq!(
        go_json(&ttl),
        r#"{"column":{"O":"t","L":"t"},"interval_expr":"1","interval_time_unit":4,"enable":true,"job_interval":"1h"}"#
    );

    let lock = TableLockInfo {
        tp: TableLockType::WRITE,
        sessions: vec![SessionInfo {
            server_id: "s".to_owned(),
            session_id: 7,
        }]
        .into(),
        state: TableLockState::PUBLIC,
        ts: 42,
    };
    assert_eq!(
        go_json(&lock),
        r#"{"Tp":4,"Sessions":[{"ServerID":"s","SessionID":7}],"State":2,"TS":42}"#
    );

    let replica = TiFlashReplicaInfo {
        count: 2,
        location_labels: vec!["z1".to_owned()].into(),
        available: true,
        available_partition_ids: vec![1].into(),
    };
    assert_eq!(
        go_json(&replica),
        r#"{"Count":2,"LocationLabels":["z1"],"Available":true,"AvailablePartitionIDs":[1]}"#
    );

    let exchange = ExchangePartitionInfo {
        exchange_partition_table_id: 3,
        exchange_partition_def_id: 4,
        xxx_exchange_partition_flag: true,
    };
    assert_eq!(
        go_json(&exchange),
        r#"{"exchange_partition_id":3,"exchange_partition_def_id":4,"exchange_partition_flag":true}"#
    );
}

// Go's `omitempty` drops every zero-valued SoftdeleteInfo field.
#[test]
fn softdelete_omitempty() {
    assert_eq!(go_json(&SoftdeleteInfo::default()), "{}");
    assert_eq!(
        go_json(&SoftdeleteInfo {
            retention: "1d".to_owned(),
            job_enable: true,
            job_interval: String::new(),
        }),
        r#"{"retention":"1d","job_enable":true}"#
    );
}

// Go distinguishes nil from allocated-empty slices and clears a slice to
// nil when the field is explicitly null.
#[test]
fn null_slices_decode_to_nil() {
    let decoded: FKInfo =
        serde_json::from_str(r#"{"id":1,"ref_cols":null,"cols":null,"fk_name":{"O":"f","L":"f"}}"#)
            .unwrap();
    assert_eq!(decoded.id, 1);
    assert!(decoded.ref_cols.is_empty());
    assert!(decoded.cols.is_empty());
    assert!(!decoded.ref_cols.is_allocated());
    assert!(!decoded.cols.is_allocated());
    assert_eq!(decoded.state, SchemaState::NONE);
    assert_eq!(decoded.version, FK_VERSION0);

    let decoded: TiFlashReplicaInfo =
        serde_json::from_str(r#"{"Count":1,"LocationLabels":null,"AvailablePartitionIDs":null}"#)
            .unwrap();
    assert_eq!(decoded.count, 1);
    assert!(decoded.location_labels.is_empty());
    assert!(!decoded.location_labels.is_allocated());
    assert!(!decoded.available_partition_ids.is_allocated());
}

#[test]
fn cistr_fields_use_persisted_go_object_semantics() {
    use crate::serde_helpers::GoJsonMerge;

    let table_name: TableNameInfo =
        serde_json::from_str(r#"{"ID":5,"name":{"o":"Table","L":"table","O":null},"unknown":1}"#)
            .unwrap();
    assert_eq!(table_name.id, 5);
    assert_eq!(table_name.name.original(), "Table");
    assert_eq!(table_name.name.lowercase(), "table");
    // `ast.CIStr.UnmarshalJSON` accepts its historical single-string form
    // and derives the lowercase spelling from it.
    let shorthand: TableNameInfo = serde_json::from_str(r#"{"name":"Table"}"#).unwrap();
    assert_eq!(shorthand.name.original(), "Table");
    assert_eq!(shorthand.name.lowercase(), "table");

    let mut table_name = TableNameInfo {
        id: 1,
        name: serde_json::from_str(r#"{"O":"before","L":"before"}"#).unwrap(),
    };
    let mut decoder = serde_json::Deserializer::from_str(r#"{"name":{"O":7,"L":"after"},"id":9}"#);
    assert!(table_name.go_json_merge(&mut decoder).is_err());
    assert_eq!(table_name.name.original(), "before");
    assert_eq!(table_name.name.lowercase(), "after");
    assert_eq!(table_name.id, 9);

    let ttl: TTLInfo = serde_json::from_str(
        r#"{"COLUMN":{"O":"ts","L":"ts"},"column":null,"interval_expr":"1","job_interval":null}"#,
    )
    .unwrap();
    assert_eq!(ttl.column_name.original(), "ts");
    assert_eq!(ttl.interval_expr_str, "1");
    assert_eq!(ttl.job_interval, "");
}

#[test]
fn cistr_slices_preserve_nil_empty_zero_elements_and_partial_state() {
    use crate::serde_helpers::GoJsonMerge;

    let nil = FKInfo::default();
    let empty: FKInfo = serde_json::from_str(r#"{"ref_cols":[],"cols":[]}"#).unwrap();
    assert!(!nil.ref_cols.is_allocated());
    assert!(empty.ref_cols.is_allocated());
    assert!(empty.cols.is_allocated());
    assert!(go_json(&nil).contains(r#""ref_cols":null,"cols":null"#));
    assert!(go_json(&empty).contains(r#""ref_cols":[],"cols":[]"#));

    let old_ref: CiString = serde_json::from_str(r#"{"O":"old","L":"old"}"#).unwrap();
    let old_col: CiString = serde_json::from_str(r#"{"O":"col","L":"col"}"#).unwrap();
    let mut foreign_key = FKInfo {
        ref_cols: vec![old_ref].into(),
        cols: vec![old_col].into(),
        on_delete: 1,
        ..Default::default()
    };
    let mut decoder = serde_json::Deserializer::from_str(
        r#"{"ref_cols":[{"O":7,"L":"merged"},null,{"O":"new","L":"new"}],"on_delete":"bad","version":1}"#,
    );
    assert!(foreign_key.go_json_merge(&mut decoder).is_err());
    assert_eq!(foreign_key.ref_cols.len(), 1);
    assert_eq!(foreign_key.ref_cols.get(0).original(), "old");
    assert_eq!(foreign_key.ref_cols.get(0).lowercase(), "merged");
    assert_eq!(foreign_key.on_delete, 1);
    assert_eq!(foreign_key.version, 0);

    let mut foreign_key = FKInfo {
        name: serde_json::from_str(r#"{"O":"before","L":"before"}"#).unwrap(),
        ..Default::default()
    };
    let mut decoder =
        serde_json::Deserializer::from_str(r#"{"fk_name":{"L":"partial","O":7},"id":9}"#);
    assert!(foreign_key.go_json_merge(&mut decoder).is_err());
    assert_eq!(foreign_key.name.original(), "before");
    assert_eq!(foreign_key.name.lowercase(), "partial");
    assert_eq!(foreign_key.id, 0);

    let referred: ReferredFKInfo = serde_json::from_str(
        r#"{"cols":[null],"child_schema":{"O":"db","L":"db"},"child_table":{"O":"t","L":"t"},"child_fk_name":{"O":"fk","L":"fk"}}"#,
    )
    .unwrap();
    assert!(referred.cols.is_allocated());
    assert_eq!(referred.cols[0].original(), "");
}

#[test]
fn scalar_table_subobjects_follow_go_field_merge_rules() {
    use crate::serde_helpers::GoJsonMerge;

    let sequence: SequenceInfo = serde_json::from_str(
        r#"{"ſequence_start":7,"sequence_start":null,"SEQUENCE_COMMENT":"kept","sequence_comment":null,"unknown":1}"#,
    )
    .unwrap();
    assert_eq!(sequence.start, 7);
    assert_eq!(sequence.comment, "kept");

    let session: SessionInfo = serde_json::from_str(
        r#"{"ſerverID":"first","ServerID":null,"SESSIONID":9,"SessionID":null}"#,
    )
    .unwrap();
    assert_eq!(session.server_id, "first");
    assert_eq!(session.session_id, 9);

    let mut exchange = ExchangePartitionInfo {
        exchange_partition_table_id: 4,
        exchange_partition_def_id: 5,
        ..Default::default()
    };
    let mut decoder = serde_json::Deserializer::from_str(
        r#"{"exchange_partition_id":"bad","exchange_partition_def_id":8,"exchange_partition_flag":true}"#,
    );
    assert!(exchange.go_json_merge(&mut decoder).is_err());
    assert_eq!(exchange.exchange_partition_table_id, 4);
    assert_eq!(exchange.exchange_partition_def_id, 8);
    assert!(exchange.xxx_exchange_partition_flag);

    let affinity: TableAffinityInfo =
        serde_json::from_str(r#"{"LEVEL":"table","level":null}"#).unwrap();
    assert_eq!(affinity.level, "table");
}

#[test]
fn time_fields_stop_on_custom_unmarshal_errors() {
    use crate::serde_helpers::GoJsonMerge;

    let mut window = StatsWindowSettings::default();
    let mut decoder = serde_json::Deserializer::from_str(
        r#"{"repeat_interval":1,"window_start":"not-a-time","repeat_interval":2}"#,
    );
    assert!(window.go_json_merge(&mut decoder).is_err());
    // time.Time.UnmarshalJSON returns directly: the later duplicate is not
    // visited, unlike a recoverable scalar type error.
    assert_eq!(window.repeat_interval, 1);
    assert_eq!(window.window_start, go_zero_time());

    let decoded: StatsWindowSettings = serde_json::from_str(
        r#"{"WINDOW_START":"1970-01-01T00:00:00Z","window_start":null,"repeat_interval":3}"#,
    )
    .unwrap();
    assert_eq!(
        decoded.window_start,
        DateTime::parse_from_rfc3339("1970-01-01T00:00:00Z").unwrap()
    );
    assert_eq!(decoded.repeat_interval, 3);
}

#[test]
fn table_lock_slices_preserve_nil_empty_elements_and_receiver_reuse() {
    use crate::serde_helpers::GoJsonMerge;

    let nil: TableLockInfo = serde_json::from_str(r#"{"Sessions":null}"#).unwrap();
    let empty: TableLockInfo = serde_json::from_str(r#"{"Sessions":[]}"#).unwrap();
    assert!(!nil.sessions.is_allocated());
    assert!(empty.sessions.is_allocated());
    assert_eq!(
        go_json(&nil),
        r#"{"Tp":0,"Sessions":null,"State":0,"TS":0}"#
    );
    assert_eq!(
        go_json(&empty),
        r#"{"Tp":0,"Sessions":[],"State":0,"TS":0}"#
    );

    let mut lock = TableLockInfo {
        sessions: vec![
            SessionInfo {
                server_id: "kept".to_owned(),
                session_id: 1,
            },
            SessionInfo {
                server_id: "discarded".to_owned(),
                session_id: 2,
            },
        ]
        .into(),
        ts: 4,
        ..Default::default()
    };
    let mut decoder = serde_json::Deserializer::from_str(
        r#"{"Sessions":[{"SessionID":"bad"},{"ServerID":"later"},null],"TS":8}"#,
    );
    assert!(lock.go_json_merge(&mut decoder).is_err());
    assert_eq!(lock.sessions.len(), 3);
    assert_eq!(lock.sessions[0].server_id, "kept");
    assert_eq!(lock.sessions[0].session_id, 1);
    assert_eq!(lock.sessions[1].server_id, "later");
    assert_eq!(lock.sessions[1].session_id, 2);
    assert_eq!(lock.sessions[2], SessionInfo::default());
    assert_eq!(lock.ts, 8);
}

#[test]
fn tiflash_slices_preserve_go_allocation_and_partial_error_state() {
    use crate::serde_helpers::GoJsonMerge;

    let labels: TiFlashReplicaInfo =
        serde_json::from_str(r#"{"LocationLabels":["zone",null],"AvailablePartitionIDs":[]}"#)
            .unwrap();
    assert_eq!(labels.location_labels.len(), 2);
    assert_eq!(labels.location_labels[0], "zone");
    assert_eq!(labels.location_labels[1], "");
    assert!(labels.available_partition_ids.is_allocated());

    let mut replica = TiFlashReplicaInfo {
        count: 1,
        available_partition_ids: vec![10, 20].into(),
        ..Default::default()
    };
    let mut decoder = serde_json::Deserializer::from_str(
        r#"{"AvailablePartitionIDs":[11,"bad",null],"Count":3}"#,
    );
    assert!(replica.go_json_merge(&mut decoder).is_err());
    assert_eq!(replica.available_partition_ids.len(), 3);
    assert_eq!(replica.available_partition_ids[0], 11);
    assert_eq!(replica.available_partition_ids[1], 20);
    assert_eq!(replica.available_partition_ids[2], 0);
    assert_eq!(replica.count, 3);
}

// Go's embedded *StatsWindowSettings is flattened when set and skipped
// entirely when nil.
#[test]
fn stats_options_embedded_window() {
    let without = StatsOptions::new();
    let encoded = go_json(&without);
    assert_eq!(
        encoded,
        r#"{"auto_recalc":true,"column_choice":0,"column_list":[],"sample_num":0,"sample_rate":0,"buckets":0,"topn":0,"concurrency":0}"#
    );
    let back: StatsOptions = serde_json::from_str(&encoded).unwrap();
    assert!(back.stats_window_settings.is_none());
    assert_eq!(back, without);

    let with = StatsOptions {
        stats_window_settings: Some(Box::new(StatsWindowSettings {
            window_start: DateTime::parse_from_rfc3339("1970-01-01T00:00:00Z").unwrap(),
            window_end: DateTime::parse_from_rfc3339("1970-01-01T00:00:00Z").unwrap(),
            repeat_type: WindowRepeatType::DAY,
            repeat_interval: 2,
        })),
        column_choice: ColumnChoice::LIST,
        column_list: Some(vec![CiString::new("a")]),
        ..StatsOptions::new()
    };
    let encoded = go_json(&with);
    assert_eq!(
        encoded,
        r#"{"window_start":"1970-01-01T00:00:00Z","window_end":"1970-01-01T00:00:00Z","repeat_type":1,"repeat_interval":2,"auto_recalc":true,"column_choice":3,"column_list":[{"O":"a","L":"a"}],"sample_num":0,"sample_rate":0,"buckets":0,"topn":0,"concurrency":0}"#
    );
    let back: StatsOptions = serde_json::from_str(&encoded).unwrap();
    assert_eq!(back, with);

    let present_null: StatsOptions = serde_json::from_str(
        r#"{"window_start":null,"auto_recalc":false,"column_choice":0,"column_list":null,"sample_num":0,"sample_rate":0,"buckets":0,"topn":0,"concurrency":0}"#,
    )
    .unwrap();
    let window = present_null.stats_window_settings.as_ref().unwrap();
    assert_eq!(window.window_start, go_zero_time());
    assert_eq!(window.window_end, go_zero_time());
    assert_eq!(
        go_json(&present_null),
        r#"{"window_start":"0001-01-01T00:00:00Z","window_end":"0001-01-01T00:00:00Z","repeat_type":0,"repeat_interval":0,"auto_recalc":false,"column_choice":0,"column_list":null,"sample_num":0,"sample_rate":0,"buckets":0,"topn":0,"concurrency":0}"#
    );
}

#[test]
fn stats_options_uses_embedded_pointer_and_receiver_merge_rules() {
    use crate::serde_helpers::GoJsonMerge;

    let mut options = StatsOptions {
        auto_recalc: true,
        column_list: Some(vec![CiString::new("old")]),
        ..Default::default()
    };
    let mut decoder = serde_json::Deserializer::from_str(
        r#"{"WINDOW_START":null,"column_list":[{"L":"merged"},null],"auto_recalc":"bad","sample_num":4,"sample_num":null}"#,
    );
    assert!(options.go_json_merge(&mut decoder).is_err());
    assert!(options.stats_window_settings.is_some());
    let columns = options.column_list.as_ref().unwrap();
    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0].original(), "old");
    assert_eq!(columns[0].lowercase(), "merged");
    assert_eq!(columns[1].original(), "");
    assert!(options.auto_recalc);
    assert_eq!(options.sample_num, 4);

    let mut fatal = StatsOptions::default();
    let mut decoder =
        serde_json::Deserializer::from_str(r#"{"window_start":"bad-time","sample_num":9}"#);
    assert!(fatal.go_json_merge(&mut decoder).is_err());
    assert!(fatal.stats_window_settings.is_some());
    assert_eq!(fatal.sample_num, 0);

    let nil: StatsOptions = serde_json::from_str(r#"{"column_list":null}"#).unwrap();
    let empty: StatsOptions = serde_json::from_str(r#"{"column_list":[]}"#).unwrap();
    assert!(nil.column_list.is_none());
    assert_eq!(empty.column_list, Some(Vec::new()));
}

// Go's time.Time marshals as RFC 3339 with trailing fractional zeros cut.
#[test]
fn go_time_format() {
    let base = DateTime::parse_from_rfc3339("1970-01-01T00:00:00Z").unwrap();
    assert_eq!(format_go_time(&base), "1970-01-01T00:00:00Z");
    let fractional = base + chrono::Duration::nanoseconds(500_000_000);
    assert_eq!(format_go_time(&fractional), "1970-01-01T00:00:00.5Z");
    let nanos = base + chrono::Duration::nanoseconds(123_456_789);
    assert_eq!(format_go_time(&nanos), "1970-01-01T00:00:00.123456789Z");
    assert_eq!(format_go_time(&go_zero_time()), "0001-01-01T00:00:00Z");

    let offset = DateTime::parse_from_rfc3339("2026-08-08T12:00:00+05:30").unwrap();
    assert_eq!(format_go_time(&offset), "2026-08-08T12:00:00+05:30");
    let decoded: StatsWindowSettings = serde_json::from_str(
        r#"{"window_start":"2026-08-08T12:00:00+05:30","window_end":"0001-01-01T00:00:00Z","repeat_type":0,"repeat_interval":0}"#,
    )
    .unwrap();
    assert_eq!(decoded.window_start.offset().local_minus_utc(), 19_800);
    assert_eq!(
        go_json(&decoded),
        r#"{"window_start":"2026-08-08T12:00:00+05:30","window_end":"0001-01-01T00:00:00Z","repeat_type":0,"repeat_interval":0}"#
    );
}

#[test]
fn tiflash_partition_available() {
    let tr = TiFlashReplicaInfo {
        count: 1,
        available_partition_ids: vec![3, 7, 11].into(),
        ..Default::default()
    };
    assert!(tr.is_partition_available(7));
    assert!(!tr.is_partition_available(5));
}
