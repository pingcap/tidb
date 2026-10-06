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

//! Dedicated semantic boundaries for `pkg/meta/model`.

use tidb_model::{
    BackfillMeta, BackfillState, ColumnInfo, DDLReorgMeta, EngineAttribute, GoAny, GoShared,
    GoSharedPointerSlice, GoSharedSlice, IndexInfo, Job, JobState, MaskingPolicyInfo,
    MultiSchemaInfo, PartitionDefinition, PartitionInfo, PlacementSettings, RenameTableArgs,
    SchemaDiff, StorageClassTransitRule, TableInfo,
};

#[test]
fn pkg_meta_model_column_boundary() {
    let column = ColumnInfo::default();
    assert_eq!(column.id, 0);
    assert_eq!(tidb_model::gen_removing_obj_name("c"), "_Tombstone$_c");
    assert!(tidb_model::column::is_removing_name("_Tombstone$_c"));
    assert_eq!(
        tidb_model::column::removing_origin_name("_Tombstone$_c"),
        "c"
    );
    let wide = ColumnInfo {
        offset: i64::MAX,
        change_state_info: Some(tidb_model::go_runtime::GoShared::new(
            tidb_model::ChangeStateInfo {
                dependency_column_offset: i64::MIN,
            },
        )),
        ..Default::default()
    };
    let encoded = serde_json::to_value(&wide).unwrap();
    assert_eq!(encoded["offset"], i64::MAX);
    assert_eq!(
        encoded["change_state_info"]["relative_col_offset"],
        i64::MIN
    );
}

#[test]
fn pkg_meta_model_engine_boundary() {
    let empty: EngineAttribute =
        tidb_model::parse_engine_attribute_from_string("").expect("empty is the zero value");
    assert!(empty.storage_class.is_none());
    assert!(tidb_model::parse_engine_attribute_from_string("not json").is_err());
    let transition = StorageClassTransitRule {
        after_days: u64::MAX,
        after_seconds: 86_399,
        ..Default::default()
    };
    assert_eq!(
        transition.total_seconds(),
        u64::MAX.wrapping_mul(86_400).wrapping_add(86_399)
    );
    let raw =
        tidb_model::parse_engine_attribute_from_string(r#"{"storage_class": {"n":1.00,"n":2}}"#)
            .unwrap()
            .storage_class
            .unwrap();
    assert_eq!(raw.get(), r#"{"n":1.00,"n":2}"#);
}

#[test]
fn pkg_meta_model_index_boundary() {
    let index = IndexInfo {
        id: 7,
        ..Default::default()
    };
    assert_eq!(index.id, 7);
    assert_eq!(
        tidb_model::indexable_fn_name_to_distance_metric(tidb_model::VEC_COSINE_DISTANCE_FN),
        Some(tidb_model::index::distance_metric::COSINE)
    );
    assert_eq!(
        tidb_model::indexable_distance_metric_to_fn_name(tidb_model::index::distance_metric::L2),
        Some(tidb_model::VEC_L2_DISTANCE_FN)
    );
    let wide = tidb_model::IndexColumn {
        offset: i64::MAX,
        length: i64::MIN,
        ..Default::default()
    };
    assert_eq!(serde_json::to_value(&wide).unwrap()["offset"], i64::MAX);
    assert_eq!(serde_json::to_value(&wide).unwrap()["length"], i64::MIN);
}

#[test]
fn pkg_meta_model_placement_boundary() {
    let settings = PlacementSettings {
        primary_region: "r1".to_owned(),
        voters: 3,
        ..Default::default()
    };
    assert_eq!(settings.to_string(), "PRIMARY_REGION=\"r1\" VOTERS=3");
}

#[test]
fn pkg_meta_model_reorg_boundary() {
    let metadata = DDLReorgMeta::default();
    assert!(metadata.warnings.is_none());
    assert_eq!(
        BackfillState::INAPPLICABLE.to_string(),
        "backfill state inapplicable"
    );
    assert_eq!(BackfillState(255).to_string(), "backfill state unknown");
    assert!(BackfillMeta::default().encode().is_ok());
}

#[test]
fn pkg_meta_model_action_boundary() {
    let action = tidb_model::ActionType::ACTION_CREATE_TABLE;
    assert_eq!(action.to_string(), "create table");
    assert_eq!(tidb_model::ActionType(255).to_string(), "none");
}

#[test]
fn pkg_meta_model_schema_diff_boundary() {
    let diff = SchemaDiff::default();
    let encoded = serde_json::to_value(&diff).expect("SchemaDiff must encode");
    assert_eq!(encoded["affected_options"], serde_json::Value::Null);
    assert!(encoded.get("sub_action_types").is_none());
}

#[test]
fn pkg_meta_model_job_boundary() {
    let mut job = Job::default();
    job.state = JobState::RUNNING;
    assert!(job.is_running());
    job.set_row_count(i64::MAX);
    assert_eq!(job.get_row_count(), i64::MAX);
    assert!(job.encode(false).is_ok());
}

#[test]
fn pkg_meta_model_job_args_boundary() {
    let rename = RenameTableArgs {
        old_schema_id: 1,
        new_schema_id: 2,
        table_id: 3,
        ..Default::default()
    };
    assert_eq!(rename.old_schema_id, 1);
    assert_eq!(
        tidb_model::index_arg_columnar_index_type(tidb_model::ColumnarIndexType::NA, true),
        tidb_model::ColumnarIndexType::VECTOR
    );
}

#[test]
fn pkg_meta_model_table_boundary() {
    let options = tidb_model::table::StatsOptions::default();
    let encoded = serde_json::to_value(&options).expect("StatsOptions must encode");
    assert_eq!(encoded["column_list"], serde_json::Value::Null);
    assert_eq!(tidb_model::DEFAULT_TTL_JOB_INTERVAL, "24h");
}

#[test]
fn pkg_meta_model_partition_boundary() {
    let partition = PartitionInfo {
        definitions: vec![PartitionDefinition {
            id: 7,
            name: tidb_ast::CiString::new("P0"),
            ..Default::default()
        }]
        .into(),
        ..Default::default()
    };
    assert_eq!(partition.get_partition_id_by_name("p0"), 7);
    assert_eq!(partition.get_partition_id_by_name("missing"), -1);
    assert_eq!(PartitionDefinition::default().memory_usage(), 48);
}

#[test]
fn pkg_meta_model_masking_boundary() {
    let policy = MaskingPolicyInfo::default();
    assert_eq!(policy.id, 0);
    assert_eq!(
        tidb_model::MaskingPolicyStatus::DISABLE.to_string(),
        "DISABLED"
    );
    assert!(tidb_model::clone_masking_policy_info(None).is_none());
}

#[test]
fn pkg_meta_model_resource_boundary() {
    let settings = tidb_model::resource_group::ResourceGroupSettings {
        ru_rate: 1,
        ..Default::default()
    };
    assert!(settings.to_string().starts_with("RU_PER_SEC=1"));
}

#[test]
fn pkg_meta_model_column_representation_boundaries() {
    let mut source = ColumnInfo::default();
    source.dependences.insert("a".to_owned());
    let clone = source.clone();
    source.dependences.insert("b".to_owned());
    let clone_mode = if clone.dependences.contains("b") {
        "shared-map-backing"
    } else {
        "owned-deep-map"
    };
    let empty_mode = if !ColumnInfo::default().dependences.is_allocated()
        && tidb_model::column::GoStringSet::allocated(std::iter::empty::<String>()).is_allocated()
    {
        "nil-and-allocated-empty"
    } else {
        "unexpected-nonempty-set"
    };
    let flag_width = if std::mem::size_of_val(&ColumnInfo::default().get_flag()) == 8 {
        "u64"
    } else {
        "non-u64"
    };
    let default_domain = if std::any::type_name::<tidb_model::GoAny>().contains("GoAny") {
        "open-go-interface-domain"
    } else {
        "unexpected-default-domain"
    };
    assert_eq!(clone_mode, "shared-map-backing");
    assert_eq!(empty_mode, "nil-and-allocated-empty");
    assert_eq!(flag_width, "u64");
    assert_eq!(default_domain, "open-go-interface-domain");
}

#[test]
fn pkg_meta_model_flag_width_integration_dependency() {
    assert_eq!(tidb_model::flags::FLAG_IGNORE_TRUNCATE, 1);
    assert_eq!(tidb_model::flags::FLAG_TRUNCATE_AS_WARNING, 1 << 1);
    assert_eq!(tidb_model::flags::FLAG_IN_RESTRICTED_SQL, 1 << 11);
    const HIGH: u64 = 1_u64 << 63;
    const LOW: u64 = tidb_datatype::FieldTypeFlags::UNSIGNED as u64;
    let mut column = ColumnInfo::default();

    column.set_flag(HIGH);
    assert_eq!(column.get_flag(), HIGH);
    column.add_flag(LOW);
    assert_eq!(column.get_flag(), HIGH | LOW);
    column.toggle_flag(HIGH | tidb_datatype::FieldTypeFlags::ZEROFILL as u64);
    assert_eq!(
        column.get_flag(),
        LOW | tidb_datatype::FieldTypeFlags::ZEROFILL as u64
    );
    column.add_flag(HIGH);
    column.del_flag(LOW);
    assert_eq!(
        column.get_flag(),
        HIGH | tidb_datatype::FieldTypeFlags::ZEROFILL as u64
    );
    column.and_flag(HIGH);
    assert_eq!(column.get_flag(), HIGH);

    let encoded = serde_json::to_value(&column).unwrap();
    assert_eq!(encoded["type"]["Flag"].as_u64(), Some(HIGH));
    let decoded: ColumnInfo = serde_json::from_str(&encoded.to_string()).unwrap();
    assert_eq!(decoded.get_flag(), HIGH);
}

#[test]
fn pkg_meta_model_raw_json_boundary() {
    let duplicate_input = r#"{"storage_class":{"a":1,"a":2}}"#;
    let duplicate = tidb_model::parse_engine_attribute_from_string(duplicate_input)
        .unwrap()
        .storage_class
        .unwrap();
    assert_eq!(duplicate.get(), r#"{"a":1,"a":2}"#);
    let whitespace_input = r#"{"storage_class": { "a" : 1 }}"#;
    let whitespace = tidb_model::parse_engine_attribute_from_string(whitespace_input)
        .unwrap()
        .storage_class
        .unwrap();
    assert_eq!(whitespace.get(), r#"{ "a" : 1 }"#);

    let outer_duplicate = tidb_model::parse_engine_attribute_from_string(
        r#"{"storage_class":{"earlier":1},"STORAGE_CLASS":{"later":2}}"#,
    )
    .unwrap()
    .storage_class
    .unwrap();
    assert_eq!(outer_duplicate.get(), r#"{"later":2}"#);

    let simple_fold = tidb_model::parse_engine_attribute_from_string(
        r#"{"\u017ftorage_cla\u017fs":{"folded":true}}"#,
    )
    .unwrap()
    .storage_class
    .unwrap();
    assert_eq!(simple_fold.get(), r#"{"folded":true}"#);

    assert!(tidb_model::parse_engine_attribute_from_string(r#"[1]"#).is_err());
    assert!(
        tidb_model::parse_engine_attribute_from_string(r#"{"storage_class":1,"later":}"#).is_err()
    );
}

#[test]
fn pkg_meta_model_vector_allocation_boundaries() {
    let index_from_null: IndexInfo = serde_json::from_str(r#"{"idx_cols":null}"#).unwrap();
    let index_from_empty: IndexInfo = serde_json::from_str(r#"{"idx_cols":[]}"#).unwrap();
    let index_mode = if serde_json::to_value(index_from_null).unwrap()
        == serde_json::to_value(index_from_empty).unwrap()
    {
        "null-and-empty-conflated"
    } else {
        "allocation-distinguished"
    };
    let table_from_null: TableInfo = serde_json::from_str(r#"{"cols":null}"#).unwrap();
    let table_from_empty: TableInfo = serde_json::from_str(r#"{"cols":[]}"#).unwrap();
    let table_mode = if serde_json::to_value(table_from_null).unwrap()
        == serde_json::to_value(table_from_empty).unwrap()
    {
        "null-and-empty-conflated"
    } else {
        "allocation-distinguished"
    };
    let clone_source = IndexInfo {
        columns: vec![tidb_model::IndexColumn {
            name: tidb_ast::CiString::new("before"),
            ..Default::default()
        }]
        .into(),
        ..Default::default()
    };
    let clone = clone_source.clone_like_go();
    clone_source
        .columns
        .get(0)
        .expect("source index column")
        .write()
        .name = tidb_ast::CiString::new("after");
    let clone_column = clone.columns.get(0).expect("cloned index column");
    let clone_mode = if clone_column.read().name.original() == "before" {
        "owned-deep-elements"
    } else {
        "shared-pointer-elements"
    };
    let equality_mode = if (IndexInfo {
        id: 1,
        ..Default::default()
    })
    .equals_id(&IndexInfo {
        id: 1,
        ..Default::default()
    }) {
        "typed-IndexInfo-only"
    } else {
        "unexpected-id-inequality"
    };
    let partition_state = if PartitionInfo::default().ddl_columns.is_empty() {
        "one-empty-ddl-columns-state"
    } else {
        "unexpected-nonempty-ddl-columns"
    };
    assert_eq!(index_mode, "allocation-distinguished");
    assert_eq!(table_mode, "allocation-distinguished");
    assert_eq!(clone_mode, "owned-deep-elements");
    assert_eq!(equality_mode, "typed-IndexInfo-only");
    assert_eq!(partition_state, "one-empty-ddl-columns-state");
}

#[test]
fn pkg_meta_model_placement_callback_surface() {
    let empty = PlacementSettings::default().to_string();
    let empty_mode = if empty.is_empty() {
        "empty-render"
    } else {
        "unexpected-nonempty-render"
    };
    let one = PlacementSettings {
        primary_region: "r1".to_owned(),
        ..Default::default()
    }
    .to_string();
    assert_eq!(empty_mode, "empty-render");
    assert_eq!(one, "PRIMARY_REGION=\"r1\"");
}

#[test]
fn pkg_meta_model_schema_diff_affected_options_boundary() {
    let nil_encoded = serde_json::to_value(SchemaDiff::default()).unwrap();
    let empty_encoded = serde_json::to_value(SchemaDiff {
        affected_options: GoSharedPointerSlice::from_nullable(Vec::new()),
        ..Default::default()
    })
    .unwrap();
    let nullable_encoded = serde_json::to_value(SchemaDiff {
        affected_options: GoSharedPointerSlice::from_handles(vec![None]),
        ..Default::default()
    })
    .unwrap();
    assert!(nil_encoded["affected_options"].is_null());
    assert_eq!(empty_encoded["affected_options"], serde_json::json!([]));
    assert_eq!(
        nullable_encoded["affected_options"],
        serde_json::json!([null])
    );
}

#[test]
fn pkg_meta_model_job_runtime_representation() {
    let multi = MultiSchemaInfo::default();
    assert!(!multi.add_columns.is_allocated());
    assert!(!multi.add_indexes.is_allocated());
    assert!(multi.add_columns.is_empty());
    assert!(multi.add_indexes.is_empty());
    assert!(GoAny::nil().is_nil());
    let job = GoShared::new(Job::default());
    let wrapper = tidb_model::JobW::new(Some(job.clone()), GoSharedSlice::from_vec(Vec::new()));
    assert!(wrapper.job.as_ref().unwrap().ptr_eq(&job));
    assert!(wrapper.bytes.is_allocated());
    assert!(wrapper.bytes.is_empty());
}

#[test]
fn pkg_meta_model_process_hooks() {
    assert!(!tidb_model::index::get_global_index_v1_supported());
    assert_eq!(tidb_model::get_job_ver_in_use(), tidb_model::JobVersion::V1);
    assert_eq!(
        tidb_model::table::TTLInfo::default()
            .get_job_interval()
            .unwrap(),
        3_600_000_000_000
    );

    for (source, expected) in [
        ("٢h", "strconv.ParseFloat: parsing \"٢\": invalid syntax"),
        ("1雪", "unknown unit é"),
    ] {
        let error = tidb_model::table::TTLInfo {
            job_interval: source.to_owned(),
            ..Default::default()
        }
        .get_job_interval()
        .unwrap_err();
        assert_eq!(error.to_string(), expected);
    }
}

#[test]
fn pkg_meta_model_reorg_identity() {
    let mut source = DDLReorgMeta::default();
    let warning_counts =
        tidb_model::go_runtime::GoShared::new(std::collections::BTreeMap::from([("w".into(), 1)]));
    source.warnings_count = Some(warning_counts.clone());
    source.set_max_write_speed(10);
    let clone = source.clone();
    warning_counts.write().insert("w".into(), 2);
    assert!(warning_counts.ptr_eq(clone.warnings_count.as_ref().unwrap()));
    assert_eq!(
        clone.warnings_count.as_ref().unwrap().read()[&tidb_datatype::GoString::from("w")],
        2
    );
    source.set_max_write_speed(20);
    assert_eq!(source.get_max_write_speed(), 20);
    assert_eq!(clone.get_max_write_speed(), 10);
}
