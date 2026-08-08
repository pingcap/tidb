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

//! Compile anchors for the complete `pkg/meta/model` package receipt.

mod pkg_meta_model_observation_emitter;
mod pkg_meta_model_package_anchors;

use tidb_model::{
    BackfillMeta, ColumnInfo, DBInfo, DDLBDRType, DDLReorgMeta, EngineAttribute, HistoryInfo,
    IndexInfo, Job, JobState, JobW, MaskingPolicyInfo, PartitionInfo, PlacementSettings,
    PolicyInfo, RenameTableArgs, ResourceGroupInfo, TableInfo, TableMode,
};

#[test]
fn pkg_meta_model_bdr_boundary() {
    assert_eq!(DDLBDRType::SAFE_DDL.to_string(), "safe DDL");
    assert_eq!(
        tidb_model::ACTION_BDR_MAP.get(&tidb_model::ActionType::ACTION_CREATE_TABLE),
        Some(&DDLBDRType::SAFE_DDL)
    );
    assert_eq!(tidb_model::ts_convert_2_time(0).timestamp_millis(), 0);
    assert_eq!(
        tidb_model::ts_convert_2_time(u64::MAX).timestamp_millis(),
        (u64::MAX >> 18) as i64
    );
}

#[test]
fn pkg_meta_model_db_boundary() {
    let left = DBInfo {
        id: 7,
        name: tidb_ast::CiString::new("Alpha"),
        ..Default::default()
    };
    let right = DBInfo {
        name: tidb_ast::CiString::new("beta"),
        ..Default::default()
    };
    assert!(tidb_model::less_db_info(&left, &right).is_lt());
    let encoded = serde_json::to_value(&left).expect("DBInfo must encode");
    assert_eq!(encoded["id"], 7);
    assert_eq!(encoded["Deprecated"], serde_json::json!({}));
}

#[test]
fn pkg_meta_model_flags_boundary() {
    assert_eq!(tidb_model::flags::FLAG_IGNORE_TRUNCATE, 1);
    assert_eq!(tidb_model::flags::FLAG_TRUNCATE_AS_WARNING, 1 << 1);
    assert_eq!(tidb_model::flags::FLAG_IN_RESTRICTED_SQL, 1 << 11);
}

#[test]
fn pkg_meta_model_table_mode_boundary() {
    assert!(TableMode::NORMAL.can_transition_to(TableMode::IMPORT));
    assert!(!TableMode::IMPORT.can_transition_to(TableMode::RESTORE));
    assert!(!TableMode::RESTORE.can_transition_to(TableMode::IMPORT));
    assert_eq!(TableMode(255).to_string(), "");
}

#[test]
fn pkg_meta_model_probe_owned_clone_boundaries() {
    let mut original = DBInfo {
        deprecated_tables: vec![TableInfo {
            name: tidb_ast::CiString::new("before"),
            ..Default::default()
        }],
        ..Default::default()
    };
    let cloned = original.clone();
    original.deprecated_tables[0].name = tidb_ast::CiString::new("after");

    let clone_observation = if cloned.deprecated_tables[0].name.original() == "before" {
        "owned-deep-copy"
    } else {
        "shared-table-identity"
    };
    let map_observation = if DBInfo::default().table_name2id.is_empty() {
        "one-empty-map-state"
    } else {
        "unexpected-nonempty-map"
    };
    pkg_meta_model_observation_emitter::emit(
        "MODEL-DB-OWNERSHIP",
        "Rust ownership cannot preserve Go DBInfo shallow pointer aliases or nil map identity",
        &[
            (
                "copy-table-alias",
                "mutate-source-table-after-copy",
                clone_observation,
            ),
            (
                "table-name-map-allocation",
                "nil-map-versus-allocated-empty-map",
                map_observation,
            ),
        ],
    );
}

#[test]
fn pkg_meta_model_column_engine_anchor() {
    let _ = std::mem::size_of::<ColumnInfo>();
    let _ = std::mem::size_of::<EngineAttribute>();
}

#[test]
fn pkg_meta_model_index_anchor() {
    let _ = std::mem::size_of::<IndexInfo>();
    assert_eq!(
        tidb_model::indexable_fn_name_to_distance_metric(tidb_model::VEC_COSINE_DISTANCE_FN),
        Some(tidb_model::index::distance_metric::COSINE)
    );
}

#[test]
fn pkg_meta_model_placement_anchor() {
    let _ = std::mem::size_of::<PlacementSettings>();
    let _ = std::mem::size_of::<PolicyInfo>();
}

#[test]
fn pkg_meta_model_reorg_anchor() {
    let _ = std::mem::size_of::<DDLReorgMeta>();
    let _ = std::mem::size_of::<BackfillMeta>();
}

#[test]
fn pkg_meta_model_job_anchor() {
    let _ = std::mem::size_of::<Job>();
    let _ = std::mem::size_of::<JobState>();
    let _ = std::mem::size_of::<HistoryInfo>();
    let _ = std::mem::size_of::<JobW>();
}

#[test]
fn pkg_meta_model_table_partition_anchor() {
    let _ = std::mem::size_of::<TableInfo>();
    let _ = std::mem::size_of::<PartitionInfo>();
}

#[test]
fn pkg_meta_model_absorbed_locks_anchor() {
    let _ = std::mem::size_of::<RenameTableArgs>();
    let _ = std::mem::size_of::<MaskingPolicyInfo>();
    let _ = std::mem::size_of::<ResourceGroupInfo>();
}
