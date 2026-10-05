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

//! Behavioral boundaries for the current `pkg/meta/model` public surface.

use tidb_model::{DBInfo, DDLBDRType, TableInfo, TableMode};

#[test]
fn pkg_meta_model_bdr_boundary() {
    assert_eq!(
        tidb_model::bdr::DDLBDRType::SAFE_DDL.to_string(),
        "safe DDL"
    );
    assert_eq!(
        tidb_model::ACTION_BDR_MAP
            .read()
            .get(&tidb_model::ActionType::ACTION_CREATE_TABLE),
        Some(&DDLBDRType::SAFE_DDL)
    );
    assert_eq!(tidb_model::ts_convert_2_time(0).unix_millis(), 0);
    assert_eq!(
        tidb_model::ts_convert_2_time(u64::MAX).unix_millis(),
        (u64::MAX >> 18) as i64
    );
}

#[test]
fn pkg_meta_model_db_boundary() {
    let left = tidb_model::db::DBInfo {
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
    assert!(tidb_model::table_mode::TableMode::NORMAL.can_transition_to(TableMode::IMPORT));
    assert!(!TableMode::IMPORT.can_transition_to(TableMode::RESTORE));
    assert!(!TableMode::RESTORE.can_transition_to(TableMode::IMPORT));
    assert_eq!(TableMode(255).to_string(), "");
}

#[test]
fn pkg_meta_model_owned_clone_boundaries() {
    let original = DBInfo {
        deprecated_tables: vec![TableInfo {
            name: tidb_ast::CiString::new("before"),
            ..Default::default()
        }]
        .into(),
        ..Default::default()
    };
    let cloned = original.clone_like_go();
    original
        .deprecated_tables
        .get(0)
        .expect("non-null table")
        .write()
        .name = tidb_ast::CiString::new("after");

    let cloned_table = cloned.deprecated_tables.get(0).expect("non-null table");
    let clone_observation = if cloned_table.read().name.original() == "before" {
        "owned-deep-copy"
    } else {
        "shared-table-identity"
    };
    let map_observation = if DBInfo::default().table_name2id.is_none() {
        "one-empty-map-state"
    } else {
        "unexpected-nonempty-map"
    };
    assert_eq!(clone_observation, "owned-deep-copy");
    assert_eq!(map_observation, "one-empty-map-state");
}
