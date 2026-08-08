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

use std::mem::size_of;

use tidb_model::{
    BackfillMeta, ColumnInfo, DBInfo, DDLBDRType, DDLReorgMeta, EngineAttribute, HistoryInfo,
    IndexInfo, Job, JobState, JobW, MaskingPolicyInfo, PartitionInfo, PlacementSettings,
    PolicyInfo, RenameTableArgs, ResourceGroupInfo, TableInfo, TableMode,
};

#[test]
fn pkg_meta_model_bdr_db_flags_mode_anchor() {
    let _ = size_of::<DDLBDRType>();
    let _ = size_of::<DBInfo>();
    let _ = size_of::<TableMode>();
    assert_eq!(tidb_model::flags::FLAG_IGNORE_TRUNCATE, 1);
}

#[test]
fn pkg_meta_model_column_engine_anchor() {
    let _ = size_of::<ColumnInfo>();
    let _ = size_of::<EngineAttribute>();
}

#[test]
fn pkg_meta_model_index_anchor() {
    let _ = size_of::<IndexInfo>();
    assert_eq!(
        tidb_model::indexable_fn_name_to_distance_metric(tidb_model::VEC_COSINE_DISTANCE_FN),
        Some(tidb_model::index::distance_metric::COSINE)
    );
}

#[test]
fn pkg_meta_model_placement_anchor() {
    let _ = size_of::<PlacementSettings>();
    let _ = size_of::<PolicyInfo>();
}

#[test]
fn pkg_meta_model_reorg_anchor() {
    let _ = size_of::<DDLReorgMeta>();
    let _ = size_of::<BackfillMeta>();
}

#[test]
fn pkg_meta_model_job_anchor() {
    let _ = size_of::<Job>();
    let _ = size_of::<JobState>();
    let _ = size_of::<HistoryInfo>();
    let _ = size_of::<JobW>();
}

#[test]
fn pkg_meta_model_table_partition_anchor() {
    let _ = size_of::<TableInfo>();
    let _ = size_of::<PartitionInfo>();
}

#[test]
fn pkg_meta_model_absorbed_locks_anchor() {
    let _ = size_of::<RenameTableArgs>();
    let _ = size_of::<MaskingPolicyInfo>();
    let _ = size_of::<ResourceGroupInfo>();
}
