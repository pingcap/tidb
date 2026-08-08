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

//! `pkg/meta/model`: TiDB schema/table metadata types.
//!
//! The atomic Go-package claim is tracked by the generated receipt under
//! `tests/pkg_meta_model_lockdown/`. Individual modules and older file-level
//! receipts are not independent whole-package completion claims.

pub mod action_type;
pub mod bdr;
pub mod column;
pub mod db;
pub mod engine_attribute;
pub mod flags;
pub mod go_duration;
pub mod index;
pub mod job;
pub mod job_args;
#[cfg(test)]
mod job_args_go_inventory;
pub mod job_enums;
pub mod masking_policy;
#[cfg(test)]
mod masking_policy_go_inventory;
pub mod partition;
pub mod placement;
pub mod reorg;
pub mod resource_group;
#[cfg(test)]
mod resource_group_go_inventory;
pub mod schema_diff;
pub mod schema_state;
pub mod serde_helpers;
mod setting_builder;
pub mod table;
pub mod table_info;
pub mod table_mode;

pub use action_type::{ActionType, ACTION_MAP};
pub use bdr::{ts_convert_2_time, DDLBDRType, ACTION_BDR_MAP, BDR_ACTION_MAP};
pub use column::{
    gen_removing_obj_name, gen_unique_changing_column_name, ChangeStateInfo, ColumnInfo,
};
pub use db::{less_db_info, DBInfo};
pub use engine_attribute::{
    parse_engine_attribute_from_string, EngineAttribute, StorageClassDef, StorageClassSettings,
    StorageClassTransitRule,
};
pub use index::{
    field_type_to_inverted_index_info, find_index_by_columns,
    find_index_by_columns_for_foreign_key, find_index_info_by_id, gen_unique_changing_index_name,
    indexable_distance_metric_to_fn_name, indexable_fn_name_to_distance_metric,
    is_index_prefix_covered, is_index_prefix_covered_for_foreign_key, ColumnarIndexType,
    FullTextIndexInfo, IndexColumn, IndexInfo, InvertedIndexInfo, RegionSplitPolicy,
    VectorIndexInfo, VEC_COSINE_DISTANCE_FN, VEC_L2_DISTANCE_FN,
};
pub use job::{
    AddForeignKeyInfo, AdminCommandOperator, HistoryInfo, InvolvingSchemaInfo,
    InvolvingSchemaInfoMode, Job, JobMeta, JobPauseReason, JobResumeReason, JobW, MultiSchemaInfo,
    ResolvedTimeZone, SubJob, TimeZoneLocation, TraceInfo,
};
pub use job_args::{
    index_arg_columnar_index_type, rename_tables_args_from_v1, IndexOp, RenameTableArgs,
};
pub use job_enums::{
    get_job_ver_in_use, modify_type_to_string, set_job_ver_in_use, str_to_job_state, JobState,
    JobVersion,
};
pub use masking_policy::{
    clone_masking_policy_info, MaskingPolicyInfo, MaskingPolicyStatus, MaskingPolicyType,
};
pub use partition::{PartitionDefinition, PartitionInfo, PartitionState, UpdateIndexInfo};
pub use placement::{PlacementSettings, PolicyInfo, PolicyRefInfo};
pub use reorg::{
    set_ddl_reorg_process_defaults, BackfillMeta, BackfillState, DDLReorgMeta, ReorgStage,
    ReorgType,
};
pub use resource_group::{
    ResourceGroupBackgroundSettings, ResourceGroupInfo, ResourceGroupRunawayAction,
    ResourceGroupRunawaySettings, ResourceGroupRunawayWatch, ResourceGroupSettings,
    ResourceGroupShared,
};
pub use schema_diff::{AffectedOption, SchemaDiff};
pub use schema_state::SchemaState;
pub use table::{
    find_fk_info_by_name, get_idx_changing_field_type, new_table_affinity_info_with_level,
    SessionInfo, StatsLoadItem, TableAffinityInfo, TableCacheStatusType, TableItemID,
    TableLockState, TableNameInfo, TempTableType, TiFlashReplicaInfo, DEFAULT_TTL_JOB_INTERVAL,
    OLD_DEFAULT_TTL_JOB_INTERVAL,
};
pub use table_info::{TableInfo, TABLE_INFO_VERSION5};
pub use table_mode::{AlterTableModeTarget, TableMode};
