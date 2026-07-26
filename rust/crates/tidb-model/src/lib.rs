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
//! PACKAGE IN PROGRESS: the Go `meta/model` package is large (~9k lines
//! across many interdependent files: `job.go`, `index.go`, `column.go`,
//! `table.go`, ...). This crate is being grown bottom-up from its
//! self-contained leaf types; only the modules declared below are ported so
//! far. It is seed evidence, not yet the complete package.

pub mod action_type;
pub mod bdr;
pub mod column;
pub mod engine_attribute;
pub mod flags;
pub mod go_duration;
pub mod index;
pub mod job_enums;
pub mod masking_policy;
pub mod partition;
pub mod placement;
pub mod reorg;
pub mod resource_group;
pub mod schema_state;
mod setting_builder;
pub mod table;
pub mod table_mode;

pub use action_type::{ActionType, ACTION_MAP};
pub use bdr::{ts_convert_2_time, DDLBDRType, ACTION_BDR_MAP, BDR_ACTION_MAP};
pub use column::{gen_removing_obj_name, ChangeStateInfo};
pub use index::{
    ColumnarIndexType, FullTextIndexInfo, IndexColumn, IndexInfo, InvertedIndexInfo,
    RegionSplitPolicy, VectorIndexInfo,
};
pub use job_enums::{
    get_job_ver_in_use, modify_type_to_string, set_job_ver_in_use, JobState, JobVersion,
};
pub use masking_policy::{MaskingPolicyInfo, MaskingPolicyStatus};
pub use partition::{PartitionDefinition, PartitionInfo, PartitionState, UpdateIndexInfo};
pub use placement::{PlacementSettings, PolicyInfo, PolicyRefInfo};
pub use reorg::{BackfillState, ReorgStage, ReorgType};
pub use resource_group::{
    ResourceGroupBackgroundSettings, ResourceGroupInfo, ResourceGroupRunawaySettings,
    ResourceGroupSettings,
};
pub use schema_state::SchemaState;
pub use table::{
    SessionInfo, TableCacheStatusType, TableLockState, TempTableType, TiFlashReplicaInfo,
};
pub use table_mode::{AlterTableModeTarget, TableMode};
