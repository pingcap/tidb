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

//! `pkg/meta/metadef`: TiDB's system database/table identifiers, the
//! memory/system-database predicates, and the system-table DDL.

pub mod bootstrap_tables;
pub mod db;
pub mod system;
pub mod system_tables_def;

pub use bootstrap_tables::{BootstrapTable, BOOTSTRAP_TABLES};
pub use db::{
    is_br_related_db, is_mem_db, is_mem_or_sys_db, is_system_db, is_system_related_db,
    CLUSTER_TABLE_INSTANCE_COLUMN_NAME, INFORMATION_SCHEMA_NAME, INFORMATION_SCHEMA_NAME_L,
    METRIC_SCHEMA_NAME, METRIC_SCHEMA_NAME_L, PERFORMANCE_SCHEMA_NAME, PERFORMANCE_SCHEMA_NAME_L,
};
pub use system::{
    is_reserved_id, MAX_USER_GLOBAL_ID, RESERVED_GLOBAL_ID_LOWER_BOUND,
    RESERVED_GLOBAL_ID_UPPER_BOUND,
};
