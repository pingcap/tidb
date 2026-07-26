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

//! `pkg/meta/metadef/db.go`: memory/system database names and predicates.
//!
//! Go stores the schema names as `ast.CIStr` values and reads their `.L`
//! (lower) form. This low-level crate keeps only the plain strings: the
//! canonical name and its lower-case form, since the predicates operate on
//! already-lower-cased input, and the `mysql.*` database names are reused
//! from [`tidb_mysql`].

use tidb_mysql::{SysDB, SystemDB, WorkloadSchema};

/// The canonical `INFORMATION_SCHEMA` database name.
pub const INFORMATION_SCHEMA_NAME: &str = "INFORMATION_SCHEMA";
/// The lower-case `information_schema` form (Go `InformationSchemaName.L`).
pub const INFORMATION_SCHEMA_NAME_L: &str = "information_schema";
/// The canonical `PERFORMANCE_SCHEMA` database name.
pub const PERFORMANCE_SCHEMA_NAME: &str = "PERFORMANCE_SCHEMA";
/// The lower-case `performance_schema` form (Go `PerformanceSchemaName.L`).
pub const PERFORMANCE_SCHEMA_NAME_L: &str = "performance_schema";
/// The canonical `METRICS_SCHEMA` database name.
pub const METRIC_SCHEMA_NAME: &str = "METRICS_SCHEMA";
/// The lower-case `metrics_schema` form (Go `MetricSchemaName.L`).
pub const METRIC_SCHEMA_NAME_L: &str = "metrics_schema";
/// The `INSTANCE` column name of the cluster tables.
pub const CLUSTER_TABLE_INSTANCE_COLUMN_NAME: &str = "INSTANCE";

/// Prefix of BR's temporary databases (Go `temporaryDBNamePrefix`).
const TEMPORARY_DB_NAME_PREFIX: &str = "__TiDB_BR_Temporary_";

/// Go `IsMemOrSysDB`: whether `db_lower_name` is a memory or system database.
#[must_use]
pub fn is_mem_or_sys_db(db_lower_name: &str) -> bool {
    is_mem_db(db_lower_name) || is_system_related_db(db_lower_name)
}

/// Go `IsMemDB`: whether `db_lower_name` is a memory database.
#[must_use]
pub fn is_mem_db(db_lower_name: &str) -> bool {
    matches!(
        db_lower_name,
        INFORMATION_SCHEMA_NAME_L | PERFORMANCE_SCHEMA_NAME_L | METRIC_SCHEMA_NAME_L
    )
}

/// Go `IsSystemRelatedDB`: whether `db_lower_name` is a system-related
/// database (the system db, `sys`, or the workload schema).
#[must_use]
pub fn is_system_related_db(db_lower_name: &str) -> bool {
    is_system_db(db_lower_name) || db_lower_name == SysDB || db_lower_name == WorkloadSchema
}

/// Go `IsSystemDB`: whether `db_lower_name` is the system database (`mysql`).
#[must_use]
pub fn is_system_db(db_lower_name: &str) -> bool {
    db_lower_name == SystemDB
}

/// Go `IsBRRelatedDB`: whether `db_origin_name` is a BR temporary database.
#[must_use]
pub fn is_br_related_db(db_origin_name: &str) -> bool {
    db_origin_name.starts_with(TEMPORARY_DB_NAME_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Go TestIsMemDB.
    #[test]
    fn is_mem_db_cases() {
        assert!(is_mem_db("information_schema"));
        assert!(is_mem_db("performance_schema"));
        assert!(is_mem_db("metrics_schema"));
        assert!(!is_mem_db("mysql"));
    }

    // Go TestIsSystemRelatedDB.
    #[test]
    fn is_system_related_db_cases() {
        assert!(is_system_related_db("mysql"));
        assert!(is_system_related_db("sys"));
        assert!(is_system_related_db("workload_schema"));
        // Upper-case is not the lower form the predicate expects.
        assert!(!is_system_related_db("INFORMATION_SCHEMA"));
    }

    // Go TestIsSystemDB.
    #[test]
    fn is_system_db_cases() {
        assert!(is_system_db("mysql"));
        assert!(!is_system_db("sys"));
    }
}
