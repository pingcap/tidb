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

//! `pkg/util/filter/schema.go`: the system-schema predicate.

use tidb_metadef::is_mem_or_sys_db;

/// The DM heartbeat schema name (Go `DMHeartbeatSchema`).
pub const DM_HEARTBEAT_SCHEMA: &str = "dm_heartbeat";
/// The `INSPECTION_SCHEMA` database name (Go `InspectionSchemaName`).
pub const INSPECTION_SCHEMA_NAME: &str = "inspection_schema";

/// Go `IsSystemSchema`: whether `schema` (already lower-cased) is a system
/// schema. Callers pass a lower-cased name; that invariant is debug-checked.
#[must_use]
pub fn is_system_schema(schema: &str) -> bool {
    debug_assert_eq!(
        schema,
        schema.to_lowercase(),
        "IsSystemSchema expects a lower-cased schema name"
    );
    schema == DM_HEARTBEAT_SCHEMA || schema == INSPECTION_SCHEMA_NAME || is_mem_or_sys_db(schema)
}
