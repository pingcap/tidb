// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! `BuildTableInfoWithStmt` is the DDL package's (`tidb_ddl_build`); this
//! module re-exports it for the job path, which owns the statement context
//! a job without a session runs under.

pub use tidb_ddl_build::table_info_build::*;

/// TiDB's shipped DDL mode in UTC, for non-session live compatibility
/// callers.
///
/// Live statement paths must pass their own context through
/// [`build_table_info_with_context`]. Keeping this constructor explicit makes
/// the legacy live wrappers deterministic without leaking bootstrap's
/// `ModeNone` into user DDL.
pub fn default_ddl_statement_context() -> tidb_executor::StmtContext {
    tidb_executor::StmtContext::for_query()
        .with_strict(true)
        .with_date_modes(tidb_datatype::DateModes::TIDB_DEFAULT_SQL_MODE)
        .with_time_zone(tidb_datatype::SessionTimeZone::utc())
}
