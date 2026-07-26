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

//! Self-contained pieces of `pkg/meta/model/job.go`: the admin-operator and
//! involving-schema enums, the pause/resume reasons, `InvolvingSchemaInfo`,
//! and `HistoryInfo`.
//!
//! DEFERRED (a larger tranche): the `Job` struct itself and `SubJob` /
//! `MultiSchemaInfo` -- Job embeds a `terror.Error`, a `sync.Mutex`,
//! `DDLReorgMeta` (needs vardef runtime), `tracing.TraceInfo`, and
//! version-dependent JSON args (`RawArgs`/`Encode`/`Decode`/`FillArgs`).

use crate::db::DBInfo;
use crate::table_info::TableInfo;

/// Go `AdminCommandOperator` (an `int`): who issued an admin DDL command.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AdminCommandOperator(pub i64);

impl AdminCommandOperator {
    /// Unknown issuer (Go `AdminCommandByNotKnown`, the zero value).
    pub const BY_NOT_KNOWN: AdminCommandOperator = AdminCommandOperator(0);
    /// Issued by an end user (Go `AdminCommandByEndUser`).
    pub const BY_END_USER: AdminCommandOperator = AdminCommandOperator(1);
    /// Issued by the system (Go `AdminCommandBySystem`).
    pub const BY_SYSTEM: AdminCommandOperator = AdminCommandOperator(2);
}

/// Go `InvolvingSchemaInfoMode` (an `int`): the lock mode for an involved
/// schema object.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct InvolvingSchemaInfoMode(pub i64);

impl InvolvingSchemaInfoMode {
    /// Exclusive involvement (Go `ExclusiveInvolving`, the zero value).
    pub const EXCLUSIVE: InvolvingSchemaInfoMode = InvolvingSchemaInfoMode(0);
    /// Shared involvement (Go `SharedInvolving`).
    pub const SHARED: InvolvingSchemaInfoMode = InvolvingSchemaInfoMode(1);
}

/// Go `InvolvingAll` (`"*"`): the wildcard for all databases/tables.
pub const INVOLVING_ALL: &str = "*";
/// Go `InvolvingNone` (`""`): no involvement.
pub const INVOLVING_NONE: &str = "";

/// Go `InvolvingSchemaInfo`: a schema object a DDL job involves (for locking).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InvolvingSchemaInfo {
    /// The database name.
    pub database: String,
    /// The table name.
    pub table: String,
    /// The placement policy name.
    pub policy: String,
    /// The resource group name.
    pub resource_group: String,
    /// The involvement mode.
    pub mode: InvolvingSchemaInfoMode,
}

/// Go `JobPauseReason`: why a DDL job was paused.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JobPauseReason {
    /// The reason type.
    pub type_: String,
    /// The reason message.
    pub message: String,
}

/// Go `JobResumeReason`: why a DDL job was resumed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JobResumeReason {
    /// The reason type.
    pub type_: String,
}

/// Go `HistoryInfo`: the schema snapshot recorded when a DDL job finishes.
#[derive(Clone, Debug, Default)]
pub struct HistoryInfo {
    /// The schema version after the job.
    pub schema_version: i64,
    /// The affected database, if any.
    pub db_info: Option<Box<DBInfo>>,
    /// The affected table, if any.
    pub table_info: Option<Box<TableInfo>>,
    /// The finish timestamp (a TSO).
    pub finished_ts: u64,
    /// Multiple affected tables (for multi-table jobs).
    pub multiple_table_infos: Vec<TableInfo>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_ast::CiString;

    #[test]
    fn enums_and_reasons() {
        assert_eq!(
            AdminCommandOperator::default(),
            AdminCommandOperator::BY_NOT_KNOWN
        );
        assert_ne!(
            AdminCommandOperator::BY_SYSTEM,
            AdminCommandOperator::BY_END_USER
        );
        assert_eq!(
            InvolvingSchemaInfoMode::default(),
            InvolvingSchemaInfoMode::EXCLUSIVE
        );
        assert_eq!(INVOLVING_ALL, "*");
        assert_eq!(INVOLVING_NONE, "");

        let inv = InvolvingSchemaInfo {
            database: INVOLVING_ALL.to_owned(),
            mode: InvolvingSchemaInfoMode::SHARED,
            ..Default::default()
        };
        assert_eq!(inv.clone(), inv);

        let r = JobPauseReason {
            type_: "kv".to_owned(),
            message: "disk full".to_owned(),
        };
        assert_eq!(r.message, "disk full");
    }

    #[test]
    fn history_info() {
        let h = HistoryInfo {
            schema_version: 42,
            table_info: Some(Box::new(TableInfo {
                name: CiString::new("t"),
                ..Default::default()
            })),
            multiple_table_infos: vec![TableInfo::default()],
            ..Default::default()
        };
        // Deep clone.
        let c = h.clone();
        assert_eq!(c.schema_version, 42);
        assert_eq!(c.table_info.unwrap().name.original(), "t");
        assert_eq!(c.multiple_table_infos.len(), 1);
    }
}
