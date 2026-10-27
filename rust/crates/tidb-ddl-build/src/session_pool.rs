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

//! Go `pkg/ddl/session`: the sessions the domain hands the DDL package, and
//! the plan values the work done through them carries.

/// Go `ReorgMeta.SQLMode` / `ReorgMeta.Location`: the session variables a
/// reorganization runs under, which its worker turns into its statement
/// context (`newReorgSessCtx`).
#[derive(Clone, Debug)]
pub struct ReorgSessionVars {
    /// The job's SQL mode.
    pub sql_mode: i64,
    /// The job's time zone (`ReorgMeta.Location.GetLocation`).
    pub time_zone: tidb_datatype::SessionTimeZone,
}

impl ReorgSessionVars {
    /// The job's reorganization variables.
    #[must_use]
    pub fn of_job(job: &tidb_model::Job) -> Self {
        let utc = tidb_datatype::SessionTimeZone::utc();
        let zone = job
            .reorg_meta
            .as_ref()
            .and_then(|meta| meta.read().location.clone())
            .map_or(utc.clone(), |location| {
                let location = location.read();
                let name = location.name.to_string();
                if location.offset == 0 {
                    match tidb_util::timeutil::load_location(&name) {
                        Ok(tidb_util::timeutil::TimeZone::Named(zone)) => {
                            tidb_datatype::SessionTimeZone::Named(zone)
                        }
                        Ok(tidb_util::timeutil::TimeZone::Local) => {
                            tidb_datatype::SessionTimeZone::Local
                        }
                        Ok(tidb_util::timeutil::TimeZone::Fixed { name, offset_secs }) => {
                            tidb_datatype::SessionTimeZone::Fixed { name, offset_secs }
                        }
                        Err(_) => utc,
                    }
                } else {
                    tidb_datatype::SessionTimeZone::Fixed {
                        name,
                        offset_secs: location.offset as i32,
                    }
                }
            });
        let sql_mode = job
            .reorg_meta
            .as_ref()
            .map_or(job.sql_mode as i64, |meta| meta.read().sql_mode as i64);
        Self {
            sql_mode,
            time_zone: zone,
        }
    }
}

/// Go `checkForeignKeyConstrain`, owed by an `ADD FOREIGN KEY` under
/// `foreign_key_checks = ON` before the key becomes public: the child's
/// existing rows against the parent, in the same transaction.
#[derive(Clone, Debug)]
pub struct ForeignKeyCheck {
    /// The child's database and its id.
    pub schema: String,
    pub schema_id: i64,
    /// The child as the change leaves it, carrying the new key.
    pub child: Box<tidb_model::TableInfo>,
    /// The parent's database, its id and the table, `None` for a self
    /// reference.
    pub parent: Option<(String, i64, Box<tidb_model::TableInfo>)>,
    /// The new key's name.
    pub name: String,
    /// The persisted collation mode captured when this DDL was planned.
    pub use_new_collation: bool,
    /// The job's reorganization variables the rows are read under.
    pub session_vars: ReorgSessionVars,
}

/// Go `pkg/ddl/session.Pool` as the DDL package uses it: the sessions the
/// domain hands DDL, for a job the domain submits itself and for the data
/// checks the owner runs as restricted SQL -- `checkModifyColumnData`
/// (`pkg/ddl/modify_column.go`), `checkForeignKeyConstrain`
/// (`pkg/ddl/foreign_key.go`) and `verifyRemainRecordsForCheckConstraint`
/// (`pkg/ddl/constraint.go`). The node's SQL layer implements it; the DDL
/// code only holds it.
pub trait DdlSessionPool: Send + Sync {
    /// A system session's DDL context, for a job no user session submits
    /// (Go's domain takes one from its system session pool).
    fn system_session(&self) -> Box<dyn crate::session_context::DdlSessionContext>;

    /// Go `checkModifyColumnData`: the first stored value of the column at
    /// `offset` that `new_type` refuses, as the error the change reports.
    fn modify_column_data(
        &self,
        table: &mut tidb_table::kv_table::KvTable,
        offset: usize,
        new_type: &tidb_datatype::FieldType,
        check_value_range: bool,
        ctx: &dyn tidb_table::context::MutateContext,
    ) -> Result<Option<tidb_table::kv_table::KvTableError>, tidb_table::kv_table::KvTableError>;

    /// Go `checkForeignKeyConstrain`: every child row under `plan`'s new key
    /// has its parent row.
    fn foreign_key(
        &self,
        plan: &ForeignKeyCheck,
        storage: &tidb_kv::cluster_storage::ClusterTableStorage,
    ) -> Result<(), tidb_error::mysql::MysqlError>;

    /// Go `verifyRemainRecordsForCheckConstraint`: every stored row of
    /// `table` satisfies its enforced constraints.
    fn check_constraint_rows(
        &self,
        table: &mut tidb_table::kv_table::KvTable,
    ) -> Result<(), tidb_table::kv_table::KvTableError>;

    /// Go `checkExchangePartitionRecordValidation`'s restricted queries:
    /// every `standalone` row belongs to the partition at `ordinal` of
    /// `partitioned` (and, with `check_constraints`, satisfies its
    /// constraints), and every row of partition `partition_id` satisfies the
    /// standalone table's.
    fn exchange_partition_rows(
        &self,
        standalone: &mut tidb_table::kv_table::KvTable,
        partitioned: &mut tidb_table::kv_table::KvTable,
        ordinal: usize,
        partition_id: i64,
        check_constraints: bool,
    ) -> Result<(), tidb_table::kv_table::KvTableError>;
}
