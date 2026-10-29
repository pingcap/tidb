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

//! The DDL session pool over the executor.
//!
//! Go's `pkg/ddl` takes its sessions from the pool the domain hands it: a
//! system session for a job the domain submits itself, and the sessions
//! `checkModifyColumnData`, `checkForeignKeyConstrain` and
//! `verifyRemainRecordsForCheckConstraint` run their restricted SQL in; the
//! DDL package never imports the executor. The worker here holds a
//! [`tidb_ddl_build::session_pool::DdlSessionPool`] the same way, and this is
//! the implementation the node hands it.

use tidb_ddl_build::session_pool::{DdlSessionPool, ForeignKeyCheck, ReorgSessionVars};
use tidb_table::table_from_meta::{cluster_table, AutoIdSource};
use tidb_error::MysqlError;
use tidb_ddl_build::partition_reorg::KvTablePartitionReorg;
use tidb_executor::{KvTableDdl, KvTableReader};
use tidb_sessionctx::StmtContext;
use tidb_kv::cluster_storage::ClusterTableStorage;
use tidb_table::context::MutateContext;
use tidb_table::kv_table::{KvTable, KvTableError, RowDecodeContext, TableCharset};

/// The executor's DDL session pool.
#[derive(Clone, Copy, Debug, Default)]
pub struct ExecutorDdlSessionPool;

impl DdlSessionPool for ExecutorDdlSessionPool {
    /// A fresh statement of a default session.
    fn system_session(&self) -> Box<dyn tidb_ddl_build::session_context::DdlSessionContext> {
        Box::new(StmtContext::for_query())
    }

    fn modify_column_data(
        &self,
        table: &mut KvTable,
        offset: usize,
        new_type: &tidb_datatype::FieldType,
        check_value_range: bool,
        ctx: &dyn MutateContext,
    ) -> Result<Option<KvTableError>, KvTableError> {
        table.check_modify_column_data(offset, new_type, check_value_range, ctx)
    }

    /// The child's existing rows against its parent, both read through the
    /// DDL transaction's snapshot and buffer, by the same check the
    /// in-process `ADD FOREIGN KEY` runs.
    fn foreign_key(
        &self,
        plan: &ForeignKeyCheck,
        storage: &ClusterTableStorage,
    ) -> Result<(), MysqlError> {
        let mut catalog = tidb_executor::Catalog::new();
        let child = cluster_table(&plan.child, storage, &AutoIdSource::Unavailable)
            .map_err(MysqlError::unknown)?
            .with_new_collation_mode(plan.use_new_collation);
        catalog.register_database_with_id_and_charset(
            &plan.schema,
            plan.schema_id,
            TableCharset::default(),
        );
        catalog
            .register_kv_in(&plan.schema, plan.child.name.original(), child)
            .map_err(|error| MysqlError::unknown(format!("{error:?}")))?;
        if let Some((schema, schema_id, parent)) = &plan.parent {
            let table = cluster_table(parent, storage, &AutoIdSource::Unavailable)
                .map_err(MysqlError::unknown)?
                .with_new_collation_mode(plan.use_new_collation);
            catalog.register_database_with_id_and_charset(
                schema,
                *schema_id,
                TableCharset::default(),
            );
            catalog
                .register_kv_in(schema, parent.name.original(), table)
                .map_err(|error| MysqlError::unknown(format!("{error:?}")))?;
        }
        tidb_executor::check_foreign_key_constrain(
            &mut catalog,
            &plan.schema,
            plan.child.name.original(),
            &plan.name,
            &reorg_statement_context(&plan.session_vars),
        )
        .map_err(tidb_executor::DriverError::to_mysql_error)
    }

    /// Every stored row under the DDL's default statement context.
    fn check_constraint_rows(&self, table: &mut KvTable) -> Result<(), KvTableError> {
        let context = tidb_ddl_build::table_info_build::default_ddl_statement_context();
        let rows = table.scan_rows_with_context(&RowDecodeContext::for_query(&context))?;
        for row in rows {
            table.validate_check_constraints(&row, &context)?;
        }
        Ok(())
    }

    /// Both directions read under a fresh statement of a default session,
    /// in the DDL transaction's snapshot.
    fn exchange_partition_rows(
        &self,
        standalone: &mut KvTable,
        partitioned: &mut KvTable,
        ordinal: usize,
        partition_id: i64,
        check_constraints: bool,
    ) -> Result<(), KvTableError> {
        let context = StmtContext::for_query();
        let rows = standalone.scan_rows_with_context(&RowDecodeContext::for_query(&context))?;
        for row in rows {
            // Go's literal condition (HASH's signed `mod(expr, num) != index`),
            // not the row's route.
            if partitioned.exchange_row_mismatches(ordinal, &row, &context)? {
                return Err(KvTableError::Mysql(
                    tidb_error::MysqlError::from_terror(
                        &tidb_error::catalog::table::ERR_ROW_DOES_NOT_MATCH_GIVEN_PARTITION_SET
                            .generate_with_stack_by_args(&[]),
                    ),
                ));
            }
            // Go applies the CHECK constraints only while
            // `vardef.EnableCheckConstraint` is on.
            if check_constraints {
                partitioned.validate_check_constraints(&row, &context)?;
            }
        }
        if !check_constraints {
            return Ok(());
        }

        // Go's second restricted query reads the target partition and applies
        // the standalone table's writable constraints. Both scans share the
        // same transaction snapshot so neither direction can race the swap.
        partitioned.restrict_read_to_partitions(&[partition_id]);
        let rows = partitioned.scan_rows_with_context(&RowDecodeContext::for_query(&context))?;
        for row in rows {
            standalone.validate_check_constraints(&row, &context)?;
        }
        Ok(())
    }
}

/// The session the check's SQL runs in: the job's SQL mode and time zone
/// over the DDL's default statement context, as a reorganization.
fn reorg_statement_context(vars: &ReorgSessionVars) -> StmtContext {
    tidb_ddl_build::table_info_build::default_ddl_statement_context()
        .with_ddl_sql_mode(vars.sql_mode)
        .with_lazy_clock(None, vars.time_zone.clone())
        .for_reorg()
}
