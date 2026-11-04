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

//! Go `session.go`'s telemetry: `updateTelemetryMetric`, `ExecuteStmt`'s
//! execution and TiFlash counts, and the builtin-function usage `Close`
//! hands over.

use std::sync::atomic::Ordering;
use std::sync::PoisonError;

use tidb_metrics::telemetry as m;
use tidb_sessionctx::telemetry_info::TelemetryInfo;

use crate::Session;

/// Go's `sessionctx.Context` as telemetry reads it.
impl tidb_telemetry::Session for Session {
    fn get_global_sys_var(&self, name: &str) -> Result<String, String> {
        self.vars.global_sysvars().get(name).map_err(|error| format!("{error:?}"))
    }

    fn cost_model_version(&self) -> i64 {
        self.vars
            .get_system(tidb_vardef::tidb_vars::TIDB_COST_MODEL_VERSION)
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0)
    }

    fn enable_paging(&self) -> bool {
        self.vars
            .get_system(tidb_vardef::tidb_vars::TIDB_ENABLE_PAGING)
            .is_ok_and(|value| tidb_vardef::option_values::tidb_opt_on(&value))
    }

    fn exec_restricted_sql(&self, sql: &str) -> Result<Vec<Vec<tidb_datatype::Datum>>, String> {
        tidb_executor::memtable_reader::exec_restricted_sql_over(self.restricted_sql_executor.as_deref(), sql)
            .map_err(|error| error.to_string())
    }

    fn refresh_txn_ctx(&mut self) -> Result<(), String> {
        // Go `RefreshTxnCtx`: commit, then `sessiontxn.NewTxn`.
        let mut conn = self.node_conn();
        conn.commit_txn().map_err(|error| error.message.clone())?;
        conn.new_txn(tidb_ast::TransactionMode::Default)
            .map_err(|error| error.message.clone())
    }

    fn info_schema(&self) -> std::sync::Arc<tidb_infoschema::SessionExtendedInfoSchema> {
        match &self.node {
            Some(node) => node.domain().catalog.get_latest(),
            None => std::sync::Arc::new(
                self.shared_catalog()
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .clone(),
            ),
        }
    }
}

impl Session {
    /// Go `Compile`'s `Ti: &TelemetryInfo{}`: a statement starts with none.
    pub(crate) fn reset_telemetry_info(&self) {
        *self.telemetry_info.lock().unwrap_or_else(PoisonError::into_inner) = TelemetryInfo::default();
    }

    /// The statement-level half of Go's executor build: `setTelemetryInfo`
    /// for a DDL and the account-lock count of CREATE/ALTER USER.
    pub(crate) fn set_statement_telemetry_info(&self, stmt: &tidb_ast::Stmt) {
        tidb_executor::builder_telemetry::set_telemetry_info(
            &mut self.telemetry_info.lock().unwrap_or_else(PoisonError::into_inner),
            stmt,
        );
    }

    /// Go `session.updateTelemetryMetric`, after the statement's `Exec`.
    pub(crate) fn update_telemetry_metric(&self) {
        self.statement_telemetry().update_telemetry_metric();
    }

    /// Go `ExecuteStmt`'s count of a statement that ran without error.
    pub(crate) fn count_executed_statement(&self) {
        self.statement_telemetry().count_executed_statement();
    }

    /// The running statement's telemetry, detached from the session for a
    /// caller that hands the session to the record set it opens.
    pub(crate) fn statement_telemetry(&self) -> StatementTelemetry {
        StatementTelemetry {
            info: std::sync::Arc::clone(&self.telemetry_info),
            internal: self.vars.restricted_sql,
            store_batch_size: self.store_batch_size(),
        }
    }

    /// Go `SessionVars.StoreBatchSize`.
    fn store_batch_size(&self) -> i64 {
        self.vars
            .get_system(tidb_vardef::tidb_vars::TIDB_STORE_BATCH_SIZE)
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0)
    }

    /// Go `ExecStmt`'s fair-locking accounting of a finished statement:
    /// `FinishExecuteStmt`'s statement counters, and -- for a statement that
    /// succeeded inside a transaction still open -- `Exec`'s `TxnCtx` flags.
    pub(crate) fn finish_statement_fair_locking(
        &mut self,
        observation: &tidb_store_driver_txn::cluster_table_storage::PessimisticLockObservation,
        succeeded: bool,
    ) {
        let (used, effective) = fair_locking_of(observation);
        if used {
            tidb_metrics::session::TRANSACTION_FAIR_LOCKING_USAGE
                .with_label_values(&["stmt-used"])
                .inc();
            if effective {
                tidb_metrics::session::TRANSACTION_FAIR_LOCKING_USAGE
                    .with_label_values(&["stmt-effective"])
                    .inc();
            }
        }
        if succeeded && used && self.in_transaction() {
            self.txn_fair_locking.0 = true;
            self.txn_fair_locking.1 |= effective;
        }
    }

    /// Go `FinishExecuteStmt`'s transaction counters once a commit ran,
    /// with the committing statement's own locks when it is an autocommit
    /// statement whose observation is still running.
    pub(crate) fn finish_txn_fair_locking(&mut self, autocommit_statement: bool) {
        let (mut used, mut effective) = std::mem::take(&mut self.txn_fair_locking);
        if autocommit_statement {
            let (statement_used, statement_effective) = fair_locking_of(
                &tidb_store_driver_txn::cluster_table_storage::peek_last_pessimistic_lock_observation(),
            );
            used |= statement_used;
            effective |= statement_used && statement_effective;
        }
        if used {
            tidb_metrics::session::TRANSACTION_FAIR_LOCKING_USAGE
                .with_label_values(&["txn-used"])
                .inc();
        }
        if effective {
            tidb_metrics::session::TRANSACTION_FAIR_LOCKING_USAGE
                .with_label_values(&["txn-effective"])
                .inc();
        }
    }

    /// A rolled-back transaction's flags end with it.
    pub(crate) fn reset_txn_fair_locking(&mut self) {
        self.txn_fair_locking = (false, false);
    }

    /// Go `executor.Compile`'s `CountStmtNode`: a compiled statement is
    /// counted under its label, an EXECUTE under its prepared statement's.
    pub(crate) fn count_stmt_node(&self, stmt: &tidb_ast::Stmt) {
        if let Some(count) = self.stmt_node_count(stmt) {
            count.record();
        }
    }

    /// See the `successful_compiles` field.
    pub(crate) fn successful_compiles(&self) -> u64 {
        self.successful_compiles
    }

    /// [`Self::count_stmt_node`]'s labels, taken before the statement is
    /// planned; [`tidb_executor::compiler::count_stmt_node::StmtNodeCount::record`]
    /// charges them once `Compile` succeeded.
    pub(crate) fn stmt_node_count(
        &self,
        stmt: &tidb_ast::Stmt,
    ) -> Option<tidb_executor::compiler::count_stmt_node::StmtNodeCount> {
        let counted = match stmt {
            tidb_ast::Stmt::Session(session) => match &**session {
                tidb_ast::SessionStmt::Execute { name, .. } => {
                    match self.prepared_statements.get(name) {
                        Some(prepared) => &prepared.statement,
                        // Go `GetPreparedStmt` fails `Compile`.
                        None => return None,
                    }
                }
                _ => stmt,
            },
            _ => stmt,
        };
        tidb_executor::compiler::count_stmt_node::stmt_node_count(
            counted,
            &self.vars.current_db,
            self.vars.restricted_sql,
            &self.statement_resource_group(stmt),
            &|schema, table| {
                tidb_telemetry::Session::info_schema(self)
                    .table_info_by_name(schema, table)
                    .is_some()
            },
        )
    }

    /// Go `session.BuiltinFunctionUsageInc`.
    pub fn builtin_function_usage_inc(&self, scalar_func_sig_name: &str) {
        self.builtin_function_usage
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .inc(scalar_func_sig_name);
    }

    /// The session's usage cell, which its planner counts into.
    pub(crate) fn builtin_function_usage_cell(
        &self,
    ) -> std::sync::Arc<std::sync::Mutex<tidb_telemetry::BuiltinFunctionsUsage>> {
        std::sync::Arc::clone(&self.builtin_function_usage)
    }

    /// Go `session.GetBuiltinFunctionUsage`: a copy of the counts.
    #[must_use]
    pub fn get_builtin_function_usage(&self) -> tidb_telemetry::BuiltinFunctionsUsage {
        self.builtin_function_usage
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Go `session.Close`'s `GlobalBuiltinFunctionsUsage.Collect`.
    pub(crate) fn collect_builtin_function_usage(&self) {
        tidb_telemetry::GLOBAL_BUILTIN_FUNCTIONS_USAGE.collect(&self.get_builtin_function_usage());
    }
}

/// One statement's telemetry cell with the session state
/// `updateTelemetryMetric` reads: Go's `s.isInternal()` and
/// `StoreBatchSize`.
pub(crate) struct StatementTelemetry {
    info: std::sync::Arc<std::sync::Mutex<TelemetryInfo>>,
    internal: bool,
    store_batch_size: i64,
}

impl StatementTelemetry {
    /// Go `session.updateTelemetryMetric`.
    pub(crate) fn update_telemetry_metric(&self) {
        if self.internal {
            return;
        }
        let ti = self.info.lock().unwrap_or_else(PoisonError::into_inner).clone();
        let cte = &m::TELEMETRY_SQL_CTE_CNT;
        if ti.use_recursive {
            cte.with_label_values(&["recurCTE"]).inc();
        } else if ti.use_non_recursive {
            cte.with_label_values(&["nonRecurCTE"]).inc();
        } else {
            cte.with_label_values(&["notCTE"]).inc();
        }
        if ti.use_index_merge {
            m::TELEMETRY_INDEX_MERGE_USAGE.inc();
        }
        if ti.use_multi_schema_change {
            m::TELEMETRY_MULTI_SCHEMA_CHANGE_CNT.inc();
        }
        if ti.use_flashback_to_cluster {
            m::TELEMETRY_FLASHBACK_CLUSTER_CNT.inc();
        }
        if ti.use_exchange_partition {
            m::TELEMETRY_EXCHANGE_PARTITION_CNT.inc();
        }
        if let Some(partition) = &ti.partition_telemetry {
            if partition.use_table_partition {
                m::TELEMETRY_TABLE_PARTITION_CNT.inc();
                #[expect(clippy::cast_precision_loss, reason = "Go float64(uint64)")]
                m::TELEMETRY_TABLE_PARTITION_MAX_PARTITIONS_CNT
                    .inc_by(partition.table_partition_max_partitions_num as f64);
            }
            for (used, counter) in [
                (partition.use_table_partition_list, &*m::TELEMETRY_TABLE_PARTITION_LIST_CNT),
                (partition.use_table_partition_range, &*m::TELEMETRY_TABLE_PARTITION_RANGE_CNT),
                (partition.use_table_partition_hash, &*m::TELEMETRY_TABLE_PARTITION_HASH_CNT),
                (
                    partition.use_table_partition_range_columns,
                    &*m::TELEMETRY_TABLE_PARTITION_RANGE_COLUMNS_CNT,
                ),
                (
                    partition.use_table_partition_range_columns_gt1,
                    &*m::TELEMETRY_TABLE_PARTITION_RANGE_COLUMNS_GT1_CNT,
                ),
                (
                    partition.use_table_partition_range_columns_gt2,
                    &*m::TELEMETRY_TABLE_PARTITION_RANGE_COLUMNS_GT2_CNT,
                ),
                (
                    partition.use_table_partition_range_columns_gt3,
                    &*m::TELEMETRY_TABLE_PARTITION_RANGE_COLUMNS_GT3_CNT,
                ),
                (
                    partition.use_table_partition_list_columns,
                    &*m::TELEMETRY_TABLE_PARTITION_LIST_COLUMNS_CNT,
                ),
                (
                    partition.use_create_interval_partition,
                    &*m::TELEMETRY_TABLE_PARTITION_CREATE_INTERVAL_PARTITIONS_CNT,
                ),
                (
                    partition.use_add_interval_partition,
                    &*m::TELEMETRY_TABLE_PARTITION_ADD_INTERVAL_PARTITIONS_CNT,
                ),
                (
                    partition.use_drop_interval_partition,
                    &*m::TELEMETRY_TABLE_PARTITION_DROP_INTERVAL_PARTITIONS_CNT,
                ),
                (partition.use_compact_table_partition, &*m::TELEMETRY_COMPACT_PARTITION_CNT),
                (partition.use_reorganize_partition, &*m::TELEMETRY_REORGANIZE_PARTITION_CNT),
            ] {
                if used {
                    counter.inc();
                }
            }
        }
        if let Some(account_lock) = &ti.account_lock_telemetry {
            let account = &m::TELEMETRY_ACCOUNT_LOCK_CNT;
            #[expect(clippy::cast_precision_loss, reason = "Go float64(int64)")]
            {
                account.with_label_values(&["lockUser"]).inc_by(account_lock.lock_user as f64);
                account.with_label_values(&["unlockUser"]).inc_by(account_lock.unlock_user as f64);
                account
                    .with_label_values(&["createOrAlterUser"])
                    .inc_by(account_lock.create_or_alter_user as f64);
            }
        }
        if ti.use_table_lookup && self.store_batch_size > 0 {
            m::TELEMETRY_STORE_BATCHED_QUERY_CNT.inc();
        }
    }

    /// Go `ExecuteStmt`'s count of a statement that ran without error.
    pub(crate) fn count_executed_statement(&self) {
        if self.internal || !tidb_config::config_tree::config::get_global_config().enable_telemetry {
            return;
        }
        tidb_telemetry::CURRENT_EXECUTE_COUNT.fetch_add(1, Ordering::Relaxed);
        let ti = self.info.lock().unwrap_or_else(PoisonError::into_inner);
        if ti.tiflash_push_down {
            tidb_telemetry::CURRENT_TIFLASH_PUSH_DOWN_COUNT.fetch_add(1, Ordering::Relaxed);
        }
        if ti.tiflash_exchange_push_down {
            tidb_telemetry::CURRENT_TIFLASH_EXCHANGE_PUSH_DOWN_COUNT.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Go `adapter.go`'s reading of `LockKeysDetail`: fair locking was used when
/// keys were locked or derived aggressively, and took effect when a key was
/// locked despite a conflict or derived from the previous retry.
fn fair_locking_of(
    observation: &tidb_store_driver_txn::cluster_table_storage::PessimisticLockObservation,
) -> (bool, bool) {
    let used = observation.aggressive_lock_new_count > 0 || observation.aggressive_lock_derived_count > 0;
    let effective = observation.locked_with_conflict_count > 0 || observation.aggressive_lock_derived_count > 0;
    (used, used && effective)
}
