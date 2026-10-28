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


//! The session side of `tidb_executor::stmt_ctx_build`: the session state
//! Go's `ResetContextOfStmt` reads.

use crate::*;

impl tidb_executor::stmt_ctx_build::StmtCtxSession for Session {


    fn stats_handle(&self) -> std::sync::Arc<tidb_stats_handle::StatsHandle> {
        std::sync::Arc::clone(&self.stats_handle)
    }


    fn active_resource_group(&self) -> &str {
        &self.active_resource_group
    }


    fn domain_info_schema(&self) -> std::sync::Arc<std::sync::Mutex<Catalog>> {
        std::sync::Arc::clone(self.catalog.inner())
    }


    fn region_store(&self) -> std::sync::Arc<dyn tidb_kv::region_map::RegionStore> {
        Session::region_store(self)
    }


    fn pushdown_blacklists(&self) -> &tidb_executor::blacklist::PushdownBlacklists {
        &self.pushdown_blacklists
    }

    fn hypo_indexes(&self) -> std::sync::Arc<tidb_executor::ddl::HypoIndexes> {
        std::sync::Arc::clone(&self.hypo_indexes)
    }

    fn advisory_locks_ref(&self) -> &tidb_executor::advisory_lock_state::AdvisoryLockSession {
        &self.advisory_locks
    }

    fn advisory_locks_mut(&mut self) -> &mut tidb_executor::advisory_lock_state::AdvisoryLockSession {
        &mut self.advisory_locks
    }

    fn binary_prepared_execution_ref(&self) -> &bool {
        &self.binary_prepared_execution
    }

    fn binary_prepared_execution_mut(&mut self) -> &mut bool {
        &mut self.binary_prepared_execution
    }

    fn cost_env_cache_ref(&self) -> &std::cell::RefCell<Option<(u64, Arc<tidb_planner::find_best_task::coster::CostEnv>)>> {
        &self.cost_env_cache
    }

    fn cost_env_cache_mut(&mut self) -> &mut std::cell::RefCell<Option<(u64, Arc<tidb_planner::find_best_task::coster::CostEnv>)>> {
        &mut self.cost_env_cache
    }

    fn current_sql_digest_key_ref(&self) -> &String {
        &self.current_sql_digest_key
    }

    fn current_sql_digest_key_mut(&mut self) -> &mut String {
        &mut self.current_sql_digest_key
    }

    fn executor_first_run_breakpoint_ref(&self) -> &Arc<std::sync::atomic::AtomicBool> {
        &self.executor_first_run_breakpoint
    }

    fn executor_first_run_breakpoint_mut(&mut self) -> &mut Arc<std::sync::atomic::AtomicBool> {
        &mut self.executor_first_run_breakpoint
    }

    fn hypo_tiflash_replicas_ref(&self) -> &Arc<tidb_executor::ddl::HypoTiFlashReplicas> {
        &self.hypo_tiflash_replicas
    }

    fn hypo_tiflash_replicas_mut(&mut self) -> &mut Arc<tidb_executor::ddl::HypoTiFlashReplicas> {
        &mut self.hypo_tiflash_replicas
    }

    fn local_temporary_tables_ref(&self) -> &Vec<(String, String, tidb_executor::KvTable)> {
        &self.local_temporary_tables
    }

    fn local_temporary_tables_mut(&mut self) -> &mut Vec<(String, String, tidb_executor::KvTable)> {
        &mut self.local_temporary_tables
    }

    fn mpp_query_info_ref(&self) -> &Arc<tidb_executor::MppQueryInfo> {
        &self.mpp_query_info
    }

    fn mpp_query_info_mut(&mut self) -> &mut Arc<tidb_executor::MppQueryInfo> {
        &mut self.mpp_query_info
    }

    fn phase_durations_ref(&self) -> &tidb_variable::phase_durations::SessionPhaseDurations {
        &self.phase_durations
    }

    fn phase_durations_mut(&mut self) -> &mut tidb_variable::phase_durations::SessionPhaseDurations {
        &mut self.phase_durations
    }

    fn planned_apply_ref(&self) -> &Arc<std::sync::atomic::AtomicBool> {
        &self.planned_apply
    }

    fn planned_apply_mut(&mut self) -> &mut Arc<std::sync::atomic::AtomicBool> {
        &mut self.planned_apply
    }

    fn prepared_params_ref(&self) -> &Option<Arc<[Datum]>> {
        &self.prepared_params
    }

    fn prepared_params_mut(&mut self) -> &mut Option<Arc<[Datum]>> {
        &mut self.prepared_params
    }

    fn process_plan_info_ref(&self) -> &Arc<std::sync::Mutex<tidb_executor::ProcessPlanInfo>> {
        &self.process_plan_info
    }

    fn process_plan_info_mut(&mut self) -> &mut Arc<std::sync::Mutex<tidb_executor::ProcessPlanInfo>> {
        &mut self.process_plan_info
    }

    fn published_last_insert_id_ref(&self) -> &Arc<std::sync::Mutex<Option<u64>>> {
        &self.published_last_insert_id
    }

    fn published_last_insert_id_mut(&mut self) -> &mut Arc<std::sync::Mutex<Option<u64>>> {
        &mut self.published_last_insert_id
    }

    fn rand_ref(&self) -> &Arc<MysqlRng> {
        &self.rand
    }

    fn rand_mut(&mut self) -> &mut Arc<MysqlRng> {
        &mut self.rand
    }

    fn retry_auto_ids_ref(&self) -> &Arc<std::sync::Mutex<tidb_executor::RetryAutoIds>> {
        &self.retry_auto_ids
    }

    fn retry_auto_ids_mut(&mut self) -> &mut Arc<std::sync::Mutex<tidb_executor::RetryAutoIds>> {
        &mut self.retry_auto_ids
    }

    fn row_id_shards_ref(&self) -> &Arc<std::sync::Mutex<tidb_executor::RowIdShardGenerator>> {
        &self.row_id_shards
    }

    fn row_id_shards_mut(&mut self) -> &mut Arc<std::sync::Mutex<tidb_executor::RowIdShardGenerator>> {
        &mut self.row_id_shards
    }

    fn selected_lock_keys_ref(&self) -> &Option<tidb_executor::select_lock::SelectedLockKeys> {
        &self.selected_lock_keys
    }

    fn selected_lock_keys_mut(&mut self) -> &mut Option<tidb_executor::select_lock::SelectedLockKeys> {
        &mut self.selected_lock_keys
    }

    fn sequence_last_values_ref(&self) -> &Arc<std::sync::Mutex<HashMap<i64, i64>>> {
        &self.sequence_last_values
    }

    fn sequence_last_values_mut(&mut self) -> &mut Arc<std::sync::Mutex<HashMap<i64, i64>>> {
        &mut self.sequence_last_values
    }

    fn server_id_getter_ref(&self) -> &Arc<dyn Fn() -> u64 + Send + Sync> {
        &self.server_id_getter
    }

    fn server_id_getter_mut(&mut self) -> &mut Arc<dyn Fn() -> u64 + Send + Sync> {
        &mut self.server_id_getter
    }

    fn session_index_usage_collector_ref(&self) -> &Option<Arc<Mutex<tidb_stats_handle_usage_indexusage::SessionIndexUsageCollector>>> {
        &self.session_index_usage_collector
    }

    fn session_index_usage_collector_mut(&mut self) -> &mut Option<Arc<Mutex<tidb_stats_handle_usage_indexusage::SessionIndexUsageCollector>>> {
        &mut self.session_index_usage_collector
    }

    fn session_memory_ref(&self) -> &tidb_executor::SessionMemory {
        &self.session_memory
    }

    fn session_memory_mut(&mut self) -> &mut tidb_executor::SessionMemory {
        &mut self.session_memory
    }

    fn sql_cpu_usages_ref(&self) -> &Arc<tidb_util::ppcpuusage::SqlCpuUsages> {
        &self.sql_cpu_usages
    }

    fn sql_cpu_usages_mut(&mut self) -> &mut Arc<tidb_util::ppcpuusage::SqlCpuUsages> {
        &mut self.sql_cpu_usages
    }

    fn staged_writes_ref(&self) -> &std::sync::Arc<tidb_executor::kv_table::StagedWrites> {
        &self.staged_writes
    }

    fn staged_writes_mut(&mut self) -> &mut std::sync::Arc<tidb_executor::kv_table::StagedWrites> {
        &mut self.staged_writes
    }

    fn statement_affected_rows_ref(&self) -> &u64 {
        &self.statement_affected_rows
    }

    fn statement_affected_rows_mut(&mut self) -> &mut u64 {
        &mut self.statement_affected_rows
    }

    fn statement_in_select_ref(&self) -> &bool {
        &self.statement_in_select
    }

    fn statement_in_select_mut(&mut self) -> &mut bool {
        &mut self.statement_in_select
    }

    fn statement_keys_read_ref(&self) -> &Arc<std::sync::atomic::AtomicU64> {
        &self.statement_keys_read
    }

    fn statement_keys_read_mut(&mut self) -> &mut Arc<std::sync::atomic::AtomicU64> {
        &mut self.statement_keys_read
    }

    fn statement_kind_ref(&self) -> &StatementKind {
        &self.statement_kind
    }

    fn statement_kind_mut(&mut self) -> &mut StatementKind {
        &mut self.statement_kind
    }

    fn statement_result_authority_ref(&self) -> &std::cell::RefCell<Option<ResultMaterializationAuthority>> {
        &self.statement_result_authority
    }

    fn statement_result_authority_mut(&mut self) -> &mut std::cell::RefCell<Option<ResultMaterializationAuthority>> {
        &mut self.statement_result_authority
    }

    fn statement_var_cache_ref(&self) -> &std::cell::RefCell<Option<std::sync::Arc<crate::stmt_ctx::StatementVarSnapshot>>> {
        &self.statement_var_cache
    }

    fn statement_var_cache_mut(&mut self) -> &mut std::cell::RefCell<Option<std::sync::Arc<crate::stmt_ctx::StatementVarSnapshot>>> {
        &mut self.statement_var_cache
    }

    fn stats_collector_ref(&self) -> &Option<std::sync::Arc<tidb_stats_handle_usage::SessionStatsItem>> {
        &self.stats_collector
    }

    fn stats_collector_mut(&mut self) -> &mut Option<std::sync::Arc<tidb_stats_handle_usage::SessionStatsItem>> {
        &mut self.stats_collector
    }

    fn stmt_hints_ref(&self) -> &tidb_hint::StmtHints {
        &self.stmt_hints
    }

    fn stmt_hints_mut(&mut self) -> &mut tidb_hint::StmtHints {
        &mut self.stmt_hints
    }

    fn tidb_decode_key_cache_ref(&self) -> &std::sync::Mutex<Option<(u64, Arc<tidb_executor::TidbDecodeKeySnapshot>)>> {
        &self.tidb_decode_key_cache
    }

    fn tidb_decode_key_cache_mut(&mut self) -> &mut std::sync::Mutex<Option<(u64, Arc<tidb_executor::TidbDecodeKeySnapshot>)>> {
        &mut self.tidb_decode_key_cache
    }

    fn transaction_table_delta_ref(&self) -> &std::sync::Arc<tidb_stats_handle_usage::TableDeltaMap> {
        &self.transaction_table_delta
    }

    fn transaction_table_delta_mut(&mut self) -> &mut std::sync::Arc<tidb_stats_handle_usage::TableDeltaMap> {
        &mut self.transaction_table_delta
    }


    fn has_statement_snapshot(&self) -> bool {
        self.statement_snapshot.is_some()
    }

    fn statement_snapshot_ts(&self) -> Option<u64> {
        self.statement_snapshot.as_ref().map(|(ts, _)| *ts)
    }


    fn in_transaction(&self) -> bool {
        Session::in_transaction(self)
    }


    fn is_autocommit(&self) -> bool {
        Session::is_autocommit(self)
    }


    fn statement_is_pessimistic(&self) -> bool {
        Session::statement_is_pessimistic(self)
    }


    fn check_noop_functions(&mut self, query: &tidb_ast::QueryStmt) -> Result<(), DriverError> {
        Session::check_noop_functions(self, query)
    }


    fn with_catalog_mut<T>(
        &mut self,
        body: impl FnOnce(&mut tidb_executor::Catalog) -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
        Session::with_catalog_mut(self, body)
    }

    fn statement_txn_mode(&self) -> SessionTxnMode {
        Session::statement_txn_mode(self)
    }

    fn txn_is_stale_read(&self) -> bool {
        Session::txn_is_stale_read(self)
    }

    fn statement_phase_observer(&self) -> Option<Arc<dyn Fn(tidb_executor::StatementPhase) + Send + Sync>> {
        Session::statement_phase_observer(self)
    }

    fn statement_kv_exec_counter(&self) -> tidb_util::topsql_stmtstats::KvExecCounterHandle {
        Session::statement_kv_exec_counter(self)
    }

    fn statement_kv_context_cell(&self) -> tidb_txnkv::StatementKvContextCell {
        Session::statement_kv_context_cell(self)
    }

    fn statement_cop_response_sink(&self) -> Option<Arc<dyn tidb_executor::CopResponseSink>> {
        Session::statement_cop_response_sink(self)
    }

    fn effective_replica_read(&self, mode: tidb_executor::ReplicaReadType) -> tidb_executor::ReplicaReadType {
        Session::effective_replica_read(self, mode)
    }

    fn current_tso(&self) -> tidb_executor::CurrentTso {
        Session::current_tso(self)
    }

    fn with_txn_catalog<T>(&self, body: impl FnOnce(&Catalog) -> T, ) -> Result<T, DriverError> {
        Session::with_txn_catalog(self, body)
    }

    fn view_definer_privilege_check(&self) -> Option<tidb_executor::ViewDefinerPrivilegeCheck> {
        Session::view_definer_privilege_check(self)
    }

    fn sys_warning_count(&self) -> usize {
        Session::sys_warning_count(self)
    }

    fn sys_error_count(&self) -> usize {
        Session::sys_error_count(self)
    }

    fn sql_digest_text_retrieval(&self) -> tidb_executor::SqlDigestTextRetrieval {
        Session::sql_digest_text_retrieval(self)
    }

    fn session_privilege_check(&self) -> Option<tidb_executor::SessionPrivilegeCheck> {
        Session::session_privilege_check(self)
    }

    fn sequence_privilege_check(&self) -> Option<tidb_executor::SequencePrivilegeCheck> {
        Session::sequence_privilege_check(self)
    }

    fn pessimistic_constraint_check_in_place(&self) -> bool {
        Session::pessimistic_constraint_check_in_place(self)
    }

    fn mdl_related_table_recorder(&self) -> Option<tidb_executor::MdlRelatedTableRecorder> {
        Session::mdl_related_table_recorder(self)
    }

    fn has_process_privilege(&self) -> bool {
        Session::has_process_privilege(self)
    }

    fn fk_cascade_table_lock_check(&self) -> Option<tidb_executor::ForeignKeyCascadeLockCheck> {
        Session::fk_cascade_table_lock_check(self)
    }

    fn current_resource_group(&self) -> &str {
        Session::current_resource_group(self)
    }

    fn breakpoint_notify_func(&self) -> Option<Arc<dyn Fn(String) + Send + Sync + 'static>> {
        Session::breakpoint_notify_func(self)
    }

    fn observe_parse_duration(&self, seconds: f64, internal: bool) {
        crate::metrics::observe_parse_duration(seconds, internal);
    }
}
