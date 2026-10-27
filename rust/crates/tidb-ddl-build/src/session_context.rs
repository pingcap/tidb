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

//! The session a DDL statement runs in, as Go's `pkg/ddl` sees it.

/// The session values Go reads when it builds a reorganizing job's
/// `ReorgMeta` (`NewDDLReorgMeta`, `initJobReorgMetaFromVariables`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DdlReorgVars {
    /// `tidb_ddl_reorg_worker_cnt`.
    pub worker_count: i64,
    /// `tidb_ddl_reorg_batch_size`.
    pub batch_size: i64,
    /// `tidb_ddl_reorg_max_write_speed`, in bytes per second.
    pub max_write_speed: i64,
    /// `tidb_max_dist_task_nodes` (-1 means automatic).
    pub max_dist_task_nodes: i64,
    /// `tidb_enable_dist_task`.
    pub enable_dist_task: bool,
    /// `tidb_ddl_enable_fast_reorg`.
    pub enable_fast_reorg: bool,
    /// `tidb_service_scope` (Go `dxfhandle.GetTargetScope`).
    pub target_scope: String,
    /// The statement's resource group.
    pub resource_group_name: String,
    /// `tidb_stats_update_during_ddl`, as the job's session variable.
    pub enable_ddl_analyze: String,
    /// `tidb_analyze_version`, as the job's session variable.
    pub analyze_version: String,
    /// `tidb_scatter_region` (Go `getScatterScopeFromSessionctx`), as the
    /// job's session variable.
    pub scatter_region: String,
    /// Go `ddlutil.GetTimeZone`: the session zone's name and offset.
    pub time_zone_name: String,
    /// See [`Self::time_zone_name`].
    pub time_zone_offset: i64,
}

impl Default for DdlReorgVars {
    fn default() -> Self {
        Self {
            worker_count: tidb_vardef::defaults::DEF_TIDB_DDL_REORG_WORKER_COUNT,
            batch_size: tidb_vardef::defaults::DEF_TIDB_DDL_REORG_BATCH_SIZE,
            max_write_speed: 0,
            max_dist_task_nodes: tidb_vardef::defaults::DEF_TIDB_MAX_DIST_TASK_NODES,
            enable_dist_task: true,
            enable_fast_reorg: tidb_vardef::defaults::DEF_TIDB_ENABLE_FAST_REORG,
            target_scope: String::new(),
            resource_group_name: String::new(),
            enable_ddl_analyze: "OFF".to_owned(),
            analyze_version: "2".to_owned(),
            scatter_region: tidb_vardef::defaults::DEF_TIDB_SCATTER_REGION.to_owned(),
            time_zone_name: "UTC".to_owned(),
            time_zone_offset: 0,
        }
    }
}

/// Go `sessionctx.Context` as `pkg/ddl`'s executor reads it while it checks a
/// statement and builds its job: the session variables the job records, the
/// statement's warnings, and the `metabuild.Context` its builders take
/// (`NewMetaBuildContextWithSctx`).
pub trait DdlSessionContext: Send + Sync {
    /// Go `NewMetaBuildContextWithSctx(sctx)`.
    fn metabuild_context(&self) -> tidb_expr::metabuild::Context;
    /// `tidb_enable_check_constraint`.
    fn enable_check_constraint(&self) -> bool;
    /// `foreign_key_checks`.
    fn foreign_key_checks(&self) -> bool;
    /// `tidb_allow_remove_auto_inc`.
    fn allow_remove_auto_inc(&self) -> bool;
    /// `tidb_enable_auto_increment_in_generated`.
    fn auto_increment_in_generated(&self) -> bool;
    /// Go `sessionctx.QueryString`: the statement text the job records.
    fn ddl_query(&self) -> &str;
    /// The complete `SessionVars.SQLMode` the job records.
    fn ddl_sql_mode(&self) -> i64;
    /// The lexer's view of the SQL mode (`ANSI_QUOTES` and friends).
    fn sql_mode(&self) -> tidb_parser::SqlMode;
    /// The `QueryString` rewrite a partition statement's sugar recorded.
    fn ddl_query_rewrite(&self) -> Option<(String, String)>;
    /// Go `ctx.SetValue(sessionctx.QueryString, ...)`.
    fn rewrite_ddl_job_query(&self, original: String, replacement: String);
    /// `SessionVars.CDCWriteSource`.
    fn ddl_cdc_write_source(&self) -> u64;
    /// The variables a reorganizing job's `ReorgMeta` records.
    fn ddl_reorg_vars(&self) -> &DdlReorgVars;
    /// The statement's trace id.
    fn ddl_trace_id(&self) -> &[u8];
    /// `tidb_session_alias`.
    fn ddl_session_alias(&self) -> &str;
    /// `tidb_ddl_reorg_priority`.
    fn ddl_reorg_priority(&self) -> i64;
    /// `SessionVars.ConnectionID`.
    fn ddl_connection_id(&self) -> u64;
    /// The connection's `(charset, collation)`.
    fn connection_charset_info(&self) -> (&str, &str);
    /// The session time zone.
    fn session_zone(&self) -> tidb_datatype::SessionTimeZone;
    /// The `LIKE` escape a pattern without `ESCAPE` uses.
    fn like_default_escape(&self) -> u8;
    /// `default_collation_for_utf8mb4`.
    fn default_collation_for_utf8mb4(&self) -> &str;
    /// Go `StmtCtx.AppendWarning`.
    fn append_warning_parts(&self, code: u16, message: &str);
    /// Go `StmtCtx.AppendError`.
    fn append_error_parts(&self, code: u16, message: &str);
    /// Go `StmtCtx.AppendNote`.
    fn append_note_parts(&self, code: u16, message: &str);
    /// Go `setHypoTiFlashReplica`: adds (`add`) or removes a hypothetical
    /// TiFlash replica in `SessionVars.HypoTiFlashReplicas`, by lowercase
    /// schema and table name.
    fn set_hypo_tiflash_replica(&self, schema: &str, table: &str, add: bool);
    /// Go `addHypoIndexIntoCtx`: `index` joins `SessionVars.HypoIndexes`
    /// under the lowercase schema and table name; a name already there is
    /// Go's "conflict hypo index name" error.
    fn add_hypo_index(
        &self,
        schema: &str,
        table: &str,
        index: tidb_model::index::IndexInfo,
    ) -> Result<(), tidb_error::mysql::MysqlError>;
    /// Go `dropHypoIndexFromCtx`: 1091 for a missing index unless
    /// `if_exists`.
    fn drop_hypo_index(
        &self,
        schema: &str,
        table: &str,
        index: &str,
        if_exists: bool,
    ) -> Result<(), tidb_error::mysql::MysqlError>;
    /// The statement's warnings, taken.
    fn take_warnings(&self) -> Vec<tidb_util::context::SqlWarn>;

    /// Go `StmtCtx.AppendWarning` / `AppendNote` / `AppendError` of a
    /// warning kept with its error.
    fn append_sql_warn(&self, warning: tidb_util::context::SqlWarn);
    /// The same session with a warning buffer of its own.
    fn with_isolated_warnings(&self) -> Box<dyn DdlSessionContext>;
}

/// A shared session is the session.
impl<T: DdlSessionContext + ?Sized> DdlSessionContext for std::sync::Arc<T> {
    fn metabuild_context(&self) -> tidb_expr::metabuild::Context {
        (**self).metabuild_context()
    }

    fn enable_check_constraint(&self) -> bool {
        (**self).enable_check_constraint()
    }

    fn foreign_key_checks(&self) -> bool {
        (**self).foreign_key_checks()
    }

    fn allow_remove_auto_inc(&self) -> bool {
        (**self).allow_remove_auto_inc()
    }

    fn auto_increment_in_generated(&self) -> bool {
        (**self).auto_increment_in_generated()
    }

    fn ddl_query(&self) -> &str {
        (**self).ddl_query()
    }

    fn ddl_sql_mode(&self) -> i64 {
        (**self).ddl_sql_mode()
    }

    fn sql_mode(&self) -> tidb_parser::SqlMode {
        (**self).sql_mode()
    }

    fn ddl_query_rewrite(&self) -> Option<(String, String)> {
        (**self).ddl_query_rewrite()
    }

    fn rewrite_ddl_job_query(&self, original: String, replacement: String) {
        (**self).rewrite_ddl_job_query(original, replacement)
    }

    fn ddl_cdc_write_source(&self) -> u64 {
        (**self).ddl_cdc_write_source()
    }

    fn ddl_reorg_vars(&self) -> &DdlReorgVars {
        (**self).ddl_reorg_vars()
    }

    fn ddl_trace_id(&self) -> &[u8] {
        (**self).ddl_trace_id()
    }

    fn ddl_session_alias(&self) -> &str {
        (**self).ddl_session_alias()
    }

    fn ddl_reorg_priority(&self) -> i64 {
        (**self).ddl_reorg_priority()
    }

    fn ddl_connection_id(&self) -> u64 {
        (**self).ddl_connection_id()
    }

    fn connection_charset_info(&self) -> (&str, &str) {
        (**self).connection_charset_info()
    }

    fn session_zone(&self) -> tidb_datatype::SessionTimeZone {
        (**self).session_zone()
    }

    fn like_default_escape(&self) -> u8 {
        (**self).like_default_escape()
    }

    fn default_collation_for_utf8mb4(&self) -> &str {
        (**self).default_collation_for_utf8mb4()
    }

    fn append_warning_parts(&self, code: u16, message: &str) {
        (**self).append_warning_parts(code, message)
    }

    fn append_error_parts(&self, code: u16, message: &str) {
        (**self).append_error_parts(code, message)
    }

    fn append_note_parts(&self, code: u16, message: &str) {
        (**self).append_note_parts(code, message)
    }

    fn set_hypo_tiflash_replica(&self, schema: &str, table: &str, add: bool) {
        (**self).set_hypo_tiflash_replica(schema, table, add)
    }

    fn add_hypo_index(
        &self,
        schema: &str,
        table: &str,
        index: tidb_model::index::IndexInfo,
    ) -> Result<(), tidb_error::mysql::MysqlError> {
        (**self).add_hypo_index(schema, table, index)
    }

    fn drop_hypo_index(
        &self,
        schema: &str,
        table: &str,
        index: &str,
        if_exists: bool,
    ) -> Result<(), tidb_error::mysql::MysqlError> {
        (**self).drop_hypo_index(schema, table, index, if_exists)
    }

    fn append_sql_warn(&self, warning: tidb_util::context::SqlWarn) {
        (**self).append_sql_warn(warning);
    }

    fn take_warnings(&self) -> Vec<tidb_util::context::SqlWarn> {
        (**self).take_warnings()
    }

    fn with_isolated_warnings(&self) -> Box<dyn DdlSessionContext> {
        (**self).with_isolated_warnings()
    }
}
