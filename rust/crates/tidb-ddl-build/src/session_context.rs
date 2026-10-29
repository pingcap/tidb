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

use tidb_variable::session_vars::DdlReorgVars;

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
    fn sql_mode(&self) -> tidb_lexer::SqlMode;
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
    ) -> Result<(), tidb_error::MysqlError>;
    /// Go `dropHypoIndexFromCtx`: 1091 for a missing index unless
    /// `if_exists`.
    fn drop_hypo_index(
        &self,
        schema: &str,
        table: &str,
        index: &str,
        if_exists: bool,
    ) -> Result<(), tidb_error::MysqlError>;
    /// The statement's warnings, taken.
    fn take_warnings(&self) -> Vec<tidb_context::SqlWarn>;

    /// Go `StmtCtx.AppendWarning` / `AppendNote` / `AppendError` of a
    /// warning kept with its error.
    fn append_sql_warn(&self, warning: tidb_context::SqlWarn);
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

    fn sql_mode(&self) -> tidb_lexer::SqlMode {
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
    ) -> Result<(), tidb_error::MysqlError> {
        (**self).add_hypo_index(schema, table, index)
    }

    fn drop_hypo_index(
        &self,
        schema: &str,
        table: &str,
        index: &str,
        if_exists: bool,
    ) -> Result<(), tidb_error::MysqlError> {
        (**self).drop_hypo_index(schema, table, index, if_exists)
    }

    fn append_sql_warn(&self, warning: tidb_context::SqlWarn) {
        (**self).append_sql_warn(warning);
    }

    fn take_warnings(&self) -> Vec<tidb_context::SqlWarn> {
        (**self).take_warnings()
    }

    fn with_isolated_warnings(&self) -> Box<dyn DdlSessionContext> {
        (**self).with_isolated_warnings()
    }
}
