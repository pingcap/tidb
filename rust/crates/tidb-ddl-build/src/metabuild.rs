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

//! Go `pkg/ddl/metabuild.go`: the `metabuild.Context` a DDL statement's
//! builders take, over the session context the statement runs in, and that
//! context as `pkg/ddl` reads it.

use tidb_sessionctx::StmtContext;

pub fn new_metabuild_context_with_sctx(ctx: &StmtContext) -> tidb_expr::metabuild::Context {
    use tidb_expr::metabuild::{
        with_clustered_index_def_mode, with_enable_auto_increment_in_generated, with_expr_ctx,
        with_info_schema, with_pre_split_regions, with_primary_key_required,
        with_shard_row_id_bits, Context, MetaOnlyInfoSchema,
    };
    Context::new([
        with_expr_ctx(std::sync::Arc::new(ctx.clone())),
        with_enable_auto_increment_in_generated(ctx.auto_increment_in_generated()),
        with_primary_key_required(ctx.primary_key_required()),
        with_clustered_index_def_mode(ctx.clustered_index_def_mode()),
        with_shard_row_id_bits(ctx.shard_row_id_bits()),
        with_pre_split_regions(ctx.pre_split_regions()),
        with_info_schema(
            ctx.latest_info_schema()
                .map(|schema| schema.meta_only() as std::sync::Arc<dyn MetaOnlyInfoSchema>),
        ),
    ])
}

/// Go's `sessionctx.Context` as `pkg/ddl` reads it: a statement's context.
impl crate::session_context::DdlSessionContext for StmtContext {
    fn metabuild_context(&self) -> tidb_expr::metabuild::Context {
        new_metabuild_context_with_sctx(self)
    }

    fn enable_check_constraint(&self) -> bool {
        StmtContext::enable_check_constraint(self)
    }

    fn foreign_key_checks(&self) -> bool {
        StmtContext::foreign_key_checks(self)
    }

    fn allow_remove_auto_inc(&self) -> bool {
        StmtContext::allow_remove_auto_inc(self)
    }

    fn auto_increment_in_generated(&self) -> bool {
        StmtContext::auto_increment_in_generated(self)
    }

    fn ddl_query(&self) -> &str {
        StmtContext::ddl_query(self)
    }

    fn ddl_sql_mode(&self) -> i64 {
        StmtContext::ddl_sql_mode(self)
    }

    fn sql_mode(&self) -> tidb_lexer::SqlMode {
        StmtContext::sql_mode(self)
    }

    fn ddl_query_rewrite(&self) -> Option<(String, String)> {
        StmtContext::ddl_query_rewrite(self)
    }

    fn rewrite_ddl_job_query(&self, original: String, replacement: String) {
        StmtContext::rewrite_ddl_job_query(self, original, replacement);
    }

    fn ddl_cdc_write_source(&self) -> u64 {
        StmtContext::ddl_cdc_write_source(self)
    }

    fn ddl_reorg_vars(&self) -> &tidb_variable::session_vars::DdlReorgVars {
        StmtContext::ddl_reorg_vars(self)
    }

    fn ddl_trace_id(&self) -> &[u8] {
        StmtContext::ddl_trace_id(self)
    }

    fn ddl_session_alias(&self) -> &str {
        StmtContext::ddl_session_alias(self)
    }

    fn ddl_reorg_priority(&self) -> i64 {
        StmtContext::ddl_reorg_priority(self)
    }

    fn ddl_connection_id(&self) -> u64 {
        StmtContext::ddl_connection_id(self)
    }

    fn connection_charset_info(&self) -> (&str, &str) {
        StmtContext::connection_charset_info(self)
    }

    fn session_zone(&self) -> tidb_datatype::SessionTimeZone {
        StmtContext::session_zone(self)
    }

    fn like_default_escape(&self) -> u8 {
        StmtContext::like_default_escape(self)
    }

    fn default_collation_for_utf8mb4(&self) -> &str {
        StmtContext::default_collation_for_utf8mb4(self)
    }

    fn append_warning_parts(&self, code: u16, message: &str) {
        StmtContext::append_warning_parts(self, code, message);
    }

    fn append_error_parts(&self, code: u16, message: &str) {
        StmtContext::append_error_parts(self, code, message);
    }

    fn append_note_parts(&self, code: u16, message: &str) {
        StmtContext::append_note_parts(self, code, message);
    }

    fn set_hypo_tiflash_replica(&self, schema: &str, table: &str, add: bool) {
        StmtContext::set_hypo_tiflash_replica(self, schema, table, add);
    }

    fn add_hypo_index(
        &self,
        schema: &str,
        table: &str,
        index: tidb_model::index::IndexInfo,
    ) -> Result<(), tidb_error::mysql::MysqlError> {
        StmtContext::add_hypo_index(self, schema, table, index)
    }

    fn drop_hypo_index(
        &self,
        schema: &str,
        table: &str,
        index: &str,
        if_exists: bool,
    ) -> Result<(), tidb_error::mysql::MysqlError> {
        StmtContext::drop_hypo_index(self, schema, table, index, if_exists)
    }

    fn append_sql_warn(&self, warning: tidb_util::context::SqlWarn) {
        StmtContext::append_sql_warn(self, warning);
    }

    fn take_warnings(&self) -> Vec<tidb_util::context::SqlWarn> {
        StmtContext::take_warnings(self)
    }

    fn with_isolated_warnings(&self) -> Box<dyn crate::session_context::DdlSessionContext> {
        Box::new(StmtContext::with_isolated_warnings(self))
    }
}




