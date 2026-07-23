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

//! Top-level SQL statement dispatch, preserving source-sensitive branch order.

use tidb_ast::{
    AdminStmt, BeginStmt, CompletionType, DdlStmt, DmlStmt, DropStatsStmt, Expr, LoadStatsStmt,
    PrepareSource, SessionStmt, Stmt, TransactionMode,
};
use tidb_lexer::TokenKind;

use crate::{decode_at_name, decode_string, prec, PResult, Parser};

impl Parser {
    // ---- statements ----

    pub(crate) fn is_query_start_at(&self, offset: usize) -> bool {
        self.is_kw_at(offset, "SELECT")
            || self.is_kw_at(offset, "WITH")
            || self.is_kw_at(offset, "TABLE")
            || self.is_kw_at(offset, "VALUES")
    }

    pub(crate) fn starts_parenthesized_query(&self) -> bool {
        let mut offset = 0;
        while self.peek_n(offset).kind == TokenKind::Op && self.peek_n(offset).text == "(" {
            offset += 1;
        }
        offset > 0 && self.is_query_start_at(offset)
    }

    pub(crate) fn parse_statement(&mut self) -> PResult<Stmt> {
        self.skip_semicolons();
        if self.is_kw("SLOW") {
            self.parse_slow_query_statement()
        } else if self.is_kw("PLAN") && self.is_kw_at(1, "REPLAYER") {
            self.parse_plan_replayer_dump_explain()
        } else if self.is_kw("TRACE") {
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(AdminStmt::Trace(
                Box::new(self.parse_trace()?),
            ))))
        } else if self.is_kw("BINLOG") {
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(AdminStmt::Binlog(
                Box::new(self.parse_binlog()?),
            ))))
        } else if self.is_kw("KILL") {
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(AdminStmt::Kill(
                Box::new(self.parse_kill()?),
            ))))
        } else if self.is_kw("RECOMMEND") && self.is_kw_at(1, "INDEX") {
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(
                AdminStmt::RecommendIndex(Box::new(self.parse_recommend_index()?)),
            )))
        } else if self.is_kw("SHUTDOWN") || self.is_kw("RESTART") || self.is_kw("HELP") {
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(
                AdminStmt::ServerControl(Box::new(self.parse_server_control()?)),
            )))
        } else if self.is_kw("CALIBRATE") && self.is_kw_at(1, "RESOURCE") {
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(
                AdminStmt::CalibrateResource(Box::new(self.parse_calibrate_resource()?)),
            )))
        } else if self.is_kw("CREATE") && self.is_kw_at(1, "STATISTICS") {
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(
                AdminStmt::CreateStatistics(Box::new(self.parse_create_statistics()?)),
            )))
        } else if self.is_kw("DROP") && self.is_kw_at(1, "STATISTICS") {
            self.bump();
            self.bump();
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(
                AdminStmt::DropStatistics(crate::table_name_token_text(self.bump())),
            )))
        } else if self.is_kw("SET") && self.is_kw_at(1, "CONFIG") {
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(AdminStmt::SetConfig(
                Box::new(self.parse_set_config()?),
            ))))
        } else if self.is_kw("CANCEL")
            && self.is_kw_at(1, "DISTRIBUTION")
            && self.is_kw_at(2, "JOB")
        {
            self.bump();
            self.bump();
            self.bump();
            let token = self.bump();
            let job_id = token
                .text
                .parse::<i64>()
                .map_err(|_| self.err_here("expected distribution job ID"))?;
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(
                AdminStmt::CancelDistributionJob(job_id),
            )))
        } else if self.is_kw("QUERY") && self.is_kw_at(1, "WATCH") {
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(
                self.parse_query_watch()?,
            )))
        } else if self.is_kw("EXPLAIN") {
            self.parse_explain()
        } else if self.is_kw("DESCRIBE") || self.is_kw("DESC") {
            self.bump();
            // Go gives all three leaders to `parseExplainStmt`.  A bare
            // table target is its ShowColumns/`DESC` normal form, while a
            // query, DML, or ALTER target remains an EXPLAIN wrapper even
            // when the source spelling was DESC or DESCRIBE.
            self.parse_explain_tail()
        } else if self.is_kw("SELECT")
            || self.is_kw("TABLE")
            || self.is_kw("VALUES")
            || self.starts_parenthesized_query()
        {
            Ok(Stmt::Query(tidb_ast::NodeBox::new(
                self.parse_select_or_setopr()?,
            )))
        } else if self.is_kw("CALL") {
            Ok(Stmt::Dml(tidb_ast::NodeBox::new(DmlStmt::Call(Box::new(
                self.parse_call()?,
            )))))
        } else if self.is_kw("BATCH") {
            Ok(Stmt::Dml(tidb_ast::NodeBox::new(DmlStmt::Batch(Box::new(
                self.parse_batch_dml()?,
            )))))
        } else if self.is_kw("DISTRIBUTE") && self.is_kw_at(1, "TABLE") {
            Ok(Stmt::Dml(tidb_ast::NodeBox::new(DmlStmt::DistributeTable(
                Box::new(self.parse_distribute_table()?),
            ))))
        } else if self.is_kw("LOAD") && self.is_kw_at(1, "STATS") {
            self.bump();
            self.bump();
            if self.peek().kind != TokenKind::Str {
                return Err(self.err_here("expected LOAD STATS path string"));
            }
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(AdminStmt::LoadStats(
                Box::new(LoadStatsStmt {
                    path: decode_string(&self.bump().text),
                }),
            ))))
        } else if self.is_kw("DROP") && self.is_kw_at(1, "STATS") {
            self.bump();
            self.bump();
            let mut tables = vec![self.parse_table_name()?];
            while self.is_op(",") {
                self.bump();
                tables.push(self.parse_table_name()?);
            }
            let global = if self.is_kw("GLOBAL") {
                self.bump();
                self.warn("'DROP STATS ... GLOBAL' is deprecated and will be removed in a future release. Please use DROP STATS ... instead");
                true
            } else {
                false
            };
            let mut partitions = Vec::new();
            if self.is_kw("PARTITION") {
                self.bump();
                partitions.push(self.parse_non_string_ident_like_name()?);
                while self.is_op(",") {
                    self.bump();
                    partitions.push(self.parse_non_string_ident_like_name()?);
                }
                self.warn("'DROP STATS ... PARTITION ...' is deprecated and will be removed in a future release.");
            }
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(AdminStmt::DropStats(
                Box::new(DropStatsStmt {
                    tables,
                    global,
                    partitions,
                }),
            ))))
        } else if self.is_kw("DO") {
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(AdminStmt::Do(
                self.parse_do_stmt()?,
            ))))
        } else if self.is_kw("BACKUP") || self.is_kw("RESTORE") {
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(AdminStmt::Brie(
                Box::new(self.parse_brie()?),
            ))))
        } else if (self.is_kw("PAUSE") || self.is_kw("RESUME") || self.is_kw("STOP"))
            && self.is_kw_at(1, "BACKUP")
        {
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(AdminStmt::Brie(
                Box::new(self.parse_brie_control()?),
            ))))
        } else if self.is_kw("PURGE") && self.is_kw_at(1, "BACKUP") {
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(AdminStmt::Brie(
                Box::new(self.parse_purge_backup_logs()?),
            ))))
        } else if self.is_kw("LOAD") && self.is_kw_at(1, "DATA") {
            Ok(Stmt::Dml(tidb_ast::NodeBox::new(DmlStmt::LoadData(
                Box::new(self.parse_load_data()?),
            ))))
        } else if self.is_kw("INSERT") || self.is_kw("REPLACE") {
            // `REPLACE` reuses `parse_insert` (same grammar; see its own
            // doc and `tidb_ast::InsertStmt::replace`).
            Ok(Stmt::Dml(tidb_ast::NodeBox::new(DmlStmt::Insert(
                Box::new(self.parse_insert()?),
            ))))
        } else if self.is_kw("UPDATE") {
            Ok(Stmt::Dml(tidb_ast::NodeBox::new(DmlStmt::Update(
                Box::new(self.parse_update()?),
            ))))
        } else if self.is_kw("DELETE") {
            Ok(Stmt::Dml(tidb_ast::NodeBox::new(DmlStmt::Delete(
                Box::new(self.parse_delete()?),
            ))))
        } else if self.is_kw("IMPORT") && self.is_kw_at(1, "INTO") {
            Ok(Stmt::Dml(tidb_ast::NodeBox::new(DmlStmt::ImportInto(
                Box::new(self.parse_import_into()?),
            ))))
        } else if self.is_kw("CANCEL") && self.is_kw_at(1, "IMPORT") && self.is_kw_at(2, "JOB") {
            self.bump();
            self.bump();
            self.bump();
            let token = self.bump();
            if token.kind != TokenKind::IntLit {
                return Err(self.err_here("expected import job ID"));
            }
            let job_id = token
                .text
                .parse::<i64>()
                .map_err(|_| self.err_here("expected import job ID"))?;
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(
                AdminStmt::CancelImportJob(job_id),
            )))
        } else if self.is_kw("CREATE") && self.is_kw_at(1, "PROCEDURE") {
            Ok(Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::CreateProcedure(
                Box::new(self.parse_create_procedure()?),
            ))))
        } else if self.is_kw("DROP") && self.is_kw_at(1, "PROCEDURE") {
            Ok(Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::DropProcedure(
                Box::new(self.parse_drop_procedure()?),
            ))))
        } else if self.is_kw("LOCK") && (self.is_kw_at(1, "TABLE") || self.is_kw_at(1, "TABLES")) {
            Ok(Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::LockTables(
                Box::new(self.parse_lock_tables()?),
            ))))
        } else if self.is_kw("UNLOCK") && (self.is_kw_at(1, "TABLE") || self.is_kw_at(1, "TABLES"))
        {
            self.parse_unlock_tables()?;
            Ok(Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::UnlockTables)))
        } else if self.is_kw("LOCK") && self.is_kw_at(1, "STATS") {
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(AdminStmt::LockStats(
                Box::new(self.parse_stats_lock(true)?),
            ))))
        } else if self.is_kw("UNLOCK") && self.is_kw_at(1, "STATS") {
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(AdminStmt::UnlockStats(
                Box::new(self.parse_stats_lock(false)?),
            ))))
        } else if self.is_kw("SPLIT") {
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(AdminStmt::SplitRegion(
                Box::new(self.parse_split_region()?),
            ))))
        } else if self.is_kw("FLUSH") {
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(AdminStmt::Flush(
                Box::new(self.parse_flush()?),
            ))))
        } else if self.is_kw("GRANT") {
            if self.is_kw_at(1, "PROXY") {
                Ok(Stmt::Admin(tidb_ast::NodeBox::new(AdminStmt::GrantProxy(
                    Box::new(self.parse_grant_proxy_stmt()?),
                ))))
            } else if self.starts_role_membership("TO") {
                Ok(Stmt::Admin(tidb_ast::NodeBox::new(AdminStmt::GrantRole(
                    Box::new(self.parse_grant_role_stmt()?),
                ))))
            } else {
                Ok(Stmt::Admin(tidb_ast::NodeBox::new(AdminStmt::Grant(
                    Box::new(self.parse_grant_privilege_stmt()?),
                ))))
            }
        } else if self.is_kw("REVOKE") {
            if self.starts_role_membership("FROM") {
                Ok(Stmt::Admin(tidb_ast::NodeBox::new(AdminStmt::RevokeRole(
                    Box::new(self.parse_revoke_role_stmt()?),
                ))))
            } else {
                Ok(Stmt::Admin(tidb_ast::NodeBox::new(AdminStmt::Revoke(
                    Box::new(self.parse_revoke_privilege_stmt()?),
                ))))
            }
        } else if self.is_kw("ANALYZE")
            && (self.is_kw_at(1, "INCREMENTAL")
                || ((self.is_kw_at(1, "NO_WRITE_TO_BINLOG") || self.is_kw_at(1, "LOCAL"))
                    && self.is_kw_at(2, "INCREMENTAL")))
        {
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(
                AdminStmt::AnalyzeIncremental(Box::new(self.parse_analyze_incremental()?)),
            )))
        } else if self.is_kw("ANALYZE") {
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(
                AdminStmt::AnalyzeTable(Box::new(self.parse_analyze_table()?)),
            )))
        } else if self.is_traffic_source_statement() {
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(
                self.parse_traffic_source_statement()?,
            )))
        } else if self.is_kw("ADMIN") && self.is_kw_at(1, "REPAIR") {
            Ok(Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::RepairTable(
                Box::new(self.parse_repair_table()?),
            ))))
        } else if self.is_kw("OPTIMIZE") {
            Ok(Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::OptimizeTable(
                Box::new(self.parse_optimize_table()?),
            ))))
        } else if self.is_kw("RECOVER") && self.is_kw_at(1, "TABLE") {
            Ok(Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::RecoverTable(
                Box::new(self.parse_recover_table()?),
            ))))
        } else if self.is_kw("FLASHBACK") {
            Ok(Stmt::Ddl(tidb_ast::NodeBox::new(
                self.parse_flashback_statement()?,
            )))
        } else if self.is_kw("ADMIN") {
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(
                self.parse_admin_statement()?,
            )))
        } else if self.is_resource_group_source_statement() {
            Ok(Stmt::Ddl(tidb_ast::NodeBox::new(
                self.parse_resource_group_source_statement()?,
            )))
        } else if self.is_create_masking_policy_source_statement() {
            Ok(Stmt::Ddl(tidb_ast::NodeBox::new(
                self.parse_create_masking_policy_source_statement()?,
            )))
        } else if self.is_placement_policy_source_statement() {
            Ok(Stmt::Ddl(tidb_ast::NodeBox::new(
                self.parse_placement_policy_source_statement()?,
            )))
        } else if self.is_user_ddl_statement() {
            Ok(Stmt::Ddl(tidb_ast::NodeBox::new(
                self.parse_user_ddl_statement()?,
            )))
        } else if self.is_kw("CREATE")
            && (self.is_kw_at(1, "DATABASE") || self.is_kw_at(1, "SCHEMA"))
        {
            let (if_not_exists, name, options) = self.parse_create_database()?;
            Ok(Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::CreateDatabase {
                if_not_exists,
                name,
                options,
            })))
        } else if self.is_kw("CREATE")
            && (self.is_kw_at(1, "INDEX")
                || (self.is_kw_at(1, "UNIQUE") && self.is_kw_at(2, "INDEX"))
                || (self.is_kw_at(1, "FULLTEXT") && self.is_kw_at(2, "INDEX"))
                || (self.is_kw_at(1, "SPATIAL") && self.is_kw_at(2, "INDEX"))
                || (self.is_kw_at(1, "VECTOR") && self.is_kw_at(2, "INDEX"))
                || (self.is_kw_at(1, "COLUMNAR") && self.is_kw_at(2, "INDEX")))
        {
            Ok(Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::CreateIndex(
                Box::new(self.parse_create_index()?),
            ))))
        } else if self.is_kw("CREATE")
            && (self.is_kw_at(1, "VIEW")
                || self.is_kw_at(1, "ALGORITHM")
                || self.is_kw_at(1, "DEFINER")
                || self.is_kw_at(1, "SQL")
                || (self.is_kw_at(1, "OR") && self.is_kw_at(2, "REPLACE"))
                || (self.is_kw_at(1, "ALGORITHM")
                    && self.is_op_at(2, "=")
                    && self.is_kw_at(4, "VIEW")))
        {
            Ok(Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::CreateView(
                Box::new(self.parse_create_view()?),
            ))))
        } else if self.is_kw("CREATE")
            && (self.is_kw_at(1, "TABLE")
                || (self.is_kw_at(1, "TEMPORARY") && self.is_kw_at(2, "TABLE"))
                || (self.is_kw_at(1, "GLOBAL")
                    && self.is_kw_at(2, "TEMPORARY")
                    && self.is_kw_at(3, "TABLE")))
        {
            Ok(Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::CreateTable(
                Box::new(self.parse_create_table()?),
            ))))
        } else if self.is_kw("CREATE")
            && (self.is_kw_at(1, "BINDING")
                || ((self.is_kw_at(1, "GLOBAL") || self.is_kw_at(1, "SESSION"))
                    && self.is_kw_at(2, "BINDING")))
        {
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(
                AdminStmt::CreateBinding(Box::new(self.parse_create_binding()?)),
            )))
        } else if self.is_kw("ALTER")
            && (self.is_kw_at(1, "DATABASE") || self.is_kw_at(1, "SCHEMA"))
        {
            let (name, options) = self.parse_alter_database()?;
            Ok(Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::AlterDatabase {
                name,
                options,
            })))
        } else if self.is_kw("ALTER")
            && (self.is_kw_at(1, "TABLE")
                || (self.is_kw_at(1, "IGNORE") && self.is_kw_at(2, "TABLE")))
        {
            self.parse_alter_table_statement()
        } else if self.is_kw("ALTER") && self.is_kw_at(1, "INSTANCE") {
            Ok(Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::AlterInstance(
                Box::new(self.parse_alter_instance()?),
            ))))
        } else if self.is_kw("ALTER") && self.is_kw_at(1, "RANGE") {
            Ok(Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::AlterRange(
                Box::new(self.parse_alter_range()?),
            ))))
        } else if self.is_kw("RENAME") && self.is_kw_at(1, "TABLE") {
            Ok(Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::RenameTable(
                Box::new(self.parse_rename_table()?),
            ))))
        } else if self.is_kw("DROP")
            && ((self.is_kw_at(1, "TABLE") || self.is_kw_at(1, "TABLES"))
                || (self.is_kw_at(1, "TEMPORARY")
                    && (self.is_kw_at(2, "TABLE") || self.is_kw_at(2, "TABLES")))
                || (self.is_kw_at(1, "GLOBAL")
                    && self.is_kw_at(2, "TEMPORARY")
                    && (self.is_kw_at(3, "TABLE") || self.is_kw_at(3, "TABLES"))))
        {
            Ok(Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::DropTable(
                Box::new(self.parse_drop_table()?),
            ))))
        } else if self.is_kw("DROP")
            && (self.is_kw_at(1, "INDEX")
                || (self.is_kw_at(1, "HYPO") && self.is_kw_at(2, "INDEX")))
        {
            Ok(Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::DropIndex(
                Box::new(self.parse_drop_index()?),
            ))))
        } else if self.is_kw("DROP")
            && (self.is_kw_at(1, "BINDING")
                || ((self.is_kw_at(1, "GLOBAL") || self.is_kw_at(1, "SESSION"))
                    && self.is_kw_at(2, "BINDING")))
        {
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(AdminStmt::DropBinding(
                Box::new(self.parse_drop_binding()?),
            ))))
        } else if self.is_kw("DROP") && self.is_kw_at(1, "VIEW") {
            self.bump(); // DROP
            self.bump(); // VIEW
            let if_exists = self.parse_if_exists()?;
            let mut names = vec![self.parse_table_name()?];
            while self.is_op(",") {
                self.bump();
                names.push(self.parse_table_name()?);
            }
            if self.is_kw("RESTRICT") || self.is_kw("CASCADE") {
                self.bump();
            }
            Ok(Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::DropView {
                if_exists,
                names,
            })))
        } else if self.is_kw("DROP") && (self.is_kw_at(1, "DATABASE") || self.is_kw_at(1, "SCHEMA"))
        {
            self.bump(); // DROP
            self.bump(); // DATABASE / SCHEMA
            let if_exists = self.parse_if_exists()?;
            Ok(Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::DropDatabase {
                if_exists,
                // Go's parseDropDatabase consumes the next token directly
                // rather than requiring a plain identifier.  Reserved
                // keywords are therefore valid bare database names here;
                // e.g. `DROP DATABASE IF EXISTS plan_cache` is accepted by
                // TiDB and restored with a quoted name.  Keep this broad
                // identifier-like slot local to DROP DATABASE rather than
                // widening the stricter parse_name contract globally.
                name: {
                    let token = self.bump();
                    if token.kind == TokenKind::Eof {
                        return Err(self.err_here("expected database name"));
                    }
                    crate::table_name_token_text(token)
                },
            })))
        } else if self.is_kw("TRUNCATE") {
            // `TRUNCATE [TABLE] name` — the `TABLE` keyword is optional
            // (always restored). A single table only.
            self.bump();
            if self.is_kw("TABLE") {
                self.bump();
            }
            Ok(Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::TruncateTable(
                Box::new(self.parse_table_name()?),
            ))))
        } else if self.is_kw("USE") {
            // `USE dbname` — a single database identifier.
            self.bump();
            Ok(Stmt::Session(tidb_ast::NodeBox::new(SessionStmt::Use(
                // Go's parseUseStmt uses isIdentLike: non-reserved
                // keywords such as PLAN_CACHE are valid bare database names,
                // while reserved grammar words such as SELECT remain errors.
                // Keep this broader slot local to USE rather than widening
                // parse_name globally.
                self.parse_ident_like_name()?,
            ))))
        } else if self.is_kw("PREPARE") {
            self.bump();
            let name = self.parse_name_or_keyword()?;
            self.expect_kw("FROM")?;
            let source = if self.peek().kind == TokenKind::UserVar {
                PrepareSource::Var(decode_at_name(&self.bump().text))
            } else if self.peek().kind == TokenKind::Str {
                PrepareSource::Sql(decode_string(&self.bump().text))
            } else {
                return Err(self.err_here("expected a string or @variable after FROM"));
            };
            Ok(Stmt::Session(tidb_ast::NodeBox::new(
                SessionStmt::Prepare { name, source },
            )))
        } else if self.is_kw("EXECUTE") {
            self.bump();
            // Go's `parseExecuteStmt` uses its ident-like `parseName`
            // production, so a non-reserved keyword is a valid prepared
            // statement name here (unlike generic strict identifier slots).
            let name = self.parse_name_or_keyword()?;
            let mut using = Vec::new();
            if self.is_kw("USING") {
                self.bump();
                loop {
                    if self.peek().kind != TokenKind::UserVar {
                        return Err(self.err_here("expected an @variable in USING"));
                    }
                    using.push(Expr::UserVar(decode_at_name(&self.bump().text)));
                    if self.is_op(",") {
                        self.bump();
                    } else {
                        break;
                    }
                }
            }
            Ok(Stmt::Session(tidb_ast::NodeBox::new(
                SessionStmt::Execute { name, using },
            )))
        } else if (self.is_kw("DEALLOCATE") || self.is_kw("DROP")) && self.is_kw_at(1, "PREPARE") {
            self.bump(); // DEALLOCATE / DROP
            self.bump(); // PREPARE
            Ok(Stmt::Session(tidb_ast::NodeBox::new(
                SessionStmt::Deallocate(self.parse_name()?),
            )))
        } else if self.is_kw("SHOW") && self.is_kw_at(1, "CREATE") && self.is_kw_at(2, "USER") {
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(
                AdminStmt::ShowCreateUser(crate::user::parse_show_create_user(self)?),
            )))
        } else if self.is_kw("SHOW") && self.ident_like_literal_is_at(1, "GRANTS") {
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(AdminStmt::ShowGrants(
                Box::new(self.parse_show_grants()?),
            ))))
        } else if self.is_kw("SHOW")
            && (self.ident_like_literal_is_at(1, "BINDINGS")
                || ((self.is_kw_at(1, "GLOBAL") || self.is_kw_at(1, "SESSION"))
                    && self.token_literal_is_at(2, "BINDINGS")))
        {
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(
                AdminStmt::ShowBindings(Box::new(self.parse_show_bindings()?)),
            )))
        } else if self.is_kw("SHOW")
            && (self.ident_like_literal_is_at(1, "BR")
                || self.ident_like_literal_is_at(1, "BACKUP"))
        {
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(AdminStmt::Brie(
                Box::new(self.parse_show_brie()?),
            ))))
        } else if self.is_kw("CANCEL") && self.is_kw_at(1, "BR") {
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(AdminStmt::Brie(
                Box::new(self.parse_cancel_brie()?),
            ))))
        } else if self.is_kw("SHOW") {
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(
                self.parse_show_inspection()?,
            )))
        } else if self.is_kw("CREATE") && self.is_kw_at(1, "SEQUENCE") {
            Ok(Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::CreateSequence(
                Box::new(self.parse_create_sequence()?),
            ))))
        } else if self.is_kw("ALTER") && self.is_kw_at(1, "SEQUENCE") {
            Ok(Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::AlterSequence(
                Box::new(self.parse_alter_sequence()?),
            ))))
        } else if self.is_kw("DROP") && self.is_kw_at(1, "SEQUENCE") {
            Ok(Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::DropSequence(
                Box::new(self.parse_drop_sequence()?),
            ))))
        } else if self.is_kw("SET") && self.is_kw_at(1, "BINDING") {
            Ok(Stmt::Admin(tidb_ast::NodeBox::new(AdminStmt::SetBinding(
                Box::new(self.parse_set_binding()?),
            ))))
        } else if self.is_specialized_set_statement() {
            Ok(Stmt::Session(tidb_ast::NodeBox::new(
                self.parse_specialized_set_statement()?,
            )))
        } else if self.is_kw("SET") {
            Ok(Stmt::Session(tidb_ast::NodeBox::new(
                self.parse_session_set_statement()?,
            )))
        } else if self.is_kw("BEGIN") {
            self.bump();
            // Go's hand parser (`parseBeginStmt`) carries an explicit
            // PESSIMISTIC/OPTIMISTIC token in `ast.BeginStmt.Mode`; it is
            // not merely execution metadata because AST restore preserves
            // the selected `BEGIN` spelling.
            let mode = if self.is_kw("OPTIMISTIC") {
                self.bump();
                TransactionMode::Optimistic
            } else if self.is_kw("PESSIMISTIC") {
                self.bump();
                TransactionMode::Pessimistic
            } else {
                TransactionMode::Default
            };
            Ok(Stmt::Session(tidb_ast::NodeBox::new(SessionStmt::Begin(
                Box::new(BeginStmt {
                    mode,
                    ..BeginStmt::default()
                }),
            ))))
        } else if self.is_kw("START") && self.is_kw_at(1, "TRANSACTION") {
            self.bump(); // START
            self.bump(); // TRANSACTION
                         // Mirrors Go's `parseBeginStmt` directly. `READ WRITE` and
                         // `WITH CONSISTENT SNAPSHOT` are intentionally represented by
                         // the all-default payload because Go restores both as a bare
                         // `START TRANSACTION`; read-only/AS OF and causal consistency
                         // are AST-visible and cannot be collapsed.
            let mut begin = BeginStmt::default();
            if self.is_kw("READ") {
                self.bump();
                if self.is_kw("WRITE") {
                    self.bump();
                } else if self.is_kw("ONLY") {
                    self.bump();
                    begin.read_only = true;
                    if self.is_kw("AS OF") {
                        self.bump();
                        self.expect_kw("TIMESTAMP")?;
                        begin.as_of = Some(self.parse_expr(prec::NONE)?);
                    }
                }
            } else if self.is_kw("WITH") {
                self.bump();
                if self.is_kw("CONSISTENT") {
                    self.bump();
                    self.expect_kw("SNAPSHOT")?;
                } else if self.is_kw("CAUSAL") {
                    self.bump();
                    self.expect_kw("CONSISTENCY")?;
                    self.expect_kw("ONLY")?;
                    begin.causal_consistency_only = true;
                }
            }
            Ok(Stmt::Session(tidb_ast::NodeBox::new(SessionStmt::Begin(
                Box::new(begin),
            ))))
        } else if self.is_kw("COMMIT") {
            self.bump();
            let completion = self.parse_completion_type()?;
            Ok(Stmt::Session(tidb_ast::NodeBox::new(SessionStmt::Commit(
                completion,
            ))))
        } else if self.is_kw("ROLLBACK") && self.is_kw_at(1, "TO") {
            self.bump(); // ROLLBACK
            self.bump(); // TO
            if self.is_kw("SAVEPOINT") {
                self.bump();
            }
            let name = self.parse_name_or_keyword()?;
            Ok(Stmt::Session(tidb_ast::NodeBox::new(
                SessionStmt::Rollback {
                    savepoint: Some(name),
                    completion: CompletionType::Default,
                },
            )))
        } else if self.is_kw("ROLLBACK") {
            self.bump();
            let completion = self.parse_completion_type()?;
            Ok(Stmt::Session(tidb_ast::NodeBox::new(
                SessionStmt::Rollback {
                    savepoint: None,
                    completion,
                },
            )))
        } else if self.is_kw("SAVEPOINT") {
            self.bump();
            let name = self.parse_name_or_keyword()?;
            Ok(Stmt::Session(tidb_ast::NodeBox::new(
                SessionStmt::Savepoint(Box::new(name)),
            )))
        } else if self.is_kw("RELEASE") {
            self.bump();
            self.expect_kw("SAVEPOINT")?;
            let name = self.parse_name_or_keyword()?;
            Ok(Stmt::Session(tidb_ast::NodeBox::new(
                SessionStmt::ReleaseSavepoint(Box::new(name)),
            )))
        } else if self.is_kw("WITH") {
            let with = self.parse_with_clause()?;
            if self.is_kw("UPDATE") {
                Ok(Stmt::Dml(tidb_ast::NodeBox::new(DmlStmt::With {
                    with,
                    statement: Box::new(DmlStmt::Update(Box::new(self.parse_update()?))),
                })))
            } else if self.is_kw("DELETE") {
                Ok(Stmt::Dml(tidb_ast::NodeBox::new(DmlStmt::With {
                    with,
                    statement: Box::new(DmlStmt::Delete(Box::new(self.parse_delete()?))),
                })))
            } else {
                Ok(Stmt::Query(tidb_ast::NodeBox::new(
                    self.attach_with_to_query(with)?,
                )))
            }
        } else {
            Err(self.err_here("unsupported statement in this phase"))
        }
    }

    /// Direct transcreation of Go `parseCompletionType` for COMMIT/ROLLBACK.
    fn parse_completion_type(&mut self) -> PResult<CompletionType> {
        if self.is_kw("AND") {
            self.bump();
            if self.is_kw("CHAIN") {
                self.bump();
                if self.is_kw("NO") {
                    self.bump();
                    self.expect_kw("RELEASE")?;
                }
                return Ok(CompletionType::Chain);
            }

            self.expect_kw("NO")?;
            self.expect_kw("CHAIN")?;
            if self.is_kw("RELEASE") {
                self.bump();
                return Ok(CompletionType::Release);
            }
            if self.is_kw("NO") {
                self.bump();
                self.expect_kw("RELEASE")?;
            }
            return Ok(CompletionType::Default);
        }
        if self.is_kw("RELEASE") {
            self.bump();
            return Ok(CompletionType::Release);
        }
        if self.is_kw("NO") {
            self.bump();
            self.expect_kw("RELEASE")?;
        }
        Ok(CompletionType::Default)
    }
}
