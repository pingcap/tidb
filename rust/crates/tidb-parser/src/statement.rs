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
    AdminStmt, BeginStmt, DdlStmt, DmlStmt, DropStatsStmt, LoadStatsStmt, PrepareSource, QueryStmt,
    SessionStmt, Stmt, TransactionMode,
};
use tidb_lexer::TokenKind;

use crate::{decode_at_name, decode_string, prec, PResult, Parser};

impl Parser {
    // ---- statements ----

    pub(crate) fn parse_statement(&mut self) -> PResult<Stmt> {
        self.skip_semicolons();
        if self.is_kw("PLAN") && self.is_kw_at(1, "REPLAYER") {
            self.parse_plan_replayer_dump_explain()
        } else if self.is_kw("EXPLAIN") {
            self.parse_explain()
        } else if self.is_kw("DESCRIBE") || self.is_kw("DESC") {
            self.bump();
            // Go gives all three leaders to `parseExplainStmt`.  A bare
            // table target is its ShowColumns/`DESC` normal form, while a
            // query, DML, or ALTER target remains an EXPLAIN wrapper even
            // when the source spelling was DESC or DESCRIBE.
            self.parse_explain_tail()
        } else if self.is_kw("SELECT") || (self.is_op("(") && self.is_kw_at(1, "SELECT")) {
            Ok(Stmt::Query(Box::new(self.parse_select_or_setopr()?)))
        } else if self.is_kw("TABLE") {
            Ok(Stmt::Query(Box::new(QueryStmt::Select(Box::new(
                self.parse_table_statement()?,
            )))))
        } else if self.is_kw("VALUES") {
            Ok(Stmt::Query(Box::new(QueryStmt::Select(Box::new(
                self.parse_values_statement()?,
            )))))
        } else if self.is_kw("BATCH") {
            Ok(Stmt::Dml(Box::new(DmlStmt::Batch(Box::new(
                self.parse_batch_dml()?,
            )))))
        } else if self.is_kw("LOAD") && self.is_kw_at(1, "STATS") {
            self.bump();
            self.bump();
            if self.peek().kind != TokenKind::Str {
                return Err(self.err_here("expected LOAD STATS path string"));
            }
            Ok(Stmt::Admin(Box::new(AdminStmt::LoadStats(Box::new(
                LoadStatsStmt {
                    path: decode_string(&self.bump().text),
                },
            )))))
        } else if self.is_kw("DROP") && self.is_kw_at(1, "STATS") {
            self.bump();
            self.bump();
            let mut tables = vec![self.parse_name_path()?];
            while self.is_op(",") {
                self.bump();
                tables.push(self.parse_name_path()?);
            }
            let global = if self.is_kw("GLOBAL") {
                self.bump();
                true
            } else {
                false
            };
            let mut partitions = Vec::new();
            if self.is_kw("PARTITION") {
                self.bump();
                partitions.push(self.parse_name()?);
                while self.is_op(",") {
                    self.bump();
                    partitions.push(self.parse_name()?);
                }
            }
            Ok(Stmt::Admin(Box::new(AdminStmt::DropStats(Box::new(
                DropStatsStmt {
                    tables,
                    global,
                    partitions,
                },
            )))))
        } else if self.is_kw("DO") {
            Ok(Stmt::Admin(Box::new(AdminStmt::Do(self.parse_do_stmt()?))))
        } else if self.is_kw("LOAD") && self.is_kw_at(1, "DATA") {
            Ok(Stmt::Dml(Box::new(DmlStmt::LoadData(Box::new(
                self.parse_load_data()?,
            )))))
        } else if self.is_kw("INSERT") || self.is_kw("REPLACE") {
            // `REPLACE` reuses `parse_insert` (same grammar; see its own
            // doc and `tidb_ast::InsertStmt::replace`).
            Ok(Stmt::Dml(Box::new(DmlStmt::Insert(Box::new(
                self.parse_insert()?,
            )))))
        } else if self.is_kw("UPDATE") {
            Ok(Stmt::Dml(Box::new(DmlStmt::Update(Box::new(
                self.parse_update()?,
            )))))
        } else if self.is_kw("DELETE") {
            Ok(Stmt::Dml(Box::new(DmlStmt::Delete(Box::new(
                self.parse_delete()?,
            )))))
        } else if self.is_kw("IMPORT") && self.is_kw_at(1, "INTO") {
            Ok(Stmt::Dml(Box::new(DmlStmt::ImportInto(Box::new(
                self.parse_import_into()?,
            )))))
        } else if self.is_kw("LOCK") && (self.is_kw_at(1, "TABLE") || self.is_kw_at(1, "TABLES")) {
            Ok(Stmt::Ddl(Box::new(DdlStmt::LockTables(Box::new(
                self.parse_lock_tables()?,
            )))))
        } else if self.is_kw("UNLOCK") && (self.is_kw_at(1, "TABLE") || self.is_kw_at(1, "TABLES"))
        {
            self.parse_unlock_tables()?;
            Ok(Stmt::Ddl(Box::new(DdlStmt::UnlockTables)))
        } else if self.is_kw("LOCK") && self.is_kw_at(1, "STATS") {
            Ok(Stmt::Admin(Box::new(AdminStmt::LockStats(Box::new(
                self.parse_stats_lock(true)?,
            )))))
        } else if self.is_kw("UNLOCK") && self.is_kw_at(1, "STATS") {
            Ok(Stmt::Admin(Box::new(AdminStmt::UnlockStats(Box::new(
                self.parse_stats_lock(false)?,
            )))))
        } else if self.is_kw("SPLIT") {
            Ok(Stmt::Admin(Box::new(AdminStmt::SplitRegion(Box::new(
                self.parse_split_region()?,
            )))))
        } else if self.is_kw("FLUSH") {
            Ok(Stmt::Admin(Box::new(AdminStmt::Flush(Box::new(
                self.parse_flush()?,
            )))))
        } else if self.is_kw("GRANT") {
            if self.starts_role_membership("TO") {
                Ok(Stmt::Admin(Box::new(AdminStmt::GrantRole(Box::new(
                    self.parse_grant_role_stmt()?,
                )))))
            } else {
                Ok(Stmt::Admin(Box::new(AdminStmt::Grant(Box::new(
                    self.parse_grant_privilege_stmt()?,
                )))))
            }
        } else if self.is_kw("REVOKE") {
            if self.starts_role_membership("FROM") {
                Ok(Stmt::Admin(Box::new(AdminStmt::RevokeRole(Box::new(
                    self.parse_revoke_role_stmt()?,
                )))))
            } else {
                Ok(Stmt::Admin(Box::new(AdminStmt::Revoke(Box::new(
                    self.parse_revoke_privilege_stmt()?,
                )))))
            }
        } else if self.is_kw("ANALYZE") && self.is_kw_at(1, "INCREMENTAL") {
            Ok(Stmt::Admin(Box::new(AdminStmt::AnalyzeIncremental(
                Box::new(self.parse_analyze_incremental()?),
            ))))
        } else if self.is_kw("ANALYZE") && self.is_kw_at(1, "TABLE") {
            Ok(Stmt::Admin(Box::new(AdminStmt::AnalyzeTable(Box::new(
                self.parse_analyze_table()?,
            )))))
        } else if self.is_traffic_source_statement() {
            Ok(Stmt::Admin(Box::new(
                self.parse_traffic_source_statement()?,
            )))
        } else if self.is_kw("ADMIN") {
            Ok(Stmt::Admin(Box::new(self.parse_admin_statement()?)))
        } else if self.is_resource_group_source_statement() {
            Ok(Stmt::Ddl(Box::new(
                self.parse_resource_group_source_statement()?,
            )))
        } else if self.is_create_masking_policy_source_statement() {
            Ok(Stmt::Ddl(Box::new(
                self.parse_create_masking_policy_source_statement()?,
            )))
        } else if self.is_placement_policy_source_statement() {
            Ok(Stmt::Ddl(Box::new(
                self.parse_placement_policy_source_statement()?,
            )))
        } else if self.is_user_ddl_statement() {
            Ok(Stmt::Ddl(Box::new(self.parse_user_ddl_statement()?)))
        } else if self.is_kw("CREATE")
            && (self.is_kw_at(1, "DATABASE") || self.is_kw_at(1, "SCHEMA"))
        {
            let (if_not_exists, name, options) = self.parse_create_database()?;
            Ok(Stmt::Ddl(Box::new(DdlStmt::CreateDatabase {
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
            Ok(Stmt::Ddl(Box::new(DdlStmt::CreateIndex(Box::new(
                self.parse_create_index()?,
            )))))
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
            Ok(Stmt::Ddl(Box::new(DdlStmt::CreateView(Box::new(
                self.parse_create_view()?,
            )))))
        } else if self.is_kw("CREATE")
            && (self.is_kw_at(1, "TABLE")
                || (self.is_kw_at(1, "TEMPORARY") && self.is_kw_at(2, "TABLE"))
                || (self.is_kw_at(1, "GLOBAL")
                    && self.is_kw_at(2, "TEMPORARY")
                    && self.is_kw_at(3, "TABLE")))
        {
            Ok(Stmt::Ddl(Box::new(DdlStmt::CreateTable(Box::new(
                self.parse_create_table()?,
            )))))
        } else if self.is_kw("CREATE")
            && (self.is_kw_at(1, "BINDING")
                || ((self.is_kw_at(1, "GLOBAL") || self.is_kw_at(1, "SESSION"))
                    && self.is_kw_at(2, "BINDING")))
        {
            Ok(Stmt::Admin(Box::new(AdminStmt::CreateBinding(Box::new(
                self.parse_create_binding()?,
            )))))
        } else if self.is_kw("ALTER")
            && (self.is_kw_at(1, "DATABASE") || self.is_kw_at(1, "SCHEMA"))
        {
            let (name, options) = self.parse_alter_database()?;
            Ok(Stmt::Ddl(Box::new(DdlStmt::AlterDatabase {
                name,
                options,
            })))
        } else if self.is_kw("ALTER") && self.is_kw_at(1, "TABLE") {
            self.parse_alter_table_statement()
        } else if self.is_kw("ALTER") && self.is_kw_at(1, "INSTANCE") {
            Ok(Stmt::Ddl(Box::new(DdlStmt::AlterInstance(Box::new(
                self.parse_alter_instance()?,
            )))))
        } else if self.is_kw("ALTER") && self.is_kw_at(1, "RANGE") {
            Ok(Stmt::Ddl(Box::new(DdlStmt::AlterRange(Box::new(
                self.parse_alter_range()?,
            )))))
        } else if self.is_kw("RENAME") && self.is_kw_at(1, "TABLE") {
            Ok(Stmt::Ddl(Box::new(DdlStmt::RenameTable(Box::new(
                self.parse_rename_table()?,
            )))))
        } else if self.is_kw("DROP")
            && ((self.is_kw_at(1, "TABLE") || self.is_kw_at(1, "TABLES"))
                || (self.is_kw_at(1, "TEMPORARY")
                    && (self.is_kw_at(2, "TABLE") || self.is_kw_at(2, "TABLES")))
                || (self.is_kw_at(1, "GLOBAL")
                    && self.is_kw_at(2, "TEMPORARY")
                    && (self.is_kw_at(3, "TABLE") || self.is_kw_at(3, "TABLES"))))
        {
            Ok(Stmt::Ddl(Box::new(DdlStmt::DropTable(Box::new(
                self.parse_drop_table()?,
            )))))
        } else if self.is_kw("DROP")
            && (self.is_kw_at(1, "INDEX")
                || (self.is_kw_at(1, "HYPO") && self.is_kw_at(2, "INDEX")))
        {
            Ok(Stmt::Ddl(Box::new(DdlStmt::DropIndex(Box::new(
                self.parse_drop_index()?,
            )))))
        } else if self.is_kw("DROP")
            && (self.is_kw_at(1, "BINDING")
                || ((self.is_kw_at(1, "GLOBAL") || self.is_kw_at(1, "SESSION"))
                    && self.is_kw_at(2, "BINDING")))
        {
            Ok(Stmt::Admin(Box::new(AdminStmt::DropBinding(Box::new(
                self.parse_drop_binding()?,
            )))))
        } else if self.is_kw("DROP") && self.is_kw_at(1, "VIEW") {
            self.bump(); // DROP
            self.bump(); // VIEW
            let if_exists = self.parse_if_exists()?;
            let mut names = vec![self.parse_name_path()?];
            while self.is_op(",") {
                self.bump();
                names.push(self.parse_name_path()?);
            }
            Ok(Stmt::Ddl(Box::new(DdlStmt::DropView { if_exists, names })))
        } else if self.is_kw("DROP") && (self.is_kw_at(1, "DATABASE") || self.is_kw_at(1, "SCHEMA"))
        {
            self.bump(); // DROP
            self.bump(); // DATABASE / SCHEMA
            let if_exists = self.parse_if_exists()?;
            Ok(Stmt::Ddl(Box::new(DdlStmt::DropDatabase {
                if_exists,
                // Go's parseDropDatabase consumes the next token directly
                // rather than requiring a plain identifier.  Reserved
                // keywords are therefore valid bare database names here;
                // e.g. `DROP DATABASE IF EXISTS plan_cache` is accepted by
                // TiDB and restored with a quoted name.  Keep this broad
                // identifier-like slot local to DROP DATABASE rather than
                // widening the stricter parse_name contract globally.
                name: self.parse_ident_like_name()?,
            })))
        } else if self.is_kw("TRUNCATE") {
            // `TRUNCATE [TABLE] name` — the `TABLE` keyword is optional
            // (always restored). A single table only.
            self.bump();
            if self.is_kw("TABLE") {
                self.bump();
            }
            Ok(Stmt::Ddl(Box::new(DdlStmt::TruncateTable(Box::new(
                self.parse_name_path()?,
            )))))
        } else if self.is_kw("USE") {
            // `USE dbname` — a single database identifier.
            self.bump();
            Ok(Stmt::Session(Box::new(SessionStmt::Use(
                // Go's parseUseStmt uses isIdentLike: non-reserved
                // keywords such as PLAN_CACHE are valid bare database names,
                // while reserved grammar words such as SELECT remain errors.
                // Keep this broader slot local to USE rather than widening
                // parse_name globally.
                self.parse_name_or_keyword()?,
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
            Ok(Stmt::Session(Box::new(SessionStmt::Prepare {
                name,
                source,
            })))
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
                    using.push(decode_at_name(&self.bump().text));
                    if self.is_op(",") {
                        self.bump();
                    } else {
                        break;
                    }
                }
            }
            Ok(Stmt::Session(Box::new(SessionStmt::Execute {
                name,
                using,
            })))
        } else if (self.is_kw("DEALLOCATE") || self.is_kw("DROP")) && self.is_kw_at(1, "PREPARE") {
            self.bump(); // DEALLOCATE / DROP
            self.bump(); // PREPARE
            Ok(Stmt::Session(Box::new(SessionStmt::Deallocate(
                self.parse_name()?,
            ))))
        } else if self.is_kw("SHOW") && self.is_kw_at(1, "CREATE") && self.is_kw_at(2, "USER") {
            Ok(Stmt::Admin(Box::new(AdminStmt::ShowCreateUser(
                crate::user::parse_show_create_user(self)?,
            ))))
        } else if self.is_kw("SHOW") && self.is_kw_at(1, "GRANTS") {
            Ok(Stmt::Admin(Box::new(AdminStmt::ShowGrants(Box::new(
                self.parse_show_grants()?,
            )))))
        } else if self.is_kw("SHOW")
            && (self.is_kw_at(1, "BINDINGS")
                || ((self.is_kw_at(1, "GLOBAL") || self.is_kw_at(1, "SESSION"))
                    && self.is_kw_at(2, "BINDINGS")))
        {
            Ok(Stmt::Admin(Box::new(AdminStmt::ShowBindings(Box::new(
                self.parse_show_bindings()?,
            )))))
        } else if self.is_kw("SHOW") {
            Ok(Stmt::Admin(Box::new(self.parse_show_inspection()?)))
        } else if self.is_kw("CREATE") && self.is_kw_at(1, "SEQUENCE") {
            Ok(Stmt::Ddl(Box::new(DdlStmt::CreateSequence(Box::new(
                self.parse_create_sequence()?,
            )))))
        } else if self.is_kw("ALTER") && self.is_kw_at(1, "SEQUENCE") {
            Ok(Stmt::Ddl(Box::new(DdlStmt::AlterSequence(Box::new(
                self.parse_alter_sequence()?,
            )))))
        } else if self.is_kw("DROP") && self.is_kw_at(1, "SEQUENCE") {
            Ok(Stmt::Ddl(Box::new(DdlStmt::DropSequence(Box::new(
                self.parse_drop_sequence()?,
            )))))
        } else if self.is_kw("SET") && self.is_kw_at(1, "BINDING") {
            Ok(Stmt::Admin(Box::new(AdminStmt::SetBinding(Box::new(
                self.parse_set_binding()?,
            )))))
        } else if self.is_specialized_set_statement() {
            Ok(Stmt::Session(Box::new(
                self.parse_specialized_set_statement()?,
            )))
        } else if self.is_kw("SET") {
            Ok(Stmt::Session(Box::new(self.parse_session_set_statement()?)))
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
            Ok(Stmt::Session(Box::new(SessionStmt::Begin(Box::new(
                BeginStmt {
                    mode,
                    ..BeginStmt::default()
                },
            )))))
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
            Ok(Stmt::Session(Box::new(SessionStmt::Begin(Box::new(begin)))))
        } else if self.is_kw("COMMIT") {
            self.bump();
            Ok(Stmt::Session(Box::new(SessionStmt::Commit)))
        } else if self.is_kw("ROLLBACK") && self.is_kw_at(1, "TO") {
            self.bump(); // ROLLBACK
            self.bump(); // TO
            if self.is_kw("SAVEPOINT") {
                self.bump();
            }
            let name = self.parse_name_or_keyword()?;
            Ok(Stmt::Session(Box::new(SessionStmt::RollbackToSavepoint(
                Box::new(name),
            ))))
        } else if self.is_kw("ROLLBACK") {
            self.bump();
            Ok(Stmt::Session(Box::new(SessionStmt::Rollback)))
        } else if self.is_kw("SAVEPOINT") {
            self.bump();
            let name = self.parse_name_or_keyword()?;
            Ok(Stmt::Session(Box::new(SessionStmt::Savepoint(Box::new(
                name,
            )))))
        } else if self.is_kw("RELEASE") {
            self.bump();
            self.expect_kw("SAVEPOINT")?;
            let name = self.parse_name_or_keyword()?;
            Ok(Stmt::Session(Box::new(SessionStmt::ReleaseSavepoint(
                Box::new(name),
            ))))
        } else if self.is_kw("WITH") {
            let with = self.parse_with_clause()?;
            if self.is_kw("UPDATE") {
                Ok(Stmt::Dml(Box::new(DmlStmt::With {
                    with,
                    statement: Box::new(DmlStmt::Update(Box::new(self.parse_update()?))),
                })))
            } else if self.is_kw("DELETE") {
                Ok(Stmt::Dml(Box::new(DmlStmt::With {
                    with,
                    statement: Box::new(DmlStmt::Delete(Box::new(self.parse_delete()?))),
                })))
            } else {
                Ok(Stmt::Query(Box::new(self.attach_with_to_query(with)?)))
            }
        } else {
            Err(self.err_here("unsupported statement in this phase"))
        }
    }
}
