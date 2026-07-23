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

//! Typed `ADMIN` grammar translated from `pkg/parser/admin_stmt_parser.go`.
//!
//! Standalone `ANALYZE` and `FLUSH` have different Go source owners and stay
//! outside this module.

use tidb_ast::{
    AdminBindingControlKind, AdminCheckHandleRange, AdminCheckStmt, AdminChecksumStmt,
    AdminCleanupTableLockStmt, AdminRecoverIndexStmt, AdminReloadKind, AdminShowDdlJobQueriesStmt,
    AdminShowDdlJobsStmt, AdminShowNextRowIdStmt, AdminShowSlowMode, AdminShowSlowStmt,
    AdminShowSlowTopScope, AdminStmt, BdrRole,
};
use tidb_lexer::TokenKind;

use crate::{PResult, Parser};

#[path = "admin/ddl_job_alter.rs"]
mod ddl_job_alter;
#[path = "admin/ddl_job_control.rs"]
mod ddl_job_control;
#[path = "admin/flush_plan_cache.rs"]
mod flush_plan_cache;

impl Parser {
    /// Routes the complete `AdminStmtType` family with the `ADMIN` leader.
    pub(crate) fn parse_admin_statement(&mut self) -> PResult<AdminStmt> {
        if let Some(scope) = flush_plan_cache::parse(self)? {
            return Ok(AdminStmt::FlushPlanCache(scope));
        }
        if let Some(control) = ddl_job_control::parse(self)? {
            return Ok(AdminStmt::DdlJobControl(Box::new(control)));
        }
        if let Some(alter) = ddl_job_alter::parse(self)? {
            return Ok(AdminStmt::AlterDdlJobs(Box::new(alter)));
        }
        if (self.is_kw_at(1, "SET") || self.is_kw_at(1, "UNSET")) && self.is_kw_at(2, "BDR") {
            return self.parse_admin_bdr_role();
        }
        if self.is_kw_at(1, "SHOW") && self.is_kw_at(2, "BDR") && self.is_kw_at(3, "ROLE") {
            self.bump();
            self.bump();
            self.bump();
            self.bump();
            return Ok(AdminStmt::ShowBdrRole);
        }
        if self.is_kw_at(1, "SHOW") && self.is_kw_at(2, "SLOW") {
            return Ok(AdminStmt::ShowSlow(Box::new(self.parse_admin_show_slow()?)));
        }
        if self.is_kw_at(1, "SHOW") && self.is_kw_at(2, "DDL") && self.is_kw_at(3, "JOBS") {
            return Ok(AdminStmt::ShowDdlJobs(Box::new(
                self.parse_admin_show_ddl_jobs()?,
            )));
        }
        if self.is_kw_at(1, "SHOW")
            && self.is_kw_at(2, "DDL")
            && self.is_kw_at(3, "JOB")
            && self.is_kw_at(4, "QUERIES")
        {
            return Ok(AdminStmt::ShowDdlJobQueries(Box::new(
                self.parse_admin_show_ddl_job_queries()?,
            )));
        }
        if self.is_kw_at(1, "SHOW") && self.is_kw_at(2, "DDL") {
            return self.parse_admin_show_ddl();
        }
        if self.is_admin_show_next_row_id() {
            return Ok(AdminStmt::ShowNextRowId(Box::new(
                self.parse_admin_show_next_row_id()?,
            )));
        }
        if self.is_kw_at(1, "RELOAD") {
            return Ok(AdminStmt::Reload(self.parse_admin_reload()?));
        }
        if self.is_kw_at(1, "CREATE") && self.is_kw_at(2, "WORKLOAD") {
            self.expect_kw("ADMIN")?;
            self.expect_kw("CREATE")?;
            self.expect_kw("WORKLOAD")?;
            if self.is_kw("SNAPSHOT") {
                self.bump();
            }
            return Ok(AdminStmt::CreateWorkloadSnapshot);
        }
        if self.is_kw_at(1, "PLUGINS") {
            return self.parse_admin_plugins();
        }
        if self.is_kw_at(1, "FLUSH") && self.is_kw_at(2, "BINDINGS") {
            self.bump();
            self.bump();
            self.bump();
            return Ok(AdminStmt::BindingControl(AdminBindingControlKind::Flush));
        }
        if self.is_kw_at(1, "CAPTURE") && self.is_kw_at(2, "BINDINGS") {
            self.bump();
            self.bump();
            self.bump();
            return Ok(AdminStmt::BindingControl(AdminBindingControlKind::Capture));
        }
        if self.is_kw_at(1, "EVOLVE") && self.is_kw_at(2, "BINDINGS") {
            self.bump();
            self.bump();
            self.bump();
            return Ok(AdminStmt::BindingControl(AdminBindingControlKind::Evolve));
        }
        if self.is_kw_at(1, "CHECKSUM") {
            return Ok(AdminStmt::AdminChecksum(Box::new(
                self.parse_admin_checksum()?,
            )));
        }
        if self.is_kw_at(1, "RECOVER") {
            return Ok(AdminStmt::AdminRecoverIndex(Box::new(
                self.parse_admin_recover_index()?,
            )));
        }
        if self.is_kw_at(1, "CLEANUP") && self.is_kw_at(2, "INDEX") {
            return Ok(AdminStmt::AdminCleanupIndex(Box::new(
                self.parse_admin_index_operation("CLEANUP")?,
            )));
        }
        if self.is_kw_at(1, "CHECK") {
            return Ok(AdminStmt::AdminCheck(Box::new(self.parse_admin_check()?)));
        }
        if self.is_kw_at(1, "CLEANUP") && self.is_kw_at(2, "TABLE") && self.is_kw_at(3, "LOCK") {
            return Ok(AdminStmt::CleanupTableLock(Box::new(
                self.parse_admin_cleanup_table_lock()?,
            )));
        }
        Err(self.err_here("unsupported ADMIN command"))
    }

    fn parse_admin_plugins(&mut self) -> PResult<AdminStmt> {
        self.expect_kw("ADMIN")?;
        self.expect_kw("PLUGINS")?;
        let enable = if self.is_kw("ENABLE") {
            self.bump();
            true
        } else {
            // Go selects DISABLE for every non-ENABLE token and consumes
            // that token without validating it, including EOF.
            self.bump();
            false
        };
        let mut plugins = Vec::new();
        while !self.at_eof() && !self.is_op(";") {
            plugins.push(crate::table_name_token_text(self.bump()));
            if self.is_op(",") {
                self.bump();
            } else {
                break;
            }
        }
        Ok(AdminStmt::Plugins { enable, plugins })
    }

    /// Parses Go's distinct `CleanupTableLockStmt` production. The table
    /// list is nonempty; restore uses Go's canonical comma spacing and quoted
    /// table-name paths.
    fn parse_admin_cleanup_table_lock(&mut self) -> PResult<AdminCleanupTableLockStmt> {
        self.expect_kw("ADMIN")?;
        self.expect_kw("CLEANUP")?;
        self.expect_kw("TABLE")?;
        self.expect_kw("LOCK")?;
        let mut tables = vec![self.parse_table_name()?];
        while self.is_op(",") {
            self.bump();
            tables.push(self.parse_table_name()?);
        }
        Ok(AdminCleanupTableLockStmt { tables })
    }

    /// Parses `ADMIN CHECK`: table lists, or one index with optional ranges.
    fn parse_admin_check(&mut self) -> PResult<AdminCheckStmt> {
        self.expect_kw("ADMIN")?;
        self.expect_kw("CHECK")?;
        if self.is_kw("TABLE") {
            self.bump();
            let mut tables = vec![self.parse_table_name()?];
            while self.is_op(",") {
                self.bump();
                tables.push(self.parse_table_name()?);
            }
            return Ok(AdminCheckStmt::Table { tables });
        }
        if self.is_kw("INDEX") {
            self.bump();
            let table = self.parse_table_name()?;
            // Go's `parseName` is deliberately narrower than table-name
            // parsing: an index name must be an identifier, not an arbitrary
            // non-reserved keyword.
            let index = self.parse_name_or_keyword()?;
            let mut handle_ranges = Vec::new();
            if self.is_op("(") {
                loop {
                    self.expect_op("(")?;
                    let begin = self.parse_admin_check_handle()?;
                    self.expect_op(",")?;
                    let end = self.parse_admin_check_handle()?;
                    self.expect_op(")")?;
                    handle_ranges.push(AdminCheckHandleRange { begin, end });
                    if self.is_op(",") {
                        self.bump();
                    } else {
                        break;
                    }
                }
            }
            return Ok(AdminCheckStmt::Index {
                table,
                index,
                handle_ranges,
            });
        }
        Err(self.err_here("expected TABLE or INDEX after ADMIN CHECK"))
    }

    /// Parses Go's distinct `ADMIN CHECKSUM TABLE table [, table ...]`
    /// production rather than folding it into `ADMIN CHECK TABLE`.
    fn parse_admin_checksum(&mut self) -> PResult<AdminChecksumStmt> {
        self.expect_kw("ADMIN")?;
        self.expect_kw("CHECKSUM")?;
        self.expect_kw("TABLE")?;
        let mut tables = vec![self.parse_table_name()?];
        while self.is_op(",") {
            self.bump();
            tables.push(self.parse_table_name()?);
        }
        Ok(AdminChecksumStmt { tables })
    }

    /// Parses Go's `ADMIN RECOVER INDEX table index` production.
    fn parse_admin_recover_index(&mut self) -> PResult<AdminRecoverIndexStmt> {
        self.parse_admin_index_operation("RECOVER")
    }

    fn parse_admin_index_operation(&mut self, operation: &str) -> PResult<AdminRecoverIndexStmt> {
        self.expect_kw("ADMIN")?;
        self.expect_kw(operation)?;
        self.expect_kw("INDEX")?;
        let table = self.parse_table_name()?;
        let index = self.parse_name_or_keyword()?;
        Ok(AdminRecoverIndexStmt { table, index })
    }

    /// Parses TiDB's BDR control commands. The unset state is a separate AST
    /// command, so an invalid empty BDR role cannot be constructed.
    fn parse_admin_bdr_role(&mut self) -> PResult<AdminStmt> {
        self.expect_kw("ADMIN")?;
        let is_set = if self.is_kw("SET") {
            self.bump();
            true
        } else {
            self.expect_kw("UNSET")?;
            false
        };
        self.expect_kw("BDR")?;
        // Go consumes the noun token without checking that it spells ROLE.
        self.bump();
        if !is_set {
            return Ok(AdminStmt::UnsetBdrRole);
        }
        if self.is_kw("PRIMARY") {
            self.bump();
            Ok(AdminStmt::SetBdrRole(BdrRole::Primary))
        } else if self.is_kw("SECONDARY") {
            self.bump();
            Ok(AdminStmt::SetBdrRole(BdrRole::Secondary))
        } else {
            Err(self.err_here("expected PRIMARY or SECONDARY after ADMIN SET BDR ROLE"))
        }
    }

    /// Direct translation of Go's value-less `ADMIN RELOAD` variants.
    fn parse_admin_reload(&mut self) -> PResult<AdminReloadKind> {
        self.expect_kw("ADMIN")?;
        self.expect_kw("RELOAD")?;
        if self.is_kw("STATISTICS") || self.is_kw("STATS_EXTENDED") {
            self.bump();
            Ok(AdminReloadKind::Statistics)
        } else if self.is_kw("OPT_RULE_BLACKLIST") {
            self.bump();
            Ok(AdminReloadKind::OptRuleBlacklist)
        } else if self.is_kw("EXPR_PUSHDOWN_BLACKLIST") {
            self.bump();
            Ok(AdminReloadKind::ExprPushdownBlacklist)
        } else if self.is_kw("BINDINGS") {
            self.bump();
            Ok(AdminReloadKind::Bindings)
        } else if self.is_kw("CLUSTER") {
            self.bump();
            if self.is_kw("BINDINGS") {
                self.bump();
            }
            Ok(AdminReloadKind::ClusterBindings)
        } else {
            Err(self.err_here("unsupported ADMIN RELOAD target"))
        }
    }

    /// Parses the unsigned integer tokens Go converts into signed handle
    /// endpoints and rejects values above `i64::MAX`.
    fn parse_admin_check_handle(&mut self) -> PResult<i64> {
        let token = self.peek().clone();
        if token.kind != TokenKind::IntLit {
            return Err(self.err_here("expected an ADMIN CHECK INDEX integer handle"));
        }
        self.bump();
        let value: u64 = token.text.parse().map_err(|_| {
            self.err_here("ADMIN CHECK INDEX handle is out of range for signed 64-bit")
        })?;
        value.try_into().map_err(|_| {
            self.err_here("ADMIN CHECK INDEX handle is out of range for signed 64-bit")
        })
    }

    /// Parses `ADMIN SHOW SLOW {RECENT | TOP [INTERNAL | ALL]} count`.
    fn parse_admin_show_slow(&mut self) -> PResult<AdminShowSlowStmt> {
        self.expect_kw("ADMIN")?;
        self.expect_kw("SHOW")?;
        self.expect_kw("SLOW")?;
        if self.at_eof() || self.is_op(";") {
            return Ok(AdminShowSlowStmt {
                mode: AdminShowSlowMode::Top(AdminShowSlowTopScope::Default),
                count: 0,
            });
        }
        let mode = if self.is_kw("RECENT") {
            self.bump();
            AdminShowSlowMode::Recent
        } else if self.is_kw("TOP") {
            self.bump();
            let scope = if self.is_kw("INTERNAL") {
                self.bump();
                AdminShowSlowTopScope::Internal
            } else if self.is_kw("ALL") {
                self.bump();
                AdminShowSlowTopScope::All
            } else {
                AdminShowSlowTopScope::Default
            };
            AdminShowSlowMode::Top(scope)
        } else {
            return Err(self.err_here("expected RECENT or TOP after ADMIN SHOW SLOW"));
        };
        let token = self.peek().clone();
        if token.kind != TokenKind::IntLit {
            return Err(self.err_here("expected an integer count after ADMIN SHOW SLOW mode"));
        }
        self.bump();
        let count = token
            .text
            .parse()
            .map_err(|_| self.err_here("expected an integer count after ADMIN SHOW SLOW mode"))?;
        Ok(AdminShowSlowStmt { mode, count })
    }

    /// Parses only Go's `ADMIN SHOW DDL JOBS [number] [WHERE expression]`
    /// branch, excluding sibling `ADMIN SHOW DDL` and `JOB QUERIES` forms.
    fn parse_admin_show_ddl_jobs(&mut self) -> PResult<AdminShowDdlJobsStmt> {
        self.expect_kw("ADMIN")?;
        self.expect_kw("SHOW")?;
        self.expect_kw("DDL")?;
        self.expect_kw("JOBS")?;
        let job_number = if self.peek().kind == TokenKind::IntLit {
            let token = self.bump();
            token.text.parse().map_err(|_| {
                self.err_here("ADMIN SHOW DDL JOBS number is out of signed 64-bit range")
            })?
        } else {
            0
        };
        let where_clause = if self.is_kw("WHERE") {
            self.bump();
            Some(self.parse_expr(crate::prec::NONE)?)
        } else {
            None
        };
        Ok(AdminShowDdlJobsStmt {
            job_number,
            where_clause,
        })
    }

    /// Parses only Go's `ADMIN SHOW DDL JOB QUERIES` list/range alternatives.
    /// Bare `ADMIN SHOW DDL` and `ADMIN SHOW DDL JOBS` keep their own leaves.
    fn parse_admin_show_ddl_job_queries(&mut self) -> PResult<AdminShowDdlJobQueriesStmt> {
        self.expect_kw("ADMIN")?;
        self.expect_kw("SHOW")?;
        self.expect_kw("DDL")?;
        self.expect_kw("JOB")?;
        self.expect_kw("QUERIES")?;
        if self.is_kw("LIMIT") {
            self.bump();
            let mut count = 0;
            let mut offset = 0;
            if self.peek().kind == TokenKind::IntLit {
                count =
                    self.bump().text.parse().map_err(|_| {
                        self.err_here("DDL job query LIMIT integer is out of range")
                    })?;
                if self.is_op(",") {
                    self.bump();
                    offset = count;
                    if self.peek().kind == TokenKind::IntLit {
                        count = self.bump().text.parse().map_err(|_| {
                            self.err_here("DDL job query LIMIT integer is out of range")
                        })?;
                    }
                } else if self.is_kw("OFFSET") {
                    self.bump();
                    if self.peek().kind == TokenKind::IntLit {
                        offset = self.bump().text.parse().map_err(|_| {
                            self.err_here("DDL job query LIMIT integer is out of range")
                        })?;
                    }
                }
            }
            return Ok(AdminShowDdlJobQueriesStmt::Limit { offset, count });
        }

        let mut job_ids = Vec::new();
        loop {
            if self.peek().kind == TokenKind::IntLit {
                job_ids.push(
                    self.bump()
                        .text
                        .parse()
                        .map_err(|_| self.err_here("DDL job query ID is out of range"))?,
                );
            }
            if self.is_op(",") {
                self.bump();
            } else {
                break;
            }
        }
        Ok(AdminShowDdlJobQueriesStmt::JobIds(job_ids))
    }

    /// Parses Go's value-less `ADMIN SHOW DDL` leaf after its typed JOBS and
    /// JOB QUERIES extensions have already claimed their longer prefixes.
    fn parse_admin_show_ddl(&mut self) -> PResult<AdminStmt> {
        self.expect_kw("ADMIN")?;
        self.expect_kw("SHOW")?;
        self.expect_kw("DDL")?;
        Ok(AdminStmt::ShowDdl)
    }

    /// Detects only Go's `ADMIN SHOW table NEXT_ROW_ID` production.
    fn is_admin_show_next_row_id(&self) -> bool {
        if !(self.is_kw("ADMIN") && self.is_kw_at(1, "SHOW")) {
            return false;
        }
        let mut offset = 2;
        if self.is_op_at(offset, "*") && self.is_op_at(offset + 1, ".") {
            offset += 3;
        } else {
            if !crate::is_table_name_first_token(self.peek_n(offset)) {
                return false;
            }
            offset += 1;
            if self.is_op_at(offset, ".") && !self.is_op_at(offset + 1, "*") {
                offset += 2;
            }
        }
        self.is_kw_at(offset, "NEXT_ROW_ID")
    }

    /// Parses the typed table payload selected by
    /// [`Self::is_admin_show_next_row_id`].
    fn parse_admin_show_next_row_id(&mut self) -> PResult<AdminShowNextRowIdStmt> {
        self.expect_kw("ADMIN")?;
        self.expect_kw("SHOW")?;
        let table = self.parse_table_name()?;
        self.expect_kw("NEXT_ROW_ID")?;
        Ok(AdminShowNextRowIdStmt { table })
    }
}
