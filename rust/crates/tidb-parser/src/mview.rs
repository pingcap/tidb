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

//! Go master's materialized view statements (`parser.y`'s "Materialized
//! View Statements" section and `mview_stmt_options.go`). This branch's
//! lexer has no token for the words Go master added (`MATERIALIZED`,
//! `FAST`, `COMPLETE`, ...); they are unreserved keywords there, so a bare
//! identifier with that spelling is the same token here.

use tidb_ast::{
    AdminStmt, AlterMaterializedViewAction, AlterMaterializedViewLogAction,
    AlterMaterializedViewLogStmt, AlterMaterializedViewStmt, CancelMaterializedViewJobStmt,
    CancelMaterializedViewJobType, CompareMaterializedViewStmt, CreateMaterializedViewLogStmt,
    CreateMaterializedViewStmt, DdlStmt, DropMaterializedViewLogStmt, DropMaterializedViewStmt,
    Expr, MLogAccumulationAlertClause, MLogPurgeClause, MViewRefreshClause, MViewRefreshMethod,
    PurgeMaterializedViewLogStmt, RefreshMaterializedViewMode, RefreshMaterializedViewObserveType,
    RefreshMaterializedViewStmt, TableOption,
};
use tidb_lexer::TokenKind;

use crate::{prec, PResult, Parser};

impl Parser {
    /// The bare word `word` `n` tokens ahead: a keyword, or an unquoted
    /// identifier spelled `word` (Go master's unreserved keyword).
    pub(crate) fn is_word_at(&self, n: usize, word: &str) -> bool {
        let token = self.peek_n(n);
        matches!(token.kind, TokenKind::Keyword | TokenKind::Ident) && token.text.eq_ignore_ascii_case(word)
    }

    pub(crate) fn expect_word(&mut self, word: &str) -> PResult<()> {
        if !self.is_word_at(0, word) {
            return Err(self.err_here(&format!("expected {word}")));
        }
        self.advance();
        Ok(())
    }

    /// `<verb> MATERIALIZED VIEW` at the current token.
    fn is_materialized_view_after(&self, verb: &str) -> bool {
        self.is_kw(verb) && self.is_word_at(1, "MATERIALIZED") && self.is_kw_at(2, "VIEW")
    }

    /// The statements of this file that are Go DDL nodes.
    pub(crate) fn is_materialized_view_ddl_statement(&self) -> bool {
        ["CREATE", "ALTER", "DROP"].iter().any(|verb| self.is_materialized_view_after(verb))
    }

    /// The statements of this file that are Go `stmtNode`s.
    pub(crate) fn is_materialized_view_misc_statement(&self) -> bool {
        ["PURGE", "CANCEL", "REFRESH"].iter().any(|verb| self.is_materialized_view_after(verb))
            || (self.is_word_at(0, "COMPARE") && self.is_word_at(1, "MATERIALIZED") && self.is_kw_at(2, "VIEW"))
    }

    /// Go `CreateMaterializedViewStmt`, `CreateMaterializedViewLogStmt`,
    /// `AlterMaterializedView[Log]Stmt` and `DropMaterializedView[Log]Stmt`.
    pub(crate) fn parse_materialized_view_ddl_statement(&mut self) -> PResult<DdlStmt> {
        let verb = self.peek().text.to_ascii_uppercase();
        self.advance();
        self.expect_word("MATERIALIZED")?;
        self.expect_kw("VIEW")?;
        // Go master: CREATE and DROP take `LOG IF [NOT] EXISTS ON`.
        let log = self.is_kw("LOG")
            && (self.is_kw_at(1, "ON") || ((verb == "DROP" || verb == "CREATE") && self.is_kw_at(1, "IF")));
        match (verb.as_str(), log) {
            ("CREATE", false) => self.parse_create_materialized_view().map(|s| DdlStmt::CreateMaterializedView(Box::new(s))),
            ("CREATE", true) => {
                self.parse_create_materialized_view_log().map(|s| DdlStmt::CreateMaterializedViewLog(Box::new(s)))
            }
            ("ALTER", false) => {
                let view_name = self.parse_table_name_path()?;
                let actions = self.parse_comma_list(Self::parse_alter_materialized_view_action)?;
                Ok(DdlStmt::AlterMaterializedView(Box::new(AlterMaterializedViewStmt { view_name, actions })))
            }
            ("ALTER", true) => {
                self.advance(); // LOG
                self.expect_kw("ON")?;
                let table = self.parse_table_name_path()?;
                let actions = self.parse_comma_list(Self::parse_alter_materialized_view_log_action)?;
                Ok(DdlStmt::AlterMaterializedViewLog(Box::new(AlterMaterializedViewLogStmt { table, actions })))
            }
            ("DROP", false) => {
                let if_exists = self.parse_if_exists()?;
                let view_name = self.parse_table_name_path()?;
                Ok(DdlStmt::DropMaterializedView(Box::new(DropMaterializedViewStmt { if_exists, view_name })))
            }
            _ => {
                self.advance(); // LOG
                let if_exists = self.parse_if_exists()?;
                self.expect_kw("ON")?;
                let table = self.parse_table_name_path()?;
                Ok(DdlStmt::DropMaterializedViewLog(Box::new(DropMaterializedViewLogStmt { if_exists, table })))
            }
        }
    }

    /// Go `PurgeMaterializedViewLogStmt`, `CancelMaterializedViewJobStmt`,
    /// `RefreshMaterializedViewStmt` and `CompareMaterializedViewStmt`.
    pub(crate) fn parse_materialized_view_misc_statement(&mut self) -> PResult<AdminStmt> {
        let verb = self.peek().text.to_ascii_uppercase();
        self.advance();
        self.expect_word("MATERIALIZED")?;
        self.expect_kw("VIEW")?;
        match verb.as_str() {
            "PURGE" => {
                self.expect_kw("LOG")?;
                self.expect_kw("ON")?;
                let table = self.parse_table_name_path()?;
                Ok(AdminStmt::PurgeMaterializedViewLog(Box::new(PurgeMaterializedViewLogStmt { table })))
            }
            "CANCEL" => {
                let tp = if self.is_kw("REFRESH") {
                    self.advance();
                    CancelMaterializedViewJobType::Refresh
                } else {
                    self.expect_kw("LOG")?;
                    self.expect_kw("PURGE")?;
                    CancelMaterializedViewJobType::LogPurge
                };
                self.expect_kw("JOB")?;
                let job_id = self.parse_mview_int64_num()?;
                Ok(AdminStmt::CancelMaterializedViewJob(Box::new(CancelMaterializedViewJobStmt { tp, job_id })))
            }
            "REFRESH" => self.parse_refresh_materialized_view().map(|s| AdminStmt::RefreshMaterializedView(Box::new(s))),
            _ => {
                let view_name = self.parse_table_name_path()?;
                let as_of = self.parse_mview_as_of_clause()?;
                let output_table = if self.is_word_at(0, "OUTPUT") {
                    self.advance();
                    self.expect_kw("INTO")?;
                    self.expect_kw("TABLE")?;
                    Some(self.parse_table_name_path()?)
                } else {
                    None
                };
                Ok(AdminStmt::CompareMaterializedView(Box::new(CompareMaterializedViewStmt {
                    view_name,
                    as_of,
                    output_table,
                })))
            }
        }
    }

    /// `item [, item ...]`.
    fn parse_comma_list<T>(&mut self, item: fn(&mut Self) -> PResult<T>) -> PResult<Vec<T>> {
        let mut items = vec![item(self)?];
        while self.is_op(",") {
            self.advance();
            items.push(item(self)?);
        }
        Ok(items)
    }

    /// Go `'(' ColumnList ')'`.
    fn parse_mview_column_list(&mut self) -> PResult<Vec<String>> {
        self.expect_op("(")?;
        let cols = self.parse_comma_list(Self::parse_name_or_keyword)?;
        self.expect_op(")")?;
        Ok(cols)
    }

    /// Go `Int64Num`.
    fn parse_mview_int64_num(&mut self) -> PResult<i64> {
        let token = self.peek().clone();
        if token.kind != TokenKind::IntLit {
            return Err(self.err_here("expected an integer"));
        }
        let value = token.text.parse::<i64>().map_err(|_| self.err_here("integer out of range"))?;
        self.advance();
        Ok(value)
    }

    /// Go `SignedNum`: an optionally signed `Int64Num`.
    fn parse_mview_signed_num(&mut self) -> PResult<i64> {
        let negative = if self.is_op("-") {
            self.advance();
            true
        } else {
            if self.is_op("+") {
                self.advance();
            }
            false
        };
        let value = self.parse_mview_int64_num()?;
        Ok(if negative { -value } else { value })
    }

    /// Go `AsOfClause`: `AS OF TIMESTAMP Expression`.
    fn parse_mview_as_of_clause(&mut self) -> PResult<Expr> {
        self.expect_kw("AS OF")?;
        self.expect_kw("TIMESTAMP")?;
        self.parse_expr(prec::NONE)
    }

    /// Go `EqOpt stringLit`.
    fn parse_mview_eq_string(&mut self) -> PResult<String> {
        self.accept_optional_equals();
        self.parse_string_literal("expected a string literal")
    }

    /// Go `MViewStartWithOrNextOpt`: `START WITH e NEXT e`, `NEXT e`, or
    /// nothing.
    fn parse_mview_start_with_or_next_opt(&mut self) -> PResult<(Option<Expr>, Option<Expr>)> {
        if self.is_kw("START") {
            self.advance();
            self.expect_kw("WITH")?;
            let start_with = self.parse_expr(prec::NONE)?;
            self.expect_kw("NEXT")?;
            let next = self.parse_expr(prec::NONE)?;
            Ok((Some(start_with), Some(next)))
        } else if self.is_kw("NEXT") {
            self.advance();
            Ok((None, Some(self.parse_expr(prec::NONE)?)))
        } else {
            Ok((None, None))
        }
    }

    /// Go `MViewTableOption` / `MLogCreateOption`: `SHARD_ROW_ID_BITS` and
    /// `PRE_SPLIT_REGIONS` with Go's duplicate checks, and `COMMENT` where
    /// `statement` is CREATE MATERIALIZED VIEW (`comment` is `Some`).
    fn parse_mview_create_options(
        &mut self,
        statement: &str,
        mut comment: Option<&mut String>,
    ) -> PResult<Vec<TableOption>> {
        let (mut has_comment, mut has_shard, mut has_pre_split) = (false, false, false);
        let mut options = Vec::new();
        loop {
            if comment.is_some() && self.is_kw("COMMENT") {
                self.advance();
                let text = self.parse_mview_eq_string()?;
                if std::mem::replace(&mut has_comment, true) {
                    return Err(self.err_action(&format!("Duplicate COMMENT specified in {statement}")));
                }
                if let Some(comment) = comment.as_deref_mut() {
                    *comment = text;
                }
            } else if self.is_kw("SHARD_ROW_ID_BITS") || self.is_kw("PRE_SPLIT_REGIONS") {
                let keyword = self.peek().text.to_ascii_uppercase();
                self.advance();
                self.accept_optional_equals();
                let value = self.parse_table_option_integer(&keyword)?;
                let (seen, option) = if keyword == "SHARD_ROW_ID_BITS" {
                    (&mut has_shard, TableOption::ShardRowIdBits(value))
                } else {
                    (&mut has_pre_split, TableOption::PreSplitRegions(value))
                };
                if std::mem::replace(seen, true) {
                    return Err(self.err_action(&format!("Duplicate {keyword} specified in {statement}")));
                }
                options.push(option);
            } else {
                return Ok(options);
            }
        }
    }

    /// Go `CreateMaterializedViewStmt`, after `CREATE MATERIALIZED VIEW`.
    fn parse_create_materialized_view(&mut self) -> PResult<CreateMaterializedViewStmt> {
        // Go master `"CREATE" "MATERIALIZED" "VIEW" "IF" NotSym "EXISTS"`.
        let if_not_exists = self.parse_if_not_exists()?;
        let view_name = self.parse_table_name_path()?;
        let cols = self.parse_mview_column_list()?;
        let mut comment = String::new();
        let options = self.parse_mview_create_options("CREATE MATERIALIZED VIEW", Some(&mut comment))?;
        let refresh = if self.is_kw("REFRESH") {
            self.advance();
            self.expect_word("FAST")?;
            let (start_with, next) = self.parse_mview_start_with_or_next_opt()?;
            Some(MViewRefreshClause { method: MViewRefreshMethod::Fast, start_with, next })
        } else {
            None
        };
        let attributes = if self.is_kw("ATTRIBUTES") {
            self.advance();
            self.parse_mview_eq_string()?
        } else {
            String::new()
        };
        self.expect_kw("AS")?;
        let select = self.parse_create_view_select()?;
        Ok(CreateMaterializedViewStmt { if_not_exists, view_name, cols, comment, refresh, attributes, options, select })
    }

    /// Go `CreateMaterializedViewLogStmt`, after `CREATE MATERIALIZED VIEW`.
    fn parse_create_materialized_view_log(&mut self) -> PResult<CreateMaterializedViewLogStmt> {
        self.expect_kw("LOG")?;
        // Go master `IfNotExists` before `ON`.
        let if_not_exists = self.parse_if_not_exists()?;
        self.expect_kw("ON")?;
        let table = self.parse_table_name_path()?;
        let cols = self.parse_mview_column_list()?;
        let options = self.parse_mview_create_options("CREATE MATERIALIZED VIEW LOG", None)?;
        let purge = if self.is_kw("PURGE") { Some(self.parse_mlog_purge_clause(false)?) } else { None };
        let accumulation_alert = if self.is_word_at(0, "ALERT") {
            self.advance();
            self.expect_kw("ROWS")?;
            Some(MLogAccumulationAlertClause { rows: self.parse_mview_signed_num()? })
        } else {
            None
        };
        Ok(CreateMaterializedViewLogStmt { if_not_exists, table, cols, options, purge, accumulation_alert })
    }

    /// Go `MLogPurgeClause` (`PURGE IMMEDIATE` or `PURGE [START WITH e] NEXT
    /// e`), and with `bare_allowed` Go `AlterMLogPurgeClause`'s bare `PURGE`.
    fn parse_mlog_purge_clause(&mut self, bare_allowed: bool) -> PResult<MLogPurgeClause> {
        self.expect_kw("PURGE")?;
        if self.is_word_at(0, "IMMEDIATE") {
            self.advance();
            return Ok(MLogPurgeClause { immediate: true, start_with: None, next: None });
        }
        if bare_allowed && !self.is_kw("START") && !self.is_kw("NEXT") {
            return Ok(MLogPurgeClause { immediate: false, start_with: None, next: None });
        }
        let start_with = if self.is_kw("START") {
            self.advance();
            self.expect_kw("WITH")?;
            Some(self.parse_expr(prec::NONE)?)
        } else {
            None
        };
        self.expect_kw("NEXT")?;
        let next = Some(self.parse_expr(prec::NONE)?);
        Ok(MLogPurgeClause { immediate: false, start_with, next })
    }

    /// Go `AlterMaterializedViewAction`.
    fn parse_alter_materialized_view_action(&mut self) -> PResult<AlterMaterializedViewAction> {
        if self.is_kw("COMMENT") {
            self.advance();
            Ok(AlterMaterializedViewAction::Comment(self.parse_mview_eq_string()?))
        } else if self.is_kw("REFRESH") {
            self.advance();
            let (start_with, next) = self.parse_mview_start_with_or_next_opt()?;
            Ok(AlterMaterializedViewAction::Refresh(MViewRefreshClause {
                method: MViewRefreshMethod::Fast,
                start_with,
                next,
            }))
        } else if self.is_kw("ATTRIBUTES") {
            self.advance();
            Ok(AlterMaterializedViewAction::Attributes(self.parse_mview_eq_string()?))
        } else {
            Err(self.err_here("expected COMMENT, REFRESH or ATTRIBUTES"))
        }
    }

    /// Go `AlterMaterializedViewLogAction`.
    fn parse_alter_materialized_view_log_action(&mut self) -> PResult<AlterMaterializedViewLogAction> {
        if self.is_kw("ADD") {
            self.advance();
            // Go `ColumnKeywordOpt`.
            if self.is_kw("COLUMN") {
                self.advance();
            }
            Ok(AlterMaterializedViewLogAction::AddColumn(self.parse_mview_column_list()?))
        } else {
            Ok(AlterMaterializedViewLogAction::Purge(self.parse_mlog_purge_clause(true)?))
        }
    }

    /// Go `RefreshMaterializedViewStmt`, after `REFRESH MATERIALIZED VIEW`.
    fn parse_refresh_materialized_view(&mut self) -> PResult<RefreshMaterializedViewStmt> {
        let view_name = self.parse_table_name_path()?;
        // Go `RefreshWithAsyncModeOpt`.
        let with_async_mode = if self.is_kw("WITH") && self.is_word_at(1, "ASYNC") {
            self.advance();
            self.advance();
            self.expect_kw("MODE")?;
            true
        } else {
            false
        };
        let (mode, as_of) = if self.is_word_at(0, "COMPLETE") {
            self.advance();
            // Go `RefreshCompleteMode`.
            let mode = if self.is_kw("IN") {
                self.advance();
                self.expect_word("PLACE")?;
                RefreshMaterializedViewMode::CompleteInPlace
            } else if self.is_kw("OUT") {
                self.advance();
                self.expect_kw("OF")?;
                self.expect_word("PLACE")?;
                RefreshMaterializedViewMode::CompleteOutOfPlace
            } else {
                self.expect_word("DELTA")?;
                self.expect_kw("APPLY")?;
                RefreshMaterializedViewMode::CompleteDeltaApply
            };
            (mode, None)
        } else {
            self.expect_word("FAST")?;
            // Go `AsOfClauseOpt`.
            let as_of = if self.is_kw("AS OF") { Some(self.parse_mview_as_of_clause()?) } else { None };
            (RefreshMaterializedViewMode::Fast, as_of)
        };
        // Go `RefreshMaterializedViewObserveOpt`.
        let observe_type = if self.is_kw("DRY") {
            self.advance();
            self.expect_kw("RUN")?;
            RefreshMaterializedViewObserveType::DryRun
        } else if self.is_kw("WITH") {
            self.advance();
            self.expect_kw("PROFILE")?;
            RefreshMaterializedViewObserveType::Profile
        } else {
            RefreshMaterializedViewObserveType::None
        };
        Ok(RefreshMaterializedViewStmt { view_name, with_async_mode, mode, observe_type, as_of })
    }
}
