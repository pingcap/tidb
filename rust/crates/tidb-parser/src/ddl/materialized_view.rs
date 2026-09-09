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

//! Materialized-view DDL grammar, translated from Go `parser.y`.

use tidb_ast::{
    AlterMaterializedViewAction, AlterMaterializedViewLogAction, AlterMaterializedViewLogStmt,
    AlterMaterializedViewStmt, CreateMaterializedViewLogStmt, CreateMaterializedViewStmt, DdlStmt,
    MLogAccumulationAlertClause, MLogPurgeClause, MViewRefreshClause, MViewRefreshMethod,
    QueryStmt, TableOption,
};
use tidb_lexer::TokenKind;

use crate::{prec, PResult, Parser};

impl Parser {
    pub(crate) fn parse_create_materialized_view(&mut self) -> PResult<CreateMaterializedViewStmt> {
        self.expect_kw("CREATE")?;
        self.expect_kw("MATERIALIZED")?;
        self.expect_kw("VIEW")?;
        let view_name = self.parse_table_name()?;
        let columns = self.parse_materialized_view_columns()?;

        let mut comment = None;
        let mut options = Vec::new();
        let mut saw_shard = false;
        let mut saw_pre_split = false;
        while !self.is_kw("REFRESH") && !self.is_kw("ATTRIBUTES") && !self.is_kw("AS") {
            if self.is_kw("COMMENT") {
                if comment.is_some() {
                    return Err(
                        self.err_here("Duplicate COMMENT specified in CREATE MATERIALIZED VIEW")
                    );
                }
                self.bump();
                self.accept_optional_equals();
                comment =
                    Some(self.parse_string_literal("expected materialized view COMMENT string")?);
            } else if self.is_kw("SHARD_ROW_ID_BITS") {
                if saw_shard {
                    return Err(self.err_here(
                        "Duplicate SHARD_ROW_ID_BITS specified in CREATE MATERIALIZED VIEW",
                    ));
                }
                saw_shard = true;
                options.push(TableOption::ShardRowIdBits(
                    self.parse_table_option_after_keyword("SHARD_ROW_ID_BITS")?,
                ));
            } else if self.is_kw("PRE_SPLIT_REGIONS") {
                if saw_pre_split {
                    return Err(self.err_here(
                        "Duplicate PRE_SPLIT_REGIONS specified in CREATE MATERIALIZED VIEW",
                    ));
                }
                saw_pre_split = true;
                options.push(TableOption::PreSplitRegions(
                    self.parse_table_option_after_keyword("PRE_SPLIT_REGIONS")?,
                ));
            } else {
                return Err(self.err_here("expected materialized view option or AS"));
            }
        }

        let refresh = if self.is_kw("REFRESH") {
            Some(self.parse_materialized_view_refresh(true)?)
        } else {
            None
        };
        let attributes = if self.is_kw("ATTRIBUTES") {
            self.bump();
            self.accept_optional_equals();
            Some(self.parse_string_literal("expected materialized view ATTRIBUTES string")?)
        } else {
            None
        };
        self.expect_kw("AS")?;
        let (query, query_parenthesized) = self.parse_materialized_view_query()?;
        Ok(CreateMaterializedViewStmt {
            view_name,
            columns,
            comment,
            refresh,
            attributes,
            options,
            query: Box::new(query),
            query_parenthesized,
        })
    }

    pub(crate) fn parse_create_materialized_view_log(
        &mut self,
    ) -> PResult<CreateMaterializedViewLogStmt> {
        self.expect_kw("CREATE")?;
        self.expect_kw("MATERIALIZED")?;
        self.expect_kw("VIEW")?;
        self.expect_kw("LOG")?;
        self.expect_kw("ON")?;
        let table = self.parse_table_name()?;
        let columns = self.parse_materialized_view_columns()?;
        let mut options = Vec::new();
        let mut saw_shard = false;
        let mut saw_pre_split = false;
        while !self.is_kw("PURGE") && !self.is_kw("ALERT") && !self.at_eof() {
            if self.is_kw("SHARD_ROW_ID_BITS") {
                if saw_shard {
                    return Err(self.err_here(
                        "Duplicate SHARD_ROW_ID_BITS specified in CREATE MATERIALIZED VIEW LOG",
                    ));
                }
                saw_shard = true;
                options.push(TableOption::ShardRowIdBits(
                    self.parse_table_option_after_keyword("SHARD_ROW_ID_BITS")?,
                ));
            } else if self.is_kw("PRE_SPLIT_REGIONS") {
                if saw_pre_split {
                    return Err(self.err_here(
                        "Duplicate PRE_SPLIT_REGIONS specified in CREATE MATERIALIZED VIEW LOG",
                    ));
                }
                saw_pre_split = true;
                options.push(TableOption::PreSplitRegions(
                    self.parse_table_option_after_keyword("PRE_SPLIT_REGIONS")?,
                ));
            } else {
                return Err(self.err_here("expected materialized view log option"));
            }
        }
        let purge = if self.is_kw("PURGE") {
            Some(self.parse_materialized_view_log_purge()?)
        } else {
            None
        };
        let accumulation_alert = if self.is_kw("ALERT") {
            Some(self.parse_materialized_view_log_alert()?)
        } else {
            None
        };
        Ok(CreateMaterializedViewLogStmt {
            table,
            columns,
            options,
            purge,
            accumulation_alert,
        })
    }

    pub(crate) fn parse_alter_materialized_view(&mut self) -> PResult<AlterMaterializedViewStmt> {
        self.expect_kw("ALTER")?;
        self.expect_kw("MATERIALIZED")?;
        self.expect_kw("VIEW")?;
        let view_name = self.parse_table_name()?;
        let mut actions = vec![self.parse_materialized_view_action()?];
        while self.is_op(",") {
            self.bump();
            actions.push(self.parse_materialized_view_action()?);
        }
        Ok(AlterMaterializedViewStmt { view_name, actions })
    }

    pub(crate) fn parse_alter_materialized_view_log(
        &mut self,
    ) -> PResult<AlterMaterializedViewLogStmt> {
        self.expect_kw("ALTER")?;
        self.expect_kw("MATERIALIZED")?;
        self.expect_kw("VIEW")?;
        self.expect_kw("LOG")?;
        self.expect_kw("ON")?;
        let table = self.parse_table_name()?;
        let mut actions = vec![self.parse_materialized_view_log_action()?];
        while self.is_op(",") {
            self.bump();
            actions.push(self.parse_materialized_view_log_action()?);
        }
        Ok(AlterMaterializedViewLogStmt { table, actions })
    }

    pub(crate) fn parse_drop_materialized_view(&mut self) -> PResult<DdlStmt> {
        self.expect_kw("DROP")?;
        self.expect_kw("MATERIALIZED")?;
        self.expect_kw("VIEW")?;
        let if_exists = self.parse_if_exists()?;
        let view_name = self.parse_table_name()?;
        Ok(DdlStmt::DropMaterializedView(Box::new(
            tidb_ast::DropMaterializedViewStmt {
                if_exists,
                view_name,
            },
        )))
    }

    pub(crate) fn parse_drop_materialized_view_log(&mut self) -> PResult<DdlStmt> {
        self.expect_kw("DROP")?;
        self.expect_kw("MATERIALIZED")?;
        self.expect_kw("VIEW")?;
        self.expect_kw("LOG")?;
        let if_exists = self.parse_if_exists()?;
        self.expect_kw("ON")?;
        let table = self.parse_table_name()?;
        Ok(DdlStmt::DropMaterializedViewLog(Box::new(
            tidb_ast::DropMaterializedViewLogStmt { if_exists, table },
        )))
    }

    fn parse_materialized_view_columns(&mut self) -> PResult<Vec<String>> {
        self.expect_op("(")?;
        if self.is_op(")") {
            return Err(self.err_here("materialized view requires at least one column"));
        }
        let mut columns = vec![self.parse_name_or_keyword()?];
        while self.is_op(",") {
            self.bump();
            columns.push(self.parse_name_or_keyword()?);
        }
        self.expect_op(")")?;
        Ok(columns)
    }

    fn parse_table_option_after_keyword(&mut self, keyword: &str) -> PResult<String> {
        self.expect_kw(keyword)?;
        self.accept_optional_equals();
        self.parse_table_option_integer(keyword)
    }

    fn parse_materialized_view_refresh(&mut self, with_fast: bool) -> PResult<MViewRefreshClause> {
        self.expect_kw("REFRESH")?;
        if with_fast {
            self.expect_kw("FAST")?;
        }
        let mut start_with = None;
        let mut next = None;
        if self.is_kw("START") {
            self.bump();
            self.expect_kw("WITH")?;
            start_with = Some(Box::new(self.parse_expr(prec::NONE)?));
            self.expect_kw("NEXT")?;
            next = Some(Box::new(self.parse_expr(prec::NONE)?));
        } else if self.is_kw("NEXT") {
            self.bump();
            next = Some(Box::new(self.parse_expr(prec::NONE)?));
        }
        Ok(MViewRefreshClause {
            method: MViewRefreshMethod::Fast,
            start_with,
            next,
        })
    }

    fn parse_materialized_view_query(&mut self) -> PResult<(QueryStmt, bool)> {
        if self.is_op("(") {
            self.bump();
            let query = if self.is_kw("WITH") {
                self.parse_with_select()?
            } else {
                self.parse_select_or_setopr()?
            };
            self.expect_op(")")?;
            Ok((query, true))
        } else if self.is_kw("WITH") {
            Ok((self.parse_with_select()?, false))
        } else {
            Ok((self.parse_select_or_setopr()?, false))
        }
    }

    fn parse_materialized_view_log_purge(&mut self) -> PResult<MLogPurgeClause> {
        self.expect_kw("PURGE")?;
        if self.is_kw("IMMEDIATE") {
            self.bump();
            return Ok(MLogPurgeClause {
                immediate: true,
                start_with: None,
                next: None,
            });
        }
        let start_with = if self.is_kw("START") {
            self.bump();
            self.expect_kw("WITH")?;
            Some(Box::new(self.parse_expr(prec::NONE)?))
        } else {
            None
        };
        self.expect_kw("NEXT")?;
        let next = Some(Box::new(self.parse_expr(prec::NONE)?));
        Ok(MLogPurgeClause {
            immediate: false,
            start_with,
            next,
        })
    }

    fn parse_materialized_view_log_alert(&mut self) -> PResult<MLogAccumulationAlertClause> {
        self.expect_kw("ALERT")?;
        self.expect_kw("ROWS")?;
        let negative = if self.is_op("-") {
            self.bump();
            true
        } else {
            false
        };
        let token = self.peek().clone();
        if token.kind != TokenKind::IntLit {
            return Err(self.err_here("expected ALERT ROWS integer"));
        }
        self.bump();
        let magnitude = token
            .text
            .parse::<i64>()
            .map_err(|_| self.err_here("ALERT ROWS out of range"))?;
        let rows = if negative { -magnitude } else { magnitude };
        Ok(MLogAccumulationAlertClause { rows })
    }

    fn parse_materialized_view_action(&mut self) -> PResult<AlterMaterializedViewAction> {
        if self.is_kw("COMMENT") {
            self.bump();
            self.accept_optional_equals();
            Ok(AlterMaterializedViewAction::Comment(
                self.parse_string_literal("expected materialized view COMMENT string")?,
            ))
        } else if self.is_kw("REFRESH") {
            self.bump();
            let mut start_with = None;
            let mut next = None;
            if self.is_kw("START") {
                self.bump();
                self.expect_kw("WITH")?;
                start_with = Some(Box::new(self.parse_expr(prec::NONE)?));
            }
            if self.is_kw("NEXT") {
                self.bump();
                next = Some(Box::new(self.parse_expr(prec::NONE)?));
            }
            Ok(AlterMaterializedViewAction::Refresh {
                schedule: Some(MViewRefreshClause {
                    method: MViewRefreshMethod::Fast,
                    start_with,
                    next,
                }),
            })
        } else if self.is_kw("ATTRIBUTES") {
            self.bump();
            self.accept_optional_equals();
            Ok(AlterMaterializedViewAction::Attributes(
                self.parse_string_literal("expected materialized view ATTRIBUTES string")?,
            ))
        } else {
            Err(self.err_here("expected materialized view action"))
        }
    }

    fn parse_materialized_view_log_action(&mut self) -> PResult<AlterMaterializedViewLogAction> {
        if self.is_kw("PURGE") {
            if self.is_kw_at(1, "IMMEDIATE")
                || self.is_kw_at(1, "START")
                || self.is_kw_at(1, "NEXT")
            {
                Ok(AlterMaterializedViewLogAction::Purge(
                    self.parse_materialized_view_log_purge()?,
                ))
            } else {
                self.bump();
                Ok(AlterMaterializedViewLogAction::Purge(MLogPurgeClause {
                    immediate: false,
                    start_with: None,
                    next: None,
                }))
            }
        } else if self.is_kw("ADD") {
            self.bump();
            if self.is_kw("COLUMN") {
                self.bump();
            }
            Ok(AlterMaterializedViewLogAction::AddColumn(
                self.parse_materialized_view_columns()?,
            ))
        } else {
            Err(self.err_here("expected materialized view log action"))
        }
    }
}
