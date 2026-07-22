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

//! `QUERY WATCH` grammar transcreated from `admin_query_parser.go`.

use tidb_ast::{
    AddQueryWatchStmt, DropQueryWatchStmt, QueryWatchOption, QueryWatchRemoveTarget,
    QueryWatchTextOption, ResourceGroupRunawayAction, RunawayWatchType,
};
use tidb_lexer::TokenKind;

use crate::{prec, PResult, Parser};

impl Parser {
    pub(crate) fn parse_query_watch(&mut self) -> PResult<tidb_ast::AdminStmt> {
        self.expect_kw("QUERY")?;
        self.expect_kw("WATCH")?;
        if self.is_kw("ADD") {
            self.bump();
            let mut options = Vec::new();
            while let Some(option) = self.parse_query_watch_option()? {
                if options.iter().any(|existing| same_kind(existing, &option)) {
                    return Err(self.err_here("duplicate QUERY WATCH option"));
                }
                options.push(option);
            }
            Ok(tidb_ast::AdminStmt::AddQueryWatch(Box::new(
                AddQueryWatchStmt { options },
            )))
        } else {
            self.expect_kw("REMOVE")?;
            let target = if self.peek().kind == TokenKind::IntLit {
                let token = self.bump();
                QueryWatchRemoveTarget::Id(
                    token
                        .text
                        .parse::<i64>()
                        .map_err(|_| self.err_here("expected query watch ID"))?,
                )
            } else {
                self.expect_kw("RESOURCE")?;
                self.expect_kw("GROUP")?;
                if self.peek().kind == TokenKind::UserVar {
                    QueryWatchRemoveTarget::ResourceGroupExpr(self.parse_expr(prec::NONE)?)
                } else {
                    QueryWatchRemoveTarget::ResourceGroup(self.parse_name_or_keyword()?)
                }
            };
            Ok(tidb_ast::AdminStmt::DropQueryWatch(Box::new(
                DropQueryWatchStmt { target },
            )))
        }
    }

    fn parse_query_watch_option(&mut self) -> PResult<Option<QueryWatchOption>> {
        if self.is_kw("RESOURCE") {
            self.bump();
            self.expect_kw("GROUP")?;
            return if self.peek().kind == TokenKind::UserVar {
                Ok(Some(QueryWatchOption::ResourceGroupExpr(
                    self.parse_expr(prec::NONE)?,
                )))
            } else {
                Ok(Some(QueryWatchOption::ResourceGroup(
                    self.parse_name_or_keyword()?,
                )))
            };
        }
        if self.is_kw("ACTION") {
            self.bump();
            if self.is_op("=") {
                self.bump();
            }
            let action = if self.is_kw("KILL") {
                self.bump();
                ResourceGroupRunawayAction::Kill
            } else if self.is_kw("COOLDOWN") {
                self.bump();
                ResourceGroupRunawayAction::Cooldown
            } else if self.is_kw("DRYRUN") {
                self.bump();
                ResourceGroupRunawayAction::DryRun
            } else if self.is_kw("SWITCH_GROUP") {
                self.bump();
                self.expect_op("(")?;
                let name = self.parse_name_or_keyword()?;
                self.expect_op(")")?;
                ResourceGroupRunawayAction::SwitchGroup(name)
            } else {
                return Err(self.err_here("expected QUERY WATCH action"));
            };
            return Ok(Some(QueryWatchOption::Action(action)));
        }

        let (watch_type, type_specified) = if self.is_kw("SQL") {
            self.bump();
            if self.is_kw("DIGEST") {
                self.bump();
                (RunawayWatchType::Similar, false)
            } else {
                self.expect_kw("TEXT")?;
                let watch_type = self.parse_query_watch_type()?;
                self.expect_kw("TO")?;
                (watch_type, true)
            }
        } else if self.is_kw("PLAN") {
            self.bump();
            self.expect_kw("DIGEST")?;
            (RunawayWatchType::Plan, false)
        } else {
            return Ok(None);
        };
        Ok(Some(QueryWatchOption::Text(QueryWatchTextOption {
            watch_type,
            pattern: self.parse_expr(prec::NONE)?,
            type_specified,
        })))
    }

    fn parse_query_watch_type(&mut self) -> PResult<RunawayWatchType> {
        for (keyword, value) in [
            ("EXACT", RunawayWatchType::Exact),
            ("SIMILAR", RunawayWatchType::Similar),
            ("PLAN", RunawayWatchType::Plan),
        ] {
            if self.is_kw(keyword) {
                self.bump();
                return Ok(value);
            }
        }
        Err(self.err_here("expected EXACT, SIMILAR, or PLAN"))
    }
}

fn same_kind(left: &QueryWatchOption, right: &QueryWatchOption) -> bool {
    matches!(
        (left, right),
        (
            QueryWatchOption::ResourceGroup(_) | QueryWatchOption::ResourceGroupExpr(_),
            QueryWatchOption::ResourceGroup(_) | QueryWatchOption::ResourceGroupExpr(_)
        ) | (QueryWatchOption::Action(_), QueryWatchOption::Action(_))
            | (QueryWatchOption::Text(_), QueryWatchOption::Text(_))
    )
}
