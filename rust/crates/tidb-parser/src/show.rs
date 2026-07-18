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

//! Ordinary SHOW inspection grammar translated from
//! `pkg/parser/ddl_show_parser.go` and `ddl_show_ident_parser.go`.
//!
//! SQL bindings, privilege/account inspection, and `ADMIN SHOW` control
//! commands stay in their semantic parser modules rather than becoming
//! keyword-shaped exceptions here.

use tidb_ast::{
    AdminStmt, ShowCollationFilter, ShowCollationStmt, ShowColumnsFilter, ShowColumnsStmt,
    ShowCreateKind, ShowDatabasesFilter, ShowDatabasesStmt, ShowErrorsFilter, ShowErrorsStmt,
    ShowIndexFilter, ShowIndexStmt, ShowStatsHistogramsFilter, ShowStatsHistogramsStmt,
    ShowStatsTopNFilter, ShowStatsTopNStmt, ShowStatusFilter, ShowStatusStmt,
    ShowTableNextRowIdStmt, ShowTableStatusFilter, ShowTableStatusStmt, ShowTablesFilter,
    ShowTablesStmt, ShowWarningsFilter, ShowWarningsStmt,
};
use tidb_lexer::TokenKind;

use crate::{decode_string, prec, PResult, Parser};

#[path = "show/character_set.rs"]
mod character_set;
#[path = "show/engines.rs"]
mod engines;
#[path = "show/open_tables.rs"]
mod open_tables;
#[path = "show/stats_buckets.rs"]
mod stats_buckets;
#[path = "show/stats_locked.rs"]
mod stats_locked;

impl Parser {
    /// Parses the currently typed ordinary `SHOW` inspection forms after the
    /// top-level dispatcher has excluded bindings and security-owned forms.
    pub(crate) fn parse_show_inspection(&mut self) -> PResult<AdminStmt> {
        self.expect_kw("SHOW")?;
        if self.is_kw("CREATE") {
            return self.parse_show_create();
        }
        if self.is_kw("MASTER") {
            self.bump();
            self.expect_kw("STATUS")?;
            return Ok(AdminStmt::ShowMasterStatus);
        }
        if self.is_kw("PRIVILEGES") {
            self.bump();
            return Ok(AdminStmt::ShowPrivileges);
        }
        if self.is_kw("BUILTINS") {
            self.bump();
            return Ok(AdminStmt::ShowBuiltins);
        }
        if self.is_kw("VARIABLES")
            || ((self.is_kw("GLOBAL") || self.is_kw("SESSION")) && self.is_kw_at(1, "VARIABLES"))
        {
            return self.parse_show_variables();
        }
        if self.is_kw("STATUS")
            || ((self.is_kw("GLOBAL") || self.is_kw("SESSION")) && self.is_kw_at(1, "STATUS"))
        {
            return Ok(AdminStmt::ShowStatus(Box::new(self.parse_show_status()?)));
        }
        if self.is_kw("WARNINGS") {
            self.bump();
            let filter = if self.is_kw("LIKE") {
                self.bump();
                Some(ShowWarningsFilter::Like(self.parse_expr(prec::UNARY)?))
            } else if self.is_kw("WHERE") {
                self.bump();
                Some(ShowWarningsFilter::Where(self.parse_expr(prec::NONE)?))
            } else {
                None
            };
            return Ok(AdminStmt::ShowWarnings(Box::new(ShowWarningsStmt {
                filter,
            })));
        }
        if self.is_kw("ERRORS") {
            return Ok(AdminStmt::ShowErrors(Box::new(
                self.parse_show_errors(false)?,
            )));
        }
        if self.is_kw("COUNT") {
            return Ok(AdminStmt::ShowErrors(Box::new(
                self.parse_show_errors(true)?,
            )));
        }
        if self.is_kw("COLLATION") {
            self.bump();
            let filter = if self.is_kw("LIKE") {
                self.bump();
                Some(ShowCollationFilter::Like(self.parse_expr(prec::UNARY)?))
            } else if self.is_kw("WHERE") {
                self.bump();
                Some(ShowCollationFilter::Where(self.parse_expr(prec::NONE)?))
            } else {
                None
            };
            return Ok(AdminStmt::ShowCollation(Box::new(ShowCollationStmt {
                filter,
            })));
        }
        if let Some(show) = character_set::parse(self)? {
            return Ok(AdminStmt::ShowCharset(Box::new(show)));
        }
        if let Some(show) = engines::parse(self)? {
            return Ok(AdminStmt::ShowEngines(Box::new(show)));
        }
        if let Some(show) = stats_locked::parse(self)? {
            return Ok(AdminStmt::ShowStatsLocked(Box::new(show)));
        }
        if let Some(show) = stats_buckets::parse(self)? {
            return Ok(AdminStmt::ShowStatsBuckets(Box::new(show)));
        }
        if self.is_kw("STATS_HISTOGRAMS") {
            return Ok(AdminStmt::ShowStatsHistograms(Box::new(
                self.parse_show_stats_histograms()?,
            )));
        }
        if self.is_kw("STATS_TOPN") {
            return Ok(AdminStmt::ShowStatsTopN(Box::new(
                self.parse_show_stats_topn()?,
            )));
        }
        if self.is_kw("DATABASES") {
            return Ok(AdminStmt::ShowDatabases(Box::new(
                self.parse_show_databases()?,
            )));
        }
        if self.is_kw("TABLES") || (self.is_kw("FULL") && self.is_kw_at(1, "TABLES")) {
            return Ok(AdminStmt::ShowTables(Box::new(self.parse_show_tables()?)));
        }
        if let Some(show) = open_tables::parse(self)? {
            return Ok(AdminStmt::ShowOpenTables(Box::new(show)));
        }
        if self.is_kw("TABLE") && self.is_kw_at(1, "STATUS") {
            return Ok(AdminStmt::ShowTableStatus(Box::new(
                self.parse_show_table_status()?,
            )));
        }
        if self.is_kw("TABLE") {
            return Ok(AdminStmt::ShowTableNextRowId(Box::new(
                self.parse_show_table_next_row_id()?,
            )));
        }
        if self.is_kw("COLUMNS") || self.is_kw("FIELDS") {
            return Ok(AdminStmt::ShowColumns(Box::new(self.parse_show_columns()?)));
        }
        if self.is_kw("INDEX") || self.is_kw("INDEXES") || self.is_kw("KEYS") {
            return Ok(AdminStmt::ShowIndex(Box::new(self.parse_show_index()?)));
        }
        Err(self.err_here("unsupported SHOW statement"))
    }

    fn parse_show_create(&mut self) -> PResult<AdminStmt> {
        self.expect_kw("CREATE")?;
        let kind = if self.is_kw("TABLE") {
            ShowCreateKind::Table
        } else if self.is_kw("VIEW") {
            ShowCreateKind::View
        } else if self.is_kw("SEQUENCE") {
            ShowCreateKind::Sequence
        } else if self.is_kw("DATABASE") || self.is_kw("SCHEMA") {
            ShowCreateKind::Database
        } else {
            return Err(self.err_here("unsupported SHOW CREATE object"));
        };
        self.bump();
        // `IF NOT EXISTS` appears only on `SHOW CREATE DATABASE`.
        let if_not_exists = if self.is_kw("IF") {
            self.bump();
            self.expect_kw("NOT")?;
            self.expect_kw("EXISTS")?;
            true
        } else {
            false
        };
        Ok(AdminStmt::ShowCreate {
            kind,
            if_not_exists,
            name: self.parse_name_path()?,
        })
    }

    fn parse_show_variables(&mut self) -> PResult<AdminStmt> {
        let global = if self.is_kw("GLOBAL") {
            self.bump();
            true
        } else {
            if self.is_kw("SESSION") {
                self.bump();
            }
            false
        };
        self.expect_kw("VARIABLES")?;
        let like = if self.is_kw("LIKE") {
            self.bump();
            let token = self.peek().clone();
            if token.kind != TokenKind::Str {
                return Err(self.err_here("expected a string pattern after LIKE"));
            }
            self.bump();
            Some(decode_string(&token.text))
        } else {
            None
        };
        let where_clause = if like.is_none() && self.is_kw("WHERE") {
            self.bump();
            Some(self.parse_expr(prec::NONE)?)
        } else {
            None
        };
        Ok(AdminStmt::ShowVariables {
            global,
            like,
            where_clause,
        })
    }

    /// Parses Go's unscoped and scoped `SHOW STATUS` forms. Go restores the
    /// default session scope explicitly, unlike source SQL which may omit it.
    fn parse_show_status(&mut self) -> PResult<ShowStatusStmt> {
        let global = if self.is_kw("GLOBAL") {
            self.bump();
            true
        } else {
            if self.is_kw("SESSION") {
                self.bump();
            }
            false
        };
        self.expect_kw("STATUS")?;
        let filter = if self.is_kw("LIKE") {
            self.bump();
            Some(ShowStatusFilter::Like(self.parse_expr(prec::UNARY)?))
        } else if self.is_kw("WHERE") {
            self.bump();
            Some(ShowStatusFilter::Where(self.parse_expr(prec::NONE)?))
        } else {
            None
        };
        Ok(ShowStatusStmt { global, filter })
    }

    /// Parses Go's `SHOW ERRORS` branch plus its count-only spelling. Go's
    /// restore canonicalizes `SHOW COUNT(*) ERRORS` to `SHOW ERRORS`, while
    /// retaining the count flag for executor result shape.
    fn parse_show_errors(&mut self, count_only: bool) -> PResult<ShowErrorsStmt> {
        if count_only {
            self.expect_kw("COUNT")?;
            self.expect_op("(")?;
            self.expect_op("*")?;
            self.expect_op(")")?;
        }
        self.expect_kw("ERRORS")?;
        let filter = if self.is_kw("LIKE") {
            self.bump();
            Some(ShowErrorsFilter::Like(self.parse_expr(prec::UNARY)?))
        } else if self.is_kw("WHERE") {
            self.bump();
            Some(ShowErrorsFilter::Where(self.parse_expr(prec::NONE)?))
        } else {
            None
        };
        Ok(ShowErrorsStmt { count_only, filter })
    }

    fn parse_show_index(&mut self) -> PResult<ShowIndexStmt> {
        if !(self.is_kw("INDEX") || self.is_kw("INDEXES") || self.is_kw("KEYS")) {
            return Err(self.err_here("expected INDEX, INDEXES, or KEYS"));
        }
        self.bump();
        if self.is_kw("FROM") || self.is_kw("IN") {
            self.bump();
        } else {
            return Err(self.err_here("expected FROM or IN"));
        }
        let table = self.parse_name_path()?;
        let filter = if self.is_kw("LIKE") {
            self.bump();
            Some(ShowIndexFilter::Like(self.parse_expr(prec::UNARY)?))
        } else if self.is_kw("WHERE") {
            self.bump();
            Some(ShowIndexFilter::Where(self.parse_expr(prec::NONE)?))
        } else {
            None
        };
        Ok(ShowIndexStmt { table, filter })
    }

    /// Parses Go's `SHOW [FULL] TABLES [FROM|IN db] [LIKE|WHERE ...]` leaf.
    fn parse_show_tables(&mut self) -> PResult<ShowTablesStmt> {
        let full = if self.is_kw("FULL") {
            self.bump();
            true
        } else {
            false
        };
        self.expect_kw("TABLES")?;
        let database = if self.is_kw("FROM") || self.is_kw("IN") {
            self.bump();
            Some(self.parse_name()?)
        } else {
            None
        };
        let filter = if self.is_kw("LIKE") {
            self.bump();
            Some(ShowTablesFilter::Like(self.parse_expr(prec::UNARY)?))
        } else if self.is_kw("WHERE") {
            self.bump();
            Some(ShowTablesFilter::Where(self.parse_expr(prec::NONE)?))
        } else {
            None
        };
        Ok(ShowTablesStmt {
            full,
            database,
            filter,
        })
    }

    /// Direct Go `parseShowTable` STATUS branch. Unlike `SHOW TABLES`, it
    /// accepts an optional database and the shared LIKE/WHERE filter grammar.
    fn parse_show_table_status(&mut self) -> PResult<ShowTableStatusStmt> {
        self.expect_kw("TABLE")?;
        self.expect_kw("STATUS")?;
        let database = if self.is_kw("FROM") || self.is_kw("IN") {
            self.bump();
            Some(self.parse_name()?)
        } else {
            None
        };
        let filter = if self.is_kw("LIKE") {
            self.bump();
            Some(ShowTableStatusFilter::Like(self.parse_expr(prec::UNARY)?))
        } else if self.is_kw("WHERE") {
            self.bump();
            Some(ShowTableStatusFilter::Where(self.parse_expr(prec::NONE)?))
        } else {
            None
        };
        Ok(ShowTableStatusStmt { database, filter })
    }

    /// Direct Go `parseShowTable` NEXT_ROW_ID branch. The singular `TABLE`
    /// form admits only a table path followed immediately by NEXT_ROW_ID.
    fn parse_show_table_next_row_id(&mut self) -> PResult<ShowTableNextRowIdStmt> {
        self.expect_kw("TABLE")?;
        let table = self.parse_name_path()?;
        self.expect_kw("NEXT_ROW_ID")?;
        Ok(ShowTableNextRowIdStmt { table })
    }

    fn parse_show_databases(&mut self) -> PResult<ShowDatabasesStmt> {
        self.expect_kw("DATABASES")?;
        let filter = if self.is_kw("LIKE") {
            self.bump();
            Some(ShowDatabasesFilter::Like(self.parse_expr(prec::UNARY)?))
        } else if self.is_kw("WHERE") {
            self.bump();
            Some(ShowDatabasesFilter::Where(self.parse_expr(prec::NONE)?))
        } else {
            None
        };
        Ok(ShowDatabasesStmt { filter })
    }

    fn parse_show_stats_histograms(&mut self) -> PResult<ShowStatsHistogramsStmt> {
        self.expect_kw("STATS_HISTOGRAMS")?;
        let filter = if self.is_kw("LIKE") {
            self.bump();
            Some(ShowStatsHistogramsFilter::Like(
                self.parse_expr(prec::UNARY)?,
            ))
        } else if self.is_kw("WHERE") {
            self.bump();
            Some(ShowStatsHistogramsFilter::Where(
                self.parse_expr(prec::NONE)?,
            ))
        } else {
            None
        };
        Ok(ShowStatsHistogramsStmt { filter })
    }

    /// Parses Go's source-owned `SHOW STATS_TOPN` entry. Its table-driven Go
    /// dispatcher calls the shared LIKE/WHERE helper for this leaf only.
    fn parse_show_stats_topn(&mut self) -> PResult<ShowStatsTopNStmt> {
        self.expect_kw("STATS_TOPN")?;
        let filter = if self.is_kw("LIKE") {
            self.bump();
            Some(ShowStatsTopNFilter::Like(self.parse_expr(prec::UNARY)?))
        } else if self.is_kw("WHERE") {
            self.bump();
            Some(ShowStatsTopNFilter::Where(self.parse_expr(prec::NONE)?))
        } else {
            None
        };
        Ok(ShowStatsTopNStmt { filter })
    }

    fn parse_show_columns(&mut self) -> PResult<ShowColumnsStmt> {
        if !(self.is_kw("COLUMNS") || self.is_kw("FIELDS")) {
            return Err(self.err_here("expected COLUMNS or FIELDS"));
        }
        self.bump();
        if self.is_kw("FROM") || self.is_kw("IN") {
            self.bump();
        } else {
            return Err(self.err_here("expected FROM or IN"));
        }
        let table = self.parse_name_path()?;
        let filter = if self.is_kw("LIKE") {
            self.bump();
            Some(ShowColumnsFilter::Like(self.parse_expr(prec::UNARY)?))
        } else if self.is_kw("WHERE") {
            self.bump();
            Some(ShowColumnsFilter::Where(self.parse_expr(prec::NONE)?))
        } else {
            None
        };
        Ok(ShowColumnsStmt { table, filter })
    }
}
