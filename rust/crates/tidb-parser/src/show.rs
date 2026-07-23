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
    AdminStmt, ShowCharsetFilter, ShowCharsetStmt, ShowCollationFilter, ShowCollationStmt,
    ShowColumnsFilter, ShowColumnsStmt, ShowCreateKind, ShowDatabasesFilter, ShowDatabasesStmt,
    ShowDistributionJobsStmt, ShowEnginesFilter, ShowEnginesStmt, ShowErrorsFilter, ShowErrorsStmt,
    ShowImportGroupsStmt, ShowImportJobsStmt, ShowIndexFilter, ShowIndexStmt, ShowInspectionFilter,
    ShowInspectionKind, ShowInspectionStmt, ShowMaskingPoliciesStmt, ShowOpenTablesStmt,
    ShowPlacementStmt, ShowPlacementTarget, ShowProfileStmt, ShowProfileType,
    ShowStatsBucketsFilter, ShowStatsBucketsStmt, ShowStatsHistogramsFilter,
    ShowStatsHistogramsStmt, ShowStatsLockedFilter, ShowStatsLockedStmt, ShowStatsTopNFilter,
    ShowStatsTopNStmt, ShowStatusFilter, ShowStatusStmt, ShowTableNextRowIdStmt,
    ShowTablePlacementKind, ShowTablePlacementStmt, ShowTableStatusFilter, ShowTableStatusStmt,
    ShowTablesFilter, ShowTablesStmt, ShowVariablesStmt, ShowWarningsFilter, ShowWarningsStmt,
};
use tidb_lexer::TokenKind;

use crate::{decode_string, prec, PResult, Parser};

impl Parser {
    /// Parses the currently typed ordinary `SHOW` inspection forms after the
    /// top-level dispatcher has excluded bindings and security-owned forms.
    pub(crate) fn parse_show_inspection(&mut self) -> PResult<AdminStmt> {
        self.expect_kw("SHOW")?;
        if self.is_kw("RAW") || self.is_kw("IMPORT") {
            return self.parse_show_import();
        }
        if self.is_kw("CREATE") {
            return self.parse_show_create();
        }
        if self.is_kw("DISTRIBUTION") {
            return self.parse_show_distribution_jobs();
        }
        if self.is_kw("PLACEMENT") {
            return self.parse_show_placement();
        }
        if self.is_kw("PROFILE") {
            return self.parse_show_profile();
        }
        if self.is_kw("MASKING") {
            return self.parse_show_masking_policies();
        }
        if self.is_kw("MASTER") {
            self.bump();
            self.expect_keyword_or_ident("STATUS")?;
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
        if let Some(show) = self.parse_common_show_inspection()? {
            return Ok(AdminStmt::ShowInspection(Box::new(show)));
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
                count_only: false,
                filter,
            })));
        }
        if self.is_kw("ERRORS") {
            return Ok(AdminStmt::ShowErrors(Box::new(
                self.parse_show_errors(false)?,
            )));
        }
        if self.is_kw("COUNT") {
            return self.parse_show_count_warnings_or_errors();
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
        if let Some(show) = self.parse_show_charset()? {
            return Ok(AdminStmt::ShowCharset(Box::new(show)));
        }
        if let Some(show) = self.parse_show_engines()? {
            return Ok(AdminStmt::ShowEngines(Box::new(show)));
        }
        if let Some(show) = self.parse_show_stats_locked()? {
            return Ok(AdminStmt::ShowStatsLocked(Box::new(show)));
        }
        if let Some(show) = self.parse_show_stats_buckets()? {
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
        if let Some(show) = self.parse_show_open_tables()? {
            return Ok(AdminStmt::ShowOpenTables(Box::new(show)));
        }
        if self.is_kw("TABLE") && self.keyword_or_ident_is_at(1, "STATUS") {
            return Ok(AdminStmt::ShowTableStatus(Box::new(
                self.parse_show_table_status()?,
            )));
        }
        if self.is_kw("TABLE") {
            return self.parse_show_table_inspection();
        }
        if self.is_kw("COLUMNS")
            || self.is_kw("FIELDS")
            || self.is_kw("EXTENDED")
            || (self.is_kw("FULL") && (self.is_kw_at(1, "COLUMNS") || self.is_kw_at(1, "FIELDS")))
        {
            return Ok(AdminStmt::ShowColumns(Box::new(self.parse_show_columns()?)));
        }
        if self.is_kw("INDEX") || self.is_kw("INDEXES") || self.is_kw("KEYS") {
            return Ok(AdminStmt::ShowIndex(Box::new(self.parse_show_index()?)));
        }
        if let Some(statement) = self.parse_show_ident_based_fallback()? {
            return Ok(statement);
        }
        Err(self.err_here("unsupported SHOW statement"))
    }

    /// Direct translation of `parseShowIdentBased`, reached only after the
    /// dedicated SHOW token arms declined the current token. Go dispatches on
    /// the decoded `isIdentLike` literal, so quoted and single-@ spellings are
    /// intentionally valid here.
    fn parse_show_ident_based_fallback(&mut self) -> PResult<Option<AdminStmt>> {
        if !crate::is_ident_like_name(self.peek()) {
            return Ok(None);
        }
        let head = crate::token_literal_text(self.peek()).to_ascii_uppercase();

        let generic = match head.as_str() {
            "TRIGGERS" => Some((ShowInspectionKind::Triggers, true, true)),
            "EVENTS" => Some((ShowInspectionKind::Events, true, true)),
            "PLUGINS" => Some((ShowInspectionKind::Plugins, false, true)),
            "STATS_EXTENDED" => Some((ShowInspectionKind::StatsExtended, false, true)),
            "STATS_META" => Some((ShowInspectionKind::StatsMeta, false, true)),
            "STATS_HEALTHY" => Some((ShowInspectionKind::StatsHealthy, false, true)),
            "HISTOGRAMS_IN_FLIGHT" => Some((ShowInspectionKind::HistogramsInFlight, false, true)),
            "COLUMN_STATS_USAGE" => Some((ShowInspectionKind::ColumnStatsUsage, false, true)),
            "BACKUPS" => Some((ShowInspectionKind::Backups, false, true)),
            "RESTORES" => Some((ShowInspectionKind::Restores, false, true)),
            "IMPORTS" => Some((ShowInspectionKind::Imports, false, true)),
            "CONFIG" => Some((ShowInspectionKind::Config, false, true)),
            "PROFILES" => Some((ShowInspectionKind::Profiles, false, false)),
            "SESSION_STATES" => Some((ShowInspectionKind::SessionStates, false, false)),
            "AFFINITY" => Some((ShowInspectionKind::Affinity, false, true)),
            _ => None,
        };
        if let Some((kind, with_database, with_filter)) = generic {
            self.bump();
            let database = if with_database {
                self.parse_show_database_name_opt()
            } else {
                None
            };
            let filter = if with_filter {
                self.parse_inspection_filter()?
            } else {
                None
            };
            return Ok(Some(AdminStmt::ShowInspection(Box::new(
                ShowInspectionStmt {
                    kind,
                    full: false,
                    database,
                    filter,
                },
            ))));
        }

        match head.as_str() {
            "DATABASES" => {
                self.bump();
                let filter = match self.parse_inspection_filter()? {
                    Some(ShowInspectionFilter::Like(expr)) => Some(ShowDatabasesFilter::Like(expr)),
                    Some(ShowInspectionFilter::Where(expr)) => {
                        Some(ShowDatabasesFilter::Where(expr))
                    }
                    None => None,
                };
                Ok(Some(AdminStmt::ShowDatabases(Box::new(
                    ShowDatabasesStmt { filter },
                ))))
            }
            "ENGINES" => {
                self.bump();
                let filter = match self.parse_inspection_filter()? {
                    Some(ShowInspectionFilter::Like(expr)) => Some(ShowEnginesFilter::Like(expr)),
                    Some(ShowInspectionFilter::Where(expr)) => Some(ShowEnginesFilter::Where(expr)),
                    None => None,
                };
                Ok(Some(AdminStmt::ShowEngines(Box::new(ShowEnginesStmt {
                    filter,
                }))))
            }
            "COLLATION" => {
                self.bump();
                let filter = match self.parse_inspection_filter()? {
                    Some(ShowInspectionFilter::Like(expr)) => Some(ShowCollationFilter::Like(expr)),
                    Some(ShowInspectionFilter::Where(expr)) => {
                        Some(ShowCollationFilter::Where(expr))
                    }
                    None => None,
                };
                Ok(Some(AdminStmt::ShowCollation(Box::new(
                    ShowCollationStmt { filter },
                ))))
            }
            "ERRORS" => {
                self.bump();
                let filter = match self.parse_inspection_filter()? {
                    Some(ShowInspectionFilter::Like(expr)) => Some(ShowErrorsFilter::Like(expr)),
                    Some(ShowInspectionFilter::Where(expr)) => Some(ShowErrorsFilter::Where(expr)),
                    None => None,
                };
                Ok(Some(AdminStmt::ShowErrors(Box::new(ShowErrorsStmt {
                    count_only: false,
                    filter,
                }))))
            }
            "CHARSET" => {
                self.bump();
                let filter = match self.parse_inspection_filter()? {
                    Some(ShowInspectionFilter::Like(expr)) => Some(ShowCharsetFilter::Like(expr)),
                    Some(ShowInspectionFilter::Where(expr)) => Some(ShowCharsetFilter::Where(expr)),
                    None => None,
                };
                Ok(Some(AdminStmt::ShowCharset(Box::new(ShowCharsetStmt {
                    filter,
                }))))
            }
            "PRIVILEGES" => {
                self.bump();
                Ok(Some(AdminStmt::ShowPrivileges))
            }
            "BUILTINS" => {
                self.bump();
                Ok(Some(AdminStmt::ShowBuiltins))
            }
            "STATS_LOCKED" => {
                self.bump();
                let filter = match self.parse_inspection_filter()? {
                    Some(ShowInspectionFilter::Like(expr)) => {
                        Some(ShowStatsLockedFilter::Like(expr))
                    }
                    Some(ShowInspectionFilter::Where(expr)) => {
                        Some(ShowStatsLockedFilter::Where(expr))
                    }
                    None => None,
                };
                Ok(Some(AdminStmt::ShowStatsLocked(Box::new(
                    ShowStatsLockedStmt { filter },
                ))))
            }
            "STATS_BUCKETS" => {
                self.bump();
                let filter = match self.parse_inspection_filter()? {
                    Some(ShowInspectionFilter::Like(expr)) => {
                        Some(ShowStatsBucketsFilter::Like(expr))
                    }
                    Some(ShowInspectionFilter::Where(expr)) => {
                        Some(ShowStatsBucketsFilter::Where(expr))
                    }
                    None => None,
                };
                Ok(Some(AdminStmt::ShowStatsBuckets(Box::new(
                    ShowStatsBucketsStmt { filter },
                ))))
            }
            "STATS_HISTOGRAMS" => {
                self.bump();
                let filter = match self.parse_inspection_filter()? {
                    Some(ShowInspectionFilter::Like(expr)) => {
                        Some(ShowStatsHistogramsFilter::Like(expr))
                    }
                    Some(ShowInspectionFilter::Where(expr)) => {
                        Some(ShowStatsHistogramsFilter::Where(expr))
                    }
                    None => None,
                };
                Ok(Some(AdminStmt::ShowStatsHistograms(Box::new(
                    ShowStatsHistogramsStmt { filter },
                ))))
            }
            "STATS_TOPN" => {
                self.bump();
                let filter = match self.parse_inspection_filter()? {
                    Some(ShowInspectionFilter::Like(expr)) => Some(ShowStatsTopNFilter::Like(expr)),
                    Some(ShowInspectionFilter::Where(expr)) => {
                        Some(ShowStatsTopNFilter::Where(expr))
                    }
                    None => None,
                };
                Ok(Some(AdminStmt::ShowStatsTopN(Box::new(
                    ShowStatsTopNStmt { filter },
                ))))
            }
            "OPEN" => {
                self.bump();
                self.expect_kw("TABLES")?;
                let database = self.parse_show_database_name_opt();
                let filter = self.parse_inspection_filter()?;
                Ok(Some(AdminStmt::ShowOpenTables(Box::new(
                    ShowOpenTablesStmt { database, filter },
                ))))
            }
            "TABLE" if self.keyword_or_ident_is_at(1, "STATUS") => {
                self.bump();
                self.bump();
                let database = self.parse_show_database_name_opt();
                let filter = match self.parse_inspection_filter()? {
                    Some(ShowInspectionFilter::Like(expr)) => {
                        Some(ShowTableStatusFilter::Like(expr))
                    }
                    Some(ShowInspectionFilter::Where(expr)) => {
                        Some(ShowTableStatusFilter::Where(expr))
                    }
                    None => None,
                };
                Ok(Some(AdminStmt::ShowTableStatus(Box::new(
                    ShowTableStatusStmt { database, filter },
                ))))
            }
            "PLACEMENT" => {
                self.bump();
                let target = if self.token_literal_is_at(0, "LABELS") {
                    self.bump();
                    ShowPlacementTarget::Labels
                } else {
                    ShowPlacementTarget::All
                };
                let filter = self.parse_inspection_filter()?;
                Ok(Some(AdminStmt::ShowPlacement(Box::new(
                    ShowPlacementStmt { target, filter },
                ))))
            }
            "BINDING_CACHE" if self.keyword_or_ident_is_at(1, "STATUS") => {
                self.bump();
                self.bump();
                Ok(Some(AdminStmt::ShowInspection(Box::new(
                    ShowInspectionStmt {
                        kind: ShowInspectionKind::BindingCacheStatus,
                        full: false,
                        database: None,
                        filter: None,
                    },
                ))))
            }
            "PROFILE" => {
                self.bump();
                Ok(Some(self.parse_show_profile_tail()?))
            }
            "EXTENDED" => {
                self.bump();
                let full = if self.keyword_or_ident_is_at(0, "FULL") {
                    self.bump();
                    true
                } else {
                    false
                };
                if !(self.keyword_or_ident_is_at(0, "COLUMNS")
                    || self.keyword_or_ident_is_at(0, "FIELDS"))
                {
                    return Ok(None);
                }
                // The tail's ordinary keyword check is bypassed only for the
                // source's literal-driven COLUMNS/FIELDS word.
                let column_word = self.bump();
                let mut statement = self.parse_show_columns_after_head(full, true)?;
                statement.extended = true;
                let _ = column_word;
                Ok(Some(AdminStmt::ShowColumns(Box::new(statement))))
            }
            "SLAVE" if self.keyword_or_ident_is_at(1, "STATUS") => {
                self.bump();
                self.bump();
                Ok(Some(AdminStmt::ShowInspection(Box::new(
                    ShowInspectionStmt {
                        kind: ShowInspectionKind::ReplicaStatus,
                        full: false,
                        database: None,
                        filter: None,
                    },
                ))))
            }
            _ => Ok(None),
        }
    }

    fn parse_show_charset(&mut self) -> PResult<Option<ShowCharsetStmt>> {
        if self.is_kw("CHARSET") {
            self.bump();
        } else if self.is_kw("CHARACTER") || self.is_kw("CHAR") {
            self.bump();
            self.expect_kw("SET")?;
        } else {
            return Ok(None);
        }
        let filter = if self.is_kw("LIKE") {
            self.bump();
            Some(ShowCharsetFilter::Like(self.parse_expr(prec::UNARY)?))
        } else if self.is_kw("WHERE") {
            self.bump();
            Some(ShowCharsetFilter::Where(self.parse_expr(prec::NONE)?))
        } else {
            None
        };
        Ok(Some(ShowCharsetStmt { filter }))
    }

    fn parse_show_engines(&mut self) -> PResult<Option<ShowEnginesStmt>> {
        if !self.is_kw("ENGINES") {
            return Ok(None);
        }
        self.bump();
        let filter = if self.is_kw("LIKE") {
            self.bump();
            Some(ShowEnginesFilter::Like(self.parse_expr(prec::UNARY)?))
        } else if self.is_kw("WHERE") {
            self.bump();
            Some(ShowEnginesFilter::Where(self.parse_expr(prec::NONE)?))
        } else {
            None
        };
        Ok(Some(ShowEnginesStmt { filter }))
    }

    fn parse_show_open_tables(&mut self) -> PResult<Option<ShowOpenTablesStmt>> {
        if !self.is_kw("OPEN") {
            return Ok(None);
        }
        self.bump();
        self.expect_kw("TABLES")?;
        let database = self.parse_show_database_name_opt();
        let filter = self.parse_inspection_filter()?;
        Ok(Some(ShowOpenTablesStmt { database, filter }))
    }

    fn parse_show_stats_buckets(&mut self) -> PResult<Option<ShowStatsBucketsStmt>> {
        if !self.peek().text.eq_ignore_ascii_case("STATS_BUCKETS") {
            return Ok(None);
        }
        self.bump();
        let filter = if self.is_kw("LIKE") {
            self.bump();
            Some(ShowStatsBucketsFilter::Like(self.parse_expr(prec::UNARY)?))
        } else if self.is_kw("WHERE") {
            self.bump();
            Some(ShowStatsBucketsFilter::Where(self.parse_expr(prec::NONE)?))
        } else {
            None
        };
        Ok(Some(ShowStatsBucketsStmt { filter }))
    }

    fn parse_show_stats_locked(&mut self) -> PResult<Option<ShowStatsLockedStmt>> {
        if !self.peek().text.eq_ignore_ascii_case("STATS_LOCKED") {
            return Ok(None);
        }
        self.bump();
        let filter = if self.is_kw("LIKE") {
            self.bump();
            Some(ShowStatsLockedFilter::Like(self.parse_expr(prec::UNARY)?))
        } else if self.is_kw("WHERE") {
            self.bump();
            Some(ShowStatsLockedFilter::Where(self.parse_expr(prec::NONE)?))
        } else {
            None
        };
        Ok(Some(ShowStatsLockedStmt { filter }))
    }

    fn parse_common_show_inspection(&mut self) -> PResult<Option<ShowInspectionStmt>> {
        let mut full = false;
        let (kind, words, allow_database, allow_filter) = if self.is_kw("TRIGGERS") {
            (ShowInspectionKind::Triggers, 1, true, true)
        } else if self.is_kw("PROCEDURE") && self.keyword_or_ident_is_at(1, "STATUS") {
            (ShowInspectionKind::ProcedureStatus, 2, false, true)
        } else if self.is_kw("FUNCTION") && self.keyword_or_ident_is_at(1, "STATUS") {
            (ShowInspectionKind::FunctionStatus, 2, false, true)
        } else if self.is_kw("EVENTS") {
            (ShowInspectionKind::Events, 1, true, true)
        } else if self.is_kw("PLUGINS") {
            (ShowInspectionKind::Plugins, 1, false, true)
        } else if self.is_kw("STATS_EXTENDED") {
            (ShowInspectionKind::StatsExtended, 1, false, true)
        } else if self.is_kw("STATS_META") {
            (ShowInspectionKind::StatsMeta, 1, false, true)
        } else if self.is_kw("STATS_HEALTHY") {
            (ShowInspectionKind::StatsHealthy, 1, false, true)
        } else if self.is_kw("HISTOGRAMS_IN_FLIGHT") {
            (ShowInspectionKind::HistogramsInFlight, 1, false, true)
        } else if self.is_kw("COLUMN_STATS_USAGE") {
            (ShowInspectionKind::ColumnStatsUsage, 1, false, true)
        } else if self.is_kw("BINDING_CACHE") && self.keyword_or_ident_is_at(1, "STATUS") {
            (ShowInspectionKind::BindingCacheStatus, 2, false, false)
        } else if self.is_kw("ANALYZE") && self.keyword_or_ident_is_at(1, "STATUS") {
            (ShowInspectionKind::AnalyzeStatus, 2, false, true)
        } else if self.is_kw("BACKUPS") {
            (ShowInspectionKind::Backups, 1, false, true)
        } else if self.is_kw("RESTORES") {
            (ShowInspectionKind::Restores, 1, false, true)
        } else if self.is_kw("IMPORTS") {
            (ShowInspectionKind::Imports, 1, false, true)
        } else if self.is_kw("CONFIG") {
            (ShowInspectionKind::Config, 1, false, true)
        } else if (self.is_kw("REPLICA") || self.is_kw("SLAVE"))
            && self.keyword_or_ident_is_at(1, "STATUS")
        {
            (ShowInspectionKind::ReplicaStatus, 2, false, false)
        } else if self.is_kw("BINARY")
            && self.token_literal_is_at(1, "LOG")
            && self.keyword_or_ident_is_at(2, "STATUS")
        {
            (ShowInspectionKind::BinaryLogStatus, 3, false, false)
        } else if self.is_kw("PROFILES") {
            (ShowInspectionKind::Profiles, 1, false, false)
        } else if self.is_kw("SESSION_STATES") {
            (ShowInspectionKind::SessionStates, 1, false, false)
        } else if self.is_kw("PROCESSLIST") {
            (ShowInspectionKind::ProcessList, 1, false, false)
        } else if self.is_kw("FULL") && self.is_kw_at(1, "PROCESSLIST") {
            full = true;
            (ShowInspectionKind::ProcessList, 2, false, false)
        } else if self.is_kw("AFFINITY") {
            (ShowInspectionKind::Affinity, 1, false, true)
        } else {
            return Ok(None);
        };
        for _ in 0..words {
            self.bump();
        }
        let database = if allow_database {
            self.parse_show_database_name_opt()
        } else {
            None
        };
        let filter = if allow_filter && self.is_kw("LIKE") {
            self.bump();
            Some(ShowInspectionFilter::Like(self.parse_expr(prec::UNARY)?))
        } else if allow_filter && self.is_kw("WHERE") {
            self.bump();
            Some(ShowInspectionFilter::Where(self.parse_expr(prec::NONE)?))
        } else {
            None
        };
        Ok(Some(ShowInspectionStmt {
            kind,
            full,
            database,
            filter,
        }))
    }

    fn parse_show_import(&mut self) -> PResult<AdminStmt> {
        let raw = if self.is_kw("RAW") {
            self.bump();
            true
        } else {
            false
        };
        self.expect_kw("IMPORT")?;
        if self.is_kw("JOB") || self.is_kw("JOBS") {
            let singular = self.is_kw("JOB");
            self.bump();
            let job_id = if singular {
                let token = self.bump();
                if token.kind != TokenKind::IntLit {
                    return Err(self.err_here("expected import job ID"));
                }
                Some(
                    token
                        .text
                        .parse::<i64>()
                        .map_err(|_| self.err_here("expected import job ID"))?,
                )
            } else {
                None
            };
            let where_clause = if !singular && self.is_kw("WHERE") {
                self.bump();
                Some(self.parse_expr(prec::NONE)?)
            } else {
                None
            };
            return Ok(AdminStmt::ShowImportJobs(Box::new(ShowImportJobsStmt {
                raw,
                job_id,
                where_clause,
            })));
        }
        if raw {
            return Err(self.err_here("RAW is valid only for SHOW IMPORT JOB(S)"));
        }
        if self.is_kw("GROUP") || self.is_kw("GROUPS") {
            let singular = self.is_kw("GROUP");
            self.bump();
            let group_key = if singular {
                let token = self.bump();
                if token.kind != TokenKind::Str {
                    return Err(self.err_here("expected import group key"));
                }
                Some(decode_string(&token.text))
            } else {
                None
            };
            let where_clause = if self.is_kw("WHERE") {
                self.bump();
                Some(self.parse_expr(prec::NONE)?)
            } else {
                None
            };
            return Ok(AdminStmt::ShowImportGroups(Box::new(
                ShowImportGroupsStmt {
                    group_key,
                    where_clause,
                },
            )));
        }
        Err(self.err_here("expected IMPORT JOB(S) or GROUP(S)"))
    }

    fn parse_show_distribution_jobs(&mut self) -> PResult<AdminStmt> {
        self.expect_kw("DISTRIBUTION")?;
        let singular = if self.is_kw("JOB") {
            true
        } else if self.is_kw("JOBS") {
            false
        } else {
            return Err(self.err_here("expected DISTRIBUTION JOB or JOBS"));
        };
        self.bump();
        let job_id = if singular {
            match self.parse_expr(prec::NONE)? {
                tidb_ast::Expr::Int(value) => value.parse::<i64>().ok(),
                _ => None,
            }
        } else {
            None
        };
        let filter = if job_id.is_none() {
            self.parse_inspection_filter()?
        } else {
            None
        };
        Ok(AdminStmt::ShowDistributionJobs(Box::new(
            ShowDistributionJobsStmt { job_id, filter },
        )))
    }

    fn parse_show_placement(&mut self) -> PResult<AdminStmt> {
        self.expect_kw("PLACEMENT")?;
        self.parse_show_placement_tail()
    }

    fn parse_show_placement_tail(&mut self) -> PResult<AdminStmt> {
        let target = if self.token_literal_is_at(0, "LABELS") {
            self.bump();
            ShowPlacementTarget::Labels
        } else if self.is_kw("FOR") {
            self.bump();
            if self.is_kw("DATABASE") || self.is_kw("SCHEMA") {
                self.bump();
                ShowPlacementTarget::Database(self.parse_non_string_ident_like_name()?)
            } else if self.is_kw("TABLE") {
                self.bump();
                let table = self.parse_table_name()?;
                if self.is_kw("PARTITION") {
                    self.bump();
                    ShowPlacementTarget::Partition {
                        table,
                        partition: self.parse_non_string_ident_like_name()?,
                    }
                } else {
                    ShowPlacementTarget::Table(table)
                }
            } else {
                return Err(self.err_here("expected DATABASE, SCHEMA, or TABLE after FOR"));
            }
        } else {
            ShowPlacementTarget::All
        };
        let filter = if matches!(
            target,
            ShowPlacementTarget::All | ShowPlacementTarget::Labels
        ) {
            self.parse_inspection_filter()?
        } else {
            None
        };
        Ok(AdminStmt::ShowPlacement(Box::new(ShowPlacementStmt {
            target,
            filter,
        })))
    }

    fn parse_show_profile(&mut self) -> PResult<AdminStmt> {
        self.expect_kw("PROFILE")?;
        self.parse_show_profile_tail()
    }

    fn parse_show_profile_tail(&mut self) -> PResult<AdminStmt> {
        let mut types = Vec::new();
        let mut type_required = false;
        loop {
            let kind = if self.is_kw("CPU") {
                Some((ShowProfileType::Cpu, 1))
            } else if self.is_kw("MEMORY") {
                Some((ShowProfileType::Memory, 1))
            } else if self.is_kw("BLOCK") && self.is_kw_at(1, "IO") {
                Some((ShowProfileType::BlockIo, 2))
            } else if self.is_kw("CONTEXT") && self.is_kw_at(1, "SWITCHES") {
                Some((ShowProfileType::ContextSwitches, 2))
            } else if self.is_kw("PAGE") && self.is_kw_at(1, "FAULTS") {
                Some((ShowProfileType::PageFaults, 2))
            } else if self.is_kw("IPC") {
                Some((ShowProfileType::Ipc, 1))
            } else if self.is_kw("SWAPS") {
                Some((ShowProfileType::Swaps, 1))
            } else if self.is_kw("SOURCE") {
                Some((ShowProfileType::Source, 1))
            } else if self.is_kw("ALL") {
                Some((ShowProfileType::All, 1))
            } else {
                None
            };
            let Some((kind, words)) = kind else {
                if type_required {
                    return Err(self.err_here("expected SHOW PROFILE type after comma"));
                }
                break;
            };
            for _ in 0..words {
                self.bump();
            }
            types.push(kind);
            if self.is_op(",") {
                self.bump();
                type_required = true;
            } else {
                break;
            }
        }
        let query_id = if self.is_kw("FOR") {
            self.bump();
            self.expect_kw("QUERY")?;
            let token = self.bump();
            if token.kind != TokenKind::IntLit {
                return Err(self.err_here("expected profile query ID"));
            }
            Some(
                token
                    .text
                    .parse::<i64>()
                    .map_err(|_| self.err_here("expected profile query ID"))?,
            )
        } else {
            None
        };
        let limit = if self.is_kw("LIMIT") {
            self.bump();
            Some(self.parse_limit()?)
        } else {
            None
        };
        Ok(AdminStmt::ShowProfile(Box::new(ShowProfileStmt {
            types,
            query_id,
            limit,
        })))
    }

    fn parse_show_masking_policies(&mut self) -> PResult<AdminStmt> {
        self.expect_kw("MASKING")?;
        self.expect_kw("POLICIES")?;
        self.expect_kw("FOR")?;
        let table = self.parse_table_name()?;
        let where_clause = if self.is_kw("WHERE") {
            self.bump();
            Some(self.parse_expr(prec::NONE)?)
        } else {
            None
        };
        Ok(AdminStmt::ShowMaskingPolicies(Box::new(
            ShowMaskingPoliciesStmt {
                table,
                where_clause,
            },
        )))
    }

    fn parse_show_count_warnings_or_errors(&mut self) -> PResult<AdminStmt> {
        self.expect_kw("COUNT")?;
        self.expect_op("(")?;
        self.expect_op("*")?;
        self.expect_op(")")?;
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
            Ok(AdminStmt::ShowWarnings(Box::new(ShowWarningsStmt {
                count_only: true,
                filter,
            })))
        } else if self.token_literal_is_at(0, "ERRORS") {
            self.bump();
            let filter = if self.is_kw("LIKE") {
                self.bump();
                Some(ShowErrorsFilter::Like(self.parse_expr(prec::UNARY)?))
            } else if self.is_kw("WHERE") {
                self.bump();
                Some(ShowErrorsFilter::Where(self.parse_expr(prec::NONE)?))
            } else {
                None
            };
            Ok(AdminStmt::ShowErrors(Box::new(ShowErrorsStmt {
                count_only: true,
                filter,
            })))
        } else {
            Err(self.err_here("expected WARNINGS or ERRORS"))
        }
    }

    fn parse_show_create(&mut self) -> PResult<AdminStmt> {
        self.expect_kw("CREATE")?;
        if self.ident_like_literal_is_at(0, "PLACEMENT") {
            self.bump();
            self.expect_token_literal("POLICY")?;
            return Ok(AdminStmt::ShowCreate {
                kind: ShowCreateKind::PlacementPolicy,
                if_not_exists: false,
                name: vec![self.parse_any_token_name()],
            });
        }
        if self.ident_like_literal_is_at(0, "RESOURCE") {
            self.bump();
            self.expect_token_literal("GROUP")?;
            return Ok(AdminStmt::ShowCreate {
                kind: ShowCreateKind::ResourceGroup,
                if_not_exists: false,
                name: vec![self.parse_any_token_name()],
            });
        }
        let kind = if self.is_kw("TABLE") {
            ShowCreateKind::Table
        } else if self.is_kw("VIEW") {
            ShowCreateKind::View
        } else if self.is_kw("SEQUENCE") {
            ShowCreateKind::Sequence
        } else if self.is_kw("DATABASE") || self.is_kw("SCHEMA") {
            ShowCreateKind::Database
        } else if self.is_kw("PROCEDURE") {
            ShowCreateKind::Procedure
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
        let name = if kind == ShowCreateKind::Database {
            vec![self.parse_any_token_name()]
        } else {
            self.parse_table_name()?
        };
        Ok(AdminStmt::ShowCreate {
            kind,
            if_not_exists,
            name,
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
            Some(self.parse_expr(prec::UNARY)?)
        } else {
            None
        };
        let where_clause = if like.is_none() && self.is_kw("WHERE") {
            self.bump();
            Some(self.parse_expr(prec::NONE)?)
        } else {
            None
        };
        Ok(AdminStmt::ShowVariables(Box::new(ShowVariablesStmt {
            global,
            like,
            where_clause,
        })))
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
        let mut table = self.parse_table_name()?;
        if let Some(database) = self.parse_show_database_name_opt() {
            let object = table
                .pop()
                .ok_or_else(|| self.err_here("expected table name"))?;
            table = vec![database, object];
        }
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
        let database = self.parse_show_database_name_opt();
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
        self.expect_keyword_or_ident("STATUS")?;
        let database = self.parse_show_database_name_opt();
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
    fn parse_show_table_inspection(&mut self) -> PResult<AdminStmt> {
        self.expect_kw("TABLE")?;
        let table = self.parse_table_name()?;
        if self.is_kw("NEXT_ROW_ID") {
            self.bump();
            return Ok(AdminStmt::ShowTableNextRowId(Box::new(
                ShowTableNextRowIdStmt { table },
            )));
        }
        let mut partitions = Vec::new();
        if self.is_kw("PARTITION") {
            self.bump();
            self.expect_op("(")?;
            loop {
                partitions.push(self.parse_non_string_ident_like_name()?);
                if self.is_op(",") {
                    self.bump();
                } else {
                    break;
                }
            }
            self.expect_op(")")?;
        }
        let index = if self.is_kw("INDEX") {
            self.bump();
            Some(self.parse_non_string_ident_like_name()?)
        } else {
            None
        };
        let kind = if self.is_kw("REGIONS") {
            self.bump();
            ShowTablePlacementKind::Regions
        } else if self.is_kw("DISTRIBUTIONS") {
            self.bump();
            ShowTablePlacementKind::Distributions
        } else {
            return Err(self.err_here("expected NEXT_ROW_ID, REGIONS, or DISTRIBUTIONS"));
        };
        let filter = if self.is_kw("WHERE") {
            self.bump();
            Some(ShowInspectionFilter::Where(self.parse_expr(prec::NONE)?))
        } else {
            None
        };
        Ok(AdminStmt::ShowTablePlacement(Box::new(
            ShowTablePlacementStmt {
                table,
                partitions,
                index,
                kind,
                filter,
            },
        )))
    }

    fn parse_inspection_filter(&mut self) -> PResult<Option<ShowInspectionFilter>> {
        if self.is_kw("LIKE") {
            self.bump();
            Ok(Some(ShowInspectionFilter::Like(
                self.parse_expr(prec::UNARY)?,
            )))
        } else if self.is_kw("WHERE") {
            self.bump();
            Ok(Some(ShowInspectionFilter::Where(
                self.parse_expr(prec::NONE)?,
            )))
        } else {
            Ok(None)
        }
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
        let mut full = false;
        let mut extended = false;
        loop {
            if self.is_kw("EXTENDED") && !extended {
                self.bump();
                extended = true;
            } else if self.is_kw("FULL") && !full {
                self.bump();
                full = true;
            } else {
                break;
            }
        }
        self.parse_show_columns_tail(full, extended)
    }

    fn parse_show_columns_tail(&mut self, full: bool, extended: bool) -> PResult<ShowColumnsStmt> {
        if !(self.is_kw("COLUMNS") || self.is_kw("FIELDS")) {
            return Err(self.err_here("expected COLUMNS or FIELDS"));
        }
        self.bump();
        self.parse_show_columns_after_head(full, extended)
    }

    fn parse_show_columns_after_head(
        &mut self,
        full: bool,
        extended: bool,
    ) -> PResult<ShowColumnsStmt> {
        if self.is_kw("FROM") || self.is_kw("IN") {
            self.bump();
        } else {
            return Err(self.err_here("expected FROM or IN"));
        }
        let table = self.parse_table_name()?;
        let database = self.parse_show_database_name_opt();
        let filter = if self.is_kw("LIKE") {
            self.bump();
            Some(ShowColumnsFilter::Like(self.parse_expr(prec::UNARY)?))
        } else if self.is_kw("WHERE") {
            self.bump();
            Some(ShowColumnsFilter::Where(self.parse_expr(prec::NONE)?))
        } else {
            None
        };
        Ok(ShowColumnsStmt {
            full,
            extended,
            table,
            database,
            filter,
        })
    }

    /// Go consumes an optional `FROM|IN` even when the following token is
    /// EOF, but its empty DBName restores as no clause.
    fn parse_show_database_name_opt(&mut self) -> Option<String> {
        if !(self.is_kw("FROM") || self.is_kw("IN")) {
            return None;
        }
        self.bump();
        let database = self.parse_any_token_name();
        (!database.is_empty()).then_some(database)
    }
}
