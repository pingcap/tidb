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
    AdminStmt, ShowStmt, ShowStmtType, 
    
    ShowProfileType,
    };
use tidb_lexer::TokenKind;

use crate::{prec, PResult, Parser};

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
            return Ok(AdminStmt::Show(Box::new(ShowStmt::new(ShowStmtType::MasterStatus))));
        }
        if self.is_kw("PRIVILEGES") {
            self.bump();
            return Ok(AdminStmt::Show(Box::new(ShowStmt::new(ShowStmtType::Privileges))));
        }
        if self.is_kw("BUILTINS") {
            self.bump();
            return Ok(AdminStmt::Show(Box::new(ShowStmt::new(ShowStmtType::Builtins))));
        }
        if let Some(show) = self.parse_common_show_inspection()? {
            return Ok(AdminStmt::Show(Box::new(show)));
        }
        if self.is_kw("VARIABLES")
            || ((self.is_kw("GLOBAL") || self.is_kw("SESSION")) && self.is_kw_at(1, "VARIABLES"))
        {
            return self.parse_show_variables();
        }
        if self.is_kw("STATUS")
            || ((self.is_kw("GLOBAL") || self.is_kw("SESSION")) && self.is_kw_at(1, "STATUS"))
        {
            return self.parse_show_status();
        }
        if self.is_kw("WARNINGS") {
            self.bump();
            return self.show_with_like_or_where(ShowStmtType::Warnings);
        }
        if self.is_kw("ERRORS") {
            self.bump();
            return self.show_with_like_or_where(ShowStmtType::Errors);
        }
        if self.is_kw("COUNT") {
            return self.parse_show_count_warnings_or_errors();
        }
        if self.is_kw("COLLATION") {
            self.bump();
            return self.show_with_like_or_where(ShowStmtType::Collation);
        }
        if let Some(show) = self.parse_show_charset()? {
            return Ok(show);
        }
        if self.is_kw("ENGINES") {
            self.bump();
            return self.show_with_like_or_where(ShowStmtType::Engines);
        }
        if self.peek().text.eq_ignore_ascii_case("STATS_LOCKED") {
            self.bump();
            return self.show_with_like_or_where(ShowStmtType::StatsLocked);
        }
        if self.peek().text.eq_ignore_ascii_case("STATS_BUCKETS") {
            self.bump();
            return self.show_with_like_or_where(ShowStmtType::StatsBuckets);
        }
        if self.is_kw("STATS_HISTOGRAMS") {
            self.bump();
            return self.show_with_like_or_where(ShowStmtType::StatsHistograms);
        }
        if self.is_kw("STATS_TOPN") {
            self.bump();
            return self.show_with_like_or_where(ShowStmtType::StatsTopN);
        }
        if self.is_kw("DATABASES") {
            self.bump();
            return self.show_with_like_or_where(ShowStmtType::Databases);
        }
        if self.is_kw("TABLES") || (self.is_kw("FULL") && self.is_kw_at(1, "TABLES")) {
            return self.parse_show_tables();
        }
        if let Some(show) = self.parse_show_open_tables()? {
            return Ok(show);
        }
        if self.is_kw("TABLE") && self.keyword_or_ident_is_at(1, "STATUS") {
            return self.parse_show_table_status();
        }
        if self.is_kw("TABLE") {
            return self.parse_show_table_inspection();
        }
        if self.is_kw("COLUMNS")
            || self.is_kw("FIELDS")
            || self.is_kw("EXTENDED")
            || (self.is_kw("FULL") && (self.is_kw_at(1, "COLUMNS") || self.is_kw_at(1, "FIELDS")))
        {
            return self.parse_show_columns();
        }
        if self.is_kw("INDEX") || self.is_kw("INDEXES") || self.is_kw("KEYS") {
            return self.parse_show_index();
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
        let head = self.token_literal_text(self.peek()).to_ascii_uppercase();

        let generic = match head.as_str() {
            "TRIGGERS" => Some((ShowStmtType::Triggers, true, true)),
            "EVENTS" => Some((ShowStmtType::Events, true, true)),
            "PLUGINS" => Some((ShowStmtType::Plugins, false, true)),
            "STATS_EXTENDED" => Some((ShowStmtType::StatsExtended, false, true)),
            "STATS_META" => Some((ShowStmtType::StatsMeta, false, true)),
            "STATS_HEALTHY" => Some((ShowStmtType::StatsHealthy, false, true)),
            "HISTOGRAMS_IN_FLIGHT" => Some((ShowStmtType::HistogramsInFlight, false, true)),
            "COLUMN_STATS_USAGE" => Some((ShowStmtType::ColumnStatsUsage, false, true)),
            "BACKUPS" => Some((ShowStmtType::Backups, false, true)),
            "RESTORES" => Some((ShowStmtType::Restores, false, true)),
            "IMPORTS" => Some((ShowStmtType::Imports, false, true)),
            "CONFIG" => Some((ShowStmtType::Config, false, true)),
            "PROFILES" => Some((ShowStmtType::Profiles, false, false)),
            "SESSION_STATES" => Some((ShowStmtType::SessionStates, false, false)),
            "AFFINITY" => Some((ShowStmtType::Affinity, false, true)),
            _ => None,
        };
        if let Some((kind, with_database, with_filter)) = generic {
            self.bump();
            let mut show = ShowStmt::new(kind);
            if with_database {
                show.db_name = self.parse_show_database_name_opt().unwrap_or_default();
            }
            if with_filter {
                self.parse_show_like_or_where(&mut show)?;
            }
            return Ok(Some(AdminStmt::Show(Box::new(show))));
        }

        match head.as_str() {
            "DATABASES" => {
                self.bump();
                self.show_with_like_or_where(ShowStmtType::Databases).map(Some)
            }
            "ENGINES" => {
                self.bump();
                self.show_with_like_or_where(ShowStmtType::Engines).map(Some)
            }
            "COLLATION" => {
                self.bump();
                self.show_with_like_or_where(ShowStmtType::Collation).map(Some)
            }
            "ERRORS" => {
                self.bump();
                self.show_with_like_or_where(ShowStmtType::Errors).map(Some)
            }
            "CHARSET" => {
                self.bump();
                self.show_with_like_or_where(ShowStmtType::Charset).map(Some)
            }
            "PRIVILEGES" => {
                self.bump();
                Ok(Some(AdminStmt::Show(Box::new(ShowStmt::new(
                    ShowStmtType::Privileges,
                )))))
            }
            "BUILTINS" => {
                self.bump();
                Ok(Some(AdminStmt::Show(Box::new(ShowStmt::new(
                    ShowStmtType::Builtins,
                )))))
            }
            "STATS_LOCKED" => {
                self.bump();
                self.show_with_like_or_where(ShowStmtType::StatsLocked).map(Some)
            }
            "STATS_BUCKETS" => {
                self.bump();
                self.show_with_like_or_where(ShowStmtType::StatsBuckets).map(Some)
            }
            "STATS_HISTOGRAMS" => {
                self.bump();
                self.show_with_like_or_where(ShowStmtType::StatsHistograms).map(Some)
            }
            "STATS_TOPN" => {
                self.bump();
                self.show_with_like_or_where(ShowStmtType::StatsTopN).map(Some)
            }
            "OPEN" => {
                self.bump();
                self.expect_kw("TABLES")?;
                self.show_with_database_name_opt(ShowStmtType::OpenTables).map(Some)
            }
            "TABLE" if self.keyword_or_ident_is_at(1, "STATUS") => {
                self.bump();
                self.bump();
                self.show_with_database_name_opt(ShowStmtType::TableStatus).map(Some)
            }
            "PLACEMENT" => {
                self.bump();
                let tp = if self.token_literal_is_at(0, "LABELS") {
                    self.bump();
                    ShowStmtType::PlacementLabels
                } else {
                    ShowStmtType::Placement
                };
                self.show_with_like_or_where(tp).map(Some)
            }
            "BINDING_CACHE" if self.keyword_or_ident_is_at(1, "STATUS") => {
                self.bump();
                self.bump();
                Ok(Some(AdminStmt::Show(Box::new(ShowStmt::new(
                    ShowStmtType::BindingCacheStatus,
                )))))
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
                self.bump();
                self.parse_show_columns_after_head(full, true).map(Some)
            }
            "SLAVE" if self.keyword_or_ident_is_at(1, "STATUS") => {
                self.bump();
                self.bump();
                Ok(Some(AdminStmt::Show(Box::new(ShowStmt::new(
                    ShowStmtType::ReplicaStatus,
                )))))
            }
            _ => Ok(None),
        }
    }

    /// Go `"SHOW" CharsetKw ShowLikeOrWhereOpt`.
    fn parse_show_charset(&mut self) -> PResult<Option<AdminStmt>> {
        if self.is_kw("CHARSET") {
            self.bump();
        } else if self.is_kw("CHARACTER") || self.is_kw("CHAR") {
            self.bump();
            self.expect_kw("SET")?;
        } else {
            return Ok(None);
        }
        self.show_with_like_or_where(ShowStmtType::Charset).map(Some)
    }

    /// Go `ShowLikeOrWhereOpt`: `LIKE` sets `Pattern` (Go's
    /// `PatternLikeOrIlikeExpr` with `Escape: '\\'`), `WHERE` sets `Where`.
    pub(crate) fn parse_show_like_or_where(&mut self, show: &mut ShowStmt) -> PResult<()> {
        if self.is_kw("LIKE") {
            self.bump();
            show.pattern = Some(self.parse_expr(prec::UNARY)?);
        } else if self.is_kw("WHERE") {
            self.bump();
            show.r#where = Some(self.parse_expr(prec::NONE)?);
        }
        Ok(())
    }

    /// A `ShowStmt` of type `tp` followed by Go's `ShowLikeOrWhereOpt`.
    fn show_with_like_or_where(&mut self, tp: ShowStmtType) -> PResult<AdminStmt> {
        let mut show = ShowStmt::new(tp);
        self.parse_show_like_or_where(&mut show)?;
        Ok(AdminStmt::Show(Box::new(show)))
    }


    /// Go `"SHOW" "OPEN" "TABLES" ShowDatabaseNameOpt ShowLikeOrWhereOpt`.
    fn parse_show_open_tables(&mut self) -> PResult<Option<AdminStmt>> {
        if !self.is_kw("OPEN") {
            return Ok(None);
        }
        self.bump();
        self.expect_kw("TABLES")?;
        self.show_with_database_name_opt(ShowStmtType::OpenTables).map(Some)
    }

    /// A `ShowStmt` of type `tp` followed by Go's `ShowDatabaseNameOpt` and
    /// `ShowLikeOrWhereOpt`.
    fn show_with_database_name_opt(&mut self, tp: ShowStmtType) -> PResult<AdminStmt> {
        let mut show = ShowStmt::new(tp);
        show.db_name = self.parse_show_database_name_opt().unwrap_or_default();
        self.parse_show_like_or_where(&mut show)?;
        Ok(AdminStmt::Show(Box::new(show)))
    }

    fn parse_common_show_inspection(&mut self) -> PResult<Option<ShowStmt>> {
        let mut full = false;
        let (kind, words, allow_database, allow_filter) = if self.is_kw("TRIGGERS") {
            (ShowStmtType::Triggers, 1, true, true)
        } else if self.is_kw("PROCEDURE") && self.keyword_or_ident_is_at(1, "STATUS") {
            (ShowStmtType::ProcedureStatus, 2, false, true)
        } else if self.is_kw("FUNCTION") && self.keyword_or_ident_is_at(1, "STATUS") {
            (ShowStmtType::FunctionStatus, 2, false, true)
        } else if self.is_kw("EVENTS") {
            (ShowStmtType::Events, 1, true, true)
        } else if self.is_kw("PLUGINS") {
            (ShowStmtType::Plugins, 1, false, true)
        } else if self.is_kw("STATS_EXTENDED") {
            (ShowStmtType::StatsExtended, 1, false, true)
        } else if self.is_kw("STATS_META") {
            (ShowStmtType::StatsMeta, 1, false, true)
        } else if self.is_kw("STATS_HEALTHY") {
            (ShowStmtType::StatsHealthy, 1, false, true)
        } else if self.is_kw("HISTOGRAMS_IN_FLIGHT") {
            (ShowStmtType::HistogramsInFlight, 1, false, true)
        } else if self.is_kw("COLUMN_STATS_USAGE") {
            (ShowStmtType::ColumnStatsUsage, 1, false, true)
        } else if self.is_kw("BINDING_CACHE") && self.keyword_or_ident_is_at(1, "STATUS") {
            (ShowStmtType::BindingCacheStatus, 2, false, false)
        } else if self.is_kw("ANALYZE") && self.keyword_or_ident_is_at(1, "STATUS") {
            (ShowStmtType::AnalyzeStatus, 2, false, true)
        } else if self.is_kw("BACKUPS") {
            (ShowStmtType::Backups, 1, false, true)
        } else if self.is_kw("RESTORES") {
            (ShowStmtType::Restores, 1, false, true)
        } else if self.is_kw("IMPORTS") {
            (ShowStmtType::Imports, 1, false, true)
        } else if self.is_kw("CONFIG") {
            (ShowStmtType::Config, 1, false, true)
        } else if (self.is_kw("REPLICA") || self.is_kw("SLAVE"))
            && self.keyword_or_ident_is_at(1, "STATUS")
        {
            (ShowStmtType::ReplicaStatus, 2, false, false)
        } else if self.is_kw("BINARY") {
            // go's grammar accepts `SHOW BINARY LOG STATUS`; any other
            // follower fails AT that follower -- `SHOW BINARY LOGS` answers
            // `line 1 column 16 near "LOGS" ` (oracle m22), not the
            // BINARY-rooted error a miss here would otherwise raise.
            if !(self.token_literal_is_at(1, "LOG") && self.keyword_or_ident_is_at(2, "STATUS")) {
                self.bump();
                return Err(self.err_here("expected LOG STATUS after BINARY"));
            }
            (ShowStmtType::BinlogStatus, 3, false, false)
        } else if self.is_kw("PROFILES") {
            (ShowStmtType::Profiles, 1, false, false)
        } else if self.is_kw("SESSION_STATES") {
            (ShowStmtType::SessionStates, 1, false, false)
        } else if self.is_kw("PROCESSLIST") {
            (ShowStmtType::ProcessList, 1, false, false)
        } else if self.is_kw("FULL") && self.is_kw_at(1, "PROCESSLIST") {
            full = true;
            (ShowStmtType::ProcessList, 2, false, false)
        } else if self.is_kw("AFFINITY") {
            (ShowStmtType::Affinity, 1, false, true)
        } else {
            return Ok(None);
        };
        for _ in 0..words {
            self.bump();
        }
        let mut show = ShowStmt::new(kind);
        show.full = full;
        if allow_database {
            show.db_name = self.parse_show_database_name_opt().unwrap_or_default();
        }
        if allow_filter {
            self.parse_show_like_or_where(&mut show)?;
        }
        Ok(Some(show))
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
            let mut show = ShowStmt::new(ShowStmtType::ImportJobs);
            show.import_job_raw = raw;
            show.import_job_id = job_id;
            if !singular {
                self.parse_show_like_or_where(&mut show)?;
            }
            return Ok(AdminStmt::Show(Box::new(show)));
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
                self.decode_string(&token.text)
            } else {
                String::new()
            };
            let mut show = ShowStmt::new(ShowStmtType::ImportGroups);
            show.show_group_key = group_key;
            self.parse_show_like_or_where(&mut show)?;
            return Ok(AdminStmt::Show(Box::new(show)));
        }
        Err(self.err_here("expected IMPORT JOB(S) or GROUP(S)"))
    }

    /// Go `parseShowStmt`'s DISTRIBUTION arm: a singular job takes an
    /// integer literal id; the plural list takes LIKE / WHERE.
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
        let mut show = ShowStmt::new(ShowStmtType::DistributionJobs);
        if singular {
            show.distribution_job_id = match self.parse_expr(prec::NONE)? {
                tidb_ast::Expr::Int(value) => value.parse::<i64>().ok(),
                _ => None,
            };
        }
        if show.distribution_job_id.is_none() {
            self.parse_show_like_or_where(&mut show)?;
        }
        Ok(AdminStmt::Show(Box::new(show)))
    }

    fn parse_show_placement(&mut self) -> PResult<AdminStmt> {
        self.expect_kw("PLACEMENT")?;
        self.parse_show_placement_tail()
    }

    /// Go `parseShowPlacement`: LABELS and the unscoped form take LIKE /
    /// WHERE; the FOR DATABASE / TABLE / PARTITION forms take none.
    fn parse_show_placement_tail(&mut self) -> PResult<AdminStmt> {
        let mut show = ShowStmt::new(ShowStmtType::Placement);
        if self.token_literal_is_at(0, "LABELS") {
            self.bump();
            show.tp = ShowStmtType::PlacementLabels;
            self.parse_show_like_or_where(&mut show)?;
        } else if self.is_kw("FOR") {
            self.bump();
            if self.is_kw("DATABASE") || self.is_kw("SCHEMA") {
                self.bump();
                show.tp = ShowStmtType::PlacementForDatabase;
                show.db_name = self.parse_non_string_ident_like_name()?;
            } else if self.is_kw("TABLE") {
                self.bump();
                show.table = Some(self.parse_table_name()?);
                if self.is_kw("PARTITION") {
                    self.bump();
                    show.tp = ShowStmtType::PlacementForPartition;
                    show.partition = self.parse_non_string_ident_like_name()?;
                } else {
                    show.tp = ShowStmtType::PlacementForTable;
                }
            } else {
                return Err(self.err_here("expected DATABASE, SCHEMA, or TABLE after FOR"));
            }
        } else {
            self.parse_show_like_or_where(&mut show)?;
        }
        Ok(AdminStmt::Show(Box::new(show)))
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
        let mut show = ShowStmt::new(ShowStmtType::Profile);
        show.show_profile_types = types;
        show.show_profile_args = query_id;
        show.show_profile_limit = limit;
        Ok(AdminStmt::Show(Box::new(show)))
    }

    fn parse_show_masking_policies(&mut self) -> PResult<AdminStmt> {
        self.expect_kw("MASKING")?;
        self.expect_kw("POLICIES")?;
        self.expect_kw("FOR")?;
        let mut show = ShowStmt::new(ShowStmtType::MaskingPolicies);
        show.table = Some(self.parse_table_name()?);
        if self.is_kw("WHERE") {
            self.bump();
            show.r#where = Some(self.parse_expr(prec::NONE)?);
        }
        Ok(AdminStmt::Show(Box::new(show)))
    }

    /// Go `"SHOW" "COUNT" '(' '*' ')' ("WARNINGS" | "ERRORS")`: the count
    /// form sets `CountWarningsOrErrors`.
    fn parse_show_count_warnings_or_errors(&mut self) -> PResult<AdminStmt> {
        self.expect_kw("COUNT")?;
        self.expect_op("(")?;
        self.expect_op("*")?;
        self.expect_op(")")?;
        let tp = if self.is_kw("WARNINGS") {
            ShowStmtType::Warnings
        } else if self.token_literal_is_at(0, "ERRORS") {
            ShowStmtType::Errors
        } else {
            return Err(self.err_here("expected WARNINGS or ERRORS"));
        };
        self.bump();
        let mut show = ShowStmt::new(tp);
        show.count_warnings_or_errors = true;
        self.parse_show_like_or_where(&mut show)?;
        Ok(AdminStmt::Show(Box::new(show)))
    }

    /// Go `parseShowCreate`: the object kind picks the type and the field
    /// the name lands in (`Table`, `Procedure`, `DBName` or
    /// `ResourceGroupName`).
    fn parse_show_create(&mut self) -> PResult<AdminStmt> {
        self.expect_kw("CREATE")?;
        let show = if self.ident_like_literal_is_at(0, "PLACEMENT") {
            self.bump();
            self.expect_token_literal("POLICY")?;
            let mut show = ShowStmt::new(ShowStmtType::CreatePlacementPolicy);
            show.db_name = self.parse_any_token_name();
            show
        } else if self.ident_like_literal_is_at(0, "RESOURCE") {
            self.bump();
            self.expect_token_literal("GROUP")?;
            let mut show = ShowStmt::new(ShowStmtType::CreateResourceGroup);
            show.resource_group_name = self.parse_any_token_name();
            show
        } else if self.is_kw("DATABASE") || self.is_kw("SCHEMA") {
            self.bump();
            let mut show = ShowStmt::new(ShowStmtType::CreateDatabase);
            if self.is_kw("IF") {
                self.bump();
                self.expect_kw("NOT")?;
                self.expect_kw("EXISTS")?;
                show.if_not_exists = true;
            }
            show.db_name = self.parse_any_token_name();
            show
        } else {
            let tp = if self.is_kw("TABLE") {
                ShowStmtType::CreateTable
            } else if self.is_kw("VIEW") {
                ShowStmtType::CreateView
            } else if self.is_kw("SEQUENCE") {
                ShowStmtType::CreateSequence
            } else if self.is_kw("PROCEDURE") {
                ShowStmtType::CreateProcedure
            } else {
                return Err(self.err_here("unsupported SHOW CREATE object"));
            };
            self.bump();
            let mut show = ShowStmt::new(tp);
            let name = self.parse_table_name()?;
            if tp == ShowStmtType::CreateProcedure {
                show.procedure = Some(name);
            } else {
                show.table = Some(name);
            }
            show
        };
        Ok(AdminStmt::Show(Box::new(show)))
    }

    /// Go `"SHOW" GlobalScope "VARIABLES" ShowLikeOrWhereOpt`.
    fn parse_show_variables(&mut self) -> PResult<AdminStmt> {
        let mut show = ShowStmt::new(ShowStmtType::Variables);
        show.global_scope = self.parse_show_global_scope();
        self.expect_kw("VARIABLES")?;
        self.parse_show_like_or_where(&mut show)?;
        Ok(AdminStmt::Show(Box::new(show)))
    }

    /// Go `GlobalScope`: `GLOBAL` is true; `SESSION` or nothing is false.
    fn parse_show_global_scope(&mut self) -> bool {
        if self.is_kw("GLOBAL") {
            self.bump();
            return true;
        }
        if self.is_kw("SESSION") {
            self.bump();
        }
        false
    }

    /// Go `"SHOW" GlobalScope "STATUS" ShowLikeOrWhereOpt`.
    fn parse_show_status(&mut self) -> PResult<AdminStmt> {
        let mut show = ShowStmt::new(ShowStmtType::Status);
        show.global_scope = self.parse_show_global_scope();
        self.expect_kw("STATUS")?;
        self.parse_show_like_or_where(&mut show)?;
        Ok(AdminStmt::Show(Box::new(show)))
    }


    /// Go `"SHOW" ShowIndexKwd ShowTableAliasOpt ShowDatabaseNameOpt
    /// ShowLikeOrWhereOpt`: a separate database becomes the table's schema.
    fn parse_show_index(&mut self) -> PResult<AdminStmt> {
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
        let mut show = ShowStmt::new(ShowStmtType::Index);
        show.table = Some(table);
        self.parse_show_like_or_where(&mut show)?;
        Ok(AdminStmt::Show(Box::new(show)))
    }

    /// Go `"SHOW" OptFull "TABLES" ShowDatabaseNameOpt ShowLikeOrWhereOpt`.
    fn parse_show_tables(&mut self) -> PResult<AdminStmt> {
        let mut show = ShowStmt::new(ShowStmtType::Tables);
        if self.is_kw("FULL") {
            self.bump();
            show.full = true;
        }
        self.expect_kw("TABLES")?;
        show.db_name = self.parse_show_database_name_opt().unwrap_or_default();
        self.parse_show_like_or_where(&mut show)?;
        Ok(AdminStmt::Show(Box::new(show)))
    }

    /// Go `"SHOW" "TABLE" "STATUS" ShowDatabaseNameOpt ShowLikeOrWhereOpt`.
    fn parse_show_table_status(&mut self) -> PResult<AdminStmt> {
        self.expect_kw("TABLE")?;
        self.expect_keyword_or_ident("STATUS")?;
        self.show_with_database_name_opt(ShowStmtType::TableStatus)
    }

    /// Go `parseShowTable` after TABLE: NEXT_ROW_ID takes the bare table;
    /// REGIONS / DISTRIBUTIONS take optional partitions, an index, and WHERE.
    fn parse_show_table_inspection(&mut self) -> PResult<AdminStmt> {
        self.expect_kw("TABLE")?;
        let mut show = ShowStmt::new(ShowStmtType::TableNextRowId);
        show.table = Some(self.parse_table_name()?);
        if self.is_kw("NEXT_ROW_ID") {
            self.bump();
            return Ok(AdminStmt::Show(Box::new(show)));
        }
        if self.is_kw("PARTITION") {
            self.bump();
            self.expect_op("(")?;
            loop {
                show.table_partitions.push(self.parse_non_string_ident_like_name()?);
                if self.is_op(",") {
                    self.bump();
                } else {
                    break;
                }
            }
            self.expect_op(")")?;
        }
        if self.is_kw("INDEX") {
            self.bump();
            show.index_name = self.parse_non_string_ident_like_name()?;
        }
        show.tp = if self.is_kw("REGIONS") {
            ShowStmtType::Regions
        } else if self.is_kw("DISTRIBUTIONS") {
            ShowStmtType::Distributions
        } else {
            return Err(self.err_here("expected NEXT_ROW_ID, REGIONS, or DISTRIBUTIONS"));
        };
        self.bump();
        if self.is_kw("WHERE") {
            self.bump();
            show.r#where = Some(self.parse_expr(prec::NONE)?);
        }
        Ok(AdminStmt::Show(Box::new(show)))
    }



    fn parse_show_columns(&mut self) -> PResult<AdminStmt> {
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

    fn parse_show_columns_tail(&mut self, full: bool, extended: bool) -> PResult<AdminStmt> {
        if !(self.is_kw("COLUMNS") || self.is_kw("FIELDS")) {
            return Err(self.err_here("expected COLUMNS or FIELDS"));
        }
        self.bump();
        self.parse_show_columns_after_head(full, extended)
    }

    /// Go `"SHOW" OptFull FieldsOrColumns ShowTableAliasOpt ShowDatabaseNameOpt
    /// ShowLikeOrWhereOpt` after its head.
    fn parse_show_columns_after_head(
        &mut self,
        full: bool,
        extended: bool,
    ) -> PResult<AdminStmt> {
        if self.is_kw("FROM") || self.is_kw("IN") {
            self.bump();
        } else {
            return Err(self.err_here("expected FROM or IN"));
        }
        let mut show = ShowStmt::new(ShowStmtType::Columns);
        show.full = full;
        show.extended = extended;
        show.table = Some(self.parse_table_name()?);
        show.db_name = self.parse_show_database_name_opt().unwrap_or_default();
        self.parse_show_like_or_where(&mut show)?;
        Ok(AdminStmt::Show(Box::new(show)))
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
