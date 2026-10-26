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

//! Go `ast.ShowStmt` (`pkg/parser/ast/dml.go`): one SHOW statement node with
//! a type tag, as Go's planner (`buildShow`, `buildShowSchema`) and executor
//! (`ShowExec`) consume it.

use crate::util::{back_quote, push_name_path};
use crate::{Expr, Limit, UserSpec};

pub use crate::show::ShowProfileType;

/// Go `ast.ShowStmtType`, in Go's order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShowStmtType {
    None,
    Engines,
    Databases,
    Tables,
    TableStatus,
    Columns,
    Warnings,
    Charset,
    Variables,
    Status,
    Collation,
    CreateTable,
    CreateView,
    CreateUser,
    CreateSequence,
    CreatePlacementPolicy,
    Grants,
    MaskingPolicies,
    Triggers,
    ProcedureStatus,
    FunctionStatus,
    Index,
    ProcessList,
    CreateDatabase,
    Config,
    Events,
    StatsExtended,
    StatsMeta,
    StatsHistograms,
    StatsTopN,
    StatsBuckets,
    StatsHealthy,
    StatsLocked,
    HistogramsInFlight,
    ColumnStatsUsage,
    Plugins,
    Profile,
    Profiles,
    MasterStatus,
    Privileges,
    Errors,
    Bindings,
    BindingCacheStatus,
    OpenTables,
    AnalyzeStatus,
    Regions,
    Builtins,
    TableNextRowId,
    Backups,
    Restores,
    Imports,
    CreateImport,
    Placement,
    PlacementForDatabase,
    PlacementForTable,
    PlacementForPartition,
    PlacementLabels,
    SessionStates,
    CreateResourceGroup,
    ImportJobs,
    ImportGroups,
    CreateProcedure,
    BinlogStatus,
    ReplicaStatus,
    Distributions,
    DistributionJobs,
    Affinity,
}

/// Go `ast.ShowStmt`.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowStmt {
    /// Go `Tp`.
    pub tp: ShowStmtType,
    /// Go `DBName`.
    pub db_name: String,
    /// Go `Table`, as its name path (`[schema,] table`).
    pub table: Option<Vec<String>>,
    /// Go `Procedure`, as its name path.
    pub procedure: Option<Vec<String>>,
    /// Go `Partition`.
    pub partition: String,
    /// Go `Column`, for `DESC table column`.
    pub column: Option<String>,
    /// Go `IndexName`.
    pub index_name: String,
    /// Go `ResourceGroupName`.
    pub resource_group_name: String,
    /// Go `Flag`, parsed modifiers such as FULL.
    pub flag: i64,
    /// Go `Full`.
    pub full: bool,
    /// Go `User`, for SHOW GRANTS / SHOW CREATE USER.
    pub user: Option<UserSpec>,
    /// Go `Roles`, for SHOW GRANTS ... USING.
    pub roles: Vec<UserSpec>,
    /// Go `IfNotExists`, for SHOW CREATE DATABASE IF NOT EXISTS.
    pub if_not_exists: bool,
    /// Go `Extended`, for SHOW EXTENDED COLUMNS.
    pub extended: bool,
    /// Go `Limit`.
    pub limit: Option<Limit>,
    /// Go `CountWarningsOrErrors`, for SHOW COUNT(*) WARNINGS | ERRORS.
    pub count_warnings_or_errors: bool,
    /// Go `GlobalScope`, for SHOW VARIABLES / STATUS / BINDINGS.
    pub global_scope: bool,
    /// Go `Pattern.Pattern`: the LIKE pattern (Go builds the
    /// `PatternLikeOrIlikeExpr` with `Escape: '\\'` and `IsLike: true`).
    pub pattern: Option<Expr>,
    /// Go `Where`.
    pub r#where: Option<Expr>,
    /// Go `ShowProfileTypes`.
    pub show_profile_types: Vec<ShowProfileType>,
    /// Go `ShowProfileArgs`, the FOR QUERY id.
    pub show_profile_args: Option<i64>,
    /// Go `ShowProfileLimit`.
    pub show_profile_limit: Option<Limit>,
    /// Go `ShowGroupKey`, for SHOW IMPORT GROUP <key>.
    pub show_group_key: String,
    /// Go `ImportJobID`, for SHOW IMPORT JOB <id>.
    pub import_job_id: Option<i64>,
    /// Go `ImportJobRaw`, for SHOW RAW IMPORT JOB(S).
    pub import_job_raw: bool,
    /// Go `DistributionJobID`, for SHOW DISTRIBUTION JOB <id>.
    pub distribution_job_id: Option<i64>,
}

impl ShowStmt {
    /// A statement of type `tp` with every other field at Go's zero value.
    #[must_use]
    pub fn new(tp: ShowStmtType) -> Self {
        Self {
            tp,
            db_name: String::new(),
            table: None,
            procedure: None,
            partition: String::new(),
            column: None,
            index_name: String::new(),
            resource_group_name: String::new(),
            flag: 0,
            full: false,
            user: None,
            roles: Vec::new(),
            if_not_exists: false,
            extended: false,
            limit: None,
            count_warnings_or_errors: false,
            global_scope: false,
            pattern: None,
            r#where: None,
            show_profile_types: Vec::new(),
            show_profile_args: None,
            show_profile_limit: None,
            show_group_key: String::new(),
            import_job_id: None,
            import_job_raw: false,
            distribution_job_id: None,
        }
    }

    fn restore_table(&self, out: &mut String) {
        if let Some(table) = &self.table {
            push_name_path(out, table);
        }
    }

    fn restore_like_or_where(&self, out: &mut String) {
        if let Some(pattern) = &self.pattern {
            out.push_str(" LIKE ");
            pattern.restore_into(out);
        } else if let Some(r#where) = &self.r#where {
            out.push_str(" WHERE ");
            r#where.restore_into(out);
        }
    }

    fn restore_database_name_opt(&self, out: &mut String) {
        if !self.db_name.is_empty() {
            out.push_str(" IN ");
            out.push_str(&back_quote(&self.db_name));
        }
    }

    /// Go `ShowStmt.Restore`.
    pub(crate) fn restore_into(&self, out: &mut String) {
        use ShowStmtType as T;
        let full = |out: &mut String| {
            if self.full {
                out.push_str("FULL ");
            }
        };
        let scope = |out: &mut String| {
            out.push_str(if self.global_scope { "GLOBAL " } else { "SESSION " });
        };
        out.push_str("SHOW ");
        match self.tp {
            T::BinlogStatus => out.push_str("BINARY LOG STATUS"),
            T::CreateTable => {
                out.push_str("CREATE TABLE ");
                self.restore_table(out);
            }
            T::CreateProcedure => {
                out.push_str("CREATE PROCEDURE ");
                if let Some(procedure) = &self.procedure {
                    push_name_path(out, procedure);
                }
            }
            T::CreateView => {
                out.push_str("CREATE VIEW ");
                self.restore_table(out);
            }
            T::CreateDatabase => {
                out.push_str("CREATE DATABASE ");
                if self.if_not_exists {
                    out.push_str("IF NOT EXISTS ");
                }
                out.push_str(&back_quote(&self.db_name));
            }
            T::CreateSequence => {
                out.push_str("CREATE SEQUENCE ");
                self.restore_table(out);
            }
            T::CreatePlacementPolicy => {
                out.push_str("CREATE PLACEMENT POLICY ");
                out.push_str(&back_quote(&self.db_name));
            }
            T::CreateResourceGroup => {
                out.push_str("CREATE RESOURCE GROUP ");
                out.push_str(&back_quote(&self.resource_group_name));
            }
            T::CreateUser => {
                out.push_str("CREATE USER ");
                if let Some(user) = &self.user {
                    user.restore_into(out);
                }
            }
            T::MaskingPolicies => {
                out.push_str("MASKING POLICIES FOR ");
                self.restore_table(out);
                if let Some(r#where) = &self.r#where {
                    out.push_str(" WHERE ");
                    r#where.restore_into(out);
                }
            }
            T::Grants => {
                out.push_str("GRANTS");
                if let Some(user) = &self.user {
                    out.push_str(" FOR ");
                    user.restore_into(out);
                }
                if !self.roles.is_empty() {
                    out.push_str(" USING ");
                    for (index, role) in self.roles.iter().enumerate() {
                        if index > 0 {
                            out.push_str(", ");
                        }
                        role.restore_into(out);
                    }
                }
            }
            T::MasterStatus => out.push_str("MASTER STATUS"),
            T::ProcessList => {
                full(out);
                out.push_str("PROCESSLIST");
            }
            T::StatsExtended
            | T::StatsMeta
            | T::StatsLocked
            | T::StatsHistograms
            | T::StatsTopN
            | T::StatsBuckets
            | T::StatsHealthy
            | T::HistogramsInFlight
            | T::ColumnStatsUsage => {
                out.push_str(match self.tp {
                    T::StatsExtended => "STATS_EXTENDED",
                    T::StatsMeta => "STATS_META",
                    T::StatsLocked => "STATS_LOCKED",
                    T::StatsHistograms => "STATS_HISTOGRAMS",
                    T::StatsTopN => "STATS_TOPN",
                    T::StatsBuckets => "STATS_BUCKETS",
                    T::StatsHealthy => "STATS_HEALTHY",
                    T::HistogramsInFlight => "HISTOGRAMS_IN_FLIGHT",
                    _ => "COLUMN_STATS_USAGE",
                });
                self.restore_like_or_where(out);
            }
            T::Profiles => out.push_str("PROFILES"),
            T::Profile => {
                out.push_str("PROFILE");
                for (index, kind) in self.show_profile_types.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    out.push(' ');
                    out.push_str(kind.restore_name());
                }
                if let Some(query_id) = self.show_profile_args {
                    out.push_str(" FOR QUERY ");
                    out.push_str(&query_id.to_string());
                }
                if let Some(limit) = &self.show_profile_limit {
                    limit.restore_into(out);
                }
            }
            T::Privileges => out.push_str("PRIVILEGES"),
            T::Builtins => out.push_str("BUILTINS"),
            T::PlacementForDatabase => {
                out.push_str("PLACEMENT FOR DATABASE ");
                out.push_str(&back_quote(&self.db_name));
            }
            T::PlacementForTable => {
                out.push_str("PLACEMENT FOR TABLE ");
                self.restore_table(out);
            }
            T::PlacementForPartition => {
                out.push_str("PLACEMENT FOR TABLE ");
                self.restore_table(out);
                out.push_str(" PARTITION ");
                out.push_str(&back_quote(&self.partition));
            }
            T::ImportJobs => {
                if self.import_job_raw {
                    out.push_str("RAW ");
                }
                if let Some(id) = self.import_job_id {
                    out.push_str("IMPORT JOB ");
                    out.push_str(&id.to_string());
                } else {
                    out.push_str("IMPORT JOBS");
                    self.restore_like_or_where(out);
                }
            }
            T::ImportGroups => {
                if self.show_group_key.is_empty() {
                    out.push_str("IMPORT GROUPS");
                    self.restore_like_or_where(out);
                } else {
                    out.push_str("IMPORT GROUP ");
                    out.push_str(&crate::util::escape_string_literal(&self.show_group_key));
                }
            }
            T::DistributionJobs => {
                if let Some(id) = self.distribution_job_id {
                    out.push_str("DISTRIBUTION JOB ");
                    out.push_str(&id.to_string());
                } else {
                    out.push_str("DISTRIBUTION JOBS");
                    self.restore_like_or_where(out);
                }
            }
            // Go's `ShowTargetFilterable` group.
            _ => {
                match self.tp {
                    T::Engines => out.push_str("ENGINES"),
                    T::Config => out.push_str("CONFIG"),
                    T::Databases => out.push_str("DATABASES"),
                    T::Charset => out.push_str("CHARSET"),
                    T::Tables => {
                        full(out);
                        out.push_str("TABLES");
                        self.restore_database_name_opt(out);
                    }
                    T::OpenTables => {
                        out.push_str("OPEN TABLES");
                        self.restore_database_name_opt(out);
                    }
                    T::TableStatus => {
                        out.push_str("TABLE STATUS");
                        self.restore_database_name_opt(out);
                    }
                    T::Index => {
                        out.push_str("INDEX IN ");
                        self.restore_table(out);
                    }
                    T::Columns => {
                        if self.extended {
                            out.push_str("EXTENDED ");
                        }
                        full(out);
                        out.push_str("COLUMNS");
                        if self.table.is_some() {
                            out.push_str(" IN ");
                            self.restore_table(out);
                        }
                        self.restore_database_name_opt(out);
                    }
                    T::Warnings => out.push_str("WARNINGS"),
                    T::Errors => out.push_str("ERRORS"),
                    T::Variables => {
                        scope(out);
                        out.push_str("VARIABLES");
                    }
                    T::Status => {
                        scope(out);
                        out.push_str("STATUS");
                    }
                    T::Collation => out.push_str("COLLATION"),
                    T::Triggers => {
                        out.push_str("TRIGGERS");
                        self.restore_database_name_opt(out);
                    }
                    T::ProcedureStatus => out.push_str("PROCEDURE STATUS"),
                    T::FunctionStatus => out.push_str("FUNCTION STATUS"),
                    T::Events => {
                        out.push_str("EVENTS");
                        self.restore_database_name_opt(out);
                    }
                    T::Plugins => out.push_str("PLUGINS"),
                    T::Bindings => {
                        scope(out);
                        out.push_str("BINDINGS");
                    }
                    T::BindingCacheStatus => out.push_str("BINDING_CACHE STATUS"),
                    T::AnalyzeStatus => out.push_str("ANALYZE STATUS"),
                    T::Distributions => {
                        out.push_str("TABLE ");
                        self.restore_table(out);
                        out.push_str(" DISTRIBUTIONS");
                        self.restore_like_or_where(out);
                        return;
                    }
                    T::Regions => {
                        out.push_str("TABLE ");
                        self.restore_table(out);
                        if !self.index_name.is_empty() {
                            out.push_str(" INDEX ");
                            out.push_str(&back_quote(&self.index_name));
                        }
                        out.push_str(" REGIONS");
                        self.restore_like_or_where(out);
                        return;
                    }
                    T::TableNextRowId => {
                        out.push_str("TABLE ");
                        self.restore_table(out);
                        out.push_str(" NEXT_ROW_ID");
                        return;
                    }
                    T::Backups => out.push_str("BACKUPS"),
                    T::Restores => out.push_str("RESTORES"),
                    T::Imports => out.push_str("IMPORTS"),
                    T::Placement => {
                        out.push_str("PLACEMENT");
                        if !self.db_name.is_empty() {
                            out.push_str(" FOR DATABASE ");
                            out.push_str(&back_quote(&self.db_name));
                        } else if self.table.is_some() {
                            out.push_str(" FOR TABLE ");
                            self.restore_table(out);
                            if !self.partition.is_empty() {
                                out.push_str(" PARTITION ");
                                out.push_str(&back_quote(&self.partition));
                            }
                        } else if !self.partition.is_empty() {
                            out.push_str(" FOR PARTITION ");
                            out.push_str(&back_quote(&self.partition));
                        }
                    }
                    T::PlacementLabels => out.push_str("PLACEMENT LABELS"),
                    T::SessionStates => out.push_str("SESSION_STATES"),
                    T::ReplicaStatus => out.push_str("REPLICA STATUS"),
                    T::Affinity => out.push_str("AFFINITY"),
                    // Go: "Unknown ShowStmt type".
                    _ => {}
                }
                self.restore_like_or_where(out);
            }
        }
    }
}

impl crate::Visitable for ShowStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        if let Some(pattern) = self.pattern.as_mut() {
            if !crate::Visitable::accept(pattern, visitor) {
                return false;
            }
        }
        if let Some(r#where) = self.r#where.as_mut() {
            if !crate::Visitable::accept(r#where, visitor) {
                return false;
            }
        }
        visitor.leave(self)
    }
}
