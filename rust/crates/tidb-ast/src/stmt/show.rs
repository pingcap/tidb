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

//! Source-visible payloads for TiDB's ordinary `SHOW` inspection grammar.
//!
//! Security-owned `SHOW GRANTS`/`SHOW CREATE USER`, SQL bindings, and
//! `ADMIN SHOW` controls deliberately remain with their semantic domains.

use crate::util::push_name_path;
use crate::Expr;

/// TiDB's character-set catalog inspection statement.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowCharsetStmt {
    /// Optional filter over the character-set catalog rows.
    pub filter: Option<ShowCharsetFilter>,
}

/// The shared SHOW filter alternatives for `CHARSET`.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowCharsetFilter {
    /// `LIKE expression`.
    Like(Expr),
    /// `WHERE expression`.
    Where(Expr),
}

impl ShowCharsetStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        restore_filtered_show(
            out,
            "CHARSET",
            self.filter.as_ref().map(ShowFilterRef::from),
        );
    }
}

/// TiDB's storage-engine catalog inspection statement.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowEnginesStmt {
    /// Optional filter over the storage-engine catalog rows.
    pub filter: Option<ShowEnginesFilter>,
}

/// The shared SHOW filter alternatives for `ENGINES`.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowEnginesFilter {
    /// `LIKE expression`.
    Like(Expr),
    /// `WHERE expression`.
    Where(Expr),
}

impl ShowEnginesStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        restore_filtered_show(
            out,
            "ENGINES",
            self.filter.as_ref().map(ShowFilterRef::from),
        );
    }
}

/// `SHOW OPEN TABLES [IN | FROM schema] [LIKE expression | WHERE expression]`.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowOpenTablesStmt {
    /// Optional schema name, restored with Go's canonical `IN` introducer.
    pub database: Option<String>,
    /// Optional shared SHOW filter.
    pub filter: Option<ShowInspectionFilter>,
}

impl ShowOpenTablesStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW OPEN TABLES");
        if let Some(database) = &self.database {
            out.push_str(" IN ");
            push_name_path(out, std::slice::from_ref(database));
        }
        restore_show_filter(out, self.filter.as_ref().map(ShowFilterRef::from));
    }
}

/// TiDB's histogram-bucket metadata inspection statement.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowStatsBucketsStmt {
    /// Optional filter over `mysql.stats_buckets` rows.
    pub filter: Option<ShowStatsBucketsFilter>,
}

/// The shared SHOW filter alternatives for `STATS_BUCKETS`.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowStatsBucketsFilter {
    /// `LIKE expression`.
    Like(Expr),
    /// `WHERE expression`.
    Where(Expr),
}

impl ShowStatsBucketsStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        restore_filtered_show(
            out,
            "STATS_BUCKETS",
            self.filter.as_ref().map(ShowFilterRef::from),
        );
    }
}

/// TiDB's locked-table-statistics metadata inspection statement.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowStatsLockedStmt {
    /// Optional filter over `mysql.stats_table_locked` rows.
    pub filter: Option<ShowStatsLockedFilter>,
}

/// The shared SHOW filter alternatives for `STATS_LOCKED`.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowStatsLockedFilter {
    /// `LIKE expression`.
    Like(Expr),
    /// `WHERE expression`.
    Where(Expr),
}

impl ShowStatsLockedStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        restore_filtered_show(
            out,
            "STATS_LOCKED",
            self.filter.as_ref().map(ShowFilterRef::from),
        );
    }
}

#[derive(Clone, Copy)]
enum ShowFilterRef<'a> {
    Like(&'a Expr),
    Where(&'a Expr),
}

macro_rules! impl_show_filter_ref {
    ($filter:ident) => {
        impl<'a> From<&'a $filter> for ShowFilterRef<'a> {
            fn from(filter: &'a $filter) -> Self {
                match filter {
                    $filter::Like(expr) => Self::Like(expr),
                    $filter::Where(expr) => Self::Where(expr),
                }
            }
        }
    };
}

impl_show_filter_ref!(ShowCharsetFilter);
impl_show_filter_ref!(ShowEnginesFilter);
impl_show_filter_ref!(ShowInspectionFilter);
impl_show_filter_ref!(ShowStatsBucketsFilter);
impl_show_filter_ref!(ShowStatsLockedFilter);

fn restore_filtered_show(out: &mut String, name: &str, filter: Option<ShowFilterRef<'_>>) {
    out.push_str("SHOW ");
    out.push_str(name);
    restore_show_filter(out, filter);
}

fn restore_show_filter(out: &mut String, filter: Option<ShowFilterRef<'_>>) {
    match filter {
        None => {}
        Some(ShowFilterRef::Like(expr)) => {
            out.push_str(" LIKE ");
            expr.restore_into(out);
        }
        Some(ShowFilterRef::Where(expr)) => {
            out.push_str(" WHERE ");
            expr.restore_into(out);
        }
    }
}

/// SHOW kinds that share Go's common optional database and LIKE/WHERE payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShowInspectionKind {
    /// `SHOW TRIGGERS`.
    Triggers,
    /// `SHOW PROCEDURE STATUS`.
    ProcedureStatus,
    /// `SHOW FUNCTION STATUS`.
    FunctionStatus,
    /// `SHOW EVENTS`.
    Events,
    /// `SHOW PLUGINS`.
    Plugins,
    /// `SHOW STATS_EXTENDED`.
    StatsExtended,
    /// `SHOW STATS_META`.
    StatsMeta,
    /// `SHOW STATS_HEALTHY`.
    StatsHealthy,
    /// `SHOW HISTOGRAMS_IN_FLIGHT`.
    HistogramsInFlight,
    /// `SHOW COLUMN_STATS_USAGE`.
    ColumnStatsUsage,
    /// `SHOW BINDING_CACHE STATUS`.
    BindingCacheStatus,
    /// `SHOW ANALYZE STATUS`.
    AnalyzeStatus,
    /// `SHOW BACKUPS`.
    Backups,
    /// `SHOW RESTORES`.
    Restores,
    /// `SHOW IMPORTS`.
    Imports,
    /// `SHOW CONFIG`.
    Config,
    /// `SHOW REPLICA STATUS`.
    ReplicaStatus,
    /// `SHOW BINARY LOG STATUS`.
    BinaryLogStatus,
    /// `SHOW PROFILES`.
    Profiles,
    /// `SHOW SESSION_STATES`.
    SessionStates,
    /// `SHOW [FULL] PROCESSLIST`.
    ProcessList,
    /// `SHOW AFFINITY`.
    Affinity,
}

impl ShowInspectionKind {
    fn restore_name(self) -> &'static str {
        match self {
            Self::Triggers => "TRIGGERS",
            Self::ProcedureStatus => "PROCEDURE STATUS",
            Self::FunctionStatus => "FUNCTION STATUS",
            Self::Events => "EVENTS",
            Self::Plugins => "PLUGINS",
            Self::StatsExtended => "STATS_EXTENDED",
            Self::StatsMeta => "STATS_META",
            Self::StatsHealthy => "STATS_HEALTHY",
            Self::HistogramsInFlight => "HISTOGRAMS_IN_FLIGHT",
            Self::ColumnStatsUsage => "COLUMN_STATS_USAGE",
            Self::BindingCacheStatus => "BINDING_CACHE STATUS",
            Self::AnalyzeStatus => "ANALYZE STATUS",
            Self::Backups => "BACKUPS",
            Self::Restores => "RESTORES",
            Self::Imports => "IMPORTS",
            Self::Config => "CONFIG",
            Self::ReplicaStatus => "REPLICA STATUS",
            Self::BinaryLogStatus => "BINARY LOG STATUS",
            Self::Profiles => "PROFILES",
            Self::SessionStates => "SESSION_STATES",
            Self::ProcessList => "PROCESSLIST",
            Self::Affinity => "AFFINITY",
        }
    }
}

/// The filter shared by Go's filterable SHOW targets.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowInspectionFilter {
    /// `LIKE expression`.
    Like(Expr),
    /// `WHERE expression`.
    Where(Expr),
}

/// A source-shaped payload for the repeated ordinary SHOW grammar.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowInspectionStmt {
    /// The inspected metadata family.
    pub kind: ShowInspectionKind,
    /// `FULL`, valid only for PROCESSLIST.
    pub full: bool,
    /// Optional database scope used by TRIGGERS and EVENTS.
    pub database: Option<String>,
    /// Optional result-row predicate.
    pub filter: Option<ShowInspectionFilter>,
}

impl ShowInspectionStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW ");
        if self.full {
            out.push_str("FULL ");
        }
        out.push_str(self.kind.restore_name());
        if let Some(database) = &self.database {
            out.push_str(" IN ");
            push_name_path(out, std::slice::from_ref(database));
        }
        match &self.filter {
            None => {}
            Some(ShowInspectionFilter::Like(expr)) => {
                out.push_str(" LIKE ");
                expr.restore_into(out);
            }
            Some(ShowInspectionFilter::Where(expr)) => {
                out.push_str(" WHERE ");
                expr.restore_into(out);
            }
        }
    }
}

/// The object kind of a `SHOW CREATE ...` statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShowCreateKind {
    /// `SHOW CREATE TABLE`.
    Table,
    /// `SHOW CREATE VIEW`.
    View,
    /// `SHOW CREATE SEQUENCE`.
    Sequence,
    /// `SHOW CREATE DATABASE` (also `SHOW CREATE SCHEMA`, restored as
    /// `DATABASE`).
    Database,
    /// `SHOW CREATE PROCEDURE`.
    Procedure,
    /// `SHOW CREATE PLACEMENT POLICY`.
    PlacementPolicy,
    /// `SHOW CREATE RESOURCE GROUP`.
    ResourceGroup,
}

/// TiDB's `SHOW WARNINGS` grammar form.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowWarningsStmt {
    /// Whether source used `SHOW COUNT(*) WARNINGS`.
    pub count_only: bool,
    /// Optional filter over the virtual `Level`, `Code`, and `Message` rows.
    pub filter: Option<ShowWarningsFilter>,
}

/// `SHOW MASKING POLICIES FOR table [WHERE expression]`.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowMaskingPoliciesStmt {
    /// Target table path.
    pub table: Vec<String>,
    /// Optional metadata-row predicate.
    pub where_clause: Option<Expr>,
}

impl ShowMaskingPoliciesStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW MASKING POLICIES FOR ");
        push_name_path(out, &self.table);
        if let Some(predicate) = &self.where_clause {
            out.push_str(" WHERE ");
            predicate.restore_into(out);
        }
    }
}

/// The two optional `SHOW WARNINGS` filters accepted by Go's parser.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowWarningsFilter {
    /// `LIKE <simple expression>`.
    Like(Expr),
    /// `WHERE <expression>`.
    Where(Expr),
}

impl ShowWarningsStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW WARNINGS");
        match &self.filter {
            None => {}
            Some(ShowWarningsFilter::Like(expr)) => {
                out.push_str(" LIKE ");
                expr.restore_into(out);
            }
            Some(ShowWarningsFilter::Where(expr)) => {
                out.push_str(" WHERE ");
                expr.restore_into(out);
            }
        }
    }
}

/// TiDB's `SHOW [GLOBAL | SESSION] STATUS` grammar form.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowStatusStmt {
    /// `GLOBAL` scope; `false` represents the explicit or implicit session
    /// scope that Go restores as `SESSION`.
    pub global: bool,
    /// Optional filter over the status-variable result rows.
    pub filter: Option<ShowStatusFilter>,
}

/// The mutually exclusive `SHOW STATUS` filters in Go's parser.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowStatusFilter {
    /// `LIKE <simple expression>`.
    Like(Expr),
    /// `WHERE <expression>`.
    Where(Expr),
}

impl ShowStatusStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str(if self.global {
            "SHOW GLOBAL STATUS"
        } else {
            "SHOW SESSION STATUS"
        });
        match &self.filter {
            None => {}
            Some(ShowStatusFilter::Like(expr)) => {
                out.push_str(" LIKE ");
                expr.restore_into(out);
            }
            Some(ShowStatusFilter::Where(expr)) => {
                out.push_str(" WHERE ");
                expr.restore_into(out);
            }
        }
    }
}

/// TiDB's `SHOW ERRORS` grammar form.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowErrorsStmt {
    /// Whether source used `SHOW COUNT(*) ERRORS`.
    pub count_only: bool,
    /// Optional filter over the virtual `Level`, `Code`, and `Message` rows.
    pub filter: Option<ShowErrorsFilter>,
}

/// The optional `SHOW ERRORS` filter accepted by Go's shared SHOW grammar.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowErrorsFilter {
    /// `LIKE <simple expression>`.
    Like(Expr),
    /// `WHERE <expression>`.
    Where(Expr),
}

impl ShowErrorsStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW ERRORS");
        match &self.filter {
            None => {}
            Some(ShowErrorsFilter::Like(expr)) => {
                out.push_str(" LIKE ");
                expr.restore_into(out);
            }
            Some(ShowErrorsFilter::Where(expr)) => {
                out.push_str(" WHERE ");
                expr.restore_into(out);
            }
        }
    }
}

/// TiDB's `SHOW COLLATION` grammar form.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowCollationStmt {
    /// Optional filter over virtual collation metadata.
    pub filter: Option<ShowCollationFilter>,
}

/// The two optional `SHOW COLLATION` filters accepted by Go's parser.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowCollationFilter {
    /// `LIKE <simple expression>`.
    Like(Expr),
    /// `WHERE <expression>`.
    Where(Expr),
}

impl ShowCollationStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW COLLATION");
        match &self.filter {
            None => {}
            Some(ShowCollationFilter::Like(expr)) => {
                out.push_str(" LIKE ");
                expr.restore_into(out);
            }
            Some(ShowCollationFilter::Where(expr)) => {
                out.push_str(" WHERE ");
                expr.restore_into(out);
            }
        }
    }
}

/// TiDB's `SHOW STATS_HISTOGRAMS` grammar form.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowStatsHistogramsStmt {
    /// Optional predicate over the virtual histogram metadata rows.
    pub filter: Option<ShowStatsHistogramsFilter>,
}

/// The optional `SHOW STATS_HISTOGRAMS` filter accepted by Go's shared SHOW
/// grammar.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowStatsHistogramsFilter {
    /// `LIKE <simple expression>`.
    Like(Expr),
    /// `WHERE <expression>`.
    Where(Expr),
}

impl ShowStatsHistogramsStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW STATS_HISTOGRAMS");
        match &self.filter {
            None => {}
            Some(ShowStatsHistogramsFilter::Like(expr)) => {
                out.push_str(" LIKE ");
                expr.restore_into(out);
            }
            Some(ShowStatsHistogramsFilter::Where(expr)) => {
                out.push_str(" WHERE ");
                expr.restore_into(out);
            }
        }
    }
}

/// TiDB's `SHOW STATS_TOPN` grammar form.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowStatsTopNStmt {
    /// Optional predicate over the virtual TopN statistics rows.
    pub filter: Option<ShowStatsTopNFilter>,
}

/// The optional filter accepted by Go's `SHOW STATS_TOPN` source entry.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowStatsTopNFilter {
    /// `LIKE <simple expression>`.
    Like(Expr),
    /// `WHERE <expression>`.
    Where(Expr),
}

impl ShowStatsTopNStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW STATS_TOPN");
        match &self.filter {
            None => {}
            Some(ShowStatsTopNFilter::Like(expr)) => {
                out.push_str(" LIKE ");
                expr.restore_into(out);
            }
            Some(ShowStatsTopNFilter::Where(expr)) => {
                out.push_str(" WHERE ");
                expr.restore_into(out);
            }
        }
    }
}

/// TiDB's `SHOW DATABASES` grammar form.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowDatabasesStmt {
    /// Optional virtual-schema filter.
    pub filter: Option<ShowDatabasesFilter>,
}

/// The optional filter carried by [`ShowDatabasesStmt`].
#[derive(Debug, Clone, PartialEq)]
pub enum ShowDatabasesFilter {
    /// `LIKE <simple expression>`.
    Like(Expr),
    /// `WHERE <expression>`.
    Where(Expr),
}

impl ShowDatabasesStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW DATABASES");
        match &self.filter {
            None => {}
            Some(ShowDatabasesFilter::Like(expr)) => {
                out.push_str(" LIKE ");
                expr.restore_into(out);
            }
            Some(ShowDatabasesFilter::Where(expr)) => {
                out.push_str(" WHERE ");
                expr.restore_into(out);
            }
        }
    }
}

/// The optional predicates accepted by Go's `SHOW TABLES` grammar.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowTablesFilter {
    /// `LIKE <simple expression>`.
    Like(Expr),
    /// `WHERE <expression>`.
    Where(Expr),
}

/// TiDB's `SHOW [FULL] TABLES` grammar form.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowTablesStmt {
    /// Whether source SQL contained the `FULL` modifier.
    pub full: bool,
    /// Optional database selected by `FROM` or `IN`, restored as `IN`.
    pub database: Option<String>,
    /// Optional predicate over the table metadata rows.
    pub filter: Option<ShowTablesFilter>,
}

impl ShowTablesStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str(if self.full {
            "SHOW FULL TABLES"
        } else {
            "SHOW TABLES"
        });
        if let Some(database) = &self.database {
            out.push_str(" IN ");
            push_name_path(out, std::slice::from_ref(database));
        }
        match &self.filter {
            None => {}
            Some(ShowTablesFilter::Like(expr)) => {
                out.push_str(" LIKE ");
                expr.restore_into(out);
            }
            Some(ShowTablesFilter::Where(expr)) => {
                out.push_str(" WHERE ");
                expr.restore_into(out);
            }
        }
    }
}

/// TiDB's `SHOW TABLE STATUS` grammar form.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowTableStatusStmt {
    /// Optional database selected by `FROM` or `IN` and restored as `IN`.
    pub database: Option<String>,
    /// Optional predicate over the virtual table-status metadata rows.
    pub filter: Option<ShowTableStatusFilter>,
}

/// The mutually exclusive `SHOW TABLE STATUS` filters in Go's parser.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowTableStatusFilter {
    /// `LIKE <simple expression>`.
    Like(Expr),
    /// `WHERE <expression>`.
    Where(Expr),
}

impl ShowTableStatusStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW TABLE STATUS");
        if let Some(database) = &self.database {
            out.push_str(" IN ");
            push_name_path(out, std::slice::from_ref(database));
        }
        match &self.filter {
            None => {}
            Some(ShowTableStatusFilter::Like(expr)) => {
                out.push_str(" LIKE ");
                expr.restore_into(out);
            }
            Some(ShowTableStatusFilter::Where(expr)) => {
                out.push_str(" WHERE ");
                expr.restore_into(out);
            }
        }
    }
}

/// `SHOW TABLE name NEXT_ROW_ID`, distinct from both `SHOW TABLES` and
/// `ADMIN SHOW table NEXT_ROW_ID` in Go's AST.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowTableNextRowIdStmt {
    /// The table whose allocator state is requested.
    pub table: Vec<String>,
}

/// The two table-placement inspection result families.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShowTablePlacementKind {
    /// `REGIONS`.
    Regions,
    /// `DISTRIBUTIONS`.
    Distributions,
}

/// `SHOW TABLE ... [PARTITION (...)] [INDEX ...] REGIONS|DISTRIBUTIONS`.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowTablePlacementStmt {
    /// Inspected table path.
    pub table: Vec<String>,
    /// Optional partition list.
    pub partitions: Vec<String>,
    /// Optional index, valid for REGIONS.
    pub index: Option<String>,
    /// Result family.
    pub kind: ShowTablePlacementKind,
    /// Optional row filter.
    pub filter: Option<ShowInspectionFilter>,
}

impl ShowTablePlacementStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW TABLE ");
        push_name_path(out, &self.table);
        if !self.partitions.is_empty() {
            out.push_str(" PARTITION(");
            for (index, partition) in self.partitions.iter().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }
                push_name_path(out, std::slice::from_ref(partition));
            }
            out.push(')');
        }
        if let Some(index) = &self.index {
            out.push_str(" INDEX ");
            push_name_path(out, std::slice::from_ref(index));
        }
        out.push_str(match self.kind {
            ShowTablePlacementKind::Regions => " REGIONS",
            ShowTablePlacementKind::Distributions => " DISTRIBUTIONS",
        });
        restore_inspection_filter(out, &self.filter);
    }
}

/// `SHOW DISTRIBUTION JOB id` or `SHOW DISTRIBUTION JOBS [filter]`.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowDistributionJobsStmt {
    /// Singular job identifier; absent for plural JOBS.
    pub job_id: Option<i64>,
    /// Optional plural-list filter.
    pub filter: Option<ShowInspectionFilter>,
}

impl ShowDistributionJobsStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW DISTRIBUTION ");
        if let Some(job_id) = self.job_id {
            out.push_str("JOB ");
            out.push_str(&job_id.to_string());
        } else {
            out.push_str("JOBS");
            restore_inspection_filter(out, &self.filter);
        }
    }
}

fn restore_inspection_filter(out: &mut String, filter: &Option<ShowInspectionFilter>) {
    match filter {
        None => {}
        Some(ShowInspectionFilter::Like(expr)) => {
            out.push_str(" LIKE ");
            expr.restore_into(out);
        }
        Some(ShowInspectionFilter::Where(expr)) => {
            out.push_str(" WHERE ");
            expr.restore_into(out);
        }
    }
}

/// Target of TiDB's placement inspection family.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShowPlacementTarget {
    /// All placement rules.
    All,
    /// One database.
    Database(String),
    /// One table.
    Table(Vec<String>),
    /// One table partition.
    Partition {
        /// Table path.
        table: Vec<String>,
        /// Partition name.
        partition: String,
    },
    /// Placement labels rather than rules.
    Labels,
}

/// `SHOW PLACEMENT` and its database/table/partition/labels variants.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowPlacementStmt {
    /// Selected placement target.
    pub target: ShowPlacementTarget,
    /// Filter accepted by the unscoped and LABELS forms.
    pub filter: Option<ShowInspectionFilter>,
}

impl ShowPlacementStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW PLACEMENT");
        match &self.target {
            ShowPlacementTarget::All => {}
            ShowPlacementTarget::Database(database) => {
                out.push_str(" FOR DATABASE ");
                push_name_path(out, std::slice::from_ref(database));
            }
            ShowPlacementTarget::Table(table) => {
                out.push_str(" FOR TABLE ");
                push_name_path(out, table);
            }
            ShowPlacementTarget::Partition { table, partition } => {
                out.push_str(" FOR TABLE ");
                push_name_path(out, table);
                out.push_str(" PARTITION ");
                push_name_path(out, std::slice::from_ref(partition));
            }
            ShowPlacementTarget::Labels => out.push_str(" LABELS"),
        }
        restore_inspection_filter(out, &self.filter);
    }
}

/// One `SHOW PROFILE` metric selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShowProfileType {
    /// CPU.
    Cpu,
    /// Memory.
    Memory,
    /// Block I/O.
    BlockIo,
    /// Context switches.
    ContextSwitches,
    /// Page faults.
    PageFaults,
    /// IPC.
    Ipc,
    /// Swaps.
    Swaps,
    /// Source locations.
    Source,
    /// All metrics.
    All,
}

impl ShowProfileType {
    fn restore_name(self) -> &'static str {
        match self {
            Self::Cpu => "CPU",
            Self::Memory => "MEMORY",
            Self::BlockIo => "BLOCK IO",
            Self::ContextSwitches => "CONTEXT SWITCHES",
            Self::PageFaults => "PAGE FAULTS",
            Self::Ipc => "IPC",
            Self::Swaps => "SWAPS",
            Self::Source => "SOURCE",
            Self::All => "ALL",
        }
    }
}

/// `SHOW PROFILE [types] [FOR QUERY id] [LIMIT ...]`.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowProfileStmt {
    /// Requested metric families.
    pub types: Vec<ShowProfileType>,
    /// Optional query identifier.
    pub query_id: Option<i64>,
    /// Optional result limit.
    pub limit: Option<crate::Limit>,
}

impl ShowProfileStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW PROFILE");
        for (index, kind) in self.types.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            out.push(' ');
            out.push_str(kind.restore_name());
        }
        if let Some(query_id) = self.query_id {
            out.push_str(" FOR QUERY ");
            out.push_str(&query_id.to_string());
        }
        if let Some(limit) = &self.limit {
            limit.restore_into(out);
        }
    }
}

impl ShowTableNextRowIdStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW TABLE ");
        push_name_path(out, &self.table);
        out.push_str(" NEXT_ROW_ID");
    }
}

/// TiDB's selected `SHOW COLUMNS` grammar form.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowColumnsStmt {
    /// Whether `FULL` was specified.
    pub full: bool,
    /// Whether `EXTENDED` was specified.
    pub extended: bool,
    /// The required table path after `FROM` or `IN`.
    pub table: Vec<String>,
    /// Optional filter over virtual column metadata rows.
    pub filter: Option<ShowColumnsFilter>,
}

/// The mutually exclusive `SHOW COLUMNS` filters accepted by this slice.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowColumnsFilter {
    /// `LIKE <simple expression>`.
    Like(Expr),
    /// `WHERE <expression>`.
    Where(Expr),
}

impl ShowColumnsStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW ");
        if self.extended {
            out.push_str("EXTENDED ");
        }
        if self.full {
            out.push_str("FULL ");
        }
        out.push_str("COLUMNS IN ");
        push_name_path(out, &self.table);
        match &self.filter {
            None => {}
            Some(ShowColumnsFilter::Like(expr)) => {
                out.push_str(" LIKE ");
                expr.restore_into(out);
            }
            Some(ShowColumnsFilter::Where(expr)) => {
                out.push_str(" WHERE ");
                expr.restore_into(out);
            }
        }
    }
}

/// TiDB's `SHOW INDEX` grammar form supported by this rewrite slice.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowIndexStmt {
    /// The table path written after the required `FROM` or `IN`.
    pub table: Vec<String>,
    /// Optional filter over the virtual index metadata rows.
    pub filter: Option<ShowIndexFilter>,
}

/// The mutually exclusive `SHOW INDEX` filters accepted by this slice.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowIndexFilter {
    /// `LIKE <simple expression>`.
    Like(Expr),
    /// `WHERE <expression>`.
    Where(Expr),
}

impl ShowIndexStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SHOW INDEX IN ");
        push_name_path(out, &self.table);
        match &self.filter {
            None => {}
            Some(ShowIndexFilter::Like(expr)) => {
                out.push_str(" LIKE ");
                expr.restore_into(out);
            }
            Some(ShowIndexFilter::Where(expr)) => {
                out.push_str(" WHERE ");
                expr.restore_into(out);
            }
        }
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for ShowCharsetStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { filter } = self;
        if let Some(value) = filter.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = filter;
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowCharsetFilter {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Like(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Where(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowEnginesStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { filter } = self;
        if let Some(value) = filter.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = filter;
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowEnginesFilter {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Like(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Where(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowOpenTablesStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { database, filter } = self;
        if let Some(value) = filter.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = database;
        let _ = filter;
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowStatsBucketsStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { filter } = self;
        if let Some(value) = filter.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = filter;
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowStatsBucketsFilter {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Like(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Where(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowStatsLockedStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { filter } = self;
        if let Some(value) = filter.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = filter;
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowStatsLockedFilter {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Like(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Where(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowInspectionKind {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Triggers => {}
            Self::ProcedureStatus => {}
            Self::FunctionStatus => {}
            Self::Events => {}
            Self::Plugins => {}
            Self::StatsExtended => {}
            Self::StatsMeta => {}
            Self::StatsHealthy => {}
            Self::HistogramsInFlight => {}
            Self::ColumnStatsUsage => {}
            Self::BindingCacheStatus => {}
            Self::AnalyzeStatus => {}
            Self::Backups => {}
            Self::Restores => {}
            Self::Imports => {}
            Self::Config => {}
            Self::ReplicaStatus => {}
            Self::BinaryLogStatus => {}
            Self::Profiles => {}
            Self::SessionStates => {}
            Self::ProcessList => {}
            Self::Affinity => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowInspectionFilter {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Like(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Where(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowInspectionStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            kind,
            full,
            database,
            filter,
        } = self;
        if !crate::Visitable::accept(kind, visitor) {
            return false;
        }
        if let Some(value) = filter.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = kind;
        let _ = full;
        let _ = database;
        let _ = filter;
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowCreateKind {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Table => {}
            Self::View => {}
            Self::Sequence => {}
            Self::Database => {}
            Self::Procedure => {}
            Self::PlacementPolicy => {}
            Self::ResourceGroup => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowWarningsStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { count_only, filter } = self;
        if let Some(value) = filter.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = count_only;
        let _ = filter;
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowMaskingPoliciesStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            table,
            where_clause,
        } = self;
        if let Some(value) = where_clause.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = table;
        let _ = where_clause;
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowWarningsFilter {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Like(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Where(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowStatusStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { global, filter } = self;
        if let Some(value) = filter.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = global;
        let _ = filter;
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowStatusFilter {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Like(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Where(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowErrorsStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { count_only, filter } = self;
        if let Some(value) = filter.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = count_only;
        let _ = filter;
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowErrorsFilter {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Like(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Where(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowCollationStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { filter } = self;
        if let Some(value) = filter.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = filter;
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowCollationFilter {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Like(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Where(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowStatsHistogramsStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { filter } = self;
        if let Some(value) = filter.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = filter;
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowStatsHistogramsFilter {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Like(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Where(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowStatsTopNStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { filter } = self;
        if let Some(value) = filter.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = filter;
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowStatsTopNFilter {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Like(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Where(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowDatabasesStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { filter } = self;
        if let Some(value) = filter.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = filter;
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowDatabasesFilter {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Like(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Where(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowTablesFilter {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Like(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Where(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowTablesStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            full,
            database,
            filter,
        } = self;
        if let Some(value) = filter.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = full;
        let _ = database;
        let _ = filter;
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowTableStatusStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { database, filter } = self;
        if let Some(value) = filter.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = database;
        let _ = filter;
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowTableStatusFilter {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Like(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Where(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowTableNextRowIdStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { table } = self;
        let _ = table;
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowTablePlacementKind {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Regions => {}
            Self::Distributions => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowTablePlacementStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            table,
            partitions,
            index,
            kind,
            filter,
        } = self;
        if !crate::Visitable::accept(kind, visitor) {
            return false;
        }
        if let Some(value) = filter.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = table;
        let _ = partitions;
        let _ = index;
        let _ = kind;
        let _ = filter;
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowDistributionJobsStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { job_id, filter } = self;
        if let Some(value) = filter.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = job_id;
        let _ = filter;
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowPlacementTarget {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::All => {}
            Self::Database(field_0) => {
                let _ = field_0;
            }
            Self::Table(field_0) => {
                let _ = field_0;
            }
            Self::Partition { table, partition } => {
                let _ = table;
                let _ = partition;
            }
            Self::Labels => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowPlacementStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { target, filter } = self;
        if !crate::Visitable::accept(target, visitor) {
            return false;
        }
        if let Some(value) = filter.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = target;
        let _ = filter;
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowProfileType {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Cpu => {}
            Self::Memory => {}
            Self::BlockIo => {}
            Self::ContextSwitches => {}
            Self::PageFaults => {}
            Self::Ipc => {}
            Self::Swaps => {}
            Self::Source => {}
            Self::All => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowProfileStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            types,
            query_id,
            limit,
        } = self;
        for value in types.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if let Some(value) = limit.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = types;
        let _ = query_id;
        let _ = limit;
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowColumnsStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            full,
            extended,
            table,
            filter,
        } = self;
        if let Some(value) = filter.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = full;
        let _ = extended;
        let _ = table;
        let _ = filter;
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowColumnsFilter {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Like(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Where(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowIndexStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { table, filter } = self;
        if let Some(value) = filter.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = table;
        let _ = filter;
        visitor.leave(self)
    }
}

impl crate::Visitable for ShowIndexFilter {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Like(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Where(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
